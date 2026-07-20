#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sassrv/ev_rv_client.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raimd/md_replay.h>
#include <raimd/cfile.h>
#include <raimd/app_a.h>
#include <raimd/enum_def.h>
#include <raimd/rv_msg.h>
#include <raimd/sass.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace md;

/* replayrv: push an MDReplay format capture file to the network at a
 * predictable rate.  The MDReplay format is a sequence of records:
 *
 *   <subject> '\n'
 *   <msglen>  '\n'
 *   <msglen bytes of message data>
 *
 * (the same format read_msg / rename_msg in raimd consume, and what
 * rename_msg emits:  printf( "%s\n%u\n", subj, len ) + raw bytes)
 *
 * Rate control follows the rv_pub model: a recurrent timer at the message
 * interval wakes the publisher, which catches the published count up to
 * elapsed_seconds * rate, so the average rate is exact and self-correcting
 * regardless of timer jitter or transient back-pressure.
 *
 * -3 wraps each record in a SASS3 feed broadcast envelope and publishes it
 * to _SASS.<feed>.PUB, where <feed> is the first segment of the record's
 * subject (no -p prefix is applied; a literal _TIC. prefix on the record
 * is stripped).  The envelope matches what raicache Sass3Svc::doFeed()
 * parses (RaiCore sass3_svc.cpp):
 *
 *   { M : 23177 (SASS3_PUB_MAGIC),
 *     T : MSG_TYPE (payload's MSG_TYPE, default UPDATE = 1),
 *     D : { <subject> : <opaque message bytes> } }
 *
 * fields the parser defaults to zero (S rec status, I indicator, E expires)
 * are omitted. */

static const uint32_t PUB_TIMER_ID = 3;
static const uint16_t SASS3_PUB_MAGIC = 23177;

struct ReplayCB : public EvConnectionNotify, public RvClientCB,
                  public EvTimerCallback, public BPData {
  EvPoll     & poll;             /* poll loop data */
  EvRvClient & client;           /* connection to rv */
  MDDict     * dict;             /* optional cfile dict for verbose print */
  MDReplay     replay;           /* the capture file */
  const char * fname,            /* replay file name, NULL/'-' = stdin */
             * prefix;
  uint64_t     rate_per_sec,     /* -m msgs/sec */
               loops,            /* -l times through the file, 0 = forever */
               loop_cnt,         /* completed passes */
               current_pub,      /* msgs published */
               rate_accum,       /* elapsed * rate = pubs allowed so far */
               start_time_ns;    /* first publish time */
  bool         started,          /* first() called */
               have_rec,         /* parsed record not yet published */
               has_rate_timer,
               done,
               sass3,            /* -3: SASS3 PUB envelope to _SASS.<feed>.PUB */
               verbose;

  ReplayCB( EvPoll &p,  EvRvClient &c,  const char *fn,  uint64_t rate,
            uint64_t lp,  bool s3,  bool verb )
    : poll( p ), client( c ), dict( 0 ), fname( fn ), prefix( "_TIC." ),
      rate_per_sec( rate ), loops( lp ), loop_cnt( 0 ), current_pub( 0 ),
      rate_accum( 0 ), start_time_ns( 0 ), started( false ), have_rec( false ),
      has_rate_timer( false ), done( false ), sass3( s3 ), verbose( verb ) {
    this->bp_flags = BP_FORWARD | BP_NOTIFY;
  }
  bool open_input( void ) noexcept;
  void finish( void ) noexcept;
  void run_publisher( void ) noexcept;
  void start_pub_timer( void ) noexcept;
  bool calc_rate_limit( void ) noexcept;
  /* after CONNECTED message */
  virtual void on_connect( EvSocket &conn ) noexcept;
  /* when disconnected */
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* message from network (nothing subscribed, inbox traffic only) */
  virtual bool on_rv_msg( EvPublish &pub ) noexcept;
  /* flush send, ready to send more */
  virtual void on_write_ready( void ) noexcept;
};

bool
ReplayCB::open_input( void ) noexcept
{
  this->started = false;
  if ( this->fname == NULL || ::strcmp( this->fname, "-" ) == 0 ) {
    this->replay.input = stdin; /* open( "-" ) leaves input NULL and next()
                                 * declines to read; feed it stdin directly */
    return true;
  }
  return this->replay.open( this->fname );
}

/* called after daemon responds with CONNECTED message */
void
ReplayCB::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  fflush( stdout );
  this->run_publisher();
}

void
ReplayCB::start_pub_timer( void ) noexcept
{
  uint32_t t;
  t = (uint32_t) ( 1000000.0 / (double) this->rate_per_sec );
  if ( t < 10 ) t = 10;
  this->poll.timer.add_timer_micros( *this, t, PUB_TIMER_ID, 0 );
  this->has_rate_timer = true;
}

bool
ReplayCB::calc_rate_limit( void ) noexcept
{
  if ( this->rate_per_sec == 0 )
    return false;
  if ( ! this->has_rate_timer )
    this->start_pub_timer();

  uint64_t ns = this->poll.current_mono_ns();
  if ( this->start_time_ns == 0 )
    this->start_time_ns = ns;
  if ( ns == this->start_time_ns )
    this->rate_accum = 0;
  else {
    this->rate_accum = (uint64_t) (
      (double) ( ( ns - this->start_time_ns ) / 1000000000.0 ) *
        (double) this->rate_per_sec );
  }
  return true;
}

void
ReplayCB::finish( void ) noexcept
{
  if ( this->done )
    return;
  this->done = true;
  uint64_t ns = this->poll.current_mono_ns();
  double   el = ( this->start_time_ns == 0 ? 0.0 :
                  (double) ( ns - this->start_time_ns ) / 1000000000.0 );
  printf( "replayed %llu msgs, %llu pass%s, %.1f secs (%.1f msgs/sec)\n",
          (unsigned long long) this->current_pub,
          (unsigned long long) this->loop_cnt,
          this->loop_cnt == 1 ? "" : "es", el,
          el > 0.0 ? (double) this->current_pub / el : 0.0 );
  fflush( stdout );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1;
}

/* publish records until caught up with the rate accumulator */
void
ReplayCB::run_publisher( void ) noexcept
{
  bool is_rate_limited;

  if ( this->done || this->poll.quit != 0 )
    return;
  is_rate_limited = this->calc_rate_limit();
  for (;;) {
    if ( this->poll.quit != 0 )
      return;
    if ( is_rate_limited && this->current_pub >= this->rate_accum )
      return; /* rate timer re-enters */
    if ( ! this->have_rec ) {
      bool b;
      if ( ! this->started ) {
        b = this->replay.first();
        this->started = true;
      }
      else
        b = this->replay.next();
      if ( ! b ) { /* end of file */
        this->loop_cnt++;
        bool is_stdin = ( this->fname == NULL ||
                          ::strcmp( this->fname, "-" ) == 0 );
        if ( ! is_stdin &&
             ( this->loops == 0 || this->loop_cnt < this->loops ) ) {
          this->replay.close();
          if ( this->open_input() )
            continue; /* next pass */
        }
        this->finish();
        return;
      }
      this->have_rec = true;
    }
    /* sniff the payload for its type id (RVMSG, TIBMSG, JSON, ...) */
    uint32_t msg_enc = 0;
    MDMsgMem mem;
    MDMsg  * m = MDMsg::unpack( this->replay.msgbuf, 0, this->replay.msglen,
                                0, this->dict, mem );
    if ( m != NULL )
      msg_enc = m->get_type_id();

    if ( this->verbose ) {
      printf( "## %.*s (%u bytes%s%s)\n", (int) this->replay.subjlen,
              this->replay.subj, (uint32_t) this->replay.msglen,
              m != NULL ? ", " : "",
              m != NULL ? m->get_proto_string() : "" );
      if ( m != NULL ) {
        MDOutput mout;
        m->print( &mout );
      }
      fflush( stdout );
    }
    const char * sub     = this->replay.subj;
    char         subj[ 1024 ];
    size_t       len     = this->replay.subjlen;
    void       * msg_buf = this->replay.msgbuf;
    size_t       msg_len = this->replay.msglen;

    if ( this->sass3 ) {
      /* the data subject is the record subject, sans any _TIC. prefix;
       * the -p prefix is not applied in envelope mode */
      size_t slen = len;
      if ( slen > 5 && ::memcmp( sub, "_TIC.", 5 ) == 0 ) {
        sub  += 5;
        slen -= 5;
      }
      /* feed = first segment of the data subject */
      const char * dot = (const char *) ::memchr( sub, '.', slen );
      size_t       feed_len = ( dot == NULL ? slen : (size_t) ( dot - sub ) );
      /* nul-terminated data subject = the D field name */
      char * dsubj = (char *) mem.make( slen + 1 );
      ::memcpy( dsubj, sub, slen );
      dsubj[ slen ] = '\0';

      /* T from the payload's MSG_TYPE when present, default UPDATE (1) */
      uint16_t msg_type = MD_UPDATE_TYPE;
      if ( m != NULL ) {
        MDFieldReader rd( *m );
        uint16_t      t;
        if ( rd.find( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN ) &&
             rd.get_uint( t ) )
          msg_type = t;
      }
      /* { M : magic, T : msg type, D : { subject : opaque payload } } */
      size_t sz = this->replay.msglen + slen + 128;
      RvMsgWriter env( mem, mem.make( sz ), sz );
      env.append_uint( "M", 2, SASS3_PUB_MAGIC )
         .append_uint( "T", 2, msg_type );
      RvMsgWriter d( mem, NULL, 0 );
      env.append_msg( "D", 2, d );
      d.append_opaque( dsubj, slen + 1, this->replay.msgbuf,
                       this->replay.msglen );
      env.update_hdr( d );
      msg_buf = env.buf;
      msg_len = env.off;
      msg_enc = RVMSG_TYPE_ID;
      /* publish to _SASS.<feed>.PUB */
      len = (size_t) ::snprintf( subj, sizeof( subj ), "_SASS.%.*s.PUB",
                                 (int) feed_len, sub );
      sub = subj;
    }
    else if ( this->prefix != NULL ) {
      len = (size_t) ::snprintf( subj, sizeof( subj ), "%s%.*s",
                                 this->prefix, (int) len, sub );
      sub = subj;
    }
    EvPublish pub( sub, len, NULL, 0, msg_buf, msg_len,
                   this->client.sub_route, this->client, 0, msg_enc );
    this->current_pub++;
    this->have_rec = false; /* queued even on flow control */
    if ( ! this->client.publish( pub ) ) {
      /* wait for ready */
      if ( this->has_back_pressure( this->poll, this->client.fd ) )
        return; /* on_write_ready re-enters */
    }
  }
}

void
ReplayCB::on_write_ready( void ) noexcept
{
  this->run_publisher();
}

bool
ReplayCB::timer_cb( uint64_t timer_id,  uint64_t ) noexcept
{
  if ( timer_id == PUB_TIMER_ID ) {
    if ( this->done )
      return false; /* stop timer */
    this->run_publisher();
    return true;
  }
  return false;
}

/* when client connection stops */
void
ReplayCB::on_shutdown( EvSocket &conn,  const char *err,
                       size_t errlen ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  fflush( stdout );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1;
}

bool
ReplayCB::on_rv_msg( EvPublish & ) noexcept
{
  return true; /* nothing subscribed */
}

static const char *
get_arg( int &x, int argc, const char *argv[], int b, const char *f,
         const char *g, const char *def ) noexcept
{
  for ( int i = 1; i < argc - b; i++ ) {
    if ( ::strcmp( f, argv[ i ] ) == 0 || ::strcmp( g, argv[ i ] ) == 0 ) {
      if ( x < i + b + 1 )
        x = i + b + 1;
      return argv[ i + b ];
    }
  }
  return def; /* default value */
}

int
main( int argc, const char *argv[] )
{
  SignalHandler sighndl;
  int x = 1, idle_count = 0;
  const char * daemon   = get_arg( x, argc, argv, 1, "-d", "-daemon", "tcp:7500" ),
             * network  = get_arg( x, argc, argv, 1, "-n", "-network", "" ),
             * service  = get_arg( x, argc, argv, 1, "-s", "-service", "7500" ),
             * prefix   = get_arg( x, argc, argv, 1, "-p", "-prefix", "_TIC." ),
             * user     = get_arg( x, argc, argv, 1, "-u", "-user", "replayrv" ),
             * path     = get_arg( x, argc, argv, 1, "-c", "-cfile", NULL ),
             * file     = get_arg( x, argc, argv, 1, "-f", "-file", NULL ),
             * msg_rate = get_arg( x, argc, argv, 1, "-m", "-rate", "100" ),
             * loop     = get_arg( x, argc, argv, 1, "-l", "-loop", "1" ),
             * sass3    = get_arg( x, argc, argv, 0, "-3", "-sass3", NULL ),
             * verbose  = get_arg( x, argc, argv, 0, "-v", "-verbose", NULL ),
             * help     = get_arg( x, argc, argv, 0, "-h", "-help", 0 );

  if ( help != NULL ) {
  help:;
    fprintf( stderr,
      "%s [-d daemon] [-n network] [-s service] [-f file] [-m rate] [-l loops]\n"
      "  -d daemon  = daemon port to connect (tcp:7500)\n"
      "  -n network = network\n"
      "  -s service = service (7500)\n"
      "  -u user    = user name (replayrv)\n"
      "  -p prefix  = prefix subject (_TIC.)\n"
      "  -c cfile   = load dictionary from cfiles (for -v decoding)\n"
      "  -f file    = MDReplay capture file (default '-' = stdin)\n"
      "               format: subject '\\n' msglen '\\n' msg-bytes, repeated\n"
      "  -m rate    = msgs per second (100; 0 = as fast as possible)\n"
      "  -l loops   = passes through the file (1; 0 = loop forever)\n"
      "  -3         = wrap records in a SASS3 PUB envelope { M, T, D } and\n"
      "               publish to _SASS.<feed>.PUB; <feed> = first segment\n"
      "               of the record subject (-p prefix is not applied)\n"
      "  -v         = print each message published\n",
      argv[ 0 ] );
    return 1;
  }
  if ( ! valid_uint64( msg_rate, ::strlen( msg_rate ) ) ) {
    fprintf( stderr, "Invalid -m/-rate\n" );
    goto help;
  }
  if ( ! valid_uint64( loop, ::strlen( loop ) ) ) {
    fprintf( stderr, "Invalid -l/-loop\n" );
    goto help;
  }
  uint64_t rate  = string_to_uint64( msg_rate, ::strlen( msg_rate ) ),
           loops = string_to_uint64( loop, ::strlen( loop ) );
  bool     is_stdin = ( file == NULL || ::strcmp( file, "-" ) == 0 );
  if ( is_stdin && loops != 1 ) {
    fprintf( stderr, "-l loops needs a seekable -f file, not stdin\n" );
    return 1;
  }

  EvPoll poll;
  poll.init( 5, false );

  EvRvClientParameters parm( daemon, network, service, user, 0 );
  EvRvClient           conn( poll );
  ReplayCB             data( poll, conn, file, rate, loops,
                             sass3 != NULL, verbose != NULL );
  if ( prefix != NULL && prefix[ 0 ] != '\0' )
    data.prefix = prefix;
  else
    data.prefix = NULL;
  /* open before connecting so a bad file fails fast */
  if ( ! data.open_input() ) {
    fprintf( stderr, "Failed to open replay file %s\n", file );
    return 1;
  }
  /* optional dictionary from cfiles, for -v field decoding */
  if ( path != NULL || (path = ::getenv( "cfile_path" )) != NULL ) {
    MDDictBuild dict_build;
    if ( AppA::parse_path( dict_build, path, "RDMFieldDictionary" ) == 0 ) {
      EnumDef::parse_path( dict_build, path, "enumtype.def" );
      dict_build.index_dict( "app_a", data.dict );
    }
    dict_build.clear_build();
    if ( CFile::parse_path( dict_build, path, "tss_fields.cf" ) == 0 ) {
      CFile::parse_path( dict_build, path, "tss_records.cf" );
      dict_build.index_dict( "cfile", data.dict );
    }
    if ( data.dict != NULL )
      printf( "Loaded dictionary from cfiles\n" );
  }
  /* connect to daemon */
  if ( ! conn.rv_connect( parm, &data, &data ) ) {
    fprintf( stderr, "Failed to connect to daemon\n" );
    return 1;
  }
  /* handle ctrl-c */
  sighndl.install();
  for (;;) {
    /* loop 5 times before quiting, time to flush writes */
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    /* dispatch network events */
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    /* wait for network events */
    poll.wait( idle_count > 255 ? 100 : 0 );
    if ( sighndl.signaled ) {
      if ( poll.quit == 0 )
        data.finish();
      poll.quit++;
    }
  }
  return 0;
}
