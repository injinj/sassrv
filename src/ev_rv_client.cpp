#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raikv/ev_publish.h>
#include <raikv/pattern_cvt.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/mc.h>
#include <raimd/json_msg.h>
#include <raimd/tib_msg.h>
#include <raimd/tib_sass_msg.h>
#include <raimd/rwf_msg.h>
#include <raimd/mf_msg.h>

using namespace rai;
using namespace sassrv;
using namespace kv;
using namespace md;
using namespace trdp;

extern "C" {
EvConnection *
rv_create_connection( EvPoll *p,  RoutePublish *sr,  EvConnectionNotify *n )
{
  return new ( aligned_malloc( sizeof( EvRvClient ) ) )
    EvRvClient( *p, *sr, n );
}
}

int rv_client_pub_verbose,
    rv_client_msg_verbose,
    rv_client_sub_verbose,
    rv_client_init;

static int
getenv_bool( const char *var )
{
  const char *val = ::getenv( var );
  return val != NULL && val[ 0 ] != 'f' && val[ 0 ] != '0';
}

static void
rv_client_static_init( void ) noexcept
{
  rv_client_init = 1;
  rv_client_pub_verbose   = getenv_bool( "RV_CLIENT_PUB_VERBOSE" );
  rv_client_msg_verbose   = getenv_bool( "RV_CLIENT_MSG_VERBOSE" );
  rv_client_sub_verbose   = getenv_bool( "RV_CLIENT_SUB_VERBOSE" );
  int all_verbose         = getenv_bool( "RV_CLIENT_VERBOSE" );
  rv_client_pub_verbose  |= all_verbose;
  rv_client_msg_verbose  |= all_verbose;
  rv_client_sub_verbose  |= all_verbose;
}

EvRvClient::EvRvClient( EvPoll &p,  RoutePublish &sr,  EvConnectionNotify *n ) noexcept
  : EvConnection( p, p.register_type( "rvclient" ), n ),
    RouteNotify( sr ), sub_route( sr ), cb( 0 ),
    rv_state( VERS_RECV ), fwd_all_msgs( 0 ), fwd_all_subs( 1 ),
    network( 0 ), service( 0 ), save_buf( 0 ), param_buf( 0 ), save_len( 0 ),
    svc( 0 ), sub_db( *this, this ), timer_id( 0 )
{
  this->start_stamp = kv_current_realtime_ns();
  if ( ! rv_client_init )
    rv_client_static_init();
  md_init_auto_unpack();
}

EvRvClient::EvRvClient( EvPoll &p ) noexcept
  : EvConnection( p, p.register_type( "rvclient" ) ),
    RouteNotify( p.sub_route ), sub_route( p.sub_route ),
    cb( 0 ), rv_state( VERS_RECV ), fwd_all_msgs( 0 ), fwd_all_subs( 1 ),
    network( 0 ), service( 0 ), save_buf( 0 ), param_buf( 0 ), save_len( 0 ),
    svc( 0 ), sub_db( *this, this )
{
  this->start_stamp = kv_current_realtime_ns();
  if ( ! rv_client_init )
    rv_client_static_init();
  md_init_auto_unpack();
}

bool RvClientCB::on_rv_msg( EvPublish & ) noexcept { return true; }

/* restart the protocol parser */
void
EvRvClient::initialize_state( bool is_null ) noexcept
{
  this->cb          = NULL;
  this->rv_state    = VERS_RECV;
  this->no_write    = false;
  this->session_len = 0;
  this->control_len = 0;
  this->userid_len  = 0;
  this->gob_len     = 0;
  this->vmaj        = 5;
  this->vmaj        = 4;
  this->vupd        = 2;
  this->cid         = 0;
  this->ipport      = 0;
  this->ipaddr      = 0;
  this->daemon      = NULL;
  this->network     = NULL;
  this->service     = NULL;
  this->notify      = NULL;
  if ( this->save_buf != NULL )
    ::free( this->save_buf );
  if ( this->param_buf != NULL )
    ::free( this->param_buf );
  this->save_buf    = NULL;
  this->param_buf   = NULL;
  this->save_len    = 0;
  this->inter_subs.release();
  this->bcast_subs.release();
  this->listen_subs.release();
  if ( is_null ) {
    this->ipaddr = 0x7f000001;
    this->ipport = 0x1234;
    while ( this->gob_len < sizeof( this->gob ) - 1 )
      this->gob[ this->gob_len++ ] = 1;
    this->gob[ this->gob_len ] = '\0';
#if 0
    char * ptr = this->session;
    uint8_t * ip = (uint8_t *) (void *) &this->ipaddr;
    /* <ipaddr>.<pid><time><ptr> */
    for ( size_t i = 0; i < 8; i += 2 ) {
      *ptr++ = hexchar2( ( ip[ i/2 ] >> 4 ) & 0xf );
      *ptr++ = hexchar2( ip[ i/2 ] & 0xf );
    }
    *ptr++ = '.';
    ptr += RvHost::time_to_str( this->start_stamp, ptr );
    this->session_len = (uint16_t) ( ptr - this->session );
#endif
    this->session[ 0 ] = '\0';
    this->session_len = 0;
    this->control_len = this->make_inbox( this->control, 1 );
    this->rv_state = DATA_RECV;
    this->no_write = true;
  }
}

int
EvRvClient::connect( EvConnectParam &param ) noexcept
{
  EvRvClientParameters parm2;
  parm2.ai     = param.ai;
  parm2.k      = param.k;
  parm2.opts   = param.opts;
  parm2.rte_id = param.rte_id;

  for ( int i = 0; i + 1 < param.argc; i += 2 ) {
    if ( ::strcmp( param.argv[ i ], "daemon" ) == 0 )
      parm2.daemon = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "connect" ) == 0 )
      parm2.daemon = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "host" ) == 0 )
      parm2.daemon = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "network" ) == 0 )
      parm2.network = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "service" ) == 0 )
      parm2.service = param.argv[ i + 1 ];
    else if ( ::strcmp( param.argv[ i ], "user" ) == 0 )
      parm2.userid = param.argv[ i + 1 ];
  }
  if ( this->rv_connect( parm2, param.n, NULL ) ) {
    for ( int i = 0; i + 1 < param.argc; i += 2 ) {
      if ( ::strcmp( param.argv[ i ], "broadcast_feed" ) == 0 )
        this->bcast_subs.add( param.argv[ i + 1 ] );
      else if ( ::strcmp( param.argv[ i ], "interactive_feed" ) == 0 )
        this->inter_subs.add( param.argv[ i + 1 ] );
      else if ( ::strcmp( param.argv[ i ], "subscriber_listen" ) == 0 ) {
        this->listen_subs.add( param.argv[ i + 1 ] );
        this->sub_db.add_wildcard( param.argv[ i + 1 ] );
      }
    }
    return 0;
  }
  return -1;
}

bool
EvRvClient::rv_connect( EvRvClientParameters &p,
                        EvConnectionNotify *n,
                        RvClientCB *c ) noexcept
{
  char * conn = NULL, buf[ 256 ];
  size_t daemon_len = 0;
  int port = p.port;
  bool is_null = false;
  if ( this->fd != -1 )
    return false;
  if ( p.ai == NULL ) {
    if ( p.daemon != NULL ) {
      daemon_len = ::strlen( p.daemon );
      size_t len = daemon_len >= sizeof( buf ) ? sizeof( buf ) - 1 : daemon_len;
      ::memcpy( buf, p.daemon, len );
      buf[ len ] = '\0';
      conn = buf;
    }
    if ( conn != NULL ) {
      char * pt;
      if ( (pt = ::strrchr( conn, ':' )) != NULL ) {
        port = atoi( pt + 1 );
        *pt = '\0';
      }
      else {
        for ( pt = conn; *pt != '\0'; pt++ )
          if ( *pt < '0' || *pt > '9' )
            break;
        if ( *pt == '\0' ) {
          port = atoi( conn );
          conn = NULL;
        }
      }
      if ( conn != NULL ) { /* strip tcp: prefix */
        if ( ::strncmp( conn, "tcp:", 4 ) == 0 )
          conn += 4;
        if ( ::strcmp( conn, "tcp" ) == 0 )
          conn += 3;
        if ( conn[ 0 ] == '\0' )
          conn = NULL;
        if ( conn != NULL && ::strcmp( conn, "null" ) == 0 )
          is_null = true;
      }
    }
  }
  this->initialize_state( is_null );
  if ( ! is_null ) {
    if ( p.ai == NULL ) {
      if ( port == 0 )
        port = 7500;
      if ( EvTcpConnection::connect( *this, conn, port, p.opts ) != 0 ) {
        this->rv_state = ERR_CLOSE;
        return false;
      }
    }
    else {
      EvConnectParam param( p.ai, p.opts, p.k, p.rte_id );
      if ( EvTcpConnection::connect3( *this, param ) != 0 ) {
        this->rv_state = ERR_CLOSE;
        return false;
      }
    }
  }
  else {
    this->fd = this->poll.get_null_fd();
    this->sock_opts = OPT_NO_POLL;
    this->PeerData::init_peer( this->poll.get_next_id(), this->fd,
                               -1, NULL, "null" );
    this->poll.add_sock( this );
  }

  const char * colon, * svc = p.service;
  char         svcbuf[ 16 ];
  if ( svc != NULL && (colon = ::strchr( svc, ':' )) != NULL ) {
    if ( (size_t) ( &colon[ 1 ] - svc ) < sizeof( svcbuf ) ) {
      size_t svclen = colon - svc;
      ::memcpy( svcbuf, svc, svclen );
      svcbuf[ svclen ] = '\0';
      svc = svcbuf;
    }
  }
  if ( daemon_len > 0 || p.network != NULL || svc != NULL ) {
    this->param_buf = ::malloc( daemon_len + 1 + CatPtr::a( p.network, svc ) );
    CatPtr cat( this->param_buf );
    this->daemon  = cat.cstr( p.daemon, daemon_len );
    this->network = cat.cstr( p.network );
    this->service = cat.cstr( svc );
  }
  if ( p.userid != NULL ) {
    size_t user_len = ::strlen( p.userid ) + 1;
    if ( user_len > sizeof( this->userid ) )
      user_len = sizeof( this->userid );
    ::memcpy( this->userid, p.userid, user_len );
    this->userid[ user_len - 1 ] = '\0';
    this->userid_len = user_len;
  }
  if ( n != NULL )
    this->notify = n;
  if ( c != NULL )
    this->cb = c;

  if ( is_null ) {
    if ( this->network != NULL ) {
      this->svc = TrdpSvc::create( this->poll, this->network, this->service );
      if ( this->svc != NULL )
        this->svc->db.conn = this;
    }
    if ( this->bcast_subs.count > 0 ) {
      for ( size_t i = 0; i < this->bcast_subs.count; i++ ) {
        const char *s = this->bcast_subs.ptr[ i ];
        this->subscribe( s, ::strlen( s ), NULL, 0 );
      }
    }
    if ( this->listen_subs.count > 0 ) {
      this->sub_db.start_subscriptions( false );
      this->timer_id = this->poll.mono_ns;
      this->poll.timer.add_timer_seconds( this->fd, 1, this->timer_id, 0 );
    }
    if ( this->notify != NULL )
      this->notify->on_connect( *this );
  }
  return true;
}

bool
EvRvClient::timer_expire( uint64_t timer_id,  uint64_t ) noexcept
{
  if ( this->timer_id == timer_id ) {
    this->sub_db.process_events();
    return true;
  }
  return false;
}

void
EvRvClient::set_prefix( const char *pref,  size_t preflen ) noexcept
{
  this->msg_in.set_prefix( pref, preflen );
}

void
EvRvClient::process( void ) noexcept
{
  uint32_t buflen, msglen;
  int      status = 0;

  /* state from VERS_RECV->INFO_RECV->INIT_RECV->CONN_RECV->DATA_RECV */
  if ( this->rv_state >= INIT_RECV ) { /* main state, rv msg envelope */
  data_recv_loop:;
    do {
      buflen = this->len - this->off;
      if ( buflen < 8 )
        goto break_loop;
      msglen = get_u32<MD_BIG>( &this->recv[ this->off ] );
      if ( buflen < msglen ) {
        this->recv_need( msglen );
        goto break_loop;
      }
      if ( rv_client_msg_verbose || rv_debug )
        this->trace_msg( '<', &this->recv[ this->off ], msglen );
      status = this->dispatch_msg( &this->recv[ this->off ], msglen );
      this->off += msglen;
    } while ( status == 0 );
  }
  else { /* initial connection states */
    for (;;) {
      buflen = this->len - this->off;
      if ( buflen < 8 )
        goto break_loop;
      switch ( this->rv_state ) {
        case VERS_RECV:
          if ( buflen < 3 * 4 )
            goto break_loop;
          if ( rv_client_msg_verbose || rv_debug )
            this->trace_msg( '<', &this->recv[ this->off ], buflen );
          this->off += 3 * 4;
          this->send_vers();          /* recv 12 bytes, send 12 bytes */
          this->rv_state = INFO_RECV;
          break;
        case INFO_RECV:
          if ( buflen < 16 * 4 )
            goto break_loop;
          if ( rv_client_msg_verbose || rv_debug )
            this->trace_msg( '<', &this->recv[ this->off ], buflen );
          if ( get_u32<MD_BIG>( &this->recv[ this->off ] ) != 1 )
            this->send_info();       /* recv 64, send 64, recv 64 */
          else {
            this->send_init_rec();   /* send network, service parms */
            this->rv_state = INIT_RECV;
          }
          this->off += 16 * 4;
          break;
        default: /* rvmsg encapaulation */
          goto data_recv_loop;
      }
    }
  }
break_loop:;
  this->pop( EV_PROCESS );
  if ( ! this->push_write() )
    this->clear_write_buffers();
  if ( status != 0 ) {
    this->rv_state = ERR_CLOSE;
    this->push( EV_CLOSE );
  }
}

void
EvRvClient::trace_msg( char dir,  void *msg,  size_t msglen ) noexcept
{
  MDOutput out;
  MDMsgMem mem;
  RvMsg  * m;
  printf( " === %s\n", dir == '>' ? "send ->" : "recv <-" );

  m = RvMsg::unpack_rv( msg, 0, msglen, 0, NULL, mem );
  if ( m != NULL )
    m->print( &out );
  else {
    out.print_hex( msg, msglen );
  }
}

void
EvRvClient::send_vers( void ) noexcept
{
  uint32_t ver_rec[ 3 ] = { 0, 4, 0 };
  ver_rec[ 1 ] = get_u32<MD_BIG>( &ver_rec[ 1 ] );
  if ( rv_client_pub_verbose || rv_debug )
    this->trace_msg( '>', ver_rec, sizeof( ver_rec ) );
  this->append( ver_rec, sizeof( ver_rec ) );
}

void
EvRvClient::send_info( void ) noexcept
{
  static const uint32_t info_rec_prefix[] = { 3, 2, 0, 1, 0, 4 << 24, 4 << 24 };
  uint32_t info_rec[ 16 ];
  size_t i;

  ::memcpy( info_rec, info_rec_prefix, sizeof( info_rec_prefix ) );
  for ( i = 0; i < sizeof( info_rec_prefix ) / 4; i++ )
    info_rec[ i ] = get_u32<MD_BIG>( &info_rec[ i ] ); /* flip */
  for ( ; i < sizeof( info_rec ) / 4; i++ )
    info_rec[ i ] = 0; /* zero rest */
  if ( rv_client_pub_verbose || rv_debug )
    this->trace_msg( '>', info_rec, sizeof( info_rec ) );
  this->append( info_rec, sizeof( info_rec ) );
}

void
EvRvClient::send_init_rec( void ) noexcept
{
  const char * svc = ( this->service ? this->service : "" ),
             * net = ( this->network ? this->network : "" );
  size_t       svclen = ::strlen( svc ) + 1,
               netlen = ::strlen( net ) + 1,
               size;
  MDMsgMem     mem;
  RvMsgWriter  rvmsg( mem, mem.make( 1024 ), 1024 );

  rvmsg.append_string( SARG( "mtype" ), SARG( "I" ) );
  if ( this->userid_len == 0 )
    rvmsg.append_string( SARG( "userid" ), SARG( "nobody" ) );
  else
    rvmsg.append_string( SARG( "userid" ), this->userid, this->userid_len );
  if ( this->session_len != 0 )
    rvmsg.append_string( SARG( "session" ), this->session,
                                            this->session_len + 1 );
  rvmsg.append_string( SARG( "service" ), svc, svclen );
  rvmsg.append_string( SARG( "network" ), net, netlen );
  if ( this->control_len != 0 )
    rvmsg.append_string( SARG( "control" ), this->control,
                                            this->control_len + 1 );
  rvmsg.append_int<int32_t>( SARG( "vmaj" ), 5 );
  rvmsg.append_int<int32_t>( SARG( "vmin" ), 4 );
  rvmsg.append_int<int32_t>( SARG( "vupd" ), 2 );
  size = rvmsg.update_hdr();
  if ( rv_client_pub_verbose || rv_debug )
    this->trace_msg( '>', rvmsg.buf, size );
  this->append( rvmsg.buf, size );
}

static bool
match_field( MDFieldIter *it,  const char *fname,  size_t fname_size,
             void *fdata,  size_t &fsize,  MDType t )
{
  MDReference mref;
  if ( it->find( fname, fname_size, mref ) == 0 ) {
    if ( mref.ftype == t && mref.fsize <= fsize ) {
      ::memcpy( fdata, mref.fptr, mref.fsize );
      fsize = mref.fsize;
      return true;
    }
  }
  fsize = 0;
  return false;
}

uint16_t
EvRvClient::make_inbox( char *inbox,  uint32_t num ) noexcept
{
  int16_t off = 7;
  ::memcpy( inbox, "_INBOX.", off );
  ::memcpy( &inbox[ off ], this->session, this->session_len );
  off += this->session_len;
  if ( off > 0 )
    inbox[ off++ ] = '.';
  off += uint32_to_string( num, &inbox[ off ] );
  inbox[ off ] = '\0';
  return off;
}

int
EvRvClient::recv_info( void ) noexcept
{
  static const char init[] = "RVD.INITRESP",
                    refu[] = "RVD.INITREFUSED";
  MDMsgMem      mem;
  RvMsg       * m;
  MDFieldIter * it;

  m = RvMsg::unpack_rv( this->msg_in.data.fptr, 0, this->msg_in.data.fsize, 0,
                        NULL, mem );
  if ( this->msg_in.sublen != sizeof( init ) - 1 ||
       ::memcmp( this->msg_in.sub, init, sizeof( init ) - 1 ) != 0 ) {
    if ( this->msg_in.sublen == sizeof( refu ) - 1 &&
         ::memcmp( this->msg_in.sub, refu, sizeof( refu ) - 1 ) == 0 ) {
      if ( m != NULL && m->get_field_iter( it ) == 0 ) {
        int32_t err;
        size_t  elen = sizeof( err );
        if ( match_field( it, SARG( "error" ), &err, elen, MD_INT ) ) {
          if ( elen == sizeof( err ) )
            return get_i32<MD_BIG>( &err );
        }
      }
    }
    return ERR_START_HOST_FAILED;
  }

  if ( m != NULL && m->get_field_iter( it ) == 0 ) {
    uint8_t * ip = (uint8_t *) (void *) &this->ipaddr,
            * pt = (uint8_t *) (void *) &this->ipport;
    size_t    alen = sizeof( this->ipaddr ),
              plen = sizeof( this->ipport ),
              glen = sizeof( this->gob );
    if ( match_field( it, SARG( "ipaddr" ), ip,        alen, MD_IPDATA ) &&
         match_field( it, SARG( "ipport" ), pt,        plen, MD_IPDATA ) &&
         match_field( it, SARG( "gob" ),    this->gob, glen, MD_STRING ) ) {
      uint16_t tmp  = 0;
      size_t   i, clen = 2;
      char   * ptr = this->session;
      this->gob_len = (uint16_t) ( glen - 1 );
      if ( match_field( it, SARG( "cid" ), &tmp, clen, MD_IPDATA ) )
        this->cid = get_u16<MD_BIG>( &tmp );
      else
        this->cid = 0;
      /* <ipaddr>.<pid><time><ptr> */
      for ( i = 0; i < 8; i += 2 ) {
        *ptr++ = hexchar2( ( ip[ i/2 ] >> 4 ) & 0xf );
        *ptr++ = hexchar2( ip[ i/2 ] & 0xf );
      }
      *ptr++ = '.';
      if ( this->cid != 0 ) {
        *ptr++ = 'U';
        for ( i = 4; i <= 16; i += 4 )
          *ptr++ = hexchar2( ( this->cid >> ( 16 - i ) ) & 0xf );
        *ptr++ = '.';
      }
      ptr += RvHost::time_to_str( this->start_stamp, ptr );
      this->session_len = (uint16_t) ( ptr - this->session );
      this->control_len = this->make_inbox( this->control, 1 );
      if ( rv_client_pub_verbose || rv_debug )
        printf( "session: %.*s control: %.*s\n", this->session_len,
                this->session, this->control_len, this->control );
      this->send_init_rec(); /* resend with session */
      this->rv_state = CONN_RECV;
      return 0;
    }
  }
  return ERR_START_HOST_FAILED;
}

int
EvRvClient::recv_conn( void ) noexcept
{
  static const char conn[] = "_RV.INFO.SYSTEM.RVD.CONNECTED";

  if ( this->msg_in.sublen != sizeof( conn ) - 1 ||
       ::memcmp( this->msg_in.sub, conn, sizeof( conn ) - 1 ) != 0 )
    return ERR_START_HOST_FAILED;

  char         inbox[ 64 ];
  MDMsgMem     mem;
  RvMsgWriter  rvmsg( mem, mem.make( 1024 ), 1024 );
  size_t       size;

  rvmsg.append_string( SARG( "mtype" ), SARG( "L" ) );
  /* control: _INBOX.<iphex>.U<cid>.<time>.1 */
  ::memcpy( inbox, this->control, this->control_len + 1 );
  inbox[ this->control_len - 1 ] = '>';
  rvmsg.append_subject( SARG( "sub" ), inbox, this->control_len );
  size = rvmsg.update_hdr();
  if ( rv_client_pub_verbose || rv_debug )
    this->trace_msg( '>', rvmsg.buf, size );
  this->append( rvmsg.buf, size );
  this->rv_state = DATA_RECV;

  /* if all subs are forwarded to RV */
  if ( this->fwd_all_subs && this->cb == NULL )
    this->sub_route.add_route_notify( *this );
  /* if all msgs are forwarded to RV */
  if ( this->fwd_all_msgs ) {
    uint32_t h = this->sub_route.prefix_seed( 0 );
    this->sub_route.add_pattern_route( h, this->fd, 0 );
  }
  if ( this->bcast_subs.count > 0 ) {
    for ( size_t i = 0; i < this->bcast_subs.count; i++ ) {
      const char *s = this->bcast_subs.ptr[ i ];
      this->subscribe( s, ::strlen( s ), NULL, 0 );
    }
  }
  if ( this->listen_subs.count > 0 ) {
    this->sub_db.start_subscriptions( false );
    this->timer_id = this->poll.mono_ns;
    this->poll.timer.add_timer_seconds( this->fd, 1, this->timer_id, 0 );
  }
  if ( this->notify != NULL )
    this->notify->on_connect( *this );
  this->flush_pending_send();
  return 0;
}

/* dispatch a msg: 'D' - data, forward to subscriptions */
int
EvRvClient::dispatch_msg( void *msgbuf, size_t msglen ) noexcept
{
  static const char *rv_status_string[] = RV_STATUS_STRINGS;
  int status;
  if ( (status = this->msg_in.unpack( msgbuf, msglen )) != 0 ) {
    if ( msglen == 8 ) /* empty msg */
      return 0;
    if ( msglen != 0 ) {
      MDOutput mout;
      fprintf( stderr, "RV unpack status %d: \"%s\"\n", status,
               rv_status_string[ status ] );
      mout.print_hex( msgbuf, msglen );
    }
    return status;
  }
  if ( this->rv_state < DATA_RECV ) {
    if ( this->rv_state == INIT_RECV ) /* initresp */
      status = this->recv_info();
    else
      status = this->recv_conn();
    if ( status != 0 ) {
      fprintf( stderr, "RV host status: %d/%s\n", status,
               get_rv_host_error( status ) );
    }
    return status;
  }
  /*this->msg_in.print();*/
  if ( this->msg_in.mtype == 'D' ||  /* publish */
       this->msg_in.mtype == 'A' ) { /* advisory */
    this->fwd_pub();
    return 0;
  }
  return -1;
}
/* when a 'D' message is received from client, forward the message data,
 * this also decapsulates the opaque data field and forwards the message
 * with the correct message type attribute:
 * old style { sub: FD.SEC.INST.EX, mtype: 'D', data: <message> }
 * new style { sub: FD.SEC.INST.EX, mtype: 'D', data: { _data_ : <message> } }*/
bool
EvRvClient::fwd_pub( void ) noexcept
{
#if 0
  char   * sub     = this->msg_in.sub,
         * rep     = this->msg_in.reply;
  size_t   sublen  = this->msg_in.sublen,
           replen  = this->msg_in.replylen;
  uint32_t h       = kv_crc_c( sub, sublen, 0 );
  void   * msg     = this->msg_in.data.fptr;
  size_t   msg_len = this->msg_in.data.fsize;
  uint32_t ftype   = this->msg_in.data.ftype;

/*  printf( "fwd %.*s\n", (int) sublen, sub );*/
  if ( ftype == MD_MESSAGE ) {
    ftype = RVMSG_TYPE_ID;
    MDMsg * m = RvMsg::opaque_extract( (uint8_t *) msg, 8, msg_len, NULL,
                                       this->msg_in.mem );
    if ( m != NULL ) {
      ftype   = m->get_type_id();
      msg     = &((uint8_t *) m->msg_buf)[ m->msg_off ];
      msg_len = m->msg_end - m->msg_off;
    }
  }
  else if ( ftype == MD_OPAQUE ) {
    uint32_t ft = MDMsg::is_msg_type( msg, 0, msg_len, 0 );
    if ( ft != 0 )
      ftype = ft;
  }
  EvPublish pub( sub, sublen, rep, replen, msg, msg_len,
                 this->sub_route, *this, h, ftype );
  if ( this->cb != NULL )
    return this->cb->on_rv_msg( pub );
  return this->sub_route.forward_msg( pub );
#endif
  void   * msg     = this->msg_in.data.fptr;
  size_t   msg_len = this->msg_in.data.fsize;
  uint32_t ftype   = this->msg_in.data.ftype;
  char     reply_buf[ 256 ];

/*  printf( "fwd %.*s\n", (int) sublen, sub );*/
  if ( ftype == MD_MESSAGE || ftype == RVMSG_TYPE_ID ) {
    ftype = RVMSG_TYPE_ID;
    MDMsg * m = RvMsg::opaque_extract( (uint8_t *) msg, 8, msg_len, NULL,
                                       this->msg_in.mem );
    if ( m != NULL ) {
      ftype   = m->get_type_id();
      msg     = &((uint8_t *) m->msg_buf)[ m->msg_off ];
      msg_len = m->msg_end - m->msg_off;
    }
  }
  else if ( ftype == MD_OPAQUE ) {
    uint32_t ft = MDMsg::is_msg_type( msg, 0, msg_len, 0 );
    if ( ft != 0 )
      ftype = ft;
  }
  char   * sub,
         * rep    = NULL;
  size_t   sublen,
           replen = 0;
  uint32_t h;
  this->msg_in.pre_subject( sub, sublen );
  h = kv_crc_c( sub, sublen, 0 );
  if ( this->msg_in.replylen > 0 ) {
    char * buf    = reply_buf;
    size_t prelen = this->msg_in.prefix_len;
    rep    = this->msg_in.reply;
    replen = this->msg_in.replylen;
    if ( prelen > 0 ) {
      if ( prelen + replen >= sizeof( reply_buf ) )
        buf = this->msg_in.mem.str_make( prelen + replen + 1 );
      replen = this->msg_in.cat_pre_subject( buf, rep, replen );
      rep    = buf;
    }
  }
  EvPublish pub( sub, sublen, rep, replen, msg, msg_len,
                 this->sub_route, *this, h, ftype );
#if 0
  if ( this->msg_in.suffix_len != 0 ) {
    uint32_t suf_len = this->msg_in.suffix_len;
    if ( &((uint8_t *) msg)[ msg_len + (size_t) suf_len ] ==
         &((uint8_t *) rvbuf)[ buflen ] ) {
      pub.msg_len += suf_len;
      pub.suf_len  = suf_len;
    }
  }
#endif
  if ( this->listen_subs.count > 0 ) {
    if ( this->sub_db.process_pub2( pub, this->msg_in.sub, this->msg_in.sublen,
                                   this->msg_in.reply, this->msg_in.replylen ) )
      return true;
  }
  if ( this->cb != NULL )
    return this->cb->on_rv_msg( pub );
  return this->sub_route.forward_msg( pub );
}

RvMsg *
EvRvClient::make_rv_msg( void *msg,  size_t msg_len,
                         uint32_t msg_enc ) noexcept
{
  bool is_rv = false;
  switch ( msg_enc ) {
    case RVMSG_TYPE_ID:
      is_rv = true;
      break;

    case MD_OPAQUE:
    case MD_STRING:
      if ( RvMsg::is_rvmsg( msg, 0, msg_len, 0 ) ) {
        is_rv = true;
        break;
      }
      if ( msg_enc == MD_STRING ) {
        if ( TibMsg::is_tibmsg( msg, 0, msg_len, 0 ) ||
             TibSassMsg::is_tibsassmsg( msg, 0, msg_len, 0 ) )
          msg_enc = MD_OPAQUE;
        else {
          if ( MDMsg::is_msg_type( msg, 0, msg_len, 0 ) == JSON_TYPE_ID ) {
    case JSON_TYPE_ID:
            if ( EvRvService::convert_json( this->spc, msg, msg_len ) ) {
              is_rv = true;
              break;
            }
          }
        }
      }
      /* FALLTHRU */
    case RAIMSG_TYPE_ID:
    case TIB_SASS_TYPE_ID:
    case TIB_SASS_FORM_TYPE_ID:
    case MARKETFEED_TYPE_ID:
    case RWF_MSG_TYPE_ID:
    default:
      break;
    case MD_MESSAGE:
      if ( RvMsg::is_rvmsg( msg, 0, msg_len, 0 ) )
        is_rv = true;
      break;
  }
  if ( is_rv )
    return RvMsg::unpack_rv( msg, 0, msg_len, 0, NULL, this->msg_in.mem );
  RvMsgWriter rvmsg( this->msg_in.mem, NULL, 0 );
  if ( msg_enc == MD_STRING )
    rvmsg.append_string( SARG( "_data_" ), (const char *) msg, msg_len );
  else
    rvmsg.append_opaque( SARG( "_data_" ), msg, msg_len );
  return RvMsg::unpack_rv( rvmsg.buf, 0, rvmsg.update_hdr(), 0, NULL,
                           this->msg_in.mem );
  return NULL;
}

/* a message from the network, forward if matched by a subscription only once
 * as it may match multiple wild subscriptions as well as a normal sub
 * each sub is looked up in order to increment the msg count */
bool
EvRvClient::on_msg( EvPublish &pub ) noexcept
{
  if ( this->equals( pub.src_route ) )
    return true;

  return this->publish( pub );
}

bool
EvRvClient::publish( EvPublish &pub ) noexcept
{
  if ( this->no_write )
    return true;

  const char * sub    = pub.subject,
             * reply  = (const char *) pub.reply;
  size_t       sublen = pub.subject_len,
               replen = pub.reply_len,
               prelen = this->msg_in.prefix_len;

  if ( sublen < prelen ) {
    fprintf( stderr, "sub %.*s is less than prefix (%u)\n", (int) sublen, sub,
             (int) prelen );
    return true;
  }
  sub = &sub[ prelen ];
  sublen -= prelen;
  if ( replen > prelen ) {
    reply   = &reply[ prelen ];
    replen -= prelen;
  }
  return this->publish2( pub, sub, sublen, reply, replen );
}

bool
EvRvClient::publish2( EvPublish &pub,  const char *sub,  size_t sublen,
                      const char *reply,  size_t replen ) noexcept
{
  size_t buf_len = 1024;
  if ( sublen + replen > 1024 - 512 )
    buf_len = sublen + replen + 512;

  MDMsgMem    mem;
  RvMsgWriter rvmsg( mem, mem.make( buf_len ), buf_len );
  /* some subjects may not encode */
  rvmsg.append_subject( SARG( "sub" ), sub, sublen )
       .append_string( SARG( "mtype" ), SARG( "D" ) );
  if ( rvmsg.err == 0 && replen > 0 ) {
    rvmsg.append_string( SARG( "return" ), (const char *) reply, replen );
  }
  if ( rvmsg.err == 0 ) {
    RvMsgWriter  submsg( rvmsg.mem(), NULL, 0 );
    uint32_t     msg_enc = pub.msg_enc;
    size_t       msg_off = 0,
                 msg_len = pub.msg_len;
    void       * msg     = (void *) pub.msg;
    /* depending on message type, encode the hdr to send to the client */
    switch ( msg_enc ) {
      case RVMSG_TYPE_ID:
      do_rvmsg:;
        rvmsg.append_msg( SARG( "data" ), submsg );
        msg_off     = rvmsg.off + submsg.off;
        submsg.off += msg_len - 8;
        msg         = &((uint8_t *) msg)[ 8 ];
        msg_len     = msg_len - 8;
        rvmsg.update_hdr( submsg );
        break;

      case MD_OPAQUE:
      case MD_STRING:
        if ( RvMsg::is_rvmsg( msg, 0, msg_len, 0 ) )
          goto do_rvmsg;
        if ( msg_enc == MD_STRING ) {
          if ( TibMsg::is_tibmsg( msg, 0, msg_len, 0 ) ||
               TibSassMsg::is_tibsassmsg( msg, 0, msg_len, 0 ) )
            msg_enc = MD_OPAQUE;
          else {
            if ( MDMsg::is_msg_type( msg, 0, msg_len, 0 ) == JSON_TYPE_ID ) {
      case JSON_TYPE_ID:
              if ( EvRvService::convert_json( this->spc, msg, msg_len ) )
                goto do_rvmsg;
            }
          }
        }
        /* FALLTHRU */
      case RAIMSG_TYPE_ID:
      case TIB_SASS_TYPE_ID:
      case TIB_SASS_FORM_TYPE_ID:
      case MARKETFEED_TYPE_ID:
      do_tibmsg:;
        if ( rvmsg.has_space( 20 ) ) {
          rvmsg.off += append_rv_field_hdr( &rvmsg.buf[ rvmsg.off ],
                                            SARG( "data" ), msg_len, msg_enc );
          msg_off    = rvmsg.off;
          rvmsg.off += msg_len;
          rvmsg.update_hdr();
        }
        break;

      case RWF_MSG_TYPE_ID:
        if ( rvmsg.has_space( 20 ) ) {
          rvmsg.append_msg( SARG( "data" ), submsg );

          msg_off     = rvmsg.off + submsg.off;
          msg_off     = append_rv_field_hdr( &rvmsg.buf[ msg_off ],
                                          SARG( "_RWFMSG" ), msg_len, msg_enc );
          submsg.off += msg_off;
          msg_off     = rvmsg.off + submsg.off;
          submsg.off += msg_len;
          rvmsg.update_hdr( submsg );
        }
        break;

      case MD_MESSAGE:
        if ( RvMsg::is_rvmsg( msg, 0, msg_len, 0 ) )
          goto do_rvmsg;
        /* FALLTHRU */
      default:
        if ( msg_len == 0 ) {
          msg_off = rvmsg.off;
          rvmsg.update_hdr();
        }
        else {
          if ( MDMsg::is_msg_type( msg, 0, msg_len, 0 ) != 0 )
            goto do_tibmsg;
          msg_off = 0;
        }
        msg     = NULL;
        msg_len = 0;
        break;
    }
    if ( rvmsg.err != 0 ) {
      fprintf( stderr, "rv msg error %d subject: %.*s %u\n",
               rvmsg.err, (int) sublen, sub, (uint32_t) msg_off );
    }
    else {
      if ( msg_off > 0 )
        return this->queue_send( rvmsg.buf, msg_off, msg, msg_len );
      fprintf( stderr, "rv unknown msg_enc %u subject: %.*s %u\n",
               msg_enc, (int) sublen, sub, (uint32_t) msg_off );
    }
  }
  return true;
}

bool
EvRvClient::queue_send( const void *buf,  size_t buflen,
                        const void *msg,  size_t msglen ) noexcept
{
  if ( this->rv_state >= DATA_RECV ) {
    this->append2( buf, buflen, msg, msglen );
    /*this->send( buf, off, msg, msg_len );*/
    return this->idle_push_write();
  }
  if ( this->rv_state == ERR_CLOSE )
    return false;
  size_t newlen = this->save_len + buflen + msglen;
  this->save_buf = ::realloc( this->save_buf, newlen );
  ::memcpy( &((char *) this->save_buf)[ this->save_len ], buf, buflen );
  this->save_len += buflen;
  ::memcpy( &((char *) this->save_buf)[ this->save_len ], msg, msglen );
  this->save_len += msglen;
  return false;
}

void
EvRvClient::flush_pending_send( void ) noexcept
{
  if ( this->save_len > 0 ) {
    this->append( this->save_buf, this->save_len );
    ::free( this->save_buf );
    this->save_buf = NULL;
    this->save_len = 0;
    this->idle_push_write();
  }
}

void
EvRvClient::process_close( void ) noexcept
{
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

void
EvRvClient::release( void ) noexcept
{
  if ( this->listen_subs.count > 0 )
    this->sub_db.unsub_all();
  if ( this->fwd_all_msgs ) {
    uint32_t h = this->sub_route.prefix_seed( 0 );
    this->sub_route.del_pattern_route( h, this->fd, 0 );
  }
  if ( this->fwd_all_subs && this->cb == NULL )
    this->sub_route.remove_route_notify( *this );
  if ( this->notify != NULL )
    this->notify->on_shutdown( *this, NULL, 0 );
  if ( this->save_buf != NULL ) {
    ::free( this->save_buf );
    this->save_buf = NULL;
    this->save_len = 0;
  }
  if ( this->param_buf != NULL ) {
    ::free( this->param_buf );
    this->param_buf = NULL;
  }
  this->inter_subs.release();
  this->bcast_subs.release();
  if ( this->listen_subs.count > 0 ) {
    this->sub_db.release();
    this->listen_subs.release();
  }
  this->EvConnection::release_buffers();
  this->rv_state = ERR_CLOSE;
  if ( this->timer_id != 0 ) {
    this->poll.timer.remove_timer( this->fd , this->timer_id, 0 );
    this->timer_id = 0;
  }
}
/* a new subscription */
void
EvRvClient::subscribe( const char *sub,  size_t sublen,
                       const char *rep,  size_t replen ) noexcept
{
  if ( this->no_write )
    return;
  size_t  len    = sublen + replen,
          buflen = 1024;

  if ( buflen < len * 2 + 32 )
    buflen = sublen * 2 + 32;

  MDMsgMem    mem;
  RvMsgWriter msg( mem, mem.make( buflen ), buflen );
  msg.append_string( SARG( "mtype" ), SARG( "L" ) );
  if ( sublen > 0 && sub[ sublen - 1 ] == '\0' )
    sublen--;
  msg.append_subject( SARG( "sub" ), sub, sublen );
  if ( replen > 0 ) {
    if ( rep[ replen - 1 ] == '\0' )
      replen--;
    msg.append_string( SARG( "return" ), rep, replen );
  }
  size_t size = msg.update_hdr();
  if ( rv_client_sub_verbose || rv_debug )
    this->trace_msg( '>', msg.buf, size );
  this->queue_send( msg.buf, size );
}

void
EvRvClient::unsubscribe( const char *sub,  size_t sublen ) noexcept
{
  if ( this->no_write )
    return;
  size_t  buflen = 1024;

  if ( buflen < (size_t) sublen * 2 + 32 )
    buflen = (size_t) sublen * 2 + 32;

  MDMsgMem    mem;
  RvMsgWriter msg( mem, mem.make( buflen ), buflen );
  msg.append_string( SARG( "mtype" ), SARG( "C" ) );
  if ( sublen > 0 && sub[ sublen - 1 ] == '\0' )
    sublen--;
  msg.append_subject( SARG( "sub" ), sub, sublen );
  size_t size = msg.update_hdr();
  if ( rv_client_sub_verbose || rv_debug )
    this->trace_msg( '>', msg.buf, size );
  this->queue_send( msg.buf, size );
}

bool
EvRvClient::get_nsub( NotifySub &nsub,  const char *&sub,  size_t &sublen,
                      const char *&rep,  size_t replen ) noexcept
{
  size_t prelen = this->msg_in.prefix_len;

  sub    = nsub.subject;
  rep    = (const char *) nsub.reply;
  sublen = nsub.subject_len;
  replen = nsub.reply_len;

  if ( prelen == 0 || 
       ( sublen > prelen &&
         ::memcmp( sub, this->msg_in.prefix_start(), prelen ) == 0 ) ) {
    sub    += prelen;
    sublen -= prelen;
    if ( replen > prelen ) {
      replen -= prelen;
      rep    += prelen;
    }
    return true;
  }
  return false;
}

bool
EvRvClient::match_filter( const char *sub,  size_t sublen ) noexcept
{
  if ( is_inbox_subject( sub, sublen ) )
    return false;
  if ( this->inter_subs.count == 0 && this->bcast_subs.count == 0 &&
       this->listen_subs.count == 0 )
    return true;
  for ( size_t i = 0; i < this->inter_subs.count; i++ ) {
    if ( match_rv_wildcard( this->inter_subs.ptr[ i ],
                            ::strlen( this->inter_subs.ptr[ i ] ),
                            sub, sublen ) )
      return true;
  }
  return false;
}

void
EvRvClient::on_sub( NotifySub &nsub ) noexcept
{
  if ( this->no_write )
    return;
  const char * sub, * rep;
  size_t sublen, replen = 0;
  if ( this->get_nsub( nsub, sub, sublen, rep, replen ) )
    if ( this->match_filter( sub, sublen ) )
      this->subscribe( sub, sublen, rep, replen );
}
/* an unsubscribed sub */
void
EvRvClient::on_unsub( NotifySub &nsub ) noexcept
{
  if ( this->no_write )
    return;
  if ( nsub.sub_count != 0 ) /* if no routes left */
    return;
  const char * sub, * rep;
  size_t sublen, replen = 0;
  if ( this->get_nsub( nsub, sub, sublen, rep, replen ) )
    if ( this->match_filter( sub, sublen ) )
      this->unsubscribe( sub, sublen );
}

void
EvRvClient::on_psub( NotifyPattern &pat ) noexcept
{
  if ( this->no_write )
    return;
  this->fwd_pat( pat, true );
}
/* an unsubscribed pattern sub */
void
EvRvClient::on_punsub( NotifyPattern &pat ) noexcept
{
  if ( this->no_write )
    return;
  const size_t prefix_len = pat.cvt.prefixlen;
  const char * prefix     = pat.pattern;
  bool fwd = false;
  if ( pat.sub_count == 0 ) {
    if ( prefix_len > 0 && prefix[ prefix_len - 1 ] == '.' )
      fwd = true;
  }
  else if ( pat.sub_count == 1 ) {
    /* EvRvClient is subscribed to > */
    if ( this->fwd_all_msgs && prefix_len == 0 )
      fwd = true;
  }
  if ( ! fwd )
    return;
  this->fwd_pat( pat, false );
}

void
EvRvClient::fwd_pat( NotifyPattern &pat,  bool start ) noexcept
{
  size_t prelen = this->msg_in.prefix_len;

  char         sub[ 1024 ];
  const char * p;
  size_t       plen;

  if ( pat.cvt.fmt != RV_PATTERN_FMT ) {
    size_t sublen = pat.cvt.prefixlen;
    if ( sublen > sizeof( sub ) - 3 )
      sublen = sizeof( sub ) - 3;

    ::memcpy( sub, pat.pattern, sublen );
    if ( sublen != 0 ) {
      sub[ sublen++ ] = '.';
    }
    sub[ sublen++ ] = '>';
    sub[ sublen ] = '\0';
    p    = sub;
    plen = sublen;
  }
  else {
    p    = pat.pattern;
    plen = pat.pattern_len;
  }
  if ( prelen == 0 || 
       ( plen > prelen &&
         ::memcmp( p, this->msg_in.prefix_start(), prelen ) == 0 ) ) {
    p    += prelen;
    plen -= prelen;
    if ( this->match_filter( p, plen ) ) {
      MDMsgMem    mem;
      RvMsgWriter msg( mem, mem.make( 1024 ), 1024 );
      if ( start )
        msg.append_string( SARG( "mtype" ), SARG( "L" ) );
      else
        msg.append_string( SARG( "mtype" ), SARG( "C" ) );
      msg.append_subject( SARG( "sub" ), p, plen );
      size_t size = msg.update_hdr();
      if ( rv_client_sub_verbose || rv_debug )
        this->trace_msg( '>', msg.buf, size );
      this->queue_send( msg.buf, size );
    }
  }
}

/* a new pattern subscription */
void
EvRvClient::do_psub( const char *prefix,  uint8_t prefix_len ) noexcept
{
  if ( this->no_write )
    return;
  char    sub[ 1024 ];
  size_t  sublen = prefix_len;

  ::memcpy( sub, prefix, sublen );
  if ( sublen != 0 )
    sub[ sublen++ ] = '.';
  sub[ sublen++ ] = '>';
  sub[ sublen ] = '\0';

  MDMsgMem    mem;
  RvMsgWriter msg( mem, mem.make( 1024 ), 1024 );
  msg.append_string( SARG( "mtype" ), SARG( "L" ) );
  msg.append_subject( SARG( "sub" ), sub, sublen );
  size_t size = msg.update_hdr();
  if ( rv_client_sub_verbose || rv_debug )
    this->trace_msg( '>', msg.buf, size );
  this->queue_send( msg.buf, size );
}

/* reassert subs after reconnect */
void
EvRvClient::on_reassert( uint32_t /*fd*/,  kv::RouteVec<kv::RouteSub> &sub_db,
                          kv::RouteVec<kv::RouteSub> &pat_db ) noexcept
{
  RouteLoc   loc;
  RouteSub * sub;

  if ( this->no_write )
    return;
  for ( sub = sub_db.first( loc ); sub != NULL; sub = sub_db.next( loc ) ) {
    this->subscribe( sub->value, sub->len, NULL, 0 );
  }
  for ( sub = pat_db.first( loc ); sub != NULL; sub = pat_db.next( loc ) ) {
    this->do_psub( sub->value, (uint8_t) sub->len );
  }
}

void
EvRvClient::on_listen_start( Start &add ) noexcept
{
  if ( rv_client_sub_verbose || rv_debug ) {
    if ( add.reply_len == 0 ) {
      printf( "%sstart%s %.*s refs %u from %.*s\n",
        add.is_listen_start ? "listen_" : "assert_",
        add.coll ? " collision" : "",
        add.sub.len, add.sub.value, add.sub.refcnt,
        add.session.len, add.session.value );
    }
    else {
      printf( "%sstart%s %.*s reply %.*s refs %u from %.*s\n",
        add.is_listen_start ? "listen_" : "assert_",
        add.coll ? " collision" : "",
        add.sub.len, add.sub.value, add.reply_len, add.reply, add.sub.refcnt,
        add.session.len, add.session.value );
    }
  }
  char sub_buf[ 256 ], reply_buf[ 256 ];
  const char * sub    = add.sub.value,
             * rep    = add.reply;
  size_t       sublen = add.sub.len,
               replen = add.reply_len;
  size_t       prelen = this->msg_in.prefix_len;

  if ( prelen > 0 ) {
    char *buf = sub_buf;
    if ( prelen + sublen >= sizeof( sub_buf ) )
      buf = this->msg_in.mem.str_make( prelen + sublen + 1 );
    sublen = this->msg_in.cat_pre_subject( buf, sub, sublen );
    sub    = buf;

    if ( replen > 0 ) {
      buf = reply_buf;
      if ( prelen + replen >= sizeof( reply_buf ) )
        buf = this->msg_in.mem.str_make( prelen + replen + 1 );
      replen = this->msg_in.cat_pre_subject( buf, rep, replen );
      rep    = buf;
    }
  }
  uint32_t h;
  if ( ! is_rv_wildcard( sub, sublen ) ) {
    h = kv_crc_c( sub, sublen, 0 );
    printf( "h %x sub %.*s\n", h, (int) sublen, sub );
    NotifySub nsub( sub, sublen, rep, replen, h, add.coll, 'X', *this );
    nsub.sub_count = add.sub.refcnt;
    if ( nsub.sub_count == 1 )
      this->sub_route.add_sub( nsub );
    else
      this->sub_route.notify_sub( nsub );
  }
  else {
    PatternCvt cvt;
    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->sub_route.prefix_seed( cvt.prefixlen ) );
      NotifyPattern npat( cvt, sub, len, rep, replen, h, add.coll, 'X', *this );
      npat.sub_count = add.sub.refcnt;
      if ( npat.sub_count == 1 )
        this->sub_route.add_pat( npat );
      else
        this->sub_route.notify_pat( npat );
    }
  }
}

void
EvRvClient::on_listen_stop( Stop &rem ) noexcept
{
  if ( rv_client_sub_verbose || rv_debug )
    printf( "%sstop%s %.*s refs %u from %.*s%s\n",
      rem.is_listen_stop ? "listen_" : "assert_",
      rem.coll ? " collision" : "",
      rem.sub.len, rem.sub.value, rem.sub.refcnt,
      rem.session.len, rem.session.value, rem.is_orphan ? " orphan" : "" );

  if ( rem.is_orphan )
    return;
  char sub_buf[ 256 ];
  const char * sub    = rem.sub.value;
  size_t       sublen = rem.sub.len;
  size_t       prelen = this->msg_in.prefix_len;

  if ( prelen > 0 ) {
    char *buf = sub_buf;
    if ( prelen + sublen >= sizeof( sub_buf ) )
      buf = this->msg_in.mem.str_make( prelen + sublen + 1 );
    sublen = this->msg_in.cat_pre_subject( buf, sub, sublen );
    sub    = buf;
  }
  uint32_t h;
  if ( ! is_rv_wildcard( sub, sublen ) ) {
    h = kv_crc_c( sub, sublen, 0 );
    NotifySub nsub( sub, sublen, NULL, 0, h, rem.coll, 'X', *this );
    nsub.sub_count = rem.sub.refcnt;
    if ( nsub.sub_count == 0 )
      this->sub_route.del_sub( nsub );
  }
  else {
    PatternCvt cvt;
    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->sub_route.prefix_seed( cvt.prefixlen ) );
      NotifyPattern npat( cvt, sub, len, NULL, 0, h, rem.coll, 'X', *this );
      npat.sub_count = rem.sub.refcnt;
      if ( npat.sub_count == 0 )
        this->sub_route.del_pat( npat );
    }
  }
}

void
EvRvClient::on_snapshot( Snap &snp ) noexcept
{
  if ( rv_client_sub_verbose || rv_debug )
    printf( "snap %.*s reply %.*s refs %u flags %u\n",
      snp.sub.len, snp.sub.value, snp.reply_len, snp.reply, snp.sub.refcnt,
      snp.flags );
}

