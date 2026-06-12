#define _POSIX_C_SOURCE 200809L

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#include <sassrv/rv7api.h>
#include <raimd/md_msg.h>
#include <raimd/rv_msg.h>

#define MIN_PARMS (2)

/*
 * pingrv7test -- a two-subject RV ping/pong round-trip latency tester.
 *
 * It runs in two roles that talk to each other over a pair of subjects:
 *
 *   reflect mode (-reflect):  listens on the ping subject and echoes every
 *                             message it receives onto the pong subject.  This
 *                             is the responder; start it first.
 *
 *   active mode  (-active, default): publishes pings on the ping subject and
 *                             listens for the echoes on the pong subject,
 *                             measuring the round-trip time (RTT) from a
 *                             monotonic timestamp it stamps into each message.
 *
 * Two subjects are used so the ping flow and the pong (echo) flow never
 * collide on a single subject: active sends on subject1 / listens on subject2,
 * reflect listens on subject1 / sends on subject2.
 *
 * By default active mode is closed-loop (ping(8) style): send one ping, wait
 * for the echo, record the RTT, optionally pause, repeat.  With -rate N it
 * switches to open-loop: it fires N messages/second on a timer WITHOUT waiting
 * for the reply, and still measures RTT asynchronously from the timestamp that
 * comes back in each echoed message.
 *
 * Field layout (carried in each message, echoed unchanged by the reflector):
 *   PSEQ  u64  monotonically increasing sequence number
 *   PTS   u64  send timestamp, nanoseconds from CLOCK_MONOTONIC
 *   PAD   opaque (optional) padding so message size can be controlled
 *
 * Subjects are published with a configurable prefix (default "_TIC.") to match
 * the SASS rv pub/sub convention used by pubrv7test/subrv7test (publishers send
 * to _TIC.<subj>, subscribers listen on the bare <subj>).  Use -prefix "" for
 * plain rvd subjects.
 */

typedef struct {
  tibrvTransport transport;
  const char *   ping_subject;   /* bare subject1 (active->reflect) */
  const char *   pong_subject;   /* bare subject2 (reflect->active) */
  char           ping_send[ TIBRV_SUBJECT_MAX + 1 ]; /* prefix+subject1 */
  char           pong_send[ TIBRV_SUBJECT_MAX + 1 ]; /* prefix+subject2 */

  /* active-mode measurement state */
  tibrv_u64      seq_sent;
  tibrv_u64      seq_recv;
  tibrv_u64      n;              /* number of RTT samples */
  tibrv_u64      lost;          /* pings that never came back (closed loop) */
  double         min_ns;
  double         max_ns;
  double         sum_ns;
  double         sum2_ns;       /* sum of squares, for stddev */
  /* interval-window stats (rate mode periodic report) */
  tibrv_u64      win_n;
  double         win_min_ns;
  double         win_max_ns;
  double         win_sum_ns;

  unsigned long  count;         /* number of pings to send, 0 = unlimited */
  unsigned char *pad;           /* padding buffer, or NULL */
  tibrv_u32      pad_size;
  int            quiet;         /* suppress per-sample lines */
  int            got_reply;     /* closed-loop: set by pong callback */
  tibrv_u32      batch;         /* rate mode: msgs per Sendv (1 = plain Send) */
  int            use_flush;     /* lib batch mode: Flush periodically */
} ping_state_t;

static volatile sig_atomic_t g_stop = 0;

static void
on_signal( int sig )
{
  (void) sig;
  g_stop = 1;
}

static tibrv_u64
mono_ns( void )
{
  struct timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return (tibrv_u64) ts.tv_sec * 1000000000ULL + (tibrv_u64) ts.tv_nsec;
}

static void
record_rtt( ping_state_t * st, double rtt_ns )
{
  if ( st->n == 0 || rtt_ns < st->min_ns ) st->min_ns = rtt_ns;
  if ( st->n == 0 || rtt_ns > st->max_ns ) st->max_ns = rtt_ns;
  st->sum_ns  += rtt_ns;
  st->sum2_ns += rtt_ns * rtt_ns;
  st->n++;

  if ( st->win_n == 0 || rtt_ns < st->win_min_ns ) st->win_min_ns = rtt_ns;
  if ( st->win_n == 0 || rtt_ns > st->win_max_ns ) st->win_max_ns = rtt_ns;
  st->win_sum_ns += rtt_ns;
  st->win_n++;
}

/* Build a fresh ping message into *out.  Caller destroys it. */
static tibrv_status
build_ping( ping_state_t * st, tibrv_u64 seq, tibrvMsg * out )
{
  tibrvMsg     msg;
  tibrv_status err;

  if ( (err = tibrvMsg_Create( &msg )) != TIBRV_OK )
    return err;
  tibrvMsg_SetSendSubject( msg, st->ping_send );
  if ( (err = tibrvMsg_AddU64( msg, "PSEQ", seq )) != TIBRV_OK )
    goto fail;
  if ( (err = tibrvMsg_AddU64( msg, "PTS", mono_ns() )) != TIBRV_OK )
    goto fail;
  if ( st->pad != NULL ) {
    if ( (err = tibrvMsg_AddOpaque( msg, "PAD", st->pad, st->pad_size )) != TIBRV_OK )
      goto fail;
  }
  *out = msg;
  return TIBRV_OK;
fail:
  tibrvMsg_Destroy( msg );
  return err;
}

static tibrv_status
send_ping( ping_state_t * st, tibrv_u64 seq )
{
  tibrvMsg     msg;
  tibrv_status err = build_ping( st, seq, &msg );
  if ( err != TIBRV_OK )
    return err;
  err = tibrvTransport_Send( st->transport, msg );
  tibrvMsg_Destroy( msg );
  return err;
}

#define PING_BATCH_MAX 4096
/* Build n pings and ship them in a single vectored send (one EvPipe round-trip
 * for the whole batch instead of one per message). */
static tibrv_status
send_ping_batch( ping_state_t * st, tibrv_u64 seq0, tibrv_u32 n )
{
  tibrvMsg     vec[ PING_BATCH_MAX ];
  tibrv_status err = TIBRV_OK;
  tibrv_u32    i, built = 0;

  if ( n > PING_BATCH_MAX )
    n = PING_BATCH_MAX;
  for ( i = 0; i < n; i++ ) {
    if ( (err = build_ping( st, seq0 + i, &vec[ i ] )) != TIBRV_OK )
      break;
    built = i + 1;
  }
  if ( built > 0 )
    err = tibrvTransport_Sendv( st->transport, vec, built );
  for ( i = 0; i < built; i++ )
    tibrvMsg_Destroy( vec[ i ] );
  return err;
}

/* reflect mode: echo each received message onto the pong subject, leaving the
 * PSEQ/PTS/PAD fields untouched so the active side can measure RTT. */
static void
reflect_callback( tibrvEvent event, tibrvMsg message, void * closure )
{
  ping_state_t * st = (ping_state_t *) closure;
  tibrvMsg       copy;
  tibrv_status   err;

  (void) event;
  err = tibrvMsg_CreateCopy( message, &copy );
  if ( err != TIBRV_OK )
    return;
  tibrvMsg_SetSendSubject( copy, st->pong_send );
  tibrvMsg_SetReplySubject( copy, NULL );
  err = tibrvTransport_Send( st->transport, copy );
  tibrvMsg_Destroy( copy );
  if ( err != TIBRV_OK )
    fprintf( stderr, "reflect: send failed: %s\n", tibrvStatus_GetText( err ) );
}

/* active mode: a pong arrived, compute and record the RTT. */
static void
pong_callback( tibrvEvent event, tibrvMsg message, void * closure )
{
  ping_state_t * st  = (ping_state_t *) closure;
  tibrv_u64      now = mono_ns();
  tibrv_u64      seq = 0, ts = 0;
  double         rtt_ns;

  (void) event;
  if ( tibrvMsg_GetU64( message, "PTS", &ts ) != TIBRV_OK )
    return;
  tibrvMsg_GetU64( message, "PSEQ", &seq );

  rtt_ns = (double) ( now - ts );
  record_rtt( st, rtt_ns );
  st->seq_recv = seq;
  st->got_reply = 1;

  if ( ! st->quiet )
    printf( "seq=%llu rtt=%.3f us\n",
            (unsigned long long) seq, rtt_ns / 1000.0 );
}

/* rate mode: timer fires -> publish one ping without waiting for a reply. */
static void
rate_timer_callback( tibrvEvent event, tibrvMsg message, void * closure )
{
  ping_state_t * st = (ping_state_t *) closure;
  tibrv_status   err;
  tibrv_u32      n;

  (void) event; (void) message;
  if ( st->count != 0 && st->seq_sent >= st->count ) {
    tibrvEvent_Destroy( event );
    return;
  }
  n = st->batch ? st->batch : 1;
  if ( st->count != 0 && st->seq_sent + n > st->count )
    n = (tibrv_u32) ( st->count - st->seq_sent );
  if ( n > 1 )
    err = send_ping_batch( st, st->seq_sent, n );
  else
    err = send_ping( st, st->seq_sent );
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "rate: send failed: %s\n", tibrvStatus_GetText( err ) );
    g_stop = 1;
    return;
  }
  st->seq_sent += n;
}

/* rate mode: periodic stats line. */
static void
report_timer_callback( tibrvEvent event, tibrvMsg message, void * closure )
{
  ping_state_t * st = (ping_state_t *) closure;

  (void) event; (void) message;
  if ( st->win_n == 0 ) {
    printf( "sent=%llu  (no replies this interval)\n",
            (unsigned long long) st->seq_sent );
  }
  else {
    printf( "sent=%llu recv=%llu  rtt(us) min=%.3f avg=%.3f max=%.3f  (%llu samples)\n",
            (unsigned long long) st->seq_sent,
            (unsigned long long) st->n,
            st->win_min_ns / 1000.0,
            ( st->win_sum_ns / (double) st->win_n ) / 1000.0,
            st->win_max_ns / 1000.0,
            (unsigned long long) st->win_n );
  }
  fflush( stdout );
  st->win_n = 0;
  st->win_sum_ns = 0;
  if ( st->use_flush )           /* bound tail latency for low-rate batches */
    tibrvTransport_Flush( st->transport );
}

static void
print_summary( ping_state_t * st )
{
  double avg, var, sd;

  printf( "\n--- pingrv7test statistics ---\n" );
  printf( "%llu pings sent, %llu replies, %llu lost\n",
          (unsigned long long) st->seq_sent,
          (unsigned long long) st->n,
          (unsigned long long) st->lost );
  if ( st->n == 0 )
    return;
  avg = st->sum_ns / (double) st->n;
  var = ( st->sum2_ns / (double) st->n ) - ( avg * avg );
  if ( var < 0 ) var = 0;
  sd  = 0;
  { /* sqrt without pulling in -lm dependency assumptions */
    double x = var, g = var > 1.0 ? var : 1.0;
    int i;
    for ( i = 0; i < 40; i++ ) g = 0.5 * ( g + x / g );
    sd = g;
  }
  printf( "rtt(us) min=%.3f avg=%.3f max=%.3f stddev=%.3f\n",
          st->min_ns / 1000.0, avg / 1000.0,
          st->max_ns / 1000.0, sd / 1000.0 );
}

void
usage( void )
{
  fprintf( stderr,
    "pingrv7test [-service service] [-network network] [-daemon daemon]\n"
    "            [-reflect | -active] [-rate N] [-count N] [-size N]\n"
    "            [-interval S] [-timeout S] [-prefix P] [-quiet]\n"
    "            ping_subject pong_subject\n"
    "\n"
    "  -reflect       responder: listen on ping_subject, echo to pong_subject\n"
    "  -active        initiator (default): ping ping_subject, time pong_subject\n"
    "  -rate N        active: publish N msgs/sec open-loop (do not wait for reply)\n"
    "  -count N       send N pings then stop (0 = unlimited, default 0)\n"
    "  -size N        pad each message with N opaque bytes\n"
    "  -interval S    closed-loop seconds between pings (default 1.0)\n"
    "  -timeout S     closed-loop seconds to wait for a reply (default 2.0)\n"
    "  -prefix P      send-subject prefix (default \"_TIC.\", use \"\" for none)\n"
    "  -quiet         do not print a line per round trip\n"
    "  -batch N       rate mode: send N msgs per vectored Sendv (default 1)\n"
    "  -libbatch B    rate mode: library TIMER_BATCH, flush every B bytes\n"
    "  -singlebatch B library SINGLE_BATCH: one shared buffer, inline flush on\n"
    "                 E every B bytes (0=timer-only)\n"
    "  -binterval S   SINGLE_BATCH flush-timer period in seconds (default 0)\n" );
  exit( 1 );
}

int
get_InitParms( int argc, char * argv[], int min_parms, char ** serviceStr,
               char ** networkStr, char ** daemonStr, int * reflect,
               double * rate, unsigned long * count, unsigned long * size,
               double * interval, double * timeout, const char ** prefix,
               int * quiet, unsigned long * batch, unsigned long * libbatch,
               int * spin, unsigned long * singlebatch, double * binterval )
{
  int i = 1;

  if ( argc < min_parms )
    usage();

  while ( i + 1 <= argc && *argv[ i ] == '-' ) {
    if ( strcmp( argv[ i ], "-service" ) == 0 && i + 2 <= argc ) {
      *serviceStr = argv[ i + 1 ]; i += 2;
    }
    else if ( strcmp( argv[ i ], "-network" ) == 0 && i + 2 <= argc ) {
      *networkStr = argv[ i + 1 ]; i += 2;
    }
    else if ( strcmp( argv[ i ], "-daemon" ) == 0 && i + 2 <= argc ) {
      *daemonStr = argv[ i + 1 ]; i += 2;
    }
    else if ( strcmp( argv[ i ], "-reflect" ) == 0 ) {
      *reflect = 1; i += 1;
    }
    else if ( strcmp( argv[ i ], "-active" ) == 0 ) {
      *reflect = 0; i += 1;
    }
    else if ( strcmp( argv[ i ], "-rate" ) == 0 && i + 2 <= argc ) {
      *rate = strtod( argv[ i + 1 ], NULL ); i += 2;
    }
    else if ( strcmp( argv[ i ], "-count" ) == 0 && i + 2 <= argc ) {
      *count = strtoul( argv[ i + 1 ], NULL, 10 ); i += 2;
    }
    else if ( strcmp( argv[ i ], "-size" ) == 0 && i + 2 <= argc ) {
      *size = strtoul( argv[ i + 1 ], NULL, 10 ); i += 2;
    }
    else if ( strcmp( argv[ i ], "-interval" ) == 0 && i + 2 <= argc ) {
      *interval = strtod( argv[ i + 1 ], NULL ); i += 2;
    }
    else if ( strcmp( argv[ i ], "-timeout" ) == 0 && i + 2 <= argc ) {
      *timeout = strtod( argv[ i + 1 ], NULL ); i += 2;
    }
    else if ( strcmp( argv[ i ], "-prefix" ) == 0 && i + 2 <= argc ) {
      *prefix = argv[ i + 1 ]; i += 2;
    }
    else if ( strcmp( argv[ i ], "-quiet" ) == 0 ) {
      *quiet = 1; i += 1;
    }
    else if ( strcmp( argv[ i ], "-batch" ) == 0 && i + 2 <= argc ) {
      *batch = strtoul( argv[ i + 1 ], NULL, 10 ); i += 2;
    }
    else if ( strcmp( argv[ i ], "-libbatch" ) == 0 && i + 2 <= argc ) {
      *libbatch = strtoul( argv[ i + 1 ], NULL, 10 ); i += 2;
    }
    else if ( strcmp( argv[ i ], "-spin" ) == 0 ) {
      *spin = 1; i += 1;
    }
    else if ( strcmp( argv[ i ], "-singlebatch" ) == 0 && i + 2 <= argc ) {
      *singlebatch = strtoul( argv[ i + 1 ], NULL, 10 ); i += 2;
    }
    else if ( strcmp( argv[ i ], "-binterval" ) == 0 && i + 2 <= argc ) {
      *binterval = strtod( argv[ i + 1 ], NULL ); i += 2;
    }
    else {
      usage();
    }
  }
  return i;
}

int
main( int argc, char ** argv )
{
  tibrv_status   err;
  int            currentArg;
  ping_state_t   st;
  tibrvEvent     listenId = 0, rateTimer = 0, reportTimer = 0;
  char *         serviceStr = NULL;
  char *         networkStr = NULL;
  char *         daemonStr  = NULL;
  int            reflect    = 0;
  double         rate       = 0.0;
  unsigned long  count      = 0;
  unsigned long  size       = 0;
  double         interval   = 1.0;
  double         timeout    = 2.0;
  const char *   prefix     = "_TIC.";
  int            quiet      = 0;
  unsigned long  batch      = 1;
  unsigned long  libbatch   = 0;
  int            spin       = 0;
  unsigned long  singlebatch = 0;
  double         binterval  = 0.0;
  char *         progname   = argv[ 0 ];

  currentArg = get_InitParms( argc, argv, MIN_PARMS, &serviceStr, &networkStr,
                              &daemonStr, &reflect, &rate, &count, &size,
                              &interval, &timeout, &prefix, &quiet, &batch,
                              &libbatch, &spin, &singlebatch, &binterval );

  if ( argc - currentArg < 2 ) {
    fprintf( stderr, "%s: need ping_subject and pong_subject\n", progname );
    usage();
  }

  memset( &st, 0, sizeof( st ) );
  st.ping_subject = argv[ currentArg ];
  st.pong_subject = argv[ currentArg + 1 ];
  st.count        = count;
  st.quiet        = quiet;
  st.batch        = ( batch < 1 ? 1 : batch );

  snprintf( st.ping_send, sizeof( st.ping_send ), "%s%s", prefix,
            st.ping_subject );
  snprintf( st.pong_send, sizeof( st.pong_send ), "%s%s", prefix,
            st.pong_subject );

  if ( size > 0 ) {
    if ( size > 0x100000 )
      size = 0x100000;
    st.pad = (unsigned char *) malloc( size );
    if ( st.pad == NULL ) {
      fprintf( stderr, "%s: out of memory for -size %lu\n", progname, size );
      exit( 1 );
    }
    memset( st.pad, 0xa5, size );
    st.pad_size = (tibrv_u32) size;
  }

  err = tibrv_Open();
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "%s: Failed to open TIB/Rendezvous: %s\n", progname,
             tibrvStatus_GetText( err ) );
    exit( 1 );
  }

  err = tibrvTransport_Create( &st.transport, serviceStr, networkStr,
                               daemonStr );
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "%s: Failed to initialize transport: %s\n", progname,
             tibrvStatus_GetText( err ) );
    exit( 1 );
  }
  tibrvTransport_SetDescription( st.transport, progname );

  if ( libbatch > 0 ) {
    /* Library-side batching: accumulate plain Send()s and ship one vectored
     * EvPipe round-trip per ~libbatch bytes, with periodic Flush for the tail. */
    tibrvTransport_SetBatchMode( st.transport, TIBRV_TRANSPORT_TIMER_BATCH );
    tibrvTransport_SetBatchSize( st.transport, (tibrv_u32) libbatch );
    st.use_flush = 1;
    printf( "pingrv7test: library TIMER_BATCH, batch_size=%lu bytes\n",
            libbatch );
  }
  else if ( singlebatch > 0 || binterval > 0.0 ) {
    /* Library SINGLE_BATCH: one shared buffer per transport, drained by an
     * inline publish on the E thread (byte threshold + periodic flush timer). */
    tibrvTransport_SetBatchMode( st.transport, TIBRV_TRANSPORT_SINGLE_BATCH );
    tibrvTransport_SetBatchSize( st.transport, (tibrv_u32) singlebatch );
    if ( binterval > 0.0 )
      tibrvTransport_SetBatchInterval( st.transport, binterval );
    st.use_flush = 1;
    printf( "pingrv7test: library SINGLE_BATCH, batch_size=%lu bytes, "
            "timer=%.4gs\n", singlebatch, binterval );
  }

  signal( SIGINT, on_signal );
  signal( SIGTERM, on_signal );

  if ( reflect ) {
    /* Responder: listen on the bare ping subject, echo to pong subject. */
    err = tibrvEvent_CreateListener( &listenId, TIBRV_DEFAULT_QUEUE,
                                     reflect_callback, st.transport,
                                     st.ping_subject, &st );
    if ( err != TIBRV_OK ) {
      fprintf( stderr, "%s: listen failed on \"%s\": %s\n", progname,
               st.ping_subject, tibrvStatus_GetText( err ) );
      exit( 2 );
    }
    printf( "pingrv7test: reflecting %s -> %s\n", st.ping_subject,
            st.pong_send );
    fflush( stdout );

    while ( ! g_stop ) {
      err = tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 0.5 );
      if ( err != TIBRV_OK && err != TIBRV_TIMEOUT )
        break;
    }
  }
  else {
    /* Initiator: listen on the bare pong subject for echoes. */
    err = tibrvEvent_CreateListener( &listenId, TIBRV_DEFAULT_QUEUE,
                                     pong_callback, st.transport,
                                     st.pong_subject, &st );
    if ( err != TIBRV_OK ) {
      fprintf( stderr, "%s: listen failed on \"%s\": %s\n", progname,
               st.pong_subject, tibrvStatus_GetText( err ) );
      exit( 2 );
    }

    if ( spin ) {
      /* Max-rate tight loop: no rate timer, isolates the send path.  Drains
       * pongs periodically; flushes when the library is batching. */
      tibrv_u64 t0, t1;
      printf( "pingrv7test: spin send%s, count=%lu\n",
              st.use_flush ? " (lib batch)" : "", count );
      fflush( stdout );
      t0 = mono_ns();
      while ( ! g_stop && ( count == 0 || st.seq_sent < count ) ) {
        send_ping( &st, st.seq_sent );
        st.seq_sent++;
        if ( ( st.seq_sent & 1023 ) == 0 ) {
          if ( st.use_flush )
            tibrvTransport_Flush( st.transport );
          tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 0.0 );
        }
      }
      if ( st.use_flush )
        tibrvTransport_Flush( st.transport );
      while ( ! g_stop && st.n < st.seq_sent ) {
        if ( tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 0.5 ) == TIBRV_TIMEOUT )
          break;
      }
      t1 = mono_ns();
      printf( "spin: %llu sent in %.3f s = %.0f msg/s\n",
              (unsigned long long) st.seq_sent,
              (double) ( t1 - t0 ) / 1e9,
              (double) st.seq_sent / ( (double) ( t1 - t0 ) / 1e9 ) );
    }
    else if ( rate > 0.0 ) {
      /* Open-loop: timer-driven publish, do not wait for replies. */
      printf( "pingrv7test: pinging %s, echoes on %s, rate=%.1f/s%s\n",
              st.ping_send, st.pong_subject, rate,
              count ? "" : " (ctrl-c to stop)" );
      fflush( stdout );

      err = tibrvEvent_CreateTimer( &rateTimer, TIBRV_DEFAULT_QUEUE,
                                    rate_timer_callback,
                                    (double) st.batch / rate, &st );
      if ( err == TIBRV_OK )
        err = tibrvEvent_CreateTimer( &reportTimer, TIBRV_DEFAULT_QUEUE,
                                      report_timer_callback, 1.0, &st );
      if ( err != TIBRV_OK ) {
        fprintf( stderr, "%s: timer create failed: %s\n", progname,
                 tibrvStatus_GetText( err ) );
        exit( 2 );
      }

      while ( ! g_stop ) {
        err = tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 0.25 );
        if ( err != TIBRV_OK && err != TIBRV_TIMEOUT )
          break;
        /* Stop once we've sent everything and drained outstanding replies. */
        if ( count != 0 && st.seq_sent >= count ) {
          if ( st.use_flush )      /* push the final partial batch */
            tibrvTransport_Flush( st.transport );
          if ( st.n >= st.seq_sent )
            break;
        }
      }
    }
    else {
      /* Closed-loop: send a ping, wait for its echo, then repeat. */
      printf( "pingrv7test: pinging %s, echoes on %s, interval=%.3fs%s\n",
              st.ping_send, st.pong_subject, interval,
              count ? "" : " (ctrl-c to stop)" );
      fflush( stdout );

      while ( ! g_stop && ( count == 0 || st.seq_sent < count ) ) {
        tibrv_u64 deadline_ns;

        st.got_reply = 0;
        err = send_ping( &st, st.seq_sent );
        if ( err != TIBRV_OK ) {
          fprintf( stderr, "%s: send failed: %s\n", progname,
                   tibrvStatus_GetText( err ) );
          break;
        }
        st.seq_sent++;

        /* Wait up to timeout for the echo. */
        deadline_ns = mono_ns() + (tibrv_u64) ( timeout * 1e9 );
        while ( ! st.got_reply && ! g_stop ) {
          double remain = ( (double) deadline_ns - (double) mono_ns() ) / 1e9;
          if ( remain <= 0.0 )
            break;
          err = tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, remain );
          if ( err == TIBRV_TIMEOUT )
            break;
          if ( err != TIBRV_OK )
            break;
        }
        if ( ! st.got_reply ) {
          st.lost++;
          if ( ! st.quiet )
            printf( "seq=%llu timeout\n",
                    (unsigned long long) ( st.seq_sent - 1 ) );
        }
        else if ( interval > 0.0 && ! g_stop &&
                  ( count == 0 || st.seq_sent < count ) ) {
          struct timespec req;
          req.tv_sec  = (time_t) interval;
          req.tv_nsec = (long) ( ( interval - (double) req.tv_sec ) * 1e9 );
          nanosleep( &req, NULL );
        }
      }
    }
    print_summary( &st );
  }

  if ( st.pad != NULL )
    free( st.pad );
  tibrv_Close();
  return 0;
}
