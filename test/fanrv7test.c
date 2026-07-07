#define _POSIX_C_SOURCE 200809L

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#include <sassrv/rv7api.h>

/*
 * fanrv7test -- one-way fan-out latency / throughput / loss tester.
 *
 * This models the dominant messaging use case: a single publisher fanning a
 * stream of messages out to many subscribers.  Unlike pingrv7test (which times
 * a round trip), this measures the ONE-WAY latency from publisher to each
 * receiver, plus jitter, throughput, and message loss.
 *
 *   -pub :  publish a numbered, timestamped stream on a subject at a rate.
 *   -sub :  subscribe, and for every message measure
 *             latency  = recv_time - publish_time   (needs synced clocks
 *                                                     cross-host; exact on one
 *                                                     host -- same clock)
 *             jitter   = |latency[i] - latency[i-1]|
 *             loss     = gaps in the sequence number
 *             rate     = messages/sec, bytes/sec
 *           Reports min/avg/max + p50/p99/p99.9 from a histogram.
 *
 * FAN-OUT: start one -pub and N -sub processes on the same subject.  Each
 * subscriber prints its own summary; comparing them shows the delivery skew
 * across receivers (does subscriber #1 get each message far ahead of #8?), and
 * how latency degrades as you add subscribers.
 *
 * Clocks: the publish timestamp is CLOCK_REALTIME nanoseconds, so one-way
 * latency is meaningful across hosts only to the accuracy of clock sync
 * (PTP/NTP).  On a single host both processes read the same clock, so it is
 * exact.  Loss, jitter and throughput are differential and need no sync.
 *
 * Field layout:
 *   FSEQ  u64  sequence number (0..count-1)
 *   FTS   u64  publish timestamp, CLOCK_REALTIME nanoseconds
 *   FPAD  opaque (optional) padding to control message size
 *   FEND  u32  =1 on the end-of-run sentinel (sent several times)
 */

#define HIST_BUCKETS (131072) /* histogram bucket count (see -hist-ns) */

typedef struct {
  /* config */
  tibrvTransport transport;
  const char *   subject;       /* bare subject (subscriber listen) */
  char           send_subject[ TIBRV_SUBJECT_MAX + 1 ];
  double         interval;      /* report interval seconds */
  double         idle_timeout;  /* sub: exit after this idle (s), 0 = never */
  tibrv_u64      warmup;        /* sub: discard first N samples */
  tibrv_u64      bucket_ns;     /* histogram bucket width, ns */
  int            quiet;         /* suppress per-interval lines */

  /* subscriber running state */
  tibrv_u64 *    hist;
  tibrv_u64      hist_over;     /* samples beyond histogram range */
  tibrv_u64      n;             /* latency samples counted */
  tibrv_u64      raw_recv;      /* total messages received (incl warmup) */
  tibrv_u64      bytes;         /* total payload bytes received */
  tibrv_u64      lost;          /* sequence gaps */
  tibrv_u64      reordered;     /* seq <= last seq seen */
  tibrv_u64      skew_neg;      /* negative latency (clock skew) samples */
  double         min_ns, max_ns, sum_ns, sum2_ns;
  double         last_lat_ns;   /* for jitter */
  double         sum_jit_ns, max_jit_ns;
  int            have_last;
  tibrv_u64      first_seq;
  tibrv_u64      next_seq;      /* expected next sequence */
  int            seen_any;
  tibrv_u64      t_first_ns;    /* recv time of first msg (mono) */
  tibrv_u64      t_last_ns;     /* recv time of last msg (mono) */
  /* interval-window state */
  tibrv_u64      win_n;
  tibrv_u64      win_bytes;
  double         win_min_ns, win_max_ns, win_sum_ns;
  tibrv_u64      win_lost;
} fan_state_t;

static volatile sig_atomic_t g_stop = 0;

static void on_signal( int sig ) { (void) sig; g_stop = 1; }

static tibrv_u64
real_ns( void )
{
  struct timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  return (tibrv_u64) ts.tv_sec * 1000000000ULL + (tibrv_u64) ts.tv_nsec;
}

static tibrv_u64
mono_ns( void )
{
  struct timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return (tibrv_u64) ts.tv_sec * 1000000000ULL + (tibrv_u64) ts.tv_nsec;
}

/* integer sqrt-ish for stddev without -lm dependency assumptions */
static double
dsqrt( double x )
{
  double g;
  int i;
  if ( x <= 0 ) return 0;
  g = x > 1.0 ? x : 1.0;
  for ( i = 0; i < 40; i++ ) g = 0.5 * ( g + x / g );
  return g;
}

/* latency (ns) at a given percentile fraction [0,1] from the histogram. */
static double
hist_pct( const fan_state_t * st, double frac )
{
  tibrv_u64 total = st->n;
  tibrv_u64 target, cum = 0;
  tibrv_u64 i;
  if ( total == 0 ) return 0;
  target = (tibrv_u64) ( frac * (double) total );
  for ( i = 0; i < HIST_BUCKETS; i++ ) {
    cum += st->hist[ i ];
    if ( cum >= target )
      return ( (double) i + 0.5 ) * (double) st->bucket_ns;
  }
  return st->max_ns; /* in overflow region */
}

static void
record_sample( fan_state_t * st, double lat_ns, tibrv_u32 nbytes )
{
  tibrv_u64 idx;

  st->bytes     += nbytes;
  st->win_bytes += nbytes;

  if ( lat_ns < 0 ) {
    st->skew_neg++;
    lat_ns = 0; /* fold into bucket 0 so percentiles stay sane */
  }

  if ( st->n == 0 || lat_ns < st->min_ns ) st->min_ns = lat_ns;
  if ( st->n == 0 || lat_ns > st->max_ns ) st->max_ns = lat_ns;
  st->sum_ns  += lat_ns;
  st->sum2_ns += lat_ns * lat_ns;

  if ( st->have_last ) {
    double j = lat_ns - st->last_lat_ns;
    if ( j < 0 ) j = -j;
    st->sum_jit_ns += j;
    if ( j > st->max_jit_ns ) st->max_jit_ns = j;
  }
  st->last_lat_ns = lat_ns;
  st->have_last   = 1;

  idx = (tibrv_u64) ( lat_ns / (double) st->bucket_ns );
  if ( idx < HIST_BUCKETS ) st->hist[ idx ]++;
  else                      st->hist_over++;
  st->n++;

  if ( st->win_n == 0 || lat_ns < st->win_min_ns ) st->win_min_ns = lat_ns;
  if ( st->win_n == 0 || lat_ns > st->win_max_ns ) st->win_max_ns = lat_ns;
  st->win_sum_ns += lat_ns;
  st->win_n++;
}

static void
sub_callback( tibrvEvent event, tibrvMsg message, void * closure )
{
  fan_state_t * st  = (fan_state_t *) closure;
  tibrv_u64     now = real_ns();
  tibrv_u64     seq = 0, ts = 0;
  tibrv_u32     end = 0, size = 0;

  (void) event;

  /* end-of-run sentinel? */
  if ( tibrvMsg_GetU32( message, "FEND", &end ) == TIBRV_OK && end ) {
    g_stop = 1;
    return;
  }
  if ( tibrvMsg_GetU64( message, "FTS", &ts ) != TIBRV_OK )
    return;
  tibrvMsg_GetU64( message, "FSEQ", &seq );
  tibrvMsg_GetByteSize( message, &size );

  st->raw_recv++;
  st->t_last_ns = mono_ns();

  /* sequence / loss accounting (single publisher assumed) */
  if ( ! st->seen_any ) {
    st->seen_any  = 1;
    st->first_seq = seq;
    st->next_seq  = seq + 1;
    st->t_first_ns = st->t_last_ns;
  }
  else if ( seq == st->next_seq ) {
    st->next_seq++;
  }
  else if ( seq > st->next_seq ) {
    tibrv_u64 gap = seq - st->next_seq;
    st->lost     += gap;
    st->win_lost += gap;
    st->next_seq  = seq + 1;
  }
  else {
    st->reordered++; /* seq < expected: dup or out-of-order */
  }

  /* discard warmup samples from latency stats but keep loss accounting */
  if ( st->raw_recv <= st->warmup )
    return;

  record_sample( st, (double) now - (double) ts, size );
}

static void
sub_report_timer( tibrvEvent event, tibrvMsg message, void * closure )
{
  fan_state_t * st = (fan_state_t *) closure;
  double secs;

  (void) event; (void) message;

  if ( st->idle_timeout > 0 && st->seen_any ) {
    double idle = ( (double) mono_ns() - (double) st->t_last_ns ) / 1e9;
    if ( idle > st->idle_timeout ) {
      g_stop = 1;
      return;
    }
  }
  if ( st->quiet )
    return;

  secs = st->interval > 0 ? st->interval : 1.0;
  if ( st->win_n == 0 ) {
    printf( "recv=%llu  (no messages this interval)\n",
            (unsigned long long) st->raw_recv );
  }
  else {
    printf( "recv=%llu rate=%.0f/s %.1f MB/s  lat(us) min=%.2f avg=%.2f max=%.2f"
            "  lost=%llu\n",
            (unsigned long long) st->raw_recv,
            (double) st->win_n / secs,
            ( (double) st->win_bytes / secs ) / ( 1024.0 * 1024.0 ),
            st->win_min_ns / 1000.0,
            ( st->win_sum_ns / (double) st->win_n ) / 1000.0,
            st->win_max_ns / 1000.0,
            (unsigned long long) st->win_lost );
  }
  fflush( stdout );
  st->win_n = st->win_bytes = st->win_lost = 0;
  st->win_sum_ns = 0;
}

static void
sub_summary( fan_state_t * st )
{
  double avg, var, sd, span_s;
  tibrv_u64 expected;

  printf( "\n--- fanrv7test subscriber summary ---\n" );
  expected = st->seen_any ? ( st->next_seq - st->first_seq ) : 0;
  span_s   = st->raw_recv > 1
             ? ( (double) st->t_last_ns - (double) st->t_first_ns ) / 1e9 : 0;
  printf( "messages: recv=%llu expected=%llu lost=%llu (%.4f%%) reordered=%llu\n",
          (unsigned long long) st->raw_recv,
          (unsigned long long) expected,
          (unsigned long long) st->lost,
          expected ? 100.0 * (double) st->lost / (double) expected : 0.0,
          (unsigned long long) st->reordered );
  if ( span_s > 0 )
    printf( "throughput: %.0f msg/s, %.2f MB/s over %.3fs\n",
            (double) st->raw_recv / span_s,
            ( (double) st->bytes / span_s ) / ( 1024.0 * 1024.0 ), span_s );
  if ( st->n == 0 ) {
    printf( "no latency samples (clock not synced? all warmup?)\n" );
    return;
  }
  avg = st->sum_ns / (double) st->n;
  var = ( st->sum2_ns / (double) st->n ) - avg * avg;
  sd  = dsqrt( var );
  printf( "one-way latency (us): min=%.2f avg=%.2f max=%.2f stddev=%.2f\n",
          st->min_ns / 1000.0, avg / 1000.0, st->max_ns / 1000.0, sd / 1000.0 );
  printf( "  p50=%.2f  p90=%.2f  p99=%.2f  p99.9=%.2f  p99.99=%.2f\n",
          hist_pct( st, 0.50 ) / 1000.0,
          hist_pct( st, 0.90 ) / 1000.0,
          hist_pct( st, 0.99 ) / 1000.0,
          hist_pct( st, 0.999 ) / 1000.0,
          hist_pct( st, 0.9999 ) / 1000.0 );
  printf( "jitter (us): avg=%.2f max=%.2f   (samples beyond histogram: %llu)\n",
          ( st->sum_jit_ns / (double) ( st->n > 1 ? st->n - 1 : 1 ) ) / 1000.0,
          st->max_jit_ns / 1000.0,
          (unsigned long long) st->hist_over );
  if ( st->skew_neg )
    printf( "warning: %llu samples had negative latency "
            "(clock skew / unsynced hosts)\n",
            (unsigned long long) st->skew_neg );
}

/* ---- publisher ---- */

static tibrv_status
build_msg( fan_state_t * st, tibrv_u64 seq, const unsigned char * pad,
           tibrv_u32 pad_size, tibrvMsg * out )
{
  tibrvMsg     msg;
  tibrv_status err;
  if ( (err = tibrvMsg_Create( &msg )) != TIBRV_OK )
    return err;
  tibrvMsg_SetSendSubject( msg, st->send_subject );
  if ( (err = tibrvMsg_AddU64( msg, "FSEQ", seq )) != TIBRV_OK ) goto fail;
  if ( pad != NULL )
    if ( (err = tibrvMsg_AddOpaque( msg, "FPAD", pad, pad_size )) != TIBRV_OK )
      goto fail;
  /* stamp the time last, as close to the send as possible */
  if ( (err = tibrvMsg_AddU64( msg, "FTS", real_ns() )) != TIBRV_OK ) goto fail;
  *out = msg;
  return TIBRV_OK;
fail:
  tibrvMsg_Destroy( msg );
  return err;
}

static void
publisher_run( fan_state_t * st, double rate, unsigned long count,
               unsigned long size )
{
  unsigned char * pad = NULL;
  tibrv_u32       pad_size = 0;
  tibrv_u64       start, seq;
  tibrv_u64       sent = 0;
  tibrvMsg        msg;
  tibrv_status    err;
  int             k;

  if ( size > 0 ) {
    pad = (unsigned char *) malloc( size );
    if ( pad == NULL ) { fprintf( stderr, "out of memory\n" ); exit( 1 ); }
    memset( pad, 0x5a, size );
    pad_size = (tibrv_u32) size;
  }

  printf( "fanrv7test: publishing %s rate=%s count=%s size=%lu\n",
          st->send_subject,
          rate > 0 ? "(paced)" : "(unpaced flood)",
          count ? "(limited)" : "(unlimited)", size );
  fflush( stdout );

  start = mono_ns();
  for ( seq = 0; ! g_stop && ( count == 0 || seq < count ); seq++ ) {
    if ( rate > 0 ) {
      tibrv_u64 target = start +
        (tibrv_u64) ( (double) seq * ( 1e9 / rate ) );
      tibrv_u64 now = mono_ns();
      if ( target > now ) {
        tibrv_u64 wait = target - now;
        if ( wait > 2000 ) { /* sleep for waits > 2us, else spin */
          struct timespec req;
          req.tv_sec  = wait / 1000000000ULL;
          req.tv_nsec = wait % 1000000000ULL;
          nanosleep( &req, NULL );
        }
        else {
          while ( mono_ns() < target ) { /* spin for sub-us precision */ }
        }
      }
    }
    err = build_msg( st, seq, pad, pad_size, &msg );
    if ( err != TIBRV_OK ) {
      fprintf( stderr, "build failed: %s\n", tibrvStatus_GetText( err ) );
      break;
    }
    err = tibrvTransport_Send( st->transport, msg );
    tibrvMsg_Destroy( msg );
    if ( err != TIBRV_OK ) {
      fprintf( stderr, "send failed: %s\n", tibrvStatus_GetText( err ) );
      break;
    }
    sent++;
  }

  /* end-of-run sentinel, sent a few times in case some are lost */
  for ( k = 0; k < 5; k++ ) {
    if ( tibrvMsg_Create( &msg ) != TIBRV_OK ) break;
    tibrvMsg_SetSendSubject( msg, st->send_subject );
    tibrvMsg_AddU32( msg, "FEND", 1 );
    tibrvMsg_AddU64( msg, "FSEQ", seq );
    tibrvTransport_Send( st->transport, msg );
    tibrvMsg_Destroy( msg );
    { struct timespec r = { 0, 2000000 }; nanosleep( &r, NULL ); } /* 2ms */
  }

  {
    double span = ( (double) mono_ns() - (double) start ) / 1e9;
    printf( "fanrv7test: sent %llu messages in %.3fs (%.0f msg/s)\n",
            (unsigned long long) sent, span,
            span > 0 ? (double) sent / span : 0.0 );
  }
  if ( pad ) free( pad );
}

void
usage( void )
{
  fprintf( stderr,
    "fanrv7test [-service s] [-network n] [-daemon d] -pub|-sub [opts] subject\n"
    "\n"
    "  -pub             publisher: send a numbered, timestamped stream\n"
    "  -sub             subscriber (default): measure one-way latency/loss\n"
    "\n"
    " publisher options:\n"
    "  -rate N          messages/sec (0 = unpaced flood, default 10000)\n"
    "  -count N         stop after N messages (0 = unlimited, default 1000000)\n"
    "  -size N          add N opaque payload bytes per message\n"
    "\n"
    " subscriber options:\n"
    "  -warmup N        discard first N samples from latency stats (default 0)\n"
    "  -idle S          exit after S seconds with no messages (0 = never, def 5)\n"
    "  -hist-ns N       latency histogram bucket width in ns (default 1000)\n"
    "\n"
    " common:\n"
    "  -interval S      report interval seconds (default 1.0)\n"
    "  -quiet           suppress per-interval report lines\n"
    "\n"
    "Fan-out: run one -pub and many -sub on the same subject; compare each\n"
    "subscriber summary for delivery skew and latency vs. fan-out degree.\n" );
  exit( 1 );
}

int
main( int argc, char ** argv )
{
  tibrv_status err;
  fan_state_t  st;
  tibrvEvent   listenId = 0, reportTimer = 0;
  char *       serviceStr = NULL, * networkStr = NULL, * daemonStr = NULL;
  int          is_pub = 0;
  double       rate = 10000.0, interval = 1.0, idle = 5.0;
  unsigned long count = 1000000, size = 0, warmup = 0, hist_ns = 1000;
  char *       progname = argv[ 0 ];
  int          i = 1;

  memset( &st, 0, sizeof( st ) );

  while ( i < argc && *argv[ i ] == '-' ) {
    if ( strcmp( argv[ i ], "-service" ) == 0 && i + 1 < argc ) {
      serviceStr = argv[ ++i ];
    } else if ( strcmp( argv[ i ], "-network" ) == 0 && i + 1 < argc ) {
      networkStr = argv[ ++i ];
    } else if ( strcmp( argv[ i ], "-daemon" ) == 0 && i + 1 < argc ) {
      daemonStr = argv[ ++i ];
    } else if ( strcmp( argv[ i ], "-pub" ) == 0 ) {
      is_pub = 1;
    } else if ( strcmp( argv[ i ], "-sub" ) == 0 ) {
      is_pub = 0;
    } else if ( strcmp( argv[ i ], "-rate" ) == 0 && i + 1 < argc ) {
      rate = strtod( argv[ ++i ], NULL );
    } else if ( strcmp( argv[ i ], "-count" ) == 0 && i + 1 < argc ) {
      count = strtoul( argv[ ++i ], NULL, 10 );
    } else if ( strcmp( argv[ i ], "-size" ) == 0 && i + 1 < argc ) {
      size = strtoul( argv[ ++i ], NULL, 10 );
    } else if ( strcmp( argv[ i ], "-warmup" ) == 0 && i + 1 < argc ) {
      warmup = strtoul( argv[ ++i ], NULL, 10 );
    } else if ( strcmp( argv[ i ], "-idle" ) == 0 && i + 1 < argc ) {
      idle = strtod( argv[ ++i ], NULL );
    } else if ( strcmp( argv[ i ], "-hist-ns" ) == 0 && i + 1 < argc ) {
      hist_ns = strtoul( argv[ ++i ], NULL, 10 );
      if ( hist_ns == 0 ) hist_ns = 1;
    } else if ( strcmp( argv[ i ], "-interval" ) == 0 && i + 1 < argc ) {
      interval = strtod( argv[ ++i ], NULL );
    } else if ( strcmp( argv[ i ], "-quiet" ) == 0 ) {
      st.quiet = 1;
    } else {
      usage();
    }
    i++;
  }

  if ( i >= argc ) {
    fprintf( stderr, "%s: missing subject\n", progname );
    usage();
  }
  st.subject      = argv[ i ];
  st.interval     = interval;
  st.idle_timeout = idle;
  st.warmup       = warmup;
  st.bucket_ns    = hist_ns;
  snprintf( st.send_subject, sizeof( st.send_subject ), "%s%s", "", st.subject );

  err = tibrv_Open();
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "%s: tibrv_Open: %s\n", progname,
             tibrvStatus_GetText( err ) );
    exit( 1 );
  }
  err = tibrvTransport_Create( &st.transport, serviceStr, networkStr,
                               daemonStr );
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "%s: transport: %s\n", progname,
             tibrvStatus_GetText( err ) );
    exit( 1 );
  }
  tibrvTransport_SetDescription( st.transport, progname );
  signal( SIGINT, on_signal );
  signal( SIGTERM, on_signal );

  if ( is_pub ) {
    publisher_run( &st, rate, count, size );
  }
  else {
    st.hist = (tibrv_u64 *) calloc( HIST_BUCKETS, sizeof( tibrv_u64 ) );
    if ( st.hist == NULL ) { fprintf( stderr, "out of memory\n" ); exit( 1 ); }

    err = tibrvEvent_CreateListener( &listenId, TIBRV_DEFAULT_QUEUE,
                                     sub_callback, st.transport,
                                     st.subject, &st );
    if ( err != TIBRV_OK ) {
      fprintf( stderr, "%s: listen \"%s\": %s\n", progname, st.subject,
               tibrvStatus_GetText( err ) );
      exit( 2 );
    }
    err = tibrvEvent_CreateTimer( &reportTimer, TIBRV_DEFAULT_QUEUE,
                                  sub_report_timer,
                                  interval > 0 ? interval : 1.0, &st );
    if ( err != TIBRV_OK ) {
      fprintf( stderr, "%s: timer: %s\n", progname,
               tibrvStatus_GetText( err ) );
      exit( 2 );
    }
    printf( "fanrv7test: subscribing to %s\n", st.subject );
    fflush( stdout );

    while ( ! g_stop ) {
      err = tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 0.25 );
      if ( err != TIBRV_OK && err != TIBRV_TIMEOUT )
        break;
    }
    sub_summary( &st );
    free( st.hist );
  }

  tibrv_Close();
  return 0;
}
