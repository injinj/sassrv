#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>

#include <sassrv/rv7api.h>
#include <raimd/md_msg.h>
#include <raimd/rv_msg.h>

#define MIN_PARMS (2)
#define MAX_FIELDS (256)

/* A parsed field spec from the command line.
 * Format: name=tz:value
 *   t = i (signed int), u (unsigned int), s (string), o (opaque), f (float)
 *   z = byte size (1/2/4/8 for ints, 4/8 for float, length for opaque, 0 for string)
 */
typedef struct {
  const char * name;   /* points into argv */
  char         type;   /* 'i' 'u' 's' 'o' 'f' */
  int          size;   /* 1/2/4/8 for ints, 4/8 for floats, byte count for opaque */
  const char * value;  /* points into argv (the part after the ':') */
} field_spec_t;

static field_spec_t g_msg_type   = { "MSG_TYPE"  , 'i', 2,    "1" },
                    g_rec_type   = { "REC_TYPE"  , 'i', 2, "5009" },
                    g_seq_no     = { "SEQ_NO"    , 'i', 2,    "0" },
                    g_rec_status = { "REC_STATUS", 'i', 2,    "0" },
                    g_fields[ MAX_FIELDS ];
static int          g_field_count = 0;

/* Backing storage for parsed field specs.  parse_field_spec needs a
 * mutable buffer to insert '\0' separators; we copy each argv string
 * here and point the field_spec_t members into our own buffer so the
 * argv strings are never modified. */
static char         g_field_storage[ MAX_FIELDS + 4 ][ 256 ];

void
usage( void )
{
  fprintf( stderr, "pubrv7test [-service service] [-network network]\n" );
  fprintf( stderr, "           [-daemon daemon] [-field name=tz:value]...\n" );
  fprintf( stderr, "           subject_list\n" );
  fprintf( stderr, "\n" );
  fprintf( stderr, "  -field name=tz:value  (repeatable) add a field to each published msg\n" );
  fprintf( stderr, "    t = i (signed int), u (unsigned int), s (string),\n" );
  fprintf( stderr, "        o (opaque, hex digits), f (float)\n" );
  fprintf( stderr, "    z = byte size:\n" );
  fprintf( stderr, "        i,u: 1, 2, 4, or 8\n" );
  fprintf( stderr, "        f:   4 or 8\n" );
  fprintf( stderr, "        s:   0 (length is strlen)\n" );
  fprintf( stderr, "        o:   number of bytes (value is 2*z hex digits)\n" );
  fprintf( stderr, "    Examples: SEQ_NO=i2:12  PRICE=f8:1.25  TAG=s0:hello  BLOB=o4:DEADBEEF\n" );
  fprintf( stderr, "\n" );
  fprintf( stderr, "  If no -field is given, a built-in default field set is sent.\n" );
  exit( 1 );
}

/* Parse one "name=tz:value" string in-place.  The argv string is split with
 * '\0' bytes so the returned pointers are valid for as long as argv lives. */
static int
parse_field_spec( char * spec, field_spec_t * out )
{
  char * eq, * colon;

  eq = strchr( spec, '=' );
  if ( eq == NULL || eq == spec )
    return -1;
  *eq = '\0';
  out->name = spec;

  /* eq+1 should be 'tz', then ':', then value. */
  if ( eq[ 1 ] == '\0' )
    return -1;
  out->type = eq[ 1 ];
  switch ( out->type ) {
    case 'i': case 'u': case 's': case 'o': case 'f':
      break;
    default:
      return -1;
  }

  colon = strchr( eq + 2, ':' );
  if ( colon == NULL )
    return -1;
  *colon = '\0';

  /* Size between type letter and colon. */
  if ( colon == eq + 2 ) {
    /* No size given. */
    out->size = 0;
  }
  else {
    char * endp;
    long   sz = strtol( eq + 2, &endp, 10 );
    if ( *endp != '\0' || sz < 0 || sz > 65535 )
      return -1;
    out->size = (int) sz;
  }
  out->value = colon + 1;

  /* Validate size against type. */
  switch ( out->type ) {
    case 'i': case 'u':
      if ( out->size != 1 && out->size != 2 && out->size != 4 && out->size != 8 )
        return -1;
      break;
    case 'f':
      if ( out->size != 4 && out->size != 8 )
        return -1;
      break;
    case 's':
      /* size 0 means "use strlen"; non-zero is accepted but unused. */
      break;
    case 'o':
      if ( out->size <= 0 )
        return -1;
      break;
  }
  return 0;
}

/* Parse 2*nbytes hex digits from src into dst.  Returns 0 on success. */
static int
hex_decode( const char * src, unsigned char * dst, int nbytes )
{
  int i;
  for ( i = 0; i < nbytes; i++ ) {
    int hi, lo;
    char c1 = src[ 2 * i ], c2 = src[ 2 * i + 1 ];
    if ( c1 == '\0' || c2 == '\0' )
      return -1;
    if      ( c1 >= '0' && c1 <= '9' ) hi = c1 - '0';
    else if ( c1 >= 'a' && c1 <= 'f' ) hi = c1 - 'a' + 10;
    else if ( c1 >= 'A' && c1 <= 'F' ) hi = c1 - 'A' + 10;
    else return -1;
    if      ( c2 >= '0' && c2 <= '9' ) lo = c2 - '0';
    else if ( c2 >= 'a' && c2 <= 'f' ) lo = c2 - 'a' + 10;
    else if ( c2 >= 'A' && c2 <= 'F' ) lo = c2 - 'A' + 10;
    else return -1;
    dst[ i ] = (unsigned char) ( ( hi << 4 ) | lo );
  }
  return 0;
}

/* Apply one parsed field to a message.  Returns the tibrv status. */
static tibrv_status
apply_field( tibrvMsg msg, const field_spec_t * f )
{
  switch ( f->type ) {
    case 'i': {
      long long v = strtoll( f->value, NULL, 0 );
      switch ( f->size ) {
        case 1: return tibrvMsg_AddI8Ex ( msg, f->name, (tibrv_i8)  v, 0 );
        case 2: return tibrvMsg_AddI16Ex( msg, f->name, (tibrv_i16) v, 0 );
        case 4: return tibrvMsg_AddI32Ex( msg, f->name, (tibrv_i32) v, 0 );
        case 8: return tibrvMsg_AddI64Ex( msg, f->name, (tibrv_i64) v, 0 );
      }
      break;
    }
    case 'u': {
      unsigned long long v = strtoull( f->value, NULL, 0 );
      switch ( f->size ) {
        case 1: return tibrvMsg_AddU8Ex ( msg, f->name, (tibrv_u8)  v, 0 );
        case 2: return tibrvMsg_AddU16Ex( msg, f->name, (tibrv_u16) v, 0 );
        case 4: return tibrvMsg_AddU32Ex( msg, f->name, (tibrv_u32) v, 0 );
        case 8: return tibrvMsg_AddU64Ex( msg, f->name, (tibrv_u64) v, 0 );
      }
      break;
    }
    case 'f': {
      double v = strtod( f->value, NULL );
      switch ( f->size ) {
        case 4: return tibrvMsg_AddF32Ex( msg, f->name, (tibrv_f32) v, 0 );
        case 8: return tibrvMsg_AddF64Ex( msg, f->name, (tibrv_f64) v, 0 );
      }
      break;
    }
    case 's':
      return tibrvMsg_AddStringEx( msg, f->name, f->value, 0 );
    case 'o': {
      unsigned char buf[ 65536 ];
      if ( f->size > (int) sizeof( buf ) )
        return TIBRV_INVALID_ARG;
      if ( hex_decode( f->value, buf, f->size ) != 0 )
        return TIBRV_INVALID_ARG;
      return tibrvMsg_AddOpaqueEx( msg, f->name, buf, (tibrv_u32) f->size, 0 );
    }
  }
  return TIBRV_INVALID_ARG;
}

/* Apply the built-in default field set (matches the historical behavior). */
static tibrv_status
apply_default_fields( tibrvMsg msg )
{
  tibrv_status err;
  if ( (err = apply_field( msg, &g_msg_type   )) != TIBRV_OK ||
       (err = apply_field( msg, &g_rec_type   )) != TIBRV_OK ||
       (err = apply_field( msg, &g_seq_no     )) != TIBRV_OK ||
       (err = apply_field( msg, &g_rec_status )) != TIBRV_OK )
    return err;
  return TIBRV_OK;
}

int
get_InitParms( int argc, char* argv[], int min_parms, char** serviceStr,
               char** networkStr, char** daemonStr )
{
  int i = 1;

  if ( argc < min_parms )
    usage();

  while ( i + 2 <= argc && *argv[ i ] == '-' ) {
    if ( strcmp( argv[ i ], "-service" ) == 0 ) {
      *serviceStr = argv[ i + 1 ];
      i += 2;
    }
    else if ( strcmp( argv[ i ], "-network" ) == 0 ) {
      *networkStr = argv[ i + 1 ];
      i += 2;
    }
    else if ( strcmp( argv[ i ], "-daemon" ) == 0 ) {
      *daemonStr = argv[ i + 1 ];
      i += 2;
    }
    else if ( strcmp( argv[ i ], "-field" ) == 0 ) {
      size_t n;
      field_spec_t * f;
      char * s;

      if ( g_field_count >= MAX_FIELDS ) {
        fprintf( stderr, "too many -field arguments (max %d)\n", MAX_FIELDS );
        exit( 1 );
      }
      if ( memcmp( "MSG_TYPE=", argv[ i + 1 ], 9 ) == 0 ) {
        f = &g_msg_type;
        s = g_field_storage[ MAX_FIELDS ];
      }
      else if ( memcmp( "REC_TYPE=", argv[ i + 1 ], 9 ) == 0 ) {
        f = &g_rec_type;
        s = g_field_storage[ MAX_FIELDS + 1 ];
      }
      else if ( memcmp( "SEQ_NO=", argv[ i + 1 ], 7 ) == 0 ) {
        f = &g_seq_no;
        s = g_field_storage[ MAX_FIELDS + 2 ];
      }
      else if ( memcmp( "REC_STATUS=", argv[ i + 1 ], 11 ) == 0 ) {
        f = &g_rec_status;
        s = g_field_storage[ MAX_FIELDS + 3 ];
      }
      else {
        f = &g_fields[ g_field_count ];
        s = g_field_storage[ g_field_count ];
        g_field_count++;
      }
      n = strlen( argv[ i + 1 ] );
      if ( n >= sizeof( g_field_storage[ 0 ] ) ) {
        fprintf( stderr, "-field spec too long: %s\n", argv[ i + 1 ] );
        exit( 1 );
      }
      memcpy( s, argv[ i + 1 ], n + 1 );
      if ( parse_field_spec( s, f ) != 0 ) {
        fprintf( stderr, "invalid -field spec: %s\n", argv[ i + 1 ] );
        usage();
      }
      i += 2;
    }
    else {
      usage();
    }
  }

  return ( i );
}

int
main( int argc, char** argv )
{
  tibrv_status   err;
  int            currentArg;
  tibrvTransport transport;
  tibrvMsg       pubMsg;
  char           pubSubject[ 1024 ];
  int            i;

  char* serviceStr = NULL;
  char* networkStr = NULL;
  char* daemonStr  = NULL;

  char* progname = argv[ 0 ];

  currentArg = get_InitParms( argc, argv, MIN_PARMS, &serviceStr, &networkStr,
                              &daemonStr );
  err        = tibrv_Open();
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "%s: Failed to open TIB/Rendezvous: %s\n", progname,
             tibrvStatus_GetText( err ) );
    exit( 1 );
  }

  err = tibrvTransport_Create( &transport, serviceStr, networkStr, daemonStr );
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "%s: Failed to initialize transport: %s\n", progname,
             tibrvStatus_GetText( err ) );
    exit( 1 );
  }

  tibrvTransport_SetDescription( transport, progname );
  i = ( argc - currentArg );
  if ( i == 0 ) {
    fprintf( stderr, "%s: No subscriptions\n", progname );
    exit( 1 );
  }

  for ( i = 0; i + currentArg < argc; i++ ) {

    printf( "pubrv7test: Publishing to subject %s\n", argv[ i + currentArg ] );

    tibrvMsg_Create( &pubMsg );
    snprintf( pubSubject, sizeof( pubSubject ), "%s", argv[ i + currentArg ] );
    tibrvMsg_SetSendSubject( pubMsg, pubSubject );

    err = apply_default_fields( pubMsg );
    if ( err == TIBRV_OK ) {
      int j;
      for ( j = 0; j < g_field_count && err == TIBRV_OK; j++ ) {
        err = apply_field( pubMsg, &g_fields[ j ] );
        if ( err != TIBRV_OK ) {
          fprintf( stderr,
                   "%s: failed to add field %s (type=%c size=%d): %s\n",
                   progname, g_fields[ j ].name, g_fields[ j ].type,
                   g_fields[ j ].size, tibrvStatus_GetText( err ) );
        }
      }
    }
    if ( err != TIBRV_OK ) {
      tibrvMsg_Destroy( pubMsg );
      exit( 2 );
    }
    tibrvMsg_Print( pubMsg );

    err = tibrvTransport_Send( transport, pubMsg );
    tibrvMsg_Destroy( pubMsg );

    if ( err != TIBRV_OK ) {
      fprintf( stderr, "%s: Error %s publishing to \"%s\"\n", progname,
               tibrvStatus_GetText( err ), argv[ i + currentArg ] );
      exit( 2 );
    }
  }

  /*while ( tibrvQueue_Dispatch( TIBRV_DEFAULT_QUEUE ) == TIBRV_OK )
    ;*/

  tibrv_Close();

  return 0;
}
