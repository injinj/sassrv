#ifndef __rai_sassrv__ev_rv_h__
#define __rai_sassrv__ev_rv_h__

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
}

#include <raikv/ev_tcp.h>
#include <raikv/route_ht.h>
#include <raimd/rv_msg.h>

namespace rai {
namespace sassrv {

struct RvSession;

struct EvRvListen : public kv::EvTcpListen {
  uint64_t timer_id;
  uint32_t ipaddr;
  uint16_t ipport;
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvRvListen( kv::EvPoll &p ) noexcept;
  virtual bool accept( void ) noexcept;
  int listen( const char *ip,  int port,  int opts ) {
    return this->kv::EvTcpListen::listen( ip, port, opts, "rv_listen" );
  }
};

struct EvPrefetchQueue;

struct RvSubRoute {
  uint32_t hash;
  uint32_t msg_cnt;
  uint16_t len;
  char     value[ 2 ];
  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

enum RvSubStatus {
  RV_SUB_OK        = 0,
  RV_SUB_EXISTS    = 1,
  RV_SUB_NOT_FOUND = 2
};

struct RvSubRoutePos {
  RvSubRoute * rt;
  uint32_t v;
  uint16_t off;
};

struct RvSubMap {
  kv::RouteVec<RvSubRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }

  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void ) {
    this->tab.release();
  }
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RvSubStatus put( uint32_t h,  const char *sub,  size_t len ) {
    kv::RouteLoc loc;
    RvSubRoute * rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      return RV_SUB_OK;
    }
    return RV_SUB_EXISTS;
  }
  /* update cnt for sub
   * tab[ sub ] => {cnt++} */
  RvSubStatus updcnt( uint32_t h,  const char *sub,  size_t len ) const {
    RvSubRoute * rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    rt->msg_cnt++;
    return RV_SUB_OK;
  }
  /* remove tab[ sub ] */
  RvSubStatus rem( uint32_t h,  const char *sub,  size_t len ) {
    if ( ! this->tab.remove( h, sub, len ) )
      return RV_SUB_NOT_FOUND;
    return RV_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( RvSubRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( RvSubRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
};

struct RvPatternRoute {
  uint32_t                  hash,
                            msg_cnt;
  pcre2_real_code_8       * re;
  pcre2_real_match_data_8 * md;
  uint16_t                  len;
  char                      value[ 2 ];

  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

struct RvPatternRoutePos {
  RvPatternRoute * rt;
  uint32_t v;
  uint16_t off;
};

struct RvPatternMap {
  kv::RouteVec<RvPatternRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }

  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void ) noexcept;
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RvSubStatus put( uint32_t h,  const char *sub,  size_t len,
                   RvPatternRoute *&rt ) {
    kv::RouteLoc loc;
    rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      rt->re = NULL;
      rt->md = NULL;
      return RV_SUB_OK;
    }
    return RV_SUB_EXISTS;
  }

  /* iterate first tab[ sub ] */
  bool first( RvPatternRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( RvPatternRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
};

static const uint32_t
  RV_SESSION_IVAL = 90;

static const size_t RV_MAX_SUBJ_LEN  = 1032; /* 4 segs, 1024 encoded */

enum RvStatus {
  RV_OK        = 0,
  ERR_RV_MSG   = 1, /* bad msg format */
  ERR_RV_REF   = 2, /* bad data reference */
  ERR_RV_MTYPE = 3, /* bad msg mtype field */
  ERR_RV_SUB   = 4, /* bad msg sub field */
  ERR_RV_DATA  = 5  /* bad msg data field */
};
#define RV_STATUS_STRINGS { "ok", "bad msg format", "bad rv reference", \
                            "bad rv mtype", "bad rv subject", "bad rv data" }
struct RvMsgIn {
  md::RvMsg       * msg;
  md::RvFieldIter * iter;
  md::MDReference   data;
  char            * reply;
  uint16_t          sublen,
                    replylen;
  bool              is_wild;
  uint8_t           mtype;
  char              sub[ RV_MAX_SUBJ_LEN ];
  md::MDMsgMem      mem;

  RvMsgIn() : msg( 0 ), iter( 0 ) {}

  int unpack( void *msgbuf,  size_t msglen ) noexcept;
};

struct EvRvService : public kv::EvConnection {
  static const uint8_t EV_RV_SOCK = 8;
  void * operator new( size_t, void *ptr ) { return ptr; }
  enum ProtoState {
    VERS_RECV,
    INFO_RECV,
    DATA_RECV
  };
  ProtoState   state;
  uint16_t     ipport;
  uint32_t     ipaddr;
  RvSubMap     sub_tab;
  RvPatternMap pat_tab;
  RvMsgIn      msg_in;
  uint64_t     ms, bs,  /* msgs sent, bytes sent */
               mr, br,  /* msgs recv, bytes recv */
               timer_id;
  char         session[ 48 ],
               control[ 64 ],
               userid[ 64 ],
               service[ 8 ],
               network[ 256 ];
  uint32_t     vmaj,
               vmin,
               vupd;

  EvRvService( kv::EvPoll &p ) : kv::EvConnection( p, EV_RV_SOCK ) {}
  void initialize_state( uint64_t id ) {
    this->state = VERS_RECV;
    this->ms = this->bs = 0;
    this->mr = this->br = 0;
    this->timer_id     = id;
    this->session[ 0 ] = '\0';
    this->control[ 0 ] = '\0';
    this->userid[ 0 ]  = '\0';
    this->service[ 0 ] = '\0';
    this->network[ 0 ] = '\0';
    this->vmaj = this->vmin = this->vupd = 0;
  }
  void send_info( bool agree ) noexcept;
  int recv_data( void *msg,  size_t msg_len ) noexcept;
  int respond_info( void ) noexcept;
  void add_sub( void ) noexcept;
  void rem_sub( void ) noexcept;
  void rem_all_sub( void ) noexcept;
  bool fwd_pub( void ) noexcept;
  void send( void *hdr,  size_t off,   const void *data,
             size_t data_len ) noexcept;
  bool fwd_msg( kv::EvPublish &pub,  const void *sid,  size_t sid_len ) noexcept;
  void push_free_list( void ) noexcept;
  void pop_free_list( void ) noexcept;
  void pub_session( uint8_t code ) noexcept;
  /* EvSocket */
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;
};

}
}
#endif
