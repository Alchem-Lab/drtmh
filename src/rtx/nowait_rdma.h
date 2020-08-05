#pragma once

#include "tx_config.h"

#if ENABLE_TXN_API
#include "txn_interface.h"
#else
#include "tx_operator.hpp"
#include "core/utils/latency_profier.h"
#include "core/utils/count_vector.hpp"
#include "dslr.h"
#endif

#include "logger.hpp"
#include "two_phase_committer.hpp"
#include "two_phase_commit_mem_manager.hpp"
#include "core/logging.h"

#include "rdma_req_helper.hpp"

#include "rwlock.hpp"

namespace nocc {

namespace rtx {

/**
 * Two-phase Locking with no-wait conflict handling.
 */
#if ENABLE_TXN_API
class NOWAIT : public TxnAlg {
#else
class NOWAIT : public TXOpBase {
#endif
#include "occ_internal_structure.h"

protected:
  // return the last index in the read-set
  int local_read(int tableid,uint64_t key,int len,yield_func_t &yield) {

    char *temp_val = (char *)malloc(len);
    uint64_t seq;

    auto node = local_get_op(tableid,key,temp_val,len,seq,db_->_schemas[tableid].meta_len);

    if(unlikely(node == NULL)) {
      free(temp_val);
      return -1;
    }
    // add to read-set
    int idx = read_set_.size();
    read_set_.emplace_back(tableid,key,node,temp_val,seq,len,node_id_);
    return idx;
  }

  // return the last index in the write-set
  int local_write(int tableid,uint64_t key,int len,yield_func_t &yield) {

    char *temp_val = (char *)malloc(len);
    uint64_t seq;

    auto node = local_get_op(tableid,key,temp_val,len,seq,db_->_schemas[tableid].meta_len);

    if(unlikely(node == NULL)) {
      free(temp_val);
      return -1;
    }

    // add to write-set
    write_set_.emplace_back(tableid,key,node,temp_val,seq,len,node_id_);
    return write_set_.size() - 1;
  }

  int local_insert(int tableid,uint64_t key,char *val,int len,yield_func_t &yield) {
    char *data_ptr = (char *)malloc(len);
    uint64_t seq;
    auto node = local_insert_op(tableid,key,seq);
    memcpy(data_ptr,val,len);
    write_set_.emplace_back(tableid,key,node,data_ptr,seq,len,node_id_);
    return write_set_.size() - 1;
  }

#if ONE_SIDED_READ
  // return the last index in the read-set
  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    // START(read_lat);
    char *data_ptr = (char *)Rmalloc(sizeof(MemNode) + len);
    ASSERT(data_ptr != NULL);

    uint64_t off = 0;
#if INLINE_OVERWRITE
    off = rdma_lookup_op(pid,tableid,key,data_ptr,yield);
    MemNode *node = (MemNode *)data_ptr;
    auto seq = node->seq;
    data_ptr = data_ptr + sizeof(MemNode);
#else
    off = rdma_read_val(pid,tableid,key,len,data_ptr,yield,sizeof(RdmaValHeader),false);
    RdmaValHeader *header = (RdmaValHeader *)data_ptr;
    auto seq = header->seq;
    data_ptr = data_ptr + sizeof(RdmaValHeader);
#endif
    ASSERT(off != 0) << "RDMA remote read key error: tab " << tableid << " key " << key;
    // END(read_lat);
    read_set_.emplace_back(tableid,key,(MemNode *)off,data_ptr,
                           seq,
                           len,pid);
    return read_set_.size() - 1;
  }

  // return the last index in the write-set
  int remote_write(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {

    char *data_ptr = (char *)Rmalloc(sizeof(MemNode) + len);
    ASSERT(data_ptr != NULL);

    uint64_t off = 0;
#if INLINE_OVERWRITE
    off = rdma_lookup_op(pid,tableid,key,data_ptr,yield);
    MemNode *node = (MemNode *)data_ptr;
    auto seq = node->seq;
    data_ptr = data_ptr + sizeof(MemNode);
#else
    off = rdma_read_val(pid,tableid,key,len,data_ptr,yield,sizeof(RdmaValHeader), false);
    RdmaValHeader *header = (RdmaValHeader *)data_ptr;
    auto seq = header->seq;
    data_ptr = data_ptr + sizeof(RdmaValHeader);
#endif
    ASSERT(off != 0) << "RDMA remote read key error: tab " << tableid << " key " << key;

    write_set_.emplace_back(tableid,key,(MemNode *)off,data_ptr,
                           seq,
                           len,pid);
    return write_set_.size() - 1;
  }

  int remote_insert(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    assert(false);
    return add_batch_insert(tableid,key,pid,len);
  }

#else

  // return the last index in the read-set
  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    return add_batch_read(tableid,key,pid,len);
  }

  // return the last index in the write-set
  int remote_write(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    return add_batch_write(tableid,key,pid,len);
  }

  int remote_insert(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    return add_batch_insert(tableid,key,pid,len);
  }

#endif

  /** helper functions to batch rpc operations below
    */
  inline __attribute__((always_inline))
  virtual void start_batch_read() {
    start_batch_rpc_op(read_batch_helper_);
  }

  inline __attribute__((always_inline))
  int add_batch_read(int tableid,uint64_t key,int pid,int len) {
    // add a batch read request
    int idx = read_set_.size();
    add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                                 /* init RTXReadItem */ RTX_REQ_READ,pid,key,tableid,len,(idx<<1));
    read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
    return idx;
  }

  inline __attribute__((always_inline))
  int add_batch_write(int tableid,uint64_t key,int pid,int len) {
    // add a batch read request
    int idx = write_set_.size();
    add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                                 /* init RTXReadItem */ RTX_REQ_READ_LOCK,pid,key,tableid,len,(idx<<1)+1);
    // fprintf(stdout, "write rpc batched: write_set idx = %d, payload = %d\n", idx, );
    write_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
    return idx;
  }

  inline __attribute__((always_inline))
  int add_batch_insert(int tableid,uint64_t key,int pid,int len) {
    assert(false);
    // add a batch read request
    int idx = read_set_.size();
    add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                                 /* init RTXReadItem */ RTX_REQ_INSERT,pid,key,tableid,len,idx);
    read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
    return idx;
  }

  inline __attribute__((always_inline))
  int send_batch_read(int idx = 0) {
    return send_batch_rpc_op(read_batch_helper_,cor_id_,RTX_RW_RPC_ID);
  }

  inline __attribute__((always_inline))
  bool parse_batch_result(int num) {

    char *ptr  = reply_buf_;
    for(uint i = 0;i < num;++i) {
      // parse a reply header
      ReplyHeader *header = (ReplyHeader *)(ptr);
      ptr += sizeof(ReplyHeader);
      for(uint j = 0;j < header->num;++j) {
        WaitDieResponse *item = (WaitDieResponse *)ptr;
        if ((item->idx & 1) == 0) { // an idx in read-set
          // fprintf(stdout, "rpc response: read_set idx = %d, payload = %d\n", item->idx, item->payload);
          item->idx >>= 1;
          read_set_[item->idx].data_ptr = (char *)malloc(read_set_[item->idx].len);
          memcpy(read_set_[item->idx].data_ptr, ptr + sizeof(WaitDieResponse),read_set_[item->idx].len);
        } else {
          // fprintf(stdout, "rpc response: write_set idx = %d, payload = %d\n", item->idx, item->payload);
          item->idx >>= 1;
          write_set_[item->idx].data_ptr = (char *)malloc(write_set_[item->idx].len);
          memcpy(write_set_[item->idx].data_ptr, ptr + sizeof(WaitDieResponse),write_set_[item->idx].len);
        }
        ptr += (sizeof(WaitDieResponse) + item->payload);
      }
    }
    return true;
  }

  /** helper functions to batch rpc operations above
    */

#if 0
  void prepare_write_contents() {
    // Notice that it should contain local records
    // This function has to be called after lock
    write_batch_helper_.clear_buf(); // only clean buf, not the mac_set

    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      if ((*it).pid != node_id_) {
        add_batch_entry_wo_mac<RtxWriteItem>(write_batch_helper_,
                                             (*it).pid,
                                             /* init write item */ (*it).pid,(*it).tableid,(*it).key,(*it).len);
        memcpy(write_batch_helper_.req_buf_end_,(*it).data_ptr,(*it).len);
        write_batch_helper_.req_buf_end_ += (*it).len;
      }
    }
  }
#endif
  /**
   * GC the read/write set is a little complex using RDMA.
   * Since some pointers are allocated from the RDMA heap, not from local heap.
   */
  void gc_helper(std::vector<ReadSetItem> &set) {
    for(auto it = set.begin();it != set.end();++it) {
      if(it->pid != node_id_) {
#if INLINE_OVERWRITE
        Rfree((*it).data_ptr - sizeof(MemNode));
#else
        Rfree((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      }
      else
        free((*it).data_ptr);

    }
  }

  // overwrite GC functions, to use Rfree
  void gc_readset() {
    gc_helper(read_set_);
  }

  void gc_writeset() {
    gc_helper(write_set_);
  }

  bool dummy_commit() {
    // clean remaining resources
    gc_readset();
    gc_writeset();
    return true;
  }

  bool try_lock_read_w_rdma(int index, yield_func_t &yield);
  bool try_lock_write_w_rdma(int index, yield_func_t &yield);
  bool try_lock_read_w_rwlock_rpc(int index, yield_func_t &yield);
  bool try_lock_write_w_rwlock_rpc(int index, yield_func_t &yield);

  void release_reads_w_rdma(yield_func_t &yield);
  void release_writes_w_rdma(yield_func_t &yield);
  void release_reads(yield_func_t &yield, bool all = true);
  void release_writes(yield_func_t &yield, bool all = true);
  
  bool prepare_commit(yield_func_t &yield);
  void broadcast_decision(bool commit_or_abort, yield_func_t &yield);

  void prepare_write_contents();
  void log_remote(yield_func_t &yield);
  void write_back_w_rdma(yield_func_t &yield);
  void write_back(yield_func_t &yield);

public:
  NOWAIT(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
          RdmaCtrl *cm,RScheduler* sched,int ms) :
#if ENABLE_TXN_API
      TxnAlg(worker,db,rpc_handler,nid,tid,cid,response_node,cm,sched,ms),
#else
      TXOpBase(worker,db,rpc_handler,cm,sched,response_node,tid,ms),// response_node shall always equal *real node id*
#endif
      read_set_(),write_set_(),
      read_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
      write_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
      rpc_op_send_buf_(rpc_->get_static_buf(MAX_MSG_SIZE)),
      cor_id_(cid),response_node_(nid)
  {
#if !ENABLE_TXN_API
        dslr_lock_manager = new DSLR(worker, db, rpc_handler, 
                                 nid, tid, cid, response_node, 
                                 cm, sched, ms);
#endif

    if(worker_id_ == 0 && cor_id_ == 0) {
#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_LOCK) != 0
      fprintf(stderr, "NOWAIT uses ONE_SIDED LOCK.\n");
#else
      fprintf(stderr, "NOWAIT uses RPC LOCK.\n");
#endif

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_LOG) != 0
      fprintf(stderr, "NOWAIT uses ONE_SIDED LOG.\n");
#else
      fprintf(stderr, "NOWAIT uses RPC LOG.\n");
#endif

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_2PC) != 0
      fprintf(stderr, "NOWAIT uses ONE_SIDED 2PC.\n");
#else
      fprintf(stderr, "NOWAIT uses RPC 2PC.\n");
#endif

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_RELEASE) != 0
      fprintf(stderr, "NOWAIT uses ONE_SIDED RELEASE.\n");
#else
      fprintf(stderr, "NOWAIT uses RPC RELEASE.\n");
#endif

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_COMMIT) != 0
      fprintf(stderr, "NOWAIT uses ONE_SIDED COMMIT.\n");
#else
      fprintf(stderr, "NOWAIT uses RPC COMMIT.");
#endif

    }

    register_default_rpc_handlers();
    memset(reply_buf_,0,MAX_MSG_SIZE);
    lock_req_ = new RDMACASLockReq(cid);
    read_req_ = new RDMAReadReq(cid);
    read_set_.clear();
    write_set_.clear();
  }

  void set_logger(Logger *log) { logger_ = log; }

  void set_two_phase_committer(TwoPhaseCommitter *committer) { two_phase_committer_ = committer; }

#if ENABLE_TXN_API
  // get the read lock of the record and actually read
  inline __attribute__((always_inline))
  virtual int read(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    if(tableid == 7) {
      int idx = read_set_.size();
      read_set_.emplace_back(tableid,key,(MemNode*)NULL,(char*)NULL,0,len,0);
      return idx;
    }
    int index;
    // step 1: find offset of the key in either local/remote memory
    if(pid == node_id_)
      index = local_read(tableid,key,len,yield);
    else {
      // remote case
      index = remote_read(pid,tableid,key,len,yield);
    }

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_LOCK) != 0
    // step 2: get the read lock. If fail, return false
    if(!try_lock_read_w_rdma(index, yield)) {
      do_release_reads(yield);
      do_release_writes(yield);
      return -1;
    }
#else
    if (!try_lock_read_w_rwlock_rpc(index, yield)) {
      do_release_reads(yield,false);
      do_release_writes(yield);
      return -1;
    }
#endif
    return index;
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int read(int pid,uint64_t key,yield_func_t &yield) {
    return read(pid, tableid, key, sizeof(V), yield);
  }

#else
  template <int tableid,typename V> // the value stored corresponding to tableid
  inline __attribute__((always_inline))
  int  read(int pid,uint64_t key,yield_func_t &yield) {
    if(pid == node_id_)
      return local_read(tableid,key,sizeof(V),yield);
    else {
      // remote case
      return remote_read(pid,tableid,key,sizeof(V),yield);
    }
  }
#endif

#if ENABLE_TXN_API
  // lock the record and add the record to the write-set
  inline __attribute__((always_inline))
  virtual int write(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    int index;
    // step 1: find offset of the key in either local/remote memory
    if(pid == node_id_)
      index = local_write(tableid,key,len,yield);
    else {
      // remote case
      index = remote_write(pid,tableid,key,len,yield);
    }

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_LOCK) != 0
    // step 3: get the write lock. If fail, return false
    // if(!try_lock_write_w_rwlock_rdma(index, yield)) {
    //   release_reads_w_rwlock_rdma(yield);
    //   release_writes_w_rwlock_rdma(yield);
    //   return -1;
    // }
    if(!try_lock_write_w_rdma(index, yield)) {
      do_release_reads(yield);
      do_release_writes(yield);
      return -1;
    }
#else
    if(!try_lock_write_w_rwlock_rpc(index, yield)) {
      do_release_reads(yield);
      do_release_writes(yield,false);
      return -1;
    }
#endif
    return index;
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int write(int pid,uint64_t key,yield_func_t &yield) {
    return write(pid, tableid, key, sizeof(V), yield);
  }

  // Actually load the data, can be done only after all locks are acquired.
  inline __attribute__((always_inline))
  virtual char* load_read(int idx, size_t len, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = read_set_;

    assert(idx < set.size());
    ASSERT(len == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

#if ONE_SIDED_READ
    return set[idx].data_ptr;
#else

    if(set[idx].data_ptr == NULL
       && set[idx].pid != node_id_) {
      // do actual reads here
      assert(false);
      START(read_lat);
      auto replies = send_batch_read();
      assert(replies > 0);
      abort_cnt[18]++;
      worker_->indirect_yield(yield);

      parse_batch_result(replies);
      assert(set[idx].data_ptr != NULL);
      END(read_lat);
      start_batch_rpc_op(read_batch_helper_);
    }
#endif
    assert(set[idx].data_ptr != NULL);
    return (set[idx].data_ptr);
  }


  // Actually load the data, can be done only after all locks are acquired.
  inline __attribute__((always_inline))
  virtual char* load_write(int idx, size_t len, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = write_set_;

    assert(idx < set.size());
    ASSERT(len == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

#if ONE_SIDED_READ
      return set[idx].data_ptr;
#else
    if(set[idx].data_ptr == NULL
       && set[idx].pid != node_id_) {
      // do actual reads here
      assert(false);
      START(read_lat);
      auto replies = send_batch_read();
      assert(replies > 0);
      abort_cnt[18]++;
      worker_->indirect_yield(yield);

      parse_batch_result(replies);
      assert(set[idx].data_ptr != NULL);
      END(read_lat);
      start_batch_rpc_op(read_batch_helper_);
    }
#endif
    assert(set[idx].data_ptr != NULL);
    return (set[idx].data_ptr);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_readset(int idx,yield_func_t &yield) {
    return (V*)load_read(idx, sizeof(V), yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_writeset(int idx,yield_func_t &yield) {
    return (V*)load_write(idx, sizeof(V), yield);
  }

#else

  template <typename V>
  inline __attribute__((always_inline))
  V *get_readset(int idx,yield_func_t &yield) {
    return get_set_helper<V>(read_set_, idx, yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_writeset(int idx,yield_func_t &yield) {
    return get_set_helper<V>(write_set_, idx, yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V* get_set_helper(std::vector<ReadSetItem> &set, int idx,yield_func_t &yield) {
    assert(idx < set.size());
    ASSERT(sizeof(V) == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

    if(set[idx].data_ptr == NULL
       && set[idx].pid != node_id_) {

      // do actual reads here
      START(read_lat);
      auto replies = send_batch_read();
      assert(replies > 0);
      abort_cnt[18]++;
      worker_->indirect_yield(yield);

      parse_batch_result(replies);
      assert(set[idx].data_ptr != NULL);
      END(read_lat);
      start_batch_rpc_op(read_batch_helper_);
    }

    return (V*)(set[idx].data_ptr);
  }
#endif
  // return the last index in the write-set
  inline __attribute__((always_inline))
  int add_to_write(int idx) {
    assert(idx >= 0 && idx < read_set_.size());
    write_set_.emplace_back(read_set_[idx]);

    // eliminate read-set
    // FIXME: is it necessary to use std::swap to avoid memcpy?
    read_set_.erase(read_set_.begin() + idx);
    return write_set_.size() - 1;
  }

  inline __attribute__((always_inline))
  int add_to_write() {
    return add_to_write(read_set_.size() - 1);
  }

  template <int tableid,typename V>
  V *get(int pid,uint64_t key,yield_func_t &yield) {
#if ENABLE_TXN_API
    int idx = read(pid,tableid,key,sizeof(V),yield);
#else
    int idx = read<tableid,V>(pid,key,yield);
#endif
    return get_readset<V>(idx,yield);
  }
  
  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int insert(int pid,uint64_t key,V *val,yield_func_t &yield) {
    // if(pid == node_id_)
    //   return local_insert(tableid,key,(char *)val,sizeof(V),yield);
    // else {
    //   return remote_insert(pid,tableid,key,sizeof(V),yield);
    // }
    return -1;
  }

  // start a TX
  virtual void begin(yield_func_t &yield) {
    read_set_.clear();
    write_set_.clear();
    #if ONE_SIDED_READ == 0 || ONE_SIDED_READ == 2
      start_batch_rpc_op(read_batch_helper_);
    #endif

    #if USE_DSLR
      dslr_lock_manager->init();
    #endif
    txn_start_time = (rwlock::get_now()<<10) + response_node_ * 80 + worker_id_*10 + cor_id_ + 1;
    // the txn_end_time is approximated using the LEASE_TIME
    txn_end_time = txn_start_time + rwlock::LEASE_TIME;
  }

#if ONE_SIDED_READ
  // commit a TX
  virtual bool commit(yield_func_t &yield) {

#if TX_ONLY_EXE
    gc_readset();
    gc_writeset();
    return dummy_commit();
#endif

    // committed.
    asm volatile("" ::: "memory");

#if TX_TWO_PHASE_COMMIT_STYLE > 0
    START(twopc)
    bool vote_commit = prepare_commit(yield); // broadcasting prepare messages and collecting votes
    // broadcast_decision(vote_commit, yield);
    END(twopc);
    if (!vote_commit) {
      do_release_reads(yield);
      do_release_writes(yield);
      return false;
    }
#endif

    prepare_write_contents();
    log_remote(yield); // log remote using *logger_*

    asm volatile("" ::: "memory");

#if 1
#if USE_DSLR
    release_reads_w_FA_rdma(yield);
    write_back_w_FA_rdma(yield);
#else
    do_release_reads(yield);
    do_write_back(yield);
#endif
#else
    /**
     * Fixme! write back w RPC now can only work with *lock_w_rpc*.
     * This is because lock_w_rpc helps fill the mac_set used in write_back.
     */
    write_back_oneshot(yield);
#endif
    abort_cnt[26]++;
    return true;
  }

#else
  virtual bool commit(yield_func_t &yield) {

    asm volatile("" ::: "memory");

#if TX_TWO_PHASE_COMMIT_STYLE > 0
    START(twopc)
    bool vote_commit = prepare_commit(yield); // broadcasting prepare messages and collecting votes
    // broadcast_decision(vote_commit, yield);
    END(twopc);
    if (!vote_commit) {
      do_release_reads(yield);
      do_release_writes(yield);
      return false;
    }
#endif

    prepare_write_contents();
    log_remote(yield); // log remote using *logger_*

    asm volatile("" ::: "memory");
  
    do_write_back(yield);
    do_release_reads(yield);
    abort_cnt[27]++;
    return true;
}


#endif

  inline void do_release_reads(yield_func_t &yield, bool release_all = true) {
#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_RELEASE) != 0
      release_reads_w_rdma(yield);
#else
      release_reads(yield, release_all);
#endif
  }

  inline void do_release_writes(yield_func_t &yield, bool release_all = true) {
#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_RELEASE) != 0
      release_writes_w_rdma(yield);
#else
      release_writes(yield, release_all);
#endif
  }

  inline void do_write_back(yield_func_t &yield) {
#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_COMMIT) != 0
      write_back_w_rdma(yield);
#else
      write_back(yield);
#endif
  }

protected:
  std::vector<ReadSetItem>  read_set_;
  std::vector<ReadSetItem>  write_set_;

  // helper to send batch read/write operations
  BatchOpCtrlBlock read_batch_helper_;
  BatchOpCtrlBlock write_batch_helper_;
  RDMACASLockReq* lock_req_;
  RDMAReadReq* read_req_;

  const int cor_id_;
  const int response_node_;

  Logger *logger_ = NULL;
  TwoPhaseCommitter *two_phase_committer_ = NULL;
  
  char* rpc_op_send_buf_;
  char reply_buf_[MAX_MSG_SIZE];

#if !ENABLE_TXN_API
  DSLR* dslr_lock_manager;
#endif

  uint64_t txn_start_time = 0;
  uint64_t txn_end_time = 0;

public:
#include "occ_statistics.h"

  // helper functions
  void register_default_rpc_handlers();

 private:
  // RPC handlers
  void read_write_rpc_handler(int id,int cid,char *msg,void *arg);
  void lock_rpc_handler(int id,int cid,char *msg,void *arg);
  void release_rpc_handler(int id,int cid,char *msg,void *arg);
  void commit_rpc_handler(int id,int cid,char *msg,void *arg);
};

} // namespace rtx
} // namespace nocc
