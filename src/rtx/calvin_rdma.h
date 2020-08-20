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

typedef rwsets_t::ReadSetItem ReadSetItem;

#if ENABLE_TXN_API
class CALVIN : public TxnAlg {
#else
class CALVIN : public TXOpBase {
#endif

protected:
  // return the last index in the read-set
  int local_read(int tableid,uint64_t key,int len,yield_func_t &yield) {
    MemNode *node = local_lookup_op(tableid, key);
    assert(node != NULL);
    assert(node->value != NULL);

    // add to read-set
    read_set_.emplace_back(tableid,key,(MemNode *)node,(char*)NULL,0,len,response_node_);
    return read_set_.size() - 1;
  }

  // return the last index in the write-set
  int local_write(int tableid,uint64_t key,int len,yield_func_t &yield) {
    MemNode *node = local_lookup_op(tableid, key);
    assert(node != NULL);
    assert(node->value != NULL);

    // add to write-set
    write_set_.emplace_back(tableid,key,(MemNode *)node,(char*)NULL,0,len,response_node_);
    return write_set_.size() - 1;
  }

  int local_insert(int tableid,uint64_t key,char *val,int len,yield_func_t &yield) {
    // char *data_ptr = (char *)malloc(len);
    // uint64_t seq;
    // auto node = local_insert_op(tableid,key,seq);
    // memcpy(data_ptr,val,len);
    // write_set_.emplace_back(tableid,key,node,data_ptr,seq,len,node_id_);
    // return write_set_.size() - 1;
    return -1;
  }

  // return the last index in the read-set
  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    MemNode *node = NULL;
    // fprintf(stderr, "response_node_ = %d, in remote read: pid = %d, tableid = %d, key = %d\n", response_node_, pid, tableid, key);
    if (pid == response_node_) {
      node = local_lookup_op(tableid, key);
      assert(node != NULL);
      assert(node->value != NULL);
    }
    read_set_.emplace_back(tableid,key,(MemNode *)node,(char*)NULL,
                           0,
                           len,pid);
    return read_set_.size() - 1;
  }

  // return the last index in the write-set
  int remote_write(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    MemNode *node = NULL;
    // fprintf(stderr, "response_node_ = %d, in remote write: pid = %d, tableid = %d, key = %d\n", response_node_, pid, tableid, key);    
    if (pid == response_node_) {
      node = local_lookup_op(tableid, key);
      assert(node != NULL);
      assert(node->value != NULL);
    }
    write_set_.emplace_back(tableid,key,(MemNode *)node,(char*)NULL,
                           0,
                           len,pid);
    return write_set_.size() - 1;
  }

  int remote_insert(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    // remote insert for calvin is not needed: all insertions happen locally.
    return -1;
  }

#if 0
  void prepare_write_contents() {
    // Notice that it should contain local records
    // This function has to be called after lock + validation success
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
      // ASSERT(node_id_ == response_node_) << 
              // "node_id_ = " << node_id_ << ", response_node_ =" << response_node_;
      if(it->pid == response_node_) {
        // fprintf(stderr, "Rfreed %p.\n", (*it).data_ptr);
        Rfree((*it).data_ptr);
      } else {
        // fprintf(stderr, "Freed %p.\n", (*it).data_ptr);
        free((*it).data_ptr);
      }
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

  
  bool prepare_commit(yield_func_t &yield);
  void broadcast_decision(bool commit_or_abort, yield_func_t &yield);

  void prepare_write_contents();
  void log_remote(yield_func_t &yield);
  void release_reads(yield_func_t &yield);
  void release_writes(yield_func_t &yield);
  void write_back(yield_func_t &yield);

public:
  CALVIN(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
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

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_TXN_BROADCAST_INPUT) != 0
          fprintf(stderr, "CALVIN uses ONE_SIDED TXN_BROADCAST_INPUT.\n");
#else
          fprintf(stderr, "CALVIN uses RPC TXN_BROADCAST_INPUT.\n");
#endif

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_VALUE_FORWARDING) != 0
          fprintf(stderr, "CALVIN uses ONE_SIDED VALUE_FORWARDING.\n");
#else
          fprintf(stderr, "CALVIN uses RPC VALUE_FORWARDING.\n");
#endif

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_EPOCH_SYNC) != 0
          fprintf(stderr, "CALVIN uses ONE_SIDED EPOCH_SYNC.\n");
#else
          fprintf(stderr, "CALVIN uses RPC EPOCH_SYNC.\n");
#endif

#if ONE_SIDED_READ == 1 || ONE_SIDED_READ == 2 && (HYBRID_CODE & RCC_USE_ONE_SIDED_ASYNC_REPLICATING) != 0
          fprintf(stderr, "CALVIN uses ONE_SIDED ASYNC_REPLICATING.\n");
#else
          fprintf(stderr, "CALVIN uses RPC ASYNC_REPLICATING.\n");
#endif
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
    int index;
    // step 1: find offset of the key in either local/remote memory
    if(pid == response_node_)
      index = local_read(tableid,key,len,yield);
    else {
      // remote case
      index = remote_read(pid,tableid,key,len,yield);
    }

    return index;
  }

  // read2 is used only by the sequencer thread to generate the read set
  static int read2(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    read_set.emplace_back(tableid,key,(MemNode *)NULL,(char*)NULL,
                           0,
                           len,pid);
    return read_set.size() - 1;
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
    int index;
    if(pid == response_node_)
      index = local_read(tableid,key,sizeof(V),yield);
    else {
      // remote case
      index = remote_read(pid,tableid,key,sizeof(V),yield);
    }

    return index;
  }
#endif

#if ENABLE_TXN_API
  // lock the record and add the record to the write-set
  inline __attribute__((always_inline))
  virtual int write(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    int index;

    // step 1: find offset of the key in either local/remote memory
    if(pid == response_node_)
      index = local_write(tableid,key,len,yield);
    else {
      // remote case
      index = remote_write(pid,tableid,key,len,yield);
    }

    return index;
  }

  // write2 is used only by the sequencer thread to generate the write set
  static int write2(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    write_set.emplace_back(tableid,key,(MemNode *)NULL,(char*)NULL,
                           0,
                           len,pid);
    return write_set.size() - 1;
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int write(int pid,uint64_t key,yield_func_t &yield) {
    return write(pid, tableid, key, sizeof(V), yield);
  }

  // Actually load the data, can be done only after all locks are acquired.
  inline __attribute__((always_inline))
  virtual char* load_read(int idx, size_t len, yield_func_t &yield) {
    // fprintf(stderr, "in load read.\n");
    std::vector<ReadSetItem> &set = read_set_;
    // fprintf(stderr, "pid = %d\n", set[idx].pid);
    
    assert(idx < set.size());
    ASSERT(len == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

    if(set[idx].pid == response_node_) {
      if (set[idx].data_ptr == NULL) {
        // local actual read
        char *temp_val = (char *)Rmalloc(len);
        // fprintf(stdout, "load read Rmalloc %p\n", temp_val);

        uint64_t seq;
        auto node = local_get_op(set[idx].tableid,set[idx].key, temp_val, set[idx].len,seq,
                                 db_->_schemas[set[idx].tableid].meta_len);
        if(unlikely(node == NULL)) {
          Rfree(temp_val);
          assert(false);
          return NULL;
        }

        set[idx].data_ptr = temp_val;
      }
    }

    return (set[idx].data_ptr);
  }


  // Actually load the data, can be done only after all locks are acquired.
  inline __attribute__((always_inline))
  virtual char* load_write(int idx, size_t len, yield_func_t &yield) {
    // fprintf(stderr, "in load write.\n");
    std::vector<ReadSetItem> &set = write_set_;
    // fprintf(stderr, "pid = %d\n", set[idx].pid);

    assert(idx < set.size());
    ASSERT(len == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx << " however len = " << len;

    // note that we must use response_node_ here to indicate the actual
    // node id.
    // fprintf(stdout, "pid in load write %d = %d. my node id = %d\n", idx, set[idx].pid, response_node_);
    if(set[idx].pid == response_node_) {
      // assert(set[idx].data_ptr == NULL);
      if (set[idx].data_ptr == NULL) {
        // local actual write
        char *temp_val = (char *)Rmalloc(len);
        // fprintf(stdout, "load write Rmalloc %p\n", temp_val);

        uint64_t seq;
        auto node = local_get_op(set[idx].tableid,set[idx].key, temp_val, set[idx].len,seq, db_->_schemas[set[idx].tableid].meta_len);
        if(unlikely(node == NULL)) {
          Rfree(temp_val);
          assert(false);
          return NULL;
        }
        
        set[idx].data_ptr = temp_val;
      }
      // fprintf(stdout, "in load write: %f@%p\n", *(float*)set[idx].data_ptr, set[idx].data_ptr);
    }

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

  void display_sets() {
    fprintf(stdout, "in reads set: \n");
    for (int i = 0; i < read_set_.size(); i++) {
      fprintf(stdout, "pid = %d", read_set_[i].pid);
      fprintf(stdout, " tableid = %d", read_set_[i].tableid);
      fprintf(stdout, " len = %d", read_set_[i].len);
      fprintf(stdout, " key = %lu\n", read_set_[i].key);      
    }
    fprintf(stdout, "in write set: \n");
    for (int i = 0; i < write_set_.size(); i++) {
      fprintf(stdout, "pid = %d", write_set_[i].pid);
      fprintf(stdout, " tableid = %d", write_set_[i].tableid);
      fprintf(stdout, " len = %d", write_set_[i].len);
      fprintf(stdout, " key = %lu\n", write_set_[i].key);      
    }
  }

  bool sync_reads(int req_idx, yield_func_t &yield);

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

    if(set[idx].pid != response_node_) {
        ;
    } else {
      if (set[idx].data_ptr == NULL) {
        // local actual read
        char *temp_val = (char *)malloc(len);
        uint64_t seq;
        auto node = local_get_op(set[idx].tableid,set[idx].key, temp_val, set[idx].len,seq,
                                 db_->_schemas[set[idx].tableid].meta_len);
        if(unlikely(node == NULL)) {
          free(temp_val);
          assert(false);
          return NULL;
        }

        set[idx].data_ptr = temp_val;
      }
    }

    return (V*)(set[idx].data_ptr);
  }
#endif

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
    if(pid == response_node_)
      return local_insert(tableid,key,(char *)val,sizeof(V),yield);
    else {
      return remote_insert(pid,tableid,key,sizeof(V),yield);
    }
    return -1;
  }

  // start a TX
  virtual void begin(yield_func_t &yield) {
    assert(false);
    read_set_.clear();
    write_set_.clear();
    #if USE_DSLR
      dslr_lock_manager->init();
    #endif
    txn_start_time = rwlock::get_now();
    // the txn_end_time is approximated using the LEASE_TIME
    txn_end_time = txn_start_time + rwlock::LEASE_TIME;
  }

  void begin(det_request* req, yield_func_t &yield) {
#if DEBUG_LEVEL==1
    fprintf(stderr, "executing txn with ts %lx of type %d. \n", req->timestamp, req->req_idx);
#endif
    req_ = req;
    read_set_.clear();
    write_set_.clear();
    rwsets_t* sets = (rwsets_t*)req->req_info;
    int reads = sets->nReads, writes = sets->nWrites;
    for (int i = 0; i < reads; i++) {
      read_set_.push_back(sets->access[i]);
    }
    for (int i = 0; i < writes; i++) {
      write_set_.push_back(sets->access[reads+i]);
    }
  }

  // commit a TX
  virtual bool commit(yield_func_t &yield) {

#if TX_ONLY_EXE
    return dummy_commit();
#endif

#if TX_TWO_PHASE_COMMIT_STYLE > 0
    START(twopc)
    bool vote_commit = prepare_commit(yield); // broadcasting prepare messages and collecting votes
    // broadcast_decision(vote_commit, yield);
    END(twopc);
    if (!vote_commit) {
      release_reads(yield);
      release_writes(yield);
      gc_readset();
      gc_writeset();
      // for calvin, we shouldn't expect an abort, during normal execution.
      // since the scheduler guarantee's there is no abort after the txn is granted locks.
      // However, however, with 2pc, aborts do happen, here we won't handle 2pc-induced aborts here
      // and we simply return false like other txns.
      return false;
    }
#endif

    asm volatile("" ::: "memory");

    prepare_write_contents();
    log_remote(yield); // log remote using *logger_*

    // write the modifications of records back
    write_back(yield);
    release_reads(yield);
    release_writes(yield);
    gc_readset();
    gc_writeset();
#if DEBUG_LEVEL==1
    fprintf(stderr, "txn with timestamp %lx committed.\n", req_->timestamp);
#endif
    return true;
  }

public:
  std::vector<ReadSetItem>  read_set_;
  std::vector<ReadSetItem>  write_set_;
  det_request* req_ = NULL;
  // this two sets are used by the sequencer to generate for different benchmarks.
  // they are not belonging to any CALVIN object and any execution threads' coroutines.
  static std::vector<ReadSetItem> read_set;
  static std::vector<ReadSetItem> write_set;
protected:
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

  friend class BenchWorker;
  friend class BankWorker;

public:
#include "occ_statistics.h"

  // helper functions
  void register_default_rpc_handlers();

 private:
  // RPC handlers
  void forward_rpc_handler(int id,int cid,char *msg,void *arg);
};

} // namespace rtx
} // namespace nocc
