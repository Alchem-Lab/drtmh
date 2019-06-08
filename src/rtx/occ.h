#ifndef NOCC_RTX_OCC_H_
#define NOCC_RTX_OCC_H_

#include "all.h"

#if ENABLE_TXN_API
#include "txn_interface.h"
#else
#include "tx_operator.hpp"
#endif

#include "logger.hpp"

#include "core/rworker.h"
#include "core/utils/latency_profier.h"
#include "core/utils/count_vector.hpp"

#include "util/timer.h"

#include <vector>

namespace nocc {

using namespace oltp;

namespace rtx {

class RdmaChecker;

#if ENABLE_TXN_API
class OCC : public TxnAlg {
#else
class OCC : public TXOpBase {
#endif

#include "occ_internal_structure.h"
 public:
  // nid: local node id. If == -1, all operations go through the network
  // resposne_node == nid: enable local accesses.
  // response_node == -1, all local operations go through network
  OCC(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int cid,int response_node);

  // provide a hook to init RDMA based contents, using TXOpBase
  OCC(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
      RdmaCtrl *cm,RScheduler *sched,int ms):
#if ENABLE_TXN_API
      TxnAlg(worker,db,rpc_handler,nid,tid,cid,response_node,cm,sched,ms),
#else
      TXOpBase(worker,db,rpc_handler,cm,sched,response_node,tid,ms),// response_node shall always equal *real node id*
#endif
      read_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
      write_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
      read_set_(),write_set_(),
      cor_id_(cid),response_node_(nid)
  {

  }

  void set_logger(Logger *log) { logger_ = log; }

  // start a TX
  virtual void begin(yield_func_t &yield);

  // commit a TX
  virtual bool commit(yield_func_t &yield);

#if ENABLE_TXN_API

  inline __attribute__((always_inline))
  virtual int read(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    if(pid == node_id_)
      return local_read(tableid,key,len,yield);
    else {
      // remote case
      return remote_read(pid,tableid,key,len,yield);
    }
  }
  
  template <int tableid,typename V>
  int read(int pid,uint64_t key,yield_func_t &yield) {
    return read(pid, tableid, key, sizeof(V), yield);    
  }

#else
  template <int tableid,typename V> // the value stored corresponding to tableid
  int  read(int pid,uint64_t key,yield_func_t &yield) {
    if(pid == node_id_)
      return local_read(tableid,key,sizeof(V),yield);
    else {
      // remote case
      return remote_read(pid,tableid,key,sizeof(V),yield);
    }    
  }
#endif
  /**
   * A read is called, but the value stored in the readset is not valid.
   * The value will become valid after user called indirect_yield.
   */
  template <int tableid,typename V> // the value stored corresponding to tableid
  int  pending_read(int pid,uint64_t key,yield_func_t &yield) {
    if(pid == node_id_)
      return local_read(tableid,key,sizeof(V),yield);
    else
      return pending_remote_read(pid,tableid,key,sizeof(V),yield);    
  }

#if ENABLE_TXN_API
  inline __attribute__((always_inline))
  virtual int write(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
      int index;

      if(pid == node_id_)
        index = local_read(tableid,key,len,yield);
      else {
        // remote case
        index = remote_read(pid,tableid,key,len,yield);
      }

      //note that either the local_read or remote_read function
      //has append a new read-set item into the read-set.
      //we just need to return true here since the read always succeeds in OCC.
      return add_to_write(index);    
  }

  template <int tableid,typename V> // the value stored corresponding to tableid
  int  write(int pid,uint64_t key,yield_func_t &yield) {
    return write(pid, tableid, key, sizeof(V), yield);
  }

  inline __attribute__((always_inline))
  virtual char* load_read(int idx, size_t len, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = read_set_;  
    assert(idx < set.size());
    ASSERT(len == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

    if(set[idx].data_ptr == NULL
       && set[idx].pid != node_id_) {

      // do actual reads here
      auto replies = send_batch_read();
      assert(replies > 0);
      worker_->indirect_yield(yield);

      parse_batch_result(replies);
      assert(set[idx].data_ptr != NULL);
      start_batch_rpc_op(read_batch_helper_);
    }

    return (set[idx].data_ptr);
  }

  inline __attribute__((always_inline))
  virtual char* load_write(int idx, size_t len, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = write_set_;  
    assert(idx < set.size());
    ASSERT(len == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

    if(set[idx].data_ptr == NULL
       && set[idx].pid != node_id_) {

      // do actual reads here
      auto replies = send_batch_read();
      assert(replies > 0);
      worker_->indirect_yield(yield);

      parse_batch_result(replies);
      assert(set[idx].data_ptr != NULL);
      start_batch_rpc_op(read_batch_helper_);
    }

    return (set[idx].data_ptr);
  }
#endif

  // directly add the record to the write-set
  template <int tableid,typename V> // the value stored corresponding to tableid
  int  add_to_write(int pid,uint64_t key,yield_func_t &yield) {
    if(pid == node_id_){
      assert(false); // not implemented
    }
    else {
      // remote case
      return add_batch_write(tableid,key,pid,sizeof(V),yield);
    }
    return -1;
  }

  template <typename V>
  V *get_readset(int idx,yield_func_t &yield) {
    assert(idx < read_set_.size());
    ASSERT(sizeof(V) == read_set_[idx].len) <<
        "excepted size " << (int)(read_set_[idx].len)  << " for table " << (int)(read_set_[idx].tableid) << "; idx " << idx;

    if(read_set_[idx].data_ptr == NULL
       && read_set_[idx].pid != node_id_) {

      // do actual reads here
      auto replies = send_batch_read();
      assert(replies > 0);
      worker_->indirect_yield(yield);

      parse_batch_result(replies);
      assert(read_set_[idx].data_ptr != NULL);
      start_batch_rpc_op(read_batch_helper_);
    }
    return (V *)(read_set_[idx].data_ptr);
  }

  template <typename V>
  V *get_writeset(int idx,yield_func_t &yield) {
    assert(idx < write_set_.size());
    return (V *)write_set_[idx].data_ptr;
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
  int insert(int pid,uint64_t key,V *val,yield_func_t &yield) {
    if(pid == node_id_)
      return local_insert(tableid,key,(char *)val,sizeof(V),yield);
    else {
      return remote_insert(pid,tableid,key,sizeof(V),yield);
    }
    return -1;    
  }

  // add a specific item in read-set to writeset
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

  virtual int      local_read(int tableid,uint64_t key,int len,yield_func_t &yield);
  virtual int      local_insert(int tableid,uint64_t key,char *val,int len,yield_func_t &yield);
  virtual int      remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield);
  virtual int      pending_remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    return remote_read(pid,tableid,key,len,yield);
  }
  virtual int      remote_insert(int pid,int tableid,uint64_t key,int len,yield_func_t &yield);

  // if local, the batch_get will return the results
  virtual void     start_batch_read();
  virtual int      add_batch_read(int tableid,uint64_t key,int pid,int len);
  virtual int      add_batch_insert(int tableid,uint64_t key,int pid,int len);
  virtual int      add_batch_write(int tableid,uint64_t key,int pid,int len,yield_func_t &yield);
  virtual int      send_batch_read(int idx = 0);
  virtual bool     parse_batch_result(int num);

  inline __attribute__((always_inline))
  virtual void gc_readset() {
    for(auto it = read_set_.begin();it != read_set_.end();++it) {
      //if(it->pid == node_id_)
      free((*it).data_ptr);
    }
  }
  inline __attribute__((always_inline))
  virtual void gc_writeset() {
    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      //if(it->pid == node_id_)
      free((*it).data_ptr);
    }
  }

  virtual bool lock_writes(yield_func_t &yield);
  virtual bool release_writes(yield_func_t &yield);
  virtual bool validate_reads(yield_func_t &yield);
  virtual void write_back(yield_func_t &yield);
  virtual void log_remote(yield_func_t &yield);

  void write_back_oneshot(yield_func_t &yield);

 protected:
  std::vector<ReadSetItem>  read_set_;
  std::vector<ReadSetItem>  write_set_;  // stores the index of readset

  // helper to send batch read/write operations
  BatchOpCtrlBlock read_batch_helper_;
  BatchOpCtrlBlock write_batch_helper_;

  const int cor_id_;
  const int response_node_;

  Logger *logger_       = NULL;

  bool abort_ = false;
  char reply_buf_[MAX_MSG_SIZE];

  // helper functions
  void register_default_rpc_handlers();

 private:
  // RPC handlers
  void read_rpc_handler(int id,int cid,char *msg,void *arg);
  void lock_rpc_handler(int id,int cid,char *msg,void *arg);
  void release_rpc_handler(int id,int cid,char *msg,void *arg);
  void commit_rpc_handler(int id,int cid,char *msg,void *arg);
  void validate_rpc_handler(int id,int cid,char *msg,void *arg);

  void commit_oneshot_handler(int id,int cid,char *msg,void *arg);

  void backup_get_handler(int id,int cid, char *msg,void *arg);
 protected:
  void prepare_write_contents();

  friend RdmaChecker;

  DISABLE_COPY_AND_ASSIGN(OCC);

 public:
  // some counting
#include "occ_statistics.h"
};

};
}; // namespace nocc

#include "occ_iterator.hpp"

#endif
