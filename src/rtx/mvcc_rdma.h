#pragma once

#include "tx_config.h"

#if ENABLE_TXN_API
#include "txn_interface.h"
#else
#include "tx_operator.hpp"
#include "core/utils/latency_profier.h"
#include "core/utils/count_vector.hpp"
#endif

#include "logger.hpp"

#include "core/logging.h"

#include "rdma_req_helper.hpp"

#include "rwlock.hpp"
#define MVCC_NOWAIT
namespace nocc {

namespace rtx {
#if ENABLE_TXN_API
class MVCC : public TxnAlg {
#else
class MVCC : public TXOpBase {
#endif
#include "occ_internal_structure.h"
protected:
// rpc functions

  bool try_lock_read_rpc(int index, yield_func_t &yield);
  bool try_read_rpc(int index, yield_func_t &yield);

  void release_reads(yield_func_t &yield);
  void release_writes(yield_func_t &yield, bool all = true);


  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    char* data_ptr = (char*)malloc(len);
    for(auto&item : write_set_) {
      if(item.key == key && item.tableid == tableid) {
        memcpy(data_ptr, item.data_ptr, len); // not efficient
        read_set_.emplace_back(tableid, key, item.node, data_ptr, 0, len, pid);
        return read_set_.size() - 1;
      }
    }
    for(auto&item : read_set_) {
      if(item.key == key && item.tableid == tableid) {
        memcpy(data_ptr, item.data_ptr, len); // not efficient
        read_set_.emplace_back(tableid, key, item.node, data_ptr, 0, len, pid);
        return read_set_.size() - 1;
      }
    }
    read_set_.emplace_back(tableid, key, (MemNode*)NULL, data_ptr, 0, len, pid);
    int index = read_set_.size() - 1;

#if ONE_SIDED_READ
#else
    if(!try_read_rpc(index, yield)) {
      release_reads(yield);
      release_writes(yield);
      return -1;
    }
    if(pid != node_id_){
      process_received_data(reply_buf_, read_set_.back());
    }
#endif
    return index;
  }

  int remote_write(int pid, int tableid, uint64_t key, int len, yield_func_t &yield) {

    int index = 0;
    for(auto& item : write_set_) {
      if(item.key == key && item.tableid == tableid) {
        LOG(7) <<"[MVCC WARNING] remote write already in write set (no data in write set now)";
        return index;
      }
      ++index;
    }
    write_set_.emplace_back(tableid,key,(MemNode*)NULL,(char *)NULL,0,len,pid);
    index = write_set_.size() - 1;
#if ONE_SIDED_READ
#else
    if(!try_lock_read_rpc(index, yield)) {
      // abort
      release_reads(yield);
      release_writes(yield, false);
      return -1;
    }
    // get the results
    if(pid != node_id_) {
      process_received_data(reply_buf_, write_set_.back());
    }
#endif
    return index;
  }

public:
  MVCC(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
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
      cor_id_(cid),response_node_(nid) {

        register_default_rpc_handlers();
        memset(reply_buf_,0,MAX_MSG_SIZE);
        read_set_.clear();
        write_set_.clear();
      }

  inline __attribute__((always_inline))
  virtual int write(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    return remote_write(pid, tableid, key, len, yield);
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int write(int pid,uint64_t key,yield_func_t &yield) {
    return write(pid, tableid, key, sizeof(V), yield);
  }

  inline __attribute__((always_inline))
  virtual int read(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    return remote_read(pid, tableid, key, len, yield);
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int read(int pid,uint64_t key,yield_func_t &yield) {
    return read(pid, tableid, key, sizeof(V), yield);
  }

  inline __attribute__((always_inline))
  virtual char* load_write(int idx, size_t len, yield_func_t &yield) {
    assert(write_set_[idx].data_ptr != NULL);
    return write_set_[idx].data_ptr;
  }

  inline __attribute__((always_inline))
  virtual char* load_read(int idx, size_t len, yield_func_t &yield) {
    auto& item = read_set_[idx];
    assert(item.data_ptr != NULL);
    return item.data_ptr;
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int insert(int pid,uint64_t key,V *val,yield_func_t &yield) {
    assert(false);
    return -1;
  }

	virtual void begin(yield_func_t &yield) {
    read_set_.clear();
    write_set_.clear();
    txn_start_time = (rwlock::get_now_nano() << 10) + response_node_ * 80 + worker_id_ * 10 + cor_id_ + 1; // TODO: may be too large
    // the txn_end_time is approximated using the LEASE_TIME
    // txn_end_time = txn_start_time + rwlock::LEASE_TIME;
  }

  virtual bool commit(yield_func_t &yield) {
#if ONE_SIDED_READ
#else
#endif
  	return true;
  }
  
  template <typename V>
  inline __attribute__((always_inline))
  V *get_writeset(int idx,yield_func_t &yield) {
    return NULL;
    // return get_set_helper<V>(write_set_, idx, yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_readset(int idx,yield_func_t &yield) {
    return NULL;
    // return get_set_helper<V>(write_set_, idx, yield);
  }
protected:
  std::vector<ReadSetItem> read_set_;
  std::vector<ReadSetItem> write_set_;


  // helper to send batch read/write operations
  BatchOpCtrlBlock read_batch_helper_;
  BatchOpCtrlBlock write_batch_helper_;

  const int cor_id_;
  const int response_node_;

  char* rpc_op_send_buf_;
  char reply_buf_[MAX_MSG_SIZE];

  uint64_t txn_start_time = 0;
  uint64_t txn_end_time = 0;

public:
#include "occ_statistics.h"

  void register_default_rpc_handlers();
private:
  void lock_read_rpc_handler(int id,int cid,char *msg,void *arg);
  void read_rpc_handler(int id,int cid,char *msg,void *arg);
  
  inline __attribute__((always_inline))
  bool check_write(MVCCHeader* header, uint64_t timestamp) {
    volatile uint64_t rts = header->rts;
    if(rts > timestamp) {
      return false;
    }
    for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
      volatile uint64_t wts = header->wts[i];
      if(wts > timestamp){
        return false;
      }
    }
    return true;
  }

  inline __attribute__((always_inline))
  int check_read(MVCCHeader* header, uint64_t timestamp) {
    // earlier write is processing
    if(header->lock < timestamp) return -1;
    int max_wts = 0;
    int pos = -1;
    for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
      if(header->wts[i] < timestamp && header->wts[i] > max_wts) {
        max_wts = header->wts[i];
        pos = i;
      }
    }
    // pos can be -1 here, meaning no available item
    return pos;
  }

  void process_received_data(char* ptr, ReadSetItem& item) {
    char* reply = ptr + 1;
    item.seq = *(uint64_t*)reply;
    if(item.data_ptr == NULL)
      item.data_ptr = (char*)malloc(item.len);
    memcpy(item.data_ptr, reply + sizeof(uint64_t), item.len);
  }
// rpc handlers
};


}
}