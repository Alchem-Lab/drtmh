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

// #define SUNDIAL_NO_LOCK

namespace nocc {

namespace rtx {
#if ENABLE_TXN_API
class SUNDIAL : public TxnAlg {
#else
class SUNDIAL : public TXOpBase {
#endif
#include "occ_internal_structure.h"
protected:
// rpc functions
  bool try_lock_read_rpc(int index, yield_func_t &yield);
  bool try_read_rpc(int index, yield_func_t &yield);
  bool try_renew_lease_rpc(uint8_t pid, uint8_t tableid, uint64_t key, uint32_t wts, uint32_t commit_id, yield_func_t &yield);
  bool try_update_rpc(yield_func_t &yield);

  void release_reads(yield_func_t &yield);
  void release_writes(yield_func_t &yield);


  bool renew_lease_local(MemNode* node, uint32_t wts, uint32_t commit_id);

  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    char* data_ptr = (char*)malloc(len);
    for(auto&item : write_set_) {
      if(item.key == key) {
        memcpy(data_ptr, item.data_ptr, len); // not efficient
        read_set_.emplace_back(tableid, key, item.node, data_ptr, 0, len, pid, -1, -1);
        return read_set_.size() - 1;
      }
    }
    for(auto&item : read_set_) {
      if(item.key == key) {
        memcpy(data_ptr, item.data_ptr, len); // not efficient
        read_set_.emplace_back(tableid, key, item.node, data_ptr, 0, len, pid, -1, -1);
        return read_set_.size() - 1;
      }
    }

    read_set_.emplace_back(tableid, key, local_lookup_op(tableid, key), data_ptr, 0, len, pid, -1, -1);
    int index = read_set_.size() - 1;

#if ONE_SIDED_READ

#else
    if(!try_read_rpc(index, yield)) {
      release_reads(yield);
      release_writes(yield);
      return -1;
    }
    process_received_data(reply_buf_, read_set_.back(), false);
#endif
    return index;
  }

  int remote_write(int pid, int tableid, uint64_t key, int len, yield_func_t &yield) {

    int index = 0;
    for(auto& item : write_set_) {
      if(item.key == key) {
        fprintf(stderr, "[SUNDIAL INFO] remote write already in write set (no data in write set now)\n");
        return index;
      }
      ++index;
    }
    write_set_.emplace_back(tableid,key,local_lookup_op(tableid, key),(char *)NULL,0,len,pid, -1, -1);
    index = write_set_.size() - 1;
    // sundial exec: lock the remote record and get the info
#if ONE_SIDED_READ
    assert(false);
#else
    if(!try_lock_read_rpc(index, yield)) {
      assert(false);
      release_reads(yield);
      release_writes(yield);
      return -1;
    }
    // get the results
    process_received_data(reply_buf_, write_set_.back(), true);
#endif

    return index;
  }

  void process_received_data(char* received_data_ptr, SundialReadSetItem& item, bool is_write = false) {
    char* value = received_data_ptr + 1 + sizeof(SundialResponse);
    SundialResponse* header = (SundialResponse*)(received_data_ptr + 1);
    item.wts = header->wts;
    item.rts = header->rts;
    if(item.data_ptr == NULL) {
      item.data_ptr = (char*)malloc(item.len);
    }
    memcpy(item.data_ptr, value, item.len);
    if(is_write)
      commit_id_ = std::max(commit_id_, item.rts + 1);
    else
      commit_id_ = std::max(commit_id_, item.wts);
  }

public:
  SUNDIAL(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
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
        if(worker_id_ == 0 && cor_id_ == 0) {
          LOG(3) << "Use one-sided for read.";
        }

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
    if(!try_renew_lease_rpc(item.pid, item.tableid, item.key, item.wts, commit_id_, yield)) {
      // abort
      LOG(3) << "fail renew lease " << (int)item.pid;
      return NULL; // to abort
    }
    assert(item.data_ptr != NULL);
    return item.data_ptr;
  }

  // void prepare(yield_func_t &yield) {
  //   int index = 0;
  //   for(auto & item : read_set_) {
  //     if(!try_renew_lease_rpc(item.pid, item.tableid, item.key, item.wts, commit_id_, yield)) {
  //       // fail in renew lease, should abort
  //     } 
  //   } 
  // }

  
    template <typename V>
  inline __attribute__((always_inline))
  V *get_readset(int idx,yield_func_t &yield) {
    return NULL;
    // return get_set_helper<V>(read_set_, idx, yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_writeset(int idx,yield_func_t &yield) {
    return NULL;
    // return get_set_helper<V>(write_set_, idx, yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V* get_set_helper(std::vector<ReadSetItem> &set, int idx,yield_func_t &yield) {
    assert(idx < set.size());
    ASSERT(sizeof(V) == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

    // if(set[idx].data_ptr == NULL
    //    && set[idx].pid != node_id_) {

    //   // do actual reads here
    //   auto replies = send_batch_read();
    //   assert(replies > 0);
    //   worker_->indirect_yield(yield);

    //   parse_batch_result(replies);
    //   assert(set[idx].data_ptr != NULL);
    //   start_batch_rpc_op(read_batch_helper_);
    // }

    return (V*)(set[idx].data_ptr);
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int insert(int pid,uint64_t key,V *val,yield_func_t &yield) {
    assert(false);
    return -1;
  }

  // start a TX
  virtual void begin(yield_func_t &yield) {
    read_set_.clear();
    write_set_.clear();
    #if USE_DSLR
      dslr_lock_manager->init();
    #endif
    txn_start_time = rwlock::get_now();
    // the txn_end_time is approximated using the LEASE_TIME
    txn_end_time = txn_start_time + rwlock::LEASE_TIME;
  }

  virtual bool commit(yield_func_t &yield) {
    return try_update_rpc(yield);
  }
protected:
  std::vector<SundialReadSetItem> read_set_;
  std::vector<SundialReadSetItem> write_set_;

  uint32_t commit_id_ = 0;

  // helper to send batch read/write operations
  BatchOpCtrlBlock read_batch_helper_;
  BatchOpCtrlBlock write_batch_helper_;
  RDMACASLockReq* lock_req_;
  RDMAReadReq* read_req_;

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
// rpc handlers
  void lock_read_rpc_handler(int id,int cid,char *msg,void *arg);
  void read_rpc_handler(int id,int cid,char *msg,void *arg);
  void renew_lease_rpc_handler(int id,int cid,char *msg,void *arg);
  void update_rpc_handler(int id,int cid,char *msg,void *arg);
};
}
}