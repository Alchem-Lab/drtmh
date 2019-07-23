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
// #define SUNDIAL_DEBUG
// #define SUNDIAL_NO_LOCK
//#define SUNDIAL_NOWAIT
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

  bool try_lock_read_rdma(int index, yield_func_t &yield);
  bool try_update_rdma(yield_func_t &yield);

  void release_reads(yield_func_t &yield);
  void release_writes(yield_func_t &yield, bool all = true);


  bool renew_lease_local(MemNode* node, uint32_t wts, uint32_t commit_id);

  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    char* data_ptr = (char*)malloc(len);
    for(auto&item : write_set_) {
      if(item.key == key && item.tableid == tableid) {
        memcpy(data_ptr, item.data_ptr, len); // not efficient
        read_set_.emplace_back(tableid, key, item.node, data_ptr, 0, len, pid, -1, -1);
        return read_set_.size() - 1;
      }
    }
    for(auto&item : read_set_) {
      if(item.key == key && item.tableid == tableid) {
        memcpy(data_ptr, item.data_ptr, len); // not efficient
        read_set_.emplace_back(tableid, key, item.node, data_ptr, 0, len, pid, -1, -1);
        return read_set_.size() - 1;
      }
    }

    read_set_.emplace_back(tableid, key, (MemNode*)NULL, data_ptr, 0, len, pid, -1, -1);
    int index = read_set_.size() - 1;

#if ONE_SIDED_READ
    START(temp);
    uint64_t off = 0;
    if(pid != node_id_) {
    // if(pid != response_node_) {
      char* data_ptr = (char*)Rmalloc(sizeof(MemNode) + len);
      // atomicly read?
      off = rdma_read_val(pid, tableid, key, len, data_ptr, yield, sizeof(RdmaValHeader));
      RdmaValHeader *header = (RdmaValHeader*)data_ptr;
      // auto seq = header->seq;
      data_ptr += sizeof(RdmaValHeader);
      read_set_.back().node = (MemNode*)off;
      read_set_.back().data_ptr = data_ptr;
      read_set_.back().wts = WTS(header->seq);
      read_set_.back().rts = RTS(header->seq);
      assert(off != 0);
    }
    else {
      auto node = local_lookup_op(tableid, key);
      assert(node != NULL);
      char* value = (char*)(node->value);
      RdmaValHeader* h = (RdmaValHeader*)value;
      // get wts and rts
      read_set_.back().wts = WTS(h->seq);
      read_set_.back().rts = RTS(h->seq);
      char* data_ptr = (char*)malloc(sizeof(RdmaValHeader) + len);
      
      memcpy(data_ptr, value, sizeof(RdmaValHeader) + len);
      // get real value
      read_set_.back().data_ptr = data_ptr + sizeof(RdmaValHeader);
      read_set_.back().value = value;
    }
    commit_id_ = std::max(commit_id_, read_set_.back().wts);
    END(temp);
#else
    if(!try_read_rpc(index, yield)) {
      // abort
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
      if(item.key == key && item.tableid == tableid) {
        // fprintf(stderr, "[SUNDIAL INFO] remote write already in write set (no data in write set now)\n");
        LOG(7) <<"[SUNDIAL WARNING] remote write already in write set (no data in write set now)";
        return index;
      }
      ++index;
    }
    write_set_.emplace_back(tableid,key,(MemNode*)NULL,(char *)NULL,0,len,pid, -1, -1);
    index = write_set_.size() - 1;
    // sundial exec: lock the remote record and get the info
#if ONE_SIDED_READ
    // if(pid != response_node_) {
    if(pid != node_id_) {
      uint64_t off = 0;
      char* data_ptr = (char*)Rmalloc(sizeof(MemNode) + len);
      // LOG(3) << "before get off, key " << (int)key;
      START(log);
      off = rdma_read_val(pid, tableid, key, len, data_ptr, yield, sizeof(RdmaValHeader), false);
      END(log);
      // LOG(3) << "after get off";
      RdmaValHeader *header = (RdmaValHeader*)data_ptr;
      // auto seq = header->seq;
      data_ptr += sizeof(RdmaValHeader);
      write_set_.back().node = (MemNode*)off;
      write_set_.back().data_ptr = data_ptr;
      assert(off != 0);
    }
    else {
      auto node = local_lookup_op(tableid, key);
      assert(node != NULL);

      char* data_ptr = (char*)malloc(sizeof(RdmaValHeader) + len);
      write_set_.back().data_ptr = data_ptr + sizeof(RdmaValHeader);
      write_set_.back().value = (char*)(node->value);
      // memcpy(data_ptr, (char*)value, sizeof(RdmaValHeader) + len);
    }
    if(!try_lock_read_rdma(index, yield)) {
      // abort
      release_reads(yield);
      release_writes(yield, false);
      return -1;
    }
#else
    if(!try_lock_read_rpc(index, yield)) {
      // abort
      release_reads(yield);
      release_writes(yield, false);
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
        lock_req_ = new RDMACASLockReq(cid);
        unlock_req_ = new RDMAFAUnlockReq(cid, 0);

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
    //if(false) {
    if(!try_renew_lease_rpc(item.pid, item.tableid, item.key, item.wts, commit_id_, yield)) {
      // abort
      release_reads(yield);
      release_writes(yield);
      return NULL; // to abort
    }
    assert(item.data_ptr != NULL);
    return item.data_ptr;
  }

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
#if ONE_SIDED_READ
    return try_update_rdma(yield);
#else
    return try_update_rpc(yield);
#endif
  }
protected:
  std::vector<SundialReadSetItem> read_set_;
  std::vector<SundialReadSetItem> write_set_;

  uint32_t commit_id_ = 0;

  // helper to send batch read/write operations
  BatchOpCtrlBlock read_batch_helper_;
  BatchOpCtrlBlock write_batch_helper_;
  RDMACASLockReq* lock_req_;
  RDMAFAUnlockReq* unlock_req_;
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
  void release_rpc_handler(int id,int cid,char *msg,void *arg);
};
}
}
