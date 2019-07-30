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
  bool try_update_rpc(yield_func_t &yield);

  bool try_read_rdma(int index, yield_func_t &yield);
  int try_lock_read_rdma(int index, yield_func_t &yield);


  void release_reads(yield_func_t &yield);
  void release_writes(yield_func_t &yield, bool all = true);
  bool try_update_rdma(yield_func_t &yield);

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

// HARD CODED!!!
    if(tableid == 7) {
      read_set_[index].data_ptr = (char*)malloc(len);
      return index;
    }

#if ONE_SIDED_READ
    if(!try_read_rdma(index, yield)) {
      release_reads(yield);
      release_writes(yield);
      return -1;
    }
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
    int ret = try_lock_read_rdma(index, yield);
    if(ret == -1) {
      release_reads(yield);
      release_writes(yield, false);
      return -1;
    }
    else if(ret == -2) {
      release_reads(yield);
      release_writes(yield);
      return -1; 
    }
    else if(ret != 0) {
      assert(false);
    }

#else
    if(!try_lock_read_rpc(index, yield)) {
      // abort
      release_reads(yield);
      release_writes(yield, false);
      return -1;
    }
    // get the results
    if(pid != node_id_) {
      process_received_data(reply_buf_, write_set_.back(), true);
    }
#endif
    ASSERT(write_set_[index].data_ptr != NULL) << index;
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
        lock_req_ = new RDMACASLockReq(cid);
        unlock_req_ = new RDMAFAUnlockReq(cid, 0);
        write_req_ = new RDMAWriteReq(cid, 0);
        memset(abort_cnt, 0, sizeof(int) * 40);
        for(int i = 0; i < 100; ++i) {
          Rmempool[i] = (char*)Rmalloc(2048);
          memptr = 0;
        }
        // init_time = (rwlock::get_now_nano() << 10);
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
    return -1;
  }

	virtual void begin(yield_func_t &yield) {
    memptr = 0;
    read_set_.clear();
    write_set_.clear();
    abort_reason = -1;
    // txn_start_time = (rwlock::get_now_nano() << 10) 
    // + response_node_ * 80 + worker_id_ * 10 + cor_id_ + 1
    // - init_time; // TODO: may be too large

    txn_start_time = ((++cnt_timer) << 10) 
    + response_node_ * 80 + worker_id_ * 10 + cor_id_ + 1;

    // LOG(3) << worker_id_ << ' ' << cor_id_ << ' ' << txn_start_time;

    // LOG(3) << "@" << txn_start_time;
    // the txn_end_time is approximated using the LEASE_TIME
    // txn_end_time = txn_start_time + rwlock::LEASE_TIME;
  }

  virtual bool commit(yield_func_t &yield) {
#if ONE_SIDED_READ
    try_update_rdma(yield);
#else
    try_update_rpc(yield);
#endif
  	return true;
  }
  
  template <typename V>
  inline __attribute__((always_inline))
  V *get_writeset(int idx,yield_func_t &yield) {
    return (V*)load_write(idx, sizeof(V), yield);
    // return get_set_helper<V>(write_set_, idx, yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_readset(int idx,yield_func_t &yield) {
    return (V*)load_read(idx, sizeof(V), yield);
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
  uint64_t cnt_timer = 0;
  uint64_t init_time = 0;
  uint64_t txn_end_time = 0;

  RDMACASLockReq* lock_req_ = NULL;
  RDMAFAUnlockReq* unlock_req_ = NULL;
  RDMAWriteReq* write_req_ = NULL;


  char* Rmempool[100];
  int memptr = 0;

public:  
  int abort_reason = -1;
  void show_abort() {
    for(int i = 0; i < 40; ++i) {
      LOG(3) << i << ": " << abort_cnt[i];
    }
  }
#include "occ_statistics.h"

  void register_default_rpc_handlers();
private:
  void lock_read_rpc_handler(int id,int cid,char *msg,void *arg);
  void read_rpc_handler(int id,int cid,char *msg,void *arg);
  void release_rpc_handler(int id,int cid,char *msg,void *arg);
  void update_rpc_handler(int id,int cid,char *msg,void *arg);
  
  inline __attribute__((always_inline))
  uint64_t check_write(MVCCHeader* header, uint64_t timestamp) {
    volatile uint64_t rts = header->rts;
    if(rts > timestamp) {
      LOG(3) << rts << " " << timestamp;
      return rts;
    }
    for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
      volatile uint64_t wts = header->wts[i];
      if(wts > timestamp){
        // LOG(3) << wts << " " << timestamp;
        return wts;
      }
    }
    return 0;
  }

  inline __attribute__((always_inline))
  int check_read(MVCCHeader* header, uint64_t timestamp) {
    // earlier write is processing
    if(header->lock != 0 && header->lock < timestamp) return -1;
    uint64_t max_wts = 0;
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

  void process_received_data(char* ptr, ReadSetItem& item, bool process_pos = false) {
    char* reply = ptr + 1;
    if(process_pos) {
      item.seq = *(uint64_t*)reply;
      ASSERT(item.seq < MVCC_VERSION_NUM) << " " << item.seq;
      reply += sizeof(uint64_t);
    }
    if(item.data_ptr == NULL)
      item.data_ptr = (char*)malloc(item.len);
    memcpy(item.data_ptr, reply, item.len);
  }

  // void unlock(ReadSetItem& item, yield_func_t &yield) {
  //   Qp* qp = get_qp(item.pid);
  //   unlock_req_->set_unlock_meta(item.off);
  //   unlock_req_->post_reqs(scheduler_, qp);
  //   if(unlikely(qp->rc_need_poll())) {
  //     worker_->indirect_yield(yield);
  //   }
  // }
// rpc handlers
};
}
}
