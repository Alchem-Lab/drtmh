#ifndef NOCC_DB_EPOCH_MANAGER
#define NOCC_DB_EPOCH_MANAGER

#include <functional>
#include <vector>
#include <stdint.h>
#include <queue>   // priority queue for sorting TS updates
#include <zmq.hpp>

#include "rocc_config.h"
#include "rdmaio.h"
#include "ralloc.h"
#include "all.h"

#include "core/rworker.h"
#include "core/commun_queue.hpp"
#include "framework/req_buf_allocator.h"
#include "framework/config.h"
#include "util/timer.h"

extern size_t nthreads;
extern int tcp_port;

namespace nocc {

using namespace oltp;
using namespace util;

extern zmq::context_t send_context;
extern std::vector<SingleQueue *>   local_comm_queues;

namespace db {

#define LARGE_VEC 0
#define ONE_CLOCK 1

#define TS_USE_MSG 1
#define RPC_EPOCH_GET 13

using namespace rdmaio;

class EpochManager : public RWorker {
 public:
  EpochManager(int worker_id,RdmaCtrl *cm,int master_id,uint64_t ts_addr)
      :RWorker(worker_id,cm),
       ts_size_(sizeof(uint64_t)),
       master_id_(master_id), cm_(cm),ts_addr_(ts_addr)
  {
    // allocte TS for TS fetch
    RThreadLocalInit();

    uint64_t *ts_buffer = (uint64_t *)((char *)(cm_->conn_buf_) + ts_addr_);
    fetched_ts_buffer_ = (char *)Rmalloc(ts_size_);
    assert(fetched_ts_buffer_ != NULL && ts_buffer != NULL);
    memset(ts_buffer,0,ts_size_);
    memset(fetched_ts_buffer_,0,ts_size_);
    running = true;
  }

  virtual void run() {

    fprintf(stdout,"[Global epoch manager running] !\n");
    init_routines(1); // only 1 routine is enough

#if USE_RDMA
    init_rdma();
    create_qps();
#endif

    MSGER_TYPE type = UD_MSG;
#if USE_TCP_MSG == 1
    create_tcp_connections(local_comm_queues[worker_id_],tcp_port,send_context);
#else
    create_rdma_ud_connections(1);
#endif

    rpc_->register_callback(std::bind(&EpochManager::epoch_get_handler,this,
                                      std::placeholders::_1,
                                      std::placeholders::_2,
                                      std::placeholders::_3,
                                      std::placeholders::_4),RPC_EPOCH_GET);

    
    this->inited = true;

  // starts the new_master_routine
    start_routine();
  }

  virtual void worker_routine(yield_func_t &yield) {
    uint64_t *ts_buffer = (uint64_t *)((char *)(cm_->conn_buf_) + ts_addr_);
    char *temp = (char *)Rmalloc(ts_size_);
    char *local_buffer = (char *)Rmalloc(ts_size_);
    memset(temp,0,ts_size_);
    memset(local_buffer,0,ts_size_);

#if USE_RDMA
    Qp *qp = cm_->get_rc_qp(worker_id_,master_id_);
#else
    Qp *qp  = NULL;
#endif
    // prepare the buffer for sending RPC
    char *req_buf = rpc_->get_static_buf(64);

    uint64_t last_time = 0;
    if (master_id_ == cm_->get_nodeid()) {
      last_time = get_now();
    }
    while(running) {
#if TS_USE_MSG
      // use message to fetch TS
      rpc_->prepare_multi_req(local_buffer,1,cor_id_);
      rpc_->append_req(req_buf,RPC_EPOCH_GET,sizeof(uint64_t),cor_id_,RRpc::REQ,master_id_);
#else
      // use one-sided READ for records
      qp->rc_post_send(IBV_WR_RDMA_READ,(char *)local_buffer,ts_size_,ts_addr_,IBV_SEND_SIGNALED,cor_id_);
      rdma_sched_->add_pending(cor_id_,qp);
#endif
      indirect_yield(yield);
      // got results back
      memcpy(temp,local_buffer,ts_size_);
      // swap the ptr
      char *swap = temp;
      temp = fetched_ts_buffer_;
      fetched_ts_buffer_ = swap;

      if (master_id_ == cm_->get_nodeid()) {
        uint64_t current_time = get_now();
        if (current_time - last_time > 10000) {
          update_lock_.lock();
          ts_buffer[0] += 1;
          update_lock_.unlock();
          last_time = current_time;
        }
      }
    }
    Rfree(local_buffer);
  } // a null worker routine,since no need for yield

  uint64_t get_current_epoch(char *buffer) {
    memcpy(buffer,fetched_ts_buffer_,ts_size_);
  }

  void print_ts(uint64_t *ts_buffer) {
    char *ts_ptr = (char *)ts_buffer;
    for(uint printed = 0; printed < ts_size_; printed += sizeof(uint64_t)) {
      fprintf(stdout,"%lu\t",*((uint64_t *)(ts_ptr + printed)));
    }
    fprintf(stdout,"\n");
  }

  void epoch_get_handler(int id,int cid,char *msg,void *temp) {
    uint64_t *ts_buffer = (uint64_t *)((char *)(cm_->conn_buf_) + ts_addr_);
    char *reply = rpc_->get_reply_buf();
    memcpy(reply,ts_buffer,ts_size_);
    rpc_->send_reply(reply,ts_size_,id,worker_id_,cid);
  }

  // members
 public:
  const int ts_size_;
  const int master_id_;
  const uint64_t ts_addr_;
 private:
  RdmaCtrl *cm_;
  char *fetched_ts_buffer_;
  std::mutex update_lock_;
}; // end class

};
};

#endif
