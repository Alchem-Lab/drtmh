#pragma once

#include "core/rdma_sched.h"
#include "rdmaio.h"
#include "two_phase_commit_mem_manager.hpp"

namespace nocc {
namespace rtx {

// namespace oltp {
//   extern char *rdma_buffer;      // start point of the local RDMA registered buffer;
// };

using namespace rdmaio;
using namespace oltp;

class RDMATwoPhaseCommitter : public TwoPhaseCommitter {
 public:
  RDMATwoPhaseCommitter(RdmaCtrl *cm, RWorker* worker, RScheduler* rdma_sched,int nid,int tid,uint64_t base_off,
                        char *local_p,int ms,int ts,int cs)
      :TwoPhaseCommitter(cm, worker),
       twopc_mem_(local_p,ms,ts,cs,base_off),
       scheduler_(rdma_sched),
       node_id_(nid),worker_id_(tid),local_base_ptr(local_p)
  {
    // init local QP vector
    fill_qp_vec(cm,worker_id_);
  }

  inline bool prepare(TXOpBase* tx, BatchOpCtrlBlock &clk, int cor_id, yield_func_t &yield) {
    memset(clk.req_buf_, TwoPhaseCommitMemManager::TWO_PHASE_PREPARE, sizeof(uint8_t));
    clk.req_buf_end_ = clk.req_buf_ + sizeof(uint8_t);

    int size = sizeof(uint8_t);
    assert(clk.mac_set_.size() > 0);
    // post the prepare message to remote participants
    for(auto it = clk.mac_set_.begin();it != clk.mac_set_.end();++it) {
      int  mac_id = *it;
        //auto qp = qp_vec_[mac_id];
        auto qp = get_qp(mac_id);
        auto off = twopc_mem_.get_remote_offset(node_id_,worker_id_,cor_id);
        // auto off = 4096 * (worker_id_ + 1);
        // LOG(3) << "off " << off << " prepare written.";
        assert(off != 0);
        scheduler_->post_send(qp,cor_id,
                              IBV_WR_RDMA_WRITE,clk.req_buf_,size,off,
                              IBV_SEND_SIGNALED | ((size < 64)?IBV_SEND_INLINE:0));
    }
    tx->abort_cnt[18]++;
    worker_->indirect_yield(yield);

    // LOG(3) << "COLLECTING VOTES";
    //fetch the vote message from remote participants
    int n_vote_commit = 0, n_vote_abort = 0;

    while (true) {
      n_vote_commit = 0;
      n_vote_abort = 0;
      for (auto it = clk.mac_set_.begin(); it != clk.mac_set_.end(); ++it) {
        int mac_id = *it;
          auto qp = get_qp(mac_id);
          auto off = twopc_mem_.get_remote_offset(node_id_,worker_id_,cor_id);

          // auto off = 4096 * (worker_id_ + 1);
          assert(off != 0);
          // char *local_buf = local_base_ptr + worker_id_ * 4096 + cor_id * CACHE_LINE_SZ;
          scheduler_->post_send(qp,cor_id,
                                IBV_WR_RDMA_READ, 
                                // local_buf, 
                                clk.req_buf_, 
                                size,
                                off,
                                // IBV_SEND_SIGNALED | ((size < 64)?IBV_SEND_INLINE:0)
                                IBV_SEND_SIGNALED
                                );
          tx->abort_cnt[18]++;
          worker_->indirect_yield(yield);


          uint8_t vote = *(uint8_t*)clk.req_buf_;
          ASSERT(vote == TwoPhaseCommitMemManager::TWO_PHASE_PREPARE ||
                 vote == TwoPhaseCommitMemManager::VOTE_COMMIT ||
                 vote == TwoPhaseCommitMemManager::VOTE_ABORT);
          if (vote == TwoPhaseCommitMemManager::VOTE_COMMIT) {
            // LOG(3) << "collected vote commit from " << mac_id;
            n_vote_commit += 1;
          }
          if (vote == TwoPhaseCommitMemManager::VOTE_ABORT) {
            // LOG(3) << "collected vote abort from " << mac_id;
            n_vote_abort += 1;
          }
      }

      if (n_vote_commit + n_vote_abort == clk.mac_set_.size())
        break;
      worker_->yield_next(yield);
    }

    return n_vote_commit == clk.mac_set_.size();
  }


  inline void broadcast_global_decision(TXOpBase* tx, BatchOpCtrlBlock& clk, uint8_t commit_or_abort, int cor_id, yield_func_t& yield) {
    memset(clk.req_buf_, commit_or_abort, sizeof(uint8_t));
    clk.req_buf_end_ = clk.req_buf_ + sizeof(uint8_t);

    int size = clk.batch_msg_size(); // log size
    assert(clk.mac_set_.size() > 0);
    // post the prepare message to remote participants
    for(auto it = clk.mac_set_.begin();it != clk.mac_set_.end();++it) {
      int  mac_id = *it;
      auto qp = get_qp(mac_id);
      auto off = twopc_mem_.get_remote_offset(node_id_,worker_id_,cor_id);
      assert(off != 0);
      scheduler_->post_send(qp,cor_id,
                            IBV_WR_RDMA_WRITE,clk.req_buf_,size,off,
                            IBV_SEND_SIGNALED | ((size < 64)?IBV_SEND_INLINE:0));
    }
    tx->abort_cnt[18]++;
    worker_->indirect_yield(yield);
  }

 private:

  RScheduler *scheduler_;
  int node_id_;
  int worker_id_;
  char* local_base_ptr;
  TwoPhaseCommitMemManager twopc_mem_;

#include "qp_selection_helper.h"
};

}; // namespace rtx
}; // namespace nocc
