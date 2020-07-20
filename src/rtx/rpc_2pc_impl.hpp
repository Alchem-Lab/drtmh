#pragma once

#include "core/rdma_sched.h"
#include "rdmaio.h"

namespace nocc {

namespace rtx {

using namespace rdmaio;
using namespace oltp;

class RpcTwoPhaseCommitter : public TwoPhaseCommitter {
 public:
  RpcTwoPhaseCommitter(RdmaCtrl *cm, RWorker *worker, RRpc *rpc,int prepare_rpc_id, int decision_rpc_id)
      :TwoPhaseCommitter(cm, worker),
      rpc_handler_(rpc),
      twoPC_prepare_rpc_id_(prepare_rpc_id), 
      twoPC_decision_rpc_id_(decision_rpc_id)
  {
    // register RPC if necessary
    rpc->register_callback(std::bind(&RpcTwoPhaseCommitter::prepare_handler,this,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3,
                                     std::placeholders::_4),
                           twoPC_prepare_rpc_id_,true);
    rpc->register_callback(std::bind(&RpcTwoPhaseCommitter::global_decision_handler,this,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3,
                                     std::placeholders::_4),
                           twoPC_decision_rpc_id_,true);
  }

  inline bool prepare(TXOpBase* tx, BatchOpCtrlBlock& clk, int cor_id, yield_func_t &yield) {
    tx->add_batch_entry_wo_mac<uint8_t>(clk, -1, TwoPhaseCommitMemManager::TWO_PHASE_PREPARE);
    int replies = tx->send_batch_rpc_op(clk, cor_id, twoPC_prepare_rpc_id_, false);
    tx->abort_cnt[18]++;
    worker_->indirect_yield(yield);

    // collect votes
    int cnt_vote_commit = 0, cnt_vote_abort = 0;
    for (int i = 0; i < replies; i++) {

      uint8_t* vote = tx->get_batch_res<uint8_t>(clk, i);
      if (*vote == TwoPhaseCommitMemManager::VOTE_ABORT) {
        cnt_vote_abort += 1;
      } else {
        ASSERT(*vote == TwoPhaseCommitMemManager::VOTE_COMMIT) << "vote should be either VOTE_COMMIT or VOTE_ABORT. vote = " << *vote;
        cnt_vote_commit += 1;
      }
    }

    // LOG(3) << cnt_vote_commit << " vote to commit & " << cnt_vote_abort << " vote to abort.";
    return cnt_vote_commit == replies;  // all participants vote to commit
  }

  inline void broadcast_global_decision(TXOpBase* tx, BatchOpCtrlBlock& clk, uint8_t commit_or_abort, int cor_id, yield_func_t& yield) {
    tx->add_batch_entry_wo_mac<uint8_t>(clk, -1, commit_or_abort);
    int replies __attribute__((unused)) = tx->send_batch_rpc_op(clk, cor_id,twoPC_decision_rpc_id_,false);
    tx->abort_cnt[18]++;
    worker_->indirect_yield(yield);
  }

private:

  void prepare_handler(int id,int cid,char *msg,void *arg) {
    int size = (uint64_t)arg;

    char* reply_msg = rpc_handler_->get_reply_buf();
    if (worker_->rand_generator_.next_uniform() < 1.0)
      *(uint8_t*)reply_msg = TwoPhaseCommitMemManager::VOTE_COMMIT;
    else
      *(uint8_t*)reply_msg = TwoPhaseCommitMemManager::VOTE_ABORT;
    rpc_handler_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
  }

  void global_decision_handler(int id,int cid,char *msg,void *arg) {
    int size = (uint64_t)arg;
    char* reply_msg = rpc_handler_->get_reply_buf();
    rpc_handler_->send_reply(reply_msg,0,id,cid); // a dummy reply
  }

 private:
  const int twoPC_prepare_rpc_id_;
  const int twoPC_decision_rpc_id_;
  RRpc *rpc_handler_ = NULL;
};


}  // namespace rtx
}  // namespace nocc
