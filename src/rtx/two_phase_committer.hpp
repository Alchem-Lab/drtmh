#pragma once

#include "core/rrpc.h"
#include "tx_config.h"
#include "tx_operator.hpp"

namespace nocc {

namespace rtx {



// Abstract logger class
class TwoPhaseCommitter {
 public:
  /*
   * rpc: rpc_handler
   */
  TwoPhaseCommitter(RdmaCtrl *cm, RWorker *worker) : cm_(cm), worker_(worker) {}

  ~TwoPhaseCommitter() {}

  // broadcasting prepare messages to participants defined in mac_set
  virtual bool prepare(TXOpBase* tx, BatchOpCtrlBlock &ctrl,int cor_id, yield_func_t &yield) = 0;

  virtual void broadcast_global_decision(TXOpBase* tx, BatchOpCtrlBlock& clk, uint8_t commit_or_abort, int cor_id, yield_func_t& yield) = 0;

 protected:
  RdmaCtrl *cm_;
  RWorker *worker_;
  DISABLE_COPY_AND_ASSIGN(TwoPhaseCommitter);
}; // TwoPhaseCommitter

}; // namespace rtx

};   // namespace nocc

// RCC provides two two-phase-commit implementations
#include "rdma_2pc_impl.hpp"
#include "rpc_2pc_impl.hpp"
