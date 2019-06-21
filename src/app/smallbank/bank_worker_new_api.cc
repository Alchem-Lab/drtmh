// implementations of smallbank on revised codebase

#include "bank_worker.h"
#include "tx_config.h"

#include "core/logging.h"

namespace nocc {

namespace oltp {

extern __thread util::fast_random   *random_generator;

namespace bank {

#if ENABLE_TXN_API

txn_result_t BankWorker::txn_sp_new_api(yield_func_t &yield) {
  int index = -1;

  rtx_->begin(yield);

  uint64_t id0,id1;
  GetTwoAccount(random_generator[cor_id_],&id0,&id1);  
  // uint64_t id0 = 100, id1 = 101;
  float amount = 5.0;

  checking::value *c0, *c1;

  // first account
  int pid = AcctToPid(id0);
  index = rtx_->write(pid,CHECK,id0,sizeof(checking::value),yield);
  if (index < 0) return txn_result_t(false,73);
  // second account
  pid = AcctToPid(id1);
  index = rtx_->write(pid,CHECK,id1,sizeof(checking::value),yield);
  if (index < 0) return txn_result_t(false,73);

  //Actual loads of values
  c0 = (checking::value*)rtx_->load_write(0,sizeof(checking::value),yield);
  c1 = (checking::value*)rtx_->load_write(1,sizeof(checking::value),yield);

  // transactional logic
  if(c0->c_balance < amount) {
  } else {
    c0->c_balance -= amount;
    c1->c_balance += amount;
  }

  // transaction commit
  auto ret = rtx_->commit(yield);
  return txn_result_t(ret,73);
}

txn_result_t BankWorker::txn_wc_new_api(yield_func_t &yield) {
  int index = -1;

  rtx_->begin(yield);

  float amount = 5.0; //from original code

  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);

  index = rtx_->read(pid,SAV,id,sizeof(savings::value),yield);
  if (index < 0) return txn_result_t(false,73);
  index = rtx_->write(pid,CHECK,id,sizeof(checking::value),yield);
  if (index < 0) return txn_result_t(false,73);
  savings::value  *sv = (savings::value*)rtx_->load_read(0, sizeof(savings::value), yield);
  checking::value *cv = (checking::value*)rtx_->load_write(0, sizeof(checking::value), yield);

  auto total = sv->s_balance + cv->c_balance;
  if(total < amount) {
    cv->c_balance -= (amount - 1);
  } else {
    cv->c_balance -= amount;
  }

  auto   ret = rtx_->commit(yield);
  return txn_result_t(ret,1);
}

txn_result_t BankWorker::txn_dc_new_api(yield_func_t &yield) {
  int index = -1;

  rtx_->begin(yield);

  float amount = 1.3;
retry:
  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);

  index = rtx_->write(pid,CHECK,id,sizeof(checking::value),yield);
  if (index < 0) return txn_result_t(false,73);
  checking::value *cv = (checking::value*)rtx_->load_write(0,sizeof(checking::value),yield);

  // fetch cached record from read-set
  assert(cv != NULL);
  cv->c_balance += amount;

  bool ret = rtx_->commit(yield);

#if 0 // check the correctness
  {
    if(ret == true) {
      rtx_->begin(yield);
      rtx_->start_batch_read();
      rtx_->add_batch_read(CHECK,id,pid,sizeof(checking::value));
      rtx_->send_batch_read();
      checking::value *cv1 = rtx_->get_readset<checking::value>(0,yield);
      ASSERT(diff_check(cv,cv1)) << "exe return: " << cv->c_balance
                                 << "; check return: " << cv1->c_balance;
    }
  }
#endif

  return txn_result_t(ret,1);
}

txn_result_t BankWorker::txn_ts_new_api(yield_func_t &yield) {
  int index = -1;

  rtx_->begin(yield);

  float amount   = 20.20; //from original code
  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);
  index = rtx_->write(pid,SAV,id,sizeof(savings::value),yield);
  if (index < 0) return txn_result_t(false,73);
  auto sv = (savings::value*)rtx_->load_write(0, sizeof(savings::value), yield);

  sv->s_balance += amount;
  auto ret = rtx_->commit(yield);
  return txn_result_t(ret,73);
}

txn_result_t BankWorker::txn_balance_new_api(yield_func_t &yield) {
  int index = -1;

  rtx_->begin(yield);

  uint64_t id;
  GetAccount(random_generator[cor_id_],&(id));
  int pid = AcctToPid(id);

  double res = 0.0;
  index = rtx_->read(pid,CHECK,id,sizeof(checking::value),yield);
  if (index < 0) return txn_result_t(false,73);
  index = rtx_->read(pid,SAV,id,sizeof(savings::value),yield);
  if (index < 0) return txn_result_t(false,73);

  auto cv = (checking::value*)rtx_->load_read(0,sizeof(checking::value),yield);
  auto sv = (savings::value*)rtx_->load_read(1,sizeof(savings::value),yield);
  res = cv->c_balance + sv->s_balance;

  bool ret = rtx_->commit(yield);
  return txn_result_t(ret,(uint64_t)0);
}


txn_result_t BankWorker::txn_amal_new_api(yield_func_t &yield) {
  int index = -1;

  rtx_->begin(yield);

  uint64_t id0,id1;
  GetTwoAccount(random_generator[cor_id_],&id0,&id1);

  int pid0 = AcctToPid(id0);
  int pid1 = AcctToPid(id1),idx1;

  index = rtx_->write(pid0,SAV,id0,sizeof(savings::value),yield);
  if (index < 0) return txn_result_t(false,73);
  index = rtx_->write(pid0,CHECK,id0,sizeof(checking::value),yield);
  if (index < 0) return txn_result_t(false,73);
  index = rtx_->write(pid1,CHECK,id1,sizeof(checking::value),yield);
  if (index < 0) return txn_result_t(false,73);

  auto s0 = (savings::value*)rtx_->load_write(0,sizeof(savings::value),yield);
  auto c0 = (checking::value*)rtx_->load_write(1,sizeof(checking::value),yield);
  auto c1 = (checking::value*)rtx_->load_write(2,sizeof(checking::value),yield);

  double total = 0;

  total = s0->s_balance + c0->c_balance;

  s0->s_balance = 0;
  c0->c_balance = 0;

  c1->c_balance += total;

  auto ret = rtx_->commit(yield);
  return txn_result_t(ret,73); // since readlock success, so no need to abort
}

#endif

}; // namespace bank

}; // namespace oltp

};