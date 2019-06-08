#include "tx_config.h"

#include "tpcc_worker.h"
#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/db_farm.h"
#include "db/txs/dbsi.h"

#include "db/forkset.h"

#include <set>
#include <limits>
#include <boost/bind.hpp>

#include "rtx/occ_rdma.h"
#include "rtx/occ_variants.hpp"

extern __thread RemoteHelper *remote_helper;

#define MICRO_DIST_NUM 100

extern size_t nclients;
extern size_t current_partition;
extern size_t total_partition;

//#define RC

using namespace nocc::util;

namespace nocc {

extern RdmaCtrl *cm;

namespace oltp {

extern __thread util::fast_random   *random_generator;

namespace tpcc {

static uint64_t npayment_executed = 0;
extern unsigned g_txn_workload_mix[5];
extern int g_new_order_remote_item_pct;
extern int g_mico_dist_num;

#if ENABLE_TXN_API

txn_result_t TpccWorker::txn_payment_api(yield_func_t &yield) {
  assert(false); // not implemented yet
  return txn_result_t(true,10);
}

txn_result_t TpccWorker::txn_delivery_api(yield_func_t &yield) {
  assert(false); // not implemented yet
  return txn_result_t(true,10);
}

txn_result_t TpccWorker::txn_stock_level_api(yield_func_t &yield) {
  assert(false); // not implemented yet
  return txn_result_t(true,10);
}

txn_result_t TpccWorker::txn_super_stock_level_api(yield_func_t &yield) {
  assert(false); // not implemented yet
  return txn_result_t(true,10);
}

txn_result_t TpccWorker::txn_order_status_api(yield_func_t &yield) {
  assert(false); // not implemented yet
  return txn_result_t(true,10);
}


txn_result_t TpccWorker::txn_payment_naive_api(yield_func_t &yield) {
  assert(false); // not implemented yet
  return txn_result_t(true,10);
}

txn_result_t TpccWorker::txn_payment_naive1_api(yield_func_t &yield) {
  assert(false); // not implemented yet
  return txn_result_t(true,10);
}

#endif
/* End namespace tpcc */
}
/* End namespace nocc framework */
}
}
