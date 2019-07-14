#ifndef NOCC_OLTP_TPCC_WORKER_H
#define NOCC_OLTP_TPCC_WORKER_H

#include "all.h"
#include "tx_config.h"
#include "./app/config.h"

#include "tpcc_schema.h"
#include "tpcc_mixin.h"
#include "memstore/memdb.h"

#include "framework/backup_worker.h"
#include "framework/bench_worker.h"

#include "db/txs/tx_handler.h"

#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"

#include "core/utils/latency_profier.h"

#define MAX_DIST 10

#define TX_STOCK_LEVEL 0
#define STOCK_LEVEL_ORDER_COUNT 10 /* shall be 20 by default */

/* which concurrency control code to use */

/* System headers */
#include <string>

namespace nocc {
namespace oltp {

namespace tpcc {

// multi-version store can use dedicated meta length
#ifdef RAD_TX
#undef META_LENGTH
#define META_LENGTH RAD_META_LEN
#endif

#ifdef SI_TX
#undef META_LENGTH
#define META_LENGTH SI_META_LEN
#endif

/* Main function */
void TpccTest(int argc,char **argv);
int  GetStartWarehouse(int partition);
int  GetEndWarehouse(int partition);
int  NumWarehouses();
int  WarehouseToPartition(int wid);

struct StockLevelInput {
  int8_t num;
  int32_t threshold;
  uint64_t tx_id;
#ifdef SI_TX
  uint64_t ts_vec[12]; //FIXME!! hard coded
#else
  uint64_t timestamp;
#endif
};
struct StockLevelReply {
  int32_t num_items;
};
struct StockLevelInputPayload {
  uint16_t warehouse_id;
  uint8_t district_id;
  uint8_t pid;
};

typedef struct { uint64_t a;uint64_t b; } order_index_type_t;

/* Tx's implementation */
class TpccWorker : public TpccMixin, public BenchWorker {

 public:
  TpccWorker(unsigned int worker_id,unsigned long seed,uint warehouse_id_start,uint warehouse_id_end,
             MemDB *db,uint64_t total_ops,
             spin_barrier *a,spin_barrier *b,
             BenchRunner *context);
  ~TpccWorker();

#if ENABLE_TXN_API
#ifdef CALVIN_TX
#define DECLARE_GEN_RWSETS_FUNC(name) \
  void name##_gen_rwsets(char* buf, yield_func_t &yield);

  txn_result_t txn_new_order_new_api(calvin_request* req, yield_func_t &yield);
  DECLARE_GEN_RWSETS_FUNC(txn_new_order_new_api)
  txn_result_t txn_payment_api(calvin_request* req, yield_func_t &yield);
  DECLARE_GEN_RWSETS_FUNC(txn_payment_api)
  txn_result_t txn_delivery_api(calvin_request* req, yield_func_t &yield);
  DECLARE_GEN_RWSETS_FUNC(txn_delivery_api)
  txn_result_t txn_stock_level_api(calvin_request* req, yield_func_t &yield);
  DECLARE_GEN_RWSETS_FUNC(txn_stock_level_api)  
  txn_result_t txn_order_status_api(calvin_request* req, yield_func_t &yield);
  DECLARE_GEN_RWSETS_FUNC(txn_order_status_api)
  txn_result_t txn_super_stock_level_api(calvin_request* req, yield_func_t &yield);
  DECLARE_GEN_RWSETS_FUNC(txn_super_stock_level_api)
  txn_result_t txn_payment_naive_api(calvin_request* req, yield_func_t &yield);    // non speculative execution  
  DECLARE_GEN_RWSETS_FUNC(txn_payment_naive_api)
  txn_result_t txn_payment_naive1_api(calvin_request* req, yield_func_t &yield);   // + batching
  DECLARE_GEN_RWSETS_FUNC(txn_payment_naive1_api)
#else
  txn_result_t txn_new_order_new_api(yield_func_t &yield);
  txn_result_t txn_payment_api(yield_func_t &yield);
  txn_result_t txn_delivery_api(yield_func_t &yield);
  txn_result_t txn_stock_level_api(yield_func_t &yield);
  txn_result_t txn_order_status_api(yield_func_t &yield);
  txn_result_t txn_super_stock_level_api(yield_func_t &yield);
  txn_result_t txn_payment_naive_api(yield_func_t &yield);    // non speculative execution  
  txn_result_t txn_payment_naive1_api(yield_func_t &yield);   // + batching
#endif
#else
  txn_result_t txn_new_order(yield_func_t &yield);
  txn_result_t txn_new_order_new(yield_func_t &yield);


  txn_result_t txn_payment(yield_func_t &yield);


  txn_result_t txn_delivery(yield_func_t &yield);


  txn_result_t txn_stock_level(yield_func_t &yield);

  txn_result_t txn_order_status(yield_func_t &yield);

  txn_result_t txn_super_stock_level(yield_func_t &yield);

  txn_result_t txn_super_stocklevel_new(yield_func_t &yield);

  txn_result_t txn_micro(yield_func_t &yield);

  // naive version of 2 TXs
  txn_result_t txn_new_order_naive(yield_func_t &yield);  // non speculative execution
  txn_result_t txn_new_order_naive1(yield_func_t &yield); // + batching

  txn_result_t txn_payment_naive(yield_func_t &yield);    // non speculative execution
  txn_result_t txn_payment_naive1(yield_func_t &yield);   // + batching  
#endif

  /* The later are used for transactions handling ro fork/join modes */
  void stock_level_piece(yield_func_t &yield,int id,int cid,char* input);

 private:
  const uint warehouse_id_start_ ;
  const uint warehouse_id_end_ ;
  uint64_t last_no_o_ids_[1024][10];
  LAT_VARS(read);

 public:
  virtual workload_desc_vec_t get_workload() const ;
  static  workload_desc_vec_t _get_workload();
  virtual void check_consistency();
  virtual void thread_local_init();
  virtual void register_callbacks();

  virtual void workload_report() {
    BenchWorker::workload_report();
    double res;
    REPORT_V(read,res);
    latencys_.push_back(res);

    // record TX's data
    rtx_hook_->record();
  }

  void exit_report() {
    LOG(4) << "worker exit.";
    latencys_.erase(0.2);
    auto one_second = util::BreakdownTimer::get_one_second_cycle();
    LOG(4) << "read time: " << util::BreakdownTimer::rdtsc_to_ms(latencys_.average(),one_second) << "ms";

    rtx_hook_->report_statics(one_second);
  }

#ifdef CALVIN_TX

  /* Wrapper for implementation of transaction */
  static txn_result_t TxnNewOrder(BenchWorker *w, calvin_request *req, yield_func_t &yield) {
#if ENABLE_TXN_API
    //txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order(yield);
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order_new_api(req, yield);
#else
    assert(false);
#endif
    return r;
  }

  static void TxnNewOrderGenRWSets(BenchWorker *w, char* buf, yield_func_t &yield) {
#if ENABLE_TXN_API
    static_cast<TpccWorker *>(w)->txn_new_order_new_api_gen_rwsets(buf, yield);
#else
    assert(false);
#endif
  }

  static txn_result_t TxnPayment(BenchWorker *w, calvin_request *req, yield_func_t &yield) {
#if ENABLE_TXN_API
#if NAIVE == 1
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_naive_api(req, yield);
#elif NAIVE == 2 || NAIVE == 3
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_naive1_api(req, yield);
#else
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_api(req, yield);
#endif
#else
    assert(false);
#endif
    return r;
  }

  static void TxnPaymentGenRWSets(BenchWorker *w, char* buf, yield_func_t &yield) {
#if ENABLE_TXN_API
#if NAIVE == 1
    static_cast<TpccWorker *>(w)->txn_payment_naive_api_gen_rwsets(buf, yield);
#elif NAIVE == 2 || NAIVE == 3
    static_cast<TpccWorker *>(w)->txn_payment_naive1_api_gen_rwsets(buf, yield);
#else
    static_cast<TpccWorker *>(w)->txn_payment_api_gen_rwsets(buf, yield);
#endif
#else
    assert(false);
#endif
  }

  static txn_result_t TxnDelivery(BenchWorker *w, calvin_request *req, yield_func_t &yield) {
#if ENABLE_TXN_API
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_delivery_api(req, yield);    
#else
    assert(false);
#endif
    return r;
  }

  static void TxnDeliveryGenRWSets(BenchWorker *w, char* buf, yield_func_t &yield) {
#if ENABLE_TXN_API
    static_cast<TpccWorker *>(w)->txn_delivery_api_gen_rwsets(buf, yield);
#else
    assert(false);
#endif
  }

  static txn_result_t TxnStockLevel(BenchWorker *w, calvin_request *req, yield_func_t &yield) {
#if ENABLE_TXN_API
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_stock_level_api(req, yield);
    //txn_result_t r = static_cast<TpccWorker *>(w)->txn_super_stock_level(yield);    
#else
    assert(false);
#endif
    return r;
  }

  static void TxnStockLevelGenRWSets(BenchWorker *w, char* buf, yield_func_t &yield) {
#if ENABLE_TXN_API
    static_cast<TpccWorker *>(w)->txn_stock_level_api_gen_rwsets(buf, yield);
#else
    assert(false);
#endif
  }

  static txn_result_t TxnSuperStockLevel(BenchWorker *w, calvin_request *req, yield_func_t &yield) {
#if ENABLE_TXN_API
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_super_stock_level_api(req, yield);
#else
    assert(false); 
#endif
    return r;
  }

  static void TxnSuperStockLevelGenRWSets(BenchWorker *w, char* buf, yield_func_t &yield) {
#if ENABLE_TXN_API
    static_cast<TpccWorker *>(w)->txn_super_stock_level_api_gen_rwsets(buf, yield);
#else
    assert(false);
#endif
  }

  static txn_result_t TxnOrderStatus(BenchWorker *w, calvin_request *req, yield_func_t &yield) {
#if ENABLE_TXN_API
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_order_status_api(req, yield);
#else
    assert(false);
#endif
    return r;
  }

  static void TxnOrderStatusGenRWSets(BenchWorker *w, char* buf, yield_func_t &yield) {
#if ENABLE_TXN_API
    static_cast<TpccWorker *>(w)->txn_order_status_api_gen_rwsets(buf, yield);
#else
    assert(false);
#endif
  }

#else

  /* Wrapper for implementation of transaction */
  static txn_result_t TxnNewOrder(BenchWorker *w,yield_func_t &yield) {
#if ENABLE_TXN_API
    //txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order(yield);
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order_new_api(yield);
#else
    //txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order(yield);
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order_new(yield);
#endif
    return r;
  }

  static txn_result_t TxnPayment(BenchWorker *w,yield_func_t &yield) {
#if ENABLE_TXN_API
#if NAIVE == 1
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_naive_api(yield);
#elif NAIVE == 2 || NAIVE == 3
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_naive1_api(yield);
#else
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_api(yield);
#endif
#else
#if NAIVE == 1
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_naive(yield);
#elif NAIVE == 2 || NAIVE == 3
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_naive1(yield);
#else
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment(yield);
#endif
#endif
    return r;
  }

  static txn_result_t TxnDelivery(BenchWorker *w,yield_func_t &yield) {
#if ENABLE_TXN_API
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_delivery_api(yield);    
#else
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_delivery(yield);
#endif
    return r;
  }

  static txn_result_t TxnStockLevel(BenchWorker *w,yield_func_t &yield) {
#if ENABLE_TXN_API
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_stock_level_api(yield);
    //txn_result_t r = static_cast<TpccWorker *>(w)->txn_super_stock_level(yield);    
#else
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_stock_level(yield);
    //txn_result_t r = static_cast<TpccWorker *>(w)->txn_super_stock_level(yield);
#endif
    return r;
  }

  static txn_result_t TxnSuperStockLevel(BenchWorker *w,yield_func_t &yield) {
#if ENABLE_TXN_API
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_super_stock_level_api(yield);
#else
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_super_stock_level(yield);    
#endif
    return r;
  }

  static txn_result_t TxnOrderStatus(BenchWorker *w,yield_func_t &yield) {
#if ENABLE_TXN_API
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_order_status_api(yield);
#else
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_order_status(yield);
#endif
    return r;
  }

#endif // CALVIN_TX
};

/* Loaders */
class TpccWarehouseLoader : public BenchLoader, public TpccMixin {
 public:
  TpccWarehouseLoader(unsigned long seed, int partition,MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccDistrictLoader : public BenchLoader, public TpccMixin {
 public:
  TpccDistrictLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccCustomerLoader : public BenchLoader, public TpccMixin {
 public:
  TpccCustomerLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccOrderLoader : public BenchLoader, public TpccMixin {
 public:
  TpccOrderLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccItemLoader : public BenchLoader, public TpccMixin {
 public:
  TpccItemLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccStockLoader : public BenchLoader, public TpccMixin {
 public:
  TpccStockLoader(unsigned long seed, int partition, MemDB *store,bool backup = false)
      : BenchLoader(seed),
        TpccMixin(store),backup_(backup) {

    partition_ = partition;
  }
  const bool backup_;
 protected:
  virtual void load();
};

}
}
}
#endif
