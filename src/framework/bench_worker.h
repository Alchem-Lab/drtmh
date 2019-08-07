#pragma once

#include "tx_config.h"
#include "core/rworker.h"
#include "core/commun_queue.hpp"
#include "core/utils/spinbarrier.h"
#include "core/utils/count_vector.hpp"

#include "util/util.h"

#include "db/txs/tx_handler.h"

#include "rtx/logger.hpp"

// #define MAX_CALVIN_REQ_CNTS (200*1000)
// #define MAX_CALVIN_REQ_CNTS (2000)
#define MAX_CALVIN_REQ_CNTS 1
#define MAX_CALVIN_SETS_SUPPRTED_IN_BITS (5)
#define MAX_CALVIN_SETS_SUPPORTED (1U<<(MAX_CALVIN_SETS_SUPPRTED_IN_BITS))  // 32 SETS
#define CALVIN_REQ_INFO_SIZE 256

#ifdef OCC_TX
#include "rtx/occ.h"

#elif defined(NOWAIT_TX)
#include "rtx/nowait_rdma.h"

#elif defined(WAITDIE_TX)
#include "rtx/waitdie_rdma.h"

#elif defined(SUNDIAL_TX)
#include "rtx/sundial_rdma.h"
#elif defined(MVCC_TX)
#include "rtx/mvcc_rdma.h"
#if ONE_SIDED_READ == 0
// TODO: this includes incurs dependecy on the rtx folder,
//       which should be refactored later after.
#include "rtx/global_vars.h"
#endif

#elif defined(CALVIN_TX)
#include "rtx/calvin_rdma.h"
#define OFFSETOF(TYPE, ELEMENT) ((size_t)&(((TYPE *)0)->ELEMENT)) 
#endif

#include <queue>          // std::queue

using namespace nocc::util;
using namespace nocc::db;

#define CS 0
#define LOCAL_CLIENT 0

extern size_t current_partition;
extern size_t total_partition;
extern size_t nthreads;
extern size_t coroutine_num;

namespace nocc {

extern __thread coroutine_func_t *routines_;
extern __thread TXHandler   **txs_;
#ifdef OCC_TX
extern __thread rtx::OCC      **new_txs_;
#elif defined(NOWAIT_TX)
extern __thread rtx::NOWAIT   **new_txs_;
#elif defined(WAITDIE_TX)
extern __thread rtx::WAITDIE  **new_txs_;
#elif defined(SUNDIAL_TX)
extern __thread rtx::SUNDIAL  **new_txs_;
#elif defined(MVCC_TX)
extern __thread rtx::MVCC  **new_txs_;
#elif defined(CALVIN_TX)
extern __thread rtx::CALVIN  **new_txs_;


typedef uint64_t timestamp_t;
struct calvin_request {
  calvin_request(int req_idx, int req_initiator, timestamp_t timestamp) : 
          req_idx(req_idx), req_initiator(req_initiator), timestamp(timestamp) {}

  calvin_request(calvin_request* copy) :
          req_idx(copy->req_idx), req_initiator(copy->req_initiator), timestamp(copy->timestamp) {
          memcpy(req_info, copy->req_info, CALVIN_REQ_INFO_SIZE);
  }

  union {
    int req_idx;
    int req_seq;
  };

  int req_initiator;
  
  timestamp_t timestamp;

  char req_info[CALVIN_REQ_INFO_SIZE];
};

struct calvin_header {
  uint8_t node_id;
  volatile uint8_t epoch_status;
  uint64_t epoch_id;
  volatile uint64_t batch_size; // the batch size
  // union {
  uint64_t chunk_size; // the number of calvin_requests in this rpc call
  volatile uint64_t received_size;
  // };
};

class calvin_request_compare {
public:
  bool operator()(const calvin_request* lhs, 
                  const calvin_request* rhs) {
    return lhs->timestamp < rhs->timestamp;
  }
};

#define CALVIN_EPOCH_READY 53
#define CALVIN_EPOCH_DONE  59
#endif

#define MAX_VAL_LENGTH 128
struct read_val_t {
  uint32_t req_seq;
  int read_or_write;
  int index_in_set;
  uint32_t len;
  char value[MAX_VAL_LENGTH];
  read_val_t(int req_seq, int rw, int index, uint32_t len, char* val) :
    req_seq(req_seq),
    read_or_write(rw),
    index_in_set(index),
    len(len) {
      assert(len < MAX_VAL_LENGTH);
      memcpy(value, val, len);
    }
  read_val_t(const read_val_t& copy) {
    req_seq = copy.req_seq;
    read_or_write = copy.read_or_write;
    index_in_set = copy.index_in_set;
    len = copy.len;
    memcpy(value, copy.value, len);
  }
  read_val_t() {}
};

struct read_compact_val_t {
  uint32_t len;
  char value[MAX_VAL_LENGTH];
};

extern     RdmaCtrl *cm;

extern __thread int *pending_counts_;

#define RPC_REQ 0
#define RPC_CALVIN_SCHEDULE 31
#define RPC_CALVIN_EPOCH_STATUS 30

namespace oltp {

class BenchWorker;
class BenchRunner;
extern View* my_view; // replication setting of the data

/* Txn result return type */
typedef std::pair<bool, double> txn_result_t;

/* Registerered Txn execution function */
#ifdef CALVIN_TX
typedef txn_result_t (*calvin_txn_fn_t)(BenchWorker *, calvin_request *, yield_func_t &yield);
#else
typedef txn_result_t (*txn_fn_t)(BenchWorker *,yield_func_t &yield);
#endif

typedef void (*txn_gensets_fn_t)(BenchWorker *, char*, yield_func_t &yield);
struct workload_desc {
  workload_desc() {}
#ifdef CALVIN_TX
  workload_desc(const std::string &name, double frequency, calvin_txn_fn_t fn, txn_gensets_fn_t gen_sets_fn = NULL)
      : name(name), frequency(frequency), fn(fn), gen_sets_fn(gen_sets_fn)
#else
  workload_desc(const std::string &name, double frequency, txn_fn_t fn)
      : name(name), frequency(frequency), fn(fn)
#endif
  {
    ALWAYS_ASSERT(frequency >= 0.0);
    ALWAYS_ASSERT(frequency <= 1.0);
  }
  std::string name;
  double frequency;

#ifdef CALVIN_TX
  calvin_txn_fn_t fn;
  txn_gensets_fn_t gen_sets_fn;
#else
  txn_fn_t fn;  
#endif

  util::BreakdownTimer latency_timer; // calculate the latency for each TX
  Profile p; // per tx profile
};

typedef std::vector<workload_desc> workload_desc_vec_t;

extern __thread std::vector<size_t> *txn_counts;
extern __thread std::vector<size_t> *txn_aborts;
extern __thread std::vector<size_t> *txn_remote_counts;

extern __thread workload_desc_vec_t *workloads;
extern __thread bool init;

/* Main benchmark worker */
class BenchWorker : public RWorker {
 public:

  // used for CS request
  struct REQ {
    int tx_id;
    int c_id;
    int c_tid;
    int cor_id;
    REQ(int t,int cid,int ctid,int cor) : tx_id(t),c_id(cid),c_tid(ctid),cor_id(cor) {}
    REQ() { }
  };

  int server_routine = 10;
  uint64_t total_ops_;

  /* methods */
  BenchWorker(unsigned worker_id,bool set_core,unsigned seed,uint64_t total_ops,
              spin_barrier *barrier_a,spin_barrier *barrier_b,BenchRunner *context = NULL,
              DBLogger *db_logger = NULL);

  void init_tx_ctx();

  virtual void events_handler();
  virtual void exit_handler();
  virtual void run(); // run -> call worker routine
  virtual void worker_routine(yield_func_t &yield);
  void req_rpc_handler(int id,int cid,char *msg,void *arg);

#ifdef CALVIN_TX
  void init_calvin();
  void worker_routine_for_calvin(yield_func_t &yield);
  void calvin_schedule_rpc_handler(int id,int cid,char *msg,void *arg);
  void check_schedule_done(int cid);
  void calvin_epoch_status_rpc_handler(int id, int cid, char *msg, void *arg);
  bool check_epoch_done();
#endif

  void change_ctx(int cor_id) {
    tx_ = txs_[cor_id];
    rtx_ = new_txs_[cor_id];
  }

  // simple wrapper to the underlying routine layer
  inline void context_transfer() {
    int next = routine_meta_->next_->id_;
    tx_      = txs_[next];
    cor_id_  = next;
    auto cur = routine_meta_;
    routine_meta_ = cur->next_;
  }

  void init_new_logger(MemDB **backup_stores) {

    assert(new_logger_ == NULL);
    uint64_t M2 = HUGE_PAGE_SZ;

#if TX_LOG_STYLE == 1 // RPC's case
    if(worker_id_ == 0)
      LOG(3) << "Use RPC for logging.";
    new_logger_ = new rtx::RpcLogger(rpc_,RTX_LOG_RPC_ID,RTX_LOG_CLEAN_ID,M2,
                                     MAX_BACKUP_NUM,(char *)(cm_->conn_buf_) + HUGE_PAGE_SZ,
                                     total_partition,nthreads,(coroutine_num + 1) * RTX_LOG_ENTRY_SIZE);
#elif TX_LOG_STYLE == 2 // one-sided RDMA case
    if(worker_id_ == 0)
      LOG(3) << "Use RDMA for logging.";
    new_logger_ = new rtx::RDMALogger(cm_,rdma_sched_,current_partition,worker_id_,M2,
                                      rpc_,RTX_LOG_CLEAN_ID,
                                      MAX_BACKUP_NUM,(char *)(cm_->conn_buf_) + HUGE_PAGE_SZ,
                                      total_partition,nthreads,(coroutine_num) * RTX_LOG_ENTRY_SIZE);
#endif

    // add backup stores)
    if(new_logger_) {
      std::set<int> backups;
      int num_backups = rtx::global_view->response_for(current_partition,backups);
      ASSERT(num_backups <= MAX_BACKUP_NUM)
          << "I'm backed for " << num_backups << "s!"
          << "Max " << MAX_BACKUP_NUM << " supported.";

      int i(0);
      for(auto it = backups.begin();it != backups.end();++it) {
        int backed_id = *it;
        assert(backed_id != current_partition);
        new_logger_->add_backup_store(backed_id,backup_stores[i++]);
      }
    }
  }

  virtual ~BenchWorker() { }
  virtual workload_desc_vec_t get_workload() const = 0;
  virtual void register_callbacks() = 0;     /*register read-only callback*/
  virtual void check_consistency() {};
  virtual void thread_local_init() {};
  virtual void workload_report() { };
  virtual void exit_report() { };

  /* we shall init msg_handler first, then init rpc_handler with msg_handler at the
     start of run time.
  */
  DBLogger *db_logger_;
  rtx::Logger *new_logger_;
  TXHandler *tx_;       /* current coroutine's tx handler */

#ifdef OCC_TX
  rtx::OCC *rtx_;
  rtx::OCC *rtx_hook_ = NULL;
#elif defined(NOWAIT_TX)
  rtx::NOWAIT  *rtx_;
  rtx::NOWAIT  *rtx_hook_ = NULL;
#elif defined(WAITDIE_TX)
  rtx::WAITDIE *rtx_;
  rtx::WAITDIE *rtx_hook_ = NULL;
#elif defined(SUNDIAL_TX)
  rtx::SUNDIAL *rtx_;
  rtx::SUNDIAL *rtx_hook_ = NULL;
#elif defined(MVCC_TX)
  rtx::MVCC *rtx_;
  rtx::MVCC *rtx_hook_ = NULL;
#elif defined(CALVIN_TX)
  rtx::CALVIN *rtx_;
  rtx::CALVIN *rtx_hook_ = NULL;
  std::vector<calvin_request*>* deterministic_requests;
  bool* epoch_done_schedule;
  std::set<int>* mach_received;

  // the following data structures should be allocated using Rmalloc
  // when using one-sided ops.
#if ONE_SIDED_READ == 0
  uint8_t** epoch_status_; // per-routine status
  std::vector<char*>* req_buffers;     // per-routine buffers
  char** send_buffers;
#else
  std::vector<char*>* req_buffers;
  uint64_t** offsets_;
#endif // ONE_SIDED_READ
#endif
  
  //forwarded related structures are used by the CALVIN CLASS
  std::map<uint64_t, read_val_t>* forwarded_values;
  uint64_t* forward_offsets_;
  char** forward_addresses;

  LAT_VARS(yield);

  /* For statistics counts */
  size_t ntxn_commits_;
  size_t ntxn_aborts_;
  size_t ntxn_executed_;

  size_t ntxn_abort_ratio_;
  size_t ntxn_strict_counts_;
  size_t ntxn_remote_counts_;
  util::BreakdownTimer latency_timer_;
  CountVector<double> latencys_;

  // Use a lot more QPs to emulate a larger cluster, if necessary
#include "rtx/qp_selection_helper.h"

 private:
  bool initilized_;
  /* Does bind the core */
  bool set_core_id_;
  spin_barrier *barrier_a_;
  spin_barrier *barrier_b_;
  BenchRunner  *context_;

  std::queue<REQ> pending_reqs_;

  friend class GlobalLockManager;
};

class BenchLoader : public ndb_thread {
 public:
  BenchLoader(unsigned long seed) ;
  void run();
 protected:
  virtual void load() = 0;
  util::fast_random random_generator_;
  int partition_;
 private:
  unsigned int worker_id_;
};


// used in a asymmetric setting
class BenchClient : public RWorker {
 public:
  BenchClient(unsigned worker_id,unsigned seed);
  virtual void run();
  virtual void worker_routine(yield_func_t &yield);
  void worker_routine_local(yield_func_t &yield);

  virtual int get_workload(char *input,fast_random &rand) = 0;
  virtual void exit_handler();

  BreakdownTimer timer_;
};

}; // oltp
extern __thread oltp::BenchWorker *worker;  // expose the worker
};   // nocc
