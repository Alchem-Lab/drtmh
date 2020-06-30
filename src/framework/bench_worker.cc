#include "rocc_config.h"
#include "tx_config.h"

#include "config.h"

#include "bench_worker.h"

#include "req_buf_allocator.h"

#include "db/txs/dbrad.h"
#include "db/txs/dbsi.h"

#ifdef CALVIN_TX
#include "db/txs/epoch_manager.hpp"
#include "framework/sequencer.h"
#include "framework/scheduler.h"
#endif

#include <queue>
#include <boost/lockfree/queue.hpp>

extern size_t coroutine_num;
extern size_t current_partition;
extern size_t nclients;
extern size_t nthreads;
extern int tcp_port;

namespace nocc {

__thread oltp::BenchWorker *worker = NULL;
__thread TXHandler   **txs_ = NULL;
#ifdef OCC_TX
__thread rtx::OCC      **new_txs_ = NULL;
#elif defined(NOWAIT_TX)
__thread rtx::NOWAIT   **new_txs_ = NULL;
#elif defined(WAITDIE_TX)
__thread rtx::WAITDIE  **new_txs_ = NULL;
#elif defined(SUNDIAL_TX)
__thread rtx::SUNDIAL  **new_txs_ = NULL;
#elif defined(MVCC_TX)
__thread rtx::MVCC  **new_txs_ = NULL;
#elif defined(CALVIN_TX)
__thread rtx::CALVIN  **new_txs_ = NULL;
// __thread std::vector<SingleQueue*> calvin_ready_requests;
db::EpochManager* epoch_manager = NULL; 
oltp::Sequencer* sequencer = NULL;
oltp::Scheduler* scheduler = NULL;
extern uint64_t per_thread_calvin_request_buffer_sz;
extern uint64_t per_thread_calvin_forward_buffer_sz;
namespace oltp {
// extern char* calvin_request_buffer;
extern char* calvin_forward_buffer;
}
#elif defined(BOHM_TX)
oltp::BOHMCCThread** bohmcc = NULL;
oltp::Sequencer* sequencer = NULL;
oltp::Scheduler* scheduler = NULL;
#endif

extern uint64_t total_ring_sz;
extern uint64_t ring_padding;


std::vector<CommQueue *> conns; // connections between clients and servers
extern std::vector<SingleQueue *>   local_comm_queues;
extern zmq::context_t send_context;

namespace oltp {

// used to calculate benchmark information ////////////////////////////////
__thread std::vector<size_t> *txn_counts = NULL;
__thread std::vector<size_t> *txn_aborts = NULL;
__thread std::vector<size_t> *txn_remote_counts = NULL;


// used to calculate the latency of each workloads
__thread workload_desc_vec_t *workloads;

extern char *rdma_buffer;

extern __thread util::fast_random *random_generator;

// per-thread memory allocator
__thread RPCMemAllocator *msg_buf_alloctors = NULL;
extern MemDB *backup_stores_[MAX_BACKUP_NUM];

SpinLock exit_lock;

BenchWorker::BenchWorker(unsigned worker_id,bool set_core,unsigned seed,uint64_t total_ops,
                         spin_barrier *barrier_a,spin_barrier *barrier_b,BenchRunner *context,
                         DBLogger *db_logger):
    RWorker(worker_id,cm,seed),
    initilized_(false),
    set_core_id_(set_core),
    ntxn_commits_(0),
    ntxn_aborts_(0),
    ntxn_executed_(0),
    ntxn_abort_ratio_(0),
    ntxn_remote_counts_(0),
    ntxn_strict_counts_(0),
    total_ops_(total_ops),
    context_(context),
    db_logger_(db_logger),
    // r-set some local members
    new_logger_(NULL)
{
  assert(cm_ != NULL);
  INIT_LAT_VARS(yield);
#if CS == 0
  nclients = 0;
  server_routine = coroutine_num;
#else
  if(nclients >= nthreads) server_routine = coroutine_num;
  else server_routine = MAX(coroutine_num,server_routine);
  //server_routine = MAX(40,server_routine);
#endif
  //server_routine = 20;
}

void BenchWorker::init_tx_ctx() {
  worker = this;
  txs_              = new TXHandler*[1 + server_routine + 2];
#ifdef OCC_TX
  new_txs_          = new rtx::OCC*[1 + server_routine + 2];
  std::fill_n(new_txs_,1 + server_routine + 2,static_cast<rtx::OCC*>(NULL));
#elif defined(NOWAIT_TX)
  new_txs_          = new rtx::NOWAIT*[1 + server_routine + 2];
  std::fill_n(new_txs_,1 + server_routine + 2,static_cast<rtx::NOWAIT*>(NULL));
#elif defined(WAITDIE_TX)
  new_txs_          = new rtx::WAITDIE*[1 + server_routine + 2];
  std::fill_n(new_txs_,1 + server_routine + 2,static_cast<rtx::WAITDIE*>(NULL));
#elif defined(SUNDIAL_TX)
  new_txs_          = new rtx::SUNDIAL*[1 + server_routine + 2];
  std::fill_n(new_txs_,1 + server_routine + 2,static_cast<rtx::SUNDIAL*>(NULL));
#elif defined(MVCC_TX)
  new_txs_          = new rtx::MVCC*[1 + server_routine + 2];
  std::fill_n(new_txs_,1 + server_routine + 2,static_cast<rtx::MVCC*>(NULL));
#elif defined(CALVIN_TX)
  new_txs_          = new rtx::CALVIN*[1 + server_routine + 2];
  std::fill_n(new_txs_,1 + server_routine + 2,static_cast<rtx::CALVIN*>(NULL));
  // calvin_ready_requests.reserve(1 + server_routine + 2);
  // std::fill_n(calvin_ready_requests.begin(),1 + server_routine + 2,static_cast<SingleQueue*>(NULL));
#else
  assert(false);
#endif

  //msg_buf_alloctors = new RPCMemAllocator[1 + server_routine];

  txn_counts = new std::vector<size_t> ();
  txn_aborts = new std::vector<size_t> ();
  txn_remote_counts = new std::vector<size_t> ();

  for(uint i = 0;i < NOCC_BENCH_MAX_TX;++i) {
    txn_counts->push_back(0);
    txn_aborts->push_back(0);
    txn_remote_counts->push_back(0);
  }
  for(uint i = 0;i < 1 + server_routine + 2;++i)
    txs_[i] = NULL;

  // init workloads
  workloads = new workload_desc_vec_t[server_routine + 2];
}


#ifdef CALVIN_TX
void BenchWorker::init_calvin() {
  // batch_size_for_current_epoch = new std::map<int, uint64_t>[1 + server_routine];
  // epoch_done_schedule = new bool[1 + server_routine];
  // memset(epoch_done_schedule, 0, sizeof(bool)*(1 + server_routine));
  // mach_received = new std::set<int>[1 + server_routine];

#if ONE_SIDED_READ == 0

  // epoch_status_ = new uint8_t*[1 + server_routine];
  // for (int i = 0; i < server_routine+1; i++) {
  //   epoch_status_[i] = new uint8_t[cm_->get_num_nodes()];
  //   for (int j = 0; j < cm_->get_num_nodes(); j++)
  //     epoch_status_[i][j] = CALVIN_EPOCH_READY;
  // }

  // req_buffers = new std::vector<char*>[1 + server_routine];
  // int buf_len = sizeof(calvin_header) + MAX_CALVIN_REQ_CNTS*sizeof(calvin_request);
  // for (int i = 0; i < server_routine+1; i++) {
  //   for (int j = 0; j < cm_->get_num_nodes(); j++)
  //     req_buffers[i].push_back((char*)malloc(buf_len));
  // }

  // send_buffers = new char*[1 + server_routine];
  // for (int i = 0; i < server_routine+1; i++)
  //   send_buffers[i] = rpc_->get_static_buf(MAX_MSG_SIZE);

  forwarded_values = new std::map<uint64_t, read_val_t>[1 + server_routine];

#elif ONE_SIDED_READ == 1

  const char *start_ptr = (char *)(cm_->conn_buf_);
  LOG(3) << "start_ptr = " << (void*)start_ptr << " addrs:";

  /* set up forward addresses and offsets */
  forward_addresses = new char*[1 + server_routine];
  forward_offsets_ = new uint64_t[1 + server_routine];

  char* forward_base_ptr_ = oltp::calvin_forward_buffer + 
                        per_thread_calvin_forward_buffer_sz * worker_id_;
  // uint64_t forward_base_offset_ = forward_base_ptr_ - start_ptr;
  LOG(3) << "forward offsets:";
  for (int i = 0; i < server_routine+1; i++) {
    LOG(3) << "routine " << i;
    int n_forwarded = MAX_CALVIN_REQ_CNTS << MAX_CALVIN_SETS_SUPPRTED_IN_BITS;
    forward_addresses[i] = forward_base_ptr_ + i * n_forwarded * sizeof(read_compact_val_t);
    forward_offsets_[i] = forward_addresses[i] - start_ptr;
    LOG(3) << "forward_address " << static_cast<void*>(forward_addresses[i]);
    LOG(3) << "forward_offset " << forward_offsets_[i];
  }

#elif ONE_SIDED_READ == 2 // hybrid (onesided broadcast + rpc forward)

  // const char *start_ptr = (char *)(cm_->conn_buf_);
  // LOG(3) << "start_ptr = " << (void*)start_ptr;

  // /*set up req_buffer and req offsets*/
  // req_buffers = new std::vector<char*>[1 + server_routine];
  // int buf_len = sizeof(calvin_header) + MAX_CALVIN_REQ_CNTS*sizeof(calvin_request);
  // char* req_base_ptr_ = oltp::calvin_request_buffer + 
  //                   per_thread_calvin_request_buffer_sz * worker_id_;
  // uint64_t req_base_offset_ = req_base_ptr_ - start_ptr;
  // LOG (3) << "req_base_offset for thread " << worker_id_ << ": " << req_base_offset_;
  // LOG(3) << "addresses:";
  // for (int i = 0; i < server_routine+1; i++) {
  //   LOG(3) << "routine " << i;
  //   for (int j = 0; j < cm_->get_num_nodes(); j++) {
  //     char* buf = req_base_ptr_ + buf_len * i * cm_->get_num_nodes() + buf_len * j;
  //     req_buffers[i].push_back(buf);
  //     calvin_header* ch = (calvin_header*)req_buffers[i][req_buffers[i].size()-1];
  //     ch->epoch_status = CALVIN_EPOCH_READY;
  //     LOG(3) << "node " << j << "'s addrs: " << static_cast<void*>(req_buffers[i][j]);
  //   }
  // }

  // LOG(3) << "offsets:";
  // offsets_ = new uint64_t*[1 + server_routine];
  // for (int i = 0; i < server_routine+1; i++) {
  //   offsets_[i] = new uint64_t[cm_->get_num_nodes()];
  //   LOG(3) << "routine " << i;
  //   for (int j = 0; j < cm_->get_num_nodes(); j++) {
  //     offsets_[i][j] = req_buffers[i][j] - start_ptr;
  //       LOG(3) << "node " << j << "'s offset: " << offsets_[i][j];
  //   }
  // }

  forwarded_values = new std::map<uint64_t, read_val_t>[1 + server_routine]; 
#endif

}
#endif

void BenchWorker::run() {

  // create connections
  exit_lock.Lock();
  if(conns.size() == 0) {
    // only create once
    for(uint i = 0;i < nthreads + nclients + 1; ++i)
      conns.push_back(new CommQueue(nthreads + nclients + 1));
  }
  exit_lock.Unlock();

  BindToCore(worker_id_); // really specified to platforms
  binding(worker_id_);
  init_tx_ctx();
  init_routines(server_routine);

  create_logger();

#if USE_RDMA
  init_rdma();
  create_qps();
#endif

#if USE_TCP_MSG == 1
  assert(local_comm_queues.size() > 0);
  create_tcp_connections(local_comm_queues[worker_id_],tcp_port,send_context);
#else
  MSGER_TYPE type;

#if USE_UD_MSG == 1
  type = UD_MSG;
#if LOCAL_CLIENT == 1 || CS == 0
  int total_connections = 1;
#else
  int total_connections = nthreads + nclients;
#endif
  create_rdma_ud_connections(total_connections);
#else
  create_rdma_rc_connections(rdma_buffer + HUGE_PAGE_SZ,
                             total_ring_sz,ring_padding);
#endif

#endif

  this->init_new_logger(backup_stores_);
  this->thread_local_init();   // application specific init

  register_callbacks();

#if CS == 1
#if LOCAL_CLIENT == 0
  create_client_connections(nthreads + nclients);
#endif
  assert(pending_reqs_.empty());
  ROCC_BIND_STUB(rpc_, &BenchWorker::req_rpc_handler, this, RPC_REQ);
#endif

#ifdef CALVIN_TX
  init_calvin();
  // ROCC_BIND_STUB(rpc_, &BenchWorker::calvin_schedule_rpc_handler, this, RPC_CALVIN_SCHEDULE);
  // ROCC_BIND_STUB(rpc_, &BenchWorker::calvin_epoch_status_rpc_handler, this, RPC_CALVIN_EPOCH_STATUS);
#endif

  // fetch QPs
  fill_qp_vec(cm_,worker_id_);

  // waiting for master to start workers
  this->inited = true;
#if 1
  while(!this->running) {
    asm volatile("" ::: "memory");
  }
#else
  this->running = true;
#endif
  // starts the new_master_routine
  start_routine(); // uses parent worker->start
}

#if CALVIN_TX
void __attribute__((optimize("O1"))) // this flag is very tricky, it should be set this way
BenchWorker::worker_routine(yield_func_t &yield) {
    uint64_t retry_count(0);
    // serve as a deterministic transaction executor
      
    /* worker routine that is used to run transactions */
    workloads[cor_id_] = get_workload();
    auto &workload = workloads[cor_id_];
    LOG(3) << "running calvin routine on worker " << worker_id_ << " coroutine " << cor_id_;

    while(true) {
        // det_request req;
        // req.req_idx = 0;                              // for mocking req
        // req.req_initiator = cm_->get_nodeid();        // for mocking req

        if (scheduler->locked_transactions.size() < nthreads) {
          // LOG(3) << "aaaa";
          cpu_relax();
          yield_next(yield);
          continue;
        }
        if (scheduler->locked_transactions[worker_id_][cor_id_] == NULL) {     
          // LOG(3) << "bbbb";
          cpu_relax();
          yield_next(yield);
          continue;
        }

        // if(scheduler->locks_4_locked_transactions[worker_id_]->Trylock()) {
        //   cpu_relax();
        //   yield_next(yield);
        //   continue;
        // }

        // ASSERT(scheduler->locks_4_locked_transactions[worker_id_]->IsLocked()) << "Lock NOT held.";
        // locked
        // if (scheduler->locked_transactions[worker_id_][cor_id_]->empty()) {
        //   scheduler->locks_4_locked_transactions[worker_id_]->Unlock();
        //   // LOG(3) << "cccc";
        //   cpu_relax();
        //   yield_next(yield);
        //   continue;
        // }

        det_request* req_ptr = NULL;
        // req = scheduler->locked_transactions[worker_id_][cor_id_]->front();
        // scheduler->locked_transactions[worker_id_][cor_id_]->pop();
        // scheduler->locks_4_locked_transactions[worker_id_]->Unlock();
        if (!scheduler->locked_transactions[worker_id_][cor_id_]->pop(req_ptr)) {
          cpu_relax();
          yield_next(yield);
          continue;
        }
        det_request& req = *req_ptr;

        ASSERT(req.req_idx < workload.size()) << 
                        "in execution seq = " << req.req_seq <<
                        ":  workload " << req.req_idx << " does not exist.";
        // printf("%d: req_idx: %d, req_seq: %d, ts: %d, reads %d, writes %d.\n", 
        //                                       req.req_initiator, 
        //                                       req.req_idx, req.req_seq, req.timestamp, 
        //                                      ((rwsets_t*)req.req_info)->nReads,
        //                                      ((rwsets_t*)req.req_info)->nWrites);



        (*txn_counts)[req.req_idx] += 1;
    abort_retry:
        if (req.req_initiator == cm_->get_nodeid())
          ntxn_executed_ += 1;

#if MOCK_WORKLOAD==1
        {
          // fake the read set and write set
          std::vector<ReadSetItem> rs;
          std::vector<ReadSetItem> ws;
          rwsets_t* sets = (rwsets_t*)req.req_info;
          int reads = sets->nReads, writes = sets->nWrites;
          for (int i = 0; i < reads; i++) {
            rs.push_back(sets->access[i]);
          }
          for (int i = 0; i < writes; i++) {
            ws.push_back(sets->access[reads+i]);
          }

          // fake txn workload
          // usleep(5);
       
          // fake releasing locks
          for(auto it = rs.begin();it != rs.end();++it) {
            if((*it).pid != cm_->get_nodeid())  // remote case
              continue;
            else {
              assert(it->node != NULL);
              volatile uint64_t* lock_ptr = &it->node->lock;
              // assert ((*lock_ptr & 0x1) == 0x1);
              // fprintf(stderr, "releasing read %d %d\n", it->tableid, it->key);
              *lock_ptr = 0;
            }
          }

          for(auto it = ws.begin();it != ws.end();++it) {
            if((*it).pid != cm_->get_nodeid())  // remote case
              continue;
            else {
              assert(it->node != NULL);
              volatile uint64_t* lock_ptr = &it->node->lock;
              // assert ((*lock_ptr & 0x1) == 0x1);
              // fprintf(stderr, "releasing read %d %d\n", it->tableid, it->key);
              *lock_ptr = 0;
            }
          }
        }

        auto ret = txn_result_t(true, 73);
#else
        // actual workload
        auto ret = workload[req.req_idx].fn(this, &req, yield);
#endif
        // fprintf(stderr, "txn with ts %lx committed.\n", req.timestamp);
    #if NO_ABORT == 1
        ret.first = true;
    #endif
        // if(current_partition == 0){
        if(likely(ret.first)) {
          // commit case
// #if CALCULATE_LAT == 1
//       if(cor_id_ == 1) {
//         //#if LATENCY == 1
//         latency_timer_.end();
//         //#else
//         workload[tx_idx].latency_timer.end();
//         //#endif
//       }
// #endif
          __atomic_fetch_add(&scheduler->req_fullfilled, 1, __ATOMIC_SEQ_CST);
          retry_count = 0;
          if (req.req_initiator == cm_->get_nodeid())
            ntxn_commits_ += 1;
          // self_generated requests
          assert(CS == 0);
        } else {
          assert(false);
          retry_count += 1;
    #if DEBUG_RETRY_TXN
          fprintf(stderr, "%d: retry transaction.\n", cor_id_);
    #endif
          ntxn_aborts_ += 1;
          yield_next(yield);
          goto abort_retry;
        }
        yield_next(yield);
    }

#if 0
    uint64_t wait_start = nocc::util::get_now();
    while (nocc::util::get_now() - wait_start < 100000)
      yield_next(yield);
#endif

    // fprintf(stderr, "done execution for iteration %d.\n", iteration);
    // fprintf(stderr, "%d %d ending for iteration %d.\n", worker_id_, cor_id_, iteration);

  // rpc_->free_static_buf(send_buf);
  // free(req_buf);

  //this yield must be there to allow current finished coroutine
  //not to block the scheduling of following coroutines in the
  //coroutine schedule list, a.k.a, the the routineMeta list.
  indirect_must_yield(yield);
  fprintf(stdout, "%d: ends.\n", cor_id_);  
}

#elif BOHM_TX

void __attribute__((optimize("O1"))) // this flag is very tricky, it should be set this way
BenchWorker::worker_routine(yield_func_t &yield) {
  LOG(3) << "running bohm routine on worker " << worker_id_;
  
  if (worker_id_ < bohm_cc_threads) {
    fprintf(stdout, "thread id = %d, I am a bohm concurrency control thread.", worker_id_);
    while (true) {
      while(scheduler.det_batch_ready == false);

      for (int i = 0; i < scheduler.deterministic_plan.size(); ++i) {
        det_request& req = scheduler.deterministic_plan[i];
        rwsets_t* sets = ((rwsets_t*)req.req_info);

        // for each record in the write set, add a new version of record
        for (int j = nReads; j < sets->nReads + sets->nWrites; j++) {
          if (sets->access[j].pid == cm_->get_nodeid() && 
              sets->access[j].key % bohm_cc_threads == worker_id_) {
              // this is the write that I am responsible for
              auto node = local_lookup_op(sets->access[j]->tableid, 
                                          sets->access[j]->key);
              assert(node != NULL);
              BOHMRecord* new_record = (BOHMRecord*)malloc(sizeof(BOHMRecord));
              new_record.start_ts = i;
              new_record.end_ts = (uint)(-1);
              new_record.txn = &req;
              
              // new_record.data is uninitialized.
              new_record.prev = (BOHMRecord*)node->value;
              ((BOHMRecord*)node->value)->end_ts = i;
              node->value = (char*)new_record;
          }
        }

        // for each record in the read set, annotate current transaction so that
        // when current transaction executes later, it will be O(1) to find out the
        // correct version to read for each record in the read set.
        for (int j = 0; j < sets->nReads; j++) {
          if (sets->access[j].pid == cm_->get_nodeid() && 
              sets->access[j].key % bohm_cc_threads == worker_id_) {
              // this is the read that I am responsible for
              auto node = local_lookup_op(sets->access[j]->tableid, 
                                          sets->access[j]->key);
              assert(node != NULL);
              sets->access[j].correct_record_version_to_read = (BOHMRecord*)node->value; 
          }
        }
      }
    }
  } else {
    fprintf(stdout, "thread id = %d, I am a bohm execution thread.", worker_id_);

  }
}

#else // for common non-det protocols

void __attribute__((optimize("O1"))) // this flag is very tricky, it should be set this way
BenchWorker::worker_routine(yield_func_t &yield) {
  assert(conns.size() != 0);
  LOG(3) << "running non-det protocol routine on worker " << worker_id_;
  
  using namespace db;
  /* worker routine that is used to run transactions */
  workloads[cor_id_] = get_workload();
  auto &workload = workloads[cor_id_];

  // Used for OCC retry
  unsigned int backoff_shifts = 0;
  unsigned long abort_seed = 73;

  while(abort_seed == random_generator[cor_id_].get_seed()) {
    abort_seed += 1;         // avoids seed collision
  }

#if SI_TX
  //if(current_partition == 0) indirect_must_yield(yield);
#endif


  uint64_t retry_count(0);
   //uint64_t max_count = 2000;
   //while (max_count-- > 0) {
  while(true) {
#if CS == 0
    /* select the workload */
    double d = random_generator[cor_id_].next_uniform();

    uint tx_idx = 0;
    for(size_t i = 0;i < workload.size();++i) {
      if((i + 1) == workload.size() || d < workload[i].frequency) {
        tx_idx = i;
        break;
      }
      d -= workload[i].frequency;
    }
#else
#if LOCAL_CLIENT
    REQ req;
    if(!conns[worker_id_]->front((char *)(&req))){
      yield_next(yield);
      continue;
    }
    conns[worker_id_]->pop();
#else
    if(pending_reqs_.empty()){
      yield_next(yield);
      continue;
    }
    REQ req = pending_reqs_.front();
    pending_reqs_.pop();
#endif
    int tx_idx = req.tx_id;
#endif

#if CALCULATE_LAT == 1
    if(cor_id_ == 1) {
      // only profile the latency for cor 1
      //#if LATENCY == 1
      latency_timer_.start();
      //#else
      (workload[tx_idx].latency_timer).start();
      //#endif
    }
#endif
    const unsigned long old_seed = random_generator[cor_id_].get_seed();
    (*txn_counts)[tx_idx] += 1;
 abort_retry:
    ntxn_executed_ += 1;
    auto ret = workload[tx_idx].fn(this,yield);
#if NO_ABORT == 1
    //ret.first = true;
#endif
    // if(current_partition == 0){
    if(likely(ret.first)) {
      // commit case
      retry_count = 0;
#if CALCULATE_LAT == 1
      if(cor_id_ == 1) {
        //#if LATENCY == 1
        latency_timer_.end();
        //#else
        workload[tx_idx].latency_timer.end();
        //#endif
      }
#endif

#if CS == 0 // self_generated requests
      ntxn_commits_ += 1;

#if PROFILE_RW_SET == 1 || PROFILE_SERVER_NUM == 1
      if(ret.second > 0)
        workload[tx_idx].p.process_rw(ret.second);
#endif
#else // send reply to clients
      ntxn_commits_ += 1;
      // reply to client
#if LOCAL_CLIENT
      char dummy = req.cor_id;
      conns[req.c_tid]->enqueue(worker_id_,(char *)(&dummy),sizeof(char));
#else
      char *reply = rpc_->get_reply_buf();
      rpc_->send_reply(reply,sizeof(uint8_t),req.c_id,req.c_tid,req.cor_id,client_handler_);
#endif
#endif
    } else {
      retry_count += 1;
#if DEBUG_RETRY_TXN
      fprintf(stdout, "%d: retry transaction.\n", cor_id_);
#endif
      //if(retry_count > 10000000) assert(false);
      // abort case
      if(old_seed != abort_seed) {
        /* avoid too much calculation */
        ntxn_abort_ratio_ += 1;
        abort_seed = old_seed;
        (*txn_aborts)[tx_idx] += 1;
      }
      ntxn_aborts_ += 1;
      yield_next(yield);

      // reset the old seed
      random_generator[cor_id_].set_seed(old_seed);
      goto abort_retry;
    }
    yield_next(yield);
    // end worker main loop
  }

  //this yield must be there to allow current finished coroutine
  //not to block the scheduling of following coroutines in the
  //coroutine schedule list, a.k.a, the the routineMeta list.
  indirect_must_yield(yield);
  fprintf(stderr, "%d: ends.\n", cor_id_);
}

#endif // routine functions for CALVIN, BOHM or non-det protocols

void BenchWorker::events_handler() {
  LOG(3) << "in bench event handler";
  RWorker::events_handler();
}

void BenchWorker::exit_handler() {

  if( worker_id_ == 0 ){

    // only sample a few worker information
    auto &workload = workloads[1];

    auto second_cycle = BreakdownTimer::get_one_second_cycle();
#if 1
    //exit_lock.Lock();
    fprintf(stderr, "stats for worker %d:\n", worker_id_);
    for(uint i = 0;i < workload.size();++i) {
      workload[i].latency_timer.calculate_detailed();
      fprintf(stdout,"%s executed %lu, latency: %f, rw_size %f, m %f, 90 %f, 99 %f\n",
              workload[i].name.c_str(),
              // (double)((*txn_aborts)[i]) / ((*txn_counts)[i] + ((*txn_counts)[i] == 0)),
              (*txn_counts)[i],
              workload[i].latency_timer.report() / second_cycle * 1000,
              workload[i].p.report(),
              workload[i].latency_timer.report_medium() / second_cycle * 1000,
              workload[i].latency_timer.report_90() / second_cycle * 1000,
              workload[i].latency_timer.report_99() / second_cycle * 1000);
    }
    fprintf(stdout,"succs ratio %f\n",(double)(ntxn_commits_) /
            (double)(ntxn_executed_));

    exit_report();
#endif
#if MVCC_TX || NOWAIT_TX || SUNDIAL_TX || OCC_TX || WAITDIE_TX
    int temp[40];
    for(int i = 0; i < 40; ++i)
        temp[i] = 0;
    for(int i = 0; i < coroutine_num; ++i)
    for(int j = 0; j < 40; ++j)
        temp[j] += dynamic_cast<rtx::TXOpBase *>(new_txs_[i])->abort_cnt[j];
    for(int j = 0; j < 40; ++j) 
      LOG(3) << j <<": " << temp[j];
    //auto hkztx = dynamic_cast<rtx::MVCC *>(new_txs_[1]);
    //hkztx->show_abort();
#endif

#if defined(CALVIN_TX) && DEBUG_DETAIL
    fprintf(stdout, "\n");
    for(uint i = 0; i < server_routine + 1;++i) {
      if (i == 1) {
#if defined(OCC_TX) || defined(NOWAIT_TX) || defined(WAITDIE_TX) || defined(CALVIN_TX)
        auto tx = dynamic_cast<rtx::TXOpBase*>(new_txs_[i]);
#else
        assert(false);
#endif
        fprintf(stdout, "cor_id_ = %d:\n", i);
        for (uint j = 0; j < rtx::TXOpBase::DEBUG_CNT; j++) {
          fprintf(stdout, "cnt:%-10d%-10d\n", j, tx->P[j]);
        }
        fprintf(stdout, "\n");
      }
    }
#endif

#if RECORD_STALE
    util::RecordsBuffer<double> total_buffer;
    for(uint i = 0; i < server_routine + 1;++i) {

      // only calculate staleness for timestamp based method
#if RAD_TX
      auto tx = dynamic_cast<DBRad *>(txs_[i]);
#elif SI_TX
      auto tx = dynamic_cast<DBSI *>(txs_[i]);
#else
      DBRad *tx = NULL;
#endif
      assert(tx != NULL);
      total_buffer.add(tx->stale_time_buffer);
    }
    fprintf(stdout,"total %d points recorded\n",total_buffer.size());
    total_buffer.sort_buffer(0);
    std::vector<int> cdf_idx({1,5,10,15,20,25,30,35,40,45,
            50,55,60,65,70,75,80,81,82,83,84,85,90,95,
            97,98,99,100});
    total_buffer.dump_as_cdf("stale.res",cdf_idx);
#endif
    check_consistency();

    //exit_lock.Unlock();

    fprintf(stdout,"master routine exit...\n");
  }
  return;
}

/* Abstract bench loader */
BenchLoader::BenchLoader(unsigned long seed)
    : random_generator_(seed) {
  worker_id_ = 0; /**/
}

void BenchLoader::run() {
  load();
}

BenchClient::BenchClient(unsigned worker_id,unsigned seed)
    :RWorker(worker_id,cm,seed) { }

void BenchClient::run() {

  fprintf(stdout,"[Client %d] started\n",worker_id_);
  // client only support ud msg for communication
  BindToCore(worker_id_);
#if USE_RDMA
  init_rdma();
#endif
  init_routines(coroutine_num);
  //create_server_connections(UD_MSG,nthreads);
#if CS == 1 && LOCAL_CLIENT == 0
  create_client_connections();
#endif
  running = true;
  start_routine();
  return;
}

void BenchClient::worker_routine_local(yield_func_t &yield) {

  uint64_t *start = new uint64_t[coroutine_num];
  for(uint i = 0;i < coroutine_num;++i) {
    BenchWorker::REQ req;
    uint8_t tx_id;
    auto node = get_workload((char *)(&tx_id),random_generator[i]);
    req.c_tid = worker_id_;
    req.tx_id = tx_id;
    req.cor_id = i;
    uint thread = random_generator[i].next() % nthreads;
    start[i] = rdtsc();
    conns[thread]->enqueue(worker_id_,(char *)(&req),sizeof(BenchWorker::REQ));
  }
      
  while(running) {
    char res[16];
    if(conns[worker_id_]->front(res)) {
      int cid = (int)res[0];
      conns[worker_id_]->pop();
      auto latency = rdtsc() - start[cid];
      timer_.emplace(latency);

      BenchWorker::REQ req;
      uint8_t tx_id;
      auto node = get_workload((char *)(&tx_id),random_generator[cid]);
      req.c_tid = worker_id_;
      req.tx_id = tx_id;
      req.cor_id = cid;
      uint thread = random_generator[cid].next() % nthreads;
      start[cid] = rdtsc();
      conns[thread]->enqueue(worker_id_,(char *)(&req),sizeof(BenchWorker::REQ));
    }
    // end while
  }
}

void BenchClient::worker_routine(yield_func_t &yield) {
#if LOCAL_CLIENT
  return worker_routine_local(yield);
#endif

  char *req_buf = rpc_->get_static_buf(64);
  char reply_buf[64];
  while(true) {
    auto start = rdtsc();

    // prepare arg
    char *arg = (char *)req_buf;
    auto node = get_workload(arg + sizeof(uint8_t),random_generator[cor_id_]);
    *((uint8_t *)(arg)) = worker_id_;
    uint thread = random_generator[cor_id_].next() % nthreads;

    // send to server
    rpc_->prepare_multi_req(reply_buf,1,cor_id_);
    rpc_->append_req(req_buf,RPC_REQ,sizeof(uint64_t),cor_id_,RRpc::REQ,node,thread);

    // yield
    indirect_yield(yield);
    // get results back
    auto latency = rdtsc() - start;
    timer_.emplace(latency);
  }
}

void BenchClient::exit_handler() {
#if LOCAL_CLIENT == 0
  auto second_cycle = util::BreakdownTimer::get_one_second_cycle();
  auto m_av = timer_.report_avg() / second_cycle * 1000;
  //exit_lock.Lock();
  fprintf(stdout,"avg latency for client %d, %3f ms\n",worker_id_,m_av);
  //exit_lock.Unlock();
#endif
}

void BenchWorker::req_rpc_handler(int id,int cid,char *msg,void *arg) {
  uint8_t *input = (uint8_t *)msg;
  pending_reqs_.emplace(input[1],id,input[0],cid);

  // no reply here since the request will be reponded
  // after the transaction has been processed.
}

}; // oltp

}; // nocc
