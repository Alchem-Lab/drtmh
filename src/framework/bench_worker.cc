#include "rocc_config.h"
#include "tx_config.h"

#include "config.h"

#include "bench_worker.h"

#include "req_buf_allocator.h"

#include "db/txs/dbrad.h"
#include "db/txs/dbsi.h"

#ifdef CALVIN_TX
#include "db/txs/epoch_manager.hpp"
#include "rtx/qp_selection_helper.h"
#endif

#include <queue>

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
db::EpochManager* epoch_manager = NULL; 
extern uint64_t per_thread_calvin_request_buffer_sz;
extern uint64_t per_thread_calvin_forward_buffer_sz;
namespace oltp {
extern char* calvin_request_buffer;
extern char* calvin_forward_buffer;
}

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
  // read_set_ptr = new std::vector<rtx::CALVIN::ReadSetItem>*[1 + server_routine + 2];
  // std::fill_n(read_set_ptr,1 + server_routine + 2,static_cast<std::vector<rtx::CALVIN::ReadSetItem>*>(NULL));
  // write_set_ptr = new std::vector<rtx::CALVIN::ReadSetItem>*[1 + server_routine + 2];
  // std::fill_n(write_set_ptr,1 + server_routine + 2,static_cast<std::vector<rtx::CALVIN::ReadSetItem>*>(NULL));

  // for (int i = 0; i < server_routine+1; i++) {
  //   assert(new_txs_[i]);
  //   read_set_ptr[i] = &new_txs_[i]->read_set_;
  //   write_set_ptr[i] = &new_txs_[i]->write_set_;
  // }

  deterministic_requests = new std::vector<calvin_request*>[1 + server_routine];
  // batch_size_for_current_epoch = new std::map<int, uint64_t>[1 + server_routine];
  epoch_done_schedule = new bool[1 + server_routine];
  memset(epoch_done_schedule, 0, sizeof(bool)*(1 + server_routine));
  mach_received = new std::set<int>[1 + server_routine];

#if ONE_SIDED_READ == 0

  epoch_status_ = new uint8_t*[1 + server_routine];
  for (int i = 0; i < server_routine+1; i++) {
    epoch_status_[i] = new uint8_t[cm_->get_num_nodes()];
    for (int j = 0; j < cm_->get_num_nodes(); j++)
      epoch_status_[i][j] = CALVIN_EPOCH_READY;
  }

  req_buffers = new std::vector<char*>[1 + server_routine];
  int buf_len = sizeof(calvin_header) + MAX_CALVIN_REQ_CNTS*sizeof(calvin_request);
  for (int i = 0; i < server_routine+1; i++) {
    for (int j = 0; j < cm_->get_num_nodes(); j++)
      req_buffers[i].push_back((char*)malloc(buf_len));
  }

  send_buffers = new char*[1 + server_routine];
  for (int i = 0; i < server_routine+1; i++)
    send_buffers[i] = rpc_->get_static_buf(MAX_MSG_SIZE);

  forwarded_values = new std::map<uint64_t, read_val_t>[1 + server_routine];

#elif ONE_SIDED_READ == 1

  const char *start_ptr = (char *)(cm_->conn_buf_);
  LOG(3) << "start_ptr = " << (void*)start_ptr << " addrs:";

  /*set up req_buffer and req offsets*/
  req_buffers = new std::vector<char*>[1+server_routine];
  int buf_len = sizeof(calvin_header) + MAX_CALVIN_REQ_CNTS*sizeof(calvin_request);
  char* req_base_ptr_ = oltp::calvin_request_buffer + 
                    per_thread_calvin_request_buffer_sz * worker_id_;
  // uint64_t req_base_offset_ = req_base_ptr_ - start_ptr;
  for (int i = 0; i < server_routine+1; i++) {
    LOG(3) << "routine " << i;
    for (int j = 0; j < cm_->get_num_nodes(); j++) {
      char* buf = req_base_ptr_ + buf_len * i * cm_->get_num_nodes() + buf_len * j;
      req_buffers[i].push_back(buf);
      calvin_header* ch = (calvin_header*)req_buffers[i][req_buffers[i].size()-1];
      ch->epoch_status = CALVIN_EPOCH_READY;
      LOG(3) << "node " << j << "'s addrs: " << static_cast<void*>(req_buffers[i][j]);
    }
  }

  LOG(3) << "offsets:";
  offsets_ = new uint64_t*[1 + server_routine];
  for (int i = 0; i < server_routine+1; i++) {
    offsets_[i] = new uint64_t[cm_->get_num_nodes()];
    LOG(3) << "routine " << i;
    for (int j = 0; j < cm_->get_num_nodes(); j++) {
      offsets_[i][j] = req_buffers[i][j] - start_ptr;
        LOG(3) << "node " << j << "'s offset: " << offsets_[i][j];
    }
  }

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

  const char *start_ptr = (char *)(cm_->conn_buf_);
  LOG(3) << "start_ptr = " << (void*)start_ptr;

  /*set up req_buffer and req offsets*/
  req_buffers = new std::vector<char*>[1 + server_routine];
  int buf_len = sizeof(calvin_header) + MAX_CALVIN_REQ_CNTS*sizeof(calvin_request);
  char* req_base_ptr_ = oltp::calvin_request_buffer + 
                    per_thread_calvin_request_buffer_sz * worker_id_;
  uint64_t req_base_offset_ = req_base_ptr_ - start_ptr;
  LOG (3) << "req_base_offset for thread " << worker_id_ << ": " << req_base_offset_;
  LOG(3) << "addresses:";
  for (int i = 0; i < server_routine+1; i++) {
    LOG(3) << "routine " << i;
    for (int j = 0; j < cm_->get_num_nodes(); j++) {
      char* buf = req_base_ptr_ + buf_len * i * cm_->get_num_nodes() + buf_len * j;
      req_buffers[i].push_back(buf);
      calvin_header* ch = (calvin_header*)req_buffers[i][req_buffers[i].size()-1];
      ch->epoch_status = CALVIN_EPOCH_READY;
      LOG(3) << "node " << j << "'s addrs: " << static_cast<void*>(req_buffers[i][j]);
    }
  }

  LOG(3) << "offsets:";
  offsets_ = new uint64_t*[1 + server_routine];
  for (int i = 0; i < server_routine+1; i++) {
    offsets_[i] = new uint64_t[cm_->get_num_nodes()];
    LOG(3) << "routine " << i;
    for (int j = 0; j < cm_->get_num_nodes(); j++) {
      offsets_[i][j] = req_buffers[i][j] - start_ptr;
        LOG(3) << "node " << j << "'s offset: " << offsets_[i][j];
    }
  }

  forwarded_values = new std::map<uint64_t, read_val_t>[1 + server_routine]; 
#endif

}
#endif

void BenchWorker::run() {

  // create connections
  exit_lock.Lock();
  if(conns.size() == 0) {
    // only create once
    for(uint i = 0;i < nthreads + nclients + 1;++i)
      conns.push_back(new CommQueue(nthreads + nclients + 1));
  }
  exit_lock.Unlock();

  BindToCore(worker_id_); // really specified to platforms
  binding(worker_id_);
  init_tx_ctx();
  init_routines(server_routine);

  //create_logger();

#if USE_RDMA
  init_rdma();
  create_qps();
#endif

#if USE_TCP_MSG == 1
  assert(local_comm_queues.size() > 0);
  assert(local_comm_queues.size() > worker_id_);
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
  ROCC_BIND_STUB(rpc_, &BenchWorker::calvin_schedule_rpc_handler, this, RPC_CALVIN_SCHEDULE);
  ROCC_BIND_STUB(rpc_, &BenchWorker::calvin_epoch_status_rpc_handler, this, RPC_CALVIN_EPOCH_STATUS);
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

void __attribute__((optimize("O1"))) // this flag is very tricky, it should be set this way
BenchWorker::worker_routine(yield_func_t &yield) {
#ifdef CALVIN_TX
  return worker_routine_for_calvin(yield);
#else

  assert(conns.size() != 0);

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
  fprintf(stdout, "%d: ends.\n", cor_id_);

#endif // CALVIN_TX
}

#ifdef CALVIN_TX
void __attribute__((optimize("O1"))) // this flag is very tricky, it should be set this way
BenchWorker::worker_routine_for_calvin(yield_func_t &yield) {

  assert(conns.size() != 0);

  using namespace db;
  /* worker routine that is used to run transactions */
  workloads[cor_id_] = get_workload();
  auto &workload = workloads[cor_id_];

  // Used for OCC retry
  // unsigned int backoff_shifts = 0;
  unsigned long abort_seed = 73;

  while(abort_seed == random_generator[cor_id_].get_seed()) {
    abort_seed += 1;         // avoids seed collision
  }

  std::set<int> mac_set_;
  for (int i = 0; i < cm_->get_num_nodes(); i++) {
    if (i != cm_->get_nodeid()) {
      mac_set_.insert(i);
      // fprintf(stdout, "machine %d added to set.\n", i);
    }
  }

  int my_timestamp = 0;
  char* const req_buf = req_buffers[cor_id_][cm_->get_nodeid()];

#if ONE_SIDED_READ == 0
  char* send_buf = send_buffers[cor_id_];
  assert(send_buf != NULL);
#endif

  // for (int iteration = 0; iteration < 100; iteration++)
  for (int iteration = 0; ; iteration++)
  {
    // fprintf(stderr, "%d %d starting for iteration %d.\n", worker_id_, cor_id_, iteration);
    char* req_buf_end = req_buf;

#if ONE_SIDED_READ == 0
    for (int i = 0; i < cm_->get_num_nodes(); i++)
      epoch_status_[cor_id_][i] = CALVIN_EPOCH_READY;
#else
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      calvin_header* ch = (calvin_header*)req_buffers[cor_id_][i];
      ch->epoch_status = CALVIN_EPOCH_READY;
    }
#endif

    epoch_done_schedule[cor_id_] = false;
    deterministic_requests[cor_id_].clear();

#if ONE_SIDED_READ == 0 || ONE_SIDED_READ == 2
    forwarded_values[cor_id_].clear();
#else
    int n_forwarded = MAX_CALVIN_REQ_CNTS << MAX_CALVIN_SETS_SUPPRTED_IN_BITS;
    memset(forward_addresses[cor_id_], 0, n_forwarded * sizeof(read_compact_val_t));
#endif

    uint64_t start = nocc::util::get_now();
    ((calvin_header*)req_buf)->node_id = cm_->get_nodeid();
    epoch_manager->get_current_epoch((char*)&((calvin_header*)req_buf)->epoch_id);
    ((calvin_header*)req_buf)->chunk_size = 0;
    // fprintf(stdout, "sequencing @ epoch %lu.\n", ((calvin_header*)req_buf)->epoch_id);

    // uint64_t max_count = 1;
    // while (max_count-- > 0) {
    req_buf_end += sizeof(calvin_header);
    uint64_t batch_size_ = 0;
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

        // uint64_t request_timestamp = nocc::util::get_now_nano() << 3;
        uint64_t request_timestamp = my_timestamp << 3;
        request_timestamp |= cm_->get_nodeid();
        my_timestamp += 1;
        // buffer for 10 milliseconds for one epoch
        if (batch_size_ < MAX_CALVIN_REQ_CNTS) {
        // if (request_timestamp - start < 10000) {
        // if (request_timestamp - start < 10000) {
          if (req_buf_end - req_buf >= sizeof(calvin_header) + MAX_CALVIN_REQ_CNTS*sizeof(calvin_request))
            assert(false);
          *(calvin_request*)req_buf_end = calvin_request(tx_idx, cm_->get_nodeid(), request_timestamp);
          req_buf_end += sizeof(calvin_request);
          batch_size_++;
          // if (batch_size_ >= 16000) break;
          // if (batch_size_ >= 1) break;
          // if (batch_size_ >= 16000) break;
        } else break;
    }
    assert(req_buf == req_buffers[cor_id_][cm_->get_nodeid()]);
    ((calvin_header*)req_buf)->batch_size = batch_size_;

    // fprintf(stdout, "collecting @ epoch %lu.\n", ((calvin_header*)req_buf)->epoch_id);


    // fprintf(stdout, "after collecting: batched %d @ epoch %lu. start broadcasting. at %lu.\n", 
    //                 ((calvin_header*)req_buf)->batch_size, 
    //                 ((calvin_header*)req_buf)->epoch_id,
    //                 nocc::util::get_now());

    // generating request info for each request
    char* ptr = req_buf + sizeof(calvin_header);
    for (uint64_t i = 0; i < batch_size_; i++) {
      assert(ptr != NULL);
      ASSERT(((calvin_request*)ptr)->req_idx < workload.size()) << "in gen sets:  workload " << ((calvin_request*)ptr)->req_idx << " does not exist.";
      assert (workload[((calvin_request*)ptr)->req_idx].gen_sets_fn != nullptr);
      assert (((calvin_request*)ptr)->req_info - req_buf >= sizeof(calvin_header));
      // fprintf(stdout, "generating req info for request %d\n", ((calvin_request*)ptr)->req_idx);
      workload[((calvin_request*)ptr)->req_idx].gen_sets_fn(this,((calvin_request*)ptr)->req_info,yield);
      ptr += sizeof(calvin_request);
    }

    // sequencer has bufferred one epoch of requests.
    // broadcast bufferred request through rpc call to other partitions
    // in the same replica.
    // fprintf(stderr, "batched %d @ epoch %lu. start broadcasting. at %lu for iteration %d.\n", 
    //                 ((calvin_header*)req_buf)->batch_size, 
    //                 ((calvin_header*)req_buf)->epoch_id,
    //                 nocc::util::get_now(),
    //                 iteration);

    int chunk_cnt = 0;

#if ONE_SIDED_READ == 0

#if 0
    // send 
    // char* send_buf = rpc_->get_static_buf(max_package_size);
    // memcpy(send_buf, req_buf, sizeof(calvin_header));
    // char* cur = req_buf + sizeof(calvin_header);

    // rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
    // rpc_->broadcast_to(req_buf,
    //                    RPC_CALVIN_SCHEDULE,
    //                    req_buf_end - req_buf,
    //                    cor_id_,RRpc::REQ,mac_set_);

    rpc_->prepare_pending();
    char* cur = req_buf + sizeof(calvin_header);
    while (cur < req_buf_end) {
      char* send_buf = rpc_->get_fly_buf(cor_id_);
      memcpy(send_buf, req_buf, sizeof(calvin_header));
      uint size = (req_buf_end - cur < max_package_size - sizeof(calvin_header)) ? 
                                            req_buf_end - cur : (max_package_size - sizeof(calvin_header))/sizeof(calvin_request)*sizeof(calvin_request);
      ((calvin_header*)send_buf)->chunk_size = size/sizeof(calvin_request);
      assert(size % sizeof(calvin_request) == 0);
      memcpy(send_buf + sizeof(calvin_header), cur, size);
      // rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
      // rpc_->broadcast_to(send_buf,
      //                    RPC_CALVIN_SCHEDULE,
      //                    sizeof(calvin_header) + size,
      //                    cor_id_,RRpc::REQ,mac_set_);
      char* reply_buf = rpc_->get_reply_buf();
      rpc_->prepare_multi_req(reply_buf, mac_set_.size(), cor_id_);
      for (auto it = mac_set_.begin(); it != mac_set_.end(); ++it) {
        rpc_->append_pending_req(send_buf, RPC_CALVIN_SCHEDULE, sizeof(calvin_header) + size, cor_id_, RRpc::REQ, *it);
      }
      cur += size;
      // yield
      // indirect_yield(yield);
      // fprintf(stdout, "chunk %d posted.\n", chunk_cnt);
      chunk_cnt++;
    }
    rpc_->flush_pending();
    indirect_yield(yield);
#else

    // for (int i = 0; i < sizeof(calvin_header); i++)
    //   fprintf(stdout, "%x ", req_buf[i] & 0xff);
    // fprintf(stdout, "\n");

    memcpy(send_buf, req_buf, sizeof(calvin_header));
    
    // assert(0 == memcmp(req_buf, send_buf, sizeof(calvin_header)));

    // for (int i = 0; i < sizeof(calvin_header); i++)
    //   fprintf(stdout, "%x %x\n", send_buf[i] & 0xff, req_buf[i] & 0xff);
    // fprintf(stdout, "\n");

    char* cur = req_buf + sizeof(calvin_header);
    char reply_buf[64];
    while (cur < req_buf_end) {
      // char* send_buf = rpc_->get_fly_buf(cor_id_);
      uint size = (req_buf_end - cur < MAX_MSG_SIZE - sizeof(calvin_header)) ? 
                                            req_buf_end - cur : 
                                            (MAX_MSG_SIZE - sizeof(calvin_header))/sizeof(calvin_request)*sizeof(calvin_request);
      assert(size % sizeof(calvin_request) == 0);
      ((calvin_header*)send_buf)->chunk_size = size/sizeof(calvin_request);
      memcpy(send_buf + sizeof(calvin_header), cur, size);
      // char* reply_buf = rpc_->get_reply_buf();
      rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
      rpc_->broadcast_to(send_buf,
                         RPC_CALVIN_SCHEDULE,
                         sizeof(calvin_header) + size,
                         cor_id_,RRpc::REQ,mac_set_);
      cur += size;
      // yield
      indirect_yield(yield);
      // fprintf(stdout, "chunk %d posted.\n", chunk_cnt);
      chunk_cnt++;
    }

#endif
#else
    rtx::RDMAWriteReq req1(cor_id_, PA);
    // fprintf(stderr, "mac_set size = %d\n", mac_set_.size());
    for (auto mac : mac_set_) {
      assert(req_buf == req_buffers[cor_id_][cm_->get_nodeid()]);
      Qp *qp = get_qp(mac);
      assert(qp != NULL);
      uint64_t remote_off = offsets_[cor_id_][cm_->get_nodeid()];
      req1.set_write_meta_for<0>(remote_off + sizeof(calvin_header), 
                    req_buf + sizeof(calvin_header), req_buf_end-req_buf-sizeof(calvin_header));
      
      // calvin_header h;
      // h.batch_size = ((calvin_header*)req_buf)->batch_size;
      // h.received_size = h.batch_size;
      uint64_t* received_size = new uint64_t;
      *received_size = ((calvin_header*)req_buf)->batch_size;
      assert(*received_size != 0);
      // fprintf(stdout, "offsetof received_size: %d\n", OFFSETOF(calvin_header, received_size));
      req1.set_write_meta_for<1>(remote_off + OFFSETOF(calvin_header, received_size),
                                    (char*)received_size, sizeof(uint64_t));
                                    
      
      // LOG(3) << worker_id_ << " " << cor_id_ << " sending batch size " << *received_size 
                       // << " to offset " << remote_off + OFFSETOF(calvin_header, received_size)
                       // << " a.k.a remote address: " << (void*)(qp->remote_attr_.memory_attr_.buf + remote_off + OFFSETOF(calvin_header, received_size))
                       // << " for mac " << mac << " for iteration " << iteration;
      req1.post_reqs(rdma_sched_, qp);

      // avoid send queue from overflow
      if (unlikely(qp->rc_need_poll())) {
        indirect_yield(yield);
      }
    }
    indirect_yield(yield);

//check




    assert(req_buf == req_buffers[cor_id_][cm_->get_nodeid()]);
    chunk_cnt = ((calvin_header*)req_buf)->batch_size;
#endif

    // fprintf(stderr, "done sending epoch w/ chunk_cnt = %d at %lu for iteration %d.\n", 
    //                   chunk_cnt, nocc::util::get_now(), iteration);

    // construct calvin_request to local buffer,
    // which which be used by the scheduler later to merge
    // ptr = req_buf + sizeof(calvin_header);
    // for (uint64_t i = 0; i < batch_size_; i++) {
    //   assert(ptr != NULL);
    //   calvin_request* cr = (calvin_request*)malloc(sizeof(calvin_request));
    //   memcpy((char*)cr, ptr, sizeof(calvin_request));
    //   received_requests[cm_->get_nodeid()].push_back(cr);
    //   // fprintf(stdout, "calvin_request size = %u.\n", sizeof(calvin_request));
    //   ptr += sizeof(calvin_request);
    // }
    mach_received[cor_id_].insert(cm_->get_nodeid());
    ((calvin_header*)req_buf)->received_size = ((calvin_header*)req_buf)->batch_size;

#if ONE_SIDED_READ == 0
    check_schedule_done(cor_id_);
    while (epoch_done_schedule[cor_id_] == false) {
      yield_next(yield);
    }
#else
    check_schedule_done(cor_id_);
    while (epoch_done_schedule[cor_id_] == false) {
      yield_next(yield);
      check_schedule_done(cor_id_);
    }
    // while (true) {
    //   if (((calvin_header*)req_buffers[cor_id_][1-cm_->get_nodeid()])->received_size != 0) {
    //     epoch_done_schedule[cor_id_] = true;
    //     break;
    //   }
    //   yield_next(yield);
    // }
#endif

    assert(epoch_done_schedule[cor_id_]);
    // fprintf(stderr, "done receiving epoch for iteration %d.\n", iteration);

    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      calvin_header* h = (calvin_header*)req_buffers[cor_id_][i];
      char* ptr = req_buffers[cor_id_][i] + sizeof(calvin_header);
      // fprintf(stdout, "%d's calvin_header. received_size = %d", i, h->received_size);
      for (int j = 0; j < h->received_size; j++) {
        ASSERT(((calvin_request*)ptr)->req_idx < workload.size()) << ((calvin_request*)ptr)->req_idx;
        deterministic_requests[cor_id_].push_back((calvin_request*)ptr);
        ptr += sizeof(calvin_request);
      }
    }
    std::sort(deterministic_requests[cor_id_].begin(), deterministic_requests[cor_id_].end(), calvin_request_compare());
    // fprintf(stderr, "done scheduling. det size = %u for iteration %d.\n", 
    //                   deterministic_requests[cor_id_].size(), iteration);

    mach_received[cor_id_].clear();
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      calvin_header* h = (calvin_header*)req_buffers[cor_id_][i];
      h->received_size = 0;
    }

    // for (int i = 0; i < deterministic_requests[cor_id_].size(); i++) {
        // auto req = deterministic_requests[cor_id_][i];
        // fprintf(stderr, "%d %d %d %lu\n", worker_id_, cor_id_, req->req_idx, req->timestamp);
    // }

    uint64_t retry_count(0);
    // serve as a deterministic transaction executor
    for (int i = 0; i < deterministic_requests[cor_id_].size(); i++) {
        auto req = deterministic_requests[cor_id_][i];
        int tx_idx = req->req_idx;
        req->req_seq = i;

        ASSERT(tx_idx < workload.size()) << "in execution seq = " << i << ":  workload " << tx_idx << " does not exist.";
        (*txn_counts)[tx_idx] += 1;
    abort_retry:
        if (req->req_initiator == cm_->get_nodeid())
          ntxn_executed_ += 1;
        // fprintf(stdout, "executing %d %d %lu\n", i, req.req_idx, req.timestamp);
        // auto ret = workload[tx_idx].fn(this, req, yield);
        // usleep(5);
        auto ret = txn_result_t(true, 73);
    #if NO_ABORT == 1
        //ret.first = true;
    #endif
        // if(current_partition == 0){
        if(likely(ret.first)) {
          // commit case
          retry_count = 0;
          if (req->req_initiator == cm_->get_nodeid())
            ntxn_commits_ += 1;
          // self_generated requests
          assert(CS == 0);
        } else {
          retry_count += 1;
    #if DEBUG_RETRY_TXN
          fprintf(stdout, "%d: retry transaction.\n", cor_id_);
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

// #if ONE_SIDED_READ == 0
//     epoch_status_[cor_id_][cm_->get_nodeid()] = CALVIN_EPOCH_DONE;
//     // broadcast my epoch status to other machines
//     // char* send_buf = rpc_->get_static_buf(cor_id_);
//     *(uint8_t*)send_buf = CALVIN_EPOCH_DONE;
//     // char* reply_buf = rpc_->get_reply_buf();
//     rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
//     rpc_->broadcast_to(send_buf,
//                        RPC_CALVIN_EPOCH_STATUS,
//                        sizeof(uint8_t),
//                        cor_id_,RRpc::REQ,mac_set_);
//     indirect_yield(yield);
// #else
//     ((calvin_header*)req_buffers[cor_id_][cm_->get_nodeid()])->epoch_status = CALVIN_EPOCH_DONE;
//     rtx::RDMAWriteOnlyReq req2(cor_id_, PA);
//     for (auto mac : mac_set_) {
//       Qp *qp = get_qp(mac);
//       assert(qp != NULL);
//       uint64_t remote_off = offsets_[cor_id_][cm_->get_nodeid()];
      
//       // h.batch_size = ((calvin_header*)req_buf)->batch_size;
//       // h.received_size = h.batch_size;
//       uint8_t* epoch_status = new uint8_t;
//       *epoch_status = CALVIN_EPOCH_DONE;
//       // fprintf(stdout, "offsetof epoch_status: %d\n", OFFSETOF(calvin_header, epoch_status));
//       req2.set_write_meta(remote_off + OFFSETOF(calvin_header, epoch_status), 
//                                   (char*)epoch_status, sizeof(uint8_t));
//       req2.post_reqs(rdma_sched_, qp);

//       // avoid send queue from overflow
//       if (unlikely(qp->rc_need_poll())) {
//         indirect_yield(yield);
//       }
//     }
//     indirect_yield(yield);
// #endif

//     while (!check_epoch_done()) {
//       yield_next(yield);
//     }

    // fprintf(stderr, "%d %d ending for iteration %d.\n", worker_id_, cor_id_, iteration);
  }

  // rpc_->free_static_buf(send_buf);
  // free(req_buf);
  //this yield must be there to allow current finished coroutine
  //not to block the scheduling of following coroutines in the
  //coroutine schedule list, a.k.a, the the routineMeta list.
  indirect_must_yield(yield);
  fprintf(stdout, "%d: ends.\n", cor_id_);  
}
#endif

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

#ifdef CALVIN_TX
void BenchWorker::calvin_schedule_rpc_handler(int id,int cid,char *msg,void *arg) {
  // static uint recv_cnt = 0;
  calvin_header* ch = (calvin_header*)msg;
  // fprintf(stderr, "received epoch: %lu. counter = %u\n", ch->epoch_id, 
  //                                                        recv_cnt++);
  // for (int i = 0; i < sizeof(calvin_header); i++)
  //   fprintf(stdout, "%x ", msg[i] & 0xff);
  // fprintf(stdout, "\n");

  uint8_t remote = ch->node_id;
  assert(remote == id);
  assert(remote != cm_->get_nodeid());

  calvin_header* h = (calvin_header*)req_buffers[cid][id];
  h->batch_size = ch->batch_size;

  calvin_request* copy_dest = (calvin_request*)(req_buffers[cid][id] + sizeof(calvin_header));
  char* ptr = msg + sizeof(calvin_header);
  for (uint64_t i = 0; i < ch->chunk_size; i++) {
    // calvin_request* cr = (calvin_request*)malloc(sizeof(calvin_request));
    memcpy(&copy_dest[h->received_size + i], ptr, sizeof(calvin_request));
    // received_requests[remote].push_back(cr);
    ptr += sizeof(calvin_request);
  }

  mach_received[cid].insert(remote);
  h->received_size += ch->chunk_size;

  check_schedule_done(cid);

  // fprintf(stdout, "received sequence from %d, %d, %d. batch size = %u, chunk size = %d\n", id, worker_id_, cid, ch->batch_size, ch->chunk_size);


  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}

#if ONE_SIDED_READ == 0
void BenchWorker::check_schedule_done(int cid) {
  bool all_received = false;
  if (mach_received[cid].size() == cm_->get_num_nodes()) {
    int i = 0;
    for (; i < cm_->get_num_nodes(); i++) {
      assert (mach_received[cid].find(i) != mach_received[cid].end());
      calvin_header* h = (calvin_header*)req_buffers[cid][i];
      if (h->received_size < h->batch_size)
        break;
    }
    if (i == cm_->get_num_nodes()) 
      all_received = true;
  }

  if (all_received) {
    // fprintf(stdout, "all sequences received.\n");
    epoch_done_schedule[cid] = true;
  }
}

#else
void BenchWorker::check_schedule_done(int cid) {

  int i = 0;
  for (; i < cm_->get_num_nodes(); i++) {
    calvin_header* h = (calvin_header*)req_buffers[cid][i];
    volatile uint64_t recv = h->received_size;
    // uint64_t batch = h->batch_size;
    // LOG(3) << "in check: " << recv << " out of " << batch << " received.";
    // if (recv == 0 || recv < batch)
    // if (i != cm_->get_nodeid()) {
      // LOG(3) << "in check address: " << (void*)&(h->received_size) << " val = " << recv;
      // calvin_request* cr = (calvin_request*)((char*)req_buffers[cid][i] + sizeof(calvin_header));
      // LOG(3) << "remote timestamp = " << cr->timestamp;
    // }
    // asm volatile("" ::: "memory");
    if (recv == 0)
      break;
  }
  if (i == cm_->get_num_nodes()) { // all received
    // for (int j = 0; j < cm_->get_num_nodes(); j++) {
    //   calvin_header* h = (calvin_header*)req_buffers[cid][j];
    //   LOG(3) << cid  << " in check: " << h->received_size << " received from machine " << j << ".";
    // }
    assert(epoch_done_schedule[cid] == false);
    epoch_done_schedule[cid] = true;
  } else {
    assert(epoch_done_schedule[cid] == false);
  }
}
#endif

void BenchWorker::calvin_epoch_status_rpc_handler(int id,int cid,char *msg,void *arg) {
  assert(id != cm_->get_nodeid());
  // assert(epoch_status_.find(cid) != epoch_status_.end());
  assert(*(uint8_t*)msg == CALVIN_EPOCH_DONE);
#if ONE_SIDED_READ == 0
  epoch_status_[cid][id] = *(uint8_t*)msg;
#else
  assert(false);
#endif
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid);
}

bool BenchWorker::check_epoch_done() {
  for (int i = 0; i < cm_->get_num_nodes(); i++) {
#if ONE_SIDED_READ == 0
      if (epoch_status_[cor_id_][i] != CALVIN_EPOCH_DONE)
#else
      volatile uint8_t es = ((calvin_header*)req_buffers[cor_id_][i])->epoch_status;
      if (es != CALVIN_EPOCH_DONE)
#endif
        return false;
  }
  return true;
}

#endif

}; // oltp

}; // nocc
