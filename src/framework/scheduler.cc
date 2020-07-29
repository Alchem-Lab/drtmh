#include "rocc_config.h"
#include "tx_config.h"
#include "config.h"

#include "bench_worker.h"
#include "scheduler.h"
#include "sequencer.h"
#include "ralloc.h"
#include "core/utils/latency_profier.h"

#include "ring_msg.h"
#include "ud_msg.h"

#include <unistd.h>

/* global config constants */
extern size_t coroutine_num;
extern size_t nthreads;
extern size_t nclients;
extern size_t scale_factor;
extern size_t current_partition;
extern size_t total_partition;
extern int tcp_port;

using namespace rdmaio::ringmsg;

namespace nocc {

extern __thread MappedLog local_log;
extern oltp::Sequencer* sequencer;
extern std::vector<SingleQueue *>   local_comm_queues;
extern zmq::context_t send_context;

namespace oltp {

#ifdef CALVIN_TX
extern char * calvin_request_buffer;
#endif

Scheduler::Scheduler(unsigned worker_id, RdmaCtrl *cm, MemDB * db):
  RWorker(worker_id,cm), db_(db) {
  for (int i = 0; i < nthreads; i++) {
    // locks_4_locked_transactions.push_back(new SpinLock());
    std::vector<boost::lockfree::queue<det_request*>* >* qv = new std::vector<boost::lockfree::queue<det_request*>* >();
    for (int j = 0; j < coroutine_num + 1; j++) {
      qv->push_back(new boost::lockfree::queue<det_request*>(0));
    }
    locked_transactions.push_back(*qv);
  }
}

Scheduler::~Scheduler() {}

void Scheduler::run() {
  BindToCore(worker_id_);
  //binding(worker_id_);
  init_routines(1);

#if 1
  create_logger();
  fprintf(stderr, "%d: next_log_entry: local_log=%p\n", worker_id_, &local_log);
  char *log_buf = next_log_entry(&local_log,64);
  assert(log_buf != NULL);
  sprintf(log_buf,"scheduler runs @thread=%d\n", worker_id_);
#endif

#if USE_RDMA
  printf("%d: scheduler in init rdma.\n", worker_id_);
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
  int total_connections = 1;

  create_rdma_ud_connections(total_connections);
#else
  create_rdma_rc_connections(rdma_buffer + HUGE_PAGE_SZ,
                             total_ring_sz,ring_padding);
#endif

#endif

  // this->init_new_logger(backup_stores_);
  this->thread_local_init();   // application specific init

  // waiting for master to start workers
  this->inited = true;
#if 1
  while(!this->running) {
    asm volatile("" ::: "memory");
  }
#else
  this->running = true;
#endif

  start_routine();
  return;
}

void Scheduler::worker_routine(yield_func_t &yield) {

  LOG(3) << worker_id_ << ": Running Scheduler routine";

  int iteration = 0;

// #if MOCK_SEQUENCER == 1
//     for (int i = 0; i < cm_->get_num_nodes(); i++) {
//       for (int j = 1; j < coroutine_num+1; j++) {
// #if ONE_SIDED_READ == 0
//           chars* req_buf = req_buffers[i][j];
//           req_buffer_state[i][j] == Scheduler::BUFFER_RECVED;
// #elif ONE_SIDED_READ == 1
//           char* req_buf = req_buffers[j][i];
//           calvin_header* h = (calvin_header*)req_buf;
//           h->received_size = sizeof(det_request)*MAX_REQUESTS_NUM;
// #endif
//           ((calvin_header*)req_buf)->node_id = i;
//           ((calvin_header*)req_buf)->epoch_id = iteration;
//           sequencer->generate_requests(req_buf, yield);
//        }
//     }
// #endif

  while (true)
  {
    deterministic_plan.clear();
    req_fullfilled = 0;

#if MOCK_SEQUENCER == 1
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      for (int j = 1; j < coroutine_num+1; j++) {
#if ONE_SIDED_READ == 0
          chars* req_buf = req_buffers[i][j];
          req_buffer_state[i][j] == Scheduler::BUFFER_RECVED;
#elif ONE_SIDED_READ == 1
          char* req_buf = req_buffers[j][i];
          calvin_header* h = (calvin_header*)req_buf;
          h->received_size = sizeof(det_request)*MAX_REQUESTS_NUM;
#endif
          ((calvin_header*)req_buf)->node_id = i;
          ((calvin_header*)req_buf)->epoch_id = iteration;
          sequencer->generate_requests(req_buf, yield);
      }
    }

    iteration += 1;
#else // !MOCK_SEQUENCER
    while (true) {
      int n_ready = 0;
      for (int i = 0; i < cm_->get_num_nodes(); i++) {
        for (int j = 1; j < coroutine_num+1; j++) {
#if ONE_SIDED_READ == 0
          if (req_buffer_state[i][j] == Scheduler::BUFFER_RECVED)
            n_ready += 1;
#elif ONE_SIDED_READ == 1
          calvin_header* h = (calvin_header*)req_buffers[j][i];
          if (h->received_size == sizeof(det_request)*MAX_REQUESTS_NUM) {
            n_ready += 1;
          }
#else
#endif
        }
      }
       
      if (n_ready == cm_->get_num_nodes() * coroutine_num)
        break;
      cpu_relax();
      yield_next(yield);
    }
#endif // MOCK_SEQUENCER

    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      for (int j = 1; j < coroutine_num+1; j++) {
        if (true) {
#if ONE_SIDED_READ == 0
          char* buf_end = req_buffers[i][j];
#elif ONE_SIDED_READ == 1
          char* buf_end = req_buffers[j][i];
#else
          char* buf_end = NULL;
          assert(false);
#endif
          calvin_header* h = (calvin_header*)buf_end;
          buf_end += sizeof(calvin_header);
 
          for (int k = 0; k < MAX_REQUESTS_NUM; k++) {
            deterministic_plan.push_back(*(det_request*)buf_end);
            deterministic_plan.back().req_seq = deterministic_plan.size()-1;
            buf_end += sizeof(det_request);
          }
  #if DEBUG_LEVEL==1
          fprintf(stderr, "%d: deterministic_plan installs requests from %d:%d for epoch %d\n", worker_id_, i, j, h->epoch_id);
  #endif

  #if ONE_SIDED_READ == 0
          req_buffer_state[i][j] = Scheduler::BUFFER_INIT;
  #elif ONE_SIDED_READ == 1
          h->received_size = 0;
  #else
  #endif
        } else {
          assert(false);
        }
      }
    }

    // for debug
    // // printf("plan:\n");
    // for (det_request& c : deterministic_plan) {
    //   printf("%d: req_idx: %d, req_seq: %d, ts: %d, reads %d, writes %d.\n", 
    //                                         c.req_initiator, 
    //                                         c.req_idx, c.req_seq, c.timestamp, 
    //                                        ((rwsets_t*)c.req_info)->nReads,
    //                                        ((rwsets_t*)c.req_info)->nWrites);
    // }

#if CALVIN_TX
    for(int i = 0; i < deterministic_plan.size(); i++) {
        // for mocking
        // {
        //   // put transaction into threads to execute in a round-robin manner.
        //   det_request& req = deterministic_plan[i];
        //   int tid = req.req_seq % nthreads;
        //   int cid = req.req_seq % coroutine_num + 1; // note that the coroutine 0 is the message handler
        //   locks_4_locked_transactions[tid]->Lock();
        //   // fprintf(stderr, "enqueue to thread%d, coroutine%d\n", tid, cid);
        //   locked_transactions[tid][cid]->push(req);
        //   locks_4_locked_transactions[tid]->Unlock();
        //   continue;
        // }

        // request locks for each transaction
        det_request& req = deterministic_plan[i];
        request_lock(req.req_seq, yield);

        // fprintf(stderr, "request %d with ts: %d locked.\n", req.req_seq, req.timestamp);
        // const rwsets_t* set = (rwsets_t*)req.req_info;
        // fprintf(stderr, "reads=%d writes=%d\n", set->nReads, set->nWrites);
        
        // put transaction into threads to execute in a round-robin manner.
        int tid = req.req_seq % nthreads;
        int cid = req.req_seq % coroutine_num + 1; // note that the coroutine 0 is the message handler
        // locks_4_locked_transactions[tid]->Lock();
        // fprintf(stderr, "enqueue to thread%d, coroutine%d\n", tid, cid);
        while(!locked_transactions[tid][cid]->push(&req)) {};
        // locks_4_locked_transactions[tid]->Unlock(); 
        // sleep(10000);
    }

    // wait until all transactions in the deterministic plan is finished and all locks
    // held by them are unlocked.
    while (req_fullfilled < deterministic_plan.size()) {
        cpu_relax();
        yield_next(yield);
        asm volatile("" ::: "memory");        
    }

#if MOCK_SEQUENCER
#else 
    epoch_done = true;
    while (epoch_done) {
      cpu_relax();
      yield_next(yield);
      asm volatile("" ::: "memory");
    }
#endif

    // fprintf(stderr, "scheduler epoch done.\n");
#elif defined(BOHM_TX)

      // Instead of locking, the scheduler will looks into each transaction,
      // and assign the records of each transaction to some concurrency control thread
      // determined deterministically by the index of the record.
      // The concurrency control thread then inserts a new version placeholders for each 
      // item in the write set.

      det_batch_ready += 1;
#endif

      // sleep(1);

    yield_next(yield);
  }
}

void Scheduler::exit_handler() {

}

void Scheduler::thread_local_init() {
#if ONE_SIDED_READ == 0
  req_buffers = (char***)malloc(sizeof(char**)*cm_->get_num_nodes());
  for (int i = 0; i < cm_->get_num_nodes(); i++)
    req_buffers[i] = (char**)malloc(sizeof(char*)*(coroutine_num+1));

  req_buffer_state = (volatile int**)malloc(sizeof(volatile int*)*cm_->get_num_nodes());
  for (int i = 0; i < cm_->get_num_nodes(); i++)
    req_buffer_state[i] = (volatile int*)malloc(sizeof(volatile int)*(coroutine_num+1));

  for (int i = 0; i < cm_->get_num_nodes(); i++) {
    for (int j = 0; j < coroutine_num+1; j++) {    
      req_buffers[i][j] = (char*)malloc(sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request));
      req_buffer_state[i][j] = BUFFER_INIT;
    }
  }

#elif ONE_SIDED_READ == 1

  const char *start_ptr = (char *)(cm_->conn_buf_);
  LOG(3) << "start_ptr = " << (void*)start_ptr << " addrs:";
  /*set up req_buffer and req offsets*/
  int buf_len = sizeof(calvin_header) + MAX_CALVIN_REQ_CNTS*sizeof(det_request);
  char* req_base_ptr_ = oltp::calvin_request_buffer;

  // uint64_t req_base_offset_ = req_base_ptr_ - start_ptr;
  req_buffers = new char**[1+coroutine_num];
  for (int i = 0; i < coroutine_num+1; i++) {
    req_buffers[i] = new char*[cm_->get_num_nodes()];
    LOG(3) << "routine " << i;
    for (int j = 0; j < cm_->get_num_nodes(); j++) {
      char* buf = req_base_ptr_ + buf_len * i * cm_->get_num_nodes() + buf_len * j;
      req_buffers[i][j] = buf;
      calvin_header* ch = (calvin_header*)req_buffers[i][j];
      ch->epoch_status = CALVIN_EPOCH_READY;
      LOG(3) << "node " << j << "'s addrs: " << static_cast<void*>(req_buffers[i][j]);
    }
  }

  LOG(3) << "offsets:";
  offsets_ = new uint64_t*[1 + coroutine_num];
  for (int i = 0; i < coroutine_num+1; i++) {
    offsets_[i] = new uint64_t[cm_->get_num_nodes()];
    LOG(3) << "routine " << i;
    for (int j = 0; j < cm_->get_num_nodes(); j++) {
      offsets_[i][j] = req_buffers[i][j] - start_ptr;
        LOG(3) << "node " << j << "'s offset: " << offsets_[i][j];
    }
  }

#else
  assert(false);
#endif

}

void Scheduler::request_lock(int req_seq, yield_func_t &yield) {
  using namespace nocc::rtx::rwlock;
  START(lock);

  det_request& req = deterministic_plan[req_seq];
  uint8_t nReads = ((rwsets_t*)req.req_info)->nReads;
  uint8_t nWrites = ((rwsets_t*)req.req_info)->nWrites;

  for (auto i = 0; i < nReads + nWrites; i++) {
    ReadSetItem& item = ((rwsets_t*)req.req_info)->access[i];
    auto it = &item;
    if (it->pid != cm_->get_nodeid()) { // skip remote read
      // fprintf(stderr, "remote-skipped table=%d key=%d for req %d\n", it->tableid, it->key, req_seq);      
      continue;
    }
    if (it->node == NULL) {
      MemNode *node = db_->stores_[it->tableid]->Get(it->key);
      assert(node != NULL);
      assert(node->value != NULL);
      it->node = node;
    }

    // fprintf(stderr, "requesting lock for req %d: table %d, key %d\n", req_seq, it->tableid, it->key);
    // fprintf(stderr, "locking write. key = %d\n", it->key);
    std::vector<uint64_t*> locked;
    while (true) {
      volatile uint64_t *lockptr = &(it->node->lock);
      volatile uint64_t l = *lockptr;
      // already locked by me previously
      if (l == LOCKED(req_seq)) 
        break;
      
      // locked by other txn
      if (l & 1 == 1) {
          cpu_relax();
          yield_next(yield);
          continue;
      }

      if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                   LOCKED(req_seq)))) {
        cpu_relax();
        yield_next(yield);
        continue;
      } else {
        // fprintf(stderr, "locked table=%d key=%d for req %d\n", it->tableid, it->key, req_seq);
        break; // lock the next local read
      }
    }
  }

  END(lock);
  
  return;
}

} // namespace oltp
} // namespace nocc
