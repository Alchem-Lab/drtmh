#include "rocc_config.h"
#include "tx_config.h"
#include "config.h"

#include "bench_worker.h"
#include "scheduler.h"
#include "ralloc.h"
#include "core/utils/latency_profier.h"

#include "ring_msg.h"
#include "ud_msg.h"

/* global config constants */
extern size_t coroutine_num;
extern size_t nthreads;
extern size_t nclients;
extern size_t scale_factor;
extern size_t current_partition;
extern size_t total_partition;

using namespace rdmaio::ringmsg;

namespace nocc {

namespace oltp {

Scheduler::Scheduler(unsigned worker_id, RdmaCtrl *cm, MemDB * db):
  RWorker(worker_id,cm), db_(db) {
  for (int i = 0; i < nthreads; i++) {
    locks_4_locked_transactions.push_back(new SpinLock());
    std::vector<std::queue<det_request>* >* qv = new std::vector<std::queue<det_request>* >();
    for (int j = 0; j < coroutine_num + 1; j++) {
      qv->push_back(new std::queue<det_request>());
    }
    locked_transactions.push_back(*qv);
  }
}

Scheduler::~Scheduler() {}

void Scheduler::run() {
  BindToCore(worker_id_);
  //binding(worker_id_);

#if USE_RDMA
  printf("scheduler in init rdma; id = %d\n", worker_id_);
  init_rdma();
  create_qps();
#endif

  init_routines(1);

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

  LOG(3) << "Running Scheduler routine.on worker " << worker_id_;

  while (true)
  {
    deterministic_plan.clear();
    
    int i = 0;
    for (; i < cm_->get_num_nodes(); i++) {
      if (true) {
        sequence_lock.Lock();
        if (req_buffers[i].empty()) {
          sequence_lock.Unlock();
          cpu_relax();
          i--;
          continue;
        }

        char* buf_end = req_buffers[i][0];
        calvin_header* h = (calvin_header*)buf_end;
        if (h->chunk_id < h->nchunks-1) {
          sequence_lock.Unlock();
          cpu_relax();
          i--;
          continue;
        }

        req_buffers[i].erase(req_buffers[i].begin());
        sequence_lock.Unlock();
        cpu_relax();

        buf_end += sizeof(calvin_header);
        for (int j = 0; j < MAX_REQUESTS_NUM; j++) {
          deterministic_plan.push_back(*(det_request*)buf_end);
          deterministic_plan.back().req_seq = deterministic_plan.size()-1;
          buf_end += sizeof(det_request);
        }

        free(buf_end);
      } else {
        // for the case when buffer is fullfilled by the sequencer on my machine.
        // char* buf_end = req_buffers[i][0];
        // if (!queue->front(buf_end)) {
        //   indirect_yield(yield);
        //   i--;
        //   continue;
        // }

        // for (int j = 0; j < MAX_REQUESTS_NUM; j++) {
        //   deterministic_plan.push_back(*(det_request*)buf_end);
        //   deterministic_plan.back().req_seq = deterministic_plan.size()-1;
        //   buf_end += sizeof(det_request);
        // }
      }
    }

    // for debug
    printf("plan:\n");
    for (det_request c : deterministic_plan) {
      printf("%d\n", c.req_initiator);
    }

#if CALVIN_TX
    std::set<int> out_of_waitinglist;
    for(i = 0; !waitinglist.empty() || i < deterministic_plan.size(); i++) {

        // check waiting list first
        auto waiter = waitinglist.begin(); 
        while(waiter != waitinglist.end()) {
          assert(!waiter->second.empty());
          int req_seq = waiter->second.front();
          det_request& req = deterministic_plan[req_seq];

          if (out_of_waitinglist.find(req.req_seq) != out_of_waitinglist.end()) {
            waiter->second.pop();
            if (waiter->second.empty())
              waiter = waitinglist.erase(waiter);
            else
              ++waiter;
              continue;  
          }

          if (request_lock(req.req_seq, true)) {
            waiter->second.pop();
            if (waiter->second.empty())
              waiter = waitinglist.erase(waiter);
            else
              ++waiter;

            out_of_waitinglist.insert(req.req_seq);
            fprintf(stderr, "request %d locked.\n", req.req_seq);
            // put transaction into threads to execute in a round-robin manner.
            int tid = req_seq % nthreads;
            int cid = req_seq % coroutine_num + 1;
            locks_4_locked_transactions[tid]->Lock();
            locked_transactions[tid][cid]->push(req);
            locks_4_locked_transactions[tid]->Unlock();
            sleep(10000);
          } else
            ++waiter;
        }
  
        if (i >= deterministic_plan.size())
          break;

        // request locks for each transaction
        det_request& req = deterministic_plan[i];
        if (!request_lock(req.req_seq, false)) {
          // indirect_yield(yield);
          cpu_relax();
          continue;
        }

        fprintf(stderr, "request %d locked.\n", req.req_seq);
        // put transaction into threads to execute in a round-robin manner.
        int tid = i % nthreads;
        int cid = i % coroutine_num + 1; // note that the coroutine 0 is the message handler
        locks_4_locked_transactions[tid]->Lock();
        fprintf(stderr, "enqueue to thread%d, coroutine%d\n", tid, cid);
        locked_transactions[tid][cid]->push(req);
        locks_4_locked_transactions[tid]->Unlock(); 
        sleep(10000);
      }

#elif defined(BOHM_TX)

      // Instead of locking, the scheduler will looks into each transaction,
      // and assign the records of each transaction to some concurrency control thread
      // determined deterministically by the index of the record.
      // The concurrency control thread then inserts a new version placeholders for each 
      // item in the write set.

      det_batch_ready += 1;
#endif
  }
}

void Scheduler::exit_handler() {

}

void Scheduler::thread_local_init() {
  for (int i = 0; i < cm_->get_num_nodes(); i++) {
      req_buffers.push_back(std::vector<char*>());
  }
}

bool Scheduler::request_lock(int req_seq, bool from_waitinglist) {
  using namespace nocc::rtx::rwlock;
  START(lock);

  det_request& req = deterministic_plan[req_seq];
  uint8_t nReads = ((rwsets_t*)req.req_info)->nReads;
  uint8_t nWrites = ((rwsets_t*)req.req_info)->nWrites;

  int success = 1;
  for (auto i = 0; i < nReads + nWrites; i++) {
    ReadSetItem& item = ((rwsets_t*)req.req_info)->access[i];
    auto it = &item;
    if (it->pid != cm_->get_nodeid())  // skip remote read
      continue;

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
        if (from_waitinglist)
          return false;

        if (waitinglist.find(lockptr) == waitinglist.end())
          waitinglist[lockptr] = std::move(std::queue<int>());
        waitinglist[lockptr].push(req_seq);
        // requesting other read/write, but remember to mark this function as failure.
        success &= 0;
        break;
      }

      if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                   LOCKED(req_seq)))) {
        continue;
      } else {
        // fprintf(stderr, "locked read %d %d\n", it->tableid, it->key);
        break; // lock the next local read
      }
    }
  }

  END(lock);
  
  return (success == 1) ? true : false;
}

// void Scheduler::check_to_notify(int worker_id_, std::vector<SingleQueue*> ready_reqs) {
//     nocc::rtx::det_request req;
//     if (locks_4_locked_transactions[worker_id_].Trylock()) {
//       if(!locked_transactions.empty() && locked_transactions[worker_id_]->front((char*)&req)) {
//         ready_reqs[req.req_seq % ready_reqs.size()]->enqueue((char*)&req, sizeof(nocc::rtx::det_request));
//         locked_transactions[worker_id_]->pop();
//       }
//       locks_4_locked_transactions[worker_id_].UnLock();
//     }
// }

} // namespace oltp
} // namespace nocc
