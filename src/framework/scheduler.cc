#include "bench_worker.h"
#include "scheduler.h"
#include "ralloc.h"
#include "core/utils/latency_profier.h"

#include "ring_msg.h"
#include "ud_msg.h"

/* global config constants */
extern size_t nthreads;
extern size_t scale_factor;
extern size_t current_partition;
extern size_t total_partition;

using namespace rdmaio::ringmsg;

namespace nocc {

namespace oltp {

#define SEQUENCER_THREAD_ID 100
#define SCHEDULER_THREAD_ID 101


SingleQueue* queue; // use to buffer request batch from local Sequencer
std::vector<SingleQueue*> locked_transactions;

Scheduler::Scheduler(unsigned worker_id):
  RWorker(worker_id,cm) {

  // assert that each backup thread can clean at least one remote thread's log
  assert(worker_id_ < nthreads);
}

Scheduler::~Scheduler() {

}

#define RPC_DET_SEQUENCE 28

void Scheduler::run() {
  BindToCore(worker_id_);

#if USE_RDMA
  init_rdma();
#endif
  init_routines(1);

  ROCC_BIND_STUB(rpc_, &Scheduler::sequencer_rpc_handler, this, RPC_DET_SEQUENCE);

  thread_local_init();

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

#define MAX_REQUESTS_NUM 10

void Scheduler::worker_routine(yield_func_t &yield) {

  while (true) {
    deterministic_plan.clear();
    
    int i = 0;
    for (; i < cm_->get_num_nodes(); i++) {
      if (i != cm_->get_nodeid()) {
        if (req_buffers[i].empty()) {
          indirect_yield(yield);
          i--;
          continue;
        }

        char* buf_end = req_buffers[i][0];
        req_buffers[i].erase(req_buffers[i].begin());
        for (int i = 0; i < MAX_REQUESTS_NUM; i++) {
          deterministic_plan.push_back(*(det_request*)buf_end);
          deterministic_plan.back().req_seq = deterministic_plan.size()-1;
          buf_end += sizeof(det_request);
        }
      } else {
        char* buf_end = req_buffers[i][0];
        if (!queue->front(buf_end)) {
          indirect_yield(yield);
          i--;
          continue;
        }

        for (int j = 0; j < MAX_REQUESTS_NUM; j++) {
          deterministic_plan.push_back(*(det_request*)buf_end);
          deterministic_plan.back().req_seq = deterministic_plan.size()-1;
          buf_end += sizeof(det_request);
        }
      }
    }

    // for debug
    printf("plan:\n");
    for (det_request c : deterministic_plan) {
      printf("%d\n", c.req_initiator);
    }

#ifdef CALVIN_TX
    for(i = 0; !waitinglist.empty() || i < deterministic_plan.size(); i++) {

        // check waiting list first
        auto waiter = waitinglist.begin(); 
        while(waiter != waitinglist.end()) {
          assert(!waiter->second.empty());
          int req_seq = waiter->second.front();
          det_request& req = deterministic_plan[req_seq];
          if (request_lock(req.req_seq)) {
            waiter->second.pop();
            if (waiter->second.empty())
              waiter = waitinglist.erase(waiter);
            else
              ++waiter;
            // put transaction into threads to execute in a round-robin manner.
            int tid = req_seq % nthreads;
            locked_transactions[tid]->enqueue((char *)(&req),sizeof(det_request));            
          } else
            ++waiter;
        }
  
        if (i >= deterministic_plan.size())
          break;

        // request locks for each transaction
        det_request& req = deterministic_plan[i];
        if (!request_lock(req.req_seq)) {
          indirect_yield(yield);
          continue;
        }

        // put transaction into threads to execute in a round-robin manner.
        int tid = i % nthreads;
        locked_transactions[tid]->enqueue((char *)(&req),sizeof(det_request));
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

void Scheduler::sequencer_rpc_handler(int id,int cid,char *msg,void *arg) {
  printf("scheduler received from machine %d.\n", id);
  assert(id < req_buffers.size() && id != cm_->get_nodeid());
  char* buf = (char*)malloc(MAX_REQUESTS_NUM*sizeof(det_request));
  memcpy(buf, msg, MAX_REQUESTS_NUM*sizeof(det_request));
  req_buffers[id].push_back(buf);
}

void Scheduler::thread_local_init() {
  for (int i = 0; i < cm_->get_num_nodes(); i++) {
      req_buffers.push_back(std::vector<char*>());
  }

  char* buf = (char*)malloc(MAX_REQUESTS_NUM*sizeof(det_request));
  req_buffers[cm_->get_nodeid()].push_back(buf);
  queue = new SingleQueue();

  if(locked_transactions.size() == 0) {
    // only create once
    for(uint i = 0;i < nthreads; ++i)
      locked_transactions.push_back(new SingleQueue());
  }
}

bool Scheduler::request_lock(int req_seq) {
  using namespace nocc::rtx::rwlock;
  START(lock);

  det_request& req = deterministic_plan[req_seq];
  uint8_t nReads = ((rwsets_t*)req.req_info)->nReads;
  uint8_t nWrites = ((rwsets_t*)req.req_info)->nWrites;

  for (auto i = 0; i < nReads + nWrites; i++) {
    ReadSetItem& item = ((rwsets_t*)req.req_info)->access[i];
    auto it = &item;
    if (it->pid != cm_->get_nodeid())  // skip remote read
      continue;

    // fprintf(stderr, "locking write. key = %d\n", it->key);
    assert (it->node != NULL);
    while (true) {
      volatile uint64_t* l = &it->node->lock;
      if (*l == LOCKED(cm_->get_nodeid()))
        break;
      if ((*l & 0x1) == W_LOCKED) {
        if (waitinglist.find(l) == waitinglist.end())
          waitinglist[l] = std::move(std::queue<int>());
        waitinglist[l].push(req_seq);
        return false;
      } else {
          volatile uint64_t *lockptr = &(it->node->lock);
          if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                       LOCKED(cm_->get_nodeid())))) {
            continue;
          } else {
            // fprintf(stderr, "locked read %d %d\n", it->tableid, it->key);
            break; // lock the next local read
          }
      }
    }
  }

  END(lock);
  return true;
}

} // namespace oltp
} // namespace nocc
