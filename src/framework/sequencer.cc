#include "sequencer.h"
#include "ralloc.h"

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

Sequencer::Sequencer(unsigned worker_id, unsigned seed, get_workload_func_t get_wl_func):
  RWorker(worker_id,cm,seed){

  // assert that each backup thread can clean at least one remote thread's log
  assert(worker_id_ < nthreads);
  get_workload_func = get_wl_func;
}

Sequencer::~Sequencer() {

}

#define RPC_DET_BACKUP 29
#define RPC_DET_SEQUENCE 28

void Sequencer::run() {
  BindToCore(worker_id_);

#if USE_RDMA
  init_rdma();
#endif
  init_routines(1);

  ROCC_BIND_STUB(rpc_, &Sequencer::logging_rpc_handler, this, RPC_DET_BACKUP);
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

void Sequencer::worker_routine(yield_func_t &yield) {

  static const auto& workload = get_workload_func();

  std::set<int> mac_set_;
  for (int i = 0; i < cm_->get_num_nodes(); i++) {
    if (i != cm_->get_nodeid()) {
      mac_set_.insert(i);
      // fprintf(stdout, "machine %d added to set.\n", i);
    }
  }

  char *req_buf = rpc_->get_static_buf(MAX_REQUESTS_NUM*sizeof(det_request));
  char* req_buf_end = req_buf;
  char reply_buf[64];

  uint64_t request_timestamp = 0;
  uint32_t count = 0;
  uint32_t epoch = 0;

  //main loop for sequencer
  while(true) {
    auto start = rdtsc();

    // generating the batch for MAX_REQUESTS_NUM transactons or ~10ms
    while (true) {
        uint tx_idx = 0;
        #if CS == 0
            /* select the workload */
            double d = rand_generator_.next_uniform();

            for(size_t i = 0;i < workload.size();++i) {
              if((i + 1) == workload.size() || d < workload[i].frequency) {
                tx_idx = i;
                break;
              }
              d -= workload[i].frequency;
            }
        #else
            assert(false);
        #endif

        request_timestamp = cm_->get_nodeid();
        request_timestamp <<= 32;
        request_timestamp |= count++;
        *(det_request*)req_buf_end = det_request(tx_idx, cm_->get_nodeid(), request_timestamp);
        workload[((det_request*)req_buf_end)->req_idx].gen_sets_fn(
                                    ((det_request*)req_buf_end)->req_info,
                                    rand_generator_,
                                    yield);
        req_buf_end += sizeof(det_request);
        if (req_buf_end - req_buf >= MAX_REQUESTS_NUM*sizeof(det_request))
          break;
    }

    // forward batch to other replica's sequencer in async-replication mode
    // [TODO]

    // broadcast the batch to other nodes's scheduler
    uint dest_thread = SCHEDULER_THREAD_ID;
    rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
    rpc_->broadcast_to(req_buf, RPC_DET_SEQUENCE, MAX_REQUESTS_NUM*sizeof(det_request), cor_id_, 
                       RRpc::REQ, mac_set_, dest_thread);
    indirect_yield(yield);
    // get results back

    epoch += 1;
    auto latency = rdtsc() - start;
    timer_.emplace(latency);
  }
}


void Sequencer::exit_handler() {

}

void Sequencer::logging_rpc_handler(int id,int cid,char *msg,void *arg) {

}

void Sequencer::thread_local_init() {}

} // namespace oltp
} // namespace nocc
