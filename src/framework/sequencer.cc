#include "rocc_config.h"
#include "tx_config.h"
#include "config.h"

#include "db/txs/epoch_manager.hpp"
#include "sequencer.h"
#include "scheduler.h"

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
extern db::EpochManager* epoch_manager;
extern oltp::Scheduler* scheduler;

namespace oltp {

Sequencer::Sequencer(unsigned worker_id, RdmaCtrl *cm, unsigned seed, get_workload_func_t get_wl_func):
  RWorker(worker_id,cm,seed){
  get_workload_func = get_wl_func;
}

Sequencer::~Sequencer() {

}

void Sequencer::run() {
  BindToCore(worker_id_);
  //binding(worker_id_);
  init_routines(1);
  
#if USE_RDMA
  printf("sequencer in init rdma; id = %d\n", worker_id_);
  init_rdma();
  create_qps();
#endif

#if USE_TCP_MSG == 1
  assert(local_comm_queues.size() > 0);
  create_tcp_connections(local_comm_queues[worker_id_],tcp_port,send_context);
#else
  MSGER_TYPE type;

#if USE_UD_MSG == 1
  // std::vector<int> threads;
  // threads.push_back(worker_id_);
  // threads.push_back(worker_id_+1);
  create_rdma_ud_connections(1);
#else
  create_rdma_rc_connections(rdma_buffer + HUGE_PAGE_SZ,
                             total_ring_sz,ring_padding);
#endif

#endif

  // this->init_new_logger(backup_stores_);
  this->thread_local_init();   // application specific init

  // register rpc handlers
  ROCC_BIND_STUB(rpc_, &Sequencer::logging_rpc_handler, this, RPC_DET_BACKUP);
  // register rpc handlers
  ROCC_BIND_STUB(rpc_, &Sequencer::sequence_rpc_handler, this, RPC_DET_SEQUENCE);
  // fetch QPs
  // fill_qp_vec(cm_,worker_id_);

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

void Sequencer::worker_routine(yield_func_t &yield) {

  LOG(3) << "Running Sequencer routine.";

  static const auto& workload = get_workload_func();
  char *req_buf = rpc_->get_static_buf(sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request));
  char* req_buf_end = req_buf;

  uint64_t request_timestamp = 0;
  uint32_t count = 0;
  uint32_t epoch = 0;

  uint64_t start = nocc::util::get_now();
  ((calvin_header*)req_buf)->node_id = cm_->get_nodeid();
  epoch_manager->get_current_epoch((char*)&((calvin_header*)req_buf)->epoch_id);
  ((calvin_header*)req_buf)->chunk_size = 0;
  // fprintf(stdout, "sequencing @ epoch %lu.\n", ((calvin_header*)req_buf)->epoch_id);
  req_buf_end += sizeof(calvin_header);
  uint64_t batch_size_ = 0;

  //main loop for sequencer
  while(epoch < 1)
  {
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
        if (req_buf_end - req_buf >= sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request))
          break;
    }

    LOG(3) << "sequencer generated a batch of request at epoch " << epoch;
    
    // logging batch to other replica's sequencer in async-replication mode
    // logging(req_buf, req_buf_end, yield);
    // broadcasting to other participants
    broadcast(req_buf, req_buf_end, yield);
    auto latency = rdtsc() - start;
    LOG(3) << "sequencer epoch: " << epoch << " with latency: " << latency;
    timer_.emplace(latency);
    epoch += 1;
  }
}


void Sequencer::exit_handler() {

}

void Sequencer::broadcast(char* req_buf, char* req_buf_end, yield_func_t &yield) {
    std::set<int> mac_set_;
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      // if (i != cm_->get_nodeid()) {
        mac_set_.insert(i);
      // }
    }

    // broadcast the batch to other nodes's scheduler
    // the static variable must be initialized to actual scheduler's thread id when
    // this line is reached.
    // uint dest_thread = Scheduler::SCHEDULER_THREAD_ID;

    LOG(3) << "trying to broadcast to dest nodes";
    // broadcast to the scheduler of my own node first
    // sequence_rpc_handler(cm_->get_nodeid(), cor_id_, req_buf, NULL);

    // broadcast to others
    char* cur = req_buf + sizeof(calvin_header);
    char* send_buf = rpc_->get_static_buf(MAX_MSG_SIZE);
    memcpy(send_buf, req_buf, sizeof(calvin_header));
    char reply_buf[64];

    int nchunks = (req_buf_end - cur + (MAX_MSG_SIZE - sizeof(calvin_header)) - 1) / (MAX_MSG_SIZE - sizeof(calvin_header));
    int size = (req_buf_end - cur) / nchunks;
    assert(size > 0);
    int chunk_id = 0;
    while (cur < req_buf_end) {
      ((calvin_header*)send_buf)->chunk_size = size;
      ((calvin_header*)send_buf)->chunk_id = chunk_id;
      ((calvin_header*)send_buf)->nchunks = nchunks;

      memcpy(send_buf + sizeof(calvin_header), cur, size);
      rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
      rpc_->broadcast_to(send_buf,
                         RPC_DET_SEQUENCE,
                         sizeof(calvin_header) + size,
                         cor_id_,RRpc::REQ,mac_set_);
      indirect_yield(yield);
      cur += size;  
      fprintf(stdout, "Sequencer: chunk %d posted to remote. size = %d\n", chunk_id, size);
      chunk_id++;
    }
    ASSERT(nchunks == chunk_id) << nchunks << " " << chunk_id;
    fprintf(stdout, "Sequencer: %d chunks posted. \n", nchunks);
}

void Sequencer::logging(char* req_buf, char* req_buf_end, yield_func_t &yield) {
    std::set<int> mac_set_;
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      if (i != cm_->get_nodeid()) {
        mac_set_.insert(i);
      }
    }

    char* cur = req_buf + sizeof(calvin_header);
    char* send_buf = rpc_->get_static_buf(MAX_MSG_SIZE);
    memcpy(send_buf, req_buf, sizeof(calvin_header));
    char reply_buf[64];

    int nchunks = (req_buf_end - cur) / (MAX_MSG_SIZE - sizeof(calvin_header));
    int chunk_id = 0;
    while (cur < req_buf_end) {
      // char* send_buf = rpc_->get_fly_buf(cor_id_);
      uint size = (req_buf_end - cur < MAX_MSG_SIZE - sizeof(calvin_header)) ? 
                                            req_buf_end - cur : 
                                            (MAX_MSG_SIZE - sizeof(calvin_header))/sizeof(det_request)*sizeof(det_request);
      assert(size > 0 && size % sizeof(det_request) == 0);
      ((calvin_header*)send_buf)->chunk_size = size;
      ((calvin_header*)send_buf)->chunk_id = chunk_id;
      ((calvin_header*)send_buf)->nchunks = nchunks;

      memcpy(send_buf + sizeof(calvin_header), cur, size);
      rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
      rpc_->broadcast_to(send_buf,
                         RPC_DET_BACKUP,
                         sizeof(calvin_header) + size,
                         cor_id_,RRpc::REQ,mac_set_);
      // don't need to yield since we assume passive ack (PA), or, asyncronous logging.
      // indirect_yield(yield);
      cur += size;
      fprintf(stdout, "chunk %d logged.\n", chunk_id);
      chunk_id++;
    }

    // rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
    // rpc_->broadcast_to(req_buf, RPC_DET_BACKUP, MAX_REQUESTS_NUM*sizeof(det_request), cor_id_, 
    //                    RRpc::REQ, mac_set_);
    // indirect_yield(yield);
}

void Sequencer::logging_rpc_handler(int id,int cid,char *msg,void *arg) {
  printf("backup server received batch from machine %d.\n", id);
  assert(id < backup_buffers.size() && id != cm_->get_nodeid());
  calvin_header* h = (calvin_header*)msg;

  if (backup_buffers[id] == NULL) {
    char* buf = (char*)malloc(sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request));
    backup_buffers[id] = buf;
    memset(backup_buffers[id], 0, sizeof(calvin_header));
    ((calvin_header*)backup_buffers[id])->epoch_id = h->epoch_id;
  }

  int received = ((calvin_header*)backup_buffers[id])->received_size;
  memcpy(backup_buffers[id] + sizeof(calvin_header) + received, msg + sizeof(calvin_header), h->chunk_size);
  ((calvin_header*)backup_buffers[id])->received_size += h->chunk_size;
  ((calvin_header*)backup_buffers[id])->chunk_id = h->chunk_id;
  ((calvin_header*)backup_buffers[id])->nchunks = h->nchunks;
}

void Sequencer::sequence_rpc_handler(int id,int cid,char *msg,void *arg) {
  printf("Received batch from machine %d.\n", id);
  assert(id < scheduler->req_buffers.size());
  calvin_header* h = (calvin_header*)msg;

  scheduler->sequence_lock.Lock();
  bool create_new_buffer = false;
  if (scheduler->req_buffers[id].empty())
    create_new_buffer = true;
  else {
    calvin_header* h = (calvin_header*)scheduler->req_buffers[id].back();
    if (h->chunk_id >= h->nchunks)
      create_new_buffer = true;
  }

  char* buf = NULL;
  if (create_new_buffer) {
    buf = (char*)malloc(sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request));
    memset(buf, 0, sizeof(calvin_header));
    ((calvin_header*)buf)->epoch_id = h->epoch_id;
    scheduler->req_buffers[id].push_back(buf);
  } else {
    buf = scheduler->req_buffers[id].back();
  }

  int received = ((calvin_header*)buf)->received_size;
  memcpy(buf + sizeof(calvin_header) + received, msg + sizeof(calvin_header), h->chunk_size);
  ((calvin_header*)buf)->received_size += h->chunk_size;
  ((calvin_header*)buf)->chunk_id = h->chunk_id;
  ((calvin_header*)buf)->nchunks = h->nchunks;
  scheduler->sequence_lock.Unlock();
  
  //reply to remote sequencer
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg, 0, id, cid);
  cpu_relax();
}

void Sequencer::thread_local_init() {
  for (int i = 0; i < cm_->get_num_nodes(); i++) {
      backup_buffers.push_back(NULL);
  }
}

} // namespace oltp
} // namespace nocc
