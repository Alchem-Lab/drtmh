#include "rocc_config.h"
#include "tx_config.h"
#include "config.h"

#include "db/txs/epoch_manager.hpp"
#include "sequencer.h"
#include "scheduler.h"
#include "rtx/rdma_req_helper.hpp"
#include "ralloc.h"

#include "ring_msg.h"
#include "ud_msg.h"

/* global config constants */
extern size_t coroutine_num;
extern size_t nthreads;
extern size_t scale_factor;
extern size_t current_partition;
extern size_t total_partition;

using namespace rdmaio::ringmsg;

namespace nocc {
extern __thread MappedLog local_log;
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
  init_routines(coroutine_num);

#if 1
  create_logger();
  fprintf(stderr, "next_log_entry: local_log=%p\n", &local_log);
  char *log_buf = next_log_entry(&local_log,64);
  assert(log_buf != NULL);
  sprintf(log_buf,"sequencer runs @thread=%d\n", worker_id_);
#endif

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

  ROCC_BIND_STUB(rpc_, &Sequencer::epoch_sync_rpc_handler, this, RPC_CALVIN_EPOCH_STATUS);
  
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

  start_routine();
  return;
}

void Sequencer::worker_routine(yield_func_t &yield) {

  LOG(3) << worker_id_ << ": Running Sequencer on routine " << cor_id_;

  static const auto& workload = get_workload_func();

#if ONE_SIDED_READ == 0
  //TODO use req_buffers instead of allocating new buffer here
  int req_buf_size = sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request);
  // fprintf(stderr, "Rmalloc %d bytes.\n", req_buf_size);
  // char * const req_buf = rpc_->get_static_buf(req_buf_size);
  char * const req_buf = (char*)malloc(req_buf_size*sizeof(char));
#elif ONE_SIDED_READ == 1
  char * const req_buf = scheduler->req_buffers[cor_id_][cm_->get_nodeid()];
#else
#endif

  uint64_t request_timestamp = 0;
  uint32_t count = 0;
  uint32_t iteration = 0;

  uint64_t start = nocc::util::get_now();
  // uint64_t batch_size_ = 0;

  //main loop for sequencer
  // while (true)
  // while(iteration < 100000)
while(iteration < 1000)
  // while(iteration < 4)
  {
    if (cor_id_ == 1) {
      scheduler->epoch_done = false; // start next sequencing iteration.
      #if ONE_SIDED_READ == 0
          for (int i = 0; i < cm_->get_num_nodes(); i++)
            epoch_status_[i] = CALVIN_EPOCH_READY;
      #elif ONE_SIDED_READ == 1
          // already handled at the end of previous iteration.
      #endif
    }

    // initialize calvin_header
    ((calvin_header*)req_buf)->node_id = cm_->get_nodeid();
    // epoch_manager->get_current_epoch((char*)&((calvin_header*)req_buf)->epoch_id);
    ((calvin_header*)req_buf)->epoch_id = iteration;
#if DEBUG_LEVEL==1
    fprintf(stderr, "%d:%d: sequencing @ epoch %lu.\n", worker_id_, cor_id_, ((calvin_header*)req_buf)->epoch_id);
#endif
    char* req_buf_end = req_buf + sizeof(calvin_header);
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

        request_timestamp = (cm_->get_nodeid() << 4) + cor_id_;
        request_timestamp <<= 32;
        request_timestamp |= count++;
        *(det_request*)req_buf_end = det_request(tx_idx, cm_->get_nodeid(), request_timestamp);
        workload[((det_request*)req_buf_end)->req_idx].gen_sets_fn(
                                    ((det_request*)req_buf_end)->req_info,
                                    rand_generator_,
                                    yield);
        // const rwsets_t* set = (rwsets_t*)((det_request*)req_buf_end)->req_info;
        // fprintf(stderr, "R%dW%d\n", set->nReads, set->nWrites);
        req_buf_end += sizeof(det_request);
        if (req_buf_end - req_buf >= sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request))
          break;
    }
    ((calvin_header*)req_buf)->batch_size = req_buf_end - req_buf - sizeof(calvin_header);

    // LOG(3) << "sequencer generated a batch of request at epoch " << ((calvin_header*)req_buf)->epoch_id;
    
    // logging batch to other replica's sequencer in async-replication mode
    START(log)
#if ONE_SIDED_READ == 0
    logging(req_buf, req_buf_end, yield);
#elif ONE_SIDED_READ == 1
    // logging_rdma(req_buf, req_buf_end, yield);
#else
    assert(false);
#endif
    END(log)

    // broadcasting to other participants
#if ONE_SIDED_READ == 0
    broadcast(req_buf, req_buf_end, yield);
#elif ONE_SIDED_READ == 1
    broadcast_rdma(req_buf, req_buf_end, yield);
#else
    assert(false);
#endif

#if DEBUG_LEVEL==1
    LOG(3) << worker_id_ << ":" << cor_id_ <<": sequencer broadcasted at epoch: " << ((calvin_header*)req_buf)->epoch_id;
#endif
    // while (true) {
    //   int n_ready = 0;
    //   for (int i = 0; i < cm_->get_num_nodes(); i++) {
    //   // while (scheduler->req_buffer_ready[id]) {
    //     if (scheduler->req_buffer_state[i] == Scheduler::BUFFER_RECVED) {
    //       n_ready += 1;
    //     }
    //   }
    //   if (n_ready == cm_->get_num_nodes())
    //     break;
    //   indirect_yield(yield);
    // }

    while (!scheduler->epoch_done) {
      cpu_relax();
      yield_next(yield);
      asm volatile("" ::: "memory");
    }

#if ONE_SIDED_READ == 0
    epoch_sync(yield);
#elif ONE_SIDED_READ == 1
    epoch_sync_rdma(yield);
#endif

#if DEBUG_LEVEL==1
    LOG(3) << worker_id_ << ":" << cor_id_ << ": sequencer epoch " << iteration << " done";
#endif
    auto latency = rdtsc() - start;
    timer_.emplace(latency);
    iteration += 1;

    yield_next(yield);
  }

#if ONE_SIDED_READ == 0
  // rpc_->free_static_buf(req_buf);
  free(req_buf);
#endif
}

void Sequencer::broadcast(char* const req_buf, char* const req_buf_end, yield_func_t &yield) {
    std::set<int> mac_set_;
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      if (i != cm_->get_nodeid()) {
        mac_set_.insert(i);
      }
    }

    // broadcast the batch to other nodes's scheduler
    // the static variable must be initialized to actual scheduler's thread id when
    // this line is reached.
    // uint dest_thread = Scheduler::SCHEDULER_THREAD_ID;

    // LOG(3) << "trying to broadcast to dest nodes";
    // broadcast to the scheduler of my own node first
    // sequence_rpc_handler(cm_->get_nodeid(), cor_id_, req_buf, NULL);

    // broadcast to others
    calvin_header* header = (calvin_header*)req_buf;
    char* cur = req_buf + sizeof(calvin_header);
    char* send_buf = rpc_->get_static_buf(MAX_MSG_SIZE);
    memcpy(send_buf, req_buf, sizeof(calvin_header));
    char reply_buf[64];

    int max_chunk_size = 2048;
    int nchunks = (req_buf_end - cur + (max_chunk_size - sizeof(calvin_header)) - 1) / (max_chunk_size - sizeof(calvin_header));
    int chunk_id = 0;
    while (cur < req_buf_end) {
      int size = (req_buf_end - cur < max_chunk_size - sizeof(calvin_header)) ?
                 req_buf_end - cur :
                 max_chunk_size - sizeof(calvin_header);
      assert(size > 0);
      ((calvin_header*)send_buf)->chunk_size = size;
      ((calvin_header*)send_buf)->chunk_id = chunk_id;
      ((calvin_header*)send_buf)->nchunks = nchunks;

      // fprintf(stderr, "Sequencer: chunk %d trying to post to remote. size = %d\n", chunk_id, size);
      memcpy(send_buf + sizeof(calvin_header), cur, size);
      sequence_rpc_handler(cm_->get_nodeid(), cor_id_, send_buf, NULL);
      rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
      rpc_->broadcast_to(send_buf,
                         RPC_DET_SEQUENCE,
                         sizeof(calvin_header) + size,
                         cor_id_,RRpc::REQ,mac_set_);
      indirect_yield(yield);
      cur += size;  
      // fprintf(stderr, "%d: Sequencer chunk %d posted to remote. size = %d\n", worker_id_, chunk_id, size);
      chunk_id++;
    }
    ASSERT(nchunks == chunk_id) << nchunks << " " << chunk_id;
    // fprintf(stderr, "Sequencer: %d chunks broadcasted for requests at epoch %lu. \n", nchunks, header->epoch_id);
    rpc_->free_static_buf(send_buf);
}

void Sequencer::broadcast_rdma(char* const req_buf, char* const req_buf_end, yield_func_t &yield) {
    std::set<int> mac_set_;
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      if (i != cm_->get_nodeid()) {
        mac_set_.insert(i);
      }
    }

    calvin_header* header = (calvin_header*)req_buf;
    header->received_size = req_buf_end-req_buf-sizeof(calvin_header);

    // LOG(3) << "trying to broadcast_rdma to dest nodes";
    nocc::rtx::RDMAWriteReq req(cor_id_, PA);
    bool need_yield = false;    
    for (auto mac : mac_set_) {
      // for all other partitions
      // Qp *qp = get_qp(mac);
      Qp *qp = cm_->get_rc_qp(worker_id_,mac);
      assert(qp != NULL);

      uint64_t remote_off = scheduler->offsets_[cor_id_][cm_->get_nodeid()];
      // LOG(3) << "broadcasting to remote off:" << remote_off;

      req.set_write_meta_for<0>(remote_off + sizeof(calvin_header), 
                    req_buf + sizeof(calvin_header), req_buf_end-req_buf-sizeof(calvin_header));
    
      // calvin_header* h = new calvin_header;
      // memcpy(h, req_buf, sizeof(calvin_header));

      req.set_write_meta_for<1>(remote_off,
                                    (char*)header, sizeof(calvin_header));

      req.post_reqs(rdma_sched_, qp);

      need_yield = true;
      if (unlikely(qp->rc_need_poll())) {
          indirect_yield(yield);
          need_yield = false;
      }
    }

    if (need_yield)
      indirect_yield(yield);
 
    // fprintf(stderr, "Sequencer: coroutine %d broadcasted requests at epoch %lu. \n", cor_id_, header->epoch_id);
}

void Sequencer::sequence_rpc_handler(int id,int cid,char *msg,void *arg) {
  assert(id < cm_->get_num_nodes());
  calvin_header* h = (calvin_header*)msg;
  // fprintf(stderr, "%d: Received batch from machine %d for epoch id = %d\n", worker_id_, id, h->epoch_id);
  // assert(!scheduler->req_buffer_ready[id]);

  char* buf = scheduler->req_buffers[id][cid];
  if (h->chunk_id == 0) { // new buffer
    // buf = (char*)malloc(sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request));
    memset(buf, 0, sizeof(calvin_header));
    ((calvin_header*)buf)->epoch_id = h->epoch_id;
    ((calvin_header*)buf)->received_size = 0;
  }

  int received = ((calvin_header*)buf)->received_size;
  memcpy(buf + sizeof(calvin_header) + received, msg + sizeof(calvin_header), h->chunk_size);

  ((calvin_header*)buf)->received_size += h->chunk_size;
  ((calvin_header*)buf)->chunk_id = h->chunk_id;
  ((calvin_header*)buf)->nchunks = h->nchunks;

  if (h->chunk_id == h->nchunks-1) {
    while(scheduler->req_buffer_state[id][cid] != Scheduler::BUFFER_INIT) {
      cpu_relax();
    }
    scheduler->req_buffer_state[id][cid] = Scheduler::BUFFER_RECVED;
#if DEBUG_LEVEL==1
    fprintf(stderr, "%d: %d:%d buffer all received @ epoch %d.\n", worker_id_, id, cid, h->epoch_id);
#endif
  }

  // scheduler->sequence_lock.Unlock();
  
  if (id != cm->get_nodeid()) {
    //reply to remote sequencer
    char* reply_msg = rpc_->get_reply_buf();
    rpc_->send_reply(reply_msg, 0, id, cid);
  }
}

void Sequencer::epoch_sync(yield_func_t &yield) {
    std::set<int> mac_set_;
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      if (i != cm_->get_nodeid()) {
        mac_set_.insert(i);
      }
    }

    if (cor_id_ == 1) {
      char* send_buf = rpc_->get_static_buf(MAX_MSG_SIZE);
      char reply_buf[64];

      // epoch_status_[cm_->get_nodeid()] = CALVIN_EPOCH_DONE;
      *(uint8_t*)send_buf = CALVIN_EPOCH_DONE;

      rpc_->prepare_multi_req(reply_buf,mac_set_.size(),cor_id_);
      rpc_->broadcast_to(send_buf,
                         RPC_CALVIN_EPOCH_STATUS,
                         sizeof(uint8_t),
                         cor_id_,RRpc::REQ,mac_set_);
      indirect_yield(yield);

      rpc_->free_static_buf(send_buf);
    }

  // fprintf(stderr, "EPOCH DONE broadcasted.\n");

  for (int i = 0; i < cm_->get_num_nodes(); i++) {
      if (i == cm_->get_nodeid()) continue;
      if (epoch_status_[i] != CALVIN_EPOCH_DONE) {
        yield_next(yield);
        asm volatile("" ::: "memory");
        i--;
      }
  }
}

void Sequencer::epoch_sync_rdma(yield_func_t &yield) {
    std::set<int> mac_set_;
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      if (i != cm_->get_nodeid()) {
        mac_set_.insert(i);
      }
    }

    if (cor_id_ == 1) {
      nocc::rtx::RDMAWriteOnlyReq req(cor_id_, PA);
      bool need_yield = false;    
      for (auto mac : mac_set_) {
        // for all other partitions
        Qp *qp = get_qp(mac);
        assert(qp != NULL);

        uint64_t remote_off = scheduler->offsets_[cor_id_][cm_->get_nodeid()];
        uint8_t* epoch_status = new uint8_t;
        *epoch_status = CALVIN_EPOCH_DONE;
        req.set_write_meta(remote_off + OFFSETOF(calvin_header, epoch_status), 
                                    (char*)epoch_status, sizeof(uint8_t));

        req.post_reqs(rdma_sched_, qp);

        need_yield = true;
        if (unlikely(qp->rc_need_poll())) {
          indirect_yield(yield);
          need_yield = false;
        }
      }

      if (need_yield)
        indirect_yield(yield);
    }

    if (cor_id_ == 1) {
      for (int i = 0; i < cm_->get_num_nodes(); i++) {
        if (i == cm_->get_nodeid()) continue;
        //note that we only need to look at the req_buffer for co-routine 1
        calvin_header* h = (calvin_header*)scheduler->req_buffers[1][i];
        if (h->epoch_status != CALVIN_EPOCH_DONE) {
          yield_next(yield);
          asm volatile("" ::: "memory");
          i--;
        } else
          h->epoch_status = CALVIN_EPOCH_READY;
      }
      cor_epoch_done[cor_id_] = true;

      //reset
      for (int i = 2; i < coroutine_num+1; i++) {
        if (!cor_epoch_done[i]) {
          yield_next(yield);
          asm volatile("" ::: "memory");
          i--;
        } else
          cor_epoch_done[i] = false;
      }
      cor_epoch_done[cor_id_] = false;

    } else {
      while (!cor_epoch_done[1])
          yield_next(yield);
      cor_epoch_done[cor_id_] = true;
    }
}

void Sequencer::epoch_sync_rpc_handler(int id,int cid,char *msg,void *arg) {
  assert(id != cm_->get_nodeid());
  assert(*(uint8_t*)msg == CALVIN_EPOCH_DONE);
  epoch_status_[id] = *(uint8_t*)msg;

  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid);
}

void Sequencer::logging(char* req_buf, char* req_buf_end, yield_func_t &yield) {
    std::set<int> mac_set_;
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      if (i != cm_->get_nodeid()) {
        mac_set_.insert(i);
      }
    }

    // logging to others replica
    calvin_header* header = (calvin_header*)req_buf;
    char* cur = req_buf + sizeof(calvin_header);
    char* send_buf = rpc_->get_fly_buf(cor_id_);
    memcpy(send_buf, req_buf, sizeof(calvin_header));
    char reply_buf[64];

    int max_chunk_size = 2048;
    int nchunks = (req_buf_end - cur + (max_chunk_size - sizeof(calvin_header)) - 1) / (max_chunk_size - sizeof(calvin_header));
    int chunk_id = 0;
    while (cur < req_buf_end) {
      int size = (req_buf_end - cur < max_chunk_size - sizeof(calvin_header)) ?
                 req_buf_end - cur :
                 max_chunk_size - sizeof(calvin_header);
      assert(size > 0);
      ((calvin_header*)send_buf)->chunk_size = size;
      ((calvin_header*)send_buf)->chunk_id = chunk_id;
      ((calvin_header*)send_buf)->nchunks = nchunks;

      // fprintf(stderr, "Sequencer: chunk %d trying to post to remote. size = %d\n", chunk_id, size);
      memcpy(send_buf + sizeof(calvin_header), cur, size);

      // note that we will not prepare the reply buffer since we use asynchronous logging and don't expect replies
      // rpc_->prepare_multi_req(reply_buf,0,cor_id_);
      rpc_->broadcast_to(send_buf,
                         RPC_DET_BACKUP,
                         sizeof(calvin_header) + size,
                         cor_id_,RRpc::REQ,mac_set_);
      // note that we don't need to yield to wait for replies
      // indirect_yield(yield);
      cur += size;  
      // fprintf(stderr, "%d: Sequencer chunk %d posted to remote. size = %d\n", worker_id_, chunk_id, size);
      chunk_id++;
    }
    ASSERT(nchunks == chunk_id) << nchunks << " " << chunk_id;
    // fprintf(stderr, "Sequencer: %d chunks broadcasted for requests at epoch %lu. \n", nchunks, header->epoch_id);
    // rpc_->free_static_buf(send_buf);
    // fprintf(stderr, "Sequencer: %d chunks logged with ts = %lu. \n", nchunks, header->epoch_id);
}

void Sequencer::logging_rdma(char* req_buf, char* req_buf_end, yield_func_t &yield) {
    assert(false);
    std::set<int> mac_set_;
    for (int i = 0; i < cm_->get_num_nodes(); i++) {
      if (i != cm_->get_nodeid()) {
        mac_set_.insert(i);
      }
    }

    // logging to others replica
    calvin_header* header = (calvin_header*)req_buf;
    char* cur = req_buf + sizeof(calvin_header);
    char* send_buf = rpc_->get_fly_buf(cor_id_);
    memcpy(send_buf, req_buf, sizeof(calvin_header));
    char reply_buf[64];

    int max_chunk_size = 2048;
    int nchunks = (req_buf_end - cur + (max_chunk_size - sizeof(calvin_header)) - 1) / (max_chunk_size - sizeof(calvin_header));
    int chunk_id = 0;
    while (cur < req_buf_end) {
      int size = (req_buf_end - cur < max_chunk_size - sizeof(calvin_header)) ?
                 req_buf_end - cur :
                 max_chunk_size - sizeof(calvin_header);
      assert(size > 0);
      ((calvin_header*)send_buf)->chunk_size = size;
      ((calvin_header*)send_buf)->chunk_id = chunk_id;
      ((calvin_header*)send_buf)->nchunks = nchunks;

      // fprintf(stderr, "Sequencer: chunk %d trying to post to remote. size = %d\n", chunk_id, size);
      memcpy(send_buf + sizeof(calvin_header), cur, size);

      // note that we will not prepare the reply buffer since we use asynchronous logging and don't expect replies
      // rpc_->prepare_multi_req(reply_buf,0,cor_id_);
      rpc_->broadcast_to(send_buf,
                         RPC_DET_BACKUP,
                         sizeof(calvin_header) + size,
                         cor_id_,RRpc::REQ,mac_set_);
      // note that we don't need to yield to wait for replies
      // indirect_yield(yield);
      cur += size;  
      // fprintf(stderr, "%d: Sequencer chunk %d posted to remote. size = %d\n", worker_id_, chunk_id, size);
      chunk_id++;
    }
    ASSERT(nchunks == chunk_id) << nchunks << " " << chunk_id;
    // fprintf(stderr, "Sequencer: %d chunks broadcasted for requests at epoch %lu. \n", nchunks, header->epoch_id);
    // rpc_->free_static_buf(send_buf);
    // fprintf(stderr, "Sequencer: %d chunks logged with ts = %lu. \n", nchunks, header->epoch_id);
}

void Sequencer::logging_rpc_handler(int id,int cid,char *msg,void *arg) {
  // fprintf(stderr, "backup server received batch from machine %d.\n", id);
  assert(id < backup_buffers.size() && id != cm_->get_nodeid());
  calvin_header* h = (calvin_header*)msg;
  /******************************************
  This function must NOT assume that chunks are received in order from 1,2,3,...
  since the logging rpc does not yield to wait for the previous chunk to be sent
  before current chunk can be sent (asynchronous logging). Therefore, in-flight 
  chunks can be received in arbitrary order by this handler.
  So we must use the received size to determine if all chunks have been received. 
  *******************************************/

  char* buf = NULL;
  if (backup_buffers[id][cid]->empty() ||
    ((calvin_header*)backup_buffers[id][cid]->back())->received_size == MAX_REQUESTS_NUM*sizeof(det_request)) {
    buf = (char*)malloc(sizeof(calvin_header) + MAX_REQUESTS_NUM*sizeof(det_request));
    memset(buf, 0, sizeof(calvin_header));
    ((calvin_header*)buf)->epoch_id = h->epoch_id;
    ((calvin_header*)buf)->received_size = 0;
    if (!backup_buffers[id][cid]->empty()) {
      while (!backup_buffers[id][cid]->empty()) {
        free(backup_buffers[id][cid]->front());
        backup_buffers[id][cid]->pop();
      }
    }
    backup_buffers[id][cid]->push(buf);
  } else
    buf = backup_buffers[id][cid]->back();

  assert(buf != NULL);
  int received = ((calvin_header*)buf)->received_size;
  int max_chunk_size = 2048;
  int chunk_size = max_chunk_size - sizeof(calvin_header);
  memcpy(buf + sizeof(calvin_header) + h->chunk_id*chunk_size, msg + sizeof(calvin_header), h->chunk_size);
  ((calvin_header*)buf)->received_size += h->chunk_size;
  ((calvin_header*)buf)->chunk_id = h->chunk_id;
  ((calvin_header*)buf)->nchunks = h->nchunks;

  // no reply for logging
  // char* reply_msg = rpc_->get_reply_buf();
  // rpc_->send_reply(reply_msg, 0, id, cid);
}


void Sequencer::thread_local_init() {
  for (int i = 0; i < cm_->get_num_nodes(); i++) {
      backup_buffers.push_back(std::vector<std::queue<char*> *>());
      for (int j = 0; j < coroutine_num+1; j++)
        backup_buffers[i].push_back(new std::queue<char*>());
  }

#if ONE_SIDED_READ == 0
  epoch_status_ = new uint8_t[cm_->get_num_nodes()];
  for (int i = 0; i < cm_->get_num_nodes(); i++)
    epoch_status_[i] = CALVIN_EPOCH_READY;
#elif ONE_SIDED_READ == 1
  cor_epoch_done = new bool[coroutine_num+1];
  for (int i = 0; i < coroutine_num+1; i++)
    cor_epoch_done[i] = false;
#endif
}

void Sequencer::exit_handler() {

}

} // namespace oltp
} // namespace nocc
