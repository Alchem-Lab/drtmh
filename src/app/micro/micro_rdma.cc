#include "tx_config.h"
#include "micro_worker.h"

extern size_t distributed_ratio; // used for some app defined parameters
extern size_t total_partition;
extern size_t current_partition;

using namespace rdmaio;

namespace nocc {
namespace oltp {

extern __thread util::fast_random   *random_generator;
extern char *rdma_buffer;
extern char *free_buffer;

namespace micro {

const uint64_t working_space = 8 * 1024 * 1024;

txn_result_t MicroWorker::micro_rdma_atomic(yield_func_t &yield) {

  int      pid    = random_generator[cor_id_].next() % total_partition;
  uint64_t offset = random_generator[cor_id_].next() % (working_space - sizeof(uint64_t));
  // align
  offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
  uint64_t size = sizeof(uint64_t);
  char *local_buf = (char *)Rmalloc(size);
  auto rc = rdma_sched_->post_cas(qp_vec_[pid],cor_id_,local_buf,offset,0,0,IBV_SEND_SIGNALED);
  ASSERT(rc == SUCC) << "post cas error " << rc << " " << strerror(errno);
  indirect_yield(yield);
  Rfree(local_buf);
  return txn_result_t(true,1);
}

txn_result_t MicroWorker::micro_rdma_write(yield_func_t &yield) {

  ASSERT(working_space <= ((uint64_t)free_buffer - (uint64_t)rdma_buffer))
      << "cannot overwrite RDMA heap's memory, w free area " << get_memory_size_g((uint64_t)free_buffer - (uint64_t)rdma_buffer) << "G";

  auto size = distributed_ratio;
  const int window_size = 1;

  for(uint i = 0;i < window_size;++i) {

    uint64_t off  = random_generator[cor_id_].next() % (working_space - MAX_MSG_SIZE);
    ASSERT((off + size) <= working_space);

 retry:
    int      pid  = random_generator[cor_id_].next() % total_partition;
    {
      char *local_buf = rdma_buffer + worker_id_ * 4096 + cor_id_ * CACHE_LINE_SZ;
      int flag = IBV_SEND_SIGNALED;
      if(size < ::rdmaio::MAX_INLINE_SIZE)
        flag |= IBV_SEND_INLINE;
      auto rc = rdma_sched_->post_send(qp_vec_[pid],cor_id_,
                                       IBV_WR_RDMA_WRITE,local_buf,size,off,flag);
      ASSERT(rc == SUCC) << "post error " << strerror(errno);
    }
  }
  indirect_yield(yield);
  ntxn_commits_ += window_size - 1;

  return txn_result_t(true,1);
}

txn_result_t MicroWorker::micro_rdma_read(yield_func_t &yield) {

  auto size = distributed_ratio;
  const int window_size = 1;

  for(uint i = 0;i < window_size;++i) {

    uint64_t off  = random_generator[cor_id_].next() % working_space;

    if(off + size >= working_space)
      off -= size;

    ASSERT(off <= working_space);
 retry:
    int      pid  = random_generator[cor_id_].next() % total_partition;
    //if(unlikely(pid == current_partition))
    //      goto retry;
    {
      char *local_buf = rdma_buffer + worker_id_ * 4096 + cor_id_ * CACHE_LINE_SZ;
      auto rc = rdma_sched_->post_send(qp_vec_[pid],cor_id_,
                                       IBV_WR_RDMA_READ,local_buf,size,off,
                                       IBV_SEND_SIGNALED);
      ASSERT(rc == SUCC) << "post error " << strerror(errno);
    }
  }
  indirect_yield(yield);
  ntxn_commits_ += window_size - 1;

  return txn_result_t(true,1);
}

const static uint VECTOR_SIZE_MAX = 16;
struct vector_buffer_t {
  uint vector_1[VECTOR_SIZE_MAX];
  uint vector_2[VECTOR_SIZE_MAX];
  enum {
    INIT = 0,
    INPUT_READY = 57,
    RESULT_READY = 59
  } status;
  union {
    uint vector_inner_product_result;
    uint vector_add_result[VECTOR_SIZE_MAX];
  };
};

#define OFFSETOF(TYPE, ELEMENT) ((size_t)&(((TYPE *)0)->ELEMENT)) 

txn_result_t MicroWorker::micro_rdma_vector_add(yield_func_t &yield) {

  auto size = sizeof(vector_buffer_t);

  uint64_t off = worker_id_ * 4096 + cor_id_ * sizeof(vector_buffer_t);
  char *local_buf = rdma_buffer + off;

retry:
  int      pid  = random_generator[cor_id_].next() % total_partition;
  if(unlikely(pid == current_partition))
       goto retry;

  vector_buffer_t* lbuf = (vector_buffer_t*)local_buf;
  lbuf->status = vector_buffer_t::INIT;
  //prepare input vectors
  LOG(2) << "vector_1: ";
  for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
    lbuf->vector_1[i] = random_generator[cor_id_].next() % 1000;
    fprintf(stderr, "%u,", lbuf->vector_1[i]);
  }
  fprintf(stderr, "\n");

  LOG(2) << "and vector_2: ";
  for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
    lbuf->vector_2[i] = random_generator[cor_id_].next() % 1000;
    fprintf(stderr, "%u,", lbuf->vector_2[i]);   
  }
  fprintf(stderr, "\n");  
  lbuf->status = vector_buffer_t::INPUT_READY;

  char *temp_buf = (char *)Rmalloc(size);
  vector_buffer_t* tbuf = (vector_buffer_t*)temp_buf;
  while (true) {
    // read remote vector buffer
    {
      auto off_ = off;
      auto size_ = OFFSETOF(vector_buffer_t, vector_add_result);
      auto rc = rdma_sched_->post_send(qp_vec_[pid],cor_id_,
                                       IBV_WR_RDMA_READ,temp_buf,size,off,
                                       IBV_SEND_SIGNALED);
      ASSERT(rc == SUCC) << "post error " << strerror(errno);
    }
    indirect_yield(yield);
    // got the remote vector in local buffer
    LOG(2) << "After RDMA_READ";

    if (tbuf->status == vector_buffer_t::INPUT_READY)
      break;
    yield_next(yield);
  }

  // actual calculation
    for(int i = 0; i < VECTOR_SIZE_MAX; i++)
      tbuf->vector_add_result[i] = tbuf->vector_1[i] + tbuf->vector_2[i];
    tbuf->status = vector_buffer_t::RESULT_READY;
  LOG(2) << "After Calculation.";
  
  // write back the vector to remote buffer
  {
    auto off_ = off + OFFSETOF(vector_buffer_t, status);
    auto size_ = size - OFFSETOF(vector_buffer_t, status);
    auto rc = rdma_sched_->post_send(qp_vec_[pid],cor_id_,
                                     IBV_WR_RDMA_WRITE,
                                     temp_buf + OFFSETOF(vector_buffer_t, status),
                                     size_,
                                     off_,
                                     IBV_SEND_SIGNALED);
    ASSERT(rc == SUCC) << "post error " << strerror(errno);
  }
  indirect_yield(yield);
  LOG(2) << "After RDMA_WRITE";

  // wait until the result of my local vector add is done
  while(lbuf->status != vector_buffer_t::RESULT_READY) {
    yield_next(yield);
  }

  LOG(2) << "The result of vector addition of length " << VECTOR_SIZE_MAX;
  for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
    fprintf(stderr, "%u,", lbuf->vector_add_result[i]);
  }
  fprintf(stderr, "\n");
  return txn_result_t(true,1);
}

txn_result_t MicroWorker::micro_rdma_vector_inner_product(yield_func_t &yield) {

  auto size = sizeof(vector_buffer_t);

  uint64_t off = worker_id_ * 4096 + cor_id_ * sizeof(vector_buffer_t);
  char *local_buf = rdma_buffer + off;

retry:
  int      pid  = random_generator[cor_id_].next() % total_partition;
  if(unlikely(pid == current_partition))
       goto retry;

  vector_buffer_t* lbuf = (vector_buffer_t*)local_buf;
  lbuf->status = vector_buffer_t::INIT;
  //prepare input vectors
  LOG(2) << "vector_1: ";
  for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
    lbuf->vector_1[i] = random_generator[cor_id_].next() % 1000;
    fprintf(stderr, "%u,", lbuf->vector_1[i]);
  }
  fprintf(stderr, "\n");

  LOG(2) << "and vector_2: ";
  for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
    lbuf->vector_2[i] = random_generator[cor_id_].next() % 1000;
    fprintf(stderr, "%u,", lbuf->vector_2[i]);   
  }
  fprintf(stderr, "\n");  
  lbuf->status = vector_buffer_t::INPUT_READY;

  char *temp_buf = (char *)Rmalloc(size);
  vector_buffer_t* tbuf = (vector_buffer_t*)temp_buf;
  while (true) {
    // read remote vector buffer
    {
      auto off_ = off;
      auto size_ = OFFSETOF(vector_buffer_t, vector_add_result);
      auto rc = rdma_sched_->post_send(qp_vec_[pid],cor_id_,
                                       IBV_WR_RDMA_READ,temp_buf,size,off,
                                       IBV_SEND_SIGNALED);
      ASSERT(rc == SUCC) << "post error " << strerror(errno);
    }
    indirect_yield(yield);
    // got the remote vector in local buffer
    LOG(2) << "After RDMA_READ";

    if (tbuf->status == vector_buffer_t::INPUT_READY)
      break;
    yield_next(yield);
  }

  // actual calculation
  tbuf->vector_inner_product_result = 0;
  for(int i = 0; i < VECTOR_SIZE_MAX; i++)
    tbuf->vector_inner_product_result += tbuf->vector_1[i] * tbuf->vector_2[i];
  tbuf->status = vector_buffer_t::RESULT_READY;
  LOG(2) << "After Calculation.";
  
  // write back the vector to remote buffer
  {
    auto off_ = off + OFFSETOF(vector_buffer_t, status);
    auto size_ = size - OFFSETOF(vector_buffer_t, status);
    auto rc = rdma_sched_->post_send(qp_vec_[pid],cor_id_,
                                     IBV_WR_RDMA_WRITE,
                                     temp_buf + OFFSETOF(vector_buffer_t, status),
                                     size_,
                                     off_,
                                     IBV_SEND_SIGNALED);
    ASSERT(rc == SUCC) << "post error " << strerror(errno);
  }
  indirect_yield(yield);
  LOG(2) << "After RDMA_WRITE";

  // wait until the result of my local vector add is done
  while(lbuf->status != vector_buffer_t::RESULT_READY) {
    yield_next(yield);
  }

  LOG(2) << "The result of vector inner product is " 
         << lbuf->vector_inner_product_result;
  return txn_result_t(true,1);
}



} // namespace micro

}

}
