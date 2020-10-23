#include "tx_config.h"
#include "micro_worker.h"

extern size_t distributed_ratio; // used for some app defined parameters
extern size_t total_partition;

namespace nocc {
namespace oltp {

extern __thread util::fast_random   *random_generator;

namespace micro {

struct Req {
  uint8_t size = 0;
};

const static uint VECTOR_SIZE_MAX = 16;
struct VectorOpReq {
  uint req_idx;
  enum {
    ADD = 0,
    INNER_PRODUCT = 1,
  } op;
  uint len;
  uint vector_1[VECTOR_SIZE_MAX];
  uint vector_2[VECTOR_SIZE_MAX];
};

const static uint MATRIX_ROW_MAX = 8;
const static uint MATRIX_COLUMN_MAX = 8;
struct MatrixOpReq {
  uint req_idx;
  enum {
    MULTIPLICATION = 0
  } op;
  int row_1;
  int col_1;
  int row_2;
  int col_2;
  uint matrix_1[MATRIX_ROW_MAX][MATRIX_COLUMN_MAX];
  uint matrix_2[MATRIX_ROW_MAX][MATRIX_COLUMN_MAX];
};

struct VectorAddReply {
  uint req_idx;
  uint len;
  uint result[VECTOR_SIZE_MAX];  
};

struct VectorInnerProductReply {
  uint req_idx;
  uint inner_product_result;
};

struct MatrixMultiplicationReply {
  uint req_idx;
  uint row;
  uint col;
  uint result[MATRIX_ROW_MAX][MATRIX_COLUMN_MAX];
};

// RPC IDs
enum {
  REQ_ID = 0,
  VECTOR_OP_REQ_ID = 1,
  MATRIX_OP_REQ_ID = 2
};

extern uint64_t working_space;
txn_result_t MicroWorker::micro_rpc(yield_func_t &yield) {

  static const int num_nodes = total_partition;

  auto size = distributed_ratio;
  assert(size > 0 && size <= MAX_MSG_SIZE);

  int window_size = 1;
  ASSERT(window_size < 64) << "window size shall be smaller than 64";
  static __thread char *req_buf   = rpc_->get_static_buf(4096);
  static __thread char *reply_buf = (char *)malloc(1024);

  char *req_ptr = req_buf;
  ASSERT(size <= 4096);

#if 1
#if !PA
  rpc_->prepare_multi_req(reply_buf, window_size,cor_id_);
#endif

  // LOG(2) << "Started Micro RPC";
  for (uint i = 0; i < window_size; ++i) {

    int pid = random_generator[cor_id_].next() % num_nodes;

    // prepare an RPC header
    Req *req = (Req *)(req_ptr);
    req->size = size;
#if 1
    rpc_->append_pending_req((char *)req,REQ_ID,sizeof(Req),cor_id_,RRpc::REQ,pid);
#else
    rpc_->append_req((char *)req_buf,REQ_ID,sizeof(Req),cor_id_,RRpc::REQ,pid);
#endif
  }
  rpc_->flush_pending();
#endif

#if !PA
  indirect_yield(yield);
#endif
  // ntxn_commits_ += (window_size - 1);

  // LOG(2) << "Finished Micro RPC";
  return txn_result_t(true,1);
}

txn_result_t MicroWorker::micro_rpc_vector_add(yield_func_t &yield) {

  static const int num_nodes = total_partition;

  auto size = distributed_ratio;
  assert(size > 0 && size <= MAX_MSG_SIZE);

  int window_size = 1;
  ASSERT(window_size < 64) << "window size shall be smaller than 64";

  static __thread char *req_buf   = rpc_->get_static_buf(4096);
  static __thread char *reply_buf = (char *)malloc(1024);

  char *req_ptr = req_buf;
  ASSERT(size <= 4096);

  rpc_->prepare_multi_req(reply_buf, window_size, cor_id_);

  // LOG(2) << "Started Vector Add";
  for (uint i = 0; i < window_size; ++i) {

    int pid = random_generator[cor_id_].next() % num_nodes;

    // prepare an RPC header
    VectorOpReq *req = (VectorOpReq *)(req_ptr);
    req->req_idx = i;
    req->op = VectorOpReq::ADD;
    req->len = VECTOR_SIZE_MAX;
    // LOG(2) << "Operation id:" << req->req_idx;
    LOG(2) << "vector_1: ";
    for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
      req->vector_1[i] = random_generator[cor_id_].next() % 1000;
      fprintf(stdout, "%u,", req->vector_1[i]);
    }
    fprintf(stdout, "\n");

    LOG(2) << "vector_2: ";
    for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
      req->vector_2[i] = random_generator[cor_id_].next() % 1000;
      fprintf(stdout, "%u,", req->vector_2[i]);   
    }
    fprintf(stdout, "\n");

    rpc_->append_pending_req((char *)req,VECTOR_OP_REQ_ID,sizeof(VectorOpReq),cor_id_,RRpc::REQ,pid);
  }
  rpc_->flush_pending();

  // the caller will yield until all replies are received.
  indirect_yield(yield);

  // find the result
  for (uint i = 0; i < window_size; ++i) {
    VectorAddReply* res = (VectorAddReply*)reply_buf;
    LOG(2) << "The result of " << res->req_idx << "'s vector addition of length " << res->len;
    for (int i = 0; i < res->len; i++) {
      fprintf(stdout, "%u,", res->result[i]);
    }
    fprintf(stdout, "\n");

    res += sizeof(VectorAddReply);
  }

  // LOG(2) << "Finished Vector Add";
  return txn_result_t(true,1);
}

txn_result_t MicroWorker::micro_rpc_vector_inner_product(yield_func_t &yield) {

  static const int num_nodes = total_partition;

  auto size = distributed_ratio;
  assert(size > 0 && size <= MAX_MSG_SIZE);

  int window_size = 1;
  ASSERT(window_size < 64) << "window size shall be smaller than 64";

  static __thread char *req_buf   = rpc_->get_static_buf(4096);
  static __thread char *reply_buf = (char *)malloc(1024);

  char *req_ptr = req_buf;
  ASSERT(size <= 4096);

  rpc_->prepare_multi_req(reply_buf, window_size, cor_id_);

  // LOG(2) << "Started Vector Add";
  for (uint i = 0; i < window_size; ++i) {

    int pid = random_generator[cor_id_].next() % num_nodes;

    // prepare an RPC header
    VectorOpReq *req = (VectorOpReq *)(req_ptr);
    req->req_idx = i;
    req->op = VectorOpReq::INNER_PRODUCT;
    req->len = VECTOR_SIZE_MAX;
    // LOG(2) << "Operation id:" << req->req_idx;
    LOG(2) << "vector_1: ";
    for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
      req->vector_1[i] = random_generator[cor_id_].next() % 1000;
      fprintf(stdout, "%u,", req->vector_1[i]);
    }
    fprintf(stdout, "\n");

    LOG(2) << "vector_2: ";
    for (int i = 0; i < VECTOR_SIZE_MAX; i++) {
      req->vector_2[i] = random_generator[cor_id_].next() % 1000;
      fprintf(stdout, "%u,", req->vector_2[i]);   
    }
    fprintf(stdout, "\n");

    rpc_->append_pending_req((char *)req,VECTOR_OP_REQ_ID,sizeof(VectorOpReq),cor_id_,RRpc::REQ,pid);
  }
  rpc_->flush_pending();

  // the caller will yield until all replies are received.
  indirect_yield(yield);

  // find the result
  for (uint i = 0; i < window_size; ++i) {
    VectorInnerProductReply* res = (VectorInnerProductReply*)reply_buf;
    LOG(2) << "The result of " << res->req_idx << "'s vector inner product is " 
                               << res->inner_product_result;
    fprintf(stdout, "\n");

    res += sizeof(VectorAddReply);
  }

  // LOG(2) << "Finished Vector Add";
  return txn_result_t(true,1);
}


txn_result_t MicroWorker::micro_rpc_matrix_multiplication(yield_func_t &yield) {

  static const int num_nodes = total_partition;

  auto size = distributed_ratio;
  assert(size > 0 && size <= MAX_MSG_SIZE);

  static __thread char *req_buf   = rpc_->get_static_buf(4096);
  static __thread char *reply_buf = (char *)malloc(1024);

  char *req_ptr = req_buf;
  ASSERT(size <= 4096);

  // prepare an RPC header
  rpc_->prepare_multi_req(reply_buf, 1, cor_id_);
  int pid = random_generator[cor_id_].next() % num_nodes;
  MatrixOpReq *req = (MatrixOpReq *)(req_ptr);
  
  {
    // prepare an RPC request
    req->req_idx = 0;
    req->op = MatrixOpReq::MULTIPLICATION;
    req->row_1 = MATRIX_ROW_MAX;
    req->col_1 = MATRIX_COLUMN_MAX;
    req->row_2 = MATRIX_ROW_MAX;
    req->col_2 = MATRIX_COLUMN_MAX;
    ASSERT(req->col_1 == req->row_2);

    LOG(2) << "matrix_1: ";
    for (int i = 0; i < req->row_1; i++) {
      for (int j = 0; j < req->col_1; j++) {
        req->matrix_1[i][j] = random_generator[cor_id_].next() % 1000;
        fprintf(stdout, "%u,", req->matrix_1[i][j]);
      }
      fprintf(stdout, "\n");
    }
    fprintf(stdout, "\n");

    LOG(2) << "matrix_2: ";
    for (int i = 0; i < req->row_2; i++) {
      for (int j = 0; j < req->col_2; j++) {
        req->matrix_2[i][j] = random_generator[cor_id_].next() % 1000;
        fprintf(stdout, "%u,", req->matrix_2[i][j]);
      }
      fprintf(stdout, "\n");
    }
    fprintf(stdout, "\n");

    /**
      * Send the RPC Request for matrix multiplication
      **/
    //  put your code here.
    
  }

  {
    // print out the result matrix and locally verify the correctness
    MatrixMultiplicationReply* res = (MatrixMultiplicationReply*)reply_buf;
    LOG(2) << "The result of matrix multiplication is ";
    ASSERT(res->row > 0) << "MATRIX MULTIPLICATION via RPC is NOT correct.";
    ASSERT(res->col > 0) << "MATRIX MULTIPLICATION via RPC is NOT correct.";
    for (int i = 0; i < res->row; i++) {
      for (int j = 0; j < res->col; j++) {
        int sum = 0;
        for (int k = 0; k < req->row_2; k++)
          sum += req->matrix_1[i][k]*req->matrix_2[k][j];
        ASSERT(sum == res->result[i][j]) << "MATRIX MULTIPLICATION via RPC is NOT correct.";
        fprintf(stdout, "%u,", res->result[i][j]);
      }
      fprintf(stdout, "\n");
    }
    fprintf(stdout, "\n");
    LOG(2) << "MATRIX MULTIPLICATION via RPC is correct.";
  }
  return txn_result_t(true,1);
}
/**
 * RPC handlers
 */

void MicroWorker::register_callbacks() {
  ROCC_BIND_STUB(rpc_,&MicroWorker::nop_rpc_handler,this,REQ_ID);
  ROCC_BIND_STUB(rpc_,&MicroWorker::vector_rpc_handler,this,VECTOR_OP_REQ_ID);  
  ROCC_BIND_STUB(rpc_,&MicroWorker::matrix_rpc_handler,this,MATRIX_OP_REQ_ID);
}

void MicroWorker::nop_rpc_handler(int id,int cid,char *msg, void *arg) {
  char *reply_msg = rpc_->get_reply_buf();
  Req *req = (Req *)msg;
  ASSERT(req->size <= ::rdmaio::UDRecvManager::MAX_PACKET_SIZE)
      << "req size "<< (int)(req->size) << " " << ::rdmaio::UDRecvManager::MAX_PACKET_SIZE;
  // LOG(2) << "received rpc request from " << id << " " << cid;
  rpc_->send_reply(reply_msg,req->size,id,worker_id_,cid); // a dummy notification
}

void MicroWorker::vector_rpc_handler(int id,int cid,char *msg, void *arg) {
  char *reply_msg = rpc_->get_reply_buf();
  VectorOpReq *req = (VectorOpReq *)msg;

  // // do the actual multiplication
  switch (req->op) {
    case VectorOpReq::ADD: 
      {
      VectorAddReply* reply = (VectorAddReply*)reply_msg;
      reply->req_idx = req->req_idx;
      reply->len = req->len;
      for (int i = 0; i < VECTOR_SIZE_MAX; ++i) {
        reply->result[i] = req->vector_1[i] + req->vector_2[i];
      }
      rpc_->send_reply(reply_msg,sizeof(VectorAddReply),id,worker_id_,cid);
      }
      break;
    case VectorOpReq::INNER_PRODUCT:
      {
      VectorInnerProductReply* reply = (VectorInnerProductReply*)reply_msg;
      reply->req_idx = req->req_idx;
      reply->inner_product_result = 0;
      for (int i = 0; i < VECTOR_SIZE_MAX; ++i) {
        reply->inner_product_result += req->vector_1[i] * req->vector_2[i];
      }
      rpc_->send_reply(reply_msg,sizeof(VectorInnerProductReply),id,worker_id_,cid);
      }
      break;
    default:
      break;
  }
}

void MicroWorker::matrix_rpc_handler(int id,int cid,char *msg, void *arg) {
  char *reply_msg = rpc_->get_reply_buf();
  MatrixOpReq *req = (MatrixOpReq *)msg;

  // // do the actual multiplication
  switch (req->op) {
    case MatrixOpReq::MULTIPLICATION: 
      {
        MatrixMultiplicationReply* reply = (MatrixMultiplicationReply*)reply_msg;
        reply->req_idx = req->req_idx;

        /**
          * fill out the rest of the fields of the reply message
          **/
        //  put your code here.

        /**
          * send the reply back to the RPC requester.
          **/
        //  put your code here.
      }
      break;
    default:
      break;
  }
}

} // end micro
}
}
