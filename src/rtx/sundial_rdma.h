#pragma once

#include "tx_config.h"

#if ENABLE_TXN_API
#include "txn_interface.h"
#else
#include "tx_operator.hpp"
#include "core/utils/latency_profier.h"
#include "core/utils/count_vector.hpp"
#include "dslr.h"
#endif

#include "logger.hpp"

#include "core/logging.h"

#include "rdma_req_helper.hpp"

#include "rwlock.hpp"

namespace nocc {

namespace rtx {
#if ENABLE_TXN_API
class SUNDIAL : public TxnAlg {
#else
class SUNDIAL : public TXOpBase {
#endif
#include "occ_internal_structure.h"
protected:

  int local_read() {

  }

  int local_write(int tableid, uint64_t key, int len, yield_func_t &yield) {

  }

  int local_insert() {

  }

  int remote_read() {

  }

  int remote_write(int pid, int tableid, uint64_t key, int len, yield_func_t &yield) {
    for(auto& item : write_set_) {
      if(item.key == key) {
        fprintf(stdout, "[SUNDIAL INFO] remote write already in write set (no data in write set now)\n");
        return 0;
      }
    }
    // sundial exec: lock the remote record and get the info
#if ONE_SIDED_READ

#else
    
#endif


  }

  int remote_insert() {

  }

public:
  SUNDIAL(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
          RdmaCtrl *cm,RScheduler* sched,int ms) :
#if ENABLE_TXN_API
      TxnAlg(worker,db,rpc_handler,nid,tid,cid,response_node,cm,sched,ms),
#else
      TXOpBase(worker,db,rpc_handler,cm,sched,response_node,tid,ms),// response_node shall always equal *real node id*
#endif
      read_set_(),write_set_(),
      read_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
      write_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
      rpc_op_send_buf_(rpc_->get_static_buf(MAX_MSG_SIZE)),
      cor_id_(cid),response_node_(nid) {

      }

  inline __attribute__((always_inline))
  virtual int write(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    int index = -1;

    if(pid == node_id_)
      index = local_write(tableid, key, len, yield);
    else
      index = remote_write(pid, tableid, key, len, yield);
    return index;
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int write(int pid,uint64_t key,yield_func_t &yield) {
    return write(pid, tableid, key, sizeof(V), yield);
  }

  inline __attribute__((always_inline))
  virtual int read() {

  }

  inline __attribute__((always_inline))
  virtual int load_write() {

  }

  inline __attribute__((always_inline))
  virtual int load_read() {

  }  

  virtual bool commit(yield_func_t &yield) {

  }
protected:
  std::vector<SundialReadSetItem> read_set_;
  std::vector<SundialReadSetItem> write_set_;

  const int cor_id_;
  const int response_node_;

}
}