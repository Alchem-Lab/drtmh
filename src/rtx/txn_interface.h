#pragma once

#include "tx_config.h"

#include "dslr.h"
#include "core/logging.h"

#include "core/rworker.h"
#include "core/utils/latency_profier.h"
#include "core/utils/count_vector.hpp"

#if ENABLE_TXN_API
// RPC ids
#define RTX_READ_RPC_ID 0
#define RTX_RW_RPC_ID 8
#define RTX_LOCK_RPC_ID 1
#define RTX_RELEASE_RPC_ID 2
#define RTX_COMMIT_RPC_ID  3
#define RTX_VAL_RPC_ID     4
#define RTX_LOG_RPC_ID     5
#define RTX_LOG_CLEAN_ID   6
#define RTX_BACKUP_GET_ID  7
#define RTX_RENEW_LEASE_RPC_ID 10
#define RTX_UPDATE_RPC_ID 9
#define RTX_LOCK_READ_RPC_ID 14

#endif

namespace nocc {

namespace rtx {

/**
 * The transaction algorithm interface.
 * Each actual transaction algorithms should
 * implement all the virtual functions
 * defined in this interface.
 */
class TxnAlg : public TXOpBase {
public:
  TxnAlg(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
          RdmaCtrl *cm,RScheduler* sched,int ms) :
      TXOpBase(worker,db,rpc_handler,cm,sched,response_node,tid,ms)
  {
    dslr_lock_manager = new DSLR(worker, db, rpc_handler,
                                 nid, tid, cid, response_node,
                                 cm, sched, ms);
  }

  // nid: local node id. If == -1, all operations go through the network
  // resposne_node == nid: enable local accesses.
  // response_node == -1, all local operations go through network
  TxnAlg(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int cid,int response_node) :
      TXOpBase(worker,db,rpc_handler,response_node) {}

  enum ACCESS_TYPE {
    READ = 0,
    WRITE
  };

  // start a TX
  virtual void begin(yield_func_t &yield) = 0;

  // commit a TX
  virtual bool commit(yield_func_t &yield) = 0;

  // read the meta-data of an entry and put it into the read-set if any
  // return the non-negative index in the read-set
  virtual int read(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) = 0;

  // read the meta-data of an entry and put it into the write-set if any
  // return the non-negative index in the write-set
  virtual int write(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) = 0;

  // actually load the entry to read from
  virtual char* load_read(int idx, size_t len, yield_func_t &yield) = 0;

  // actually load the entry to write to
  virtual char* load_write(int idx, size_t len, yield_func_t &yield) = 0;

protected:
  virtual inline bool dummy_commit() {
    return true;
  }

  DSLR* dslr_lock_manager;
};

} // namespace rtx
} // namespace nocc
