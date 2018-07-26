#include "tx_config.h"

namespace nocc {
namespace rtx  {

inline __attribute__((always_inline))
uint64_t TXOpBase::rdma_lookup_op(int pid,int tableid,uint64_t key,char *val,
                                  yield_func_t &yield,int meta_len) {
  Qp* qp = qp_vec_[pid];
  assert(qp != NULL);
  // MemNode will be stored in val, if necessary
  auto off = db_->stores_[tableid]->RemoteTraverse(key,qp,scheduler_,yield,val);
  return off;
}

inline __attribute__ ((always_inline))
uint64_t TXOpBase::rdma_read_val(int pid,int tableid,uint64_t key,int len,char *val,yield_func_t &yield,int meta_len) {

  // store the memnode in val
  auto off = rdma_lookup_op(pid,tableid,key,val,yield,meta_len);
  MemNode *node = (MemNode *)val;

  auto data_off = off;
#if !RDMA_CACHE
  data_off = node->off; // fetch the offset from the content
#endif
  ASSERT(data_off % sizeof(uint64_t) == 0) << " check lock " << node->lock
                                           << " check seq "  << node->seq << " key " << key;

  // fetch the content
  Qp* qp = qp_vec_[pid];
  qp->rc_post_send(IBV_WR_RDMA_READ,val + sizeof(MemNode),len,data_off,
                   IBV_SEND_SIGNALED,worker_->cor_id());
  scheduler_->add_pending(worker_->cor_id(),qp);
  worker_->indirect_yield(yield); // yield for waiting for NIC's completion
  return data_off;
}

} // namespace rtx

} // namespace nocc