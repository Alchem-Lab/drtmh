#include "calvin_rdma.h"
#include "framework/bench_worker.h"

#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {

bool CALVIN::try_lock_read_w_rdma(int index, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = read_set_;
    auto it = set.begin() + index;
    START(lock);
    //TODO: currently READ lock is implemented the same as WRITE lock.
    //      which is too strict, thus limiting concurrency. 
    //      We need to implement the read-lock-compatible read-lock. 
    uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;

      // post RDMA requests
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      #if INLINE_OVERWRITE
      char *local_buf = (char *)((*it).data_ptr) - sizeof(MemNode);
      MemNode *h = (MemNode *)local_buf;
      #else
      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;
      #endif

      lock_req_->set_lock_meta(off,0,lock_content,local_buf);
      lock_req_->post_reqs(scheduler_,qp);
      worker_->indirect_yield(yield);
      
      // write_batch_helper_.mac_set_.insert(it->pid);
      if (h->lock != 0) {
        #if !NO_ABORT
        END(lock);
        return false;
        #endif
      }
    }
    else { //local access
      if(unlikely(!local_try_lock_op(it->node,
                                     ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1)))){
        #if !NO_ABORT
        END(lock);
        return false;
        #endif
      } // check local lock
    }

    END(lock);
    return true;
}

bool CALVIN::try_lock_write_w_rdma(int index, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = write_set_;
    auto it = set.begin() + index;
    START(lock);
    //TODO: currently READ lock is implemented the same as WRITE lock.
    //      which is too strict, thus limiting concurrency. 
    //      We need to implement the read-lock-compatible read-lock. 
    uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;

      // post RDMA requests
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      #if INLINE_OVERWRITE
      char *local_buf = (char *)((*it).data_ptr) - sizeof(MemNode);
      MemNode *h = (MemNode *)local_buf;
      #else
      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;
      #endif

      lock_req_->set_lock_meta(off,0,lock_content,local_buf);
      lock_req_->post_reqs(scheduler_,qp);
      worker_->indirect_yield(yield);
      
      // write_batch_helper_.mac_set_.insert(it->pid);
      if (h->lock != 0) {
        #if !NO_ABORT
        END(lock);
        return false;
        #endif
      }
    }
    else { //local access
      if(unlikely(!local_try_lock_op(it->node,
                                     ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1)))){
        #if !NO_ABORT
        END(lock);
        return false;
        #endif
      } // check local lock
    }

    END(lock);
    return true;
}

bool CALVIN::try_lock_read_w_FA_rdma(int index, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = read_set_;
    auto it = set.begin() + index;
    RDMAReadReq read_req(cor_id_);
    
    START(lock);
    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;

      // post RDMA requests
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

#if INLINE_OVERWRITE
      char *local_buf = (char *)((*it).data_ptr) - sizeof(MemNode);
#else
      // copy the seq out, since it will be overwritten by the remote op
      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;
      it->seq = h->seq;
#endif

      DSLR::Lock l(qp, off, local_buf, DSLR::SHARED);

      bool ret = dslr_lock_manager->acquireLock(yield, l);
      if (!ret) {
        END(lock);
        return false;
      }

      //read seq while locked
      read_req.set_read_meta(off + sizeof(uint64_t),local_buf + sizeof(uint64_t));
      read_req.post_reqs(scheduler_,qp);
      // read request need to be polled
      worker_->indirect_yield(yield);

      write_batch_helper_.mac_set_.insert(it->pid);

      if(!dslr_lock_manager->isLocked(std::make_pair(qp, off))) { // check locks
      #if !NO_ABORT
          END(lock);
          return false;
      #endif
      }
      if(h->seq != (*it).seq) {     // check seqs
      #if !NO_ABORT
          END(lock);
          return false;
      #endif
      }
    }
    else {
      assert(false);

      if(unlikely(!local_try_lock_op(it->node,
                                     ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1)))){
#if !NO_ABORT
        END(lock);
        return false;
#endif
      } // check local lock
      if(unlikely(!local_validate_op(it->node,it->seq))) {
#if !NO_ABORT
        END(lock);
        return false;
#endif
      } // check seq
    }

    END(lock);
    return true;
}

bool CALVIN::try_lock_write_w_FA_rdma(int index, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = write_set_;
    auto it = set.begin() + index;
    RDMAReadReq read_req(cor_id_);
    
    START(lock);
    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;

      // post RDMA requests
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

#if INLINE_OVERWRITE
      char *local_buf = (char *)((*it).data_ptr) - sizeof(MemNode);
#else
      // copy the seq out, since it will be overwritten by the remote op
      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;
      it->seq = h->seq;
#endif

      DSLR::Lock l(qp, off, local_buf, DSLR::EXCLUSIVE);

      bool ret = dslr_lock_manager->acquireLock(yield, l);
      if (!ret) {
        END(lock);
        return false;
      }

      //read seq while locked
      read_req.set_read_meta(off + sizeof(uint64_t),local_buf + sizeof(uint64_t));
      read_req.post_reqs(scheduler_,qp);
      // read request need to be polled
      worker_->indirect_yield(yield);

      write_batch_helper_.mac_set_.insert(it->pid);

      if(!dslr_lock_manager->isLocked(std::make_pair(qp, off))) { // check locks
      #if !NO_ABORT
          END(lock);
          return false;
      #endif
      }
      if(h->seq != (*it).seq) {     // check seqs
      #if !NO_ABORT
          END(lock);
          return false;
      #endif
      }
    }
    else {
      assert(false);

      if(unlikely(!local_try_lock_op(it->node,
                                     ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1)))){
#if !NO_ABORT
        END(lock);
        return false;
#endif
      } // check local lock
      if(unlikely(!local_validate_op(it->node,it->seq))) {
#if !NO_ABORT
        END(lock);
        return false;
#endif
      } // check seq
    }

    END(lock);
    return true;
}

bool CALVIN::try_lock_read_w_rwlock_rdma(int index, uint64_t end_time, yield_func_t &yield) {
using namespace nocc::rtx::rwlock;

    std::vector<ReadSetItem> &set = read_set_;
    auto it = set.begin() + index;
    RDMACASLockReq cas_req(cor_id_);
    RDMAReadReq read_req(cor_id_);
    uint64_t _state = INIT;

    START(lock);
    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;
      // post RDMA requests
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      #if INLINE_OVERWRITE
      char *local_buf = (char *)((*it).data_ptr) - sizeof(MemNode);
      MemNode *h = (MemNode *)local_buf;
      #else
      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;
      #endif

      while (true) {
        lock_req_->set_lock_meta(off,_state,R_LEASE(end_time),local_buf);
        lock_req_->post_reqs(scheduler_,qp);
        worker_->indirect_yield(yield);

        uint64_t old_state = *(uint64_t*)local_buf;
        if (old_state == _state) { // success 
          END(lock);
          return true;
        }
        else if ((old_state & 1) == W_LOCKED) { // write-locked
          END(lock);
          return false; 
        }
        else {
          if (EXPIRED(END_TIME(old_state))) {
            _state = old_state; // retry with corrected state
            continue;
          } else { // success: unexpired read leased
            END(lock);
            return true;
          }
        }
      }
    } else { // local access
      while(true) {
        volatile uint64_t l = it->node->lock;
        if(l & 0x1 == W_LOCKED) {
          END(lock);
          return false;
        } else {
          if (EXPIRED(END_TIME(l))) {
            volatile uint64_t *lockptr = &(it->node->lock);
            if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                         R_LEASE(end_time)))) {
              continue;
            } else {
              END(lock);
              return true;
            }
          } else {
            END(lock);
            return true;
          }
        }
      }
    }
}

bool CALVIN::try_lock_write_w_rwlock_rdma(int index, yield_func_t &yield) {
using namespace nocc::rtx::rwlock;

    std::vector<ReadSetItem> &set = write_set_;
    auto it = set.begin() + index;
    RDMACASLockReq cas_req(cor_id_);
    RDMAReadReq read_req(cor_id_);
    uint64_t _state = INIT;

    START(lock);
    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;
      // post RDMA requests
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      #if INLINE_OVERWRITE
      char *local_buf = (char *)((*it).data_ptr) - sizeof(MemNode);
      MemNode *h = (MemNode *)local_buf;
      #else
      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;
      #endif

      // fprintf(stderr, "post write lock at off %x.\n", off);
      while (true) {
        lock_req_->set_lock_meta(off,_state,LOCKED(response_node_),local_buf);
        lock_req_->post_reqs(scheduler_,qp);
        worker_->indirect_yield(yield);

        uint64_t old_state = *(uint64_t*)local_buf;
        if (old_state == _state) { // success
          END(lock);
          return true;
        } else if ((old_state & 1) == W_LOCKED) { // write-locked
          END(lock);
          return false;
        } else {
          if (EXPIRED(END_TIME(old_state))) {
            _state = old_state; // retry with corrected state
            continue;
          } else { // success: unexpired read leased
            END(lock);
            return false;
          }
        }
      }
    } else { // local access
      while (true) {
        volatile uint64_t l = it->node->lock;
        if(l & 0x1 == W_LOCKED) {
          END(lock);
          return false;
        } else {
          if (EXPIRED(END_TIME(l))) {
            volatile uint64_t *lockptr = &(it->node->lock);
            if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                         LOCKED(response_node_)))) {
              continue;
            } else {
              END(lock);
              return true; 
            }       
          } else { //read locked
            END(lock);
            return false;
          }
        }
      }
    }
}

void CALVIN::release_reads_w_rdma(yield_func_t &yield) {
  // can only work with lock_w_rdma
  uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    if((*it).pid != node_id_) {
#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      if(node->lock == 0) { // successfull locked
        //Qp *qp = qp_vec_[(*it).pid];
        Qp *qp = get_qp((*it).pid);
        assert(qp != NULL);
        node->lock = 0;
        scheduler_->post_send(qp,cor_id_,IBV_WR_RDMA_WRITE,(char *)(node),sizeof(uint64_t),
                              (*it).off,IBV_SEND_INLINE | IBV_SEND_SIGNALED);
      }
    } else {
      while (!unlikely(local_try_release_op(it->tableid, it->key, lock_content))) ;
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void CALVIN::release_writes_w_rdma(yield_func_t &yield) {
  uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) {
#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      if(node->lock == 0) { // successfull locked
        //Qp *qp = qp_vec_[(*it).pid];
        Qp *qp = get_qp((*it).pid);
        assert(qp != NULL);
        node->lock = 0;
        scheduler_->post_send(qp,cor_id_,IBV_WR_RDMA_WRITE,(char *)(node),sizeof(uint64_t),
                              (*it).off,IBV_SEND_INLINE | IBV_SEND_SIGNALED);
      }
    } else {
      while (!unlikely(local_try_release_op(it->tableid, it->key, lock_content))) ;
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void CALVIN::release_reads_w_rwlock_rdma(yield_func_t &yield) {
  return;
}

void CALVIN::release_writes_w_rwlock_rdma(yield_func_t &yield) {
  using namespace rwlock;

  uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) {
#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      if(node->lock == 0) { // successfull locked
        //Qp *qp = qp_vec_[(*it).pid];
        Qp *qp = get_qp((*it).pid);
        assert(qp != NULL);
        node->lock = rwlock::INIT;
        scheduler_->post_send(qp,cor_id_,IBV_WR_RDMA_WRITE,(char *)(node),sizeof(uint64_t),
                              (*it).off,IBV_SEND_INLINE | IBV_SEND_SIGNALED);
      }
    } else {
      while (!unlikely(local_try_release_op(it->tableid, it->key, LOCKED(response_node_)))) ;
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void CALVIN::release_reads_w_FA_rdma(yield_func_t &yield) {
  // can only work with lock_w_rdma
  uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    if((*it).pid != node_id_) {
#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));      
#endif
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);
      if(dslr_lock_manager->isLocked(std::make_pair(qp, (*it).off))) { // successfull locked
        dslr_lock_manager->releaseLock(yield, std::make_pair(qp, (*it).off));
      }
    } else {
      assert(false); // not implemented
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void CALVIN::release_writes_w_FA_rdma(yield_func_t &yield) {
  // can only work with lock_w_rdma
  uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) {
#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));      
#endif
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);
      if(dslr_lock_manager->isLocked(std::make_pair(qp, (*it).off))) { // successfull locked
        dslr_lock_manager->releaseLock(yield, std::make_pair(qp, (*it).off));
      }
    } else {
      assert(false); // not implemented

    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void CALVIN::write_back_w_rdma(yield_func_t &yield) {

  /**
   * XD: it is harder to apply PA for one-sided operations.
   * This is because signaled requests are mixed with unsignaled requests.
   * It got little improvements, though. So I skip it now.
   */
  RDMAWriteReq req(cor_id_,PA /* whether to use passive ack*/);
  START(commit);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {

    if((*it).pid != node_id_) {

#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      node->lock = 0;
      req.set_write_meta((*it).off + sizeof(RdmaValHeader),(*it).data_ptr,(*it).len);
      req.set_unlock_meta((*it).off);      
      req.post_reqs(scheduler_,qp);

      // avoid send queue from overflow
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
      }

    } else { // local write
      inplace_write_op(it->node,it->data_ptr,it->len);
    } // check pid
  }   // for
  // gather results
  worker_->indirect_yield(yield);
  END(commit);
}

void CALVIN::write_back_w_rwlock_rdma(yield_func_t &yield) {

  /**
   * XD: it is harder to apply PA for one-sided operations.
   * This is because signaled requests are mixed with unsignaled requests.
   * It got little improvements, though. So I skip it now.
   */
  RDMAWriteReq req(cor_id_,PA /* whether to use passive ack*/);
  START(commit);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {

    if((*it).pid != node_id_) {

#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      node->lock = rwlock::INIT;
      req.set_write_meta((*it).off + sizeof(RdmaValHeader),(*it).data_ptr,(*it).len);
      req.set_unlock_meta((*it).off);
      req.post_reqs(scheduler_,qp);

      // avoid send queue from overflow
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
      }

    } else { // local write
      inplace_write_op(it->node,it->data_ptr,it->len);
    } // check pid
  }   // for
  // gather results
  worker_->indirect_yield(yield);
  END(commit);
}

void CALVIN::write_back_w_FA_rdma(yield_func_t &yield) {

  /**
   * XD: it is harder to apply PA for one-sided operations.
   * This is because signaled requests are mixed with unsignaled requests.
   * It got little improvements, though. So I skip it now.
   */
  RDMAWriteOnlyReq req(cor_id_,PA /* whether to use passive ack*/);
  START(commit);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {

    if((*it).pid != node_id_) {
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      req.set_write_meta((*it).off + sizeof(RdmaValHeader),(*it).data_ptr,(*it).len);
      req.post_reqs(scheduler_,qp);
      // avoid send queue from overflow
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
      }
    } else { // local write
      inplace_write_op(it->node,it->data_ptr,it->len);
    } // check pid
  }   // for
  // gather results
  worker_->indirect_yield(yield);
  END(commit);
}

bool CALVIN::try_lock_read_w_rwlock_rpc(int index, uint64_t end_time, yield_func_t &yield) {
  using namespace rwlock;

  START(lock);
  std::vector<ReadSetItem> &set = read_set_;
  auto it = set.begin() + index;
  if((*it).pid != node_id_) {

    rpc_op<RTXLockRequestItem>(cor_id_, RTX_LOCK_RPC_ID, (*it).pid, 
                               rpc_op_send_buf_,reply_buf_, 
                               /*init RTXLockRequestItem*/  
                               RTX_REQ_LOCK_READ,
                               (*it).pid,(*it).tableid,(*it).key,(*it).seq, 
                               txn_start_time
    );

    worker_->indirect_yield(yield);
    END(lock);

    // got the response
    uint8_t resp_lock_status = *(uint8_t*)reply_buf_;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC)
      return true;
    else if (resp_lock_status == LOCK_FAIL_MAGIC)
      return false;
    assert(false);

  } else {

      while(true) {
        volatile uint64_t l = it->node->lock;
        if(l & 0x1 == W_LOCKED) {
          END(lock);
          return false;
        } else {
          if (EXPIRED(END_TIME(l))) {
            volatile uint64_t *lockptr = &(it->node->lock);
            if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                         R_LEASE(end_time)))) {
              continue;
            } else {
              END(lock);
              return true;
            }
          } else {
            END(lock);
            return true;
          }
        }
      }
  }
  assert(false);
}

bool CALVIN::try_lock_write_w_rwlock_rpc(int index, yield_func_t &yield) {
  using namespace rwlock;

  START(lock);
  std::vector<ReadSetItem> &set = write_set_;
  auto it = set.begin() + index;

  if((*it).pid != node_id_) {
    rpc_op<RTXLockRequestItem>(cor_id_, RTX_LOCK_RPC_ID, (*it).pid, 
                               rpc_op_send_buf_,reply_buf_, 
                               /*init RTXLockRequestItem*/  
                               RTX_REQ_LOCK_WRITE,
                               (*it).pid,(*it).tableid,(*it).key,(*it).seq, 
                               txn_start_time
    );

    worker_->indirect_yield(yield);
    END(lock);

    // got the response
    uint8_t resp_lock_status = *(uint8_t*)reply_buf_;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC)
      return true;
    else if (resp_lock_status == LOCK_FAIL_MAGIC)
      return false;
    assert(false);
  } else {

      while (true) {
        volatile uint64_t l = it->node->lock;
        if(l & 0x1 == W_LOCKED) {
          END(lock);
          return false;
        } else {
          if (EXPIRED(END_TIME(l))) {
            volatile uint64_t *lockptr = &(it->node->lock);
            if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                         LOCKED(response_node_)))) {
              continue;
            } else {
              END(lock);
              return true; 
            }       
          } else { //read locked
            END(lock);
            return false;
          }
        }
      }
  }
}


// return false I am not supposed to execute
// the actual transaction logic. (i.e., I am either not participating or am just a passive participant)
bool CALVIN::sync_reads(int req_seq, yield_func_t &yield) {
  // sync_reads accomplishes phase 3 and phase 4: 
  // serving remote reads and collecting remote reads result.
  // LOG(3) << "rsize in sync: " << read_set_.size();
  // LOG(3) << "wsize in sync: " << write_set_.size();
  assert (!read_set_.empty() || !write_set_.empty());
  
  std::set<int> passive_participants;
  std::set<int> active_participants;
  for (int i = 0; i < write_set_.size(); ++i) {
    // fprintf(stdout, "wpid = %d\n", write_set_[i].pid);
    // LOG(3) << "wset pid:" << write_set_[i].pid;
    active_participants.insert(write_set_[i].pid);
  }
  for (int i = 0; i < read_set_.size(); ++i) {
    // fprintf(stdout, "rpid = %d\n", read_set_[i].pid);  
    // LOG(3) << "rset pid:" << read_set_[i].pid;
    if (active_participants.find(read_set_[i].pid) == active_participants.end()) {
        passive_participants.insert(read_set_[i].pid);
      }
  }
  bool am_I_active_participant = active_participants.find(response_node_) != active_participants.end();
  bool am_I_passive_participant = passive_participants.find(response_node_) != passive_participants.end();

  if (!am_I_passive_participant && !am_I_active_participant)
    return false;

  // phase 3: serving remote reads to active participants
  // If I am an active participant, only send to *other* active participants
#if 1
// #if ONE_SIDED_READ == 0
  start_batch_read();

  // fprintf(stdout, "active participants: \n");
  // for (auto itr = active_participants.begin(); itr != active_participants.end(); itr++) {
  //   fprintf(stdout, "%d ", *itr);
  // }
  // fprintf(stdout, "\n");
  
  // fprintf(stdout, "passive participants: \n");
  // for (auto itr = passive_participants.begin(); itr != passive_participants.end(); itr++) {
  //   fprintf(stdout, "%d ", *itr);
  // }

  // fprintf(stdout, "\n");

  // broadcast the active participants ONLY.
  
  for (auto itr = active_participants.begin(); itr != active_participants.end(); itr++) {
    if (*itr != response_node_)
      add_mac(read_batch_helper_, *itr);
  }


  if (!read_batch_helper_.mac_set_.empty()) {
    for (int i = 0; i < read_set_.size(); ++i) {
      if (read_set_[i].pid == response_node_) {
        // fprintf(stdout, "forward read idx %d\n", i);
        assert(read_set_[i].data_ptr != NULL);
        add_batch_entry_wo_mac<read_val_t>(read_batch_helper_,
                                     read_set_[i].pid,
                                     /* init read_val_t */ 
                                     req_seq, 0, i, read_set_[i].len, 
                                     read_set_[i].data_ptr);
      }
    }

    // for (int i = 0; i < write_set_.size(); ++i) {
    //   if (write_set_[i].pid == response_node_) {
    //     fprintf(stdout, "forward write idx %d\n", i);
    //     assert(write_set_[i].data_ptr != NULL);
    //     add_batch_entry_wo_mac<read_val_t>(read_batch_helper_,
    //                                  write_set_[i].pid,
    //                                  /* init read_val_t */ 
    //                                  req_seq, 1, i, write_set_[i].len, 
    //                                  write_set_[i].data_ptr);
    //   }
    // }
  
    // fprintf(stdout, "forward read start to machine: \n");
    // for (auto m : read_batch_helper_.mac_set_)
    //   fprintf(stdout, "%d ", m);
    // fprintf(stdout, "\n");
    auto replies = send_batch_read(RTX_CALVIN_FORWARD_RPC_ID);
    assert(replies > 0);
    worker_->indirect_yield(yield);
    // fprintf(stdout, "forward read done.\n");
  } else {
    // fprintf(stdout, "No need to forward.\n");
  }


  start_batch_read();

  for (auto itr = active_participants.begin(); itr != active_participants.end(); itr++) {
    if (*itr != response_node_)
      add_mac(read_batch_helper_, *itr);
  }

  if (!read_batch_helper_.mac_set_.empty()) {
    for (int i = 0; i < write_set_.size(); ++i) {
      if (write_set_[i].pid == response_node_) {
        // fprintf(stdout, "forward write idx %d\n", i);
        assert(write_set_[i].data_ptr != NULL);
        add_batch_entry_wo_mac<read_val_t>(read_batch_helper_,
                                     write_set_[i].pid,
                                     /* init read_val_t */ 
                                     req_seq, 1, i, write_set_[i].len, 
                                     write_set_[i].data_ptr);
      }
    }
  
    // fprintf(stdout, "forward write start to machine: \n");
    // for (auto m : read_batch_helper_.mac_set_)
    //   fprintf(stdout, "%d ", m);
    // fprintf(stdout, "\n");
    auto replies = send_batch_read(RTX_CALVIN_FORWARD_RPC_ID);
    assert(replies > 0);
    worker_->indirect_yield(yield);
    // fprintf(stdout, "forward write done.\n");
  } else {
    // fprintf(stdout, "No need to forward.\n");
  }


  if (!am_I_active_participant)
    return false;
  
  // phase 4: check if all read_set and write_set has been collected.
  //          if not, wait.
  // fprintf(stdout, "collecting missing reads and writes...\n");
  std::map<uint64_t, read_val_t>& fv = static_cast<BenchWorker*>(worker_)->forwarded_values[cor_id_];
  while (true) {
    bool has_collected_all = true;
    for (auto i = 0; i < read_set_.size(); ++i) {
      if (read_set_[i].data_ptr == NULL) {
        uint64_t key = req_seq << 6;
        key |= (i<<1);
        auto it = fv.find(key);
        if (it != fv.end()) {
          read_set_[i].data_ptr = (char*)malloc(it->second.len);
          memcpy(read_set_[i].data_ptr, it->second.value, it->second.len);
          // fprintf(stdout, "key %d read idx %d found.\n", key, i);
        } else {
          has_collected_all = false;
          break;
        }
      }
    }
    for (auto i = 0; i < write_set_.size(); ++i) {
      if (write_set_[i].data_ptr == NULL) {
        uint64_t key = req_seq << 6;
        key |= ((i<<1) + 1);
        auto it = fv.find(key);
        if (it != fv.end()) {
          write_set_[i].data_ptr = (char*)malloc(it->second.len);
          memcpy(write_set_[i].data_ptr, it->second.value, it->second.len);
          // fprintf(stdout, "key %d write idx %d found.\n", key, i);
        } else {
          has_collected_all = false;
          break;
        }
      }
    }

    if (has_collected_all) break;
    else {
      // fprintf(stdout, "waiting for read/write set ready.\n");
      worker_->yield_next(yield);
    }
  }

#else
  assert(false);
#endif

  return am_I_active_participant;
}

bool CALVIN::request_locks(yield_func_t &yield) {
using namespace nocc::rtx::rwlock;
  assert(read_set_.size() > 0 || write_set_.size() > 0);

  // lock local reads
  for (auto i = 0; i < read_set_.size(); i++) {
    if (read_set_[i].pid != response_node_)  // skip remote read
      continue;

    auto it = read_set_.begin() + i;
    // char* temp_val = (char *)malloc(it->len);
    // uint64_t seq;
    // auto node = local_get_op(it->tableid, it->key, temp_val, it->len, seq, db_->_schemas[it->tableid].meta_len);
    MemNode *node = local_lookup_op(it->tableid, it->key);
    assert(node != NULL);
    assert(node->value != NULL);

    // if (unlikely(node == NULL)) {
    //   free(temp_val);
    //   release_reads(yield);
    //   release_writes(yield);
    //   return false;
    // }

    it->node = node;

    while(true) {
      volatile uint64_t l = it->node->lock;
      if(l & 0x1 == W_LOCKED) {
        release_reads(yield);
        release_writes(yield);
        return false;
      } else {
        if (EXPIRED(END_TIME(l))) {
          volatile uint64_t *lockptr = &(it->node->lock);
          if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                       R_LEASE(txn_end_time)))) {
            worker_->yield_next(yield);       
            continue;
          } else {
            break; // lock the next local read
          }
        } else {
          break;
        }
      }
    }
  }

  // lock local writes
  for (auto i = 0; i < write_set_.size(); i++) {
    if (write_set_[i].pid != response_node_)  // skip remote read
      continue;

    auto it = write_set_.begin() + i;
    char* temp_val = (char *)malloc(it->len);
    uint64_t seq;
    auto node = local_get_op(it->tableid, it->key, temp_val, it->len, seq, db_->_schemas[it->tableid].meta_len);
    if (unlikely(node == NULL)) {
      free(temp_val);
      release_reads(yield);
      release_writes(yield);
      return false;
    }
    it->node = node;
  
    while (true) {
      volatile uint64_t l = it->node->lock;
      if(l & 0x1 == W_LOCKED) {
        release_reads(yield);
        release_writes(yield);
        return false;
      } else {
        if (EXPIRED(END_TIME(l))) {
          volatile uint64_t *lockptr = &(it->node->lock);
          if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                       LOCKED(response_node_)))) {
            worker_->yield_next(yield);
            continue;
          } else {
            break; // lock the next local read
          }
        } else { //read locked
          release_reads(yield);
          release_writes(yield);
          return false;
        }
      }
    }


  }

  return true;
}

void CALVIN::release_reads(yield_func_t &yield) {
  using namespace rwlock;

  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    if((*it).pid != response_node_)  // remote case
      continue;
    else {
      auto res = local_try_release_op(it->tableid,it->key,
                                R_LEASE(txn_start_time + LEASE_TIME));
    }
  }
}

void CALVIN::release_writes(yield_func_t &yield) {
  using namespace rwlock;

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != response_node_)  // remote case
      continue;
    else {
      auto res = local_try_release_op(it->tableid,it->key,
                                    LOCKED(it->pid));
    }
  }
}

void CALVIN::write_back(yield_func_t &yield) {
  // step 5: applying writes
  // ignore remote writes since they will be viewed as local writes
  // at some apropriate node.
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != response_node_) { // ignore remote write
    }
    else {
      // fprintf(stdout, "write back %f@%p with len = %d for table %d key %d\n", *(float*)(it->data_ptr), it->data_ptr, it->len, it->tableid, it->key);
      assert(it->node != NULL);
      //the meta_len para cannot be ignored since it defaults to 0!
      inplace_write_op(it->node,it->data_ptr,it->len, db_->_schemas[it->tableid].meta_len);
    }
  }
}

/* RPC handlers */
void CALVIN::forward_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  char *reply = reply_msg + sizeof(ReplyHeader);

  std::map<uint64_t, read_val_t>& fv = static_cast<BenchWorker*>(worker_)->forwarded_values[cid];
  
  // fprintf(stdout, "in calvin forward rpc handler.\n");
  
  assert(id != response_node_);

  int num_returned(0);
  RTX_ITER_ITEM(msg,sizeof(read_val_t)) {

    read_val_t *item = (read_val_t *)ttptr;

    // fprintf(stdout, "got forwarded value: len=%d, val=%s", item->len, item->value);
    // find the read/write set of the corresponding coroutine
    // and update the value using the forwarded value.
    
    if (item->read_or_write == 0)  { // READ
      // ReadSetItem& set_item = (*(static_cast<BenchWorker*>(worker_))->read_set_ptr[cid])[item->index_in_set];
      // assert(set_item.pid != response_node_);
      // // assert(data_ptr == NULL);
      // if (set_item.data_ptr == NULL)
      //   fprintf(stdout, "data_ptr @ %p updated.\n", &set_item.data_ptr);
      // else
      //   fprintf(stdout, "data_ptr @ %p re-updated.\n", &set_item.data_ptr);

      // set_item.data_ptr = (char*)malloc(item->len);
      // memcpy(set_item.data_ptr, item->value, item->len);

      uint64_t key = item->req_seq << 6;
      key |= ((item->index_in_set << 1));
      fv[key] = *item;

      // fprintf(stdout, "key %u installed for read idx %d.\n", key, item->index_in_set);
    } else if (item->read_or_write == 1) { // WRITE
      // ReadSetItem& set_item = (*(static_cast<BenchWorker*>(worker_))->write_set_ptr[cid])[item->index_in_set];
      // assert(set_item.pid != response_node_);
      // assert(data_ptr == NULL);
      // if (set_item.data_ptr == NULL)
      //   fprintf(stdout, "data_ptr @ %p updated.\n", &set_item.data_ptr);
      // else
      //   fprintf(stdout, "data_ptr @ %p re-updated.\n", &set_item.data_ptr);

      // set_item.data_ptr = (char*)malloc(item->len);
      // memcpy(set_item.data_ptr, item->value, item->len);
      uint64_t key = item->req_seq << 6;
      key |= ((item->index_in_set << 1) + 1);
      fv[key] = *item;

      // fprintf(stdout, "key %u installed for write idx %d.\n", key, item->index_in_set);
    } else
      assert(false);

    // num_returned += 1;
  } // end for

  num_returned = 1;
  ((ReplyHeader *)reply_msg)->num = num_returned;
  assert(num_returned > 0);
  // fprintf(stdout, "forward handler reply.\n");
  rpc_->send_reply(reply_msg,reply - reply_msg,id,cid);
  // send reply
}

void CALVIN::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&CALVIN::forward_rpc_handler,this,RTX_CALVIN_FORWARD_RPC_ID);
}

} // namespace rtx

} // namespace nocc
