#include "nowait_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {

bool NOWAIT::try_lock_read_w_rdma(int index, yield_func_t &yield) {
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
        abort_cnt[0]++;
        return false;
        #endif
      }
    }
    else { //local access
      if(unlikely(!local_try_lock_op(it->node,
                                     ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1)))){
        #if !NO_ABORT
        END(lock);
        abort_cnt[1]++;
        return false;
        #endif
      } // check local lock
    }

    END(lock);
    return true;
}

bool NOWAIT::try_lock_write_w_rdma(int index, yield_func_t &yield) {
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
        abort_cnt[2]++;
        return false;
        #endif
      }
    }
    else { //local access
      if(unlikely(!local_try_lock_op(it->node,
                                     ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1)))){
        #if !NO_ABORT
        END(lock);
        abort_cnt[3]++;
        return false;
        #endif
      } // check local lock
    }

    END(lock);
    return true;
}

bool NOWAIT::try_lock_read_w_FA_rdma(int index, yield_func_t &yield) {
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
        abort_cnt[4]++;
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
          abort_cnt[5]++;
          return false;
      #endif
      }
      if(h->seq != (*it).seq) {     // check seqs
      #if !NO_ABORT
          END(lock);
          abort_cnt[6]++;
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
      abort_cnt[7]++;
        return false;
#endif
      } // check local lock
      if(unlikely(!local_validate_op(it->node,it->seq))) {
#if !NO_ABORT
        END(lock);
        abort_cnt[8]++;
        return false;
#endif
      } // check seq
    }

    END(lock);
    return true;
}

bool NOWAIT::try_lock_write_w_FA_rdma(int index, yield_func_t &yield) {
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
        abort_cnt[9]++;
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
          abort_cnt[9]++;
          return false;
      #endif
      }
      if(h->seq != (*it).seq) {     // check seqs
      #if !NO_ABORT
          END(lock);
          abort_cnt[10]++;
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
        abort_cnt[11]++;
        return false;
#endif
      } // check local lock
      if(unlikely(!local_validate_op(it->node,it->seq))) {
#if !NO_ABORT
        END(lock);
        abort_cnt[12]++;
        return false;
#endif
      } // check seq
    }

    END(lock);
    return true;
}

bool NOWAIT::try_lock_read_w_rwlock_rdma(int index, uint64_t end_time, yield_func_t &yield) {
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
          abort_cnt[13]++;
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
          abort_cnt[14]++;
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

bool NOWAIT::try_lock_write_w_rwlock_rdma(int index, yield_func_t &yield) {
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
          abort_cnt[15]++;
          return false;
        } else {
          if (EXPIRED(END_TIME(old_state))) {
            _state = old_state; // retry with corrected state
            worker_->yield_next(yield);
            continue;
          } else { // success: unexpired read leased
            END(lock);
            abort_cnt[16]++;
            return false;
          }
        }
      }
    } else { // local access
      while (true) {
        volatile uint64_t l = it->node->lock;
        if(l & 0x1 == W_LOCKED) {
          END(lock);
          abort_cnt[17]++;
          return false;
        } else {
          if (EXPIRED(END_TIME(l))) {
            volatile uint64_t *lockptr = &(it->node->lock);
            if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                         LOCKED(response_node_)))) {
              worker_->yield_next(yield);
              continue;
            } else {
              END(lock);
              return true; 
            }       
          } else { //read locked
            END(lock);
            abort_cnt[18]++;
            return false;
          }
        }
      }
    }
}

void NOWAIT::release_reads_w_rdma(yield_func_t &yield) {
  // can only work with lock_w_rdma
  uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    if((*it).tableid == 7) continue;
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

void NOWAIT::release_writes_w_rdma(yield_func_t &yield) {
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

void NOWAIT::release_reads_w_rwlock_rdma(yield_func_t &yield) {
  return;
}

void NOWAIT::release_writes_w_rwlock_rdma(yield_func_t &yield) {
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

void NOWAIT::release_reads_w_FA_rdma(yield_func_t &yield) {
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

void NOWAIT::release_writes_w_FA_rdma(yield_func_t &yield) {
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

void NOWAIT::write_back_w_rdma(yield_func_t &yield) {

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

void NOWAIT::write_back_w_rwlock_rdma(yield_func_t &yield) {

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

void NOWAIT::write_back_w_FA_rdma(yield_func_t &yield) {

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

bool NOWAIT::try_lock_read_w_rwlock_rpc(int index, uint64_t end_time, yield_func_t &yield) {
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
          abort_cnt[19]++;
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

bool NOWAIT::try_lock_write_w_rwlock_rpc(int index, yield_func_t &yield) {
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
          abort_cnt[20]++;
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
            abort_cnt[21]++;
            return false;
          }
        }
      }
  }
}

void NOWAIT::release_reads(yield_func_t &yield) {
  using namespace rwlock;

  start_batch_rpc_op(write_batch_helper_);
  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    if((*it).pid != node_id_) { // remote case
      add_batch_entry<RTXLockRequestItem>(write_batch_helper_, (*it).pid,
                                   /*init RTXLockRequestItem */ RTX_REQ_LOCK_READ, (*it).pid,(*it).tableid,(*it).key,(*it).seq, txn_start_time);
    }
    else {
      auto res = local_try_release_op(it->tableid,it->key,
                                R_LEASE(txn_start_time + LEASE_TIME));
    }
  }
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);
  worker_->indirect_yield(yield);
}

void NOWAIT::release_writes(yield_func_t &yield) {
  using namespace rwlock;

  start_batch_rpc_op(write_batch_helper_);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) { // remote case
      add_batch_entry<RTXLockRequestItem>(write_batch_helper_, (*it).pid,
                                   /*init RTXLockRequestItem */ RTX_REQ_LOCK_WRITE, (*it).pid,(*it).tableid,(*it).key,(*it).seq, txn_start_time);
    }
    else {
      auto res = local_try_release_op(it->tableid,it->key,
                                    LOCKED(it->pid));
    }
  }
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);
  worker_->indirect_yield(yield);
}

#if 0

void NOWAIT::write_back(yield_func_t &yield) {
  // note here we are not using the functionality provided by write_batch_helper
  // itself, i.e., send_batch_rpc_op(write_batch_helper_, ...)
  // instead, we use a different mechanism.
  char *cur_ptr = write_batch_helper_.req_buf_;
  START(commit);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) {

      RtxWriteItem *item = (RtxWriteItem *)cur_ptr;

      item->pid = (*it).pid;
      item->tableid = (*it).tableid;
      item->key = (*it).key;
      item->len = (*it).len;

      memcpy(cur_ptr + sizeof(RtxWriteItem),it->data_ptr,it->len);

      fprintf(stdout, "write back to %d %d %d.\n", item->pid, item->tableid, item->key);
#if !PA
      rpc_->prepare_multi_req(write_batch_helper_.reply_buf_,1,cor_id_);
#endif
      rpc_->append_pending_req(cur_ptr,RTX_COMMIT_RPC_ID,sizeof(RtxWriteItem) + it->len,cor_id_,RRpc::REQ,(*it).pid);

      cur_ptr += sizeof(RtxWriteItem) + it->len;
    } else {
      inplace_write_op(it->node,it->data_ptr,it->len);
    }
  }
  rpc_->flush_pending();

  worker_->indirect_yield(yield);
  END(commit);
}

#else

void NOWAIT::write_back(yield_func_t &yield) {
  start_batch_rpc_op(write_batch_helper_);
  
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) { // remote case

      // fprintf(stdout, "write back to %d %d %d. my node_id_= %d.\n", it->pid, it->tableid, it->key, node_id_);      
      add_batch_entry<RtxWriteItem>(write_batch_helper_, (*it).pid,
                                   /*init RTXWriteItem */ (*it).pid,(*it).tableid,(*it).key,(*it).len);
      
      memcpy(write_batch_helper_.req_buf_end_,(*it).data_ptr,(*it).len);
      write_batch_helper_.req_buf_end_ += (*it).len;
    }
    else {
      inplace_write_op(it->node,it->data_ptr,it->len);
    }
  }

  // for (char* ptr = write_batch_helper_.req_buf_; ptr != write_batch_helper_.req_buf_end_; ptr++) {
    // fprintf(stdout, "%x ", *ptr);
  // }
  // fprintf(stdout, "\n");

  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_COMMIT_RPC_ID);
  worker_->indirect_yield(yield);
}

#endif

/* RPC handlers */
void NOWAIT::read_write_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  char *reply = reply_msg + sizeof(ReplyHeader);
  int num_returned(0);

  RTX_ITER_ITEM(msg,sizeof(RTXReadItem)) {

    RTXReadItem *item = (RTXReadItem *)ttptr;

    if(item->pid != response_node_) {
      continue;
    }

    OCCResponse *reply_item = (OCCResponse *)reply;

    switch(item->type) {
      case RTX_REQ_READ: {
        // fetch the record
        uint64_t seq;
        auto node = local_get_op(item->tableid,item->key,reply + sizeof(OCCResponse),item->len,seq,
                                 db_->_schemas[item->tableid].meta_len);
        reply_item->seq = seq;
        reply_item->idx = item->idx;
        reply_item->payload = item->len;

        reply += (sizeof(OCCResponse) + item->len);
      }
        break;
      case RTX_REQ_READ_LOCK: {
        // fetch the record
        uint64_t seq;
        auto node = local_get_op(item->tableid,item->key,reply + sizeof(OCCResponse),item->len,seq,
                                 db_->_schemas[item->tableid].meta_len);

        reply_item->seq = seq;
        reply_item->idx = item->idx;
        reply_item->payload = item->len;

        reply += (sizeof(OCCResponse) + item->len);
      }
        break;
      default:
        assert(false);
    }
    num_returned += 1;
  } // end for

  ((ReplyHeader *)reply_msg)->num = num_returned;
  assert(num_returned > 0);
  rpc_->send_reply(reply_msg,reply - reply_msg,id,cid);
  // send reply
}

void NOWAIT::lock_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock;

  char* reply_msg = rpc_->get_reply_buf();

  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  int request_item_parsed = 0;

  RTX_ITER_ITEM(msg,sizeof(RTXLockRequestItem)) {

    auto item = (RTXLockRequestItem *)ttptr;

    // no batching of lock request.
    request_item_parsed++;
    assert(request_item_parsed <= 1);

    if(item->pid != response_node_)
      continue;

    MemNode *node = db_->stores_[item->tableid]->Get(item->key);
    assert(node != NULL && node->value != NULL);

    switch(item->type) {
      case RTX_REQ_LOCK_READ: {
          while (true) {
            uint64_t l = node->lock;
            if(l & 0x1 == W_LOCKED) {
                res = LOCK_FAIL_MAGIC;
                goto END;
            } else {
              if (EXPIRED(END_TIME(l))) {
                // clear expired lease (optimization)
                volatile uint64_t *lockptr = &(node->lock);
                if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                             R_LEASE(item->txn_starting_timestamp + LEASE_TIME))))
                  continue;
                else
                  goto NEXT_ITEM;  // successfully read locked this item
              } else { // read locked: not conflict
                goto NEXT_ITEM;    // successfully read locked this item
              }
            }
          }
      }
        break;
      case RTX_REQ_LOCK_WRITE: {
        while(true) {
          uint64_t l = node->lock;
          if(l & 0x1 == W_LOCKED) {
              res = LOCK_FAIL_MAGIC;
              goto END;
          } else {
            if (EXPIRED(END_TIME(l))) {
              // clear expired lease (optimization)
              volatile uint64_t *lockptr = &(node->lock);
              if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                           LOCKED(item->pid))))
                continue;
              else
                goto NEXT_ITEM;
            } else { //read locked: conflict
                res = LOCK_FAIL_MAGIC;
                goto END;
            }
          }
        }
      }
        break;
      default:
        assert(false);
    }

NEXT_ITEM:
    ;
  }

END:
  assert(res != LOCK_WAIT_MAGIC);
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}

void NOWAIT::release_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock;
  
  RTX_ITER_ITEM(msg,sizeof(RTXLockRequestItem)) {
    auto item = (RTXLockRequestItem *)ttptr;

    if(item->pid != response_node_)
      continue;

    if (item->type == RTX_REQ_LOCK_READ)
    auto res = local_try_release_op(item->tableid,item->key,
                                    R_LEASE(item->txn_starting_timestamp + LEASE_TIME));
    else if (item->type == RTX_REQ_LOCK_WRITE)
    auto res = local_try_release_op(item->tableid,item->key,
                                    LOCKED(item->pid));
  }

  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}

void NOWAIT::commit_rpc_handler(int id,int cid,char *msg,void *arg) {
  RTX_ITER_ITEM(msg,sizeof(RtxWriteItem)) {
    auto item = (RtxWriteItem *)ttptr;
    ttptr += item->len;

    // for (int i = 0; i < sizeof(RtxWriteItem) + item->len; i++) {
      // fprintf(stdout, "%x ", *((char*)item + i));
    // }

    if(item->pid != response_node_) {
      continue;
    }

    // fprintf(stdout, "handler: write back to %d %d %d.\n", item->pid, item->tableid, item->key);

    inplace_write_op(item->tableid,item->key,  // find key
                                 (char *)item + sizeof(RtxWriteItem),item->len);
  } // end for

  // fprintf(stdout, "\n");

#if PA == 0
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
#endif
}

void NOWAIT::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&NOWAIT::read_write_rpc_handler,this,RTX_RW_RPC_ID);
  ROCC_BIND_STUB(rpc_,&NOWAIT::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&NOWAIT::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&NOWAIT::commit_rpc_handler,this,RTX_COMMIT_RPC_ID);
}

} // namespace rtx

} // namespace nocc
