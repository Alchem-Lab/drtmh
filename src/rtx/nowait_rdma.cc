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
        // if failed, release all locks acquired.
        release_reads_w_rdma(yield);
        release_writes_w_rdma(yield);
        END(lock);
        return false;

        #endif
      }
    }
    else { //local access
      assert(false);
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

        // if failed, release all locks acquired.
        release_reads_w_rdma(yield);
        release_writes_w_rdma(yield);
        END(lock);
        return false;
        #endif
      }
    }
    else { //local access
      assert(false);
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
      if(it->node->lock & 0x1 == W_LOCKED) {
        END(lock);
        return false;
      } else {
        END(lock);
        return true;
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

      while (true) {
        lock_req_->set_lock_meta(off,_state,LOCKED(node_id_),local_buf);
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
      if(it->node->lock & 0x1 == W_LOCKED) {
        END(lock);
        return false;
      } else {
        if (EXPIRED(END_TIME(it->node->lock))) {
          // clear expired lease (optimization)
          volatile uint64_t *lockptr = &(it->node->lock);
          *lockptr &= 0x1ff; // clear the higher 55-bits to clear the expired lease
        } else { //read locked
          END(lock);
          return false;
        }
      }
    }

    END(lock);
    return true;
}

void NOWAIT::release_reads_w_rdma(yield_func_t &yield) {
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
      assert(false); // not implemented
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
      assert(false); // not implemented
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void NOWAIT::release_reads_w_rwlock_rdma(yield_func_t &yield) {
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
        node->lock = rwlock::INIT;
        scheduler_->post_send(qp,cor_id_,IBV_WR_RDMA_WRITE,(char *)(node),sizeof(uint64_t),
                              (*it).off,IBV_SEND_INLINE | IBV_SEND_SIGNALED);
      }
    } else {
      assert(false); // not implemented
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void NOWAIT::release_writes_w_rwlock_rdma(yield_func_t &yield) {
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
      assert(false); // not implemented
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
      assert(false);
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
      assert(false);
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
      assert(false);
      inplace_write_op(it->node,it->data_ptr,it->len);
    } // check pid
  }   // for
  // gather results
  worker_->indirect_yield(yield);
  END(commit);
}

/* RPC handlers */
void NOWAIT::read_rpc_handler(int id,int cid,char *msg,void *arg) {
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
        uint64_t seq;
        MemNode *node = NULL;

        OCCResponse *reply_item = (OCCResponse *)reply;
        if(unlikely((node = local_try_lock_op(item->tableid,item->key,
                                              ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1))) == NULL)) {
          reply_item->seq = 0;
          reply_item->idx = item->idx;
          reply_item->payload = 0;
          reply += sizeof(OCCResponse);
          break;
        } else {
          reply_item->seq = node->seq;
          reply_item->idx = item->idx;
          reply_item->payload = item->len;

          local_get_op(node,reply + sizeof(OCCResponse),seq,item->len);

          reply += (sizeof(OCCResponse) + item->len);
        }
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

  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {

    //ASSERT(num < 25) << "[Lock RPC handler] lock " << num << " items.";

    auto item = (RtxLockItem *)ttptr;

    if(item->pid != response_node_)
      continue;

    MemNode *node = NULL;

    if(unlikely((node = local_try_lock_op(item->tableid,item->key,
                                          ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1))) == NULL)) {
      res = LOCK_FAIL_MAGIC;
      break;
    }
    if(unlikely(node->seq != item->seq)){
      res = LOCK_FAIL_MAGIC;
      break;
    }
  }

  //char *log_buf = next_log_entry(&local_log,32);
  //assert(log_buf != NULL);
  //sprintf(log_buf,"reply to  %d c:%d, \n",id,cid);
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}

void NOWAIT::release_rpc_handler(int id,int cid,char *msg,void *arg) {

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {
    auto item = (RtxLockItem *)ttptr;

    if(item->pid != response_node_)
      continue;
    auto res = local_try_release_op(item->tableid,item->key,
                                    ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1));
  }

  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}

void NOWAIT::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&NOWAIT::read_rpc_handler,this,RTX_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&NOWAIT::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&NOWAIT::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  
}

} // namespace rtx

} // namespace nocc
