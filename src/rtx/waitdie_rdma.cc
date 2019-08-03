#include "waitdie_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {


bool WAITDIE::try_lock_read_w_rdma(int index, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = read_set_;
    auto it = set.begin() + index;
    START(lock);
    //TODO: currently READ lock is implemented the same as WRITE lock.
    //      which is too strict, thus limiting concurrency. 
    //      We need to implement the read-lock-compatible read-lock. 
    uint64_t lock_content = R_LEASE(txn_start_time);
    // uint64_t old_state = 0;

    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;

      // post RDMA requests
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);
      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;

      while(true) {
        lock_req_->set_lock_meta(off,0,lock_content,local_buf);
        lock_req_->post_reqs(scheduler_,qp);
        worker_->indirect_yield(yield);
        
        if (h->lock != 0) {
          if(lock_content < h->lock) {
            worker_->yield_next(yield);
            // old_state = h->lock;
            continue;
          }
          else {
            END(lock);
            // LOG(3) << lock_content << ' ' << h->lock << ' ' << old_state;
            // LOG(3) << index << ' ' << (*it).key;
            abort_cnt[1]++;
            return false;
          }
        }
        else {
          // LOG(3) << "succ:"<<lock_content << ' ' << h->lock << ' ' << old_state;
          // LOG(3) << "succ:" << index << ' ' << (*it).key;
          scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, local_buf + sizeof(RdmaValHeader),
              (*it).len, off + sizeof(RdmaValHeader), IBV_SEND_SIGNALED);
          worker_->indirect_yield(yield);
          END(lock);
          h->lock = 333; // success get the lock
          return true;
        }
      }
    }
    else { //local access
      assert(false);
      if(unlikely(!local_try_lock_op(it->node,
                                     R_LEASE(txn_start_time)))){
        #if !NO_ABORT
        END(lock);
        return false;
        #endif
      } // check local lock
    }

    END(lock);
    return true;
}

bool WAITDIE::try_lock_write_w_rdma(int index, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = write_set_;
    auto it = set.begin() + index;
    START(lock);
    //TODO: currently READ lock is implemented the same as WRITE lock.
    //      which is too strict, thus limiting concurrency. 
    //      We need to implement the read-lock-compatible read-lock. 
    uint64_t lock_content = (R_LEASE(txn_start_time)) + 1;
    // uint64_t old_state = 0;

    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;

      // post RDMA requests
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;

      while(true) {
        lock_req_->set_lock_meta(off,0,lock_content,local_buf);
        lock_req_->post_reqs(scheduler_,qp);
        worker_->indirect_yield(yield);

        if(h->lock != 0) {
          //if(lock_content < h->lock) {
          if(false) { // nowait
            worker_->yield_next(yield);
            // old_state = h->lock;
            continue;
          }
          else {
            END(lock);
            abort_cnt[2]++;
            return false;
          }
        }
        else {
          scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, local_buf + sizeof(RdmaValHeader),
              (*it).len, off + sizeof(RdmaValHeader), IBV_SEND_SIGNALED);
          worker_->indirect_yield(yield);
          h->lock = 333; // success get the lock
          END(lock);
          return true;
        }
      }
    }
    else { //local access
      if(unlikely(!local_try_lock_op(it->node,
                                     R_LEASE(txn_start_time) + 1))){
        #if !NO_ABORT
        END(lock);
        return false;
        #endif
      } // check local lock
    }

    END(lock);
    return true;
}

void WAITDIE::release_reads_w_rdma(yield_func_t &yield) {
  // can only work with lock_w_rdma
  uint64_t lock_content = R_LEASE(txn_start_time);

  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    if((*it).pid != node_id_) {
      RdmaValHeader *header = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
      if(header->lock == 333) { // successfull locked
        //Qp *qp = qp_vec_[(*it).pid];
        Qp *qp = get_qp((*it).pid);
        assert(qp != NULL);
        header->lock = 0;
        scheduler_->post_send(qp,cor_id_,IBV_WR_RDMA_WRITE,(char *)(header),sizeof(uint64_t),
                              (*it).off,IBV_SEND_INLINE | IBV_SEND_SIGNALED);
      }
      else {
        // LOG(3) << header->lock;
      }
    } else {
      while (!unlikely(local_try_release_op(it->tableid, it->key, lock_content))) ;
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void WAITDIE::release_writes_w_rdma(yield_func_t &yield) {
  uint64_t lock_content =  R_LEASE(txn_start_time) + 1;

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) {
      RdmaValHeader *header = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
      if(header->lock == 333) { // successfull locked
        //Qp *qp = qp_vec_[(*it).pid];
        Qp *qp = get_qp((*it).pid);
        assert(qp != NULL);
        header->lock = 0;
        scheduler_->post_send(qp,cor_id_,IBV_WR_RDMA_WRITE,(char *)(header),sizeof(uint64_t),
                              (*it).off,IBV_SEND_INLINE | IBV_SEND_SIGNALED);
      }
      else {
        // LOG(3) << header->lock;
      }
    } else {
      while (!unlikely(local_try_release_op(it->tableid, it->key, lock_content))) ;
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void WAITDIE::write_back_w_rdma(yield_func_t &yield) {

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
      RdmaValHeader *header = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      header->lock = 0;
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

bool WAITDIE::try_lock_read_w_rwlock_rpc(int index, yield_func_t &yield) {
  using namespace rwlock_4_waitdie;

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
    else {
      assert(false);
    }

  } else {
      while (true) {
        RdmaValHeader* header = (RdmaValHeader*)it->node->value;
        volatile uint64_t l = header->lock;
        if(l & 0x1 == W_LOCKED) {
          if (txn_start_time < START_TIME(l))
            //need some random backoff?
            continue;
          else {
            END(lock);
            return false;
          }
        } else {
          if (EXPIRED(START_TIME(l), LEASE_DURATION(l))) {
            // clear expired lease (optimization)
            volatile uint64_t *lockptr = &(header->lock);
            if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                         R_LOCKED_WORD(txn_start_time, rwlock::LEASE_TIME))))
              continue;
            else {
              END(lock);
              return true;
            }
          } else { //read locked: not conflict
            END(lock);
            return true;
          }
        }
      }
  }

  assert(false);
}

bool WAITDIE::try_lock_write_w_rwlock_rpc(int index, yield_func_t &yield) {
  using namespace rwlock_4_waitdie;

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
    else {
      assert(false);  
    }
  } else {
      while(true) {
        RdmaValHeader* header = (RdmaValHeader*)it->node->value;
        volatile uint64_t l = header->lock;
        if(l & 0x1 == W_LOCKED) {
          if (txn_start_time < START_TIME(l))
            //need some random backoff?
            continue;
          else {
            END(lock);
            return false;
          }
        } else {
          if (EXPIRED(START_TIME(l), LEASE_DURATION(l))) {
            // clear expired lease (optimization)
            volatile uint64_t *lockptr = &(header->lock);
            if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                         W_LOCKED_WORD(txn_start_time, response_node_))))
              continue;
            else {
              END(lock);
              return true;
            }
          } else { //read locked
            if (txn_start_time < START_TIME(l))
              //need some random backoff?
              continue;
            else {
              END(lock);
              return false;
            }
          }
        }
      }
  }

  worker_->indirect_yield(yield);
  END(lock);

  // get the response
}

void WAITDIE::release_reads(yield_func_t &yield, bool release_all) {
  using namespace rwlock_4_waitdie;
  int num = read_set_.size();
  if(!release_all) {
    num -= 1;
  }
  start_batch_rpc_op(write_batch_helper_);
  for(int i = 0; i < num; ++i) {
    if(read_set_[i].pid != node_id_) { // remote case
      add_batch_entry<RTXLockRequestItem>(write_batch_helper_, read_set_[i].pid,
                                   /*init RTXLockRequestItem */ 
        RTX_REQ_LOCK_READ, read_set_[i].pid,read_set_[i].tableid,
        read_set_[i].key,read_set_[i].seq, txn_start_time);
    }
    else {
      auto res = local_try_release_op(read_set_[i].node,R_LEASE(txn_start_time));
    }
  }
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);
  worker_->indirect_yield(yield);
}

void WAITDIE::release_writes(yield_func_t &yield, bool release_all) {
  using namespace rwlock_4_waitdie;
  int num = write_set_.size();
  if(!release_all) {
    num -= 1;
  }
  start_batch_rpc_op(write_batch_helper_);
  for(int i = 0; i < num; ++i) {
    if(write_set_[i].pid != node_id_) { // remote case
      add_batch_entry<RTXLockRequestItem>(write_batch_helper_, write_set_[i].pid,
                                   /*init RTXLockRequestItem */ RTX_REQ_LOCK_WRITE,
        write_set_[i].pid,write_set_[i].tableid,write_set_[i].key,write_set_[i].seq, txn_start_time);
    }
    else {
      auto res = local_try_release_op(write_set_[i].node,R_LEASE(txn_start_time) + 1);
    }
  }
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);
  worker_->indirect_yield(yield);
}


void WAITDIE::write_back(yield_func_t &yield) {
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

  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_COMMIT_RPC_ID);
  worker_->indirect_yield(yield);
}


/* RPC handlers */
void WAITDIE::read_write_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  char *reply = reply_msg + sizeof(ReplyHeader);
  int num_returned(0);

  RTX_ITER_ITEM(msg,sizeof(RTXReadItem)) {

    RTXReadItem *item = (RTXReadItem *)ttptr;

    if(item->pid != response_node_) {
      continue;
    }

    WaitDieResponse *reply_item = (WaitDieResponse *)reply;

    switch(item->type) {
      case RTX_REQ_READ: {
        // fetch the record
        uint64_t seq;
        auto node = local_get_op(item->tableid,item->key,reply + sizeof(WaitDieResponse),item->len,seq,
                                 db_->_schemas[item->tableid].meta_len);
        reply_item->idx = item->idx;
        reply_item->payload = item->len;

        reply += (sizeof(WaitDieResponse) + item->len);
      }
        break;
      case RTX_REQ_READ_LOCK: {
        // fetch the record
        uint64_t seq;
        auto node = local_get_op(item->tableid,item->key,reply + sizeof(WaitDieResponse),item->len,seq,
                                 db_->_schemas[item->tableid].meta_len);
        reply_item->idx = item->idx;
        reply_item->payload = item->len;

        reply += (sizeof(WaitDieResponse) + item->len);
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

void WAITDIE::lock_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock_4_waitdie;

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

    MemNode *node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL);
    assert(node->value != NULL);

    switch(item->type) {
      case RTX_REQ_LOCK_READ:
      case RTX_REQ_LOCK_WRITE: {
        RdmaValHeader* header = (RdmaValHeader*)node->value;
        while (true) {
          volatile uint64_t l = header->lock;
          // if(l & 0x1 == W_LOCKED) {
          if(l != 0) {
            //if (false) { // nowait
            if (R_LEASE(item->txn_starting_timestamp) < l) {
              // wait for the lock
              lock_waiter_t waiter = {
                .type = item->type,
                .pid = id,
                .tid = worker_id_,
                .cid = cid,
                .txn_start_time = item->txn_starting_timestamp
              };
              // LOG(3) << "add to wait";
              global_lock_manager->add_to_waitlist(&(header->lock), waiter);
              res = LOCK_WAIT_MAGIC;
              // res = LOCK_FAIL_MAGIC;
              goto END;
            } else {
              res = LOCK_FAIL_MAGIC;
              // LOG(3) << l << ' ' << R_LEASE(item->txn_starting_timestamp) ;
              // LOG(3)  << ' ' << item->key;
              abort_cnt[4]++;
              goto END;
            }
          }
          else {
            uint64_t add = 0;
            if(item->type == RTX_REQ_LOCK_WRITE) add = 1;
            volatile uint64_t *lockptr = &(header->lock);
            if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                  R_LEASE(item->txn_starting_timestamp) + add))) {
              continue;
            }
            else {
              // LOG(3) << "succ" << R_LEASE(item->txn_starting_timestamp) << ' ' << l << ' ' << item->key;
              break;
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
  if (res != LOCK_WAIT_MAGIC) {
    *((uint8_t *)reply_msg) = res;
    rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
  }
}

void WAITDIE::release_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock_4_waitdie;
  
  RTX_ITER_ITEM(msg,sizeof(RTXLockRequestItem)) {
    auto item = (RTXLockRequestItem *)ttptr;

    if(item->pid != response_node_)
      continue;

    if (item->type == RTX_REQ_LOCK_READ) {
      // auto res = local_try_release_op(item->tableid,item->key,
      //                                 R_LEASE(item->txn_starting_timestamp));
      auto node = local_lookup_op(item->tableid, item->key);
      RdmaValHeader* header = (RdmaValHeader*)node->value;
      ASSERT(header->lock == R_LEASE(item->txn_starting_timestamp)) << header->lock << ' '
        << R_LEASE(item->txn_starting_timestamp);
      header->lock = 0;
      // LOG(3) << "read release " << item->key << R_LEASE(item->txn_starting_timestamp);
    }
    else if (item->type == RTX_REQ_LOCK_WRITE) {
      // auto res = local_try_release_op(item->tableid,item->key,
      //                               R_LEASE(item->txn_starting_timestamp) + 1);  
      auto node = local_lookup_op(item->tableid, item->key);
      RdmaValHeader* header = (RdmaValHeader*)node->value;
      ASSERT(header->lock == R_LEASE(item->txn_starting_timestamp) + 1) << header->lock << ' '
        << R_LEASE(item->txn_starting_timestamp) + 1;
      header->lock = 0;
      // LOG(3) << "write release " << item->key << R_LEASE(item->txn_starting_timestamp);
    }
    
  }

  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}

void WAITDIE::commit_rpc_handler(int id,int cid,char *msg,void *arg) {
  RTX_ITER_ITEM(msg,sizeof(RtxWriteItem)) {

    auto item = (RtxWriteItem *)ttptr;
    ttptr += item->len;

    if(item->pid != response_node_) {
      continue;
    }
    // LOG(3) << "commit release " << item->key;
    auto node = inplace_write_op(item->tableid,item->key,  // find key
                                 (char *)item + sizeof(RtxWriteItem),item->len);
  } // end for
#if PA == 0
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
#endif
}

void WAITDIE::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&WAITDIE::read_write_rpc_handler,this,RTX_RW_RPC_ID);
  ROCC_BIND_STUB(rpc_,&WAITDIE::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&WAITDIE::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&WAITDIE::commit_rpc_handler,this,RTX_COMMIT_RPC_ID);
}

} // namespace rtx

} // namespace nocc
