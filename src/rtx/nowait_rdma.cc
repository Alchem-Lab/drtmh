#include "nowait_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {


bool NOWAIT::try_lock_read_w_rdma(int index, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = read_set_;
    auto it = set.begin() + index;
    RDMALockReq req(cor_id_ /* whether to use passive ack*/);
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
        req.set_lock_meta(off,0,lock_content,local_buf);
        req.set_read_meta(off+sizeof(RdmaValHeader), local_buf + sizeof(RdmaValHeader), (*it).len);
        req.post_reqs(scheduler_,qp);
        abort_cnt[18]++;
        worker_->indirect_yield(yield);
        
        if (h->lock != 0) {
          if(false) { // nowait
          //if(lock_content < h->lock) {
            worker_->yield_next(yield);
            // old_state = h->lock;
            continue;
          }
          else {
            // END(lock);
            // LOG(3) << lock_content << ' ' << h->lock << ' ' << old_state;
            // LOG(3) << index << ' ' << (*it).key;
            abort_cnt[1]++;
            return false;
          }
        }
        else {
          // LOG(3) << "succ:"<<lock_content << ' ' << h->lock << ' ' << old_state;
          // LOG(3) << "succ:" << index << ' ' << (*it).key;
          //scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, local_buf + sizeof(RdmaValHeader),
          //    (*it).len, off + sizeof(RdmaValHeader), IBV_SEND_SIGNALED);
          //worker_->indirect_yield(yield);
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
        return false;
        #endif
      } // check local lock
    }

    return true;
}

bool NOWAIT::try_lock_write_w_rdma(int index, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = write_set_;
    auto it = set.begin() + index;

    RDMALockReq req(cor_id_ /* whether to use passive ack*/);
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
        req.set_lock_meta(off,0,lock_content,local_buf);
        req.set_read_meta(off+sizeof(RdmaValHeader), local_buf + sizeof(RdmaValHeader), (*it).len);
        req.post_reqs(scheduler_,qp);
        abort_cnt[18]++;
        worker_->indirect_yield(yield);

        if(h->lock != 0) {
          //if(lock_content < h->lock) {
          if(false) { // nowait
            worker_->yield_next(yield);
            // old_state = h->lock;
            continue;
          }
          else {
            abort_cnt[2]++;
            return false;
          }
        }
        else {
         // scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, local_buf + sizeof(RdmaValHeader),
         //     (*it).len, off + sizeof(RdmaValHeader), IBV_SEND_SIGNALED);
         // worker_->indirect_yield(yield);
          h->lock = 333; // success get the lock
          return true;
        }
      }
    }
    else { //local access
      assert(false);
      if(unlikely(!local_try_lock_op(it->node,
                                     R_LEASE(txn_start_time) + 1))){
        #if !NO_ABORT
        return false;
        #endif
      } // check local lock
    }

    return true;
}

void NOWAIT::release_reads_w_rdma(yield_func_t &yield) {
  // can only work with lock_w_rdma
  START(release_write);
  uint64_t lock_content = R_LEASE(txn_start_time);
  abort_cnt[19]+=read_set_.size();
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
    } 
    else {
      while (!unlikely(local_try_release_op(it->tableid, it->key, lock_content))) ;
    } // check pid
  }   // for
  abort_cnt[18]++;
  worker_->indirect_yield(yield);
  END(release_write);
  return;
}

void NOWAIT::release_writes_w_rdma(yield_func_t &yield) {
  START(release_write);
  uint64_t lock_content =  R_LEASE(txn_start_time) + 1;
  abort_cnt[19]+=write_set_.size();
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
    } 
    else {
      while (!unlikely(local_try_release_op(it->tableid, it->key, lock_content))) ;
    } // check pid
  }   // for
  abort_cnt[18]++;
  worker_->indirect_yield(yield);
  END(release_write);
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
        abort_cnt[18]++;
        worker_->indirect_yield(yield);
      }

    } else { // local write
      inplace_write_op(it->node,it->data_ptr,it->len);
    } // check pid
  }   // for
  // gather results
  abort_cnt[18]++;
  worker_->indirect_yield(yield);
  END(commit);
}

bool NOWAIT::try_lock_read_w_rwlock_rpc(int index, yield_func_t &yield) {
  using namespace rwlock_4_waitdie;

  std::vector<ReadSetItem> &set = read_set_;
  auto it = set.begin() + index;
  if((*it).pid != node_id_) {

    rpc_op<RTXLockRequestItem>(cor_id_, RTX_LOCK_RPC_ID, (*it).pid, 
                               rpc_op_send_buf_,reply_buf_, 
                               /*init RTXLockRequestItem*/  
                               RTX_REQ_LOCK_READ,
                               (*it).pid,(*it).tableid,(*it).len,(*it).key,(*it).seq, 
                               txn_start_time
    );
    abort_cnt[18]++;
    worker_->indirect_yield(yield);

    // got the response
    uint8_t resp_lock_status = *(uint8_t*)reply_buf_;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC) {
      if((*it).data_ptr == NULL) {
        (*it).data_ptr = (char*)malloc((*it).len);
      }
      memcpy((*it).data_ptr, (char*)reply_buf_ + sizeof(uint8_t), (*it).len);
      // LOG(3) << (*it).pid << " " << (*it).tableid << " " << (*it).key << " r locked.";
      return true;
    }
    else if (resp_lock_status == LOCK_FAIL_MAGIC){
      abort_cnt[1]++;
      return false;
    }
    else {
      assert(false);
    }
  }
  else {
    while (true) {
      RdmaValHeader* header = (RdmaValHeader*)it->node->value;
      volatile uint64_t l = header->lock;
      if(l != 0) {
        //if (R_LEASE(txn_start_time) < l){
        if(false) { // nowait
          continue;
        }
        else {
          abort_cnt[27]++;
          return false;
        }
      }
      else {
        volatile uint64_t* lockptr = &(header->lock);
        if(unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                R_LEASE(txn_start_time)))) {
          continue;
        }
        else {
          // LOG(3) << (*it).pid << " " << (*it).tableid << " " << (*it).key << " r locally locked.";
          return true;
        }
      }
    }
  }

  assert(false);
}

bool NOWAIT::try_lock_write_w_rwlock_rpc(int index, yield_func_t &yield) {
  using namespace rwlock_4_waitdie;

  std::vector<ReadSetItem> &set = write_set_;
  auto it = set.begin() + index;

  if((*it).pid != node_id_) {
    rpc_op<RTXLockRequestItem>(cor_id_, RTX_LOCK_RPC_ID, (*it).pid, 
                               rpc_op_send_buf_,reply_buf_, 
                               /*init RTXLockRequestItem*/  
                               RTX_REQ_LOCK_WRITE,
                               (*it).pid,(*it).tableid,(*it).len,(*it).key,(*it).seq, 
                               txn_start_time
    );
    abort_cnt[18]++;
    worker_->indirect_yield(yield);

    // got the response
    uint8_t resp_lock_status = *(uint8_t*)reply_buf_;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC) {
      if((*it).data_ptr == NULL) {
        (*it).data_ptr = (char*)malloc((*it).len);
      }
      memcpy((*it).data_ptr, (char*)reply_buf_ + sizeof(uint8_t), (*it).len);
      // LOG(3) << (*it).pid << " " << (*it).tableid << " " << (*it).key << " w locked.";
      return true;
    }
    else if (resp_lock_status == LOCK_FAIL_MAGIC){
      abort_cnt[2]++;    
      return false;
    }
    else {
        ASSERT(false) << (int)resp_lock_status;
    }
  } else {
    while (true) {
      RdmaValHeader* header = (RdmaValHeader*)it->node->value;
      volatile uint64_t l = header->lock;
      if(l != 0) {
        //if (R_LEASE(txn_start_time) < l){
        if(false) { // nowait
          continue;
        }
        else {
          abort_cnt[28]++;
          return false;
        }
      }
      else {
        volatile uint64_t* lockptr = &(header->lock);
        if(unlikely(!__sync_bool_compare_and_swap(lockptr,l,
                R_LEASE(txn_start_time) + 1))) {
          continue;
        }
        else {
          // LOG(3) << (*it).pid << " " << (*it).tableid << " " << (*it).key << " w locally locked.";
          return true;
        }
      }
    }
  }

  // worker_->indirect_yield(yield);

  // get the response
}

void NOWAIT::release_reads(yield_func_t &yield, bool release_all) {
  using namespace rwlock_4_waitdie;
  START(release_write);
  int num = read_set_.size();
  if(!release_all) {
    num -= 1;
  }
  abort_cnt[19]+=num;
  start_batch_rpc_op(write_batch_helper_);
  for(int i = 0; i < num; ++i) {
    if(read_set_[i].pid != node_id_) { // remote case
      add_batch_entry<RTXLockRequestItem>(write_batch_helper_, read_set_[i].pid,
                                   /*init RTXLockRequestItem */ 
        RTX_REQ_LOCK_READ, read_set_[i].pid,read_set_[i].tableid,read_set_[i].len,
        read_set_[i].key,read_set_[i].seq, txn_start_time);    
    }
    else {
      auto res = local_try_release_op(read_set_[i].node,R_LEASE(txn_start_time));
    }
  }
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);
  abort_cnt[18]++;
  worker_->indirect_yield(yield);
  END(release_write);
}

void NOWAIT::release_writes(yield_func_t &yield, bool release_all) {
  using namespace rwlock_4_waitdie;
  START(release_write);
  int num = write_set_.size();
  if(!release_all) {
    num -= 1;
  }
  abort_cnt[19]+=num;
  start_batch_rpc_op(write_batch_helper_);
  for(int i = 0; i < num; ++i) {
    if(write_set_[i].pid != node_id_) { // remote case
      add_batch_entry<RTXLockRequestItem>(write_batch_helper_, write_set_[i].pid,
                                   /*init RTXLockRequestItem */ RTX_REQ_LOCK_WRITE,
        write_set_[i].pid,write_set_[i].tableid,write_set_[i].len,
        write_set_[i].key,write_set_[i].seq, txn_start_time);
    }
    else {
      auto res = local_try_release_op(write_set_[i].node,R_LEASE(txn_start_time) + 1);
    }
  }
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);
  abort_cnt[18]++;
  worker_->indirect_yield(yield);
  END(release_write);
}

void NOWAIT::prepare_write_contents() {
    write_batch_helper_.clear_buf();

    for(auto it = write_set_.begin();it != write_set_.end();++it) {
        if(it->pid != node_id_) {
            add_batch_entry_wo_mac<RtxWriteItem>(write_batch_helper_,
                    (*it).pid,
                    /* init write item */ (*it).pid,(*it).tableid,(*it).key,(*it).len);
            memcpy(write_batch_helper_.req_buf_end_,(*it).data_ptr,(*it).len);
            write_batch_helper_.req_buf_end_ += (*it).len;
        }
    }
}

void NOWAIT::log_remote(yield_func_t &yield) {

  if(write_set_.size() > 0 && global_view->rep_factor_ > 0) {

    // re-use write_batch_helper_'s data structure
    BatchOpCtrlBlock cblock(write_batch_helper_.req_buf_,write_batch_helper_.reply_buf_);
    cblock.batch_size_  = write_batch_helper_.batch_size_;
    cblock.req_buf_end_ = write_batch_helper_.req_buf_end_;

#if EM_FASST
    global_view->add_backup(response_node_,cblock.mac_set_);
    ASSERT(cblock.mac_set_.size() == global_view->rep_factor_)
        << "FaSST should uses rep-factor's log entries, current num "
        << cblock.mac_set_.size() << "; rep-factor " << global_view->rep_factor_;
#else
    for(auto it = write_batch_helper_.mac_set_.begin();
        it != write_batch_helper_.mac_set_.end();++it) {
      global_view->add_backup(*it,cblock.mac_set_);
    }
    // add local server
    global_view->add_backup(current_partition,cblock.mac_set_);
#endif

#if CHECKS
    LOG(3) << "log to " << cblock.mac_set_.size() << " macs";
#endif

    START(log);
    logger_->log_remote(cblock,cor_id_);
    abort_cnt[18]++;
    worker_->indirect_yield(yield);
    END(log);
#if 1
    cblock.req_buf_ = rpc_->get_fly_buf(cor_id_);
    memcpy(cblock.req_buf_,write_batch_helper_.req_buf_,write_batch_helper_.batch_msg_size());
    cblock.req_buf_end_ = cblock.req_buf_ + write_batch_helper_.batch_msg_size();
    //log ack
    logger_->log_ack(cblock,cor_id_); // need to yield
    abort_cnt[18]++;
    worker_->indirect_yield(yield);
#endif
  } // end check whether it is necessary to log
}

bool NOWAIT::prepare_commit(yield_func_t &yield) {
    BatchOpCtrlBlock& clk = write_batch_helper_;
    start_batch_rpc_op(clk);

    for (auto it = write_set_.begin();it != write_set_.end();++it)
      clk.add_mac(it->pid);

    if (clk.mac_set_.size() == 0) {
      // LOG(3) << "no 2pc prepare message sent due to read-only txn.";
      return true;
    }
    // LOG(3) << "sending prepare messages to " << clk.mac_set_.size() << " macs";
    return two_phase_committer_->prepare(this, clk, cor_id_, yield);
}

void NOWAIT::broadcast_decision(bool commit_or_abort, yield_func_t &yield) {
    BatchOpCtrlBlock& clk = write_batch_helper_;
    start_batch_rpc_op(clk);

    for (auto it = write_set_.begin();it != write_set_.end();++it)
      clk.add_mac(it->pid);
    if (clk.mac_set_.size() == 0) {
      // LOG(3) << "no 2pc decision message sent due to read-only txn.";
      return;
    }
    // LOG(3) << "sending decision messages to " << clk.mac_set_.size() << " macs";
    two_phase_committer_->broadcast_global_decision(this, clk, commit_or_abort ? 
                                                   TwoPhaseCommitMemManager::TWO_PHASE_DECISION_COMMIT : 
                                                   TwoPhaseCommitMemManager::TWO_PHASE_DECISION_ABORT, cor_id_, yield);
}

void NOWAIT::write_back(yield_func_t &yield) {
  START(commit);
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

  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_COMMIT_RPC_ID, PA);
  abort_cnt[18]++;
#if PA == 0
  worker_->indirect_yield(yield);
#endif
  END(commit);
}


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

void NOWAIT::lock_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock_4_waitdie;

  char* reply_msg = rpc_->get_reply_buf();

  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  int request_item_parsed = 0;
  size_t nodelen = 0;

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
            if (false) { // nowait
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
              char* reply_data= reply_msg + sizeof(uint8_t);
              nodelen = item->len;
              memcpy(reply_data, (char*)node->value + sizeof(RdmaValHeader), item->len);
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
    rpc_->send_reply(reply_msg,sizeof(uint8_t) + nodelen,id,cid);
  }
}

void NOWAIT::release_rpc_handler(int id,int cid,char *msg,void *arg) {
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
      // LOG(3) << item->pid << " " << item->tableid << " " << item->key << " r released.";
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
      // LOG(3) << item->pid << " " << item->tableid << " " << item->key << " w released.";
    }
    
  }

  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}

void NOWAIT::commit_rpc_handler(int id,int cid,char *msg,void *arg) {
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

void NOWAIT::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&NOWAIT::read_write_rpc_handler,this,RTX_RW_RPC_ID);
  ROCC_BIND_STUB(rpc_,&NOWAIT::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&NOWAIT::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&NOWAIT::commit_rpc_handler,this,RTX_COMMIT_RPC_ID);
}

} // namespace rtx

} // namespace nocc
