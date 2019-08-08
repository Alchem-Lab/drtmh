#include "sundial_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {

bool SUNDIAL::try_update_rdma(yield_func_t &yield) {
  RDMAWriteReq req(cor_id_,0 /* whether to use passive ack*/);
  bool need_yield = false;
  START(commit);
  for(auto& item : write_set_){
    if(item.pid != node_id_) {
      RdmaValHeader *header = (RdmaValHeader*)(item.data_ptr - sizeof(RdmaValHeader));
      uint64_t newlease = (uint64_t)commit_id_ + (((uint64_t)(commit_id_)) << 32);
#ifdef SUNDIAL_DEBUG
      LOG(3) << "write back new lease " << commit_id_;
#endif
      header->seq = newlease;
      Qp *qp = get_qp(item.pid);
      assert(qp != NULL);
      req.set_write_meta(item.off + sizeof(RdmaValHeader) - sizeof(uint64_t),
          (char*)item.data_ptr - sizeof(uint64_t), item.len + sizeof(uint64_t));
      req.set_unlock_meta(item.off);
      req.post_reqs(scheduler_, qp);
      need_yield = true;
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
        need_yield = false;
      }
    }
    else {
#ifdef SUNDIAL_DEBUG
      LOG(3) << "in else";
#endif
      uint64_t newlease = (uint64_t)commit_id_ + (((uint64_t)(commit_id_)) << 32);
      RdmaValHeader *h = (RdmaValHeader*)((char*)item.data_ptr - sizeof(RdmaValHeader));
      h->seq = newlease;
      memcpy((char*)item.value + sizeof(RdmaValHeader) - sizeof(uint64_t),
        (char*)item.data_ptr - sizeof(uint64_t), item.len + sizeof(uint64_t)); // write tss and real value back
      // release lock
      volatile uint64_t* lockptr = (uint64_t*)item.value;
      volatile uint64_t l = *lockptr;
      *lockptr = 0;
    }
  }
  if(need_yield){
    worker_->indirect_yield(yield);
  }
  END(commit);
  return true;
}

bool SUNDIAL::try_update_rpc(yield_func_t &yield) {
  start_batch_rpc_op(write_batch_helper_);
  bool need_send = false;
  START(commit);
  for(auto& item : write_set_){
    if(item.pid != node_id_) {//
    // if(item.pid != response_node_) {//
      need_send = true;
      add_batch_entry<RTXUpdateItem>(write_batch_helper_, item.pid,
        /* init RTXUpdateItem*/item.pid, item.tableid, item.key, item.len, commit_id_);
      memcpy(write_batch_helper_.req_buf_end_, item.data_ptr, item.len);
      write_batch_helper_.req_buf_end_ += item.len;
    }
    else { // local
      auto node = inplace_write_op(item.tableid, item.key, item.data_ptr, item.len, commit_id_);
      assert(node != NULL);
      RdmaValHeader* header = (RdmaValHeader*)node->value;
      uint64_t l = header->lock;
      volatile uint64_t *lockptr = &(header->lock);
      assert(__sync_bool_compare_and_swap(lockptr, l, 0)); // TODO
    }
  }
  if(need_send) {
    send_batch_rpc_op(write_batch_helper_, cor_id_, RTX_UPDATE_RPC_ID);
    worker_->indirect_yield(yield);
  }
  END(commit);
  return true;
}

void SUNDIAL::update_rpc_handler(int id,int cid,char *msg,void *arg) {
  RTX_ITER_ITEM(msg, sizeof(RTXUpdateItem)) {
    auto item = (RTXUpdateItem*)ttptr;
    ttptr += item->len;
    if(item->pid != response_node_)
      continue;
    auto node = inplace_write_op(item->tableid, item->key, (char*)item + sizeof(RTXUpdateItem),
      item->len, item->commit_id);
    assert(node != NULL);

    while(true){
      RdmaValHeader* header = (RdmaValHeader*)node->value;
      volatile uint64_t l = header->lock;
      volatile uint64_t *lockptr = &(header->lock);
      if(l == 0) {
        LOG(3) << "already unlocked!";
        break;
      }
      if(!__sync_bool_compare_and_swap(lockptr, l ,0)) { // TODO
        LOG(3) << "fail release lock " << id << ' ' << cid;
      }
      else{
        break;
      }
    }
  }
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg, 0, id, cid);
}

bool SUNDIAL::try_renew_all_lease_rdma(uint32_t commit_id, yield_func_t &yield) {
  bool need_yield = false;
  START(renew_lease);
  for(auto& item : read_set_) {
    if(item.pid != node_id_) {
      Qp *qp = get_qp(item.pid);
      assert(qp != NULL);
      char* local_buf = (char*)item.data_ptr - sizeof(RdmaValHeader);
      scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, local_buf, 
        sizeof(RdmaValHeader), item.off, IBV_SEND_SIGNALED);
      need_yield = true;
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
        need_yield = false;
      }
    }
    else {
      assert(false);
    }
  }
  if(need_yield) {
    worker_->indirect_yield(yield);
  }
  need_yield = false;
  for(auto& item : read_set_) {
    RdmaValHeader* header = (RdmaValHeader*)((char*)item.data_ptr - sizeof(RdmaValHeader));
    uint32_t node_rts = RTS(header->seq);
    uint32_t node_wts = WTS(header->seq);
    if(item.wts != node_wts || (commit_id > node_rts && header->lock != 0)) {
      abort_cnt[36]++;
      END(renew_lease);
      return false;
    }
    else {
      if(node_rts < commit_id) {
        header->seq = header->seq & 0xffffffff00000000;
        header->seq += commit_id;
        Qp *qp = get_qp(item.pid);
        assert(qp != NULL);
        RDMAWriteOnlyReq req(cor_id_, 0);
        req.set_write_meta(item.off + sizeof(uint64_t), (char*)header + sizeof(uint64_t),
          sizeof(uint64_t));
        req.post_reqs(scheduler_, qp);
        need_yield = true;
        if(unlikely(qp->rc_need_poll())) {
          worker_->indirect_yield(yield);
          need_yield = false;
        }
      }
    }
    if(need_yield) {
      worker_->indirect_yield(yield);
    }
  }
  END(renew_lease);
  return true;
}

bool SUNDIAL::try_renew_lease_rdma(int index, uint32_t commit_id, yield_func_t &yield) {
  auto& item = read_set_[index];
  START(renew_lease);
  Qp *qp = get_qp(item.pid);
  assert(qp != NULL);
  char* local_buf = (char*)Rmalloc(sizeof(RdmaValHeader));
  RdmaValHeader* header = (RdmaValHeader*)local_buf;

  scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, local_buf, 
    sizeof(RdmaValHeader), item.off, IBV_SEND_SIGNALED);
  worker_->indirect_yield(yield);
  uint64_t l = header->lock;
  uint64_t tss = header->seq;
  uint32_t node_wts = WTS(tss);
  uint32_t node_rts = RTS(tss);
  if(item.wts != node_wts || (commit_id > node_rts && l != 0)) { // !!
    abort_cnt[35]++;
    END(renew_lease);
    return false;
  }
  else {
    if(node_rts < commit_id) {
      header->seq = tss & 0xffffffff00000000;
      header->seq += commit_id;
      RDMAWriteOnlyReq req(cor_id_, 0);
      req.set_write_meta(item.off + sizeof(uint64_t), local_buf + sizeof(uint64_t),
        sizeof(uint64_t));
      req.post_reqs(scheduler_, qp);
      // if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
      // }
    }
    END(renew_lease);
    return true;
  }
  return false;
}

bool SUNDIAL::renew_lease_local(MemNode* node, uint32_t wts, uint32_t commit_id) {
  // atomic?
  // retry?
  assert(node != NULL);
#if ONE_SIDED_READ
  // RdmaValHeader* h = (RdmaValHeader*)((char*)(node->value) - sizeof(RdmaValHeader));
  RdmaValHeader* h = (RdmaValHeader*)((char*)(node->value));
  uint64_t l = h->lock;
  uint64_t tss = h->seq;
#else
  // uint64_t l = node->lock;
  // uint64_t tss = node->read_lock;
  RdmaValHeader* h = (RdmaValHeader*)((char*)(node->value));
  uint64_t l = h->lock;
  uint64_t tss = h->seq;
#endif
  uint32_t node_wts = WTS(tss);
  uint32_t node_rts = RTS(tss);
  if(wts != node_wts || (commit_id > node_rts && l != 0)) { // !!
    return false;
  }
  else {
    if(node_rts < commit_id) {
      uint64_t newl = tss & 0xffffffff00000000;
      newl += commit_id;
// #if ONE_SIDED_READ
      h->seq = newl;
// #else
      // node->read_lock = newl;
// #endif
    }
    else{
    }
    return true;
  }
}

bool SUNDIAL::try_renew_lease_rpc(uint8_t pid, uint8_t tableid, uint64_t key, uint32_t wts, uint32_t commit_id, yield_func_t &yield) {
  // if(pid != response_node_) {
  START(log);
  if(pid != node_id_) {
    START(renew_lease);
    rpc_op<RTXRenewLeaseItem>(cor_id_, RTX_RENEW_LEASE_RPC_ID, pid,
                                 rpc_op_send_buf_,reply_buf_,
                                 /*init RTXRenewLeaseItem*/
                                 pid, tableid, key, wts, commit_id);
    worker_->indirect_yield(yield);
    END(renew_lease);

    uint8_t resp_status = *(uint8_t*)reply_buf_;
    if(resp_status == LOCK_SUCCESS_MAGIC)
      return true;
    else
      return false;
  } else { // local renew lease
    auto node = local_lookup_op(tableid,key);
    assert(node != NULL);
    return renew_lease_local(node, wts, commit_id);
  }
  END(log);
}



void SUNDIAL::renew_lease_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC;
  int request_item_parsed = 0;
  RTX_ITER_ITEM(msg, sizeof(RTXRenewLeaseItem)) {
    auto item = (RTXRenewLeaseItem*)ttptr;
    request_item_parsed++;
    assert(request_item_parsed <= 1);
    if(item->pid != response_node_)
      continue;
    auto node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL);
    if(renew_lease_local(node, item->wts, item->commit_id))
      res = LOCK_SUCCESS_MAGIC;
    else
      res = LOCK_FAIL_MAGIC;
  }
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}

bool SUNDIAL::try_read_rpc(int index, yield_func_t &yield) {
  std::vector<SundialReadSetItem> &set = read_set_;
  assert(index < set.size());
  auto it = set.begin() + index;
  START(read_lat);
  if((*it).pid != node_id_) {
    rpc_op<RTXSundialReadItem>(cor_id_, RTX_READ_RPC_ID, (*it).pid,
                                 rpc_op_send_buf_,reply_buf_,
                                 /*init RTXSundialReadItem*/
                                 (*it).pid, (*it).key, (*it).tableid,(*it).len, txn_start_time);
    worker_->indirect_yield(yield);

    uint8_t resp_status = *(uint8_t*)reply_buf_;
    if(resp_status == LOCK_SUCCESS_MAGIC){
      END(read_lat);
      return true;
    }
    else if (resp_status == LOCK_FAIL_MAGIC){
      END(read_lat);
      return false;
    }
    assert(false);
  }
  else {
    global_lock_manager[0].prepare_buf(reply_buf_, (*it).tableid, (*it).key,
      (*it).len, db_);
    return true;
  }
  END(read_lat);
}


void SUNDIAL::read_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();

  uint8_t res = LOCK_SUCCESS_MAGIC;
  int request_item_parsed = 0;
  size_t nodelen = 0;

  RTX_ITER_ITEM(msg, sizeof(RTXSundialReadItem)) {
    auto item = (RTXSundialReadItem*) ttptr;
    request_item_parsed ++;
    assert(request_item_parsed <= 1);

    if(item->pid != response_node_)
      continue;

    global_lock_manager[0].prepare_buf(reply_msg, item, db_);
    nodelen = item->len + sizeof(SundialResponse);
    goto NEXT_ITEM;
  }
NEXT_ITEM:
  ;
END:
  assert(res == LOCK_SUCCESS_MAGIC);
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t) + nodelen,id,cid);
NO_REPLY:
  ;
}

bool SUNDIAL::try_lock_read_rdma(int index, yield_func_t &yield) {
  std::vector<SundialReadSetItem> &set = write_set_;
  auto it = set.begin() + index;
  START(lock);
  if((*it).pid != node_id_) {
  // if((*it).pid != response_node_) {
    auto off = (*it).off;
    Qp *qp = get_qp((*it).pid);
    assert(qp != NULL);
    char* local_buf = (char*)((*it).data_ptr) - sizeof(RdmaValHeader);
    RdmaValHeader *h = (RdmaValHeader*)local_buf;
    // uint64_t state = 0;
    while(true) {
      // LOG(3) << "lock with " << (int)off;
      // debug
      lock_req_->set_lock_meta(off, 0, txn_start_time, local_buf);
      lock_req_->post_reqs(scheduler_, qp);
      worker_->indirect_yield(yield);
      // if(false) {
      volatile uint64_t newlock = *(uint64_t*)local_buf;
      if(newlock != 0) {
#ifdef SUNDIAL_NOWAIT
        END(lock);
        abort_cnt[0]++;
        return false;
        
#else
        if(txn_start_time < newlock) {
          // continue wait
          worker_->yield_next(yield);
          // state = newlock;
          continue;
        }
        // else if(txn_start_time == newlock) {
        //   // assert(false);
        //   // LOG(3) << txn_start_time;
        // }
        else {
          END(lock);
          abort_cnt[32]++;
          return false;
        }
#endif
      }
      else {
        Qp *qp = get_qp((*it).pid);
        auto off = (*it).off;
        scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, local_buf, 
          (*it).len + sizeof(RdmaValHeader), off, IBV_SEND_SIGNALED);
        worker_->indirect_yield(yield);
        volatile uint64_t l = *((uint64_t*)local_buf);
        ASSERT(l == txn_start_time) << l << ' ' << txn_start_time;
        uint64_t tss = h->seq;
        (*it).wts = WTS(tss);
        (*it).rts = RTS(tss);
#ifdef SUNDIAL_DEBUG
        LOG(3) << "get remote tss " << (*it).wts << ' ' << (*it).rts;
#endif
        commit_id_ = std::max(commit_id_, (*it).rts + 1);
        END(lock);
        return true;
      }
    }
  }
  else {
    // char* local_buf = (char*)((*it).off) - sizeof(RdmaValHeader);
    RdmaValHeader* h = (RdmaValHeader*)((*it).value);
    while(true) {
      volatile uint64_t* lockptr = &(h->lock);
      if(unlikely(!__sync_bool_compare_and_swap(lockptr, 0, txn_start_time))) {
#ifdef SUNDIAL_NOWAIT
        return false;
#else
        volatile uint64_t l = h->lock;
        if(txn_start_time < l || l == 0) {
          worker_->yield_next(yield);
          continue;
        }
        else {
          abort_cnt[34]++;
          return false;
        }
#endif
      }
      else {
        memcpy((*it).data_ptr, (char*)((*it).value) + sizeof(RdmaValHeader), (*it).len);
        uint64_t tss = h->seq;
        (*it).wts = WTS(tss);
        (*it).rts = RTS(tss);
        commit_id_ = std::max(commit_id_, (*it).rts + 1);
        return true;
      }
    }
  }
}

bool SUNDIAL::try_lock_read_rpc(int index, yield_func_t &yield) {
  using namespace rwlock;
  START(lock);
  std::vector<SundialReadSetItem> &set = write_set_;
  auto it = set.begin() + index;
  // if((*it).pid != response_node_) {//
  if((*it).pid != node_id_) {//
    rpc_op<RTXSundialReadItem>(cor_id_, RTX_LOCK_READ_RPC_ID, (*it).pid,
                               rpc_op_send_buf_,reply_buf_,
                               /*init RTXSundialReadItem*/
                               (*it).pid, (*it).key, (*it).tableid,(*it).len,txn_start_time);
    worker_->indirect_yield(yield);
    END(lock);
    // got the response
    uint8_t resp_lock_status = *(uint8_t*)reply_buf_;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC) {
      //LOG(3) << "rpc recv success" << txn_start_time << ' ' << (*it).key;
      return true;
    }
    else if (resp_lock_status == LOCK_FAIL_MAGIC){
      //LOG(3) << "rpc recv fail" << txn_start_time << ' ' << (*it).key;
      return false;
    }
    assert(false);
  } else {
    if(it->node == NULL){
      it->node = local_lookup_op(it->tableid, it->key);
    }
    RdmaValHeader* header = (RdmaValHeader*)((char*)(it->node->value));
    while (true) {
      volatile uint64_t l = header->lock;
#ifdef SUNDIAL_NO_LOCK
      if(false){ // debug
#else
      if(l != 0) {
#endif

#ifdef SUNDIAL_NOWAIT
        return false;
#else
        if(txn_start_time < l){
          worker_->yield_next(yield);  
        }
        else 
        {
          abort_cnt[32]++;
          return false;
        }
#endif
      }
      else {
        volatile uint64_t *lockptr = &(header->lock);
        if( unlikely(!__sync_bool_compare_and_swap(lockptr, 0, txn_start_time)))
          continue;
        else {
          END(lock);
          // get local data
          global_lock_manager[0].prepare_buf(reply_buf_, (*it).tableid, (*it).key,
            (*it).len, db_);
          return true;
        }
      }
    }
  }
}


void SUNDIAL::lock_read_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock;
  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success
  int request_item_parsed = 0;
  MemNode *node = NULL;
  size_t nodelen = 0;

  RTX_ITER_ITEM(msg,sizeof(RTXSundialReadItem)) {
    auto item = (RTXSundialReadItem *)ttptr;
    request_item_parsed++;
    assert(request_item_parsed <= 1); // no batching of lock request.
    if(item->pid != response_node_)
      continue;
    node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL && node->value != NULL);
    RdmaValHeader* header = (RdmaValHeader*)((char*)(node->value));
    while(true) {
      volatile uint64_t l = header->lock;
#ifdef SUNDIAL_NO_LOCK
      if(false){ // debug
#else
      if(l != 0) {
#endif

#ifdef SUNDIAL_NOWAIT
        // abort
        res = LOCK_FAIL_MAGIC;
        goto END;
#else
        if(item->timestamp < l) {
        //if(false) {
          lock_waiter_t waiter = {
              .type = SUNDIAL_REQ_LOCK_READ,
              .pid = id,
              .tid = worker_id_,
              .cid = cid,
              .txn_start_time = item->timestamp,
              .item = *item,
              .db = db_,
            };
          global_lock_manager[worker_id_].add_to_waitlist(&header->lock, waiter);
          //LOG(3) << "add to wait" << l << ' ' << item->key;
          goto NO_REPLY;  
        } 
        else if(item->timestamp == l) {
          assert(false);
        }
        else {
          res = LOCK_FAIL_MAGIC;
          abort_cnt[36]++;
          //LOG(3) << item->timestamp << ' ' << l << ' ' << item->key;
          goto END;
        }
#endif
      }
      else {
        volatile uint64_t *lockptr = &(header->lock);
        if( unlikely(!__sync_bool_compare_and_swap(lockptr, 0, item->timestamp))) { // locked
          continue;
        }
        else {
          //LOG(3) << "lock " << item->timestamp << ' ' << item->key;
          volatile uint64_t *lockptr = &(header->lock);
          assert((*lockptr) == item->timestamp);
          global_lock_manager[0].prepare_buf(reply_msg, item, db_);
          nodelen = item->len + sizeof(SundialResponse);
          goto NEXT_ITEM;
        }
      }
    }
NEXT_ITEM:
    ;
  }

END:
  *((uint8_t *)reply_msg) = res;
  //nodelen = 0; // DEBUG
  rpc_->send_reply(reply_msg,sizeof(uint8_t) + nodelen,id,cid);
NO_REPLY:
  ;
}


void SUNDIAL::release_reads(yield_func_t &yield) {
  return; // no need release read, there is no read lock
}

void SUNDIAL::release_writes(yield_func_t &yield, bool all) {
  START(release_write);
  int release_num = write_set_.size();
  if(!all)
    release_num -= 1;
#if ONE_SIDED_READ
  // the back of write set fail to get lock, no need to unlock
  bool need_yield = false;
  for(int i = 0; i < release_num; ++i) {
    auto& item = write_set_[i];
    // if(item.pid != response_node_) {
    if(item.pid != node_id_) {
      Qp *qp = get_qp(item.pid);
      unlock_req_->set_unlock_meta(item.off);
      unlock_req_->post_reqs(scheduler_, qp);
      need_yield = true;
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
        need_yield = false;
      }
    }
    else {
      RdmaValHeader* h = (RdmaValHeader*)item.value;
      volatile uint64_t* lockptr = &(h->lock);
      volatile uint64_t l = h->lock;
      assert(l == txn_start_time);
      // assert(__sync_bool_compare_and_swap(lockptr, l, 0));
      *lockptr = 0;
    }
  }
  if(need_yield) {
    worker_->indirect_yield(yield);
  }
#else
  using namespace rwlock;
  start_batch_rpc_op(write_batch_helper_);
  bool need_send = false;

  // for(auto it = write_set_.begin();it != write_set_.end();++it) {
  for(int i = 0; i < release_num; ++i) {
    auto& item = write_set_[i];
    if(item.pid != node_id_) { // remote case
    // if(item.pid != response_node_) { // remote case
       //LOG(3) << "rpc releasing"  << (item).key;
      add_batch_entry<RTXSundialUnlockItem>(write_batch_helper_, item.pid,
                                   /*init RTXSundialUnlockItem */
                                   item.pid,item.key,item.tableid);
      need_send = true;
    }
    else {
      auto res = local_try_release_op(item.tableid, item.key, txn_start_time);
    }
  }
  if(need_send) {
    // LOG(3) << "release write once";
    send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);
    worker_->indirect_yield(yield);
  }
#endif
  END(release_write);
}

void SUNDIAL::release_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock;
  int cnt = 0;
  assert(msg != NULL);

  RTX_ITER_ITEM(msg,sizeof(RTXSundialUnlockItem)) {
    auto item = (RTXSundialUnlockItem *)ttptr;
    assert(item != NULL);
    if(item->pid != response_node_)
      continue;
    auto node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL);
    RdmaValHeader* header = (RdmaValHeader*)node->value;
    // volatile uint64_t *lockptr = &(header->lock);
    volatile uint64_t l = header->lock;
    header->lock = 0;
    //LOG(3) << "release " << l << item->key;
    //__sync_bool_compare_and_swap(lockptr, l, 0); // TODO
  }
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}


void SUNDIAL::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&SUNDIAL::lock_read_rpc_handler,this,RTX_LOCK_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::read_rpc_handler,this,RTX_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::renew_lease_rpc_handler,this,RTX_RENEW_LEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::update_rpc_handler,this,RTX_UPDATE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
}

}
}
