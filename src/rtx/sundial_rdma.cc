#include "sundial_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {

bool SUNDIAL::try_update_rdma(yield_func_t &yield) {
  RDMAWriteReq req(cor_id_,0 /* whether to use passive ack*/);
  for(auto& item : write_set_){
    if(item.pid != node_id_) {
      RdmaValHeader *node = (RdmaValHeader*)(item.data_ptr - sizeof(RdmaValHeader));
      Qp *qp = get_qp(item.pid);
      assert(qp != NULL);
      req.set_write_meta(item.off + sizeof(RdmaValHeader), item.data_ptr, item.len);
      req.set_unlock_meta(item.off);
      req.post_reqs(scheduler_, qp);
      // if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
      // }
    }
    else {
      LOG(3) << "in else";
      memcpy((char*)item.off, item.data_ptr, item.len);
      volatile void* lockptr = (char*)item.off - sizeof(RdmaValHeader);
      uint64_t l = ((RdmaValHeader*)lockptr)->lock;
      assert(__sync_bool_compare_and_swap((uint64_t*)lockptr, l, 0));
    }
  }
  worker_->indirect_yield(yield);
  return true;
}

bool SUNDIAL::try_update_rpc(yield_func_t &yield) {
  start_batch_rpc_op(write_batch_helper_);
  for(auto& item : write_set_){
    if(item.pid != node_id_) {
      add_batch_entry<RTXUpdateItem>(write_batch_helper_, item.pid,
        /* init RTXUpdateItem*/item.pid, item.tableid, item.key, item.len, commit_id_);
      memcpy(write_batch_helper_.req_buf_end_, item.data_ptr, item.len);
      write_batch_helper_.req_buf_end_ += item.len;
    }
    else { // local
     auto node = inplace_write_op(item.tableid, item.key, item.data_ptr, item.len, commit_id_);
     assert(node != NULL);
     uint64_t l = node->lock;
     volatile uint64_t *lockptr = &(node->lock);
     __sync_bool_compare_and_swap(lockptr, l, WUNLOCK(l));
     // *lockptr = WUNLOCK(l);
    }
  }
  send_batch_rpc_op(write_batch_helper_, cor_id_, RTX_UPDATE_RPC_ID);
  worker_->indirect_yield(yield);
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
      volatile uint64_t l = node->lock;
      volatile uint64_t *lockptr = &(node->lock);
      if(l == WUNLOCK(l)) {
        LOG(3) << "already unlocked!";
        break;
      }
      if(!__sync_bool_compare_and_swap(lockptr, l ,WUNLOCK(l))){
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

bool SUNDIAL::renew_lease_local(MemNode* node, uint32_t wts, uint32_t commit_id) {
  // atomic?
  // retry?
  assert(node != NULL);
#if ONE_SIDED_READ
  RdmaValHeader* h = (RdmaValHeader*)((char*)(node->value) - sizeof(RdmaValHeader));
  uint64_t l = h->lock;
  uint64_t tss = h->seq;
#else
  uint64_t l = node->lock;
  uint64_t tss = node->read_lock;
#endif
  uint32_t node_wts = WTS(tss);
  uint32_t node_rts = RTS(tss);
  if(wts != node_wts || (commit_id > node_rts && WLOCKTS(node->lock))) {
    // LOG(3) << "no renew " << wts << ' ' << node_wts << ' ' << commit_id << ' ' << node_rts << ' '
    //   << (int)(WLOCKTS(node->lock));
    return false;
  }
  else {
    if(node_rts < commit_id) {
      LOG(3) << "success renew lease" << node_rts << ' ' << commit_id ;
      uint64_t newl = tss & 0xffffffff00000000;
      newl += commit_id;
#if ONE_SIDED_READ
      h->seq = newl;
#else
      node->read_lock = newl;
#endif
    }
    else{
      LOG(3) << "nosu renew lease";
    }
    return true;
  }
}

bool SUNDIAL::try_renew_lease_rpc(uint8_t pid, uint8_t tableid, uint64_t key, uint32_t wts, uint32_t commit_id, yield_func_t &yield) {
  if(pid != node_id_) {
    rpc_op<RTXRenewLeaseItem>(cor_id_, RTX_RENEW_LEASE_RPC_ID, pid,
                                 rpc_op_send_buf_,reply_buf_,
                                 /*init RTXRenewLeaseItem*/
                                 pid, tableid, key, wts, commit_id);
    worker_->indirect_yield(yield);

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
  if((*it).pid != node_id_) {
    rpc_op<RTXReadItem>(cor_id_, RTX_READ_RPC_ID, (*it).pid,
                                 rpc_op_send_buf_,reply_buf_,
                                 /*init RTXReadItem*/
                                 RTX_REQ_READ_LOCK,
                                 (*it).pid, (*it).key, (*it).tableid,(*it).len,0);
    worker_->indirect_yield(yield);

    uint8_t resp_status = *(uint8_t*)reply_buf_;
    if(resp_status == LOCK_SUCCESS_MAGIC)
      return true;
    else if (resp_status == LOCK_FAIL_MAGIC)
      return false;
    assert(false);
  } else {
    while(true) {
      volatile uint64_t l = it->node->lock;
#ifdef SUNDIAL_NO_LOCK
      if(false){ // debug
#else
      // if(WLOCKTS(l)) {
      if(false){ // when read, dont have to care about write lock
#endif

        worker_->yield_next(yield);
      } else {
        // atomic?
        // ++it->node->read_lock;
        it->node->lock += READLOCKADDONE;
        // get the header(wts ,rts) and the real value
        global_lock_manager->prepare_buf(rpc_->get_reply_buf(), (*it).tableid, (*it).key,
          (*it).len, db_);
        // atomic?
        // --(it->node->read_lock);
        it->node->lock -= READLOCKADDONE;
        return true;
      }
    }
  }
}


void SUNDIAL::read_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();

  uint8_t res = LOCK_SUCCESS_MAGIC;
  int request_item_parsed = 0;
  MemNode *node = NULL;
  size_t nodelen = 0;

  RTX_ITER_ITEM(msg, sizeof(RTXReadItem)) {
    auto item = (RTXReadItem*) ttptr;
    request_item_parsed ++;
    assert(request_item_parsed <= 1);

    if(item->pid != response_node_)
      continue;

    node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL);
    // read lock
    while (true) {
      volatile uint64_t l = node->lock;
#ifdef SUNDIAL_NO_LOCK
      assert(false);
      if (false){ // debug
#else
      // if(WLOCKTS(l)) {
      if (false) { // do not have to wait for the write lock
#endif
        lock_waiter_t waiter = {
                  .type = SUNDIAL_REQ_READ,
                  .pid = id,
                  .tid = worker_id_,
                  .cid = cid,
                  .txn_start_time = 0,
                  .item = *item,
                  .db = db_,
                };
        global_lock_manager->add_to_waitlist(&node->lock, waiter);
        goto NO_REPLY;
      }
      else {
        // ++node->read_lock; // TODO: should be atomic
        node->lock += READLOCKADDONE;
        // get local data
        global_lock_manager->prepare_buf(reply_msg, item, db_);
        nodelen = item->len + sizeof(SundialResponse);
        // --node->read_lock;
        node->lock -= READLOCKADDONE;
        goto NEXT_ITEM;
      }
    }
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
  if((*it).pid != node_id_) {
    auto off = (*it).off;
    Qp *qp = get_qp((*it).pid);
    assert(qp != NULL);
    char* local_buf = (char*)((*it).data_ptr) - sizeof(RdmaValHeader);
    RdmaValHeader *h = (RdmaValHeader*)local_buf;
    while(true) {
      // LOG(3) << "lock with " << (int)off;
      lock_req_->set_lock_meta(off, 0, SUNDIALWLOCK, local_buf);
      lock_req_->post_reqs(scheduler_, qp);
      // if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
      // }
      if(h->lock != 0) { // fail to lock remote
      // if(false) {
        // debug
        // LOG(3) << "fail to get lock " << (int)(h->lock);
        continue;
      }
      else { // get the lock
        // LOG(3) << "success get lock";
        off = rdma_read_val((*it).pid, (*it).tableid, (*it).key, (*it).len,
          local_buf, yield, sizeof(RdmaValHeader)); // reread the data, can optimize
        uint64_t tss = h->seq;
        (*it).wts = WTS(tss);
        (*it).rts = RTS(tss);
        return true;
      }
    }
  }
  else {
    char* local_buf = (char*)((*it).off) - sizeof(RdmaValHeader);
    RdmaValHeader* h = (RdmaValHeader*)local_buf;
    while(true) {
      volatile RdmaValHeader* lockptr = h;
      if(unlikely(!__sync_bool_compare_and_swap((uint64_t*)lockptr, 0, 1)))
        continue;
      else {
        memcpy((*it).data_ptr, (char*)((*it).value), (*it).len);
        RdmaValHeader* h = (RdmaValHeader*)((char*)((*it).value) - 2*sizeof(uint64_t));
        uint64_t tss = h->seq;
        (*it).wts = WTS(tss);
        (*it).rts = RTS(tss);
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
  if((*it).pid != node_id_) {
    rpc_op<RTXReadItem>(cor_id_, RTX_LOCK_READ_RPC_ID, (*it).pid,
                               rpc_op_send_buf_,reply_buf_,
                               /*init RTXReadItem*/
                               RTX_REQ_READ_LOCK,
                               (*it).pid, (*it).key, (*it).tableid,(*it).len,0); // index ?
    worker_->indirect_yield(yield);
    END(lock);
    // got the response
    uint8_t resp_lock_status = *(uint8_t*)reply_buf_;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC) {
      return true;
    }
    else if (resp_lock_status == LOCK_FAIL_MAGIC){
      assert(false);
      return false;
    }
    assert(false);
  } else {
    assert(it->node != NULL);
    while (true) {
      volatile uint64_t l = it->node->lock;
      // volatile uint64_t readl = it->node->read_lock;

#ifdef SUNDIAL_NO_LOCK
      if(false){ // debug
#else
      if((WLOCKTS(l)) || RLOCKTS(l)) {
#endif
        worker_->yield_next(yield);
      } else {
        volatile uint64_t *lockptr = &(it->node->lock);
        if( unlikely(!__sync_bool_compare_and_swap(lockptr, l, l | SUNDIALWLOCK)))
          continue;
        else {
          END(lock);
          // get local data
          global_lock_manager->prepare_buf(rpc_->get_reply_buf(), (*it).tableid, (*it).key,
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

  RTX_ITER_ITEM(msg,sizeof(RTXReadItem)) {

    auto item = (RTXReadItem *)ttptr;

    // no batching of lock request.
    request_item_parsed++;
    assert(request_item_parsed <= 1);

    if(item->pid != response_node_)
      continue;

    node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL && node->value != NULL);

    while(true) {
      volatile uint64_t l = node->lock;
      // volatile uint64_t readl = node->read_lock;

#ifdef SUNDIAL_NO_LOCK
      if(false){ // debug
#else
      if(WLOCKTS(l) || RLOCKTS(l)) {
        // if(true) {
        // if(false){
#endif
        lock_waiter_t waiter = {
              .type = SUNDIAL_REQ_LOCK_READ,
              .pid = id,
              .tid = worker_id_,
              .cid = cid,
              .txn_start_time = 0,
              .item = *item,
              .db = db_,
            };
        global_lock_manager->add_to_waitlist(&node->lock, waiter);
        goto NO_REPLY;
      } else {
        volatile uint64_t *lockptr = &(node->lock);
        // if( unlikely(!__sync_bool_compare_and_swap(lockptr, l, l | SUNDIALWLOCK))) { // locked
        if( unlikely(!__sync_bool_compare_and_swap(lockptr, 0, SUNDIALWLOCK))) { // locked
          continue;
        }
        else {
          volatile uint64_t *lockptr = &(node->lock);
          assert((*lockptr) != WUNLOCK(*lockptr));
          global_lock_manager->prepare_buf(reply_msg, item, db_);
          nodelen = item->len + sizeof(SundialResponse);
          goto NEXT_ITEM;
        }
      }
    }

NEXT_ITEM:
    ;
  }

END:
  assert(res == LOCK_SUCCESS_MAGIC);
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t) + nodelen,id,cid);
NO_REPLY:
  ;
}


void SUNDIAL::release_reads(yield_func_t &yield) {
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

void SUNDIAL::release_writes(yield_func_t &yield) {
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



void SUNDIAL::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&SUNDIAL::lock_read_rpc_handler,this,RTX_LOCK_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::read_rpc_handler,this,RTX_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::renew_lease_rpc_handler,this,RTX_RENEW_LEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::update_rpc_handler,this,RTX_UPDATE_RPC_ID);
  // ROCC_BIND_STUB(rpc_,&SUNDIAL::read_rdma_rpc_handler,this,RTX_RDMA_READ_RPC_ID);
}

}
}
