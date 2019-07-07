#include "sundial_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {


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
    if(item->pid != response_node_) continue;
    auto node = inplace_write_op(item->tableid, item->key, (char*)item + sizeof(RTXUpdateItem), 
      item->len, item->commit_id);

    // SUNDIAL: release lock
    // need retry?
    volatile uint64_t l = node->lock;
    volatile uint64_t *lockptr = &(node->lock);
    __sync_bool_compare_and_swap(lockptr, l ,WUNLOCK(l));
  }
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply ,will it send?
}

bool SUNDIAL::renew_lease_local(MemNode* node, uint32_t wts, uint32_t commit_id) {
  // atomic?
  // retry?
  uint64_t l = node->lock;
  uint32_t node_wts = WTS(l);
  uint32_t node_rts = RTS(l);
  if(wts != node_wts || (commit_id > node_rts && (WLOCKTS(node->lock) == SUNDIALWLOCK))) {
    return false;
  }
  else {
    if(node_rts < commit_id) {
      uint64_t newl = l & 0xffffffff80000000;
      newl += commit_id;
      node->lock = newl;
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
  std::vector<SundialReadSetItem> &set = write_set_;
  auto it = set.begin() + index;
  if((*it).pid != node_id_) {
    rpc_op<RTXReadItem>(cor_id_, RTX_READ_RPC_ID, (*it).pid, 
                                 rpc_op_send_buf_,reply_buf_, 
                                 /*init RTXReadItem*/  
                                 RTX_REQ_READ_LOCK,
                                 (*it).pid, (*it).key, (*it).tableid,(*it).len,0); // index ?
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
      if(WLOCKTS(l) == SUNDIALWLOCK) {
        worker_->yield_next(yield);
      } else {
        // atomic?
        ++it->node->read_lock;
        // get the header(wts ,rts) and the real value
        global_lock_manager->prepare_buf(rpc_->get_reply_buf(), (*it).tableid, (*it).key,
          (*it).len, db_);
        // atomic?
        --(it->node->read_lock);
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
    // read lock
    while (true) {
      volatile uint64_t l = node->lock;
      if(WLOCKTS(l) == SUNDIALWLOCK) {
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
        ++node->read_lock; // TODO: should be atomic
        // get local data
        global_lock_manager->prepare_buf(reply_msg, item, db_);
        nodelen = item->len + sizeof(SundialResponse);
        --node->read_lock;
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
    else if (resp_lock_status == LOCK_FAIL_MAGIC)
      return false;
    assert(false);
  } else {
      while (true) {
        volatile uint64_t l = it->node->lock;
        volatile uint64_t readl = it->node->read_lock;
        if((WLOCKTS(l) == SUNDIALWLOCK) || (readl > 0)) {
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
      volatile uint64_t readl = node->read_lock;
      if((WLOCKTS(l) == SUNDIALWLOCK) || (readl > 0)) {
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
        if( unlikely(!__sync_bool_compare_and_swap(lockptr, l, l | SUNDIALWLOCK))) // locked
          continue;
        else {
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
}

}
}