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
     inplace_write_op(item.node, item.data_ptr, item.len);
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
    inplace_write_op(item->tableid, item->key, (char*)item + sizeof(RTXUpdateItem), item->len, item->commit_id);
    MemNode *node = db_->stores_[item->tableid]->Get(item->key);
    uint64_t l = node->lock;
    volatile uint64_t *lockptr = &(node->lock);
    __sync_bool_compare_and_swap(lockptr, l ,WUNLOCK(l)); // release the lock
    // *lockptr =  WUNLOCK(l);  
  }
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}

bool SUNDIAL::try_renew_lease_rpc(uint8_t pid, uint8_t tableid, uint64_t key, uint32_t wts, uint32_t commit_id, yield_func_t &yield) {
  if(pid != node_id_) {
    rpc_op<RTXRenewLeaseItem>(cor_id_, RTX_Renew_Lease_RPC_ID, pid, 
                                 rpc_op_send_buf_,reply_buf_, 
                                 /*init RTXReadItem*/  
                                 pid, tableid, key, wts, commit_id);
    worker_->indirect_yield(yield);

    uint8_t resp_status = *(uint8_t*)reply_buf_;
    if(resp_status == RPC_SUCCESS_MAGIC) { // ok
      return true;
    }
    return false;
  } else { // local renew lease
    auto node = db_->stores_[tableid]->Get(key);
    assert(node != NULL);
    uint32_t &node_wts = *(uint32_t*)(&(node->read_lock));
    uint32_t &node_rts = *((uint32_t*)(&(node->read_lock)) + 1);
    if(wts != node_wts || (commit_id > node_rts && (node->lock & 0x1) == rwlock::W_LOCKED)) {
      return false;
    }
    else
      if(node_rts < commit_id) {
        node_rts = commit_id;
        return true;
      }
  }
}



void SUNDIAL::renew_lease_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = RPC_SUCCESS_MAGIC;
  int request_item_parsed = 0;
  RTX_ITER_ITEM(msg, sizeof(RTXRenewLeaseItem)) {
    auto item = (RTXRenewLeaseItem*)ttptr;
    request_item_parsed++;
    assert(request_item_parsed <= 1);
    if(item->pid != response_node_)
      continue;
    auto node = db_->stores_[item->tableid]->Get(item->key);
    assert(node != NULL);
//
    uint32_t &node_wts = *(uint32_t*)(&(node->read_lock));
    uint32_t &node_rts = *((uint32_t*)(&(node->read_lock)) + 1);

    if(item->wts != node_wts || (item->commit_id > node_rts && (node->lock & 0x1) == rwlock::W_LOCKED))
      res = RPC_FAIL_MAGIC;
    else
      if(node_rts < item->commit_id)
        node_rts = item->commit_id;
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
    if(resp_status == RPC_SUCCESS_MAGIC)
      return true;
    else if (resp_status == RPC_FAIL_MAGIC)
      return false;
    assert(false);
  } else {
    while(true) {
      volatile uint64_t l = it->node->lock;
      if(WLOCKTS(l) == SUNDIALWLOCK) {
        worker_->yield_next(yield);
      } else {
        ++it->node->read_lock;
        global_lock_manager->prepare_buf(rpc_->get_reply_buf(), (*it).tableid, (*it).key,
          (*it).len, db_);
        --(it->node->read_lock);
        return true;
      }
    }
  }
}


void SUNDIAL::read_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();

  uint8_t res = RPC_SUCCESS_MAGIC;
  int request_item_parsed = 0;
  MemNode *node = NULL;
  size_t nodelen = 0;

  RTX_ITER_ITEM(msg, sizeof(RTXReadItem)) {
    auto item = (RTXReadItem*) ttptr;
    request_item_parsed ++;
    assert(request_item_parsed <= 1);

    if(item->pid != response_node_)
      continue;

    uint64_t seq;
    node = db_->stores_[item->tableid]->Get(item->key);
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
  assert(res == RPC_SUCCESS_MAGIC);
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
    rpc_op<RTXReadItem>(cor_id_, RTX_LOCK_RPC_ID, (*it).pid, 
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


void SUNDIAL::lock_rpc_handler(int id,int cid,char *msg,void *arg) {
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

    node = db_->stores_[item->tableid]->Get(item->key);
    assert(node != NULL && node->value != NULL);

    switch(item->type) {
      case RTX_REQ_LOCK_WRITE: {
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
      }
        break;
      default:
        assert(false);
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



void SUNDIAL::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&SUNDIAL::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::read_rpc_handler,this,RTX_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::renew_lease_rpc_handler,this,RTX_Renew_Lease_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::update_rpc_handler,this,RTX_UPDATE_RPC_ID);
}

}
}