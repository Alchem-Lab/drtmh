#include "sundial_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {


void SUNDIAL::write_back(yield_func_t &yield) {
  start_batch_rpc_op(write_batch_helper_);

  for(auto& item : write_set_){
    if(item.pid != node_id_) {
      add_batch_entry<RTXUpdateItem>(write_batch_helper_, item.pid, 
        /* init RTXUpdateItem*/item.pid, item.tableid, item.key, item.len, commit_id_);
      memcpy(write_batch_helper_.req_buf_end_, item.data_ptr, item.len);
      write_batch_helper_.req_buf_end_ += item.len;
    }
    else {
     inplace_write_op(item.node, item.data_ptr, item.len);
    }
  }
  send_batch_rpc_op(write_batch_helper_, cor_id_, RTX_UPDATE_RPC_ID);
  worker_->indirect_yield(yield);
}

void SUNDIAL::update_rpc_handler(int id,int cid,char *msg,void *arg) {
  RTX_ITER_ITEM(msg, sizeof(RTXUpdateItem)) {
    auto item = (RTXUpdateItem*)ttptr;
    ttptr += item->len;
    if(item->pid != response_node_) continue;
    inplace_write_op(item->tableid, item->key, (char*)item + sizeof(RTXUpdateItem), item->len, item->commit_id);
    local_try_release_op(item->tableid,item->key,
                                    rwlock::LOCKED(item->pid));
  }
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}

bool SUNDIAL::renew_lease_rpc(uint8_t pid, uint8_t tableid, uint64_t key, uint32_t wts, uint32_t commit_id, yield_func_t &yield) {
  rpc_op<RTXRenewLeaseItem>(cor_id_, RTX_Renew_Lease_RPC_ID, pid, 
                               rpc_op_send_buf_,reply_buf_, 
                               /*init RTXReadItem*/  
                               pid, tableid, key, wts, commit_id);
  worker_->indirect_yield(yield);

  uint8_t resp_status = *(uint8_t*)reply_buf_;
  if(resp_status == 1) { // ok
    return true;
  }
  return false;
}

void SUNDIAL::renew_lease_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = 1;
  int request_item_parsed = 0;
  RTX_ITER_ITEM(msg, sizeof(RTXRenewLeaseItem)) {
    auto item = (RTXRenewLeaseItem*)ttptr;
    request_item_parsed++;
    assert(request_item_parsed <= 1);
    if(item->pid != response_node_)
      continue;
    auto node = db_->stores_[item->tableid]->Get(item->key);
    assert(node != NULL);

    uint32_t &node_wts = *(uint32_t*)(&(node->read_lock));
    uint32_t &node_rts = *((uint32_t*)(&(node->read_lock)) + 1);

    if(item->wts != node_wts || (item->commit_id > node_rts && node->lock & 0x1 == rwlock::W_LOCKED))
      res = 0;
    else
      if(node_rts < item->commit_id)
        node_rts = item->commit_id;
  }
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}

bool SUNDIAL::try_remote_read_rpc(int index, yield_func_t &yield) {
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
    if(resp_status == 1) 
      return true;
    else if (resp_status == 0)
      return false;
    assert(false);
  } else {
    assert(false); // read from local
  }

}

bool SUNDIAL::try_lock_write_w_rwlock_rpc(int index, yield_func_t &yield) {
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
    // uint8_t* wrts_ptr = (uint8_t*)reply_buf_ + 1;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC) { 
      // get remote wts and rts
      // it->wts = *((uint32_t*)wrts_ptr);
      // it->rts = *((uint32_t*)wrts_ptr + 1);
      return true;
    }
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
              // get local wts and rts
              it->wts = *((uint32_t*)(&it->node->read_lock));
              it->rts = *(((uint32_t*)(&it->node->read_lock) + 1));
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

void SUNDIAL::read_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  char* reply = reply_msg + 1;

  uint8_t res = 1;
  int request_item_parsed = 0;
  MemNode *node = NULL;
  size_t nodelen = 0;

  RTX_ITER_ITEM(msg, sizeof(RTXReadItem)) {
    auto item = (RTXReadItem*) ttptr;
    request_item_parsed ++;
    assert(request_item_parsed <= 1);

    if(item->pid != response_node_)
      continue;

    node = db_->stores_[item->tableid]->Get(item->key);
    assert(node != NULL && node->value != NULL);

    uint64_t seq;
    auto node = local_get_op(item->tableid, item->key, reply + sizeof(SundialResponse),
      item->len, seq, db_->_schemas[item->tableid].meta_len);
    SundialResponse *reply_item = (SundialResponse *)reply;
    reply_item->wts = *((uint32_t*)node->read_lock);
    reply_item->rts = *((uint32_t*)node->read_lock + 1);
    nodelen = item->len + sizeof(SundialResponse);
    goto NEXT_ITEM;
  }
NEXT_ITEM:
  ;
END:
  assert(res != 0);
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t) + nodelen,id,cid);
}


void SUNDIAL::lock_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock;

  char* reply_msg = rpc_->get_reply_buf();
  char* reply = reply_msg + 1;

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
          uint64_t l = node->lock;
          if(l & 0x1 == W_LOCKED) {
            continue; // TODO: now is busy waiting, can optimize?
          } else {
            if (EXPIRED(END_TIME(l))) {
              // clear expired lease (optimization)
              volatile uint64_t *lockptr = &(node->lock);
              if( unlikely(!__sync_bool_compare_and_swap(lockptr,l, LOCKED(item->pid)))) // locked
                continue;
              else{
                uint64_t seq;
                auto node = local_get_op(item->tableid, item->key, reply + sizeof(SundialResponse),
                  item->len, seq, db_->_schemas[item->tableid].meta_len);
                SundialResponse *reply_item = (SundialResponse *)reply;
                reply_item->wts = *((uint32_t*)node->read_lock);
                reply_item->rts = *((uint32_t*)node->read_lock + 1);
                nodelen = item->len + sizeof(SundialResponse);
                goto NEXT_ITEM;
              }
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
  rpc_->send_reply(reply_msg,sizeof(uint8_t) + nodelen,id,cid);
}



void SUNDIAL::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&SUNDIAL::read_write_rpc_handler,this,RTX_RW_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::read_rpc_handler,this,RTX_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::commit_rpc_handler,this,RTX_COMMIT_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::renew_lease_rpc_handler,this,RTX_Renew_Lease_RPC_ID);
}
}
}