#include "sundial_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {
bool SUNDIAL::try_lock_write_w_rwlock_rpc(int index, yield_func_t &yield) {
  using namespace rwlock;
  START(lock);
  std::vector<SundialReadSetItem> &set = write_set_;
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
    uint8_t* wrts_ptr = (uint8_t*)reply_buf_ + 1;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC) { 
      // get remote wts and rts
      it->wts = *((uint32_t*)wrts_ptr);
      it->rts = *((uint32_t*)wrts_ptr + 1);
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
              it->wts = *((uint32_t*)it->node->read_lock);
              it->rts = *(((uint32_t*)it->node->read_lock) + 1);
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


void SUNDIAL::lock_rpc_handler(int id,int cid,char *msg,void *arg) {
  using namespace rwlock;

  char* reply_msg = rpc_->get_reply_buf();

  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  int request_item_parsed = 0;
  MemNode *node = NULL;

  RTX_ITER_ITEM(msg,sizeof(RTXLockRequestItem)) {

    auto item = (RTXLockRequestItem *)ttptr;

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
              if( unlikely(!__sync_bool_compare_and_swap(lockptr,l, LOCKED(item->pid))))
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
  // send message back (TODO: need send data, wts, rts back)
  assert(res != LOCK_WAIT_MAGIC);
  *((uint8_t *)reply_msg) = res;
  // as read lock is not in used in sundial, we for the moment use it to contain wts and rts
  *((uint64_t*)((uint8_t *)reply_msg + 1)) = node->read_lock; 
  rpc_->send_reply(reply_msg,sizeof(uint8_t) * 9,id,cid);
}



void SUNDIAL::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&SUNDIAL::read_write_rpc_handler,this,RTX_RW_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&SUNDIAL::commit_rpc_handler,this,RTX_COMMIT_RPC_ID);
}
}
}