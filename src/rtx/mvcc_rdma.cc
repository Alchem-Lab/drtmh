#include "mvcc_rdma.h"
#include "rdma_req_helper.hpp"
namespace nocc {

namespace rtx {


void MVCC::release_reads(yield_func_t &yield) {
  return; // no need release read, there is no read lock
}

void MVCC::release_writes(yield_func_t &yield, bool all) {
  int release_num = write_set_.size();
  if(!all) {
    release_num -= 1;
  }
#if ONE_SIDED_READ
#else
  start_batch_rpc_op(write_batch_helper_);
  bool need_send = false;

  for(int i = 0; i < release_num; ++i) {
    auto& item = write_set_[i];
    if(item.pid != node_id_) {
      add_batch_entry<RTXMVCCUnlockItem>(write_batch_helper_, item.pid,
                                   /*init RTXMVCCUnlockItem */
                                   item.pid,item.key,item.tableid,txn_start_time);
      need_send = true;
    }
    else {
      auto node = local_lookup_op(item.tableid, item.key);
      MVCCHeader* header = (MVCCHeader*)node->value;
      ASSERT(header->lock == txn_start_time) << "release lock: "
        << header->lock << "!=" << txn_start_time;
      header->lock = 0;
    }
  }
  if(need_send) {
    send_batch_rpc_op(write_batch_helper_, cor_id_, RTX_RELEASE_RPC_ID);
    worker_->indirect_yield(yield);
  }
#endif
}

bool MVCC::try_read_rpc(int index, yield_func_t &yield) {
  std::vector<ReadSetItem> &set = read_set_;
  assert(index < set.size());
  auto it = set.begin() + index;
  START(temp);
  if((*it).pid != node_id_) {
    rpc_op<RTXMVCCWriteRequestItem>(cor_id_, RTX_READ_RPC_ID, (*it).pid,
                                 rpc_op_send_buf_,reply_buf_,
                                 /*init RTXMVCCWriteRequestItem*/
                                 (*it).pid, (*it).key, (*it).tableid,(*it).len, txn_start_time);
    worker_->indirect_yield(yield);
    uint8_t resp_status = *(uint8_t*)reply_buf_;
    if(resp_status == LOCK_SUCCESS_MAGIC)
      return true;
    else if (resp_status == LOCK_FAIL_MAGIC)
      return false;
    assert(false);
  }
  else {
    if((*it).node == NULL) {
      (*it).node = local_lookup_op((*it).tableid, (*it).key);
    }
    MVCCHeader* header = (MVCCHeader*)((*it).node->value);
    int pos = -1;
    if((pos = check_read(header, txn_start_time)) == -1) return false; // cannot read
    while(true) {
      volatile uint64_t rts = header->rts;
      volatile uint64_t* rts_ptr = &(header->rts);
      if(txn_start_time > rts) {
        if(!__sync_bool_compare_and_swap(rts_ptr, rts, txn_start_time)) {
          continue;
        }
        else {
          break;
        }
      }
      else {
        break;
      }
    }
    uint64_t before_reading_wts = header->wts[pos];
    // read the data here
    char* raw_data = (char*)((*it).node->value) + sizeof(MVCCHeader);
    if((*it).data_ptr == NULL)
      (*it).data_ptr = (char*)malloc((*it).len);
    memcpy((*it).data_ptr, raw_data + pos * (*it).len, (*it).len);
    int new_pos = check_read(header, txn_start_time);
    if(new_pos != pos || header->wts[new_pos] != before_reading_wts) {
      return false;
    }
  }
  END(temp);
  return true;
}


// rpc
bool MVCC::try_lock_read_rpc(int index, yield_func_t &yield) {
  START(lock);
  std::vector<ReadSetItem> &set = write_set_;
  auto it = set.begin() + index;
  if((*it).pid != node_id_) {//
    rpc_op<RTXMVCCWriteRequestItem>(cor_id_, RTX_LOCK_READ_RPC_ID, (*it).pid,
                               rpc_op_send_buf_,reply_buf_,
                               /*init RTXMVCCWriteRequestItem*/
                               (*it).pid, (*it).key, (*it).tableid,(*it).len,txn_start_time);
    worker_->indirect_yield(yield);
    END(lock);
    // got the response
    uint8_t resp_lock_status = *(uint8_t*)reply_buf_;
    if(resp_lock_status == LOCK_SUCCESS_MAGIC) {
      return true;
    }
    else if (resp_lock_status == LOCK_FAIL_MAGIC){
      return false;
    }
    assert(false);
  } 
  else {
    if((*it).node == NULL) {
      (*it).node = local_lookup_op((*it).tableid, (*it).key);
    }
    MVCCHeader *header = (MVCCHeader*)((*it).node->value);
    if(!check_write(header, txn_start_time)) return false;
    volatile uint64_t l = header->lock;
    if(l > txn_start_time) return false;
    while (true) {
      volatile uint64_t* lockptr = &(header->lock);
      if(unlikely(!__sync_bool_compare_and_swap(lockptr, 0, txn_start_time))) {
        worker_->yield_next(yield);
        continue;
      }
      else { // get the lock
        if(header->rts > txn_start_time) {
          *lockptr = 0;
          return false;
        }
        uint64_t max_wts = 0, min_wts = 0xffffffffffffffff;
        int pos = -1;
        for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
          if(header->wts[i] > max_wts) {
            max_wts = header->wts[i];
          }
          if(header->wts[i] < min_wts) {
            min_wts = header->wts[i];
            pos = i;
          }
        }
        if(max_wts > txn_start_time) {
          *lockptr = 0;
          return false;
        }
        (*it).seq = (uint64_t)pos;
        assert((*it).seq < MVCC_VERSION_NUM);
        if((*it).data_ptr == NULL) 
          (*it).data_ptr = (char*)malloc((*it).len);
        char* raw_data = (char*)((*it).node->value) + sizeof(MVCCHeader);
        memcpy((*it).data_ptr, raw_data + pos * (*it).len, (*it).len);
      }
    }
  }
  return true;
}

bool MVCC::try_update_rpc(yield_func_t &yield) {
  start_batch_rpc_op(write_batch_helper_);
  bool need_send = false;
  START(commit);
  for(auto& item : write_set_) {
    if(item.pid != node_id_) {
      need_send = true;
      assert(item.seq < MVCC_VERSION_NUM);
      add_batch_entry<RTXMVCCUpdateItem>(write_batch_helper_, item.pid,
        /* init RTXMVCCUpdateItem*/
        item.pid, item.key, item.tableid, 
        item.len, (uint16_t)item.seq, txn_start_time);
      assert(item.data_ptr != NULL);
      memcpy(write_batch_helper_.req_buf_end_, item.data_ptr, item.len);
      write_batch_helper_.req_buf_end_ += item.len;
    }
    else {
      assert(item.data_ptr != NULL);
      assert(item.seq < MVCC_VERSION_NUM);
      auto node = local_lookup_op(item.tableid, item.key);
      assert(node != NULL);
      MVCCHeader* header = (MVCCHeader*)node->value;
      ASSERT(header->lock == txn_start_time) << "release lock: "
        << header->lock << "!=" << txn_start_time;      
      int pos = (int)item.seq;
      header->wts[pos] = txn_start_time;
      // LOG(3) << txn_start_time;
      char* raw_data = (char*)node->value + sizeof(MVCCHeader);
      memcpy(raw_data + pos * item.len, item.data_ptr, item.len);
      header->lock = 0;
    }
  }
  if(need_send) {
    send_batch_rpc_op(write_batch_helper_, cor_id_, RTX_UPDATE_RPC_ID);
    worker_->indirect_yield(yield);
  }
  END(commit);
  return true;
}

void MVCC::read_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success
  int request_item_parsed = 0;
  MemNode *node = NULL;
  size_t nodelen = 0;

  RTX_ITER_ITEM(msg,sizeof(RTXMVCCWriteRequestItem)) {
    auto item = (RTXMVCCWriteRequestItem *)ttptr;
    request_item_parsed++;
    assert(request_item_parsed <= 1); // no batching of lock request.
    if(item->pid != response_node_)
      continue;
    node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL && node->value != NULL);
    MVCCHeader* header = (MVCCHeader*)(node->value);
    int pos = -1;
    if((pos = check_read(header, item->txn_starting_timestamp)) == -1) {
      res = LOCK_FAIL_MAGIC;
      goto END;
    }
    while(true) {
      volatile uint64_t rts = header->rts;
      volatile uint64_t* rts_ptr = &(header->rts);
      if(item->txn_starting_timestamp > rts) {
        if(!__sync_bool_compare_and_swap(rts_ptr, rts, item->txn_starting_timestamp)) {
          continue;
        }
        else break;
      }
      else break;
    }
    uint64_t before_reading_wts = header->wts[pos];
    char* raw_data = (char*)(node->value) + sizeof(MVCCHeader);
    char* reply = reply_msg + 1;
    *(uint64_t*)reply = (uint64_t)pos;
    memcpy(reply + sizeof(uint64_t), raw_data + pos * item->len, item->len);
    int new_pos = check_read(header, item->txn_starting_timestamp);
    if(new_pos != pos || header->wts[new_pos] != before_reading_wts) {
      res = LOCK_FAIL_MAGIC;
      goto END;
    }
    nodelen = sizeof(uint64_t) + item->len;
    goto END;
  }
END:
  *((uint8_t*)reply_msg) = res;
  rpc_->send_reply(reply_msg, sizeof(uint8_t) + nodelen, id, cid);
NO_REPLY:
  ;
}


void MVCC::lock_read_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success
  int request_item_parsed = 0;
  MemNode *node = NULL;
  size_t nodelen = 0;
  RTX_ITER_ITEM(msg,sizeof(RTXMVCCWriteRequestItem)) {
    auto item = (RTXMVCCWriteRequestItem *)ttptr;
    request_item_parsed++;
    assert(request_item_parsed <= 1); // no batching of lock request.
    if(item->pid != response_node_)
      continue;
    // LOG(3) << (int)item->tableid << ' ' << item->key;
    node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL && node->value != NULL);
    MVCCHeader* header = (MVCCHeader*)(node->value);
    assert(header != NULL);
    if(!check_write(header, item->txn_starting_timestamp)) {
      res = LOCK_FAIL_MAGIC;
      goto END;
    }
    volatile uint64_t l = header->lock;
    if(l > item->txn_starting_timestamp) {
      res = LOCK_FAIL_MAGIC;
      goto END;
    }
    while(true) {
      volatile uint64_t* lockptr = &(header->lock);
      if(unlikely(!__sync_bool_compare_and_swap(lockptr, 0, item->txn_starting_timestamp))) {
#ifdef MVCC_NOWAIT
        res = LOCK_FAIL_MAGIC;
        goto END;
#else
        assert(false);
#endif
      }
      else {
        volatile uint64_t rts = header->rts;
        if(rts > item->txn_starting_timestamp) {
          res = LOCK_FAIL_MAGIC;
          header->lock = 0; // release lock
          goto END;
        }
        uint64_t max_wts = 0, min_wts = 0xffffffffffffffff;
        int pos = -1;
        for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
          // LOG(3) << header->wts[i] << " $$$ " << min_wts;
          // fprintf(stderr, "%x\n", header->wts[i]);
          if(header->wts[i] > max_wts) {
            max_wts = header->wts[i];
          }
          if(header->wts[i] < min_wts) {
            pos = i;
            min_wts = header->wts[i];
          }
        }
        if(max_wts > item->txn_starting_timestamp) {
          res = LOCK_FAIL_MAGIC;
          header->lock = 0;
          goto END;
        }
        assert(pos != -1);
        char* reply = (char*)reply_msg + 1;
        *(uint64_t*)reply = (uint64_t)pos;
        assert((uint64_t)pos < MVCC_VERSION_NUM);
        char* raw_data = (char*)(node->value) + sizeof(MVCCHeader);
        memcpy(reply + sizeof(uint64_t), raw_data + pos * item->len, item->len);
        nodelen = sizeof(uint64_t) + item->len;
        goto END;
      }
    }
  }
END:
  *((uint8_t*)reply_msg) = res;
  rpc_->send_reply(reply_msg, sizeof(uint8_t) + nodelen, id, cid);
NO_REPLY:
  ;
}

void MVCC::release_rpc_handler(int id, int cid, char* msg, void* arg) {
  int cnt = 0;
  RTX_ITER_ITEM(msg, sizeof(RTXMVCCUnlockItem)) {
    auto item = (RTXMVCCUnlockItem*)ttptr;
    assert(item != NULL);
    if(item->pid != response_node_)
      continue;
    auto node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL);
    MVCCHeader* header = (MVCCHeader*)node->value;
    ASSERT(header->lock == item->txn_starting_timestamp) << "release lock: "
    << header->lock << "!=" << item->txn_starting_timestamp;
    header->lock = 0;
  }
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg, 0, id, cid);
}

void MVCC::update_rpc_handler(int id, int cid, char* msg, void* arg) {
  RTX_ITER_ITEM(msg, sizeof(RTXMVCCUpdateItem)) {
    auto item = (RTXMVCCUpdateItem*)ttptr;
    ttptr += item->len;
    if(item->pid != response_node_)
      continue;
    auto node = local_lookup_op(item->tableid, item->key);
    assert(node != NULL);
    MVCCHeader* header = (MVCCHeader*)node->value;
    ASSERT(header->lock == item->txn_starting_timestamp) << "release lock: "
      << header->lock << "!=" << item->txn_starting_timestamp;
    int pos = (int)item->pos;
    assert(pos < MVCC_VERSION_NUM);
    header->wts[pos] = item->txn_starting_timestamp;
    // LOG(3) << item->txn_starting_timestamp;
    char* raw_data = (char*)node->value + sizeof(MVCCHeader);
    memcpy(raw_data + pos * item->len, (char*)item + sizeof(RTXMVCCUpdateItem),
      item->len);
    header->lock = 0; // unlock
  }
  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg, 0, id, cid);

}

void MVCC::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&MVCC::read_rpc_handler,this,RTX_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&MVCC::lock_read_rpc_handler,this,RTX_LOCK_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&MVCC::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&MVCC::update_rpc_handler,this,RTX_UPDATE_RPC_ID);
}


}
}