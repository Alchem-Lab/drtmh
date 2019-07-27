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
  bool need_yield = false;
  for(int i = 0; i < release_num; ++i) {
    auto& item = write_set_[i];
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
      auto node = local_lookup_op(item.tableid, item.key);
      MVCCHeader* header = (MVCCHeader*)node->value;
      assert(header->lock != 0);
      header->lock = 0;
    }
  }
  if(need_yield) {
    worker_->indirect_yield(yield);
  }
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
    if(!check_write(header, txn_start_time)) {
      return false;
    }
    volatile uint64_t l = header->lock;
    if(l > txn_start_time) return false;
    while (true) {
      volatile uint64_t* lockptr = &(header->lock);
      if(unlikely(!__sync_bool_compare_and_swap(lockptr, 0, txn_start_time))) {
#ifdef MVCC_NOWAIT
        return false;
#else
        worker_->yield_next(yield);
        continue;
#endif
      }
      else { // get the lock
        if(header->rts > txn_start_time) {
          *lockptr = 0;
          return false;
        }
        uint64_t max_wts = 0, min_wts = 0xffffffffffffffff;
        int pos = -1;
        int maxpos = -1;
        for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
          if(header->wts[i] > max_wts) {
            max_wts = header->wts[i];
            maxpos = i;
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
        memcpy((*it).data_ptr, raw_data + maxpos * (*it).len, (*it).len);
        return true;
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
        int maxpos = -1;
        for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
          // LOG(3) << header->wts[i] << " $$$ " << min_wts;
          // fprintf(stderr, "%x\n", header->wts[i]);
          if(header->wts[i] > max_wts) {
            maxpos = i;
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
        memcpy(reply + sizeof(uint64_t), raw_data + maxpos * item->len, item->len);
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

int MVCC::try_lock_read_rdma(int index, yield_func_t &yield) {
  START(lock);
  auto& item = write_set_[index];
  if(item.pid != node_id_) {
    // step 1: get off
    uint64_t off = 0;
    char* local_buf = (char*)Rmalloc(item.len * MVCC_VERSION_NUM + 
      sizeof(MVCCHeader));
    off = rdma_read_val(item.pid, item.tableid, item.key, item.len,
     local_buf, yield, sizeof(MVCCHeader), false); // metalen?
    assert(off != 0);
    item.node = (MemNode*)off;
    item.data_ptr = local_buf;
    item.off = off;

    // step 2: lock remote
    Qp *qp = get_qp(item.pid);
    assert(qp != NULL);
    MVCCHeader* header = (MVCCHeader*)local_buf;
    while(true) {
      lock_req_->set_lock_meta(off, 0, txn_start_time, local_buf);
      lock_req_->post_reqs(scheduler_, qp);
      worker_->indirect_yield(yield);
      if(header->lock > txn_start_time) { // a newer write is processing
        END(lock);
        abort_cnt[0]++;
        return -1;
      }
      else if(header->lock != 0) {
#ifdef MVCC_NOWAIT
        END(lock);
        abort_cnt[1]++;
        return -1;
#else
        worker_->yield_next(yield);
        continue;
#endif
      }
      else { // get the lock
        END(lock);
        break;
      }
    }

    // step 3: get remote meta and data, check
    scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, local_buf, 
      item.len * MVCC_VERSION_NUM + sizeof(MVCCHeader), off, IBV_SEND_SIGNALED);
    worker_->indirect_yield(yield);
    volatile uint64_t l = *((uint64_t*)local_buf);
    ASSERT(l == txn_start_time) << l << ' ' << txn_start_time;
    if(header->rts > txn_start_time) {
      abort_cnt[2]++;
      return -2;
    }
    uint64_t max_wts = 0, min_wts = 0xffffffffffffffff;
    int pos = -1, maxpos = -1;
    for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
      if(header->wts[i] > max_wts) {
        max_wts = header->wts[i];
        maxpos = i;
      }
      if(header->wts[i] < min_wts) {
        min_wts = header->wts[i];
        pos = i;
      }
    }
    if(max_wts > txn_start_time) {
      // LOG(3) << max_wts << ' ' << txn_start_time;
      cnt_timer = max_wts >> 10;
      abort_cnt[3]++;
      return -2;
    }
    assert(pos < MVCC_VERSION_NUM && pos >= 0);
    assert(maxpos < MVCC_VERSION_NUM && maxpos >= 0);
    item.seq = (uint64_t)pos + (uint64_t)maxpos * MVCC_VERSION_NUM;
    item.data_ptr = local_buf + sizeof(MVCCHeader) + maxpos * item.len;
    return 0;
  }
  else {
    if(item.node == NULL) {
      item.node = local_lookup_op(item.tableid, item.key);
    }
    MVCCHeader* header = (MVCCHeader*)(item.node->value);
    if(!check_write(header, txn_start_time)) {
      abort_cnt[4]++;
      return -1;
    }
    while(true) {
      volatile uint64_t* lock_ptr = &(header->lock);
      volatile uint64_t l = *lock_ptr;
      if(l > txn_start_time) {
        abort_cnt[5]++;
        return -1;
      }
      if(unlikely(!__sync_bool_compare_and_swap(lock_ptr, l, txn_start_time))) {
#ifdef MVCC_NOWAIT
        abort_cnt[6]++;
        return -1;
#else
        worker_->yield_next(yield);
        continue;
#endif
      }
      else {
        break; // get the lock
      }
    }
    assert(header->lock == txn_start_time);
    if(header->rts > txn_start_time) {
      header->lock = 0;
      abort_cnt[7]++;
      return -1;
    }
    uint64_t max_wts = 0, min_wts = 0xffffffffffffffff;
    int pos = -1;
    int maxpos = -1;
    for(int i = 0; i < MVCC_VERSION_NUM; ++i) {
      if(header->wts[i] > max_wts) {
        max_wts = header->wts[i];
        maxpos = i;
      }
      if(header->wts[i] < min_wts) {
        min_wts = header->wts[i];
        pos = i;
      }
    }
    if(max_wts > txn_start_time) {
      header->lock = 0;
      abort_cnt[8]++;
      return -1;
    }
    item.seq = (uint64_t)pos;
    assert(((uint32_t)maxpos) < MVCC_VERSION_NUM);
    assert(item.seq < MVCC_VERSION_NUM);
    if(item.data_ptr == NULL) {
      item.data_ptr = (char*)malloc(item.len);
    }
    char* raw_data= ((char*)item.node->value) + sizeof(MVCCHeader);
    memcpy(item.data_ptr, raw_data + maxpos * item.len, item.len);
    return 0;
  }
  return 0;
}

bool MVCC::try_read_rdma(int index, yield_func_t &yield) {
  auto& item = read_set_[index];
  if(item.pid != node_id_) {
    uint64_t off = 0;
    char* recv_ptr = (char*)Rmalloc(item.len * MVCC_VERSION_NUM + sizeof(MVCCHeader));
    off = rdma_read_val(item.pid, item.tableid, item.key, item.len, 
      recv_ptr, yield, sizeof(MVCCHeader), false);

    // step 1: read the meta and check if i can read
    Qp* qp = get_qp(item.pid);
    assert(qp != NULL);
    MVCCHeader* header = (MVCCHeader*)recv_ptr;
    scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, recv_ptr,
      sizeof(MVCCHeader), off, IBV_SEND_SIGNALED);
    worker_->indirect_yield(yield);
    int pos = -1;
    if((pos = check_read(header, txn_start_time)) == -1) {
      abort_cnt[9]++;
      return false;
    }
    uint64_t before_reading_wts = header->wts[pos];

    // step 2: read the data and meta, check if i can read the data
    scheduler_->post_send(qp, cor_id_, IBV_WR_RDMA_READ, recv_ptr,
      sizeof(MVCCHeader) + MVCC_VERSION_NUM * item.len, off, 
      IBV_SEND_SIGNALED);
    worker_->indirect_yield(yield);
    int new_pos = check_read(header, txn_start_time);
    if(new_pos != pos || header->wts[new_pos] != before_reading_wts) {
      abort_cnt[10]++;
      return false;
    }

    // step 3: write the rts back
    uint64_t compare = header->rts;
    uint64_t back = 0;
    while(compare < txn_start_time) {
      lock_req_->set_lock_meta(off + sizeof(uint64_t), compare, 
        txn_start_time, (char*)(&back));
      worker_->indirect_yield(yield);
      if(compare == back)
        break;
      else {
        compare = back;
      }
    }
    item.data_ptr = recv_ptr + sizeof(MVCCHeader) + pos * item.len;
    return true;
  }
  else {
    auto node = local_lookup_op(item.tableid, item.key);
    assert(node != NULL);
    MVCCHeader* header = (MVCCHeader*)node->value;
    int pos = -1;
    if((pos = check_read(header, txn_start_time)) == -1){
      abort_cnt[11]++;
      return false;
    }
    uint64_t before_reading_wts = header->wts[pos];

    char* raw_data = (char*)(node->value) + sizeof(MVCCHeader);
    if(item.data_ptr == NULL)
      item.data_ptr = (char*)malloc(item.len);
    memcpy(item.data_ptr, raw_data + pos * item.len, item.len);

    int new_pos = check_read(header, txn_start_time);
    if(new_pos != pos || header->wts[new_pos] != before_reading_wts) {
      abort_cnt[12]++;
      return false;
    }
    while(true) {
      volatile uint64_t* rts_ptr = &(header->rts);
      volatile uint64_t rts = header->rts;
      if(txn_start_time > rts) {
        if(!__sync_bool_compare_and_swap(rts_ptr, rts, txn_start_time)) {
          continue;
        }
        else { // already change the rts
          break;
        }
      }
      else {
        break;
      }
    }
    return true;
  }
  return true;
}

bool MVCC::try_update_rdma(yield_func_t &yield) {
  
  bool need_yield = false;
  for(auto& item : write_set_) {
    if(item.pid != node_id_) {
      START(commit);
      assert(item.seq < MVCC_VERSION_NUM * MVCC_VERSION_NUM);
      int pos = (int)(item.seq % MVCC_VERSION_NUM);
      int maxpos = (int)(item.seq / MVCC_VERSION_NUM);
      char* local_buf = item.data_ptr - sizeof(MVCCHeader) - maxpos * item.len;
      MVCCHeader* header = (MVCCHeader*)local_buf;
      assert(header->lock == txn_start_time);
      header->wts[pos] = txn_start_time;
      char* raw_data = local_buf + sizeof(MVCCHeader);
      memcpy(raw_data + pos * item.len, item.data_ptr, item.len);
      Qp *qp = get_qp(item.pid);
      assert(qp != NULL);
      write_req_->set_write_meta(item.off + 2 * sizeof(uint64_t), 
        local_buf + 2 * sizeof(uint64_t), 
        sizeof(MVCCHeader) - 2 * sizeof(uint64_t) + (pos + 1) * item.len);
      write_req_->set_unlock_meta(item.off);
      write_req_->post_reqs(scheduler_, qp);
      need_yield = true;
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
        need_yield = false;
      }
      END(commit);
    }
    else {
      assert(item.seq < MVCC_VERSION_NUM);
      auto node = local_lookup_op(item.tableid, item.key);
      assert(node != NULL);
      MVCCHeader* header = (MVCCHeader*)node->value;
      ASSERT(header->lock == txn_start_time) << "release lock: "
        << header->lock << "!=" << txn_start_time;
      int pos = (int)item.seq;
      // update wts
      header->wts[pos] = txn_start_time;
      char* raw_data = (char*)node->value + sizeof(MVCCHeader);
      memcpy(raw_data + pos * item.len, item.data_ptr, item.len);
      header->lock = 0;
    }
  }
  if(need_yield) {
    worker_->indirect_yield(yield);
  }
  return true;
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
