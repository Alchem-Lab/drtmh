#pragma once

#include "tx_config.h"
    
#if ENABLE_TXN_API
#include "txn_interface.h"
#else
#include "tx_operator.hpp"
#include "core/utils/latency_profier.h"
#include "core/utils/count_vector.hpp"
#include "dslr.h"
#endif

#include "logger.hpp"

#include "core/logging.h"

#include "rdma_req_helper.hpp"

#include "rwlock.hpp"

namespace nocc {

namespace rtx {

#define MAX_VAL_LENGTH 128
struct read_val_t {
  uint32_t req_seq;
  int read_or_write;
  int index_in_set;
  uint32_t len;
  char value[MAX_VAL_LENGTH];
  read_val_t(int req_seq, int rw, int index, uint32_t len, char* val) :
    req_seq(req_seq),
    read_or_write(rw),
    index_in_set(index),
    len(len) {
      assert(len < MAX_VAL_LENGTH);
      memcpy(value, val, len);
    }
  read_val_t(const read_val_t& copy) {
    req_seq = copy.req_seq;
    read_or_write = copy.read_or_write;
    index_in_set = copy.index_in_set;
    len = copy.len;
    memcpy(value, copy.value, len);
  }
  read_val_t() {}
};

#if ENABLE_TXN_API
class CALVIN : public TxnAlg {
#else
class CALVIN : public TXOpBase {
#endif
#include "occ_internal_structure.h"

protected:
  // return the last index in the read-set
  int local_read(int tableid,uint64_t key,int len,yield_func_t &yield) {

    char *temp_val = (char *)malloc(len);
    uint64_t seq;

    auto node = local_get_op(tableid,key,temp_val,len,seq,db_->_schemas[tableid].meta_len);

    if(unlikely(node == NULL)) {
      free(temp_val);
      return -1;
    }
    // add to read-set
    int idx = read_set_.size();
    read_set_.emplace_back(tableid,key,node,temp_val,seq,len,node_id_);
    return idx;
  }

  // return the last index in the write-set
  int local_write(int tableid,uint64_t key,int len,yield_func_t &yield) {

    char *temp_val = (char *)malloc(len);
    uint64_t seq;

    auto node = local_get_op(tableid,key,temp_val,len,seq,db_->_schemas[tableid].meta_len);

    if(unlikely(node == NULL)) {
      free(temp_val);
      return -1;
    }

    // add to write-set
    write_set_.emplace_back(tableid,key,node,temp_val,seq,len,node_id_);
    return write_set_.size() - 1;
  }

  int local_insert(int tableid,uint64_t key,char *val,int len,yield_func_t &yield) {
    char *data_ptr = (char *)malloc(len);
    uint64_t seq;
    auto node = local_insert_op(tableid,key,seq);
    memcpy(data_ptr,val,len);
    write_set_.emplace_back(tableid,key,node,data_ptr,seq,len,node_id_);
    return write_set_.size() - 1;
  }

#if ONE_SIDED_READ
  // return the last index in the read-set
  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char*)NULL,
                           0,
                           len,pid);
    return read_set_.size() - 1;
  }

  // return the last index in the write-set
  int remote_write(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    write_set_.emplace_back(tableid,key,(MemNode *)NULL,(char*)NULL,
                           0,
                           len,pid);
    return write_set_.size() - 1;
  }

  int remote_insert(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    assert(false); // not implemented
  }

#else

  // return the last index in the read-set
  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    return add_batch_read(tableid,key,pid,len);
  }

  // return the last index in the write-set
  int remote_write(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    return add_batch_write(tableid,key,pid,len);
  }

  int remote_insert(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
    return add_batch_insert(tableid,key,pid,len);
  }

#endif



  /** helper functions to batch rpc operations below
    */
  inline __attribute__((always_inline))
  void start_batch_read() {
    start_batch_rpc_op(read_batch_helper_);
  }

  inline __attribute__((always_inline))
  int add_batch_read(int tableid,uint64_t key,int pid,int len) {
    // add a batch read request
    int idx = read_set_.size();
    // add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                                 // /* init RTXReadItem */ RTX_REQ_READ,pid,key,tableid,len,(idx<<1));
    read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
    return idx;
  }

  inline __attribute__((always_inline))
  int add_batch_write(int tableid,uint64_t key,int pid,int len) {
    // add a batch read request
    int idx = write_set_.size();
    // add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                                 // /* init RTXReadItem */ RTX_REQ_READ_LOCK,pid,key,tableid,len,(idx<<1)+1);
    // fprintf(stdout, "write rpc batched: write_set idx = %d, payload = %d\n", idx, );
    write_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
    return idx;
  }

  inline __attribute__((always_inline))
  int add_batch_insert(int tableid,uint64_t key,int pid,int len) {
    assert(false);
    // add a batch read request
    int idx = read_set_.size();
    // add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
    //                              /* init RTXReadItem */ RTX_REQ_INSERT,pid,key,tableid,len,idx);
    read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
    return idx;
  }

  inline __attribute__((always_inline))
  int send_batch_read(int rpc_id, int idx = 0) {
    return send_batch_rpc_op(read_batch_helper_,cor_id_,rpc_id);
  }

  inline __attribute__((always_inline))
  bool parse_batch_result(int num) {

    char *ptr  = reply_buf_;
    for(uint i = 0;i < num;++i) {
      // parse a reply header
      ReplyHeader *header = (ReplyHeader *)(ptr);
      ptr += sizeof(ReplyHeader);
      for(uint j = 0;j < header->num;++j) {
        OCCResponse *item = (OCCResponse *)ptr;
        if (item->idx & 1 == 0) { // an idx in read-set
          // fprintf(stdout, "rpc response: read_set idx = %d, payload = %d\n", item->idx, item->payload);
          item->idx >>= 1;
          read_set_[item->idx].data_ptr = (char *)malloc(read_set_[item->idx].len);
          memcpy(read_set_[item->idx].data_ptr, ptr + sizeof(OCCResponse),read_set_[item->idx].len);
          read_set_[item->idx].seq      = item->seq;
        } else {
          // fprintf(stdout, "rpc response: write_set idx = %d, payload = %d\n", item->idx, item->payload);
          item->idx >>= 1;
          write_set_[item->idx].data_ptr = (char *)malloc(write_set_[item->idx].len);
          memcpy(write_set_[item->idx].data_ptr, ptr + sizeof(OCCResponse),write_set_[item->idx].len);
          write_set_[item->idx].seq      = item->seq;
        }
        ptr += (sizeof(OCCResponse) + item->payload);
      }
    }
    return true;
  }

  /** helper functions to batch rpc operations above
    */

#if 0
  void prepare_write_contents() {
    // Notice that it should contain local records
    // This function has to be called after lock + validation success
    write_batch_helper_.clear_buf(); // only clean buf, not the mac_set

    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      if ((*it).pid != node_id_) {
        add_batch_entry_wo_mac<RtxWriteItem>(write_batch_helper_,
                                             (*it).pid,
                                             /* init write item */ (*it).pid,(*it).tableid,(*it).key,(*it).len);
        memcpy(write_batch_helper_.req_buf_end_,(*it).data_ptr,(*it).len);
        write_batch_helper_.req_buf_end_ += (*it).len;
      }
    }
  }

#endif
  /**
   * GC the read/write set is a little complex using RDMA.
   * Since some pointers are allocated from the RDMA heap, not from local heap.
   */
  void gc_helper(std::vector<ReadSetItem> &set) {
    for(auto it = set.begin();it != set.end();++it) {
      if(it->pid != node_id_) {
#if INLINE_OVERWRITE
        Rfree((*it).data_ptr - sizeof(MemNode));
#else
        Rfree((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      }
      else
        free((*it).data_ptr);

    }
  }

  // overwrite GC functions, to use Rfree
  void gc_readset() {
    gc_helper(read_set_);
  }

  void gc_writeset() {
    gc_helper(write_set_);
  }

  bool dummy_commit() {
    // clean remaining resources
    gc_readset();
    gc_writeset();
    return true;
  }

  bool try_lock_read_w_rdma(int index, yield_func_t &yield);
  bool try_lock_write_w_rdma(int index, yield_func_t &yield);
  bool try_lock_read_w_rwlock_rdma(int index, uint64_t txn_end_time, yield_func_t &yield);
  bool try_lock_write_w_rwlock_rdma(int index, yield_func_t &yield);
  bool try_lock_read_w_FA_rdma(int index, yield_func_t &yield);
  bool try_lock_write_w_FA_rdma(int index, yield_func_t &yield);
  bool try_lock_read_w_rwlock_rpc(int index, uint64_t txn_end_time, yield_func_t &yield);
  bool try_lock_write_w_rwlock_rpc(int index, yield_func_t &yield);

  void release_reads_w_rdma(yield_func_t &yield);
  void release_writes_w_rdma(yield_func_t &yield);
  void release_reads_w_rwlock_rdma(yield_func_t &yield);
  void release_writes_w_rwlock_rdma(yield_func_t &yield);
  void release_reads_w_FA_rdma(yield_func_t &yield);
  void release_writes_w_FA_rdma(yield_func_t &yield);
  void release_reads(yield_func_t &yield);
  void release_writes(yield_func_t &yield);

  void write_back_w_rdma(yield_func_t &yield);
  void write_back_w_rwlock_rdma(yield_func_t &yield);
  void write_back_w_FA_rdma(yield_func_t &yield);
  void write_back(yield_func_t &yield);

public:
  CALVIN(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
          RdmaCtrl *cm,RScheduler* sched,int ms) :
#if ENABLE_TXN_API
      TxnAlg(worker,db,rpc_handler,nid,tid,cid,response_node,cm,sched,ms),
#else
      TXOpBase(worker,db,rpc_handler,cm,sched,response_node,tid,ms),// response_node shall always equal *real node id*
#endif
      read_set_(),write_set_(),
      read_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
      write_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
      rpc_op_send_buf_(rpc_->get_static_buf(MAX_MSG_SIZE)),
      cor_id_(cid),response_node_(nid)
  {
#if !ENABLE_TXN_API
        dslr_lock_manager = new DSLR(worker, db, rpc_handler, 
                                 nid, tid, cid, response_node, 
                                 cm, sched, ms);
#endif

    if(worker_id_ == 0 && cor_id_ == 0) {
      LOG(3) << "Use one-sided for read.";
    }

    register_default_rpc_handlers();
    memset(reply_buf_,0,MAX_MSG_SIZE);
    lock_req_ = new RDMACASLockReq(cid);
    read_req_ = new RDMAReadReq(cid);
    read_set_.clear();
    write_set_.clear();
  }

  void set_logger(Logger *log) { logger_ = log; }

#if ENABLE_TXN_API
  // get the read lock of the record and actually read
  inline __attribute__((always_inline))
  virtual int read(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    int index;
    // step 1: find offset of the key in either local/remote memory
    if(pid == node_id_)
      index = local_read(tableid,key,len,yield);
    else {
      // remote case
      index = remote_read(pid,tableid,key,len,yield);
    }

    return index;
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int read(int pid,uint64_t key,yield_func_t &yield) {
    return read(pid, tableid, key, sizeof(V), yield);
  }

#else
  template <int tableid,typename V> // the value stored corresponding to tableid
  inline __attribute__((always_inline))
  int  read(int pid,uint64_t key,yield_func_t &yield) {
    int index;
    if(pid == node_id_)
      index = local_read(tableid,key,sizeof(V),yield);
    else {
      // remote case
      index = remote_read(pid,tableid,key,sizeof(V),yield);
    }

    return index;
  }
#endif

#if ENABLE_TXN_API
  // lock the record and add the record to the write-set
  inline __attribute__((always_inline))
  virtual int write(int pid, int tableid, uint64_t key, size_t len, yield_func_t &yield) {
    int index;

    // step 1: find offset of the key in either local/remote memory
    if(pid == node_id_)
      index = local_write(tableid,key,len,yield);
    else {
      // remote case
      index = remote_write(pid,tableid,key,len,yield);
    }

    return index;
  }

  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int write(int pid,uint64_t key,yield_func_t &yield) {
    return write(pid, tableid, key, sizeof(V), yield);
  }

  // Actually load the data, can be done only after all locks are acquired.
  inline __attribute__((always_inline))
  virtual char* load_read(int idx, size_t len, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = read_set_;
    
    assert(idx < set.size());
    ASSERT(len == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

    if(set[idx].pid != response_node_) {
        ;
    } else {
      if (set[idx].data_ptr == NULL) {
        // local actual read
        char *temp_val = (char *)malloc(len);
        uint64_t seq;
        auto node = local_get_op(set[idx].tableid,set[idx].key, temp_val, set[idx].len,seq,
                                 db_->_schemas[set[idx].tableid].meta_len);
        if(unlikely(node == NULL)) {
          free(temp_val);
          assert(false);
          return NULL;
        }

        set[idx].data_ptr = temp_val;
      }
    }

    return (set[idx].data_ptr);
  }


  // Actually load the data, can be done only after all locks are acquired.
  inline __attribute__((always_inline))
  virtual char* load_write(int idx, size_t len, yield_func_t &yield) {
    std::vector<ReadSetItem> &set = write_set_;
    
    assert(idx < set.size());
    ASSERT(len == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

    // note that we must use response_node_ here to indicate the actual
    // node id.
    // fprintf(stdout, "pid in load write %d = %d. my node id = %d\n", idx, set[idx].pid, response_node_);
    if(set[idx].pid != response_node_) {
        ;
    } else {
      // assert(set[idx].data_ptr == NULL);
      if (set[idx].data_ptr == NULL) {
        // local actual read
        char *temp_val = (char *)malloc(len);
        uint64_t seq;
        auto node = local_get_op(set[idx].tableid,set[idx].key, temp_val, set[idx].len,seq, db_->_schemas[set[idx].tableid].meta_len);
        if(unlikely(node == NULL)) {
          free(temp_val);
          assert(false);
          return NULL;
        }
        
        set[idx].data_ptr = temp_val;
      }

      // fprintf(stdout, "in load write: %f@%p\n", *(float*)set[idx].data_ptr, set[idx].data_ptr);
    }

    return (set[idx].data_ptr);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_readset(int idx,yield_func_t &yield) {
    return (V*)load_read(idx, sizeof(V), yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_writeset(int idx,yield_func_t &yield) {
    return (V*)load_write(idx, sizeof(V), yield);
  }

  void serialize_read_set(char*& buf) {
    *(uint64_t*)buf = read_set_.size();
    buf += sizeof(uint64_t);
    memset(buf, 0, sizeof(ReadSetItem)*MAX_SET_ITEMS);
    assert(read_set_.size() <= MAX_SET_ITEMS);
    int i = 0;
    for (; i < read_set_.size(); i++) {
      *(ReadSetItem*)buf = read_set_[i];
      buf += sizeof(ReadSetItem);
    }
    buf += sizeof(ReadSetItem)*(MAX_SET_ITEMS-i);
  }

  void serialize_write_set(char*& buf) {
    *(uint64_t*)buf = write_set_.size();
    buf += sizeof(uint64_t);
    memset(buf, 0, sizeof(ReadSetItem)*MAX_SET_ITEMS);
    assert(write_set_.size() <= MAX_SET_ITEMS);
    int i = 0;
    for (; i < write_set_.size(); i++) {
      *(ReadSetItem*)buf = write_set_[i];
      // fprintf(stdout, "serializing write %d\n", i);
      // for (int j = 0; j < sizeof(ReadSetItem); j++) {
      //   fprintf(stdout, "%x ", buf[j]);
      // }
      // fprintf(stdout, "\n");
      // fprintf(stdout, "pid = %d", ((ReadSetItem*)buf)->pid);
      // fprintf(stdout, "tableid = %d", ((ReadSetItem*)buf)->tableid);
      // fprintf(stdout, "len = %d", ((ReadSetItem*)buf)->len);
      // fprintf(stdout, "key = %d", ((ReadSetItem*)buf)->key);

      buf += sizeof(ReadSetItem);
    }
    buf += sizeof(ReadSetItem)*(MAX_SET_ITEMS-i);
  }

  void deserialize_read_set(uint64_t n, ReadSetItem* items) {
    read_set_.clear();
    assert(n < MAX_SET_ITEMS);
    for (int i = 0; i < n; i++) {
      read_set_.push_back(items[i]);
      // fprintf(stdout, "deserializing write %d\n", i);
      // for (int j = 0; j < sizeof(ReadSetItem); j++) {
      //   fprintf(stdout, "%x ", ((char*)(items+i))[j]);
      // }
      // fprintf(stdout, "\n");
      // fprintf(stdout, "pid = %d", items[i].pid);
      // fprintf(stdout, "tableid = %d", items[i].tableid);
      // fprintf(stdout, "len = %d", items[i].len);
      // fprintf(stdout, "key = %d", items[i].key);
    }
  }

  void deserialize_write_set(uint64_t n, ReadSetItem* items) {
    write_set_.clear();
    assert(n < MAX_SET_ITEMS);
    for (int i = 0; i < n; i++) {
      write_set_.push_back(items[i]);
      // fprintf(stdout, "deserializing write %d\n", i);
      // for (int j = 0; j < sizeof(ReadSetItem); j++) {
      //   fprintf(stdout, "%x ", ((char*)(items+i))[j]);
      // }
      // fprintf(stdout, "\n");
      // fprintf(stdout, "pid = %d", items[i].pid);
      // fprintf(stdout, "tableid = %d", items[i].tableid);
      // fprintf(stdout, "len = %d", items[i].len);
      // fprintf(stdout, "key = %d", items[i].key);
    }
  }

  void clear_read_set() {
    read_set_.clear();
  }

  void clear_write_set() {
    write_set_.clear();
  }

  bool sync_reads(int req_idx, yield_func_t &yield);
  bool request_locks(yield_func_t &yield);

#else

  template <typename V>
  inline __attribute__((always_inline))
  V *get_readset(int idx,yield_func_t &yield) {
    return get_set_helper<V>(read_set_, idx, yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V *get_writeset(int idx,yield_func_t &yield) {
    return get_set_helper<V>(write_set_, idx, yield);
  }

  template <typename V>
  inline __attribute__((always_inline))
  V* get_set_helper(std::vector<ReadSetItem> &set, int idx,yield_func_t &yield) {
    assert(idx < set.size());
    ASSERT(sizeof(V) == set[idx].len) <<
        "excepted size " << (int)(set[idx].len)  << " for table " << (int)(set[idx].tableid) << "; idx " << idx;

    if(set[idx].pid != response_node_) {
        ;
    } else {
      if (set[idx].data_ptr == NULL) {
        // local actual read
        char *temp_val = (char *)malloc(len);
        uint64_t seq;
        auto node = local_get_op(set[idx].tableid,set[idx].key, temp_val, set[idx].len,seq,
                                 db_->_schemas[set[idx].tableid].meta_len);
        if(unlikely(node == NULL)) {
          free(temp_val);
          assert(false);
          return NULL;
        }

        set[idx].data_ptr = temp_val;
      }
    }

    return (V*)(set[idx].data_ptr);
  }
#endif

  template <int tableid,typename V>
  V *get(int pid,uint64_t key,yield_func_t &yield) {
#if ENABLE_TXN_API
    int idx = read(pid,tableid,key,sizeof(V),yield);
#else
    int idx = read<tableid,V>(pid,key,yield);
#endif
    return get_readset<V>(idx,yield);
  }
  
  template <int tableid,typename V>
  inline __attribute__((always_inline))
  int insert(int pid,uint64_t key,V *val,yield_func_t &yield) {
    if(pid == node_id_)
      return local_insert(tableid,key,(char *)val,sizeof(V),yield);
    else {
      return remote_insert(pid,tableid,key,sizeof(V),yield);
    }
    return -1;
  }

  // start a TX
  virtual void begin(yield_func_t &yield) {
    read_set_.clear();
    write_set_.clear();
    #if USE_DSLR
      dslr_lock_manager->init();
    #endif
    txn_start_time = rwlock::get_now();
    // the txn_end_time is approximated using the LEASE_TIME
    txn_end_time = txn_start_time + rwlock::LEASE_TIME;
  }

#if ONE_SIDED_READ
  // commit a TX
  virtual bool commit(yield_func_t &yield) {

#if TX_ONLY_EXE
    gc_readset();
    gc_writeset();    
    return dummy_commit();
#endif

    asm volatile("" ::: "memory");

    // prepare_write_contents();
    // log_remote(yield); // log remote using *logger_*

    // write the modifications of records back
    write_back(yield);
    return true;
  }
  
#else
  
  virtual bool commit(yield_func_t &yield) {
  
#if TX_ONLY_EXE
  gc_readset();
  gc_writeset();
  return dummy_commit();
#endif

  // prepare_write_contents();
  // log_remote(yield); // log remote using *logger_*

  // write the modifications of records back
  write_back(yield);
  return true;
}

#endif

public:
  std::vector<ReadSetItem>  read_set_;
  std::vector<ReadSetItem>  write_set_;

protected:
  // helper to send batch read/write operations
  BatchOpCtrlBlock read_batch_helper_;
  BatchOpCtrlBlock write_batch_helper_;
  RDMACASLockReq* lock_req_;
  RDMAReadReq* read_req_;

  const int cor_id_;
  const int response_node_;

  Logger *logger_ = NULL;

  char* rpc_op_send_buf_;
  char reply_buf_[MAX_MSG_SIZE];

#if !ENABLE_TXN_API
  DSLR* dslr_lock_manager;
#endif

  uint64_t txn_start_time = 0;
  uint64_t txn_end_time = 0;

  friend class BenchWorker;
  friend class BankWorker;

public:
#include "occ_statistics.h"

  // helper functions
  void register_default_rpc_handlers();

 private:
  // RPC handlers
  void forward_rpc_handler(int id,int cid,char *msg,void *arg);
};

} // namespace rtx
} // namespace nocc