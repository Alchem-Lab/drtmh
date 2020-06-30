#ifndef GLOBAL_VARS_H_
#define GLOBAL_VARS_H_

#include "view.h"
#include "global_lock_manager.h"
#define MVCC_VERSION_NUM 4

// #define MAX_CALVIN_REQ_CNTS (200*1000)
#define MAX_CALVIN_REQ_CNTS (10)  // sequencer's req_buffer use malloc() instead
                                    // so large CALVIN_REQ_CNTS can be supported.
// #define MAX_CALVIN_REQ_CNTS 30  // For now, this value cannot be larger, 
                                // otherwise it causes Rmalloc() to fail.
                                // Note: current Rmalloc does not support
                                // Rmalloc size greater than 64K.


#define MAX_CALVIN_SETS_SUPPRTED_IN_BITS (5)
#define MAX_CALVIN_SETS_SUPPORTED (1U<<(MAX_CALVIN_SETS_SUPPRTED_IN_BITS))  // 32 SETS
#define CALVIN_REQ_INFO_SIZE 256  // obselete, to be deleted.
#define CALVIN_EPOCH_INIT  61
#define CALVIN_EPOCH_READY 53
#define CALVIN_EPOCH_DONE  59
#define MAX_VAL_LENGTH 128

namespace nocc {

namespace rtx {

/**
 * New meta data for each record
 */

struct MVCCHeader {
	uint64_t lock;
	uint64_t rts;
	uint64_t wts[MVCC_VERSION_NUM];
	// uint64_t off[MVCC_VERSION_NUM];
};
 
#define MAX_RECORD_LEN 64


struct rwsets_t {
  #include "occ_internal_structure.h"
  uint8_t nReads;
  uint8_t nWrites;
  ReadSetItem access[MAX_CALVIN_SETS_SUPPORTED];
};

#if BOHM
struct BOHMRecord {
  uint begin_ts;
  uint end_ts;
  rwsets_t* txn; // the transaction that can generate the actual data of this record
  char data[MAX_RECORD_LEN]; // record length. FIXME: do not use hard coded number
  struct BHOMRecord* prev;
};
#endif

struct ReplyHeader {
  uint16_t num;
};

typedef uint64_t timestamp_t;
struct det_request {
  det_request(int req_idx, int req_initiator, timestamp_t timestamp) : 
          req_idx(req_idx), req_initiator(req_initiator), timestamp(timestamp) {}

  det_request(det_request* copy) :
          req_idx(copy->req_idx), req_seq(copy->req_seq), req_initiator(copy->req_initiator), timestamp(copy->timestamp) {
          memcpy(req_info, copy->req_info, sizeof(rwsets_t));
  }
  det_request(const det_request& copy) :
          req_idx(copy.req_idx), req_seq(copy.req_seq), req_initiator(copy.req_initiator), timestamp(copy.timestamp) {
          memcpy(req_info, copy.req_info, sizeof(rwsets_t));
  }
  det_request() {}

  int req_idx; 		 // the index indicating the workload type.
  int req_seq; 		 // the sequence number in the deterministic batch.
  int req_initiator; // the if of the node that creates this request
  
  timestamp_t timestamp;

  char req_info[sizeof(rwsets_t)];
};

class det_request_compare {
public:
  bool operator()(const det_request* lhs, 
                  const det_request* rhs) {
    return lhs->timestamp < rhs->timestamp;
  }
};

struct calvin_header {
  volatile uint8_t epoch_status;

  // note that the following field must be AFTER the epoch_status field.
  // to ensure that correct one-sided broadcast while not writing the remote epoch_status.
  // the remote epoch_status can only be updated at epoch_sync_rdma method.
  uint8_t node_id;
  uint64_t epoch_id;
  volatile uint64_t batch_size; // the batch size

  uint64_t chunk_size; // the number of det_requests in this rpc call
  int chunk_id;
  int nchunks;

  volatile uint64_t received_size;
};

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

struct read_compact_val_t {
  uint32_t len;
  char value[MAX_VAL_LENGTH];
};

extern SymmetricView *global_view;
extern GlobalLockManager *global_lock_manager;

//extern int ycsb_set_length;
//extern int ycsb_write_num;


} // namespace rtx
} // namespace nocc

#endif
