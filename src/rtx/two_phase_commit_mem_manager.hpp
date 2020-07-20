#pragma once

#include <map>

#include "core/logging.h"

namespace nocc {

namespace rtx {

class TwoPhaseCommitMemManager {
 public:
  TwoPhaseCommitMemManager(char *local_p,int ms,int ts,int cs, uint64_t base_off = 0) :
      mac_num_(ms),
      thread_num_(ts),
      coroutine_num_(cs),
      local_buffer_(local_p),
      base_offset_(base_off)
  {
    thread_buf_size_ = coroutine_num_ * sizeof(uint8_t);
    mac_buf_size_ = thread_num_ * thread_buf_size_;
  }

  ~TwoPhaseCommitMemManager() {}

  inline uint64_t total_size() {
    return mac_buf_size_ * mac_num_;
  }


  inline uint64_t get_remote_offset(int from_mac,int from_tid,int from_cid) {
    return from_mac * mac_buf_size_ +
           from_tid * thread_buf_size_ +
           from_cid * sizeof(uint8_t);
           + base_offset_; // the start pointer of 2pc status area
  }

  inline char *get_local_ptr(int from_mac,int from_tid,int from_cid) {
    return (char *)local_buffer_ + get_remote_offset(from_mac, from_tid, from_cid);
  }

  // total number of machines
  const int mac_num_;

  // total number of thread
  const int thread_num_;

  // total number of coroutines 
  const int coroutine_num_;

  // the start pointer of 2pc status area
  const char *local_buffer_;

  // total buffer used at each thread
  int thread_buf_size_;
  
  // total log size used by one mac
  int mac_buf_size_;

  enum {
    TWO_PHASE_PREPARE=129,
    TWO_PHASE_DECISION_COMMIT=131,
    TWO_PHASE_DECISION_ABORT=137,
    VOTE_COMMIT=151,
    VOTE_ABORT=153
  } TwoPhaseCommitterMsgType;
  
 private:
  uint64_t base_offset_;
};
}; // namespace rtx

};   // namespace nocc
