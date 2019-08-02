#ifndef NOCC_RTX_RWLOCK_H
#define NOCC_RTX_RWLOCK_H

#define USE_RWLOCK 1
#include <chrono>
#include <sys/timex.h>

// #include "core/rworker.h"
// #include "db/txs/ts_manager.hpp"


// the following macros are used by namespace rwlock
#define R_LEASE(end_time) ((end_time) << (1+8))
#define END_TIME(state) ((state) >> (1+8) | (get_now_ntp() & (~0x7fffffffffffff)) )
// the following macros are used by namespace rwlock_4_waitdie
#define START_TIME(state) ((state) >> (1+8) | (get_now_ntp() & (~0x7fffffffffffff)) )
#define LEASE_DURATION(state) (((state) & 0x1ff) >> 1)

// used by sundial 
#define WTS(y) (uint32_t)(((y)&0x7fffffff00000000) >> 32)
#define RTS(y) (uint32_t)((y)&0x000000007fffffff)
// #define RLOCKTS(y) ((y)&0x8000000000000000)
// #define WLOCKTS(y) (((y)&0x0000000080000000) == 0x0000000080000000)
// #define WLOCKTS(y) (((y)&0x0000000000000001) == 0x0000000000000001)
// #define RLOCKTS(y) (((y)&0xffffffff00000000) > 0x0000000000000000) 
// #define SUNDIALRLOCK 0x8000000000000000
// #define SUNDIALWLOCK 0x0000000080000000
// #define SUNDIALWLOCK 0x0000000000000001
// #define WUNLOCK(y) (y & 0xffffffff7fffffff)
// #define WUNLOCK(y) (y & 0xffffffff00000000)
#define READLOCKADDONE 0x0000000100000000


namespace nocc {

namespace rtx {

namespace rwlock {

// the state of rwlock
// read lease end time (55 bits) / owner machine ID (8 bits) / write_lock (1 bit)
const uint64_t INIT = 0x0;
const uint64_t W_LOCKED = 0x1;
const uint64_t DELTA = 50; // 50 micro-seconds
const uint64_t LEASE_TIME = 3000; // 0.4 milli-seconds
const uint64_t LEASE_TIME_RPC = 1500;

// get current wall-time to the precision of microseconds
inline __attribute__((always_inline))
uint64_t get_now() {
    using namespace std::chrono;
    // Get current time with precision of microseconds
    auto now = time_point_cast<microseconds>(system_clock::now());
    // sys_microseconds is type time_point<system_clock, microseconds>
    using sys_microseconds = decltype(now);
    // Convert time_point to signed integral type
    auto integral_duration = now.time_since_epoch().count();

    return (uint64_t)integral_duration;
}

inline __attribute__((always_inline))
uint64_t get_now_ntp() {
  // ntptimeval tv;
  // int ret;
  // if ((ret = ntp_gettime(&tv)) == 0) {
  //   return (tv.time.tv_sec * 1000000 + tv.time.tv_usec);
  // } 
  // else {
  //   switch(ret) {
  //   case EFAULT:
  //     fprintf(stderr,"efault\n");
  //     break;
  //   case EOVERFLOW:
  //     fprintf(stderr,"eoverflow\n");
  //     break;
  //   default:
  //     fprintf(stderr,"gettime ret: %d\n",ret);
  //   }
  //   assert(false);
  // }
  return get_now();
}

inline __attribute__((always_inline))
uint64_t get_now_nano() {
    using namespace std::chrono;
    // Get current time with precision of microseconds
    auto now = time_point_cast<nanoseconds>(system_clock::now());
    // sys_microseconds is type time_point<system_clock, microseconds>
    using sys_nanoseconds = decltype(now);
    // Convert time_point to signed integral type
    auto integral_duration = now.time_since_epoch().count();

    return (uint64_t)integral_duration;
}

inline __attribute__((always_inline))
uint64_t LOCKED(uint64_t owner_id) {
  return (((owner_id & 0xff) << 1) | W_LOCKED);
}

inline __attribute__((always_inline))
bool EXPIRED(uint64_t end_time) {
  return get_now_ntp() > (end_time) + DELTA;
}

inline __attribute__((always_inline))
bool VALID(uint64_t end_time) {
  return get_now_ntp() < (end_time) - DELTA;
}

} // namespace rw-lock

namespace rwlock_4_waitdie {
// the state of rwlock
 /************************************************************
  * txn starting timestamp (55 bits) / 
  * read lease duration (8 bits) for read or owner machine ID (8 bits) for write / 
  * write_lock (1 bit)
  ************************************************************/

const uint64_t INIT = 0x0;
const uint64_t W_LOCKED = 0x1;
const uint64_t DELTA = 50; // 50 micro-seconds
const uint64_t LEASE_TIME = 400; // 0.4 milli-seconds

// get current wall-time to the precision of microseconds
inline __attribute__((always_inline))
uint64_t get_now() {
    using namespace std::chrono;
    // Get current time with precision of microseconds
    auto now = time_point_cast<microseconds>(system_clock::now());
    // sys_microseconds is type time_point<system_clock, microseconds>
    using sys_microseconds = decltype(now);
    // Convert time_point to signed integral type
    auto integral_duration = now.time_since_epoch().count();

    return (uint64_t)integral_duration;
}

inline __attribute__((always_inline))
uint64_t get_now_ntp() {
  ntptimeval tv;
  int ret;
  if ((ret = ntp_gettime(&tv)) == 0) {
    return (tv.time.tv_sec * 1000000 + tv.time.tv_usec);
  } 
  else {
    switch(ret) {
    case EFAULT:
      fprintf(stderr,"efault\n");
      break;
    case EOVERFLOW:
      fprintf(stderr,"eoverflow\n");
      break;
    default:
      fprintf(stderr,"gettime ret: %d\n",ret);
    }
    assert(false);
  }
}

inline __attribute__((always_inline))
uint64_t R_LOCKED_WORD(uint64_t txn_start_time, uint64_t duration) {
  return ((txn_start_time << (8+1)) | ((duration & 0xff) << 1));
}

inline __attribute__((always_inline))
uint64_t W_LOCKED_WORD(uint64_t txn_start_time, uint64_t owner_id) {
  return ((txn_start_time << (8+1)) | ((owner_id & 0xff) << 1) | W_LOCKED);
}

inline __attribute__((always_inline))
bool EXPIRED(uint64_t txn_start_time, uint64_t duration) {
  return get_now_ntp() > (txn_start_time + duration) + DELTA;
}

inline __attribute__((always_inline))
bool VALID(uint64_t txn_start_time, uint64_t duration) {
  return get_now_ntp() < (txn_start_time + duration) - DELTA;
}

} // namespace rwlock_4_waitdie

} // namespace rtx

} // namespace nocc

#endif
