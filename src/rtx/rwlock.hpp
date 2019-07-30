#ifndef NOCC_RTX_RWLOCK_H
#define NOCC_RTX_RWLOCK_H

#define USE_RWLOCK 1
// #include "core/rrpc.h"
#include "util/timer.h"
// #include "db/txs/ts_manager.hpp"


// the following macros are used by namespace rwlock
#define R_LEASE(end_time) ((end_time) << (1+8))
#define END_TIME(state) ((get_now() & (~0x7fffffffffffff)) | (state) >> (1+8))

// the following macros are used by namespace rwlock_4_waitdie
#define START_TIME(state) ((get_now() & (~0x7fffffffffffff)) | (state) >> (1+8))
#define LEASE_DURATION(state) (((state) & 0x1ff) >> 1)

namespace nocc {

namespace rtx {

namespace rwlock {

// the state of rwlock
// read lease end time (55 bits) / owner machine ID (8 bits) / write_lock (1 bit)
const uint64_t INIT = 0x0;
const uint64_t W_LOCKED = 0x1;
const uint64_t DELTA = 50; // 50 micro-seconds
const uint64_t LEASE_TIME = 400; // 0.4 milli-seconds

// get current wall-time to the precision of microseconds
inline __attribute__((always_inline))
uint64_t get_now() {
  return nocc::util::get_now();
}

inline __attribute__((always_inline))
uint64_t LOCKED(uint64_t owner_id) {
  return ((owner_id & 0xff) << 1) | W_LOCKED;
}

inline __attribute__((always_inline))
bool EXPIRED(uint64_t end_time) {
  return get_now() > (end_time) + DELTA;
}

inline __attribute__((always_inline))
bool VALID(uint64_t end_time) {
  return get_now() < (end_time) - DELTA;
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
  return nocc::util::get_now();
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
  return (get_now() > (txn_start_time + duration) + DELTA);
}

inline __attribute__((always_inline))
bool VALID(uint64_t txn_start_time, uint64_t duration) {
  return (get_now() < (txn_start_time + duration) - DELTA);
}

} // namespace rwlock_4_waitdie

} // namespace rtx

} // namespace nocc

#endif