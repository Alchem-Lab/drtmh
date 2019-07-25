#pragma once

// This header contains common msg definiation used in RTX
#include "memstore/memdb.h"

namespace nocc {

namespace rtx {

enum req_type_t {
  RTX_REQ_READ = 0,
  RTX_REQ_READ_LOCK,
  RTX_REQ_INSERT
};

// header of the requests message
struct RTXRequestHeader {
  uint8_t  cor_id;
  int8_t   num;     // num TX items in the message
  uint64_t padding; // a padding is left for user-defined entries
} __attribute__ ((aligned (8)));

struct RTXReadItem {
  req_type_t type;
  uint8_t  pid;
  uint64_t key;
  uint8_t  tableid;
  uint16_t idx;
  uint16_t len;
  inline RTXReadItem(req_type_t type,uint8_t pid,uint64_t key,uint8_t tableid,uint16_t len,uint8_t idx)
      :type(type),pid(pid),key(key),tableid(tableid),len(len),idx(idx)
  {
  }
  RTXReadItem(){}
} __attribute__ ((aligned (8)));

struct RTXSundialReadItem {
  uint8_t  pid;
  uint64_t key;
  uint8_t  tableid;
  uint16_t len;
  inline RTXSundialReadItem(uint8_t pid,uint64_t key,uint8_t tableid,uint16_t len)
      :pid(pid),key(key),tableid(tableid),len(len)
  {
  }
  RTXSundialReadItem(){}
} __attribute__ ((aligned (8)));

struct RTXSundialUnlockItem {
  uint8_t  pid;
  uint64_t key;
  uint8_t  tableid;
  inline RTXSundialUnlockItem(uint8_t pid,uint64_t key,uint8_t tableid)
      :pid(pid),key(key),tableid(tableid)
  {
  }
  RTXSundialUnlockItem(){}
} __attribute__ ((aligned (8)));

struct RTXMVCCUnlockItem {
  uint8_t  pid;
  uint64_t key;
  uint8_t  tableid;
  uint64_t txn_starting_timestamp;
  inline RTXMVCCUnlockItem(uint8_t pid,uint64_t key,uint8_t tableid, uint64_t txn_starting_timestamp)
      :pid(pid),key(key),tableid(tableid),txn_starting_timestamp(txn_starting_timestamp)
  {
  }
} __attribute__ ((aligned (8)));

struct RTXRenewLeaseItem {
  uint8_t pid;
  uint8_t tableid;
  uint64_t key;
  uint32_t wts;
  uint32_t commit_id;
  inline RTXRenewLeaseItem(uint8_t pid, uint8_t tableid, uint64_t key, uint32_t wts, uint32_t commit_id)
      : pid(pid), tableid(tableid),key(key), wts(wts), commit_id(commit_id)
  {
  }
} __attribute__ ((aligned (8)));

// entries of the request message
struct RTXRemoteWriteItem {
  uint8_t pid;
  union {
    MemNode *node;
    uint64_t key;
  };
  uint16_t payload;
  uint8_t  tableid;
} __attribute__ ((aligned (8)));

struct RtxWriteItem {
  uint8_t  pid;
  uint8_t  tableid;
  uint64_t key;
  uint16_t len;
  RtxWriteItem(int pid,int tableid,uint64_t key,int len) :
      pid(pid),tableid(tableid),key(key),len(len) {

  }
} __attribute__ ((aligned (8)));

struct RTXUpdateItem {
  uint8_t  pid;
  uint8_t  tableid;
  uint64_t key;
  uint16_t len;
  uint32_t commit_id;
  RTXUpdateItem(int pid,int tableid,uint64_t key,int len, uint32_t commit_id) :
      pid(pid),tableid(tableid),key(key),len(len), commit_id(commit_id) {

  }
} __attribute__ ((aligned (8)));

enum req_lock_type_t {
  RTX_REQ_LOCK_READ = 0,
  RTX_REQ_LOCK_WRITE,
  RTX_REQ_LOCK_INSERT, // may be not necessary?
  SUNDIAL_REQ_READ,
  SUNDIAL_REQ_LOCK_READ,
};

struct RTXLockRequestItem {
  req_lock_type_t type;
  uint8_t  pid;
  uint8_t  tableid;
  uint64_t key;
  uint64_t seq;
  uint64_t txn_starting_timestamp;
  inline RTXLockRequestItem(req_lock_type_t type,uint8_t pid,uint8_t tableid,uint64_t key,uint64_t seq,uint64_t txn_start_time)
      :type(type),pid(pid),tableid(tableid),key(key),seq(seq),txn_starting_timestamp(txn_start_time)
  {
  }
} __attribute__ ((aligned (8)));

struct RTXMVCCWriteRequestItem {
  uint8_t  pid;
  uint8_t  tableid;
  uint64_t key;
  uint16_t len;
  uint64_t txn_starting_timestamp;
  inline RTXMVCCWriteRequestItem(uint8_t pid,uint64_t key,uint8_t tableid, uint16_t len, uint64_t txn_start_time)
      :pid(pid),key(key),tableid(tableid),len(len),txn_starting_timestamp(txn_start_time)
  {
  }
} __attribute__ ((aligned (8)));

#define LOCK_SUCCESS_MAGIC 73
#define LOCK_FAIL_MAGIC 12
#define LOCK_WAIT_MAGIC 41
#define RPC_SUCCESS_MAGIC 26
#define RPC_FAIL_MAGIC 27

}; // namespace rtx
}; // namespace nocc
