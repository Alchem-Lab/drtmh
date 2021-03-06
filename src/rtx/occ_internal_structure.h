// class RtxOCC
protected:
// a simple ReadSetItem for buffering read/write records
struct ReadSetItem {
  uint8_t  tableid;
  uint32_t  len;
  uint64_t key;
  union {
    MemNode *node;
    uint64_t off;
    char* value;
  };
  char    *data_ptr;
  uint64_t seq; // buffered seq
  uint8_t  pid;

  inline ReadSetItem(int tableid,uint64_t key,MemNode *node,char *data_ptr,uint64_t seq,int len,int pid):
      tableid(tableid),
      key(key),
      node(node),
      data_ptr(data_ptr),
      seq(seq),
      len(len),
      pid(pid)
  {
  }

  inline ReadSetItem(const ReadSetItem &item) :
      tableid(item.tableid),
      key(item.key),
      node(item.node),
      data_ptr(item.data_ptr),
      seq(item.seq),
      len(item.len),
      pid(item.pid)
  {
  }

}  __attribute__ ((aligned (8)));

struct SundialReadSetItem : public ReadSetItem {
  uint32_t wts = 0, rts = 0;
  inline SundialReadSetItem(int tableid,uint64_t key,MemNode *node,char *data_ptr,uint64_t seq,
    int len,int pid, int wts = 0, int rts = 0): ReadSetItem (tableid, key, node, data_ptr, seq, len,
    pid), wts(wts), rts(rts)
    {
    }

  inline SundialReadSetItem(const SundialReadSetItem &item) : ReadSetItem(item) {
    wts = item.wts;
    rts = item.rts;
  }
};

struct RtxLockItem {
  uint8_t pid;
  uint8_t tableid;
  uint64_t key;
  uint64_t seq;
  uint64_t index; // the index of the lock item in the write set so that
                  // the lock handler can correctly reply with the this index
                  // so that the lock requester can safely mark each item
                  // as 'locked' in the write set
                  // so that following potential one-sided release of lock
                  // can pinpoint that right item in the write set to release.
  RtxLockItem(uint8_t pid,uint8_t tableid,uint64_t key,uint64_t seq,uint64_t idx = -1)
      :pid(pid),tableid(tableid),key(key),seq(seq), index(idx)
  {
  }
} __attribute__ ((aligned (8)));

struct CommitItem {
  uint32_t len;
  uint32_t tableid;
  uint64_t key;
} __attribute__ ((aligned (8)));

struct ReadItem {
  uint32_t pid;
  uint32_t tableid;
  uint64_t key;
} __attribute__ ((aligned (8)));


struct ReplyHeader {
  uint16_t num;
};

struct OCCLockReplyHeader {
  uint16_t num;
  uint8_t lock_status; // this status is the success/failure of the locking operation.
};

struct OCCLockResponse {
  uint16_t idx;
  uint8_t status; // this status is the locking status of the tuple. Being locked means a release is required.
};

struct OCCResponse {
  uint16_t payload;
  uint16_t idx;
  uint64_t seq;
};

struct WaitDieResponse {
  uint16_t payload;
  uint16_t idx;
};

struct SundialResponse {
  uint32_t wts;
  uint32_t rts;
};

/* 16 bit mac | 6 bit thread | 10 bit cor_id  */
#define ENCODE_LOCK_CONTENT(mac,tid,cor_id) ( ((mac) << 16) | ((tid) << 10) | (cor_id) )
#define DECODE_LOCK_MAC(lock) (((lock) & 0xffffffff ) >> 16)
#define DECODE_LOCK_TID(lock) (((lock) & 0xffff ) >> 10)
#define DECODE_LOCK_CID(lock) ( (lock) & 0x3ff)
