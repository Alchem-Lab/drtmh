#ifndef GLOBAL_VARS_H_
#define GLOBAL_VARS_H_

#include "view.h"
#include "global_lock_manager.h"
#define MVCC_VERSION_NUM 4

namespace nocc {

namespace rtx {

/**
 * New meta data for each record
 */
struct RdmaValHeader {
  uint64_t lock;
  uint64_t seq;
};

struct MVCCHeader {
	uint64_t lock;
	uint64_t rts;
	uint64_t wts[MVCC_VERSION_NUM];
	// uint64_t off[MVCC_VERSION_NUM];
};

extern SymmetricView *global_view;
extern GlobalLockManager *global_lock_manager;

} // namespace rtx
} // namespace nocc

#endif
