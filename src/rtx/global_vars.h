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

struct MVCCHeader {
	uint64_t lock;
	uint64_t rts;
#if USE_LINKED_LIST_FOR_MVCC
	uint64_t wts;
#else
	uint64_t wts[MVCC_VERSION_NUM];
#endif
	// uint64_t off[MVCC_VERSION_NUM];
};

extern SymmetricView *global_view;
extern GlobalLockManager *global_lock_manager;

//extern int ycsb_set_length;
//extern int ycsb_write_num;


} // namespace rtx
} // namespace nocc

#endif
