#ifndef GLOBAL_VARS_H_
#define GLOBAL_VARS_H_

#include "view.h"
#include "global_lock_manager.h"

namespace nocc {

namespace rtx {

/**
 * New meta data for each record
 */
struct RdmaValHeader {
  uint64_t lock;
  uint64_t seq;
};

extern SymmetricView *global_view;
extern GlobalLockManager *global_lock_manager;

} // namespace rtx
} // namespace nocc

#endif
