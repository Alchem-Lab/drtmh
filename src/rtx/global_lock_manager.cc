#include "global_lock_manager.h"

namespace nocc {

namespace rtx {

thread_local std::map<volatile uint64_t*, std::vector<lock_waiter_t> >* GlobalLockManager::waiters = NULL;

} // namespace rtx

} // namespace nocc