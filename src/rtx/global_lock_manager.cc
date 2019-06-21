#include "global_lock_manager.h"

namespace nocc {

namespace rtx {

std::mutex* GlobalLockManager::mtx = new std::mutex();
std::map<volatile uint64_t*, std::vector<lock_waiter_t> >* GlobalLockManager::waiters = new std::map<volatile uint64_t*, std::vector<lock_waiter_t> >();
thread_local std::map<volatile uint64_t*, uint64_t>* GlobalLockManager::locks_to_check = NULL;

} // namespace rtx

} // namespace nocc