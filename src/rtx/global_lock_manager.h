#ifndef GLOBAL_LOCK_MANAGER_H
#define GLOBAL_LOCK_MANAGER_H

#include <map>
#include <list>
#include <mutex>
#include "msg_format.hpp"
#include "rwlock.hpp"
#include "core/rrpc.h"

namespace nocc {

namespace rtx {

struct lock_waiter_t {
	req_lock_type_t type;
	int pid;
	int tid;
	int cid;
	uint64_t txn_start_time;
};

class GlobalLockManager {
public:
	GlobalLockManager() {}

	inline __attribute__((always_inline))
	void add_to_waitlist(volatile uint64_t* lock_addr, lock_waiter_t& waiter) {
		mtx->lock();
		(*waiters)[lock_addr].push_back(waiter);
		mtx->unlock();
		if (locks_to_check->find(lock_addr) == locks_to_check->end())
			(*locks_to_check)[lock_addr] = 0;
		(*locks_to_check)[lock_addr]++;
	}

	inline __attribute__((always_inline))
	void check_to_notify(int my_worker_id, oltp::RRpc *rpc_) {
	using namespace rwlock_4_waitdie;

		for (auto itr = locks_to_check->begin(); itr != locks_to_check->end(); ) {
			volatile uint64_t* lockptr = itr->first;
			assert(itr->second > 0);

			mtx->lock();
			assert(!(*waiters)[lockptr].empty());
			lock_waiter_t& first_waiter = *(*waiters)[lockptr].begin();
			mtx->unlock();

			if (first_waiter.tid != my_worker_id)
				continue;

			char* reply_msg = rpc_->get_reply_buf();

		    switch(first_waiter.type) {
		      case RTX_REQ_LOCK_READ: {
		          while (true) {
		            uint64_t l = *lockptr;
		            if(l & 0x1 == W_LOCKED) {
		            	goto NEXT_ITEM; //continue waiting
		            } else {
		              if (EXPIRED(START_TIME(l), LEASE_DURATION(l))) {
		                // clear expired lease (optimization)
		                if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
		                             R_LOCKED_WORD(first_waiter.txn_start_time, rwlock::LEASE_TIME))))
		                  continue;
		                else
		                  goto SUCCESS;  //successfully read locked this item
		              } else { // read locked: not conflict
		                goto SUCCESS;    //successfully read locked this item
		              }
		            }
		          }
		      }
		        break;
		      case RTX_REQ_LOCK_WRITE: {
		        while(true) {
		          uint64_t l = *lockptr;
		          if(l & 0x1 == W_LOCKED) {
		          	goto NEXT_ITEM;  //continue waiting
		          } else {
		            if (EXPIRED(START_TIME(l), LEASE_DURATION(l))) {
		              // clear expired lease (optimization)
		              if( unlikely(!__sync_bool_compare_and_swap(lockptr,l,
		                           W_LOCKED_WORD(first_waiter.txn_start_time, first_waiter.pid))))
		                continue;
		              else
		                goto SUCCESS;
		            } else { //read locked: conflict
		            	goto NEXT_ITEM; //continue waiting
		            }
		          }
		        }
		      }
		        break;
		      default:
		        assert(false);
		    }

SUCCESS:
	  		*((uint8_t *)reply_msg) = LOCK_SUCCESS_MAGIC;
	  		rpc_->send_reply(reply_msg,sizeof(uint8_t),first_waiter.pid,first_waiter.cid);
			itr->second -= 1;
			mtx->lock();
			(*waiters)[lockptr].erase((*waiters)[lockptr].begin());
			mtx->unlock();
NEXT_ITEM:
			if (itr->second == 0) {
				itr = locks_to_check->erase(itr);
			} else {
				++itr;
			}
		}
	}

	void thread_local_init() {
		locks_to_check = new std::map<volatile uint64_t*, uint64_t>();
	}
private:


public:
	// a map from the lock addr to a vector of waiters.
	static std::map<volatile uint64_t*, std::vector<lock_waiter_t> >* waiters;
	static std::mutex* mtx;

	// the set of lock addresses each thread needs to keep an eye on.
	thread_local static std::map<volatile uint64_t*, uint64_t>* locks_to_check;
};


} // namespace rtx

} // namespace nocc

#endif