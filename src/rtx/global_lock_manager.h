#ifndef GLOBAL_LOCK_MANAGER_H
#define GLOBAL_LOCK_MANAGER_H

#include <map>
#include <list>
#include <mutex>
#include "msg_format.hpp"
#include "rwlock.hpp"
#include "core/rrpc.h"
// #include "core/rworker.h"

#define CONFLICT_WRITE_FLAG 73
// #define SUNDIAL_WAIT_NO_LOCK
namespace nocc {

namespace rtx {

struct RdmaValHeader {
  uint64_t lock;
  uint64_t seq;
};

struct lock_waiter_t {
	req_lock_type_t type;
	int pid;
	int tid;
	int cid;
	uint64_t txn_start_time;
	RTXSundialReadItem item;
	MemDB* db;
};

struct wait_list_node {
    volatile uint64_t* lock_ptr;
    lock_waiter_t waiter;
    wait_list_node* next = NULL;
    wait_list_node* prev = NULL;
    wait_list_node(volatile uint64_t* ptr, lock_waiter_t wait):lock_ptr(ptr),waiter(wait){}
};

class GlobalLockManager {
public:

	GlobalLockManager() {
		waiters = new std::map<volatile uint64_t*, std::vector<lock_waiter_t> >[20];
    }
    int which_worker = -1;
    wait_list_node* head = NULL;
    int cnt = 0;
    inline __attribute__((always_inline))
	void add_to_waitlist(volatile uint64_t* lock_addr, lock_waiter_t waiter) {

        //LOG(3) << "start at " << waiter.pid << ' ' << waiter.cid << ' ' << waiter.txn_start_time;
        if(head == NULL) head = new wait_list_node(lock_addr, waiter);
        else {
            head->prev = new wait_list_node(lock_addr, waiter);
            head->prev->next = head;
            head = head->prev;
        }
        cnt++;
    }
	//inline __attribute__((always_inline))
	//void add_to_waitlist(volatile uint64_t* lock_addr, lock_waiter_t waiter) {
    //    if(which_worker == -1) which_worker = waiter.tid;
    //    assert(waiter.tid == which_worker);
	//	(waiters[waiter.cid])[lock_addr].push_back(waiter);
    //    //auto it = waiters->find(lock_addr);
    //    //if(it == waiters->end()) {
    //    //   waiters->insert(std::pair<volatile uint64_t*, std::vector<lock_waiter_t> >(lock_addr, std::vector<lock_waiter_t>()));
    //    //}
	//	//(*waiters)[lock_addr].push_back(waiter);
	//}
    
	inline __attribute__((always_inline))
	void check_to_notify(int my_worker_id, oltp::RRpc *rpc_) {
        using namespace rwlock_4_waitdie;
        if(head == NULL) return;
        auto iter = head;
        while(iter != NULL) {
        //if(waiters[i].size() == 0) continue;
		uint8_t res = LOCK_SUCCESS_MAGIC;
			volatile uint64_t* lockptr = iter->lock_ptr;
            
			lock_waiter_t& first_waiter = iter->waiter;
			ASSERT(first_waiter.tid == my_worker_id) << first_waiter.tid << ' ' << my_worker_id;

			char* reply_msg = rpc_->get_reply_buf();
			size_t more = 0;
		    switch(first_waiter.type) {
		      case RTX_REQ_LOCK_READ:
		      case RTX_REQ_LOCK_WRITE:
		       	{
			       	uint64_t my_ts = R_LEASE(first_waiter.txn_start_time);
			       	if(first_waiter.type == RTX_REQ_LOCK_WRITE) {
			       		my_ts += 1;
			       	}
			        while (true) {
						volatile uint64_t l = *lockptr;
						if(l == 0) {
							if(unlikely(!__sync_bool_compare_and_swap(lockptr, l,
								my_ts))) {
								continue;
							}
							else {
								res = LOCK_SUCCESS_MAGIC;
								goto SUCCESS;	
							}
						}
						else if(my_ts < l) {
							// goon waiting
							goto NEXT_ITEM;
						}
						else {
							// LOG(3) << "dengbudao suo";
							res = LOCK_FAIL_MAGIC;
							goto SUCCESS;
						}
		          	}
		      	}
		        break;
		    case SUNDIAL_REQ_LOCK_READ: { // sundial lock and read
		    	auto node = local_lookup_op(first_waiter.item.tableid, first_waiter.item.key, first_waiter.db);
		    	while(true) {
		    		volatile uint64_t l = *lockptr;
		    		if(l == 0) {
		    			if(unlikely(!__sync_bool_compare_and_swap(lockptr, 0, first_waiter.txn_start_time))){
							LOG(3) << "fail change lock";
		    				//continue;
                            goto NEXT_ITEM;
						}
		    			else {
		    				prepare_buf(reply_msg, &first_waiter.item, first_waiter.db);
		    				more = first_waiter.item.len + sizeof(SundialResponse);
		    				// LOG(3) << "dengdao suo";
		    				res = LOCK_SUCCESS_MAGIC;
		    				goto SUCCESS;
		    			}
		    			goto NEXT_ITEM;
		    		}
		    		else if(first_waiter.txn_start_time < l) {
		    			goto NEXT_ITEM;
		    		}
		    		else {
		    			res = LOCK_FAIL_MAGIC;
		    			// LOG(3) << "dengbudao suo";
		    			goto SUCCESS;
		    		}
		    	}
		    }
            break;
		      default:
		        assert(false);
		    }

SUCCESS:
	  		*((uint8_t *)reply_msg) = res;
	  		rpc_->send_reply(reply_msg,sizeof(uint8_t) + more, first_waiter.pid, first_waiter.cid);
            //LOG(3) << "back to " << first_waiter.pid << ' ' << first_waiter.cid << ' ' << first_waiter.txn_start_time;
            if(iter->prev != NULL) {
                iter->prev->next = iter->next;
                if(iter->next != NULL)
                    iter->next->prev = iter->prev;
            }
            else {
                assert(iter == head);
                head = head->next;
                if(head != NULL)
                    head->prev = NULL;
            }
            --cnt;
            assert(cnt >=0);
NEXT_ITEM:
            iter = iter->next;
    }
    }

	//inline __attribute__((always_inline))
	//void check_to_notify(int my_worker_id, oltp::RRpc *rpc_) {
	//using namespace rwlock_4_waitdie;
	//if(waiters == NULL) return;
    //for(int i = 0; i < 20; ++i){
    //    if(waiters[i].size() == 0) continue;
	//	uint8_t res = LOCK_SUCCESS_MAGIC;
	//	for (auto itr = waiters[i].begin(); itr != waiters[i].end(); ) {
	//		volatile uint64_t* lockptr = itr->first;
	//		//assert(itr->second.size() > 0);
    //        //LOG(3)<<waiters->size();
    //        if(itr->second.size() == 0) {
    //            //itr++;
    //            //waiters->erase(itr++);
    //            itr = waiters[i].erase(itr);
    //            //continue;
    //            break;
    //        }
	//		lock_waiter_t& first_waiter = *(itr->second.begin());
	//		ASSERT(first_waiter.tid == my_worker_id) << first_waiter.tid << ' ' << my_worker_id;

	//		char* reply_msg = rpc_->get_reply_buf();
	//		size_t more = 0;
	//	    switch(first_waiter.type) {
	//	      case RTX_REQ_LOCK_READ:
	//	      case RTX_REQ_LOCK_WRITE:
	//	       	{
	//		       	uint64_t my_ts = R_LEASE(first_waiter.txn_start_time);
	//		       	if(first_waiter.type == RTX_REQ_LOCK_WRITE) {
	//		       		my_ts += 1;
	//		       	}
	//		        while (true) {
	//					volatile uint64_t l = *lockptr;
	//					if(l == 0) {
	//						if(unlikely(!__sync_bool_compare_and_swap(lockptr, l,
	//							my_ts))) {
	//							continue;
	//						}
	//						else {
	//							res = LOCK_SUCCESS_MAGIC;
	//							goto SUCCESS;	
	//						}
	//					}
	//					else if(my_ts < l) {
	//						// goon waiting
	//						goto NEXT_ITEM;
	//					}
	//					else {
	//						// LOG(3) << "dengbudao suo";
	//						res = LOCK_FAIL_MAGIC;
	//						goto SUCCESS;
	//					}
	//	          	}
	//	      	}
	//	        break;
	//	    case SUNDIAL_REQ_LOCK_READ: { // sundial lock and read
	//	    	auto node = local_lookup_op(first_waiter.item.tableid, first_waiter.item.key, first_waiter.db);
	//	    	while(true) {
	//	    		volatile uint64_t l = *lockptr;
	//	    		if(l == 0) {
	//	    			if(unlikely(!__sync_bool_compare_and_swap(lockptr, 0, first_waiter.txn_start_time))){
	//						LOG(3) << "fail change lock";
	//	    				//continue;
    //                        goto NEXT_ITEM;
	//					}
	//	    			else {
	//	    				prepare_buf(reply_msg, &first_waiter.item, first_waiter.db);
	//	    				more = first_waiter.item.len + sizeof(SundialResponse);
	//	    				// LOG(3) << "dengdao suo";
	//	    				res = LOCK_SUCCESS_MAGIC;
	//	    				goto SUCCESS;
	//	    			}
	//	    			goto NEXT_ITEM;
	//	    		}
	//	    		else if(first_waiter.txn_start_time < l) {
	//	    			goto NEXT_ITEM;
	//	    		}
	//	    		else {
	//	    			res = LOCK_FAIL_MAGIC;
	//	    			// LOG(3) << "dengbudao suo";
	//	    			goto SUCCESS;
	//	    		}
	//	    	}
	//	    }
    //        break;
	//	      default:
	//	        assert(false);
	//	    }

//SUCCESS:
	//  		*((uint8_t *)reply_msg) = res;
	//  		rpc_->send_reply(reply_msg,sizeof(uint8_t) + more, first_waiter.pid, first_waiter.cid);
    //        //LOG(3) << "back to " << first_waiter.pid << ' ' << first_waiter.cid << ' ' << first_waiter.
    //        itr->second.begin()->pid = -1;
	//		itr->second.erase(itr->second.begin());
//NEXT_ITEM:
	//		//if (itr->second.empty()) {
	//		//	itr = waiters->erase(itr);
	//		//} else {
	//		//	++itr;
	//		//}
    //        itr++;
	//	}
	//}
    //}
	void thread_local_init() {
		//waiters = new std::map<volatile uint64_t*, std::vector<lock_waiter_t> >();

	}
#include "occ_internal_structure.h"
private:

	inline __attribute__((always_inline))
	MemNode *local_get_op(MemNode *node,char *val,uint64_t &seq,int len,int meta) {
	retry: // retry if there is a concurrent writer
	  char *cur_val = (char *)(node->value);
	  seq = node->seq;
	  asm volatile("" ::: "memory");
	#if INLINE_OVERWRITE
	  memcpy(val,node->padding + meta,len);
	#else
	  memcpy(val,cur_val + meta,len);
	#endif
	  asm volatile("" ::: "memory");
	  if( unlikely(node->seq != seq || seq == CONFLICT_WRITE_FLAG) ) {
	    goto retry;
	  }
	  return node;
	}

	inline __attribute__((always_inline))
	MemNode *local_lookup_op(int tableid,uint64_t key, MemDB *db) {
	  MemNode *node = db->stores_[tableid]->Get(key);
	  return node;
	}

	inline __attribute__((always_inline))
	MemNode * local_get_op(int tableid,uint64_t key,char *val,int len,uint64_t &seq,int meta, MemDB* db) {
	  MemNode *node = local_lookup_op(tableid,key, db);
	  assert(node != NULL);
	  assert(node->value != NULL);
	  return local_get_op(node,val,seq,len,meta);
	}




public:
	// a map from the lock addr to a vector of waiters.
	std::map<volatile uint64_t*, std::vector<lock_waiter_t> >* waiters = NULL;
	// static std::mutex* mtx;

	// the set of lock addresses each thread needs to keep an eye on.
	// thread_local static std::map<volatile uint64_t*, waiter_t*>* locks_to_check;

	bool prepare_buf(char* reply_msg, RTXSundialReadItem *item, MemDB* db) {
		char* reply = reply_msg + 1;
		uint64_t seq;
		auto node = local_get_op(item->tableid, item->key, reply + sizeof(SundialResponse),
			item->len, seq, db->_schemas[item->tableid].meta_len, db);

		RdmaValHeader* header = (RdmaValHeader*)((char*)(node->value));
		SundialResponse *reply_item = (SundialResponse*)reply;
		// reply_item->wts = WTS(node->read_lock);
		// reply_item->rts = RTS(node->read_lock);
		reply_item->wts = WTS(header->seq);
		reply_item->rts = RTS(header->seq);

		*((uint8_t*)reply_msg) = LOCK_SUCCESS_MAGIC;
		return true;
	}

	bool prepare_buf(char* reply_msg, uint32_t tableid, uint64_t key, int len, MemDB* db) {
		char* reply = reply_msg + 1;
		uint64_t seq;
		auto node = local_get_op(tableid, key, reply + sizeof(SundialResponse),
			len, seq, db->_schemas[tableid].meta_len, db);
		SundialResponse *reply_item = (SundialResponse*)reply;
		// reply_item->wts = WTS(node->read_lock);
		// reply_item->rts = RTS(node->read_lock);
		RdmaValHeader* header = (RdmaValHeader*)((char*)(node->value));
		reply_item->wts = WTS(header->seq);
		reply_item->rts = RTS(header->seq);
		*((uint8_t*)reply_msg) = LOCK_SUCCESS_MAGIC;
		return true;
	}

};


} // namespace rtx

} // namespace nocc

#endif
