#include "dslr.h"

namespace nocc {

namespace rtx {

DSLR::DSLR(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
      RdmaCtrl *cm,RScheduler *sched,int ms) :
		TXOpBase(worker,db,rpc_handler,cm,sched,response_node,tid,ms) {// response_node shall always equal *real node id*
		cor_id_ = cid;
		fa_req = new RDMAFALockReq(cid);
		cas_req = new RDMACASLockReq(cid);
		read_req = new RDMAReadReq(cid);
		rdm = new leveldb::Random(time(NULL));
		locked.clear();
}

void DSLR::init() {
	locked.clear();
}

bool DSLR::acquireLock(yield_func_t &yield, Lock& l) {
	if (l.mode == SHARED) {

		fa_req->set_lock_meta(l.remote_off, MAXS, 1, l.local_buf);
	    fa_req->post_reqs(scheduler_,l.qp);

	    // two request need to be polled
	    // if(unlikely(l.qp->rc_need_poll())) {
	    worker_->indirect_yield(yield);
	    // }

		uint64_t prev_lock = *(uint64_t*)l.local_buf;
		uint64_t prev_nx = DECODE_DSLR_LOCK_NX(prev_lock);
		uint64_t prev_ns = DECODE_DSLR_LOCK_NS(prev_lock);
		uint64_t prev_maxx = DECODE_DSLR_LOCK_MAXX(prev_lock);
		uint64_t prev_maxs = DECODE_DSLR_LOCK_MAXS(prev_lock);

#if DEBUG_DSLR
		fprintf(stdout, "[nx=%d, ns=%d, maxx=%d, maxs=%d]\n", 
								prev_nx, prev_ns, prev_maxx, prev_maxs);
#endif

		if (prev_maxs >= COUNT_MAX || prev_maxx >= COUNT_MAX) {
			fa_req->set_lock_meta(l.remote_off, MAXS, (uint64_t)-1, l.local_buf);
		    fa_req->post_reqs(scheduler_,l.qp);

		    // two request need to be polled
		    // if(unlikely(l.qp->rc_need_poll())) {
		    worker_->indirect_yield(yield);
	    	// }

	    	l.consecutive_failure_times += 1;
	    	// perform random backoff: sleep backoff microseconds.
	    	// TODO to improve performance, here indirect_yield may be needed instead of thread sleep.
	   		l.consecutive_failure_times = (l.consecutive_failure_times <= 31) ? l.consecutive_failure_times : 31;
	   		uint32_t backoff = rdm->Uniform(std::min(R*(1<<(l.consecutive_failure_times-1)), L));
	   		// usleep(backoff);
			worker_->indirect_yield_timeout(yield, backoff);
	   		
	   		if (DECODE_DSLR_LOCK_NX(prev_lock) == DECODE_DSLR_LOCK_NX(last_lock) &&
	   				DECODE_DSLR_LOCK_NS(prev_lock) == DECODE_DSLR_LOCK_NS(last_lock) && 
	   				nocc::util::BreakdownTimer::rdtsc_to_microsec(rdtsc()-last_lock_failed_at) > 2*LEASE_TIME) {
	   			uint64_t reset_val = 0;
	   			if (l.mode == SHARED)
	   				reset_val = ENCODE_DSLR_LOCK_CONTENT(prev_maxx, prev_maxs+1, prev_maxx, prev_maxs);
	   			else if (l.mode == EXCLUSIVE)
	   				reset_val = ENCODE_DSLR_LOCK_CONTENT(prev_maxx+1, prev_maxs, prev_maxx, prev_maxs);
	   			reset(yield, l, prev_lock, reset_val);		   		
	   		}

	   		last_lock = prev_lock;
	   		last_lock_failed_at = rdtsc();
	   		//failed to acquire the lock
#if DEBUG_DSLR
	   		fprintf(stdout, "Share lock failed at off %d. prev_maxs or prev_maxx too large!\n", l.remote_off);
#endif
	   		return false;
		} else if (prev_maxs == COUNT_MAX-1) {
			l.resetFrom = ENCODE_DSLR_LOCK_CONTENT(prev_maxx, COUNT_MAX, prev_maxx, COUNT_MAX);
		}

		if (prev_nx == prev_maxx) {
			prev_lock = (uint64_t)-1;
			last_lock_failed_at = 0;
			l.elapsed = rdtsc();
			locked[std::make_pair(l.qp, l.remote_off)] = l;
#if DEBUG_DSLR
			fprintf(stdout, "Share locked at off %d.\n", l.remote_off);
#endif
			return true;
		}
		bool ret = handleConflict(yield, l, prev_lock);
		if (!ret) {
			last_lock = prev_lock;
	   		last_lock_failed_at = rdtsc();
#if DEBUG_DSLR
	   		fprintf(stdout, "Share lock failed at off %d. \n", l.remote_off);
#endif
		} else {
			prev_lock = (uint64_t)-1;
			last_lock_failed_at = 0;
			l.elapsed = rdtsc();
			locked[std::make_pair(l.qp, l.remote_off)] = l;
#if DEBUG_DSLR
			fprintf(stdout, "Share locked at off %d.\n", l.remote_off);
#endif
		}
		return ret;
	} else if (l.mode == EXCLUSIVE) {
		fa_req->set_lock_meta(l.remote_off, MAXX, 1, l.local_buf);
	    fa_req->post_reqs(scheduler_,l.qp);

	    // fa request need to be polled
	    // if(unlikely(l.qp->rc_need_poll())) {
	    worker_->indirect_yield(yield);
	    // }

		uint64_t prev_lock = *(uint64_t*)l.local_buf;
		uint64_t prev_nx = DECODE_DSLR_LOCK_NX(prev_lock);
		uint64_t prev_ns = DECODE_DSLR_LOCK_NS(prev_lock);
		uint64_t prev_maxx = DECODE_DSLR_LOCK_MAXX(prev_lock);
		uint64_t prev_maxs = DECODE_DSLR_LOCK_MAXS(prev_lock);

#if DEBUG_DSLR
		fprintf(stdout, "%d: [nx=%d, ns=%d, maxx=%d, maxs=%d]\n", 
							cor_id_, prev_nx, prev_ns, prev_maxx, prev_maxs);
#endif
		if (prev_maxs >= COUNT_MAX || prev_maxx >= COUNT_MAX) {
			fa_req->set_lock_meta(l.remote_off, MAXX, 0xffffffffffff, l.local_buf);
		    fa_req->post_reqs(scheduler_,l.qp);

		    // two request need to be polled
		    // if(unlikely(l.qp->rc_need_poll())) {
		    worker_->indirect_yield(yield);
	    	// }

			uint64_t reverted_lock = *(uint64_t*)l.local_buf;
			uint64_t reverted_nx = DECODE_DSLR_LOCK_NX(reverted_lock);
			uint64_t reverted_ns = DECODE_DSLR_LOCK_NS(reverted_lock);
			uint64_t reverted_maxx = DECODE_DSLR_LOCK_MAXX(reverted_lock);
			uint64_t reverted_maxs = DECODE_DSLR_LOCK_MAXS(reverted_lock);
#if DEBUG_DSLR
			fprintf(stdout, "%d: before revert: [nx=%d, ns=%d, maxx=%d, maxs=%d]\n", 
							cor_id_, reverted_nx, reverted_ns, reverted_maxx, reverted_maxs);		    
#endif
	      	read_req->set_read_meta(l.remote_off, l.local_buf);
	      	read_req->post_reqs(scheduler_,l.qp);
	      	worker_->indirect_yield(yield);

			reverted_lock = *(uint64_t*)l.local_buf;
			reverted_nx = DECODE_DSLR_LOCK_NX(reverted_lock);
			reverted_ns = DECODE_DSLR_LOCK_NS(reverted_lock);
			reverted_maxx = DECODE_DSLR_LOCK_MAXX(reverted_lock);
			reverted_maxs = DECODE_DSLR_LOCK_MAXS(reverted_lock);

#if DEBUG_DSLR
			fprintf(stdout, "%d: after revert: [nx=%d, ns=%d, maxx=%d, maxs=%d]\n", 
							cor_id_, reverted_nx, reverted_ns, reverted_maxx, reverted_maxs);			      	
#endif
	    	l.consecutive_failure_times += 1;

#if DEBUG_DSLR
	   		fprintf(stdout, "%d: Exclusive lock failed at off %d. prev_maxs or prev_maxx too large!\n", cor_id_, l.remote_off);
#endif
	   		return false;

	    	// perform random backoff: sleep backoff microseconds.
	    	// TODO to improve performance, here indirect_yield may be needed instead of thread sleep.
	   		l.consecutive_failure_times = (l.consecutive_failure_times <= 31) ? l.consecutive_failure_times : 31;
	   		uint32_t backoff = rdm->Uniform(std::min(R*(1<<(l.consecutive_failure_times-1)), L));
	   		// usleep(backoff);
			worker_->indirect_yield_timeout(yield, backoff);

	   		if (DECODE_DSLR_LOCK_NX(prev_lock) == DECODE_DSLR_LOCK_NX(last_lock) &&
	   				DECODE_DSLR_LOCK_NS(prev_lock) == DECODE_DSLR_LOCK_NS(last_lock) &&
	   				nocc::util::BreakdownTimer::rdtsc_to_microsec(rdtsc()-last_lock_failed_at) > 2*LEASE_TIME) {
	   			uint64_t reset_val = 0;
	   			if (l.mode == SHARED)
	   				reset_val = ENCODE_DSLR_LOCK_CONTENT(prev_maxx, prev_maxs+1, prev_maxx, prev_maxs);
	   			else if (l.mode == EXCLUSIVE)
	   				reset_val = ENCODE_DSLR_LOCK_CONTENT(prev_maxx+1, prev_maxs, prev_maxx, prev_maxs);
	   			else
	   				assert(false);
	   			reset(yield, l, prev_lock, reset_val);
	   		}
	   		
	   		last_lock = prev_lock;
	   		last_lock_failed_at = rdtsc();
	   		
	   		//failed to acquire the lock
#if DEBUG_DSLR
	   		fprintf(stdout, "%d: Exclusive lock failed at off %d. prev_maxs or prev_maxx too large!\n", cor_id_, l.remote_off);
#endif
	   		return false;
		} else if (prev_maxx == COUNT_MAX-1) {
			l.resetFrom = ENCODE_DSLR_LOCK_CONTENT(COUNT_MAX, prev_maxs, COUNT_MAX, prev_maxs);
		}

		if (prev_nx == prev_maxx && prev_ns == prev_maxs) {
			prev_lock = (uint64_t)-1;
			last_lock_failed_at = 0;
			l.elapsed = rdtsc();
			locked[std::make_pair(l.qp, l.remote_off)] = l;
#if DEBUG_DSLR
			fprintf(stdout, "%d: Exclusive locked at off %d.\n", cor_id_, l.remote_off);
#endif			
			return true;
		}
		bool ret = handleConflict(yield, l, prev_lock);
		if (!ret) {
			last_lock = prev_lock;
	   		last_lock_failed_at = rdtsc();
#if DEBUG_DSLR
	   		fprintf(stdout, "%d: Exclusive lock failed at off %d. \n", cor_id_, l.remote_off);
#endif		
		} else {
			prev_lock = (uint64_t)-1;
			last_lock_failed_at = 0;
			l.elapsed = rdtsc();
			locked[std::make_pair(l.qp, l.remote_off)] = l;
#if DEBUG_DSLR
			fprintf(stdout, "%d: Exclusive locked at off %d.\n", cor_id_, l.remote_off);
#endif		
		}
		return ret;
	} else {
		assert(false);
	}
}

//Pre condition: lid must be locked, a.k.a, in the locked map.
bool DSLR::releaseLock(yield_func_t &yield, lock_id lid) {
	assert(locked.find(lid) != locked.end());

	Lock& l = locked[lid];
	l.elapsed = rdtsc() - l.elapsed;
	double elapsed = nocc::util::BreakdownTimer::rdtsc_to_microsec(l.elapsed);
	if (elapsed < LEASE_TIME || l.resetFrom > 0) {
		if (l.mode == SHARED) {
			fa_req->set_lock_meta(l.remote_off, NS, 1, l.local_buf);
		    fa_req->post_reqs(scheduler_,l.qp);

		    // fa request need to be polled
		    // if(unlikely(l.qp->rc_need_poll())) {
		    worker_->indirect_yield(yield);
	    	// }

			uint64_t prev_lock = *(uint64_t*)l.local_buf;
			uint64_t prev_nx = DECODE_DSLR_LOCK_NX(prev_lock);
			uint64_t prev_ns = DECODE_DSLR_LOCK_NS(prev_lock);
			uint64_t prev_maxx = DECODE_DSLR_LOCK_MAXX(prev_lock);
			uint64_t prev_maxs = DECODE_DSLR_LOCK_MAXS(prev_lock);
#if DEBUG_DSLR
			fprintf(stdout, "%d: [nx=%d, ns=%d, maxx=%d, maxs=%d]\n", 
							cor_id_, prev_nx, prev_ns, prev_maxx, prev_maxs);
			fprintf(stdout, "%d: Shared locked released at off %d.\n", cor_id_, l.remote_off);	    	
#endif		
		} else if (l.mode == EXCLUSIVE) {
			fa_req->set_lock_meta(l.remote_off, NX, 1, l.local_buf);
		    fa_req->post_reqs(scheduler_,l.qp);

		    // fa request need to be polled
		    // if(unlikely(l.qp->rc_need_poll())) {
		    worker_->indirect_yield(yield);
	    	// }

			uint64_t prev_lock = *(uint64_t*)l.local_buf;
			uint64_t prev_nx = DECODE_DSLR_LOCK_NX(prev_lock);
			uint64_t prev_ns = DECODE_DSLR_LOCK_NS(prev_lock);
			uint64_t prev_maxx = DECODE_DSLR_LOCK_MAXX(prev_lock);
			uint64_t prev_maxs = DECODE_DSLR_LOCK_MAXS(prev_lock);
#if DEBUG_DSLR
			fprintf(stdout, "%d: [nx=%d, ns=%d, maxx=%d, maxs=%d]\n", 
							 cor_id_, prev_nx, prev_ns, prev_maxx, prev_maxs);	    	
			fprintf(stdout, "%d: Exclusive locked released at off %d.\n", cor_id_, l.remote_off);	    	
#endif		
		} else assert(false);

		if (l.resetFrom > 0) {
			while (true) {
		      cas_req->set_lock_meta(l.remote_off,l.resetFrom,0UL,l.local_buf);
		      cas_req->post_reqs(scheduler_,l.qp);

		      // cas request need to be polled
		      // if(unlikely(l.qp->rc_need_poll())) {
		      worker_->indirect_yield(yield);
		      // }

		      if (*(uint64_t*)l.local_buf == l.resetFrom) break;
		  	}
#if DEBUG_DSLR
		  fprintf(stdout, "%d: %d reset to 0 done.\n", cor_id_, l.remote_off);	
#endif		  
		  l.resetFrom = 0;
		}
	}

	return true;
}

bool DSLR::isLocked(lock_id lid) {
	return locked.find(lid) != locked.end();
}

bool DSLR::handleConflict(yield_func_t &yield, Lock& l, uint64_t prev_lock) {
	// uint64_t last_lock = (uint64_t)-1;
	// nocc::util::BreakdownTimer timer;
	
	while (true) {
		  //TODO: This is actually an RDMA read, thus don't use cas_req
	      read_req->set_read_meta(l.remote_off, l.local_buf);
	      read_req->post_reqs(scheduler_,l.qp);

	      // two request need to be polled
	      // if(unlikely(l.qp->rc_need_poll())) {
	      worker_->indirect_yield(yield);
	      // }

	      uint64_t prev_nx = DECODE_DSLR_LOCK_NX(prev_lock);
		  uint64_t prev_ns = DECODE_DSLR_LOCK_NS(prev_lock);
		  uint64_t prev_maxx = DECODE_DSLR_LOCK_MAXX(prev_lock);
		  uint64_t prev_maxs = DECODE_DSLR_LOCK_MAXS(prev_lock);

	      uint64_t cur_lock = *(uint64_t*)l.local_buf;
	      uint64_t val_nx = DECODE_DSLR_LOCK_NX(cur_lock);
		  uint64_t val_ns = DECODE_DSLR_LOCK_NS(cur_lock);
		  uint64_t val_maxx = DECODE_DSLR_LOCK_MAXX(cur_lock);
		  uint64_t val_maxs = DECODE_DSLR_LOCK_MAXS(cur_lock);

	      if (prev_maxx < val_nx || prev_maxs < val_ns) {
	      	return false;
	      }
	      if (l.mode == SHARED) {
	      	if(prev_maxx == val_nx)
	      		return true;
	  	  }
	      else if (l.mode == EXCLUSIVE) {
	      	if (prev_maxx == val_nx && prev_maxs == val_ns)
	      		return true;
	      }

	      //TODO: this possible reset after 2*LEASE_TIME should be fixed.
	      
// 	      if (last_lock != (uint64_t)-1) {
// 	      	if (timer.sum < 2*LEASE_TIME &&
// 	      		DECODE_DSLR_LOCK_NX(last_lock) == DECODE_DSLR_LOCK_NX(cur_lock) &&
// 	      		DECODE_DSLR_LOCK_NS(last_lock) == DECODE_DSLR_LOCK_NS(cur_lock)) {
// 	      		timer.start();
// 	      	} else {
// 	      		timer.end();
// 	      		if (timer.sum >= 2*LEASE_TIME) {
// 	      			uint64_t reset_val = 0;
// 	      			if (l.mode == SHARED) {
// 	      				reset_val = ENCODE_DSLR_LOCK_CONTENT(prev_maxx, prev_maxs+1, val_maxx, val_maxs);
// 	      			} else if (l.mode == EXCLUSIVE) {
// 	      				reset_val = ENCODE_DSLR_LOCK_CONTENT(prev_maxx+1, prev_maxs, val_maxx, val_maxs);
// 	      			}
// 	      			bool ret = reset(yield, l, cur_lock, reset_val);
// #if DEBUG_DSLR
// 	      			fprintf(stdout, "%d: Conflict! Resetting cur_lock to reset_val.\n", cor_id_);
// #endif
// 	      			if (!ret) return ret;
// 	      		}
// 	      	}
// 	      } else
// 		  	last_lock = cur_lock;

	      // int wait_count = (prev_maxx - val_nx) + (prev_maxs - val_ns);
	      // assert(wait_count >= 0);
	      // usleep(wait_count*OMEGA);
	      // worker_->indirect_yield_timeout(yield, wait_count*OMEGA);
	}
}

bool DSLR::reset(yield_func_t &yield, Lock& l, uint64_t val, uint64_t reset_val) {
  	cas_req->set_lock_meta(l.remote_off,val,reset_val,l.local_buf);
  	cas_req->post_reqs(scheduler_,l.qp);

  	// two request need to be polled
  	// if(unlikely(l.qp->rc_need_poll())) {
    worker_->indirect_yield(yield);
  	// }

  	if (*(uint64_t*)l.local_buf == val) {
#if DEBUG_DSLR
   		fprintf(stdout, "%d: %d reset to reset_val done.\n", cor_id_, l.remote_off);
#endif
  		if (DECODE_DSLR_LOCK_MAXX(reset_val) >= COUNT_MAX || DECODE_DSLR_LOCK_MAXS(reset_val) >= COUNT_MAX) {
  			//reset L to zero
  			if (l.resetFrom > 0) {
  				while (true) {
			      	cas_req->set_lock_meta(l.remote_off,l.resetFrom,0,l.local_buf);
			      	cas_req->post_reqs(scheduler_,l.qp);

			      	// two request need to be polled
			      	// if(unlikely(l.qp->rc_need_poll())) {
			        worker_->indirect_yield(yield);
			      	// }

			      	if (*(uint64_t*)l.local_buf == l.resetFrom) break;
  				}
#if DEBUG_DSLR
  				fprintf(stdout, "%d: %d reset to 0 done.\n", cor_id_, l.remote_off);	
#endif
  				l.resetFrom = 0;
  			}
  		}

  		return false;
  	}
	return true;
}


} // namespace rtx

} // namespace nocc