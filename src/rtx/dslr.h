#ifndef NOCC_RTX_DSLR_H_
#define NOCC_RTX_DSLR_H_

#include "rdma_req_helper.hpp"
#include "tx_operator.hpp"
#include "util/random.h"
#include "time.h"
#include <unistd.h>

namespace nocc {

namespace rtx {

class DSLR : public TXOpBase {
public:
	typedef std::pair<Qp*, uint64_t> lock_id;

	enum Mode {
		SHARED = 0,
		EXCLUSIVE = 1
	};

	struct Lock {
	public:
		Lock() {}
		Lock(Qp *qp, uint64_t remote_off, char* local_buf, Mode mode) : 
			qp(qp), remote_off(remote_off), local_buf(local_buf), mode(mode),
			consecutive_failure_times(0), resetFrom(0), elapsed(0) {}
		Lock(const Lock& that) {
			this->qp = that.qp;
			this->remote_off = that.remote_off;
			this->local_buf = that.local_buf;
			this->mode = that.mode;
			this->consecutive_failure_times = that.consecutive_failure_times;
			this->resetFrom = that.resetFrom;
			this->elapsed = that.elapsed;
		}
	private:
		Qp *qp;
		uint64_t remote_off;
		char* local_buf;
		Mode mode;
		uint consecutive_failure_times; // the number of consecutive deadlocks/timeouts for the current lock object.
		uint64_t resetFrom;
		uint64_t elapsed;	// the time elapsed since lock acquisition

		friend class DSLR;
	};

	DSLR(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
      RdmaCtrl *cm,RScheduler *sched,int ms);

	void init();

	bool acquireLock(yield_func_t &yield, Lock& l);

	bool isLocked(DSLR::lock_id lid);
	
	bool releaseLock(yield_func_t &yield, DSLR::lock_id lid);

private:
	const unsigned COUNT_MAX = 32768;
	// const unsigned COUNT_MAX = 1;
	const uint64_t LEASE_TIME = 10*1000;  // 10ms by default
	// const uint64_t OMEGA = 10*1000;  // omega: the default_wait_time: 10ms by default
	const uint64_t OMEGA = 1;
	// the following parameters are used for random backoff
	const uint32_t R = 10; //The default backoff time: 10 micro seconds
	const uint32_t L = 10*1000; //10ms by default

	leveldb::Random* rdm;
	int cor_id_ = 0; 
	RDMAFALockReq* fa_req = NULL;
	RDMACASLockReq* cas_req = NULL;
	RDMAReadReq* read_req = NULL;
	
	uint64_t last_lock = 0;				// the last lock content
	uint64_t last_lock_failed_at = 0;	// the time at which the last lock failed to acquire.	

	struct comp {
		template<typename T>
		bool operator()(const T& l, const T& r) const {
			if (l.first == r.first)
				return l.second < r.second;
			return l.first < r.first;
		}
	};

	// a map of all locked locks with each lock represented by a pair of qp and remote_off.
	std::map<lock_id, Lock, comp> locked;

private:
	bool handleConflict(yield_func_t &yield, Lock& l, uint64_t prev_lock);

	bool reset(yield_func_t &yield, Lock& l, uint64_t val, uint64_t reset_val);
};


} // namespace rtx

} // namespace nocc

#endif