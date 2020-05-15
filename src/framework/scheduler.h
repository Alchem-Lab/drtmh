#ifndef NOCC_OLTP_SCHEDULER_H
#define NOCC_OLTP_SCHEDULER_H

//#include "framework.h"

#include "all.h"
#include "core/utils/thread.h"
#include "util/util.h"
#include "util/spinlock.h"
#include "bench_worker.h"
#include <string>
#include <vector>

using namespace std;
using namespace rdmaio;

extern     RdmaCtrl *cm;
#define MAX_REQUESTS_NUM 10

namespace nocc {

	using namespace util;

	namespace oltp {

		class Scheduler : public RWorker {
		public:
			Scheduler(unsigned worker_id, RdmaCtrl *cm, MemDB* db);
			~Scheduler();
  			virtual void run();
  			virtual void worker_routine(yield_func_t &yield);
  			virtual void exit_handler();
  			// static void chseck_to_notify(int worker_id_, std::vector<SingleQueue*>& ready_reqs);
		private:
			SpinLock sequence_lock;
			std::vector<std::vector<char*> > req_buffers;
			std::vector<det_request> deterministic_plan;
			std::vector<std::vector<std::queue<det_request>* > > locked_transactions;
			std::vector<SpinLock*> locks_4_locked_transactions;
			volatile int det_batch_ready = 0;
			std::map<volatile uint64_t*, std::queue<int> > waitinglist;
			MemDB * db_;
			BreakdownTimer timer_;
			void thread_local_init();
			bool request_lock(int req_seq, bool from_waitinglist);
			#include "rtx/occ_statistics.h"
		private:

		friend class BenchWorker;
		friend class Sequencer;
		};
	}

}


#endif
