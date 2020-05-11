#ifndef NOCC_OLTP_SCHEDULER_H
#define NOCC_OLTP_SCHEDULER_H

//#include "framework.h"

#include "all.h"
#include "core/utils/thread.h"
#include "util/util.h"
#include "bench_worker.h"
#include <string>
#include <vector>

using namespace std;
using namespace rdmaio;

extern     RdmaCtrl *cm;

namespace nocc {

	using namespace util;

	namespace oltp {

		class Scheduler : public RWorker {
		public:
			Scheduler(unsigned worker_id);
			~Scheduler();
  			virtual void run();
  			virtual void worker_routine(yield_func_t &yield);
  			virtual void exit_handler();
		private:
			std::vector<std::vector<char*> > req_buffers;
			std::vector<det_request> deterministic_plan;
			volatile int det_batch_ready = 0;
			std::map<volatile uint64_t*, std::queue<int> > waitinglist;
			BreakdownTimer timer_;
			void thread_local_init();
			bool request_lock(int req_seq);
			#include "rtx/occ_statistics.h"
		private:
			//rpc handler
			void sequencer_rpc_handler(int id,int cid,char *msg,void *arg);

		friend class BenchWorker;
		};
	}

}


#endif
