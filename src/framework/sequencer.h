#ifndef NOCC_OLTP_SEQUENCER_H
#define NOCC_OLTP_SEQUENCER_H

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

		typedef workload_desc_vec_t (*get_workload_func_t)();
		
		class Sequencer : public RWorker {
		public:
			Sequencer(unsigned worker_id, unsigned seed, get_workload_func_t get_wl_func);
			~Sequencer();
  			virtual void run();
  			virtual void worker_routine(yield_func_t &yield);
  			virtual void exit_handler();
		private:
			std::vector<det_request> batch;
			get_workload_func_t get_workload_func;
			BreakdownTimer timer_;
			void thread_local_init();

		public:
			#include "rtx/occ_statistics.h"

		private:
			//backup rpc handler
			void logging_rpc_handler(int id,int cid,char *msg,void *arg);
		};
	}

}


#endif
