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

#define RPC_DET_BACKUP 28
#define RPC_DET_SEQUENCE 29

namespace nocc {

	using namespace util;

	namespace oltp {

		typedef workload_desc_vec_t (*get_workload_func_t)();
		
		class Sequencer : public RWorker {
		public:
			Sequencer(unsigned worker_id, RdmaCtrl *cm, unsigned seed, get_workload_func_t get_wl_func);
			~Sequencer();
  			virtual void run();
  			virtual void worker_routine(yield_func_t &yield);
  			virtual void exit_handler();
		private:
			std::vector<det_request> batch;
			std::vector<char*> backup_buffers; // the backup batch for current epoch
			get_workload_func_t get_workload_func;
			BreakdownTimer timer_;
			#if ONE_SIDED_READ == 0
			  uint8_t* epoch_status_;
			#else
			  uint64_t** offsets_;
			#endif // ONE_SIDED_READ
			void thread_local_init();
			void logging(char* buffer_start, char* buffer_end, yield_func_t &yield);
			void broadcast(char* buffer_start, char* buffer_end, yield_func_t &yield);
			
			void epoch_sync(yield_func_t &yield);
		public:
			#include "rtx/occ_statistics.h"

		private:
			//backup rpc handler
			void logging_rpc_handler(int id,int cid,char *msg,void *arg);
			//sequence rpc handler
			void sequence_rpc_handler(int id,int cid,char *msg,void *arg);
			//epoch sync rpc handler
			void epoch_sync_rpc_handler(int id,int cid,char *msg,void *arg);
		};
	}

}


#endif
