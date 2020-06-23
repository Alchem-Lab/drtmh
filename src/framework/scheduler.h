#ifndef NOCC_OLTP_SCHEDULER_H
#define NOCC_OLTP_SCHEDULER_H

//#include "framework.h"

#include "all.h"
#include "core/utils/thread.h"
#include "util/util.h"
#include "util/spinlock.h"
#include "util/mapped_log.h"
#include "bench_worker.h"
#include <string>
#include <vector>
#include <boost/lockfree/queue.hpp>

using namespace std;
using namespace rdmaio;

extern     RdmaCtrl *cm;
#define MAX_REQUESTS_NUM MAX_CALVIN_REQ_CNTS

// struct QNode {
// 	volatile struct QNode* next;
// 	char* _buf;
// 	QNode(char* buf) {
// 		_buf = buf;
// 		next = NULL;
// 	}
// };

// struct ConcurrentQ {
// 	ConcurrentQ() {
// 		head = tail = NULL;
// 		dummyHead = new QNode(head);
// 	}
// public:
// 	void enqueue(char* buf) {
// 		assert(buf != NULL);
// 		QNode* n = new QNode(buf);
// 		n->next = head;

// 	}
// 	dequeue() {

// 	}
// private:
// 	struct QNode* dummyHead;	//always points to the first dummy node.
// 	volatile struct QNode* head;
// 	volatile struct QNode* tail;	
// };

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
			
			enum {
				BUFFER_INIT = 0,
				BUFFER_RECVED = 1,
				BUFFER_READY = 2
			} BufferState;
#if ONE_SIDED_READ == 0
			char*** req_buffers;
			volatile int** req_buffer_state;
#elif ONE_SIDED_READ == 1
			volatile int** req_buffer_state;   //unused. only declared to make compiler happy.
			char*** req_buffers;
			uint64_t** offsets_;
#else
#endif
			std::vector<det_request> deterministic_plan;
			volatile int req_fullfilled = 0;
			volatile int epoch_done = -1;
			std::vector<std::vector<boost::lockfree::queue<det_request*>* > > locked_transactions;
			// std::vector<SpinLock*> locks_4_locked_transactions;
			volatile int det_batch_ready = 0;
			std::map<volatile uint64_t*, std::queue<int> > waitinglist;
			MemDB * db_;
			BreakdownTimer timer_;
			void thread_local_init();
			void request_lock(int req_seq, yield_func_t &yield);
			#include "rtx/occ_statistics.h"
		private:

		friend class BenchWorker;
		friend class Sequencer;
		};
	}

}


#endif
