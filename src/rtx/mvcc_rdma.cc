#include "mvcc_rdma.h"
#include "rdma_req_helper.hpp"
namespace nocc {

namespace rtx {


void MVCC::release_reads(yield_func_t &yield) {
  return; // no need release read, there is no read lock
}

void MVCC::release_writes(yield_func_t &yield, bool all) {
  int release_num = write_set_.size();
  if(!all) {
    release_num -= 1;
}
}



void MVCC::register_default_rpc_handlers() {
  // register rpc handlers
}

}
}