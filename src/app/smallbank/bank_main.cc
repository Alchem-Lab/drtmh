#include "tx_config.h"

#include "core/logging.h"
#include "rtx/occ.h"
#include "rtx/occ_rdma.h"

#include "bank_schema.h"
#include "bank_worker.h"
#include "bank_log_cleaner.h"

#include "db/txs/dbsi.h"
#include "db/txs/ts_manager.hpp"

#ifdef CALVIN_TX
#include "db/txs/epoch_manager.hpp"
#include "framework/sequencer.h"
#include "framework/scheduler.h"
#elif defined(BOHM_TX)
#include "framework/sequencer.h"
#include "framework/scheduler.h"
#endif

#include "framework/bench_runner.h"

#include "rdmaio.h"
#include "rtx/global_vars.h"
using namespace rdmaio;

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <string>

using namespace std;

extern size_t scale_factor;
extern size_t nthreads;
extern size_t nclients;
extern size_t current_partition;
extern size_t total_partition;
extern int verbose;
extern uint64_t ops_per_worker;
extern int ycsb_set_length;
extern int ycsb_write_num;
extern int num_hot;
extern int num_accounts;
extern int sleep_time;
extern int tx_hot;

namespace nocc {

extern RdmaCtrl *cm;       // global RDMA handler

#ifdef CALVIN_TX
extern db::EpochManager* epoch_manager;
extern oltp::Sequencer* sequencer;
extern oltp::Scheduler* scheduler;
#elif defined(BOHM_TX)
extern oltp::Sequencer* sequencer;
extern oltp::Scheduler* scheduler;
#endif

namespace oltp {

extern char *store_buffer; // the buffer used to store DrTM-kv
extern MemDB *backup_stores_[MAX_BACKUP_NUM];

namespace bank {

/* sp, dc, payment, ts, wc, aml */
//unsigned g_txn_workload_mix[6] = {25,15,15,15,15,15};
unsigned g_txn_workload_mix[7] = {0,0,0,0,0,0,100};

class BankClient : public BenchClient {
 public:
  BankClient(unsigned worker_id,unsigned seed) : BenchClient(worker_id,seed) {

  }
  virtual int get_workload(char *input,util::fast_random &rand) {
    // pick execute machine
    uint pid  = rand.next() % total_partition;

    // pick tx idx
    static auto workload = BankWorker::_get_workload();
    double d = rand.next_uniform();

    uint tx_idx = 0;
    for(size_t i = 0;i < workload.size();++i) {
      if((i + 1) == workload.size() || d < workload[i].frequency) {
        tx_idx = i;
        break;
      }
      d -= workload[i].frequency;
    }
    *(uint8_t *)input = tx_idx;
    return pid;
  }
};

class BankMainRunner : public BenchRunner  {
 public:
  BankMainRunner(std::string &config_file) ;
  virtual void init_put() {}
  virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB* store = NULL);
  virtual std::vector<RWorker *> make_workers();
  virtual std::vector<BackupBenchWorker *> make_backup_workers();
  virtual void init_store(MemDB* &store);
  virtual void init_backup_store(MemDB* &store);
  virtual void populate_cache();

  virtual void bootstrap_with_rdma(RdmaCtrl *r) {
  }

  virtual void warmup_buffer(char *buffer) {
  }
};


void BankTest(int argc,char **argv) {
  BankMainRunner runner (nocc::oltp::config_file_name);
  runner.run();
  return ;
}

BankMainRunner::BankMainRunner(std::string &config_file) : BenchRunner(config_file) {

  using boost::property_tree::ptree;
  using namespace boost;
  using namespace property_tree;

  // parse input xml
  try {
    // parse each TX's ratio
    ptree pt;
    read_xml(config_file,pt);

    int sp = pt.get<int> ("bench.bank.sp");
    int dc = pt.get<int> ("bench.bank.dc");
    int payment = pt.get<int> ("bench.bank.payment");
    int ts = pt.get<int> ("bench.bank.ts");
    int wc = pt.get<int> ("bench.bank.wc");
    int aml = pt.get<int> ("bench.bank.aml");
    int ycsb = pt.get<int> ("bench.bank.ycsb");
    ycsb_set_length = pt.get<int>("bench.ycsb.set_len");
    ycsb_write_num = pt.get<int>("bench.ycsb.write_num");
    
    tx_hot = pt.get<int>("bench.ycsb.tx_hot");
    num_hot = pt.get<int>("bench.ycsb.num_hot");
    num_accounts = pt.get<int>("bench.ycsb.num_accounts");
    sleep_time = pt.get<int>("bench.ycsb.sleep_time");
    LOG(3)<< "ycsb param:" << "set len=" << ycsb_set_length << " write num=" << 
        ycsb_write_num << "tx hot" << tx_hot << "num_hot" << num_hot << "num_accounts " << num_accounts;
    g_txn_workload_mix[0] = sp;
    g_txn_workload_mix[1] = dc;
    g_txn_workload_mix[2] = payment;
    g_txn_workload_mix[3] = ts;
    g_txn_workload_mix[4] = wc;
    g_txn_workload_mix[5] = aml;
    g_txn_workload_mix[6] = ycsb;
    LOG(3) << "here" << ycsb;

  } catch (const ptree_error &e) {
    //pass
  }

  fprintf(stdout,"[Bank]: check workload %u, %u, %u, %u, %u, %u, %u\n",
          g_txn_workload_mix[0],g_txn_workload_mix[1],g_txn_workload_mix[2],g_txn_workload_mix[3],
          g_txn_workload_mix[4],g_txn_workload_mix[5],g_txn_workload_mix[6]);
}

void BankMainRunner::init_store(MemDB* &store){
  assert(store == NULL);
  // Should not give store_buffer to backup MemDB!
  store = new MemDB(store_buffer);

  int meta_size = META_SIZE;
#if 1
// #if ONE_SIDED_READ
#if BOHM_TX
  meta_size = 0;  // we don't need the meta header since the bohm record 
                  // already contains the metadata of a version of record
#elif MVCC_TX
  meta_size = sizeof(rtx::MVCCHeader);
#else  
  meta_size = sizeof(rtx::RdmaValHeader);
#endif

#endif

  //store->AddSchema(ACCT, TAB_HASH,sizeof(uint64_t),sizeof(account::value),meta_size);
#if MVCC_TX
  store->AddSchema(SAV,  TAB_HASH,sizeof(uint64_t),sizeof(savings::value) * MVCC_VERSION_NUM,meta_size,
                   NumAccounts() / total_partition * 1.5);
  store->AddSchema(CHECK,TAB_HASH,sizeof(uint64_t),sizeof(checking::value) * MVCC_VERSION_NUM,meta_size,
                   NumAccounts() / total_partition * 1.5);
  store->AddSchema(YCSB, TAB_HASH, sizeof(uint64_t), sizeof(ycsb_record::value) * MVCC_VERSION_NUM, meta_size,
                   NumAccounts() / total_partition * 1.5);
#else
  store->AddSchema(SAV,  TAB_HASH,sizeof(uint64_t),sizeof(savings::value),meta_size,
                   NumAccounts() / total_partition * 1.5);
  store->AddSchema(CHECK,TAB_HASH,sizeof(uint64_t),sizeof(checking::value),meta_size,
                   NumAccounts() / total_partition * 1.5);
  store->AddSchema(YCSB, TAB_HASH, sizeof(uint64_t), sizeof(ycsb_record::value), meta_size,
                   NumAccounts() / total_partition * 1.5);
#endif

#if 1
// #if ONE_SIDED_READ == 1
  //store->EnableRemoteAccess(ACCT,cm);
  store->EnableRemoteAccess(SAV,cm);
  store->EnableRemoteAccess(CHECK,cm);
  store->EnableRemoteAccess(YCSB,cm);
#endif
}

void BankMainRunner::init_backup_store(MemDB* &store){
  assert(store == NULL);
  store = new MemDB();
  int meta_size = META_SIZE;

#if 1
// #if ONE_SIDED_READ
  meta_size = sizeof(rtx::RdmaValHeader);
#endif

#if MVCC_TX
  assert(false);
#endif

  store->AddSchema(SAV,  TAB_HASH,sizeof(uint64_t),sizeof(savings::value),meta_size,
                   NumAccounts() / total_partition,false);
  store->AddSchema(CHECK,TAB_HASH,sizeof(uint64_t),sizeof(checking::value),meta_size,
                   NumAccounts() / total_partition,false);
}

class BankLoader : public BenchLoader {
  MemDB *store_;
  bool is_primary_;
 public:
  BankLoader(unsigned long seed, int partition, MemDB *store, bool is_primary) : BenchLoader(seed) {
    store_ = store;
    partition_ = partition;
    is_primary_ = is_primary;
  }

  void load() {

#if 1
// #if ONE_SIDED_READ == 1
    if(is_primary_)
      RThreadLocalInit();
#endif
    fprintf(stdout,"[Bank], total %lu accounts loaded\n", NumAccounts());
    int meta_size = store_->_schemas[CHECK].meta_len;

    char acct_name[32];
    const char *acctNameFormat = "%lld 32 d";

    uint64_t loaded_acct(0),loaded_hot(0);

    for(uint64_t i = 0;i <= NumAccounts();++i){

      uint64_t pid = AcctToPid(i);
      assert(0 <= pid && pid < total_partition);
      if(pid != partition_) continue;

      uint64_t round_sz = CACHE_LINE_SZ << 1; // 128 = 2 * cacheline to avoid false sharing

      char *wrapper_acct(NULL), *wrapper_saving(NULL), *wrapper_check(NULL), *wrapper_ycsb(NULL);
#if MVCC_TX
      int save_size = meta_size + MVCC_VERSION_NUM * sizeof(savings::value);
#elif BOHM_TX
      assert(sizeof(savings::value) < MAX_RECORD_LEN);
      int save_size = meta_size + sizeof(BOHMRecord);
#else
      int save_size = meta_size + sizeof(savings::value);
#endif
      save_size = Round<int>(save_size,sizeof(uint64_t));
      
#if MVCC_TX
      int check_size = meta_size + MVCC_VERSION_NUM * sizeof(checking::value);
#elif BOHM_TX
      assert(sizeof(checking::value) < MAX_RECORD_LEN);
      int check_size = meta_size + sizeof(BOHMRecord);
#else
      int check_size = meta_size + sizeof(checking::value); 
#endif
      check_size = Round<int>(check_size, sizeof(uint64_t));
      ASSERT(check_size % sizeof(uint64_t) == 0) << "cache size " << check_size;

#if MVCC_TX
      int ycsb_size = meta_size + MVCC_VERSION_NUM * sizeof(ycsb_record::value);
#elif BOHM_TX
      assert(sizeof(ycsb_record::value) < MAX_RECORD_LEN);
      int ycsb_size = meta_size + sizeof(BOHMRecord);
#else
      int ycsb_size = meta_size + sizeof(ycsb_record::value); 
#endif
      ycsb_size = Round<int>(ycsb_size, sizeof(uint64_t));

#if 1
// #if ONE_SIDED_READ == 1
      if(is_primary_){
        wrapper_saving = (char *)Rmalloc(save_size);
        wrapper_check  = (char *)Rmalloc(check_size);
        wrapper_ycsb = (char*)Rmalloc(ycsb_size);
      } else {
        wrapper_saving = new char[save_size];
        wrapper_check  = new char[check_size];
        wrapper_ycsb = new char[ycsb_size];
      }
#else
      wrapper_saving = new char[save_size];
      wrapper_check  = new char[check_size];
#endif
      wrapper_acct = new char[meta_size + sizeof(account::value)];
      assert(wrapper_saving != NULL);
      assert(wrapper_check != NULL);
      assert(wrapper_acct != NULL);
      assert(wrapper_ycsb != NULL);

      loaded_acct += 1;

      if(i < NumHotAccounts())
        loaded_hot += 1;

      sprintf(acct_name,acctNameFormat,i);

      memset(wrapper_acct, 0, meta_size);
      memset(wrapper_saving, 0, meta_size);
      memset(wrapper_check,  0, meta_size);
      memset(wrapper_ycsb, 0, meta_size);

      account::value *a = (account::value*)(wrapper_acct + meta_size);
      a->a_name.assign(std::string(acct_name) );

      float balance_c = (float)_RandomNumber(random_generator_, MIN_BALANCE, MAX_BALANCE);
      float balance_s = (float)_RandomNumber(random_generator_, MIN_BALANCE, MAX_BALANCE);

#if BOHM_TX
      BOHMRecord* record = (BOHMRecord*)(wrapper_saving + meta_size);
      savings::value *s = (savings::value *)record->data;
      s->s_balance = balance_s;
      auto node = store_->Put(SAV,i,(uint64_t *)wrapper_saving,sizeof(BOHMRecord));
#else
      savings::value *s = (savings::value *)(wrapper_saving + meta_size);
      s->s_balance = balance_s;
#if MVCC_TX
      ((rtx::MVCCHeader *)wrapper_saving)->wts[0] = 1;
#endif
      auto node = store_->Put(SAV,i,(uint64_t *)wrapper_saving,sizeof(savings::value));
#endif
// here send the size of value instead of mvcc_version_num * sizeof(value), the value inside is of no use

      if (is_primary_) {
      // if(is_primary_ && ONE_SIDED_READ) {
        node->off = (uint64_t)wrapper_saving - (uint64_t)(cm->conn_buf_);
        ASSERT(node->off % sizeof(uint64_t) == 0) << "saving value size " << save_size;
      }

#if BOHM_TX
      BOHMRecord* record = (BOHMRecord*)(wrapper_saving + meta_size);
      checking::value *c = (checking::value *)record->data;
      c->c_balance = balance_c;
      assert(c->c_balance > 0);
      node = store_->Put(CHECK,i,(uint64_t *)wrapper_check,sizeof(BOHMRecord));
#else
      checking::value *c = (checking::value *)(wrapper_check + meta_size);
      c->c_balance = balance_c;
      assert(c->c_balance > 0);
#if MVCC_TX
      // TODO: MVCC may set the meta data to correct
      ((rtx::MVCCHeader *)wrapper_check)->wts[0] = 1; // first wts
#endif
      node = store_->Put(CHECK,i,(uint64_t *)wrapper_check,sizeof(checking::value));
#endif

      if(i == 0)
        LOG(3) << "check cv balance " << c->c_balance;

      if (is_primary_) {
      // if(is_primary_ && ONE_SIDED_READ) {
        node->off =  (uint64_t)wrapper_check - (uint64_t)(cm->conn_buf_);
        ASSERT(node->off % sizeof(uint64_t) == 0) << "check value size " << check_size;
      }

#if BOHM_TX
      node = store_->Put(YCSB, i, (uint64_t*)wrapper_ycsb, sizeof(BOHMRecord));
#else
#if MVCC_TX
      ((rtx::MVCCHeader*)wrapper_ycsb)->wts[0] = 1;
#endif
      node = store_->Put(YCSB, i, (uint64_t*)wrapper_ycsb, sizeof(ycsb_record::value));
#endif

      if(is_primary_) {
        node->off = (uint64_t)wrapper_ycsb - (uint64_t)(cm->conn_buf_);
      }

      assert(node->seq == 2);

    }
    //check_remote_traverse();
  }

  void check_remote_traverse() {
    // check remote traverse, if possible
    char *rbuf = (char *)(cm->conn_buf_);

    int dev_id = cm->get_active_dev(0);
    int port_idx = cm->get_active_port(0);

    cm->thread_local_init();
    cm->open_device(dev_id);
    cm->register_connect_mr(dev_id); // register memory on the specific device

    cm->link_connect_qps(nthreads + nthreads + 1,dev_id,port_idx,0,IBV_QPT_RC);

    Qp *qp = cm->get_rc_qp(nthreads + nthreads + 1,current_partition,0);

    for(uint i = 0;i <= NumAccounts();++i) {

      uint64_t pid = AcctToPid(i);
      if(pid != current_partition) continue;

      // fetch
      MemNode *res_local = store_->stores_[CHECK]->Get(i);
      MemNode remote_node;
      uint64_t remote_off = store_->stores_[CHECK]->RemoteTraverse(i,qp,(char *)(&remote_node));
      assert((rbuf + remote_off) == (char *)res_local);
      assert(memcmp((char *)res_local,(char *)(&remote_node),sizeof(MemNode)) == 0);
    }
    fprintf(stdout,"check passed!\n");
  }
};


std::vector<BenchLoader *> BankMainRunner::make_loaders(int partition, MemDB* store) {
  std::vector<BenchLoader *> ret;
  if(store == NULL){
    ret.push_back(new BankLoader(9234,partition,store_,true));
  } else {
    ret.push_back(new BankLoader(9234,partition,store,false));
  }
  return ret;
}

std::vector<RWorker *> BankMainRunner::make_workers() {
  std::vector<RWorker *> ret;
  util::fast_random r(23984543 + current_partition * 73);

  for(uint i = 0;i < nthreads;++i) {
    ret.push_back(new BankWorker(i,r.next(),store_,ops_per_worker,&barrier_a_,&barrier_b_,this));
  }

#if SI_TX
  // add ts worker
  ts_manager = new TSManager(nthreads + nclients + 1,cm,0,0);
  ret.push_back(ts_manager);
#endif

#if defined(NOWAIT_TX) || defined(WAITDIE_TX) || defined(SUNDIAL_TX) || defined(MVCC_TX)
  // add ts worker
  ts_manager = new TSManager(nthreads + nclients + 1,cm,0,0);
  ret.push_back(ts_manager);  
#endif

#if defined(CALVIN_TX)
  // // add epoch manager
  epoch_manager = new EpochManager(nthreads + nclients + 1,cm,0,0);
  ret.push_back(epoch_manager);

  // note that the thread id nthreads + nclients + 2 is already occupied 
  // by the BenchLocalListener

  sequencer = new oltp::Sequencer(nthreads + nclients + 3,cm,0,BankWorker::_get_workload);
  ret.push_back(sequencer);

  scheduler = new oltp::Scheduler(nthreads + nclients + 4,cm,store_);
  ret.push_back(scheduler);
#endif

#if defined(BOHM_TX)
  sequencer = new oltp::Sequencer(nthreads + 1, 0, BankWorker::_get_workload);
  ret.push_back(sequencer);

  scheduler = new oltp::Scheduler(nthreads + 2);
  ret.push_back(scheduler);
#endif

#if CS == 1
  for(uint i = 0;i < nclients;++i)
    ret.push_back(new BankClient(nthreads + i,r.next()));
#endif
  return ret;
}

std::vector<BackupBenchWorker *> BankMainRunner::make_backup_workers() {
  std::vector<BackupBenchWorker *> ret;
  assert(false);
  int num_backups = my_view->is_backup(current_partition);
  LogCleaner* log_cleaner = new BankLogCleaner;
  for(uint j = 0; j < num_backups; j++){
    assert(backup_stores_[j] != NULL);
    log_cleaner->add_backup_store(backup_stores_[j]);
  }

  DBLogger::set_log_cleaner(log_cleaner);
  for(uint i = 0; i < backup_nthreads; i++){
    ret.push_back(new BackupBenchWorker(i));
  }
  return ret;
}

void BankMainRunner::populate_cache() {

#if RDMA_CACHE == 1
// #if ONE_SIDED_READ == 1 && RDMA_CACHE == 1
  LOG(2) << "loading cache.";

  // create a temporal QP for usage
  int dev_id = cm->get_active_dev(0);
  int port_idx = cm->get_active_port(0);

  cm->thread_local_init();
  cm->open_device(dev_id);
  cm->register_connect_mr(dev_id); // register memory on the specific device

  cm->link_connect_qps(nthreads + nthreads + 1,dev_id,port_idx,0,IBV_QPT_RC);

  // calculate the time of populating the cache
  struct  timeval start;
  struct  timeval end;

  gettimeofday(&start,NULL);

  auto db = store_;
  char *temp = (char *)Rmalloc(256);

  for(uint64_t i = 0;i <= NumAccounts();++i) {

    auto pid = AcctToPid(i);

    auto off = db->stores_[CHECK]->RemoteTraverse(i,
                                                  cm->get_rc_qp(nthreads + nthreads + 1,pid,0),temp);
    assert(off != 0);

    off = db->stores_[SAV]->RemoteTraverse(i,
                                           cm->get_rc_qp(nthreads + nthreads + 1,pid,0),temp);
    assert(off != 0);

    off = db->stores_[YCSB]->RemoteTraverse(i,
                                           cm->get_rc_qp(nthreads + nthreads + 1,pid,0),temp);
    assert(off != 0);

    if(i % (total_partition * 10000) == 0)
      PrintProgress((double)i / NumAccounts());

  }
  Rfree(temp);
  gettimeofday(&end,NULL);

  auto diff = (end.tv_sec-start.tv_sec) + (end.tv_usec - start.tv_usec) /  1000000.0;
  fprintf(stdout,"Time to loader the caching is %f second\n",diff);
#endif
}


uint64_t NumAccounts(){
  return (uint64_t)(num_accounts * total_partition * scale_factor);
}
uint64_t NumHotAccounts(){
  return (uint64_t)(num_hot * total_partition * scale_factor);
}

uint64_t GetStartAcct() {
  return current_partition * num_accounts * scale_factor;
}

uint64_t GetEndAcct() {
  return (current_partition + 1) * num_accounts * scale_factor - 1;
}

}; // end namespace bank
}; // end banesspace oltp
} // end namespace nocc
