/**
 * This file should be included in occ.h
 */

LAT_VARS(lock);
LAT_VARS(validate);
LAT_VARS(commit);
LAT_VARS(log);
LAT_VARS(twopc);
LAT_VARS(temp);
LAT_VARS(read_lat);
LAT_VARS(renew_lease);
LAT_VARS(release_write);
LAT_VARS(release_read);

CYCLE_VARS(lock);
CYCLE_VARS(validate);
CYCLE_VARS(commit);
CYCLE_VARS(log);
CYCLE_VARS(twopc);
CYCLE_VARS(temp);
CYCLE_VARS(read);
CYCLE_VARS(renew_lease);
CYCLE_VARS(release_write);
CYCLE_VARS(release_read);

#if NOCC_STATICS
CountVector<double> lock_;
CountVector<double> validate_;
CountVector<double> commit_;
CountVector<double> log_;
CountVector<double> twopc_;
CountVector<double> temp_;
CountVector<double> read_lat_;
CountVector<double> renew_lease_;
CountVector<double> release_write_;
CountVector<double> release_read_;

CountVector<double> lock_cycle_;
CountVector<double> validate_cycle_;
CountVector<double> commit_cycle_;
CountVector<double> log_cycle_;
CountVector<double> twopc_cycle_;
CountVector<double> temp_cycle_;
CountVector<double> read_cycle_;
CountVector<double> renew_lease_cycle_;
CountVector<double> release_write_cycle_;
CountVector<double> release_read_cycle_;

void report_statics(uint64_t one_second) {

  lock_.erase(0.1);
  validate_.erase(0.1);
  commit_.erase(0.1);
  log_.erase(0.1);
  twopc_.erase(0.1);
  temp_.erase(0.1);
  read_lat_.erase(0.1);
  renew_lease_.erase(0.1);
  release_read_.erase(0.1);
  release_write_.erase(0.1);

  LOG(4) << "read_lat time: "  << util::BreakdownTimer::rdtsc_to_ms(read_lat_.average(),one_second) << "ms";
  LOG(4) << "lock time: "    << util::BreakdownTimer::rdtsc_to_ms(lock_.average(),one_second) << "ms";
  LOG(4) << "release_write time: "  << util::BreakdownTimer::rdtsc_to_ms(release_write_.average(),one_second) << "ms";
  LOG(4) << "release_read time: "  << util::BreakdownTimer::rdtsc_to_ms(release_read_.average(),one_second) << "ms";
  LOG(4) << "renew_lease time: "  << util::BreakdownTimer::rdtsc_to_ms(renew_lease_.average(),one_second) << "ms";
  LOG(4) << "validate time: "  << util::BreakdownTimer::rdtsc_to_ms(validate_.average(),one_second) << "ms";
  LOG(4)<< "log time: " << util::BreakdownTimer::rdtsc_to_ms(log_.average(),one_second) << "ms";
  LOG(4)<< "2pc time: " << util::BreakdownTimer::rdtsc_to_ms(twopc_.average(),one_second) << "ms";
  LOG(4) << "commit time: "  << util::BreakdownTimer::rdtsc_to_ms(commit_.average(),one_second) << "ms";

  LOG(4)   << util::BreakdownTimer::rdtsc_to_ms(read_lat_.average(),one_second)     ;
  LOG(4) << util::BreakdownTimer::rdtsc_to_ms(lock_.average(),one_second)           ;
  LOG(4)   << util::BreakdownTimer::rdtsc_to_ms(release_write_.average(),one_second);
  LOG(4)  << util::BreakdownTimer::rdtsc_to_ms(renew_lease_.average(),one_second)   ;
  LOG(4) << util::BreakdownTimer::rdtsc_to_ms(commit_.average(),one_second)         ;


  LOG(4) << "temp time: "  << util::BreakdownTimer::rdtsc_to_ms(temp_.average(),one_second) << "ms";

}

void report_cycle_statics(uint64_t one_second) {

  lock_cycle_.erase(0.1);
  validate_cycle_.erase(0.1);
  commit_cycle_.erase(0.1);
  log_cycle_.erase(0.1);
  twopc_cycle_.erase(0.1);
  temp_cycle_.erase(0.1);
  read_cycle_.erase(0.1);
  renew_lease_cycle_.erase(0.1);
  release_read_cycle_.erase(0.1);
  release_write_cycle_.erase(0.1);

  LOG(4) << "read cpu time: "  << util::BreakdownTimer::rdtsc_to_ms(read_cycle_.average(),one_second) << "ms";
  LOG(4) << "lock cpu time: "    << util::BreakdownTimer::rdtsc_to_ms(lock_cycle_.average(),one_second) << "ms";
  LOG(4) << "release_write cpu time: "  << util::BreakdownTimer::rdtsc_to_ms(release_write_cycle_.average(),one_second) << "ms";
  LOG(4) << "release_read cpu time: "  << util::BreakdownTimer::rdtsc_to_ms(release_read_cycle_.average(),one_second) << "ms";
  LOG(4) << "renew_lease cpu time: "  << util::BreakdownTimer::rdtsc_to_ms(renew_lease_cycle_.average(),one_second) << "ms";
  LOG(4) << "validate cpu time: "  << util::BreakdownTimer::rdtsc_to_ms(validate_cycle_.average(),one_second) << "ms";
  LOG(4)<< "log cpu time: " << util::BreakdownTimer::rdtsc_to_ms(log_cycle_.average(),one_second) << "ms";
  LOG(4)<< "2pc cpu time: " << util::BreakdownTimer::rdtsc_to_ms(twopc_cycle_.average(),one_second) << "ms";
  LOG(4) << "commit cpu time: "  << util::BreakdownTimer::rdtsc_to_ms(commit_cycle_.average(),one_second) << "ms";
  LOG(4) << "temp time: "  << util::BreakdownTimer::rdtsc_to_ms(temp_cycle_.average(),one_second) << "ms";
}

void record() {
  double res;
  REPORT_V(lock,res);
  lock_.push_back(res);

  CYCLE_REPORT_V(lock,res);
  lock_cycle_.push_back(res);

  REPORT_V(validate,res);
  validate_.push_back(res);

  CYCLE_REPORT_V(validate,res);
  validate_cycle_.push_back(res);

  REPORT_V(log,res);
  log_.push_back(res);

  CYCLE_REPORT_V(log,res);
  log_cycle_.push_back(res);

  REPORT_V(twopc,res);
  twopc_.push_back(res);

  CYCLE_REPORT_V(twopc,res);
  twopc_cycle_.push_back(res);

  REPORT_V(commit,res);
  commit_.push_back(res);

  CYCLE_REPORT_V(commit,res);
  commit_cycle_.push_back(res);

  REPORT_V(temp,res);
  temp_.push_back(res);

  CYCLE_REPORT_V(temp,res);
  temp_cycle_.push_back(res);

  REPORT_V(read_lat,res);
  read_lat_.push_back(res);

  CYCLE_REPORT_V(read,res);
  read_cycle_.push_back(res);

  REPORT_V(renew_lease, res);
  renew_lease_.push_back(res);

  CYCLE_REPORT_V(renew_lease, res);
  renew_lease_cycle_.push_back(res);

  REPORT_V(release_read,res);
  release_read_.push_back(res);

  CYCLE_REPORT_V(release_read,res);
  release_read_cycle_.push_back(res);

  REPORT_V(release_write, res);
  release_write_.push_back(res);

  CYCLE_REPORT_V(release_write, res);
  release_write_cycle_.push_back(res);
}

#else
// no counting, return null
void report_statics(uint64_t) {

}

void record() {

}
#endif
