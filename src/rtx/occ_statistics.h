/**
 * This file should be included in occ.h
 */

LAT_VARS(lock);
LAT_VARS(validate);
LAT_VARS(commit);
LAT_VARS(log);
LAT_VARS(twopc)
LAT_VARS(temp);
LAT_VARS(read_lat);
LAT_VARS(renew_lease);
LAT_VARS(release_write);
LAT_VARS(release_read);

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


void report_statics(uint64_t one_second) {

  lock_.erase(0.1);
  //validate_.erase(0.1);
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
  LOG(4) << "renew_lease time: "  << util::BreakdownTimer::rdtsc_to_ms(renew_lease_.average(),one_second) << "ms";
  LOG(4) << "commit time: "  << util::BreakdownTimer::rdtsc_to_ms(commit_.average(),one_second) << "ms";

  LOG(4)   << util::BreakdownTimer::rdtsc_to_ms(read_lat_.average(),one_second)     ;
  LOG(4) << util::BreakdownTimer::rdtsc_to_ms(lock_.average(),one_second)           ;
  LOG(4)   << util::BreakdownTimer::rdtsc_to_ms(release_write_.average(),one_second);
  LOG(4)  << util::BreakdownTimer::rdtsc_to_ms(renew_lease_.average(),one_second)   ;
  LOG(4) << util::BreakdownTimer::rdtsc_to_ms(commit_.average(),one_second)         ;

  LOG(4)<< "log time: " << util::BreakdownTimer::rdtsc_to_ms(log_.average(),one_second)            ;
  LOG(4)<< "2pc time: " << util::BreakdownTimer::rdtsc_to_ms(twopc_.average(),one_second)            ;
  LOG(4) << "temp time: "  << util::BreakdownTimer::rdtsc_to_ms(temp_.average(),one_second) << "ms";
  LOG(4) << "release_read time: "  << util::BreakdownTimer::rdtsc_to_ms(release_read_.average(),one_second) << "ms";
}

void record() {
  double res;
  REPORT_V(lock,res);
  lock_.push_back(res);

  REPORT_V(log,res);
  log_.push_back(res);

  REPORT_V(twopc,res);
  twopc_.push_back(res);

  REPORT_V(commit,res);
  commit_.push_back(res);

  REPORT_V(temp,res);
  temp_.push_back(res);

  REPORT_V(read_lat,res);
  read_lat_.push_back(res);

  REPORT_V(renew_lease, res);
  renew_lease_.push_back(res);


  REPORT_V(release_read,res);
  release_read_.push_back(res);

  REPORT_V(release_write, res);
  release_write_.push_back(res);
}

#else
// no counting, return null
void report_statics(uint64_t) {

}

void record() {

}
#endif
