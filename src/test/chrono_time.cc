#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <cassert>


inline __attribute__ ((always_inline))
uint64_t rdtsc(void)
{
  uint32_t hi, lo;
  __asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)lo)|(((uint64_t)hi)<<32);
}

int main ()
{
    using namespace std::chrono;
    // Get current time with precision of microseconds
    auto now = time_point_cast<microseconds>(system_clock::now());
    // sys_microseconds is type time_point<system_clock, microseconds>
    using sys_microseconds = decltype(now);
    // Convert time_point to signed integral type
    auto integral_duration = now.time_since_epoch().count();
    // Convert signed integral type to time_point
    sys_microseconds dt{microseconds{integral_duration}};
    // test
    // if (dt != now)
    //     std::cout << "Failure." << std::endl;
    // else
    //     std::cout << "Success." << std::endl;
    assert(dt == now);
    fprintf(stdout, "%lu\n", integral_duration);
    fprintf(stdout, "%x\n", integral_duration);
    fprintf(stdout, "%x\n", rdtsc());
    return 0;
}