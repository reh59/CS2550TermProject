// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BENCHMARK_THREADBENCH_H__
#define BENCHMARK_THREADBENCH_H__

#include <stdint.h>
#include <vector>

namespace benchmark {

/** Interface for performing work in a performance test. */
class BenchmarkWorker {
public:
    virtual ~BenchmarkWorker() {}

    /** Called once in the worker thread/process before the test begins. */
    virtual void initialize() = 0;

    /** Called repeatedly in the worker's measurement loop to do work.
    @param measure true if this call begins in the measurement period. */
    virtual void work(bool measure) = 0;

    /** Called once at the end of the test, before the thread/process exits. This can perform any
    clean-up that might be needed. */
    virtual void finish() {};

    /** Called after the performance test to get results. Required for the forking benchmark,
    since memory is not shared. Not needed for the threaded benchmark. */
    virtual int result() { return 0; }
};

/** @return the number of microseconds the measurement period ran for. */
int64_t runThreadBench(
        int warmup_secs,
        int measure_secs,
        const std::vector<BenchmarkWorker*>& workers,
        int* work_count);

/** Helper template which will create a new temporary vector of the correct type. */
template <typename T>
inline int64_t runThreadBench(
        int warmup_secs,
        int measure_secs,
        const std::vector<T*>& workers,
        int* work_count) {
    return runThreadBench(warmup_secs, measure_secs,
            std::vector<benchmark::BenchmarkWorker*>(workers.begin(), workers.end()), work_count);
}

/** @return the number of work() calls per second. */
double runThreadLatencyBench(
        int warmup_secs,
        int measure_secs,
        const std::vector<BenchmarkWorker*>& workers,
        int desired_requests_per_second,
        std::vector<int>* latencies);

/** Helper template which will create a new temporary vector of the correct type. */
template <typename T>
inline double runThreadLatencyBench(
        int warmup_secs,
        int measure_secs,
        const std::vector<T*>& workers,
        int desired_requests_per_second,
        std::vector<int>* latencies) {
    return runThreadLatencyBench(warmup_secs, measure_secs,
            std::vector<BenchmarkWorker*>(workers.begin(), workers.end()),
            desired_requests_per_second, latencies);
}

/** @return the sum of result() from each worker, divided by the number of measured seconds. */
// TODO: Provide a more generic way for data to be passed back from the benchmark?
double runForkBench(
        int warmup_secs,
        int measure_secs,
        const std::vector<BenchmarkWorker*>& workers);

/** Helper template which will create a new temporary vector of the correct type. */
template <typename T>
inline double runForkBench(
        int warmup_secs,
        int measure_secs,
        const std::vector<T*>& workers) {
    return runForkBench(warmup_secs, measure_secs,
            std::vector<benchmark::BenchmarkWorker*>(workers.begin(), workers.end()));
}

}  // namespace benchmark
#endif
