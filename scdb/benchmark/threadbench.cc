// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "benchmark/benchmark.h"

#include <cassert>
#include <tr1/functional>

#include <unistd.h>

#include "base/chunkedarray.h"
#include "base/function_thread.h"
#include "base/mutex.h"
#include "base/stlutil.h"
#include "base/time.h"

using std::vector;

namespace {

using benchmark::BenchmarkWorker;

// We limit the work queue to fall behind by this much "time" (converted to requests)
// 20 ms because the typically Linux time slice is 10 ms (I think).
static const int WORK_QUEUE_TIME_LIMIT_MS = 20;

/** Contains the state for executing performance tests with multiple threads.
This effectively manages the state machine for the test. */
class BenchmarkState {
public:
    BenchmarkState(int num_threads, bool rate_limited, int queue_limit) :
            num_threads_(num_threads),
            queue_limit_(queue_limit),
            state_(INVALID),
            cond_(&mu_),
            ready_count_(0),
            work_available_(0),
            workers_waiting_(0) {
        assert(num_threads_ > 0);
        if (!rate_limited) {
            work_available_ = -1;
        } else {
            assert(queue_limit_ > 0);
        }
    }
    
    enum State {
        INVALID,
        WARMUP,
        MEASURE,
        DONE,
        EXIT,
    };

    State state() const {
        // See stateChange about volatile
        const volatile State* s = &state_;
        return *s;
    }

    /** Wait for num_threads to call this. Returns once all the threads have entered. */
    void blockForStart() {
        mu_.lock();
        assert(state_ == INVALID);
        assert(0 <= ready_count_ && ready_count_ < num_threads_);
        ready_count_ += 1;
        if (ready_count_ == num_threads_) {
            // we are the last one to enter! Time to exit.
            stateChange(WARMUP);
            cond_.broadcast();
        } else {
            // Wait for the broadcast
            while (ready_count_ < num_threads_) {
                cond_.wait();
            }
        }
        // a thread could take a long time to wake and we could already be in MEASURE
        // this occured while running with valgrind
        assert(state_ != INVALID);
        assert(ready_count_ == num_threads_);
        mu_.unlock();
    }

    void startMeasure() {
        assert(state_ == WARMUP);
        stateChange(MEASURE);
    }

    void doneMeasure() {
        assert(state_ == MEASURE);
        stateChange(DONE);
        // The master thread must also signal that it is done
        signalDone();
    }

    /** Notify that this thread has entered the done state. */
    void signalDone() {
        mu_.lock();
        assert(state_ == DONE);
        assert(0 < ready_count_ && ready_count_ <= num_threads_);
        ready_count_ -= 1;
        if (ready_count_ == 0) {
            // We are the last thread to observe the test finishing
            stateChange(EXIT);
            cond_.broadcast();
        }
        mu_.unlock();
    }

    bool isRateLimited() const {
        // Dirty read, but should be thread-safe due to only being used during initialization
        return work_available_ != -1;
    }

    /** Add a request to do work.
    @return false if the work_available_count hits the limit.
    */
    bool addWork(int amount) {
        assert(amount > 0);

        mu_.lock();
        assert(work_available_ >= 0);
        //~ printf("addWork %d %d\n", work_available_, workers_waiting_);
        if (work_available_ == queue_limit_) {
            mu_.unlock();
            return false;
        }

        // Add a portion that is limited by amount, and by queue_limit_
        amount = std::min(queue_limit_ - work_available_, amount);
        assert(amount > 0);

        int wake_count = std::min(workers_waiting_ - work_available_, amount);
        work_available_ += amount;
        assert(work_available_ <= queue_limit_);
        // Wake workers if some are waiting
        while (wake_count > 0) {
            cond_.signal();
            wake_count -= 1;
        }
        mu_.unlock();

        return true;
    }

    /** Called by ThreadPoolThreads when waiting for work. */
    State fetchWork() {
        mu_.lock();
        //~ printf("fetchWork %d %d\n", work_available_, workers_waiting_);
        assert(work_available_ >= 0);
        if (work_available_ == 0) {
            workers_waiting_ += 1;
            while (work_available_ == 0) {
                if (state() == EXIT) {
                    mu_.unlock();
                    return EXIT;
                }
                cond_.wait();
            }
            workers_waiting_ -= 1;
        }
        assert(work_available_ > 0);
        work_available_ -= 1;

        State out = state();
        mu_.unlock();
        return out;
    }

private:
    const int num_threads_;
    const int queue_limit_;
    State state_;

    void stateChange(State next_state) {
        // Cast to volatile because we want all reads/writes to actually go to memory
        // TODO: On x86 this should be fine, but it may need memory fences on
        // other platforms to unsure atomic uncached reads and writes.
        //~ printf("changing state %d\n", next_state);
        volatile State* s = &state_;
        *s = next_state;
    }

    // The following members are protected by the mutex.
    Mutex mu_;
    Condition cond_;
    // Counts active threads  to wait for start and shutdown
    int ready_count_;

    // Counts available work units (should call doWork())
    int work_available_;
    // Counts workers waiting on work
    int workers_waiting_;
};

class ThreadBenchThread {
public:
    ThreadBenchThread(BenchmarkState* test_state, BenchmarkWorker* work) :
            test_state_(test_state),
            work_(work),
            work_count_(0),
            // Spawn the thread
            thread_(std::tr1::bind(&ThreadBenchThread::run, this)) {
        assert(test_state_ != NULL);
        assert(work_ != NULL);
    }

    void join() {
        thread_.join();
    }

    int work_count() const { return work_count_; }

    void appendLatencies(vector<int>* latencies) const {
        for (int i = 0; i < latencies_.size(); ++i) {
            latencies->push_back(latencies_.at(i));
        }
    }

private:
    void run() {
        const bool rate_limited = test_state_->isRateLimited();

        // initialize in parallel
        work_->initialize();

        // wait for everyone to start running
        test_state_->blockForStart();

        bool seen_done = false;
        BenchmarkState::State state = test_state_->state();
        while (state != BenchmarkState::EXIT) {
            if (state == BenchmarkState::DONE && !seen_done) {
                // This is the first time we have observed that the test is done
                // notify the global test state, then continue applying load
                seen_done = true;
                test_state_->signalDone();
            }

            // apply load
            if (rate_limited) {
                // re-reads the state because it could have changed if we blocked
                state = test_state_->fetchWork();
            }
            
            struct timeval start;
            if (rate_limited && state == BenchmarkState::MEASURE) {
                gettimeofday(&start, NULL);
            }
            work_->work(state == BenchmarkState::MEASURE);
            if (state == BenchmarkState::MEASURE) {
                work_count_ += 1;
                if (rate_limited) {
                    struct timeval end;
                    gettimeofday(&end, NULL);
                    latencies_.push_back((int) base::timevalDiffMicroseconds(end, start));
                }
            }
            state = test_state_->state();
        }

        // Clean up in the thread
        work_->finish();
    }

    BenchmarkState* test_state_;
    BenchmarkWorker* work_;
    int work_count_;
    FunctionThread thread_;
    base::ChunkedArray<int> latencies_;
};


// TODO: Make this more generic?
class ThreadPool {
public:
    ThreadPool(BenchmarkState* test_state, const vector<BenchmarkWorker*>& workers) {
        for (int i = 0; i < workers.size(); ++i) {
            threads_.push_back(new ThreadBenchThread(test_state, workers[i]));
        }
    }

    ~ThreadPool() {
        STLDeleteElements(&threads_);
    }

    void waitForThreadExit() {
        for (int i = 0; i < threads_.size(); ++i) {
            threads_[i]->join();
        }
    }

    void copyLatencies(vector<int>* latencies) const {
        latencies->clear();
        for (int i = 0; i < threads_.size(); ++i) {
            threads_[i]->appendLatencies(latencies);
        }
    }

    int work_count() const {
        int sum = 0;
        for (size_t i = 0; i < threads_.size(); ++i) {
            sum += threads_[i]->work_count();
        }
        return sum;
    }

private:
    vector<ThreadBenchThread*> threads_;
};

}  // namespace

namespace benchmark {

int64_t runThreadBench(
        int warmup_secs,
        int measure_secs,
        const vector<BenchmarkWorker*>& workers,
        int* work_count) {
    // + 1 to include this "master" thread
    BenchmarkState test(static_cast<int>(workers.size() + 1), false, 0);

    ThreadPool pool(&test, workers);

    // Wait for all the threads to start
    test.blockForStart();

    // Running: wait for the warm up period
    sleep(warmup_secs);

    // Run the measurement period
    //~ printf("measure begin\n");
    struct timeval start;
    int error = gettimeofday(&start, NULL);
    assert(error == 0);

    test.startMeasure();
    sleep(measure_secs);
    test.doneMeasure();
    //~ printf("measure end\n");

    struct timeval end;
    error = gettimeofday(&end, NULL);
    assert(error == 0);

    // Wait for the workers to finish
    pool.waitForThreadExit();

    if (work_count != NULL) {
        *work_count = pool.work_count();
    }
    return base::timevalDiffMicroseconds(end, start);
}

double runThreadLatencyBench(
        int warmup_secs,
        int measure_secs,
        const vector<BenchmarkWorker*>& workers,
        int desired_requests_per_second,
        vector<int>* latencies) {
    CHECK(desired_requests_per_second > 0);
    int queue_limit = std::max((int) workers.size() * 2,
            (int)(desired_requests_per_second / 1000.0 * WORK_QUEUE_TIME_LIMIT_MS + 0.5));
    // + 1 includes this master thread
    BenchmarkState test((int) workers.size() + 1, true, queue_limit);
    ThreadPool thread_pool(&test, workers);

    // Wait for all the threads to start
    test.blockForStart();

    struct timeval start;
    gettimeofday(&start, NULL);
    struct timeval measure_start = start;
    measure_start.tv_sec += warmup_secs;
    struct timeval measure_end = { 0, 0 };

    // Use nanoseconds because microseconds are not precise enough (think 30000 reqs/s)
    const int interval_ns = 
            (int) ((1.0 / (double) desired_requests_per_second) * 1000000000. + 0.5);

    struct timeval now = start;

    struct timespec next_interval;
    next_interval.tv_sec = start.tv_sec;
    next_interval.tv_nsec = start.tv_usec * 1000;
    base::timespecAddNanoseconds(&next_interval, interval_ns);

    int next_to_add = 1;
    while (true) {
        bool success = test.addWork(next_to_add);
        if (!success) {
            fprintf(stderr, "ERROR: Maximum work queue size (%d) exceeded. This means we\n",
                    queue_limit);
            fprintf(stderr, "could not keep up with the desired rate (%d reqs/s = %d ns/req).\n",
                    desired_requests_per_second, interval_ns);
            abort();
        }

        struct timeval now;
        gettimeofday(&now, NULL);
        struct timeval next_interval_us;
        base::timespecToTimeval(&next_interval_us, next_interval);

        // Sleep until the next message time
        int64_t diff = base::timevalDiffMicroseconds(next_interval_us, now);
        while (diff > 0) {
            int error = usleep((useconds_t) diff);
            ASSERT(error == 0);
            gettimeofday(&now, NULL);
            diff = base::timevalDiffMicroseconds(next_interval_us, now);
        }
        assert(diff <= 0);

        // Compute how many messages to deliver
        next_to_add = - ((int) diff) * 1000 / interval_ns + 1;
        base::timespecAddNanoseconds(&next_interval, interval_ns * next_to_add);

        BenchmarkState::State state = test.state();
        if (state == BenchmarkState::WARMUP && base::timevalDiffMicroseconds(measure_start, now) <= 0) {
            test.startMeasure();
            measure_start = now;
            measure_end = now;
            measure_end.tv_sec += measure_secs;
        } else if (state == BenchmarkState::MEASURE && base::timevalDiffMicroseconds(measure_end, now) <= 0) {
            test.doneMeasure();
            measure_end = now;
        } else if (state == BenchmarkState::EXIT) {
            // This mean if the state is DONE, we continue adding load until all the threads notice
            break;
        }
    }

    thread_pool.waitForThreadExit();
    thread_pool.copyLatencies(latencies);
    return (double) latencies->size() /
            (double) base::timevalDiffMicroseconds(measure_end, measure_start) * 1000000.;
}

}  // namespace benchmark
