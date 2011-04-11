// Copyright 2011 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cassert>
#include <cstdio>
#include <tr1/functional>
#include <vector>

#include <sys/time.h>

#include "scdb.h"

#include "base/function_thread.h"
#include "base/stlutil.h"
#include "base/time.h"
#include "benchmark/benchmark.h"
#include "randomgenerator.h"

using std::vector;

// CONSTANTS: control the test behaviour

// 50 MB data size
static const size_t DATA_SIZE = 50 * (1 << 20);
// log size = 3 x data size
static const size_t LOG_SIZE_FACTOR = 3;
static const size_t LOG_SIZE = DATA_SIZE * LOG_SIZE_FACTOR;
// how many keys are needed for DATA_SIZE elements?
// each key = key (int64_t), value (int64_t), lengths 2*int32_t =
static const size_t DESIRED_NUM_ROWS = DATA_SIZE / (2*sizeof(int64_t) + 2*sizeof(int32_t));

// Number of threads used to initialize the index
static const int NUM_INITIALIZE_THREADS = 64;
// Number of rows that is an even multiple of threads
static const size_t NUM_ROWS =
        (DESIRED_NUM_ROWS / NUM_INITIALIZE_THREADS) * NUM_INITIALIZE_THREADS;

// Warm up the test for this long (run, don't measure)
static const int WARM_UP_S = 10;
// Run test for this long
static const int TEST_S = 120;

// probability for each operation
static const int READ_P = 45;
static const int WRITE_P = 40;
static const int DELETE_P = 5;
static const int CAS_P = 5;
static const int SCAN_P = 5;

// maximum number of rows to scan
static const int MAX_SCAN_ROWS = 10;

// Swaps the bytes in v.
uint64_t bswap64(uint64_t v) {
// GCC 4.3 introduced __builtin_bswap64
#if defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 3))
    return __builtin_bswap64(v);
#else
    return (((v & 0xff00000000000000ULL) >> 56)
         | ((v & 0x00ff000000000000ULL) >> 40)
         | ((v & 0x0000ff0000000000ULL) >> 24)
         | ((v & 0x000000ff00000000ULL) >>  8)
         | ((v & 0x00000000ff000000ULL) <<  8)
         | ((v & 0x0000000000ff0000ULL) << 24)
         | ((v & 0x000000000000ff00ULL) << 40)
         | ((v & 0x00000000000000ffULL) << 56));
#endif
}

// If we are on a little-endian host, this will byte swap v. If we are on a big endian host, it
// does nothing. Used to convert native integers to/from big endian integers.
uint64_t convertBigEndian64(uint64_t v) {
#if defined(__i386__) || defined(__x86_64__)
    return bswap64(v);
#elif defined(__ppc__) || defined(__sparc__)
    return v;
#else
#error Unknown CPU type: is this big endian or little endian?
#endif
}


class SCDBDriver : public benchmark::BenchmarkWorker {
public:
    SCDBDriver(scdb_index* index) : index_(index) {}

    virtual ~SCDBDriver() {}

    /** Called once in the worker thread/process before the test begins. */
    virtual void initialize() {
        rng_.seedDefault();
    }

    /** Called repeatedly in the worker's measurement loop to do work.
    @param measure true if this call begins in the measurement period. */
    virtual void work(bool measure) {
        int rnd = rng_.random() % 100;
        int64_t key = selectKey();

        int64_t value;
        size_t value_length = sizeof(value);
        if (rnd < READ_P) {
            // read key
            //~printf("read row (%p) (%d)\n", (void*) key, (signed) convertBigEndian64(key));
            scdb_status status = scdb_read(index_, (char*) &key, sizeof(key), (char*) &value, &value_length);
            assert(status == SCDB_OK || status == SCDB_NOT_FOUND);
            assert(status == SCDB_NOT_FOUND || value_length == sizeof(value));
        } else if (rnd < READ_P + SCAN_P) {
            // scan: start at key, scan for rows_to_scan rows or end
            int rows_to_scan = (rng_.random() % MAX_SCAN_ROWS) + 1;
            int64_t last = convertBigEndian64(key) - 1;

            scdb_iterator* iterator = scdb_iterate(index_, (char*) &key, sizeof(key));

            size_t key_length = sizeof(key);
            size_t value_length = sizeof(value);
            scdb_status status;
            int count = 0;
            while ((status = scdb_next(iterator, (char*) &key, &key_length, (char*) &value, &value_length)) == SCDB_OK) {
                int64_t next = convertBigEndian64(key);
                assert(next > last);
                assert(value_length == sizeof(value));
                last = next;

                key_length = sizeof(key);
                value_length = sizeof(value);

                count += 1;
                if (count == rows_to_scan) break;
            }
            assert(status == SCDB_OK || status == SCDB_NOT_FOUND);
            scdb_iterator_close(iterator);
            iterator = NULL;
        } else if (rnd < READ_P + WRITE_P) {
            // 40% chance of writes: set key = 0
            int64_t value = 0;
            scdb_status status = scdb_write(index_, (char*) &key, sizeof(key), (char*) &value, sizeof(value));
            assert(status == SCDB_OK);
        } else {
            // 10% chance of delete: delete key
            scdb_status status = scdb_delete(index_, (char*) &key, sizeof(key));
            assert(status == SCDB_OK || status == SCDB_NOT_FOUND);
        }
    }

    int64_t selectKey() {
        return convertBigEndian64(rng_.random() % NUM_ROWS);
    }

private:
    scdb_index* index_;
    RandomGenerator rng_;
};

static void joinAndPrintElapsed(vector<FunctionThread*>* threads, const struct timeval& start) {
    for (int i = 0; i < threads->size(); ++i) {
        (*threads)[i]->join();
        delete (*threads)[i];
    }
    threads->clear();

    struct timeval end;
    gettimeofday(&end, NULL);
    int diff_s = (int) (base::timevalDiffMicroseconds(end, start) + 500000) / 1000000;
    printf("elapsed: %d:%02d\n", diff_s / 60, diff_s % 60);
}

static void createRowThread(scdb_index* index, int start_row, int end_row) {
    int64_t value = 0;
    for (int64_t key = start_row; key < end_row; ++key) {
        int64_t keyBigEndian = convertBigEndian64(key);
        scdb_status status =
                scdb_write(index, (char*) &keyBigEndian, sizeof(keyBigEndian), (char*) &value, sizeof(value));
        assert(status == SCDB_OK);
    }
}

static void randomWriteThread(scdb_index* index, int num_writes) {
    RandomGenerator rng;

    int64_t value;
    for (int i = 0; i < num_writes; ++i) {
        int64_t key = convertBigEndian64(rng.random() % NUM_ROWS);
        value = rng.random() % 1000000;
        scdb_status status =
                scdb_write(index, (char*) &key, sizeof(key), (char*) &value, sizeof(value));
        assert(status == SCDB_OK);
    }
}

int main(int argc, const char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "simpledriver [log file] [test threads]\n");
        return 1;
    }
    const char* const log_file = argv[1];
    int test_threads = atoi(argv[2]);
    if (test_threads <= 0) {
        fprintf(stderr, "Invalid number of threads: %s\n", argv[2]);
    }

    // Open the index
    scdb_index* index = scdb_open(LOG_SIZE, log_file);

    // Delete any existing data
    printf("deleting existing data ...\n");
    scdb_iterator* iterator = scdb_iterate(index, NULL, 0);
    char key_buffer[SCDB_MAX_KEY_LENGTH];
    char value_buffer[SCDB_MAX_VALUE_LENGTH];
    size_t key_length = sizeof(key_buffer);
    size_t value_length = sizeof(value_buffer);
    scdb_status status;
    while ((status = scdb_next(iterator, key_buffer, &key_length, value_buffer, &value_length)) == SCDB_OK) {
        // close the iterator: locking implementations may hold a lock on this key
        scdb_iterator_close(iterator);
        status = scdb_delete(index, key_buffer, key_length);
        assert(status == SCDB_OK);

        // re-open the iterator
        iterator = scdb_iterate(index, NULL, 0);
        key_length = sizeof(key_buffer);
        value_length = sizeof(value_buffer);
    }
    assert(status == SCDB_NOT_FOUND);
    scdb_iterator_close(iterator);
    iterator = NULL;

    // initialize NUM_ROWS integers to zero
    static const int ROWS_PER_THREAD = NUM_ROWS / NUM_INITIALIZE_THREADS;
    printf("creating %d rows (~%zd MB data size)... ",
            ROWS_PER_THREAD * NUM_INITIALIZE_THREADS, DATA_SIZE / (1<<20));
    fflush(stdout);
    struct timeval start;
    gettimeofday(&start, NULL);
    vector<FunctionThread*> threads;
    for (int i = 0; i < NUM_INITIALIZE_THREADS; ++i) {
        threads.push_back(new FunctionThread(std::tr1::bind(
                createRowThread, index, i * ROWS_PER_THREAD, (i+1) * ROWS_PER_THREAD)));
    }
    joinAndPrintElapsed(&threads, start);

    // Fill the log with enough data that we measure the performance while garbage collecting
    static const int NUM_WRITES = (int)(NUM_ROWS * (LOG_SIZE_FACTOR-1));
    printf("filling the log with %d writes... ", NUM_WRITES);
    fflush(stdout);
    gettimeofday(&start, NULL);
    for (int i = 0; i < NUM_INITIALIZE_THREADS; ++i) {
        threads.push_back(new FunctionThread(
                std::tr1::bind(randomWriteThread, index, NUM_WRITES/NUM_INITIALIZE_THREADS)));
    }
    joinAndPrintElapsed(&threads, start);

    // Create the testers
    vector<SCDBDriver*> testers;
    for (int i = 0; i < test_threads; ++i) {
        testers.push_back(new SCDBDriver(index));
    }

    // Run the test
    int requests = 0;
    printf("warming up for %d seconds; running for %d seconds ...\n",
            WARM_UP_S, TEST_S);
    int64_t us = benchmark::runThreadBench(
            WARM_UP_S, TEST_S, testers, &requests);
    double requests_per_second = requests / (double) us * 1e6;
    printf("%f requests/second\n", requests_per_second);

    STLDeleteElements(&testers);
    scdb_close(index);
}
