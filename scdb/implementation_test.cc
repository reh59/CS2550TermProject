// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstring>
#include <tr1/functional>

#include <dirent.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "base/function_thread.h"
#include "base/mutex.h"
#include "scdb.h"
#include "strings/utils.h"
#include "stupidunit/stupidunit.h"

using std::string;

// Prefix for the database files
static const char LOG_PREFIX[] = "index";

// A "test harness" that sets up an empty test index.
class SCDBTest : public Test {
public:
    // Test log maximum size = 30 MB
    static const int TEST_LOG_SIZE = 30 << 20;

    SCDBTest() : iterator_(NULL), status_(SCDB_OK), value_length_(sizeof(value_buffer_)) {
        string index_path = temp_dir_.name() + "/index";
        index_ = scdb_open(TEST_LOG_SIZE, index_path.c_str());
    }

    ~SCDBTest() {
        scdb_close(index_);
    }

    void readNextIterator(scdb_iterator* iterator) {
        key_length_ = sizeof(key_buffer_);
        value_length_ = sizeof(value_buffer_);
        status_ = scdb_next(iterator, key_buffer_, &key_length_, value_buffer_, &value_length_);
    }

    // Creates a temporary directory for log files
    stupidunit::ChTempDir temp_dir_;

    scdb_index* index_;
    scdb_iterator* iterator_;
    scdb_status status_;

    char key_buffer_[SCDB_MAX_KEY_LENGTH];
    size_t key_length_;
    char value_buffer_[SCDB_MAX_VALUE_LENGTH];
    size_t value_length_;
};

// Defines a new test called SCDBTest.IteratorEmpty. It inherits from
// SCDBTest, so it has access to public and protected members
//
// Iterates over an empty index, which should do nothing
TEST_F(SCDBTest, IteratorEmpty) {
    iterator_ = scdb_iterate(index_, NULL, 0);
    // aborts the test if this is false since de-referencing NULL will crash.
    ASSERT_TRUE(iterator_ != NULL);

    // This is an empty index: this must return not found
    readNextIterator(iterator_);
    // prints a message if this fails, but continues the test. Using EXPECT_EQ
    // instead of EXPECT_TRUE permits the printed message to be more useful.
    EXPECT_EQ(SCDB_NOT_FOUND, status_);
    // it might be nice, but we don't require these values to be set on error
    //EXPECT_EQ(0, key_length_);
    //EXPECT_EQ(0, value_length_);

    // Executing next again should still return NOT_FOUND
    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_NOT_FOUND, status_);
    //EXPECT_EQ(0, key_length_);
    //EXPECT_EQ(0, value_length_);

    scdb_iterator_close(iterator_);
}

// Read/write a single key
TEST_F(SCDBTest, ReadWriteDeleteSingle) {
    // does not exist
    status_ = scdb_read(index_, NULL, 0, value_buffer_, &value_length_);
    EXPECT_EQ(SCDB_NOT_FOUND, status_);

    // write the "empty string" key
    value_buffer_[0] = 0x42;
    status_ = scdb_write(index_, NULL, 0, value_buffer_, 1);
    EXPECT_EQ(SCDB_OK, status_);

    // read the empty string key
    value_buffer_[0] = 0x0;
    value_length_ = sizeof(value_buffer_);
    status_ = scdb_read(index_, NULL, 0, value_buffer_, &value_length_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(0x42, value_buffer_[0]);
    EXPECT_EQ(1, value_length_);

    // delete the key
    status_ = scdb_delete(index_, NULL, 0);
    EXPECT_EQ(SCDB_OK, status_);

    // key not found
    value_length_ = sizeof(value_buffer_);
    status_ = scdb_read(index_, NULL, 0, value_buffer_, &value_length_);
    EXPECT_EQ(SCDB_NOT_FOUND, status_);
}

static const char KEY1[] = "a";
static const char KEY2[] = "aaab";
static const char KEY3[] = "b";

TEST_F(SCDBTest, SimpleIterator) {
    static const char* const KEYS[] = { KEY1, KEY3, KEY2 };
    static const size_t KEY_LENGTHS[] = { sizeof(KEY1), sizeof(KEY3), sizeof(KEY2) };

    value_buffer_[0] = 0x1;
    value_buffer_[1] = 0x2;
    value_buffer_[2] = 0x3;
    value_buffer_[3] = 0x4;

    // write all keys
    for (int i = 0; i < (int)(sizeof(KEYS)/sizeof(*KEYS)); ++i) {
        status_ = scdb_write(index_, KEYS[i], KEY_LENGTHS[i], value_buffer_, 4);
        ASSERT_EQ(SCDB_OK, status_);
    }

    // iterate over all keys
    iterator_ = scdb_iterate(index_, NULL, 0);
    ASSERT_TRUE(iterator_ != NULL);

    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(sizeof(KEY1), key_length_);
    EXPECT_EQ(4, value_length_);
    EXPECT_EQ(0, strcmp(KEY1, key_buffer_));

    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(sizeof(KEY2), key_length_);
    EXPECT_EQ(4, value_length_);
    EXPECT_EQ(0, strcmp(KEY2, key_buffer_));

    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(sizeof(KEY3), key_length_);
    EXPECT_EQ(4, value_length_);
    EXPECT_EQ(0, strcmp(KEY3, key_buffer_));

    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_NOT_FOUND, status_);
    scdb_iterator_close(iterator_);

    // Skip key1
    iterator_ = scdb_iterate(index_, "aa", 2);
    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(sizeof(KEY2), key_length_);
    EXPECT_EQ(0, strcmp(KEY2, key_buffer_));
    scdb_iterator_close(iterator_);

    // Start at key2
    iterator_ = scdb_iterate(index_, KEY2, sizeof(KEY2));
    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(sizeof(KEY2), key_length_);
    EXPECT_EQ(0, strcmp(KEY2, key_buffer_));

    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(sizeof(KEY3), key_length_);
    EXPECT_EQ(0, strcmp(KEY3, key_buffer_));
    scdb_iterator_close(iterator_);

    // start past the end of the database
    iterator_ = scdb_iterate(index_, "c", 1);
    readNextIterator(iterator_);
    EXPECT_EQ(SCDB_NOT_FOUND, status_);
    scdb_iterator_close(iterator_);

    // we used to have some concurrent updates here, but BDB uses page level
    // locking which interfered with it. This is an acceptable implementation
    // technique, so we'll skip those tests
}

// CAS operations from one thread
TEST_F(SCDBTest, SimpleCAS) {
    key_buffer_[0] = 0x42;
    value_buffer_[0] = 0x43;

    // does not exist
    status_ = scdb_cas(index_, KEY1, sizeof(KEY1), key_buffer_, 1, value_buffer_, 1);
    EXPECT_EQ(SCDB_NOT_FOUND, status_);

    // write the initial value
    status_ = scdb_write(index_, KEY1, sizeof(KEY1), key_buffer_, 1);
    ASSERT_EQ(SCDB_OK, status_);

    // successful cas
    status_ = scdb_cas(index_, KEY1, sizeof(KEY1), key_buffer_, 1, value_buffer_, 1);
    EXPECT_EQ(SCDB_OK, status_);

    // read the value
    status_ = scdb_read(index_, KEY1, sizeof(KEY1), value_buffer_, &value_length_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(0x43, value_buffer_[0]);
    EXPECT_EQ(1, value_length_);

    // failed cas
    value_buffer_[0] = 0x99;
    status_ = scdb_cas(index_, KEY1, sizeof(KEY1), key_buffer_, 1, value_buffer_, 1);
    EXPECT_EQ(SCDB_COMPARE_FAILED, status_);

    // the value must be unchanged
    value_length_ = sizeof(value_buffer_);
    status_ = scdb_read(index_, KEY1, sizeof(KEY1), value_buffer_, &value_length_);
    EXPECT_EQ(SCDB_OK, status_);
    EXPECT_EQ(0x43, value_buffer_[0]);
    EXPECT_EQ(1, value_length_);
}

// Based on java.util.concurrent.CountDownLatch
class CountDownLatch {
public:
    CountDownLatch(int count) : count_(count), cond_(&mu_) {
        assert(count_ >= 0);
    }

    void countDown() {
        mu_.lock();
        count_ -= 1;
        assert(count_ >= 0);

        if (count_ == 0) {
            cond_.broadcast();
        }
        mu_.unlock();
    }

    void await() {
        mu_.lock();
        while (count_ > 0) {
            cond_.wait();
        }
        mu_.unlock();
    }

private:
    int count_;
    Mutex mu_;
    Condition cond_;
};

static const int NUM_INCREMENTS = 100;
static void casThread(CountDownLatch* start_latch, scdb_index* index) {
    start_latch->countDown();
    start_latch->await();

    for (int i = 0; i < NUM_INCREMENTS; ++i) {
        scdb_status status = SCDB_COMPARE_FAILED;
        while (status == SCDB_COMPARE_FAILED) {
            // read the current value
            int value;
            size_t value_length = sizeof(value);
            status = scdb_read(index, KEY1, sizeof(KEY1), (char*) &value, &value_length);
            assert(status == SCDB_OK);
            assert(value_length == sizeof(value));

            // CAS the new value
            int next_value = value + 1;
            status = scdb_cas(index, KEY1, sizeof(KEY1), (char*) &value, sizeof(value), (char*) &next_value, sizeof(next_value));
        }
        assert(status == SCDB_OK);
    }
}

// Increment a variable from multiple threads using CAS. Should produce the correct count.
TEST_F(SCDBTest, CASIncrement) {
    // write 0 into the cas key
    static const int value = 0;
    status_ = scdb_write(index_, KEY1, sizeof(KEY1), (char*) &value, sizeof(value));
    ASSERT_EQ(SCDB_OK, status_);

    // Create a latch to prevent threads from starting until all are ready
    static const int NUM_THREADS = 10;
    CountDownLatch latch(NUM_THREADS);

    // start all the threads
    FunctionThread* threads[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads[i] = new FunctionThread(std::tr1::bind(casThread, &latch, index_));
    }

    // wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads[i]->join();
        delete threads[i];
    }

    // Check that the cas key value is correct
    value_length_ = 4;
    int out;
    status_ = scdb_read(index_, KEY1, sizeof(KEY1), (char*) &out, &value_length_);
    EXPECT_EQ(4, value_length_);
    EXPECT_EQ(NUM_THREADS * NUM_INCREMENTS, out);
}

/** Returns the number of bytes of allocated space in directory. */
static off_t recursiveDiskSpaceUsage(const string& directory) {
    DIR* dir = opendir(directory.c_str());

    off_t used_file_space = 0;
    for (struct dirent* entry = readdir(dir); entry != NULL; entry = readdir(dir)) {
        // Skip special directories
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        struct stat64 statinfo;
        //~ printf("%s\n", entry->d_name);
        string path = directory + "/" + entry->d_name;
        int error = stat64(path.c_str(), &statinfo);
        assert(error == 0);
        if ((statinfo.st_mode & S_IFMT) == S_IFDIR) {
            used_file_space += recursiveDiskSpaceUsage(path);
        } else {
            assert((statinfo.st_mode & S_IFMT) == S_IFREG);
            //~ printf("%s = %ld bytes\n", entry->d_name, statinfo.st_size);
            used_file_space += statinfo.st_size;
        }
    }

    int error = closedir(dir);
    assert(error == 0);

    return used_file_space;
}

// Overwrite a single key repeatedly: there must be a maximum of TEST_LOG_SIZE disk space used
TEST_F(SCDBTest, OverwriteDiskSpace_VerySlowTest) {
    memset(key_buffer_, 0x01, sizeof(key_buffer_));
    memset(value_buffer_, 0x42, sizeof(value_buffer_));
    static const int NUM_WRITES_TO_OVERFLOW_LOG =
            (int) ((TEST_LOG_SIZE / (sizeof(key_buffer_) + sizeof(value_buffer_))) * 1.2);
    for (int i = 0; i < NUM_WRITES_TO_OVERFLOW_LOG; ++i) {
        status_ = scdb_write(
                index_, key_buffer_, sizeof(key_buffer_), value_buffer_, sizeof(value_buffer_));
        ASSERT_EQ(SCDB_OK, status_);
    }

    // search for all files and check the disk space used
    off_t used_file_space = recursiveDiskSpaceUsage(temp_dir_.name());

    // Occupied file space
    if (used_file_space > TEST_LOG_SIZE) {
        string message = strings::StringPrintf(
                "Database size exceeded limit of %d (using %d bytes)",
                TEST_LOG_SIZE, (int) used_file_space);
        fail(__FILE__, __LINE__, message.c_str());
    }
}

// Tests that a write is durable using fsync or fdatasync. Relies on 
// fsync_interpose.so being compiled in the current directory
TEST_F(SCDBTest, DurableWrites) {
    // attempt to locate the fdatasync count variable in the preload library
    int* scdb_fdatasync_count_ = (int*) dlsym(NULL, "scdb_fdatasync_count_");
    if (scdb_fdatasync_count_ == NULL) {
        fail(__FILE__, __LINE__,
                "Cannot find the fsync/fdatasync tester;\n"
                "Run with LD_PRELOAD=./fsync_interpose.so ./implementation_test");
        return;
    }

    *scdb_fdatasync_count_ = 0;
    static const char KEY[] = "key";
    static const char VALUE[] = "value";
    status_ = scdb_write(index_, KEY, sizeof(KEY), VALUE, sizeof(VALUE));

    // The write must have triggered at least one fsync/fdatasync
    // If you want to use an alternative mechanism, email the contest
    // administrators
    EXPECT_LT(0, *scdb_fdatasync_count_);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
