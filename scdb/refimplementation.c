/*
Copyright 2011 Massachusetts Institute of Technology.
All rights reserved. Use of this source code is governed by a
BSD-style license that can be found in the LICENSE file.
*/

/** @file
A skeleton implementation of the SIGMOD 2011 programming contest API.
*/

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <db.h>

#include "scdb.h"

struct scdb_index
{
  DB* dbp;
  DB_ENV* env;

  /* we checkpoint when we have written approximately this much data. */
  int log_high_water_kbyte;

  /* block all other threads while one is attempting to checkpoint. gross but works. */
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int is_checkpointing;
};

struct scdb_iterator
{
  DBC* cursor;
  enum {
      ITERATOR_FIRST,
      ITERATOR_END,
      ITERATOR_OK,
  } state;
};

DBT* initialize_dbt(){
    DBT* dbt = (DBT*)malloc(sizeof(DBT));
    memset(&dbt, 0, sizeof(dbt));
    return dbt;
}

static const char INDEX_NAME[] = "index";

scdb_index* scdb_open(size_t max_disk_bytes, const char* disk_path) {
    scdb_index* index = (scdb_index*)malloc(sizeof(scdb_index));

    int ret = pthread_mutex_init(&index->mutex, NULL);
    assert(ret == 0);
    ret = pthread_cond_init(&index->condition, NULL);
    assert(ret == 0);
    index->is_checkpointing = 0;

    // create a directory to store environment info into
    ret = mkdir(disk_path, S_IRWXU);
    if (ret != 0 && errno != EEXIST) {
      perror("Error while creating the environment directory");
      free(index);
      return NULL;
    }

    // Create the Environment
    if ((ret = db_env_create(&(index->env), 0)) !=0) {
      printf("Error while creating the Environment: %s\n", db_strerror(ret));
      free(index);
      return NULL;
    }

    /* autocommit must be enabled to make operations durable. */
    ret = index->env->set_flags(index->env, DB_AUTO_COMMIT, 1);
    if (ret != 0) {
      fprintf(stderr, "Warning: Failed to enable DB_AUTO_COMMIT; "
          "operations will not be durable: %s\n", db_strerror(ret));
    }

    /* get BDB to garbage collect its own logs. */
    ret = index->env->log_set_config(index->env, DB_LOG_AUTO_REMOVE, 1);
    if (ret != 0) {
      fprintf(stderr, "Warning: Failed to enable DB_LOG_AUTO_REMOVE; "
          "logs may grow without bound: %s\n", db_strerror(ret));
    }

    /* make the cache = data set size * 2 */
    const int data_size = (int) (max_disk_bytes / 3.0);
    const int cache_size = data_size * 2;
    const int gigabytes = cache_size / (1 << 30);
    const int bytes = cache_size % (1 << 30);
    ret = index->env->set_cachesize(index->env, gigabytes, bytes, 1);
    if (ret != 0) {
      fprintf(stderr, "Warning: Failed to set cache size to %d bytes (%d GB + %d bytes): %s\n",
          cache_size, gigabytes, bytes, db_strerror(ret));
    }

    /* make the log buffer 1 MB */
    ret = index->env->set_lg_bsize(index->env, 1 << 20);
    if (ret != 0) {
      fprintf(stderr, "Warning: Failed to make the log buffer size 1MB: %s\n", db_strerror(ret));
    }

    /* enable automatic deadlock detection: deadlocks can even occur on pure "put" workloads due
    to page splits. */
    ret = index->env->set_lk_detect(index->env, DB_LOCK_RANDOM);
    if (ret != 0) {
      fprintf(stderr, "Warning: Failed to enable deadloc detection: %s\n", db_strerror(ret));
    }

    // Open the Environment
    ret = index->env->open(index->env, disk_path,
                           DB_CREATE | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG |
                           DB_RECOVER | DB_THREAD | DB_INIT_MPOOL | DB_PRIVATE,
                           S_IRUSR | S_IWUSR);
    if (ret != 0) {
      printf("Error while opening the Environment: %s\n", db_strerror(ret));
      index->env->close(index->env, 0);
      free(index);
      return NULL;
    }

    /* max_disk_bytes is 3X the data size, so we want 1X data, 1X log and 1X "extra", so set this
    to 90% of 1/3: give us time to checkpoint!
    NOTE: in the worst case, the checkpoint is at the end of one log. we want to keep the worst
    case number of logs below the disk space, so we remove one.
    NOTE: Calling get_lg_max returns 0 until after opening the environment. */
    const int max_log_bytes = (int) (data_size * 0.9);
    uint32_t log_max_bytes;
    ret = index->env->get_lg_max(index->env, &log_max_bytes);
    assert(ret == 0);
    int max_logs = (max_log_bytes / log_max_bytes) - 1;
    if (max_logs <= 0) {
        max_logs = 1;
    }
    index->log_high_water_kbyte = (max_logs * log_max_bytes) >> 10;

    if ((ret = db_create(&(index->dbp), index->env, 0)) != 0){
      fprintf(stderr, "Error while creating the Database! %s\n",db_strerror(ret));
      index->env->close(index->env, 0);
      free(index);
      return NULL;
    }

    if ((ret = index->dbp->open(index->dbp, NULL, INDEX_NAME, NULL, DB_BTREE, DB_CREATE | DB_THREAD, 0664)) !=0){
      fprintf(stderr, "Error while opening the Database! %s\n",db_strerror(ret));
      index->dbp->close(index->dbp, 0);
      index->env->close(index->env, 0);
      free(index);
      return NULL;
    }

    return index;
}

void scdb_close(scdb_index* index) {
//    printf("scdb_close\n");
    int ret;
    if ((ret = index->dbp->close(index->dbp,0)) !=0){
      fprintf(stderr, "Error while closing the Database! %s\n",db_strerror(ret));
    }
    if ((ret = index->env->close(index->env, 0)) !=0){
      fprintf(stderr, "Error closing the environment: %s\n",db_strerror(ret));
    }

    ret = pthread_cond_destroy(&index->condition);
    assert(ret == 0);
    ret = pthread_mutex_destroy(&index->mutex);
    assert(ret == 0);

    free(index);
}

scdb_status scdb_read(scdb_index* index, const char* key, size_t key_length,
        char* value_buffer, size_t* value_buffer_length) {
//    printf("scdb_read\n");
    if (key_length > SCDB_MAX_KEY_LENGTH){
      return SCDB_ERROR;
    }
    DBT dbtKey, dbtValue;
    int ret;
    memset(&dbtKey, 0, sizeof(dbtKey));
    memset(&dbtValue, 0, sizeof(dbtValue));
    dbtKey.data = (void*) key;
    dbtKey.size = key_length;
    dbtValue.data = (void*) value_buffer;
    dbtValue.ulen = *value_buffer_length;
    dbtValue.flags = DB_DBT_USERMEM;

    do {
        ret = index->dbp->get(index->dbp, NULL, &dbtKey, &dbtValue, 0);
    } while (ret == DB_LOCK_DEADLOCK);

    if (ret == DB_NOTFOUND){
        *value_buffer_length = 0;
        return SCDB_NOT_FOUND;
    } else if (ret != 0) {
        fprintf(stderr, "scdb_read failed because: %s\n",db_strerror(ret));
        return SCDB_ERROR;
    }
    assert(ret == 0);

    *value_buffer_length = dbtValue.size;
    return SCDB_OK;
}

static void tryCheckpoint(scdb_index* index) {
    /* Permit one thread to try to checkpoint at a time. All other threads wait for the
    checkpointer to finish.
    TODO: This is probably horribly slow, but without this, we ran into a problem when many
    threads are inserting in parallel, BDB keeps too many logs around and runs out of space.
    This solves the problem! */
    int ret = pthread_mutex_lock(&index->mutex);
    assert(ret == 0);
    if (!index->is_checkpointing) {
        // record that we are attempting to checkpoint and unlock
        index->is_checkpointing = 1;
        ret = pthread_mutex_unlock(&index->mutex);
        assert(ret == 0);

        ret = index->env->txn_checkpoint(index->env, index->log_high_water_kbyte, 0, 0);
        if (ret != 0) {
            fprintf(stderr, "warning: checkpoint failed: %s\n", db_strerror(ret));
        }

        // re-acquire the mutex; tell any waiters we are done
        int ret = pthread_mutex_lock(&index->mutex);
        assert(ret == 0);
        index->is_checkpointing = 0;
        ret = pthread_cond_broadcast(&index->condition);
        assert(ret == 0);
    } else {
        // wait for the checkpointer to finish before continuing
        while (index->is_checkpointing) {
            ret = pthread_cond_wait(&index->condition, &index->mutex);
            assert(ret == 0);
        }
    }

    ret = pthread_mutex_unlock(&index->mutex);
    assert(ret == 0);
}

scdb_status scdb_write(scdb_index* index, const char* key, size_t key_length,
        const char* value, size_t value_length) {
    assert(key_length <= SCDB_MAX_KEY_LENGTH);
    assert(value_length <= SCDB_MAX_VALUE_LENGTH);

  //printf("scdb_write\n");
    DBT dbtKey, dbtValue;
    int ret;

    dbtKey.data = (void*) key;
    dbtKey.size = key_length;
    dbtKey.flags = 0;
    dbtValue.data = (void*) value;
    dbtValue.size = value_length;
    dbtValue.flags = 0;

    do {
        ret = index->dbp->put(index->dbp, NULL, &dbtKey, &dbtValue, 0);
    } while (ret == DB_LOCK_DEADLOCK);

    if (ret != 0) {
      fprintf(stderr, "scdb_write failed: %s\n",db_strerror(ret));
      return SCDB_ERROR;
    }

    // check if we need to checkpoint
    tryCheckpoint(index);
    return SCDB_OK;
}

scdb_status scdb_delete(scdb_index* index, const char* key, size_t key_length) {
  //printf("scdb_delete\n");
    DBT dbtKey;
    int ret;
    memset(&dbtKey, 0, sizeof(dbtKey));
    dbtKey.data = (void*) key;
    dbtKey.size = key_length;
    
    do {
        ret = index->dbp->del(index->dbp, NULL, &dbtKey, 0);
    } while (ret == DB_LOCK_DEADLOCK);

    if (ret == DB_NOTFOUND) {
        return SCDB_NOT_FOUND;
    } else if (ret != 0) {
        printf("deletion failed because: %s\n",db_strerror(ret));
        return SCDB_ERROR;
    }

    tryCheckpoint(index);
    return SCDB_OK;
}

scdb_status scdb_cas(scdb_index* index, const char* key, size_t key_length,
        const char* current_value, size_t current_value_length,
        const char* new_value, size_t new_value_length) {
    int ret;
    DB_TXN* tid = NULL;

    // read the original value
    DBT dbtKey;
    dbtKey.data = (void*) key;
    dbtKey.size = key_length;
    dbtKey.flags = 0;
    char value_buffer[SCDB_MAX_VALUE_LENGTH];
    DBT dbtValue;
    dbtValue.data = value_buffer;
    dbtValue.ulen = sizeof(value_buffer);
    dbtValue.flags = DB_DBT_USERMEM;

    while (1) {
        // begin transaction
        ret = index->env->txn_begin(index->env, NULL, &tid, 0);
        if (ret != 0) {
            fprintf(stderr, "scdb_cas transaction begin failed: %s\n", db_strerror(ret));
            return SCDB_ERROR;
        }
        assert(tid != NULL);

        ret = index->dbp->get(index->dbp, tid, &dbtKey, &dbtValue, DB_RMW);
        if (ret == DB_LOCK_DEADLOCK) {
            /* abort and retry in case of deadlock; after this we have all locks */
            ret = tid->abort(tid);
            if (ret != 0) {
                fprintf(stderr, "scdb_cas abort failed: %s\n", db_strerror(ret));
                return SCDB_ERROR;
            }
            continue;
        } else if (ret == DB_NOTFOUND) {
            ret = tid->abort(tid);
            if (ret != 0) {
                fprintf(stderr, "scdb_cas abort failed: %s\n", db_strerror(ret));
                return SCDB_ERROR;
            }
            return SCDB_NOT_FOUND;
        } else if (ret != 0) {
            // abort transaction and return error
            fprintf(stderr, "scdb_cas read failed: %s\n", db_strerror(ret));
            ret = tid->abort(tid);
            if (ret != 0) {
                fprintf(stderr, "error aborting: %s\n", db_strerror(ret));
            }
            return SCDB_ERROR;
        }
        break;
    }

    // compare the values
    if (dbtValue.size != current_value_length ||
            memcmp(dbtValue.data, current_value, current_value_length) != 0) {
        // no match: abort
        ret = tid->abort(tid);
        if (ret != 0) {
            fprintf(stderr, "error aborting: %s\n", db_strerror(ret));
        }
        return SCDB_COMPARE_FAILED;
    }

    // the values match: write the new value and commit
    dbtValue.data = (void*) new_value;
    dbtValue.size = new_value_length;
    dbtValue.flags = 0;
    ret = index->dbp->put(index->dbp, tid, &dbtKey, &dbtValue, 0);
    if (ret != 0) {
        fprintf(stderr, "scdb_cas write failed: %s\n", db_strerror(ret));
        ret = tid->abort(tid);
        if (ret != 0) {
            fprintf(stderr, "error aborting: %s\n", db_strerror(ret));
        }
        return SCDB_ERROR;
    }

    ret = tid->commit(tid, 0);
    if (ret != 0) {
        fprintf(stderr, "scdb_cas commit failed: %s\n", db_strerror(ret));
        return SCDB_ERROR;
    }

    tryCheckpoint(index);
    return SCDB_OK;
}

scdb_iterator* scdb_iterate(scdb_index* index,
        const char* key, size_t key_length) {
  //printf("scdb_iterate\n");
    int ret;
    scdb_iterator* iter = (scdb_iterator*)malloc(sizeof(scdb_iterator));
    iter->state = ITERATOR_FIRST;
    DBT dbt_key, dbt_value;

    // use zero length buffers and do zero byte reads
    dbt_key.data = (void*)key;
    dbt_key.size = key_length;
    dbt_key.ulen = 0;
    dbt_key.dlen = 0;
    dbt_key.doff = 0;
    dbt_key.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    dbt_value.data = NULL;
    dbt_value.ulen = 0;
    dbt_value.dlen = 0;
    dbt_value.doff = 0;
    dbt_value.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    do {
        ret = index->dbp->cursor(index->dbp, NULL, &(iter->cursor), DB_READ_COMMITTED);
    } while (ret == DB_LOCK_DEADLOCK);
    if (ret !=0) {
        fprintf(stderr, "failed opening iterator because: %s\n",db_strerror(ret));
        return NULL;
    }

    ret = iter->cursor->c_get(iter->cursor, &dbt_key, &dbt_value, DB_SET_RANGE | DB_READ_COMMITTED);
    if(ret == 0) {
        return iter;
    } else if (ret==DB_NOTFOUND) {
        iter->state = ITERATOR_END;
        return iter;
    }

    assert(ret != 0);
    fprintf(stderr, "scdb_iterate failed: %s\n", db_strerror(ret));
    iter->cursor->close(iter->cursor);
    free(iter);
    return NULL;
}

void scdb_iterator_close(scdb_iterator* iterator) {
  //printf("scdb_iterator_close\n");
    int ret = iterator->cursor->close(iterator->cursor);
    if (ret != 0) {
        fprintf(stderr, "iterator close failed: %s\n", db_strerror(ret));
    }
    free(iterator);
}

scdb_status scdb_next(scdb_iterator* iterator,
        char* key_buffer, size_t* key_buffer_length,
        char* value_buffer, size_t* value_buffer_length) {
  //printf("scdb_next\n");
    DBT dbtKey;
    DBT dbtValue;
    int ret;

    if (iterator->state == ITERATOR_END) return SCDB_NOT_FOUND;

    dbtKey.data = (void*) key_buffer;
    dbtKey.ulen = *key_buffer_length;
    dbtKey.flags = DB_DBT_USERMEM;

    dbtValue.data = (void*) value_buffer;
    dbtValue.ulen = *value_buffer_length;
    dbtValue.flags = DB_DBT_USERMEM;

    /* handle the first request differently than the subsequent ones: BDB's "seek" returns data. */
    int flags = DB_NEXT | DB_READ_COMMITTED;
    if (iterator->state == ITERATOR_FIRST) {
        flags = DB_CURRENT | DB_READ_COMMITTED;
        iterator->state = ITERATOR_OK;
    } else {
        assert(iterator->state == ITERATOR_OK);
    }

    ret = iterator->cursor->get(iterator->cursor, &dbtKey, &dbtValue, flags);
    if (ret == 0) {
      *key_buffer_length = dbtKey.size;
      *value_buffer_length = dbtValue.size;
      return SCDB_OK;
    } else if(ret == DB_NOTFOUND){
      return SCDB_NOT_FOUND;
    } else if (ret == DB_BUFFER_SMALL) {
      if(*key_buffer_length<dbtKey.size) return SCDB_KEY_BUFFER_TOO_SMALL;
      assert(*value_buffer_length<dbtValue.size);
      return SCDB_VALUE_BUFFER_TOO_SMALL;
    }

    return SCDB_ERROR;
}
