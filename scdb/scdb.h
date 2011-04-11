/*
Copyright 2011 Massachusetts Institute of Technology.
All rights reserved. Use of this source code is governed by a
BSD-style license that can be found in the LICENSE file.
*/

/** @file
API for the SIGMOD 2011 programming contest.

This interface provides a durable, order-preserving, main memory index
(key/value map).
*/

#ifndef SCDB_H__
#define SCDB_H__

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct scdb_index;
/** Opaque type used as a handle to the index (key/value map). */
typedef struct scdb_index scdb_index;

struct scdb_iterator;
/** Opaque type used as a handle for an iteration over the index. */
typedef struct scdb_iterator scdb_iterator;

/** Status code return by SCDB function calls. */
typedef enum scdb_status {
    /** Operation completed successfully. */
    SCDB_OK,
    /** The specified key was not found so the operation was not perfomed. */
    SCDB_NOT_FOUND,
    /** The buffer provided for the key is too small. */
    SCDB_KEY_BUFFER_TOO_SMALL,
    /** The buffer provided for the value is too small. */
    SCDB_VALUE_BUFFER_TOO_SMALL,
    /** For compare and swap, the current value did not match. */
    SCDB_COMPARE_FAILED,
    /** Some other error occurred. */
    SCDB_ERROR,
} scdb_status;

/** Maximum length for keys that must be supported. */
#define SCDB_MAX_KEY_LENGTH 1024
/** Maximum length for values that must be supported. */
#define SCDB_MAX_VALUE_LENGTH 4096

/** Opens an existing index, or creates one if it does not exist. Any number of
files can be used to persist the index, provided that the total amount of space
used does not exceed \p max_disk_bytes. This index must be thread-safe, meaning
that it can be passed between multiple threads.

@param max_disk_bytes the maximum amount of disk space available for durability.
@param disk_path the prefix that must be used for storing the non-volatile data.
@return pointer to the index that will be passed into all future API calls. */
scdb_index* scdb_open(size_t max_disk_bytes, const char* disk_path);

/** Closes an open index. This should free all resources associated with
index. After this is called, the pointer cannot be used by any thread.

@param index the index to be closed.
*/
void scdb_close(scdb_index* index);

/** Reads the value corresponding to the specified key (exact match). If the
provided buffer is too small, this will return SCDB_VALUE_BUFFER_TOO_SMALL and
not copy any bytes into \p value_buffer. However, \p value_buffer_length is
always set to the full length of the value stored in the index. The value
returned must reflect the most recent successfully completed (committed) value,
even if that update was performed by another thread.

@param index the index to modify.
@param key pointer to the key bytes.
@param key_length length of the key in bytes. Must be <= SCDB_MAX_KEY_LENGTH.
@param value_buffer pointer to memory where the value will be copied, cannot be NULL.
@param[in,out] value_buffer_length pointer to the length of value buffer. This
    will be set to the length of the value in the index.
@return \ref SCDB_OK if the operation is successful,
    \ref SCDB_NOT_FOUND if the key does not exist,
    \ref SCDB_VALUE_BUFFER_TOO_SMALL if the value is longer than \p value_buffer,
    \ref SCDB_ERROR if some other error occurs.
*/
scdb_status scdb_read(scdb_index* index, const char* key, size_t key_length,
        char* value_buffer, size_t* value_buffer_length);


/** Sets the specified key to map to the specified value. This inserts a new
key-value pair or updates the existing key. After this returns, the update must
be committed and durable.

@param index the index to modify.
@param key pointer to the key bytes.
@param key_length length of the key in bytes. Must be <= SCDB_MAX_KEY_LENGTH.
@param value_buffer pointer to the value bytes, cannot be NULL.
@param value_length length of the value in bytes. Must be <= SCDB_MAX_VALUE_LENGTH.
@return \ref SCDB_OK if the operation is successful,
    \ref SCDB_ERROR if some other error occurs.
*/
scdb_status scdb_write(scdb_index* index, const char* key, size_t key_length,
        const char* value_buffer, size_t value_length);

/** Removes the specified key and associated value from the index. After this
returns, the delete must be committed and durable.

@param index the index to modify.
@param key pointer to the key bytes.
@param key_length length of the key in bytes. Must be <= SCDB_MAX_KEY_LENGTH.
@return \ref SCDB_OK if the operation is successful,
    \ref SCDB_NOT_FOUND if the key does not exist,
    \ref SCDB_ERROR if some other error occurs.
*/
scdb_status scdb_delete(scdb_index* index, const char* key, size_t key_length);

/** Performs an atomic compare and swap. If the current value for key matches
the provided value, then update the key's value to new_value. The compare and
swap occur atomically, each thread can assume that no other thread's updates are
applied between the compare and swap operation. This primitive can be used to
implement higher-level concurrency control, e.g. locks or validation for
optimistic concurrency control.

@param index the index to modify.
@param key pointer to the key bytes.
@param key_length length of the key in bytes. Must be <= SCDB_MAX_KEY_LENGTH.
@param current_value pointer to the bytes to be compared with the value
    stored in the index, cannot be NULL.
@param current_value_length length of current_value in bytes. Must be
    <= SCDB_MAX_VALUE_LENGTH.
@param new_value pointer to the value to be written if the comparison is
    successful, cannot be NULL.
@param new_value_length length of the new value in bytes, must be >= 0.
@return \ref SCDB_OK if the operation is successful,
    \ref SCDB_NOT_FOUND if the key does not exist,
    \ref SCDB_COMPARE_FAILED if the stored value does not match \p current_value,
    \ref SCDB_ERROR if some other error occurs.
*/
scdb_status scdb_cas(scdb_index* index, const char* key, size_t key_length,
        const char* current_value, size_t current_value_length,
        const char* new_value, size_t new_value_length);

/** Create an iterator over the index, starting at the smallest key that is
greater than or equal to the provided key. The keys are sorted in
lexicographical byte order, with bytes treated as 8-bit unsigned integers. For
example, the following order is correct: { 0x7f }, { 0x7f, 0x00 }, { 0x80 }.
To start at the beginning of the index, pass in a NULL or zero-length key.

@param index the index to iterate over.
@param key pointer to the key bytes.
@param key_length length of the key in bytes. Must be <= SCDB_MAX_KEY_LENGTH.
@return pointer to an iterator to be used to read values from the index, or
    NULL if some error occurred.
*/
scdb_iterator* scdb_iterate(scdb_index* index,
        const char* key, size_t key_length);

/** Closes an open iterator. The provided iterator pointer cannot be used after
calling this.

@param iterator pointer to the iterator to close.
*/
void scdb_iterator_close(scdb_iterator* iterator);

/** Reads the current key/value pair from the iterator, then advances the
iterator. The iterator does not need to be repeatable or isolated with respect
to other transactions. The only restriction is that keys must be returned in
strictly increasing order, and it must only return committed values.

If there is an error, the iterator is not advanced. If the buffers are too
small, the length fields will indicate the length of the data to be returned.

@param iterator the iterator to read from.
@param key_buffer pointer to memory where the key will be copied.
@param[in,out] key_buffer_length pointer to the length of value buffer. This
    will be set to the length of the key in the index.
@param value_buffer pointer to memory where the value will be copied.
@param[in,out] value_buffer_length pointer to the length of value buffer. This
    will be set to the length of the value in the index.

@return @return \ref SCDB_OK if the operation is successful,
    \ref SCDB_NOT_FOUND if there are no more keys in the iteration,
    \ref SCDB_KEY_BUFFER_TOO_SMALL if the key is longer than the provided buffer,
    \ref SCDB_VALUE_BUFFER_TOO_SMALL if the value is longer than the provided buffer,
    \ref SCDB_ERROR if some other error occurs. */
scdb_status scdb_next(scdb_iterator* iterator,
        char* key_buffer, size_t* key_buffer_length,
        char* value_buffer, size_t* value_buffer_length);


#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif
