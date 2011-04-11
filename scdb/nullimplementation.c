/*
Copyright 2011 Massachusetts Institute of Technology.
All rights reserved. Use of this source code is governed by a
BSD-style license that can be found in the LICENSE file.
*/

/** @file
A skeleton implementation of the SIGMOD 2011 programming contest API.
*/

#include "scdb.h"

scdb_index* scdb_open(size_t max_disk_bytes, const char* disk_path) {
    return NULL;
}

void scdb_close(scdb_index* index) {
}

scdb_status scdb_read(scdb_index* index, const char* key, size_t key_length,
        char* value_buffer, size_t* value_buffer_length) {
    return SCDB_ERROR;
}

scdb_status scdb_write(scdb_index* index, const char* key, size_t key_length,
        const char* value, size_t value_length) {
    return SCDB_ERROR;
}

scdb_status scdb_delete(scdb_index* index, const char* key, size_t key_length) {
    return SCDB_ERROR;
}

scdb_status scdb_cas(scdb_index* index, const char* key, size_t key_length,
        const char* current_value, size_t current_value_length,
        const char* new_value, size_t new_value_length) {
    return SCDB_ERROR;
}

scdb_iterator* scdb_iterate(scdb_index* index,
        const char* key, size_t key_length) {
    return NULL;
}

void scdb_iterator_close(scdb_iterator* iterator) {
}

scdb_status scdb_next(scdb_iterator* iterator,
        char* key_buffer, size_t* key_buffer_length,
        char* value_buffer, size_t* value_buffer_length) {
    return SCDB_ERROR;
}

