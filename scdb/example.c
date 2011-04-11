/*
Copyright 2011 Massachusetts Institute of Technology.
All rights reserved. Use of this source code is governed by a
BSD-style license that can be found in the LICENSE file.
*/

/** @file
Example client that uses the SCDB API. */

#include "scdb.h"

#include <assert.h>
#include <stdio.h>

/* limit example log to 1 GB */
#define MAX_LOG (1 << 30)

int main(int argc, const char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "example [path]\n");
        fprintf(stderr,
                "Opens an example database at path and shows the content.\n");
        return 1;
    }

    scdb_index* index = scdb_open(MAX_LOG, argv[1]);
    assert(index != NULL);

    /* set key -> value */
    static const char key[] = "key";
    static const char value[] = "value";
    static const char key1[] = "newKey";
    static const char value1[] = "newValue";
    scdb_status status = scdb_write(index, key, sizeof(key)-1, value, sizeof(value)-1);
    assert(status == SCDB_OK);
    printf("set index[\"%s\"] -> \"%s\"\n", key, value);

    status = scdb_write(index, key1, sizeof(key1)-1, value1, sizeof(value1)-1);
    assert(status == SCDB_OK);
    printf("set index[\"%s\"] -> \"%s\"\n", key1, value1);

    /* read key */
    char value_buffer[SCDB_MAX_VALUE_LENGTH];
    size_t value_length = sizeof(value_buffer);
    status = scdb_read(index, key, sizeof(key)-1, value_buffer, &value_length);
    assert(status == SCDB_OK);
    assert(value_length == sizeof(value)-1);
    value_buffer[value_length] = '\0';
    printf("read index[\"%s\"] = \"%s\"\n", key, value_buffer);

    status = scdb_delete(index, key, sizeof(key)-1);
    assert(status == SCDB_OK);
    printf("deleted index[\"%s\"] ok\n", key);
    status = scdb_delete(index, key, sizeof(key)-1);
    assert(status == SCDB_NOT_FOUND);
    printf("deleted index[\"%s\"] not found\n", key);

    value_length = sizeof(value_buffer);
    status = scdb_read(index, key, sizeof(key)-1, value_buffer, &value_length);
    assert(status == SCDB_NOT_FOUND);
    printf("read index[\"%s\"] not found\n", key);

    scdb_iterator* iter = scdb_iterate(index,key,sizeof(key)-1);
    assert(iter!=0);
    printf("iterator retrieved\n");

    char key_buffer[SCDB_MAX_VALUE_LENGTH];
    size_t key_length = sizeof(key_buffer);
    value_length = sizeof(value_buffer);
    status = scdb_next(iter, key_buffer, &key_length, value_buffer, &value_length);
    assert(status==SCDB_OK);

    printf("read index[\"%.*s\"] = \"%.*s\"\n", (int) key_length, key_buffer, (int) value_length, value_buffer);
    status = scdb_next(iter, key_buffer, &key_length, value_buffer, &value_length);
    assert(status==SCDB_NOT_FOUND);
    scdb_iterator_close(iter);
    printf("iterator closed\n");

    scdb_close(index);
    return 0;
}
