/*
Copyright 2008,2009,2010 Massachusetts Institute of Technology.
All rights reserved. Use of this source code is governed by a
BSD-style license that can be found in the LICENSE file.
*/

/*
This library interposes on fsync() and fdatasync() to ensure that they get
called by software that is supposed to ensure data integrity (durability).

Compile with:
gcc -Wall -Wextra -shared -fPIC fsync_interpose.c -o fsync_interpose.so -ldl

Run with:
LD_PRELOAD=fsync_interpose.o program

TODO: This should probably buffer writes in order to make crash testing
easier. eg. if we buffer all write() data until fsync() is called, then if we
kill a process before fsync() the data will be "gone." Without this, the OS
will happily complete the write for us, which means we need real power plug
testing.

TODO: fdatasync() only really works for overwrites, not for appends. Verify
that the caller does this correctly.

TODO: Support sync_file_range, open(O_SYNC) or open(O_DSYNC).
*/

#define _GNU_SOURCE  /* needed to get RTLD_NEXT */
#include <assert.h>
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
 
static int (*real_fsync)(int) = NULL;
static int (*real_fdatasync)(int) = NULL;

static pthread_once_t resolve_real_functions_once = PTHREAD_ONCE_INIT;
void resolve_real_functions() {
    assert(real_fsync == NULL);
    assert(real_fdatasync == NULL);
    real_fsync = dlsym(RTLD_NEXT, "fsync");
    assert(real_fsync != NULL);
    real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    assert(real_fdatasync != NULL);
}

/* counts the number of times fsync/fdatasync are called. */
int scdb_fdatasync_count_ = 0;

int fsync(int fd) {
    pthread_once(&resolve_real_functions_once, resolve_real_functions);

    scdb_fdatasync_count_ += 1;
    /*printf("fsync(%d) = %d\n", fd, scdb_fdatasync_count_);*/
    return real_fsync(fd);
}

int fdatasync(int fd) {
    pthread_once(&resolve_real_functions_once, resolve_real_functions);

    scdb_fdatasync_count_ += 1;
    /*printf("fdatasync(%d) = %d\n", fd, scdb_fdatasync_count_);*/
    return real_fdatasync(fd);
}
