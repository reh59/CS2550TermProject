# Copyright 2011 Massachusetts Institute of Technology.
# All rights reserved. Use of this source code is governed by a
# BSD-style license that can be found in the LICENSE file.

CFLAGS=-Wall -Wextra -Wno-sign-compare -Wno-unused-parameter -g -I.
CXXFLAGS=$(CFLAGS)
LDFLAGS=-lpthread -ldb -ldl

IMPLEMENTATION = refimplementation.o
PROGRAMS = example implementation_test simpledriver fsync_interpose.so

all: $(PROGRAMS)

example: $(IMPLEMENTATION)
implementation_test: $(IMPLEMENTATION) stupidunit.o function_thread.o utils.o
simpledriver: $(IMPLEMENTATION) threadbench.o function_thread.o randomgenerator.o


fsync_interpose.so: fsync_interpose.c
	$(CC) $(CFLAGS) -o fsync_interpose.so -shared -fPIC fsync_interpose.c

stupidunit.o: stupidunit/stupidunit.cc
	$(CXX) $(CFLAGS) -I. -c -o $@ $<

threadbench.o: benchmark/threadbench.cc
	$(CXX) $(CFLAGS) -I. -c -o $@ $<

utils.o: strings/utils.cc
	$(CXX) $(CFLAGS) -I. -c -o $@ $<

function_thread.o: base/function_thread.cc
	$(CXX) $(CFLAGS) -I. -c -o $@ $<


#generates html documentation files in html
doc:
	doxygen doxygen.conf

clean:
	$(RM) $(PROGRAMS) *.o
	$(RM) -r html
