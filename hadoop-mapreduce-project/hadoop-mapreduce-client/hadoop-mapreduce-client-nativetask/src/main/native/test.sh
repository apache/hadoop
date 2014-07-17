#!/bin/bash

# do all tests
if [ "$1" == "all" ]; then
shift
./nttest $@
exit $?
fi

# do performance tests only
if [ "$1" == "perf" ]; then
shift
./nttest --gtest_filter=Perf.* $@
exit $?
fi

# do not do performance test by default
./nttest --gtest_filter=-Perf.* $@
