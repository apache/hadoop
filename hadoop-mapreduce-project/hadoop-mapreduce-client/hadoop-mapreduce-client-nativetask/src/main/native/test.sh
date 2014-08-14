#!/bin/sh

# only do normal tests by default
FILTER="--gtest_filter=-Perf.*"

# do all tests
if [ "$1" = "all" ]; then
  shift
  FILTER=""
fi

# do performance tests only
if [ "$1" = "perf" ]; then
  shift
  FILTER="--gtest_filter=Perf.*"
fi

if [ "${SYSTEM_MAC}" = "TRUE" ]; then
  # MACOSX already setup RPATH, no extra help required
  ./nttest $FILTER $@
else
  JAVA_JVM_LIBRARY_DIR=`dirname ${JAVA_JVM_LIBRARY}`
  LD_LIBRARY_PATH=$JAVA_JVM_LIBRARY_DIR:$LD_LIBRARY_PATH ./nttest $FILTER $@
fi

