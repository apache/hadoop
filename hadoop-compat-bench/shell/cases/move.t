#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"

echo "1..2"

# 1. moveFromLocal
expect_ret "moveFromLocal" 0 hadoop fs -moveFromLocal "${localDir}/dat" "${baseDir}/"

# 2. mv
hadoop fs -mv "${baseDir}/dat" "${baseDir}/dat2"
expect_ret "mv" 0 hadoop fs -test -f "${baseDir}/dat2"

# moveToLocal is not achieved on HDFS
# hadoop fs -moveToLocal "${baseDir}/dat2" "${localDir}/"
# expect_ret "moveToLocal" 0 test -f "${localDir}/dat2"
