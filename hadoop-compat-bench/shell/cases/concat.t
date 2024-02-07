#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"
hadoop fs -put "${localDir}/dat" "${baseDir}/src1"
hadoop fs -put "${localDir}/dat" "${baseDir}/src2"

echo "1..3"

# 1. touchz
hadoop fs -touchz "${baseDir}/dat"
expect_out "touchz" "size:0" hadoop fs -stat "size:%b" "${baseDir}/dat"

# 2. concat
expect_ret "concat" 0 hadoop fs -concat "${baseDir}/dat" "${baseDir}/src1" "${baseDir}/src2"
# expect_out "size:26" hadoop fs -stat "size:%b" "${baseDir}/dat"

# 3. getmerge
hadoop fs -getmerge "${baseDir}" "${localDir}/merged"
expect_ret "getmerge" 0 test -s "${localDir}/merged"
