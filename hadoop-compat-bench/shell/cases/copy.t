#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"

echo "1..3"

# 1. copyFromLocal
expect_ret "copyFromLocal" 0 hadoop fs -copyFromLocal "${localDir}/dat" "${baseDir}/"

# 2. cp
hadoop fs -cp "${baseDir}/dat" "${baseDir}/dat2"
expect_ret "cp" 0 hadoop fs -test -f "${baseDir}/dat2"

# 3. copyToLocal
hadoop fs -copyToLocal "${baseDir}/dat2" "${localDir}/"
expect_ret "copyToLocal" 0 test -f "${localDir}/dat2"
