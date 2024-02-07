#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"
hadoop fs -mkdir -p "${baseDir}/dir/sub"
hadoop fs -put "${localDir}/dat" "${baseDir}/dir/"
hadoop fs -put "${localDir}/dat" "${baseDir}/dir/sub/"

echo "1..4"

# 1. rm
hadoop fs -rm -f -skipTrash "${baseDir}/dir/dat"
expect_ret "rm" 1 hadoop fs -test -e "${baseDir}/dir/dat"

# 2. rmr
hadoop fs -rmr "${baseDir}/dir/sub"
expect_ret "rmr" 1 hadoop fs -test -e "${baseDir}/dir/sub"

# 3. rmdir
hadoop fs -rmdir "${baseDir}/dir"
expect_ret "rmdir" 1 hadoop fs -test -e "${baseDir}/dir"

# 4. expunge
expect_ret "expunge" 0 hadoop fs -expunge -immediate -fs "${baseDir}"
