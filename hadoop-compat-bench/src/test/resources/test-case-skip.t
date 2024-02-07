#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "1..3"

expect_ret "mkdir (ut)" 0 hadoop fs -mkdir -p "${baseDir}/dir"
expect_ret "nonExistCommand (ut)" 0 hadoop fs -nonExistCommand "${baseDir}/dir"
expect_ret "rm (ut)" 0 hadoop fs -rm -r -skipTrash "${baseDir}/dir"
