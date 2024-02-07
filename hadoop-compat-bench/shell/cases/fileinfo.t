#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"
hadoop fs -put "${localDir}/dat" "${baseDir}/"
hadoop fs -mkdir -p "${baseDir}/dir/sub"

echo "1..9"

# 1. ls
expect_lines "ls" 2 ".*dat.*" ".*dir.*" hadoop fs -ls "${baseDir}"

# 2. lsr
expect_lines "lsr" 3 ".*dat.*" ".*dir.*" ".*sub.*" hadoop fs -lsr "${baseDir}"

# 3. count
expect_out "count" ".*13.*" hadoop fs -count "${baseDir}"

# 4. du
expect_out "du" ".*13.*" hadoop fs -du "${baseDir}"

# 5. dus
expect_out "dus" ".*13.*" hadoop fs -dus "${baseDir}"

# 6. df
expect_ret "df" 0 hadoop fs -df "${baseDir}"

# 7. stat
expect_out "stat" "size:13" hadoop fs -stat "size:%b" "${baseDir}/dat"

# 8. test
expect_ret "test" 0 hadoop fs -test -f "${baseDir}/dat"

# 9. find
expect_out "find" ".*dat.*" hadoop fs -find "${baseDir}" -name "dat" -print
