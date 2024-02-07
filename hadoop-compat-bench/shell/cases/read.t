#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"
hadoop fs -put "${localDir}/dat" "${baseDir}/"

echo "1..5"

# 1. get
hadoop fs -get "${baseDir}/dat" "${localDir}/"
expect_ret "get" 0 test -f "${localDir}/dat"

# 2. cat
expect_out "cat" "Hello World!" hadoop fs -cat "${baseDir}/dat"

# 3. text
expect_out "text" "Hello World!" hadoop fs -text "${baseDir}/dat"

# 4. head
expect_out "head" "Hello World!" hadoop fs -head "${baseDir}/dat"

# 5. tail
expect_out "tail" "Hello World!" hadoop fs -tail "${baseDir}/dat"
