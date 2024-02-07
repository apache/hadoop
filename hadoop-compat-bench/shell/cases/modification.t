#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"

echo "1..4"

# 1. mkdir
expect_ret "mkdir" 0 hadoop fs -mkdir -p "${baseDir}/dir"

# 2. put
expect_ret "put" 0 hadoop fs -put "${localDir}/dat" "${baseDir}/"

# 3. appendToFile
expect_ret "appendToFile" 0 hadoop fs -appendToFile "${localDir}/dat" "${baseDir}/dat"

# 4. truncate
expect_ret "truncate" 0 hadoop fs -truncate 13 "${baseDir}/dat"
