#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"
hadoop fs -put "${localDir}/dat" "${baseDir}/"

echo "1..5"

# 1. listPolicies
expect_ret "listPolicies" 0 hdfs storagepolicies -Dfs.defaultFS="${baseDir}" -listPolicies

# 2. setStoragePolicy
expect_out "setStoragePolicy" "Set storage policy ${storagePolicy} .*" hdfs storagepolicies -setStoragePolicy -path "${baseDir}" -policy "${storagePolicy}"

# 3. getStoragePolicy
expect_out "getStoragePolicy" ".*${storagePolicy}.*" hdfs storagepolicies -getStoragePolicy -path "${baseDir}"

# 4. satisfyStoragePolicy
expect_out "satisfyStoragePolicy" "Scheduled blocks to move .*" hdfs storagepolicies -satisfyStoragePolicy -path "${baseDir}"

# 5. unsetStoragePolicy
expect_out "unsetStoragePolicy" "Unset storage policy .*" hdfs storagepolicies -unsetStoragePolicy -path "${baseDir}"
