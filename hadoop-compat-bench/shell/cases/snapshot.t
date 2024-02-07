#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "1..3"

# 1. createSnapshot
expect_out "createSnapshot" "Created snapshot .*" hdfs dfs -createSnapshot "${snapshotDir}" "s-name"

# 2. renameSnapshot
expect_ret "renameSnapshot" 0 hdfs dfs -renameSnapshot "${snapshotDir}" "s-name" "d-name"

# 3. deleteSnapshot
expect_ret "deleteSnapshot" 0 hdfs dfs -deleteSnapshot "${snapshotDir}" "d-name"
