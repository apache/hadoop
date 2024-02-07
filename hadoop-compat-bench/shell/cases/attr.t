#!/bin/sh
. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"
hadoop fs -put "${localDir}/dat" "${baseDir}/"

echo "1..10"

# 1. chown
hadoop fs -chown "hadoop-compat-bench-user" "${baseDir}/dat"
expect_out "chown" "user:hadoop-compat-bench-user" hadoop fs -stat "user:%u" "${baseDir}/dat"

# 2. chgrp
hadoop fs -chgrp "hadoop-compat-bench-group" "${baseDir}/dat"
expect_out "chgrp" "group:hadoop-compat-bench-group" hadoop fs -stat "group:%g" "${baseDir}/dat"

# 3. chmod
hadoop fs -chmod 777 "${baseDir}/dat"
expect_out "chmod" "perm:777" hadoop fs -stat "perm:%a" "${baseDir}/dat"

# 4. touch
hadoop fs -touch -m -t "20000615:000000" "${baseDir}/dat"
expect_out "touch" "date:2000-06-.*" hadoop fs -stat "date:%y" "${baseDir}/dat"

# 5. setfattr
expect_ret "setfattr" 0 hadoop fs -setfattr -n "user.key" -v "value" "${baseDir}/dat"

# 6. getfattr
expect_out "getfattr" ".*value.*" hadoop fs -getfattr -n "user.key" "${baseDir}/dat"

# 7. setfacl
expect_ret "setfacl" 0 hadoop fs -setfacl -m "user:foo:---" "${baseDir}/dat"

# 8. getfacl
expect_out "getfacl" ".*foo.*" hadoop fs -getfacl "${baseDir}/dat"

# 9. setrep
hadoop fs -setrep 1 "${baseDir}/dat"
expect_out "setrep" "replication:1" hadoop fs -stat "replication:%r" "${baseDir}/dat"

# 10. checksum
expect_ret "checksum" 0 hadoop fs -checksum "${baseDir}/dat"  # TODO
