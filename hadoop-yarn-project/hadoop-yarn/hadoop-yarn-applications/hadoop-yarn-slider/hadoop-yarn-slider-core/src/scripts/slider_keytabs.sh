#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script exists to create the keytab set for a node on the cluster
# including hbase and ZK alongside then YARN cores.

# usage
# keytabs <realm> <hostname>
# validate the args

num_vars=$#
if [[ $num_vars < 2 ]]
then
  echo "Usage: $0 <realm> <hostname>"
  exit -2
fi

realm="$1"
hostname="$2"
dest="."

kadmin=kadmin.local

${kadmin} <<EOF
addprinc -randkey hdfs/${hostname}@${realm}
addprinc -randkey yarn/${hostname}@${realm}
addprinc -randkey HTTP/${hostname}@${realm}
addprinc -randkey hbase/${hostname}@${realm}
addprinc -randkey zookeeper/${hostname}@${realm}

ktadd -norandkey -k ${dest}/hdfs.keytab  \
  hdfs/${hostname}@${realm} \
  HTTP/${hostname}@${realm}

ktadd -norandkey -k ${dest}/yarn.keytab  \
  yarn/${hostname}@${realm} \
  HTTP/${hostname}@${realm}

ktadd -norandkey -k ${dest}/hbase.keytab  \
  hbase/${hostname}@${realm} 

ktadd -norandkey -k ${dest}/zookeeper.keytab  \
  zookeeper/${hostname}@${realm} 
EOF

exitcode=$?
if  [[ $exitcode != 0 ]]
then
  echo "keytab generation from ${kadmin} failed with exit code $exitcode"
  exit $exitcode
else
  echo "keytab files for ${hostname}@${realm} created"
fi
