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


# ------------------------------------------------------------------
#
# The purpose of this script is to distribute the exclude file (see
# "dfs.hosts.exclude" in hdfs-site.xml).
#
# Input of the script is a local exclude file. The exclude file
# will be distributed to all the namenodes. The location on the namenodes
# is determined by the configuration "dfs.hosts.exclude" in hdfs-site.xml
# (this value is read from the local copy of hdfs-site.xml and must be same
# on all the namenodes).
#
# The user running this script needs write permissions on the target
# directory on namenodes.
#
# After this command, run refresh-namenodes.sh so that namenodes start
# using the new exclude file.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

HADOOP_DEFAULT_LIBEXEC_DIR="$bin"/../libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$HADOOP_DEFAULT_LIBEXEC_DIR}
. $HADOOP_LIBEXEC_DIR/hdfs-config.sh

if [ "$1" = '' ] ; then
  "Error: please specify local exclude file as a first argument"
  exit 1
else
  excludeFilenameLocal=$1
fi

if [ ! -f "$excludeFilenameLocal" ] ; then
  echo "Error: exclude file [$excludeFilenameLocal] does not exist."
  exit 1
fi

namenodes=$("$HADOOP_PREFIX/bin/hdfs" getconf -namenodes)
excludeFilenameRemote=$("$HADOOP_PREFIX/bin/hdfs" getconf -excludeFile)

if [ "$excludeFilenameRemote" = '' ] ; then
  echo \
  "Error: hdfs getconf -excludeFile returned empty string, " \
  "please setup dfs.hosts.exclude in hdfs-site.xml in local cluster " \
  "configuration and on all namenodes"
  exit 1
fi

echo "Copying exclude file [$excludeFilenameRemote] to namenodes:"

for namenode in $namenodes ; do
  echo "    [$namenode]"
  scp "$excludeFilenameLocal" "$namenode:$excludeFilenameRemote"
  if [ "$?" != '0' ] ; then errorFlag='1' ; fi
done

if [ "$errorFlag" = '1' ] ; then
  echo "Error: transfer of exclude file failed, see error messages above."
  exit 1
else
  echo "Transfer of exclude file to all namenodes succeeded."
fi

# eof
