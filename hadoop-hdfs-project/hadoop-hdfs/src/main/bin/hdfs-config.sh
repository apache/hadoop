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

# included in all the hdfs scripts with source command
# should not be executed directly

function hadoop_subproject_init
{
  if [[ -z "${HADOOP_HDFS_ENV_PROCESSED}" ]]; then
    if [[ -e "${HADOOP_CONF_DIR}/hdfs-env.sh" ]]; then
      . "${HADOOP_CONF_DIR}/hdfs-env.sh"
      export HADOOP_HDFS_ENV_PROCESSED=true
    fi
  fi
  
  # at some point in time, someone thought it would be a good idea to
  # create separate vars for every subproject.  *sigh*
  # let's perform some overrides and setup some defaults for bw compat
  # this way the common hadoop var's == subproject vars and can be
  # used interchangeable from here on out
  # ...
  # this should get deprecated at some point.

  hadoop_deprecate_envvar HADOOP_HDFS_LOG_DIR HADOOP_LOG_DIR

  hadoop_deprecate_envvar HADOOP_HDFS_LOGFILE HADOOP_LOGFILE

  hadoop_deprecate_envvar HADOOP_HDFS_NICENESS HADOOP_NICENESS

  hadoop_deprecate_envvar HADOOP_HDFS_STOP_TIMEOUT HADOOP_STOP_TIMEOUT
  
  hadoop_deprecate_envvar HADOOP_HDFS_PID_DIR HADOOP_PID_DIR

  hadoop_deprecate_envvar HADOOP_HDFS_ROOT_LOGGER HADOOP_ROOT_LOGGER

  hadoop_deprecate_envvar HADOOP_HDFS_IDENT_STRING HADOOP_IDENT_STRING
  
  HADOOP_HDFS_HOME="${HADOOP_HDFS_HOME:-$HADOOP_HOME}"
  
  # turn on the defaults
  export HDFS_AUDIT_LOGGER=${HDFS_AUDIT_LOGGER:-INFO,NullAppender}
  export HADOOP_NAMENODE_OPTS=${HADOOP_NAMENODE_OPTS:-"-Dhadoop.security.logger=INFO,RFAS"}
  export HADOOP_SECONDARYNAMENODE_OPTS=${HADOOP_SECONDARYNAMENODE_OPTS:-"-Dhadoop.security.logger=INFO,RFAS"}
  export HADOOP_DATANODE_OPTS=${HADOOP_DATANODE_OPTS:-"-Dhadoop.security.logger=ERROR,RFAS"}
  export HADOOP_DN_SECURE_EXTRA_OPTS=${HADOOP_DN_SECURE_EXTRA_OPTS:-"-jvm server"}
  export HADOOP_NFS3_SECURE_EXTRA_OPTS=${HADOOP_NFS3_SECURE_EXTRA_OPTS:-"-jvm server"}
  export HADOOP_PORTMAP_OPTS=${HADOOP_PORTMAP_OPTS:-"-Xmx512m"}
}

if [[ -z "${HADOOP_LIBEXEC_DIR}" ]]; then
  _hd_this="${BASH_SOURCE-$0}"
  HADOOP_LIBEXEC_DIR=$(cd -P -- "$(dirname -- "${_hd_this}")" >/dev/null && pwd -P)
fi

if [[ -n "${HADOOP_COMMON_HOME}" ]] &&
   [[ -e "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh" ]]; then
  . "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh"
elif [[ -e "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
elif [ -e "${HADOOP_HOME}/libexec/hadoop-config.sh" ]; then
  . "${HADOOP_HOME}/libexec/hadoop-config.sh"
else
  echo "ERROR: Hadoop common not found." 2>&1
  exit 1
fi

