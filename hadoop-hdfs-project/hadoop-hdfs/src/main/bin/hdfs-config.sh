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

  hadoop_deprecate_envvar HADOOP_DN_SECURE_EXTRA_OPTS HDFS_DATANODE_SECURE_EXTRA_OPTS

  hadoop_deprecate_envvar HADOOP_NFS3_SECURE_EXTRA_OPTS HDFS_NFS3_SECURE_EXTRA_OPTS

  hadoop_deprecate_envvar HADOOP_SECURE_DN_USER HDFS_DATANODE_SECURE_USER

  hadoop_deprecate_envvar HADOOP_PRIVILEGED_NFS_USER HDFS_NFS3_SECURE_USER

  HADOOP_HDFS_HOME="${HADOOP_HDFS_HOME:-$HADOOP_HOME}"

  # turn on the defaults
  export HDFS_AUDIT_LOGGER=${HDFS_AUDIT_LOGGER:-INFO,NullAppender}
  export HDFS_NAMENODE_OPTS=${HDFS_NAMENODE_OPTS:-"-Dhadoop.security.logger=INFO,RFAS"}
  export HDFS_SECONDARYNAMENODE_OPTS=${HDFS_SECONDARYNAMENODE_OPTS:-"-Dhadoop.security.logger=INFO,RFAS"}
  export HDFS_DATANODE_OPTS=${HDFS_DATANODE_OPTS:-"-Dhadoop.security.logger=ERROR,RFAS"}
  export HDFS_PORTMAP_OPTS=${HDFS_PORTMAP_OPTS:-"-Xmx512m"}

  # depending upon what is being used to start Java, these may need to be
  # set empty. (thus no colon)
  export HDFS_DATANODE_SECURE_EXTRA_OPTS=${HDFS_DATANODE_SECURE_EXTRA_OPTS-"-jvm server"}
  export HDFS_NFS3_SECURE_EXTRA_OPTS=${HDFS_NFS3_SECURE_EXTRA_OPTS-"-jvm server"}
}

if [[ -z "${HADOOP_LIBEXEC_DIR}" ]]; then
  _hd_this="${BASH_SOURCE-$0}"
  HADOOP_LIBEXEC_DIR=$(cd -P -- "$(dirname -- "${_hd_this}")" >/dev/null && pwd -P)
fi

# shellcheck source=./hadoop-common-project/hadoop-common/src/main/bin/hadoop-config.sh

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

