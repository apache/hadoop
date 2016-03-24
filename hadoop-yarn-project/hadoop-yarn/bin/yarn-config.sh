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

function hadoop_subproject_init
{
  
  # at some point in time, someone thought it would be a good idea to
  # create separate vars for every subproject.  *sigh*
  # let's perform some overrides and setup some defaults for bw compat
  # this way the common hadoop var's == subproject vars and can be
  # used interchangeable from here on out
  # ...
  # this should get deprecated at some point.
  
  if [[ -z "${HADOOP_YARN_ENV_PROCESSED}" ]]; then
    if [[ -e "${YARN_CONF_DIR}/yarn-env.sh" ]]; then
      . "${YARN_CONF_DIR}/yarn-env.sh"
    elif [[ -e "${HADOOP_CONF_DIR}/yarn-env.sh" ]]; then
      . "${HADOOP_CONF_DIR}/yarn-env.sh"
    fi
    export HADOOP_YARN_ENV_PROCESSED=true
  fi
  
  hadoop_deprecate_envvar YARN_CONF_DIR HADOOP_CONF_DIR

  hadoop_deprecate_envvar YARN_LOG_DIR HADOOP_LOG_DIR

  hadoop_deprecate_envvar YARN_LOGFILE HADOOP_LOGFILE
  
  hadoop_deprecate_envvar YARN_NICENESS HADOOP_NICENESS
  
  hadoop_deprecate_envvar YARN_STOP_TIMEOUT HADOOP_STOP_TIMEOUT
  
  hadoop_deprecate_envvar YARN_PID_DIR HADOOP_PID_DIR
  
  hadoop_deprecate_envvar YARN_ROOT_LOGGER HADOOP_ROOT_LOGGER

  hadoop_deprecate_envvar YARN_IDENT_STRING HADOOP_IDENT_STRING

  hadoop_deprecate_envvar YARN_OPTS HADOOP_OPTS

  hadoop_deprecate_envvar YARN_SLAVES HADOOP_SLAVES
  
  HADOOP_YARN_HOME="${HADOOP_YARN_HOME:-$HADOOP_HOME}"
  
  # YARN-1429 added the completely superfluous YARN_USER_CLASSPATH
  # env var.  We're going to override HADOOP_USER_CLASSPATH to keep
  # consistency with the rest of the duplicate/useless env vars

  hadoop_deprecate_envvar YARN_USER_CLASSPATH HADOOP_USER_CLASSPATH

  hadoop_deprecate_envvar YARN_USER_CLASSPATH_FIRST HADOOP_USER_CLASSPATH_FIRST
}

if [[ -z "${HADOOP_LIBEXEC_DIR}" ]]; then
  _yc_this="${BASH_SOURCE-$0}"
  HADOOP_LIBEXEC_DIR=$(cd -P -- "$(dirname -- "${_yc_this}")" >/dev/null && pwd -P)
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
