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

hadoop_add_profile yarn

function _yarn_hadoop_classpath
{
  local i
  #
  # get all of the yarn jars+config in the path
  #
  # developers
  if [[ -n "${HADOOP_ENABLE_BUILD_PATHS}" ]]; then
    for i in yarn-api yarn-common yarn-mapreduce yarn-master-worker \
             yarn-server/yarn-server-nodemanager \
             yarn-server/yarn-server-common \
             yarn-server/yarn-server-resourcemanager; do
      hadoop_add_classpath "${HADOOP_YARN_HOME}/$i/target/classes"
    done

    hadoop_add_classpath "${HADOOP_YARN_HOME}/build/test/classes"
    hadoop_add_classpath "${HADOOP_YARN_HOME}/build/tools"
  fi

  if [[ -d "${HADOOP_YARN_HOME}/${YARN_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_YARN_HOME}/${YARN_DIR}"
  fi

  hadoop_add_classpath "${HADOOP_YARN_HOME}/${YARN_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath  "${HADOOP_YARN_HOME}/${YARN_DIR}"'/*'
}

function _yarn_hadoop_finalize
{
  # Add YARN custom options to comamnd line in case someone actaully
  # used these.
  #
  # Note that we are replacing ' ' with '\ ' so that when we exec
  # stuff it works
  #
  local yld=$HADOOP_LOG_DIR
  hadoop_translate_cygwin_path yld
  hadoop_add_param HADOOP_OPTS yarn.log.dir "-Dyarn.log.dir=${yld}"
  hadoop_add_param HADOOP_OPTS yarn.log.file "-Dyarn.log.file=${HADOOP_LOGFILE}"
  local yhd=$HADOOP_YARN_HOME
  hadoop_translate_cygwin_path yhd
  hadoop_add_param HADOOP_OPTS yarn.home.dir "-Dyarn.home.dir=${yhd}"
  hadoop_add_param HADOOP_OPTS yarn.root.logger "-Dyarn.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"
}
