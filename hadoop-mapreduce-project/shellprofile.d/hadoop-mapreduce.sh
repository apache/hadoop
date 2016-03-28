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

hadoop_add_profile mapred

function _mapred_hadoop_classpath
{
  #
  # get all of the mapreduce jars+config in the path
  #
  # developers
  if [[ -n "${HADOOP_ENABLE_BUILD_PATHS}" ]]; then
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-shuffle/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-common/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-hs/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-hs-plugins/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-app/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-jobclient/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-core/target/classes"
  fi

  if [[ -d "${HADOOP_MAPRED_HOME}/${MAPRED_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/${MAPRED_DIR}"
  fi

  hadoop_add_classpath "${HADOOP_MAPRED_HOME}/${MAPRED_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_MAPRED_HOME}/${MAPRED_DIR}"'/*'
}
