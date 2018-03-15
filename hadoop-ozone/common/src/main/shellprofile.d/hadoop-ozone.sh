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

if [[ "${HADOOP_SHELL_EXECNAME}" = oz ]]; then
   hadoop_add_profile ozone
fi


## @description  Profile for hdsl/cblock/ozone components.
## @audience     private
## @stability    evolving
function _ozone_hadoop_classpath
{
  #
  # get all of the ozone jars+config in the path
  #

  if [[ -d "${HADOOP_HDFS_HOME}/${HDSL_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_HDFS_HOME}/${HDSL_DIR}"
  fi

  if [[ -d "${HADOOP_HDFS_HOME}/${HDSL_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_HDFS_HOME}/${OZONE_DIR}"
  fi

  hadoop_add_classpath "${HADOOP_HDFS_HOME}/${HDSL_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_HDFS_HOME}/${HDSL_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_HDFS_HOME}/${OZONE_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_HDFS_HOME}/${OZONE_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_HDFS_HOME}/${CBLOCK_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_HDFS_HOME}/${CBLOCK_DIR}"'/*'

}
