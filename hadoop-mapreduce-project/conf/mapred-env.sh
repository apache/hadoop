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

##
## THIS FILE ACTS AS AN OVERRIDE FOR hadoop-env.sh FOR ALL
## WORK DONE BY THE mapred AND RELATED COMMANDS.
##
## Precedence rules:
##
## mapred-env.sh > hadoop-env.sh > hard-coded defaults
##
## MAPRED_xyz > HADOOP_xyz > hard-coded defaults
##

###
# Generic settings for MapReduce
###

#Override the log4j settings for all MR apps
# export MAPRED_ROOT_LOGGER="INFO,console"

# Override Hadoop's log directory & file
# export HADOOP_MAPRED_LOG_DIR=""

# Override Hadoop's pid directory
# export HADOOP_MAPRED_PID_DIR=

# Override Hadoop's identity string. $USER by default.
# This is used in writing log and pid files, so keep that in mind!
# export HADOOP_MAPRED_IDENT_STRING=$USER

# Override Hadoop's process priority
# Note that sub-processes will also run at this level!
# export HADOOP_MAPRED_NICENESS=0

###
# Job History Server specific parameters
###

# Specify the max heapsize for the Job History Server using a numerical value
# in the scale of MB. For example, to specify an jvm option of -Xmx1000m, set
# the value to 1000.
# This value will be overridden by an Xmx setting specified in either
# MAPRED_OPTS, HADOOP_OPTS, and/or HADOOP_JOB_HISTORYSERVER_OPTS.
# If not specified, the default value will be picked from either YARN_HEAPMAX
# or JAVA_HEAP_MAX with YARN_HEAPMAX as the preferred option of the two.
#
#export HADOOP_JOB_HISTORYSERVER_HEAPSIZE=1000

# Specify the JVM options to be used when starting the ResourceManager.
# These options will be appended to the options specified as YARN_OPTS
# and therefore may override any similar flags set in YARN_OPTS
#export HADOOP_JOB_HISTORYSERVER_OPTS=

# Specify the log4j settings for the JobHistoryServer
#export HADOOP_JHS_LOGGER=INFO,RFA



