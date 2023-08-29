#
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
## WORK DONE BY THE yarn AND RELATED COMMANDS.
##
## Precedence rules:
##
## yarn-env.sh > hadoop-env.sh > hard-coded defaults
##
## YARN_xyz > HADOOP_xyz > hard-coded defaults
##

###
# Resource Manager specific parameters
###

# Specify the max heapsize for the ResourceManager.  If no units are
# given, it will be assumed to be in MB.
# This value will be overridden by an Xmx setting specified in either
# HADOOP_OPTS and/or YARN_RESOURCEMANAGER_OPTS.
# Default is the same as HADOOP_HEAPSIZE_MAX
#export YARN_RESOURCEMANAGER_HEAPSIZE=

# Specify the JVM options to be used when starting the ResourceManager.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# Examples for a Sun/Oracle JDK:
# a) override the appsummary log file:
# export YARN_RESOURCEMANAGER_OPTS="-Dyarn.server.resourcemanager.appsummary.log.file=rm-appsummary.log -Dyarn.server.resourcemanager.appsummary.logger=INFO,RMSUMMARY"
#
# b) Set JMX options
# export YARN_RESOURCEMANAGER_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=1026"
#
# c) Set garbage collection logs from hadoop-env.sh
# export YARN_RESOURCE_MANAGER_OPTS="${HADOOP_GC_SETTINGS} -Xloggc:${HADOOP_LOG_DIR}/gc-rm.log-$(date +'%Y%m%d%H%M')"
#
# d) ... or set them directly
# export YARN_RESOURCEMANAGER_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:${HADOOP_LOG_DIR}/gc-rm.log-$(date +'%Y%m%d%H%M')"
#
# e) Enable ResourceManager audit logging
# export YARN_RESOURCEMANAGER_OPTS="-Drm.audit.logger=INFO,RMAUDIT"
#
#
# export YARN_RESOURCEMANAGER_OPTS=

###
# Node Manager specific parameters
###

# Specify the max heapsize for the NodeManager.  If no units are
# given, it will be assumed to be in MB.
# This value will be overridden by an Xmx setting specified in either
# HADOOP_OPTS and/or YARN_NODEMANAGER_OPTS.
# Default is the same as HADOOP_HEAPSIZE_MAX.
#export YARN_NODEMANAGER_HEAPSIZE=

# Specify the JVM options to be used when starting the NodeManager.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# a) Enable NodeManager audit logging
# export YARN_NODEMANAGER_OPTS="-Dnm.audit.logger=INFO,NMAUDIT"
#
# See ResourceManager for some examples
#
#export YARN_NODEMANAGER_OPTS=

###
# TimeLineServer specific parameters
###

# Specify the max heapsize for the timelineserver.  If no units are
# given, it will be assumed to be in MB.
# This value will be overridden by an Xmx setting specified in either
# HADOOP_OPTS and/or YARN_TIMELINESERVER_OPTS.
# Default is the same as HADOOP_HEAPSIZE_MAX.
#export YARN_TIMELINE_HEAPSIZE=

# Specify the JVM options to be used when starting the TimeLineServer.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# See ResourceManager for some examples
#
#export YARN_TIMELINESERVER_OPTS=

###
# TimeLineReader specific parameters
###

# Specify the JVM options to be used when starting the TimeLineReader.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# See ResourceManager for some examples
#
#export YARN_TIMELINEREADER_OPTS=

###
# Web App Proxy Server specifc parameters
###

# Specify the max heapsize for the web app proxy server.  If no units are
# given, it will be assumed to be in MB.
# This value will be overridden by an Xmx setting specified in either
# HADOOP_OPTS and/or YARN_PROXYSERVER_OPTS.
# Default is the same as HADOOP_HEAPSIZE_MAX.
#export YARN_PROXYSERVER_HEAPSIZE=

# Specify the JVM options to be used when starting the proxy server.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# See ResourceManager for some examples
#
#export YARN_PROXYSERVER_OPTS=

###
# Shared Cache Manager specific parameters
###
# Specify the JVM options to be used when starting the
# shared cache manager server.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# See ResourceManager for some examples
#
#export YARN_SHAREDCACHEMANAGER_OPTS=

###
# Router specific parameters
###

# Specify the max heapsize for the Router.  If no units are
# given, it will be assumed to be in MB.
# Default is the same as HADOOP_HEAPSIZE_MAX
#export YARN_ROUTER_HEAPSIZE=

# Specify the JVM options to be used when starting the Router.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# See ResourceManager for some examples
#
#export YARN_ROUTER_OPTS="-Drouter.audit.logger=INFO,ROUTERAUDIT"

###
# Global Policy Generator specific parameters
###

# Specify the JVM options to be used when starting the GPG.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# See ResourceManager for some examples
#
#export YARN_GLOBALPOLICYGENERATOR_OPTS=

###
# Registry DNS specific parameters
# This is deprecated and should be done in hadoop-env.sh
###
# For privileged registry DNS, user to run as after dropping privileges
# This will replace the hadoop.id.str Java property in secure mode.
# export YARN_REGISTRYDNS_SECURE_USER=yarn

# Supplemental options for privileged registry DNS
# By default, Hadoop uses jsvc which needs to know to launch a
# server jvm.
# export YARN_REGISTRYDNS_SECURE_EXTRA_OPTS="-jvm server"

###
# YARN Services parameters
###
# Directory containing service examples
# export YARN_SERVICE_EXAMPLES_DIR = $HADOOP_YARN_HOME/share/hadoop/yarn/yarn-service-examples
# export YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE=true
