#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

# Set httpfs specific environment variables here.
#
# hadoop-env.sh is read prior to this file.
#

# HTTPFS temporary directory
#
# export HTTPFS_TEMP=${HADOOP_HOME}/temp

# The HTTP port used by HTTPFS
#
# export HTTPFS_HTTP_PORT=14000

# The Admin port used by HTTPFS
#
# export HTTPFS_ADMIN_PORT=$((HTTPFS_HTTP_PORT + 1))

# The maximum number of Tomcat handler threads
#
# export HTTPFS_MAX_THREADS=1000

# The hostname HttpFS server runs on
#
# export HTTPFS_HTTP_HOSTNAME=$(hostname -f)

# The maximum size of Tomcat HTTP header
#
# export HTTPFS_MAX_HTTP_HEADER_SIZE=65536

# The location of the SSL keystore if using SSL
#
# export HTTPFS_SSL_KEYSTORE_FILE=${HOME}/.keystore

#
# The password of the SSL keystore if using SSL
#
# export HTTPFS_SSL_KEYSTORE_PASS=password

##
## Tomcat specific settings
##
#
# Location of tomcat
#
# export HTTPFS_CATALINA_HOME=${HADOOP_HOME}/share/hadoop/httpfs/tomcat

# Java System properties for HTTPFS should be specified in this variable.
# The java.library.path and hadoop.home.dir properties are automatically
# configured.  In order to supplement java.library.path,
# one should add to the JAVA_LIBRARY_PATH env var.
#
# export CATALINA_OPTS=

# PID file
#
# export CATALINA_PID=${HADOOP_PID_DIR}/hadoop-${HADOOP_IDENT_STRING}-httpfs.pid

# Output file
#
# export CATALINA_OUT=${HTTPFS_LOG}/hadoop-${HADOOP_IDENT_STRING}-httpfs-${HOSTNAME}.out

