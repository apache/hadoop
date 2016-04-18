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

# Set kms specific environment variables here.
#
# hadoop-env.sh is read prior to this file.
#

# KMS temporary directory
#
# export KMS_TEMP=${HADOOP_HOME}/temp

# The HTTP port used by KMS
#
# export KMS_HTTP_PORT=9600

# The Admin port used by KMS
#
# export KMS_ADMIN_PORT=$((KMS_HTTP_PORT + 1))

# The maximum number of Tomcat handler threads
#
# export KMS_MAX_THREADS=1000

# The maximum size of Tomcat HTTP header
#
# export KMS_MAX_HTTP_HEADER_SIZE=65536

# The location of the SSL keystore if using SSL
#
# export KMS_SSL_KEYSTORE_FILE=${HOME}/.keystore

#
# The password of the SSL keystore if using SSL
#
# export KMS_SSL_KEYSTORE_PASS=password

#
# The password of the truststore
#
# export KMS_SSL_TRUSTSTORE_PASS=


##
## Tomcat specific settings
##
#
# Location of tomcat
#
# export KMS_CATALINA_HOME=${HADOOP_HOME}/share/hadoop/kms/tomcat

# Java System properties for KMS should be specified in this variable.
# The java.library.path and hadoop.home.dir properties are automatically
# configured.  In order to supplement java.library.path,
# one should add to the JAVA_LIBRARY_PATH env var.
#
# export CATALINA_OPTS=

# PID file
#
# export CATALINA_PID=${HADOOP_PID_DIR}/hadoop-${HADOOP_IDENT_STRING}-kms.pid

# Output file
#
# export CATALINA_OUT=${KMS_LOG}/hadoop-${HADOOP_IDENT_STRING}-kms-${HOSTNAME}.out

