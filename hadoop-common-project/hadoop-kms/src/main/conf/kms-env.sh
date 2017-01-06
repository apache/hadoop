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

# KMS config directory
#
# export KMS_CONFIG=${HADOOP_CONF_DIR}

# KMS log directory
#
# export KMS_LOG=${HADOOP_LOG_DIR}

# KMS temporary directory
#
# export KMS_TEMP=${HADOOP_HOME}/temp

# The HTTP port used by KMS
#
# export KMS_HTTP_PORT=9600

# The maximum number of HTTP handler threads
#
# export KMS_MAX_THREADS=1000

# The maximum size of HTTP header
#
# export KMS_MAX_HTTP_HEADER_SIZE=65536

# Whether SSL is enabled
#
# export KMS_SSL_ENABLED=false

# The location of the SSL keystore if using SSL
#
# export KMS_SSL_KEYSTORE_FILE=${HOME}/.keystore

# The password of the SSL keystore if using SSL
#
# export KMS_SSL_KEYSTORE_PASS=password