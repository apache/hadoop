#!/bin/bash
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

# Settings for the Embedded Tomcat that runs HttpFS
# Java System properties for HttpFS should be specified in this variable
#
# export CATALINA_OPTS=

# HttpFS logs directory
#
# export HTTPFS_LOG=${HTTPFS_HOME}/logs

# HttpFS temporary directory
#
# export HTTPFS_TEMP=${HTTPFS_HOME}/temp

# The HTTP port used by HttpFS
#
# export HTTPFS_HTTP_PORT=14000

# The Admin port used by HttpFS
#
# export HTTPFS_ADMIN_PORT=`expr ${HTTPFS_HTTP_PORT} + 1`

# The hostname HttpFS server runs on
#
# export HTTPFS_HTTP_HOSTNAME=`hostname -f`
