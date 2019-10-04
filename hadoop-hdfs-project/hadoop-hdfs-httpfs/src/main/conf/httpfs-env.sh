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

# Indicates if HttpFS is using SSL
#
# export HTTPFS_SSL_ENABLED=false

# Set to 'true' if you want the SSL stack to require a valid certificate chain
# from the client before accepting a connection. Set to 'want' if you want the
# SSL stack to request a client Certificate, but not fail if one isn't
# presented. A 'false' value (which is the default) will not require a
# certificate chain unless the client requests a resource protected by a
# security constraint that uses CLIENT-CERT authentication.
#
# export HTTPFS_SSL_CLIENT_AUTH=false

# The comma separated list of SSL protocols to support
#
# export HTTPFS_SSL_ENABLED_PROTOCOLS="TLSv1,TLSv1.1,TLSv1.2,SSLv2Hello"

# The comma separated list of encryption ciphers for SSL
#
# export HTTPFS_SSL_CIPHERS=

# The maximum size of Tomcat HTTP header
#
# export HTTPFS_MAX_HTTP_HEADER_SIZE=65536

# The location of the SSL keystore if using SSL
#
# export HTTPFS_SSL_KEYSTORE_FILE=${HOME}/.keystore

# The password of the SSL keystore if using SSL
#
# export HTTPFS_SSL_KEYSTORE_PASS=password

# The full path to any native libraries that need to be loaded
# (For eg. location of natively compiled tomcat Apache portable
# runtime (APR) libraries
#
# export JAVA_LIBRARY_PATH=${HOME}/lib/native
