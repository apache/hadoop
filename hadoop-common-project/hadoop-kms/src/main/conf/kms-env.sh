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

# Set kms specific environment variables here.

# Settings for the Embedded Tomcat that runs KMS
# Java System properties for KMS should be specified in this variable
#
# export CATALINA_OPTS=

# KMS logs directory
#
# export KMS_LOG=${KMS_HOME}/logs

# KMS temporary directory
#
# export KMS_TEMP=${KMS_HOME}/temp

# The HTTP port used by KMS
#
# export KMS_HTTP_PORT=16000

# The Admin port used by KMS
#
# export KMS_ADMIN_PORT=`expr ${KMS_HTTP_PORT} + 1`

# The Tomcat protocol to use for handling requests.
# The default HTTP/1.1 handler is thread-per-request.
# The NIO handler multiplexes multiple requests per thread.
#
# export KMS_PROTOCOL="HTTP/1.1"
# export KMS_PROTOCOL="org.apache.coyote.http11.Http11NioProtocol"

# The maximum number of Tomcat handler threads
#
# export KMS_MAX_THREADS=1000

# The maximum queue length for incoming connection requests when all possible
# request processing threads are in use. Any requests received when the queue
# is full will be refused.
#
# export KMS_ACCEPT_COUNT=500

# The number of threads to be used to accept connections. Increase this value
# on a multi CPU machine, although you would never really need more than 2.
# Also, with a lot of non keep alive connections, you might want to increase
# this value as well.
#
# Increasing this has no effect unless using the NIO protocol.
#
# export KMS_ACCEPTOR_THREAD_COUNT=1

# The maximum size of Tomcat HTTP header
#
# export KMS_MAX_HTTP_HEADER_SIZE=65536

# Set to 'true' if you want the SSL stack to require a valid certificate chain
# from the client before accepting a connection. Set to 'want' if you want the
# SSL stack to request a client Certificate, but not fail if one isn't
# presented. A 'false' value (which is the default) will not require a
# certificate chain unless the client requests a resource protected by a
# security constraint that uses CLIENT-CERT authentication.
#
# export KMS_SSL_CLIENT_AUTH=false

# The comma separated list of SSL protocols to support
#
# export KMS_SSL_ENABLED_PROTOCOLS="TLSv1,TLSv1.1,TLSv1.2,SSLv2Hello"

# The comma separated list of encryption ciphers for SSL
#
# export KMS_SSL_CIPHERS=

# The location of the SSL keystore if using SSL
#
# export KMS_SSL_KEYSTORE_FILE=${HOME}/.keystore

# The password of the SSL keystore if using SSL
#
# export KMS_SSL_KEYSTORE_PASS=password

# The full path to any native libraries that need to be loaded
# (For eg. location of natively compiled tomcat Apache portable
# runtime (APR) libraries
#
# export JAVA_LIBRARY_PATH=${HOME}/lib/native
