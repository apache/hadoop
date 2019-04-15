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

date=$(date +"%Y-%m-%d--%H-%M-%S-%Z")
fileformat=".MiniOzoneChaosCluster.log"
heapformat=".dump"
current="/tmp/"
filename="${current}${date}${fileformat}"
heapdumpfile="${current}${date}${heapformat}"

export MAVEN_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${heapdumpfile}"

echo "logging to ${filename}"
echo "heapdump to ${heapdumpfile}"

echo "Starting MiniOzoneChaosCluster"
mvn clean install -DskipTests > "${filename}" 2>&1
mvn exec:java \
  -Dexec.mainClass="org.apache.hadoop.ozone.TestMiniChaosOzoneCluster" \
  -Dexec.classpathScope=test \
  -Dexec.args="$*" >> "${filename}" 2>&1
