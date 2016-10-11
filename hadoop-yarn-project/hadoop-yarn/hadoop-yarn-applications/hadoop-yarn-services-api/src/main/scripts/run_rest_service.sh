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

export SLIDER_VERSION=${project.version}
export HDP_VERSION=${HDP_VERSION:-$SLIDER_VERSION}
export SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
export LIB_PARENT_DIR=`dirname $SCRIPT_DIR`
export JAVA_HOME=${JAVA_HOME:-/usr/jdk64/jdk1.8.0_40}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
export REST_SERVICE_PORT=${REST_SERVICE_PORT:-9191}
export APP_RUNAS_USER=${APP_RUNAS_USER:-root}
export REST_SERVICE_LOG_DIR=${REST_SERVICE_LOG_DIR:-/tmp/}
export JAVA_OPTS="-Xms256m -Xmx1024m -XX:+PrintGC -Xloggc:$REST_SERVICE_LOG_DIR/gc.log"
$JAVA_HOME/bin/java $JAVA_OPTS -cp .:$HADOOP_CONF_DIR:$LIB_PARENT_DIR/services-api/*:$LIB_PARENT_DIR/slider/* -DREST_SERVICE_LOG_DIR=$REST_SERVICE_LOG_DIR -Dlog4j.configuration=log4j-server.properties -Dslider.libdir=$LIB_PARENT_DIR/slider org.apache.hadoop.yarn.services.webapp.ApplicationApiWebApp 1>>$REST_SERVICE_LOG_DIR/restservice-out.log 2>&1
