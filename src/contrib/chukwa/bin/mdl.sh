#!/bin/bash
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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/chukwa-config.sh

if [ $# -lt 2 ]; then
    echo "Usage: mdl.sh <cluster name> <chukwa sequence file>"
    echo ""
    exit 1
fi

${JAVA_HOME}/bin/java -Xms2048M -Xmx3096M -DDATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml -Djava.library.path=${JAVA_LIBRARY_PATH} -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DAPP=MDL -Dlog4j.configuration=chukwa-log4j.properties -classpath ${CLASSPATH}:/homes/eyang/chukwa-core-0.1.1.jar:${HADOOP_JAR}:${COMMON}:${tools}:${CHUKWA_HOME}/conf org.apache.hadoop.chukwa.extraction.database.MetricDataLoader $@

