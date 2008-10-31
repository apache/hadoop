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
java=$JAVA_HOME/bin/java

. "$bin"/chukwa-config.sh

# stop processSinkFiles.sh
pidFile=$CHUKWA_HOME/var/run/ProcessSinkFiles.pid
if [ -f $pidFile ]; then  
   echo -n "Shutting down Data Processors.."
   DP_PID=`head ${pidFile}`
   kill -TERM ${DP_PID}
   rm ${pidFile}
   echo "done"
else
  echo " no $pidFile"
fi

# stop dbAdmin.sh
pidFile=$CHUKWA_HOME/var/run/dbAdmin.pid
if [ -f $pidFile ]; then  
   echo -n "Shutting down Database Admin.."
   DBADMIN_PID=`head ${pidFile}`
   kill -TERM ${DBADMIN_PID}
   rm ${pidFile}
   echo "done"
else
  echo " no $pidFile"
fi

