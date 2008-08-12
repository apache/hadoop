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

# remove watchdog
crontab -l | grep -v ${CHUKWA_HOME}/bin/watchdog.sh > /tmp/cron.${CURRENT_DATE}
crontab /tmp/cron.${CURRENT_DATE}
rm -f /tmp/cron.${CURRENT_DATE}

# stop torque data loader
pidFile=$CHUKWA_HOME/var/run/TorqueDataLoader.pid
if [ -f $pidFile ]; then  
   echo -n "Shutting down Torque Data Loader.."
   torquepid=`head ${pidFile}`
   kill -HUP ${torquepid}
   # kill -HUP `ps eww |grep TorqueDataLoader |grep -v grep |cut -b 1-5` >/dev/null 2>&1
   rm ${pidFile}
   echo "done"
else
  echo " no $pidFile"
fi

# stop util data loader
pidFile=$CHUKWA_HOME/var/run/UtilDataLoader.pid
if [ -f $pidFile ]; then  
    echo -n "Shutting down Util Data Loader.."
    utilpid=`head ${pidFile}`
    #kill -HUP `ps eww |grep UtilDataLoader |grep -v grep |cut -b 1-5` >/dev/null 2>&1
    kill -HUP ${utilpid}
    rm ${pidFile}
    echo "done"
else
  echo " no $pidFile"
fi

# stop queue info data loader
pidFile=$CHUKWA_HOME/var/run/QueueInfoDataLoader.pid
if [ -f $pidFile ]; then  
    echo -n "Shutting down Queue Info Data Loader.."
    queuepid=`head ${pidFile}`
    #kill -HUP `ps eww |grep QueueInfoDataLoader |grep -v grep |cut -b 1-5` >/dev/null 2>&1
    kill -HUP ${queuepid}
    rm ${pidFile}
    echo "done"
else 
  echo " no $pidFile"
fi


# stop queue info data loader
pidFile=$CHUKWA_HOME/var/run/MapReduceLogLoader.pid
if [ -f $pidFile ]; then  
    echo -n "Shutting down Map Reduce Log Loader.."
    logpid=`head ${pidFile}`
    #kill -HUP `ps eww |grep MapReduceLogLoader |grep -v grep |cut -b 1-5` >/dev/null 2>&1
    kill -HUP ${logpid}
    rm ${pidFile}
    echo "done"
else
  echo " no $pidFile"
fi
 
