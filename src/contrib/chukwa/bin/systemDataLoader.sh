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

pid=$$

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/chukwa-config.sh

echo "${pid}" > "$CHUKWA_HOME/var/run/systemDataLoader.pid"

${JAVA_HOME}/bin/java -DCHUKWA_HOME=${CHUKWA_HOME} -DRECORD_TYPE=Sar -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${chukwaCore}:${hadoop_jar}:${common}:${tools}:${CHUKWA_HOME}/conf org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec sar -q -r -n FULL 55 &
${JAVA_HOME}/bin/java -DCHUKWA_HOME=${CHUKWA_HOME} -DRECORD_TYPE=Iostat -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${chukwaCore}:${hadoop_jar}:${common}:${tools}:${CHUKWA_HOME}/conf org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec iostat -x 55 2 &
${JAVA_HOME}/bin/java -DCHUKWA_HOME=${CHUKWA_HOME} -DRECORD_TYPE=Top -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${chukwaCore}:${hadoop_jar}:${common}:${tools}:${CHUKWA_HOME}/conf org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec top -b -n 1 -c &
#${JAVA_HOME}/bin/java -DRECORD_TYPE=Df -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${chukwaAgent}:${hadoop_jar}:${common}:${tools}:${CHUKWA_HOME}/conf org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec df -x nfs -x none &

