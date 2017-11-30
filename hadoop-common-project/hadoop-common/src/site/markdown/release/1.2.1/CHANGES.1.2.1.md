
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# Apache Hadoop Changelog

## Release 1.2.1 - 2013-08-01



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-4838](https://issues.apache.org/jira/browse/MAPREDUCE-4838) | Add extra info to JH files |  Major | . | Arun C Murthy | Zhijie Shen |
| [MAPREDUCE-5368](https://issues.apache.org/jira/browse/MAPREDUCE-5368) | Save memory by  set capacity, load factor and concurrency level for ConcurrentHashMap in TaskInProgress |  Major | mrv1 | yunjiong zhao | yunjiong zhao |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-4581](https://issues.apache.org/jira/browse/HDFS-4581) | DataNode#checkDiskError should not be called on network errors |  Major | datanode | Rohit Kochar | Rohit Kochar |
| [HDFS-4699](https://issues.apache.org/jira/browse/HDFS-4699) | TestPipelinesFailover#testPipelineRecoveryStress fails sporadically |  Major | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5206](https://issues.apache.org/jira/browse/MAPREDUCE-5206) | JT can show the same job multiple times in Retired Jobs section |  Minor | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-5148](https://issues.apache.org/jira/browse/MAPREDUCE-5148) | Syslog missing from Map/Reduce tasks |  Major | tasktracker | Yesha Vora | Arun C Murthy |
| [MAPREDUCE-3859](https://issues.apache.org/jira/browse/MAPREDUCE-3859) | CapacityScheduler incorrectly utilizes extra-resources of queue for high-memory jobs |  Major | capacity-sched | Sergey Tryuber | Sergey Tryuber |
| [HDFS-4261](https://issues.apache.org/jira/browse/HDFS-4261) | TestBalancerWithNodeGroup times out |  Major | balancer & mover | Tsz Wo Nicholas Sze | Junping Du |
| [HDFS-4880](https://issues.apache.org/jira/browse/HDFS-4880) | Diagnostic logging while loading name/edits files |  Major | namenode | Arpit Agarwal | Suresh Srinivas |
| [MAPREDUCE-5260](https://issues.apache.org/jira/browse/MAPREDUCE-5260) | Job failed because of JvmManager running into inconsistent state |  Major | tasktracker | yunjiong zhao | yunjiong zhao |
| [MAPREDUCE-5318](https://issues.apache.org/jira/browse/MAPREDUCE-5318) | Ampersand in JSPUtil.java is not escaped |  Minor | jobtracker | Bohou Li | Bohou Li |
| [HADOOP-9665](https://issues.apache.org/jira/browse/HADOOP-9665) | BlockDecompressorStream#decompress will throw EOFException instead of return -1 when EOF |  Critical | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5375](https://issues.apache.org/jira/browse/MAPREDUCE-5375) | Delegation Token renewal exception in jobtracker logs |  Critical | . | Venkat Ranganathan | Venkat Ranganathan |
| [HADOOP-9504](https://issues.apache.org/jira/browse/HADOOP-9504) | MetricsDynamicMBeanBase has concurrency issues in createMBeanInfo |  Critical | metrics | Liang Xie | Liang Xie |
| [MAPREDUCE-5256](https://issues.apache.org/jira/browse/MAPREDUCE-5256) | CombineInputFormat isn't thread safe affecting HiveServer |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5351](https://issues.apache.org/jira/browse/MAPREDUCE-5351) | JobTracker memory leak caused by CleanupQueue reopening FileSystem |  Critical | jobtracker | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5364](https://issues.apache.org/jira/browse/MAPREDUCE-5364) | Deadlock between RenewalTimerTask methods cancel() and run() |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9730](https://issues.apache.org/jira/browse/HADOOP-9730) | fix hadoop.spec to add task-log4j.properties |  Major | build | Giridharan Kesavan | Giridharan Kesavan |
| [MAPREDUCE-4490](https://issues.apache.org/jira/browse/MAPREDUCE-4490) | JVM reuse is incompatible with LinuxTaskController (and therefore incompatible with Security) |  Critical | task-controller, tasktracker | George Datskos | sam liu |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10371](https://issues.apache.org/jira/browse/HADOOP-10371) | The eclipse-plugin cannot work  in my environment |  Minor | contrib/eclipse-plugin | huangxing |  |


