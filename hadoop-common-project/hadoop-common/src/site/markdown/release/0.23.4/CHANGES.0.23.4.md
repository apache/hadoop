
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

## Release 0.23.4 - 2012-10-15

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7788](https://issues.apache.org/jira/browse/HADOOP-7788) | HA: Simple HealthMonitor class to watch an HAService |  Major | ha | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-4651](https://issues.apache.org/jira/browse/MAPREDUCE-4651) | Benchmarking random reads with DFSIO |  Major | benchmarks, test | Konstantin Shvachko | Konstantin Shvachko |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8623](https://issues.apache.org/jira/browse/HADOOP-8623) | hadoop jar command should respect HADOOP\_OPTS |  Minor | scripts | Steven Willis | Steven Willis |
| [HADOOP-8183](https://issues.apache.org/jira/browse/HADOOP-8183) | Stop using "mapred.used.genericoptionsparser" to avoid unnecessary warnings |  Minor | util | Harsh J | Harsh J |
| [MAPREDUCE-4645](https://issues.apache.org/jira/browse/MAPREDUCE-4645) | Providing a random seed to Slive should make the sequence of filenames completely deterministic |  Major | performance, test | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4408](https://issues.apache.org/jira/browse/MAPREDUCE-4408) | allow jobs to set a JAR that is in the distributed cached |  Major | mrv1, mrv2 | Alejandro Abdelnur | Robert Kanter |
| [MAPREDUCE-2786](https://issues.apache.org/jira/browse/MAPREDUCE-2786) | TestDFSIO should also test compression reading/writing from command-line. |  Minor | benchmarks | Plamen Jeliazkov | Plamen Jeliazkov |
| [YARN-137](https://issues.apache.org/jira/browse/YARN-137) | Change the default scheduler to the CapacityScheduler |  Major | scheduler | Siddharth Seth | Siddharth Seth |
| [YARN-57](https://issues.apache.org/jira/browse/YARN-57) | Plugable process tree |  Major | nodemanager | Radim Kolar | Radim Kolar |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8843](https://issues.apache.org/jira/browse/HADOOP-8843) | Old trash directories are never deleted on upgrade from 1.x |  Critical | . | Robert Joseph Evans | Jason Lowe |
| [HADOOP-8822](https://issues.apache.org/jira/browse/HADOOP-8822) | relnotes.py was deleted post mavenization |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8684](https://issues.apache.org/jira/browse/HADOOP-8684) | Deadlock between WritableComparator and WritableComparable |  Minor | io | Hiroshi Ikeda | Jing Zhao |
| [HADOOP-8310](https://issues.apache.org/jira/browse/HADOOP-8310) | FileContext#checkPath should handle URIs with no port |  Major | fs | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3922](https://issues.apache.org/jira/browse/HDFS-3922) | 0.22 and 0.23 namenode throws away blocks under construction on restart |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-3860](https://issues.apache.org/jira/browse/HDFS-3860) | HeartbeatManager#Monitor may wrongly hold the writelock of namesystem |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-3831](https://issues.apache.org/jira/browse/HDFS-3831) | Failure to renew tokens due to test-sources left in classpath |  Critical | security | Jason Lowe | Jason Lowe |
| [HDFS-3731](https://issues.apache.org/jira/browse/HDFS-3731) | 2.0 release upgrade must handle blocks being written from 1.0 |  Blocker | datanode | Suresh Srinivas | Kihwal Lee |
| [HDFS-3626](https://issues.apache.org/jira/browse/HDFS-3626) | Creating file with invalid path can corrupt edit log |  Blocker | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3553](https://issues.apache.org/jira/browse/HDFS-3553) | Hftp proxy tokens are broken |  Blocker | . | Daryn Sharp | Daryn Sharp |
| [HDFS-3373](https://issues.apache.org/jira/browse/HDFS-3373) | FileContext HDFS implementation can leak socket caches |  Major | hdfs-client | Todd Lipcon | John George |
| [MAPREDUCE-4691](https://issues.apache.org/jira/browse/MAPREDUCE-4691) | Historyserver can report "Unknown job" after RM says job has completed |  Critical | jobhistoryserver, mrv2 | Jason Lowe | Robert Joseph Evans |
| [MAPREDUCE-4689](https://issues.apache.org/jira/browse/MAPREDUCE-4689) | JobClient.getMapTaskReports on failed job results in NPE |  Major | client | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4647](https://issues.apache.org/jira/browse/MAPREDUCE-4647) | We should only unjar jobjar if there is a lib directory in it. |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4646](https://issues.apache.org/jira/browse/MAPREDUCE-4646) | client does not receive job diagnostics for failed jobs |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4193](https://issues.apache.org/jira/browse/MAPREDUCE-4193) | broken doc link for yarn-default.xml in site.xml |  Major | documentation | Patrick Hunt | Patrick Hunt |
| [YARN-138](https://issues.apache.org/jira/browse/YARN-138) | Improve default config values for YARN |  Major | resourcemanager, scheduler | Arun C Murthy | Harsh J |
| [YARN-108](https://issues.apache.org/jira/browse/YARN-108) | FSDownload can create cache directories with the wrong permissions |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-106](https://issues.apache.org/jira/browse/YARN-106) | Nodemanager needs to set permissions of local directories |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-93](https://issues.apache.org/jira/browse/YARN-93) | Diagnostics missing from applications that have finished but failed |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-88](https://issues.apache.org/jira/browse/YARN-88) | DefaultContainerExecutor can fail to set proper permissions |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-75](https://issues.apache.org/jira/browse/YARN-75) | RMContainer should handle a RELEASE event while RUNNING |  Major | . | Siddharth Seth | Siddharth Seth |
| [YARN-42](https://issues.apache.org/jira/browse/YARN-42) | Node Manager throws NPE on startup |  Major | nodemanager | Devaraj K | Devaraj K |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


