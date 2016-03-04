
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

## Release 1.1.2 - 2013-02-15

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8567](https://issues.apache.org/jira/browse/HADOOP-8567) | Port conf servlet to dump running configuration  to branch 1.x |  Major | conf | Junping Du | Jing Zhao |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9111](https://issues.apache.org/jira/browse/HADOOP-9111) | Fix failed testcases with @ignore annotation In branch-1 |  Minor | test | Jing Zhao | Jing Zhao |
| [HADOOP-8561](https://issues.apache.org/jira/browse/HADOOP-8561) | Introduce HADOOP\_PROXY\_USER for secure impersonation in child hadoop client processes |  Major | security | Luke Lu | Yu Gao |
| [HDFS-4252](https://issues.apache.org/jira/browse/HDFS-4252) | Improve confusing log message that prints exception when editlog read is completed |  Major | namenode | Suresh Srinivas | Jing Zhao |
| [MAPREDUCE-4397](https://issues.apache.org/jira/browse/MAPREDUCE-4397) | Introduce HADOOP\_SECURITY\_CONF\_DIR for task-controller |  Major | task-controller | Luke Lu | Yu Gao |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9115](https://issues.apache.org/jira/browse/HADOOP-9115) | Deadlock in configuration when writing configuration to hdfs |  Blocker | . | Arpit Gupta | Jing Zhao |
| [HADOOP-8880](https://issues.apache.org/jira/browse/HADOOP-8880) | Missing jersey jars as dependency in the pom causes hive tests to fail |  Major | . | Giridharan Kesavan | Giridharan Kesavan |
| [HADOOP-8419](https://issues.apache.org/jira/browse/HADOOP-8419) | GzipCodec NPE upon reset with IBM JDK |  Major | io | Luke Lu | Yu Li |
| [HADOOP-8418](https://issues.apache.org/jira/browse/HADOOP-8418) | Fix UGI for IBM JDK running on Windows |  Major | security | Luke Lu | Yu Gao |
| [HDFS-5996](https://issues.apache.org/jira/browse/HDFS-5996) | hadoop 1.1.2.  hdfs  write bug |  Major | fuse-dfs | WangMeng |  |
| [HDFS-4423](https://issues.apache.org/jira/browse/HDFS-4423) | Checkpoint exception causes fatal damage to fsimage. |  Blocker | namenode | ChenFolin | Chris Nauroth |
| [HDFS-4208](https://issues.apache.org/jira/browse/HDFS-4208) | NameNode could be stuck in SafeMode due to never-created blocks |  Critical | namenode | Brandon Li | Brandon Li |
| [HDFS-3727](https://issues.apache.org/jira/browse/HDFS-3727) | When using SPNEGO, NN should not try to log in using KSSL principal |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-4888](https://issues.apache.org/jira/browse/MAPREDUCE-4888) | NLineInputFormat drops data in 1.1 and beyond |  Blocker | mrv1 | Robert Joseph Evans | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4859](https://issues.apache.org/jira/browse/MAPREDUCE-4859) | TestRecoveryManager fails on branch-1 |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4858](https://issues.apache.org/jira/browse/MAPREDUCE-4858) | TestWebUIAuthorization fails on branch-1 |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4798](https://issues.apache.org/jira/browse/MAPREDUCE-4798) | TestJobHistoryServer fails some times with 'java.lang.AssertionError: Address already in use' |  Minor | jobhistoryserver, test | sam liu | sam liu |
| [MAPREDUCE-4699](https://issues.apache.org/jira/browse/MAPREDUCE-4699) | TestFairScheduler & TestCapacityScheduler fails due to JobHistory exception |  Minor | . | Gopal V | Gopal V |
| [MAPREDUCE-4697](https://issues.apache.org/jira/browse/MAPREDUCE-4697) | TestMapredHeartbeat fails assertion on HeartbeatInterval |  Minor | . | Gopal V | Gopal V |
| [MAPREDUCE-4696](https://issues.apache.org/jira/browse/MAPREDUCE-4696) | TestMRServerPorts throws NullReferenceException |  Minor | . | Gopal V | Gopal V |
| [MAPREDUCE-4478](https://issues.apache.org/jira/browse/MAPREDUCE-4478) | TaskTracker's heartbeat is out of control |  Major | . | Liyin Liang | Liyin Liang |
| [MAPREDUCE-4396](https://issues.apache.org/jira/browse/MAPREDUCE-4396) | Make LocalJobRunner work with private distributed cache |  Minor | client | Luke Lu | Yu Gao |
| [MAPREDUCE-4272](https://issues.apache.org/jira/browse/MAPREDUCE-4272) | SortedRanges.Range#compareTo is not spec compliant |  Major | task | Luke Lu | Yu Gao |
| [MAPREDUCE-2374](https://issues.apache.org/jira/browse/MAPREDUCE-2374) | "Text File Busy" errors launching MR tasks |  Major | . | Todd Lipcon | Andy Isaacson |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9051](https://issues.apache.org/jira/browse/HADOOP-9051) | “ant test” will build failed for  trying to delete a file |  Minor | test | meng gong | Luke Lu |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


