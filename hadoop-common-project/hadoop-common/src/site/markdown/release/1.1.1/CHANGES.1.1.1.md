
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

## Release 1.1.1 - 2012-11-27

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-528](https://issues.apache.org/jira/browse/HDFS-528) | Add ability for safemode to wait for a minimum number of live datanodes |  Major | scripts | Todd Lipcon | Todd Lipcon |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8823](https://issues.apache.org/jira/browse/HADOOP-8823) | ant package target should not depend on cn-docs |  Major | build | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4174](https://issues.apache.org/jira/browse/HDFS-4174) | Backport HDFS-1031 to branch-1: to list a few of the corrupted files in WebUI |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-1539](https://issues.apache.org/jira/browse/HDFS-1539) | prevent data loss when a cluster suffers a power loss |  Major | datanode, hdfs-client, namenode | dhruba borthakur | dhruba borthakur |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9017](https://issues.apache.org/jira/browse/HADOOP-9017) | fix hadoop-client-pom-template.xml and hadoop-client-pom-template.xml for version |  Major | build | Giridharan Kesavan | Giridharan Kesavan |
| [HADOOP-8995](https://issues.apache.org/jira/browse/HADOOP-8995) | Remove unnecessary bogus exception log from Configuration |  Minor | . | Jing Zhao | Jing Zhao |
| [HADOOP-8882](https://issues.apache.org/jira/browse/HADOOP-8882) | uppercase namenode host name causes fsck to fail when useKsslAuth is on |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-8878](https://issues.apache.org/jira/browse/HADOOP-8878) | uppercase namenode hostname causes hadoop dfs calls with webhdfs filesystem and fsck to fail when security is on |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-8745](https://issues.apache.org/jira/browse/HADOOP-8745) | Incorrect version numbers in hadoop-core POM |  Minor | . | Matthias Friedrich | Matthias Friedrich |
| [HDFS-4161](https://issues.apache.org/jira/browse/HDFS-4161) | HDFS keeps a thread open for every file writer |  Major | hdfs-client | Suresh Srinivas | Tsz Wo Nicholas Sze |
| [HDFS-4134](https://issues.apache.org/jira/browse/HDFS-4134) | hadoop namenode & datanode entry points should return negative exit code on bad arguments |  Minor | namenode | Steve Loughran |  |
| [HDFS-4105](https://issues.apache.org/jira/browse/HDFS-4105) | the SPNEGO user for secondary namenode should use the web keytab |  Major | . | Arpit Gupta | Arpit Gupta |
| [HDFS-3846](https://issues.apache.org/jira/browse/HDFS-3846) | Namenode deadlock in branch-1 |  Major | namenode | Tsz Wo Nicholas Sze | Brandon Li |
| [HDFS-3791](https://issues.apache.org/jira/browse/HDFS-3791) | Backport HDFS-173 to Branch-1 :  Recursively deleting a directory with millions of files makes NameNode unresponsive for other commands until the deletion completes |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-3658](https://issues.apache.org/jira/browse/HDFS-3658) | TestDFSClientRetries#testNamenodeRestart failed |  Major | . | Eli Collins | Tsz Wo Nicholas Sze |
| [HDFS-2815](https://issues.apache.org/jira/browse/HDFS-2815) | Namenode is not coming out of safemode when we perform ( NN crash + restart ) .  Also FSCK report shows blocks missed. |  Critical | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-4792](https://issues.apache.org/jira/browse/MAPREDUCE-4792) | Unit Test TestJobTrackerRestartWithLostTracker fails with ant-1.8.4 |  Major | test | Amir Sanjar | Amir Sanjar |
| [MAPREDUCE-4782](https://issues.apache.org/jira/browse/MAPREDUCE-4782) | NLineInputFormat skips first line of last InputSplit |  Blocker | client | Mark Fuhs | Mark Fuhs |
| [MAPREDUCE-4749](https://issues.apache.org/jira/browse/MAPREDUCE-4749) | Killing multiple attempts of a task taker longer as more attempts are killed |  Major | . | Arpit Gupta | Arpit Gupta |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-1108](https://issues.apache.org/jira/browse/HDFS-1108) | Log newly allocated blocks |  Major | ha, namenode | dhruba borthakur | Todd Lipcon |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


