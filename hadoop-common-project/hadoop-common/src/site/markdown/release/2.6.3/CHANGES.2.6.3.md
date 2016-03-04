
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

## Release 2.6.3 - 2015-12-17

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-12413](https://issues.apache.org/jira/browse/HADOOP-12413) | AccessControlList should avoid calling getGroupNames in isUserInList with empty groups. |  Major | security | zhihai xu | zhihai xu |
| [HDFS-9434](https://issues.apache.org/jira/browse/HDFS-9434) | Recommission a datanode with 500k blocks may pause NN for 30 seconds |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-12577](https://issues.apache.org/jira/browse/HADOOP-12577) | Bump up commons-collections version to 3.2.2 to address a security flaw |  Blocker | build, security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12526](https://issues.apache.org/jira/browse/HADOOP-12526) | [Branch-2] there are duplicate dependency definitions in pom's |  Major | build | Sangjin Lee | Sangjin Lee |
| [HADOOP-12230](https://issues.apache.org/jira/browse/HADOOP-12230) | hadoop-project declares duplicate, conflicting curator dependencies |  Minor | build | Steve Loughran | Rakesh R |
| [HADOOP-11267](https://issues.apache.org/jira/browse/HADOOP-11267) | TestSecurityUtil fails when run with JDK8 because of empty principal names |  Minor | security, test | Stephen Chu | Stephen Chu |
| [HADOOP-10134](https://issues.apache.org/jira/browse/HADOOP-10134) | [JDK8] Fix Javadoc errors caused by incorrect or illegal tags in doc comments |  Minor | . | Andrew Purtell | Andrew Purtell |
| [HADOOP-9242](https://issues.apache.org/jira/browse/HADOOP-9242) | Duplicate surefire plugin config in hadoop-common |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HDFS-9470](https://issues.apache.org/jira/browse/HDFS-9470) | Encryption zone on root not loaded from fsimage after NN restart |  Critical | . | Xiao Chen | Xiao Chen |
| [HDFS-9431](https://issues.apache.org/jira/browse/HDFS-9431) | DistributedFileSystem#concat fails if the target path is relative. |  Major | hdfs-client | Kazuho Fujii | Kazuho Fujii |
| [HDFS-9289](https://issues.apache.org/jira/browse/HDFS-9289) | Make DataStreamer#block thread safe and verify genStamp in commitBlock |  Critical | . | Chang Li | Chang Li |
| [HDFS-9273](https://issues.apache.org/jira/browse/HDFS-9273) | ACLs on root directory may be lost after NN restart |  Critical | namenode | Xiao Chen | Xiao Chen |
| [HDFS-9083](https://issues.apache.org/jira/browse/HDFS-9083) | Replication violates block placement policy. |  Blocker | namenode | Rushabh S Shah | Rushabh S Shah |
| [HDFS-8615](https://issues.apache.org/jira/browse/HDFS-8615) | Correct HTTP method in WebHDFS document |  Major | documentation | Akira AJISAKA | Brahma Reddy Battula |
| [MAPREDUCE-6549](https://issues.apache.org/jira/browse/MAPREDUCE-6549) | multibyte delimiters with LineRecordReader cause duplicate records |  Major | mrv1, mrv2 | Dustin Cote | Wilfred Spiegelenburg |
| [MAPREDUCE-6540](https://issues.apache.org/jira/browse/MAPREDUCE-6540) | TestMRTimelineEventHandling fails |  Major | test | Sangjin Lee | Sangjin Lee |
| [MAPREDUCE-6528](https://issues.apache.org/jira/browse/MAPREDUCE-6528) | Memory leak for HistoryFileManager.getJobSummary() |  Critical | jobhistoryserver | Junping Du | Junping Du |
| [MAPREDUCE-6481](https://issues.apache.org/jira/browse/MAPREDUCE-6481) | LineRecordReader may give incomplete record and wrong position/key information for uncompressed input sometimes. |  Critical | mrv2 | zhihai xu | zhihai xu |
| [MAPREDUCE-6377](https://issues.apache.org/jira/browse/MAPREDUCE-6377) | JHS sorting on state column not working in webUi |  Minor | jobhistoryserver | Bibin A Chundatt | zhihai xu |
| [MAPREDUCE-6273](https://issues.apache.org/jira/browse/MAPREDUCE-6273) | HistoryFileManager should check whether summaryFile exists to avoid FileNotFoundException causing HistoryFileInfo into MOVE\_FAILED state |  Minor | jobhistoryserver | zhihai xu | zhihai xu |
| [MAPREDUCE-5948](https://issues.apache.org/jira/browse/MAPREDUCE-5948) | org.apache.hadoop.mapred.LineRecordReader does not handle multibyte record delimiters well |  Critical | . | Kris Geusebroek | Akira AJISAKA |
| [MAPREDUCE-5883](https://issues.apache.org/jira/browse/MAPREDUCE-5883) | "Total megabyte-seconds" in job counters is slightly misleading |  Minor | . | Nathan Roberts | Nathan Roberts |
| [YARN-4434](https://issues.apache.org/jira/browse/YARN-4434) | NodeManager Disk Checker parameter documentation is not correct |  Minor | documentation, nodemanager | Takashi Ohnishi | Weiwei Yang |
| [YARN-4424](https://issues.apache.org/jira/browse/YARN-4424) | Fix deadlock in RMAppImpl |  Blocker | . | Yesha Vora | Jian He |
| [YARN-4365](https://issues.apache.org/jira/browse/YARN-4365) | FileSystemNodeLabelStore should check for root dir existence on startup |  Major | resourcemanager | Jason Lowe | Kuhu Shukla |
| [YARN-4348](https://issues.apache.org/jira/browse/YARN-4348) | ZKRMStateStore.syncInternal shouldn't wait for sync completion for avoiding blocking ZK's event thread |  Blocker | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-4344](https://issues.apache.org/jira/browse/YARN-4344) | NMs reconnecting with changed capabilities can lead to wrong cluster resource calculations |  Critical | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-4326](https://issues.apache.org/jira/browse/YARN-4326) | Fix TestDistributedShell timeout as AHS in MiniYarnCluster no longer binds to default port 8188 |  Major | . | MENG DING | MENG DING |
| [YARN-4320](https://issues.apache.org/jira/browse/YARN-4320) | TestJobHistoryEventHandler fails as AHS in MiniYarnCluster no longer binds to default port 8188 |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-4312](https://issues.apache.org/jira/browse/YARN-4312) | TestSubmitApplicationWithRMHA fails on branch-2.7 and branch-2.6 as some of the test cases time out |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-4241](https://issues.apache.org/jira/browse/YARN-4241) | Fix typo of property name in yarn-default.xml |  Major | documentation | Anthony Rojas | Anthony Rojas |
| [YARN-3925](https://issues.apache.org/jira/browse/YARN-3925) | ContainerLogsUtils#getContainerLogFile fails to read container log files from full disks. |  Critical | nodemanager | zhihai xu | zhihai xu |
| [YARN-3878](https://issues.apache.org/jira/browse/YARN-3878) | AsyncDispatcher can hang while stopping if it is configured for draining events on stop |  Critical | . | Varun Saxena | Varun Saxena |
| [YARN-2859](https://issues.apache.org/jira/browse/YARN-2859) | ApplicationHistoryServer binds to default port 8188 in MiniYARNCluster |  Critical | timelineserver | Hitesh Shah | Vinod Kumar Vavilapalli |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10668](https://issues.apache.org/jira/browse/HADOOP-10668) | TestZKFailoverControllerStress#testExpireBackAndForth occasionally fails |  Major | test | Ted Yu | Ming Ma |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
