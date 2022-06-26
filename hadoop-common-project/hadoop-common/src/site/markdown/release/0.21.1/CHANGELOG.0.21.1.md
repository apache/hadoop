
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

## Release 0.21.1 - Unreleased (as of 2016-03-04)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-1905](https://issues.apache.org/jira/browse/MAPREDUCE-1905) | Context.setStatus() and progress() api are ignored |  Blocker | task | Amareshwari Sriramadasu | Amareshwari Sriramadasu |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-2040](https://issues.apache.org/jira/browse/MAPREDUCE-2040) | Forrest Documentation for Dynamic Priority Scheduler |  Minor | contrib/dynamic-scheduler | Thomas Sandholm | Thomas Sandholm |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7193](https://issues.apache.org/jira/browse/HADOOP-7193) | Help message is wrong for touchz command. |  Minor | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7177](https://issues.apache.org/jira/browse/HADOOP-7177) | CodecPool should report which compressor it is using |  Trivial | native | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-7117](https://issues.apache.org/jira/browse/HADOOP-7117) | Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml |  Major | conf | Patrick Angeles | Harsh J |
| [HADOOP-6786](https://issues.apache.org/jira/browse/HADOOP-6786) | test-patch needs to verify Herriot integrity |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1596](https://issues.apache.org/jira/browse/HDFS-1596) | Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml |  Major | documentation, namenode | Patrick Angeles | Harsh J |
| [HDFS-1343](https://issues.apache.org/jira/browse/HDFS-1343) | Instrumented build should be concentrated in one build area |  Minor | build | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2140](https://issues.apache.org/jira/browse/MAPREDUCE-2140) | Re-generate fair scheduler design doc PDF |  Trivial | . | Matei Zaharia | Matei Zaharia |
| [MAPREDUCE-1501](https://issues.apache.org/jira/browse/MAPREDUCE-1501) | FileInputFormat to support multi-level/recursive directory listing |  Major | . | Zheng Shao | Zheng Shao |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7215](https://issues.apache.org/jira/browse/HADOOP-7215) | RPC clients must connect over a network interface corresponding to the host name in the client's kerberos principal key |  Blocker | security | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7194](https://issues.apache.org/jira/browse/HADOOP-7194) | Potential Resource leak in IOUtils.java |  Major | io | Devaraj K | Devaraj K |
| [HADOOP-7183](https://issues.apache.org/jira/browse/HADOOP-7183) | WritableComparator.get should not cache comparator objects |  Blocker | . | Todd Lipcon | Tom White |
| [HADOOP-7174](https://issues.apache.org/jira/browse/HADOOP-7174) | null is displayed in the console,if the src path is invalid while doing copyToLocal operation from commandLine |  Minor | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7162](https://issues.apache.org/jira/browse/HADOOP-7162) | FsShell: call srcFs.listStatus(src) twice |  Minor | fs | Alexey Diomin | Alexey Diomin |
| [HADOOP-7120](https://issues.apache.org/jira/browse/HADOOP-7120) | 200 new Findbugs warnings |  Major | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7053](https://issues.apache.org/jira/browse/HADOOP-7053) | wrong FSNamesystem Audit logging setting in conf/log4j.properties |  Minor | conf | Jingguo Yao | Jingguo Yao |
| [HADOOP-7052](https://issues.apache.org/jira/browse/HADOOP-7052) | misspelling of threshold in conf/log4j.properties |  Major | conf | Jingguo Yao | Jingguo Yao |
| [HADOOP-7019](https://issues.apache.org/jira/browse/HADOOP-7019) | Refactor build targets to enable faster cross project dev cycles. |  Major | build | Owen O'Malley | Luke Lu |
| [HADOOP-6993](https://issues.apache.org/jira/browse/HADOOP-6993) | Broken link on cluster setup page of docs |  Major | documentation | Aaron T. Myers | Eli Collins |
| [HADOOP-6971](https://issues.apache.org/jira/browse/HADOOP-6971) | Clover build doesn't generate per-test coverage |  Major | build, test | Konstantin Boudnik | Konstantin Boudnik |
| [HADOOP-6969](https://issues.apache.org/jira/browse/HADOOP-6969) | CHANGES.txt does not reflect the release of version 0.21.0. |  Major | . | Konstantin Shvachko | Tom White |
| [HADOOP-6954](https://issues.apache.org/jira/browse/HADOOP-6954) | Sources JARs are not correctly published to the Maven repository |  Major | build | Tom White | Tom White |
| [HADOOP-6925](https://issues.apache.org/jira/browse/HADOOP-6925) | BZip2Codec incorrectly implements read() |  Critical | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-6833](https://issues.apache.org/jira/browse/HADOOP-6833) | IPC leaks call parameters when exceptions thrown |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1750](https://issues.apache.org/jira/browse/HDFS-1750) | fs -ls hftp://file not working |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1728](https://issues.apache.org/jira/browse/HDFS-1728) | SecondaryNameNode.checkpointSize is in byte but not MB. |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1665](https://issues.apache.org/jira/browse/HDFS-1665) | Balancer sleeps inadequately |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1612](https://issues.apache.org/jira/browse/HDFS-1612) | HDFS Design Documentation is outdated |  Minor | documentation | Joe Crobak | Joe Crobak |
| [HDFS-1598](https://issues.apache.org/jira/browse/HDFS-1598) | ListPathsServlet excludes .\*.crc files |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1552](https://issues.apache.org/jira/browse/HDFS-1552) | Remove java5 dependencies from build |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1548](https://issues.apache.org/jira/browse/HDFS-1548) | Fault-injection tests are executed multiple times if invoked with run-test-hdfs-fault-inject target |  Major | build, test | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1474](https://issues.apache.org/jira/browse/HDFS-1474) | ant binary-system is broken |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1452](https://issues.apache.org/jira/browse/HDFS-1452) | ant compile-contrib is broken |  Major | contrib/hdfsproxy | Jakob Homan | Konstantin Boudnik |
| [HDFS-1444](https://issues.apache.org/jira/browse/HDFS-1444) | Test related code of build.xml is error-prone and needs to be re-aligned. |  Minor | build | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1420](https://issues.apache.org/jira/browse/HDFS-1420) | Clover build doesn't generate per-test coverage |  Major | build, test | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1416](https://issues.apache.org/jira/browse/HDFS-1416) | CHANGES.txt does not reflect the release version 0.21.0. |  Major | . | Konstantin Shvachko | Tom White |
| [HDFS-1413](https://issues.apache.org/jira/browse/HDFS-1413) | Broken links to HDFS Wiki in hdfs site and documentation. |  Major | documentation | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-1411](https://issues.apache.org/jira/browse/HDFS-1411) | The startup command of the Backup Node is "bin/hdfs namenode -backup" |  Minor | documentation | Ching-Shen Chen | Ching-Shen Chen |
| [HDFS-1377](https://issues.apache.org/jira/browse/HDFS-1377) | Quota bug for partial blocks allows quotas to be violated |  Blocker | namenode | Eli Collins | Eli Collins |
| [HDFS-1363](https://issues.apache.org/jira/browse/HDFS-1363) | startFileInternal should return the last block of the file opened for append as an under-construction block |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-1206](https://issues.apache.org/jira/browse/HDFS-1206) | TestFiHFlush fails intermittently |  Major | test | Tsz Wo Nicholas Sze | Konstantin Boudnik |
| [HDFS-1189](https://issues.apache.org/jira/browse/HDFS-1189) | Quota counts missed between clear quota and set quota |  Major | namenode | Kang Xiao | John George |
| [HDFS-996](https://issues.apache.org/jira/browse/HDFS-996) | JUnit tests should never depend on anything in conf |  Blocker | test | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2317](https://issues.apache.org/jira/browse/MAPREDUCE-2317) | HadoopArchives throwing NullPointerException while creating hadoop archives (.har files) |  Minor | harchive | Devaraj K | Devaraj K |
| [MAPREDUCE-2228](https://issues.apache.org/jira/browse/MAPREDUCE-2228) | Remove java5 dependencies from build |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2223](https://issues.apache.org/jira/browse/MAPREDUCE-2223) | TestMRCLI might fail on Ubuntu with default /etc/hosts |  Minor | test | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2134](https://issues.apache.org/jira/browse/MAPREDUCE-2134) | ant binary-system is broken in mapreduce project. |  Major | build | Vinay Kumar Thota | Konstantin Boudnik |
| [MAPREDUCE-2090](https://issues.apache.org/jira/browse/MAPREDUCE-2090) | Clover build doesn't generate per-test coverage |  Major | build, test | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2086](https://issues.apache.org/jira/browse/MAPREDUCE-2086) | CHANGES.txt does not reflect the release of version 0.21.0. |  Major | . | Konstantin Shvachko | Tom White |
| [MAPREDUCE-2032](https://issues.apache.org/jira/browse/MAPREDUCE-2032) | TestJobOutputCommitter fails in ant test run |  Major | task | Amareshwari Sriramadasu | Dick King |
| [MAPREDUCE-1984](https://issues.apache.org/jira/browse/MAPREDUCE-1984) | herriot TestCluster fails because exclusion is not there |  Major | . | Balaji Rajagopalan | Balaji Rajagopalan |
| [MAPREDUCE-1929](https://issues.apache.org/jira/browse/MAPREDUCE-1929) | Allow artifacts to be published to the staging Apache Nexus Maven Repository |  Blocker | build | Tom White | Tom White |
| [MAPREDUCE-1897](https://issues.apache.org/jira/browse/MAPREDUCE-1897) | trunk build broken on compile-mapred-test |  Major | test | Greg Roelofs | Konstantin Boudnik |
| [MAPREDUCE-1280](https://issues.apache.org/jira/browse/MAPREDUCE-1280) | Eclipse Plugin does not work with Eclipse Ganymede (3.4) |  Major | . | Aaron Kimball | Alex Kozlov |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6934](https://issues.apache.org/jira/browse/HADOOP-6934) | test for ByteWritable comparator |  Major | record | Johannes Zillmann | Johannes Zillmann |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7072](https://issues.apache.org/jira/browse/HADOOP-7072) | Remove java5 dependencies from build |  Major | build | Konstantin Boudnik | Konstantin Boudnik |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6944](https://issues.apache.org/jira/browse/HADOOP-6944) | [Herriot] Implement a functionality for getting proxy users definitions like groups and hosts. |  Major | test | Vinay Kumar Thota | Vinay Kumar Thota |
| [MAPREDUCE-1809](https://issues.apache.org/jira/browse/MAPREDUCE-1809) | Ant build changes for Streaming system tests in contrib projects. |  Major | build | Vinay Kumar Thota | Vinay Kumar Thota |


