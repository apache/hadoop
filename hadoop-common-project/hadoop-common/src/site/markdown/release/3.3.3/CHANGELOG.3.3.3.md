
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

## Release 3.3.3 - 2022-05-09



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-7373](https://issues.apache.org/jira/browse/MAPREDUCE-7373) | Building MapReduce NativeTask fails on Fedora 34+ |  Major | build, nativetask | Kengo Seki | Kengo Seki |
| [HDFS-16355](https://issues.apache.org/jira/browse/HDFS-16355) | Improve the description of dfs.block.scanner.volume.bytes.per.second |  Minor | documentation, hdfs | guophilipse | guophilipse |
| [HADOOP-18155](https://issues.apache.org/jira/browse/HADOOP-18155) | Refactor tests in TestFileUtil |  Trivial | common | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18088](https://issues.apache.org/jira/browse/HADOOP-18088) | Replace log4j 1.x with reload4j |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16501](https://issues.apache.org/jira/browse/HDFS-16501) | Print the exception when reporting a bad block |  Major | datanode | qinyuren | qinyuren |
| [HADOOP-18214](https://issues.apache.org/jira/browse/HADOOP-18214) | Update BUILDING.txt |  Minor | build, documentation | Steve Loughran |  |
| [HDFS-16556](https://issues.apache.org/jira/browse/HDFS-16556) | Fix typos in distcp |  Minor | documentation | guophilipse | guophilipse |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-17341](https://issues.apache.org/jira/browse/HADOOP-17341) | Upgrade commons-codec to 1.15 |  Minor | . | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-17650](https://issues.apache.org/jira/browse/HADOOP-17650) | Fails to build using Maven 3.8.1 |  Major | build | Wei-Chiu Chuang | Viraj Jasani |
| [HADOOP-18178](https://issues.apache.org/jira/browse/HADOOP-18178) | Upgrade jackson to 2.13.2 and jackson-databind to 2.13.2.2 |  Major | . | PJ Fanning | PJ Fanning |
| [HDFS-16535](https://issues.apache.org/jira/browse/HDFS-16535) | SlotReleaser should reuse the domain socket based on socket paths |  Major | hdfs-client | Quanlong Huang |  |
| [HADOOP-18109](https://issues.apache.org/jira/browse/HADOOP-18109) | Ensure that default permissions of directories under internal ViewFS directories are the same as directories on target filesystems |  Major | viewfs | Chentao Yu | Chentao Yu |
| [HDFS-16422](https://issues.apache.org/jira/browse/HDFS-16422) | Fix thread safety of EC decoding during concurrent preads |  Critical | dfsclient, ec, erasure-coding | daimin | daimin |
| [HDFS-16437](https://issues.apache.org/jira/browse/HDFS-16437) | ReverseXML processor doesn't accept XML files without the SnapshotDiffSection. |  Critical | hdfs | yanbin.zhang | yanbin.zhang |
| [HDFS-16507](https://issues.apache.org/jira/browse/HDFS-16507) | [SBN read] Avoid purging edit log which is in progress |  Critical | . | Tao Li | Tao Li |
| [YARN-10720](https://issues.apache.org/jira/browse/YARN-10720) | YARN WebAppProxyServlet should support connection timeout to prevent proxy server from hanging |  Critical | . | Qi Zhu | Qi Zhu |
| [HDFS-16428](https://issues.apache.org/jira/browse/HDFS-16428) | Source path with storagePolicy cause wrong typeConsumed while rename |  Major | hdfs, namenode | lei w | lei w |
| [YARN-11014](https://issues.apache.org/jira/browse/YARN-11014) | YARN incorrectly validates maximum capacity resources on the validation API |  Major | . | Benjamin Teke | Benjamin Teke |
| [YARN-11075](https://issues.apache.org/jira/browse/YARN-11075) | Explicitly declare serialVersionUID in LogMutation class |  Major | . | Benjamin Teke | Benjamin Teke |
| [HDFS-11041](https://issues.apache.org/jira/browse/HDFS-11041) | Unable to unregister FsDatasetState MBean if DataNode is shutdown twice |  Trivial | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-18160](https://issues.apache.org/jira/browse/HADOOP-18160) | \`org.wildfly.openssl\` should not be shaded by Hadoop build |  Major | build | André F. | André F. |
| [HADOOP-18202](https://issues.apache.org/jira/browse/HADOOP-18202) | create-release fails fatal: unsafe repository ('/build/source' is owned by someone else) |  Major | build | Steve Loughran | Steve Loughran |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-18125](https://issues.apache.org/jira/browse/HADOOP-18125) | Utility to identify git commit / Jira fixVersion discrepancies for RC preparation |  Major | . | Viraj Jasani | Viraj Jasani |


