
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

## Release 2.6.6 - Unreleased (as of 2017-08-28)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13812](https://issues.apache.org/jira/browse/HADOOP-13812) | Upgrade Tomcat to 6.0.48 |  Blocker | kms | John Zhuge | John Zhuge |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4328](https://issues.apache.org/jira/browse/YARN-4328) | Findbugs warning in resourcemanager in branch-2.7 and branch-2.6 |  Minor | resourcemanager | Varun Saxena | Akira Ajisaka |
| [HDFS-11180](https://issues.apache.org/jira/browse/HDFS-11180) | Intermittent deadlock in NameNode when failover happens. |  Blocker | namenode | Abhishek Modi | Akira Ajisaka |
| [HDFS-11352](https://issues.apache.org/jira/browse/HDFS-11352) | Potential deadlock in NN when failing over |  Critical | namenode | Erik Krogen | Erik Krogen |
| [HADOOP-13433](https://issues.apache.org/jira/browse/HADOOP-13433) | Race in UGI.reloginFromKeytab |  Major | security | Duo Zhang | Duo Zhang |
| [YARN-6056](https://issues.apache.org/jira/browse/YARN-6056) | Yarn NM using LCE shows a failure when trying to delete a non-existing dir |  Major | yarn | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-6615](https://issues.apache.org/jira/browse/YARN-6615) | AmIpFilter drops query parameters on redirect |  Major | . | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-14474](https://issues.apache.org/jira/browse/HADOOP-14474) | Use OpenJDK 7 instead of Oracle JDK 7 to avoid oracle-java7-installer failures |  Major | build | Akira Ajisaka | Akira Ajisaka |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-11290](https://issues.apache.org/jira/browse/HDFS-11290) | TestFSNameSystemMBean should wait until JMX cache is cleared |  Major | test | Akira Ajisaka | Erik Krogen |
