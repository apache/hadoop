
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

## Release 2.0.4-alpha - 2013-04-25

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
| [HADOOP-9379](https://issues.apache.org/jira/browse/HADOOP-9379) | capture the ulimit info after printing the log to the console |  Trivial | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-9374](https://issues.apache.org/jira/browse/HADOOP-9374) | Add tokens from -tokenCacheFile into UGI |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-443](https://issues.apache.org/jira/browse/YARN-443) | allow OS scheduling priority of NM to be different than the containers it launches |  Major | nodemanager | Thomas Graves | Thomas Graves |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9471](https://issues.apache.org/jira/browse/HADOOP-9471) | hadoop-client wrongfully excludes jetty-util JAR, breaking webhdfs |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-9467](https://issues.apache.org/jira/browse/HADOOP-9467) | Metrics2 record filtering (.record.filter.include/exclude) does not filter by name |  Major | metrics | Chris Nauroth | Chris Nauroth |
| [HADOOP-9444](https://issues.apache.org/jira/browse/HADOOP-9444) | $var shell substitution in properties are not expanded in hadoop-policy.xml |  Blocker | conf | Konstantin Boudnik | Roman Shaposhnik |
| [HADOOP-9406](https://issues.apache.org/jira/browse/HADOOP-9406) | hadoop-client leaks dependency on JDK tools jar |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-9405](https://issues.apache.org/jira/browse/HADOOP-9405) | TestGridmixSummary#testExecutionSummarizer is broken |  Minor | test, tools | Andrew Wang | Andrew Wang |
| [HADOOP-9399](https://issues.apache.org/jira/browse/HADOOP-9399) | protoc maven plugin doesn't work on mvn 3.0.2 |  Minor | build | Todd Lipcon | Konstantin Boudnik |
| [HADOOP-9301](https://issues.apache.org/jira/browse/HADOOP-9301) | hadoop client servlet/jsp/jetty/tomcat JARs creating conflicts in Oozie & HttpFS |  Blocker | build | Roman Shaposhnik | Alejandro Abdelnur |
| [HADOOP-9299](https://issues.apache.org/jira/browse/HADOOP-9299) | kerberos name resolution is kicking in even when kerberos is not configured |  Blocker | security | Roman Shaposhnik | Daryn Sharp |
| [HDFS-4649](https://issues.apache.org/jira/browse/HDFS-4649) | Webhdfs cannot list large directories |  Blocker | namenode, security, webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-4646](https://issues.apache.org/jira/browse/HDFS-4646) | createNNProxyWithClientProtocol ignores configured timeout value |  Minor | namenode | Jagane Sundar | Jagane Sundar |
| [HDFS-4581](https://issues.apache.org/jira/browse/HDFS-4581) | DataNode#checkDiskError should not be called on network errors |  Major | datanode | Rohit Kochar | Rohit Kochar |
| [HDFS-4576](https://issues.apache.org/jira/browse/HDFS-4576) | Webhdfs authentication issues |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-4571](https://issues.apache.org/jira/browse/HDFS-4571) | WebHDFS should not set the service hostname on the server side |  Major | webhdfs | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3344](https://issues.apache.org/jira/browse/HDFS-3344) | Unreliable corrupt blocks counting in TestProcessCorruptBlocks |  Major | namenode | Tsz Wo Nicholas Sze | Kihwal Lee |
| [MAPREDUCE-5117](https://issues.apache.org/jira/browse/MAPREDUCE-5117) | With security enabled HS delegation token renewer fails |  Blocker | security | Roman Shaposhnik | Siddharth Seth |
| [MAPREDUCE-5094](https://issues.apache.org/jira/browse/MAPREDUCE-5094) | Disable mem monitoring by default in MiniMRYarnCluster |  Major | . | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-5088](https://issues.apache.org/jira/browse/MAPREDUCE-5088) | MR Client gets an renewer token exception while Oozie is submitting a job |  Blocker | . | Roman Shaposhnik | Daryn Sharp |
| [MAPREDUCE-5083](https://issues.apache.org/jira/browse/MAPREDUCE-5083) | MiniMRCluster should use a random component when creating an actual cluster |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-5053](https://issues.apache.org/jira/browse/MAPREDUCE-5053) | java.lang.InternalError from decompression codec cause reducer to fail |  Major | . | Robert Parker | Robert Parker |
| [MAPREDUCE-5023](https://issues.apache.org/jira/browse/MAPREDUCE-5023) | History Server Web Services missing Job Counters |  Critical | jobhistoryserver, webapps | Kendall Thrapp | Ravi Prakash |
| [MAPREDUCE-5006](https://issues.apache.org/jira/browse/MAPREDUCE-5006) | streaming tests failing |  Major | contrib/streaming | Alejandro Abdelnur | Sandy Ryza |
| [MAPREDUCE-4549](https://issues.apache.org/jira/browse/MAPREDUCE-4549) | Distributed cache conflicts breaks backwards compatability |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3685](https://issues.apache.org/jira/browse/MAPREDUCE-3685) | There are some bugs in implementation of MergeManager |  Critical | mrv2 | anty.rao | anty |
| [YARN-470](https://issues.apache.org/jira/browse/YARN-470) | Support a way to disable resource monitoring on the NodeManager |  Major | nodemanager | Hitesh Shah | Siddharth Seth |
| [YARN-449](https://issues.apache.org/jira/browse/YARN-449) | HBase test failures when running against Hadoop 2 |  Blocker | . | Siddharth Seth |  |
| [YARN-429](https://issues.apache.org/jira/browse/YARN-429) | capacity-scheduler config missing from yarn-test artifact |  Blocker | resourcemanager | Siddharth Seth | Siddharth Seth |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-4577](https://issues.apache.org/jira/browse/HDFS-4577) | Webhdfs operations should declare if authentication is required |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-4567](https://issues.apache.org/jira/browse/HDFS-4567) | Webhdfs does not need a token for token operations |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-4566](https://issues.apache.org/jira/browse/HDFS-4566) | Webdhfs token cancelation should use authentication |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-4560](https://issues.apache.org/jira/browse/HDFS-4560) | Webhdfs cannot use tokens obtained by another user |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-4548](https://issues.apache.org/jira/browse/HDFS-4548) | Webhdfs doesn't renegotiate SPNEGO token |  Blocker | . | Daryn Sharp | Daryn Sharp |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


