
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

## Release 0.23.11 - 2014-06-25

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
| [HADOOP-10454](https://issues.apache.org/jira/browse/HADOOP-10454) | Provide FileContext version of har file system |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10164](https://issues.apache.org/jira/browse/HADOOP-10164) | Allow UGI to login with a known Subject |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-7688](https://issues.apache.org/jira/browse/HADOOP-7688) | When a servlet filter throws an exception in init(..), the Jetty server failed silently. |  Major | . | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HDFS-6191](https://issues.apache.org/jira/browse/HDFS-6191) | Disable quota checks when replaying edit log. |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-5637](https://issues.apache.org/jira/browse/HDFS-5637) | try to refeatchToken while local read InvalidToken occurred |  Major | hdfs-client, security | Liang Xie | Liang Xie |
| [HDFS-4461](https://issues.apache.org/jira/browse/HDFS-4461) | DirectoryScanner: volume path prefix takes up memory for every block that is scanned |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10588](https://issues.apache.org/jira/browse/HADOOP-10588) | Workaround for jetty6 acceptor startup issue |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10332](https://issues.apache.org/jira/browse/HADOOP-10332) | HttpServer's jetty audit log always logs 200 OK |  Major | . | Daryn Sharp | Jonathan Eagles |
| [HADOOP-10146](https://issues.apache.org/jira/browse/HADOOP-10146) | Workaround JDK7 Process fd close bug |  Critical | util | Daryn Sharp | Daryn Sharp |
| [HADOOP-10129](https://issues.apache.org/jira/browse/HADOOP-10129) | Distcp may succeed when it fails |  Critical | tools/distcp | Daryn Sharp | Daryn Sharp |
| [HADOOP-10112](https://issues.apache.org/jira/browse/HADOOP-10112) | har file listing  doesn't work with wild card |  Major | tools | Brandon Li | Brandon Li |
| [HADOOP-10110](https://issues.apache.org/jira/browse/HADOOP-10110) | hadoop-auth has a build break due to missing dependency |  Blocker | build | Chuan Liu | Chuan Liu |
| [HADOOP-10081](https://issues.apache.org/jira/browse/HADOOP-10081) | Client.setupIOStreams can leak socket resources on exception or error |  Critical | ipc | Jason Lowe | Tsuyoshi Ozawa |
| [HADOOP-9230](https://issues.apache.org/jira/browse/HADOOP-9230) | TestUniformSizeInputFormat fails intermittently |  Major | test | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-8826](https://issues.apache.org/jira/browse/HADOOP-8826) | Docs still refer to 0.20.205 as stable line |  Minor | . | Robert Joseph Evans | Mit Desai |
| [HDFS-6449](https://issues.apache.org/jira/browse/HDFS-6449) | Incorrect counting in ContentSummaryComputationContext in 0.23. |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6166](https://issues.apache.org/jira/browse/HDFS-6166) | revisit balancer so\_timeout |  Blocker | balancer & mover | Nathan Roberts | Nathan Roberts |
| [HDFS-5881](https://issues.apache.org/jira/browse/HDFS-5881) | Fix skip() of the short-circuit local reader (legacy). |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-5806](https://issues.apache.org/jira/browse/HDFS-5806) | balancer should set SoTimeout to avoid indefinite hangs |  Major | balancer & mover | Nathan Roberts | Nathan Roberts |
| [HDFS-5728](https://issues.apache.org/jira/browse/HDFS-5728) | [Diskfull] Block recovery will fail if the metafile does not have crc for all chunks of the block |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-4576](https://issues.apache.org/jira/browse/HDFS-4576) | Webhdfs authentication issues |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-5789](https://issues.apache.org/jira/browse/MAPREDUCE-5789) | Average Reduce time is incorrect on Job Overview page |  Major | jobhistoryserver, webapps | Rushabh S Shah | Rushabh S Shah |
| [MAPREDUCE-5778](https://issues.apache.org/jira/browse/MAPREDUCE-5778) | JobSummary does not escape newlines in the job name |  Major | jobhistoryserver | Jason Lowe | Akira AJISAKA |
| [MAPREDUCE-5757](https://issues.apache.org/jira/browse/MAPREDUCE-5757) | ConcurrentModificationException in JobControl.toList |  Major | client | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5746](https://issues.apache.org/jira/browse/MAPREDUCE-5746) | Job diagnostics can implicate wrong task for a failed job |  Major | jobhistoryserver | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5744](https://issues.apache.org/jira/browse/MAPREDUCE-5744) | Job hangs because RMContainerAllocator$AssignedRequests.preemptReduce() violates the comparator contract |  Blocker | . | Sangjin Lee | Gera Shegalov |
| [MAPREDUCE-5689](https://issues.apache.org/jira/browse/MAPREDUCE-5689) | MRAppMaster does not preempt reducers when scheduled maps cannot be fulfilled |  Critical | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [MAPREDUCE-5623](https://issues.apache.org/jira/browse/MAPREDUCE-5623) | TestJobCleanup fails because of RejectedExecutionException and NPE. |  Major | . | Tsuyoshi Ozawa | Jason Lowe |
| [MAPREDUCE-5454](https://issues.apache.org/jira/browse/MAPREDUCE-5454) | TestDFSIO fails intermittently on JDK7 |  Major | test | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-3191](https://issues.apache.org/jira/browse/MAPREDUCE-3191) | docs for map output compression incorrectly reference SequenceFile |  Trivial | . | Todd Lipcon | Chen He |
| [YARN-3829](https://issues.apache.org/jira/browse/YARN-3829) | The History Tracking UI is broken for Tez application on ResourceManager WebUI |  Critical | applications | Irina Easterling |  |
| [YARN-1932](https://issues.apache.org/jira/browse/YARN-1932) | Javascript injection on the job status page |  Blocker | . | Mit Desai | Mit Desai |
| [YARN-1670](https://issues.apache.org/jira/browse/YARN-1670) | aggregated log writer can write more log data then it says is the log length |  Critical | . | Thomas Graves | Mit Desai |
| [YARN-1592](https://issues.apache.org/jira/browse/YARN-1592) | CapacityScheduler tries to reserve more than a node's total memory on branch-0.23 |  Major | capacityscheduler | Thomas Graves | Thomas Graves |
| [YARN-1180](https://issues.apache.org/jira/browse/YARN-1180) | Update capacity scheduler docs to include types on the configs |  Trivial | capacityscheduler | Thomas Graves | Chen He |
| [YARN-1145](https://issues.apache.org/jira/browse/YARN-1145) | Potential file handle leak in aggregated logs web ui |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-1053](https://issues.apache.org/jira/browse/YARN-1053) | Diagnostic message from ContainerExitEvent is ignored in ContainerImpl |  Blocker | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-853](https://issues.apache.org/jira/browse/YARN-853) | maximum-am-resource-percent doesn't work after refreshQueues command |  Major | capacityscheduler | Devaraj K | Devaraj K |
| [YARN-500](https://issues.apache.org/jira/browse/YARN-500) | ResourceManager webapp is using next port if configured port is already in use |  Major | resourcemanager | Nishan Shetty | Kenji Kikushima |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10148](https://issues.apache.org/jira/browse/HADOOP-10148) | backport hadoop-10107 to branch-0.23 |  Minor | ipc | Chen He | Chen He |
| [YARN-1575](https://issues.apache.org/jira/browse/YARN-1575) | Public localizer crashes with "Localized unkown resource" |  Critical | nodemanager | Jason Lowe | Jason Lowe |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


