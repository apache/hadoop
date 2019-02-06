
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

## Release 1.0.0 - 2011-12-27

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-617](https://issues.apache.org/jira/browse/HDFS-617) | Support for non-recursive create() in HDFS |  Major | hdfs-client, namenode | Kan Zhang | Kan Zhang |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2137](https://issues.apache.org/jira/browse/HDFS-2137) | Datanode Disk Fail Inplace |  Major | datanode | Bharath Mundlapudi |  |
| [HDFS-2316](https://issues.apache.org/jira/browse/HDFS-2316) | [umbrella] WebHDFS: a complete FileSystem implementation for accessing HDFS over HTTP |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5124](https://issues.apache.org/jira/browse/HADOOP-5124) | A few optimizations to FsNamesystem#RecentInvalidateSets |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-7710](https://issues.apache.org/jira/browse/HADOOP-7710) | create a script to setup application in order to create root directories for application such hbase, hcat, hive etc |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-7664](https://issues.apache.org/jira/browse/HADOOP-7664) | o.a.h.conf.Configuration complains of overriding final parameter even if the value with which its attempting to override is the same. |  Minor | conf | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3169](https://issues.apache.org/jira/browse/MAPREDUCE-3169) | Create a new MiniMRCluster equivalent which only provides client APIs cross MR1 and MR2 |  Major | mrv1, mrv2, test | Todd Lipcon | Ahmed Radwan |
| [HDFS-2246](https://issues.apache.org/jira/browse/HDFS-2246) | Shortcut a local client reads to a Datanodes files directly |  Major | . | Sanjay Radia | Jitendra Nath Pandey |
| [HADOOP-7804](https://issues.apache.org/jira/browse/HADOOP-7804) | enable hadoop config generator to set dfs.block.local-path-access.user to enable short circuit read |  Major | conf | Arpit Gupta | Arpit Gupta |
| [HDFS-2604](https://issues.apache.org/jira/browse/HDFS-2604) | Add a log message to show if WebHDFS is enabled |  Minor | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-6886](https://issues.apache.org/jira/browse/HADOOP-6886) | LocalFileSystem Needs createNonRecursive API |  Minor | fs | Nicolas Spiegelberg | Nicolas Spiegelberg |
| [HADOOP-6840](https://issues.apache.org/jira/browse/HADOOP-6840) | Support non-recursive create() in FileSystem & SequenceFile.Writer |  Minor | fs, io | Nicolas Spiegelberg | Nicolas Spiegelberg |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-611](https://issues.apache.org/jira/browse/HDFS-611) | Heartbeats times from Datanodes increase when there are plenty of blocks to delete |  Major | datanode | dhruba borthakur | Zheng Shao |
| [HDFS-1257](https://issues.apache.org/jira/browse/HDFS-1257) | Race condition on FSNamesystem#recentInvalidateSets introduced by HADOOP-5124 |  Major | namenode | Ramkumar Vadali | Eric Payne |
| [HADOOP-7728](https://issues.apache.org/jira/browse/HADOOP-7728) | hadoop-setup-conf.sh should be modified to enable task memory manager |  Major | conf | Ramya Sunil | Ramya Sunil |
| [HADOOP-7765](https://issues.apache.org/jira/browse/HADOOP-7765) | Debian package contain both system and tar ball layout |  Major | build | Eric Yang | Eric Yang |
| [HDFS-2450](https://issues.apache.org/jira/browse/HDFS-2450) | Only complete hostname is supported to access data via hdfs:// |  Major | . | Rajit Saha | Daryn Sharp |
| [HADOOP-7740](https://issues.apache.org/jira/browse/HADOOP-7740) | security audit logger is not on by default, fix the log4j properties to enable the logger |  Minor | conf | Arpit Gupta | Arpit Gupta |
| [HDFS-2065](https://issues.apache.org/jira/browse/HDFS-2065) | Fix NPE in DFSClient.getFileChecksum |  Major | . | Bharath Mundlapudi | Uma Maheswara Rao G |
| [HADOOP-7784](https://issues.apache.org/jira/browse/HADOOP-7784) | secure datanodes fail to come up stating jsvc not found |  Major | . | Arpit Gupta | Eric Yang |
| [HDFS-1943](https://issues.apache.org/jira/browse/HDFS-1943) | fail to start datanode while start-dfs.sh is executed by root user |  Blocker | scripts | Wei Yongjun | Matt Foley |
| [MAPREDUCE-3374](https://issues.apache.org/jira/browse/MAPREDUCE-3374) | src/c++/task-controller/configure is not set executable in the tarball and that prevents task-controller from rebuilding |  Major | task-controller | Roman Shaposhnik |  |
| [HDFS-2346](https://issues.apache.org/jira/browse/HDFS-2346) | TestHost2NodesMap & TestReplicasMap will fail depending upon execution order of test methods |  Blocker | test | Uma Maheswara Rao G | Laxman |
| [HADOOP-7816](https://issues.apache.org/jira/browse/HADOOP-7816) | Allow HADOOP\_HOME deprecated warning suppression based on config specified in hadoop-env.sh |  Major | . | Dave Thompson | Dave Thompson |
| [HDFS-2590](https://issues.apache.org/jira/browse/HDFS-2590) | Some links in WebHDFS forrest doc do not work |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7815](https://issues.apache.org/jira/browse/HADOOP-7815) | Map memory mb is being incorrectly set by hadoop-setup-conf.sh |  Minor | conf | Ramya Sunil | Ramya Sunil |
| [MAPREDUCE-3480](https://issues.apache.org/jira/browse/MAPREDUCE-3480) | TestJvmReuse fails in 1.0 |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7865](https://issues.apache.org/jira/browse/HADOOP-7865) | Test Failures in 1.0.0 hdfs/common |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7854](https://issues.apache.org/jira/browse/HADOOP-7854) | UGI getCurrentUser is not synchronized |  Critical | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-7869](https://issues.apache.org/jira/browse/HADOOP-7869) | HADOOP\_HOME warning happens all of the time |  Critical | scripts | Owen O'Malley | Owen O'Malley |
| [HADOOP-7461](https://issues.apache.org/jira/browse/HADOOP-7461) | Jackson Dependency Not Declared in Hadoop POM |  Major | build | Ron Bodkin | Giridharan Kesavan |
| [HDFS-2589](https://issues.apache.org/jira/browse/HDFS-2589) | unnecessary hftp token fetch and renewal thread |  Major | security | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3319](https://issues.apache.org/jira/browse/MAPREDUCE-3319) | multifilewc from hadoop examples seems to be broken in 0.20.205.0 |  Blocker | examples | Roman Shaposhnik | Subroto Sanyal |
| [HDFS-2673](https://issues.apache.org/jira/browse/HDFS-2673) | While Namenode processing the blocksBeingWrittenReport, it will log incorrect number blocks count |  Trivial | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7903](https://issues.apache.org/jira/browse/HADOOP-7903) | hadoop artifacts do not contain 64 bit libhdfs native lib |  Major | build | Arpit Gupta | Matt Foley |
| [MAPREDUCE-1744](https://issues.apache.org/jira/browse/MAPREDUCE-1744) | DistributedCache creates its own FileSytem instance when adding a file/archive to the path |  Major | . | Dick King | Dick King |
| [HADOOP-7116](https://issues.apache.org/jira/browse/HADOOP-7116) | raise contrib junit test jvm memory size to 512mb |  Major | test | Owen O'Malley | Owen O'Malley |
| [HADOOP-8067](https://issues.apache.org/jira/browse/HADOOP-8067) | Errors building hadoop-gpl-compression with Hadoop 1.0.0 and HBase 0.92.0 |  Major | build | John Lewis |  |
| [HDFS-838](https://issues.apache.org/jira/browse/HDFS-838) | libhdfs causes a segfault due to race condition |  Major | libhdfs | Brian Bockelman |  |
| [HADOOP-9128](https://issues.apache.org/jira/browse/HADOOP-9128) | MetricsDynamicMBeanBase can cause high cpu load |  Major | metrics | Nate Putnam |  |
| [HADOOP-8389](https://issues.apache.org/jira/browse/HADOOP-8389) | MetricsDynamicMBeanBase throws IllegalArgumentException for empty attribute list |  Major | metrics | Elias Ross |  |
| [MAPREDUCE-4935](https://issues.apache.org/jira/browse/MAPREDUCE-4935) | Support timeout limitation to MRv1 job end notifications |  Major | jobtracker, mrv1 | Olga Shen | Brad Liu |
| [MAPREDUCE-3475](https://issues.apache.org/jira/browse/MAPREDUCE-3475) | JT can't renew its own tokens |  Major | jobtracker | Daryn Sharp | Daryn Sharp |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2441](https://issues.apache.org/jira/browse/HDFS-2441) | webhdfs returns two content-type headers |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2428](https://issues.apache.org/jira/browse/HDFS-2428) | webhdfs api parameter validation should be better |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2424](https://issues.apache.org/jira/browse/HDFS-2424) | webhdfs liststatus json does not convert to a valid xml document |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2439](https://issues.apache.org/jira/browse/HDFS-2439) | webhdfs open an invalid path leads to a 500 which states a npe, we should return a 404 with appropriate error message |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2453](https://issues.apache.org/jira/browse/HDFS-2453) | tail using a webhdfs uri throws an error |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2427](https://issues.apache.org/jira/browse/HDFS-2427) | webhdfs mkdirs api call creates path with 777 permission, we should default it to 755 |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2501](https://issues.apache.org/jira/browse/HDFS-2501) | add version prefix and root methods to webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2494](https://issues.apache.org/jira/browse/HDFS-2494) | [webhdfs] When Getting the file using OP=OPEN with DN http address, ESTABLISHED sockets are growing. |  Major | webhdfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-2432](https://issues.apache.org/jira/browse/HDFS-2432) | webhdfs setreplication api should return a 403 when called on a directory |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2416](https://issues.apache.org/jira/browse/HDFS-2416) | distcp with a webhdfs uri on a secure cluster fails |  Major | webhdfs | Arpit Gupta | Jitendra Nath Pandey |
| [HDFS-2527](https://issues.apache.org/jira/browse/HDFS-2527) | Remove the use of Range header from webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2528](https://issues.apache.org/jira/browse/HDFS-2528) | webhdfs rest call to a secure dn fails when a token is sent |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2540](https://issues.apache.org/jira/browse/HDFS-2540) | Change WebHdfsFileSystem to two-step create/append |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2539](https://issues.apache.org/jira/browse/HDFS-2539) | Support doAs and GETHOMEDIRECTORY in webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2552](https://issues.apache.org/jira/browse/HDFS-2552) | Add WebHdfs Forrest doc |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7923](https://issues.apache.org/jira/browse/HADOOP-7923) | Automatically update doc versions |  Major | build, documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |


