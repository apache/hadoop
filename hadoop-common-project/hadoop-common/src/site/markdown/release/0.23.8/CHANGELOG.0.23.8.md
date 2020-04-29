
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

## Release 0.23.8 - 2013-06-05



### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-4383](https://issues.apache.org/jira/browse/MAPREDUCE-4383) | HadoopPipes.cc needs to include unistd.h |  Minor | pipes | Andy Isaacson | Andy Isaacson |
| [YARN-71](https://issues.apache.org/jira/browse/YARN-71) | Ensure/confirm that the NodeManager cleans up local-dirs on restart |  Critical | nodemanager | Vinod Kumar Vavilapalli | Xuan Gong |
| [HDFS-4690](https://issues.apache.org/jira/browse/HDFS-4690) | Namenode exits if entering safemode while secret manager is edit logging |  Critical | namenode, security | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-5065](https://issues.apache.org/jira/browse/MAPREDUCE-5065) | DistCp should skip checksum comparisons if block-sizes are different on source/target. |  Major | distcp | Mithun Radhakrishnan | Mithun Radhakrishnan |
| [YARN-476](https://issues.apache.org/jira/browse/YARN-476) | ProcfsBasedProcessTree info message confuses users |  Minor | . | Jason Lowe | Sandy Ryza |
| [HDFS-4699](https://issues.apache.org/jira/browse/HDFS-4699) | TestPipelinesFailover#testPipelineRecoveryStress fails sporadically |  Major | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5147](https://issues.apache.org/jira/browse/MAPREDUCE-5147) | Maven build should create hadoop-mapreduce-client-app-VERSION.jar directly |  Major | mrv2 | Robert Parker | Robert Parker |
| [HADOOP-9469](https://issues.apache.org/jira/browse/HADOOP-9469) | mapreduce/yarn source jars not included in dist tarball |  Major | . | Thomas Graves | Robert Parker |
| [YARN-363](https://issues.apache.org/jira/browse/YARN-363) | yarn proxyserver fails to find webapps/proxy directory on startup |  Major | . | Jason Lowe | Kenji Kikushima |
| [MAPREDUCE-5168](https://issues.apache.org/jira/browse/MAPREDUCE-5168) | Reducer can OOM during shuffle because on-disk output stream not released |  Critical | mrv2 | Jason Lowe | Jason Lowe |
| [HDFS-4477](https://issues.apache.org/jira/browse/HDFS-4477) | Secondary namenode may retain old tokens |  Critical | security | Kihwal Lee | Daryn Sharp |
| [YARN-690](https://issues.apache.org/jira/browse/YARN-690) | RM exits on token cancel/renew problems |  Blocker | resourcemanager | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4927](https://issues.apache.org/jira/browse/MAPREDUCE-4927) | Historyserver 500 error due to NPE when accessing specific counters page for failed job |  Major | jobhistoryserver | Jason Lowe | Ashwin Shankar |
| [HDFS-4835](https://issues.apache.org/jira/browse/HDFS-4835) | Port trunk WebHDFS changes to branch-0.23 |  Critical | webhdfs | Robert Parker | Robert Parker |
| [HDFS-3875](https://issues.apache.org/jira/browse/HDFS-3875) | Issue handling checksum errors in write pipeline |  Critical | datanode, hdfs-client | Todd Lipcon | Kihwal Lee |
| [HDFS-4807](https://issues.apache.org/jira/browse/HDFS-4807) | DFSOutputStream.createSocketForPipeline() should not include timeout extension on connect |  Major | . | Kihwal Lee | Cristina L. Abad |
| [HDFS-4714](https://issues.apache.org/jira/browse/HDFS-4714) | Log short messages in Namenode RPC server for exceptions meant for clients |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HADOOP-9614](https://issues.apache.org/jira/browse/HADOOP-9614) | smart-test-patch.sh hangs for new version of patch (2.7.1) |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-5211](https://issues.apache.org/jira/browse/MAPREDUCE-5211) | Reducer intermediate files can collide during merge |  Blocker | mrv2 | Jason Lowe | Jason Lowe |
| [HADOOP-9504](https://issues.apache.org/jira/browse/HADOOP-9504) | MetricsDynamicMBeanBase has concurrency issues in createMBeanInfo |  Critical | metrics | Liang Xie | Liang Xie |
| [MAPREDUCE-5059](https://issues.apache.org/jira/browse/MAPREDUCE-5059) | Job overview shows average merge time larger than for any reduce attempt |  Major | jobhistoryserver, webapps | Jason Lowe | Omkar Vinit Joshi |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9222](https://issues.apache.org/jira/browse/HADOOP-9222) | Cover package with org.apache.hadoop.io.lz4 unit tests |  Major | . | Vadim Bondarev | Vadim Bondarev |
| [HADOOP-9233](https://issues.apache.org/jira/browse/HADOOP-9233) | Cover package org.apache.hadoop.io.compress.zlib with unit tests |  Major | . | Vadim Bondarev | Vadim Bondarev |
| [MAPREDUCE-5015](https://issues.apache.org/jira/browse/MAPREDUCE-5015) | Coverage fix for org.apache.hadoop.mapreduce.tools.CLI |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-548](https://issues.apache.org/jira/browse/YARN-548) | Add tests for YarnUncaughtExceptionHandler |  Major | . | Vadim Bondarev | Vadim Bondarev |


