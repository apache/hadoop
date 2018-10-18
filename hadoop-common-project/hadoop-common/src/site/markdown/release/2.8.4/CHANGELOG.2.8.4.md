
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

## Release 2.8.4 - 2018-05-15



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-12817](https://issues.apache.org/jira/browse/HADOOP-12817) | Enable TLS v1.1 and 1.2 |  Major | security | Robert Kanter | Robert Kanter |
| [HDFS-11409](https://issues.apache.org/jira/browse/HDFS-11409) | DatanodeInfo getNetworkLocation and setNetworkLocation shoud use volatile instead of synchronized |  Minor | performance | Chen Liang | Chen Liang |
| [HADOOP-14246](https://issues.apache.org/jira/browse/HADOOP-14246) | Authentication Tokens should use SecureRandom instead of Random and 256 bit secrets |  Major | security | Robert Kanter | Robert Kanter |
| [HADOOP-14920](https://issues.apache.org/jira/browse/HADOOP-14920) | KMSClientProvider won't work with KMS delegation token retrieved from non-Java client. |  Major | kms | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-14987](https://issues.apache.org/jira/browse/HADOOP-14987) | Improve KMSClientProvider log around delegation token checking |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-7495](https://issues.apache.org/jira/browse/YARN-7495) | Improve robustness of the AggregatedLogDeletionService |  Major | log-aggregation | Jonathan Eagles | Jonathan Eagles |
| [YARN-7678](https://issues.apache.org/jira/browse/YARN-7678) | Ability to enable logging of container memory stats |  Major | nodemanager | Jim Brennan | Jim Brennan |
| [YARN-7590](https://issues.apache.org/jira/browse/YARN-7590) | Improve container-executor validation check |  Major | security, yarn | Eric Yang | Eric Yang |
| [HADOOP-15212](https://issues.apache.org/jira/browse/HADOOP-15212) | Add independent secret manager method for logging expired tokens |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-7728](https://issues.apache.org/jira/browse/YARN-7728) | Expose container preemptions related information in Capacity Scheduler queue metrics |  Major | . | Eric Payne | Eric Payne |
| [MAPREDUCE-7048](https://issues.apache.org/jira/browse/MAPREDUCE-7048) | Uber AM can crash due to unknown task in statusUpdate |  Major | mr-am | Peter Bacsko | Peter Bacsko |
| [HADOOP-13972](https://issues.apache.org/jira/browse/HADOOP-13972) | ADLS to support per-store configuration |  Major | fs/adl | John Zhuge | Sharad Sonker |
| [YARN-7813](https://issues.apache.org/jira/browse/YARN-7813) | Capacity Scheduler Intra-queue Preemption should be configurable for each queue |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HDFS-11187](https://issues.apache.org/jira/browse/HDFS-11187) | Optimize disk access for last partial chunk checksum of Finalized replica |  Major | datanode | Wei-Chiu Chuang | Gabor Bota |
| [HADOOP-15279](https://issues.apache.org/jira/browse/HADOOP-15279) | increase maven heap size recommendations |  Minor | build, documentation, test | Allen Wittenauer | Allen Wittenauer |
| [HDFS-12884](https://issues.apache.org/jira/browse/HDFS-12884) | BlockUnderConstructionFeature.truncateBlock should be of type BlockInfo |  Major | namenode | Konstantin Shvachko | chencan |
| [HADOOP-14841](https://issues.apache.org/jira/browse/HADOOP-14841) | Kms client should disconnect if unable to get output stream from connection. |  Major | kms | Xiao Chen | Rushabh S Shah |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13988](https://issues.apache.org/jira/browse/HADOOP-13988) | KMSClientProvider does not work with WebHDFS and Apache Knox w/ProxyUser |  Major | common, kms | Greg Senia | Xiaoyu Yao |
| [HADOOP-14029](https://issues.apache.org/jira/browse/HADOOP-14029) | Fix KMSClientProvider for non-secure proxyuser use case |  Major | kms | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-12614](https://issues.apache.org/jira/browse/HDFS-12614) | FSPermissionChecker#getINodeAttrs() throws NPE when INodeAttributesProvider configured |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7489](https://issues.apache.org/jira/browse/YARN-7489) | ConcurrentModificationException in RMAppImpl#getRMAppMetrics |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7525](https://issues.apache.org/jira/browse/YARN-7525) | Incorrect query parameters in cluster nodes REST API document |  Minor | documentation | Tao Yang | Tao Yang |
| [HDFS-12754](https://issues.apache.org/jira/browse/HDFS-12754) | Lease renewal can hit a deadlock |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-11754](https://issues.apache.org/jira/browse/HDFS-11754) | Make FsServerDefaults cache configurable. |  Minor | . | Rushabh S Shah | Mikhail Erofeev |
| [MAPREDUCE-5124](https://issues.apache.org/jira/browse/MAPREDUCE-5124) | AM lacks flow control for task events |  Major | mr-am | Jason Lowe | Peter Bacsko |
| [HDFS-12833](https://issues.apache.org/jira/browse/HDFS-12833) | Distcp : Update the usage of delete option for dependency with update and overwrite option |  Minor | distcp, hdfs | Harshakiran Reddy | usharani |
| [HDFS-12907](https://issues.apache.org/jira/browse/HDFS-12907) | Allow read-only access to reserved raw for non-superusers |  Major | namenode | Daryn Sharp | Rushabh S Shah |
| [HDFS-12881](https://issues.apache.org/jira/browse/HDFS-12881) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Lowe | Ajay Kumar |
| [YARN-7595](https://issues.apache.org/jira/browse/YARN-7595) | Container launching code suppresses close exceptions after writes |  Major | nodemanager | Jason Lowe | Jim Brennan |
| [YARN-7661](https://issues.apache.org/jira/browse/YARN-7661) | NodeManager metrics return wrong value after update node resource |  Major | . | Yang Wang | Yang Wang |
| [HDFS-12347](https://issues.apache.org/jira/browse/HDFS-12347) | TestBalancerRPCDelay#testBalancerRPCDelay fails very frequently |  Critical | test | Xiao Chen | Bharat Viswanadham |
| [HADOOP-15143](https://issues.apache.org/jira/browse/HADOOP-15143) | NPE due to Invalid KerberosTicket in UGI |  Major | . | Jitendra Nath Pandey | Mukul Kumar Singh |
| [MAPREDUCE-7028](https://issues.apache.org/jira/browse/MAPREDUCE-7028) | Concurrent task progress updates causing NPE in Application Master |  Blocker | mr-am | Gergo Repas | Gergo Repas |
| [YARN-7619](https://issues.apache.org/jira/browse/YARN-7619) | Max AM Resource value in Capacity Scheduler UI has to be refreshed for every user |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [YARN-7663](https://issues.apache.org/jira/browse/YARN-7663) | RMAppImpl:Invalid event: START at KILLED |  Major | resourcemanager | lujie | lujie |
| [YARN-6948](https://issues.apache.org/jira/browse/YARN-6948) | Invalid event: ATTEMPT\_ADDED at FINAL\_SAVING |  Major | yarn | lujie | lujie |
| [HDFS-6804](https://issues.apache.org/jira/browse/HDFS-6804) | Add test for race condition between transferring block and appending block causes "Unexpected checksum mismatch exception" |  Major | datanode | Gordon Wang | Brahma Reddy Battula |
| [HDFS-9049](https://issues.apache.org/jira/browse/HDFS-9049) | Make Datanode Netty reverse proxy port to be configurable |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [HADOOP-15150](https://issues.apache.org/jira/browse/HADOOP-15150) | in FsShell, UGI params should be overidden through env vars(-D arg) |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-15181](https://issues.apache.org/jira/browse/HADOOP-15181) | Typo in SecureMode.md |  Trivial | documentation | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-7102](https://issues.apache.org/jira/browse/YARN-7102) | NM heartbeat stuck when responseId overflows MAX\_INT |  Critical | . | Botong Huang | Botong Huang |
| [MAPREDUCE-7020](https://issues.apache.org/jira/browse/MAPREDUCE-7020) | Task timeout in uber mode can crash AM |  Major | mr-am | Akira Ajisaka | Peter Bacsko |
| [HDFS-13100](https://issues.apache.org/jira/browse/HDFS-13100) | Handle IllegalArgumentException when GETSERVERDEFAULTS is not implemented in webhdfs. |  Critical | hdfs, webhdfs | Yongjun Zhang | Yongjun Zhang |
| [YARN-7849](https://issues.apache.org/jira/browse/YARN-7849) | TestMiniYarnClusterNodeUtilization#testUpdateNodeUtilization fails due to heartbeat sync error |  Major | test | Jason Lowe | Botong Huang |
| [HDFS-13120](https://issues.apache.org/jira/browse/HDFS-13120) | Snapshot diff could be corrupted after concat |  Major | namenode, snapshots | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-10453](https://issues.apache.org/jira/browse/HDFS-10453) | ReplicationMonitor thread could stuck for long time due to the race between replication and delete of same file in a large cluster. |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [HDFS-8693](https://issues.apache.org/jira/browse/HDFS-8693) | refreshNamenodes does not support adding a new standby to a running DN |  Critical | datanode, ha | Jian Fang | Ajith S |
| [MAPREDUCE-7052](https://issues.apache.org/jira/browse/MAPREDUCE-7052) | TestFixedLengthInputFormat#testFormatCompressedIn is flaky |  Major | client, test | Peter Bacsko | Peter Bacsko |
| [HDFS-13112](https://issues.apache.org/jira/browse/HDFS-13112) | Token expiration edits may cause log corruption or deadlock |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-7053](https://issues.apache.org/jira/browse/MAPREDUCE-7053) | Timed out tasks can fail to produce thread dump |  Major | . | Jason Lowe | Jason Lowe |
| [HADOOP-15206](https://issues.apache.org/jira/browse/HADOOP-15206) | BZip2 drops and duplicates records when input split size is small |  Major | . | Aki Tanaka | Aki Tanaka |
| [YARN-7947](https://issues.apache.org/jira/browse/YARN-7947) | Capacity Scheduler intra-queue preemption can NPE for non-schedulable apps |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HADOOP-14903](https://issues.apache.org/jira/browse/HADOOP-14903) | Add json-smart explicitly to pom.xml |  Major | common | Ray Chiang | Ray Chiang |
| [HDFS-12070](https://issues.apache.org/jira/browse/HDFS-12070) | Failed block recovery leaves files open indefinitely and at risk for data loss |  Major | . | Daryn Sharp | Kihwal Lee |
| [HADOOP-15283](https://issues.apache.org/jira/browse/HADOOP-15283) | Upgrade from findbugs 3.0.1 to spotbugs 3.1.2 in branch-2 to fix docker image build |  Major | . | Xiao Chen | Akira Ajisaka |
| [HDFS-13164](https://issues.apache.org/jira/browse/HDFS-13164) | File not closed if streamer fail with DSQuotaExceededException |  Major | hdfs-client | Xiao Chen | Xiao Chen |
| [HDFS-13109](https://issues.apache.org/jira/browse/HDFS-13109) | Support fully qualified hdfs path in EZ commands |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [MAPREDUCE-6930](https://issues.apache.org/jira/browse/MAPREDUCE-6930) | mapreduce.map.cpu.vcores and mapreduce.reduce.cpu.vcores are both present twice in mapred-default.xml |  Major | mrv2 | Daniel Templeton | Sen Zhao |
| [HDFS-12156](https://issues.apache.org/jira/browse/HDFS-12156) | TestFSImage fails without -Pnative |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-8054](https://issues.apache.org/jira/browse/YARN-8054) | Improve robustness of the LocalDirsHandlerService MonitoringTimerTask thread |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-13195](https://issues.apache.org/jira/browse/HDFS-13195) | DataNode conf page  cannot display the current value after reconfig |  Minor | datanode | maobaolong | maobaolong |
| [HADOOP-12862](https://issues.apache.org/jira/browse/HADOOP-12862) | LDAP Group Mapping over SSL can not specify trust store |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-15317](https://issues.apache.org/jira/browse/HADOOP-15317) | Improve NetworkTopology chooseRandom's loop |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-13427](https://issues.apache.org/jira/browse/HDFS-13427) | Fix the section titles of transparent encryption document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-8120](https://issues.apache.org/jira/browse/YARN-8120) | JVM can crash with SIGSEGV when exiting due to custom leveldb logger |  Major | nodemanager, resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-8147](https://issues.apache.org/jira/browse/YARN-8147) | TestClientRMService#testGetApplications sporadically fails |  Major | test | Jason Lowe | Jason Lowe |
| [HADOOP-14970](https://issues.apache.org/jira/browse/HADOOP-14970) | MiniHadoopClusterManager doesn't respect lack of format option |  Minor | . | Erik Krogen | Erik Krogen |
| [HADOOP-15180](https://issues.apache.org/jira/browse/HADOOP-15180) | branch-2 : daemon processes' sysout overwrites 'ulimit -a' in daemon's out file |  Minor | scripts | Ranith Sardar | Ranith Sardar |
| [HDFS-10183](https://issues.apache.org/jira/browse/HDFS-10183) | Prevent race condition during class initialization |  Minor | fs | Pavel Avgustinov | Pavel Avgustinov |
| [HADOOP-15390](https://issues.apache.org/jira/browse/HADOOP-15390) | Yarn RM logs flooded by DelegationTokenRenewer trying to renew KMS tokens |  Critical | . | Xiao Chen | Xiao Chen |
| [HADOOP-15385](https://issues.apache.org/jira/browse/HADOOP-15385) | Many tests are failing in hadoop-distcp project in branch-2 |  Critical | tools/distcp | Rushabh S Shah | Jason Lowe |
| [MAPREDUCE-7073](https://issues.apache.org/jira/browse/MAPREDUCE-7073) | Optimize TokenCache#obtainTokensForNamenodesInternal |  Major | . | Bibin A Chundatt | Bibin A Chundatt |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13174](https://issues.apache.org/jira/browse/HADOOP-13174) | Add more debug logs for delegation tokens and authentication |  Minor | security | Xiao Chen | Xiao Chen |
| [HADOOP-14799](https://issues.apache.org/jira/browse/HADOOP-14799) | Update nimbus-jose-jwt to 4.41.1 |  Major | . | Ray Chiang | Ray Chiang |
| [HDFS-12396](https://issues.apache.org/jira/browse/HDFS-12396) | Webhdfs file system should get delegation token from kms provider. |  Major | encryption, kms, webhdfs | Rushabh S Shah | Rushabh S Shah |
| [HDFS-12574](https://issues.apache.org/jira/browse/HDFS-12574) | Add CryptoInputStream to WebHdfsFileSystem read call. |  Major | encryption, kms, webhdfs | Rushabh S Shah | Rushabh S Shah |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15177](https://issues.apache.org/jira/browse/HADOOP-15177) | Update the release year to 2018 |  Blocker | build | Akira Ajisaka | Bharat Viswanadham |
