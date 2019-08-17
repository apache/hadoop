
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

## Release 2.7.6 - 2018-04-16



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9477](https://issues.apache.org/jira/browse/HADOOP-9477) | Add posixGroups support for LDAP groups mapping service |  Major | . | Kai Zheng | Dapeng Sun |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-12472](https://issues.apache.org/jira/browse/HADOOP-12472) | Make GenericTestUtils.assertExceptionContains robust |  Minor | test | Steve Loughran | Steve Loughran |
| [HADOOP-12568](https://issues.apache.org/jira/browse/HADOOP-12568) | Update core-default.xml to describe posixGroups support |  Minor | documentation | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13105](https://issues.apache.org/jira/browse/HADOOP-13105) | Support timeouts in LDAP queries in LdapGroupsMapping. |  Major | security | Chris Nauroth | Mingliang Liu |
| [HADOOP-13263](https://issues.apache.org/jira/browse/HADOOP-13263) | Reload cached groups in background after expiry |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-11003](https://issues.apache.org/jira/browse/HDFS-11003) | Expose "XmitsInProgress" through DataNodeMXBean |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-14246](https://issues.apache.org/jira/browse/HADOOP-14246) | Authentication Tokens should use SecureRandom instead of Random and 256 bit secrets |  Major | security | Robert Kanter | Robert Kanter |
| [YARN-7590](https://issues.apache.org/jira/browse/YARN-7590) | Improve container-executor validation check |  Major | security, yarn | Eric Yang | Eric Yang |
| [HADOOP-15212](https://issues.apache.org/jira/browse/HADOOP-15212) | Add independent secret manager method for logging expired tokens |  Major | security | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-7048](https://issues.apache.org/jira/browse/MAPREDUCE-7048) | Uber AM can crash due to unknown task in statusUpdate |  Major | mr-am | Peter Bacsko | Peter Bacsko |
| [HDFS-11187](https://issues.apache.org/jira/browse/HDFS-11187) | Optimize disk access for last partial chunk checksum of Finalized replica |  Major | datanode | Wei-Chiu Chuang | Gabor Bota |
| [HADOOP-15279](https://issues.apache.org/jira/browse/HADOOP-15279) | increase maven heap size recommendations |  Minor | build, documentation, test | Allen Wittenauer | Allen Wittenauer |
| [HDFS-12884](https://issues.apache.org/jira/browse/HDFS-12884) | BlockUnderConstructionFeature.truncateBlock should be of type BlockInfo |  Major | namenode | Konstantin Shvachko | chencan |
| [HADOOP-15345](https://issues.apache.org/jira/browse/HADOOP-15345) | Backport HADOOP-12185 to branch-2.7: NetworkTopology is not efficient adding/getting/removing nodes |  Major | . | He Xiaoqiao | He Xiaoqiao |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-3425](https://issues.apache.org/jira/browse/YARN-3425) | NPE from RMNodeLabelsManager.serviceStop when NodeLabelsManager.serviceInit failed |  Minor | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-12001](https://issues.apache.org/jira/browse/HADOOP-12001) | Limiting LDAP search conflicts with posixGroup addition |  Blocker | security | Patrick White | Patrick White |
| [YARN-4167](https://issues.apache.org/jira/browse/YARN-4167) | NPE on RMActiveServices#serviceStop when store is null |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-12181](https://issues.apache.org/jira/browse/HADOOP-12181) | Fix intermittent test failure of TestZKSignerSecretProvider |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7959](https://issues.apache.org/jira/browse/HDFS-7959) | WebHdfs logging is missing on Datanode |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-4210](https://issues.apache.org/jira/browse/HDFS-4210) | Throw helpful exception when DNS entry for JournalNode cannot be resolved |  Trivial | ha, journal-node, namenode | Damien Hardy | John Zhuge |
| [HADOOP-13375](https://issues.apache.org/jira/browse/HADOOP-13375) | o.a.h.security.TestGroupsCaching.testBackgroundRefreshCounters seems flaky |  Major | security, test | Mingliang Liu | Weiwei Yang |
| [HADOOP-12611](https://issues.apache.org/jira/browse/HADOOP-12611) | TestZKSignerSecretProvider#testMultipleInit occasionally fail |  Major | . | Wei-Chiu Chuang | Eric Badger |
| [HADOOP-13508](https://issues.apache.org/jira/browse/HADOOP-13508) | FsPermission string constructor does not recognize sticky bit |  Major | . | Atul Sikaria | Atul Sikaria |
| [HDFS-12299](https://issues.apache.org/jira/browse/HDFS-12299) | Race Between update pipeline and DN Re-Registration |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-12371](https://issues.apache.org/jira/browse/HDFS-12371) | "BlockVerificationFailures" and "BlocksVerified" show up as 0 in Datanode JMX |  Major | metrics | Sai Nukavarapu | Hanisha Koneru |
| [MAPREDUCE-5124](https://issues.apache.org/jira/browse/MAPREDUCE-5124) | AM lacks flow control for task events |  Major | mr-am | Jason Lowe | Peter Bacsko |
| [HDFS-12881](https://issues.apache.org/jira/browse/HDFS-12881) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Lowe | Ajay Kumar |
| [YARN-7661](https://issues.apache.org/jira/browse/YARN-7661) | NodeManager metrics return wrong value after update node resource |  Major | . | Yang Wang | Yang Wang |
| [HDFS-12347](https://issues.apache.org/jira/browse/HDFS-12347) | TestBalancerRPCDelay#testBalancerRPCDelay fails very frequently |  Critical | test | Xiao Chen | Bharat Viswanadham |
| [YARN-6632](https://issues.apache.org/jira/browse/YARN-6632) | Backport YARN-3425 to branch 2.7 |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15143](https://issues.apache.org/jira/browse/HADOOP-15143) | NPE due to Invalid KerberosTicket in UGI |  Major | . | Jitendra Nath Pandey | Mukul Kumar Singh |
| [MAPREDUCE-7028](https://issues.apache.org/jira/browse/MAPREDUCE-7028) | Concurrent task progress updates causing NPE in Application Master |  Blocker | mr-am | Gergo Repas | Gergo Repas |
| [HADOOP-12751](https://issues.apache.org/jira/browse/HADOOP-12751) | While using kerberos Hadoop incorrectly assumes names with '@' to be non-simple |  Critical | security | Bolke de Bruin | Bolke de Bruin |
| [MAPREDUCE-7020](https://issues.apache.org/jira/browse/MAPREDUCE-7020) | Task timeout in uber mode can crash AM |  Major | mr-am | Akira Ajisaka | Peter Bacsko |
| [HDFS-13126](https://issues.apache.org/jira/browse/HDFS-13126) | Backport [HDFS-7959] to branch-2.7 to re-enable HTTP request logging for WebHDFS |  Major | datanode, webhdfs | Erik Krogen | Erik Krogen |
| [HDFS-13120](https://issues.apache.org/jira/browse/HDFS-13120) | Snapshot diff could be corrupted after concat |  Major | namenode, snapshots | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-10453](https://issues.apache.org/jira/browse/HDFS-10453) | ReplicationMonitor thread could stuck for long time due to the race between replication and delete of same file in a large cluster. |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [MAPREDUCE-7052](https://issues.apache.org/jira/browse/MAPREDUCE-7052) | TestFixedLengthInputFormat#testFormatCompressedIn is flaky |  Major | client, test | Peter Bacsko | Peter Bacsko |
| [HDFS-13112](https://issues.apache.org/jira/browse/HDFS-13112) | Token expiration edits may cause log corruption or deadlock |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-15206](https://issues.apache.org/jira/browse/HADOOP-15206) | BZip2 drops and duplicates records when input split size is small |  Major | . | Aki Tanaka | Aki Tanaka |
| [HADOOP-15283](https://issues.apache.org/jira/browse/HADOOP-15283) | Upgrade from findbugs 3.0.1 to spotbugs 3.1.2 in branch-2 to fix docker image build |  Major | . | Xiao Chen | Akira Ajisaka |
| [HDFS-13195](https://issues.apache.org/jira/browse/HDFS-13195) | DataNode conf page  cannot display the current value after reconfig |  Minor | datanode | maobaolong | maobaolong |
| [HADOOP-12862](https://issues.apache.org/jira/browse/HADOOP-12862) | LDAP Group Mapping over SSL can not specify trust store |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-7249](https://issues.apache.org/jira/browse/YARN-7249) | Fix CapacityScheduler NPE issue when a container preempted while the node is being removed |  Blocker | . | Wangda Tan | Wangda Tan |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-6633](https://issues.apache.org/jira/browse/YARN-6633) | Backport YARN-4167 to branch 2.7 |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15177](https://issues.apache.org/jira/browse/HADOOP-15177) | Update the release year to 2018 |  Blocker | build | Akira Ajisaka | Bharat Viswanadham |
