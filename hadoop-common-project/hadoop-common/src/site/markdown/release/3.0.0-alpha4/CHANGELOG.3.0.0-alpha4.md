
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

## Release 3.0.0-alpha4 - 2017-07-07

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-10860](https://issues.apache.org/jira/browse/HDFS-10860) | Switch HttpFS from Tomcat to Jetty |  Blocker | httpfs | John Zhuge | John Zhuge |
| [HADOOP-13119](https://issues.apache.org/jira/browse/HADOOP-13119) | Add ability to secure log servlet using proxy users |  Major | . | Jeffrey E  Rodriguez | Yuanbo Liu |
| [HADOOP-13929](https://issues.apache.org/jira/browse/HADOOP-13929) | ADLS connector should not check in contract-test-options.xml |  Major | fs/adl, test | John Zhuge | John Zhuge |
| [HDFS-11100](https://issues.apache.org/jira/browse/HDFS-11100) | Recursively deleting file protected by sticky bit should fail |  Critical | fs | John Zhuge | John Zhuge |
| [HADOOP-13805](https://issues.apache.org/jira/browse/HADOOP-13805) | UGI.getCurrentUser() fails if user does not have a keytab associated |  Major | security | Alejandro Abdelnur | Xiao Chen |
| [HDFS-11405](https://issues.apache.org/jira/browse/HDFS-11405) | Rename "erasurecode" CLI subcommand to "ec" |  Blocker | erasure-coding | Andrew Wang | Manoj Govindassamy |
| [HDFS-11426](https://issues.apache.org/jira/browse/HDFS-11426) | Refactor EC CLI to be similar to storage policies CLI |  Major | erasure-coding, shell | Andrew Wang | Andrew Wang |
| [HDFS-11427](https://issues.apache.org/jira/browse/HDFS-11427) | Rename "rs-default" to "rs" |  Major | erasure-coding | Andrew Wang | Andrew Wang |
| [HDFS-11382](https://issues.apache.org/jira/browse/HDFS-11382) | Persist Erasure Coding Policy ID in a new optional field in INodeFile in FSImage |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-11428](https://issues.apache.org/jira/browse/HDFS-11428) | Change setErasureCodingPolicy to take a required string EC policy name |  Major | erasure-coding | Andrew Wang | Andrew Wang |
| [HADOOP-14138](https://issues.apache.org/jira/browse/HADOOP-14138) | Remove S3A ref from META-INF service discovery, rely on existing core-default entry |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-11152](https://issues.apache.org/jira/browse/HDFS-11152) | Start erasure coding policy ID number from 1 instead of 0 to void potential unexpected errors |  Blocker | erasure-coding | Sammi Chen | Sammi Chen |
| [HDFS-11314](https://issues.apache.org/jira/browse/HDFS-11314) | Enforce set of enabled EC policies on the NameNode |  Blocker | erasure-coding | Andrew Wang | Andrew Wang |
| [HDFS-11505](https://issues.apache.org/jira/browse/HDFS-11505) | Do not enable any erasure coding policies by default |  Major | erasure-coding | Andrew Wang | Manoj Govindassamy |
| [HADOOP-10101](https://issues.apache.org/jira/browse/HADOOP-10101) | Update guava dependency to the latest version |  Major | . | Rakesh R | Tsuyoshi Ozawa |
| [HADOOP-14267](https://issues.apache.org/jira/browse/HADOOP-14267) | Make DistCpOptions class immutable |  Major | tools/distcp | Mingliang Liu | Mingliang Liu |
| [HADOOP-14202](https://issues.apache.org/jira/browse/HADOOP-14202) | fix jsvc/secure user var inconsistencies |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-14174](https://issues.apache.org/jira/browse/HADOOP-14174) | Set default ADLS access token provider type to ClientCredential |  Major | fs/adl | John Zhuge | John Zhuge |
| [YARN-6298](https://issues.apache.org/jira/browse/YARN-6298) | Metric preemptCall is not used in new preemption |  Blocker | fairscheduler | Yufei Gu | Yufei Gu |
| [HADOOP-14285](https://issues.apache.org/jira/browse/HADOOP-14285) | Update minimum version of Maven from 3.0 to 3.3 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14225](https://issues.apache.org/jira/browse/HADOOP-14225) | Remove xmlenc dependency |  Minor | . | Chris Douglas | Chris Douglas |
| [HADOOP-13665](https://issues.apache.org/jira/browse/HADOOP-13665) | Erasure Coding codec should support fallback coder |  Blocker | io | Wei-Chiu Chuang | Kai Sasaki |
| [HADOOP-14248](https://issues.apache.org/jira/browse/HADOOP-14248) | Retire SharedInstanceProfileCredentialsProvider in trunk. |  Major | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HDFS-11565](https://issues.apache.org/jira/browse/HDFS-11565) | Use compact identifiers for built-in ECPolicies in HdfsFileStatus |  Blocker | erasure-coding | Andrew Wang | Andrew Wang |
| [YARN-3427](https://issues.apache.org/jira/browse/YARN-3427) | Remove deprecated methods from ResourceCalculatorProcessTree |  Blocker | . | Karthik Kambatla | Miklos Szegedi |
| [HDFS-11402](https://issues.apache.org/jira/browse/HDFS-11402) | HDFS Snapshots should capture point-in-time copies of OPEN files |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-10105](https://issues.apache.org/jira/browse/HADOOP-10105) | remove httpclient dependency |  Blocker | build | Colin P. McCabe | Akira Ajisaka |
| [HADOOP-13200](https://issues.apache.org/jira/browse/HADOOP-13200) | Implement customizable and configurable erasure coders |  Blocker | . | Kai Zheng | Tim Yao |
| [YARN-2962](https://issues.apache.org/jira/browse/YARN-2962) | ZKRMStateStore: Limit the number of znodes under a znode |  Critical | resourcemanager | Karthik Kambatla | Varun Saxena |
| [HADOOP-14386](https://issues.apache.org/jira/browse/HADOOP-14386) | Rewind trunk from Guava 21.0 back to Guava 11.0.2 |  Blocker | . | Andrew Wang | Andrew Wang |
| [HADOOP-14401](https://issues.apache.org/jira/browse/HADOOP-14401) | maven-project-info-reports-plugin can be removed |  Major | . | Andras Bokor | Andras Bokor |
| [HADOOP-14375](https://issues.apache.org/jira/browse/HADOOP-14375) | Remove tomcat support from hadoop-functions.sh |  Minor | scripts | Allen Wittenauer | John Zhuge |
| [HADOOP-14419](https://issues.apache.org/jira/browse/HADOOP-14419) | Remove findbugs report from docs profile |  Minor | . | Andras Bokor | Andras Bokor |
| [HADOOP-14426](https://issues.apache.org/jira/browse/HADOOP-14426) | Upgrade Kerby version from 1.0.0-RC2 to 1.0.0 |  Blocker | security | Jiajia Li | Jiajia Li |
| [HADOOP-13921](https://issues.apache.org/jira/browse/HADOOP-13921) | Remove Log4j classes from JobConf |  Critical | conf | Sean Busbey | Sean Busbey |
| [HADOOP-8143](https://issues.apache.org/jira/browse/HADOOP-8143) | Change distcp to have -pb on by default |  Minor | . | Dave Thompson | Mithun Radhakrishnan |
| [HADOOP-14502](https://issues.apache.org/jira/browse/HADOOP-14502) | Confusion/name conflict between NameNodeActivity#BlockReportNumOps and RpcDetailedActivity#BlockReportNumOps |  Minor | metrics | Erik Krogen | Erik Krogen |
| [HDFS-11067](https://issues.apache.org/jira/browse/HDFS-11067) | DFS#listStatusIterator(..) should throw FileNotFoundException if the directory deleted before fetching next batch of entries |  Major | hdfs-client | Vinayakumar B | Vinayakumar B |
| [YARN-6127](https://issues.apache.org/jira/browse/YARN-6127) | Add support for work preserving NM restart when AMRMProxy is enabled |  Major | amrmproxy, nodemanager | Subru Krishnan | Botong Huang |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-5910](https://issues.apache.org/jira/browse/YARN-5910) | Support for multi-cluster delegation tokens |  Minor | security | Clay B. | Jian He |
| [YARN-5864](https://issues.apache.org/jira/browse/YARN-5864) | YARN Capacity Scheduler - Queue Priorities |  Major | . | Wangda Tan | Wangda Tan |
| [HDFS-11194](https://issues.apache.org/jira/browse/HDFS-11194) | Maintain aggregated peer performance metrics on NameNode |  Major | namenode | Xiaobing Zhou | Arpit Agarwal |
| [HADOOP-14049](https://issues.apache.org/jira/browse/HADOOP-14049) | Honour AclBit flag associated to file/folder permission for Azure datalake account |  Major | fs/adl | Vishwajeet Dusane | Vishwajeet Dusane |
| [YARN-5280](https://issues.apache.org/jira/browse/YARN-5280) | Allow YARN containers to run with Java Security Manager |  Minor | nodemanager, yarn | Greg Phillips | Greg Phillips |
| [HADOOP-14048](https://issues.apache.org/jira/browse/HADOOP-14048) | REDO operation of WASB#AtomicRename should create placeholder blob for destination folder |  Critical | fs/azure | NITIN VERMA | NITIN VERMA |
| [YARN-6451](https://issues.apache.org/jira/browse/YARN-6451) | Add RM monitor validating metrics invariants |  Major | . | Carlo Curino | Carlo Curino |
| [MAPREDUCE-6871](https://issues.apache.org/jira/browse/MAPREDUCE-6871) | Allow users to specify racks and nodes for strict locality for AMs |  Major | client | Robert Kanter | Robert Kanter |
| [HDFS-11417](https://issues.apache.org/jira/browse/HDFS-11417) | Add datanode admin command to get the storage info. |  Major | . | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-679](https://issues.apache.org/jira/browse/YARN-679) | add an entry point that can start any Yarn service |  Major | api | Steve Loughran | Steve Loughran |
| [HDFS-10480](https://issues.apache.org/jira/browse/HDFS-10480) | Add an admin command to list currently open files |  Major | . | Kihwal Lee | Manoj Govindassamy |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14002](https://issues.apache.org/jira/browse/HADOOP-14002) | Document -DskipShade property in BUILDING.txt |  Minor | build, documentation | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-13956](https://issues.apache.org/jira/browse/HADOOP-13956) | Read ADLS credentials from Credential Provider |  Critical | fs/adl | John Zhuge | John Zhuge |
| [HADOOP-13962](https://issues.apache.org/jira/browse/HADOOP-13962) | Update ADLS SDK to 2.1.4 |  Major | fs/adl | John Zhuge | John Zhuge |
| [HADOOP-13990](https://issues.apache.org/jira/browse/HADOOP-13990) | Document KMS usage of CredentialProvider API |  Minor | documentation, kms | John Zhuge | John Zhuge |
| [HDFS-10534](https://issues.apache.org/jira/browse/HDFS-10534) | NameNode WebUI should display DataNode usage histogram |  Major | namenode, ui | Zhe Zhang | Kai Sasaki |
| [MAPREDUCE-6829](https://issues.apache.org/jira/browse/MAPREDUCE-6829) | Add peak memory usage counter for each task |  Major | mrv2 | Yufei Gu | Miklos Szegedi |
| [HDFS-11374](https://issues.apache.org/jira/browse/HDFS-11374) | Skip FSync in Test util CreateEditsLog to speed up edit log generation |  Minor | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-9884](https://issues.apache.org/jira/browse/HDFS-9884) | Use doxia macro to generate in-page TOC of HDFS site documentation |  Major | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-6131](https://issues.apache.org/jira/browse/YARN-6131) | FairScheduler: Lower update interval for faster tests |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-6106](https://issues.apache.org/jira/browse/YARN-6106) | Document FairScheduler 'allowPreemptionFrom' queue property |  Minor | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-4658](https://issues.apache.org/jira/browse/YARN-4658) | Typo in o.a.h.yarn.server.resourcemanager.scheduler.fair.TestFairScheduler comment |  Major | . | Daniel Templeton | Udai Kiran Potluri |
| [MAPREDUCE-6644](https://issues.apache.org/jira/browse/MAPREDUCE-6644) | Use doxia macro to generate in-page TOC of MapReduce site documentation |  Major | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-11370](https://issues.apache.org/jira/browse/HDFS-11370) | Optimize NamenodeFsck#getReplicaInfo |  Minor | namenode | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-11112](https://issues.apache.org/jira/browse/HDFS-11112) | Journal Nodes should refuse to format non-empty directories |  Major | . | Arpit Agarwal | Yiqun Lin |
| [HDFS-11353](https://issues.apache.org/jira/browse/HDFS-11353) | Improve the unit tests relevant to DataNode volume failure testing |  Major | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-14053](https://issues.apache.org/jira/browse/HADOOP-14053) | Update the link to HTrace SpanReceivers |  Minor | documentation | Akira Ajisaka | Yiqun Lin |
| [HADOOP-12097](https://issues.apache.org/jira/browse/HADOOP-12097) | Allow port range to be specified while starting webapp |  Major | . | Varun Saxena | Varun Saxena |
| [HDFS-10219](https://issues.apache.org/jira/browse/HDFS-10219) | Change the default value for dfs.namenode.reconstruction.pending.timeout-sec from -1 to 300 |  Minor | . | Akira Ajisaka | Yiqun Lin |
| [MAPREDUCE-6404](https://issues.apache.org/jira/browse/MAPREDUCE-6404) | Allow AM to specify a port range for starting its webapp |  Major | applicationmaster | Varun Saxena | Varun Saxena |
| [MAPREDUCE-6842](https://issues.apache.org/jira/browse/MAPREDUCE-6842) | Update the links in PiEstimator document |  Minor | documentation | Akira Ajisaka | Jung Yoo |
| [HDFS-11210](https://issues.apache.org/jira/browse/HDFS-11210) | Enhance key rolling to guarantee new KeyVersion is returned from generateEncryptedKeys after a key is rolled |  Major | encryption, kms | Xiao Chen | Xiao Chen |
| [HDFS-11409](https://issues.apache.org/jira/browse/HDFS-11409) | DatanodeInfo getNetworkLocation and setNetworkLocation shoud use volatile instead of synchronized |  Minor | performance | Chen Liang | Chen Liang |
| [YARN-6061](https://issues.apache.org/jira/browse/YARN-6061) | Add an UncaughtExceptionHandler for critical threads in RM |  Major | resourcemanager | Yufei Gu | Yufei Gu |
| [YARN-4753](https://issues.apache.org/jira/browse/YARN-4753) | Use doxia macro to generate in-page TOC of YARN site documentation |  Major | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-11333](https://issues.apache.org/jira/browse/HDFS-11333) | Print a user friendly error message when plugins are not found |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-6174](https://issues.apache.org/jira/browse/YARN-6174) | Log files pattern should be same for both running and finished container |  Major | yarn | Sumana Sathish | Xuan Gong |
| [HDFS-11375](https://issues.apache.org/jira/browse/HDFS-11375) | Display the volume storage type in datanode UI |  Minor | datanode, ui | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-6125](https://issues.apache.org/jira/browse/YARN-6125) | The application attempt's diagnostic message should have a maximum size |  Critical | resourcemanager | Daniel Templeton | Andras Piros |
| [HDFS-11406](https://issues.apache.org/jira/browse/HDFS-11406) | Remove unused getStartInstance and getFinalizeInstance in FSEditLogOp |  Trivial | . | Andrew Wang | Alison Yu |
| [HDFS-11438](https://issues.apache.org/jira/browse/HDFS-11438) | Fix typo in error message of StoragePolicyAdmin tool |  Trivial | . | Alison Yu | Alison Yu |
| [YARN-6194](https://issues.apache.org/jira/browse/YARN-6194) | Cluster capacity in SchedulingPolicy is updated only on allocation file reload |  Major | fairscheduler | Karthik Kambatla | Yufei Gu |
| [HADOOP-13321](https://issues.apache.org/jira/browse/HADOOP-13321) | Deprecate FileSystem APIs that promote inefficient call patterns. |  Major | fs | Chris Nauroth | Mingliang Liu |
| [HADOOP-14097](https://issues.apache.org/jira/browse/HADOOP-14097) | Remove Java6 specific code from GzipCodec.java |  Minor | . | Akira Ajisaka | Elek, Marton |
| [HADOOP-13817](https://issues.apache.org/jira/browse/HADOOP-13817) | Add a finite shell command timeout to ShellBasedUnixGroupsMapping |  Minor | security | Harsh J | Harsh J |
| [HDFS-11295](https://issues.apache.org/jira/browse/HDFS-11295) | Check storage remaining instead of node remaining in BlockPlacementPolicyDefault.chooseReplicaToDelete() |  Major | namenode | X. Liang | Elek, Marton |
| [HADOOP-14127](https://issues.apache.org/jira/browse/HADOOP-14127) | Add log4j configuration to enable logging in hadoop-distcp's tests |  Minor | test | Xiao Chen | Xiao Chen |
| [HDFS-11466](https://issues.apache.org/jira/browse/HDFS-11466) | Change dfs.namenode.write-lock-reporting-threshold-ms default from 1000ms to 5000ms |  Major | namenode | Andrew Wang | Andrew Wang |
| [YARN-6189](https://issues.apache.org/jira/browse/YARN-6189) | Improve application status log message when RM restarted when app is in NEW state |  Major | . | Yesha Vora | Junping Du |
| [HDFS-11432](https://issues.apache.org/jira/browse/HDFS-11432) | Federation : Support fully qualified path for Quota/Snapshot/cacheadmin/cryptoadmin commands |  Major | federation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11461](https://issues.apache.org/jira/browse/HDFS-11461) | DataNode Disk Outlier Detection |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11416](https://issues.apache.org/jira/browse/HDFS-11416) | Refactor out system default erasure coding policy |  Major | erasure-coding | Andrew Wang | Andrew Wang |
| [HADOOP-13930](https://issues.apache.org/jira/browse/HADOOP-13930) | Azure: Add Authorization support to WASB |  Major | fs/azure | Dushyanth | Sivaguru Sankaridurg |
| [HDFS-11494](https://issues.apache.org/jira/browse/HDFS-11494) | Log message when DN is not selected for block replication |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-8741](https://issues.apache.org/jira/browse/HDFS-8741) | Proper error msg to be printed when invalid operation type is given to WebHDFS operations |  Minor | webhdfs | Archana T | Surendra Singh Lilhore |
| [HADOOP-14108](https://issues.apache.org/jira/browse/HADOOP-14108) | CLI MiniCluster: add an option to specify NameNode HTTP port |  Minor | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-10838](https://issues.apache.org/jira/browse/HDFS-10838) | Last full block report received time for each DN should be easily discoverable |  Major | ui | Arpit Agarwal | Surendra Singh Lilhore |
| [HDFS-11477](https://issues.apache.org/jira/browse/HDFS-11477) | Simplify file IO profiling configuration |  Minor | . | Hanisha Koneru | Hanisha Koneru |
| [YARN-6287](https://issues.apache.org/jira/browse/YARN-6287) | RMCriticalThreadUncaughtExceptionHandler.rmContext should be final |  Minor | resourcemanager | Daniel Templeton | Corey Barker |
| [HADOOP-14150](https://issues.apache.org/jira/browse/HADOOP-14150) | Implement getHomeDirectory() method in NativeAzureFileSystem |  Critical | fs/azure | Namit Maheshwari | Santhosh G Nayak |
| [YARN-6300](https://issues.apache.org/jira/browse/YARN-6300) | NULL\_UPDATE\_REQUESTS is redundant in TestFairScheduler |  Minor | . | Daniel Templeton | Yuanbo Liu |
| [HDFS-11506](https://issues.apache.org/jira/browse/HDFS-11506) | Move ErasureCodingPolicyManager#getSystemDefaultPolicy to test code |  Major | erasure-coding | Andrew Wang | Manoj Govindassamy |
| [HADOOP-13946](https://issues.apache.org/jira/browse/HADOOP-13946) | Document how HDFS updates timestamps in the FS spec; compare with object stores |  Minor | documentation, fs | Steve Loughran | Steve Loughran |
| [YARN-6042](https://issues.apache.org/jira/browse/YARN-6042) | Dump scheduler and queue state information into FairScheduler DEBUG log |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [HDFS-11511](https://issues.apache.org/jira/browse/HDFS-11511) | Support Timeout when checking single disk |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-10601](https://issues.apache.org/jira/browse/HDFS-10601) | Improve log message to include hostname when the NameNode is in safemode |  Minor | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-11517](https://issues.apache.org/jira/browse/HDFS-11517) | Expose slow disks via DataNode JMX |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-14169](https://issues.apache.org/jira/browse/HADOOP-14169) | Implement listStatusIterator, listLocatedStatus for ViewFs |  Minor | viewfs | Erik Krogen | Erik Krogen |
| [HDFS-11547](https://issues.apache.org/jira/browse/HDFS-11547) | Add logs for slow BlockReceiver while writing data to disk |  Major | datanode | Xiaobing Zhou | Xiaobing Zhou |
| [MAPREDUCE-6865](https://issues.apache.org/jira/browse/MAPREDUCE-6865) | Fix typo in javadoc for DistributedCache |  Trivial | . | Attila Sasvari | Attila Sasvari |
| [YARN-6309](https://issues.apache.org/jira/browse/YARN-6309) | Fair scheduler docs should have the queue and queuePlacementPolicy elements listed in bold so that they're easier to see |  Minor | fairscheduler | Daniel Templeton | esmaeil mirzaee |
| [HADOOP-13945](https://issues.apache.org/jira/browse/HADOOP-13945) | Azure: Add Kerberos and Delegation token support to WASB client. |  Major | fs/azure | Santhosh G Nayak | Santhosh G Nayak |
| [HDFS-11545](https://issues.apache.org/jira/browse/HDFS-11545) | Propagate DataNode's slow disks info to the NameNode via Heartbeat |  Major | . | Hanisha Koneru | Hanisha Koneru |
| [YARN-6284](https://issues.apache.org/jira/browse/YARN-6284) | hasAlreadyRun should be final in ResourceManager.StandByTransitionRunnable |  Major | resourcemanager | Daniel Templeton | Laura Adams |
| [HADOOP-14213](https://issues.apache.org/jira/browse/HADOOP-14213) | Move Configuration runtime check for hadoop-site.xml to initialization |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-10649](https://issues.apache.org/jira/browse/HDFS-10649) | Remove unused PermissionStatus#applyUMask |  Trivial | . | John Zhuge | Chen Liang |
| [HDFS-11574](https://issues.apache.org/jira/browse/HDFS-11574) | Spelling mistakes in the Java source |  Trivial | . | hu xiaodong | hu xiaodong |
| [HDFS-11534](https://issues.apache.org/jira/browse/HDFS-11534) | Add counters for number of blocks in pending IBR |  Major | hdfs | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-5956](https://issues.apache.org/jira/browse/YARN-5956) | Refactor ClientRMService to unify error handling across apis |  Minor | resourcemanager | Kai Sasaki | Kai Sasaki |
| [YARN-6379](https://issues.apache.org/jira/browse/YARN-6379) | Remove unused argument in ClientRMService |  Trivial | . | Kai Sasaki | Kai Sasaki |
| [HADOOP-14233](https://issues.apache.org/jira/browse/HADOOP-14233) | Delay construction of PreCondition.check failure message in Configuration#set |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-14240](https://issues.apache.org/jira/browse/HADOOP-14240) | Configuration#get return value optimization |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-6339](https://issues.apache.org/jira/browse/YARN-6339) | Improve performance for createAndGetApplicationReport |  Major | . | yunjiong zhao | yunjiong zhao |
| [HDFS-11170](https://issues.apache.org/jira/browse/HDFS-11170) | Add builder-based create API to FileSystem |  Major | . | Sammi Chen | Sammi Chen |
| [YARN-6329](https://issues.apache.org/jira/browse/YARN-6329) | Remove unnecessary TODO comment from AppLogAggregatorImpl.java |  Minor | . | Akira Ajisaka | victor bertschinger |
| [HDFS-9705](https://issues.apache.org/jira/browse/HDFS-9705) | Refine the behaviour of getFileChecksum when length = 0 |  Minor | . | Kai Zheng | Sammi Chen |
| [HADOOP-14250](https://issues.apache.org/jira/browse/HADOOP-14250) | Correct spelling of 'separate' and variants |  Minor | . | Doris Gu | Doris Gu |
| [HDFS-10974](https://issues.apache.org/jira/browse/HDFS-10974) | Document replication factor for EC files. |  Major | documentation, erasure-coding | Wei-Chiu Chuang | Yiqun Lin |
| [HDFS-11551](https://issues.apache.org/jira/browse/HDFS-11551) | Handle SlowDiskReport from DataNode at the NameNode |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11603](https://issues.apache.org/jira/browse/HDFS-11603) | Improve slow mirror/disk warnings in BlockReceiver |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-11560](https://issues.apache.org/jira/browse/HDFS-11560) | Expose slow disks via NameNode JMX |  Major | namenode | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11598](https://issues.apache.org/jira/browse/HDFS-11598) | Improve -setrep for Erasure Coded files |  Major | shell | Wei-Chiu Chuang | Yiqun Lin |
| [HDFS-9651](https://issues.apache.org/jira/browse/HDFS-9651) | All web UIs should include a robots.txt file |  Minor | . | Lars Francke | Lars Francke |
| [HADOOP-14280](https://issues.apache.org/jira/browse/HADOOP-14280) | Fix compilation of TestKafkaMetrics |  Major | tools | Andrew Wang | Andrew Wang |
| [HDFS-11628](https://issues.apache.org/jira/browse/HDFS-11628) | Clarify the behavior of HDFS Mover in documentation |  Major | documentation | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-6381](https://issues.apache.org/jira/browse/YARN-6381) | FSAppAttempt has several variables that should be final |  Major | fairscheduler | Daniel Templeton | Ameet Zaveri |
| [HDFS-11302](https://issues.apache.org/jira/browse/HDFS-11302) | Improve Logging for SSLHostnameVerifier |  Major | security | Xiaoyu Yao | Chen Liang |
| [HADOOP-14104](https://issues.apache.org/jira/browse/HADOOP-14104) | Client should always ask namenode for kms provider path. |  Major | kms | Rushabh S Shah | Rushabh S Shah |
| [YARN-5797](https://issues.apache.org/jira/browse/YARN-5797) | Add metrics to the node manager for cleaning the PUBLIC and PRIVATE caches |  Major | . | Chris Trezzo | Chris Trezzo |
| [HADOOP-14276](https://issues.apache.org/jira/browse/HADOOP-14276) | Add a nanosecond API to Time/Timer/FakeTimer |  Minor | util | Erik Krogen | Erik Krogen |
| [HDFS-11623](https://issues.apache.org/jira/browse/HDFS-11623) | Move system erasure coding policies into hadoop-hdfs-client |  Major | erasure-coding | Andrew Wang | Andrew Wang |
| [HADOOP-14008](https://issues.apache.org/jira/browse/HADOOP-14008) | Upgrade to Apache Yetus 0.4.0 |  Major | build, documentation, test | Allen Wittenauer | Allen Wittenauer |
| [YARN-6195](https://issues.apache.org/jira/browse/YARN-6195) | Export UsedCapacity and AbsoluteUsedCapacity to JMX |  Major | capacityscheduler, metrics, yarn | Benson Qiu | Benson Qiu |
| [HDFS-11558](https://issues.apache.org/jira/browse/HDFS-11558) | BPServiceActor thread name is too long |  Minor | datanode | Tsz Wo Nicholas Sze | Xiaobing Zhou |
| [HADOOP-14246](https://issues.apache.org/jira/browse/HADOOP-14246) | Authentication Tokens should use SecureRandom instead of Random and 256 bit secrets |  Major | security | Robert Kanter | Robert Kanter |
| [HDFS-11645](https://issues.apache.org/jira/browse/HDFS-11645) | DataXceiver thread should log the actual error when getting InvalidMagicNumberException |  Minor | datanode | Chen Liang | Chen Liang |
| [HDFS-11648](https://issues.apache.org/jira/browse/HDFS-11648) | Lazy construct the IIP pathname |  Major | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-14274](https://issues.apache.org/jira/browse/HADOOP-14274) | Azure: Simplify Ranger-WASB policy model |  Major | fs/azure | Sivaguru Sankaridurg | Sivaguru Sankaridurg |
| [MAPREDUCE-6673](https://issues.apache.org/jira/browse/MAPREDUCE-6673) | Add a test example job that grows in memory usage over time |  Major | test | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-11794](https://issues.apache.org/jira/browse/HADOOP-11794) | Enable distcp to copy blocks in parallel |  Major | tools/distcp | dhruba borthakur | Yongjun Zhang |
| [YARN-6406](https://issues.apache.org/jira/browse/YARN-6406) | Remove SchedulerRequestKeys when no more pending ResourceRequest |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-11652](https://issues.apache.org/jira/browse/HDFS-11652) | Improve ECSchema and ErasureCodingPolicy toString, hashCode, equals |  Minor | erasure-coding | Andrew Wang | Andrew Wang |
| [HDFS-11634](https://issues.apache.org/jira/browse/HDFS-11634) | Optimize BlockIterator when iterating starts in the middle. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-11531](https://issues.apache.org/jira/browse/HDFS-11531) | Expose hedged read metrics via libHDFS API |  Major | libhdfs | Sailesh Mukil | Sailesh Mukil |
| [HADOOP-14316](https://issues.apache.org/jira/browse/HADOOP-14316) | Switch from Findbugs to Spotbugs |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [YARN-6164](https://issues.apache.org/jira/browse/YARN-6164) | Expose Queue Configurations per Node Label through YARN client api |  Major | . | Benson Qiu | Benson Qiu |
| [YARN-6392](https://issues.apache.org/jira/browse/YARN-6392) | Add submit time to Application Summary log |  Minor | resourcemanager | zhihai xu | zhihai xu |
| [HADOOP-12856](https://issues.apache.org/jira/browse/HADOOP-12856) | FileUtil.checkDest() and RawLocalFileSystem.mkdirs() to throw stricter IOEs; RawLocalFS contract tests to verify |  Minor | fs | Steve Loughran | Steve Loughran |
| [HADOOP-14340](https://issues.apache.org/jira/browse/HADOOP-14340) | Enable KMS and HttpFS to exclude SSL ciphers |  Minor | kms | John Zhuge | John Zhuge |
| [HDFS-11384](https://issues.apache.org/jira/browse/HDFS-11384) | Add option for balancer to disperse getBlocks calls to avoid NameNode's rpc.CallQueueLength spike |  Major | balancer & mover | yunjiong zhao | Konstantin Shvachko |
| [HADOOP-14309](https://issues.apache.org/jira/browse/HADOOP-14309) | Add PowerShell NodeFencer |  Minor | ha | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14359](https://issues.apache.org/jira/browse/HADOOP-14359) | Remove unnecessary shading of commons-httpclient |  Minor | . | Akira Ajisaka | Wei-Chiu Chuang |
| [HADOOP-14367](https://issues.apache.org/jira/browse/HADOOP-14367) | Remove unused setting from pom.xml |  Minor | build | Akira Ajisaka | Chen Liang |
| [HADOOP-14352](https://issues.apache.org/jira/browse/HADOOP-14352) | Make some HttpServer2 SSL properties optional |  Minor | kms | John Zhuge | John Zhuge |
| [HDFS-11722](https://issues.apache.org/jira/browse/HDFS-11722) | Change Datanode file IO profiling sampling to percentage |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11687](https://issues.apache.org/jira/browse/HDFS-11687) | Add new public encryption APIs required by Hive |  Major | encryption | Andrew Wang | Lei (Eddy) Xu |
| [HADOOP-14382](https://issues.apache.org/jira/browse/HADOOP-14382) | Remove usages of MoreObjects.toStringHelper |  Minor | metrics | Andrew Wang | Andrew Wang |
| [HDFS-9807](https://issues.apache.org/jira/browse/HDFS-9807) | Add an optional StorageID to writes |  Major | . | Chris Douglas | Ewan Higgs |
| [HADOOP-14390](https://issues.apache.org/jira/browse/HADOOP-14390) | Correct spelling of 'succeed' and variants |  Trivial | . | Dongtao Zhang | Dongtao Zhang |
| [HADOOP-14383](https://issues.apache.org/jira/browse/HADOOP-14383) | Implement FileSystem that reads from HTTP / HTTPS endpoints |  Major | fs | Haohui Mai | Haohui Mai |
| [YARN-6457](https://issues.apache.org/jira/browse/YARN-6457) | Allow custom SSL configuration to be supplied in WebApps |  Major | webapp, yarn | Sanjay M Pujare | Sanjay M Pujare |
| [HADOOP-14216](https://issues.apache.org/jira/browse/HADOOP-14216) | Improve Configuration XML Parsing Performance |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-6883](https://issues.apache.org/jira/browse/MAPREDUCE-6883) | AuditLogger and TestAuditLogger are dead code |  Minor | client | Daniel Templeton | Vrushali C |
| [HDFS-11800](https://issues.apache.org/jira/browse/HDFS-11800) | Document output of 'hdfs count -u' should contain PATHNAME |  Minor | hdfs | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-14413](https://issues.apache.org/jira/browse/HADOOP-14413) | Add Javadoc comment for jitter parameter on CachingGetSpaceUsed |  Trivial | . | Erik Krogen | Erik Krogen |
| [HDFS-11757](https://issues.apache.org/jira/browse/HDFS-11757) | Query StreamCapabilities when creating balancer's lock file |  Major | balancer & mover | Andrew Wang | Sammi Chen |
| [HDFS-11641](https://issues.apache.org/jira/browse/HDFS-11641) | Reduce cost of audit logging by using FileStatus instead of HdfsFileStatus |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [YARN-6447](https://issues.apache.org/jira/browse/YARN-6447) | Provide container sandbox policies for groups |  Minor | nodemanager, yarn | Greg Phillips | Greg Phillips |
| [HADOOP-14415](https://issues.apache.org/jira/browse/HADOOP-14415) | Use java.lang.AssertionError instead of junit.framework.AssertionFailedError |  Minor | . | Akira Ajisaka | Chen Liang |
| [HDFS-11803](https://issues.apache.org/jira/browse/HDFS-11803) | Add -v option for du command to show header line |  Major | hdfs | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-6493](https://issues.apache.org/jira/browse/YARN-6493) | Print requested node partition in assignContainer logs |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-11793](https://issues.apache.org/jira/browse/HDFS-11793) | Allow to enable user defined erasure coding policy |  Major | erasure-coding | Sammi Chen | Sammi Chen |
| [HADOOP-14407](https://issues.apache.org/jira/browse/HADOOP-14407) | DistCp - Introduce a configurable copy buffer size |  Major | tools/distcp | Omkar Aradhya K S | Omkar Aradhya K S |
| [YARN-6582](https://issues.apache.org/jira/browse/YARN-6582) | FSAppAttempt demand can be updated atomically in updateDemand() |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HDFS-11421](https://issues.apache.org/jira/browse/HDFS-11421) | Make WebHDFS' ACLs RegEx configurable |  Major | webhdfs | Harsh J | Harsh J |
| [HDFS-11891](https://issues.apache.org/jira/browse/HDFS-11891) | DU#refresh should print the path of the directory when an exception is caught |  Minor | . | Chen Liang | Chen Liang |
| [HADOOP-14442](https://issues.apache.org/jira/browse/HADOOP-14442) | Owner support for ranger-wasb integration |  Major | fs, fs/azure | Varada Hemeswari | Varada Hemeswari |
| [HDFS-11832](https://issues.apache.org/jira/browse/HDFS-11832) | Switch leftover logs to slf4j format in BlockManager.java |  Minor | namenode | Hui Xu | Chen Liang |
| [YARN-6477](https://issues.apache.org/jira/browse/YARN-6477) | Dispatcher no longer needs the raw types suppression |  Minor | . | Daniel Templeton | Maya Wexler |
| [YARN-6497](https://issues.apache.org/jira/browse/YARN-6497) | Method length of ResourceManager#serviceInit() is too long |  Minor | resourcemanager | Yufei Gu | Gergely Novák |
| [HDFS-11383](https://issues.apache.org/jira/browse/HDFS-11383) | Intern strings in BlockLocation and ExtendedBlock |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [YARN-6208](https://issues.apache.org/jira/browse/YARN-6208) | Improve the log when FinishAppEvent sent to the NodeManager which didn't run the application |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14440](https://issues.apache.org/jira/browse/HADOOP-14440) | Add metrics for connections dropped |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14485](https://issues.apache.org/jira/browse/HADOOP-14485) | Redundant 'final' modifier in try-with-resources statement |  Minor | . | Wenxin He | Wenxin He |
| [HADOOP-14491](https://issues.apache.org/jira/browse/HADOOP-14491) | Azure has messed doc structure |  Major | documentation, fs/azure | Mingliang Liu | Mingliang Liu |
| [HDFS-11914](https://issues.apache.org/jira/browse/HDFS-11914) | Add more diagnosis info for fsimage transfer failure. |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-11840](https://issues.apache.org/jira/browse/HDFS-11840) | Log HDFS Mover exception message of exit to its own log |  Minor | balancer & mover | LiXin Ge | LiXin Ge |
| [HDFS-11861](https://issues.apache.org/jira/browse/HDFS-11861) | ipc.Client.Connection#sendRpcRequest should log request name |  Trivial | ipc | John Zhuge | John Zhuge |
| [HADOOP-14465](https://issues.apache.org/jira/browse/HADOOP-14465) | LdapGroupsMapping - support user and group search base |  Major | common, security | Shwetha G S | Shwetha G S |
| [HADOOP-14310](https://issues.apache.org/jira/browse/HADOOP-14310) | RolloverSignerSecretProvider.LOG should be @VisibleForTesting |  Minor | security | Daniel Templeton | Arun Shanmugam Kumar |
| [HDFS-11907](https://issues.apache.org/jira/browse/HDFS-11907) | Add metric for time taken by NameNode resource check |  Major | . | Chen Liang | Chen Liang |
| [HADOOP-14503](https://issues.apache.org/jira/browse/HADOOP-14503) | Make RollingAverages a mutable metric |  Major | common | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-14506](https://issues.apache.org/jira/browse/HADOOP-14506) | Add create() contract test that verifies ancestor dir creation |  Minor | fs | Aaron Fabbri | Sean Mackrory |
| [HADOOP-14523](https://issues.apache.org/jira/browse/HADOOP-14523) | OpensslAesCtrCryptoCodec.finalize() holds excessive amounts of memory |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [HADOOP-14524](https://issues.apache.org/jira/browse/HADOOP-14524) | Make CryptoCodec Closeable so it can be cleaned up proactively |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-14424](https://issues.apache.org/jira/browse/HADOOP-14424) | Add CRC32C performance test. |  Minor | common | LiXin Ge | LiXin Ge |
| [HDFS-11345](https://issues.apache.org/jira/browse/HDFS-11345) | Document the configuration key for FSNamesystem lock fairness |  Minor | documentation, namenode | Zhe Zhang | Erik Krogen |
| [HDFS-11647](https://issues.apache.org/jira/browse/HDFS-11647) | Add -E option in hdfs "count" command to show erasure policy summarization |  Major | . | Sammi Chen | luhuichun |
| [HDFS-11789](https://issues.apache.org/jira/browse/HDFS-11789) | Maintain Short-Circuit Read Statistics |  Major | hdfs-client | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11943](https://issues.apache.org/jira/browse/HDFS-11943) | [Erasure coding] Warn log frequently print to screen in doEncode/doDecode functions |  Major | erasure-coding, native | liaoyuxiangqin | liaoyuxiangqin |
| [HDFS-11992](https://issues.apache.org/jira/browse/HDFS-11992) | Replace commons-logging APIs with slf4j in FsDatasetImpl |  Major | . | Akira Ajisaka | hu xiaodong |
| [HDFS-11993](https://issues.apache.org/jira/browse/HDFS-11993) | Add log info when connect to datanode socket address failed |  Major | hdfs-client | chencan | chencan |
| [HDFS-12045](https://issues.apache.org/jira/browse/HDFS-12045) | Add log when Diskbalancer volume is transient storage type |  Major | diskbalancer | steven-wugang | steven-wugang |
| [HADOOP-14536](https://issues.apache.org/jira/browse/HADOOP-14536) | Update azure-storage sdk to version 5.3.0 |  Major | fs/azure | Mingliang Liu | Georgi Chalakov |
| [YARN-6738](https://issues.apache.org/jira/browse/YARN-6738) | LevelDBCacheTimelineStore should reuse ObjectMapper instances |  Major | timelineserver | Zoltan Haindrich | Zoltan Haindrich |
| [HADOOP-14515](https://issues.apache.org/jira/browse/HADOOP-14515) | Specifically configure zookeeper-related log levels in KMS log4j |  Major | kms | Xiao Chen | Xiao Chen |
| [HDFS-11646](https://issues.apache.org/jira/browse/HDFS-11646) | Add -E option in 'ls' to list erasure coding policy of each file and directory if applicable |  Major | erasure-coding | Sammi Chen | luhuichun |
| [HDFS-11881](https://issues.apache.org/jira/browse/HDFS-11881) | NameNode consumes a lot of memory for snapshot diff report generation |  Major | hdfs, snapshots | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-14602](https://issues.apache.org/jira/browse/HADOOP-14602) | allow custom release notes/changelog during create-release |  Minor | build, scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-14611](https://issues.apache.org/jira/browse/HADOOP-14611) | NetworkTopology.DEFAULT\_HOST\_LEVEL is unused |  Trivial | . | Daniel Templeton | Chen Liang |
| [YARN-6751](https://issues.apache.org/jira/browse/YARN-6751) | Display reserved resources in web UI per queue |  Major | fairscheduler, webapp | Abdullah Yousufi | Abdullah Yousufi |
| [YARN-6280](https://issues.apache.org/jira/browse/YARN-6280) | Introduce deselect query param to skip ResourceRequest from getApp/getApps REST API |  Major | resourcemanager, restapi | Lantao Jin | Lantao Jin |
| [YARN-6634](https://issues.apache.org/jira/browse/YARN-6634) | [API] Refactor ResourceManager WebServices to make API explicit |  Critical | resourcemanager | Subru Krishnan | Giovanni Matteo Fumarola |
| [YARN-5547](https://issues.apache.org/jira/browse/YARN-5547) | NMLeveldbStateStore should be more tolerant of unknown keys |  Major | nodemanager | Jason Lowe | Ajith S |
| [HADOOP-14077](https://issues.apache.org/jira/browse/HADOOP-14077) | Improve the patch of HADOOP-13119 |  Major | security | Yuanbo Liu | Yuanbo Liu |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13858](https://issues.apache.org/jira/browse/HADOOP-13858) | TestGridmixMemoryEmulation and TestResourceUsageEmulators fail on the environment other than Linux or Windows |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-6012](https://issues.apache.org/jira/browse/YARN-6012) | Remove node label (removeFromClusterNodeLabels) document is missing |  Major | documentation | Weiwei Yang | Ying Zhang |
| [YARN-6117](https://issues.apache.org/jira/browse/YARN-6117) | SharedCacheManager does not start up |  Major | . | Chris Trezzo | Chris Trezzo |
| [YARN-6082](https://issues.apache.org/jira/browse/YARN-6082) | Invalid REST api response for getApps since queueUsagePercentage is coming as INF |  Critical | . | Sunil Govindan | Sunil Govindan |
| [HDFS-11365](https://issues.apache.org/jira/browse/HDFS-11365) | Log portnumber in PrivilegedNfsGatewayStarter |  Minor | nfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [MAPREDUCE-6808](https://issues.apache.org/jira/browse/MAPREDUCE-6808) | Log map attempts as part of shuffle handler audit log |  Major | . | Jonathan Eagles | Gergő Pásztor |
| [HADOOP-13989](https://issues.apache.org/jira/browse/HADOOP-13989) | Remove erroneous source jar option from hadoop-client shade configuration |  Minor | build | Joe Pallas | Joe Pallas |
| [HDFS-11369](https://issues.apache.org/jira/browse/HDFS-11369) | Change exception message in StorageLocationChecker |  Minor | datanode | Arpit Agarwal | Arpit Agarwal |
| [YARN-4975](https://issues.apache.org/jira/browse/YARN-4975) | Fair Scheduler: exception thrown when a parent queue marked 'parent' has configured child queues |  Major | fairscheduler | Ashwin Shankar | Yufei Gu |
| [HDFS-11364](https://issues.apache.org/jira/browse/HDFS-11364) | Add a test to verify Audit log entries for setfacl/getfacl commands over FS shell |  Major | hdfs, test | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-11376](https://issues.apache.org/jira/browse/HDFS-11376) | Revert HDFS-8377 Support HTTP/2 in datanode |  Major | datanode | Andrew Wang | Xiao Chen |
| [HADOOP-13988](https://issues.apache.org/jira/browse/HADOOP-13988) | KMSClientProvider does not work with WebHDFS and Apache Knox w/ProxyUser |  Major | common, kms | Greg Senia | Xiaoyu Yao |
| [HADOOP-14029](https://issues.apache.org/jira/browse/HADOOP-14029) | Fix KMSClientProvider for non-secure proxyuser use case |  Major | kms | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-5641](https://issues.apache.org/jira/browse/YARN-5641) | Localizer leaves behind tarballs after container is complete |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-13992](https://issues.apache.org/jira/browse/HADOOP-13992) | KMS should load SSL configuration the same way as SSLFactory |  Major | kms, security | John Zhuge | John Zhuge |
| [HDFS-11378](https://issues.apache.org/jira/browse/HDFS-11378) | Verify multiple DataNodes can be decommissioned/maintenance at the same time |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-6103](https://issues.apache.org/jira/browse/YARN-6103) | Log updates for ZKRMStateStore |  Trivial | . | Bibin A Chundatt | Daniel Sturman |
| [HADOOP-14018](https://issues.apache.org/jira/browse/HADOOP-14018) | shaded jars of hadoop-client modules are missing hadoop's root LICENSE and NOTICE files |  Critical | . | John Zhuge | Elek, Marton |
| [HDFS-11335](https://issues.apache.org/jira/browse/HDFS-11335) | Remove HdfsClientConfigKeys.DFS\_CLIENT\_SLOW\_IO\_WARNING\_THRESHOLD\_KEY usage from DNConf |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-13895](https://issues.apache.org/jira/browse/HADOOP-13895) | Make FileStatus Serializable |  Minor | fs | Chris Douglas | Chris Douglas |
| [HADOOP-14045](https://issues.apache.org/jira/browse/HADOOP-14045) | Aliyun OSS documentation missing from website |  Major | documentation, fs/oss | Andrew Wang | Yiqun Lin |
| [HDFS-11363](https://issues.apache.org/jira/browse/HDFS-11363) | Need more diagnosis info when seeing Slow waitForAckedSeqno |  Major | . | Yongjun Zhang | Xiao Chen |
| [HDFS-11387](https://issues.apache.org/jira/browse/HDFS-11387) | Socket reuse address option is not honored in PrivilegedNfsGatewayStarter |  Major | nfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-14044](https://issues.apache.org/jira/browse/HADOOP-14044) | Synchronization issue in delegation token cancel functionality |  Major | . | Hrishikesh Gadre | Hrishikesh Gadre |
| [HDFS-11371](https://issues.apache.org/jira/browse/HDFS-11371) | Document missing metrics of erasure coding |  Minor | documentation, erasure-coding | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-6338](https://issues.apache.org/jira/browse/MAPREDUCE-6338) | MR AppMaster does not honor ephemeral port range |  Major | mr-am, mrv2 | Frank Nguyen | Frank Nguyen |
| [HDFS-11377](https://issues.apache.org/jira/browse/HDFS-11377) | Balancer hung due to no available mover threads |  Major | balancer & mover | yunjiong zhao | yunjiong zhao |
| [HADOOP-14047](https://issues.apache.org/jira/browse/HADOOP-14047) | Require admin to access KMS instrumentation servlets |  Minor | kms | John Zhuge | John Zhuge |
| [YARN-6135](https://issues.apache.org/jira/browse/YARN-6135) | Node manager REST API documentation is not up to date |  Trivial | nodemanager, restapi | Miklos Szegedi | Miklos Szegedi |
| [YARN-6145](https://issues.apache.org/jira/browse/YARN-6145) | Improve log message on fail over |  Major | . | Jian He | Jian He |
| [YARN-6031](https://issues.apache.org/jira/browse/YARN-6031) | Application recovery has failed when node label feature is turned off during RM recovery |  Minor | scheduler | Ying Zhang | Ying Zhang |
| [YARN-6137](https://issues.apache.org/jira/browse/YARN-6137) | Yarn client implicitly invoke ATS client which accesses HDFS |  Major | . | Yesha Vora | Li Lu |
| [HADOOP-13433](https://issues.apache.org/jira/browse/HADOOP-13433) | Race in UGI.reloginFromKeytab |  Major | security | Duo Zhang | Duo Zhang |
| [YARN-6112](https://issues.apache.org/jira/browse/YARN-6112) | UpdateCallDuration is calculated only when debug logging is enabled |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-6144](https://issues.apache.org/jira/browse/YARN-6144) | FairScheduler: preempted resources can become negative |  Blocker | fairscheduler, resourcemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-6118](https://issues.apache.org/jira/browse/YARN-6118) | Add javadoc for Resources.isNone |  Minor | scheduler | Karthik Kambatla | Andres Perez |
| [YARN-6166](https://issues.apache.org/jira/browse/YARN-6166) | Unnecessary INFO logs in AMRMClientAsyncImpl$CallbackHandlerThread.run |  Trivial | . | Grant W | Grant W |
| [HADOOP-14055](https://issues.apache.org/jira/browse/HADOOP-14055) | SwiftRestClient includes pass length in exception if auth fails |  Minor | security | Marcell Hegedus | Marcell Hegedus |
| [HDFS-11403](https://issues.apache.org/jira/browse/HDFS-11403) | Zookeper ACLs on NN HA enabled clusters to be handled consistently |  Major | hdfs | Laszlo Puskas | Hanisha Koneru |
| [HADOOP-13233](https://issues.apache.org/jira/browse/HADOOP-13233) | help of stat is confusing |  Trivial | documentation, fs | Xiaohe Lan | Attila Bukor |
| [YARN-3933](https://issues.apache.org/jira/browse/YARN-3933) | FairScheduler: Multiple calls to completedContainer are not safe |  Major | fairscheduler | Lavkesh Lahngir | Shiwei Guo |
| [HDFS-11407](https://issues.apache.org/jira/browse/HDFS-11407) | Document the missing usages of OfflineImageViewer processors |  Minor | documentation, tools | Yiqun Lin | Yiqun Lin |
| [HDFS-11408](https://issues.apache.org/jira/browse/HDFS-11408) | The config name of balance bandwidth is out of date |  Minor | balancer & mover, documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-14058](https://issues.apache.org/jira/browse/HADOOP-14058) | Fix NativeS3FileSystemContractBaseTest#testDirWithDifferentMarkersWorks |  Minor | fs/s3, test | Akira Ajisaka | Yiqun Lin |
| [HDFS-11084](https://issues.apache.org/jira/browse/HDFS-11084) | Add a regression test for sticky bit support of OIV ReverseXML processor |  Major | tools | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11391](https://issues.apache.org/jira/browse/HDFS-11391) | Numeric usernames do no work with WebHDFS FS (write access) |  Major | webhdfs | Pierre Villard | Pierre Villard |
| [HADOOP-13924](https://issues.apache.org/jira/browse/HADOOP-13924) | Update checkstyle and checkstyle plugin version to handle indentation of JDK8 Lambdas |  Major | . | Xiaoyu Yao | Akira Ajisaka |
| [HDFS-11238](https://issues.apache.org/jira/browse/HDFS-11238) | Fix checkstyle warnings in NameNode#createNameNode |  Trivial | namenode | Ethan Li | Ethan Li |
| [YARN-4212](https://issues.apache.org/jira/browse/YARN-4212) | FairScheduler: Can't create a DRF queue under a FAIR policy queue |  Major | . | Arun Suresh | Yufei Gu |
| [YARN-6177](https://issues.apache.org/jira/browse/YARN-6177) | Yarn client should exit with an informative error message if an incompatible Jersey library is used at client |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-6171](https://issues.apache.org/jira/browse/YARN-6171) | ConcurrentModificationException on FSAppAttempt.containersToPreempt |  Major | fairscheduler | Miklos Szegedi | Miklos Szegedi |
| [HDFS-11410](https://issues.apache.org/jira/browse/HDFS-11410) | Use the cached instance when edit logging SetAclOp, SetXAttrOp and RemoveXAttrOp |  Major | namenode | Xiao Chen | Xiao Chen |
| [YARN-6188](https://issues.apache.org/jira/browse/YARN-6188) | Fix OOM issue with decommissioningNodesWatcher in the case of clusters with large number of nodes |  Major | resourcemanager | Ajay Jadhav | Ajay Jadhav |
| [HDFS-11379](https://issues.apache.org/jira/browse/HDFS-11379) | DFSInputStream may infinite loop requesting block locations |  Critical | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HADOOP-14092](https://issues.apache.org/jira/browse/HADOOP-14092) | Typo in hadoop-aws index.md |  Trivial | fs/s3 | John Zhuge | John Zhuge |
| [HDFS-11177](https://issues.apache.org/jira/browse/HDFS-11177) | 'storagepolicies -getStoragePolicy' command should accept URI based path. |  Major | shell | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11404](https://issues.apache.org/jira/browse/HDFS-11404) | Increase timeout on TestShortCircuitLocalRead.testDeprecatedGetBlockLocalPathInfoRpc |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-13826](https://issues.apache.org/jira/browse/HADOOP-13826) | S3A Deadlock in multipart copy due to thread pool limits. |  Critical | fs/s3 | Sean Mackrory | Sean Mackrory |
| [HADOOP-14017](https://issues.apache.org/jira/browse/HADOOP-14017) | User friendly name for ADLS user and group |  Major | fs/adl | John Zhuge | Vishwajeet Dusane |
| [MAPREDUCE-6825](https://issues.apache.org/jira/browse/MAPREDUCE-6825) | YARNRunner#createApplicationSubmissionContext method is longer than 150 lines |  Trivial | . | Chris Trezzo | Gergely Novák |
| [YARN-6210](https://issues.apache.org/jira/browse/YARN-6210) | FS: Node reservations can interfere with preemption |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-6211](https://issues.apache.org/jira/browse/YARN-6211) | Synchronization improvement for moveApplicationAcrossQueues and updateApplicationPriority |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-6222](https://issues.apache.org/jira/browse/YARN-6222) | TestFairScheduler.testReservationMetrics is flaky |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [HADOOP-14114](https://issues.apache.org/jira/browse/HADOOP-14114) | S3A can no longer handle unencoded + in URIs |  Minor | fs/s3 | Sean Mackrory | Sean Mackrory |
| [HADOOP-14116](https://issues.apache.org/jira/browse/HADOOP-14116) | FailoverOnNetworkExceptionRetry does not wait when failover on certain exception |  Major | . | Jian He | Jian He |
| [HDFS-11433](https://issues.apache.org/jira/browse/HDFS-11433) | Document missing usages of OfflineEditsViewer processors |  Minor | documentation, tools | Yiqun Lin | Yiqun Lin |
| [HDFS-11462](https://issues.apache.org/jira/browse/HDFS-11462) | Fix occasional BindException in TestNameNodeMetricsLogger |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-14028](https://issues.apache.org/jira/browse/HADOOP-14028) | S3A BlockOutputStreams doesn't delete temporary files in multipart uploads or handle part upload failures |  Critical | fs/s3 | Seth Fitzsimmons | Steve Loughran |
| [YARN-6172](https://issues.apache.org/jira/browse/YARN-6172) | FSLeafQueue demand update needs to be atomic |  Major | resourcemanager | Varun Saxena | Miklos Szegedi |
| [HADOOP-14119](https://issues.apache.org/jira/browse/HADOOP-14119) | Remove unused imports from GzipCodec.java |  Minor | . | Akira Ajisaka | Yiqun Lin |
| [MAPREDUCE-6841](https://issues.apache.org/jira/browse/MAPREDUCE-6841) | Fix dead link in MapReduce tutorial document |  Minor | documentation | Akira Ajisaka | Victor Nee |
| [YARN-6231](https://issues.apache.org/jira/browse/YARN-6231) | FairSchedulerTestBase helper methods should call scheduler.update to avoid flakiness |  Major | . | Arun Suresh | Karthik Kambatla |
| [YARN-6239](https://issues.apache.org/jira/browse/YARN-6239) | Fix javac warnings in YARN that caused by deprecated FileSystem APIs |  Minor | . | Yiqun Lin | Yiqun Lin |
| [YARN-1728](https://issues.apache.org/jira/browse/YARN-1728) | Workaround guice3x-undecoded pathInfo in YARN WebApp |  Major | . | Abraham Elmahrek | Yuanbo Liu |
| [HADOOP-12556](https://issues.apache.org/jira/browse/HADOOP-12556) | KafkaSink jar files are created but not copied to target dist |  Major | . | Babak Behzad | Babak Behzad |
| [HDFS-11479](https://issues.apache.org/jira/browse/HDFS-11479) | Socket re-use address option should be used in SimpleUdpServer |  Major | nfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-11478](https://issues.apache.org/jira/browse/HDFS-11478) | Update EC commands in HDFSCommands.md |  Minor | documentation, erasure-coding | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-6852](https://issues.apache.org/jira/browse/MAPREDUCE-6852) | Job#updateStatus() failed with NPE due to race condition |  Major | . | Junping Du | Junping Du |
| [MAPREDUCE-6753](https://issues.apache.org/jira/browse/MAPREDUCE-6753) | Variable in byte printed directly in mapreduce client |  Major | client | Nemo Chen | Kai Sasaki |
| [HADOOP-6801](https://issues.apache.org/jira/browse/HADOOP-6801) | io.sort.mb and io.sort.factor were renamed and moved to mapreduce but are still in CommonConfigurationKeysPublic.java and used in SequenceFile.java |  Minor | . | Erik Steffl | Harsh J |
| [YARN-6263](https://issues.apache.org/jira/browse/YARN-6263) | NMTokenSecretManagerInRM.createAndGetNMToken is not thread safe |  Major | yarn | Haibo Chen | Haibo Chen |
| [YARN-6218](https://issues.apache.org/jira/browse/YARN-6218) | Fix TestAMRMClient when using FairScheduler |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-11476](https://issues.apache.org/jira/browse/HDFS-11476) | Fix NPE in FsDatasetImpl#checkAndUpdate |  Major | datanode | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-6271](https://issues.apache.org/jira/browse/YARN-6271) | yarn rmadin -getGroups returns information from standby RM |  Critical | yarn | Sumana Sathish | Jian He |
| [YARN-6270](https://issues.apache.org/jira/browse/YARN-6270) | WebUtils.getRMWebAppURLWithScheme() needs to honor RM HA setting |  Major | . | Sumana Sathish | Xuan Gong |
| [YARN-6248](https://issues.apache.org/jira/browse/YARN-6248) | user is not removed from UsersManager’s when app is killed with pending container requests. |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-14026](https://issues.apache.org/jira/browse/HADOOP-14026) | start-build-env.sh: invalid docker image name |  Major | build | Gergő Pásztor | Gergő Pásztor |
| [HDFS-11441](https://issues.apache.org/jira/browse/HDFS-11441) | Add escaping to error message in KMS web UI |  Minor | security | Aaron T. Myers | Aaron T. Myers |
| [YARN-5665](https://issues.apache.org/jira/browse/YARN-5665) | Enhance documentation for yarn.resourcemanager.scheduler.class property |  Trivial | documentation | Miklos Szegedi | Yufei Gu |
| [HDFS-11498](https://issues.apache.org/jira/browse/HDFS-11498) | Make RestCsrfPreventionHandler and WebHdfsHandler compatible with Netty 4.0 |  Major | . | Andrew Wang | Andrew Wang |
| [MAPREDUCE-6855](https://issues.apache.org/jira/browse/MAPREDUCE-6855) | Specify charset when create String in CredentialsTestJob |  Minor | . | Akira Ajisaka | Kai Sasaki |
| [HADOOP-14087](https://issues.apache.org/jira/browse/HADOOP-14087) | S3A typo in pom.xml test exclusions |  Major | fs/s3 | Aaron Fabbri | Aaron Fabbri |
| [HDFS-11508](https://issues.apache.org/jira/browse/HDFS-11508) | Fix bind failure in SimpleTCPServer & Portmap where bind fails because socket is in TIME\_WAIT state |  Major | nfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [MAPREDUCE-6839](https://issues.apache.org/jira/browse/MAPREDUCE-6839) | TestRecovery.testCrashed failed |  Major | test | Gergő Pásztor | Gergő Pásztor |
| [YARN-6207](https://issues.apache.org/jira/browse/YARN-6207) | Move application across queues should handle delayed event processing |  Major | capacity scheduler | Bibin A Chundatt | Bibin A Chundatt |
| [MAPREDUCE-6859](https://issues.apache.org/jira/browse/MAPREDUCE-6859) | hadoop-mapreduce-client-jobclient.jar sets a main class that isn't in the JAR |  Minor | client | Daniel Templeton | Daniel Templeton |
| [YARN-6297](https://issues.apache.org/jira/browse/YARN-6297) | TestAppLogAggregatorImp.verifyFilesUploaded() should check # of filed uploaded with that of files expected |  Major | . | Haibo Chen | Haibo Chen |
| [YARN-6165](https://issues.apache.org/jira/browse/YARN-6165) | Intra-queue preemption occurs even when preemption is turned off for a specific queue. |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HADOOP-14052](https://issues.apache.org/jira/browse/HADOOP-14052) | Fix dead link in KMS document |  Minor | documentation | Akira Ajisaka | Christina Vu |
| [YARN-6264](https://issues.apache.org/jira/browse/YARN-6264) | AM not launched when a single vcore is available on the cluster |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-6310](https://issues.apache.org/jira/browse/YARN-6310) | OutputStreams in AggregatedLogFormat.LogWriter can be left open upon exceptions |  Major | yarn | Haibo Chen | Haibo Chen |
| [HADOOP-14062](https://issues.apache.org/jira/browse/HADOOP-14062) | ApplicationMasterProtocolPBClientImpl.allocate fails with EOFException when RPC privacy is enabled |  Critical | . | Steven Rand | Steven Rand |
| [YARN-6321](https://issues.apache.org/jira/browse/YARN-6321) | TestResources test timeouts are too aggressive |  Major | test | Jason Lowe | Eric Badger |
| [HDFS-11340](https://issues.apache.org/jira/browse/HDFS-11340) | DataNode reconfigure for disks doesn't remove the failed volumes |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-14156](https://issues.apache.org/jira/browse/HADOOP-14156) | Fix grammar error in ConfTest.java |  Trivial | test | Andrey Dyatlov | Andrey Dyatlov |
| [HDFS-11512](https://issues.apache.org/jira/browse/HDFS-11512) | Increase timeout on TestShortCircuitLocalRead#testSkipWithVerifyChecksum |  Minor | . | Eric Badger | Eric Badger |
| [HDFS-11499](https://issues.apache.org/jira/browse/HDFS-11499) | Decommissioning stuck because of failing recovery |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-11395](https://issues.apache.org/jira/browse/HDFS-11395) | RequestHedgingProxyProvider#RequestHedgingInvocationHandler hides the Exception thrown from NameNode |  Major | ha | Nanda kumar | Nanda kumar |
| [HDFS-11526](https://issues.apache.org/jira/browse/HDFS-11526) | Fix confusing block recovery message |  Minor | datanode | Wei-Chiu Chuang | Yiqun Lin |
| [YARN-6327](https://issues.apache.org/jira/browse/YARN-6327) | Removing queues from CapacitySchedulerQueueManager and ParentQueue should be done with iterator |  Major | capacityscheduler | Jonathan Hung | Jonathan Hung |
| [HADOOP-14170](https://issues.apache.org/jira/browse/HADOOP-14170) | FileSystemContractBaseTest is not cleaning up test directory clearly |  Major | fs | Mingliang Liu | Mingliang Liu |
| [YARN-6331](https://issues.apache.org/jira/browse/YARN-6331) | Fix flakiness in TestFairScheduler#testDumpState |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-6328](https://issues.apache.org/jira/browse/YARN-6328) | Fix a spelling mistake in CapacityScheduler |  Trivial | capacity scheduler | Jin Yibo | Jin Yibo |
| [HDFS-11420](https://issues.apache.org/jira/browse/HDFS-11420) | Edit file should not be processed by the same type processor in OfflineEditsViewer |  Major | tools | Yiqun Lin | Yiqun Lin |
| [YARN-6294](https://issues.apache.org/jira/browse/YARN-6294) | ATS client should better handle Socket closed case |  Major | timelineclient | Sumana Sathish | Li Lu |
| [YARN-6332](https://issues.apache.org/jira/browse/YARN-6332) | Make RegistrySecurity use short user names for ZK ACLs |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-4051](https://issues.apache.org/jira/browse/YARN-4051) | ContainerKillEvent lost when container is still recovering and application finishes |  Critical | nodemanager | sandflee | sandflee |
| [HDFS-11533](https://issues.apache.org/jira/browse/HDFS-11533) | reuseAddress option should be used for child channels in Portmap and SimpleTcpServer |  Major | nfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-14191](https://issues.apache.org/jira/browse/HADOOP-14191) | Duplicate hadoop-minikdc dependency in hadoop-common module |  Minor | build | Akira Ajisaka | Xiaobing Zhou |
| [HDFS-10394](https://issues.apache.org/jira/browse/HDFS-10394) | move declaration of okhttp version from hdfs-client to hadoop-project POM |  Minor | build | Steve Loughran | Xiaobing Zhou |
| [HDFS-11516](https://issues.apache.org/jira/browse/HDFS-11516) | Admin command line should print message to stderr in failure case |  Minor | . | Kai Sasaki | Kai Sasaki |
| [YARN-6217](https://issues.apache.org/jira/browse/YARN-6217) | TestLocalCacheDirectoryManager test timeout is too aggressive |  Major | test | Jason Lowe | Miklos Szegedi |
| [YARN-6353](https://issues.apache.org/jira/browse/YARN-6353) | Clean up OrderingPolicy javadoc |  Minor | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HADOOP-14059](https://issues.apache.org/jira/browse/HADOOP-14059) | typo in s3a rename(self, subdir) error message |  Minor | . | Steve Loughran | Steve Loughran |
| [HDFS-6648](https://issues.apache.org/jira/browse/HDFS-6648) | Order of namenodes in ConfiguredFailoverProxyProvider is undefined |  Major | ha, hdfs-client | Rafal Wojdyla | Íñigo Goiri |
| [HADOOP-14204](https://issues.apache.org/jira/browse/HADOOP-14204) | S3A multipart commit failing, "UnsupportedOperationException at java.util.Collections$UnmodifiableList.sort" |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-14187](https://issues.apache.org/jira/browse/HADOOP-14187) | Update ZooKeeper dependency to 3.4.9 and Curator dependency to 2.12.0 |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-5934](https://issues.apache.org/jira/browse/YARN-5934) | Fix TestTimelineWebServices.testPrimaryFilterNumericString |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11561](https://issues.apache.org/jira/browse/HDFS-11561) | HttpFS doc errors |  Trivial | documentation, httpfs, test | Yuanbo Liu | Yuanbo Liu |
| [HADOOP-9631](https://issues.apache.org/jira/browse/HADOOP-9631) | ViewFs should use underlying FileSystem's server side defaults |  Major | fs, viewfs | Lohit Vijayarenu | Erik Krogen |
| [HADOOP-14214](https://issues.apache.org/jira/browse/HADOOP-14214) | DomainSocketWatcher::add()/delete() should not self interrupt while looping await() |  Critical | hdfs-client | Mingliang Liu | Mingliang Liu |
| [HADOOP-14195](https://issues.apache.org/jira/browse/HADOOP-14195) | CredentialProviderFactory$getProviders is not thread-safe |  Major | security | Vihang Karajgaonkar | Vihang Karajgaonkar |
| [HADOOP-14211](https://issues.apache.org/jira/browse/HADOOP-14211) | FilterFs and ChRootedFs are too aggressive about enforcing "authorityNeeded" |  Major | viewfs | Erik Krogen | Erik Krogen |
| [YARN-6360](https://issues.apache.org/jira/browse/YARN-6360) | Prevent FS state dump logger from cramming other log files |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-6334](https://issues.apache.org/jira/browse/YARN-6334) | TestRMFailover#testAutomaticFailover always passes even when it should fail |  Major | . | Yufei Gu | Yufei Gu |
| [MAPREDUCE-6866](https://issues.apache.org/jira/browse/MAPREDUCE-6866) | Fix getNumMapTasks() documentation in JobConf |  Minor | documentation | Joe Mészáros | Joe Mészáros |
| [MAPREDUCE-6868](https://issues.apache.org/jira/browse/MAPREDUCE-6868) | License check for jdiff output files should be ignored |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11555](https://issues.apache.org/jira/browse/HDFS-11555) | Fix typos in class OfflineImageReconstructor |  Trivial | . | Yiqun Lin | Yiqun Lin |
| [HDFS-10506](https://issues.apache.org/jira/browse/HDFS-10506) | OIV's ReverseXML processor cannot reconstruct some snapshot details |  Major | tools | Colin P. McCabe | Akira Ajisaka |
| [HDFS-11486](https://issues.apache.org/jira/browse/HDFS-11486) | Client close() should not fail fast if the last block is being decommissioned |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-6359](https://issues.apache.org/jira/browse/YARN-6359) | TestRM#testApplicationKillAtAcceptedState fails rarely due to race condition |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-5368](https://issues.apache.org/jira/browse/YARN-5368) | Memory leak in timeline server |  Critical | timelineserver | Wataru Yukawa | Jonathan Eagles |
| [YARN-6050](https://issues.apache.org/jira/browse/YARN-6050) | AMs can't be scheduled on racks or nodes |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-11571](https://issues.apache.org/jira/browse/HDFS-11571) | Typo in DataStorage exception message |  Minor | datanode | Daniel Templeton | Anna Budai |
| [YARN-5685](https://issues.apache.org/jira/browse/YARN-5685) | RM configuration allows all failover methods to disabled when automatic failover is enabled |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HADOOP-14223](https://issues.apache.org/jira/browse/HADOOP-14223) | Extend FileStatus#toString() to include details like Erasure Coding and Encryption |  Major | fs | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-14247](https://issues.apache.org/jira/browse/HADOOP-14247) | FileContextMainOperationsBaseTest should clean up test root path |  Minor | fs, test | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-6862](https://issues.apache.org/jira/browse/MAPREDUCE-6862) | Fragments are not handled correctly by resource limit checking |  Minor | . | Chris Trezzo | Chris Trezzo |
| [MAPREDUCE-6873](https://issues.apache.org/jira/browse/MAPREDUCE-6873) | MR Job Submission Fails if MR framework application path not on defaultFS |  Minor | mrv2 | Erik Krogen | Erik Krogen |
| [HADOOP-14256](https://issues.apache.org/jira/browse/HADOOP-14256) | [S3A DOC] Correct the format for "Seoul" example |  Minor | documentation, fs/s3 | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6850](https://issues.apache.org/jira/browse/MAPREDUCE-6850) | Shuffle Handler keep-alive connections are closed from the server side |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-6836](https://issues.apache.org/jira/browse/MAPREDUCE-6836) | exception thrown when accessing the job configuration web UI |  Minor | webapps | Sangjin Lee | Haibo Chen |
| [HDFS-11592](https://issues.apache.org/jira/browse/HDFS-11592) | Closing a file has a wasteful preconditions in NameNode |  Major | namenode | Eric Badger | Eric Badger |
| [YARN-6354](https://issues.apache.org/jira/browse/YARN-6354) | LeveldbRMStateStore can parse invalid keys when recovering reservations |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-5703](https://issues.apache.org/jira/browse/YARN-5703) | ReservationAgents are not correctly configured |  Major | capacity scheduler, resourcemanager | Sean Po | Manikandan R |
| [HADOOP-14268](https://issues.apache.org/jira/browse/HADOOP-14268) | Fix markdown itemization in hadoop-aws documents |  Minor | documentation, fs/s3 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14271](https://issues.apache.org/jira/browse/HADOOP-14271) | Correct spelling of 'occurred' and variants |  Trivial | . | Yeliang Cang | Yeliang Cang |
| [HADOOP-14272](https://issues.apache.org/jira/browse/HADOOP-14272) | Azure: WasbRemoteCallHelper should use String equals for comparison. |  Major | fs/azure | Santhosh G Nayak | Santhosh G Nayak |
| [HADOOP-14273](https://issues.apache.org/jira/browse/HADOOP-14273) | Azure: NativeAzureFileSystem should respect config for kerberosSupportEnabled flag |  Major | fs/azure | Santhosh G Nayak | Santhosh G Nayak |
| [YARN-6436](https://issues.apache.org/jira/browse/YARN-6436) | TestSchedulingPolicy#testParseSchedulingPolicy timeout is too low |  Major | test | Jason Lowe | Eric Badger |
| [YARN-6004](https://issues.apache.org/jira/browse/YARN-6004) | Refactor TestResourceLocalizationService#testDownloadingResourcesOnContainer so that it is less than 150 lines |  Trivial | test | Chris Trezzo | Chris Trezzo |
| [YARN-6420](https://issues.apache.org/jira/browse/YARN-6420) | RM startup failure due to wrong order in nodelabel editlog |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [MAPREDUCE-6824](https://issues.apache.org/jira/browse/MAPREDUCE-6824) | TaskAttemptImpl#createCommonContainerLaunchContext is longer than 150 lines |  Trivial | . | Chris Trezzo | Chris Trezzo |
| [YARN-6403](https://issues.apache.org/jira/browse/YARN-6403) | Invalid local resource request can raise NPE and make NM exit |  Major | nodemanager | Tao Yang | Tao Yang |
| [HDFS-11538](https://issues.apache.org/jira/browse/HDFS-11538) | Move ClientProtocol HA proxies into hadoop-hdfs-client |  Blocker | hdfs-client | Andrew Wang | Huafeng Wang |
| [YARN-6437](https://issues.apache.org/jira/browse/YARN-6437) | TestSignalContainer#testSignalRequestDeliveryToNM fails intermittently |  Major | test | Jason Lowe | Jason Lowe |
| [YARN-6448](https://issues.apache.org/jira/browse/YARN-6448) | Continuous scheduling thread crashes while sorting nodes |  Major | . | Yufei Gu | Yufei Gu |
| [HDFS-11596](https://issues.apache.org/jira/browse/HDFS-11596) | hadoop-hdfs-client jar is in the wrong directory in release tarball |  Critical | build | Andrew Wang | Yuanbo Liu |
| [MAPREDUCE-6846](https://issues.apache.org/jira/browse/MAPREDUCE-6846) | Fragments specified for libjar paths are not handled correctly |  Minor | . | Chris Trezzo | Chris Trezzo |
| [HDFS-11131](https://issues.apache.org/jira/browse/HDFS-11131) | TestThrottledAsyncChecker#testCancellation is flaky |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-11362](https://issues.apache.org/jira/browse/HDFS-11362) | StorageDirectory should initialize a non-null default StorageDirType |  Minor | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11608](https://issues.apache.org/jira/browse/HDFS-11608) | HDFS write crashed with block size greater than 2 GB |  Critical | hdfs-client | Xiaobing Zhou | Xiaobing Zhou |
| [MAPREDUCE-6201](https://issues.apache.org/jira/browse/MAPREDUCE-6201) | TestNetworkedJob fails on trunk |  Major | . | Robert Kanter | Peter Bacsko |
| [YARN-6288](https://issues.apache.org/jira/browse/YARN-6288) | Exceptions during aggregated log writes are mishandled |  Critical | log-aggregation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14287](https://issues.apache.org/jira/browse/HADOOP-14287) | Compiling trunk with -DskipShade fails |  Major | build | Arpit Agarwal | Arun Suresh |
| [YARN-6368](https://issues.apache.org/jira/browse/YARN-6368) | Decommissioning an NM results in a -1 exit code |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-11633](https://issues.apache.org/jira/browse/HDFS-11633) | FSImage failover disables all erasure coding policies |  Critical | erasure-coding, namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-14066](https://issues.apache.org/jira/browse/HADOOP-14066) | VersionInfo should be marked as public API |  Critical | common | Thejas M Nair | Akira Ajisaka |
| [YARN-6343](https://issues.apache.org/jira/browse/YARN-6343) | Docker docs MR example is broken |  Major | nodemanager | Daniel Templeton | Prashant Jha |
| [HADOOP-14293](https://issues.apache.org/jira/browse/HADOOP-14293) | Initialize FakeTimer with a less trivial value |  Major | test | Andrew Wang | Andrew Wang |
| [HADOOP-13545](https://issues.apache.org/jira/browse/HADOOP-13545) | Upgrade HSQLDB to 2.3.4 |  Minor | build | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-11637](https://issues.apache.org/jira/browse/HDFS-11637) | Fix javac warning caused by the deprecated key used in TestDFSClientRetries#testFailuresArePerOperation |  Minor | . | Yiqun Lin | Yiqun Lin |
| [YARN-6461](https://issues.apache.org/jira/browse/YARN-6461) | TestRMAdminCLI has very low test timeouts |  Major | test | Jason Lowe | Eric Badger |
| [YARN-6463](https://issues.apache.org/jira/browse/YARN-6463) | correct spelling mistake in FileSystemRMStateStore |  Trivial | . | Yeliang Cang | Yeliang Cang |
| [YARN-6439](https://issues.apache.org/jira/browse/YARN-6439) | Fix ReservationSystem creation of default ReservationQueue |  Major | . | Carlo Curino | Carlo Curino |
| [HDFS-11630](https://issues.apache.org/jira/browse/HDFS-11630) | TestThrottledAsyncCheckerTimeout fails intermittently in Jenkins builds |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11163](https://issues.apache.org/jira/browse/HDFS-11163) | Mover should move the file blocks to default storage once policy is unset |  Major | balancer & mover | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-6421](https://issues.apache.org/jira/browse/YARN-6421) | Upgrade frontend-maven-plugin to 1.1 to fix new YARN UI build error in ppc64le |  Major | yarn-ui-v2 | Sonia Garudi | Sonia Garudi |
| [YARN-6450](https://issues.apache.org/jira/browse/YARN-6450) | TestContainerManagerWithLCE requires override for each new test added to ContainerManagerTest |  Major | test | Jason Lowe | Jason Lowe |
| [YARN-3760](https://issues.apache.org/jira/browse/YARN-3760) | FSDataOutputStream leak in AggregatedLogFormat.LogWriter.close() |  Critical | nodemanager | Daryn Sharp | Haibo Chen |
| [YARN-6216](https://issues.apache.org/jira/browse/YARN-6216) | Unify Container Resizing code paths with Container Updates making it scheduler agnostic |  Major | capacity scheduler, fairscheduler, resourcemanager | Arun Suresh | Arun Suresh |
| [YARN-5994](https://issues.apache.org/jira/browse/YARN-5994) | TestCapacityScheduler.testAMLimitUsage fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [YARN-6433](https://issues.apache.org/jira/browse/YARN-6433) | Only accessible cgroup mount directories should be selected for a controller |  Major | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-6480](https://issues.apache.org/jira/browse/YARN-6480) | Timeout is too aggressive for TestAMRestart.testPreemptedAMRestartOnRMRestart |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14311](https://issues.apache.org/jira/browse/HADOOP-14311) | Add python2.7-dev to Dockerfile |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6875](https://issues.apache.org/jira/browse/MAPREDUCE-6875) | Rename mapred-site.xml.template to mapred-site.xml |  Minor | build | Allen Wittenauer | Yuanbo Liu |
| [YARN-6304](https://issues.apache.org/jira/browse/YARN-6304) | Skip rm.transitionToActive call to RM if RM is already active. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-11615](https://issues.apache.org/jira/browse/HDFS-11615) | FSNamesystemLock metrics can be inaccurate due to millisecond precision |  Major | hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-14318](https://issues.apache.org/jira/browse/HADOOP-14318) | Remove non-existent setfattr command option from FileSystemShell.md |  Minor | documentation | Doris Gu | Doris Gu |
| [HADOOP-14315](https://issues.apache.org/jira/browse/HADOOP-14315) | Python example in the rack awareness document doesn't work due to bad indentation |  Minor | documentation | Kengo Seki | Kengo Seki |
| [HDFS-11665](https://issues.apache.org/jira/browse/HDFS-11665) | HttpFSServerWebServer$deprecateEnv may leak secret |  Major | httpfs, security | John Zhuge | John Zhuge |
| [HADOOP-14317](https://issues.apache.org/jira/browse/HADOOP-14317) | KMSWebServer$deprecateEnv may leak secret |  Major | kms, security | John Zhuge | John Zhuge |
| [HADOOP-13997](https://issues.apache.org/jira/browse/HADOOP-13997) | Typo in metrics docs |  Trivial | documentation | Daniel Templeton | Ana Krasteva |
| [YARN-6438](https://issues.apache.org/jira/browse/YARN-6438) | Code can be improved in ContainersMonitorImpl.java |  Minor | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-6365](https://issues.apache.org/jira/browse/YARN-6365) | Get static SLS html resources from classpath |  Blocker | scheduler-load-simulator | Allen Wittenauer | Yufei Gu |
| [YARN-6202](https://issues.apache.org/jira/browse/YARN-6202) | Configuration item Dispatcher.DISPATCHER\_EXIT\_ON\_ERROR\_KEY is disregarded |  Major | nodemanager, resourcemanager | Yufei Gu | Yufei Gu |
| [YARN-6302](https://issues.apache.org/jira/browse/YARN-6302) | Fail the node if Linux Container Executor is not configured properly |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-11671](https://issues.apache.org/jira/browse/HDFS-11671) | TestReconstructStripedBlocks#test2RecoveryTasksForSameBlockGroup fails |  Major | erasure-coding, test | Andrew Wang | Andrew Wang |
| [HDFS-11660](https://issues.apache.org/jira/browse/HDFS-11660) | TestFsDatasetCache#testPageRounder fails intermittently with AssertionError |  Major | test | Andrew Wang | Andrew Wang |
| [YARN-6453](https://issues.apache.org/jira/browse/YARN-6453) | fairscheduler-statedump.log gets generated regardless of service |  Blocker | fairscheduler, scheduler | Allen Wittenauer | Yufei Gu |
| [YARN-6363](https://issues.apache.org/jira/browse/YARN-6363) | Extending SLS: Synthetic Load Generator |  Major | . | Carlo Curino | Carlo Curino |
| [YARN-6153](https://issues.apache.org/jira/browse/YARN-6153) | keepContainer does not work when AM retry window is set |  Major | resourcemanager | kyungwan nam | kyungwan nam |
| [HDFS-11689](https://issues.apache.org/jira/browse/HDFS-11689) | New exception thrown by DFSClient#isHDFSEncryptionEnabled broke hacky hive code |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [YARN-5889](https://issues.apache.org/jira/browse/YARN-5889) | Improve and refactor user-limit calculation in capacity scheduler |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [HDFS-11529](https://issues.apache.org/jira/browse/HDFS-11529) | Add libHDFS API to return last exception |  Critical | libhdfs | Sailesh Mukil | Sailesh Mukil |
| [YARN-6500](https://issues.apache.org/jira/browse/YARN-6500) | Do not mount inaccessible cgroups directories in CgroupsLCEResourcesHandler |  Major | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [HDFS-11691](https://issues.apache.org/jira/browse/HDFS-11691) | Add a proper scheme to the datanode links in NN web UI |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-14341](https://issues.apache.org/jira/browse/HADOOP-14341) | Support multi-line value for ssl.server.exclude.cipher.list |  Major | . | John Zhuge | John Zhuge |
| [YARN-5617](https://issues.apache.org/jira/browse/YARN-5617) | AMs only intended to run one attempt can be run more than once |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-6510](https://issues.apache.org/jira/browse/YARN-6510) | Fix profs stat file warning caused by process names that includes parenthesis |  Major | . | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-14351](https://issues.apache.org/jira/browse/HADOOP-14351) | Azure: RemoteWasbAuthorizerImpl and RemoteSASKeyGeneratorImpl should not use Kerberos interactive user cache |  Major | fs/azure | Santhosh G Nayak | Santhosh G Nayak |
| [MAPREDUCE-6881](https://issues.apache.org/jira/browse/MAPREDUCE-6881) | Fix warnings from Spotbugs in hadoop-mapreduce |  Major | . | Weiwei Yang | Weiwei Yang |
| [HADOOP-14346](https://issues.apache.org/jira/browse/HADOOP-14346) | CryptoOutputStream throws IOException on flush() if stream is closed |  Major | . | Pierre Lacave | Pierre Lacave |
| [HDFS-11709](https://issues.apache.org/jira/browse/HDFS-11709) | StandbyCheckpointer should handle an non-existing legacyOivImageDir gracefully |  Critical | ha, namenode | Zhe Zhang | Erik Krogen |
| [YARN-6534](https://issues.apache.org/jira/browse/YARN-6534) | ResourceManager failed due to TimelineClient try to init SSLFactory even https is not enabled |  Blocker | . | Junping Du | Rohith Sharma K S |
| [HADOOP-14354](https://issues.apache.org/jira/browse/HADOOP-14354) | SysInfoWindows is not thread safe |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-5894](https://issues.apache.org/jira/browse/YARN-5894) | fixed license warning caused by de.ruedigermoeller:fst:jar:2.24 |  Blocker | yarn | Haibo Chen | Haibo Chen |
| [YARN-6472](https://issues.apache.org/jira/browse/YARN-6472) | Improve Java sandbox regex |  Major | . | Miklos Szegedi | Greg Phillips |
| [HADOOP-14320](https://issues.apache.org/jira/browse/HADOOP-14320) | TestIPC.testIpcWithReaderQueuing fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [YARN-6536](https://issues.apache.org/jira/browse/YARN-6536) | TestAMRMClient.testAMRMClientWithSaslEncryption fails intermittently |  Major | . | Eric Badger | Jason Lowe |
| [HDFS-11718](https://issues.apache.org/jira/browse/HDFS-11718) | DFSStripedOutputStream hsync/hflush should not throw UnsupportedOperationException |  Blocker | erasure-coding | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-14363](https://issues.apache.org/jira/browse/HADOOP-14363) | Inconsistent default block location in FileSystem javadoc |  Trivial | fs | Mingliang Liu | Chen Liang |
| [HADOOP-13901](https://issues.apache.org/jira/browse/HADOOP-13901) | Fix ASF License warnings |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-6518](https://issues.apache.org/jira/browse/YARN-6518) | Fix warnings from Spotbugs in hadoop-yarn-server-timelineservice |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-6520](https://issues.apache.org/jira/browse/YARN-6520) | Fix warnings from Spotbugs in hadoop-yarn-client |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-11609](https://issues.apache.org/jira/browse/HDFS-11609) | Some blocks can be permanently lost if nodes are decommissioned while dead |  Blocker | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-11724](https://issues.apache.org/jira/browse/HDFS-11724) | libhdfs compilation is broken on OS X |  Blocker | libhdfs | Allen Wittenauer | John Zhuge |
| [HDFS-8498](https://issues.apache.org/jira/browse/HDFS-8498) | Blocks can be committed with wrong size |  Critical | hdfs-client | Daryn Sharp | Jing Zhao |
| [HDFS-11714](https://issues.apache.org/jira/browse/HDFS-11714) | Newly added NN storage directory won't get initialized and cause space exhaustion |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-14366](https://issues.apache.org/jira/browse/HADOOP-14366) | maven upgrade broke start-build-env.sh |  Blocker | build | Allen Wittenauer | Akira Ajisaka |
| [HDFS-11593](https://issues.apache.org/jira/browse/HDFS-11593) | Change SimpleHttpProxyHandler#exceptionCaught log level from info to debug |  Minor | datanode | Xiaoyu Yao | Xiaobing Zhou |
| [HDFS-11710](https://issues.apache.org/jira/browse/HDFS-11710) | hadoop-hdfs-native-client build fails in trunk in Windows after HDFS-11529 |  Blocker | native | Vinayakumar B | Sailesh Mukil |
| [HADOOP-14371](https://issues.apache.org/jira/browse/HADOOP-14371) | License error in TestLoadBalancingKMSClientProvider.java |  Major | . | hu xiaodong | hu xiaodong |
| [HADOOP-14369](https://issues.apache.org/jira/browse/HADOOP-14369) | NetworkTopology calls expensive toString() when logging |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14281](https://issues.apache.org/jira/browse/HADOOP-14281) | Fix TestKafkaMetrics#testPutMetrics |  Major | metrics | Akira Ajisaka | Alison Yu |
| [YARN-6519](https://issues.apache.org/jira/browse/YARN-6519) | Fix warnings from Spotbugs in hadoop-yarn-server-resourcemanager |  Major | resourcemanager | Weiwei Yang | Weiwei Yang |
| [YARN-6481](https://issues.apache.org/jira/browse/YARN-6481) | Yarn top shows negative container number in FS |  Major | yarn | Yufei Gu | Tao Jie |
| [HADOOP-14306](https://issues.apache.org/jira/browse/HADOOP-14306) | TestLocalFileSystem tests have very low timeouts |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14372](https://issues.apache.org/jira/browse/HADOOP-14372) | TestSymlinkLocalFS timeouts are too low |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11739](https://issues.apache.org/jira/browse/HDFS-11739) | Fix regression in tests caused by YARN-679 |  Major | test | Steve Loughran | Steve Loughran |
| [HDFS-11643](https://issues.apache.org/jira/browse/HDFS-11643) | Add shouldReplicate option to create builder |  Blocker | balancer & mover, erasure-coding | Andrew Wang | Sammi Chen |
| [HADOOP-14380](https://issues.apache.org/jira/browse/HADOOP-14380) | Make the Guava version Hadoop which builds with configurable |  Major | build | Steve Loughran | Steve Loughran |
| [HDFS-11448](https://issues.apache.org/jira/browse/HDFS-11448) | JN log segment syncing should support HA upgrade |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-14207](https://issues.apache.org/jira/browse/HADOOP-14207) | "dfsadmin -refreshCallQueue" fails with DecayRpcScheduler |  Blocker | rpc-server | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-14298](https://issues.apache.org/jira/browse/HADOOP-14298) | TestHadoopArchiveLogsRunner fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11702](https://issues.apache.org/jira/browse/HDFS-11702) | Remove indefinite caching of key provider uri in DFSClient |  Major | hdfs-client | Rushabh S Shah | Rushabh S Shah |
| [YARN-3839](https://issues.apache.org/jira/browse/YARN-3839) | Quit throwing NMNotYetReadyException |  Major | nodemanager | Karthik Kambatla | Manikandan R |
| [HADOOP-14374](https://issues.apache.org/jira/browse/HADOOP-14374) | License error in GridmixTestUtils.java |  Major | . | lixinglong | lixinglong |
| [HADOOP-14100](https://issues.apache.org/jira/browse/HADOOP-14100) | Upgrade Jsch jar to latest version to fix vulnerability in old versions |  Critical | . | Vinayakumar B | Vinayakumar B |
| [YARN-5301](https://issues.apache.org/jira/browse/YARN-5301) | NM mount cpu cgroups failed on some systems |  Major | . | sandflee | Miklos Szegedi |
| [HADOOP-14377](https://issues.apache.org/jira/browse/HADOOP-14377) | Increase Common test timeouts from 1 second to 10 seconds |  Major | . | Eric Badger | Eric Badger |
| [YARN-3742](https://issues.apache.org/jira/browse/YARN-3742) | YARN RM  will shut down if ZKClient creation times out |  Major | resourcemanager | Wilfred Spiegelenburg | Daniel Templeton |
| [HADOOP-14373](https://issues.apache.org/jira/browse/HADOOP-14373) | License error In org.apache.hadoop.metrics2.util.Servers |  Major | . | hu xiaodong | hu xiaodong |
| [HADOOP-14400](https://issues.apache.org/jira/browse/HADOOP-14400) | Fix warnings from spotbugs in hadoop-tools |  Major | tools | Weiwei Yang | Weiwei Yang |
| [YARN-6552](https://issues.apache.org/jira/browse/YARN-6552) | Increase YARN test timeouts from 1 second to 10 seconds |  Major | . | Eric Badger | Eric Badger |
| [MAPREDUCE-6882](https://issues.apache.org/jira/browse/MAPREDUCE-6882) | Increase MapReduce test timeouts from 1 second to 10 seconds |  Major | . | Eric Badger | Eric Badger |
| [YARN-6475](https://issues.apache.org/jira/browse/YARN-6475) | Fix some long function checkstyle issues |  Trivial | . | Miklos Szegedi | Soumabrata Chakraborty |
| [HADOOP-14405](https://issues.apache.org/jira/browse/HADOOP-14405) | Fix performance regression due to incorrect use of DataChecksum |  Major | native, performance | LiXin Ge | LiXin Ge |
| [HDFS-11745](https://issues.apache.org/jira/browse/HDFS-11745) | Increase HDFS test timeouts from 1 second to 10 seconds |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11755](https://issues.apache.org/jira/browse/HDFS-11755) | Underconstruction blocks can be considered missing |  Major | . | Nathan Roberts | Nathan Roberts |
| [YARN-6571](https://issues.apache.org/jira/browse/YARN-6571) | Fix JavaDoc issues in SchedulingPolicy |  Trivial | fairscheduler | Daniel Templeton | Weiwei Yang |
| [YARN-6473](https://issues.apache.org/jira/browse/YARN-6473) | Create ReservationInvariantChecker to validate ReservationSystem + Scheduler operations |  Major | . | Carlo Curino | Carlo Curino |
| [HADOOP-14361](https://issues.apache.org/jira/browse/HADOOP-14361) | Azure: NativeAzureFileSystem.getDelegationToken() call fails sometimes when invoked concurrently |  Major | fs/azure | Trupti Dhavle | Santhosh G Nayak |
| [HDFS-11681](https://issues.apache.org/jira/browse/HDFS-11681) | DatanodeStorageInfo#getBlockIterator() should return an iterator to an unmodifiable set. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HADOOP-14410](https://issues.apache.org/jira/browse/HADOOP-14410) | Correct spelling of  'beginning' and variants |  Trivial | . | Dongtao Zhang | Dongtao Zhang |
| [YARN-5543](https://issues.apache.org/jira/browse/YARN-5543) | ResourceManager SchedulingMonitor could potentially terminate the preemption checker thread |  Major | capacityscheduler, resourcemanager | Min Shen | Min Shen |
| [YARN-6380](https://issues.apache.org/jira/browse/YARN-6380) | FSAppAttempt keeps redundant copy of the queue |  Major | fairscheduler | Daniel Templeton | Daniel Templeton |
| [HDFS-11674](https://issues.apache.org/jira/browse/HDFS-11674) | reserveSpaceForReplicas is not released if append request failed due to mirror down and replica recovered |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [YARN-6580](https://issues.apache.org/jira/browse/YARN-6580) | Incorrect logger for FairSharePolicy |  Minor | fairscheduler | Yufei Gu | Vrushali C |
| [HADOOP-14376](https://issues.apache.org/jira/browse/HADOOP-14376) | Memory leak when reading a compressed file using the native library |  Major | common, io | Eli Acherkan | Eli Acherkan |
| [HDFS-11818](https://issues.apache.org/jira/browse/HDFS-11818) | TestBlockManager.testSufficientlyReplBlocksUsesNewRack fails intermittently |  Major | . | Eric Badger | Nathan Roberts |
| [HDFS-11644](https://issues.apache.org/jira/browse/HDFS-11644) | Support for querying outputstream capabilities |  Major | erasure-coding | Andrew Wang | Manoj Govindassamy |
| [YARN-6598](https://issues.apache.org/jira/browse/YARN-6598) | History server getApplicationReport NPE when fetching report for pre-2.8 job |  Blocker | timelineserver | Jason Lowe | Jason Lowe |
| [YARN-6603](https://issues.apache.org/jira/browse/YARN-6603) | NPE in RMAppsBlock |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-11833](https://issues.apache.org/jira/browse/HDFS-11833) | HDFS architecture documentation describes outdated placement policy |  Minor | . | dud | Chen Liang |
| [HADOOP-14416](https://issues.apache.org/jira/browse/HADOOP-14416) | Path starting with 'wasb:///' not resolved correctly while authorizing with WASB-Ranger |  Major | fs/azure, security | Sivaguru Sankaridurg | Sivaguru Sankaridurg |
| [YARN-6535](https://issues.apache.org/jira/browse/YARN-6535) | Program needs to exit when SLS finishes. |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [HDFS-11827](https://issues.apache.org/jira/browse/HDFS-11827) | NPE is thrown when log level changed in BlockPlacementPolicyDefault#chooseRandom() method |  Major | . | xupeng | xupeng |
| [HADOOP-14412](https://issues.apache.org/jira/browse/HADOOP-14412) | HostsFileReader#getHostDetails is very expensive on large clusters |  Major | util | Jason Lowe | Jason Lowe |
| [MAPREDUCE-6459](https://issues.apache.org/jira/browse/MAPREDUCE-6459) | Native task crashes when merging spilled file on ppc64 |  Major | . | Tao Jie | Tao Jie |
| [HADOOP-14427](https://issues.apache.org/jira/browse/HADOOP-14427) | Avoid reloading of Configuration in ViewFileSystem creation. |  Major | viewfs | Vinayakumar B | Vinayakumar B |
| [HADOOP-14434](https://issues.apache.org/jira/browse/HADOOP-14434) | Use MoveFileEx to allow renaming a file when the destination exists |  Major | native | Lukas Majercak | Lukas Majercak |
| [HDFS-11842](https://issues.apache.org/jira/browse/HDFS-11842) | TestDataNodeOutlierDetectionViaMetrics UT fails |  Major | . | Yesha Vora | Hanisha Koneru |
| [YARN-6577](https://issues.apache.org/jira/browse/YARN-6577) | Remove unused ContainerLocalization classes |  Minor | nodemanager | ZhangBing Lin | ZhangBing Lin |
| [HADOOP-11869](https://issues.apache.org/jira/browse/HADOOP-11869) | Suppress ParameterNumber checkstyle violations for overridden methods |  Major | . | Sidharta Seethana | Jonathan Eagles |
| [YARN-6618](https://issues.apache.org/jira/browse/YARN-6618) | TestNMLeveldbStateStoreService#testCompactionCycle can fail if compaction occurs more than once |  Minor | test | Jason Lowe | Jason Lowe |
| [YARN-6540](https://issues.apache.org/jira/browse/YARN-6540) | Resource Manager is spelled "Resource Manger" in ResourceManagerRestart.md and ResourceManagerHA.md |  Trivial | site | Grant Sohn | Grant Sohn |
| [YARN-6249](https://issues.apache.org/jira/browse/YARN-6249) | TestFairSchedulerPreemption fails inconsistently. |  Major | fairscheduler, resourcemanager | Sean Po | Tao Jie |
| [YARN-6602](https://issues.apache.org/jira/browse/YARN-6602) | Impersonation does not work if standby RM is contacted first |  Blocker | client | Robert Kanter | Robert Kanter |
| [HDFS-11862](https://issues.apache.org/jira/browse/HDFS-11862) | Option -v missing for du command in FileSystemShell.md |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-11863](https://issues.apache.org/jira/browse/HDFS-11863) | Document missing metrics for blocks count in pending IBR |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-11849](https://issues.apache.org/jira/browse/HDFS-11849) | JournalNode startup failure exception should be logged in log file |  Major | journal-node | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11866](https://issues.apache.org/jira/browse/HDFS-11866) | JournalNode Sync should be off by default in hdfs-default.xml |  Major | . | Hanisha Koneru | Hanisha Koneru |
| [YARN-6584](https://issues.apache.org/jira/browse/YARN-6584) | Correct license headers in hadoop-common, hdfs, yarn and mapreduce |  Trivial | . | Yeliang Cang | Yeliang Cang |
| [HDFS-11864](https://issues.apache.org/jira/browse/HDFS-11864) | Document  Metrics to track usage of memory for writes |  Major | documentation | Brahma Reddy Battula | Yiqun Lin |
| [YARN-6615](https://issues.apache.org/jira/browse/YARN-6615) | AmIpFilter drops query parameters on redirect |  Major | . | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-14449](https://issues.apache.org/jira/browse/HADOOP-14449) | The ASF Header in ComparableVersion.java and SSLHostnameVerifier.java is not correct |  Minor | common, documentation | ZhangBing Lin | ZhangBing Lin |
| [HADOOP-14166](https://issues.apache.org/jira/browse/HADOOP-14166) | Reset the DecayRpcScheduler AvgResponseTime metric to zero when queue is not used |  Major | common | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11877](https://issues.apache.org/jira/browse/HDFS-11877) | FileJournalManager#getLogFile should ignore in progress edit logs during JN sync |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11661](https://issues.apache.org/jira/browse/HDFS-11661) | GetContentSummary uses excessive amounts of memory |  Blocker | namenode | Nathan Roberts | Wei-Chiu Chuang |
| [HADOOP-14180](https://issues.apache.org/jira/browse/HADOOP-14180) | FileSystem contract tests to replace JUnit 3 with 4 |  Major | fs | Mingliang Liu | Xiaobing Zhou |
| [YARN-6141](https://issues.apache.org/jira/browse/YARN-6141) | ppc64le on Linux doesn't trigger \_\_linux get\_executable codepath |  Major | nodemanager | Sonia Garudi | Ayappan |
| [HADOOP-14399](https://issues.apache.org/jira/browse/HADOOP-14399) | Configuration does not correctly XInclude absolute file URIs |  Blocker | conf | Andrew Wang | Jonathan Eagles |
| [HADOOP-14430](https://issues.apache.org/jira/browse/HADOOP-14430) | the accessTime of FileStatus returned by SFTPFileSystem's getFileStatus method is always 0 |  Trivial | fs | Hongyuan Li | Hongyuan Li |
| [HDFS-11445](https://issues.apache.org/jira/browse/HDFS-11445) | FSCK shows overall health status as corrupt even one replica is corrupt |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11878](https://issues.apache.org/jira/browse/HDFS-11878) | Fix journal missing log httpServerUrl address in JournalNodeSyncer |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11879](https://issues.apache.org/jira/browse/HDFS-11879) | Fix JN sync interval in case of exception |  Major | . | Hanisha Koneru | Hanisha Koneru |
| [YARN-6643](https://issues.apache.org/jira/browse/YARN-6643) | TestRMFailover fails rarely due to port conflict |  Major | test | Robert Kanter | Robert Kanter |
| [HDFS-11817](https://issues.apache.org/jira/browse/HDFS-11817) | A faulty node can cause a lease leak and NPE on accessing data |  Critical | . | Kihwal Lee | Kihwal Lee |
| [YARN-6641](https://issues.apache.org/jira/browse/YARN-6641) | Non-public resource localization on a bad disk causes subsequent containers failure |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-14431](https://issues.apache.org/jira/browse/HADOOP-14431) | ModifyTime of FileStatus returned by SFTPFileSystem's getFileStatus method is wrong |  Major | fs | Hongyuan Li | Hongyuan Li |
| [YARN-6646](https://issues.apache.org/jira/browse/YARN-6646) | Modifier 'static' is redundant for inner enums |  Minor | . | ZhangBing Lin | ZhangBing Lin |
| [HDFS-11078](https://issues.apache.org/jira/browse/HDFS-11078) | Fix NPE in LazyPersistFileScrubber |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14464](https://issues.apache.org/jira/browse/HADOOP-14464) | hadoop-aws doc header warning #5 line wrapped |  Trivial | documentation, fs/s3 | John Zhuge | John Zhuge |
| [MAPREDUCE-6887](https://issues.apache.org/jira/browse/MAPREDUCE-6887) | Modifier 'static' is redundant for inner enums |  Minor | . | ZhangBing Lin | ZhangBing Lin |
| [HADOOP-14458](https://issues.apache.org/jira/browse/HADOOP-14458) | Add missing imports to TestAliyunOSSFileSystemContract.java |  Trivial | fs/oss, test | Mingliang Liu | Mingliang Liu |
| [HADOOP-14456](https://issues.apache.org/jira/browse/HADOOP-14456) | Modifier 'static' is redundant for inner enums |  Minor | . | ZhangBing Lin | ZhangBing Lin |
| [HDFS-11659](https://issues.apache.org/jira/browse/HDFS-11659) | TestDataNodeHotSwapVolumes.testRemoveVolumeBeingWritten fail due to no DataNode available for pipeline recovery. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-6366](https://issues.apache.org/jira/browse/YARN-6366) | Refactor the NodeManager DeletionService to support additional DeletionTask types. |  Major | nodemanager, yarn | Shane Kumpf | Shane Kumpf |
| [HDFS-11901](https://issues.apache.org/jira/browse/HDFS-11901) | Modifier 'static' is redundant for inner enums |  Minor | . | ZhangBing Lin | ZhangBing Lin |
| [HDFS-5042](https://issues.apache.org/jira/browse/HDFS-5042) | Completed files lost after power failure |  Critical | . | Dave Latham | Vinayakumar B |
| [YARN-6649](https://issues.apache.org/jira/browse/YARN-6649) | RollingLevelDBTimelineServer throws RuntimeException if object decoding ever fails runtime exception |  Critical | . | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-9849](https://issues.apache.org/jira/browse/HADOOP-9849) | License information is missing for native CRC32 code |  Critical | . | Timothy St. Clair | Andrew Wang |
| [HADOOP-14466](https://issues.apache.org/jira/browse/HADOOP-14466) | Remove useless document from TestAliyunOSSFileSystemContract.java |  Minor | documentation | Akira Ajisaka | Chen Liang |
| [HDFS-11893](https://issues.apache.org/jira/browse/HDFS-11893) | Fix TestDFSShell.testMoveWithTargetPortEmpty failure. |  Major | test | Konstantin Shvachko | Brahma Reddy Battula |
| [HADOOP-14460](https://issues.apache.org/jira/browse/HADOOP-14460) | Azure: update doc for live and contract tests |  Major | documentation, fs/azure | Mingliang Liu | Mingliang Liu |
| [HDFS-11741](https://issues.apache.org/jira/browse/HDFS-11741) | Long running balancer may fail due to expired DataEncryptionKey |  Major | balancer & mover | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11856](https://issues.apache.org/jira/browse/HDFS-11856) | Ability to re-add Upgrading Nodes (remote) to pipeline for future pipeline updates |  Major | hdfs-client, rolling upgrades | Vinayakumar B | Vinayakumar B |
| [HDFS-11905](https://issues.apache.org/jira/browse/HDFS-11905) | Fix license header inconsistency in hdfs |  Trivial | . | Yeliang Cang | Yeliang Cang |
| [HADOOP-14436](https://issues.apache.org/jira/browse/HADOOP-14436) | Remove the redundant colon in ViewFs.md |  Major | documentation | maobaolong | maobaolong |
| [HDFS-11899](https://issues.apache.org/jira/browse/HDFS-11899) | ASF License warnings generated intermittently in trunk |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-6458](https://issues.apache.org/jira/browse/YARN-6458) | Use yarn package manager to lock down dependency versions for new web UI |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [HADOOP-14428](https://issues.apache.org/jira/browse/HADOOP-14428) | s3a: mkdir appears to be broken |  Blocker | fs/s3 | Aaron Fabbri | Mingliang Liu |
| [YARN-6683](https://issues.apache.org/jira/browse/YARN-6683) | Invalid event: COLLECTOR\_UPDATE at KILLED |  Major | . | Jian He | Rohith Sharma K S |
| [HDFS-11928](https://issues.apache.org/jira/browse/HDFS-11928) | Segment overflow in FileDistributionCalculator |  Major | tools | LiXin Ge | LiXin Ge |
| [HDFS-10816](https://issues.apache.org/jira/browse/HDFS-10816) | TestComputeInvalidateWork#testDatanodeReRegistration fails due to race between test and replication monitor |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14478](https://issues.apache.org/jira/browse/HADOOP-14478) | Optimize NativeAzureFsInputStream for positional reads |  Major | fs/azure | Rajesh Balamohan | Rajesh Balamohan |
| [HADOOP-14472](https://issues.apache.org/jira/browse/HADOOP-14472) | Azure: TestReadAndSeekPageBlobAfterWrite fails intermittently |  Major | fs/azure, test | Mingliang Liu | Mingliang Liu |
| [HDFS-11932](https://issues.apache.org/jira/browse/HDFS-11932) | BPServiceActor thread name is not correctly set |  Major | hdfs | Chen Liang | Chen Liang |
| [YARN-6547](https://issues.apache.org/jira/browse/YARN-6547) | Enhance SLS-based tests leveraging invariant checker |  Major | . | Carlo Curino | Carlo Curino |
| [HDFS-11708](https://issues.apache.org/jira/browse/HDFS-11708) | Positional read will fail if replicas moved to different DNs after stream is opened |  Critical | hdfs-client | Vinayakumar B | Vinayakumar B |
| [HDFS-11929](https://issues.apache.org/jira/browse/HDFS-11929) | Document missing processor of hdfs oiv\_legacy command |  Minor | documentation, tools | LiXin Ge | LiXin Ge |
| [HDFS-11711](https://issues.apache.org/jira/browse/HDFS-11711) | DN should not delete the block On "Too many open files" Exception |  Critical | datanode | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6676](https://issues.apache.org/jira/browse/MAPREDUCE-6676) | NNBench should Throw IOException when rename and delete fails |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-14500](https://issues.apache.org/jira/browse/HADOOP-14500) | Azure: TestFileSystemOperationExceptionHandling{,MultiThreaded} fails |  Major | fs/azure, test | Mingliang Liu | Rajesh Balamohan |
| [HDFS-11851](https://issues.apache.org/jira/browse/HDFS-11851) | getGlobalJNIEnv() may deadlock if exception is thrown |  Blocker | libhdfs | Henry Robinson | Sailesh Mukil |
| [HDFS-11945](https://issues.apache.org/jira/browse/HDFS-11945) | Internal lease recovery may not be retried for a long time |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HADOOP-14283](https://issues.apache.org/jira/browse/HADOOP-14283) | Upgrade AWS SDK to 1.11.134 |  Critical | fs/s3 | Aaron Fabbri | Aaron Fabbri |
| [HADOOP-14512](https://issues.apache.org/jira/browse/HADOOP-14512) | WASB atomic rename should not throw exception if the file is neither in src nor in dst when doing the rename |  Major | fs/azure | Duo Xu | Duo Xu |
| [YARN-6585](https://issues.apache.org/jira/browse/YARN-6585) | RM fails to start when upgrading from 2.7 to 2.8 for clusters with node labels. |  Blocker | . | Eric Payne | Sunil Govindan |
| [YARN-6703](https://issues.apache.org/jira/browse/YARN-6703) | RM startup failure with old state store due to version mismatch |  Critical | . | Bibin A Chundatt | Varun Saxena |
| [HADOOP-14501](https://issues.apache.org/jira/browse/HADOOP-14501) | Switch from aalto-xml to woodstox to handle odd XML features |  Blocker | conf | Andrew Wang | Jonathan Eagles |
| [HDFS-11967](https://issues.apache.org/jira/browse/HDFS-11967) | TestJMXGet fails occasionally |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-11947](https://issues.apache.org/jira/browse/HDFS-11947) | When constructing a thread name, BPOfferService may print a bogus warning message |  Minor | datanode | Tsz Wo Nicholas Sze | Weiwei Yang |
| [MAPREDUCE-6896](https://issues.apache.org/jira/browse/MAPREDUCE-6896) | Document wrong spelling in usage of MapredTestDriver tools. |  Major | documentation | LiXin Ge | LiXin Ge |
| [MAPREDUCE-6895](https://issues.apache.org/jira/browse/MAPREDUCE-6895) | Job end notification not send due to YarnRuntimeException |  Major | applicationmaster | yunjiong zhao | yunjiong zhao |
| [HDFS-11682](https://issues.apache.org/jira/browse/HDFS-11682) | TestBalancer#testBalancerWithStripedFile is flaky |  Major | test | Andrew Wang | Lei (Eddy) Xu |
| [HADOOP-14494](https://issues.apache.org/jira/browse/HADOOP-14494) | ITestJets3tNativeS3FileSystemContract tests NPEs in teardown if store undefined |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-14486](https://issues.apache.org/jira/browse/HADOOP-14486) | TestSFTPFileSystem#testGetAccessTime test failure using openJDK 1.8.0 |  Major | fs | Sonia Garudi | Hongyuan Li |
| [MAPREDUCE-6897](https://issues.apache.org/jira/browse/MAPREDUCE-6897) | Add Unit Test to make sure Job end notification get sent even appMaster stop get YarnRuntimeException |  Minor | . | Junping Du | Gergely Novák |
| [YARN-6517](https://issues.apache.org/jira/browse/YARN-6517) | Fix warnings from Spotbugs in hadoop-yarn-common |  Major | . | Weiwei Yang | Weiwei Yang |
| [HADOOP-14540](https://issues.apache.org/jira/browse/HADOOP-14540) | Replace MRv1 specific terms in HostsFileReader |  Minor | documentation | Akira Ajisaka | hu xiaodong |
| [HDFS-11995](https://issues.apache.org/jira/browse/HDFS-11995) | HDFS Architecture documentation incorrectly describes writing to a local temporary file. |  Minor | documentation | Chris Nauroth | Nanda kumar |
| [HDFS-11890](https://issues.apache.org/jira/browse/HDFS-11890) | Handle NPE in BlockRecoveryWorker when DN is getting shoutdown. |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11736](https://issues.apache.org/jira/browse/HDFS-11736) | OIV tests should not write outside 'target' directory. |  Major | . | Konstantin Shvachko | Yiqun Lin |
| [YARN-6713](https://issues.apache.org/jira/browse/YARN-6713) | Fix dead link in the Javadoc of FairSchedulerEventLog.java |  Minor | documentation | Akira Ajisaka | Weiwei Yang |
| [HADOOP-14533](https://issues.apache.org/jira/browse/HADOOP-14533) | Size of args cannot be less than zero in TraceAdmin#run as its linkedlist |  Trivial | common, tracing | Weisen Han | Weisen Han |
| [HDFS-11978](https://issues.apache.org/jira/browse/HDFS-11978) | Remove invalid '-usage' command of 'ec' and add missing commands 'addPolicies' 'listCodecs' in doc |  Minor | documentation | Wenxin He | Wenxin He |
| [HDFS-11960](https://issues.apache.org/jira/browse/HDFS-11960) | Successfully closed files can stay under-replicated. |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-14429](https://issues.apache.org/jira/browse/HADOOP-14429) | FTPFileSystem#getFsAction  always returns FsAction.NONE |  Major | fs | Hongyuan Li | Hongyuan Li |
| [HDFS-11933](https://issues.apache.org/jira/browse/HDFS-11933) | Arguments check for ErasureCodingPolicy-\>composePolicyName |  Minor | hdfs-client | lufei | lufei |
| [HDFS-12010](https://issues.apache.org/jira/browse/HDFS-12010) | TestCopyPreserveFlag fails consistently because of mismatch in access time |  Major | hdfs, test | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12009](https://issues.apache.org/jira/browse/HDFS-12009) | Accept human-friendly units in dfsadmin -setBalancerBandwidth and -setQuota |  Major | shell | Andrew Wang | Andrew Wang |
| [HADOOP-14568](https://issues.apache.org/jira/browse/HADOOP-14568) | GenericTestUtils#waitFor missing parameter verification |  Major | test | Yiqun Lin | Yiqun Lin |
| [HDFS-12024](https://issues.apache.org/jira/browse/HDFS-12024) | Fix typo's in FsDatasetImpl.java |  Major | . | Yasen Liu | Yasen Liu |
| [HADOOP-14146](https://issues.apache.org/jira/browse/HADOOP-14146) | KerberosAuthenticationHandler should authenticate with SPN in AP-REQ |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-5876](https://issues.apache.org/jira/browse/YARN-5876) | TestResourceTrackerService#testGracefulDecommissionWithApp fails intermittently on trunk |  Major | . | Varun Saxena | Robert Kanter |
| [HADOOP-14543](https://issues.apache.org/jira/browse/HADOOP-14543) | ZKFC should use getAversion() while setting the zkacl |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-5006](https://issues.apache.org/jira/browse/YARN-5006) | ResourceManager quit due to ApplicationStateData exceed the limit  size of znode in zk |  Critical | resourcemanager | dongtingting | Bibin A Chundatt |
| [HADOOP-14461](https://issues.apache.org/jira/browse/HADOOP-14461) | Azure: handle failure gracefully in case of missing account access key |  Major | fs/azure | Mingliang Liu | Mingliang Liu |
| [HDFS-12032](https://issues.apache.org/jira/browse/HDFS-12032) | Inaccurate comment on DatanodeDescriptor#getNumberOfBlocksToBeErasureCoded |  Trivial | namenode | Andrew Wang | Andrew Wang |
| [HDFS-11956](https://issues.apache.org/jira/browse/HDFS-11956) | Do not require a storage ID or target storage IDs when writing a block |  Blocker | . | Andrew Wang | Ewan Higgs |
| [HDFS-12033](https://issues.apache.org/jira/browse/HDFS-12033) | DatanodeManager picking EC recovery tasks should also consider the number of regular replication tasks. |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [MAPREDUCE-6904](https://issues.apache.org/jira/browse/MAPREDUCE-6904) | HADOOP\_JOB\_HISTORY\_OPTS should be HADOOP\_JOB\_HISTORYSERVER\_OPTS in mapred-config.sh |  Critical | scripts | Robert Kanter | Robert Kanter |
| [HDFS-12040](https://issues.apache.org/jira/browse/HDFS-12040) | TestFsDatasetImpl.testCleanShutdownOfVolume fails |  Major | test | Akira Ajisaka | hu xiaodong |
| [HADOOP-14594](https://issues.apache.org/jira/browse/HADOOP-14594) | ITestS3AFileOperationCost::testFakeDirectoryDeletion to uncomment metric assertions |  Major | fs/s3 | Mingliang Liu | Mingliang Liu |
| [YARN-6467](https://issues.apache.org/jira/browse/YARN-6467) | CSQueueMetrics needs to update the current metrics for default partition only |  Major | capacity scheduler | Naganarasimha G R | Manikandan R |
| [YARN-6743](https://issues.apache.org/jira/browse/YARN-6743) | yarn.resourcemanager.zk-max-znode-size.bytes description needs spaces in yarn-default.xml |  Trivial | . | Daniel Templeton | Lori Loberg |
| [MAPREDUCE-6536](https://issues.apache.org/jira/browse/MAPREDUCE-6536) | hadoop-pipes doesn't use maven properties for openssl |  Blocker | pipes | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-14479](https://issues.apache.org/jira/browse/HADOOP-14479) | Erasurecode testcase failures with native enabled |  Critical | common | Ayappan | Sammi Chen |
| [YARN-6344](https://issues.apache.org/jira/browse/YARN-6344) | Add parameter for rack locality delay in CapacityScheduler |  Major | capacityscheduler | Konstantinos Karanasos | Konstantinos Karanasos |
| [MAPREDUCE-6697](https://issues.apache.org/jira/browse/MAPREDUCE-6697) | Concurrent task limits should only be applied when necessary |  Major | mrv2 | Jason Lowe | Nathan Roberts |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-11570](https://issues.apache.org/jira/browse/HDFS-11570) | Unit test for NameNodeStatusMXBean |  Major | hdfs, test | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-14218](https://issues.apache.org/jira/browse/HADOOP-14218) | Replace assertThat with assertTrue in MetricsAsserts |  Minor | . | Akira Ajisaka | Akira Ajisaka |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-11296](https://issues.apache.org/jira/browse/HDFS-11296) | Maintenance state expiry should be an epoch time and not jvm monotonic |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-11121](https://issues.apache.org/jira/browse/HDFS-11121) | Add assertions to BlockInfo#addStorage to protect from breaking reportedBlock-blockGroup mapping |  Critical | erasure-coding | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-6099](https://issues.apache.org/jira/browse/YARN-6099) | Improve webservice to list aggregated log files |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-11124](https://issues.apache.org/jira/browse/HDFS-11124) | Report blockIds of internal blocks for EC files in Fsck |  Major | erasure-coding | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-5830](https://issues.apache.org/jira/browse/YARN-5830) | FairScheduler: Avoid preempting AM containers |  Major | fairscheduler | Karthik Kambatla | Yufei Gu |
| [YARN-3637](https://issues.apache.org/jira/browse/YARN-3637) | Handle localization sym-linking correctly at the YARN level |  Major | . | Chris Trezzo | Chris Trezzo |
| [YARN-6126](https://issues.apache.org/jira/browse/YARN-6126) | Obtaining app logs for Running application fails with "Unable to parse json from webservice. Error:" |  Major | . | Sumana Sathish | Xuan Gong |
| [YARN-5866](https://issues.apache.org/jira/browse/YARN-5866) | Fix few issues reported by jshint in new YARN UI |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-6100](https://issues.apache.org/jira/browse/YARN-6100) | improve YARN webservice to output aggregated container logs |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-6108](https://issues.apache.org/jira/browse/YARN-6108) | Improve AHS webservice to accept NM address as a parameter to get container logs |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5917](https://issues.apache.org/jira/browse/YARN-5917) | Make navigation link active when selecting sub tabs in "Applications" and "Nodes" page for new UI |  Minor | yarn-ui-v2 | Kai Sasaki | Kai Sasaki |
| [YARN-5258](https://issues.apache.org/jira/browse/YARN-5258) | Document Use of Docker with LinuxContainerExecutor |  Critical | documentation | Daniel Templeton | Daniel Templeton |
| [HADOOP-14065](https://issues.apache.org/jira/browse/HADOOP-14065) | AliyunOSS: oss directory filestatus should use meta time |  Major | fs/oss | Fei Hui | Fei Hui |
| [HADOOP-14032](https://issues.apache.org/jira/browse/HADOOP-14032) | Reduce fair call queue priority inversion |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-14034](https://issues.apache.org/jira/browse/HADOOP-14034) | Allow ipc layer exceptions to selectively close connections |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-14033](https://issues.apache.org/jira/browse/HADOOP-14033) | Reduce fair call queue lock contention |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-13768](https://issues.apache.org/jira/browse/HADOOP-13768) | AliyunOSS: handle the failure in the batch delete operation \`deleteDirs\`. |  Major | fs | Genmao Yu | Genmao Yu |
| [YARN-6170](https://issues.apache.org/jira/browse/YARN-6170) | TimelineReaderServer should wait to join with HttpServer2 |  Minor | timelinereader | Sangjin Lee | Sangjin Lee |
| [HADOOP-13075](https://issues.apache.org/jira/browse/HADOOP-13075) | Add support for SSE-KMS and SSE-C in s3a filesystem |  Major | fs/s3 | Andrew Olson | Steve Moist |
| [HADOOP-14069](https://issues.apache.org/jira/browse/HADOOP-14069) | AliyunOSS: listStatus returns wrong file info |  Major | fs/oss | Fei Hui | Fei Hui |
| [HADOOP-13769](https://issues.apache.org/jira/browse/HADOOP-13769) | AliyunOSS: update oss sdk version |  Major | fs, fs/oss | Genmao Yu | Genmao Yu |
| [YARN-6113](https://issues.apache.org/jira/browse/YARN-6113) | re-direct NM Web Service to get container logs for finished applications |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5966](https://issues.apache.org/jira/browse/YARN-5966) | AMRMClient changes to support ExecutionType update |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-5912](https://issues.apache.org/jira/browse/YARN-5912) | Fix breadcrumb issues for various pages in new YARN UI |  Minor | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HADOOP-14072](https://issues.apache.org/jira/browse/HADOOP-14072) | AliyunOSS: Failed to read from stream when seek beyond the download size |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-6156](https://issues.apache.org/jira/browse/YARN-6156) | AM blacklisting to consider node label partition |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-6183](https://issues.apache.org/jira/browse/YARN-6183) | Few missing informations in Application and Application Attempt pages for new YARN UI. |  Major | . | Akhil PB | Akhil PB |
| [HDFS-11265](https://issues.apache.org/jira/browse/HDFS-11265) | Extend visualization for Maintenance Mode under Datanode tab in the NameNode UI |  Major | datanode, namenode | Manoj Govindassamy | Elek, Marton |
| [YARN-6163](https://issues.apache.org/jira/browse/YARN-6163) | FS Preemption is a trickle for severely starved applications |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-5798](https://issues.apache.org/jira/browse/YARN-5798) | Set UncaughtExceptionHandler for all FairScheduler threads |  Major | fairscheduler | Karthik Kambatla | Yufei Gu |
| [YARN-4675](https://issues.apache.org/jira/browse/YARN-4675) | Reorganize TimelineClient and TimelineClientImpl into separate classes for ATSv1.x and ATSv2 |  Major | timelineserver | Naganarasimha G R | Naganarasimha G R |
| [HADOOP-14019](https://issues.apache.org/jira/browse/HADOOP-14019) | fix some typos in the s3a docs |  Minor | documentation, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-14040](https://issues.apache.org/jira/browse/HADOOP-14040) | Use shaded aws-sdk uber-JAR 1.11.86 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-6193](https://issues.apache.org/jira/browse/YARN-6193) | FairScheduler might not trigger preemption when using DRF |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-14081](https://issues.apache.org/jira/browse/HADOOP-14081) | S3A: Consider avoiding array copy in S3ABlockOutputStream (ByteArrayBlock) |  Minor | fs/s3 | Rajesh Balamohan | Rajesh Balamohan |
| [YARN-6159](https://issues.apache.org/jira/browse/YARN-6159) | Documentation changes for TimelineV2Client |  Minor | documentation | Varun Saxena | Naganarasimha G R |
| [HDFS-11430](https://issues.apache.org/jira/browse/HDFS-11430) | Separate class InnerNode from class NetworkTopology and make it extendable |  Major | namenode | Chen Liang | Tsz Wo Nicholas Sze |
| [YARN-6184](https://issues.apache.org/jira/browse/YARN-6184) | Introduce loading icon in each page of new YARN UI |  Major | . | Akhil PB | Akhil PB |
| [HADOOP-14099](https://issues.apache.org/jira/browse/HADOOP-14099) | Split S3 testing documentation out into its own file |  Minor | documentation, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-11411](https://issues.apache.org/jira/browse/HDFS-11411) | Avoid OutOfMemoryError in TestMaintenanceState test runs |  Major | datanode, namenode | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-14102](https://issues.apache.org/jira/browse/HADOOP-14102) | Relax error message assertion in S3A test ITestS3AEncryptionSSEC |  Minor | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HDFS-4025](https://issues.apache.org/jira/browse/HDFS-4025) | QJM: Sychronize past log segments to JNs that missed them |  Major | ha | Todd Lipcon | Hanisha Koneru |
| [YARN-6069](https://issues.apache.org/jira/browse/YARN-6069) | CORS support in timeline v2 |  Major | timelinereader | Sreenath Somarajapuram | Rohith Sharma K S |
| [YARN-6143](https://issues.apache.org/jira/browse/YARN-6143) | Fix incompatible issue caused by YARN-3583 |  Blocker | rolling upgrade | Wangda Tan | Sunil Govindan |
| [HADOOP-14113](https://issues.apache.org/jira/browse/HADOOP-14113) | review ADL Docs |  Minor | documentation, fs/adl | Steve Loughran | Steve Loughran |
| [YARN-4779](https://issues.apache.org/jira/browse/YARN-4779) | Fix AM container allocation logic in SLS |  Major | scheduler-load-simulator | Wangda Tan | Wangda Tan |
| [YARN-6228](https://issues.apache.org/jira/browse/YARN-6228) | EntityGroupFSTimelineStore should allow configurable cache stores. |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-6215](https://issues.apache.org/jira/browse/YARN-6215) | FairScheduler preemption and update should not run concurrently |  Major | fairscheduler, test | Sunil Govindan | Tao Jie |
| [YARN-6123](https://issues.apache.org/jira/browse/YARN-6123) | [YARN-5864] Add a test to make sure queues of orderingPolicy will be updated when childQueues is added or removed. |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-14118](https://issues.apache.org/jira/browse/HADOOP-14118) | move jets3t into a dependency on hadoop-aws JAR |  Major | build, fs/s3 | Steve Loughran | Akira Ajisaka |
| [YARN-5335](https://issues.apache.org/jira/browse/YARN-5335) | Use em-table in app/nodes pages for new YARN UI |  Major | . | Sunil Govindan | Sunil Govindan |
| [HDFS-11450](https://issues.apache.org/jira/browse/HDFS-11450) | HDFS specific network topology classes with storage type info included |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-11412](https://issues.apache.org/jira/browse/HDFS-11412) | Maintenance minimum replication config value allowable range should be [0, DefaultReplication] |  Major | datanode, namenode | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-14057](https://issues.apache.org/jira/browse/HADOOP-14057) | Fix package.html to compile with Java 9 |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-8112](https://issues.apache.org/jira/browse/HDFS-8112) | Relax permission checking for EC related operations |  Blocker | erasure-coding | Kai Zheng | Andrew Wang |
| [HADOOP-14056](https://issues.apache.org/jira/browse/HADOOP-14056) | Update maven-javadoc-plugin to 2.10.4 |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-6275](https://issues.apache.org/jira/browse/YARN-6275) | Fail to show real-time tracking charts in SLS |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [HDFS-10983](https://issues.apache.org/jira/browse/HDFS-10983) | OIV tool should make an EC file explicit |  Major | . | Wei-Chiu Chuang | Manoj Govindassamy |
| [YARN-5669](https://issues.apache.org/jira/browse/YARN-5669) | Add support for Docker pull |  Major | yarn | Zhankun Tang | luhuichun |
| [YARN-1047](https://issues.apache.org/jira/browse/YARN-1047) | Expose # of pre-emptions as a queue counter |  Major | fairscheduler | Philip Zeyliger | Karthik Kambatla |
| [HADOOP-14123](https://issues.apache.org/jira/browse/HADOOP-14123) | Remove misplaced ADL service provider config file for FileSystem |  Minor | fs/adl | John Zhuge | John Zhuge |
| [HADOOP-14153](https://issues.apache.org/jira/browse/HADOOP-14153) | ADL module has messed doc structure |  Major | fs/adl | Mingliang Liu | Mingliang Liu |
| [YARN-6196](https://issues.apache.org/jira/browse/YARN-6196) | Improve Resource Donut chart with better label in Node page of new YARN UI |  Major | . | Akhil PB | Akhil PB |
| [HADOOP-14111](https://issues.apache.org/jira/browse/HADOOP-14111) | cut some obsolete, ignored s3 tests in TestS3Credentials |  Minor | fs/s3, test | Steve Loughran | Yuanbo Liu |
| [YARN-6281](https://issues.apache.org/jira/browse/YARN-6281) | Cleanup when AMRMProxy fails to initialize a new interceptor chain |  Minor | . | Botong Huang | Botong Huang |
| [HADOOP-14173](https://issues.apache.org/jira/browse/HADOOP-14173) | Remove unused AdlConfKeys#ADL\_EVENTS\_TRACKING\_SOURCE |  Trivial | fs/adl | John Zhuge | John Zhuge |
| [HDFS-11482](https://issues.apache.org/jira/browse/HDFS-11482) | Add storage type demand to into DFSNetworkTopology#chooseRandom |  Major | namenode | Chen Liang | Chen Liang |
| [YARN-5496](https://issues.apache.org/jira/browse/YARN-5496) | Make Node Heatmap Chart categories clickable in new YARN UI |  Major | . | Yesha Vora | Gergely Novák |
| [YARN-6314](https://issues.apache.org/jira/browse/YARN-6314) | Potential infinite redirection on YARN log redirection web service |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-6313](https://issues.apache.org/jira/browse/YARN-6313) | yarn logs cli should provide logs for a completed container even when application is still running |  Major | . | Siddharth Seth | Xuan Gong |
| [HDFS-11514](https://issues.apache.org/jira/browse/HDFS-11514) | DFSTopologyNodeImpl#chooseRandom optimizations |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-10530](https://issues.apache.org/jira/browse/HDFS-10530) | BlockManager reconstruction work scheduling should correctly adhere to EC block placement policy |  Major | namenode | Rui Gao | Manoj Govindassamy |
| [HADOOP-14192](https://issues.apache.org/jira/browse/HADOOP-14192) | Aliyun OSS FileSystem contract test should implement getTestBaseDir() |  Major | fs/oss | Mingliang Liu | Mingliang Liu |
| [YARN-6362](https://issues.apache.org/jira/browse/YARN-6362) | Use frontend-maven-plugin 0.0.22 version for new yarn ui |  Major | . | Kai Sasaki | Kai Sasaki |
| [HDFS-11358](https://issues.apache.org/jira/browse/HDFS-11358) | DiskBalancer: Report command supports reading nodes from host file |  Major | diskbalancer | Yiqun Lin | Yiqun Lin |
| [YARN-6367](https://issues.apache.org/jira/browse/YARN-6367) | YARN logs CLI needs alway check containerLogsInfo/containerLogInfo before parse the JSON object from NMWebService |  Major | . | Siddharth Seth | Xuan Gong |
| [YARN-6326](https://issues.apache.org/jira/browse/YARN-6326) | Shouldn't use AppAttemptIds to fetch applications while AM Simulator tracks app in SLS |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [HADOOP-14120](https://issues.apache.org/jira/browse/HADOOP-14120) | needless S3AFileSystem.setOptionalPutRequestParameters in S3ABlockOutputStream putObject() |  Minor | fs/s3 | Steve Loughran | Yuanbo Liu |
| [HADOOP-14135](https://issues.apache.org/jira/browse/HADOOP-14135) | Remove URI parameter in AWSCredentialProvider constructors |  Major | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HADOOP-14196](https://issues.apache.org/jira/browse/HADOOP-14196) | Azure Data Lake doc is missing required config entry |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HADOOP-14197](https://issues.apache.org/jira/browse/HADOOP-14197) | Fix ADLS doc for credential provider |  Major | documentation, fs/adl | John Zhuge | John Zhuge |
| [HADOOP-13715](https://issues.apache.org/jira/browse/HADOOP-13715) | Add isErasureCoded() API to FileStatus class |  Blocker | fs | Wei-Chiu Chuang | Manoj Govindassamy |
| [HADOOP-14230](https://issues.apache.org/jira/browse/HADOOP-14230) | TestAdlFileSystemContractLive fails to clean up |  Minor | fs/adl, test | John Zhuge | John Zhuge |
| [HADOOP-14038](https://issues.apache.org/jira/browse/HADOOP-14038) | Rename ADLS credential properties |  Minor | fs/adl | John Zhuge | John Zhuge |
| [HDFS-11577](https://issues.apache.org/jira/browse/HDFS-11577) | Combine the old and the new chooseRandom for better performance |  Major | namenode | Chen Liang | Chen Liang |
| [YARN-6357](https://issues.apache.org/jira/browse/YARN-6357) | Implement putEntitiesAsync API in TimelineCollector |  Major | ATSv2, timelineserver | Joep Rottinghuis | Haibo Chen |
| [HDFS-10971](https://issues.apache.org/jira/browse/HDFS-10971) | Distcp should not copy replication factor if source file is erasure coded |  Blocker | distcp | Wei-Chiu Chuang | Manoj Govindassamy |
| [HDFS-11541](https://issues.apache.org/jira/browse/HDFS-11541) | Call RawErasureEncoder and RawErasureDecoder release() methods |  Major | erasure-coding | László Bence Nagy | Sammi Chen |
| [YARN-5654](https://issues.apache.org/jira/browse/YARN-5654) | Not be able to run SLS with FairScheduler |  Major | . | Wangda Tan | Yufei Gu |
| [YARN-6342](https://issues.apache.org/jira/browse/YARN-6342) | Make TimelineV2Client's drain timeout after stop configurable |  Major | . | Jian He | Haibo Chen |
| [YARN-6376](https://issues.apache.org/jira/browse/YARN-6376) | Exceptions caused by synchronous putEntities requests can be swallowed |  Critical | ATSv2 | Haibo Chen | Haibo Chen |
| [YARN-6414](https://issues.apache.org/jira/browse/YARN-6414) | ATSv2 HBase related tests fail due to guava version upgrade |  Major | timelineserver | Sonia Garudi | Haibo Chen |
| [YARN-6377](https://issues.apache.org/jira/browse/YARN-6377) | NMTimelinePublisher#serviceStop does not stop timeline clients |  Major | yarn | Haibo Chen | Haibo Chen |
| [YARN-6109](https://issues.apache.org/jira/browse/YARN-6109) | Add an ability to convert ChildQueue to ParentQueue |  Major | capacity scheduler | Xuan Gong | Xuan Gong |
| [YARN-6424](https://issues.apache.org/jira/browse/YARN-6424) | TimelineCollector is not stopped when an app finishes in RM |  Critical | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-6258](https://issues.apache.org/jira/browse/YARN-6258) | localBaseAddress for CORS proxy configuration is not working when suffixed with forward slash in new YARN UI. |  Major | . | Gergely Novák | Gergely Novák |
| [HADOOP-14290](https://issues.apache.org/jira/browse/HADOOP-14290) | Update SLF4J from 1.7.10 to 1.7.25 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-5153](https://issues.apache.org/jira/browse/YARN-5153) | Add a toggle button to switch between timeline view / table view for containers and application-attempts in new YARN UI |  Major | webapp | Wangda Tan | Akhil PB |
| [YARN-6372](https://issues.apache.org/jira/browse/YARN-6372) | Add default value for NM disk validator |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-10996](https://issues.apache.org/jira/browse/HDFS-10996) | Ability to specify per-file EC policy at create time |  Major | erasure-coding | Andrew Wang | Sammi Chen |
| [HADOOP-14255](https://issues.apache.org/jira/browse/HADOOP-14255) | S3A to delete unnecessary fake directory objects in mkdirs() |  Major | fs/s3 | Mingliang Liu | Mingliang Liu |
| [YARN-6432](https://issues.apache.org/jira/browse/YARN-6432) | FairScheduler: Reserve preempted resources for corresponding applications |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [HADOOP-14321](https://issues.apache.org/jira/browse/HADOOP-14321) | Explicitly exclude S3A root dir ITests from parallel runs |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-14241](https://issues.apache.org/jira/browse/HADOOP-14241) | Add ADLS sensitive config keys to default list |  Minor | fs, fs/adl, security | John Zhuge | John Zhuge |
| [YARN-6402](https://issues.apache.org/jira/browse/YARN-6402) | Move 'Long Running Services' to an independent tab at top level for new Yarn UI |  Major | webapp | Sunil Govindan | Akhil PB |
| [HADOOP-14324](https://issues.apache.org/jira/browse/HADOOP-14324) | Refine S3 server-side-encryption key as encryption secret; improve error reporting and diagnostics |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-11604](https://issues.apache.org/jira/browse/HDFS-11604) | Define and parse erasure code policies |  Major | erasure-coding | Kai Zheng | Frank Zeng |
| [HADOOP-14261](https://issues.apache.org/jira/browse/HADOOP-14261) | Some refactoring work for erasure coding raw coder |  Major | . | Kai Zheng | Frank Zeng |
| [YARN-6291](https://issues.apache.org/jira/browse/YARN-6291) | Introduce query parameters (sort, filter, etc.) for tables to keep state on refresh/navigation in new YARN UI |  Major | . | Gergely Novák | Gergely Novák |
| [HADOOP-14305](https://issues.apache.org/jira/browse/HADOOP-14305) | S3A SSE tests won't run in parallel: Bad request in directory GetFileStatus |  Minor | fs/s3, test | Steve Loughran | Steve Moist |
| [YARN-6423](https://issues.apache.org/jira/browse/YARN-6423) | Queue metrics doesn't work for Fair Scheduler in SLS |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [HADOOP-14349](https://issues.apache.org/jira/browse/HADOOP-14349) | Rename ADLS CONTRACT\_ENABLE\_KEY |  Minor | fs/adl | Mingliang Liu | Mingliang Liu |
| [HDFS-6708](https://issues.apache.org/jira/browse/HDFS-6708) | StorageType should be encoded in the block token |  Major | datanode, namenode | Arpit Agarwal | Ewan Higgs |
| [HDFS-11697](https://issues.apache.org/jira/browse/HDFS-11697) | Add javadoc for storage policy and erasure coding policy |  Minor | documentation | Kai Sasaki | Kai Sasaki |
| [HADOOP-11614](https://issues.apache.org/jira/browse/HADOOP-11614) | Remove httpclient dependency from hadoop-openstack |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-6455](https://issues.apache.org/jira/browse/YARN-6455) | Enhance the timelinewriter.flush() race condition fix |  Major | yarn | Haibo Chen | Haibo Chen |
| [HDFS-11605](https://issues.apache.org/jira/browse/HDFS-11605) | Allow user to customize new erasure code policies |  Major | erasure-coding | Kai Zheng | Huafeng Wang |
| [YARN-4359](https://issues.apache.org/jira/browse/YARN-4359) | Update LowCost agents logic to take advantage of YARN-4358 |  Major | capacityscheduler, fairscheduler, resourcemanager | Carlo Curino | Ishai Menache |
| [YARN-6542](https://issues.apache.org/jira/browse/YARN-6542) | Fix the logger in TestAlignedPlanner and TestGreedyReservationAgent |  Major | reservation system | Subru Krishnan | Subru Krishnan |
| [YARN-5331](https://issues.apache.org/jira/browse/YARN-5331) | Extend RLESparseResourceAllocation with period for supporting recurring reservations in YARN ReservationSystem |  Major | resourcemanager | Subru Krishnan | Sangeetha Abdu Jyothi |
| [YARN-6374](https://issues.apache.org/jira/browse/YARN-6374) | Improve test coverage and add utility classes for common Docker operations |  Major | nodemanager, yarn | Shane Kumpf | Shane Kumpf |
| [YARN-6375](https://issues.apache.org/jira/browse/YARN-6375) | App level aggregation should not consider metric values reported in the previous aggregation cycle |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-6522](https://issues.apache.org/jira/browse/YARN-6522) | Make SLS JSON input file format simple and scalable |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [HDFS-11530](https://issues.apache.org/jira/browse/HDFS-11530) | Use HDFS specific network topology to choose datanode in BlockPlacementPolicyDefault |  Major | namenode | Yiqun Lin | Yiqun Lin |
| [YARN-6565](https://issues.apache.org/jira/browse/YARN-6565) | Fix memory leak and finish app trigger in AMRMProxy |  Critical | . | Botong Huang | Botong Huang |
| [HDFS-9342](https://issues.apache.org/jira/browse/HDFS-9342) | Erasure coding: client should update and commit block based on acknowledged size |  Critical | erasure-coding | Zhe Zhang | Sammi Chen |
| [YARN-6234](https://issues.apache.org/jira/browse/YARN-6234) | Support multiple attempts on the node when AMRMProxy is enabled |  Major | amrmproxy, federation, nodemanager | Subru Krishnan | Giovanni Matteo Fumarola |
| [HADOOP-14384](https://issues.apache.org/jira/browse/HADOOP-14384) | Reduce the visibility of FileSystem#newFSDataOutputStreamBuilder before the API becomes stable |  Blocker | fs | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-6563](https://issues.apache.org/jira/browse/YARN-6563) | ConcurrentModificationException in TimelineCollectorManager while stopping RM |  Major | resourcemanager | Rohith Sharma K S | Haibo Chen |
| [YARN-6435](https://issues.apache.org/jira/browse/YARN-6435) | [ATSv2] Can't retrieve more than 1000 versions of metrics in time series |  Critical | timelineserver | Rohith Sharma K S | Vrushali C |
| [YARN-6561](https://issues.apache.org/jira/browse/YARN-6561) | Update exception message during timeline collector aux service initialization |  Minor | timelineserver | Vrushali C | Vrushali C |
| [YARN-6306](https://issues.apache.org/jira/browse/YARN-6306) | NMClient API change for container upgrade |  Major | . | Jian He | Arun Suresh |
| [HADOOP-11572](https://issues.apache.org/jira/browse/HADOOP-11572) | s3a delete() operation fails during a concurrent delete of child entries |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-6560](https://issues.apache.org/jira/browse/YARN-6560) | SLS doesn't honor node total resource specified in sls-runner.xml |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-14432](https://issues.apache.org/jira/browse/HADOOP-14432) | S3A copyFromLocalFile to be robust, tested |  Major | fs | Steve Loughran | Steve Loughran |
| [YARN-5705](https://issues.apache.org/jira/browse/YARN-5705) | Show timeline data from ATS v2 in new web UI |  Major | . | Sunil Govindan | Akhil PB |
| [YARN-6111](https://issues.apache.org/jira/browse/YARN-6111) | Rumen input does't work in SLS |  Major | scheduler-load-simulator | YuJie Huang | Yufei Gu |
| [HDFS-11535](https://issues.apache.org/jira/browse/HDFS-11535) | Performance analysis of new DFSNetworkTopology#chooseRandom |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-11794](https://issues.apache.org/jira/browse/HDFS-11794) | Add ec sub command -listCodec to show currently supported ec codecs |  Major | erasure-coding | Sammi Chen | Sammi Chen |
| [HDFS-11823](https://issues.apache.org/jira/browse/HDFS-11823) | Extend TestDFSStripedIutputStream/TestDFSStripedOutputStream with a random EC policy |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-6587](https://issues.apache.org/jira/browse/YARN-6587) | Refactor of ResourceManager#startWebApp in a Util class |  Major | nodemanager, resourcemanager | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-6555](https://issues.apache.org/jira/browse/YARN-6555) | Store application flow context in NM state store for work-preserving restart |  Major | timelineserver | Vrushali C | Rohith Sharma K S |
| [HDFS-11446](https://issues.apache.org/jira/browse/HDFS-11446) | TestMaintenanceState#testWithNNAndDNRestart fails intermittently |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-6635](https://issues.apache.org/jira/browse/YARN-6635) | Refactor yarn-app pages in new YARN UI. |  Major | . | Akhil PB | Akhil PB |
| [YARN-6246](https://issues.apache.org/jira/browse/YARN-6246) | Identifying starved apps does not need the scheduler writelock |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HDFS-11904](https://issues.apache.org/jira/browse/HDFS-11904) | Reuse iip in unprotectedRemoveXAttrs calls |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-11359](https://issues.apache.org/jira/browse/HDFS-11359) | DFSAdmin report command supports displaying maintenance state datanodes |  Major | datanode, namenode | Yiqun Lin | Yiqun Lin |
| [YARN-6316](https://issues.apache.org/jira/browse/YARN-6316) | Provide help information and documentation for TimelineSchemaCreator |  Major | timelineserver | Li Lu | Haibo Chen |
| [HADOOP-14035](https://issues.apache.org/jira/browse/HADOOP-14035) | Reduce fair call queue backoff's impact on clients |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [YARN-6604](https://issues.apache.org/jira/browse/YARN-6604) | Allow metric TTL for Application table to be specified through cmd |  Major | ATSv2 | Haibo Chen | Haibo Chen |
| [YARN-6679](https://issues.apache.org/jira/browse/YARN-6679) | Reduce Resource instance overhead via non-PBImpl |  Major | resourcemanager | Daryn Sharp | Daryn Sharp |
| [HADOOP-13174](https://issues.apache.org/jira/browse/HADOOP-13174) | Add more debug logs for delegation tokens and authentication |  Minor | security | Xiao Chen | Xiao Chen |
| [HADOOP-13854](https://issues.apache.org/jira/browse/HADOOP-13854) | KMS should log error details in KMSExceptionsProvider |  Major | kms | Xiao Chen | Xiao Chen |
| [YARN-6682](https://issues.apache.org/jira/browse/YARN-6682) | Improve performance of AssignmentInformation datastructures |  Major | . | Daryn Sharp | Daryn Sharp |
| [YARN-6707](https://issues.apache.org/jira/browse/YARN-6707) | [ATSv2] Update HBase version to 1.2.6 |  Major | timelineserver | Varun Saxena | Vrushali C |
| [HDFS-10999](https://issues.apache.org/jira/browse/HDFS-10999) | Introduce separate stats for Replicated and Erasure Coded Blocks apart from the current Aggregated stats |  Major | erasure-coding | Wei-Chiu Chuang | Manoj Govindassamy |
| [HADOOP-14394](https://issues.apache.org/jira/browse/HADOOP-14394) | Provide Builder pattern for DistributedFileSystem.create |  Major | fs | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-14289](https://issues.apache.org/jira/browse/HADOOP-14289) | Move log4j APIs over to slf4j in hadoop-common |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14395](https://issues.apache.org/jira/browse/HADOOP-14395) | Provide Builder pattern for DistributedFileSystem.append |  Major | fs | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-14538](https://issues.apache.org/jira/browse/HADOOP-14538) | Fix TestFilterFileSystem and TestHarFileSystem failures after DistributedFileSystem.append API |  Major | fs | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-11916](https://issues.apache.org/jira/browse/HDFS-11916) | Extend TestErasureCodingPolicies/TestErasureCodingPolicyWithSnapshot with a random EC policy |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-6680](https://issues.apache.org/jira/browse/YARN-6680) | Avoid locking overhead for NO\_LABEL lookups |  Major | resourcemanager | Daryn Sharp | Daryn Sharp |
| [HADOOP-14296](https://issues.apache.org/jira/browse/HADOOP-14296) | Move logging APIs over to slf4j in hadoop-tools |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11606](https://issues.apache.org/jira/browse/HDFS-11606) | Add CLI cmd to remove an erasure code policy |  Major | erasure-coding | Kai Zheng | Tim Yao |
| [HDFS-11998](https://issues.apache.org/jira/browse/HDFS-11998) | Enable DFSNetworkTopology as default |  Major | namenode | Chen Liang | Chen Liang |
| [HADOOP-14542](https://issues.apache.org/jira/browse/HADOOP-14542) | Add IOUtils.cleanupWithLogger that accepts slf4j logger API |  Major | . | Akira Ajisaka | Chen Liang |
| [HADOOP-12940](https://issues.apache.org/jira/browse/HADOOP-12940) | Fix warnings from Spotbugs in hadoop-common |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14547](https://issues.apache.org/jira/browse/HADOOP-14547) | [WASB] the configured retry policy is not used for all storage operations. |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [HADOOP-14549](https://issues.apache.org/jira/browse/HADOOP-14549) | Use GenericTestUtils.setLogLevel when available in hadoop-tools |  Major | . | Akira Ajisaka | Wenxin He |
| [HADOOP-14573](https://issues.apache.org/jira/browse/HADOOP-14573) | regression: Azure tests which capture logs failing with move to SLF4J |  Major | fs/azure, test | Steve Loughran | Steve Loughran |
| [HADOOP-14546](https://issues.apache.org/jira/browse/HADOOP-14546) | Azure: Concurrent I/O does not work when secure.mode is enabled |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [HADOOP-14190](https://issues.apache.org/jira/browse/HADOOP-14190) | add more on s3 regions to the s3a documentation |  Minor | documentation, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-14297](https://issues.apache.org/jira/browse/HADOOP-14297) | Update the documentation about the new ec codecs config keys |  Major | documentation | Kai Sasaki | Kai Sasaki |
| [HADOOP-14609](https://issues.apache.org/jira/browse/HADOOP-14609) | NPE in AzureNativeFileSystemStore.checkContainer() if StorageException lacks an error code |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [YARN-5311](https://issues.apache.org/jira/browse/YARN-5311) | Document graceful decommission CLI and usage |  Major | documentation | Junping Du | Elek, Marton |
| [HADOOP-14601](https://issues.apache.org/jira/browse/HADOOP-14601) | Azure: Reuse ObjectMapper |  Major | fs/azure | Mingliang Liu | Mingliang Liu |
| [HADOOP-14596](https://issues.apache.org/jira/browse/HADOOP-14596) | AWS SDK 1.11+ aborts() on close() if \> 0 bytes in stream; logs error |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-2113](https://issues.apache.org/jira/browse/YARN-2113) | Add cross-user preemption within CapacityScheduler's leaf-queue |  Major | capacity scheduler | Vinod Kumar Vavilapalli | Sunil Govindan |
| [YARN-6627](https://issues.apache.org/jira/browse/YARN-6627) | Use deployed webapp folder to launch new YARN UI |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-11026](https://issues.apache.org/jira/browse/HDFS-11026) | Convert BlockTokenIdentifier to use Protobuf |  Major | hdfs, hdfs-client | Ewan Higgs | Ewan Higgs |
| [HADOOP-14091](https://issues.apache.org/jira/browse/HADOOP-14091) | AbstractFileSystem implementaion for 'wasbs' scheme |  Major | fs/azure | Varada Hemeswari | Varada Hemeswari |
| [YARN-6274](https://issues.apache.org/jira/browse/YARN-6274) | Documentation refers to incorrect nodemanager health checker interval property |  Trivial | documentation | Charles Zhang | Weiwei Yang |
| [YARN-6411](https://issues.apache.org/jira/browse/YARN-6411) | Clean up the overwrite of createDispatcher() in subclass of MockRM |  Minor | resourcemanager | Yufei Gu | Yufei Gu |
| [HADOOP-14344](https://issues.apache.org/jira/browse/HADOOP-14344) | Revert HADOOP-13606 swift FS to add a service load metadata file |  Major | . | John Zhuge | John Zhuge |
| [HDFS-11717](https://issues.apache.org/jira/browse/HDFS-11717) | Add unit test for HDFS-11709 StandbyCheckpointer should handle non-existing legacyOivImageDir gracefully |  Minor | ha, namenode | Erik Krogen | Erik Krogen |
| [HDFS-11870](https://issues.apache.org/jira/browse/HDFS-11870) | Add CLI cmd to enable/disable an erasure code policy |  Major | erasure-coding | Sammi Chen | lufei |


