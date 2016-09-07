
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
# "Apache Hadoop" Changelog

## Release 3.0.0-alpha1 - 2016-08-30

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8124](https://issues.apache.org/jira/browse/HADOOP-8124) | Remove the deprecated Syncable.sync() method |  Major | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3034](https://issues.apache.org/jira/browse/HDFS-3034) | Remove the deprecated Syncable.sync() method |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-5079](https://issues.apache.org/jira/browse/HDFS-5079) | Cleaning up NNHAStatusHeartbeat.State DatanodeProtocolProtos. |  Major | datanode, namenode | Konstantin Shvachko | Tao Luo |
| [HDFS-5570](https://issues.apache.org/jira/browse/HDFS-5570) | Deprecate hftp / hsftp and replace them with webhdfs / swebhdfs |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-10485](https://issues.apache.org/jira/browse/HADOOP-10485) | Remove dead classes in hadoop-streaming |  Major | tools | Haohui Mai | Haohui Mai |
| [HDFS-6246](https://issues.apache.org/jira/browse/HDFS-6246) | Remove 'dfs.support.append' flag from trunk code |  Minor | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-10474](https://issues.apache.org/jira/browse/HADOOP-10474) | Move o.a.h.record to hadoop-streaming |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-2538](https://issues.apache.org/jira/browse/HDFS-2538) | option to disable fsck dots |  Minor | . | Allen Wittenauer | Mohammad Kamrul Islam |
| [HADOOP-9902](https://issues.apache.org/jira/browse/HADOOP-9902) | Shell script rewrite |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11041](https://issues.apache.org/jira/browse/HADOOP-11041) | VersionInfo output specifies subversion |  Minor | build | Guo Ruijing | Tsuyoshi Ozawa |
| [MAPREDUCE-5972](https://issues.apache.org/jira/browse/MAPREDUCE-5972) | Fix typo 'programatically' in job.xml (and a few other places) |  Trivial | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-11356](https://issues.apache.org/jira/browse/HADOOP-11356) | Removed deprecated o.a.h.fs.permission.AccessControlException |  Major | . | Haohui Mai | Li Lu |
| [HADOOP-10950](https://issues.apache.org/jira/browse/HADOOP-10950) | rework  heap management  vars |  Major | scripts | Allen Wittenauer | John Smith |
| [MAPREDUCE-5785](https://issues.apache.org/jira/browse/MAPREDUCE-5785) | Derive heap size or mapreduce.\*.memory.mb automatically |  Major | mr-am, task | Gera Shegalov | Gera Shegalov |
| [YARN-2428](https://issues.apache.org/jira/browse/YARN-2428) | LCE default banned user list should have yarn |  Trivial | nodemanager | Allen Wittenauer | Varun Saxena |
| [MAPREDUCE-5653](https://issues.apache.org/jira/browse/MAPREDUCE-5653) | DistCp does not honour config-overrides for mapreduce.[map,reduce].memory.mb |  Major | distcp | Mithun Radhakrishnan | Ratandeep Ratti |
| [HADOOP-11657](https://issues.apache.org/jira/browse/HADOOP-11657) | Align the output of `hadoop fs -du` to be more Unix-like |  Minor | . | Nick Dimiduk | Akira Ajisaka |
| [HDFS-7302](https://issues.apache.org/jira/browse/HDFS-7302) | namenode -rollingUpgrade downgrade may finalize a rolling upgrade |  Major | namenode | Tsz Wo Nicholas Sze | Kai Sasaki |
| [HADOOP-6857](https://issues.apache.org/jira/browse/HADOOP-6857) | FsShell should report raw disk usage including replication factor |  Major | fs | Alex Kozlov | Byron Wong |
| [HADOOP-10115](https://issues.apache.org/jira/browse/HADOOP-10115) | Exclude duplicate jars in hadoop package under different component's lib |  Major | build | Vinayakumar B | Vinayakumar B |
| [YARN-3154](https://issues.apache.org/jira/browse/YARN-3154) | Should not upload partial logs for MR jobs or other "short-running' applications |  Blocker | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [MAPREDUCE-4424](https://issues.apache.org/jira/browse/MAPREDUCE-4424) | 'mapred job -list' command should show the job name as well |  Minor | mrv2 | Harsh J | Avinash Kujur |
| [YARN-3241](https://issues.apache.org/jira/browse/YARN-3241) | FairScheduler handles "invalid" queue names inconsistently |  Major | fairscheduler | zhihai xu | zhihai xu |
| [HDFS-7985](https://issues.apache.org/jira/browse/HDFS-7985) | WebHDFS should be always enabled |  Major | . | Haohui Mai | Li Lu |
| [HDFS-6353](https://issues.apache.org/jira/browse/HDFS-6353) | Check and make checkpoint before stopping the NameNode |  Major | namenode | Suresh Srinivas | Jing Zhao |
| [HADOOP-11553](https://issues.apache.org/jira/browse/HADOOP-11553) | Formalize the shell API |  Blocker | documentation, scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11731](https://issues.apache.org/jira/browse/HADOOP-11731) | Rework the changelog and releasenotes |  Major | documentation | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11746](https://issues.apache.org/jira/browse/HADOOP-11746) | rewrite test-patch.sh |  Major | build, test | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11627](https://issues.apache.org/jira/browse/HADOOP-11627) | Remove io.native.lib.available |  Major | native | Akira Ajisaka | Brahma Reddy Battula |
| [HDFS-7281](https://issues.apache.org/jira/browse/HDFS-7281) | Missing block is marked as corrupted block |  Major | namenode | Ming Ma | Ming Ma |
| [HDFS-8226](https://issues.apache.org/jira/browse/HDFS-8226) | Non-HA rollback compatibility broken |  Blocker | . | J.Andreina | J.Andreina |
| [MAPREDUCE-2632](https://issues.apache.org/jira/browse/MAPREDUCE-2632) | Avoid calling the partitioner when the numReduceTasks is 1. |  Major | . | Ravi Teja Ch N V | Sunil G |
| [HDFS-8241](https://issues.apache.org/jira/browse/HDFS-8241) | Remove unused NameNode startup option -finalize |  Minor | namenode | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8332](https://issues.apache.org/jira/browse/HDFS-8332) | DFS client API calls should check filesystem closed |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-11698](https://issues.apache.org/jira/browse/HADOOP-11698) | Remove DistCpV1 and Logalyzer |  Major | tools/distcp | Allen Wittenauer | Brahma Reddy Battula |
| [HADOOP-11772](https://issues.apache.org/jira/browse/HADOOP-11772) | RPC Invoker relies on static ClientCache which has synchronized(this) blocks |  Major | ipc, performance | Gopal V | Haohui Mai |
| [YARN-2336](https://issues.apache.org/jira/browse/YARN-2336) | Fair scheduler REST api returns a missing '[' bracket JSON for deep queue tree |  Major | fairscheduler | Kenji Kikushima | Akira Ajisaka |
| [HDFS-8135](https://issues.apache.org/jira/browse/HDFS-8135) | Remove the deprecated FSConstants class |  Major | . | Haohui Mai | Li Lu |
| [MAPREDUCE-6336](https://issues.apache.org/jira/browse/MAPREDUCE-6336) | Enable v2 FileOutputCommitter by default |  Major | mrv2 | Gera Shegalov | Siqi Li |
| [YARN-41](https://issues.apache.org/jira/browse/YARN-41) | The RM should handle the graceful shutdown of the NM. |  Major | nodemanager, resourcemanager | Ravi Teja Ch N V | Devaraj K |
| [HDFS-8591](https://issues.apache.org/jira/browse/HDFS-8591) | Remove support for deprecated configuration key dfs.namenode.decommission.nodes.per.interval |  Minor | namenode | Andrew Wang | Andrew Wang |
| [HDFS-6564](https://issues.apache.org/jira/browse/HDFS-6564) | Use slf4j instead of common-logging in hdfs-client |  Major | build | Haohui Mai | Rakesh R |
| [MAPREDUCE-6427](https://issues.apache.org/jira/browse/MAPREDUCE-6427) | Fix typo in JobHistoryEventHandler |  Minor | . | Brahma Reddy Battula | Ray Chiang |
| [HADOOP-12269](https://issues.apache.org/jira/browse/HADOOP-12269) | Update aws-sdk dependency to 1.10.6; move to aws-sdk-s3 |  Major | fs/s3 | Thomas Demoor | Thomas Demoor |
| [HDFS-8895](https://issues.apache.org/jira/browse/HDFS-8895) | Remove deprecated BlockStorageLocation APIs |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-8900](https://issues.apache.org/jira/browse/HDFS-8900) | Compact XAttrs to optimize memory footprint. |  Major | namenode | Yi Liu | Yi Liu |
| [HDFS-8981](https://issues.apache.org/jira/browse/HDFS-8981) | Adding revision to data node jmx getVersion() method |  Minor | . | Siqi Li | Siqi Li |
| [YARN-4087](https://issues.apache.org/jira/browse/YARN-4087) | Followup fixes after YARN-2019 regarding RM behavior when state-store error occurs |  Major | . | Jian He | Jian He |
| [HADOOP-12416](https://issues.apache.org/jira/browse/HADOOP-12416) | Trash messages should be handled by Logger instead of being delivered on System.out |  Major | trash | Ashutosh Chauhan | Mingliang Liu |
| [HDFS-9085](https://issues.apache.org/jira/browse/HDFS-9085) | Show renewer information in DelegationTokenIdentifier#toString |  Trivial | security | zhihai xu | zhihai xu |
| [HADOOP-12495](https://issues.apache.org/jira/browse/HADOOP-12495) | Fix posix\_spawn error on OS X |  Major | util | Allen Wittenauer | Allen Wittenauer |
| [HDFS-9070](https://issues.apache.org/jira/browse/HDFS-9070) | Allow fsck display pending replica location information for being-written blocks |  Major | . | Rui Gao | Rui Gao |
| [HDFS-9278](https://issues.apache.org/jira/browse/HDFS-9278) | Fix preferredBlockSize typo in OIV XML output |  Trivial | tools | Nicole Pazmany | Nicole Pazmany |
| [HADOOP-10787](https://issues.apache.org/jira/browse/HADOOP-10787) | Rename/remove non-HADOOP\_\*, etc from the shell scripts |  Blocker | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12294](https://issues.apache.org/jira/browse/HADOOP-12294) | Throw an Exception when fs.permissions.umask-mode is misconfigured |  Major | conf | Chang Li | Chang Li |
| [HADOOP-10465](https://issues.apache.org/jira/browse/HADOOP-10465) | Fix use of generics within SortedMapWritable |  Minor | . | Bertrand Dechoux | Bertrand Dechoux |
| [HDFS-5165](https://issues.apache.org/jira/browse/HDFS-5165) | Remove the TotalFiles metrics |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-9047](https://issues.apache.org/jira/browse/HDFS-9047) | Retire libwebhdfs |  Major | webhdfs | Allen Wittenauer | Haohui Mai |
| [HADOOP-12651](https://issues.apache.org/jira/browse/HADOOP-12651) | Replace dev-support with wrappers to Yetus |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6613](https://issues.apache.org/jira/browse/MAPREDUCE-6613) | Change mapreduce.jobhistory.jhist.format default from json to binary |  Minor | . | Ray Chiang | Ray Chiang |
| [HDFS-9395](https://issues.apache.org/jira/browse/HDFS-9395) | Make HDFS audit logging consistant |  Major | . | Kihwal Lee | Kuhu Shukla |
| [HADOOP-12552](https://issues.apache.org/jira/browse/HADOOP-12552) | Fix undeclared/unused dependency to httpclient |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-12850](https://issues.apache.org/jira/browse/HADOOP-12850) | pull shell code out of hadoop-dist |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11792](https://issues.apache.org/jira/browse/HADOOP-11792) | Remove all of the CHANGES.txt files |  Major | build | Allen Wittenauer | Andrew Wang |
| [HADOOP-12857](https://issues.apache.org/jira/browse/HADOOP-12857) | Rework hadoop-tools |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12967](https://issues.apache.org/jira/browse/HADOOP-12967) | Remove FileUtil#copyMerge |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12811](https://issues.apache.org/jira/browse/HADOOP-12811) | Change kms server port number which conflicts with HMaster port number |  Critical | kms | Yufeng Jiang | Xiao Chen |
| [YARN-4784](https://issues.apache.org/jira/browse/YARN-4784) | Fairscheduler: defaultQueueSchedulingPolicy should not accept FIFO |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [HDFS-9427](https://issues.apache.org/jira/browse/HDFS-9427) | HDFS should not default to ephemeral ports |  Critical | datanode, hdfs-client, namenode | Arpit Agarwal | Xiaobing Zhou |
| [HADOOP-12563](https://issues.apache.org/jira/browse/HADOOP-12563) | Updated utility to create/modify token files |  Major | . | Allen Wittenauer | Matthew Paduano |
| [HADOOP-13045](https://issues.apache.org/jira/browse/HADOOP-13045) | hadoop\_add\_classpath is not working in .hadooprc |  Major | . | Akira Ajisaka | Allen Wittenauer |
| [MAPREDUCE-6526](https://issues.apache.org/jira/browse/MAPREDUCE-6526) | Remove usage of metrics v1 from hadoop-mapreduce |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12504](https://issues.apache.org/jira/browse/HADOOP-12504) | Remove metrics v1 |  Blocker | metrics | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10337](https://issues.apache.org/jira/browse/HDFS-10337) | OfflineEditsViewer stats option should print 0 instead of null for the count of operations |  Minor | . | Akira Ajisaka | Yiqun Lin |
| [HADOOP-11858](https://issues.apache.org/jira/browse/HADOOP-11858) | [JDK8] Set minimum version of Hadoop 3 to JDK 8 |  Blocker | build | Robert Kanter | Robert Kanter |
| [YARN-5035](https://issues.apache.org/jira/browse/YARN-5035) | FairScheduler: Adjust maxAssign dynamically when assignMultiple is turned on |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-13175](https://issues.apache.org/jira/browse/HADOOP-13175) | Remove hadoop-ant from hadoop-tools |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-12892](https://issues.apache.org/jira/browse/HADOOP-12892) | fix/rewrite create-release |  Blocker | build | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-9613](https://issues.apache.org/jira/browse/HADOOP-9613) | [JDK8] Update jersey version to latest 1.x release |  Major | build | Timothy St. Clair | Tsuyoshi Ozawa |
| [HADOOP-13209](https://issues.apache.org/jira/browse/HADOOP-13209) | replace slaves with workers |  Major | scripts | John Smith | John Smith |
| [HDFS-6434](https://issues.apache.org/jira/browse/HDFS-6434) | Default permission for creating file should be 644 for WebHdfs/HttpFS |  Minor | webhdfs | Juan Yu | Wellington Chevreuil |
| [HADOOP-12864](https://issues.apache.org/jira/browse/HADOOP-12864) | Remove bin/rcc script |  Blocker | tools | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12709](https://issues.apache.org/jira/browse/HADOOP-12709) | Cut s3:// from trunk |  Major | fs/s3 | Steve Loughran | Mingliang Liu |
| [HADOOP-12064](https://issues.apache.org/jira/browse/HADOOP-12064) | [JDK8] Update guice version to 4.0 |  Blocker | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-10548](https://issues.apache.org/jira/browse/HDFS-10548) | Remove the long deprecated BlockReaderRemote |  Major | hdfs | Kai Zheng | Kai Zheng |
| [HADOOP-13382](https://issues.apache.org/jira/browse/HADOOP-13382) | remove unneeded commons-httpclient dependencies from POM files in Hadoop and sub-projects |  Major | build | Matt Foley | Matt Foley |
| [HDFS-10689](https://issues.apache.org/jira/browse/HDFS-10689) | Hdfs dfs chmod should reset sticky bit permission when the bit is omitted in the octal mode |  Minor | fs | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-7933](https://issues.apache.org/jira/browse/HDFS-7933) | fsck should also report decommissioning replicas. |  Major | namenode | Jitendra Nath Pandey | Xiaoyu Yao |
| [HADOOP-12726](https://issues.apache.org/jira/browse/HADOOP-12726) | Unsupported FS operations should throw UnsupportedOperationException |  Major | fs | Daniel Templeton | Daniel Templeton |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4785](https://issues.apache.org/jira/browse/YARN-4785) | inconsistent value type of the "type" field for LeafQueueInfo in response of RM REST API - cluster/scheduler |  Major | webapp | Jayesh | Varun Vasudev |
| [HADOOP-11393](https://issues.apache.org/jira/browse/HADOOP-11393) | Revert HADOOP\_PREFIX, go back to HADOOP\_HOME |  Major | scripts, tracing | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12930](https://issues.apache.org/jira/browse/HADOOP-12930) | [Umbrella] Dynamic subcommands for hadoop shell scripts |  Critical | scripts | Allen Wittenauer | Allen Wittenauer |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-4887](https://issues.apache.org/jira/browse/MAPREDUCE-4887) | Rehashing partitioner for better distribution |  Major | . | Radim Kolar | Radim Kolar |
| [HADOOP-11013](https://issues.apache.org/jira/browse/HADOOP-11013) | CLASSPATH handling should be consolidated, debuggable |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-6590](https://issues.apache.org/jira/browse/HADOOP-6590) | Add a username check for hadoop sub-commands |  Minor | scripts | Scott Chen | John Smith |
| [HADOOP-11353](https://issues.apache.org/jira/browse/HADOOP-11353) | Add support for .hadooprc |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-9044](https://issues.apache.org/jira/browse/HADOOP-9044) | add FindClass main class to provide classpath checking of installations |  Minor | util | Steve Loughran | Steve Loughran |
| [HADOOP-11485](https://issues.apache.org/jira/browse/HADOOP-11485) | Pluggable shell integration |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-8934](https://issues.apache.org/jira/browse/HADOOP-8934) | Shell command ls should include sort options |  Minor | fs | Jonathan Allen | Jonathan Allen |
| [HADOOP-11554](https://issues.apache.org/jira/browse/HADOOP-11554) | Expose HadoopKerberosName as a hadoop subcommand |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11565](https://issues.apache.org/jira/browse/HADOOP-11565) | Add --slaves shell option |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-9477](https://issues.apache.org/jira/browse/HADOOP-9477) | Add posixGroups support for LDAP groups mapping service |  Major | . | Kai Zheng | Dapeng Sun |
| [HDFS-8009](https://issues.apache.org/jira/browse/HDFS-8009) | Signal congestion on the DataNode |  Major | datanode | Haohui Mai | Haohui Mai |
| [YARN-2901](https://issues.apache.org/jira/browse/YARN-2901) | Add errors and warning metrics page to RM, NM web UI |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [HADOOP-9642](https://issues.apache.org/jira/browse/HADOOP-9642) | Configuration to resolve environment variables via ${env.VARIABLE} references |  Minor | conf, scripts | Steve Loughran | Kengo Seki |
| [HDFS-7891](https://issues.apache.org/jira/browse/HDFS-7891) | A block placement policy with best rack failure tolerance |  Minor | namenode | Walter Su | Walter Su |
| [HADOOP-11843](https://issues.apache.org/jira/browse/HADOOP-11843) | Make setting up the build environment easier |  Major | build | Niels Basjes | Niels Basjes |
| [MAPREDUCE-6284](https://issues.apache.org/jira/browse/MAPREDUCE-6284) | Add Task Attempt State API to MapReduce Application Master REST API |  Minor | . | Ryu Kobayashi | Ryu Kobayashi |
| [HADOOP-10971](https://issues.apache.org/jira/browse/HADOOP-10971) | Add -C flag to make `hadoop fs -ls` print filenames only |  Major | fs | Ryan Williams | Kengo Seki |
| [MAPREDUCE-6364](https://issues.apache.org/jira/browse/MAPREDUCE-6364) | Add a "Kill" link to Task Attempts page |  Minor | applicationmaster | Ryu Kobayashi | Ryu Kobayashi |
| [MAPREDUCE-6304](https://issues.apache.org/jira/browse/MAPREDUCE-6304) | Specifying node labels when submitting MR jobs |  Major | . | Jian Fang | Naganarasimha G R |
| [HADOOP-7947](https://issues.apache.org/jira/browse/HADOOP-7947) | Validate XMLs if a relevant tool is available, when using scripts |  Major | scripts | Harsh J | Kengo Seki |
| [HDFS-8487](https://issues.apache.org/jira/browse/HDFS-8487) | Generalize BlockInfo in preparation of merging HDFS-7285 into trunk and branch-2 |  Major | namenode | Zhe Zhang | Zhe Zhang |
| [HDFS-8608](https://issues.apache.org/jira/browse/HDFS-8608) | Merge HDFS-7912 to trunk and branch-2 (track BlockInfo instead of Block in UnderReplicatedBlocks and PendingReplicationBlocks) |  Major | . | Zhe Zhang | Zhe Zhang |
| [HDFS-6440](https://issues.apache.org/jira/browse/HDFS-6440) | Support more than 2 NameNodes |  Major | auto-failover, ha, namenode | Jesse Yates | Jesse Yates |
| [HADOOP-5732](https://issues.apache.org/jira/browse/HADOOP-5732) | Add SFTP FileSystem |  Minor | fs | Íñigo Goiri | ramtin |
| [HDFS-8622](https://issues.apache.org/jira/browse/HDFS-8622) | Implement GETCONTENTSUMMARY operation for WebImageViewer |  Major | . | Jagadesh Kiran N | Jagadesh Kiran N |
| [HDFS-8155](https://issues.apache.org/jira/browse/HDFS-8155) | Support OAuth2 in WebHDFS |  Major | webhdfs | Jakob Homan | Jakob Homan |
| [MAPREDUCE-6415](https://issues.apache.org/jira/browse/MAPREDUCE-6415) | Create a tool to combine aggregated logs into HAR files |  Major | . | Robert Kanter | Robert Kanter |
| [HADOOP-12360](https://issues.apache.org/jira/browse/HADOOP-12360) | Create StatsD metrics2 sink |  Minor | metrics | Dave Marion | Dave Marion |
| [HDFS-7285](https://issues.apache.org/jira/browse/HDFS-7285) | Erasure Coding Support inside HDFS |  Major | . | Weihua Jiang | Zhe Zhang |
| [HADOOP-10949](https://issues.apache.org/jira/browse/HADOOP-10949) | metrics2 sink plugin for Apache Kafka |  Major | metrics | Babak Behzad | Babak Behzad |
| [YARN-261](https://issues.apache.org/jira/browse/YARN-261) | Ability to fail AM attempts |  Major | api | Jason Lowe | Rohith Sharma K S |
| [HDFS-9184](https://issues.apache.org/jira/browse/HDFS-9184) | Logging HDFS operation's caller context into audit logs |  Major | . | Mingliang Liu | Mingliang Liu |
| [HDFS-9057](https://issues.apache.org/jira/browse/HDFS-9057) | allow/disallow snapshots via webhdfs |  Major | webhdfs | Allen Wittenauer | Brahma Reddy Battula |
| [YARN-4349](https://issues.apache.org/jira/browse/YARN-4349) | Support CallerContext in YARN |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-12366](https://issues.apache.org/jira/browse/HADOOP-12366) | expose calculated paths |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12321](https://issues.apache.org/jira/browse/HADOOP-12321) | Make JvmPauseMonitor an AbstractService |  Major | . | Steve Loughran | Sunil G |
| [YARN-3623](https://issues.apache.org/jira/browse/YARN-3623) | We should have a config to indicate the Timeline Service version |  Major | timelineserver | Zhijie Shen | Xuan Gong |
| [HADOOP-12657](https://issues.apache.org/jira/browse/HADOOP-12657) | Add a option to skip newline on empty files with getMerge -nl |  Minor | . | Jan Filipiak | Kanaka Kumar Avvaru |
| [YARN-3458](https://issues.apache.org/jira/browse/YARN-3458) | CPU resource monitoring in Windows |  Minor | nodemanager | Inigo Goiri | Inigo Goiri |
| [HADOOP-12691](https://issues.apache.org/jira/browse/HADOOP-12691) | Add CSRF Filter for REST APIs to Hadoop Common |  Major | security | Larry McCay | Larry McCay |
| [HADOOP-12635](https://issues.apache.org/jira/browse/HADOOP-12635) | Adding Append API support for WASB |  Major | azure | Dushyanth | Dushyanth |
| [HDFS-9525](https://issues.apache.org/jira/browse/HDFS-9525) | hadoop utilities need to support provided delegation tokens |  Blocker | security | Allen Wittenauer | HeeSoo Kim |
| [HADOOP-12702](https://issues.apache.org/jira/browse/HADOOP-12702) | Add an HDFS metrics sink |  Major | metrics | Daniel Templeton | Daniel Templeton |
| [HADOOP-12426](https://issues.apache.org/jira/browse/HADOOP-12426) | Add Entry point for Kerberos health check |  Minor | security | Steve Loughran | Steve Loughran |
| [HDFS-9244](https://issues.apache.org/jira/browse/HDFS-9244) | Support nested encryption zones |  Major | encryption | Xiaoyu Yao | Zhe Zhang |
| [HADOOP-12548](https://issues.apache.org/jira/browse/HADOOP-12548) | Read s3a creds from a Credential Provider |  Major | fs/s3 | Allen Wittenauer | Larry McCay |
| [HDFS-9711](https://issues.apache.org/jira/browse/HDFS-9711) | Integrate CSRF prevention filter in WebHDFS. |  Major | datanode, namenode, webhdfs | Chris Nauroth | Chris Nauroth |
| [HDFS-9835](https://issues.apache.org/jira/browse/HDFS-9835) | OIV: add ReverseXML processor which reconstructs an fsimage from an XML file |  Major | tools | Colin P. McCabe | Colin P. McCabe |
| [HDFS-9239](https://issues.apache.org/jira/browse/HDFS-9239) | DataNode Lifeline Protocol: an alternative protocol for reporting DataNode liveness |  Major | datanode, namenode | Chris Nauroth | Chris Nauroth |
| [HADOOP-12909](https://issues.apache.org/jira/browse/HADOOP-12909) | Change ipc.Client to support asynchronous calls |  Major | ipc | Tsz Wo Nicholas Sze | Xiaobing Zhou |
| [HDFS-9945](https://issues.apache.org/jira/browse/HDFS-9945) | Datanode command for evicting writers |  Major | datanode | Kihwal Lee | Kihwal Lee |
| [HADOOP-13008](https://issues.apache.org/jira/browse/HADOOP-13008) | Add XFS Filter for UIs to Hadoop Common |  Major | security | Larry McCay | Larry McCay |
| [HADOOP-13065](https://issues.apache.org/jira/browse/HADOOP-13065) | Add a new interface for retrieving FS and FC Statistics |  Major | fs | Ram Venkatesh | Mingliang Liu |
| [HADOOP-12723](https://issues.apache.org/jira/browse/HADOOP-12723) | S3A: Add ability to plug in any AWSCredentialsProvider |  Major | fs/s3 | Steven K. Wong | Steven K. Wong |
| [HADOOP-12847](https://issues.apache.org/jira/browse/HADOOP-12847) | hadoop daemonlog should support https and SPNEGO for Kerberized cluster |  Major | security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13226](https://issues.apache.org/jira/browse/HADOOP-13226) | Support async call retry and failover |  Major | io, ipc | Xiaobing Zhou | Tsz Wo Nicholas Sze |
| [HADOOP-12537](https://issues.apache.org/jira/browse/HADOOP-12537) | S3A to support Amazon STS temporary credentials |  Minor | fs/s3 | Sean Mackrory | Sean Mackrory |
| [HADOOP-12666](https://issues.apache.org/jira/browse/HADOOP-12666) | Support Microsoft Azure Data Lake - as a file system in Hadoop |  Major | fs, fs/azure, tools | Vishwajeet Dusane | Vishwajeet Dusane |
| [HDFS-1312](https://issues.apache.org/jira/browse/HDFS-1312) | Re-balance disks within a Datanode |  Major | datanode | Travis Crawford | Anu Engineer |
| [YARN-2928](https://issues.apache.org/jira/browse/YARN-2928) | YARN Timeline Service v.2: alpha 1 |  Critical | timelineserver | Sangjin Lee | Sangjin Lee |
| [MAPREDUCE-6331](https://issues.apache.org/jira/browse/MAPREDUCE-6331) | [Umbrella] Make MapReduce work with Timeline Service Nextgen (YARN-2928) |  Major | . | Vinod Kumar Vavilapalli | Sangjin Lee |
| [HADOOP-12747](https://issues.apache.org/jira/browse/HADOOP-12747) | support wildcard in libjars argument |  Major | util | Sangjin Lee | Sangjin Lee |
| [HDFS-9804](https://issues.apache.org/jira/browse/HDFS-9804) | Allow long-running Balancer to login with keytab |  Major | balancer & mover, security | Xiao Chen | Xiao Chen |
| [MAPREDUCE-6690](https://issues.apache.org/jira/browse/MAPREDUCE-6690) | Limit the number of resources a single map reduce job can submit for localization |  Major | . | Chris Trezzo | Chris Trezzo |
| [MAPREDUCE-6578](https://issues.apache.org/jira/browse/MAPREDUCE-6578) | Add support for HDFS heterogeneous storage testing to TestDFSIO |  Major | . | Wei Zhou | Wei Zhou |
| [HADOOP-13396](https://issues.apache.org/jira/browse/HADOOP-13396) | Allow pluggable audit loggers in KMS |  Major | kms | Xiao Chen | Xiao Chen |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-2944](https://issues.apache.org/jira/browse/MAPREDUCE-2944) | Improve checking of input for Api displayTasks() |  Trivial | client | XieXianshan | XieXianshan |
| [HADOOP-8017](https://issues.apache.org/jira/browse/HADOOP-8017) | Configure hadoop-main pom to get rid of M2E plugin execution not covered |  Major | build | Eric Charles | Eric Charles |
| [HADOOP-8078](https://issues.apache.org/jira/browse/HADOOP-8078) | Add capability to turn on security in unit tests. |  Major | test | Jitendra Nath Pandey | Jaimin D Jetly |
| [MAPREDUCE-3956](https://issues.apache.org/jira/browse/MAPREDUCE-3956) | Remove the use of the deprecated Syncable.sync() method |  Minor | examples | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-8147](https://issues.apache.org/jira/browse/HADOOP-8147) | test-patch should run tests with -fn to avoid masking test failures |  Major | test | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8290](https://issues.apache.org/jira/browse/HADOOP-8290) | Remove two remaining references to "hadoop.native.lib" oldprop |  Trivial | test | Harsh J | Harsh J |
| [HDFS-2743](https://issues.apache.org/jira/browse/HDFS-2743) | Streamline usage of bookkeeper journal manager |  Major | ha | Ivan Kelly | Ivan Kelly |
| [HADOOP-8312](https://issues.apache.org/jira/browse/HADOOP-8312) | testpatch.sh should provide a simpler way to see which warnings changed |  Major | scripts | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-3293](https://issues.apache.org/jira/browse/HDFS-3293) | Implement equals for journainfo class. |  Minor | namenode | Hari Mankude | Hari Mankude |
| [HADOOP-8375](https://issues.apache.org/jira/browse/HADOOP-8375) | test-patch should stop immediately once it has found compilation errors |  Major | build | Tsz Wo Nicholas Sze | Robert Joseph Evans |
| [HADOOP-8297](https://issues.apache.org/jira/browse/HADOOP-8297) | Writable javadocs don't carry default constructor |  Trivial | documentation | Harsh J | Harsh J |
| [HADOOP-7659](https://issues.apache.org/jira/browse/HADOOP-7659) | fs -getmerge isn't guaranteed to work well over non-HDFS filesystems |  Minor | fs | Harsh J | Harsh J |
| [MAPREDUCE-3302](https://issues.apache.org/jira/browse/MAPREDUCE-3302) | Remove the last dependency call from org.apache.hadoop.record package in MR. |  Minor | client | Harsh J | Harsh J |
| [MAPREDUCE-2384](https://issues.apache.org/jira/browse/MAPREDUCE-2384) | The job submitter should make sure to validate jobs before creation of necessary files |  Major | job submission, test | Denny Ye | Harsh J |
| [HDFS-3476](https://issues.apache.org/jira/browse/HDFS-3476) | Correct the default used in TestDFSClientRetries.busyTest() after HDFS-3462 |  Minor | test | Harsh J | Harsh J |
| [HDFS-3478](https://issues.apache.org/jira/browse/HDFS-3478) | Test quotas with Long.Max\_Value |  Minor | namenode | Sujay Rau | Sujay Rau |
| [MAPREDUCE-4371](https://issues.apache.org/jira/browse/MAPREDUCE-4371) | Check for cyclic dependencies in Jobcontrol job DAG |  Major | mrv1 | madhukara phatak | madhukara phatak |
| [HADOOP-8523](https://issues.apache.org/jira/browse/HADOOP-8523) | test-patch.sh doesn't validate patches before building |  Minor | build | Jack Dintruff | Jack Dintruff |
| [HADOOP-8521](https://issues.apache.org/jira/browse/HADOOP-8521) | Port StreamInputFormat to new Map Reduce API |  Major | streaming | madhukara phatak | madhukara phatak |
| [HADOOP-8593](https://issues.apache.org/jira/browse/HADOOP-8593) | add  the missed @Override to methods in Metric/Metric2 package |  Minor | metrics | Brandon Li | Brandon Li |
| [HDFS-3768](https://issues.apache.org/jira/browse/HDFS-3768) | Exception in TestJettyHelper is incorrect |  Minor | test | Jakob Homan | Eli Reisman |
| [HDFS-3834](https://issues.apache.org/jira/browse/HDFS-3834) | Unused Command.java fields NAME, DESCRIPTION and USAGE |  Minor | shell | Jing Zhao | Jing Zhao |
| [HDFS-2580](https://issues.apache.org/jira/browse/HDFS-2580) | NameNode#main(...) can make use of GenericOptionsParser. |  Minor | namenode | Harsh J | Harsh J |
| [HADOOP-8588](https://issues.apache.org/jira/browse/HADOOP-8588) | SerializationFactory shouldn't throw a NullPointerException if the serializations list is empty |  Minor | io | Harsh J | Sho Shimauchi |
| [HADOOP-7930](https://issues.apache.org/jira/browse/HADOOP-7930) | Kerberos relogin interval in UserGroupInformation should be configurable |  Major | security | Alejandro Abdelnur | Robert Kanter |
| [HADOOP-8838](https://issues.apache.org/jira/browse/HADOOP-8838) | Colorize the test-patch output sent to JIRA |  Trivial | build | Harsh J | Harsh J |
| [MAPREDUCE-4686](https://issues.apache.org/jira/browse/MAPREDUCE-4686) | hadoop-mapreduce-client-core fails compilation in Eclipse due to missing Avro-generated classes |  Major | build | Chris Nauroth | Chris Nauroth |
| [HADOOP-8864](https://issues.apache.org/jira/browse/HADOOP-8864) | Addendum to HADOOP-8840: Add a coloring case for +0 results too. |  Trivial | precommit | Harsh J | Harsh J |
| [HDFS-3995](https://issues.apache.org/jira/browse/HDFS-3995) | Use DFSTestUtil.createFile() for file creation and writing in test cases |  Minor | test | Jing Zhao | Jing Zhao |
| [HADOOP-8920](https://issues.apache.org/jira/browse/HADOOP-8920) | Add more javadoc to metrics2 related classes |  Minor | metrics | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-8776](https://issues.apache.org/jira/browse/HADOOP-8776) | Provide an option in test-patch that can enable / disable compiling native code |  Minor | build | Hemanth Yamijala | Chris Nauroth |
| [MAPREDUCE-4735](https://issues.apache.org/jira/browse/MAPREDUCE-4735) | Make arguments in TestDFSIO case insensitive |  Trivial | test | Robert Kanter | Brandon Li |
| [HADOOP-6616](https://issues.apache.org/jira/browse/HADOOP-6616) | Improve documentation for rack awareness |  Major | documentation | Jeff Hammerbacher | Adam Faris |
| [HADOOP-9075](https://issues.apache.org/jira/browse/HADOOP-9075) | FileContext#FSLinkResolver should be made static |  Minor | . | Suresh Srinivas | Arpit Agarwal |
| [HDFS-3358](https://issues.apache.org/jira/browse/HDFS-3358) | Specify explicitly that the NN UI status total is talking of persistent objects on heap. |  Trivial | namenode | Harsh J | Harsh J |
| [HADOOP-9277](https://issues.apache.org/jira/browse/HADOOP-9277) | Improve javadoc for FileContext |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-9394](https://issues.apache.org/jira/browse/HADOOP-9394) | Port findHangingTest.sh from HBase to Hadoop |  Minor | . | Andrew Wang | Andrew Wang |
| [HDFS-4665](https://issues.apache.org/jira/browse/HDFS-4665) | Move TestNetworkTopologyWithNodeGroup to Common |  Minor | test | Luke Lu | Junping Du |
| [MAPREDUCE-5232](https://issues.apache.org/jira/browse/MAPREDUCE-5232) | log classpath and other key properties on child JVM start |  Major | mrv1, mrv2 | Sangjin Lee | Sangjin Lee |
| [HDFS-5041](https://issues.apache.org/jira/browse/HDFS-5041) | Add the time of last heartbeat to dead server Web UI |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-10044](https://issues.apache.org/jira/browse/HADOOP-10044) | Improve the javadoc of rpc code |  Minor | . | Sanjay Radia | Sanjay Radia |
| [MAPREDUCE-5197](https://issues.apache.org/jira/browse/MAPREDUCE-5197) | Checkpoint Service: a library component to facilitate checkpoint of task state |  Major | mrv2 | Carlo Curino | Carlo Curino |
| [MAPREDUCE-5196](https://issues.apache.org/jira/browse/MAPREDUCE-5196) | CheckpointAMPreemptionPolicy implements preemption in MR AM via checkpointing |  Major | mr-am, mrv2 | Carlo Curino | Carlo Curino |
| [HDFS-6228](https://issues.apache.org/jira/browse/HDFS-6228) | comments typo fix for FsDatasetImpl.java |  Trivial | . | yunjiong zhao | yunjiong zhao |
| [HADOOP-8719](https://issues.apache.org/jira/browse/HADOOP-8719) | Workaround for kerberos-related log errors upon running any hadoop command on OSX |  Trivial | scripts | Jianbin Wei | Jianbin Wei |
| [HADOOP-10834](https://issues.apache.org/jira/browse/HADOOP-10834) | Typo in CredentialShell usage |  Minor | security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-6013](https://issues.apache.org/jira/browse/MAPREDUCE-6013) | mapred version is missing |  Major | scripts | Allen Wittenauer | Akira Ajisaka |
| [MAPREDUCE-2841](https://issues.apache.org/jira/browse/MAPREDUCE-2841) | Task level native optimization |  Major | task | Binglin Chang | Sean Zhong |
| [YARN-2438](https://issues.apache.org/jira/browse/YARN-2438) | yarn-env.sh cleanup |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11092](https://issues.apache.org/jira/browse/HADOOP-11092) | hadoop shell commands should print usage if not given a class |  Major | scripts | Bruno Mahé | Allen Wittenauer |
| [HDFS-7278](https://issues.apache.org/jira/browse/HDFS-7278) | Add a command that allows sysadmins to manually trigger full block reports from a DN |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11025](https://issues.apache.org/jira/browse/HADOOP-11025) | hadoop-daemons.sh should just call hdfs directly |  Major | scripts | Allen Wittenauer | Masatake Iwasaki |
| [HADOOP-11150](https://issues.apache.org/jira/browse/HADOOP-11150) | hadoop command should show the reason on failure by invalid COMMAND or CLASSNAME |  Minor | scripts | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11208](https://issues.apache.org/jira/browse/HADOOP-11208) | Replace "daemon" with better name in script subcommands |  Major | scripts | Yongjun Zhang | Allen Wittenauer |
| [HADOOP-10926](https://issues.apache.org/jira/browse/HADOOP-10926) | Improve smart-apply-patch.sh to apply binary diffs |  Major | . | Andrew Wang | Colin P. McCabe |
| [HADOOP-11081](https://issues.apache.org/jira/browse/HADOOP-11081) | Document hadoop properties expected to be set by the shell code in \*-env.sh |  Major | documentation, scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-2472](https://issues.apache.org/jira/browse/YARN-2472) | yarn-daemons.sh should just call yarn directly |  Major | scripts | Allen Wittenauer | Masatake Iwasaki |
| [YARN-2301](https://issues.apache.org/jira/browse/YARN-2301) | Improve yarn container command |  Major | . | Jian He | Naganarasimha G R |
| [HADOOP-11352](https://issues.apache.org/jira/browse/HADOOP-11352) | Clean up test-patch.sh to disable "+1 contrib tests" |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-2437](https://issues.apache.org/jira/browse/YARN-2437) | start-yarn.sh/stop-yarn should give info |  Major | scripts | Allen Wittenauer | Varun Saxena |
| [HDFS-7531](https://issues.apache.org/jira/browse/HDFS-7531) | Improve the concurrent access on FsVolumeList |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11058](https://issues.apache.org/jira/browse/HADOOP-11058) | Missing HADOOP\_CONF\_DIR generates strange results |  Major | scripts | Allen Wittenauer | Masatake Iwasaki |
| [HDFS-7579](https://issues.apache.org/jira/browse/HDFS-7579) | Improve log reporting during block report rpc failure |  Minor | datanode | Charles Lamb | Charles Lamb |
| [HDFS-7446](https://issues.apache.org/jira/browse/HDFS-7446) | HDFS inotify should have the ability to determine what txid it has read up to |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7182](https://issues.apache.org/jira/browse/HDFS-7182) | JMX metrics aren't accessible when NN is busy |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-11466](https://issues.apache.org/jira/browse/HADOOP-11466) | FastByteComparisons: do not use UNSAFE\_COMPARER on the SPARC architecture because it is slower there |  Minor | io, performance, util | Suman Somasundar | Suman Somasundar |
| [HADOOP-11460](https://issues.apache.org/jira/browse/HADOOP-11460) | Deprecate shell vars |  Major | scripts | Allen Wittenauer | John Smith |
| [HADOOP-11346](https://issues.apache.org/jira/browse/HADOOP-11346) | Rewrite sls/rumen to use new shell framework |  Major | scripts, tools | Allen Wittenauer | John Smith |
| [HADOOP-7713](https://issues.apache.org/jira/browse/HADOOP-7713) | dfs -count -q should label output column |  Trivial | . | Nigel Daley | Jonathan Allen |
| [HADOOP-6964](https://issues.apache.org/jira/browse/HADOOP-6964) | Allow compact property description in xml |  Major | conf | Owen O'Malley | Kengo Seki |
| [HADOOP-11506](https://issues.apache.org/jira/browse/HADOOP-11506) | Configuration variable expansion regex expensive for long values |  Major | conf | Dmitriy V. Ryaboy | Gera Shegalov |
| [HDFS-7546](https://issues.apache.org/jira/browse/HDFS-7546) | Document, and set an accepting default for dfs.namenode.kerberos.principal.pattern |  Minor | security | Harsh J | Harsh J |
| [HDFS-7694](https://issues.apache.org/jira/browse/HDFS-7694) | FSDataInputStream should support "unbuffer" |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7322](https://issues.apache.org/jira/browse/HDFS-7322) | deprecate sbin/hadoop-daemon.sh |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-2796](https://issues.apache.org/jira/browse/YARN-2796) | deprecate sbin/yarn-daemon.sh |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6250](https://issues.apache.org/jira/browse/MAPREDUCE-6250) | deprecate sbin/mr-jobhistory-daemon.sh |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6260](https://issues.apache.org/jira/browse/MAPREDUCE-6260) | Convert site documentation to markdown |  Major | documentation | Allen Wittenauer | Masatake Iwasaki |
| [HADOOP-11575](https://issues.apache.org/jira/browse/HADOOP-11575) | Daemon log documentation is misleading |  Major | documentation | Jagadesh Kiran N | Naganarasimha G R |
| [HADOOP-11596](https://issues.apache.org/jira/browse/HADOOP-11596) | Allow smart-apply-patch.sh to add new files in binary git patches |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-11593](https://issues.apache.org/jira/browse/HADOOP-11593) | Convert site documentation from apt to markdown (stragglers) |  Major | documentation | Allen Wittenauer | Masatake Iwasaki |
| [YARN-3230](https://issues.apache.org/jira/browse/YARN-3230) | Clarify application states on the web UI |  Major | . | Jian He | Jian He |
| [HDFS-7460](https://issues.apache.org/jira/browse/HDFS-7460) | Rewrite httpfs to use new shell framework |  Major | scripts | Allen Wittenauer | John Smith |
| [YARN-3168](https://issues.apache.org/jira/browse/YARN-3168) | Convert site documentation from apt to markdown |  Major | documentation | Allen Wittenauer | Gururaj Shetty |
| [YARN-3199](https://issues.apache.org/jira/browse/YARN-3199) | Fair Scheduler documentation improvements |  Minor | fairscheduler | Rohit Agarwal | Rohit Agarwal |
| [MAPREDUCE-6267](https://issues.apache.org/jira/browse/MAPREDUCE-6267) | Refactor JobSubmitter#copyAndConfigureFiles into it's own class |  Minor | . | Chris Trezzo | Chris Trezzo |
| [YARN-3249](https://issues.apache.org/jira/browse/YARN-3249) | Add a "kill application" button to Resource Manager's Web UI |  Minor | resourcemanager | Ryu Kobayashi | Ryu Kobayashi |
| [HADOOP-11673](https://issues.apache.org/jira/browse/HADOOP-11673) | Use org.junit.Assume to skip tests instead of return |  Minor | test | Akira Ajisaka | Brahma Reddy Battula |
| [HADOOP-11692](https://issues.apache.org/jira/browse/HADOOP-11692) | Improve authentication failure WARN message to avoid user confusion |  Major | ipc | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-11226](https://issues.apache.org/jira/browse/HADOOP-11226) | Add a configuration to set ipc.Client's traffic class with IPTOS\_LOWDELAY\|IPTOS\_RELIABILITY |  Major | ipc | Gopal V | Gopal V |
| [YARN-2280](https://issues.apache.org/jira/browse/YARN-2280) | Resource manager web service fields are not accessible |  Trivial | resourcemanager | Krisztian Horvath | Krisztian Horvath |
| [HADOOP-11711](https://issues.apache.org/jira/browse/HADOOP-11711) | Provide a default value for AES/CTR/NoPadding CryptoCodec classes |  Minor | . | Andrew Wang | Andrew Wang |
| [MAPREDUCE-4414](https://issues.apache.org/jira/browse/MAPREDUCE-4414) | Add main methods to JobConf and YarnConfiguration, for debug purposes |  Major | client | Harsh J | Plamen Jeliazkov |
| [MAPREDUCE-6105](https://issues.apache.org/jira/browse/MAPREDUCE-6105) | Inconsistent configuration in property mapreduce.reduce.shuffle.merge.percent |  Trivial | . | Dongwook Kwon | Ray Chiang |
| [HDFS-2360](https://issues.apache.org/jira/browse/HDFS-2360) | Ugly stacktrace when quota exceeds |  Minor | hdfs-client | Rajit Saha | Harsh J |
| [MAPREDUCE-6100](https://issues.apache.org/jira/browse/MAPREDUCE-6100) | replace "mapreduce.job.credentials.binary" with MRJobConfig.MAPREDUCE\_JOB\_CREDENTIALS\_BINARY for better readability. |  Trivial | mrv2 | zhihai xu | zhihai xu |
| [MAPREDUCE-5755](https://issues.apache.org/jira/browse/MAPREDUCE-5755) | MapTask.MapOutputBuffer#compare/swap should have @Override annotation |  Trivial | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [MAPREDUCE-4653](https://issues.apache.org/jira/browse/MAPREDUCE-4653) | TestRandomAlgorithm has an unused "import" statement |  Trivial | contrib/gridmix | Amir Sanjar | Amir Sanjar |
| [HADOOP-11659](https://issues.apache.org/jira/browse/HADOOP-11659) | o.a.h.fs.FileSystem.Cache#remove should use a single hash map lookup |  Minor | fs | Gera Shegalov | Brahma Reddy Battula |
| [HADOOP-11709](https://issues.apache.org/jira/browse/HADOOP-11709) | Time.NANOSECONDS\_PER\_MILLISECOND - use class-level final constant instead of method variable |  Trivial | . | Ajith S | Ajith S |
| [HDFS-7835](https://issues.apache.org/jira/browse/HDFS-7835) | make initial sleeptime in locateFollowingBlock configurable for DFSClient. |  Major | hdfs-client | zhihai xu | zhihai xu |
| [HDFS-7829](https://issues.apache.org/jira/browse/HDFS-7829) | Code clean up for LocatedBlock |  Minor | . | Jing Zhao | Takanobu Asanuma |
| [MAPREDUCE-6282](https://issues.apache.org/jira/browse/MAPREDUCE-6282) | Reuse historyFileAbsolute.getFileSystem in CompletedJob#loadFullHistoryData for code optimization. |  Trivial | jobhistoryserver | zhihai xu | zhihai xu |
| [HADOOP-11447](https://issues.apache.org/jira/browse/HADOOP-11447) | Add a more meaningful toString method to SampleStat and MutableStat |  Minor | metrics | Karthik Kambatla | Karthik Kambatla |
| [YARN-3350](https://issues.apache.org/jira/browse/YARN-3350) | YARN RackResolver spams logs with messages at info level |  Major | . | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [MAPREDUCE-6239](https://issues.apache.org/jira/browse/MAPREDUCE-6239) | Consolidate TestJobConf classes in hadoop-mapreduce-client-jobclient and hadoop-mapreduce-client-core |  Minor | client | Varun Saxena | Varun Saxena |
| [MAPREDUCE-5190](https://issues.apache.org/jira/browse/MAPREDUCE-5190) | Unnecessary condition test in RandomSampler |  Minor | mrv2 | Jingguo Yao | Jingguo Yao |
| [MAPREDUCE-6287](https://issues.apache.org/jira/browse/MAPREDUCE-6287) | Deprecated methods in org.apache.hadoop.examples.Sort |  Minor | examples | Chao Zhang | Chao Zhang |
| [HADOOP-11737](https://issues.apache.org/jira/browse/HADOOP-11737) | mockito's version in hadoop-nfs’ pom.xml shouldn't be specified |  Minor | nfs | Kengo Seki | Kengo Seki |
| [YARN-2868](https://issues.apache.org/jira/browse/YARN-2868) | FairScheduler: Metric for latency to allocate first container for an application |  Major | . | Ray Chiang | Ray Chiang |
| [HDFS-7875](https://issues.apache.org/jira/browse/HDFS-7875) | Improve log message when wrong value configured for dfs.datanode.failed.volumes.tolerated |  Trivial | datanode | nijel | nijel |
| [HDFS-7793](https://issues.apache.org/jira/browse/HDFS-7793) | Refactor DFSOutputStream separating DataStreamer out |  Major | hdfs-client | Kai Zheng | Li Bo |
| [HADOOP-11741](https://issues.apache.org/jira/browse/HADOOP-11741) | Add LOG.isDebugEnabled() guard for some LOG.debug() |  Major | . | Walter Su | Walter Su |
| [MAPREDUCE-579](https://issues.apache.org/jira/browse/MAPREDUCE-579) | Streaming "slowmatch" documentation |  Trivial | contrib/streaming | Bo Adler | Harsh J |
| [HDFS-7928](https://issues.apache.org/jira/browse/HDFS-7928) | Scanning blocks from disk during rolling upgrade startup takes a lot of time if disks are busy |  Major | datanode | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-11524](https://issues.apache.org/jira/browse/HADOOP-11524) | hadoop\_do\_classpath\_subcommand throws a shellcheck warning |  Minor | scripts | Allen Wittenauer | Chris Nauroth |
| [HADOOP-11719](https://issues.apache.org/jira/browse/HADOOP-11719) | [Fsshell] Remove bin/hadoop reference from GenericOptionsParser default help text |  Minor | scripts | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8004](https://issues.apache.org/jira/browse/HDFS-8004) | Use KeyProviderCryptoExtension#warmUpEncryptedKeys when creating an encryption zone |  Trivial | encryption | Andrew Wang | Andrew Wang |
| [HDFS-7890](https://issues.apache.org/jira/browse/HDFS-7890) | Improve information on Top users for metrics in RollingWindowsManager and lower log level |  Major | . | J.Andreina | J.Andreina |
| [HDFS-4396](https://issues.apache.org/jira/browse/HDFS-4396) | Add START\_MSG/SHUTDOWN\_MSG for ZKFC |  Major | auto-failover, ha, tools | Liang Xie | Liang Xie |
| [HADOOP-11660](https://issues.apache.org/jira/browse/HADOOP-11660) | Add support for hardware crc of HDFS checksums on ARM aarch64 architecture |  Minor | native | Edward Nevill | Edward Nevill |
| [HDFS-7645](https://issues.apache.org/jira/browse/HDFS-7645) | Rolling upgrade is restoring blocks from trash multiple times |  Major | datanode | Nathan Roberts | Keisuke Ogiwara |
| [HDFS-3918](https://issues.apache.org/jira/browse/HDFS-3918) | EditLogTailer shouldn't log WARN when other node is in standby mode |  Major | ha | Todd Lipcon | Todd Lipcon |
| [HDFS-7944](https://issues.apache.org/jira/browse/HDFS-7944) | Minor cleanup of BlockPoolManager#getAllNamenodeThreads |  Minor | . | Arpit Agarwal | Arpit Agarwal |
| [YARN-3258](https://issues.apache.org/jira/browse/YARN-3258) | FairScheduler: Need to add more logging to investigate allocations |  Minor | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-7671](https://issues.apache.org/jira/browse/HDFS-7671) | hdfs user guide should point to the common rack awareness doc |  Major | documentation | Allen Wittenauer | Kai Sasaki |
| [YARN-3412](https://issues.apache.org/jira/browse/YARN-3412) | RM tests should use MockRM where possible |  Major | resourcemanager, test | Karthik Kambatla | Karthik Kambatla |
| [YARN-3428](https://issues.apache.org/jira/browse/YARN-3428) | Debug log resources to be localized for a container |  Trivial | nodemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-3424](https://issues.apache.org/jira/browse/YARN-3424) | Change logs for ContainerMonitorImpl's resourse monitoring from info to debug |  Major | nodemanager | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-3248](https://issues.apache.org/jira/browse/YARN-3248) | Display count of nodes blacklisted by apps in the web UI |  Major | capacityscheduler, resourcemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-7978](https://issues.apache.org/jira/browse/HDFS-7978) | Add LOG.isDebugEnabled() guard for some LOG.debug(..) |  Major | . | Walter Su | Walter Su |
| [HDFS-8008](https://issues.apache.org/jira/browse/HDFS-8008) | Support client-side back off when the datanodes are congested |  Major | hdfs-client | Haohui Mai | Haohui Mai |
| [HDFS-7888](https://issues.apache.org/jira/browse/HDFS-7888) | Change DataStreamer/DFSOutputStream/DFSPacket for convenience of subclassing |  Minor | hdfs-client | Li Bo | Li Bo |
| [HDFS-8035](https://issues.apache.org/jira/browse/HDFS-8035) | Move checkBlocksProperlyReplicated() in FSNamesystem to BlockManager |  Minor | namenode | Haohui Mai | Haohui Mai |
| [HADOOP-9805](https://issues.apache.org/jira/browse/HADOOP-9805) | Refactor RawLocalFileSystem#rename for improved testability. |  Minor | fs, test | Chris Nauroth | Jean-Pierre Matsumoto |
| [HADOOP-11785](https://issues.apache.org/jira/browse/HADOOP-11785) | Reduce number of listStatus operation in distcp buildListing() |  Minor | tools/distcp | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [HADOOP-11717](https://issues.apache.org/jira/browse/HADOOP-11717) | Add Redirecting WebSSO behavior with JWT Token in Hadoop Auth |  Major | security | Larry McCay | Larry McCay |
| [YARN-3294](https://issues.apache.org/jira/browse/YARN-3294) | Allow dumping of Capacity Scheduler debug logs via web UI for a fixed time period |  Major | capacityscheduler | Varun Vasudev | Varun Vasudev |
| [HDFS-8073](https://issues.apache.org/jira/browse/HDFS-8073) | Split BlockPlacementPolicyDefault.chooseTarget(..) so it can be easily overrided. |  Trivial | namenode | Walter Su | Walter Su |
| [HDFS-8076](https://issues.apache.org/jira/browse/HDFS-8076) | Code cleanup for DFSInputStream: use offset instead of LocatedBlock when possible |  Major | . | Zhe Zhang | Zhe Zhang |
| [HDFS-7979](https://issues.apache.org/jira/browse/HDFS-7979) | Initialize block report IDs with a random number |  Minor | datanode | Andrew Wang | Andrew Wang |
| [HDFS-8101](https://issues.apache.org/jira/browse/HDFS-8101) | DFSClient use of non-constant DFSConfigKeys pulls in WebHDFS classes at runtime |  Minor | hdfs-client | Sean Busbey | Sean Busbey |
| [YARN-3293](https://issues.apache.org/jira/browse/YARN-3293) | Track and display capacity scheduler health metrics in web UI |  Major | capacityscheduler | Varun Vasudev | Varun Vasudev |
| [MAPREDUCE-6291](https://issues.apache.org/jira/browse/MAPREDUCE-6291) | Correct mapred queue usage command |  Minor | client | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6307](https://issues.apache.org/jira/browse/MAPREDUCE-6307) | Remove property mapreduce.tasktracker.taskmemorymanager.monitoringinterval |  Minor | . | Akira Ajisaka | J.Andreina |
| [YARN-3348](https://issues.apache.org/jira/browse/YARN-3348) | Add a 'yarn top' tool to help understand cluster usage |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-3469](https://issues.apache.org/jira/browse/YARN-3469) | ZKRMStateStore: Avoid setting watches that are not required |  Minor | . | Jun Gong | Jun Gong |
| [HDFS-8117](https://issues.apache.org/jira/browse/HDFS-8117) | More accurate verification in SimulatedFSDataset: replace DEFAULT\_DATABYTE with patterned data |  Major | . | Zhe Zhang | Zhe Zhang |
| [HDFS-8144](https://issues.apache.org/jira/browse/HDFS-8144) | Split TestLazyPersistFiles into multiple tests |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [YARN-3404](https://issues.apache.org/jira/browse/YARN-3404) | View the queue name to YARN Application page |  Minor | . | Ryu Kobayashi | Ryu Kobayashi |
| [YARN-3451](https://issues.apache.org/jira/browse/YARN-3451) | Add start time and Elapsed in ApplicationAttemptReport and display the same in RMAttemptBlock WebUI |  Major | api, webapp | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-7863](https://issues.apache.org/jira/browse/HDFS-7863) | Missing description of some methods and parameters in javadoc of FSDirDeleteOp |  Minor | . | Yongjun Zhang | Brahma Reddy Battula |
| [HDFS-8152](https://issues.apache.org/jira/browse/HDFS-8152) | Refactoring of lazy persist storage cases |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8133](https://issues.apache.org/jira/browse/HDFS-8133) | Improve readability of deleted block check |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-11812](https://issues.apache.org/jira/browse/HADOOP-11812) | Implement listLocatedStatus for ViewFileSystem to speed up split calculation |  Blocker | fs | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-6297](https://issues.apache.org/jira/browse/MAPREDUCE-6297) | Task Id of the failed task in diagnostics should link to the task page |  Minor | jobhistoryserver | Siqi Li | Siqi Li |
| [HADOOP-11827](https://issues.apache.org/jira/browse/HADOOP-11827) | Speed-up distcp buildListing() using threadpool |  Major | tools/distcp | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [YARN-3494](https://issues.apache.org/jira/browse/YARN-3494) | Expose AM resource limit and usage in QueueMetrics |  Major | . | Jian He | Rohith Sharma K S |
| [YARN-3503](https://issues.apache.org/jira/browse/YARN-3503) | Expose disk utilization percentage and bad local and log dir counts on NM via JMX |  Major | nodemanager | Varun Vasudev | Varun Vasudev |
| [YARN-3410](https://issues.apache.org/jira/browse/YARN-3410) | YARN admin should be able to remove individual application records from RMStateStore |  Critical | resourcemanager, yarn | Wangda Tan | Rohith Sharma K S |
| [HDFS-8215](https://issues.apache.org/jira/browse/HDFS-8215) | Refactor NamenodeFsck#check method |  Minor | namenode | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-3511](https://issues.apache.org/jira/browse/YARN-3511) | Add errors and warnings page to ATS |  Major | timelineserver | Varun Vasudev | Varun Vasudev |
| [HDFS-8176](https://issues.apache.org/jira/browse/HDFS-8176) | Record from/to snapshots in audit log for snapshot diff report |  Minor | snapshots | J.Andreina | J.Andreina |
| [YARN-3406](https://issues.apache.org/jira/browse/YARN-3406) | Display count of running containers in the RM's Web UI |  Minor | . | Ryu Kobayashi | Ryu Kobayashi |
| [HADOOP-11357](https://issues.apache.org/jira/browse/HADOOP-11357) | Print information of the build enviornment in test-patch.sh |  Minor | scripts | Haohui Mai | Allen Wittenauer |
| [HADOOP-11857](https://issues.apache.org/jira/browse/HADOOP-11857) | Fix CommandFormat#commandFormat java doc annotation |  Trivial | . | Xiaoyu Yao | J.Andreina |
| [MAPREDUCE-6057](https://issues.apache.org/jira/browse/MAPREDUCE-6057) | Remove obsolete entries from mapred-default.xml |  Minor | . | Ray Chiang | Ray Chiang |
| [HDFS-8204](https://issues.apache.org/jira/browse/HDFS-8204) | Mover/Balancer should not schedule two replicas to the same DN |  Minor | balancer & mover | Walter Su | Walter Su |
| [HDFS-8280](https://issues.apache.org/jira/browse/HDFS-8280) | Code Cleanup in DFSInputStream |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-8283](https://issues.apache.org/jira/browse/HDFS-8283) | DataStreamer cleanup and some minor improvement |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-5574](https://issues.apache.org/jira/browse/HDFS-5574) | Remove buffer copy in BlockReader.skip |  Trivial | . | Binglin Chang | Binglin Chang |
| [HDFS-7770](https://issues.apache.org/jira/browse/HDFS-7770) | Need document for storage type label of data node storage locations under dfs.datanode.data.dir |  Major | documentation | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-8200](https://issues.apache.org/jira/browse/HDFS-8200) | Refactor FSDirStatAndListingOp |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-3363](https://issues.apache.org/jira/browse/YARN-3363) | add localization and container launch time to ContainerMetrics at NM to show these timing information for each active container. |  Major | nodemanager | zhihai xu | zhihai xu |
| [HDFS-7397](https://issues.apache.org/jira/browse/HDFS-7397) | Add more detail to the documentation for the conf key "dfs.client.read.shortcircuit.streams.cache.size" |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Brahma Reddy Battula |
| [YARN-2980](https://issues.apache.org/jira/browse/YARN-2980) | Move health check script related functionality to hadoop-common |  Blocker | . | Ming Ma | Varun Saxena |
| [HADOOP-11911](https://issues.apache.org/jira/browse/HADOOP-11911) | test-patch should allow configuration of default branch |  Minor | . | Sean Busbey | Sean Busbey |
| [HDFS-7758](https://issues.apache.org/jira/browse/HDFS-7758) | Retire FsDatasetSpi#getVolumes() and use FsDatasetSpi#getVolumeRefs() instead |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [MAPREDUCE-6192](https://issues.apache.org/jira/browse/MAPREDUCE-6192) | Create unit test to automatically compare MR related classes and mapred-default.xml |  Minor | . | Ray Chiang | Ray Chiang |
| [HADOOP-11813](https://issues.apache.org/jira/browse/HADOOP-11813) | releasedocmaker.py should use today's date instead of unreleased |  Minor | build | Allen Wittenauer | Darrell Taylor |
| [YARN-3491](https://issues.apache.org/jira/browse/YARN-3491) | PublicLocalizer#addResource is too slow. |  Critical | nodemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-6279](https://issues.apache.org/jira/browse/MAPREDUCE-6279) | AM should explicity exit JVM after all services have stopped |  Major | . | Jason Lowe | Eric Payne |
| [HDFS-8108](https://issues.apache.org/jira/browse/HDFS-8108) | Fsck should provide the info on mandatory option to be used along with "-blocks , -locations and -racks" |  Trivial | documentation | J.Andreina | J.Andreina |
| [HDFS-8207](https://issues.apache.org/jira/browse/HDFS-8207) | Improper log message when blockreport interval compared with initial delay |  Minor | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6079](https://issues.apache.org/jira/browse/MAPREDUCE-6079) | Rename JobImpl#username to reporterUserName |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-8209](https://issues.apache.org/jira/browse/HDFS-8209) | Support different number of datanode directories in MiniDFSCluster. |  Minor | test | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-3169](https://issues.apache.org/jira/browse/YARN-3169) | Drop YARN's overview document |  Major | documentation | Allen Wittenauer | Brahma Reddy Battula |
| [YARN-2784](https://issues.apache.org/jira/browse/YARN-2784) | Make POM project names consistent |  Minor | build | Rohith Sharma K S | Rohith Sharma K S |
| [MAPREDUCE-5981](https://issues.apache.org/jira/browse/MAPREDUCE-5981) | Log levels of certain MR logs can be changed to DEBUG |  Major | . | Varun Saxena | Varun Saxena |
| [HADOOP-6842](https://issues.apache.org/jira/browse/HADOOP-6842) | "hadoop fs -text" does not give a useful text representation of MapWritable objects |  Major | io | Steven K. Wong | Akira Ajisaka |
| [YARN-20](https://issues.apache.org/jira/browse/YARN-20) | More information for "yarn.resourcemanager.webapp.address" in yarn-default.xml |  Trivial | documentation, resourcemanager | Nemon Lou | Bartosz Ługowski |
| [HDFS-5640](https://issues.apache.org/jira/browse/HDFS-5640) | Add snapshot methods to FileContext. |  Major | hdfs-client, snapshots | Chris Nauroth | Rakesh R |
| [HDFS-8284](https://issues.apache.org/jira/browse/HDFS-8284) | Update documentation about how to use HTrace with HDFS |  Major | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7433](https://issues.apache.org/jira/browse/HDFS-7433) | Optimize performance of DatanodeManager's node map |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-11590](https://issues.apache.org/jira/browse/HADOOP-11590) | Update sbin commands and documentation to use new --slaves option |  Blocker | documentation, scripts | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-5248](https://issues.apache.org/jira/browse/MAPREDUCE-5248) | Let NNBenchWithoutMR specify the replication factor for its test |  Minor | client, test | Erik Paulson | Erik Paulson |
| [HADOOP-9737](https://issues.apache.org/jira/browse/HADOOP-9737) | JarFinder#getJar should delete the jar file upon destruction of the JVM |  Major | util | Esteban Gutierrez | Jean-Baptiste Onofré |
| [YARN-1050](https://issues.apache.org/jira/browse/YARN-1050) | Document the Fair Scheduler REST API |  Major | documentation, fairscheduler | Sandy Ryza | Kenji Kikushima |
| [YARN-3271](https://issues.apache.org/jira/browse/YARN-3271) | FairScheduler: Move tests related to max-runnable-apps from TestFairScheduler to TestAppRunnability |  Major | . | Karthik Kambatla | nijel |
| [YARN-2206](https://issues.apache.org/jira/browse/YARN-2206) | Update document for applications REST API response examples |  Minor | documentation | Kenji Kikushima | Brahma Reddy Battula |
| [HDFS-6757](https://issues.apache.org/jira/browse/HDFS-6757) | Simplify lease manager with INodeID |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-8327](https://issues.apache.org/jira/browse/HDFS-8327) | Simplify quota calculations for snapshots and truncate |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1287](https://issues.apache.org/jira/browse/YARN-1287) | Consolidate MockClocks |  Major | . | Sandy Ryza | Sebastian Wong |
| [HDFS-8357](https://issues.apache.org/jira/browse/HDFS-8357) | Consolidate parameters of INode.CleanSubtree() into a parameter objects. |  Major | . | Haohui Mai | Li Lu |
| [HADOOP-11950](https://issues.apache.org/jira/browse/HADOOP-11950) | Add cli option to test-patch to set the project-under-test |  Minor | . | Sean Busbey | Sean Busbey |
| [HADOOP-11948](https://issues.apache.org/jira/browse/HADOOP-11948) | test-patch's issue matching regex should be configurable. |  Major | . | Sean Busbey | Sean Busbey |
| [MAPREDUCE-5465](https://issues.apache.org/jira/browse/MAPREDUCE-5465) | Tasks are often killed before they exit on their own |  Major | mr-am, mrv2 | Radim Kolar | Ming Ma |
| [YARN-3513](https://issues.apache.org/jira/browse/YARN-3513) | Remove unused variables in ContainersMonitorImpl and add debug log for overall resource usage by all containers |  Trivial | nodemanager | Naganarasimha G R | Naganarasimha G R |
| [HDFS-8255](https://issues.apache.org/jira/browse/HDFS-8255) | Rename getBlockReplication to getPreferredBlockReplication |  Major | . | Zhe Zhang | Zhe Zhang |
| [YARN-3613](https://issues.apache.org/jira/browse/YARN-3613) | TestContainerManagerSecurity should init and start Yarn cluster in setup instead of individual methods |  Minor | test | Karthik Kambatla | nijel |
| [HDFS-6184](https://issues.apache.org/jira/browse/HDFS-6184) | Capture NN's thread dump when it fails over |  Major | namenode | Ming Ma | Ming Ma |
| [YARN-3539](https://issues.apache.org/jira/browse/YARN-3539) | Compatibility doc to state that ATS v1 is a stable REST API |  Major | documentation | Steve Loughran | Steve Loughran |
| [HADOOP-9723](https://issues.apache.org/jira/browse/HADOOP-9723) | Improve error message when hadoop archive output path already exists |  Trivial | . | Stephen Chu | Yongjun Zhang |
| [HADOOP-10993](https://issues.apache.org/jira/browse/HADOOP-10993) | Dump java command line to \*.out file |  Minor | . | Allen Wittenauer | Kengo Seki |
| [HADOOP-11713](https://issues.apache.org/jira/browse/HADOOP-11713) | ViewFileSystem should support snapshot methods. |  Major | fs | Chris Nauroth | Rakesh R |
| [HDFS-8350](https://issues.apache.org/jira/browse/HDFS-8350) | Remove old webhdfs.xml and other outdated documentation stuff |  Major | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [HADOOP-11960](https://issues.apache.org/jira/browse/HADOOP-11960) | Enable Azure-Storage Client Side logging. |  Major | tools | Dushyanth | Dushyanth |
| [HDFS-6888](https://issues.apache.org/jira/browse/HDFS-6888) | Allow selectively audit logging ops |  Major | . | Kihwal Lee | Chen He |
| [HDFS-8397](https://issues.apache.org/jira/browse/HDFS-8397) | Refactor the error handling code in DataStreamer |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-8394](https://issues.apache.org/jira/browse/HDFS-8394) | Move getAdditionalBlock() and related functionalities into a separate class |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-11939](https://issues.apache.org/jira/browse/HADOOP-11939) | Deprecate DistCpV1 and Logalyzer |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-4185](https://issues.apache.org/jira/browse/HDFS-4185) | Add a metric for number of active leases |  Major | namenode | Kihwal Lee | Rakesh R |
| [HADOOP-1540](https://issues.apache.org/jira/browse/HADOOP-1540) | Support file exclusion list in distcp |  Minor | util | Senthil Subramanian | Rich Haase |
| [HADOOP-11103](https://issues.apache.org/jira/browse/HADOOP-11103) | Clean up RemoteException |  Trivial | ipc | Sean Busbey | Sean Busbey |
| [HDFS-8131](https://issues.apache.org/jira/browse/HDFS-8131) | Implement a space balanced block placement policy |  Minor | namenode | Liu Shaohui | Liu Shaohui |
| [HADOOP-11970](https://issues.apache.org/jira/browse/HADOOP-11970) | Replace uses of ThreadLocal\<Random\> with JDK7 ThreadLocalRandom |  Major | . | Sean Busbey | Sean Busbey |
| [HADOOP-11995](https://issues.apache.org/jira/browse/HADOOP-11995) | Make jetty version configurable from the maven command line |  Trivial | build, ld | sriharsha devineni | sriharsha devineni |
| [HDFS-4383](https://issues.apache.org/jira/browse/HDFS-4383) | Document the lease limits |  Minor | . | Eli Collins | Arshad Mohammad |
| [HADOOP-10366](https://issues.apache.org/jira/browse/HADOOP-10366) | Add whitespaces between the classes for values in core-default.xml to fit better in browser |  Minor | documentation | Chengwei Yang | Kanaka Kumar Avvaru |
| [HADOOP-11594](https://issues.apache.org/jira/browse/HADOOP-11594) | Improve the readability of site index of documentation |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-160](https://issues.apache.org/jira/browse/YARN-160) | nodemanagers should obtain cpu/memory values from underlying OS |  Major | nodemanager | Alejandro Abdelnur | Varun Vasudev |
| [HADOOP-11242](https://issues.apache.org/jira/browse/HADOOP-11242) | Record the time of calling in tracing span of IPC server |  Minor | ipc | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-5033](https://issues.apache.org/jira/browse/HDFS-5033) | Bad error message for fs -put/copyFromLocal if user doesn't have permissions to read the source |  Minor | . | Karthik Kambatla | Darrell Taylor |
| [YARN-3722](https://issues.apache.org/jira/browse/YARN-3722) | Merge multiple TestWebAppUtils into o.a.h.yarn.webapp.util.TestWebAppUtils |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11142](https://issues.apache.org/jira/browse/HADOOP-11142) | Remove hdfs dfs reference from file system shell documentation |  Minor | documentation | Jonathan Allen | Kengo Seki |
| [HADOOP-11894](https://issues.apache.org/jira/browse/HADOOP-11894) | Bump the version of Apache HTrace to 3.2.0-incubating |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3489](https://issues.apache.org/jira/browse/YARN-3489) | RMServerUtils.validateResourceRequests should only obtain queue info once |  Major | resourcemanager | Jason Lowe | Varun Saxena |
| [HDFS-8443](https://issues.apache.org/jira/browse/HDFS-8443) | Document dfs.namenode.service.handler.count in hdfs-site.xml |  Major | documentation | Akira Ajisaka | J.Andreina |
| [YARN-3547](https://issues.apache.org/jira/browse/YARN-3547) | FairScheduler: Apps that have no resource demand should not participate scheduling |  Major | fairscheduler | Xianyin Xin | Xianyin Xin |
| [YARN-3713](https://issues.apache.org/jira/browse/YARN-3713) | Remove duplicate function call storeContainerDiagnostics in ContainerDiagnosticsUpdateTransition |  Minor | nodemanager | zhihai xu | zhihai xu |
| [HADOOP-12043](https://issues.apache.org/jira/browse/HADOOP-12043) | Display warning if defaultFs is not set when running fs commands. |  Minor | fs | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-3467](https://issues.apache.org/jira/browse/YARN-3467) | Expose allocatedMB, allocatedVCores, and runningContainers metrics on running Applications in RM Web UI |  Minor | webapp, yarn | Anthony Rojas | Anubhav Dhoot |
| [HDFS-8490](https://issues.apache.org/jira/browse/HDFS-8490) | Typo in trace enabled log in ExceptionHandler of WebHDFS |  Trivial | webhdfs | Jakob Homan | Archana T |
| [HDFS-8521](https://issues.apache.org/jira/browse/HDFS-8521) | Add @VisibleForTesting annotation to {{BlockPoolSlice#selectReplicaToDelete}} |  Trivial | . | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-6174](https://issues.apache.org/jira/browse/MAPREDUCE-6174) | Combine common stream code into parent class for InMemoryMapOutput and OnDiskMapOutput. |  Major | mrv2 | Eric Payne | Eric Payne |
| [HDFS-8532](https://issues.apache.org/jira/browse/HDFS-8532) | Make the visibility of DFSOutputStream#streamer member variable to private |  Trivial | . | Rakesh R | Rakesh R |
| [HDFS-8535](https://issues.apache.org/jira/browse/HDFS-8535) | Clarify that dfs usage in dfsadmin -report output includes all block replicas. |  Minor | documentation | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [MAPREDUCE-6383](https://issues.apache.org/jira/browse/MAPREDUCE-6383) | Pi job (QuasiMonteCarlo) should not try to read the results file if its job fails |  Major | examples | Harsh J | Harsh J |
| [YARN-3259](https://issues.apache.org/jira/browse/YARN-3259) | FairScheduler: Trigger fairShare updates on node events |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-12059](https://issues.apache.org/jira/browse/HADOOP-12059) | S3Credentials should support use of CredentialProvider |  Major | fs/s3 | Sean Busbey | Sean Busbey |
| [MAPREDUCE-6354](https://issues.apache.org/jira/browse/MAPREDUCE-6354) | ShuffleHandler should be able to log shuffle connections |  Major | . | Chang Li | Chang Li |
| [HADOOP-12055](https://issues.apache.org/jira/browse/HADOOP-12055) | Deprecate usage of NativeIO#link |  Major | native | Andrew Wang | Andrew Wang |
| [HDFS-8432](https://issues.apache.org/jira/browse/HDFS-8432) | Introduce a minimum compatible layout version to allow downgrade in more rolling upgrade use cases. |  Major | namenode, rolling upgrades | Chris Nauroth | Chris Nauroth |
| [HDFS-8116](https://issues.apache.org/jira/browse/HDFS-8116) | Cleanup uncessary if LOG.isDebugEnabled() from RollingWindowManager |  Trivial | namenode | Xiaoyu Yao | Brahma Reddy Battula |
| [YARN-2716](https://issues.apache.org/jira/browse/YARN-2716) | Refactor ZKRMStateStore retry code with Apache Curator |  Major | . | Jian He | Karthik Kambatla |
| [HDFS-8553](https://issues.apache.org/jira/browse/HDFS-8553) | Document hdfs class path options |  Major | documentation | Xiaoyu Yao | Brahma Reddy Battula |
| [YARN-3786](https://issues.apache.org/jira/browse/YARN-3786) | Document yarn class path options |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6392](https://issues.apache.org/jira/browse/MAPREDUCE-6392) | Document mapred class path options |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8549](https://issues.apache.org/jira/browse/HDFS-8549) | Abort the balancer if an upgrade is in progress |  Major | balancer & mover | Andrew Wang | Andrew Wang |
| [HDFS-8573](https://issues.apache.org/jira/browse/HDFS-8573) | Move creation of restartMeta file logic from BlockReceiver to ReplicaInPipeline |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11971](https://issues.apache.org/jira/browse/HADOOP-11971) | Move test utilities for tracing from hadoop-hdfs to hadoop-common |  Minor | tracing | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8361](https://issues.apache.org/jira/browse/HDFS-8361) | Choose SSD over DISK in block placement |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-3789](https://issues.apache.org/jira/browse/YARN-3789) | Improve logs for LeafQueue#activateApplications() |  Minor | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-8606](https://issues.apache.org/jira/browse/HDFS-8606) | Cleanup DFSOutputStream by removing unwanted changes |  Minor | hdfs-client | Rakesh R | Rakesh R |
| [YARN-3148](https://issues.apache.org/jira/browse/YARN-3148) | Allow CORS related headers to passthrough in WebAppProxyServlet |  Major | . | Prakash Ramachandran | Varun Saxena |
| [HDFS-8589](https://issues.apache.org/jira/browse/HDFS-8589) | Fix unused imports in BPServiceActor and BlockReportLeaseManager |  Trivial | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-7139](https://issues.apache.org/jira/browse/HADOOP-7139) | Allow appending to existing SequenceFiles |  Major | io | Stephen Rose | Kanaka Kumar Avvaru |
| [HDFS-8605](https://issues.apache.org/jira/browse/HDFS-8605) | Merge Refactor of DFSOutputStream from HDFS-7285 branch |  Major | . | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-6395](https://issues.apache.org/jira/browse/MAPREDUCE-6395) | Improve the commit failure messages in MRAppMaster recovery |  Major | applicationmaster | Gera Shegalov | Brahma Reddy Battula |
| [HDFS-8582](https://issues.apache.org/jira/browse/HDFS-8582) | Support getting a list of reconfigurable config properties and do not generate spurious reconfig warnings |  Minor | datanode, hdfs-client | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [MAPREDUCE-6316](https://issues.apache.org/jira/browse/MAPREDUCE-6316) | Task Attempt List entries should link to the task overview |  Major | . | Siqi Li | Siqi Li |
| [MAPREDUCE-6305](https://issues.apache.org/jira/browse/MAPREDUCE-6305) | AM/Task log page should be able to link back to the job |  Major | . | Siqi Li | Siqi Li |
| [YARN-3834](https://issues.apache.org/jira/browse/YARN-3834) | Scrub debug logging of tokens during resource localization. |  Major | nodemanager | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6408](https://issues.apache.org/jira/browse/MAPREDUCE-6408) | Queue name and user name should be printed on the job page |  Major | applicationmaster | Siqi Li | Siqi Li |
| [HDFS-8639](https://issues.apache.org/jira/browse/HDFS-8639) | Option for HTTP port of NameNode by MiniDFSClusterManager |  Minor | test | Kai Sasaki | Kai Sasaki |
| [YARN-3360](https://issues.apache.org/jira/browse/YARN-3360) | Add JMX metrics to TimelineDataManager |  Major | timelineserver | Jason Lowe | Jason Lowe |
| [HADOOP-12049](https://issues.apache.org/jira/browse/HADOOP-12049) | Control http authentication cookie persistence via configuration |  Major | security | Benoy Antony | Huizhi Lu |
| [HDFS-8462](https://issues.apache.org/jira/browse/HDFS-8462) | Implement GETXATTRS and LISTXATTRS operations for WebImageViewer |  Major | . | Akira Ajisaka | Jagadesh Kiran N |
| [HDFS-8640](https://issues.apache.org/jira/browse/HDFS-8640) | Make reserved RBW space visible through JMX |  Major | . | Kanaka Kumar Avvaru | Kanaka Kumar Avvaru |
| [HDFS-8546](https://issues.apache.org/jira/browse/HDFS-8546) | Use try with resources in DataStorage and Storage |  Minor | datanode | Andrew Wang | Andrew Wang |
| [HADOOP-11203](https://issues.apache.org/jira/browse/HADOOP-11203) | Allow ditscp to accept bandwitdh in fraction MegaBytes |  Major | tools/distcp | Raju Bairishetti | Raju Bairishetti |
| [HADOOP-11807](https://issues.apache.org/jira/browse/HADOOP-11807) | add a lint mode to releasedocmaker |  Minor | build, documentation, yetus | Allen Wittenauer | ramtin |
| [HDFS-8653](https://issues.apache.org/jira/browse/HDFS-8653) | Code cleanup for DatanodeManager, DatanodeDescriptor and DatanodeStorageInfo |  Major | . | Zhe Zhang | Zhe Zhang |
| [HDFS-8659](https://issues.apache.org/jira/browse/HDFS-8659) | Block scanner INFO message is spamming logs |  Major | datanode | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-12149](https://issues.apache.org/jira/browse/HADOOP-12149) | copy all of test-patch BINDIR prior to re-exec |  Blocker | yetus | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6384](https://issues.apache.org/jira/browse/MAPREDUCE-6384) | Add the last reporting reducer info for too many fetch failure diagnostics |  Major | . | Chang Li | Chang Li |
| [HADOOP-12158](https://issues.apache.org/jira/browse/HADOOP-12158) | Improve error message in TestCryptoStreamsWithOpensslAesCtrCryptoCodec when OpenSSL is not installed |  Trivial | test | Andrew Wang | Andrew Wang |
| [HADOOP-12172](https://issues.apache.org/jira/browse/HADOOP-12172) | FsShell mkdir -p makes an unnecessary check for the existence of the parent. |  Minor | fs | Chris Nauroth | Chris Nauroth |
| [HDFS-8703](https://issues.apache.org/jira/browse/HDFS-8703) | Merge refactor of DFSInputStream from ErasureCoding branch |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-8709](https://issues.apache.org/jira/browse/HDFS-8709) | Clarify automatic sync in FSEditLog#logEdit |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-12045](https://issues.apache.org/jira/browse/HADOOP-12045) | Enable LocalFileSystem#setTimes to change atime |  Minor | fs | Kazuho Fujii | Kazuho Fujii |
| [HADOOP-12185](https://issues.apache.org/jira/browse/HADOOP-12185) | NetworkTopology is not efficient adding/getting/removing nodes |  Major | . | Inigo Goiri | Inigo Goiri |
| [HADOOP-12135](https://issues.apache.org/jira/browse/HADOOP-12135) | cleanup releasedocmaker |  Major | yetus | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12195](https://issues.apache.org/jira/browse/HADOOP-12195) | Add annotation to package-info.java file to workaround MCOMPILER-205 |  Trivial | . | Andrew Wang | Andrew Wang |
| [HADOOP-12193](https://issues.apache.org/jira/browse/HADOOP-12193) | Rename Touchz.java to Touch.java |  Trivial | . | Andrew Wang | Andrew Wang |
| [HDFS-8711](https://issues.apache.org/jira/browse/HDFS-8711) | setSpaceQuota command should print the available storage type when input storage type is wrong |  Major | hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-8712](https://issues.apache.org/jira/browse/HDFS-8712) | Remove "public" and "abstract" modifiers in FsVolumeSpi and FsDatasetSpi |  Trivial | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-12194](https://issues.apache.org/jira/browse/HADOOP-12194) | Support for incremental generation in the protoc plugin |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-12180](https://issues.apache.org/jira/browse/HADOOP-12180) | Move ResourceCalculatorPlugin from YARN to Common |  Major | util | Chris Douglas | Chris Douglas |
| [HADOOP-12210](https://issues.apache.org/jira/browse/HADOOP-12210) | Collect network usage on the node |  Major | . | Robert Grandl | Robert Grandl |
| [YARN-3069](https://issues.apache.org/jira/browse/YARN-3069) | Document missing properties in yarn-default.xml |  Major | documentation | Ray Chiang | Ray Chiang |
| [YARN-3381](https://issues.apache.org/jira/browse/YARN-3381) | Fix typo InvalidStateTransitonException |  Minor | api | Xiaoshuang LU | Brahma Reddy Battula |
| [HADOOP-12211](https://issues.apache.org/jira/browse/HADOOP-12211) | Collect disks usages on the node |  Major | . | Robert Grandl | Robert Grandl |
| [HDFS-8722](https://issues.apache.org/jira/browse/HDFS-8722) | Optimize datanode writes for small writes and flushes |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-12232](https://issues.apache.org/jira/browse/HADOOP-12232) | Upgrade Tomcat dependency to 6.0.44. |  Major | build | Chris Nauroth | Chris Nauroth |
| [YARN-3170](https://issues.apache.org/jira/browse/YARN-3170) | YARN architecture document needs updating |  Major | documentation | Allen Wittenauer | Brahma Reddy Battula |
| [YARN-3174](https://issues.apache.org/jira/browse/YARN-3174) | Consolidate the NodeManager and NodeManagerRestart documentation into one |  Major | documentation | Allen Wittenauer | Masatake Iwasaki |
| [HDFS-7314](https://issues.apache.org/jira/browse/HDFS-7314) | When the DFSClient lease cannot be renewed, abort open-for-write files rather than the entire DFSClient |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-10979](https://issues.apache.org/jira/browse/HADOOP-10979) | Auto-entries in hadoop\_usage |  Minor | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11893](https://issues.apache.org/jira/browse/HADOOP-11893) | Mark org.apache.hadoop.security.token.Token as @InterfaceAudience.Public |  Major | security | Steve Loughran | Brahma Reddy Battula |
| [HADOOP-12081](https://issues.apache.org/jira/browse/HADOOP-12081) | Fix UserGroupInformation.java to support 64-bit zLinux |  Major | security | Adam Roberts | Akira Ajisaka |
| [HADOOP-12214](https://issues.apache.org/jira/browse/HADOOP-12214) | Parse 'HadoopArchive' commandline using cli Options. |  Minor | . | Vinayakumar B | Vinayakumar B |
| [YARN-3261](https://issues.apache.org/jira/browse/YARN-3261) | rewrite resourcemanager restart doc to remove roadmap bits |  Major | documentation | Allen Wittenauer | Gururaj Shetty |
| [YARN-2921](https://issues.apache.org/jira/browse/YARN-2921) | Fix MockRM/MockAM#waitForState sleep too long |  Major | test | Karthik Kambatla | Tsuyoshi Ozawa |
| [HADOOP-12161](https://issues.apache.org/jira/browse/HADOOP-12161) | Add getStoragePolicy API to the FileSystem interface |  Major | . | Arpit Agarwal | Brahma Reddy Battula |
| [HADOOP-12189](https://issues.apache.org/jira/browse/HADOOP-12189) | Improve CallQueueManager#swapQueue to make queue elements drop nearly impossible. |  Major | ipc, test | zhihai xu | zhihai xu |
| [HADOOP-12009](https://issues.apache.org/jira/browse/HADOOP-12009) | Clarify FileSystem.listStatus() sorting order & fix FileSystemContractBaseTest:testListStatus |  Minor | documentation, fs, test | Jakob Homan | J.Andreina |
| [HADOOP-12259](https://issues.apache.org/jira/browse/HADOOP-12259) | Utility to Dynamic port allocation |  Major | test, util | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8735](https://issues.apache.org/jira/browse/HDFS-8735) | Inotify : All events classes should implement toString() API. |  Major | hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-7858](https://issues.apache.org/jira/browse/HDFS-7858) | Improve HA Namenode Failover detection on the client |  Major | hdfs-client | Arun Suresh | Arun Suresh |
| [HDFS-8180](https://issues.apache.org/jira/browse/HDFS-8180) | AbstractFileSystem Implementation for WebHdfs |  Major | webhdfs | Santhosh G Nayak | Santhosh G Nayak |
| [HDFS-8811](https://issues.apache.org/jira/browse/HDFS-8811) | Move BlockStoragePolicy name's constants from HdfsServerConstants.java to HdfsConstants.java |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-8822](https://issues.apache.org/jira/browse/HDFS-8822) | Add SSD storagepolicy tests in TestBlockStoragePolicy#testDefaultPolicies |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-3950](https://issues.apache.org/jira/browse/YARN-3950) | Add unique YARN\_SHELL\_ID environment variable to DistributedShell |  Major | applications/distributed-shell | Robert Kanter | Robert Kanter |
| [YARN-2768](https://issues.apache.org/jira/browse/YARN-2768) | Avoid cloning Resource in FSAppAttempt#updateDemand |  Minor | fairscheduler | Hong Zhiguo | Hong Zhiguo |
| [HDFS-8816](https://issues.apache.org/jira/browse/HDFS-8816) | Improve visualization for the Datanode tab in the NN UI |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-8821](https://issues.apache.org/jira/browse/HDFS-8821) | Explain message "Operation category X is not supported in state standby" |  Minor | . | Gautam Gopalakrishnan | Gautam Gopalakrishnan |
| [HADOOP-12271](https://issues.apache.org/jira/browse/HADOOP-12271) | Hadoop Jar Error Should Be More Explanatory |  Minor | . | Jesse Anderson | Josh Elser |
| [HADOOP-12249](https://issues.apache.org/jira/browse/HADOOP-12249) | pull argument parsing into a function |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12183](https://issues.apache.org/jira/browse/HADOOP-12183) | Annotate the HTrace span created by FsShell with the command-line arguments passed by the user |  Minor | tracing | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3978](https://issues.apache.org/jira/browse/YARN-3978) | Configurably turn off the saving of container info in Generic AHS |  Major | timelineserver, yarn | Eric Payne | Eric Payne |
| [YARN-3965](https://issues.apache.org/jira/browse/YARN-3965) | Add startup timestamp to nodemanager UI |  Minor | nodemanager | Hong Zhiguo | Hong Zhiguo |
| [HADOOP-12280](https://issues.apache.org/jira/browse/HADOOP-12280) | Skip unit tests based on maven profile rather than NativeCodeLoader.isNativeCodeLoaded |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8815](https://issues.apache.org/jira/browse/HDFS-8815) | DFS getStoragePolicy implementation using single RPC call |  Major | hdfs-client | Arpit Agarwal | Surendra Singh Lilhore |
| [YARN-3961](https://issues.apache.org/jira/browse/YARN-3961) | Expose pending, running and reserved containers of a queue in REST api and yarn top |  Major | capacityscheduler, fairscheduler, webapp | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-4019](https://issues.apache.org/jira/browse/YARN-4019) | Add JvmPauseMonitor to ResourceManager and NodeManager |  Major | nodemanager, resourcemanager | Robert Kanter | Robert Kanter |
| [MAPREDUCE-6443](https://issues.apache.org/jira/browse/MAPREDUCE-6443) | Add JvmPauseMonitor to Job History Server |  Major | jobhistoryserver | Robert Kanter | Robert Kanter |
| [HDFS-8887](https://issues.apache.org/jira/browse/HDFS-8887) | Expose storage type and storage ID in BlockLocation |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-12318](https://issues.apache.org/jira/browse/HADOOP-12318) | Expose underlying LDAP exceptions in SaslPlainServer |  Minor | security | Mike Yoder | Mike Yoder |
| [HADOOP-12295](https://issues.apache.org/jira/browse/HADOOP-12295) | Improve NetworkTopology#InnerNode#remove logic |  Major | . | Yi Liu | Yi Liu |
| [HDFS-7649](https://issues.apache.org/jira/browse/HDFS-7649) | Multihoming docs should emphasize using hostnames in configurations |  Major | documentation | Arpit Agarwal | Brahma Reddy Battula |
| [YARN-4055](https://issues.apache.org/jira/browse/YARN-4055) | Report node resource utilization in heartbeat |  Major | nodemanager | Inigo Goiri | Inigo Goiri |
| [HDFS-8713](https://issues.apache.org/jira/browse/HDFS-8713) | Convert DatanodeDescriptor to use SLF4J logging |  Trivial | . | Andrew Wang | Andrew Wang |
| [HDFS-8883](https://issues.apache.org/jira/browse/HDFS-8883) | NameNode Metrics : Add FSNameSystem lock Queue Length |  Major | namenode | Anu Engineer | Anu Engineer |
| [HDFS-6407](https://issues.apache.org/jira/browse/HDFS-6407) | Add sorting and pagination in the datanode tab of the NN Web UI |  Critical | namenode | Nathan Roberts | Haohui Mai |
| [HDFS-8880](https://issues.apache.org/jira/browse/HDFS-8880) | NameNode metrics logging |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-4057](https://issues.apache.org/jira/browse/YARN-4057) | If ContainersMonitor is not enabled, only print related log info one time |  Minor | nodemanager | Jun Gong | Jun Gong |
| [HADOOP-12050](https://issues.apache.org/jira/browse/HADOOP-12050) | Enable MaxInactiveInterval for hadoop http auth token |  Major | security | Benoy Antony | Huizhi Lu |
| [HDFS-8435](https://issues.apache.org/jira/browse/HDFS-8435) | Support CreateFlag in WebHdfs |  Major | webhdfs | Vinoth Sathappan | Jakob Homan |
| [HDFS-8911](https://issues.apache.org/jira/browse/HDFS-8911) | NameNode Metric : Add Editlog counters as a JMX metric |  Major | namenode | Anu Engineer | Anu Engineer |
| [HDFS-8917](https://issues.apache.org/jira/browse/HDFS-8917) | Cleanup BlockInfoUnderConstruction from comments and tests |  Minor | namenode | Zhe Zhang | Zhe Zhang |
| [HDFS-8884](https://issues.apache.org/jira/browse/HDFS-8884) | Fail-fast check in BlockPlacementPolicyDefault#chooseTarget |  Major | . | Yi Liu | Yi Liu |
| [HDFS-8828](https://issues.apache.org/jira/browse/HDFS-8828) | Utilize Snapshot diff report to build diff copy list in distcp |  Major | distcp, snapshots | Yufei Gu | Yufei Gu |
| [HDFS-8924](https://issues.apache.org/jira/browse/HDFS-8924) | Add pluggable interface for reading replicas in DFSClient |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-12325](https://issues.apache.org/jira/browse/HADOOP-12325) | RPC Metrics : Add the ability track and log slow RPCs |  Major | ipc, metrics | Anu Engineer | Anu Engineer |
| [HDFS-8928](https://issues.apache.org/jira/browse/HDFS-8928) | Improvements for BlockUnderConstructionFeature: ReplicaUnderConstruction as a separate class and replicas as an array |  Minor | namenode | Zhe Zhang | Jing Zhao |
| [HDFS-2390](https://issues.apache.org/jira/browse/HDFS-2390) | dfsadmin -setBalancerBandwidth doesnot validate -ve value |  Minor | balancer & mover | Rajit Saha | Gautam Gopalakrishnan |
| [HDFS-8865](https://issues.apache.org/jira/browse/HDFS-8865) | Improve quota initialization performance |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-8983](https://issues.apache.org/jira/browse/HDFS-8983) | NameNode support for protected directories |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8946](https://issues.apache.org/jira/browse/HDFS-8946) | Improve choosing datanode storage for block placement |  Major | namenode | Yi Liu | Yi Liu |
| [HDFS-8965](https://issues.apache.org/jira/browse/HDFS-8965) | Harden edit log reading code against out of memory errors |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-12368](https://issues.apache.org/jira/browse/HADOOP-12368) | Mark ViewFileSystemBaseTest and ViewFsBaseTest as abstract |  Trivial | . | Andrew Wang | Andrew Wang |
| [HADOOP-12367](https://issues.apache.org/jira/browse/HADOOP-12367) | Move TestFileUtil's test resources to resources folder |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-12369](https://issues.apache.org/jira/browse/HADOOP-12369) | Point hadoop-project/pom.xml java.security.krb5.conf within target folder |  Minor | . | Andrew Wang | Andrew Wang |
| [HDFS-328](https://issues.apache.org/jira/browse/HDFS-328) | Improve fs -setrep error message for invalid replication factors |  Major | namenode | Tsz Wo Nicholas Sze | Daniel Templeton |
| [HADOOP-5323](https://issues.apache.org/jira/browse/HADOOP-5323) | Trash documentation should describe its directory structure and configurations |  Minor | documentation | Suman Sehgal | Weiwei Yang |
| [HDFS-9021](https://issues.apache.org/jira/browse/HDFS-9021) | Use a yellow elephant rather than a blue one in diagram |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-12358](https://issues.apache.org/jira/browse/HADOOP-12358) | Add -safely flag to rm to prompt when deleting many files |  Major | fs | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-4024](https://issues.apache.org/jira/browse/YARN-4024) | YARN RM should avoid unnecessary resolving IP when NMs doing heartbeat |  Major | resourcemanager | Wangda Tan | Hong Zhiguo |
| [HADOOP-12384](https://issues.apache.org/jira/browse/HADOOP-12384) | Add "-direct" flag option for fs copy so that user can choose not to create ".\_COPYING\_" file |  Major | fs | Chen He | J.Andreina |
| [HDFS-9019](https://issues.apache.org/jira/browse/HDFS-9019) | Adding informative message to sticky bit permission denied exception |  Minor | security | Thejas M Nair | Xiaoyu Yao |
| [HDFS-8384](https://issues.apache.org/jira/browse/HDFS-8384) | Allow NN to startup if there are files having a lease but are not under construction |  Minor | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-8929](https://issues.apache.org/jira/browse/HDFS-8929) | Add a metric to expose the timestamp of the last journal |  Major | journal-node | Akira Ajisaka | Surendra Singh Lilhore |
| [HDFS-7116](https://issues.apache.org/jira/browse/HDFS-7116) | Add a command to get the balancer bandwidth |  Major | balancer & mover | Akira Ajisaka | Rakesh R |
| [YARN-4086](https://issues.apache.org/jira/browse/YARN-4086) | Allow Aggregated Log readers to handle HAR files |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-8974](https://issues.apache.org/jira/browse/HDFS-8974) | Convert docs in xdoc format to markdown |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-4145](https://issues.apache.org/jira/browse/YARN-4145) | Make RMHATestBase abstract so its not run when running all tests under that namespace |  Minor | . | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-12324](https://issues.apache.org/jira/browse/HADOOP-12324) | Better exception reporting in SaslPlainServer |  Minor | security | Mike Yoder | Mike Yoder |
| [YARN-2005](https://issues.apache.org/jira/browse/YARN-2005) | Blacklisting support for scheduling AMs |  Major | resourcemanager | Jason Lowe | Anubhav Dhoot |
| [HDFS-8829](https://issues.apache.org/jira/browse/HDFS-8829) | Make SO\_RCVBUF and SO\_SNDBUF size configurable for DataTransferProtocol sockets and allow configuring auto-tuning |  Major | datanode | He Tianyi | He Tianyi |
| [HDFS-9065](https://issues.apache.org/jira/browse/HDFS-9065) | Include commas on # of files, blocks, total filesystem objects in NN Web UI |  Minor | namenode | Daniel Templeton | Daniel Templeton |
| [HADOOP-12413](https://issues.apache.org/jira/browse/HADOOP-12413) | AccessControlList should avoid calling getGroupNames in isUserInList with empty groups. |  Major | security | zhihai xu | zhihai xu |
| [HDFS-8953](https://issues.apache.org/jira/browse/HDFS-8953) | DataNode Metrics logging |  Major | . | Kanaka Kumar Avvaru | Kanaka Kumar Avvaru |
| [YARN-4158](https://issues.apache.org/jira/browse/YARN-4158) | Remove duplicate close for LogWriter in AppLogAggregatorImpl#uploadLogsForContainers |  Minor | nodemanager | zhihai xu | zhihai xu |
| [YARN-4149](https://issues.apache.org/jira/browse/YARN-4149) | yarn logs -am should provide an option to fetch all the log files |  Major | client, nodemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-9082](https://issues.apache.org/jira/browse/HDFS-9082) | Change the log level in WebHdfsFileSystem.initialize() from INFO to DEBUG |  Minor | webhdfs | Santhosh G Nayak | Santhosh G Nayak |
| [YARN-4135](https://issues.apache.org/jira/browse/YARN-4135) | Improve the assertion message in MockRM while failing after waiting for the state. |  Trivial | . | nijel | nijel |
| [MAPREDUCE-6478](https://issues.apache.org/jira/browse/MAPREDUCE-6478) | Add an option to skip cleanupJob stage or ignore cleanup failure during commitJob(). |  Major | . | Junping Du | Junping Du |
| [HADOOP-12404](https://issues.apache.org/jira/browse/HADOOP-12404) | Disable caching for JarURLConnection to avoid sharing JarFile with other users when loading resource from URL in Configuration class. |  Minor | conf | zhihai xu | zhihai xu |
| [HADOOP-12428](https://issues.apache.org/jira/browse/HADOOP-12428) | Fix inconsistency between log-level guards and statements |  Minor | . | Jackie Chang | Jagadesh Kiran N |
| [YARN-4095](https://issues.apache.org/jira/browse/YARN-4095) | Avoid sharing AllocatorPerContext object in LocalDirAllocator between ShuffleHandler and LocalDirsHandlerService. |  Major | nodemanager | zhihai xu | zhihai xu |
| [HDFS-5795](https://issues.apache.org/jira/browse/HDFS-5795) | RemoteBlockReader2#checkSuccess() shoud print error status |  Trivial | . | Brandon Li | Xiao Chen |
| [HDFS-9112](https://issues.apache.org/jira/browse/HDFS-9112) | Improve error message for Haadmin when multiple name service IDs are configured |  Major | tools | Anu Engineer | Anu Engineer |
| [HDFS-9132](https://issues.apache.org/jira/browse/HDFS-9132) | Pass genstamp to ReplicaAccessorBuilder |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-8873](https://issues.apache.org/jira/browse/HDFS-8873) | Allow the directoryScanner to be rate-limited |  Major | datanode | Nathan Roberts | Daniel Templeton |
| [HADOOP-12442](https://issues.apache.org/jira/browse/HADOOP-12442) | Display help if the  command option to "hdfs dfs " is not valid |  Minor | . | nijel | nijel |
| [HADOOP-11984](https://issues.apache.org/jira/browse/HADOOP-11984) | Enable parallel JUnit tests in pre-commit. |  Major | build, scripts, test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6471](https://issues.apache.org/jira/browse/MAPREDUCE-6471) | Document distcp incremental copy |  Major | distcp | Arpit Agarwal | Neelesh Srinivas Salian |
| [HDFS-9148](https://issues.apache.org/jira/browse/HDFS-9148) | Incorrect assert message in TestWriteToReplica#testWriteToTemporary |  Trivial | test | Tony Wu | Tony Wu |
| [HDFS-8859](https://issues.apache.org/jira/browse/HDFS-8859) | Improve DataNode ReplicaMap memory footprint to save about 45% |  Major | datanode | Yi Liu | Yi Liu |
| [HDFS-8696](https://issues.apache.org/jira/browse/HDFS-8696) | Make the lower and higher watermark in the DN Netty server configurable |  Major | webhdfs | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-3727](https://issues.apache.org/jira/browse/YARN-3727) | For better error recovery, check if the directory exists before using it for localization. |  Major | nodemanager | zhihai xu | zhihai xu |
| [HDFS-9175](https://issues.apache.org/jira/browse/HDFS-9175) | Change scope of 'AccessTokenProvider.getAccessToken()' and 'CredentialBasedAccessTokenProvider.getCredential()' abstract methods to public |  Major | webhdfs | Santhosh G Nayak | Santhosh G Nayak |
| [HADOOP-12458](https://issues.apache.org/jira/browse/HADOOP-12458) | Retries is typoed to spell Retires in parts of hadoop-yarn and hadoop-common |  Minor | documentation | Neelesh Srinivas Salian | Neelesh Srinivas Salian |
| [HDFS-9151](https://issues.apache.org/jira/browse/HDFS-9151) | Mover should print the exit status/reason on console like balancer tool. |  Minor | balancer & mover | Archana T | Surendra Singh Lilhore |
| [HADOOP-12350](https://issues.apache.org/jira/browse/HADOOP-12350) | WASB Logging: Improve WASB Logging around deletes, reads and writes |  Major | tools | Dushyanth | Dushyanth |
| [HADOOP-12284](https://issues.apache.org/jira/browse/HADOOP-12284) | UserGroupInformation doAs can throw misleading exception |  Trivial | security | Aaron Dossett | Aaron Dossett |
| [YARN-4228](https://issues.apache.org/jira/browse/YARN-4228) | FileSystemRMStateStore use IOUtils#close instead of fs#close |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3943](https://issues.apache.org/jira/browse/YARN-3943) | Use separate threshold configurations for disk-full detection and disk-not-full detection. |  Critical | nodemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-6479](https://issues.apache.org/jira/browse/MAPREDUCE-6479) | Add missing mapred job command options in mapreduce document |  Major | documentation | nijel | nijel |
| [HADOOP-11104](https://issues.apache.org/jira/browse/HADOOP-11104) | org.apache.hadoop.metrics2.lib.MetricsRegistry needs numerical parameter checking |  Minor | . | Ray Chiang | Ray Chiang |
| [HDFS-9181](https://issues.apache.org/jira/browse/HDFS-9181) | Better handling of exceptions thrown during upgrade shutdown |  Minor | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9110](https://issues.apache.org/jira/browse/HDFS-9110) | Use Files.walkFileTree in NNUpgradeUtil#doPreUpgrade for better efficiency |  Minor | . | Charlie Helin | Charlie Helin |
| [HDFS-9221](https://issues.apache.org/jira/browse/HDFS-9221) | HdfsServerConstants#ReplicaState#getState should avoid calling values() since it creates a temporary array |  Major | performance | Staffan Friberg | Staffan Friberg |
| [HDFS-8988](https://issues.apache.org/jira/browse/HDFS-8988) | Use LightWeightHashSet instead of LightWeightLinkedSet in BlockManager#excessReplicateMap |  Major | . | Yi Liu | Yi Liu |
| [HDFS-9139](https://issues.apache.org/jira/browse/HDFS-9139) | Enable parallel JUnit tests for HDFS Pre-commit |  Major | test | Vinayakumar B | Vinayakumar B |
| [HDFS-9145](https://issues.apache.org/jira/browse/HDFS-9145) | Tracking methods that hold FSNamesytemLock for too long |  Major | namenode | Jing Zhao | Mingliang Liu |
| [HADOOP-10775](https://issues.apache.org/jira/browse/HADOOP-10775) | Shell operations to fail with meaningful errors on windows if winutils.exe not found |  Minor | util | Steve Loughran | Steve Loughran |
| [YARN-4253](https://issues.apache.org/jira/browse/YARN-4253) | Standardize on using PrivilegedOperationExecutor for all invocations of container-executor in LinuxContainerExecutor |  Major | . | Sidharta Seethana | Sidharta Seethana |
| [YARN-4252](https://issues.apache.org/jira/browse/YARN-4252) | Log container-executor invocation details when exit code is non-zero |  Minor | nodemanager | Sidharta Seethana | Sidharta Seethana |
| [HDFS-9238](https://issues.apache.org/jira/browse/HDFS-9238) | Update TestFileCreation#testLeaseExpireHardLimit() to avoid using DataNodeTestUtils#getFile() |  Trivial | test | Tony Wu | Tony Wu |
| [HDFS-9188](https://issues.apache.org/jira/browse/HDFS-9188) | Make block corruption related tests FsDataset-agnostic. |  Major | test | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-9205](https://issues.apache.org/jira/browse/HDFS-9205) | Do not schedule corrupt blocks for replication |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-12481](https://issues.apache.org/jira/browse/HADOOP-12481) | JWTRedirectAuthenticationHandler doesn't Retain Original Query String |  Major | security | Larry McCay | Larry McCay |
| [HDFS-9257](https://issues.apache.org/jira/browse/HDFS-9257) | improve error message for "Absolute path required" in INode.java to contain the rejected path |  Trivial | namenode | Marcell Szabo | Marcell Szabo |
| [HDFS-9253](https://issues.apache.org/jira/browse/HDFS-9253) | Refactor tests of libhdfs into a directory |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-12450](https://issues.apache.org/jira/browse/HADOOP-12450) | UserGroupInformation should not log at WARN level if no groups are found |  Minor | security | Elliott Clark | Elliott Clark |
| [HADOOP-12460](https://issues.apache.org/jira/browse/HADOOP-12460) | Add overwrite option for 'get' shell command |  Major | . | Keegan Witt | Jagadesh Kiran N |
| [HDFS-9250](https://issues.apache.org/jira/browse/HDFS-9250) | Add Precondition check to LocatedBlock#addCachedLoc |  Major | namenode | Xiao Chen | Xiao Chen |
| [HDFS-9251](https://issues.apache.org/jira/browse/HDFS-9251) | Refactor TestWriteToReplica and TestFsDatasetImpl to avoid explicitly creating Files in tests code. |  Major | test | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [MAPREDUCE-6489](https://issues.apache.org/jira/browse/MAPREDUCE-6489) | Fail fast rogue tasks that write too much to local disk |  Major | task | Maysam Yabandeh | Maysam Yabandeh |
| [HDFS-8647](https://issues.apache.org/jira/browse/HDFS-8647) | Abstract BlockManager's rack policy into BlockPlacementPolicy |  Major | . | Ming Ma | Brahma Reddy Battula |
| [HDFS-7087](https://issues.apache.org/jira/browse/HDFS-7087) | Ability to list /.reserved |  Major | . | Andrew Wang | Xiao Chen |
| [HADOOP-12436](https://issues.apache.org/jira/browse/HADOOP-12436) | GlobPattern regex library has performance issues with wildcard characters |  Major | fs | Matthew Paduano | Matthew Paduano |
| [HDFS-9280](https://issues.apache.org/jira/browse/HDFS-9280) | Document NFS gateway export point parameter |  Trivial | documentation | Zhe Zhang | Xiao Chen |
| [HADOOP-12334](https://issues.apache.org/jira/browse/HADOOP-12334) | Change Mode Of Copy Operation of HBase WAL Archiving to bypass Azure Storage Throttling after retries |  Major | tools | Gaurav Kanade | Gaurav Kanade |
| [HADOOP-7266](https://issues.apache.org/jira/browse/HADOOP-7266) | Deprecate metrics v1 |  Blocker | metrics | Luke Lu | Akira Ajisaka |
| [YARN-2913](https://issues.apache.org/jira/browse/YARN-2913) | Fair scheduler should have ability to set MaxResourceDefault for each queue |  Major | . | Siqi Li | Siqi Li |
| [HDFS-9264](https://issues.apache.org/jira/browse/HDFS-9264) | Minor cleanup of operations on FsVolumeList#volumes |  Minor | . | Walter Su | Walter Su |
| [HDFS-8808](https://issues.apache.org/jira/browse/HDFS-8808) | dfs.image.transfer.bandwidthPerSec should not apply to -bootstrapStandby |  Major | . | Gautam Gopalakrishnan | Zhe Zhang |
| [HDFS-9297](https://issues.apache.org/jira/browse/HDFS-9297) | Update TestBlockMissingException to use corruptBlockOnDataNodesByDeletingBlockFile() |  Trivial | test | Tony Wu | Tony Wu |
| [HDFS-4015](https://issues.apache.org/jira/browse/HDFS-4015) | Safemode should count and report orphaned blocks |  Major | namenode | Todd Lipcon | Anu Engineer |
| [YARN-3528](https://issues.apache.org/jira/browse/YARN-3528) | Tests with 12345 as hard-coded port break jenkins |  Blocker | . | Steve Loughran | Brahma Reddy Battula |
| [YARN-4285](https://issues.apache.org/jira/browse/YARN-4285) | Display resource usage as percentage of queue and cluster in the RM UI |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-7284](https://issues.apache.org/jira/browse/HDFS-7284) | Add more debug info to BlockInfoUnderConstruction#setGenerationStampAndVerifyReplicas |  Major | namenode | Hu Liu, | Wei-Chiu Chuang |
| [HADOOP-12472](https://issues.apache.org/jira/browse/HADOOP-12472) | Make GenericTestUtils.assertExceptionContains robust |  Minor | test | Steve Loughran | Steve Loughran |
| [HDFS-9291](https://issues.apache.org/jira/browse/HDFS-9291) | Fix TestInterDatanodeProtocol to be FsDataset-agnostic. |  Minor | test | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-8945](https://issues.apache.org/jira/browse/HDFS-8945) | Update the description about replica placement in HDFS Architecture documentation |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-9292](https://issues.apache.org/jira/browse/HDFS-9292) | Make TestFileConcorruption independent to underlying FsDataset Implementation. |  Minor | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-9259](https://issues.apache.org/jira/browse/HDFS-9259) | Make SO\_SNDBUF size configurable at DFSClient side for hdfs write scenario |  Major | . | Ming Ma | Mingliang Liu |
| [HDFS-9299](https://issues.apache.org/jira/browse/HDFS-9299) | Give ReplicationMonitor a readable thread name |  Trivial | namenode | Staffan Friberg | Staffan Friberg |
| [HDFS-9307](https://issues.apache.org/jira/browse/HDFS-9307) | fuseConnect should be private to fuse\_connect.c |  Trivial | fuse-dfs | Colin P. McCabe | Mingliang Liu |
| [HADOOP-12520](https://issues.apache.org/jira/browse/HADOOP-12520) | Use XInclude in hadoop-azure test configuration to isolate Azure Storage account keys for service integration tests. |  Major | azure, test | Chris Nauroth | Chris Nauroth |
| [HDFS-9311](https://issues.apache.org/jira/browse/HDFS-9311) | Support optional offload of NameNode HA service health checks to a separate RPC server. |  Major | ha, namenode | Chris Nauroth | Chris Nauroth |
| [HADOOP-12385](https://issues.apache.org/jira/browse/HADOOP-12385) | include nested stack trace in SaslRpcClient.getServerToken() |  Minor | security | Steve Loughran | Steve Loughran |
| [HDFS-9255](https://issues.apache.org/jira/browse/HDFS-9255) | Consolidate block recovery related implementation into a single class |  Minor | datanode | Walter Su | Walter Su |
| [YARN-2573](https://issues.apache.org/jira/browse/YARN-2573) | Integrate ReservationSystem with the RM failover mechanism |  Major | capacityscheduler, fairscheduler, resourcemanager | Subru Krishnan | Subru Krishnan |
| [HDFS-6200](https://issues.apache.org/jira/browse/HDFS-6200) | Create a separate jar for hdfs-client |  Major | build | Haohui Mai | Haohui Mai |
| [HDFS-8545](https://issues.apache.org/jira/browse/HDFS-8545) | Refactor FS#getUsed() to use ContentSummary and add an API to fetch the total file length from a specific path |  Minor | . | J.Andreina | J.Andreina |
| [HDFS-9229](https://issues.apache.org/jira/browse/HDFS-9229) | Expose size of NameNode directory as a metric |  Minor | namenode | Zhe Zhang | Surendra Singh Lilhore |
| [YARN-4310](https://issues.apache.org/jira/browse/YARN-4310) | FairScheduler: Log skipping reservation messages at DEBUG level |  Minor | fairscheduler | Arun Suresh | Arun Suresh |
| [HDFS-9323](https://issues.apache.org/jira/browse/HDFS-9323) | Randomize the DFSStripedOutputStreamWithFailure tests |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-12133](https://issues.apache.org/jira/browse/HADOOP-12133) | Add schemas to Maven Assembly XMLs |  Minor | . | Gabor Liptak | Gabor Liptak |
| [HDFS-9312](https://issues.apache.org/jira/browse/HDFS-9312) | Fix TestReplication to be FsDataset-agnostic. |  Minor | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-9308](https://issues.apache.org/jira/browse/HDFS-9308) | Add truncateMeta() and deleteMeta() to MiniDFSCluster |  Minor | test | Tony Wu | Tony Wu |
| [HADOOP-12541](https://issues.apache.org/jira/browse/HADOOP-12541) | make re2j dependency consistent |  Major | build | Allen Wittenauer | Matthew Paduano |
| [HDFS-9331](https://issues.apache.org/jira/browse/HDFS-9331) | Modify TestNameNodeMXBean#testNameNodeMXBeanInfo() to account for filesystem entirely allocated for DFS use |  Trivial | test | Tony Wu | Tony Wu |
| [HDFS-9363](https://issues.apache.org/jira/browse/HDFS-9363) | Add fetchReplica() to FsDatasetTestUtils to return FsDataset-agnostic Replica. |  Minor | test | Tony Wu | Tony Wu |
| [HDFS-9282](https://issues.apache.org/jira/browse/HDFS-9282) | Make data directory count and storage raw capacity related tests FsDataset-agnostic |  Minor | test | Tony Wu | Tony Wu |
| [HADOOP-12344](https://issues.apache.org/jira/browse/HADOOP-12344) | Improve validateSocketPathSecurity0 error message |  Trivial | net | Casey Brotherton | Casey Brotherton |
| [HDFS-9398](https://issues.apache.org/jira/browse/HDFS-9398) | Make ByteArraryManager log message in one-line format |  Minor | hdfs-client | Mingliang Liu | Mingliang Liu |
| [HDFS-9234](https://issues.apache.org/jira/browse/HDFS-9234) | WebHdfs : getContentSummary() should give quota for storage types |  Major | webhdfs | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-9369](https://issues.apache.org/jira/browse/HDFS-9369) | Use ctest to run tests for hadoop-hdfs-native-client |  Minor | . | Haohui Mai | Haohui Mai |
| [HADOOP-12562](https://issues.apache.org/jira/browse/HADOOP-12562) | Make hadoop dockerfile usable by Yetus |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [YARN-4287](https://issues.apache.org/jira/browse/YARN-4287) | Capacity Scheduler: Rack Locality improvement |  Major | capacityscheduler | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-5485](https://issues.apache.org/jira/browse/MAPREDUCE-5485) | Allow repeating job commit by extending OutputCommitter API |  Critical | . | Nemon Lou | Junping Du |
| [MAPREDUCE-6499](https://issues.apache.org/jira/browse/MAPREDUCE-6499) | Add elapsed time for retired job in JobHistoryServer WebUI |  Major | webapps | Yiqun Lin | Yiqun Lin |
| [HDFS-9252](https://issues.apache.org/jira/browse/HDFS-9252) | Change TestFileTruncate to use FsDatasetTestUtils to get block file size and genstamp. |  Major | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-12568](https://issues.apache.org/jira/browse/HADOOP-12568) | Update core-default.xml to describe posixGroups support |  Minor | documentation | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4279](https://issues.apache.org/jira/browse/YARN-4279) | Mark ApplicationId and ApplicationAttemptId static methods as @Public, @Unstable |  Minor | client | Steve Loughran | Steve Loughran |
| [HADOOP-12575](https://issues.apache.org/jira/browse/HADOOP-12575) | Add build instruction for docker toolbox instead of boot2docker |  Trivial | documentation | Kai Sasaki | Kai Sasaki |
| [HDFS-8056](https://issues.apache.org/jira/browse/HDFS-8056) | Decommissioned dead nodes should continue to be counted as dead after NN restart |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-11901](https://issues.apache.org/jira/browse/HADOOP-11901) | BytesWritable fails to support 2G chunks due to integer overflow |  Major | . | Reynold Xin | Reynold Xin |
| [HDFS-9439](https://issues.apache.org/jira/browse/HDFS-9439) | Include status of closeAck into exception message in DataNode#run |  Trivial | . | Xiao Chen | Xiao Chen |
| [HDFS-9402](https://issues.apache.org/jira/browse/HDFS-9402) | Switch DataNode.LOG to use slf4j |  Minor | . | Walter Su | Walter Su |
| [HDFS-3302](https://issues.apache.org/jira/browse/HDFS-3302) | Review and improve HDFS trash documentation |  Major | documentation | Harsh J | Harsh J |
| [HADOOP-10035](https://issues.apache.org/jira/browse/HADOOP-10035) | Cleanup TestFilterFileSystem |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-10555](https://issues.apache.org/jira/browse/HADOOP-10555) | Add offset support to MurmurHash |  Trivial | . | Sergey Shelukhin | Sergey Shelukhin |
| [HADOOP-10068](https://issues.apache.org/jira/browse/HADOOP-10068) | Improve log4j regex in testFindContainingJar |  Trivial | . | Robert Rati | Robert Rati |
| [HDFS-9024](https://issues.apache.org/jira/browse/HDFS-9024) | Deprecate the TotalFiles metric |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-7988](https://issues.apache.org/jira/browse/HDFS-7988) | Replace usage of ExactSizeInputStream with LimitInputStream. |  Minor | . | Chris Nauroth | Walter Su |
| [HDFS-9314](https://issues.apache.org/jira/browse/HDFS-9314) | Improve BlockPlacementPolicyDefault's picking of excess replicas |  Major | . | Ming Ma | Xiao Chen |
| [MAPREDUCE-5870](https://issues.apache.org/jira/browse/MAPREDUCE-5870) | Support for passing Job priority through Application Submission Context in Mapreduce Side |  Major | client | Sunil G | Sunil G |
| [HDFS-9434](https://issues.apache.org/jira/browse/HDFS-9434) | Recommission a datanode with 500k blocks may pause NN for 30 seconds |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-4132](https://issues.apache.org/jira/browse/YARN-4132) | Separate configs for nodemanager to resourcemanager connection timeout and retries |  Major | nodemanager | Chang Li | Chang Li |
| [HDFS-8512](https://issues.apache.org/jira/browse/HDFS-8512) | WebHDFS : GETFILESTATUS should return LocatedBlock with storage type info |  Major | webhdfs | Sumana Sathish | Xiaoyu Yao |
| [HADOOP-12600](https://issues.apache.org/jira/browse/HADOOP-12600) | FileContext and AbstractFileSystem should be annotated as a Stable interface. |  Blocker | fs | Chris Nauroth | Chris Nauroth |
| [HDFS-9269](https://issues.apache.org/jira/browse/HDFS-9269) | Update the documentation and wrapper for fuse-dfs |  Minor | fuse-dfs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9485](https://issues.apache.org/jira/browse/HDFS-9485) | Make BlockManager#removeFromExcessReplicateMap accept BlockInfo instead of Block |  Minor | namenode | Mingliang Liu | Mingliang Liu |
| [HDFS-9490](https://issues.apache.org/jira/browse/HDFS-9490) | MiniDFSCluster should change block generation stamp via FsDatasetTestUtils |  Minor | test | Tony Wu | Tony Wu |
| [HDFS-8831](https://issues.apache.org/jira/browse/HDFS-8831) | Trash Support for deletion in HDFS encryption zone |  Major | encryption | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-9267](https://issues.apache.org/jira/browse/HDFS-9267) | TestDiskError should get stored replicas through FsDatasetTestUtils. |  Minor | test | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-9474](https://issues.apache.org/jira/browse/HDFS-9474) | TestPipelinesFailover should not fail when printing debug message |  Major | . | Yongjun Zhang | John Zhuge |
| [HDFS-9491](https://issues.apache.org/jira/browse/HDFS-9491) | Tests should get the number of pending async delets via FsDatasetTestUtils |  Minor | test | Tony Wu | Tony Wu |
| [YARN-3456](https://issues.apache.org/jira/browse/YARN-3456) | Improve handling of incomplete TimelineEntities |  Minor | timelineserver | Steve Loughran | Varun Saxena |
| [HADOOP-12625](https://issues.apache.org/jira/browse/HADOOP-12625) | Add a config to disable the /logs endpoints |  Major | security | Robert Kanter | Robert Kanter |
| [HDFS-9527](https://issues.apache.org/jira/browse/HDFS-9527) | The return type of FSNamesystem.getBlockCollection should be changed to INodeFile |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-9472](https://issues.apache.org/jira/browse/HDFS-9472) | concat() API does not give proper exception messages on ./reserved relative path |  Major | namenode | Rakesh R | Rakesh R |
| [YARN-4341](https://issues.apache.org/jira/browse/YARN-4341) | add doc about timeline performance tool usage |  Major | . | Chang Li | Chang Li |
| [HDFS-9532](https://issues.apache.org/jira/browse/HDFS-9532) | Detailed exception info is lost in reportTo method of ErrorReportAction and ReportBadBlockAction |  Trivial | datanode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-9528](https://issues.apache.org/jira/browse/HDFS-9528) | Cleanup namenode audit/log/exception messages |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-9281](https://issues.apache.org/jira/browse/HDFS-9281) | Change TestDeleteBlockPool to not explicitly use File to check block pool existence. |  Minor | test | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-8860](https://issues.apache.org/jira/browse/HDFS-8860) | Remove unused Replica copyOnWrite code |  Major | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [MAPREDUCE-6436](https://issues.apache.org/jira/browse/MAPREDUCE-6436) | JobHistory cache issue |  Blocker | . | Ryu Kobayashi | Kai Sasaki |
| [HADOOP-12639](https://issues.apache.org/jira/browse/HADOOP-12639) | Imrpove JavaDoc for getTrimmedStrings |  Trivial | util | BELUGA BEHR | BELUGA BEHR |
| [HDFS-9557](https://issues.apache.org/jira/browse/HDFS-9557) | Reduce object allocation in PB conversion |  Major | hdfs-client | Daryn Sharp | Daryn Sharp |
| [YARN-4207](https://issues.apache.org/jira/browse/YARN-4207) | Add a non-judgemental YARN app completion status |  Major | . | Sergey Shelukhin | Rich Haase |
| [HDFS-9198](https://issues.apache.org/jira/browse/HDFS-9198) | Coalesce IBR processing in the NN |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-9552](https://issues.apache.org/jira/browse/HDFS-9552) | Document types of permission checks performed for HDFS operations. |  Major | documentation | Chris Nauroth | Chris Nauroth |
| [HDFS-8477](https://issues.apache.org/jira/browse/HDFS-8477) | describe dfs.ha.zkfc.port in hdfs-default.xml |  Minor | . | Kanaka Kumar Avvaru | Kanaka Kumar Avvaru |
| [HDFS-9350](https://issues.apache.org/jira/browse/HDFS-9350) | Avoid creating temprorary strings in Block.toString() and getBlockName() |  Minor | performance | Staffan Friberg | Staffan Friberg |
| [HADOOP-12570](https://issues.apache.org/jira/browse/HADOOP-12570) | HDFS Secure Mode Documentation updates |  Major | documentation | Arpit Agarwal | Arpit Agarwal |
| [YARN-4480](https://issues.apache.org/jira/browse/YARN-4480) | Clean up some inappropriate imports |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-9582](https://issues.apache.org/jira/browse/HDFS-9582) | TestLeaseRecoveryStriped file missing Apache License header and not well formatted |  Minor | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-4290](https://issues.apache.org/jira/browse/YARN-4290) | Add -showDetails option to YARN Nodes CLI to print all nodes reports information |  Major | client | Wangda Tan | Sunil G |
| [YARN-4400](https://issues.apache.org/jira/browse/YARN-4400) | AsyncDispatcher.waitForDrained should be final |  Trivial | yarn | Daniel Templeton | Daniel Templeton |
| [HADOOP-12566](https://issues.apache.org/jira/browse/HADOOP-12566) | Add NullGroupMapping |  Major | . | Daniel Templeton | Daniel Templeton |
| [YARN-2934](https://issues.apache.org/jira/browse/YARN-2934) | Improve handling of container's stderr |  Critical | . | Gera Shegalov | Naganarasimha G R |
| [MAPREDUCE-6584](https://issues.apache.org/jira/browse/MAPREDUCE-6584) | Remove trailing whitespaces from mapred-default.xml |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12686](https://issues.apache.org/jira/browse/HADOOP-12686) | Update FileSystemShell documentation to mention the meaning of each columns of fs -du |  Minor | documentation, fs | Daisuke Kobayashi | Daisuke Kobayashi |
| [HADOOP-12663](https://issues.apache.org/jira/browse/HADOOP-12663) | Remove Hard-Coded Values From FileSystem.java |  Trivial | fs | BELUGA BEHR | BELUGA BEHR |
| [YARN-4544](https://issues.apache.org/jira/browse/YARN-4544) | All the log messages about rolling monitoring interval are shown with WARN level |  Minor | log-aggregation, nodemanager | Takashi Ohnishi | Takashi Ohnishi |
| [YARN-4438](https://issues.apache.org/jira/browse/YARN-4438) | Implement RM leader election with curator |  Major | . | Jian He | Jian He |
| [HDFS-9630](https://issues.apache.org/jira/browse/HDFS-9630) | DistCp minor refactoring and clean up |  Minor | distcp | Kai Zheng | Kai Zheng |
| [YARN-4582](https://issues.apache.org/jira/browse/YARN-4582) | Label-related invalid resource request exception should be able to properly handled by application |  Major | scheduler | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9569](https://issues.apache.org/jira/browse/HDFS-9569) | Log the name of the fsimage being loaded for better supportability |  Trivial | namenode | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-6473](https://issues.apache.org/jira/browse/MAPREDUCE-6473) | Job submission can take a long time during Cluster initialization |  Major | performance | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-12683](https://issues.apache.org/jira/browse/HADOOP-12683) | Add number of samples in last interval in snapshot of MutableStat |  Minor | metrics | Vikram Srivastava | Vikram Srivastava |
| [HADOOP-8887](https://issues.apache.org/jira/browse/HADOOP-8887) | Use a Maven plugin to build the native code using CMake |  Minor | build | Colin P. McCabe | Colin P. McCabe |
| [HDFS-9415](https://issues.apache.org/jira/browse/HDFS-9415) | Document dfs.cluster.administrators and dfs.permissions.superusergroup |  Major | documentation | Arpit Agarwal | Xiaobing Zhou |
| [HADOOP-12713](https://issues.apache.org/jira/browse/HADOOP-12713) | Disable spurious checkstyle checks |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-12662](https://issues.apache.org/jira/browse/HADOOP-12662) | The build should fail if a -Dbundle option fails |  Minor | . | Kai Zheng | Kai Zheng |
| [HDFS-6054](https://issues.apache.org/jira/browse/HDFS-6054) | MiniQJMHACluster should not use static port to avoid binding failure in unit test |  Major | test | Brandon Li | Yongjun Zhang |
| [HDFS-9576](https://issues.apache.org/jira/browse/HDFS-9576) | HTrace: collect position/length information on read operations |  Major | hdfs-client, tracing | Zhe Zhang | Zhe Zhang |
| [YARN-4603](https://issues.apache.org/jira/browse/YARN-4603) | FairScheduler should mention user requested queuename in error message when failed in queue ACL check |  Trivial | fairscheduler | Tao Jie | Tao Jie |
| [YARN-4492](https://issues.apache.org/jira/browse/YARN-4492) | Add documentation for preemption supported in Capacity scheduler |  Minor | capacity scheduler | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9674](https://issues.apache.org/jira/browse/HDFS-9674) | The HTrace span for OpWriteBlock should record the maxWriteToDisk time |  Major | datanode, tracing | Colin P. McCabe | Colin P. McCabe |
| [YARN-4371](https://issues.apache.org/jira/browse/YARN-4371) | "yarn application -kill" should take multiple application ids |  Major | . | Tsuyoshi Ozawa | Sunil G |
| [YARN-4496](https://issues.apache.org/jira/browse/YARN-4496) | Improve HA ResourceManager Failover detection on the client |  Major | client, resourcemanager | Arun Suresh | Jian He |
| [HDFS-9653](https://issues.apache.org/jira/browse/HDFS-9653) | Expose the number of blocks pending deletion through dfsadmin report command |  Major | hdfs-client, tools | Weiwei Yang | Weiwei Yang |
| [HADOOP-12731](https://issues.apache.org/jira/browse/HADOOP-12731) | Remove useless boxing/unboxing code |  Minor | performance | Kousuke Saruta | Kousuke Saruta |
| [YARN-3542](https://issues.apache.org/jira/browse/YARN-3542) | Re-factor support for CPU as a resource using the new ResourceHandler mechanism |  Critical | nodemanager | Sidharta Seethana | Varun Vasudev |
| [HDFS-9541](https://issues.apache.org/jira/browse/HDFS-9541) | Add hdfsStreamBuilder API to libhdfs to support defaultBlockSizes greater than 2 GB |  Major | libhdfs | Colin P. McCabe | Colin P. McCabe |
| [YARN-4462](https://issues.apache.org/jira/browse/YARN-4462) | FairScheduler: Disallow preemption from a queue |  Major | fairscheduler | Tao Jie | Tao Jie |
| [HDFS-9677](https://issues.apache.org/jira/browse/HDFS-9677) | Rename generationStampV1/generationStampV2 to legacyGenerationStamp/generationStamp |  Minor | namenode | Jing Zhao | Mingliang Liu |
| [MAPREDUCE-6431](https://issues.apache.org/jira/browse/MAPREDUCE-6431) | JobClient should be an AutoClosable |  Major | . | André Kelpe | Haibo Chen |
| [HDFS-9654](https://issues.apache.org/jira/browse/HDFS-9654) | Code refactoring for HDFS-8578 |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-7764](https://issues.apache.org/jira/browse/HDFS-7764) | DirectoryScanner shouldn't abort the scan if one directory had an error |  Major | datanode | Rakesh R | Rakesh R |
| [HDFS-9706](https://issues.apache.org/jira/browse/HDFS-9706) | Log more details in debug logs in BlockReceiver's constructor |  Minor | . | Xiao Chen | Xiao Chen |
| [YARN-4647](https://issues.apache.org/jira/browse/YARN-4647) | Make RegisterNodeManagerRequestPBImpl thread-safe |  Major | nodemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-9638](https://issues.apache.org/jira/browse/HDFS-9638) | Improve DistCp Help and documentation |  Minor | distcp | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9566](https://issues.apache.org/jira/browse/HDFS-9566) | Remove expensive 'BlocksMap#getStorages(Block b, final DatanodeStorage.State state)' method |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [YARN-4649](https://issues.apache.org/jira/browse/YARN-4649) | Add additional logging to some NM state store operations |  Minor | . | Sidharta Seethana | Sidharta Seethana |
| [HDFS-9721](https://issues.apache.org/jira/browse/HDFS-9721) | Allow Delimited PB OIV tool to run upon fsimage that contains INodeReference |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-9669](https://issues.apache.org/jira/browse/HDFS-9669) | TcpPeerServer should respect ipc.server.listen.queue.size |  Major | . | Elliott Clark | Elliott Clark |
| [HDFS-9260](https://issues.apache.org/jira/browse/HDFS-9260) | Improve the performance and GC friendliness of NameNode startup and full block reports |  Major | datanode, namenode, performance | Staffan Friberg | Staffan Friberg |
| [HDFS-9715](https://issues.apache.org/jira/browse/HDFS-9715) | Check storage ID uniqueness on datanode startup |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-4662](https://issues.apache.org/jira/browse/YARN-4662) | Document some newly added metrics |  Major | . | Jian He | Jian He |
| [HDFS-9629](https://issues.apache.org/jira/browse/HDFS-9629) | Update the footer of Web UI to show year 2016 |  Major | . | Xiao Chen | Xiao Chen |
| [MAPREDUCE-6566](https://issues.apache.org/jira/browse/MAPREDUCE-6566) | Add retry support to mapreduce CLI tool |  Major | . | Varun Vasudev | Varun Vasudev |
| [HDFS-9726](https://issues.apache.org/jira/browse/HDFS-9726) | Refactor IBR code to a new class |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-12772](https://issues.apache.org/jira/browse/HADOOP-12772) | NetworkTopologyWithNodeGroup.getNodeGroup() can loop infinitely for invalid 'loc' values |  Minor | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-12758](https://issues.apache.org/jira/browse/HADOOP-12758) | Extend CSRF Filter with UserAgent Checks |  Major | security | Larry McCay | Larry McCay |
| [HDFS-9686](https://issues.apache.org/jira/browse/HDFS-9686) | Remove useless boxing/unboxing code |  Minor | performance | Kousuke Saruta | Kousuke Saruta |
| [HADOOP-12759](https://issues.apache.org/jira/browse/HADOOP-12759) | RollingFileSystemSink should eagerly rotate directories |  Critical | . | Daniel Templeton | Daniel Templeton |
| [MAPREDUCE-6626](https://issues.apache.org/jira/browse/MAPREDUCE-6626) | Reuse ObjectMapper instance in MapReduce |  Minor | performance | Yiqun Lin | Yiqun Lin |
| [HADOOP-12788](https://issues.apache.org/jira/browse/HADOOP-12788) | OpensslAesCtrCryptoCodec should log which random number generator is used. |  Minor | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4628](https://issues.apache.org/jira/browse/YARN-4628) | Display application priority in yarn top |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4655](https://issues.apache.org/jira/browse/YARN-4655) | Log uncaught exceptions/errors in various thread pools in YARN |  Major | . | Sidharta Seethana | Sidharta Seethana |
| [HADOOP-12764](https://issues.apache.org/jira/browse/HADOOP-12764) | Increase default value of KMS maxHttpHeaderSize and make it configurable |  Minor | . | Zhe Zhang | Zhe Zhang |
| [HDFS-9637](https://issues.apache.org/jira/browse/HDFS-9637) | Tests for RollingFileSystemSink |  Major | test | Daniel Templeton | Daniel Templeton |
| [HDFS-9795](https://issues.apache.org/jira/browse/HDFS-9795) | OIV Delimited should show which files are ACL-enabled. |  Trivial | tools | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-12776](https://issues.apache.org/jira/browse/HADOOP-12776) | Remove getaclstatus call for non-acl commands in getfacl. |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9768](https://issues.apache.org/jira/browse/HDFS-9768) | Reuse objectMapper instance in HDFS to improve the performance |  Major | performance | Yiqun Lin | Yiqun Lin |
| [HDFS-9644](https://issues.apache.org/jira/browse/HDFS-9644) | Update encryption documentation to reflect nested EZs |  Major | documentation, encryption | Zhe Zhang | Zhe Zhang |
| [HDFS-9700](https://issues.apache.org/jira/browse/HDFS-9700) | DFSClient and DFSOutputStream should set TCP\_NODELAY on sockets for DataTransferProtocol |  Major | hdfs-client | Gary Helmling | Gary Helmling |
| [HDFS-9797](https://issues.apache.org/jira/browse/HDFS-9797) | Log Standby exceptions thrown by RequestHedgingProxyProvider at DEBUG Level |  Minor | hdfs-client | Inigo Goiri | Inigo Goiri |
| [YARN-4682](https://issues.apache.org/jira/browse/YARN-4682) | AMRM client to log when AMRM token updated |  Major | client | Steve Loughran | Prabhu Joseph |
| [HADOOP-12805](https://issues.apache.org/jira/browse/HADOOP-12805) | Annotate CanUnbuffer with @InterfaceAudience.Public |  Major | . | Ted Yu | Ted Yu |
| [YARN-4569](https://issues.apache.org/jira/browse/YARN-4569) | Remove incorrect part of maxResources in FairScheduler documentation |  Major | documentation | Ray Chiang | Ray Chiang |
| [YARN-4690](https://issues.apache.org/jira/browse/YARN-4690) | Skip object allocation in FSAppAttempt#getResourceUsage when possible |  Major | . | Ming Ma | Ming Ma |
| [MAPREDUCE-6634](https://issues.apache.org/jira/browse/MAPREDUCE-6634) | Log uncaught exceptions/errors in various thread pools in mapreduce |  Major | . | Sidharta Seethana | Sidharta Seethana |
| [HADOOP-12817](https://issues.apache.org/jira/browse/HADOOP-12817) | Enable TLS v1.1 and 1.2 |  Major | security | Robert Kanter | Robert Kanter |
| [HADOOP-10865](https://issues.apache.org/jira/browse/HADOOP-10865) | Add a Crc32 chunked verification benchmark for both directly and non-directly buffer cases |  Minor | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4946](https://issues.apache.org/jira/browse/HDFS-4946) | Allow preferLocalNode in BlockPlacementPolicyDefault to be configurable |  Major | namenode | James Kinley | Nathan Roberts |
| [HADOOP-11031](https://issues.apache.org/jira/browse/HADOOP-11031) | Design Document for Credential Provider API |  Major | site | Larry McCay | Larry McCay |
| [MAPREDUCE-6627](https://issues.apache.org/jira/browse/MAPREDUCE-6627) | Add machine-readable output to mapred job -history command |  Major | client | Robert Kanter | Robert Kanter |
| [HADOOP-12828](https://issues.apache.org/jira/browse/HADOOP-12828) | Print user when services are started |  Trivial | . | Brandon Li | Wei-Chiu Chuang |
| [HADOOP-12794](https://issues.apache.org/jira/browse/HADOOP-12794) | Support additional compression levels for GzipCodec |  Major | io | Ravi Mutyala | Ravi Mutyala |
| [YARN-4708](https://issues.apache.org/jira/browse/YARN-4708) | Missing default mapper type in TimelineServer performance test tool usage |  Minor | timelineserver | Kai Sasaki | Kai Sasaki |
| [HDFS-9425](https://issues.apache.org/jira/browse/HDFS-9425) | Expose number of blocks per volume as a metric |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12668](https://issues.apache.org/jira/browse/HADOOP-12668) | Support excluding weak Ciphers in HttpServer2 through ssl-server.conf |  Critical | security | Vijay Singh | Vijay Singh |
| [HADOOP-12555](https://issues.apache.org/jira/browse/HADOOP-12555) | WASB to read credentials from a credential provider |  Minor | azure | Chris Nauroth | Larry McCay |
| [HDFS-8578](https://issues.apache.org/jira/browse/HDFS-8578) | On upgrade, Datanode should process all storage/data dirs in parallel |  Critical | datanode | Raju Bairishetti | Vinayakumar B |
| [HADOOP-12829](https://issues.apache.org/jira/browse/HADOOP-12829) | StatisticsDataReferenceCleaner swallows interrupt exceptions |  Minor | fs | Gregory Chanan | Gregory Chanan |
| [MAPREDUCE-6640](https://issues.apache.org/jira/browse/MAPREDUCE-6640) | mapred job -history command should be able to take Job ID |  Major | client | Robert Kanter | Robert Kanter |
| [HADOOP-12535](https://issues.apache.org/jira/browse/HADOOP-12535) | Run FileSystem contract tests with hadoop-azure. |  Major | azure, test | Chris Nauroth | madhumita chakraborty |
| [HDFS-9854](https://issues.apache.org/jira/browse/HDFS-9854) | Log cipher suite negotiation more verbosely |  Major | encryption | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4697](https://issues.apache.org/jira/browse/YARN-4697) | NM aggregation thread pool is not bound by limits |  Critical | nodemanager | Haibo Chen | Haibo Chen |
| [HDFS-9843](https://issues.apache.org/jira/browse/HDFS-9843) | Document distcp options required for copying between encrypted locations |  Major | distcp, documentation, encryption | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-9838](https://issues.apache.org/jira/browse/HDFS-9838) | Refactor the excessReplicateMap to a class |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-9734](https://issues.apache.org/jira/browse/HDFS-9734) | Refactoring of checksum failure report related codes |  Major | . | Kai Zheng | Kai Zheng |
| [HADOOP-12824](https://issues.apache.org/jira/browse/HADOOP-12824) | Collect network and disk usage on the node running Windows |  Major | . | Inigo Goiri | Inigo Goiri |
| [YARN-4579](https://issues.apache.org/jira/browse/YARN-4579) | Allow DefaultContainerExecutor container log directory permissions to be configurable |  Major | yarn | Ray Chiang | Ray Chiang |
| [YARN-4720](https://issues.apache.org/jira/browse/YARN-4720) | Skip unnecessary NN operations in log aggregation |  Major | . | Ming Ma | Jun Gong |
| [HADOOP-12841](https://issues.apache.org/jira/browse/HADOOP-12841) | Update s3-related properties in core-default.xml |  Minor | fs/s3 | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9831](https://issues.apache.org/jira/browse/HDFS-9831) | Document webhdfs retry configuration keys introduced by HDFS-5219/HDFS-5122 |  Major | documentation, webhdfs | Xiaoyu Yao | Xiaobing Zhou |
| [HDFS-9710](https://issues.apache.org/jira/browse/HDFS-9710) | Change DN to send block receipt IBRs in batches |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-6622](https://issues.apache.org/jira/browse/MAPREDUCE-6622) | Add capability to set JHS job cache to a task-based limit |  Critical | jobhistoryserver | Ray Chiang | Ray Chiang |
| [YARN-4718](https://issues.apache.org/jira/browse/YARN-4718) | Rename variables in SchedulerNode to reduce ambiguity post YARN-1011 |  Major | scheduler | Karthik Kambatla | Inigo Goiri |
| [HADOOP-12825](https://issues.apache.org/jira/browse/HADOOP-12825) | Log slow name resolutions |  Major | . | Sidharta Seethana | Sidharta Seethana |
| [HDFS-9733](https://issues.apache.org/jira/browse/HDFS-9733) | Refactor DFSClient#getFileChecksum and DataXceiver#blockChecksum |  Major | . | Kai Zheng | Kai Zheng |
| [YARN-4671](https://issues.apache.org/jira/browse/YARN-4671) | There is no need to acquire CS lock when completing a container |  Major | . | MENG DING | MENG DING |
| [HADOOP-12853](https://issues.apache.org/jira/browse/HADOOP-12853) | Change WASB documentation regarding page blob support |  Minor | fs/azure | madhumita chakraborty | madhumita chakraborty |
| [HDFS-9887](https://issues.apache.org/jira/browse/HDFS-9887) | WebHdfs socket timeouts should be configurable |  Major | fs, webhdfs | Austin Donnelly | Austin Donnelly |
| [HADOOP-12859](https://issues.apache.org/jira/browse/HADOOP-12859) | Disable hiding field style checks in class setters |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-9534](https://issues.apache.org/jira/browse/HDFS-9534) | Add CLI command to clear storage policy from a path. |  Major | tools | Chris Nauroth | Xiaobing Zhou |
| [HADOOP-12793](https://issues.apache.org/jira/browse/HADOOP-12793) | Write a new group mapping service guide |  Major | documentation | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12470](https://issues.apache.org/jira/browse/HADOOP-12470) | In-page TOC of documentation should be automatically generated by doxia macro |  Major | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-9889](https://issues.apache.org/jira/browse/HDFS-9889) | Update balancer/mover document about HDFS-6133 feature |  Minor | . | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-6648](https://issues.apache.org/jira/browse/MAPREDUCE-6648) | Add yarn.app.mapreduce.am.log.level to mapred-default.xml |  Trivial | documentation | Harsh J | Harsh J |
| [HDFS-9521](https://issues.apache.org/jira/browse/HDFS-9521) | TransferFsImage.receiveFile should account and log separate times for image download and fsync to disk |  Minor | . | Wellington Chevreuil | Wellington Chevreuil |
| [HDFS-9906](https://issues.apache.org/jira/browse/HDFS-9906) | Remove spammy log spew when a datanode is restarted |  Major | namenode | Elliott Clark | Brahma Reddy Battula |
| [HADOOP-12901](https://issues.apache.org/jira/browse/HADOOP-12901) | Add warning log when KMSClientProvider cannot create a connection to the KMS server |  Minor | . | Xiao Chen | Xiao Chen |
| [HADOOP-12789](https://issues.apache.org/jira/browse/HADOOP-12789) | log classpath of ApplicationClassLoader at INFO level |  Minor | util | Sangjin Lee | Sangjin Lee |
| [HDFS-9882](https://issues.apache.org/jira/browse/HDFS-9882) | Add heartbeatsTotal in Datanode metrics |  Minor | datanode | Hua Liu | Hua Liu |
| [HADOOP-12860](https://issues.apache.org/jira/browse/HADOOP-12860) | Expand section "Data Encryption on HTTP" in SecureMode documentation |  Minor | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4465](https://issues.apache.org/jira/browse/YARN-4465) | SchedulerUtils#validateRequest for Label check should happen only when nodelabel enabled |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-12904](https://issues.apache.org/jira/browse/HADOOP-12904) | Update Yetus to 0.2.0 |  Blocker | build | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12798](https://issues.apache.org/jira/browse/HADOOP-12798) | Update changelog and release notes (2016-03-04) |  Major | documentation | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12905](https://issues.apache.org/jira/browse/HADOOP-12905) | Clean up CHANGES.txt RAT exclusions from pom.xml files. |  Trivial | build | Chris Nauroth | Chris Nauroth |
| [HDFS-9927](https://issues.apache.org/jira/browse/HDFS-9927) | Document the new OIV ReverseXML processor |  Minor | documentation | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-11404](https://issues.apache.org/jira/browse/HADOOP-11404) | Clarify the "expected client Kerberos principal is null" authorization message |  Minor | security | Stephen Chu | Stephen Chu |
| [HDFS-9942](https://issues.apache.org/jira/browse/HDFS-9942) | Add an HTrace span when refreshing the groups for a username |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-9941](https://issues.apache.org/jira/browse/HDFS-9941) | Do not log StandbyException on NN, other minor logging fixes |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-4719](https://issues.apache.org/jira/browse/YARN-4719) | Add a helper library to maintain node state and allows common queries |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-12923](https://issues.apache.org/jira/browse/HADOOP-12923) | Move the test code in ipc.Client to test |  Minor | ipc | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-9928](https://issues.apache.org/jira/browse/HDFS-9928) | Make HDFS commands guide up to date |  Major | documentation | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4560](https://issues.apache.org/jira/browse/YARN-4560) | Make scheduler error checking message more user friendly |  Trivial | yarn | Ray Chiang | Ray Chiang |
| [HDFS-9579](https://issues.apache.org/jira/browse/HDFS-9579) | Provide bytes-read-by-network-distance metrics at FileSystem.Statistics level |  Major | . | Ming Ma | Ming Ma |
| [YARN-4732](https://issues.apache.org/jira/browse/YARN-4732) | \*ProcessTree classes have too many whitespace issues |  Trivial | . | Karthik Kambatla | Gabor Liptak |
| [HDFS-9405](https://issues.apache.org/jira/browse/HDFS-9405) | Warmup NameNode EDEK caches in background thread |  Major | encryption, namenode | Zhe Zhang | Xiao Chen |
| [HDFS-9951](https://issues.apache.org/jira/browse/HDFS-9951) | Use string constants for XML tags in OfflineImageReconstructor |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-12952](https://issues.apache.org/jira/browse/HADOOP-12952) | /BUILDING example of zero-docs dist should skip javadocs |  Trivial | build, documentation | Steve Loughran | Steve Loughran |
| [HADOOP-12947](https://issues.apache.org/jira/browse/HADOOP-12947) | Update documentation Hadoop Groups Mapping to add static group mapping, negative cache |  Minor | documentation, security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4117](https://issues.apache.org/jira/browse/YARN-4117) | End to end unit test with mini YARN cluster for AMRMProxy Service |  Major | nodemanager, resourcemanager | Kishore Chaliparambil | Giovanni Matteo Fumarola |
| [HADOOP-10965](https://issues.apache.org/jira/browse/HADOOP-10965) | Print fully qualified path in CommandWithDestination error messages |  Minor | . | André Kelpe | John Zhuge |
| [YARN-4436](https://issues.apache.org/jira/browse/YARN-4436) | DistShell ApplicationMaster.ExecBatScripStringtPath is misspelled |  Trivial | applications/distributed-shell | Daniel Templeton | Matt LaMantia |
| [MAPREDUCE-6663](https://issues.apache.org/jira/browse/MAPREDUCE-6663) | [NNBench] Refactor nnbench as a Tool implementation. |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12886](https://issues.apache.org/jira/browse/HADOOP-12886) | Exclude weak ciphers in SSLFactory through ssl-server.xml |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4884](https://issues.apache.org/jira/browse/YARN-4884) | Fix missing documentation about rmadmin command regarding node labels |  Minor | . | Kai Sasaki | Kai Sasaki |
| [HADOOP-12916](https://issues.apache.org/jira/browse/HADOOP-12916) | Allow RPC scheduler/callqueue backoff using response times |  Major | ipc | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-12950](https://issues.apache.org/jira/browse/HADOOP-12950) | ShutdownHookManager should have a timeout for each of the Registered shutdown hook |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11661](https://issues.apache.org/jira/browse/HADOOP-11661) | Deprecate FileUtil#copyMerge |  Major | util | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-11687](https://issues.apache.org/jira/browse/HADOOP-11687) | Ignore x-\* and response headers when copying an Amazon S3 object |  Major | fs/s3 | Denis Jannot | Harsh J |
| [YARN-4607](https://issues.apache.org/jira/browse/YARN-4607) | Pagination support for AppAttempt page TotalOutstandingResource Requests table |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-11212](https://issues.apache.org/jira/browse/HADOOP-11212) | NetUtils.wrapException to handle SocketException explicitly |  Major | util | Steve Loughran | Steve Loughran |
| [HADOOP-12672](https://issues.apache.org/jira/browse/HADOOP-12672) | RPC timeout should not override IPC ping interval |  Major | ipc | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-10235](https://issues.apache.org/jira/browse/HDFS-10235) | [NN UI] Last contact for Live Nodes should be relative time |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4769](https://issues.apache.org/jira/browse/YARN-4769) | Add support for CSRF header in the dump capacity scheduler logs and kill app buttons in RM web UI |  Major | . | Varun Vasudev | Varun Vasudev |
| [HADOOP-12984](https://issues.apache.org/jira/browse/HADOOP-12984) | Add GenericTestUtils.getTestDir method and use it for temporary directory in tests |  Major | build, test | Steve Loughran | Steve Loughran |
| [HADOOP-8813](https://issues.apache.org/jira/browse/HADOOP-8813) | RPC Server and Client classes need InterfaceAudience and InterfaceStability annotations |  Trivial | ipc | Brandon Li | Brandon Li |
| [YARN-4756](https://issues.apache.org/jira/browse/YARN-4756) | Unnecessary wait in Node Status Updater during reboot |  Major | . | Eric Badger | Eric Badger |
| [HDFS-10266](https://issues.apache.org/jira/browse/HDFS-10266) | Remove unused properties dfs.client.file-block-storage-locations.num-threads and dfs.client.file-block-storage-locations.timeout.millis |  Minor | . | Ray Chiang | Ray Chiang |
| [HADOOP-12951](https://issues.apache.org/jira/browse/HADOOP-12951) | Improve documentation on KMS ACLs and delegation tokens |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-12994](https://issues.apache.org/jira/browse/HADOOP-12994) | Specify PositionedReadable, add contract tests, fix problems |  Major | fs | Steve Loughran | Steve Loughran |
| [YARN-4630](https://issues.apache.org/jira/browse/YARN-4630) | Remove useless boxing/unboxing code |  Minor | yarn | Kousuke Saruta | Kousuke Saruta |
| [HDFS-10279](https://issues.apache.org/jira/browse/HDFS-10279) | Improve validation of the configured number of tolerated failed volumes |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-4541](https://issues.apache.org/jira/browse/YARN-4541) | Change log message in LocalizedResource#handle() to DEBUG |  Minor | . | Ray Chiang | Ray Chiang |
| [HADOOP-12822](https://issues.apache.org/jira/browse/HADOOP-12822) | Change "Metrics intern cache overflow" log level from WARN to INFO |  Minor | metrics | Akira Ajisaka | Andras Bokor |
| [HADOOP-12969](https://issues.apache.org/jira/browse/HADOOP-12969) | Mark IPC.Client and IPC.Server as @Public, @Evolving |  Minor | ipc | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-12963](https://issues.apache.org/jira/browse/HADOOP-12963) | Allow using path style addressing for accessing the s3 endpoint |  Minor | fs/s3 | Andrew Baptist | Stephen Montgomery |
| [HDFS-10280](https://issues.apache.org/jira/browse/HDFS-10280) | Document new dfsadmin command -evictWriters |  Minor | documentation | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-10292](https://issues.apache.org/jira/browse/HDFS-10292) | Add block id when client got Unable to close file exception |  Minor | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9412](https://issues.apache.org/jira/browse/HDFS-9412) | getBlocks occupies FSLock and takes too long to complete |  Major | . | He Tianyi | He Tianyi |
| [HDFS-10302](https://issues.apache.org/jira/browse/HDFS-10302) | BlockPlacementPolicyDefault should use default replication considerload value |  Trivial | . | Yiqun Lin | Yiqun Lin |
| [HDFS-10264](https://issues.apache.org/jira/browse/HDFS-10264) | Logging improvements in FSImageFormatProtobuf.Saver |  Major | namenode | Konstantin Shvachko | Xiaobing Zhou |
| [HADOOP-12985](https://issues.apache.org/jira/browse/HADOOP-12985) | Support MetricsSource interface for DecayRpcScheduler Metrics |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-9894](https://issues.apache.org/jira/browse/HDFS-9894) | Add unsetStoragePolicy API to FileContext/AbstractFileSystem and derivatives |  Major | . | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-12891](https://issues.apache.org/jira/browse/HADOOP-12891) | S3AFileSystem should configure Multipart Copy threshold and chunk size |  Major | fs/s3 | Andrew Olson | Andrew Olson |
| [HADOOP-13033](https://issues.apache.org/jira/browse/HADOOP-13033) | Add missing Javadoc enries to Interns.java |  Minor | metrics | Andras Bokor | Andras Bokor |
| [HDFS-10298](https://issues.apache.org/jira/browse/HDFS-10298) | Document the usage of distcp -diff option |  Major | distcp, documentation | Akira Ajisaka | Takashi Ohnishi |
| [YARN-1297](https://issues.apache.org/jira/browse/YARN-1297) | FairScheduler: Move some logs to debug and check if debug logging is enabled |  Major | fairscheduler | Sandy Ryza | Yufei Gu |
| [HADOOP-13039](https://issues.apache.org/jira/browse/HADOOP-13039) | Add documentation for configuration property ipc.maximum.data.length for controlling maximum RPC message size. |  Major | documentation | Chris Nauroth | Mingliang Liu |
| [HDFS-10330](https://issues.apache.org/jira/browse/HDFS-10330) | Add Corrupt Blocks Information in Metasave Output |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-5470](https://issues.apache.org/jira/browse/HADOOP-5470) | RunJar.unJar() should write the last modified time found in the jar entry to the uncompressed file |  Minor | util | Colin Evans | Andras Bokor |
| [HDFS-3702](https://issues.apache.org/jira/browse/HDFS-3702) | Add an option for NOT writing the blocks locally if there is a datanode on the same box as the client |  Minor | hdfs-client | Nicolas Liochon | Lei (Eddy) Xu |
| [HDFS-10297](https://issues.apache.org/jira/browse/HDFS-10297) | Increase default balance bandwidth and concurrent moves |  Minor | balancer & mover | John Zhuge | John Zhuge |
| [HADOOP-12957](https://issues.apache.org/jira/browse/HADOOP-12957) | Limit the number of outstanding async calls |  Major | ipc | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-5003](https://issues.apache.org/jira/browse/YARN-5003) | Add container resource to RM audit log |  Major | resourcemanager, scheduler | Nathan Roberts | Nathan Roberts |
| [HDFS-9902](https://issues.apache.org/jira/browse/HDFS-9902) | Support different values of dfs.datanode.du.reserved per storage type |  Major | datanode | Pan Yuxuan | Brahma Reddy Battula |
| [HADOOP-13068](https://issues.apache.org/jira/browse/HADOOP-13068) | Clean up RunJar and related test class |  Trivial | util | Andras Bokor | Andras Bokor |
| [HADOOP-13103](https://issues.apache.org/jira/browse/HADOOP-13103) | Group resolution from LDAP may fail on javax.naming.ServiceUnavailableException |  Minor | security | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-12866](https://issues.apache.org/jira/browse/HADOOP-12866) | add a subcommand for gridmix |  Major | tools | Allen Wittenauer | Kai Sasaki |
| [MAPREDUCE-6678](https://issues.apache.org/jira/browse/MAPREDUCE-6678) | Allow ShuffleHandler readahead without drop-behind |  Major | nodemanager | Nathan Roberts | Nathan Roberts |
| [HADOOP-12982](https://issues.apache.org/jira/browse/HADOOP-12982) | Document missing S3A and S3 properties |  Minor | documentation, fs/s3, tools | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-10172](https://issues.apache.org/jira/browse/HDFS-10172) | hdfs erasurecode command should remove the redundant -usage option |  Minor | erasure-coding | Wei-Chiu Chuang | Yuanbo Liu |
| [YARN-4995](https://issues.apache.org/jira/browse/YARN-4995) | FairScheduler: Display per-queue demand on the scheduler page |  Minor | . | xupeng | xupeng |
| [YARN-4577](https://issues.apache.org/jira/browse/YARN-4577) | Enable aux services to have their own custom classpath/jar file |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5053](https://issues.apache.org/jira/browse/YARN-5053) | More informative diagnostics when applications killed by a user |  Major | resourcemanager | Jason Lowe | Eric Badger |
| [HADOOP-13142](https://issues.apache.org/jira/browse/HADOOP-13142) | Change project version from 3.0.0 to 3.0.0-alpha1 |  Blocker | . | Andrew Wang | Andrew Wang |
| [HADOOP-12868](https://issues.apache.org/jira/browse/HADOOP-12868) | Fix hadoop-openstack undeclared and unused dependencies |  Major | tools | Allen Wittenauer | Masatake Iwasaki |
| [HADOOP-12971](https://issues.apache.org/jira/browse/HADOOP-12971) | FileSystemShell doc should explain relative path |  Critical | documentation | John Zhuge | John Zhuge |
| [YARN-5068](https://issues.apache.org/jira/browse/YARN-5068) | Expose scheduler queue to application master |  Major | . | Harish Jaiprakash | Harish Jaiprakash |
| [HADOOP-13148](https://issues.apache.org/jira/browse/HADOOP-13148) | TestDistCpViewFs to include IOExceptions in test error reports |  Minor | tools/distcp | Steve Loughran | Steve Loughran |
| [HADOOP-13146](https://issues.apache.org/jira/browse/HADOOP-13146) | Refactor RetryInvocationHandler |  Minor | io | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-13161](https://issues.apache.org/jira/browse/HADOOP-13161) | remove JDK7 from Dockerfile |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [HDFS-9732](https://issues.apache.org/jira/browse/HDFS-9732) | Improve DelegationTokenIdentifier.toString() for better logging |  Major | . | Steve Loughran | Yongjun Zhang |
| [HDFS-10383](https://issues.apache.org/jira/browse/HDFS-10383) | Safely close resources in DFSTestUtil |  Major | test | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-6686](https://issues.apache.org/jira/browse/MAPREDUCE-6686) | Add a way to download the job config from the mapred CLI |  Major | client | Robert Kanter | Robert Kanter |
| [HADOOP-12782](https://issues.apache.org/jira/browse/HADOOP-12782) | Faster LDAP group name resolution with ActiveDirectory |  Major | security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4002](https://issues.apache.org/jira/browse/YARN-4002) | make ResourceTrackerService.nodeHeartbeat more concurrent |  Critical | . | Hong Zhiguo | Hong Zhiguo |
| [HDFS-10417](https://issues.apache.org/jira/browse/HDFS-10417) | Improve error message from checkBlockLocalPathAccess |  Minor | datanode | Tianyin Xu | Tianyin Xu |
| [HADOOP-13168](https://issues.apache.org/jira/browse/HADOOP-13168) | Support Future.get with timeout in ipc async calls |  Major | ipc | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-6696](https://issues.apache.org/jira/browse/MAPREDUCE-6696) | Add a configuration to limit the number of map tasks allowed per job. |  Major | job submission | zhihai xu | zhihai xu |
| [HADOOP-13145](https://issues.apache.org/jira/browse/HADOOP-13145) | In DistCp, prevent unnecessary getFileStatus call when not preserving metadata. |  Major | tools/distcp | Chris Nauroth | Chris Nauroth |
| [HADOOP-13112](https://issues.apache.org/jira/browse/HADOOP-13112) | Change CredentialShell to use CommandShell base class |  Minor | . | Matthew Paduano | Matthew Paduano |
| [YARN-4878](https://issues.apache.org/jira/browse/YARN-4878) | Expose scheduling policy and max running apps over JMX for Yarn queues |  Major | yarn | Yufei Gu | Yufei Gu |
| [HDFS-9782](https://issues.apache.org/jira/browse/HDFS-9782) | RollingFileSystemSink should have configurable roll interval |  Major | . | Daniel Templeton | Daniel Templeton |
| [HADOOP-13198](https://issues.apache.org/jira/browse/HADOOP-13198) | Add support for OWASP's dependency-check |  Minor | build, security | Mike Yoder | Mike Yoder |
| [HDFS-10217](https://issues.apache.org/jira/browse/HDFS-10217) | show 'blockScheduled' tooltip in datanodes table. |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-13160](https://issues.apache.org/jira/browse/HADOOP-13160) | Suppress checkstyle JavadocPackage check for test source |  Minor | . | John Zhuge | John Zhuge |
| [YARN-4766](https://issues.apache.org/jira/browse/YARN-4766) | NM should not aggregate logs older than the retention policy |  Major | log-aggregation, nodemanager | Haibo Chen | Haibo Chen |
| [HADOOP-12911](https://issues.apache.org/jira/browse/HADOOP-12911) | Upgrade Hadoop MiniKDC with Kerby |  Major | test | Jiajia Li | Jiajia Li |
| [HADOOP-13199](https://issues.apache.org/jira/browse/HADOOP-13199) | Add doc for distcp -filters |  Trivial | documentation | John Zhuge | John Zhuge |
| [HADOOP-13193](https://issues.apache.org/jira/browse/HADOOP-13193) | Upgrade to Apache Yetus 0.3.0 |  Major | documentation, test | Allen Wittenauer | Kengo Seki |
| [HDFS-10341](https://issues.apache.org/jira/browse/HDFS-10341) | Add a metric to expose the timeout number of pending replication blocks |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-13109](https://issues.apache.org/jira/browse/HADOOP-13109) | Add ability to edit existing token file via dtutil -alias flag |  Minor | security | Matthew Paduano | Matthew Paduano |
| [HADOOP-13105](https://issues.apache.org/jira/browse/HADOOP-13105) | Support timeouts in LDAP queries in LdapGroupsMapping. |  Major | security | Chris Nauroth | Mingliang Liu |
| [HADOOP-12807](https://issues.apache.org/jira/browse/HADOOP-12807) | S3AFileSystem should read AWS credentials from environment variables |  Minor | fs/s3 | Tobin Baker | Tobin Baker |
| [MAPREDUCE-5044](https://issues.apache.org/jira/browse/MAPREDUCE-5044) | Have AM trigger jstack on task attempts that timeout before killing them |  Major | mr-am | Jason Lowe | Eric Payne |
| [HADOOP-10048](https://issues.apache.org/jira/browse/HADOOP-10048) | LocalDirAllocator should avoid holding locks while accessing the filesystem |  Major | . | Jason Lowe | Jason Lowe |
| [HADOOP-13220](https://issues.apache.org/jira/browse/HADOOP-13220) | Follow on fixups after upgraded mini-kdc using Kerby |  Major | test | Jiajia Li | Jiajia Li |
| [MAPREDUCE-6714](https://issues.apache.org/jira/browse/MAPREDUCE-6714) | Refactor UncompressedSplitLineReader.fillBuffer() |  Major | . | Daniel Templeton | Daniel Templeton |
| [HADOOP-12291](https://issues.apache.org/jira/browse/HADOOP-12291) | Add support for nested groups in LdapGroupsMapping |  Major | security | Gautam Gopalakrishnan | Esther Kundin |
| [HDFS-7541](https://issues.apache.org/jira/browse/HDFS-7541) | Upgrade Domains in HDFS |  Major | . | Ming Ma | Ming Ma |
| [HDFS-10256](https://issues.apache.org/jira/browse/HDFS-10256) | Use GenericTestUtils.getTestDir method in tests for temporary directories |  Major | build, test | Vinayakumar B | Vinayakumar B |
| [YARN-5083](https://issues.apache.org/jira/browse/YARN-5083) | YARN CLI for AM logs does not give any error message if entered invalid am value |  Major | yarn | Sumana Sathish | Jian He |
| [HADOOP-12943](https://issues.apache.org/jira/browse/HADOOP-12943) | Add -w -r options in dfs -test command |  Major | fs, scripts, tools | Weiwei Yang | Weiwei Yang |
| [HDFS-10493](https://issues.apache.org/jira/browse/HDFS-10493) | Add links to datanode web UI in namenode datanodes page |  Major | namenode, ui | Weiwei Yang | Weiwei Yang |
| [YARN-5082](https://issues.apache.org/jira/browse/YARN-5082) | Limit ContainerId increase in fair scheduler if the num of  node app reserved reached the limit |  Major | . | sandflee | sandflee |
| [YARN-4958](https://issues.apache.org/jira/browse/YARN-4958) | The file localization process should allow for wildcards to reduce the application footprint in the state store |  Critical | nodemanager | Daniel Templeton | Daniel Templeton |
| [HADOOP-13296](https://issues.apache.org/jira/browse/HADOOP-13296) | Cleanup javadoc for Path |  Minor | documentation | Daniel Templeton | Daniel Templeton |
| [MAPREDUCE-6719](https://issues.apache.org/jira/browse/MAPREDUCE-6719) | The list of -libjars archives should be replaced with a wildcard in the distributed cache to reduce the application footprint in the state store |  Critical | distributed-cache | Daniel Templeton | Daniel Templeton |
| [HDFS-7597](https://issues.apache.org/jira/browse/HDFS-7597) | DelegationTokenIdentifier should cache the TokenIdentifier to UGI mapping |  Critical | webhdfs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-6721](https://issues.apache.org/jira/browse/MAPREDUCE-6721) | mapreduce.reduce.shuffle.memory.limit.percent=0.0 should be legal to enforce shuffle to disk |  Major | mrv2, task | Gera Shegalov | Gera Shegalov |
| [YARN-5278](https://issues.apache.org/jira/browse/YARN-5278) | Remove unused argument in TestRMWebServicesForCSWithPartitions#setupQueueConfiguration |  Minor | test | Akira Ajisaka | Tao Jie |
| [HADOOP-13307](https://issues.apache.org/jira/browse/HADOOP-13307) | add rsync to Dockerfile so that precommit archive works |  Trivial | build | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13067](https://issues.apache.org/jira/browse/HADOOP-13067) | cleanup the dockerfile |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13227](https://issues.apache.org/jira/browse/HADOOP-13227) | AsyncCallHandler should use an event driven architecture to handle async calls |  Major | io, ipc | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-13263](https://issues.apache.org/jira/browse/HADOOP-13263) | Reload cached groups in background after expiry |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-10440](https://issues.apache.org/jira/browse/HDFS-10440) | Improve DataNode web UI |  Major | datanode, ui | Weiwei Yang | Weiwei Yang |
| [HADOOP-13034](https://issues.apache.org/jira/browse/HADOOP-13034) | Log message about input options in distcp lacks some items |  Minor | tools/distcp | Takashi Ohnishi | Takashi Ohnishi |
| [HADOOP-13235](https://issues.apache.org/jira/browse/HADOOP-13235) | Use Date and Time API in KafkaSink |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10568](https://issues.apache.org/jira/browse/HDFS-10568) | Reuse ObjectMapper instance in CombinedHostsFileReader and CombinedHostsFileWriter |  Major | hdfs-client | Yiqun Lin | Yiqun Lin |
| [HADOOP-13337](https://issues.apache.org/jira/browse/HADOOP-13337) | Update maven-enforcer-plugin version to 1.4.1 |  Major | build | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-4568](https://issues.apache.org/jira/browse/YARN-4568) | Fix message when NodeManager runs into errors initializing the recovery directory |  Major | yarn | Ray Chiang | Ray Chiang |
| [HDFS-9805](https://issues.apache.org/jira/browse/HDFS-9805) | TCP\_NODELAY not set before SASL handshake in data transfer pipeline |  Major | datanode | Gary Helmling | Gary Helmling |
| [HDFS-10582](https://issues.apache.org/jira/browse/HDFS-10582) | Change deprecated configuration fs.checkpoint.dir to dfs.namenode.checkpoint.dir in HDFS Commands Doc |  Minor | documentation | Pan Yuxuan | Pan Yuxuan |
| [HDFS-10488](https://issues.apache.org/jira/browse/HDFS-10488) | Update WebHDFS documentation regarding CREATE and MKDIR default permissions |  Minor | documentation, webhdfs | Wellington Chevreuil | Wellington Chevreuil |
| [MAPREDUCE-6717](https://issues.apache.org/jira/browse/MAPREDUCE-6717) | Remove deprecated StringUtils.getFormattedTimeWithDiff |  Minor | . | Akira Ajisaka | Shen Yinjie |
| [HDFS-10300](https://issues.apache.org/jira/browse/HDFS-10300) | TestDistCpSystem should share MiniDFSCluster |  Trivial | test | John Zhuge | John Zhuge |
| [HDFS-10387](https://issues.apache.org/jira/browse/HDFS-10387) | DataTransferProtocol#writeBlock missing some javadocs |  Minor | datanode, hdfs | Yongjun Zhang | John Zhuge |
| [YARN-5339](https://issues.apache.org/jira/browse/YARN-5339) | passing file to -out for YARN log CLI doesnt give warning or error code |  Major | . | Sumana Sathish | Xuan Gong |
| [HADOOP-13290](https://issues.apache.org/jira/browse/HADOOP-13290) | Appropriate use of generics in FairCallQueue |  Major | ipc | Konstantin Shvachko | Jonathan Hung |
| [YARN-5303](https://issues.apache.org/jira/browse/YARN-5303) | Clean up ContainerExecutor JavaDoc |  Minor | nodemanager | Daniel Templeton | Daniel Templeton |
| [HADOOP-13289](https://issues.apache.org/jira/browse/HADOOP-13289) | Remove unused variables in TestFairCallQueue |  Minor | test | Konstantin Shvachko | Ye Zhou |
| [YARN-5380](https://issues.apache.org/jira/browse/YARN-5380) | NMTimelinePublisher should use getMemorySize instead of getMemory |  Major | timelineserver | Karthik Kambatla | Vrushali C |
| [HDFS-10628](https://issues.apache.org/jira/browse/HDFS-10628) | Log HDFS Balancer exit message to its own log |  Minor | balancer & mover | Jiayi Zhou | Jiayi Zhou |
| [MAPREDUCE-6652](https://issues.apache.org/jira/browse/MAPREDUCE-6652) | Add configuration property to prevent JHS from loading jobs with a task count greater than X |  Major | jobhistoryserver | Haibo Chen | Haibo Chen |
| [YARN-5181](https://issues.apache.org/jira/browse/YARN-5181) | ClusterNodeTracker: add method to get list of nodes matching a specific resourceName |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-13298](https://issues.apache.org/jira/browse/HADOOP-13298) | Fix the leftover L&N files in hadoop-build-tools/src/main/resources/META-INF/ |  Minor | . | Xiao Chen | Tsuyoshi Ozawa |
| [YARN-4996](https://issues.apache.org/jira/browse/YARN-4996) | Make TestNMReconnect.testCompareRMNodeAfterReconnect() scheduler agnostic, or better yet parameterized |  Minor | resourcemanager, test | Daniel Templeton | Kai Sasaki |
| [HDFS-10647](https://issues.apache.org/jira/browse/HDFS-10647) | Add a link to HDFS disk balancer document in site.xml |  Major | documentation | Akira Ajisaka | Yiqun Lin |
| [MAPREDUCE-6365](https://issues.apache.org/jira/browse/MAPREDUCE-6365) | Refactor JobResourceUploader#uploadFilesInternal |  Minor | . | Chris Trezzo | Chris Trezzo |
| [YARN-4883](https://issues.apache.org/jira/browse/YARN-4883) | Make consistent operation name in AdminService |  Minor | resourcemanager | Kai Sasaki | Kai Sasaki |
| [HADOOP-13383](https://issues.apache.org/jira/browse/HADOOP-13383) | Update release notes for 3.0.0-alpha1 |  Blocker | documentation | Andrew Wang | Andrew Wang |
| [YARN-1126](https://issues.apache.org/jira/browse/YARN-1126) | Add validation of users input nodes-states options to nodes CLI |  Major | . | Wei Yan | Wei Yan |
| [HDFS-10287](https://issues.apache.org/jira/browse/HDFS-10287) | MiniDFSCluster should implement AutoCloseable |  Minor | test | John Zhuge | Andras Bokor |
| [HDFS-10225](https://issues.apache.org/jira/browse/HDFS-10225) | DataNode hot swap drives should disallow storage type changes. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-10660](https://issues.apache.org/jira/browse/HDFS-10660) | Expose storage policy apis via HDFSAdmin interface |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-13272](https://issues.apache.org/jira/browse/HADOOP-13272) | ViewFileSystem should support storage policy related API |  Major | fs, viewfs | Peter Shi | Peter Shi |
| [HDFS-9937](https://issues.apache.org/jira/browse/HDFS-9937) | Update dfsadmin command line help and HdfsQuotaAdminGuide |  Minor | . | Wei-Chiu Chuang | Kai Sasaki |
| [HDFS-10667](https://issues.apache.org/jira/browse/HDFS-10667) | Report more accurate info about data corruption location |  Major | datanode, hdfs | Yongjun Zhang | Yuanbo Liu |
| [HADOOP-13354](https://issues.apache.org/jira/browse/HADOOP-13354) | Update WASB driver to use the latest version (4.2.0) of SDK for Microsoft Azure Storage Clients |  Major | fs/azure | Sivaguru Sankaridurg | Sivaguru Sankaridurg |
| [HDFS-10519](https://issues.apache.org/jira/browse/HDFS-10519) | Add a configuration option to enable in-progress edit log tailing |  Minor | ha | Jiayi Zhou | Jiayi Zhou |
| [HDFS-10676](https://issues.apache.org/jira/browse/HDFS-10676) | Add namenode metric to measure time spent in generating EDEKs |  Major | namenode | Hanisha Koneru | Hanisha Koneru |
| [MAPREDUCE-6746](https://issues.apache.org/jira/browse/MAPREDUCE-6746) | Replace org.apache.commons.io.Charsets with java.nio.charset.StandardCharsets |  Minor | . | Vincent Poon | Vincent Poon |
| [HDFS-10703](https://issues.apache.org/jira/browse/HDFS-10703) | HA NameNode Web UI should show last checkpoint time |  Minor | ui | John Zhuge | John Zhuge |
| [MAPREDUCE-6729](https://issues.apache.org/jira/browse/MAPREDUCE-6729) | Accurately compute the test execute time in DFSIO |  Minor | benchmarks, performance, test | mingleizhang | mingleizhang |
| [HADOOP-13444](https://issues.apache.org/jira/browse/HADOOP-13444) | Replace org.apache.commons.io.Charsets with java.nio.charset.StandardCharsets |  Minor | . | Vincent Poon | Vincent Poon |
| [HADOOP-13081](https://issues.apache.org/jira/browse/HADOOP-13081) | add the ability to create multiple UGIs/subjects from one kerberos login |  Major | security | Sergey Shelukhin | Sergey Shelukhin |
| [YARN-5460](https://issues.apache.org/jira/browse/YARN-5460) | Change container runtime type logging in DelegatingLinuxContainerRuntime to debug |  Trivial | yarn | Shane Kumpf | Shane Kumpf |
| [HADOOP-13458](https://issues.apache.org/jira/browse/HADOOP-13458) | LoadBalancingKMSClientProvider#doOp should log IOException stacktrace |  Trivial | kms | Wei-Chiu Chuang | Chen Liang |
| [MAPREDUCE-6730](https://issues.apache.org/jira/browse/MAPREDUCE-6730) | Use StandardCharsets instead of String overload in TextOutputFormat |  Minor | . | Sahil Kang | Sahil Kang |
| [HDFS-10707](https://issues.apache.org/jira/browse/HDFS-10707) | Replace org.apache.commons.io.Charsets with java.nio.charset.StandardCharsets |  Minor | . | Vincent Poon | Vincent Poon |
| [HADOOP-13442](https://issues.apache.org/jira/browse/HADOOP-13442) | Optimize UGI group lookups |  Major | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-13466](https://issues.apache.org/jira/browse/HADOOP-13466) | Add an AutoCloseableLock class |  Major | . | Chen Liang | Chen Liang |
| [MAPREDUCE-6748](https://issues.apache.org/jira/browse/MAPREDUCE-6748) | Enhance logging for Cluster.java around InetSocketAddress |  Minor | . | sarun singla | Vrushali C |
| [YARN-4910](https://issues.apache.org/jira/browse/YARN-4910) | Fix incomplete log info in ResourceLocalizationService |  Trivial | . | Jun Gong | Jun Gong |
| [HADOOP-13380](https://issues.apache.org/jira/browse/HADOOP-13380) | TestBasicDiskValidator should not write data to /tmp |  Minor | . | Lei (Eddy) Xu | Yufei Gu |
| [MAPREDUCE-6741](https://issues.apache.org/jira/browse/MAPREDUCE-6741) | add MR support to redact job conf properties |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [YARN-5483](https://issues.apache.org/jira/browse/YARN-5483) | Optimize RMAppAttempt#pullJustFinishedContainers |  Major | . | sandflee | sandflee |
| [HDFS-10677](https://issues.apache.org/jira/browse/HDFS-10677) | Über-jira: Enhancements to NNThroughputBenchmark tool |  Major | benchmarks, tools | Mingliang Liu | Mingliang Liu |
| [HDFS-10342](https://issues.apache.org/jira/browse/HDFS-10342) | BlockManager#createLocatedBlocks should not check corrupt replicas if none are corrupt |  Major | hdfs | Daryn Sharp | Kuhu Shukla |
| [MAPREDUCE-6751](https://issues.apache.org/jira/browse/MAPREDUCE-6751) | Add debug log message when splitting is not possible due to unsplittable compression |  Minor | client, mrv1, mrv2 | Peter Vary | Peter Vary |
| [YARN-5455](https://issues.apache.org/jira/browse/YARN-5455) | Update Javadocs for LinuxContainerExecutor |  Major | nodemanager | Daniel Templeton | Daniel Templeton |
| [YARN-4702](https://issues.apache.org/jira/browse/YARN-4702) | FairScheduler: Allow setting maxResources for ad hoc queues |  Major | fairscheduler | Karthik Kambatla | Daniel Templeton |
| [HADOOP-13503](https://issues.apache.org/jira/browse/HADOOP-13503) | Improve SaslRpcClient failure logging |  Major | security | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-13527](https://issues.apache.org/jira/browse/HADOOP-13527) | Add Spark to CallerContext LimitedPrivate scope |  Minor | ipc | Weiqing Yang | Weiqing Yang |
| [HDFS-10645](https://issues.apache.org/jira/browse/HDFS-10645) | Make block report size as a metric and add this metric to datanode web ui |  Major | datanode, ui | Yuanbo Liu | Yuanbo Liu |
| [MAPREDUCE-6587](https://issues.apache.org/jira/browse/MAPREDUCE-6587) | Remove unused params in connection-related methods of Fetcher |  Minor | . | Yiqun Lin | Yiqun Lin |
| [YARN-4491](https://issues.apache.org/jira/browse/YARN-4491) | yarn list command to support filtering by tags |  Minor | client | Steve Loughran | Varun Saxena |
| [HADOOP-13538](https://issues.apache.org/jira/browse/HADOOP-13538) | Deprecate getInstance and initialize methods with Path in TrashPolicy |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-8986](https://issues.apache.org/jira/browse/HDFS-8986) | Add option to -du to calculate directory space usage excluding snapshots |  Major | snapshots | Gautam Gopalakrishnan | Xiao Chen |
| [HDFS-10795](https://issues.apache.org/jira/browse/HDFS-10795) | Fix an error in ReaderStrategy#ByteBufferStrategy |  Major | . | SammiChen | SammiChen |
| [HDFS-10798](https://issues.apache.org/jira/browse/HDFS-10798) | Make the threshold of reporting FSNamesystem lock contention configurable |  Major | logging, namenode | Zhe Zhang | Erik Krogen |
| [HDFS-10814](https://issues.apache.org/jira/browse/HDFS-10814) | Add assertion for getNumEncryptionZones when no EZ is created |  Minor | test | Vinitha Reddy Gankidi | Vinitha Reddy Gankidi |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2299](https://issues.apache.org/jira/browse/HDFS-2299) | TestOfflineEditsViewer is failing on trunk |  Major | test | Aaron T. Myers | Uma Maheswara Rao G |
| [HDFS-2310](https://issues.apache.org/jira/browse/HDFS-2310) | TestBackupNode fails since HADOOP-7524 went in. |  Major | test | Ivan Kelly | Ivan Kelly |
| [HDFS-2313](https://issues.apache.org/jira/browse/HDFS-2313) | Rat excludes has a typo for excluding editsStored files |  Major | test | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-2978](https://issues.apache.org/jira/browse/MAPREDUCE-2978) | hudson findbugs not reporting properly |  Major | mrv2 | Thomas Graves | Tom White |
| [HDFS-46](https://issues.apache.org/jira/browse/HDFS-46) | The namespace quota of root directory should not be Integer.MAX\_VALUE |  Major | namenode | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HDFS-2373](https://issues.apache.org/jira/browse/HDFS-2373) | Commands using webhdfs and hftp print unnecessary debug information on the console with security enabled |  Major | webhdfs | Arpit Gupta | Arpit Gupta |
| [HADOOP-6220](https://issues.apache.org/jira/browse/HADOOP-6220) | HttpServer wraps InterruptedExceptions by IOExceptions if interrupted in startup |  Minor | util | Steve Loughran | Steve Loughran |
| [MAPREDUCE-3149](https://issues.apache.org/jira/browse/MAPREDUCE-3149) | add a test to verify that buildDTAuthority works for cases with no authority. |  Major | test | John George | John George |
| [HADOOP-7769](https://issues.apache.org/jira/browse/HADOOP-7769) | TestJMXJsonServlet is failing |  Major | test | Tom White | Tom White |
| [HADOOP-8177](https://issues.apache.org/jira/browse/HADOOP-8177) | MBeans shouldn't try to register when it fails to create MBeanName |  Minor | metrics | Devaraj K | Devaraj K |
| [HDFS-3116](https://issues.apache.org/jira/browse/HDFS-3116) | Typo in fetchdt error message |  Trivial | hdfs-client | Aaron T. Myers | AOE Takashi |
| [HADOOP-7757](https://issues.apache.org/jira/browse/HADOOP-7757) | Test file reference count is at least 3x actual value |  Major | build | Jonathan Eagles | Jonathan Eagles |
| [HDFS-3265](https://issues.apache.org/jira/browse/HDFS-3265) | PowerPc Build error. |  Major | build | Kumar Ravi | Kumar Ravi |
| [HADOOP-8339](https://issues.apache.org/jira/browse/HADOOP-8339) | jenkins complaining about 16 javadoc warnings |  Major | build | Thomas Graves | Robert Joseph Evans |
| [HADOOP-8354](https://issues.apache.org/jira/browse/HADOOP-8354) | test-patch findbugs may fail if a dependent module is changed |  Major | build | Tom White | Robert Joseph Evans |
| [HADOOP-8395](https://issues.apache.org/jira/browse/HADOOP-8395) | Text shell command unnecessarily demands that a SequenceFile's key class be WritableComparable |  Trivial | util | Harsh J | Harsh J |
| [HDFS-2312](https://issues.apache.org/jira/browse/HDFS-2312) | FSNamesystem header comment says it's for the DN |  Trivial | namenode | Aaron T. Myers | Harsh J |
| [HDFS-3197](https://issues.apache.org/jira/browse/HDFS-3197) | Incorrect class comments in a few tests |  Trivial | test | Aaron T. Myers | Andy Isaacson |
| [HADOOP-8360](https://issues.apache.org/jira/browse/HADOOP-8360) | empty-configuration.xml fails xml validation |  Minor | test | Radim Kolar | Radim Kolar |
| [HADOOP-8413](https://issues.apache.org/jira/browse/HADOOP-8413) | test-patch.sh gives out the wrong links for newPatchFindbugsWarnings |  Minor | precommit | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-6871](https://issues.apache.org/jira/browse/HADOOP-6871) | When the value of a configuration key is set to its unresolved form, it causes the IllegalStateException in Configuration.get() stating that substitution depth is too large. |  Major | conf | Arvind Prabhakar | Arvind Prabhakar |
| [MAPREDUCE-3990](https://issues.apache.org/jira/browse/MAPREDUCE-3990) | MRBench allows Long-sized input-lines value but parses CLI argument as an Integer |  Trivial | benchmarks | Harsh J | Harsh J |
| [HDFS-3462](https://issues.apache.org/jira/browse/HDFS-3462) | TestDFSClientRetries.busyTest() should restore default xceiver count in the config. |  Major | test | Konstantin Shvachko | madhukara phatak |
| [HDFS-3550](https://issues.apache.org/jira/browse/HDFS-3550) | raid added javadoc warnings |  Minor | build | Thomas Graves | Jason Lowe |
| [HDFS-3549](https://issues.apache.org/jira/browse/HDFS-3549) | dist tar build fails in hadoop-hdfs-raid project |  Critical | build | Jason Lowe | Jason Lowe |
| [HADOOP-8548](https://issues.apache.org/jira/browse/HADOOP-8548) | test-patch.sh shows an incorrect link in Jekins builds |  Major | build | Kihwal Lee | Kihwal Lee |
| [HDFS-3614](https://issues.apache.org/jira/browse/HDFS-3614) | Revert unused MiniDFSCluster constructor from HDFS-3049 |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-8584](https://issues.apache.org/jira/browse/HADOOP-8584) | test-patch.sh should not immediately exit when no tests are added or modified |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-8818](https://issues.apache.org/jira/browse/HADOOP-8818) | Should use equals() rather than == to compare String or Text in MD5MD5CRC32FileChecksum and TFileDumper |  Minor | fs, io | Brandon Li | Brandon Li |
| [HADOOP-8821](https://issues.apache.org/jira/browse/HADOOP-8821) | Findbugs warning Configuration.dumpDeprecatedKeys() concatenates strings using + in a loop |  Trivial | conf | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7256](https://issues.apache.org/jira/browse/HADOOP-7256) | Resource leak during failure scenario of closing of resources. |  Minor | util | ramkrishna.s.vasudevan | ramkrishna.s.vasudevan |
| [HADOOP-8436](https://issues.apache.org/jira/browse/HADOOP-8436) | NPE In getLocalPathForWrite ( path, conf ) when the required context item is not configured |  Major | fs | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-8840](https://issues.apache.org/jira/browse/HADOOP-8840) | Fix the test-patch colorizer to cover all sorts of +1 lines. |  Major | build | Harsh J | Harsh J |
| [MAPREDUCE-4685](https://issues.apache.org/jira/browse/MAPREDUCE-4685) | DBCount should not use ACCESS |  Major | examples | Viji | Viji |
| [MAPREDUCE-3223](https://issues.apache.org/jira/browse/MAPREDUCE-3223) | Remove MR1 configs from mapred-default.xml |  Major | documentation, mrv2 | Todd Lipcon | Todd Lipcon |
| [HADOOP-8839](https://issues.apache.org/jira/browse/HADOOP-8839) | test-patch's -1 on @author tag presence doesn't cause a -1 to the overall result |  Trivial | build | Harsh J | Harsh J |
| [MAPREDUCE-4695](https://issues.apache.org/jira/browse/MAPREDUCE-4695) | Fix LocalRunner on trunk after MAPREDUCE-3223 broke it |  Blocker | test | Harsh J | Harsh J |
| [HDFS-3735](https://issues.apache.org/jira/browse/HDFS-3735) | NameNode WebUI should allow sorting live datanode list by fields Block Pool Used, Block Pool Used(%) and Failed Volumes. |  Major | namenode | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-4003](https://issues.apache.org/jira/browse/HDFS-4003) | test-patch should build the common native libs before running hdfs tests |  Major | build | Eli Collins | Colin P. McCabe |
| [MAPREDUCE-4574](https://issues.apache.org/jira/browse/MAPREDUCE-4574) | Fix TotalOrderParitioner to work with non-WritableComparable key types |  Trivial | client | Harsh J | Harsh J |
| [HADOOP-8918](https://issues.apache.org/jira/browse/HADOOP-8918) | dev-support/test-patch.sh is parsing modified files wrong |  Major | build | Raja Aluri | Raja Aluri |
| [HADOOP-8910](https://issues.apache.org/jira/browse/HADOOP-8910) | Add examples to GlobExpander#expand method |  Minor | fs | Suresh Srinivas | Suresh Srinivas |
| [HDFS-2434](https://issues.apache.org/jira/browse/HDFS-2434) | TestNameNodeMetrics.testCorruptBlock fails intermittently |  Major | test | Uma Maheswara Rao G | Jing Zhao |
| [HDFS-4067](https://issues.apache.org/jira/browse/HDFS-4067) | TestUnderReplicatedBlocks may fail due to ReplicaAlreadyExistsException |  Major | test | Eli Collins | Jing Zhao |
| [HDFS-4115](https://issues.apache.org/jira/browse/HDFS-4115) | TestHDFSCLI.testAll fails one test due to number format |  Minor | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8974](https://issues.apache.org/jira/browse/HADOOP-8974) | TestDFVariations fails on Windows |  Major | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-9037](https://issues.apache.org/jira/browse/HADOOP-9037) | Bug in test-patch.sh and precommit build process |  Critical | build | Kihwal Lee | Kihwal Lee |
| [HADOOP-9121](https://issues.apache.org/jira/browse/HADOOP-9121) | InodeTree.java has redundant check for vName while throwing exception |  Major | fs | Arup Malakar | Arup Malakar |
| [HDFS-4310](https://issues.apache.org/jira/browse/HDFS-4310) | fix test org.apache.hadoop.hdfs.server.datanode.TestStartSecureDataNode |  Major | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HDFS-4275](https://issues.apache.org/jira/browse/HDFS-4275) | MiniDFSCluster-based tests fail on Windows due to failure to delete test namenode directory |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4338](https://issues.apache.org/jira/browse/HDFS-4338) | TestNameNodeMetrics#testCorruptBlock is flaky |  Major | namenode | Andrew Wang | Andrew Wang |
| [HADOOP-9139](https://issues.apache.org/jira/browse/HADOOP-9139) | improve script hadoop-common-project/hadoop-common/src/test/resources/kdc/killKdc.sh |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-9202](https://issues.apache.org/jira/browse/HADOOP-9202) | test-patch.sh fails during mvn eclipse:eclipse if patch adds a new module to the build |  Major | build | Chris Nauroth | Chris Nauroth |
| [HADOOP-9242](https://issues.apache.org/jira/browse/HADOOP-9242) | Duplicate surefire plugin config in hadoop-common |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HADOOP-9249](https://issues.apache.org/jira/browse/HADOOP-9249) | hadoop-maven-plugins version-info goal causes build failure when running with Clover |  Major | build | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5012](https://issues.apache.org/jira/browse/MAPREDUCE-5012) | Typo in javadoc for IdentityMapper class |  Trivial | documentation | Adam Monsen | Adam Monsen |
| [HADOOP-8419](https://issues.apache.org/jira/browse/HADOOP-8419) | GzipCodec NPE upon reset with IBM JDK |  Major | io | Luke Lu | Yu Li |
| [HDFS-4391](https://issues.apache.org/jira/browse/HDFS-4391) | TestDataTransferKeepalive fails when tests are executed in a certain order. |  Major | test | Konstantin Shvachko | Andrew Wang |
| [HADOOP-9393](https://issues.apache.org/jira/browse/HADOOP-9393) | TestRPC fails with JDK7 |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-9431](https://issues.apache.org/jira/browse/HADOOP-9431) | TestSecurityUtil#testLocalHostNameForNullOrWild on systems where hostname contains capital letters |  Trivial | security, test | Chris Nauroth | Chris Nauroth |
| [HADOOP-9261](https://issues.apache.org/jira/browse/HADOOP-9261) | S3n filesystem can move a directory under itself -and so lose data |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-9265](https://issues.apache.org/jira/browse/HADOOP-9265) | S3 blockstore filesystem breaks part of the Filesystem contract |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-9433](https://issues.apache.org/jira/browse/HADOOP-9433) | TestLocalFileSystem#testHasFileDescriptor leaks file handle |  Major | fs, test | Chris Nauroth | Chris Nauroth |
| [YARN-524](https://issues.apache.org/jira/browse/YARN-524) | TestYarnVersionInfo failing if generated properties doesn't include an SVN URL |  Minor | api | Steve Loughran | Steve Loughran |
| [HDFS-4757](https://issues.apache.org/jira/browse/HDFS-4757) | Update FSDirectory#inodeMap when replacing an INodeDirectory while setting quota |  Minor | namenode | Jing Zhao | Jing Zhao |
| [HDFS-4761](https://issues.apache.org/jira/browse/HDFS-4761) | Refresh INodeMap in FSDirectory#reset() |  Minor | namenode | Jing Zhao | Jing Zhao |
| [HDFS-4904](https://issues.apache.org/jira/browse/HDFS-4904) | Remove JournalService |  Major | namenode | Suresh Srinivas | Arpit Agarwal |
| [HADOOP-9186](https://issues.apache.org/jira/browse/HADOOP-9186) | test-patch.sh should report build failure to JIRA |  Major | . | Binglin Chang | Binglin Chang |
| [HDFS-4366](https://issues.apache.org/jira/browse/HDFS-4366) | Block Replication Policy Implementation May Skip Higher-Priority Blocks for Lower-Priority Blocks |  Major | . | Derek Dagit | Derek Dagit |
| [HADOOP-9583](https://issues.apache.org/jira/browse/HADOOP-9583) | test-patch gives +1 despite build failure when running tests |  Critical | . | Jason Lowe | Jason Lowe |
| [YARN-1471](https://issues.apache.org/jira/browse/YARN-1471) | The SLS simulator is not running the preemption policy for CapacityScheduler |  Minor | . | Carlo Curino | Carlo Curino |
| [HDFS-5794](https://issues.apache.org/jira/browse/HDFS-5794) | Fix the inconsistency of layout version number of ADD\_DATANODE\_AND\_STORAGE\_UUIDS between trunk and branch-2 |  Minor | namenode | Jing Zhao | Jing Zhao |
| [MAPREDUCE-5717](https://issues.apache.org/jira/browse/MAPREDUCE-5717) | Task pings are interpreted as task progress |  Major | . | Jason Lowe | Jason Lowe |
| [HADOOP-10325](https://issues.apache.org/jira/browse/HADOOP-10325) | improve jenkins javadoc warnings from test-patch.sh |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-5912](https://issues.apache.org/jira/browse/MAPREDUCE-5912) | Task.calculateOutputSize does not handle Windows files after MAPREDUCE-5196 |  Major | client | Remus Rusanu | Remus Rusanu |
| [HADOOP-10831](https://issues.apache.org/jira/browse/HADOOP-10831) | UserProvider is not thread safe |  Major | security | Benoy Antony | Benoy Antony |
| [YARN-2436](https://issues.apache.org/jira/browse/YARN-2436) | yarn application help doesn't work |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11002](https://issues.apache.org/jira/browse/HADOOP-11002) | shell escapes are incompatible with previous releases |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-10996](https://issues.apache.org/jira/browse/HADOOP-10996) | Stop violence in the \*\_HOME |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11033](https://issues.apache.org/jira/browse/HADOOP-11033) | shell scripts ignore JAVA\_HOME on OS X |  Major | scripts | Lei (Eddy) Xu | Allen Wittenauer |
| [HADOOP-11052](https://issues.apache.org/jira/browse/HADOOP-11052) | hadoop\_verify\_secure\_prereq's results aren't checked in bin/hdfs |  Critical | scripts | Allen Wittenauer | Allen Wittenauer |
| [HDFS-6893](https://issues.apache.org/jira/browse/HDFS-6893) | crypto subcommand is not sorted properly in hdfs's hadoop\_usage |  Trivial | scripts | Allen Wittenauer | David Luo |
| [YARN-2525](https://issues.apache.org/jira/browse/YARN-2525) | yarn logs command gives error on trunk |  Minor | scripts | Prakash Ramachandran | Akira Ajisaka |
| [HADOOP-11055](https://issues.apache.org/jira/browse/HADOOP-11055) | non-daemon pid files are missing |  Blocker | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11022](https://issues.apache.org/jira/browse/HADOOP-11022) | User replaced functions get lost 2-3 levels deep (e.g., sbin) |  Critical | scripts | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-5875](https://issues.apache.org/jira/browse/MAPREDUCE-5875) | Make Counter limits consistent across JobClient, MRAppMaster, and YarnChild |  Major | applicationmaster, client, task | Gera Shegalov | Gera Shegalov |
| [HDFS-7204](https://issues.apache.org/jira/browse/HDFS-7204) | balancer doesn't run as a daemon |  Blocker | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11240](https://issues.apache.org/jira/browse/HADOOP-11240) | Jenkins build seems to be broken by changes in test-patch.sh |  Blocker | . | Zhijie Shen | Steve Loughran |
| [HDFS-7213](https://issues.apache.org/jira/browse/HDFS-7213) | processIncrementalBlockReport performance degradation |  Critical | namenode | Daryn Sharp | Eric Payne |
| [HDFS-7235](https://issues.apache.org/jira/browse/HDFS-7235) | DataNode#transferBlock should report blocks that don't exist using reportBadBlock |  Major | datanode, namenode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-7263](https://issues.apache.org/jira/browse/HDFS-7263) | Snapshot read can reveal future bytes for appended files. |  Major | hdfs-client | Konstantin Shvachko | Tao Luo |
| [HDFS-7319](https://issues.apache.org/jira/browse/HDFS-7319) | Remove dead link to HFTP documentation from index.xml |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11267](https://issues.apache.org/jira/browse/HADOOP-11267) | TestSecurityUtil fails when run with JDK8 because of empty principal names |  Minor | security, test | Stephen Chu | Stephen Chu |
| [HADOOP-10786](https://issues.apache.org/jira/browse/HADOOP-10786) | Fix UGI#reloginFromKeytab on Java 8 |  Major | security | Tobi Vollebregt | Stephen Chu |
| [HADOOP-11284](https://issues.apache.org/jira/browse/HADOOP-11284) | Fix variable name mismatches in hadoop-functions.sh |  Minor | scripts | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11298](https://issues.apache.org/jira/browse/HADOOP-11298) | slaves.sh and stop-all.sh are missing slashes |  Trivial | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-2856](https://issues.apache.org/jira/browse/YARN-2856) | Application recovery throw InvalidStateTransitonException: Invalid event: ATTEMPT\_KILLED at ACCEPTED |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [MAPREDUCE-6161](https://issues.apache.org/jira/browse/MAPREDUCE-6161) | mapred hsadmin command missing from trunk |  Major | scripts | Jason Lowe | Allen Wittenauer |
| [YARN-2816](https://issues.apache.org/jira/browse/YARN-2816) | NM fail to start with NPE during container recovery |  Major | nodemanager | zhihai xu | zhihai xu |
| [YARN-2414](https://issues.apache.org/jira/browse/YARN-2414) | RM web UI: app page will crash if app is failed before any attempt has been created |  Major | webapp | Zhijie Shen | Wangda Tan |
| [HDFS-7225](https://issues.apache.org/jira/browse/HDFS-7225) | Remove stale block invalidation work when DN re-registers with different UUID |  Major | namenode | Zhe Zhang | Zhe Zhang |
| [HDFS-7407](https://issues.apache.org/jira/browse/HDFS-7407) | Minor typo in privileged pid/out/log names |  Minor | . | Allen Wittenauer | Allen Wittenauer |
| [YARN-2865](https://issues.apache.org/jira/browse/YARN-2865) | Application recovery continuously fails with "Application with id already present. Cannot duplicate" |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-7425](https://issues.apache.org/jira/browse/HDFS-7425) | NameNode block deletion logging uses incorrect appender. |  Minor | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-4882](https://issues.apache.org/jira/browse/HDFS-4882) | Prevent the Namenode's LeaseManager from looping forever in checkLeases |  Critical | hdfs-client, namenode | Zesheng Wu | Ravi Prakash |
| [YARN-2906](https://issues.apache.org/jira/browse/YARN-2906) | CapacitySchedulerPage shows HTML tags for a queue's Active Users |  Major | capacityscheduler | Jason Lowe | Jason Lowe |
| [HADOOP-11333](https://issues.apache.org/jira/browse/HADOOP-11333) | Fix deadlock in DomainSocketWatcher when the notification pipe is full |  Major | . | yunjiong zhao | yunjiong zhao |
| [YARN-2905](https://issues.apache.org/jira/browse/YARN-2905) | AggregatedLogsBlock page can infinitely loop if the aggregated log file is corrupted |  Blocker | . | Jason Lowe | Varun Saxena |
| [YARN-2894](https://issues.apache.org/jira/browse/YARN-2894) | When ACL's are enabled, if RM switches then application can not be viewed from web. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11296](https://issues.apache.org/jira/browse/HADOOP-11296) | hadoop-daemons.sh throws 'host1: bash: host3: command not found...' |  Major | scripts | Vinayakumar B | Vinayakumar B |
| [YARN-2874](https://issues.apache.org/jira/browse/YARN-2874) | Dead lock in "DelegationTokenRenewer" which blocks RM to execute any further apps |  Blocker | resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [HADOOP-11343](https://issues.apache.org/jira/browse/HADOOP-11343) | Overflow is not properly handled in caclulating final iv for AES CTR |  Blocker | security | Jerry Chen | Jerry Chen |
| [HADOOP-10134](https://issues.apache.org/jira/browse/HADOOP-10134) | [JDK8] Fix Javadoc errors caused by incorrect or illegal tags in doc comments |  Minor | . | Andrew Purtell | Andrew Purtell |
| [HADOOP-11368](https://issues.apache.org/jira/browse/HADOOP-11368) | Fix SSLFactory truststore reloader thread leak in KMSClientProvider |  Major | kms | Arun Suresh | Arun Suresh |
| [HDFS-7489](https://issues.apache.org/jira/browse/HDFS-7489) | Incorrect locking in FsVolumeList#checkDirs can hang datanodes |  Critical | datanode | Noah Lorang | Noah Lorang |
| [YARN-2910](https://issues.apache.org/jira/browse/YARN-2910) | FSLeafQueue can throw ConcurrentModificationException |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-7503](https://issues.apache.org/jira/browse/HDFS-7503) | Namenode restart after large deletions can cause slow processReport (due to logging) |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-2917](https://issues.apache.org/jira/browse/YARN-2917) | Potential deadlock in AsyncDispatcher when system.exit called in AsyncDispatcher#dispatch and AsyscDispatcher#serviceStop from shutdown hook |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11238](https://issues.apache.org/jira/browse/HADOOP-11238) | Update the NameNode's Group Cache in the background when possible |  Minor | . | Chris Li | Chris Li |
| [HADOOP-11380](https://issues.apache.org/jira/browse/HADOOP-11380) | Restore Rack Awareness documentation |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6166](https://issues.apache.org/jira/browse/MAPREDUCE-6166) | Reducers do not validate checksum of map outputs when fetching directly to disk |  Major | mrv2 | Eric Payne | Eric Payne |
| [HADOOP-11397](https://issues.apache.org/jira/browse/HADOOP-11397) | Can't override HADOOP\_IDENT\_STRING |  Trivial | . | Allen Wittenauer | Kengo Seki |
| [YARN-2964](https://issues.apache.org/jira/browse/YARN-2964) | RM prematurely cancels tokens for jobs that submit jobs (oozie) |  Blocker | resourcemanager | Daryn Sharp | Jian He |
| [HDFS-7552](https://issues.apache.org/jira/browse/HDFS-7552) | change FsVolumeList toString() to fix TestDataNodeVolumeFailureToleration |  Major | datanode, test | Liang Xie | Liang Xie |
| [HDFS-7443](https://issues.apache.org/jira/browse/HDFS-7443) | Datanode upgrade to BLOCKID\_BASED\_LAYOUT fails if duplicate block files are present in the same volume |  Blocker | . | Kihwal Lee | Colin P. McCabe |
| [YARN-2952](https://issues.apache.org/jira/browse/YARN-2952) | Incorrect version check in RMStateStore |  Major | . | Jian He | Rohith Sharma K S |
| [YARN-2975](https://issues.apache.org/jira/browse/YARN-2975) | FSLeafQueue app lists are accessed without required locks |  Blocker | . | Karthik Kambatla | Karthik Kambatla |
| [YARN-2340](https://issues.apache.org/jira/browse/YARN-2340) | NPE thrown when RM restart after queue is STOPPED. There after RM can not recovery application's and remain in standby |  Critical | resourcemanager, scheduler | Nishan Shetty | Rohith Sharma K S |
| [YARN-2992](https://issues.apache.org/jira/browse/YARN-2992) | ZKRMStateStore crashes due to session expiry |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-2922](https://issues.apache.org/jira/browse/YARN-2922) | ConcurrentModificationException in CapacityScheduler's LeafQueue |  Major | capacityscheduler, resourcemanager, scheduler | Jason Tufo | Rohith Sharma K S |
| [HADOOP-10908](https://issues.apache.org/jira/browse/HADOOP-10908) | Common needs updates for shell rewrite |  Major | documentation | Allen Wittenauer | Allen Wittenauer |
| [YARN-2978](https://issues.apache.org/jira/browse/YARN-2978) | ResourceManager crashes with NPE while getting queue info |  Critical | . | Jason Tufo | Varun Saxena |
| [YARN-2997](https://issues.apache.org/jira/browse/YARN-2997) | NM keeps sending already-sent completed containers to RM until containers are removed from context |  Major | nodemanager | Chengbing Liu | Chengbing Liu |
| [YARN-3002](https://issues.apache.org/jira/browse/YARN-3002) | YARN documentation needs updating post-shell rewrite |  Major | documentation | Allen Wittenauer | Allen Wittenauer |
| [HDFS-7596](https://issues.apache.org/jira/browse/HDFS-7596) | NameNode should prune dead storages from storageMap |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7533](https://issues.apache.org/jira/browse/HDFS-7533) | Datanode sometimes does not shutdown on receiving upgrade shutdown command |  Major | . | Kihwal Lee | Eric Payne |
| [HADOOP-11473](https://issues.apache.org/jira/browse/HADOOP-11473) | test-patch says "-1 overall" even when all checks are +1 |  Major | build | Jason Lowe | Jason Lowe |
| [HDFS-7470](https://issues.apache.org/jira/browse/HDFS-7470) | SecondaryNameNode need twice memory when calling reloadFromImageFile |  Major | namenode | yunjiong zhao | yunjiong zhao |
| [YARN-2637](https://issues.apache.org/jira/browse/YARN-2637) | maximum-am-resource-percent could be respected for both LeafQueue/User when trying to activate applications. |  Critical | resourcemanager | Wangda Tan | Craig Welch |
| [HDFS-7581](https://issues.apache.org/jira/browse/HDFS-7581) | HDFS documentation needs updating post-shell rewrite |  Major | documentation | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11350](https://issues.apache.org/jira/browse/HADOOP-11350) | The size of header buffer of HttpServer is too small when HTTPS is enabled |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-7610](https://issues.apache.org/jira/browse/HDFS-7610) | Fix removal of dynamically added DN volumes |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11484](https://issues.apache.org/jira/browse/HADOOP-11484) | hadoop-mapreduce-client-nativetask fails to build on ARM AARCH64 due to x86 asm statements |  Minor | native | Edward Nevill | Edward Nevill |
| [HDFS-3443](https://issues.apache.org/jira/browse/HDFS-3443) | Fix NPE when namenode transition to active during startup by adding checkNNStartup() in NameNodeRpcServer |  Major | auto-failover, ha | suja s | Vinayakumar B |
| [HDFS-7575](https://issues.apache.org/jira/browse/HDFS-7575) | Upgrade should generate a unique storage ID for each volume |  Critical | . | Lars Francke | Arpit Agarwal |
| [HADOOP-11482](https://issues.apache.org/jira/browse/HADOOP-11482) | Use correct UGI when KMSClientProvider is called by a proxy user |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-7667](https://issues.apache.org/jira/browse/HDFS-7667) | Various typos and improvements to HDFS Federation doc |  Minor | documentation | Charles Lamb | Charles Lamb |
| [HDFS-3750](https://issues.apache.org/jira/browse/HDFS-3750) | API docs don't include HDFS |  Critical | documentation | Eli Collins | Jolly Chen |
| [HDFS-7320](https://issues.apache.org/jira/browse/HDFS-7320) | The appearance of hadoop-hdfs-httpfs site docs is inconsistent |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3024](https://issues.apache.org/jira/browse/YARN-3024) | LocalizerRunner should give DIE action when all resources are localized |  Major | nodemanager | Chengbing Liu | Chengbing Liu |
| [HADOOP-11316](https://issues.apache.org/jira/browse/HADOOP-11316) | "mvn package -Pdist,docs -DskipTests -Dtar" fails because of non-ascii characters |  Blocker | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-3103](https://issues.apache.org/jira/browse/YARN-3103) | AMRMClientImpl does not update AMRM token properly |  Blocker | client | Jason Lowe | Jason Lowe |
| [MAPREDUCE-6230](https://issues.apache.org/jira/browse/MAPREDUCE-6230) | MR AM does not survive RM restart if RM activated a new AMRM secret key |  Blocker | mr-am | Jason Lowe | Jason Lowe |
| [HDFS-7707](https://issues.apache.org/jira/browse/HDFS-7707) | Edit log corruption due to delayed block removal again |  Major | namenode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-7733](https://issues.apache.org/jira/browse/HDFS-7733) | NFS: readdir/readdirplus return null directory attribute on failure |  Major | nfs | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-6237](https://issues.apache.org/jira/browse/MAPREDUCE-6237) | Multiple mappers with DBInputFormat don't work because of reusing conections |  Major | mrv2 | Kannan Rajah | Kannan Rajah |
| [HDFS-7670](https://issues.apache.org/jira/browse/HDFS-7670) | HDFS Quota guide has typos, incomplete command lines |  Major | documentation | Allen Wittenauer | Brahma Reddy Battula |
| [HDFS-7669](https://issues.apache.org/jira/browse/HDFS-7669) | HDFS Design Doc references commands that no longer exist. |  Major | documentation | Allen Wittenauer | Brahma Reddy Battula |
| [HDFS-7755](https://issues.apache.org/jira/browse/HDFS-7755) | httpfs shell code has hardcoded path to bash |  Major | scripts | Dmitry Sivachenko | Dmitry Sivachenko |
| [YARN-3094](https://issues.apache.org/jira/browse/YARN-3094) | reset timer for liveness monitors after RM recovery |  Major | resourcemanager | Jun Gong | Jun Gong |
| [MAPREDUCE-4413](https://issues.apache.org/jira/browse/MAPREDUCE-4413) | MR lib dir contains jdiff (which is gpl) |  Critical | build | Eli Collins | Nemon Lou |
| [HDFS-7714](https://issues.apache.org/jira/browse/HDFS-7714) | Simultaneous restart of HA NameNodes and DataNode can cause DataNode to register successfully with only one NameNode. |  Major | datanode | Chris Nauroth | Vinayakumar B |
| [YARN-2246](https://issues.apache.org/jira/browse/YARN-2246) | Job History Link in RM UI is redirecting to the URL which contains Job Id twice |  Major | webapp | Devaraj K | Devaraj K |
| [HADOOP-11559](https://issues.apache.org/jira/browse/HADOOP-11559) | Add links to RackAwareness and InterfaceClassification to site index |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11581](https://issues.apache.org/jira/browse/HADOOP-11581) | Fix Multithreaded correctness Warnings #org.apache.hadoop.fs.shell.Ls |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-11580](https://issues.apache.org/jira/browse/HADOOP-11580) | Remove SingleNodeSetup.md from trunk |  Major | documentation | Allen Wittenauer | Akira Ajisaka |
| [HADOOP-11583](https://issues.apache.org/jira/browse/HADOOP-11583) | Fix syntax error in SecureMode.md |  Trivial | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11437](https://issues.apache.org/jira/browse/HADOOP-11437) | Remove the version and author information from distcp's README file |  Major | documentation | Allen Wittenauer | Brahma Reddy Battula |
| [HADOOP-11585](https://issues.apache.org/jira/browse/HADOOP-11585) | Fix formatting in Tracing.md |  Trivial | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7711](https://issues.apache.org/jira/browse/HDFS-7711) | Fix various typos in ClusterSetup.md |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-7791](https://issues.apache.org/jira/browse/HDFS-7791) | dfs count -v should be added to quota documentation |  Major | documentation | Allen Wittenauer | Akira Ajisaka |
| [HDFS-7803](https://issues.apache.org/jira/browse/HDFS-7803) | Wrong command mentioned in HDFSHighAvailabilityWithQJM documentation |  Minor | documentation | Arshad Mohammad | Arshad Mohammad |
| [MAPREDUCE-6234](https://issues.apache.org/jira/browse/MAPREDUCE-6234) | TestHighRamJob fails due to the change in MAPREDUCE-5785 |  Major | contrib/gridmix, mrv2 | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11295](https://issues.apache.org/jira/browse/HADOOP-11295) | RPC Server Reader thread can't shutdown if RPCCallQueue is full |  Major | . | Ming Ma | Ming Ma |
| [HDFS-7788](https://issues.apache.org/jira/browse/HDFS-7788) | Post-2.6 namenode may not start up with an image containing inodes created with an old release. |  Blocker | . | Kihwal Lee | Rushabh S Shah |
| [YARN-3194](https://issues.apache.org/jira/browse/YARN-3194) | RM should handle NMContainerStatuses sent by NM while registering if NM is Reconnected node |  Blocker | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11604](https://issues.apache.org/jira/browse/HADOOP-11604) | Prevent ConcurrentModificationException while closing domain sockets during shutdown of DomainSocketWatcher thread. |  Critical | net | Liang Xie | Chris Nauroth |
| [YARN-3238](https://issues.apache.org/jira/browse/YARN-3238) | Connection timeouts to nodemanagers are retried at multiple levels |  Blocker | . | Jason Lowe | Jason Lowe |
| [YARN-3207](https://issues.apache.org/jira/browse/YARN-3207) | secondary filter matches entites which do not have the key being filtered for. |  Major | timelineserver | Prakash Ramachandran | Zhijie Shen |
| [HDFS-7009](https://issues.apache.org/jira/browse/HDFS-7009) | Active NN and standby NN have different live nodes |  Major | datanode | Ming Ma | Ming Ma |
| [HADOOP-11625](https://issues.apache.org/jira/browse/HADOOP-11625) | Minor fixes to command manual & SLA doc |  Major | documentation | Allen Wittenauer | Allen Wittenauer |
| [HDFS-7763](https://issues.apache.org/jira/browse/HDFS-7763) | fix zkfc hung issue due to not catching exception in a corner case |  Major | ha | Liang Xie | Liang Xie |
| [YARN-3239](https://issues.apache.org/jira/browse/YARN-3239) | WebAppProxy does not support a final tracking url which has query fragments and params |  Major | . | Hitesh Shah | Jian He |
| [HADOOP-11637](https://issues.apache.org/jira/browse/HADOOP-11637) | bash location hard-coded in shell scripts |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6223](https://issues.apache.org/jira/browse/MAPREDUCE-6223) | TestJobConf#testNegativeValueForTaskVmem failures |  Major | test | Gera Shegalov | Varun Saxena |
| [HADOOP-10774](https://issues.apache.org/jira/browse/HADOOP-10774) | Update KerberosTestUtils for hadoop-auth tests when using IBM Java |  Major | security | sangamesh | sangamesh |
| [HDFS-4681](https://issues.apache.org/jira/browse/HDFS-4681) | TestBlocksWithNotEnoughRacks#testCorruptBlockRereplicatedAcrossRacks fails using IBM java |  Major | test | Tian Hong Wang | Ayappan |
| [HDFS-7871](https://issues.apache.org/jira/browse/HDFS-7871) | NameNodeEditLogRoller can keep printing "Swallowing exception" message |  Critical | . | Jing Zhao | Jing Zhao |
| [YARN-3222](https://issues.apache.org/jira/browse/YARN-3222) | RMNodeImpl#ReconnectNodeTransition should send scheduler events in sequential order |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-3231](https://issues.apache.org/jira/browse/YARN-3231) | FairScheduler: Changing queueMaxRunningApps interferes with pending jobs |  Critical | . | Siqi Li | Siqi Li |
| [YARN-3242](https://issues.apache.org/jira/browse/YARN-3242) | Asynchrony in ZK-close can lead to ZKRMStateStore watcher receiving events for old client |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [HADOOP-11674](https://issues.apache.org/jira/browse/HADOOP-11674) | oneByteBuf in CryptoInputStream and CryptoOutputStream should be non static |  Critical | io | Sean Busbey | Sean Busbey |
| [HDFS-7885](https://issues.apache.org/jira/browse/HDFS-7885) | Datanode should not trust the generation stamp provided by client |  Critical | datanode | vitthal (Suhas) Gogate | Tsz Wo Nicholas Sze |
| [YARN-3227](https://issues.apache.org/jira/browse/YARN-3227) | Timeline renew delegation token fails when RM user's TGT is expired |  Critical | . | Jonathan Eagles | Zhijie Shen |
| [HADOOP-11653](https://issues.apache.org/jira/browse/HADOOP-11653) | shellprofiles should require .sh extension |  Major | scripts | Allen Wittenauer | Brahma Reddy Battula |
| [YARN-3287](https://issues.apache.org/jira/browse/YARN-3287) | TimelineClient kerberos authentication failure uses wrong login context. |  Major | . | Jonathan Eagles | Daryn Sharp |
| [HADOOP-11668](https://issues.apache.org/jira/browse/HADOOP-11668) | hadoop-daemons.sh bw compat broke with --slaves change |  Major | scripts | Vinayakumar B | Vinayakumar B |
| [HDFS-7830](https://issues.apache.org/jira/browse/HDFS-7830) | DataNode does not release the volume lock when adding a volume fails. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11703](https://issues.apache.org/jira/browse/HADOOP-11703) | git should ignore .DS\_Store files on Mac OS X |  Major | . | Abin Shahab | Abin Shahab |
| [HADOOP-10027](https://issues.apache.org/jira/browse/HADOOP-10027) | \*Compressor\_deflateBytesDirect passes instance instead of jclass to GetStaticObjectField |  Minor | native | Eric Abbott | Hui Zheng |
| [YARN-3267](https://issues.apache.org/jira/browse/YARN-3267) | Timelineserver applies the ACL rules after applying the limit on the number of records |  Major | . | Prakash Ramachandran | Chang Li |
| [HDFS-7915](https://issues.apache.org/jira/browse/HDFS-7915) | The DataNode can sometimes allocate a ShortCircuitShm slot and fail to tell the DFSClient about it because of a network error |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5356](https://issues.apache.org/jira/browse/HDFS-5356) | MiniDFSCluster shoud close all open FileSystems when shutdown() |  Critical | test | haosdent | Rakesh R |
| [YARN-3197](https://issues.apache.org/jira/browse/YARN-3197) | Confusing log generated by CapacityScheduler |  Minor | capacityscheduler | Hitesh Shah | Varun Saxena |
| [YARN-3243](https://issues.apache.org/jira/browse/YARN-3243) | CapacityScheduler should pass headroom from parent to children to make sure ParentQueue obey its capacity limits. |  Major | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3305](https://issues.apache.org/jira/browse/YARN-3305) | AM-Used Resource for leafqueue is wrongly populated if AM ResourceRequest is less than minimumAllocation |  Major | scheduler | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-3205](https://issues.apache.org/jira/browse/YARN-3205) | FileSystemRMStateStore should disable FileSystem Cache to avoid get a Filesystem with an old configuration. |  Major | resourcemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-5807](https://issues.apache.org/jira/browse/MAPREDUCE-5807) | Print usage for TeraSort job. |  Trivial | examples | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-3351](https://issues.apache.org/jira/browse/YARN-3351) | AppMaster tracking URL is broken in HA |  Major | webapp | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-7929](https://issues.apache.org/jira/browse/HDFS-7929) | inotify unable fetch pre-upgrade edit log segments once upgrade starts |  Major | . | Zhe Zhang | Zhe Zhang |
| [HDFS-7587](https://issues.apache.org/jira/browse/HDFS-7587) | Edit log corruption can happen if append fails with a quota violation |  Blocker | namenode | Kihwal Lee | Jing Zhao |
| [HDFS-7867](https://issues.apache.org/jira/browse/HDFS-7867) | Update action param from "start" to "prepare" in rolling upgrade javadoc |  Trivial | . | J.Andreina | J.Andreina |
| [MAPREDUCE-6281](https://issues.apache.org/jira/browse/MAPREDUCE-6281) | Fix javadoc in Terasort |  Trivial | . | Albert Chu | Albert Chu |
| [HDFS-7930](https://issues.apache.org/jira/browse/HDFS-7930) | commitBlockSynchronization() does not remove locations |  Blocker | namenode | Konstantin Shvachko | Yi Liu |
| [YARN-3369](https://issues.apache.org/jira/browse/YARN-3369) | Missing NullPointer check in AppSchedulingInfo causes RM to die |  Blocker | resourcemanager | Giovanni Matteo Fumarola | Brahma Reddy Battula |
| [YARN-3269](https://issues.apache.org/jira/browse/YARN-3269) | Yarn.nodemanager.remote-app-log-dir could not be configured to fully qualified path |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-6213](https://issues.apache.org/jira/browse/MAPREDUCE-6213) | NullPointerException caused by job history server addr not resolvable |  Minor | applicationmaster | Peng Zhang | Peng Zhang |
| [MAPREDUCE-5448](https://issues.apache.org/jira/browse/MAPREDUCE-5448) | MapFileOutputFormat#getReaders bug with invisible files/folders |  Minor | mrv2 | Maysam Yabandeh | Maysam Yabandeh |
| [MAPREDUCE-6242](https://issues.apache.org/jira/browse/MAPREDUCE-6242) | Progress report log is incredibly excessive in application master |  Major | applicationmaster | Jian Fang | Varun Saxena |
| [YARN-3393](https://issues.apache.org/jira/browse/YARN-3393) | Getting application(s) goes wrong when app finishes before starting the attempt |  Critical | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7884](https://issues.apache.org/jira/browse/HDFS-7884) | NullPointerException in BlockSender |  Blocker | datanode | Tsz Wo Nicholas Sze | Brahma Reddy Battula |
| [HDFS-7960](https://issues.apache.org/jira/browse/HDFS-7960) | The full block report should prune zombie storages even if they're not empty |  Critical | . | Lei (Eddy) Xu | Colin P. McCabe |
| [HDFS-3325](https://issues.apache.org/jira/browse/HDFS-3325) | When configuring "dfs.namenode.safemode.threshold-pct" to a value greater or equal to 1 there is mismatch in the UI report |  Minor | . | J.Andreina | J.Andreina |
| [YARN-3383](https://issues.apache.org/jira/browse/YARN-3383) | AdminService should use "warn" instead of "info" to log exception when operation fails |  Major | resourcemanager | Wangda Tan | Li Lu |
| [YARN-3397](https://issues.apache.org/jira/browse/YARN-3397) | yarn rmadmin should skip -failover |  Minor | resourcemanager | J.Andreina | J.Andreina |
| [HADOOP-11724](https://issues.apache.org/jira/browse/HADOOP-11724) | DistCp throws NPE when the target directory is root. |  Minor | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-3400](https://issues.apache.org/jira/browse/YARN-3400) | [JDK 8] Build Failure due to unreported exceptions in RPCUtil |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-7804](https://issues.apache.org/jira/browse/HDFS-7804) | haadmin command usage #HDFSHighAvailabilityWithQJM.html |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-7990](https://issues.apache.org/jira/browse/HDFS-7990) | IBR delete ack should not be delayed |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-11760](https://issues.apache.org/jira/browse/HADOOP-11760) | Fix typo of javadoc in DistCp |  Trivial | . | Chen He | Brahma Reddy Battula |
| [MAPREDUCE-6294](https://issues.apache.org/jira/browse/MAPREDUCE-6294) | Remove an extra parameter described in Javadoc of TockenCache |  Trivial | . | Chen He | Brahma Reddy Battula |
| [HDFS-7501](https://issues.apache.org/jira/browse/HDFS-7501) | TransactionsSinceLastCheckpoint can be negative on SBNs |  Major | namenode | Harsh J | Gautam Gopalakrishnan |
| [HDFS-7742](https://issues.apache.org/jira/browse/HDFS-7742) | favoring decommissioning node for replication can cause a block to stay underreplicated for long periods |  Major | namenode | Nathan Roberts | Nathan Roberts |
| [HDFS-8002](https://issues.apache.org/jira/browse/HDFS-8002) | Website refers to /trash directory |  Major | documentation | Mike Drob | Brahma Reddy Battula |
| [HDFS-7261](https://issues.apache.org/jira/browse/HDFS-7261) | storageMap is accessed without synchronization in DatanodeDescriptor#updateHeartbeatState() |  Major | . | Ted Yu | Brahma Reddy Battula |
| [HDFS-7997](https://issues.apache.org/jira/browse/HDFS-7997) | The first non-existing xattr should also throw IOException |  Minor | . | zhouyingchao | zhouyingchao |
| [HDFS-6945](https://issues.apache.org/jira/browse/HDFS-6945) | BlockManager should remove a block from excessReplicateMap and decrement ExcessBlocks metric when the block is removed |  Critical | namenode | Akira Ajisaka | Akira Ajisaka |
| [YARN-3425](https://issues.apache.org/jira/browse/YARN-3425) | NPE from RMNodeLabelsManager.serviceStop when NodeLabelsManager.serviceInit failed |  Minor | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-7922](https://issues.apache.org/jira/browse/HDFS-7922) | ShortCircuitCache#close is not releasing ScheduledThreadPoolExecutors |  Major | . | Rakesh R | Rakesh R |
| [HDFS-8026](https://issues.apache.org/jira/browse/HDFS-8026) | Trace FSOutputSummer#writeChecksumChunks rather than DFSOutputStream#writeChunk |  Minor | . | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-6303](https://issues.apache.org/jira/browse/MAPREDUCE-6303) | Read timeout when retrying a fetch error can be fatal to a reducer |  Blocker | . | Jason Lowe | Jason Lowe |
| [YARN-3415](https://issues.apache.org/jira/browse/YARN-3415) | Non-AM containers can be counted towards amResourceUsage of a Fair Scheduler queue |  Critical | fairscheduler | Rohit Agarwal | zhihai xu |
| [HADOOP-11797](https://issues.apache.org/jira/browse/HADOOP-11797) | releasedocmaker.py needs to put ASF headers on output |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11800](https://issues.apache.org/jira/browse/HADOOP-11800) | Clean up some test methods in TestCodec.java |  Major | test | Akira Ajisaka | Brahma Reddy Battula |
| [MAPREDUCE-4844](https://issues.apache.org/jira/browse/MAPREDUCE-4844) | Counters / AbstractCounters have constant references not declared final |  Major | . | Gera Shegalov | Brahma Reddy Battula |
| [YARN-3435](https://issues.apache.org/jira/browse/YARN-3435) | AM container to be allocated Appattempt AM container shown as null |  Trivial | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-7999](https://issues.apache.org/jira/browse/HDFS-7999) | FsDatasetImpl#createTemporary sometimes holds the FSDatasetImpl lock for a very long time |  Major | . | zhouyingchao | zhouyingchao |
| [YARN-3429](https://issues.apache.org/jira/browse/YARN-3429) | TestAMRMTokens.testTokenExpiry fails Intermittently with error message:Invalid AMRMToken |  Major | test | zhihai xu | zhihai xu |
| [HDFS-5215](https://issues.apache.org/jira/browse/HDFS-5215) | dfs.datanode.du.reserved is not considered while computing available space |  Major | datanode | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-3457](https://issues.apache.org/jira/browse/YARN-3457) | NPE when NodeManager.serviceInit fails and stopRecoveryStore called |  Minor | nodemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3459](https://issues.apache.org/jira/browse/YARN-3459) | Fix failiure of TestLog4jWarningErrorMetricsAppender |  Blocker | . | Li Lu | Varun Vasudev |
| [HDFS-8072](https://issues.apache.org/jira/browse/HDFS-8072) | Reserved RBW space is not released if client terminates while writing block |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8046](https://issues.apache.org/jira/browse/HDFS-8046) | Allow better control of getContentSummary |  Major | . | Kihwal Lee | Kihwal Lee |
| [YARN-2890](https://issues.apache.org/jira/browse/YARN-2890) | MiniYarnCluster should turn on timeline service if configured to do so |  Major | . | Mit Desai | Mit Desai |
| [HDFS-7725](https://issues.apache.org/jira/browse/HDFS-7725) | Incorrect "nodes in service" metrics caused all writes to fail |  Major | . | Ming Ma | Ming Ma |
| [HDFS-8096](https://issues.apache.org/jira/browse/HDFS-8096) | DatanodeMetrics#blocksReplicated will get incremented early and even for failed transfers |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [YARN-3465](https://issues.apache.org/jira/browse/YARN-3465) | Use LinkedHashMap to preserve order of resource requests |  Major | nodemanager | zhihai xu | zhihai xu |
| [HDFS-8099](https://issues.apache.org/jira/browse/HDFS-8099) | Change "DFSInputStream has been closed already" message to debug log level |  Minor | hdfs-client | Charles Lamb | Charles Lamb |
| [YARN-3055](https://issues.apache.org/jira/browse/YARN-3055) | The token is not renewed properly if it's shared by jobs (oozie) in DelegationTokenRenewer |  Blocker | security | Yi Liu | Daryn Sharp |
| [HDFS-8091](https://issues.apache.org/jira/browse/HDFS-8091) | ACLStatus and XAttributes not properly presented to INodeAttributesProvider before returning to client |  Major | namenode | Arun Suresh | Arun Suresh |
| [MAPREDUCE-6266](https://issues.apache.org/jira/browse/MAPREDUCE-6266) | Job#getTrackingURL should consistently return a proper URL |  Minor | . | Ray Chiang | Ray Chiang |
| [HDFS-8081](https://issues.apache.org/jira/browse/HDFS-8081) | Split getAdditionalBlock() into two methods. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-7939](https://issues.apache.org/jira/browse/HDFS-7939) | Two fsimage\_rollback\_\* files are created which are not deleted after rollback. |  Critical | . | J.Andreina | J.Andreina |
| [MAPREDUCE-6314](https://issues.apache.org/jira/browse/MAPREDUCE-6314) | TestPipeApplication fails on trunk |  Major | test | Varun Vasudev | Varun Vasudev |
| [YARN-3466](https://issues.apache.org/jira/browse/YARN-3466) | Fix RM nodes web page to sort by node HTTP-address, #containers and node-label column |  Major | resourcemanager, webapp | Jason Lowe | Jason Lowe |
| [HDFS-7931](https://issues.apache.org/jira/browse/HDFS-7931) | DistributedFIleSystem should not look for keyProvider in cache if Encryption is disabled |  Minor | hdfs-client | Arun Suresh | Arun Suresh |
| [HDFS-8111](https://issues.apache.org/jira/browse/HDFS-8111) | NPE thrown when invalid FSImage filename given for "hdfs oiv\_legacy" cmd |  Minor | tools | Archana T | Surendra Singh Lilhore |
| [HADOOP-11811](https://issues.apache.org/jira/browse/HADOOP-11811) | Fix typos in hadoop-project/pom.xml and TestAccessControlList |  Minor | . | Chen He | Brahma Reddy Battula |
| [YARN-3382](https://issues.apache.org/jira/browse/YARN-3382) | Some of UserMetricsInfo metrics are incorrectly set to root queue metrics |  Major | webapp | Rohit Agarwal | Rohit Agarwal |
| [YARN-3472](https://issues.apache.org/jira/browse/YARN-3472) | Possible leak in DelegationTokenRenewer#allTokens |  Major | . | Jian He | Rohith Sharma K S |
| [YARN-3394](https://issues.apache.org/jira/browse/YARN-3394) | WebApplication  proxy documentation is incomplete |  Minor | resourcemanager | Bibin A Chundatt | Naganarasimha G R |
| [HADOOP-11819](https://issues.apache.org/jira/browse/HADOOP-11819) | HttpServerFunctionalTest#prepareTestWebapp should create web app directory if it does not exist. |  Minor | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-6666](https://issues.apache.org/jira/browse/HDFS-6666) | Abort NameNode and DataNode startup if security is enabled but block access token is not enabled. |  Minor | datanode, namenode, security | Chris Nauroth | Vijay Bhat |
| [HDFS-8055](https://issues.apache.org/jira/browse/HDFS-8055) | NullPointerException when topology script is missing. |  Major | namenode | Anu Engineer | Anu Engineer |
| [YARN-3266](https://issues.apache.org/jira/browse/YARN-3266) | RMContext inactiveNodes should have NodeId as map key |  Major | resourcemanager | Chengbing Liu | Chengbing Liu |
| [YARN-3436](https://issues.apache.org/jira/browse/YARN-3436) | Fix URIs in documention of YARN web service REST APIs |  Minor | documentation, resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-8127](https://issues.apache.org/jira/browse/HDFS-8127) | NameNode Failover during HA upgrade can cause DataNode to finalize upgrade |  Blocker | ha | Jing Zhao | Jing Zhao |
| [HDFS-8151](https://issues.apache.org/jira/browse/HDFS-8151) | Always use snapshot path as source when invalid snapshot names are used for diff based distcp |  Minor | distcp | Sushmitha Sreenivasan | Jing Zhao |
| [HDFS-7934](https://issues.apache.org/jira/browse/HDFS-7934) | Update RollingUpgrade rollback documentation: should use bootstrapstandby for standby NN |  Critical | documentation | J.Andreina | J.Andreina |
| [HDFS-8149](https://issues.apache.org/jira/browse/HDFS-8149) | The footer of the Web UI "Hadoop, 2014" is old |  Major | . | Akira Ajisaka | Brahma Reddy Battula |
| [HDFS-8142](https://issues.apache.org/jira/browse/HDFS-8142) | DistributedFileSystem encryption zone commands should resolve relative paths |  Major | . | Rakesh R | Rakesh R |
| [MAPREDUCE-6300](https://issues.apache.org/jira/browse/MAPREDUCE-6300) | Task list sort by task id broken |  Minor | . | Siqi Li | Siqi Li |
| [YARN-3021](https://issues.apache.org/jira/browse/YARN-3021) | YARN's delegation-token handling disallows certain trust setups to operate properly over DistCp |  Major | security | Harsh J | Yongjun Zhang |
| [HDFS-8153](https://issues.apache.org/jira/browse/HDFS-8153) | Error Message points to wrong parent directory in case of path component name length error |  Major | namenode | Anu Engineer | Anu Engineer |
| [YARN-3493](https://issues.apache.org/jira/browse/YARN-3493) | RM fails to come up with error "Failed to load/recover state" when  mem settings are changed |  Critical | yarn | Sumana Sathish | Jian He |
| [HDFS-8043](https://issues.apache.org/jira/browse/HDFS-8043) | NPE in MiniDFSCluster teardown |  Major | test | Steve Loughran | Brahma Reddy Battula |
| [HDFS-8173](https://issues.apache.org/jira/browse/HDFS-8173) | NPE thrown at DataNode shutdown when HTTP server was not able to create |  Minor | datanode | Archana T | Surendra Singh Lilhore |
| [YARN-3497](https://issues.apache.org/jira/browse/YARN-3497) | ContainerManagementProtocolProxy modifies IPC timeout conf without making a copy |  Major | client | Jason Lowe | Jason Lowe |
| [MAPREDUCE-6238](https://issues.apache.org/jira/browse/MAPREDUCE-6238) | MR2 can't run local jobs with -libjars command options which is a regression from MR1 |  Critical | mrv2 | zhihai xu | zhihai xu |
| [HDFS-8179](https://issues.apache.org/jira/browse/HDFS-8179) | DFSClient#getServerDefaults returns null within 1 hour of system start |  Blocker | . | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7993](https://issues.apache.org/jira/browse/HDFS-7993) | Provide each Replica details in fsck |  Major | . | Ming Ma | J.Andreina |
| [HDFS-8163](https://issues.apache.org/jira/browse/HDFS-8163) | Using monotonicNow for block report scheduling causes test failures on recently restarted systems |  Blocker | datanode | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-11704](https://issues.apache.org/jira/browse/HADOOP-11704) | DelegationTokenAuthenticationFilter must pass ipaddress instead of hostname to ProxyUsers#authorize() |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-3495](https://issues.apache.org/jira/browse/YARN-3495) | Confusing log generated by FairScheduler |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6293](https://issues.apache.org/jira/browse/MAPREDUCE-6293) | Set job classloader on uber-job's LocalContainerLauncher event thread |  Major | mr-am | Sangjin Lee | Sangjin Lee |
| [HADOOP-11846](https://issues.apache.org/jira/browse/HADOOP-11846) | TestCertificateUtil.testCorruptPEM failing on Jenkins JDK8 |  Major | build, security | Steve Loughran | Larry McCay |
| [HADOOP-11859](https://issues.apache.org/jira/browse/HADOOP-11859) | PseudoAuthenticationHandler fails with httpcomponents v4.4 |  Major | . | Eugene Koifman | Eugene Koifman |
| [MAPREDUCE-6330](https://issues.apache.org/jira/browse/MAPREDUCE-6330) | Fix typo in Task Attempt API's URL in documentations |  Minor | documentation | Ryu Kobayashi | Ryu Kobayashi |
| [HADOOP-11848](https://issues.apache.org/jira/browse/HADOOP-11848) | Incorrect arguments to sizeof in DomainSocket.c |  Major | native | Malcolm Kavalsky | Malcolm Kavalsky |
| [HADOOP-11868](https://issues.apache.org/jira/browse/HADOOP-11868) | Invalid user logins trigger large backtraces in server log |  Major | . | Chang Li | Chang Li |
| [HADOOP-11861](https://issues.apache.org/jira/browse/HADOOP-11861) | test-patch.sh rewrite addendum patch |  Major | build | Anu Engineer | Allen Wittenauer |
| [HADOOP-11864](https://issues.apache.org/jira/browse/HADOOP-11864) | JWTRedirectAuthenticationHandler breaks java8 javadocs |  Major | build | Steve Loughran | Larry McCay |
| [HDFS-4448](https://issues.apache.org/jira/browse/HDFS-4448) | Allow HA NN to start in secure mode with wildcard address configured |  Major | ha, namenode, security | Aaron T. Myers | Aaron T. Myers |
| [YARN-3522](https://issues.apache.org/jira/browse/YARN-3522) | DistributedShell uses the wrong user to put timeline data |  Blocker | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-8147](https://issues.apache.org/jira/browse/HDFS-8147) | Mover should not schedule two replicas to the same DN storage |  Major | balancer & mover | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-11730](https://issues.apache.org/jira/browse/HADOOP-11730) | Regression: s3n read failure recovery broken |  Major | fs/s3 | Takenori Sato | Takenori Sato |
| [YARN-3516](https://issues.apache.org/jira/browse/YARN-3516) | killing ContainerLocalizer action doesn't take effect when private localizer receives FETCH\_FAILURE status. |  Minor | nodemanager | zhihai xu | zhihai xu |
| [HADOOP-11802](https://issues.apache.org/jira/browse/HADOOP-11802) | DomainSocketWatcher thread terminates sometimes after there is an I/O error during requestShortCircuitShm |  Major | . | Eric Payne | Colin P. McCabe |
| [HDFS-8070](https://issues.apache.org/jira/browse/HDFS-8070) | Pre-HDFS-7915 DFSClient cannot use short circuit on post-HDFS-7915 DataNode |  Blocker | caching | Gopal V | Colin P. McCabe |
| [HDFS-8217](https://issues.apache.org/jira/browse/HDFS-8217) | During block recovery for truncate Log new Block Id in case of copy-on-truncate is true. |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-8231](https://issues.apache.org/jira/browse/HDFS-8231) | StackTrace displayed at client while QuotaByStorageType exceeds |  Major | hdfs-client | J.Andreina | J.Andreina |
| [HDFS-8110](https://issues.apache.org/jira/browse/HDFS-8110) | Remove unsupported 'hdfs namenode -rollingUpgrade downgrade' from document |  Minor | documentation | J.Andreina | J.Andreina |
| [HDFS-8191](https://issues.apache.org/jira/browse/HDFS-8191) | Fix byte to integer casting in SimulatedFSDataset#simulatedByte |  Minor | . | Zhe Zhang | Zhe Zhang |
| [YARN-3387](https://issues.apache.org/jira/browse/YARN-3387) | Previous AM's container complete message couldn't pass to current am if am restarted and rm changed |  Critical | resourcemanager | sandflee | sandflee |
| [HADOOP-11876](https://issues.apache.org/jira/browse/HADOOP-11876) | Refactor code to make it more readable, minor maybePrintStats bug |  Trivial | tools/distcp | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [YARN-3444](https://issues.apache.org/jira/browse/YARN-3444) | Fix typo capabililty |  Trivial | applications/distributed-shell | Gabor Liptak | Gabor Liptak |
| [YARN-3537](https://issues.apache.org/jira/browse/YARN-3537) | NPE when NodeManager.serviceInit fails and stopRecoveryStore invoked |  Major | nodemanager | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8211](https://issues.apache.org/jira/browse/HDFS-8211) | DataNode UUID is always null in the JMX counter |  Major | datanode | Anu Engineer | Anu Engineer |
| [MAPREDUCE-6333](https://issues.apache.org/jira/browse/MAPREDUCE-6333) | TestEvents,TestAMWebServicesTasks,TestAppController are broken due to MAPREDUCE-6297 |  Major | . | Siqi Li | Siqi Li |
| [HDFS-7673](https://issues.apache.org/jira/browse/HDFS-7673) | synthetic load generator docs give incorrect/incomplete commands |  Major | documentation | Allen Wittenauer | Brahma Reddy Battula |
| [HDFS-8206](https://issues.apache.org/jira/browse/HDFS-8206) | Fix the typos in hadoop-hdfs-httpfs |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-3464](https://issues.apache.org/jira/browse/YARN-3464) | Race condition in LocalizerRunner kills localizer before localizing all resources |  Critical | nodemanager | zhihai xu | zhihai xu |
| [HADOOP-11865](https://issues.apache.org/jira/browse/HADOOP-11865) | Incorrect path mentioned in document for accessing script files |  Major | documentation | J.Andreina | J.Andreina |
| [MAPREDUCE-6252](https://issues.apache.org/jira/browse/MAPREDUCE-6252) | JobHistoryServer should not fail when encountering a missing directory |  Major | jobhistoryserver | Craig Welch | Craig Welch |
| [YARN-3530](https://issues.apache.org/jira/browse/YARN-3530) | ATS throws exception on trying to filter results without otherinfo. |  Critical | timelineserver | Sreenath Somarajapuram | Zhijie Shen |
| [HDFS-8205](https://issues.apache.org/jira/browse/HDFS-8205) | CommandFormat#parse() should not parse option as value of option |  Blocker | . | Peter Shi | Peter Shi |
| [HADOOP-11870](https://issues.apache.org/jira/browse/HADOOP-11870) | [JDK8] AuthenticationFilter, CertificateUtil, SignerSecretProviders, KeyAuthorizationKeyProvider Javadoc issues |  Major | build | Robert Kanter | Robert Kanter |
| [MAPREDUCE-6324](https://issues.apache.org/jira/browse/MAPREDUCE-6324) | Uber jobs fail to update AMRM token when it rolls over |  Blocker | mr-am | Jason Lowe | Jason Lowe |
| [HDFS-8232](https://issues.apache.org/jira/browse/HDFS-8232) | Missing datanode counters when using Metrics2 sink interface |  Major | datanode | Anu Engineer | Anu Engineer |
| [MAPREDUCE-6334](https://issues.apache.org/jira/browse/MAPREDUCE-6334) | Fetcher#copyMapOutput is leaking usedMemory upon IOException during InMemoryMapOutput shuffle handler |  Blocker | . | Eric Payne | Eric Payne |
| [MAPREDUCE-6343](https://issues.apache.org/jira/browse/MAPREDUCE-6343) | JobConf.parseMaximumHeapSizeMB() fails to parse value greater than 2GB expressed in bytes |  Major | . | Hao Xia | Hao Xia |
| [HDFS-8273](https://issues.apache.org/jira/browse/HDFS-8273) | FSNamesystem#Delete() should not call logSync() when holding the lock |  Blocker | namenode | Jing Zhao | Haohui Mai |
| [YARN-3485](https://issues.apache.org/jira/browse/YARN-3485) | FairScheduler headroom calculation doesn't consider maxResources for Fifo and FairShare policies |  Critical | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HDFS-8269](https://issues.apache.org/jira/browse/HDFS-8269) | getBlockLocations() does not resolve the .reserved path and generates incorrect edit logs when updating the atime |  Blocker | . | Yesha Vora | Haohui Mai |
| [YARN-3517](https://issues.apache.org/jira/browse/YARN-3517) | RM web ui for dumping scheduler logs should be for admins only |  Blocker | resourcemanager, security | Varun Vasudev | Varun Vasudev |
| [HDFS-8214](https://issues.apache.org/jira/browse/HDFS-8214) | Secondary NN Web UI shows wrong date for Last Checkpoint |  Major | namenode | Charles Lamb | Charles Lamb |
| [YARN-3533](https://issues.apache.org/jira/browse/YARN-3533) | Test: Fix launchAM in MockRM to wait for attempt to be scheduled |  Major | yarn | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-6339](https://issues.apache.org/jira/browse/MAPREDUCE-6339) | Job history file is not flushed correctly because isTimerActive flag is not set true when flushTimerTask is scheduled. |  Critical | mrv2 | zhihai xu | zhihai xu |
| [HADOOP-11821](https://issues.apache.org/jira/browse/HADOOP-11821) | Fix findbugs warnings in hadoop-sls |  Major | tools | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-3564](https://issues.apache.org/jira/browse/YARN-3564) | Fix TestContainerAllocation.testAMContainerAllocationWhenDNSUnavailable fails randomly |  Major | . | Jian He | Jian He |
| [HADOOP-11891](https://issues.apache.org/jira/browse/HADOOP-11891) | OsSecureRandom should lazily fill its reservoir |  Major | security | Arun Suresh | Arun Suresh |
| [HADOOP-11866](https://issues.apache.org/jira/browse/HADOOP-11866) | increase readability and reliability of checkstyle, shellcheck, and whitespace reports |  Minor | . | Naganarasimha G R | Allen Wittenauer |
| [HDFS-8292](https://issues.apache.org/jira/browse/HDFS-8292) | Move conditional in fmt\_time from dfs-dust.js to status.html |  Minor | namenode | Charles Lamb | Charles Lamb |
| [HDFS-8300](https://issues.apache.org/jira/browse/HDFS-8300) | Fix unit test failures and findbugs warning caused by HDFS-8283 |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-8276](https://issues.apache.org/jira/browse/HDFS-8276) | LazyPersistFileScrubber should be disabled if scrubber interval configured zero |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-11889](https://issues.apache.org/jira/browse/HADOOP-11889) | Make checkstyle runnable from root project |  Major | build, test | Gera Shegalov | Gera Shegalov |
| [HDFS-8213](https://issues.apache.org/jira/browse/HDFS-8213) | DFSClient should use hdfs.client.htrace HTrace configuration prefix rather than hadoop.htrace |  Critical | . | Billie Rinaldi | Colin P. McCabe |
| [HDFS-8229](https://issues.apache.org/jira/browse/HDFS-8229) | LAZY\_PERSIST file gets deleted after NameNode restart. |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-2893](https://issues.apache.org/jira/browse/YARN-2893) | AMLaucher: sporadic job failures due to EOFException in readTokenStorageStream |  Major | resourcemanager | Gera Shegalov | zhihai xu |
| [HADOOP-11491](https://issues.apache.org/jira/browse/HADOOP-11491) | HarFs incorrectly declared as requiring an authority |  Critical | fs | Gera Shegalov | Brahma Reddy Battula |
| [HADOOP-11900](https://issues.apache.org/jira/browse/HADOOP-11900) | Add failIfNoTests=false to hadoop-build-tools pom |  Major | test | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-6345](https://issues.apache.org/jira/browse/MAPREDUCE-6345) | Documentation fix for when CRLA is enabled for MRAppMaster logs |  Trivial | documentation | Rohit Agarwal | Rohit Agarwal |
| [YARN-2454](https://issues.apache.org/jira/browse/YARN-2454) | Fix compareTo of variable UNBOUNDED in o.a.h.y.util.resource.Resources. |  Major | . | Xu Yang | Xu Yang |
| [YARN-1993](https://issues.apache.org/jira/browse/YARN-1993) | Cross-site scripting vulnerability in TextView.java |  Major | webapp | Ted Yu | Kenji Kikushima |
| [MAPREDUCE-5905](https://issues.apache.org/jira/browse/MAPREDUCE-5905) | CountersStrings.toEscapedCompactStrings outputs unnecessary "null" strings |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-6349](https://issues.apache.org/jira/browse/MAPREDUCE-6349) | Fix typo in property org.apache.hadoop.mapreduce.lib.chain.Chain.REDUCER\_INPUT\_VALUE\_CLASS |  Minor | . | Ray Chiang | Ray Chiang |
| [HADOOP-9658](https://issues.apache.org/jira/browse/HADOOP-9658) | SnappyCodec#checkNativeCodeLoaded may unexpectedly fail when native code is not loaded |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-3097](https://issues.apache.org/jira/browse/YARN-3097) | Logging of resource recovery on NM restart has redundancies |  Minor | nodemanager | Jason Lowe | Eric Payne |
| [HDFS-8290](https://issues.apache.org/jira/browse/HDFS-8290) | WebHDFS calls before namesystem initialization can cause NullPointerException. |  Minor | webhdfs | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5649](https://issues.apache.org/jira/browse/MAPREDUCE-5649) | Reduce cannot use more than 2G memory  for the final merge |  Major | mrv2 | stanley shi | Gera Shegalov |
| [MAPREDUCE-6259](https://issues.apache.org/jira/browse/MAPREDUCE-6259) | IllegalArgumentException due to missing job submit time |  Major | jobhistoryserver | zhihai xu | zhihai xu |
| [YARN-3375](https://issues.apache.org/jira/browse/YARN-3375) | NodeHealthScriptRunner.shouldRun() check is performing 3 times for starting NodeHealthScriptRunner |  Minor | nodemanager | Devaraj K | Devaraj K |
| [YARN-2725](https://issues.apache.org/jira/browse/YARN-2725) | Adding test cases of retrying requests about ZKRMStateStore |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [MAPREDUCE-6165](https://issues.apache.org/jira/browse/MAPREDUCE-6165) | [JDK8] TestCombineFileInputFormat failed on JDK8 |  Minor | . | Wei Yan | Akira Ajisaka |
| [HADOOP-11328](https://issues.apache.org/jira/browse/HADOOP-11328) | ZKFailoverController does not log Exception when doRun raises errors |  Major | ha | Tianyin Xu | Tianyin Xu |
| [HADOOP-11916](https://issues.apache.org/jira/browse/HADOOP-11916) | TestStringUtils#testLowerAndUpperStrings failed on MAC due to a JVM bug |  Minor | . | Ming Ma | Ming Ma |
| [YARN-3552](https://issues.apache.org/jira/browse/YARN-3552) | RM Web UI shows -1 running containers for completed apps |  Trivial | webapp | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11120](https://issues.apache.org/jira/browse/HADOOP-11120) | hadoop fs -rmr gives wrong advice |  Major | . | Allen Wittenauer | Juliet Hougland |
| [YARN-3396](https://issues.apache.org/jira/browse/YARN-3396) | Handle URISyntaxException in ResourceLocalizationService |  Major | nodemanager | Chengbing Liu | Brahma Reddy Battula |
| [YARN-2123](https://issues.apache.org/jira/browse/YARN-2123) | Progress bars in Web UI always at 100% due to non-US locale |  Major | webapp | Johannes Simon | Akira Ajisaka |
| [HDFS-8305](https://issues.apache.org/jira/browse/HDFS-8305) | HDFS INotify: the destination field of RenameOp should always end with the file name |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11917](https://issues.apache.org/jira/browse/HADOOP-11917) | test-patch.sh should work with ${BASEDIR}/patchprocess setups |  Blocker | . | Allen Wittenauer | Allen Wittenauer |
| [HDFS-7847](https://issues.apache.org/jira/browse/HDFS-7847) | Modify NNThroughputBenchmark to be able to operate on a remote NameNode |  Major | . | Colin P. McCabe | Charles Lamb |
| [HDFS-8219](https://issues.apache.org/jira/browse/HDFS-8219) | setStoragePolicy with folder behavior is different after cluster restart |  Major | . | Peter Shi | Surendra Singh Lilhore |
| [HADOOP-11926](https://issues.apache.org/jira/browse/HADOOP-11926) | test-patch.sh mv does wrong math |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-11912](https://issues.apache.org/jira/browse/HADOOP-11912) | Extra configuration key used in TraceUtils should respect prefix |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3582](https://issues.apache.org/jira/browse/YARN-3582) | NPE in WebAppProxyServlet |  Major | . | Jian He | Jian He |
| [HDFS-2484](https://issues.apache.org/jira/browse/HDFS-2484) | checkLease should throw FileNotFoundException when file does not exist |  Major | namenode | Konstantin Shvachko | Rakesh R |
| [YARN-3385](https://issues.apache.org/jira/browse/YARN-3385) | Race condition: KeeperException$NoNodeException will cause RM shutdown during ZK node deletion. |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-7833](https://issues.apache.org/jira/browse/HDFS-7833) | DataNode reconfiguration does not recalculate valid volumes required, based on configured failed volumes tolerated. |  Major | datanode | Chris Nauroth | Lei (Eddy) Xu |
| [YARN-3577](https://issues.apache.org/jira/browse/YARN-3577) | Misspelling of threshold in log4j.properties for tests |  Minor | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8325](https://issues.apache.org/jira/browse/HDFS-8325) | Misspelling of threshold in log4j.properties for tests |  Minor | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-10387](https://issues.apache.org/jira/browse/HADOOP-10387) | Misspelling of threshold in log4j.properties for tests in hadoop-common-project |  Minor | conf, test | Kenji Kikushima | Brahma Reddy Battula |
| [MAPREDUCE-6356](https://issues.apache.org/jira/browse/MAPREDUCE-6356) | Misspelling of threshold in log4j.properties for tests |  Minor | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-3523](https://issues.apache.org/jira/browse/YARN-3523) | Cleanup ResourceManagerAdministrationProtocol interface audience |  Major | client, resourcemanager | Wangda Tan | Naganarasimha G R |
| [HDFS-7980](https://issues.apache.org/jira/browse/HDFS-7980) | Incremental BlockReport will dramatically slow down the startup of  a namenode |  Major | . | Hui Zheng | Walter Su |
| [HADOOP-11936](https://issues.apache.org/jira/browse/HADOOP-11936) | Dockerfile references a removed image |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [YARN-3584](https://issues.apache.org/jira/browse/YARN-3584) | [Log mesage correction] : MIssing space in Diagnostics message |  Trivial | . | nijel | nijel |
| [HDFS-8321](https://issues.apache.org/jira/browse/HDFS-8321) | CacheDirectives and CachePool operations should throw RetriableException in safemode |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-8037](https://issues.apache.org/jira/browse/HDFS-8037) | CheckAccess in WebHDFS silently accepts malformed FsActions parameters |  Minor | webhdfs | Jake Low | Walter Su |
| [YARN-1832](https://issues.apache.org/jira/browse/YARN-1832) | Fix wrong MockLocalizerStatus#equals implementation |  Minor | nodemanager | Hong Zhiguo | Hong Zhiguo |
| [YARN-3572](https://issues.apache.org/jira/browse/YARN-3572) | Correct typos in WritingYarnApplications.md |  Minor | documentation | Sandeep Khurana | Gabor Liptak |
| [HADOOP-11922](https://issues.apache.org/jira/browse/HADOOP-11922) | Misspelling of threshold in log4j.properties for tests in hadoop-tools |  Minor | . | Brahma Reddy Battula | Gabor Liptak |
| [HDFS-8257](https://issues.apache.org/jira/browse/HDFS-8257) | Namenode rollingUpgrade option is incorrect in document |  Major | documentation | J.Andreina | J.Andreina |
| [HDFS-8067](https://issues.apache.org/jira/browse/HDFS-8067) | haadmin prints out stale help messages |  Minor | hdfs-client | Ajith S | Ajith S |
| [YARN-3592](https://issues.apache.org/jira/browse/YARN-3592) | Fix typos in RMNodeLabelsManager |  Trivial | resourcemanager | Junping Du | Sunil G |
| [HDFS-8174](https://issues.apache.org/jira/browse/HDFS-8174) | Update replication count to live rep count in fsck report |  Minor | . | J.Andreina | J.Andreina |
| [HDFS-6291](https://issues.apache.org/jira/browse/HDFS-6291) | FSImage may be left unclosed in BootstrapStandby#doRun() |  Minor | ha | Ted Yu | Sanghyun Yun |
| [YARN-3358](https://issues.apache.org/jira/browse/YARN-3358) | Audit log not present while refreshing Service ACLs |  Minor | resourcemanager | Varun Saxena | Varun Saxena |
| [HDFS-8349](https://issues.apache.org/jira/browse/HDFS-8349) | Remove .xml and documentation references to dfs.webhdfs.enabled |  Minor | . | Ray Chiang | Ray Chiang |
| [YARN-3589](https://issues.apache.org/jira/browse/YARN-3589) | RM and AH web UI display DOCTYPE wrongly |  Major | webapp | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-7998](https://issues.apache.org/jira/browse/HDFS-7998) | HDFS Federation : Command mentioned to add a NN to existing federated cluster is wrong |  Minor | documentation | Ajith S | Ajith S |
| [HDFS-8222](https://issues.apache.org/jira/browse/HDFS-8222) | Remove usage of "dfsadmin -upgradeProgress " from document which  is no longer supported |  Major | documentation | J.Andreina | J.Andreina |
| [HDFS-8187](https://issues.apache.org/jira/browse/HDFS-8187) | Remove usage of "-setStoragePolicy" and "-getStoragePolicy" using dfsadmin cmd (as it is not been supported) |  Major | documentation | J.Andreina | J.Andreina |
| [HDFS-8175](https://issues.apache.org/jira/browse/HDFS-8175) | Provide information on snapshotDiff for supporting the comparison between snapshot and current status |  Major | documentation | J.Andreina | J.Andreina |
| [MAPREDUCE-6342](https://issues.apache.org/jira/browse/MAPREDUCE-6342) | Make POM project names consistent |  Minor | build | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-6576](https://issues.apache.org/jira/browse/HDFS-6576) | Datanode log is generating at root directory in security mode |  Minor | datanode, scripts | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-11877](https://issues.apache.org/jira/browse/HADOOP-11877) | SnappyDecompressor's Logger class name is wrong |  Major | conf | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-3384](https://issues.apache.org/jira/browse/HDFS-3384) | DataStreamer thread should be closed immediatly when failed to setup a PipelineForAppendOrRecovery |  Major | hdfs-client | Brahma Reddy Battula | Uma Maheswara Rao G |
| [YARN-3554](https://issues.apache.org/jira/browse/YARN-3554) | Default value for maximum nodemanager connect wait time is too high |  Major | . | Jason Lowe | Naganarasimha G R |
| [HDFS-7894](https://issues.apache.org/jira/browse/HDFS-7894) | Rolling upgrade readiness is not updated in jmx until query command is issued. |  Critical | . | Kihwal Lee | Brahma Reddy Battula |
| [YARN-3600](https://issues.apache.org/jira/browse/YARN-3600) | AM container link is broken (on a killed application, at least) |  Major | . | Sergey Shelukhin | Naganarasimha G R |
| [HDFS-8346](https://issues.apache.org/jira/browse/HDFS-8346) | libwebhdfs build fails during link due to unresolved external symbols. |  Major | native | Chris Nauroth | Chris Nauroth |
| [HDFS-8274](https://issues.apache.org/jira/browse/HDFS-8274) | NFS configuration nfs.dump.dir not working |  Major | nfs | Ajith S | Ajith S |
| [HDFS-8340](https://issues.apache.org/jira/browse/HDFS-8340) | Fix NFS documentation of nfs.wtmax |  Minor | documentation, nfs | Ajith S | Ajith S |
| [HADOOP-10356](https://issues.apache.org/jira/browse/HADOOP-10356) | Corrections in winutils/chmod.c |  Trivial | bin | René Nyffenegger | René Nyffenegger |
| [HADOOP-7165](https://issues.apache.org/jira/browse/HADOOP-7165) | listLocatedStatus(path, filter) is not redefined in FilterFs |  Major | fs | Hairong Kuang | Hairong Kuang |
| [MAPREDUCE-3383](https://issues.apache.org/jira/browse/MAPREDUCE-3383) | Duplicate job.getOutputValueGroupingComparator() in ReduceTask |  Major | . | Binglin Chang | Binglin Chang |
| [HDFS-8311](https://issues.apache.org/jira/browse/HDFS-8311) | DataStreamer.transfer() should timeout the socket InputStream. |  Major | hdfs-client | Esteban Gutierrez | Esteban Gutierrez |
| [HDFS-8113](https://issues.apache.org/jira/browse/HDFS-8113) | Add check for null BlockCollection pointers in BlockInfoContiguous structures |  Major | namenode | Chengbing Liu | Chengbing Liu |
| [HADOOP-9729](https://issues.apache.org/jira/browse/HADOOP-9729) | The example code of org.apache.hadoop.util.Tool is incorrect |  Major | util | hellojinjie | hellojinjie |
| [MAPREDUCE-2094](https://issues.apache.org/jira/browse/MAPREDUCE-2094) | LineRecordReader should not seek into non-splittable, compressed streams. |  Major | task | Niels Basjes | Niels Basjes |
| [HADOOP-11942](https://issues.apache.org/jira/browse/HADOOP-11942) | Add links to SLGUserGuide to site index |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8245](https://issues.apache.org/jira/browse/HDFS-8245) | Standby namenode doesn't process DELETED\_BLOCK if the add block request is in edit log. |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [YARN-3018](https://issues.apache.org/jira/browse/YARN-3018) | Unify the default value for yarn.scheduler.capacity.node-locality-delay in code and default xml file |  Trivial | capacityscheduler | nijel | nijel |
| [HDFS-8326](https://issues.apache.org/jira/browse/HDFS-8326) | Documentation about when checkpoints are run is out of date |  Major | documentation | Misty Stanley-Jones | Misty Stanley-Jones |
| [YARN-3604](https://issues.apache.org/jira/browse/YARN-3604) | removeApplication in ZKRMStateStore should also disable watch. |  Minor | resourcemanager | zhihai xu | zhihai xu |
| [YARN-3476](https://issues.apache.org/jira/browse/YARN-3476) | Nodemanager can fail to delete local logs if log aggregation fails |  Major | log-aggregation, nodemanager | Jason Lowe | Rohith Sharma K S |
| [YARN-3473](https://issues.apache.org/jira/browse/YARN-3473) | Fix RM Web UI configuration for some properties |  Minor | resourcemanager | Ray Chiang | Ray Chiang |
| [HDFS-8097](https://issues.apache.org/jira/browse/HDFS-8097) | TestFileTruncate is failing intermittently |  Major | test | Rakesh R | Rakesh R |
| [HADOOP-11775](https://issues.apache.org/jira/browse/HADOOP-11775) | Fix Javadoc typos in hadoop-openstack module |  Trivial | documentation | Chen He | Yanjun Wang |
| [YARN-1912](https://issues.apache.org/jira/browse/YARN-1912) | ResourceLocalizer started without any jvm memory control |  Major | nodemanager | stanley shi | Masatake Iwasaki |
| [MAPREDUCE-6359](https://issues.apache.org/jira/browse/MAPREDUCE-6359) | RM HA setup, "Cluster" tab links populated with AM hostname instead of RM |  Minor | . | Aroop Maliakkal | yunjiong zhao |
| [MAPREDUCE-6353](https://issues.apache.org/jira/browse/MAPREDUCE-6353) | Divide by zero error in MR AM when calculating available containers |  Major | mr-am | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-3395](https://issues.apache.org/jira/browse/YARN-3395) | FairScheduler: Trim whitespaces when using username for queuename |  Major | fairscheduler | zhihai xu | zhihai xu |
| [HDFS-8351](https://issues.apache.org/jira/browse/HDFS-8351) | Remove namenode -finalize option from document |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-3587](https://issues.apache.org/jira/browse/YARN-3587) | Fix the javadoc of DelegationTokenSecretManager in projects of yarn, etc. |  Minor | documentation | Akira Ajisaka | Gabor Liptak |
| [HADOOP-11663](https://issues.apache.org/jira/browse/HADOOP-11663) | Remove description about Java 6 from docs |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11928](https://issues.apache.org/jira/browse/HADOOP-11928) | Test-patch check for @author tags incorrectly flags removal of @author tags |  Major | . | Sean Busbey | Kengo Seki |
| [HADOOP-11951](https://issues.apache.org/jira/browse/HADOOP-11951) | test-patch should give better info about failures to handle dev-support updates without resetrepo option |  Minor | . | Sean Busbey | Sean Busbey |
| [HADOOP-11947](https://issues.apache.org/jira/browse/HADOOP-11947) | test-patch should return early from determine-issue  when run in jenkins mode. |  Minor | . | Sean Busbey | Sean Busbey |
| [HDFS-7916](https://issues.apache.org/jira/browse/HDFS-7916) | 'reportBadBlocks' from datanodes to standby Node BPServiceActor goes for infinite loop |  Critical | datanode | Vinayakumar B | Rushabh S Shah |
| [YARN-3434](https://issues.apache.org/jira/browse/YARN-3434) | Interaction between reservations and userlimit can result in significant ULF violation |  Major | capacityscheduler | Thomas Graves | Thomas Graves |
| [HDFS-8362](https://issues.apache.org/jira/browse/HDFS-8362) | Java Compilation Error in TestHdfsConfigFields.java |  Major | . | Arshad Mohammad | Arshad Mohammad |
| [MAPREDUCE-6360](https://issues.apache.org/jira/browse/MAPREDUCE-6360) | TestMapreduceConfigFields is placed in wrong dir, introducing compile error |  Major | . | Vinayakumar B | Arshad Mohammad |
| [MAPREDUCE-6361](https://issues.apache.org/jira/browse/MAPREDUCE-6361) | NPE issue in shuffle caused by concurrent issue between copySucceeded() in one thread and copyFailed() in another thread on the same host |  Critical | . | Junping Du | Junping Du |
| [YARN-3629](https://issues.apache.org/jira/browse/YARN-3629) | NodeID is always printed as "null" in node manager initialization log. |  Major | . | nijel | nijel |
| [MAPREDUCE-6251](https://issues.apache.org/jira/browse/MAPREDUCE-6251) | JobClient needs additional retries at a higher level to address not-immediately-consistent dfs corner cases |  Major | jobhistoryserver, mrv2 | Craig Welch | Craig Welch |
| [MAPREDUCE-6366](https://issues.apache.org/jira/browse/MAPREDUCE-6366) | mapreduce.terasort.final.sync configuration in TeraSort  doesn't work |  Trivial | examples | Takuya Fukudome | Takuya Fukudome |
| [HDFS-6300](https://issues.apache.org/jira/browse/HDFS-6300) | Prevent multiple balancers from running simultaneously |  Critical | balancer & mover | Rakesh R | Rakesh R |
| [HDFS-8358](https://issues.apache.org/jira/browse/HDFS-8358) | TestTraceAdmin fails |  Major | . | Kihwal Lee | Masatake Iwasaki |
| [HADOOP-11966](https://issues.apache.org/jira/browse/HADOOP-11966) | Variable cygwin is undefined in hadoop-config.sh when executed through hadoop-daemon.sh. |  Critical | scripts | Chris Nauroth | Chris Nauroth |
| [HDFS-8380](https://issues.apache.org/jira/browse/HDFS-8380) | Always call addStoredBlock on blocks which have been shifted from one storage to another |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-8174](https://issues.apache.org/jira/browse/HADOOP-8174) | Remove confusing comment in Path#isAbsolute() |  Trivial | fs | Suresh Srinivas | Suresh Srinivas |
| [HDFS-8150](https://issues.apache.org/jira/browse/HDFS-8150) | Make getFileChecksum fail for blocks under construction |  Critical | . | Kihwal Lee | J.Andreina |
| [MAPREDUCE-5708](https://issues.apache.org/jira/browse/MAPREDUCE-5708) | Duplicate String.format in YarnOutputFiles.getSpillFileForWrite |  Minor | . | Konstantin Weitz | Konstantin Weitz |
| [YARN-1519](https://issues.apache.org/jira/browse/YARN-1519) | check if sysconf is implemented before using it |  Major | nodemanager | Radim Kolar | Radim Kolar |
| [HDFS-8371](https://issues.apache.org/jira/browse/HDFS-8371) | Fix test failure in TestHdfsConfigFields for spanreceiver properties |  Major | . | Ray Chiang | Ray Chiang |
| [MAPREDUCE-6273](https://issues.apache.org/jira/browse/MAPREDUCE-6273) | HistoryFileManager should check whether summaryFile exists to avoid FileNotFoundException causing HistoryFileInfo into MOVE\_FAILED state |  Minor | jobhistoryserver | zhihai xu | zhihai xu |
| [YARN-2421](https://issues.apache.org/jira/browse/YARN-2421) | RM still allocates containers to an app in the FINISHING state |  Major | scheduler | Thomas Graves | Chang Li |
| [YARN-3526](https://issues.apache.org/jira/browse/YARN-3526) | ApplicationMaster tracking URL is incorrectly redirected on a QJM cluster |  Major | resourcemanager, webapp | Weiwei Yang | Weiwei Yang |
| [HADOOP-11988](https://issues.apache.org/jira/browse/HADOOP-11988) | Fix typo in the document for hadoop fs -find |  Trivial | documentation | Akira Ajisaka | Kengo Seki |
| [HADOOP-10582](https://issues.apache.org/jira/browse/HADOOP-10582) | Fix the test case for copying to non-existent dir in TestFsShellCopy |  Minor | fs | Kousuke Saruta | Kousuke Saruta |
| [HDFS-8412](https://issues.apache.org/jira/browse/HDFS-8412) | Fix the test failures in HTTPFS: In some tests setReplication called after fs close. |  Major | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-8345](https://issues.apache.org/jira/browse/HDFS-8345) | Storage policy APIs must be exposed via the FileSystem interface |  Major | hdfs-client | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8405](https://issues.apache.org/jira/browse/HDFS-8405) | Fix a typo in NamenodeFsck |  Minor | namenode | Tsz Wo Nicholas Sze | Takanobu Asanuma |
| [HDFS-6348](https://issues.apache.org/jira/browse/HDFS-6348) | SecondaryNameNode not terminating properly on runtime exceptions |  Major | namenode | Rakesh R | Rakesh R |
| [YARN-3601](https://issues.apache.org/jira/browse/YARN-3601) | Fix UT TestRMFailover.testRMWebAppRedirect |  Critical | resourcemanager, webapp | Weiwei Yang | Weiwei Yang |
| [HDFS-8404](https://issues.apache.org/jira/browse/HDFS-8404) | Pending block replication can get stuck using older genstamp |  Major | namenode | Nathan Roberts | Nathan Roberts |
| [HADOOP-11973](https://issues.apache.org/jira/browse/HADOOP-11973) | Ensure ZkDelegationTokenSecretManager namespace znodes get created with ACLs |  Major | security | Gregory Chanan | Gregory Chanan |
| [HADOOP-11963](https://issues.apache.org/jira/browse/HADOOP-11963) | Metrics documentation for FSNamesystem misspells PendingDataNodeMessageCount. |  Trivial | documentation | Chris Nauroth | Anu Engineer |
| [YARN-2821](https://issues.apache.org/jira/browse/YARN-2821) | Distributed shell app master becomes unresponsive sometimes |  Major | applications/distributed-shell | Varun Vasudev | Varun Vasudev |
| [YARN-3677](https://issues.apache.org/jira/browse/YARN-3677) | Fix findbugs warnings in yarn-server-resourcemanager |  Minor | resourcemanager | Akira Ajisaka | Vinod Kumar Vavilapalli |
| [YARN-3681](https://issues.apache.org/jira/browse/YARN-3681) | yarn cmd says "could not find main class 'queue'" in windows |  Blocker | yarn | Sumana Sathish | Varun Saxena |
| [YARN-3654](https://issues.apache.org/jira/browse/YARN-3654) | ContainerLogsPage web UI should not have meta-refresh |  Major | yarn | Xuan Gong | Xuan Gong |
| [YARN-3694](https://issues.apache.org/jira/browse/YARN-3694) | Fix dead link for TimelineServer REST API |  Minor | documentation | Akira Ajisaka | Jagadesh Kiran N |
| [YARN-3646](https://issues.apache.org/jira/browse/YARN-3646) | Applications are getting stuck some times in case of retry policy forever |  Major | client | Raju Bairishetti | Raju Bairishetti |
| [HDFS-8421](https://issues.apache.org/jira/browse/HDFS-8421) | Move startFile() and related operations into FSDirWriteFileOp |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-8451](https://issues.apache.org/jira/browse/HDFS-8451) | DFSClient probe for encryption testing interprets empty URI property for "enabled" |  Blocker | encryption | Steve Loughran | Steve Loughran |
| [YARN-3675](https://issues.apache.org/jira/browse/YARN-3675) | FairScheduler: RM quits when node removal races with continousscheduling on the same node |  Critical | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-12016](https://issues.apache.org/jira/browse/HADOOP-12016) | Typo in FileSystem:: listStatusIterator |  Trivial | fs | Jakob Homan | Arthur Vigil |
| [HADOOP-12014](https://issues.apache.org/jira/browse/HADOOP-12014) | hadoop-config.cmd displays a wrong error message |  Minor | scripts | Kengo Seki | Kengo Seki |
| [HADOOP-11955](https://issues.apache.org/jira/browse/HADOOP-11955) | Fix a typo in the cluster setup doc |  Trivial | . | Kihwal Lee | Yanjun Wang |
| [HDFS-8268](https://issues.apache.org/jira/browse/HDFS-8268) | Port conflict log for data node server is not sufficient |  Minor | datanode | Mohammad Shahid Khan | Mohammad Shahid Khan |
| [YARN-3594](https://issues.apache.org/jira/browse/YARN-3594) | WintuilsProcessStubExecutor.startStreamReader leaks streams |  Trivial | nodemanager | Steve Loughran | Lars Francke |
| [HADOOP-11743](https://issues.apache.org/jira/browse/HADOOP-11743) | maven doesn't clean all the site files |  Minor | documentation | Allen Wittenauer | ramtin |
| [HADOOP-11927](https://issues.apache.org/jira/browse/HADOOP-11927) | Fix "undefined reference to dlopen" error when compiling libhadooppipes |  Major | build, native, tools | Xianyin Xin | Xianyin Xin |
| [YARN-3701](https://issues.apache.org/jira/browse/YARN-3701) | Isolating the error of generating a single app report when getting all apps from generic history service |  Blocker | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-3707](https://issues.apache.org/jira/browse/YARN-3707) | RM Web UI queue filter doesn't work |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-2238](https://issues.apache.org/jira/browse/YARN-2238) | filtering on UI sticks even if I move away from the page |  Major | webapp | Sangjin Lee | Jian He |
| [HADOOP-8751](https://issues.apache.org/jira/browse/HADOOP-8751) | NPE in Token.toString() when Token is constructed using null identifier |  Minor | security | Vlad Rozov | Kanaka Kumar Avvaru |
| [HADOOP-11969](https://issues.apache.org/jira/browse/HADOOP-11969) | ThreadLocal initialization in several classes is not thread safe |  Critical | io | Sean Busbey | Sean Busbey |
| [YARN-3626](https://issues.apache.org/jira/browse/YARN-3626) | On Windows localized resources are not moved to the front of the classpath when they should be |  Major | yarn | Craig Welch | Craig Welch |
| [HADOOP-9891](https://issues.apache.org/jira/browse/HADOOP-9891) | CLIMiniCluster instructions fail with MiniYarnCluster ClassNotFoundException |  Minor | documentation | Steve Loughran | Darrell Taylor |
| [YARN-2355](https://issues.apache.org/jira/browse/YARN-2355) | MAX\_APP\_ATTEMPTS\_ENV may no longer be a useful env var for a container |  Major | . | Zhijie Shen | Darrell Taylor |
| [HDFS-8431](https://issues.apache.org/jira/browse/HDFS-8431) | hdfs crypto class not found in Windows |  Critical | scripts | Sumana Sathish | Anu Engineer |
| [HADOOP-12004](https://issues.apache.org/jira/browse/HADOOP-12004) | test-patch breaks with reexec in certain situations |  Critical | . | Allen Wittenauer | Sean Busbey |
| [YARN-3723](https://issues.apache.org/jira/browse/YARN-3723) | Need to clearly document primaryFilter and otherInfo value type |  Critical | timelineserver | Zhijie Shen | Zhijie Shen |
| [HADOOP-11406](https://issues.apache.org/jira/browse/HADOOP-11406) | xargs -P is not portable |  Critical | scripts | Allen Wittenauer | Kengo Seki |
| [HADOOP-11983](https://issues.apache.org/jira/browse/HADOOP-11983) | HADOOP\_USER\_CLASSPATH\_FIRST works the opposite of what it is supposed to do |  Major | scripts | Sangjin Lee | Sangjin Lee |
| [HDFS-8407](https://issues.apache.org/jira/browse/HDFS-8407) | libhdfs hdfsListDirectory must set errno to 0 on success |  Major | native | Juan Yu | Masatake Iwasaki |
| [HDFS-8429](https://issues.apache.org/jira/browse/HDFS-8429) | Avoid stuck threads if there is an error in DomainSocketWatcher that stops the thread |  Major | . | zhouyingchao | zhouyingchao |
| [HADOOP-11959](https://issues.apache.org/jira/browse/HADOOP-11959) | WASB should configure client side socket timeout in storage client blob request options |  Major | tools | Ivan Mitic | Ivan Mitic |
| [HADOOP-11934](https://issues.apache.org/jira/browse/HADOOP-11934) | Use of JavaKeyStoreProvider in LdapGroupsMapping causes infinite loop |  Blocker | security | Mike Yoder | Larry McCay |
| [HDFS-7401](https://issues.apache.org/jira/browse/HDFS-7401) | Add block info to DFSInputStream' WARN message when it adds node to deadNodes |  Minor | . | Ming Ma | Arshad Mohammad |
| [HADOOP-12042](https://issues.apache.org/jira/browse/HADOOP-12042) | Users may see TrashPolicy if hdfs dfs -rm is run |  Major | . | Allen Wittenauer | J.Andreina |
| [HDFS-7609](https://issues.apache.org/jira/browse/HDFS-7609) | Avoid retry cache collision when Standby NameNode loading edits |  Critical | namenode | Carrey Zhan | Ming Ma |
| [HADOOP-11885](https://issues.apache.org/jira/browse/HADOOP-11885) | hadoop-dist dist-layout-stitching.sh does not work with dash |  Major | build | Andrew Wang | Andrew Wang |
| [YARN-3725](https://issues.apache.org/jira/browse/YARN-3725) | App submission via REST API is broken in secure mode due to Timeline DT service address is empty |  Blocker | resourcemanager, timelineserver | Zhijie Shen | Zhijie Shen |
| [HADOOP-9905](https://issues.apache.org/jira/browse/HADOOP-9905) | remove dependency of zookeeper for hadoop-client |  Major | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-12037](https://issues.apache.org/jira/browse/HADOOP-12037) | Fix wrong classname in example configuration of hadoop-auth documentation |  Trivial | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8256](https://issues.apache.org/jira/browse/HDFS-8256) | fsck "-storagepolicies , -blockId ,-replicaDetails " options are missed out in usage and from documentation |  Major | documentation | J.Andreina | J.Andreina |
| [HDFS-8486](https://issues.apache.org/jira/browse/HDFS-8486) | DN startup may cause severe data loss |  Blocker | datanode | Daryn Sharp | Daryn Sharp |
| [HDFS-8386](https://issues.apache.org/jira/browse/HDFS-8386) | Improve synchronization of 'streamer' reference in DFSOutputStream |  Major | hdfs-client | Rakesh R | Rakesh R |
| [HDFS-8513](https://issues.apache.org/jira/browse/HDFS-8513) | Rename BlockPlacementPolicyRackFaultTolarent to BlockPlacementPolicyRackFaultTolerant |  Minor | namenode | Andrew Wang | Andrew Wang |
| [HADOOP-11991](https://issues.apache.org/jira/browse/HADOOP-11991) | test-patch.sh isn't re-executed even if smart-apply-patch.sh is modified |  Major | test | Kengo Seki | Kengo Seki |
| [HDFS-8270](https://issues.apache.org/jira/browse/HDFS-8270) | create() always retried with hardcoded timeout when file already exists with open lease |  Major | hdfs-client | Andrey Stepachev | J.Andreina |
| [HDFS-8470](https://issues.apache.org/jira/browse/HDFS-8470) | fsimage loading progress should update inode, delegation token and cache pool count. |  Minor | namenode | tongshiquan | Surendra Singh Lilhore |
| [HDFS-8523](https://issues.apache.org/jira/browse/HDFS-8523) | Remove usage information on unsupported operation "fsck -showprogress" from branch-2 |  Major | documentation | J.Andreina | J.Andreina |
| [HDFS-3716](https://issues.apache.org/jira/browse/HDFS-3716) | Purger should remove stale fsimage ckpt files |  Minor | namenode | suja s | J.Andreina |
| [YARN-3751](https://issues.apache.org/jira/browse/YARN-3751) | TestAHSWebServices fails after YARN-3467 |  Major | . | Zhijie Shen | Sunil G |
| [YARN-3585](https://issues.apache.org/jira/browse/YARN-3585) | NodeManager cannot exit on SHUTDOWN event triggered and NM recovery is enabled |  Critical | . | Peng Zhang | Rohith Sharma K S |
| [MAPREDUCE-6374](https://issues.apache.org/jira/browse/MAPREDUCE-6374) | Distributed Cache File visibility should check permission of full path |  Major | . | Chang Li | Chang Li |
| [YARN-3762](https://issues.apache.org/jira/browse/YARN-3762) | FairScheduler: CME on FSParentQueue#getQueueUserAclInfo |  Critical | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-3749](https://issues.apache.org/jira/browse/YARN-3749) | We should make a copy of configuration when init MiniYARNCluster with multiple RMs |  Major | . | Chun Chen | Chun Chen |
| [MAPREDUCE-5965](https://issues.apache.org/jira/browse/MAPREDUCE-5965) | Hadoop streaming throws error if list of input files is high. Error is: "error=7, Argument list too long at if number of input file is high" |  Major | . | Arup Malakar | Wilfred Spiegelenburg |
| [HADOOP-12018](https://issues.apache.org/jira/browse/HADOOP-12018) | smart-apply-patch.sh fails if the patch edits CR+LF files and is created by 'git diff --no-prefix' |  Minor | build | Akira Ajisaka | Kengo Seki |
| [HADOOP-12019](https://issues.apache.org/jira/browse/HADOOP-12019) | update BUILDING.txt to include python for 'mvn site' in windows |  Major | . | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-6382](https://issues.apache.org/jira/browse/MAPREDUCE-6382) | Don't escape HTML links in Diagnostics in JHS job overview |  Major | . | Siqi Li | Siqi Li |
| [HADOOP-12058](https://issues.apache.org/jira/browse/HADOOP-12058) | Fix dead links to DistCp and Hadoop Archives pages. |  Minor | documentation, site | Kazuho Fujii | Kazuho Fujii |
| [HDFS-8463](https://issues.apache.org/jira/browse/HDFS-8463) | Calling DFSInputStream.seekToNewSource just after stream creation causes  NullPointerException |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3764](https://issues.apache.org/jira/browse/YARN-3764) | CapacityScheduler should forbid moving LeafQueue from one parent to another |  Blocker | . | Wangda Tan | Wangda Tan |
| [HADOOP-11994](https://issues.apache.org/jira/browse/HADOOP-11994) | smart-apply-patch wrongly assumes that git is infallible |  Major | test | Allen Wittenauer | Kengo Seki |
| [HADOOP-11924](https://issues.apache.org/jira/browse/HADOOP-11924) | Tolerate JDK-8047340-related exceptions in Shell#isSetSidAvailable preventing class init |  Major | . | Gera Shegalov | Tsuyoshi Ozawa |
| [YARN-3733](https://issues.apache.org/jira/browse/YARN-3733) | Fix DominantRC#compare() does not work as expected if cluster resource is empty |  Blocker | resourcemanager | Bibin A Chundatt | Rohith Sharma K S |
| [MAPREDUCE-6377](https://issues.apache.org/jira/browse/MAPREDUCE-6377) | JHS sorting on state column not working in webUi |  Minor | jobhistoryserver | Bibin A Chundatt | zhihai xu |
| [MAPREDUCE-6387](https://issues.apache.org/jira/browse/MAPREDUCE-6387) | Serialize the recently added Task#encryptedSpillKey field at the end |  Minor | . | Arun Suresh | Arun Suresh |
| [HADOOP-12056](https://issues.apache.org/jira/browse/HADOOP-12056) | Use DirectoryStream in DiskChecker#checkDirs to detect errors when listing a directory |  Major | util | zhihai xu | zhihai xu |
| [HDFS-8522](https://issues.apache.org/jira/browse/HDFS-8522) | Change heavily recorded NN logs from INFO to DEBUG level |  Major | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-3655](https://issues.apache.org/jira/browse/YARN-3655) | FairScheduler: potential livelock due to maxAMShare limitation and container reservation |  Critical | fairscheduler | zhihai xu | zhihai xu |
| [HDFS-8539](https://issues.apache.org/jira/browse/HDFS-8539) | Hdfs doesnt have class 'debug' in windows |  Major | scripts | Sumana Sathish | Anu Engineer |
| [YARN-3780](https://issues.apache.org/jira/browse/YARN-3780) | Should use equals when compare Resource in RMNodeImpl#ReconnectNodeTransition |  Minor | resourcemanager | zhihai xu | zhihai xu |
| [YARN-3747](https://issues.apache.org/jira/browse/YARN-3747) | TestLocalDirsHandlerService should delete the created test directory logDir2 |  Minor | test | David Moore | David Moore |
| [HDFS-8554](https://issues.apache.org/jira/browse/HDFS-8554) | TestDatanodeLayoutUpgrade fails on Windows. |  Major | test | Chris Nauroth | Chris Nauroth |
| [YARN-3778](https://issues.apache.org/jira/browse/YARN-3778) | Fix Yarn resourcemanger CLI usage |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12054](https://issues.apache.org/jira/browse/HADOOP-12054) | RPC client should not retry for InvalidToken exceptions |  Critical | ipc | Daryn Sharp | Varun Saxena |
| [HADOOP-11347](https://issues.apache.org/jira/browse/HADOOP-11347) | RawLocalFileSystem#mkdir and create should honor umask |  Major | fs | Chris Nauroth | Varun Saxena |
| [HDFS-8552](https://issues.apache.org/jira/browse/HDFS-8552) | Fix hdfs CLI usage message for namenode and zkfc |  Major | . | Xiaoyu Yao | Brahma Reddy Battula |
| [HADOOP-12073](https://issues.apache.org/jira/browse/HADOOP-12073) | Azure FileSystem PageBlobInputStream does not return -1 on EOF |  Major | tools | Ivan Mitic | Ivan Mitic |
| [HDFS-8568](https://issues.apache.org/jira/browse/HDFS-8568) | TestClusterId#testFormatWithEmptyClusterIdOption is failing |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-7817](https://issues.apache.org/jira/browse/HADOOP-7817) | RawLocalFileSystem.append() should give FSDataOutputStream with accurate .getPos() |  Major | fs | Kristofer Tomasette | Kanaka Kumar Avvaru |
| [MAPREDUCE-6350](https://issues.apache.org/jira/browse/MAPREDUCE-6350) | JobHistory doesn't support fully-functional search |  Critical | jobhistoryserver | Siqi Li | Siqi Li |
| [MAPREDUCE-6389](https://issues.apache.org/jira/browse/MAPREDUCE-6389) | Fix BaileyBorweinPlouffe CLI usage message |  Trivial | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12052](https://issues.apache.org/jira/browse/HADOOP-12052) | IPC client downgrades all exception types to IOE, breaks callers trying to use them |  Critical | . | Steve Loughran | Brahma Reddy Battula |
| [YARN-3785](https://issues.apache.org/jira/browse/YARN-3785) | Support for Resource as an argument during submitApp call in MockRM test class |  Minor | resourcemanager | Sunil G | Sunil G |
| [HADOOP-12074](https://issues.apache.org/jira/browse/HADOOP-12074) | in Shell.java#runCommand() rethrow InterruptedException as InterruptedIOException |  Minor | . | Lavkesh Lahngir | Lavkesh Lahngir |
| [HDFS-8566](https://issues.apache.org/jira/browse/HDFS-8566) | HDFS documentation about debug commands wrongly identifies them as "hdfs dfs" commands |  Major | documentation | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-8583](https://issues.apache.org/jira/browse/HDFS-8583) | Document that NFS gateway does not work with rpcbind on SLES 11 |  Major | documentation | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8572](https://issues.apache.org/jira/browse/HDFS-8572) | DN always uses HTTP/localhost@REALM principals in SPNEGO |  Blocker | . | Haohui Mai | Haohui Mai |
| [YARN-3794](https://issues.apache.org/jira/browse/YARN-3794) | TestRMEmbeddedElector fails because of ambiguous LOG reference |  Major | test | Chengbing Liu | Chengbing Liu |
| [HDFS-8593](https://issues.apache.org/jira/browse/HDFS-8593) | Calculation of effective layout version mishandles comparison to current layout version in storage. |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-8596](https://issues.apache.org/jira/browse/HDFS-8596) | TestDistributedFileSystem et al tests are broken in branch-2 due to incorrect setting of "datanode" attribute |  Blocker | datanode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-8595](https://issues.apache.org/jira/browse/HDFS-8595) | TestCommitBlockSynchronization fails in branch-2.7 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8607](https://issues.apache.org/jira/browse/HDFS-8607) | TestFileCorruption doesn't work as expected |  Major | test | Walter Su | Walter Su |
| [HADOOP-12001](https://issues.apache.org/jira/browse/HADOOP-12001) | Limiting LDAP search conflicts with posixGroup addition |  Blocker | security | Patrick White | Patrick White |
| [HADOOP-12078](https://issues.apache.org/jira/browse/HADOOP-12078) | The default retry policy does not handle RetriableException correctly |  Critical | ipc | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-6396](https://issues.apache.org/jira/browse/MAPREDUCE-6396) | TestPipeApplication fails by NullPointerException |  Major | test | Akira Ajisaka | Brahma Reddy Battula |
| [HDFS-8576](https://issues.apache.org/jira/browse/HDFS-8576) |  Lease recovery should return true if the lease can be released and the file can be closed |  Major | namenode | J.Andreina | J.Andreina |
| [HDFS-8592](https://issues.apache.org/jira/browse/HDFS-8592) | SafeModeException never get unwrapped |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-12095](https://issues.apache.org/jira/browse/HADOOP-12095) | org.apache.hadoop.fs.shell.TestCount fails |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-4660](https://issues.apache.org/jira/browse/HDFS-4660) | Block corruption can happen during pipeline recovery |  Blocker | datanode | Peng Zhang | Kihwal Lee |
| [HDFS-8548](https://issues.apache.org/jira/browse/HDFS-8548) | Minicluster throws NPE on shutdown |  Major | . | Mike Drob | Surendra Singh Lilhore |
| [YARN-3714](https://issues.apache.org/jira/browse/YARN-3714) | AM proxy filter can not get RM webapp address from yarn.resourcemanager.hostname.rm-id |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8551](https://issues.apache.org/jira/browse/HDFS-8551) | Fix hdfs datanode CLI usage message |  Major | . | Xiaoyu Yao | Brahma Reddy Battula |
| [HADOOP-12076](https://issues.apache.org/jira/browse/HADOOP-12076) | Incomplete Cache Mechanism in CredentialProvider API |  Major | security | Larry McCay | Larry McCay |
| [YARN-3617](https://issues.apache.org/jira/browse/YARN-3617) | Fix WindowsResourceCalculatorPlugin.getCpuFrequency() returning always -1 |  Minor | . | Georg Berendt | J.Andreina |
| [YARN-3804](https://issues.apache.org/jira/browse/YARN-3804) | Both RM are on standBy state when kerberos user not in yarn.admin.acl |  Critical | resourcemanager | Bibin A Chundatt | Varun Saxena |
| [HDFS-8446](https://issues.apache.org/jira/browse/HDFS-8446) | Separate safemode related operations in GetBlockLocations() |  Minor | . | Haohui Mai | Haohui Mai |
| [HDFS-8615](https://issues.apache.org/jira/browse/HDFS-8615) | Correct HTTP method in WebHDFS document |  Major | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [MAPREDUCE-6373](https://issues.apache.org/jira/browse/MAPREDUCE-6373) | The logger reports total input paths but it is referring to input files |  Trivial | . | Andi Chirita Amdocs | Bibin A Chundatt |
| [YARN-3824](https://issues.apache.org/jira/browse/YARN-3824) | Fix two minor nits in member variable properties of YarnConfiguration |  Trivial | yarn | Ray Chiang | Ray Chiang |
| [HADOOP-12100](https://issues.apache.org/jira/browse/HADOOP-12100) | ImmutableFsPermission should not override applyUmask since that method doesn't modify the FsPermission |  Major | . | Robert Kanter | Bibin A Chundatt |
| [YARN-3802](https://issues.apache.org/jira/browse/YARN-3802) | Two RMNodes for the same NodeId are used in RM sometimes after NM is reconnected. |  Major | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-8633](https://issues.apache.org/jira/browse/HDFS-8633) | Fix setting of dfs.datanode.readahead.bytes in hdfs-default.xml to match DFSConfigKeys |  Minor | datanode | Ray Chiang | Ray Chiang |
| [HDFS-8626](https://issues.apache.org/jira/browse/HDFS-8626) | Reserved RBW space is not released if creation of RBW File fails |  Blocker | . | Kanaka Kumar Avvaru | Kanaka Kumar Avvaru |
| [MAPREDUCE-6405](https://issues.apache.org/jira/browse/MAPREDUCE-6405) | NullPointerException in App Attempts page |  Major | . | Siqi Li | Siqi Li |
| [HADOOP-12103](https://issues.apache.org/jira/browse/HADOOP-12103) | Small refactoring of DelegationTokenAuthenticationFilter to allow code sharing |  Minor | security | Yongjun Zhang | Yongjun Zhang |
| [HDFS-8337](https://issues.apache.org/jira/browse/HDFS-8337) | Accessing httpfs via webhdfs doesn't work from a jar with kerberos |  Major | security, webhdfs | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-6403](https://issues.apache.org/jira/browse/MAPREDUCE-6403) | Fix typo in the usage of NNBench |  Trivial | documentation | Akira Ajisaka | Jagadesh Kiran N |
| [HDFS-8480](https://issues.apache.org/jira/browse/HDFS-8480) | Fix performance and timeout issues in HDFS-7929 by using hard-links to preserve old edit logs instead of copying them |  Critical | . | Zhe Zhang | Zhe Zhang |
| [MAPREDUCE-5948](https://issues.apache.org/jira/browse/MAPREDUCE-5948) | org.apache.hadoop.mapred.LineRecordReader does not handle multibyte record delimiters well |  Critical | . | Kris Geusebroek | Akira Ajisaka |
| [HDFS-8542](https://issues.apache.org/jira/browse/HDFS-8542) | WebHDFS getHomeDirectory behavior does not match specification |  Major | webhdfs | Jakob Homan | Kanaka Kumar Avvaru |
| [YARN-3842](https://issues.apache.org/jira/browse/YARN-3842) | NMProxy should retry on NMNotYetReadyException |  Critical | . | Karthik Kambatla | Robert Kanter |
| [YARN-3835](https://issues.apache.org/jira/browse/YARN-3835) | hadoop-yarn-server-resourcemanager test package bundles core-site.xml, yarn-site.xml |  Minor | resourcemanager | Vamsee Yarlagadda | Vamsee Yarlagadda |
| [MAPREDUCE-6406](https://issues.apache.org/jira/browse/MAPREDUCE-6406) | Update FileOutputCommitter.FILEOUTPUTCOMMITTER\_ALGORITHM\_VERSION\_DEFAULT to match mapred-default.xml |  Minor | mrv2 | Ray Chiang | Ray Chiang |
| [MAPREDUCE-6410](https://issues.apache.org/jira/browse/MAPREDUCE-6410) | Aggregated Logs Deletion doesnt work after refreshing Log Retention Settings in secure cluster |  Critical | . | Zhang Wei | Varun Saxena |
| [MAPREDUCE-6400](https://issues.apache.org/jira/browse/MAPREDUCE-6400) | Multiple shuffle transfer fails because input is closed too early |  Blocker | task | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-3809](https://issues.apache.org/jira/browse/YARN-3809) | Failed to launch new attempts because ApplicationMasterLauncher's threads all hang |  Major | resourcemanager | Jun Gong | Jun Gong |
| [YARN-3832](https://issues.apache.org/jira/browse/YARN-3832) | Resource Localization fails on a cluster due to existing cache directories |  Critical | nodemanager | Ranga Swamy | Brahma Reddy Battula |
| [YARN-3790](https://issues.apache.org/jira/browse/YARN-3790) | usedResource from rootQueue metrics may get stale data for FS scheduler after recovering the container |  Major | fairscheduler, test | Rohith Sharma K S | zhihai xu |
| [HADOOP-11958](https://issues.apache.org/jira/browse/HADOOP-11958) | MetricsSystemImpl fails to show backtrace when an error occurs |  Major | . | Jason Lowe | Jason Lowe |
| [HDFS-8646](https://issues.apache.org/jira/browse/HDFS-8646) | Prune cached replicas from DatanodeDescriptor state on replica invalidation |  Major | caching | Andrew Wang | Andrew Wang |
| [YARN-3826](https://issues.apache.org/jira/browse/YARN-3826) | Race condition in ResourceTrackerService leads to wrong diagnostics messages |  Major | resourcemanager | Chengbing Liu | Chengbing Liu |
| [YARN-3745](https://issues.apache.org/jira/browse/YARN-3745) | SerializedException should also try to instantiate internal exception with the default constructor |  Major | . | Lavkesh Lahngir | Lavkesh Lahngir |
| [MAPREDUCE-6413](https://issues.apache.org/jira/browse/MAPREDUCE-6413) | TestLocalJobSubmission is failing with unknown host |  Major | test | Jason Lowe | zhihai xu |
| [HDFS-8665](https://issues.apache.org/jira/browse/HDFS-8665) | Fix replication check in DFSTestUtils#waitForReplication |  Trivial | test | Andrew Wang | Andrew Wang |
| [HADOOP-8151](https://issues.apache.org/jira/browse/HADOOP-8151) | Error handling in snappy decompressor throws invalid exceptions |  Major | io, native | Todd Lipcon | Matt Foley |
| [YARN-3850](https://issues.apache.org/jira/browse/YARN-3850) | NM fails to read files from full disks which can lead to container logs being lost and other issues |  Blocker | log-aggregation, nodemanager | Varun Saxena | Varun Saxena |
| [HDFS-8656](https://issues.apache.org/jira/browse/HDFS-8656) | Preserve compatibility of ClientProtocol#rollingUpgrade after finalization |  Critical | rolling upgrades | Andrew Wang | Andrew Wang |
| [YARN-3859](https://issues.apache.org/jira/browse/YARN-3859) | LeafQueue doesn't print user properly for application add |  Minor | capacityscheduler | Devaraj K | Varun Saxena |
| [HDFS-8681](https://issues.apache.org/jira/browse/HDFS-8681) | BlockScanner is incorrectly disabled by default |  Blocker | datanode | Andrew Wang | Arpit Agarwal |
| [YARN-3860](https://issues.apache.org/jira/browse/YARN-3860) | rmadmin -transitionToActive should check the state of non-target node |  Major | resourcemanager | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8586](https://issues.apache.org/jira/browse/HDFS-8586) | Dead Datanode is allocated for write when client is  from deadnode |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12119](https://issues.apache.org/jira/browse/HADOOP-12119) | hadoop fs -expunge does not work for federated namespace |  Major | . | Vrushali C | J.Andreina |
| [HDFS-8628](https://issues.apache.org/jira/browse/HDFS-8628) | Update missing command option for fetchdt |  Major | documentation | J.Andreina | J.Andreina |
| [YARN-3695](https://issues.apache.org/jira/browse/YARN-3695) | ServerProxy (NMProxy, etc.) shouldn't retry forever for non network exception. |  Major | . | Junping Du | Raju Bairishetti |
| [HADOOP-12089](https://issues.apache.org/jira/browse/HADOOP-12089) | StorageException complaining " no lease ID" when updating FolderLastModifiedTime in WASB |  Major | tools | Duo Xu | Duo Xu |
| [YARN-3770](https://issues.apache.org/jira/browse/YARN-3770) | SerializedException should also handle java.lang.Error |  Major | . | Lavkesh Lahngir | Lavkesh Lahngir |
| [HDFS-8687](https://issues.apache.org/jira/browse/HDFS-8687) | Remove the duplicate usage message from Dfsck.java |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12107](https://issues.apache.org/jira/browse/HADOOP-12107) | long running apps may have a huge number of StatisticsData instances under FileSystem |  Critical | fs | Sangjin Lee | Sangjin Lee |
| [HDFS-8579](https://issues.apache.org/jira/browse/HDFS-8579) | Update HDFS usage with missing options |  Minor | . | J.Andreina | J.Andreina |
| [HADOOP-12154](https://issues.apache.org/jira/browse/HADOOP-12154) | FileSystem#getUsed() returns the file length only from root '/' |  Major | . | tongshiquan | J.Andreina |
| [HDFS-8627](https://issues.apache.org/jira/browse/HDFS-8627) | NPE thrown if unable to fetch token from Namenode |  Major | . | J.Andreina | J.Andreina |
| [YARN-3768](https://issues.apache.org/jira/browse/YARN-3768) | ArrayIndexOutOfBoundsException with empty environment variables |  Major | yarn | Joe Ferner | zhihai xu |
| [HADOOP-10798](https://issues.apache.org/jira/browse/HADOOP-10798) | globStatus() should always return a sorted list of files |  Minor | . | Felix Borchers | Colin P. McCabe |
| [HADOOP-12124](https://issues.apache.org/jira/browse/HADOOP-12124) | Add HTrace support for FsShell |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-12159](https://issues.apache.org/jira/browse/HADOOP-12159) | Move DistCpUtils#compareFs() to org.apache.hadoop.fs.FileUtil and fix for HA namespaces |  Major | . | Ray Chiang | Ray Chiang |
| [MAPREDUCE-6121](https://issues.apache.org/jira/browse/MAPREDUCE-6121) | JobResourceUpdater#compareFs() doesn't handle HA namespaces |  Major | mrv2 | Thomas Graves | Ray Chiang |
| [HADOOP-12116](https://issues.apache.org/jira/browse/HADOOP-12116) | Fix unrecommended syntax usages in hadoop/hdfs/yarn script for cygwin in branch-2 |  Major | scripts | Li Lu | Li Lu |
| [HADOOP-12164](https://issues.apache.org/jira/browse/HADOOP-12164) | Fix TestMove and TestFsShellReturnCode failed to get command name using reflection. |  Minor | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-3823](https://issues.apache.org/jira/browse/YARN-3823) | Fix mismatch in default values for yarn.scheduler.maximum-allocation-vcores property |  Minor | . | Ray Chiang | Ray Chiang |
| [YARN-3830](https://issues.apache.org/jira/browse/YARN-3830) | AbstractYarnScheduler.createReleaseCache may try to clean a null attempt |  Major | scheduler | nijel | nijel |
| [HDFS-8706](https://issues.apache.org/jira/browse/HDFS-8706) | Fix typo in datanode startup options in HDFSCommands.html |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6420](https://issues.apache.org/jira/browse/MAPREDUCE-6420) | Interrupted Exception in LocalContainerLauncher should be logged in warn/info level |  Major | . | Chang Li | Chang Li |
| [MAPREDUCE-6418](https://issues.apache.org/jira/browse/MAPREDUCE-6418) | MRApp should not shutdown LogManager during shutdown |  Major | test | Chang Li | Chang Li |
| [YARN-3793](https://issues.apache.org/jira/browse/YARN-3793) | Several NPEs when deleting local files on NM recovery |  Major | nodemanager | Karthik Kambatla | Varun Saxena |
| [HDFS-8666](https://issues.apache.org/jira/browse/HDFS-8666) | speedup TestMover |  Major | test | Walter Su | Walter Su |
| [HADOOP-12171](https://issues.apache.org/jira/browse/HADOOP-12171) | Shorten overly-long htrace span names for server |  Major | tracing | Colin P. McCabe | Colin P. McCabe |
| [YARN-3875](https://issues.apache.org/jira/browse/YARN-3875) | FSSchedulerNode#reserveResource() doesn't print Application Id properly in log |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3508](https://issues.apache.org/jira/browse/YARN-3508) | Prevent processing preemption events on the main RM dispatcher |  Major | resourcemanager, scheduler | Jason Lowe | Varun Saxena |
| [HADOOP-12173](https://issues.apache.org/jira/browse/HADOOP-12173) | NetworkTopology#add calls NetworkTopology#toString always |  Major | . | Inigo Goiri | Inigo Goiri |
| [HDFS-8577](https://issues.apache.org/jira/browse/HDFS-8577) | Avoid retrying to recover lease on a file which does not exist |  Major | . | J.Andreina | J.Andreina |
| [YARN-3882](https://issues.apache.org/jira/browse/YARN-3882) | AggregatedLogFormat should close aclScanner and ownerScanner after create them. |  Minor | nodemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-6425](https://issues.apache.org/jira/browse/MAPREDUCE-6425) | ShuffleHandler passes wrong "base" parameter to getMapOutputInfo if mapId is not in the cache. |  Major | mrv2, nodemanager | zhihai xu | zhihai xu |
| [HADOOP-12186](https://issues.apache.org/jira/browse/HADOOP-12186) | ActiveStandbyElector shouldn't call monitorLockNodeAsync multiple times |  Major | ha | zhihai xu | zhihai xu |
| [HDFS-8686](https://issues.apache.org/jira/browse/HDFS-8686) | WebHdfsFileSystem#getXAttr(Path p, final String name) doesn't work if namespace is in capitals |  Major | webhdfs | Jagadesh Kiran N | Kanaka Kumar Avvaru |
| [YARN-3837](https://issues.apache.org/jira/browse/YARN-3837) | javadocs of TimelineAuthenticationFilterInitializer give wrong prefix for auth options |  Minor | timelineserver | Steve Loughran | Bibin A Chundatt |
| [HADOOP-12117](https://issues.apache.org/jira/browse/HADOOP-12117) | Potential NPE from Configuration#loadProperty with allowNullValueProperties set. |  Minor | conf | zhihai xu | zhihai xu |
| [MAPREDUCE-6038](https://issues.apache.org/jira/browse/MAPREDUCE-6038) | A boolean may be set error in the Word Count v2.0 in MapReduce Tutorial |  Minor | . | Pei Ma | Tsuyoshi Ozawa |
| [YARN-3892](https://issues.apache.org/jira/browse/YARN-3892) | NPE on RMStateStore#serviceStop when CapacityScheduler#serviceInit fails |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3690](https://issues.apache.org/jira/browse/YARN-3690) | [JDK8] 'mvn site' fails |  Major | api, site | Akira Ajisaka | Brahma Reddy Battula |
| [HADOOP-12202](https://issues.apache.org/jira/browse/HADOOP-12202) | releasedocmaker drops missing component and assignee entries |  Blocker | yetus | Allen Wittenauer | Allen Wittenauer |
| [HDFS-8642](https://issues.apache.org/jira/browse/HDFS-8642) | Make TestFileTruncate more reliable |  Minor | . | Rakesh R | Rakesh R |
| [HADOOP-11878](https://issues.apache.org/jira/browse/HADOOP-11878) | FileContext.java # fixRelativePart should check for not null for a more informative exception |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12201](https://issues.apache.org/jira/browse/HADOOP-12201) | Add tracing to FileSystem#createFileSystem and Globber#glob |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-12200](https://issues.apache.org/jira/browse/HADOOP-12200) | TestCryptoStreamsWithOpensslAesCtrCryptoCodec should be skipped in non-native profile |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [MAPREDUCE-6426](https://issues.apache.org/jira/browse/MAPREDUCE-6426) | TestShuffleHandler#testGetMapOutputInfo is failing |  Major | test | Devaraj K | zhihai xu |
| [HDFS-8729](https://issues.apache.org/jira/browse/HDFS-8729) | Fix testTruncateWithDataNodesRestartImmediately occasionally failed |  Minor | . | Walter Su | Walter Su |
| [YARN-3888](https://issues.apache.org/jira/browse/YARN-3888) | ApplicationMaster link is broken in RM WebUI when appstate is NEW |  Minor | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-8749](https://issues.apache.org/jira/browse/HDFS-8749) | Fix findbugs warning in BlockManager.java |  Minor | . | Akira Ajisaka | Brahma Reddy Battula |
| [HDFS-2956](https://issues.apache.org/jira/browse/HDFS-2956) | calling fetchdt without a --renewer argument throws NPE |  Major | security | Todd Lipcon | Vinayakumar B |
| [HDFS-8751](https://issues.apache.org/jira/browse/HDFS-8751) | Remove setBlocks API from INodeFile and misc code cleanup |  Major | namenode | Zhe Zhang | Zhe Zhang |
| [YARN-3849](https://issues.apache.org/jira/browse/YARN-3849) | Too much of preemption activity causing continuos killing of containers across queues |  Critical | capacityscheduler | Sunil G | Sunil G |
| [YARN-3917](https://issues.apache.org/jira/browse/YARN-3917) | getResourceCalculatorPlugin for the default should intercept all exceptions |  Major | . | Gera Shegalov | Gera Shegalov |
| [YARN-3894](https://issues.apache.org/jira/browse/YARN-3894) | RM startup should fail for wrong CS xml NodeLabel capacity configuration |  Critical | capacityscheduler | Bibin A Chundatt | Bibin A Chundatt |
| [MAPREDUCE-6421](https://issues.apache.org/jira/browse/MAPREDUCE-6421) | Fix findbugs warning in RMContainerAllocator.reduceNodeLabelExpression |  Major | . | Ray Chiang | Brahma Reddy Battula |
| [HADOOP-12191](https://issues.apache.org/jira/browse/HADOOP-12191) | Bzip2Factory is not thread safe |  Major | io | Jason Lowe | Brahma Reddy Battula |
| [HDFS-7608](https://issues.apache.org/jira/browse/HDFS-7608) | hdfs dfsclient  newConnectedPeer has no write timeout |  Major | fuse-dfs, hdfs-client | zhangshilong | Xiaoyu Yao |
| [HADOOP-12153](https://issues.apache.org/jira/browse/HADOOP-12153) | ByteBufferReadable doesn't declare @InterfaceAudience and @InterfaceStability |  Minor | fs | Steve Loughran | Brahma Reddy Battula |
| [HDFS-8778](https://issues.apache.org/jira/browse/HDFS-8778) | TestBlockReportRateLimiting#testLeaseExpiration can deadlock |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-10615](https://issues.apache.org/jira/browse/HADOOP-10615) | FileInputStream in JenkinsHash#main() is never closed |  Minor | . | Ted Yu | Chen He |
| [HADOOP-12240](https://issues.apache.org/jira/browse/HADOOP-12240) | Fix tests requiring native library to be skipped in non-native profile |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3805](https://issues.apache.org/jira/browse/YARN-3805) | Update the documentation of Disk Checker based on YARN-90 |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8767](https://issues.apache.org/jira/browse/HDFS-8767) | RawLocalFileSystem.listStatus() returns null for UNIX pipefile |  Critical | . | Haohui Mai | Kanaka Kumar Avvaru |
| [YARN-3885](https://issues.apache.org/jira/browse/YARN-3885) | ProportionalCapacityPreemptionPolicy doesn't preempt if queue is more than 2 level |  Blocker | yarn | Ajith S | Ajith S |
| [YARN-3453](https://issues.apache.org/jira/browse/YARN-3453) | Fair Scheduler: Parts of preemption logic uses DefaultResourceCalculator even in DRF mode causing thrashing |  Major | fairscheduler | Ashwin Shankar | Arun Suresh |
| [YARN-3535](https://issues.apache.org/jira/browse/YARN-3535) | Scheduler must re-request container resources when RMContainer transitions from ALLOCATED to KILLED |  Critical | capacityscheduler, fairscheduler, resourcemanager | Peng Zhang | Peng Zhang |
| [YARN-3905](https://issues.apache.org/jira/browse/YARN-3905) | Application History Server UI NPEs when accessing apps run after RM restart |  Major | timelineserver | Eric Payne | Eric Payne |
| [HADOOP-12235](https://issues.apache.org/jira/browse/HADOOP-12235) | hadoop-openstack junit & mockito dependencies should be "provided" |  Minor | build, fs/swift | Steve Loughran | Ted Yu |
| [HADOOP-12209](https://issues.apache.org/jira/browse/HADOOP-12209) | Comparable type should be in FileStatus |  Minor | fs | Yong Zhang | Yong Zhang |
| [HADOOP-12088](https://issues.apache.org/jira/browse/HADOOP-12088) | KMSClientProvider uses equalsIgnoreCase("application/json") |  Major | kms | Steve Loughran | Brahma Reddy Battula |
| [HADOOP-12051](https://issues.apache.org/jira/browse/HADOOP-12051) | ProtobufRpcEngine.invoke() should use Exception.toString() over getMessage in logging/span events |  Minor | ipc | Steve Loughran | Varun Saxena |
| [HADOOP-12237](https://issues.apache.org/jira/browse/HADOOP-12237) | releasedocmaker.py doesn't work behind a proxy |  Major | yetus | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-8657](https://issues.apache.org/jira/browse/HDFS-8657) | Update docs for mSNN |  Minor | . | Jesse Yates | Jesse Yates |
| [HDFS-7582](https://issues.apache.org/jira/browse/HDFS-7582) | Enforce maximum number of ACL entries separately per access and default. |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HDFS-8800](https://issues.apache.org/jira/browse/HDFS-8800) | hdfs --daemon stop namenode corrupts logs |  Major | scripts | John Smith | John Smith |
| [YARN-3915](https://issues.apache.org/jira/browse/YARN-3915) | scmadmin help message correction |  Minor | yarn | Bibin A Chundatt | Bibin A Chundatt |
| [MAPREDUCE-5801](https://issues.apache.org/jira/browse/MAPREDUCE-5801) | Uber mode's log message is missing a vcore reason |  Minor | . | Steven K. Wong | Steven K. Wong |
| [HADOOP-11762](https://issues.apache.org/jira/browse/HADOOP-11762) | Enable swift distcp to secure HDFS |  Major | fs/swift | Chen He | Chen He |
| [HDFS-8773](https://issues.apache.org/jira/browse/HDFS-8773) | Few FSNamesystem metrics are not documented in the Metrics page |  Major | documentation | Rakesh R | Rakesh R |
| [YARN-3878](https://issues.apache.org/jira/browse/YARN-3878) | AsyncDispatcher can hang while stopping if it is configured for draining events on stop |  Critical | . | Varun Saxena | Varun Saxena |
| [HDFS-7728](https://issues.apache.org/jira/browse/HDFS-7728) | Avoid updating quota usage while loading edits |  Major | . | Jing Zhao | Jing Zhao |
| [HADOOP-11962](https://issues.apache.org/jira/browse/HADOOP-11962) | Sasl message with MD5 challenge text shouldn't be LOG out even in debug level. |  Critical | ipc, security | Junping Du | Junping Du |
| [HADOOP-12017](https://issues.apache.org/jira/browse/HADOOP-12017) | Hadoop archives command should use configurable replication factor when closing |  Major | . | Zhe Zhang | Bibin A Chundatt |
| [HADOOP-12239](https://issues.apache.org/jira/browse/HADOOP-12239) | StorageException complaining " no lease ID" when updating FolderLastModifiedTime in WASB |  Major | azure, tools | Duo Xu | Duo Xu |
| [YARN-3932](https://issues.apache.org/jira/browse/YARN-3932) | SchedulerApplicationAttempt#getResourceUsageReport and UserInfo should based on total-used-resources |  Major | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3954](https://issues.apache.org/jira/browse/YARN-3954) | TestYarnConfigurationFields#testCompareConfigurationClassAgainstXml fails in trunk |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-2019](https://issues.apache.org/jira/browse/YARN-2019) | Retrospect on decision of making RM crashed if any exception throw in ZKRMStateStore |  Critical | . | Junping Du | Jian He |
| [HDFS-8797](https://issues.apache.org/jira/browse/HDFS-8797) | WebHdfsFileSystem creates too many connections for pread |  Major | webhdfs | Jing Zhao | Jing Zhao |
| [YARN-3941](https://issues.apache.org/jira/browse/YARN-3941) | Proportional Preemption policy should try to avoid sending duplicate PREEMPT\_CONTAINER event to scheduler |  Major | capacityscheduler | Sunil G | Sunil G |
| [YARN-3900](https://issues.apache.org/jira/browse/YARN-3900) | Protobuf layout  of yarn\_security\_token causes errors in other protos that include it |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-3845](https://issues.apache.org/jira/browse/YARN-3845) | Scheduler page does not render RGBA color combinations in IE11 |  Minor | . | Jagadesh Kiran N | Mohammad Shahid Khan |
| [HDFS-8806](https://issues.apache.org/jira/browse/HDFS-8806) | Inconsistent metrics: number of missing blocks with replication factor 1 not properly cleared |  Major | . | Zhe Zhang | Zhe Zhang |
| [YARN-3967](https://issues.apache.org/jira/browse/YARN-3967) | Fetch the application report from the AHS if the RM does not know about it |  Major | . | Mit Desai | Mit Desai |
| [YARN-3957](https://issues.apache.org/jira/browse/YARN-3957) | FairScheduler NPE In FairSchedulerQueueInfo causing scheduler page to return 500 |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-3925](https://issues.apache.org/jira/browse/YARN-3925) | ContainerLogsUtils#getContainerLogFile fails to read container log files from full disks. |  Critical | nodemanager | zhihai xu | zhihai xu |
| [YARN-3973](https://issues.apache.org/jira/browse/YARN-3973) | Recent changes to application priority management break reservation system from YARN-1051 |  Major | resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-3958](https://issues.apache.org/jira/browse/YARN-3958) | TestYarnConfigurationFields should be moved to hadoop-yarn-api module |  Major | . | Varun Saxena | Varun Saxena |
| [HDFS-8810](https://issues.apache.org/jira/browse/HDFS-8810) | Correct assertions in TestDFSInotifyEventInputStream class. |  Minor | test | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-8785](https://issues.apache.org/jira/browse/HDFS-8785) | TestDistributedFileSystem is failing in trunk |  Major | test | Arpit Agarwal | Xiaoyu Yao |
| [YARN-2194](https://issues.apache.org/jira/browse/YARN-2194) | Cgroups cease to work in RHEL7 |  Critical | nodemanager | Wei Yan | Wei Yan |
| [YARN-3846](https://issues.apache.org/jira/browse/YARN-3846) | RM Web UI queue filter is not working |  Major | yarn | Mohammad Shahid Khan | Mohammad Shahid Khan |
| [HADOOP-12245](https://issues.apache.org/jira/browse/HADOOP-12245) | References to misspelled REMAINING\_QUATA in FileSystemShell.md |  Minor | documentation | Gera Shegalov | Gabor Liptak |
| [YARN-3982](https://issues.apache.org/jira/browse/YARN-3982) | container-executor parsing of container-executor.cfg broken in trunk and branch-2 |  Blocker | nodemanager | Varun Vasudev | Varun Vasudev |
| [HADOOP-12175](https://issues.apache.org/jira/browse/HADOOP-12175) | FsShell must load SpanReceiverHost to support tracing |  Major | tracing | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8670](https://issues.apache.org/jira/browse/HDFS-8670) | Better to exclude decommissioned nodes for namenode NodeUsage JMX |  Major | . | Ming Ma | J.Andreina |
| [HADOOP-10945](https://issues.apache.org/jira/browse/HADOOP-10945) | 4-digit octal umask permissions throws a parse error |  Major | fs | Jason Lowe | Chang Li |
| [YARN-3919](https://issues.apache.org/jira/browse/YARN-3919) | NPEs' while stopping service after exception during CommonNodeLabelsManager#start |  Trivial | resourcemanager | Varun Saxena | Varun Saxena |
| [YARN-3963](https://issues.apache.org/jira/browse/YARN-3963) | AddNodeLabel on duplicate label addition shows success |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [MAPREDUCE-6433](https://issues.apache.org/jira/browse/MAPREDUCE-6433) | launchTime may be negative |  Major | jobhistoryserver, mrv2 | Allen Wittenauer | zhihai xu |
| [YARN-3990](https://issues.apache.org/jira/browse/YARN-3990) | AsyncDispatcher may overloaded with RMAppNodeUpdateEvent when Node is connected/disconnected |  Critical | resourcemanager | Rohith Sharma K S | Bibin A Chundatt |
| [HDFS-6860](https://issues.apache.org/jira/browse/HDFS-6860) | BlockStateChange logs are too noisy |  Major | namenode | Arpit Agarwal | Chang Li |
| [HADOOP-12268](https://issues.apache.org/jira/browse/HADOOP-12268) | AbstractContractAppendTest#testRenameFileBeingAppended misses rename operation. |  Major | test | zhihai xu | zhihai xu |
| [HDFS-8847](https://issues.apache.org/jira/browse/HDFS-8847) | change TestHDFSContractAppend to not override testRenameFileBeingAppended method. |  Major | test | zhihai xu | zhihai xu |
| [HDFS-8850](https://issues.apache.org/jira/browse/HDFS-8850) | VolumeScanner thread exits with exception if there is no block pool to be scanned but there are suspicious blocks |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-8844](https://issues.apache.org/jira/browse/HDFS-8844) | TestHDFSCLI does not cleanup the test directory |  Minor | test | Akira Ajisaka | Masatake Iwasaki |
| [HADOOP-12274](https://issues.apache.org/jira/browse/HADOOP-12274) | Remove direct download link from BUILDING.txt |  Minor | documentation | Caleb Severn | Caleb Severn |
| [HADOOP-12302](https://issues.apache.org/jira/browse/HADOOP-12302) | Fix native compilation on Windows after HADOOP-7824 |  Blocker | . | Vinayakumar B | Vinayakumar B |
| [YARN-3983](https://issues.apache.org/jira/browse/YARN-3983) | Make CapacityScheduler to easier extend application allocation logic |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-12304](https://issues.apache.org/jira/browse/HADOOP-12304) | Applications using FileContext fail with the default file system configured to be wasb/s3/etc. |  Blocker | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-11932](https://issues.apache.org/jira/browse/HADOOP-11932) |  MetricsSinkAdapter hangs when being stopped |  Critical | . | Jian He | Brahma Reddy Battula |
| [MAPREDUCE-6257](https://issues.apache.org/jira/browse/MAPREDUCE-6257) | Document encrypted spills |  Major | security | Allen Wittenauer | Bibin A Chundatt |
| [HDFS-8856](https://issues.apache.org/jira/browse/HDFS-8856) | Make LeaseManager#countPath O(1) |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8772](https://issues.apache.org/jira/browse/HDFS-8772) | Fix TestStandbyIsHot#testDatanodeRestarts which occasionally fails |  Major | . | Walter Su | Walter Su |
| [YARN-3966](https://issues.apache.org/jira/browse/YARN-3966) | Fix excessive loggings in CapacityScheduler |  Major | . | Jian He | Jian He |
| [HDFS-8866](https://issues.apache.org/jira/browse/HDFS-8866) | Typo in docs: Rumtime -\> Runtime |  Trivial | documentation, webhdfs | Jakob Homan | Gabor Liptak |
| [YARN-3999](https://issues.apache.org/jira/browse/YARN-3999) | RM hangs on draining events |  Major | . | Jian He | Jian He |
| [YARN-4026](https://issues.apache.org/jira/browse/YARN-4026) | FiCaSchedulerApp: ContainerAllocator should be able to choose how to order pending resource requests |  Major | . | Wangda Tan | Wangda Tan |
| [HDFS-8879](https://issues.apache.org/jira/browse/HDFS-8879) | Quota by storage type usage incorrectly initialized upon namenode restart |  Major | namenode | Kihwal Lee | Xiaoyu Yao |
| [HADOOP-12258](https://issues.apache.org/jira/browse/HADOOP-12258) | Need translate java.nio.file.NoSuchFileException to FileNotFoundException to avoid regression |  Critical | fs | zhihai xu | zhihai xu |
| [YARN-4005](https://issues.apache.org/jira/browse/YARN-4005) | Completed container whose app is finished is not removed from NMStateStore |  Major | . | Jun Gong | Jun Gong |
| [YARN-4047](https://issues.apache.org/jira/browse/YARN-4047) | ClientRMService getApplications has high scheduler lock contention |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-3987](https://issues.apache.org/jira/browse/YARN-3987) | am container complete msg ack to NM once RM receive it |  Major | resourcemanager | sandflee | sandflee |
| [HADOOP-12322](https://issues.apache.org/jira/browse/HADOOP-12322) | typos in rpcmetrics.java |  Trivial | ipc | Anu Engineer | Anu Engineer |
| [HDFS-8565](https://issues.apache.org/jira/browse/HDFS-8565) | Typo in dfshealth.html - "Decomissioning" |  Trivial | . | nijel | nijel |
| [MAPREDUCE-5817](https://issues.apache.org/jira/browse/MAPREDUCE-5817) | Mappers get rescheduled on node transition even after all reducers are completed |  Major | applicationmaster | Sangjin Lee | Sangjin Lee |
| [HDFS-8891](https://issues.apache.org/jira/browse/HDFS-8891) | HDFS concat should keep srcs order |  Blocker | . | Yong Zhang | Yong Zhang |
| [MAPREDUCE-6439](https://issues.apache.org/jira/browse/MAPREDUCE-6439) | AM may fail instead of retrying if RM shuts down during the allocate call |  Critical | . | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-8845](https://issues.apache.org/jira/browse/HDFS-8845) | DiskChecker should not traverse the entire tree |  Major | . | Chang Li | Chang Li |
| [HDFS-8852](https://issues.apache.org/jira/browse/HDFS-8852) | HDFS architecture documentation of version 2.x is outdated about append write support |  Major | documentation | Hong Dai Thanh | Ajith S |
| [YARN-3857](https://issues.apache.org/jira/browse/YARN-3857) | Memory leak in ResourceManager with SIMPLE mode |  Critical | resourcemanager | mujunchao | mujunchao |
| [YARN-4028](https://issues.apache.org/jira/browse/YARN-4028) | AppBlock page key update and diagnostics value null on recovery |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-8908](https://issues.apache.org/jira/browse/HDFS-8908) | TestAppendSnapshotTruncate may fail with IOException: Failed to replace a bad datanode |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-8867](https://issues.apache.org/jira/browse/HDFS-8867) | Enable optimized block reports |  Major | . | Rushabh S Shah | Daryn Sharp |
| [HADOOP-12317](https://issues.apache.org/jira/browse/HADOOP-12317) | Applications fail on NM restart on some linux distro because NM container recovery declares AM container as LOST |  Critical | . | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-8863](https://issues.apache.org/jira/browse/HDFS-8863) | The remaining space check in BlockPlacementPolicyDefault is flawed |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-8922](https://issues.apache.org/jira/browse/HDFS-8922) | Link the native\_mini\_dfs test library with libdl, since IBM Java requires it |  Major | build | Ayappan | Ayappan |
| [HDFS-8809](https://issues.apache.org/jira/browse/HDFS-8809) | HDFS fsck reports under construction blocks as "CORRUPT" |  Major | tools | Sudhir Prakash | Jing Zhao |
| [MAPREDUCE-6357](https://issues.apache.org/jira/browse/MAPREDUCE-6357) | MultipleOutputs.write() API should document that output committing is not utilized when input path is absolute |  Major | documentation | Ivan Mitic | Dustin Cote |
| [YARN-3986](https://issues.apache.org/jira/browse/YARN-3986) | getTransferredContainers in AbstractYarnScheduler should be present in YarnScheduler interface instead |  Major | scheduler | Varun Saxena | Varun Saxena |
| [HADOOP-12347](https://issues.apache.org/jira/browse/HADOOP-12347) | Fix mismatch parameter name in javadocs of AuthToken#setMaxInactives |  Trivial | . | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-8942](https://issues.apache.org/jira/browse/HDFS-8942) | Update hyperlink to rack awareness page in HDFS Architecture documentation |  Trivial | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3896](https://issues.apache.org/jira/browse/YARN-3896) | RMNode transitioned from RUNNING to REBOOTED because its response id had not been reset synchronously |  Major | resourcemanager | Jun Gong | Jun Gong |
| [HDFS-8930](https://issues.apache.org/jira/browse/HDFS-8930) | Block report lease may leak if the 2nd full block report comes when NN is still in safemode |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-8932](https://issues.apache.org/jira/browse/HDFS-8932) | NPE thrown in NameNode when try to get "TotalSyncCount" metric before editLogStream initialization |  Major | . | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-12352](https://issues.apache.org/jira/browse/HADOOP-12352) | Delay in checkpointing Trash can leave trash for 2 intervals before deleting |  Trivial | trash | Casey Brotherton | Casey Brotherton |
| [HDFS-8846](https://issues.apache.org/jira/browse/HDFS-8846) | Add a unit test for INotify functionality across a layout version upgrade |  Major | namenode | Zhe Zhang | Zhe Zhang |
| [HDFS-8896](https://issues.apache.org/jira/browse/HDFS-8896) | DataNode object isn't GCed when shutdown, because it has GC root in ShutdownHookManager |  Minor | test | Walter Su | Walter Su |
| [HDFS-8961](https://issues.apache.org/jira/browse/HDFS-8961) | Surpress findbug warnings of o.a.h.hdfs.shortcircuit.DfsClientShmManager.EndpointShmManager in hdfs-client |  Major | . | Haohui Mai | Mingliang Liu |
| [HADOOP-12362](https://issues.apache.org/jira/browse/HADOOP-12362) | Set hadoop.tmp.dir and hadoop.log.dir in pom |  Major | . | Charlie Helin | Charlie Helin |
| [HDFS-8969](https://issues.apache.org/jira/browse/HDFS-8969) | Clean up findbugs warnings for HDFS-8823 and HDFS-8932 |  Major | namenode | Anu Engineer | Anu Engineer |
| [HDFS-8963](https://issues.apache.org/jira/browse/HDFS-8963) | Fix incorrect sign extension of xattr length in HDFS-8900 |  Critical | . | Haohui Mai | Colin P. McCabe |
| [YARN-1556](https://issues.apache.org/jira/browse/YARN-1556) | NPE getting application report with a null appId |  Minor | client | Steve Loughran | Weiwei Yang |
| [MAPREDUCE-6452](https://issues.apache.org/jira/browse/MAPREDUCE-6452) | NPE when intermediate encrypt enabled for LocalRunner |  Major | . | Bibin A Chundatt | zhihai xu |
| [HDFS-8950](https://issues.apache.org/jira/browse/HDFS-8950) | NameNode refresh doesn't remove DataNodes that are no longer in the allowed list |  Major | datanode, namenode | Daniel Templeton | Daniel Templeton |
| [HADOOP-12346](https://issues.apache.org/jira/browse/HADOOP-12346) | Increase some default timeouts / retries for S3a connector |  Major | fs/s3 | Sean Mackrory | Sean Mackrory |
| [HADOOP-12359](https://issues.apache.org/jira/browse/HADOOP-12359) | hadoop fs -getmerge doc is wrong |  Major | documentation | Daniel Templeton | Jagadesh Kiran N |
| [HADOOP-10365](https://issues.apache.org/jira/browse/HADOOP-10365) | BufferedOutputStream in FileUtil#unpackEntries() should be closed in finally block |  Minor | util | Ted Yu | Kiran Kumar M R |
| [HDFS-8995](https://issues.apache.org/jira/browse/HDFS-8995) | Flaw in registration bookeeping can make DN die on reconnect |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-8388](https://issues.apache.org/jira/browse/HDFS-8388) | Time and Date format need to be in sync in NameNode UI page |  Minor | . | Archana T | Surendra Singh Lilhore |
| [YARN-4073](https://issues.apache.org/jira/browse/YARN-4073) | Unused ApplicationACLsManager in ContainerManagerImpl |  Trivial | . | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9003](https://issues.apache.org/jira/browse/HDFS-9003) | ForkJoin thread pool leaks |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-8885](https://issues.apache.org/jira/browse/HDFS-8885) | ByteRangeInputStream used in webhdfs does not override available() |  Minor | webhdfs | Shradha Revankar | Shradha Revankar |
| [HADOOP-10318](https://issues.apache.org/jira/browse/HADOOP-10318) | Incorrect reference to nodeFile in RumenToSLSConverter error message |  Minor | . | Ted Yu | Wei Yan |
| [HADOOP-12213](https://issues.apache.org/jira/browse/HADOOP-12213) | Interrupted exception can occur when Client#stop is called |  Minor | . | Oleg Zhurakousky | Kuhu Shukla |
| [HDFS-9009](https://issues.apache.org/jira/browse/HDFS-9009) | Send metrics logs to NullAppender by default |  Major | logging | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8964](https://issues.apache.org/jira/browse/HDFS-8964) | When validating the edit log, do not read at or beyond the file offset that is being written |  Major | journal-node, namenode | Zhe Zhang | Zhe Zhang |
| [HDFS-8939](https://issues.apache.org/jira/browse/HDFS-8939) | Test(S)WebHdfsFileContextMainOperations failing on branch-2 |  Major | webhdfs | Jakob Homan | Chris Nauroth |
| [YARN-4103](https://issues.apache.org/jira/browse/YARN-4103) | RM WebServices missing scheme for appattempts logLinks |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-4105](https://issues.apache.org/jira/browse/YARN-4105) | Capacity Scheduler headroom for DRF is wrong |  Major | capacityscheduler | Chang Li | Chang Li |
| [MAPREDUCE-6442](https://issues.apache.org/jira/browse/MAPREDUCE-6442) | Stack trace is missing when error occurs in client protocol provider's constructor |  Major | client | Chang Li | Chang Li |
| [YARN-3591](https://issues.apache.org/jira/browse/YARN-3591) | Resource localization on a bad disk causes subsequent containers failure |  Major | . | Lavkesh Lahngir | Lavkesh Lahngir |
| [YARN-4121](https://issues.apache.org/jira/browse/YARN-4121) | Typos in capacity scheduler documentation. |  Trivial | documentation | Kai Sasaki | Kai Sasaki |
| [YARN-4096](https://issues.apache.org/jira/browse/YARN-4096) | App local logs are leaked if log aggregation fails to initialize for the app |  Major | log-aggregation, nodemanager | Jason Lowe | Jason Lowe |
| [HADOOP-12388](https://issues.apache.org/jira/browse/HADOOP-12388) | Fix components' version information in the web page 'About the Cluster' |  Minor | util | Jun Gong | Jun Gong |
| [HDFS-9033](https://issues.apache.org/jira/browse/HDFS-9033) | dfsadmin -metasave prints "NaN" for cache used% |  Major | . | Archana T | Brahma Reddy Battula |
| [HDFS-8716](https://issues.apache.org/jira/browse/HDFS-8716) | introduce a new config specifically for safe mode block count |  Major | . | Chang Li | Chang Li |
| [HDFS-8581](https://issues.apache.org/jira/browse/HDFS-8581) | ContentSummary on / skips further counts on yielding lock |  Minor | namenode | tongshiquan | J.Andreina |
| [HDFS-6763](https://issues.apache.org/jira/browse/HDFS-6763) | Initialize file system-wide quota once on transitioning to active |  Major | ha, namenode | Daryn Sharp | Kihwal Lee |
| [MAPREDUCE-6474](https://issues.apache.org/jira/browse/MAPREDUCE-6474) | ShuffleHandler can possibly exhaust nodemanager file descriptors |  Major | mrv2, nodemanager | Nathan Roberts | Kuhu Shukla |
| [YARN-4106](https://issues.apache.org/jira/browse/YARN-4106) | NodeLabels for NM in distributed mode is not updated even after clusterNodelabel addition in RM |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4115](https://issues.apache.org/jira/browse/YARN-4115) | Reduce loglevel of ContainerManagementProtocolProxy to Debug |  Minor | . | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-12348](https://issues.apache.org/jira/browse/HADOOP-12348) | MetricsSystemImpl creates MetricsSourceAdapter with wrong time unit parameter. |  Major | metrics | zhihai xu | zhihai xu |
| [HDFS-9042](https://issues.apache.org/jira/browse/HDFS-9042) | Update document for the Storage policy name |  Minor | documentation | J.Andreina | J.Andreina |
| [HDFS-9036](https://issues.apache.org/jira/browse/HDFS-9036) | In BlockPlacementPolicyWithNodeGroup#chooseLocalStorage , random node is selected eventhough fallbackToLocalRack is true. |  Major | . | J.Andreina | J.Andreina |
| [HADOOP-12407](https://issues.apache.org/jira/browse/HADOOP-12407) | Test failing: hadoop.ipc.TestSaslRPC |  Critical | security, test | Steve Loughran | Steve Loughran |
| [HADOOP-12087](https://issues.apache.org/jira/browse/HADOOP-12087) | [JDK8] Fix javadoc errors caused by incorrect or illegal tags |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-4126](https://issues.apache.org/jira/browse/YARN-4126) | RM should not issue delegation tokens in unsecure mode |  Major | . | Jian He | Bibin A Chundatt |
| [HDFS-9069](https://issues.apache.org/jira/browse/HDFS-9069) | TestNameNodeMetricsLogger failing -port in use |  Critical | test | Steve Loughran | Steve Loughran |
| [HDFS-8996](https://issues.apache.org/jira/browse/HDFS-8996) | Consolidate validateLog and scanLog in FJM#EditLogFile |  Major | journal-node, namenode | Zhe Zhang | Zhe Zhang |
| [YARN-4151](https://issues.apache.org/jira/browse/YARN-4151) | Fix findbugs errors in hadoop-yarn-server-common module |  Major | . | MENG DING | MENG DING |
| [HDFS-9067](https://issues.apache.org/jira/browse/HDFS-9067) | o.a.h.hdfs.server.datanode.fsdataset.impl.TestLazyWriter is failing in trunk |  Critical | . | Haohui Mai | Surendra Singh Lilhore |
| [MAPREDUCE-6472](https://issues.apache.org/jira/browse/MAPREDUCE-6472) | MapReduce AM should have java.io.tmpdir=./tmp to be consistent with tasks |  Major | mr-am | Jason Lowe | Naganarasimha G R |
| [HADOOP-12374](https://issues.apache.org/jira/browse/HADOOP-12374) | Description of hdfs expunge command is confusing |  Major | documentation, trash | Weiwei Yang | Weiwei Yang |
| [YARN-4078](https://issues.apache.org/jira/browse/YARN-4078) | getPendingResourceRequestForAttempt is present in AbstractYarnScheduler should be present in YarnScheduler interface instead |  Minor | resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9072](https://issues.apache.org/jira/browse/HDFS-9072) | Fix random failures in TestJMXGet |  Critical | test | J.Andreina | J.Andreina |
| [HDFS-9073](https://issues.apache.org/jira/browse/HDFS-9073) | Fix failures in TestLazyPersistLockedMemory#testReleaseOnEviction |  Critical | test | J.Andreina | J.Andreina |
| [YARN-3433](https://issues.apache.org/jira/browse/YARN-3433) | Jersey tests failing with Port in Use -again |  Critical | build, test | Steve Loughran | Brahma Reddy Battula |
| [HADOOP-12417](https://issues.apache.org/jira/browse/HADOOP-12417) | TestWebDelegationToken failing with port in use |  Major | test | Steve Loughran | Mingliang Liu |
| [MAPREDUCE-6481](https://issues.apache.org/jira/browse/MAPREDUCE-6481) | LineRecordReader may give incomplete record and wrong position/key information for uncompressed input sometimes. |  Critical | mrv2 | zhihai xu | zhihai xu |
| [MAPREDUCE-5002](https://issues.apache.org/jira/browse/MAPREDUCE-5002) | AM could potentially allocate a reduce container to a map attempt |  Major | mr-am | Jason Lowe | Chang Li |
| [MAPREDUCE-5982](https://issues.apache.org/jira/browse/MAPREDUCE-5982) | Task attempts that fail from the ASSIGNED state can disappear |  Major | mr-am | Jason Lowe | Chang Li |
| [HADOOP-12386](https://issues.apache.org/jira/browse/HADOOP-12386) | RetryPolicies.RETRY\_FOREVER should be able to specify a retry interval |  Major | . | Wangda Tan | Sunil G |
| [YARN-3697](https://issues.apache.org/jira/browse/YARN-3697) | FairScheduler: ContinuousSchedulingThread can fail to shutdown |  Critical | fairscheduler | zhihai xu | zhihai xu |
| [HDFS-6955](https://issues.apache.org/jira/browse/HDFS-6955) | DN should reserve disk space for a full block when creating tmp files |  Major | datanode | Arpit Agarwal | Kanaka Kumar Avvaru |
| [HDFS-5802](https://issues.apache.org/jira/browse/HDFS-5802) | NameNode does not check for inode type before traversing down a path |  Trivial | namenode | Harsh J | Xiao Chen |
| [HDFS-9063](https://issues.apache.org/jira/browse/HDFS-9063) | Correctly handle snapshot path for getContentSummary |  Major | namenode | Jing Zhao | Jing Zhao |
| [MAPREDUCE-6460](https://issues.apache.org/jira/browse/MAPREDUCE-6460) | TestRMContainerAllocator.testAttemptNotFoundCausesRMCommunicatorException fails |  Major | test | zhihai xu | zhihai xu |
| [YARN-4167](https://issues.apache.org/jira/browse/YARN-4167) | NPE on RMActiveServices#serviceStop when store is null |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4188](https://issues.apache.org/jira/browse/YARN-4188) | MoveApplicationAcrossQueuesResponse should be an abstract class |  Minor | resourcemanager | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-9043](https://issues.apache.org/jira/browse/HDFS-9043) | Doc updation for commands in HDFS Federation |  Minor | documentation | J.Andreina | J.Andreina |
| [HDFS-9013](https://issues.apache.org/jira/browse/HDFS-9013) | Deprecate NameNodeMXBean#getNNStarted in branch2 and remove from trunk |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-3975](https://issues.apache.org/jira/browse/YARN-3975) | WebAppProxyServlet should not redirect to RM page if AHS is enabled |  Major | . | Mit Desai | Mit Desai |
| [HADOOP-12438](https://issues.apache.org/jira/browse/HADOOP-12438) | Reset RawLocalFileSystem.useDeprecatedFileStatus in TestLocalFileSystem |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [HDFS-9128](https://issues.apache.org/jira/browse/HDFS-9128) | TestWebHdfsFileContextMainOperations and TestSWebHdfsFileContextMainOperations fail due to invalid HDFS path on Windows. |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [YARN-4152](https://issues.apache.org/jira/browse/YARN-4152) | NM crash with NPE when LogAggregationService#stopContainer called for absent container |  Critical | log-aggregation, nodemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4044](https://issues.apache.org/jira/browse/YARN-4044) | Running applications information changes such as movequeue is not published to TimeLine server |  Critical | resourcemanager, timelineserver | Sunil G | Sunil G |
| [HDFS-9076](https://issues.apache.org/jira/browse/HDFS-9076) | Log full path instead of inodeId in DFSClient#closeAllFilesBeingWritten() |  Major | hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [MAPREDUCE-6484](https://issues.apache.org/jira/browse/MAPREDUCE-6484) | Yarn Client uses local address instead of RM address as token renewer in a secure cluster when RM HA is enabled. |  Major | client, security | zhihai xu | zhihai xu |
| [HADOOP-12437](https://issues.apache.org/jira/browse/HADOOP-12437) | Allow SecurityUtil to lookup alternate hostnames |  Major | net, security | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-12252](https://issues.apache.org/jira/browse/HADOOP-12252) | LocalDirAllocator should not throw NPE with empty string configuration. |  Minor | fs | zhihai xu | zhihai xu |
| [YARN-3624](https://issues.apache.org/jira/browse/YARN-3624) | ApplicationHistoryServer reverses the order of the filters it gets |  Major | timelineserver | Mit Desai | Mit Desai |
| [HDFS-9123](https://issues.apache.org/jira/browse/HDFS-9123) | Copying from the root to a subdirectory should be forbidden |  Minor | fs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-6480](https://issues.apache.org/jira/browse/MAPREDUCE-6480) | archive-logs tool may miss applications |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-9107](https://issues.apache.org/jira/browse/HDFS-9107) | Prevent NN's unrecoverable death spiral after full GC |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-9133](https://issues.apache.org/jira/browse/HDFS-9133) | ExternalBlockReader and ReplicaAccessor need to return -1 on read when at EOF |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-9080](https://issues.apache.org/jira/browse/HDFS-9080) | update htrace version to 4.0.1 |  Major | tracing | Colin P. McCabe | Colin P. McCabe |
| [YARN-4204](https://issues.apache.org/jira/browse/YARN-4204) | ConcurrentModificationException in FairSchedulerQueueInfo |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-9106](https://issues.apache.org/jira/browse/HDFS-9106) | Transfer failure during pipeline recovery causes permanent write failures |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9147](https://issues.apache.org/jira/browse/HDFS-9147) | Fix the setting of visibleLength in ExternalBlockReader |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-6492](https://issues.apache.org/jira/browse/MAPREDUCE-6492) | AsyncDispatcher exit with NPE on TaskAttemptImpl#sendJHStartEventForAssignedFailTask |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-12440](https://issues.apache.org/jira/browse/HADOOP-12440) | TestRPC#testRPCServerShutdown did not produce the desired thread states before shutting down |  Minor | . | Xiao Chen | Xiao Chen |
| [HDFS-9092](https://issues.apache.org/jira/browse/HDFS-9092) | Nfs silently drops overlapping write requests and causes data copying to fail |  Major | nfs | Yongjun Zhang | Yongjun Zhang |
| [YARN-4180](https://issues.apache.org/jira/browse/YARN-4180) | AMLauncher does not retry on failures when talking to NM |  Critical | resourcemanager | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-9141](https://issues.apache.org/jira/browse/HDFS-9141) | Thread leak in Datanode#refreshVolumes |  Major | datanode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-4066](https://issues.apache.org/jira/browse/YARN-4066) | Large number of queues choke fair scheduler |  Major | fairscheduler | Johan Gustavsson | Johan Gustavsson |
| [HADOOP-12447](https://issues.apache.org/jira/browse/HADOOP-12447) | Clean up some htrace integration issues |  Major | tracing | Colin P. McCabe | Colin P. McCabe |
| [HDFS-9174](https://issues.apache.org/jira/browse/HDFS-9174) | Fix findbugs warnings in FSOutputSummer.tracer and DirectoryScanner$ReportCompiler.currentThread |  Critical | . | Yi Liu | Yi Liu |
| [HDFS-9001](https://issues.apache.org/jira/browse/HDFS-9001) | DFSUtil.getNsServiceRpcUris() can return too many entries in a non-HA, non-federated cluster |  Major | . | Daniel Templeton | Daniel Templeton |
| [HADOOP-12448](https://issues.apache.org/jira/browse/HADOOP-12448) | TestTextCommand: use mkdirs rather than mkdir to create test directory |  Major | test | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-6494](https://issues.apache.org/jira/browse/MAPREDUCE-6494) | Permission issue when running archive-logs tool as different users |  Major | . | Robert Kanter | Robert Kanter |
| [MAPREDUCE-6497](https://issues.apache.org/jira/browse/MAPREDUCE-6497) | Fix wrong value of JOB\_FINISHED event in JobHistoryEventHandler |  Major | . | Shinichi Yamashita | Shinichi Yamashita |
| [HADOOP-10296](https://issues.apache.org/jira/browse/HADOOP-10296) | Incorrect null check in SwiftRestClient#buildException() |  Minor | . | Ted Yu | Kanaka Kumar Avvaru |
| [HADOOP-8437](https://issues.apache.org/jira/browse/HADOOP-8437) | getLocalPathForWrite should throw IOException for invalid paths |  Major | fs | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6485](https://issues.apache.org/jira/browse/MAPREDUCE-6485) | MR job hanged forever because all resources are taken up by reducers and the last map attempt never get resource to run |  Critical | applicationmaster | Bob.zhao | Xianyin Xin |
| [HDFS-9185](https://issues.apache.org/jira/browse/HDFS-9185) | Fix null tracer in ErasureCodingWorker |  Critical | erasure-coding | Rakesh R | Rakesh R |
| [HDFS-9191](https://issues.apache.org/jira/browse/HDFS-9191) | Typo in  Hdfs.java.  NoSuchElementException is misspelled |  Trivial | documentation | Catherine Palmer | Catherine Palmer |
| [HDFS-9100](https://issues.apache.org/jira/browse/HDFS-9100) | HDFS Balancer does not respect dfs.client.use.datanode.hostname |  Major | balancer & mover | Yongjun Zhang | Casey Brotherton |
| [YARN-3619](https://issues.apache.org/jira/browse/YARN-3619) | ContainerMetrics unregisters during getMetrics and leads to ConcurrentModificationException |  Major | nodemanager | Jason Lowe | zhihai xu |
| [HDFS-9193](https://issues.apache.org/jira/browse/HDFS-9193) | Fix incorrect references the usages of the DN in dfshealth.js |  Minor | . | Chang Li | Chang Li |
| [HADOOP-11098](https://issues.apache.org/jira/browse/HADOOP-11098) | [JDK8] Max Non Heap Memory default changed between JDK7 and 8 |  Major | . | Travis Thompson | Tsuyoshi Ozawa |
| [HADOOP-12441](https://issues.apache.org/jira/browse/HADOOP-12441) | Fix kill command behavior under some Linux distributions. |  Critical | . | Wangda Tan | Wangda Tan |
| [HADOOP-12452](https://issues.apache.org/jira/browse/HADOOP-12452) | Fix tracing documention reflecting the update to htrace-4 |  Major | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-4176](https://issues.apache.org/jira/browse/YARN-4176) | Resync NM nodelabels with RM periodically for distributed nodelabels |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-7899](https://issues.apache.org/jira/browse/HDFS-7899) | Improve EOF error message |  Minor | hdfs-client | Harsh J | Jagadesh Kiran N |
| [MAPREDUCE-6503](https://issues.apache.org/jira/browse/MAPREDUCE-6503) | archive-logs tool should use HADOOP\_PREFIX instead of HADOOP\_HOME |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-4209](https://issues.apache.org/jira/browse/YARN-4209) | RMStateStore FENCED state doesn't work due to updateFencedState called by stateMachine.doTransition |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-9196](https://issues.apache.org/jira/browse/HDFS-9196) | Fix TestWebHdfsContentLength |  Major | . | Tsuyoshi Ozawa | Masatake Iwasaki |
| [HDFS-9178](https://issues.apache.org/jira/browse/HDFS-9178) | Slow datanode I/O can cause a wrong node to be marked bad |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9182](https://issues.apache.org/jira/browse/HDFS-9182) | Cleanup the findbugs and other issues after HDFS EC merged to trunk. |  Critical | erasure-coding | Yi Liu | Uma Maheswara Rao G |
| [HADOOP-12465](https://issues.apache.org/jira/browse/HADOOP-12465) | Incorrect javadoc in WritableUtils.java |  Minor | documentation | Martin Petricek | Jagadesh Kiran N |
| [HDFS-9176](https://issues.apache.org/jira/browse/HDFS-9176) | TestDirectoryScanner#testThrottling often fails. |  Minor | test | Yi Liu | Daniel Templeton |
| [HDFS-9137](https://issues.apache.org/jira/browse/HDFS-9137) | DeadLock between DataNode#refreshVolumes and BPOfferService#registrationSucceeded |  Major | datanode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-8164](https://issues.apache.org/jira/browse/HDFS-8164) | cTime is 0 in VERSION file for newly formatted NameNode. |  Minor | namenode | Chris Nauroth | Xiao Chen |
| [HDFS-9204](https://issues.apache.org/jira/browse/HDFS-9204) | DatanodeDescriptor#PendingReplicationWithoutTargets is wrongly calculated |  Major | . | Jing Zhao | Mingliang Liu |
| [HDFS-9217](https://issues.apache.org/jira/browse/HDFS-9217) | Fix broken findbugsExcludeFile.xml for hadoop-hdfs-client module |  Critical | build | Mingliang Liu | Mingliang Liu |
| [YARN-4235](https://issues.apache.org/jira/browse/YARN-4235) | FairScheduler PrimaryGroup does not handle empty groups returned for a user |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-6302](https://issues.apache.org/jira/browse/MAPREDUCE-6302) | Preempt reducers after a configurable timeout irrespective of headroom |  Critical | . | mai shurong | Karthik Kambatla |
| [HDFS-9142](https://issues.apache.org/jira/browse/HDFS-9142) | Separating Configuration object for namenode(s) in MiniDFSCluster |  Major | . | Siqi Li | Siqi Li |
| [HDFS-8941](https://issues.apache.org/jira/browse/HDFS-8941) | DistributedFileSystem listCorruptFileBlocks API should resolve relative path |  Major | hdfs-client | Rakesh R | Rakesh R |
| [HDFS-9222](https://issues.apache.org/jira/browse/HDFS-9222) | Add hadoop-hdfs-client as a dependency of hadoop-hdfs-native-client |  Major | . | Haohui Mai | Mingliang Liu |
| [YARN-4201](https://issues.apache.org/jira/browse/YARN-4201) | AMBlacklist does not work for minicluster |  Major | resourcemanager | Jun Gong | Jun Gong |
| [HDFS-9215](https://issues.apache.org/jira/browse/HDFS-9215) | Suppress the RAT warnings in hdfs-native-client module |  Minor | . | Haohui Mai | Haohui Mai |
| [HDFS-9224](https://issues.apache.org/jira/browse/HDFS-9224) | TestFileTruncate fails intermittently with BindException |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4017](https://issues.apache.org/jira/browse/YARN-4017) | container-executor overuses PATH\_MAX |  Major | nodemanager | Allen Wittenauer | Sidharta Seethana |
| [HDFS-8676](https://issues.apache.org/jira/browse/HDFS-8676) | Delayed rolling upgrade finalization can cause heartbeat expiration and write failures |  Critical | . | Kihwal Lee | Walter Su |
| [HADOOP-12474](https://issues.apache.org/jira/browse/HADOOP-12474) | MiniKMS should use random ports for Jetty server by default |  Major | . | Mingliang Liu | Mingliang Liu |
| [HADOOP-12449](https://issues.apache.org/jira/browse/HADOOP-12449) | TestDNS and TestNetUtils failing if no network |  Minor | test | Steve Loughran | Steve Loughran |
| [HADOOP-11515](https://issues.apache.org/jira/browse/HADOOP-11515) | Upgrade jsch lib to jsch-0.1.51 to avoid problems running on java7 |  Major | build | Johannes Zillmann | Tsuyoshi Ozawa |
| [HDFS-9235](https://issues.apache.org/jira/browse/HDFS-9235) | hdfs-native-client build getting errors when built with cmake 2.6 |  Minor | hdfs-client | Eric Payne | Eric Payne |
| [HDFS-8779](https://issues.apache.org/jira/browse/HDFS-8779) | WebUI fails to display block IDs that are larger than 2^53 - 1 |  Minor | webhdfs | Walter Su | Haohui Mai |
| [HDFS-9187](https://issues.apache.org/jira/browse/HDFS-9187) | Fix null pointer error in Globber when FS was not constructed via FileSystem#createFileSystem |  Major | tracing | stack | Colin P. McCabe |
| [HDFS-1172](https://issues.apache.org/jira/browse/HDFS-1172) | Blocks in newly completed files are considered under-replicated too quickly |  Major | namenode | Todd Lipcon | Masatake Iwasaki |
| [YARN-4250](https://issues.apache.org/jira/browse/YARN-4250) | NPE in AppSchedulingInfo#isRequestLabelChanged |  Major | resourcemanager, scheduler | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12364](https://issues.apache.org/jira/browse/HADOOP-12364) | Deleting pid file after stop is causing the daemons to keep restarting |  Minor | . | Siqi Li | Siqi Li |
| [HADOOP-12478](https://issues.apache.org/jira/browse/HADOOP-12478) | Shell.getWinUtilsPath()  has been renamed Shell.getWinutilsPath() |  Critical | util | Steve Loughran | Steve Loughran |
| [HDFS-9220](https://issues.apache.org/jira/browse/HDFS-9220) | Reading small file (\< 512 bytes) that is open for append fails due to incorrect checksum |  Blocker | . | Bogdan Raducanu | Jing Zhao |
| [HADOOP-12479](https://issues.apache.org/jira/browse/HADOOP-12479) | ProtocMojo does not log the reason for a protoc compilation failure. |  Minor | build | Chris Nauroth | Chris Nauroth |
| [HADOOP-11628](https://issues.apache.org/jira/browse/HADOOP-11628) | SPNEGO auth does not work with CNAMEs in JDK8 |  Blocker | security | Daryn Sharp | Daryn Sharp |
| [YARN-2597](https://issues.apache.org/jira/browse/YARN-2597) | MiniYARNCluster should propagate reason for AHS not starting |  Major | test | Steve Loughran | Steve Loughran |
| [YARN-4155](https://issues.apache.org/jira/browse/YARN-4155) | TestLogAggregationService.testLogAggregationServiceWithInterval failing |  Critical | test | Steve Loughran | Bibin A Chundatt |
| [HADOOP-10941](https://issues.apache.org/jira/browse/HADOOP-10941) | Proxy user verification NPEs if remote host is unresolvable |  Critical | ipc, security | Daryn Sharp | Benoy Antony |
| [HADOOP-12483](https://issues.apache.org/jira/browse/HADOOP-12483) | Maintain wrapped SASL ordering for postponed IPC responses |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-9237](https://issues.apache.org/jira/browse/HDFS-9237) | NPE at TestDataNodeVolumeFailureToleration#tearDown |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12464](https://issues.apache.org/jira/browse/HADOOP-12464) | Interrupted client may try to fail-over and retry |  Major | ipc | Kihwal Lee | Kihwal Lee |
| [HADOOP-12493](https://issues.apache.org/jira/browse/HADOOP-12493) | bash unit tests are failing |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [YARN-4270](https://issues.apache.org/jira/browse/YARN-4270) | Limit application resource reservation on nodes for non-node/rack specific requests |  Major | fairscheduler | Arun Suresh | Arun Suresh |
| [HDFS-9208](https://issues.apache.org/jira/browse/HDFS-9208) | Disabling atime may fail clients like distCp |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9270](https://issues.apache.org/jira/browse/HDFS-9270) | TestShortCircuitLocalRead should not leave socket after unit test |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-12418](https://issues.apache.org/jira/browse/HADOOP-12418) | TestRPC.testRPCInterruptedSimple fails intermittently |  Major | test | Steve Loughran | Kihwal Lee |
| [MAPREDUCE-6495](https://issues.apache.org/jira/browse/MAPREDUCE-6495) | Docs for archive-logs tool |  Major | documentation | Robert Kanter | Robert Kanter |
| [HDFS-3059](https://issues.apache.org/jira/browse/HDFS-3059) | ssl-server.xml causes NullPointer |  Minor | datanode, security | Evert Lammerts | Xiao Chen |
| [HDFS-9274](https://issues.apache.org/jira/browse/HDFS-9274) | Default value of dfs.datanode.directoryscan.throttle.limit.ms.per.sec should be consistent |  Trivial | datanode | Yi Liu | Yi Liu |
| [MAPREDUCE-6518](https://issues.apache.org/jira/browse/MAPREDUCE-6518) | Set SO\_KEEPALIVE on shuffle connections |  Major | mrv2, nodemanager | Nathan Roberts | Chang Li |
| [HDFS-9225](https://issues.apache.org/jira/browse/HDFS-9225) | Fix intermittent test failure of TestBlockManager.testBlocksAreNotUnderreplicatedInSingleRack |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-9273](https://issues.apache.org/jira/browse/HDFS-9273) | ACLs on root directory may be lost after NN restart |  Critical | namenode | Xiao Chen | Xiao Chen |
| [YARN-4000](https://issues.apache.org/jira/browse/YARN-4000) | RM crashes with NPE if leaf queue becomes parent queue during restart |  Major | capacityscheduler, resourcemanager | Jason Lowe | Varun Saxena |
| [HADOOP-9692](https://issues.apache.org/jira/browse/HADOOP-9692) | Improving log message when SequenceFile reader throws EOFException on zero-length file |  Major | . | Chu Tong | Zhe Zhang |
| [YARN-4256](https://issues.apache.org/jira/browse/YARN-4256) | YARN fair scheduler vcores with decimal values |  Minor | fairscheduler | Prabhu Joseph | Jun Gong |
| [HADOOP-12484](https://issues.apache.org/jira/browse/HADOOP-12484) | Single File Rename Throws Incorrectly In Potential Race Condition Scenarios |  Major | tools | Gaurav Kanade | Gaurav Kanade |
| [HDFS-9286](https://issues.apache.org/jira/browse/HDFS-9286) | HttpFs does not parse ACL syntax correctly for operation REMOVEACLENTRIES |  Major | fs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4009](https://issues.apache.org/jira/browse/YARN-4009) | CORS support for ResourceManager REST API |  Major | . | Prakash Ramachandran | Varun Vasudev |
| [YARN-4041](https://issues.apache.org/jira/browse/YARN-4041) | Slow delegation token renewal can severely prolong RM recovery |  Major | resourcemanager | Jason Lowe | Sunil G |
| [HDFS-9290](https://issues.apache.org/jira/browse/HDFS-9290) | DFSClient#callAppend() is not backward compatible for slightly older NameNodes |  Blocker | . | Tony Wu | Tony Wu |
| [HDFS-9301](https://issues.apache.org/jira/browse/HDFS-9301) | HDFS clients can't construct HdfsConfiguration instances |  Major | . | Steve Loughran | Mingliang Liu |
| [YARN-4294](https://issues.apache.org/jira/browse/YARN-4294) | [JDK8] Fix javadoc errors caused by wrong reference and illegal tag |  Blocker | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-4289](https://issues.apache.org/jira/browse/YARN-4289) | TestDistributedShell failing with bind exception |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4296](https://issues.apache.org/jira/browse/YARN-4296) | DistributedShell Log.info is not friendly |  Major | applications/distributed-shell | Xiaowei Wang | Xiaowei Wang |
| [YARN-4246](https://issues.apache.org/jira/browse/YARN-4246) | NPE while listing app attempt |  Major | . | Varun Saxena | nijel |
| [YARN-4223](https://issues.apache.org/jira/browse/YARN-4223) | Findbugs warnings in hadoop-yarn-server-nodemanager project |  Minor | nodemanager | Varun Saxena | Varun Saxena |
| [HADOOP-12513](https://issues.apache.org/jira/browse/HADOOP-12513) | Dockerfile lacks initial 'apt-get update' |  Trivial | build | Akihiro Suda | Akihiro Suda |
| [YARN-4284](https://issues.apache.org/jira/browse/YARN-4284) | condition for AM blacklisting is too narrow |  Major | resourcemanager | Sangjin Lee | Sangjin Lee |
| [HDFS-9268](https://issues.apache.org/jira/browse/HDFS-9268) | fuse\_dfs chown crashes when uid is passed as -1 |  Minor | . | Wei-Chiu Chuang | Colin P. McCabe |
| [HDFS-9284](https://issues.apache.org/jira/browse/HDFS-9284) | fsck command should not print exception trace when file not found |  Major | . | Jagadesh Kiran N | Jagadesh Kiran N |
| [HDFS-9305](https://issues.apache.org/jira/browse/HDFS-9305) | Delayed heartbeat processing causes storm of subsequent heartbeats |  Major | datanode | Chris Nauroth | Arpit Agarwal |
| [YARN-4169](https://issues.apache.org/jira/browse/YARN-4169) | Fix racing condition of TestNodeStatusUpdaterForLabels |  Critical | test | Steve Loughran | Naganarasimha G R |
| [YARN-4300](https://issues.apache.org/jira/browse/YARN-4300) | [JDK8] Fix javadoc errors caused by wrong tags |  Blocker | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12515](https://issues.apache.org/jira/browse/HADOOP-12515) | Mockito dependency is missing in hadoop-kafka module |  Major | test | Kai Zheng | Kai Zheng |
| [YARN-4302](https://issues.apache.org/jira/browse/YARN-4302) | SLS not able start due to NPE in SchedulerApplicationAttempt#getResourceUsageReport |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-12178](https://issues.apache.org/jira/browse/HADOOP-12178) | NPE during handling of SASL setup if problem with SASL resolver class |  Minor | ipc | Steve Loughran | Steve Loughran |
| [HADOOP-12494](https://issues.apache.org/jira/browse/HADOOP-12494) | fetchdt stores the token based on token kind instead of token service |  Major | . | HeeSoo Kim | HeeSoo Kim |
| [HADOOP-11685](https://issues.apache.org/jira/browse/HADOOP-11685) | StorageException complaining " no lease ID" during HBase distributed log splitting |  Major | tools | Duo Xu | Duo Xu |
| [HDFS-9231](https://issues.apache.org/jira/browse/HDFS-9231) | fsck doesn't list correct file path when Bad Replicas/Blocks are in a snapshot |  Major | snapshots | Xiao Chen | Xiao Chen |
| [HDFS-9302](https://issues.apache.org/jira/browse/HDFS-9302) | WebHDFS truncate throws NullPointerException if newLength is not provided |  Minor | webhdfs | Karthik Palaniappan | Jagadesh Kiran N |
| [YARN-4251](https://issues.apache.org/jira/browse/YARN-4251) | TestAMRMClientOnRMRestart#testAMRMClientOnAMRMTokenRollOverOnRMRestart is failing |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12519](https://issues.apache.org/jira/browse/HADOOP-12519) | hadoop-azure tests should avoid creating a metrics configuration file in the module root directory. |  Minor | azure, test | Chris Nauroth | Chris Nauroth |
| [HDFS-9279](https://issues.apache.org/jira/browse/HDFS-9279) | Decomissioned capacity should not be considered for configured/used capacity |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-9044](https://issues.apache.org/jira/browse/HDFS-9044) | Give Priority to FavouredNodes , before selecting nodes from FavouredNode's Node Group |  Major | . | J.Andreina | J.Andreina |
| [YARN-4130](https://issues.apache.org/jira/browse/YARN-4130) | Duplicate declaration of ApplicationId in RMAppManager#submitApplication method |  Trivial | resourcemanager | Kai Sasaki | Kai Sasaki |
| [YARN-4288](https://issues.apache.org/jira/browse/YARN-4288) | NodeManager restart should keep retrying to register to RM while connection exception happens during RM failed over. |  Critical | nodemanager | Junping Du | Junping Du |
| [MAPREDUCE-6515](https://issues.apache.org/jira/browse/MAPREDUCE-6515) | Update Application priority in AM side from AM-RM heartbeat |  Major | applicationmaster | Sunil G | Sunil G |
| [HDFS-9332](https://issues.apache.org/jira/browse/HDFS-9332) | Fix Precondition failures from NameNodeEditLogRoller while saving namespace |  Major | . | Andrew Wang | Andrew Wang |
| [YARN-4313](https://issues.apache.org/jira/browse/YARN-4313) | Race condition in MiniMRYarnCluster when getting history server address |  Major | . | Jian He | Jian He |
| [YARN-4320](https://issues.apache.org/jira/browse/YARN-4320) | TestJobHistoryEventHandler fails as AHS in MiniYarnCluster no longer binds to default port 8188 |  Major | . | Varun Saxena | Varun Saxena |
| [MAPREDUCE-6528](https://issues.apache.org/jira/browse/MAPREDUCE-6528) | Memory leak for HistoryFileManager.getJobSummary() |  Critical | jobhistoryserver | Junping Du | Junping Du |
| [MAPREDUCE-6451](https://issues.apache.org/jira/browse/MAPREDUCE-6451) | DistCp has incorrect chunkFilePath for multiple jobs when strategy is dynamic |  Major | distcp | Kuhu Shukla | Kuhu Shukla |
| [MAPREDUCE-6525](https://issues.apache.org/jira/browse/MAPREDUCE-6525) | Fix test failure of TestMiniMRClientCluster.testRestart |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-12533](https://issues.apache.org/jira/browse/HADOOP-12533) | Introduce FileNotFoundException in WASB for read and seek API |  Major | tools | Dushyanth | Dushyanth |
| [HADOOP-12508](https://issues.apache.org/jira/browse/HADOOP-12508) | delete fails with exception when lease is held on blob |  Blocker | azure | Gaurav Kanade | Gaurav Kanade |
| [HDFS-9329](https://issues.apache.org/jira/browse/HDFS-9329) | TestBootstrapStandby#testRateThrottling is flaky because fsimage size is smaller than IO buffer size |  Minor | test | Zhe Zhang | Zhe Zhang |
| [HDFS-9313](https://issues.apache.org/jira/browse/HDFS-9313) | Possible NullPointerException in BlockManager if no excess replica can be chosen |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-12542](https://issues.apache.org/jira/browse/HADOOP-12542) | TestDNS fails on Windows after HADOOP-12437. |  Major | net | Chris Nauroth | Chris Nauroth |
| [YARN-4326](https://issues.apache.org/jira/browse/YARN-4326) | Fix TestDistributedShell timeout as AHS in MiniYarnCluster no longer binds to default port 8188 |  Major | . | MENG DING | MENG DING |
| [HDFS-9289](https://issues.apache.org/jira/browse/HDFS-9289) | Make DataStreamer#block thread safe and verify genStamp in commitBlock |  Critical | . | Chang Li | Chang Li |
| [YARN-4127](https://issues.apache.org/jira/browse/YARN-4127) | RM fail with noAuth error if switched from failover mode to non-failover mode |  Major | resourcemanager | Jian He | Varun Saxena |
| [HDFS-9351](https://issues.apache.org/jira/browse/HDFS-9351) | checkNNStartup() need to be called when fsck calls FSNamesystem.getSnapshottableDirs() |  Major | namenode | Yongjun Zhang | Xiao Chen |
| [HADOOP-12296](https://issues.apache.org/jira/browse/HADOOP-12296) | when setnetgrent returns 0 in linux, exception should be thrown |  Major | . | Chang Li | Chang Li |
| [HDFS-9357](https://issues.apache.org/jira/browse/HDFS-9357) | NN UI renders icons of decommissioned DN incorrectly |  Critical | . | Archana T | Surendra Singh Lilhore |
| [HADOOP-12540](https://issues.apache.org/jira/browse/HADOOP-12540) | TestAzureFileSystemInstrumentation#testClientErrorMetrics fails intermittently due to assumption that a lease error will be thrown. |  Major | azure, test | Chris Nauroth | Gaurav Kanade |
| [HDFS-9360](https://issues.apache.org/jira/browse/HDFS-9360) | Storage type usage isn't updated properly after file deletion |  Major | . | Ming Ma | Ming Ma |
| [HDFS-9378](https://issues.apache.org/jira/browse/HDFS-9378) | hadoop-hdfs-client tests do not write logs. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4937](https://issues.apache.org/jira/browse/HDFS-4937) | ReplicationMonitor can infinite-loop in BlockPlacementPolicyDefault#chooseRandom() |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-9372](https://issues.apache.org/jira/browse/HDFS-9372) | Remove dead code in DataStorage.recoverTransitionRead |  Major | datanode | Duo Zhang | Duo Zhang |
| [MAPREDUCE-3914](https://issues.apache.org/jira/browse/MAPREDUCE-3914) | Mismatched free() / delete / delete [] in HadoopPipes |  Major | pipes | Charles Earl | Joe Mudd |
| [MAPREDUCE-1125](https://issues.apache.org/jira/browse/MAPREDUCE-1125) | SerialUtils.cc: deserializeFloat is out of sync with SerialUtils.hh |  Major | pipes | Simone Leo | Simone Leo |
| [HDFS-9384](https://issues.apache.org/jira/browse/HDFS-9384) | TestWebHdfsContentLength intermittently hangs and fails due to TCP conversation mismatch between client and server. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-6481](https://issues.apache.org/jira/browse/HDFS-6481) | DatanodeManager#getDatanodeStorageInfos() should check the length of storageIDs |  Minor | namenode | Ted Yu | Tsz Wo Nicholas Sze |
| [HDFS-9318](https://issues.apache.org/jira/browse/HDFS-9318) | considerLoad factor can be improved |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-9236](https://issues.apache.org/jira/browse/HDFS-9236) | Missing sanity check for block size during block recovery |  Major | datanode | Tony Wu | Tony Wu |
| [HDFS-9394](https://issues.apache.org/jira/browse/HDFS-9394) | branch-2 hadoop-hdfs-client fails during FileSystem ServiceLoader initialization, because HftpFileSystem is missing. |  Critical | hdfs-client | Chris Nauroth | Mingliang Liu |
| [HADOOP-12553](https://issues.apache.org/jira/browse/HADOOP-12553) | [JDK8] Fix javadoc error caused by illegal tag |  Blocker | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-5763](https://issues.apache.org/jira/browse/MAPREDUCE-5763) | Warn message about httpshuffle in NM logs |  Major | . | Sandy Ryza | Akira Ajisaka |
| [HDFS-9383](https://issues.apache.org/jira/browse/HDFS-9383) | TestByteArrayManager#testByteArrayManager fails |  Major | . | Kihwal Lee | Tsz Wo Nicholas Sze |
| [HDFS-9249](https://issues.apache.org/jira/browse/HDFS-9249) | NPE is thrown if an IOException is thrown in NameNode constructor |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-2261](https://issues.apache.org/jira/browse/HDFS-2261) | AOP unit tests are not getting compiled or run |  Minor | test | Giridharan Kesavan | Haohui Mai |
| [HDFS-9401](https://issues.apache.org/jira/browse/HDFS-9401) | Fix findbugs warnings in BlockRecoveryWorker |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12482](https://issues.apache.org/jira/browse/HADOOP-12482) | Race condition in JMX cache update |  Major | . | Tony Wu | Tony Wu |
| [HDFS-9364](https://issues.apache.org/jira/browse/HDFS-9364) | Unnecessary DNS resolution attempts when creating NameNodeProxies |  Major | ha, performance | Xiao Chen | Xiao Chen |
| [HADOOP-12560](https://issues.apache.org/jira/browse/HADOOP-12560) | Fix sprintf warnings in {{DomainSocket.c}} introduced by HADOOP-12344 |  Major | native | Colin P. McCabe | Mingliang Liu |
| [HDFS-9245](https://issues.apache.org/jira/browse/HDFS-9245) | Fix findbugs warnings in hdfs-nfs/WriteCtx |  Major | nfs | Mingliang Liu | Mingliang Liu |
| [YARN-4241](https://issues.apache.org/jira/browse/YARN-4241) | Fix typo of property name in yarn-default.xml |  Major | documentation | Anthony Rojas | Anthony Rojas |
| [MAPREDUCE-6533](https://issues.apache.org/jira/browse/MAPREDUCE-6533) | testDetermineCacheVisibilities of TestClientDistributedCacheManager is broken |  Major | . | Chang Li | Chang Li |
| [HDFS-9396](https://issues.apache.org/jira/browse/HDFS-9396) | Total files and directories on jmx and web UI on standby is uninitialized |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-6540](https://issues.apache.org/jira/browse/MAPREDUCE-6540) | TestMRTimelineEventHandling fails |  Major | test | Sangjin Lee | Sangjin Lee |
| [YARN-4347](https://issues.apache.org/jira/browse/YARN-4347) | Resource manager fails with Null pointer exception |  Major | yarn | Yesha Vora | Jian He |
| [HADOOP-12545](https://issues.apache.org/jira/browse/HADOOP-12545) | Hadoop javadoc has broken links for AccessControlList, ImpersonationProvider, DefaultImpersonationProvider, and DistCp |  Major | documentation | Arshad Mohammad | Arshad Mohammad |
| [YARN-4354](https://issues.apache.org/jira/browse/YARN-4354) | Public resource localization fails with NPE |  Blocker | nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-9413](https://issues.apache.org/jira/browse/HDFS-9413) | getContentSummary() on standby should throw StandbyException |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9358](https://issues.apache.org/jira/browse/HDFS-9358) | TestNodeCount#testNodeCount timed out |  Major | test | Wei-Chiu Chuang | Masatake Iwasaki |
| [HDFS-9397](https://issues.apache.org/jira/browse/HDFS-9397) | Fix typo for readChecksum() LOG.warn in BlockSender.java |  Trivial | . | Enrique Flores | Enrique Flores |
| [HDFS-9400](https://issues.apache.org/jira/browse/HDFS-9400) | TestRollingUpgradeRollback fails on branch-2. |  Blocker | . | Chris Nauroth | Brahma Reddy Battula |
| [HDFS-9431](https://issues.apache.org/jira/browse/HDFS-9431) | DistributedFileSystem#concat fails if the target path is relative. |  Major | hdfs-client | Kazuho Fujii | Kazuho Fujii |
| [YARN-2859](https://issues.apache.org/jira/browse/YARN-2859) | ApplicationHistoryServer binds to default port 8188 in MiniYARNCluster |  Critical | timelineserver | Hitesh Shah | Vinod Kumar Vavilapalli |
| [YARN-4374](https://issues.apache.org/jira/browse/YARN-4374) | RM capacity scheduler UI rounds user limit factor |  Major | capacityscheduler | Chang Li | Chang Li |
| [YARN-3769](https://issues.apache.org/jira/browse/YARN-3769) | Consider user limit when calculating total pending resource for preemption policy in Capacity Scheduler |  Major | capacityscheduler | Eric Payne | Eric Payne |
| [HDFS-9443](https://issues.apache.org/jira/browse/HDFS-9443) | Disabling HDFS client socket cache causes logging message printed to console for CLI commands. |  Trivial | hdfs-client | Chris Nauroth | Chris Nauroth |
| [HADOOP-11218](https://issues.apache.org/jira/browse/HADOOP-11218) | Add TLSv1.1,TLSv1.2 to KMS, HttpFS, SSLFactory |  Critical | kms | Robert Kanter | Vijay Singh |
| [HADOOP-12467](https://issues.apache.org/jira/browse/HADOOP-12467) | Respect user-defined JAVA\_LIBRARY\_PATH in Windows Hadoop scripts |  Minor | scripts | Radhey Shah | Radhey Shah |
| [HADOOP-12181](https://issues.apache.org/jira/browse/HADOOP-12181) | Fix intermittent test failure of TestZKSignerSecretProvider |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-6885](https://issues.apache.org/jira/browse/HDFS-6885) | Fix wrong use of BytesWritable in FSEditLogOp#RenameOp |  Minor | namenode | Yi Liu | Yi Liu |
| [HADOOP-12098](https://issues.apache.org/jira/browse/HADOOP-12098) | Remove redundant test dependencies in Hadoop Archives |  Minor | . | Varun Saxena | Varun Saxena |
| [HDFS-7897](https://issues.apache.org/jira/browse/HDFS-7897) | Shutdown metrics when stopping JournalNode |  Major | . | zhouyingchao | zhouyingchao |
| [HADOOP-11149](https://issues.apache.org/jira/browse/HADOOP-11149) | Increase the timeout of TestZKFailoverController |  Major | test | Rajat Jain | Steve Loughran |
| [HADOOP-11677](https://issues.apache.org/jira/browse/HADOOP-11677) | Add cookie flags for logs and static contexts |  Major | . | nijel | nijel |
| [HDFS-9356](https://issues.apache.org/jira/browse/HDFS-9356) | Decommissioning node does not have Last Contact value in the UI |  Major | . | Archana T | Surendra Singh Lilhore |
| [HADOOP-12313](https://issues.apache.org/jira/browse/HADOOP-12313) | NPE in JvmPauseMonitor when calling stop() before start() |  Critical | . | Rohith Sharma K S | Gabor Liptak |
| [HDFS-9428](https://issues.apache.org/jira/browse/HDFS-9428) | Fix intermittent failure of TestDNFencing.testQueueingWithAppend |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-9435](https://issues.apache.org/jira/browse/HDFS-9435) | TestBlockRecovery#testRBWReplicas is failing intermittently |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-12577](https://issues.apache.org/jira/browse/HADOOP-12577) | Bump up commons-collections version to 3.2.2 to address a security flaw |  Blocker | build, security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4344](https://issues.apache.org/jira/browse/YARN-4344) | NMs reconnecting with changed capabilities can lead to wrong cluster resource calculations |  Critical | resourcemanager | Varun Vasudev | Varun Vasudev |
| [HADOOP-9822](https://issues.apache.org/jira/browse/HADOOP-9822) | create constant MAX\_CAPACITY in RetryCache rather than hard-coding 16 in RetryCache constructor |  Minor | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-9423](https://issues.apache.org/jira/browse/HDFS-9423) | Fix intermittent failure of TestEditLogTailer |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-4298](https://issues.apache.org/jira/browse/YARN-4298) | Fix findbugs warnings in hadoop-yarn-common |  Minor | . | Varun Saxena | Sunil G |
| [YARN-4387](https://issues.apache.org/jira/browse/YARN-4387) | Fix typo in FairScheduler log message |  Minor | fairscheduler | Xin Wang | Xin Wang |
| [HDFS-6101](https://issues.apache.org/jira/browse/HDFS-6101) | TestReplaceDatanodeOnFailure fails occasionally |  Major | test | Arpit Agarwal | Wei-Chiu Chuang |
| [HDFS-8855](https://issues.apache.org/jira/browse/HDFS-8855) | Webhdfs client leaks active NameNode connections |  Major | webhdfs | Bob Hansen | Xiaobing Zhou |
| [HDFS-8335](https://issues.apache.org/jira/browse/HDFS-8335) | FSNamesystem should construct FSPermissionChecker only if permission is enabled |  Major | . | David Bryson | Gabor Liptak |
| [MAPREDUCE-5883](https://issues.apache.org/jira/browse/MAPREDUCE-5883) | "Total megabyte-seconds" in job counters is slightly misleading |  Minor | . | Nathan Roberts | Nathan Roberts |
| [YARN-4365](https://issues.apache.org/jira/browse/YARN-4365) | FileSystemNodeLabelStore should check for root dir existence on startup |  Major | resourcemanager | Jason Lowe | Kuhu Shukla |
| [HDFS-8807](https://issues.apache.org/jira/browse/HDFS-8807) | dfs.datanode.data.dir does not handle spaces between storageType and URI correctly |  Major | datanode | Anu Engineer | Anu Engineer |
| [HADOOP-12415](https://issues.apache.org/jira/browse/HADOOP-12415) | hdfs and nfs builds broken on -missing compile-time dependency on netty |  Major | nfs | Konstantin Boudnik | Tom Zeng |
| [MAPREDUCE-6553](https://issues.apache.org/jira/browse/MAPREDUCE-6553) | Replace '\u2b05' with '\<-' in rendering job configuration |  Minor | jobhistoryserver | Akira Ajisaka | Gabor Liptak |
| [MAPREDUCE-6555](https://issues.apache.org/jira/browse/MAPREDUCE-6555) | TestMRAppMaster fails on trunk |  Major | . | Varun Saxena | Junping Du |
| [HADOOP-12598](https://issues.apache.org/jira/browse/HADOOP-12598) | add XML namespace declarations for some hadoop/tools modules |  Minor | build, tools | Xin Wang | Xin Wang |
| [YARN-4380](https://issues.apache.org/jira/browse/YARN-4380) | TestResourceLocalizationService.testDownloadingResourcesOnContainerKill fails intermittently |  Major | test | Tsuyoshi Ozawa | Varun Saxena |
| [MAPREDUCE-6557](https://issues.apache.org/jira/browse/MAPREDUCE-6557) | Some tests in mapreduce-client-app are writing outside of target |  Blocker | build | Allen Wittenauer | Akira Ajisaka |
| [HDFS-9459](https://issues.apache.org/jira/browse/HDFS-9459) | hadoop-hdfs-native-client fails test build on Windows after transition to ctest. |  Blocker | build, test | Chris Nauroth | Chris Nauroth |
| [HDFS-9451](https://issues.apache.org/jira/browse/HDFS-9451) | Clean up depreated umasks and related unit tests |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9407](https://issues.apache.org/jira/browse/HDFS-9407) | TestFileTruncate fails with BindException |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9467](https://issues.apache.org/jira/browse/HDFS-9467) | Fix data race accessing writeLockHeldTimeStamp in FSNamesystem |  Major | namenode | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-6549](https://issues.apache.org/jira/browse/MAPREDUCE-6549) | multibyte delimiters with LineRecordReader cause duplicate records |  Major | mrv1, mrv2 | Dustin Cote | Wilfred Spiegelenburg |
| [MAPREDUCE-6550](https://issues.apache.org/jira/browse/MAPREDUCE-6550) | archive-logs tool changes log ownership to the Yarn user when using DefaultContainerExecutor |  Major | . | Robert Kanter | Robert Kanter |
| [HADOOP-12468](https://issues.apache.org/jira/browse/HADOOP-12468) | Partial group resolution failure should not result in user lockout |  Minor | security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9426](https://issues.apache.org/jira/browse/HDFS-9426) | Rollingupgrade finalization is not backward compatible |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10406](https://issues.apache.org/jira/browse/HADOOP-10406) | TestIPC.testIpcWithReaderQueuing may fail |  Major | ipc | Tsz Wo Nicholas Sze | Xiao Chen |
| [HDFS-9470](https://issues.apache.org/jira/browse/HDFS-9470) | Encryption zone on root not loaded from fsimage after NN restart |  Critical | . | Xiao Chen | Xiao Chen |
| [HDFS-9336](https://issues.apache.org/jira/browse/HDFS-9336) | deleteSnapshot throws NPE when snapshotname is null |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12609](https://issues.apache.org/jira/browse/HADOOP-12609) | Fix intermittent failure of TestDecayRpcScheduler |  Minor | ipc, test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-6533](https://issues.apache.org/jira/browse/HDFS-6533) | TestBPOfferService#testBasicFunctionalitytest fails intermittently |  Major | datanode, hdfs-client | Yongjun Zhang | Wei-Chiu Chuang |
| [YARN-4398](https://issues.apache.org/jira/browse/YARN-4398) | Yarn recover functionality causes the cluster running slowly and the cluster usage rate is far below 100 |  Major | resourcemanager | NING DING | NING DING |
| [HDFS-9294](https://issues.apache.org/jira/browse/HDFS-9294) | DFSClient  deadlock when close file and failed to renew lease |  Blocker | hdfs-client | DENG FEI | Brahma Reddy Battula |
| [HADOOP-12565](https://issues.apache.org/jira/browse/HADOOP-12565) | Replace DSA with RSA for SSH key type in SingleCluster.md |  Minor | documentation | Alexander Veit | Mingliang Liu |
| [YARN-4408](https://issues.apache.org/jira/browse/YARN-4408) | NodeManager still reports negative running containers |  Major | nodemanager | Robert Kanter | Robert Kanter |
| [HDFS-9430](https://issues.apache.org/jira/browse/HDFS-9430) | Remove waitForLoadingFSImage since checkNNStartup has ensured image loaded and namenode started. |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4392](https://issues.apache.org/jira/browse/YARN-4392) | ApplicationCreatedEvent event time resets after RM restart/failover |  Critical | . | Xuan Gong | Naganarasimha G R |
| [YARN-4422](https://issues.apache.org/jira/browse/YARN-4422) | Generic AHS sometimes doesn't show started, node, or logs on App page |  Major | . | Eric Payne | Eric Payne |
| [YARN-4424](https://issues.apache.org/jira/browse/YARN-4424) | Fix deadlock in RMAppImpl |  Blocker | . | Yesha Vora | Jian He |
| [HADOOP-12617](https://issues.apache.org/jira/browse/HADOOP-12617) | SPNEGO authentication request to non-default realm gets default realm name inserted in target server principal |  Major | security | Matt Foley | Matt Foley |
| [YARN-4431](https://issues.apache.org/jira/browse/YARN-4431) | Not necessary to do unRegisterNM() if NM get stop due to failed to connect to RM |  Major | nodemanager | Junping Du | Junping Du |
| [YARN-4421](https://issues.apache.org/jira/browse/YARN-4421) | Remove dead code in RmAppImpl.RMAppRecoveredTransition |  Trivial | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HADOOP-12618](https://issues.apache.org/jira/browse/HADOOP-12618) | NPE in TestSequenceFile |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4434](https://issues.apache.org/jira/browse/YARN-4434) | NodeManager Disk Checker parameter documentation is not correct |  Minor | documentation, nodemanager | Takashi Ohnishi | Weiwei Yang |
| [HADOOP-12602](https://issues.apache.org/jira/browse/HADOOP-12602) | TestMetricsSystemImpl#testQSize occasionally fail |  Major | test | Wei-Chiu Chuang | Masatake Iwasaki |
| [HDFS-9519](https://issues.apache.org/jira/browse/HDFS-9519) | Some coding improvement in SecondaryNameNode#main |  Major | namenode | Yongjun Zhang | Xiao Chen |
| [HDFS-9514](https://issues.apache.org/jira/browse/HDFS-9514) | TestDistributedFileSystem.testDFSClientPeerWriteTimeout failing; exception being swallowed |  Major | hdfs-client, test | Steve Loughran | Wei-Chiu Chuang |
| [HDFS-9535](https://issues.apache.org/jira/browse/HDFS-9535) | Newly completed blocks in IBR should not be considered under-replicated too quickly |  Major | namenode | Jing Zhao | Mingliang Liu |
| [YARN-4418](https://issues.apache.org/jira/browse/YARN-4418) | AM Resource Limit per partition can be updated to ResourceUsage as well |  Major | resourcemanager | Sunil G | Sunil G |
| [HADOOP-12638](https://issues.apache.org/jira/browse/HADOOP-12638) | UnsatisfiedLinkError while checking ISA-L in checknative command. |  Minor | native | Kai Sasaki | Kai Sasaki |
| [YARN-4403](https://issues.apache.org/jira/browse/YARN-4403) | (AM/NM/Container)LivelinessMonitor should use monotonic time when calculating period |  Critical | . | Junping Du | Junping Du |
| [YARN-4402](https://issues.apache.org/jira/browse/YARN-4402) | TestNodeManagerShutdown And TestNodeManagerResync fails with bind exception |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4439](https://issues.apache.org/jira/browse/YARN-4439) | Clarify NMContainerStatus#toString method. |  Major | . | Jian He | Jian He |
| [YARN-4440](https://issues.apache.org/jira/browse/YARN-4440) | FSAppAttempt#getAllowedLocalityLevelByTime should init the lastScheduler time |  Major | fairscheduler | Yiqun Lin | Yiqun Lin |
| [HDFS-9516](https://issues.apache.org/jira/browse/HDFS-9516) | truncate file fails with data dirs on multiple disks |  Major | datanode | Bogdan Raducanu | Plamen Jeliazkov |
| [HDFS-8894](https://issues.apache.org/jira/browse/HDFS-8894) | Set SO\_KEEPALIVE on DN server sockets |  Major | datanode | Nathan Roberts | Kanaka Kumar Avvaru |
| [HADOOP-12192](https://issues.apache.org/jira/browse/HADOOP-12192) | update releasedocmaker commands |  Blocker | . | Allen Wittenauer | Allen Wittenauer |
| [YARN-4461](https://issues.apache.org/jira/browse/YARN-4461) | Redundant nodeLocalityDelay log in LeafQueue |  Trivial | capacityscheduler | Jason Lowe | Eric Payne |
| [YARN-4452](https://issues.apache.org/jira/browse/YARN-4452) | NPE when submit Unmanaged application |  Critical | . | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9570](https://issues.apache.org/jira/browse/HDFS-9570) | Minor typos, grammar, and case sensitivity cleanup in HdfsPermissionsGuide.md's |  Trivial | documentation | Travis Campbell | Travis Campbell |
| [HDFS-9565](https://issues.apache.org/jira/browse/HDFS-9565) | TestDistributedFileSystem.testLocatedFileStatusStorageIdsTypes is flaky due to race condition |  Minor | fs, test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4225](https://issues.apache.org/jira/browse/YARN-4225) | Add preemption status to yarn queue -status for capacity scheduler |  Minor | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HADOOP-12615](https://issues.apache.org/jira/browse/HADOOP-12615) | Fix NPE in MiniKMS.start() |  Minor | kms, test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9515](https://issues.apache.org/jira/browse/HDFS-9515) | NPE when MiniDFSCluster#shutdown is invoked on uninitialized reference |  Minor | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9572](https://issues.apache.org/jira/browse/HDFS-9572) | Prevent DataNode log spam if a client connects on the data transfer port but sends no data. |  Major | datanode | Chris Nauroth | Chris Nauroth |
| [HDFS-9533](https://issues.apache.org/jira/browse/HDFS-9533) | seen\_txid in the shared edits directory is modified during bootstrapping |  Major | ha, namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-9571](https://issues.apache.org/jira/browse/HDFS-9571) | Fix ASF Licence warnings in Jenkins reports |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9393](https://issues.apache.org/jira/browse/HDFS-9393) | After choosing favored nodes, choosing nodes for remaining replicas should go through BlockPlacementPolicy |  Major | . | J.Andreina | J.Andreina |
| [HADOOP-12647](https://issues.apache.org/jira/browse/HADOOP-12647) | Add /usr/lib default for ISAL library and fix missing .c file in CMakeLists |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-9347](https://issues.apache.org/jira/browse/HDFS-9347) | Invariant assumption in TestQuorumJournalManager.shutdown() is wrong |  Major | test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12656](https://issues.apache.org/jira/browse/HADOOP-12656) | MiniKdc throws "address in use" BindException |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12636](https://issues.apache.org/jira/browse/HADOOP-12636) | Prevent ServiceLoader failure init for unused FileSystems |  Major | fs | Inigo Goiri | Inigo Goiri |
| [MAPREDUCE-6583](https://issues.apache.org/jira/browse/MAPREDUCE-6583) | Clarify confusing sentence in MapReduce tutorial document |  Minor | documentation | chris snow | Kai Sasaki |
| [HDFS-9505](https://issues.apache.org/jira/browse/HDFS-9505) | HDFS Architecture documentation needs to be refreshed. |  Major | documentation | Chris Nauroth | Masatake Iwasaki |
| [YARN-4454](https://issues.apache.org/jira/browse/YARN-4454) | NM to nodelabel mapping going wrong after RM restart |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9580](https://issues.apache.org/jira/browse/HDFS-9580) | TestComputeInvalidateWork#testDatanodeReRegistration failed due to unexpected number of invalidate blocks. |  Major | datanode, namenode, test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4477](https://issues.apache.org/jira/browse/YARN-4477) | FairScheduler: Handle condition which can result in an infinite loop in attemptScheduling. |  Major | fairscheduler | Tao Jie | Tao Jie |
| [HDFS-9589](https://issues.apache.org/jira/browse/HDFS-9589) | Block files which have been hardlinked should be duplicated before the DataNode appends to the them |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-9458](https://issues.apache.org/jira/browse/HDFS-9458) | TestBackupNode always binds to port 50070, which can cause bind failures. |  Major | test | Chris Nauroth | Xiao Chen |
| [YARN-4109](https://issues.apache.org/jira/browse/YARN-4109) | Exception on RM scheduler page loading with labels |  Minor | . | Bibin A Chundatt | Mohammad Shahid Khan |
| [YARN-4156](https://issues.apache.org/jira/browse/YARN-4156) | TestAMRestart#testAMBlacklistPreventsRestartOnSameNode assumes CapacityScheduler |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-6419](https://issues.apache.org/jira/browse/MAPREDUCE-6419) | JobHistoryServer doesn't sort properly based on Job ID when Job id's exceed 9999 |  Major | webapps | Devaraj K | Mohammad Shahid Khan |
| [HDFS-9597](https://issues.apache.org/jira/browse/HDFS-9597) | BaseReplicationPolicyTest should update data node stats after adding a data node |  Blocker | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-7163](https://issues.apache.org/jira/browse/HDFS-7163) | WebHdfsFileSystem should retry reads according to the configured retry policy. |  Major | webhdfs | Eric Payne | Eric Payne |
| [HADOOP-12559](https://issues.apache.org/jira/browse/HADOOP-12559) | KMS connection failures should trigger TGT renewal |  Major | security | Zhe Zhang | Zhe Zhang |
| [MAPREDUCE-6574](https://issues.apache.org/jira/browse/MAPREDUCE-6574) | MR AM should print host of failed tasks. |  Major | . | Wangda Tan | Mohammad Shahid Khan |
| [YARN-4417](https://issues.apache.org/jira/browse/YARN-4417) | Make RM and Timeline-server REST APIs more consistent |  Major | . | Wangda Tan | Wangda Tan |
| [MAPREDUCE-6589](https://issues.apache.org/jira/browse/MAPREDUCE-6589) | TestTaskLog outputs a log under directory other than target/test-dir |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-4315](https://issues.apache.org/jira/browse/YARN-4315) | NaN in Queue percentage for cluster apps page |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9376](https://issues.apache.org/jira/browse/HDFS-9376) | TestSeveralNameNodes fails occasionally |  Major | test | Kihwal Lee | Masatake Iwasaki |
| [YARN-1382](https://issues.apache.org/jira/browse/YARN-1382) | Remove unusableRMNodesConcurrentSet (never used) in NodeListManager to get rid of memory leak |  Major | resourcemanager | Alejandro Abdelnur | Rohith Sharma K S |
| [HADOOP-12682](https://issues.apache.org/jira/browse/HADOOP-12682) | Fix TestKMS#testKMSRestart\* failure |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12608](https://issues.apache.org/jira/browse/HADOOP-12608) | Fix exception message in WASB when connecting with anonymous credential |  Major | tools | Dushyanth | Dushyanth |
| [YARN-4524](https://issues.apache.org/jira/browse/YARN-4524) | Cleanup AppSchedulingInfo |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-4522](https://issues.apache.org/jira/browse/YARN-4522) | Queue acl can be checked at app submission |  Major | . | Jian He | Jian He |
| [YARN-4510](https://issues.apache.org/jira/browse/YARN-4510) | Fix SLS startup failure caused by NPE |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4530](https://issues.apache.org/jira/browse/YARN-4530) | LocalizedResource trigger a NPE Cause the NodeManager exit |  Major | . | tangshangwen | tangshangwen |
| [HADOOP-11252](https://issues.apache.org/jira/browse/HADOOP-11252) | RPC client does not time out by default |  Critical | ipc | Wilfred Spiegelenburg | Masatake Iwasaki |
| [MAPREDUCE-6593](https://issues.apache.org/jira/browse/MAPREDUCE-6593) | TestJobHistoryEventHandler.testTimelineEventHandling fails on trunk because of NPE |  Major | . | Tsuyoshi Ozawa | Naganarasimha G R |
| [HDFS-9445](https://issues.apache.org/jira/browse/HDFS-9445) | Datanode may deadlock while handling a bad volume |  Blocker | . | Kihwal Lee | Walter Su |
| [HADOOP-12658](https://issues.apache.org/jira/browse/HADOOP-12658) | Clear javadoc and check style issues around DomainSocket |  Trivial | . | Kai Zheng | Kai Zheng |
| [HADOOP-12604](https://issues.apache.org/jira/browse/HADOOP-12604) | Exception may be swallowed in KMSClientProvider |  Major | kms | Yongjun Zhang | Yongjun Zhang |
| [HDFS-9605](https://issues.apache.org/jira/browse/HDFS-9605) | Add links to failed volumes to explorer.html in HDFS Web UI |  Minor | . | Archana T | Archana T |
| [MAPREDUCE-6577](https://issues.apache.org/jira/browse/MAPREDUCE-6577) | MR AM unable to load native library without MR\_AM\_ADMIN\_USER\_ENV set |  Critical | mr-am | Sangjin Lee | Sangjin Lee |
| [HADOOP-12689](https://issues.apache.org/jira/browse/HADOOP-12689) | S3 filesystem operations stopped working correctly |  Major | tools | Matthew Paduano | Matthew Paduano |
| [YARN-4546](https://issues.apache.org/jira/browse/YARN-4546) | ResourceManager crash due to scheduling opportunity overflow |  Critical | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-9615](https://issues.apache.org/jira/browse/HDFS-9615) | Fix variable name typo in DFSConfigKeys |  Trivial | . | Ray Chiang | Ray Chiang |
| [HADOOP-12634](https://issues.apache.org/jira/browse/HADOOP-12634) | Change Lazy Rename Pending Operation Completion of WASB to address case of potential data loss due to partial copy |  Critical | . | Gaurav Kanade | Gaurav Kanade |
| [HDFS-9600](https://issues.apache.org/jira/browse/HDFS-9600) | do not check replication if the block is under construction |  Critical | . | Phil Yang | Phil Yang |
| [HDFS-9619](https://issues.apache.org/jira/browse/HDFS-9619) | SimulatedFSDataset sometimes can not find blockpool for the correct namenode |  Major | datanode, test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12675](https://issues.apache.org/jira/browse/HADOOP-12675) | Fix description about retention period in usage of expunge command |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-12655](https://issues.apache.org/jira/browse/HADOOP-12655) | TestHttpServer.testBindAddress bind port range is wider than expected |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12613](https://issues.apache.org/jira/browse/HADOOP-12613) | TestFind.processArguments occasionally fails |  Major | test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12573](https://issues.apache.org/jira/browse/HADOOP-12573) | TestRPC.testClientBackOff failing |  Major | test | Steve Loughran | Xiao Chen |
| [MAPREDUCE-6508](https://issues.apache.org/jira/browse/MAPREDUCE-6508) | TestNetworkedJob fails consistently due to delegation token changes on RM. |  Major | test | Rohith Sharma K S | Akira Ajisaka |
| [HDFS-9574](https://issues.apache.org/jira/browse/HDFS-9574) | Reduce client failures during datanode restart |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9493](https://issues.apache.org/jira/browse/HDFS-9493) | Test o.a.h.hdfs.server.namenode.TestMetaSave fails in trunk |  Major | test | Mingliang Liu | Tony Wu |
| [HADOOP-12678](https://issues.apache.org/jira/browse/HADOOP-12678) | Handle empty rename pending metadata file during atomic rename in redo path |  Critical | fs/azure | madhumita chakraborty | madhumita chakraborty |
| [HADOOP-12590](https://issues.apache.org/jira/browse/HADOOP-12590) | TestCompressorDecompressor failing without stack traces |  Critical | test | Steve Loughran | John Zhuge |
| [HADOOP-12587](https://issues.apache.org/jira/browse/HADOOP-12587) | Hadoop AuthToken refuses to work without a maxinactive attribute in issued token |  Blocker | security | Steve Loughran | Benoy Antony |
| [HADOOP-12551](https://issues.apache.org/jira/browse/HADOOP-12551) | Introduce FileNotFoundException for WASB FileSystem API |  Major | tools | Dushyanth | Dushyanth |
| [MAPREDUCE-6068](https://issues.apache.org/jira/browse/MAPREDUCE-6068) | Illegal progress value warnings in map tasks |  Major | mrv2, task | Todd Lipcon | Binglin Chang |
| [HDFS-9639](https://issues.apache.org/jira/browse/HDFS-9639) | Inconsistent Logging in BootstrapStandby |  Minor | ha | BELUGA BEHR | Xiaobing Zhou |
| [HDFS-9584](https://issues.apache.org/jira/browse/HDFS-9584) | NPE in distcp when ssl configuration file does not exist in class path. |  Major | distcp | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-12584](https://issues.apache.org/jira/browse/HADOOP-12584) | Disable browsing the static directory in HttpServer2 |  Major | security | Robert Kanter | Robert Kanter |
| [YARN-4567](https://issues.apache.org/jira/browse/YARN-4567) | javadoc failing on java 8 |  Blocker | build | Steve Loughran | Steve Loughran |
| [YARN-4414](https://issues.apache.org/jira/browse/YARN-4414) | Nodemanager connection errors are retried at multiple levels |  Major | nodemanager | Jason Lowe | Chang Li |
| [HADOOP-12653](https://issues.apache.org/jira/browse/HADOOP-12653) | Use SO\_REUSEADDR to avoid getting "Address already in use" when using kerberos and attempting to bind to any port on the local IP address |  Major | net | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-12603](https://issues.apache.org/jira/browse/HADOOP-12603) | TestSymlinkLocalFSFileContext#testSetTimesSymlinkToDir occasionally fail |  Major | test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4571](https://issues.apache.org/jira/browse/YARN-4571) | Make app id/name available to the yarn authorizer provider for better auditing |  Major | . | Jian He | Jian He |
| [YARN-4551](https://issues.apache.org/jira/browse/YARN-4551) | Address the duplication between StatusUpdateWhenHealthy and StatusUpdateWhenUnhealthy transitions |  Minor | nodemanager | Karthik Kambatla | Sunil G |
| [HDFS-9517](https://issues.apache.org/jira/browse/HDFS-9517) | Fix missing @Test annotation on TestDistCpUtils.testUnpackAttributes |  Trivial | distcp | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4534](https://issues.apache.org/jira/browse/YARN-4534) | Remove the redundant symbol in yarn rmadmin help msg |  Trivial | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-12700](https://issues.apache.org/jira/browse/HADOOP-12700) | Remove unused import in TestCompressorDecompressor.java |  Minor | . | John Zhuge | John Zhuge |
| [MAPREDUCE-6601](https://issues.apache.org/jira/browse/MAPREDUCE-6601) | Fix typo in Job#setUseNewAPI |  Trivial | . | Kai Sasaki | Kai Sasaki |
| [YARN-3446](https://issues.apache.org/jira/browse/YARN-3446) | FairScheduler headroom calculation should exclude nodes in the blacklist |  Major | fairscheduler | zhihai xu | zhihai xu |
| [HDFS-9648](https://issues.apache.org/jira/browse/HDFS-9648) | TestStartup.testImageChecksum is broken by HDFS-9569's message change |  Trivial | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12706](https://issues.apache.org/jira/browse/HADOOP-12706) | TestLocalFsFCStatistics#testStatisticsThreadLocalDataCleanUp times out occasionally |  Major | test | Jason Lowe | Sangjin Lee |
| [YARN-4581](https://issues.apache.org/jira/browse/YARN-4581) | AHS writer thread leak makes RM crash while RM is recovering |  Major | resourcemanager | sandflee | sandflee |
| [MAPREDUCE-6554](https://issues.apache.org/jira/browse/MAPREDUCE-6554) | MRAppMaster servicestart failing  with NPE in MRAppMaster#parsePreviousJobHistory |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4389](https://issues.apache.org/jira/browse/YARN-4389) | "yarn.am.blacklisting.enabled" and "yarn.am.blacklisting.disable-failure-threshold" should be app specific rather than a setting for whole YARN cluster |  Critical | applications | Junping Du | Sunil G |
| [HDFS-9612](https://issues.apache.org/jira/browse/HDFS-9612) | DistCp worker threads are not terminated after jobs are done. |  Major | distcp | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9624](https://issues.apache.org/jira/browse/HDFS-9624) | DataNode start slowly due to the initial DU command operations |  Major | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-12712](https://issues.apache.org/jira/browse/HADOOP-12712) | Fix some cmake plugin and native build warnings |  Minor | native | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-12714](https://issues.apache.org/jira/browse/HADOOP-12714) | Fix hadoop-mapreduce-client-nativetask unit test which fails because it is not able to open the "glibc bug spill" file. |  Major | native | Colin P. McCabe | Colin P. McCabe |
| [YARN-4538](https://issues.apache.org/jira/browse/YARN-4538) | QueueMetrics pending  cores and memory metrics wrong |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4596](https://issues.apache.org/jira/browse/YARN-4596) | SystemMetricPublisher should not swallow error messages from TimelineClient#putEntities |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-4502](https://issues.apache.org/jira/browse/YARN-4502) | Fix two AM containers get allocated when AM restart |  Critical | . | Yesha Vora | Vinod Kumar Vavilapalli |
| [HDFS-9623](https://issues.apache.org/jira/browse/HDFS-9623) | Update example configuration of block state change log in log4j.properties |  Minor | logging | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-4565](https://issues.apache.org/jira/browse/YARN-4565) | When sizeBasedWeight enabled for FairOrderingPolicy in CapacityScheduler, Sometimes lead to situation where all queue resources consumed by AMs only |  Major | capacity scheduler, capacityscheduler | Karam Singh | Wangda Tan |
| [HADOOP-12356](https://issues.apache.org/jira/browse/HADOOP-12356) | Fix computing CPU usage statistics on Windows |  Major | util | Yunqi Zhang | Inigo Goiri |
| [HDFS-9661](https://issues.apache.org/jira/browse/HDFS-9661) | Deadlock in DN.FsDatasetImpl between moveBlockAcrossStorage and createRbw |  Major | datanode | ade | ade |
| [HDFS-9655](https://issues.apache.org/jira/browse/HDFS-9655) | NN should start JVM pause monitor before loading fsimage |  Critical | . | John Zhuge | John Zhuge |
| [YARN-4559](https://issues.apache.org/jira/browse/YARN-4559) | Make leader elector and zk store share the same curator client |  Major | . | Jian He | Jian He |
| [YARN-4611](https://issues.apache.org/jira/browse/YARN-4611) | Fix scheduler load simulator to support multi-layer network location |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-12605](https://issues.apache.org/jira/browse/HADOOP-12605) | Fix intermittent failure of TestIPC.testIpcWithReaderQueuing |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-9625](https://issues.apache.org/jira/browse/HDFS-9625) | set replication for empty file  failed when set storage policy |  Major | namenode | DENG FEI | DENG FEI |
| [HADOOP-12423](https://issues.apache.org/jira/browse/HADOOP-12423) | Handle failure of registering shutdownhook by ShutdownHookManager in static block |  Minor | fs | Abhishek Agarwal | Abhishek Agarwal |
| [HDFS-9634](https://issues.apache.org/jira/browse/HDFS-9634) | webhdfs client side exceptions don't provide enough details |  Major | webhdfs | Eric Payne | Eric Payne |
| [YARN-4608](https://issues.apache.org/jira/browse/YARN-4608) | Redundant code statement in WritingYarnApplications |  Minor | documentation | Kai Sasaki | Kai Sasaki |
| [HADOOP-7161](https://issues.apache.org/jira/browse/HADOOP-7161) | Remove unnecessary oro package from dependency management section |  Minor | build | Todd Lipcon | Sean Busbey |
| [YARN-4610](https://issues.apache.org/jira/browse/YARN-4610) | Reservations continue looking for one app causes other apps to starve |  Blocker | capacityscheduler | Jason Lowe | Jason Lowe |
| [HADOOP-12659](https://issues.apache.org/jira/browse/HADOOP-12659) | Incorrect usage of config parameters in token manager of KMS |  Major | security | Tianyin Xu | Mingliang Liu |
| [YARN-4584](https://issues.apache.org/jira/browse/YARN-4584) | RM startup failure when AM attempts greater than max-attempts |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9618](https://issues.apache.org/jira/browse/HDFS-9618) | Fix mismatch between log level and guard in BlockManager#computeRecoveryWorkForBlocks |  Minor | namenode | Masatake Iwasaki | Masatake Iwasaki |
| [MAPREDUCE-6605](https://issues.apache.org/jira/browse/MAPREDUCE-6605) | Fix typos mapreduce.map.skip.proc.count.autoincr and mapreduce.reduce.skip.proc.count.autoincr in mapred-default.xml |  Major | documentation | Dong Zhen | Kai Sasaki |
| [YARN-4605](https://issues.apache.org/jira/browse/YARN-4605) | Spelling mistake in the help message of "yarn applicationattempt" command |  Trivial | client, yarn | Manjunath Ballur | Weiwei Yang |
| [HDFS-9682](https://issues.apache.org/jira/browse/HDFS-9682) | Fix a typo "aplication" in HttpFS document |  Trivial | documentation | Weiwei Yang | Weiwei Yang |
| [HADOOP-12730](https://issues.apache.org/jira/browse/HADOOP-12730) | Hadoop streaming -mapper and -reducer options are wrongly documented as required |  Major | documentation | DeepakVohra | Kengo Seki |
| [YARN-4497](https://issues.apache.org/jira/browse/YARN-4497) | RM might fail to restart when recovering apps whose attempts are missing |  Critical | . | Jun Gong | Jun Gong |
| [HDFS-8898](https://issues.apache.org/jira/browse/HDFS-8898) | Create API and command-line argument to get quota and quota usage without detailed content summary |  Major | fs | Joep Rottinghuis | Ming Ma |
| [YARN-4598](https://issues.apache.org/jira/browse/YARN-4598) | Invalid event: RESOURCE\_FAILED at CONTAINER\_CLEANEDUP\_AFTER\_KILL |  Major | nodemanager | tangshangwen | tangshangwen |
| [YARN-4592](https://issues.apache.org/jira/browse/YARN-4592) | Remove unused GetContainerStatus proto |  Minor | . | Chang Li | Chang Li |
| [YARN-4520](https://issues.apache.org/jira/browse/YARN-4520) | FinishAppEvent is leaked in leveldb if no app's container running on this node |  Major | nodemanager | sandflee | sandflee |
| [MAPREDUCE-6610](https://issues.apache.org/jira/browse/MAPREDUCE-6610) | JobHistoryEventHandler should not swallow timeline response |  Trivial | . | Li Lu | Li Lu |
| [HDFS-9690](https://issues.apache.org/jira/browse/HDFS-9690) | ClientProtocol.addBlock is not idempotent after HDFS-8071 |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-12743](https://issues.apache.org/jira/browse/HADOOP-12743) | Fix git environment check during test-patch |  Major | . | Ray Chiang | Allen Wittenauer |
| [YARN-4612](https://issues.apache.org/jira/browse/YARN-4612) | Fix rumen and scheduler load simulator handle killed tasks properly |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-12492](https://issues.apache.org/jira/browse/HADOOP-12492) | maven install triggers bats test |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12735](https://issues.apache.org/jira/browse/HADOOP-12735) | core-default.xml misspells hadoop.workaround.non.threadsafe.getpwuid |  Minor | . | Ray Chiang | Ray Chiang |
| [HDFS-8999](https://issues.apache.org/jira/browse/HDFS-8999) | Allow a file to be closed with COMMITTED but not yet COMPLETE blocks. |  Major | namenode | Jitendra Nath Pandey | Tsz Wo Nicholas Sze |
| [MAPREDUCE-6595](https://issues.apache.org/jira/browse/MAPREDUCE-6595) | Fix findbugs warnings in OutputCommitter and FileOutputCommitter |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-6563](https://issues.apache.org/jira/browse/MAPREDUCE-6563) | Streaming documentation contains a stray '%' character. |  Trivial | documentation | Chris Nauroth | Chris Nauroth |
| [YARN-4519](https://issues.apache.org/jira/browse/YARN-4519) | potential deadlock of CapacityScheduler between decrease container and assign containers |  Major | capacityscheduler | sandflee | MENG DING |
| [MAPREDUCE-6616](https://issues.apache.org/jira/browse/MAPREDUCE-6616) | Fail to create jobhistory file if there are some multibyte characters in the job name |  Major | jobhistoryserver | Akira Ajisaka | Kousuke Saruta |
| [YARN-4411](https://issues.apache.org/jira/browse/YARN-4411) | RMAppAttemptImpl#createApplicationAttemptReport throws IllegalArgumentException |  Major | resourcemanager | yarntime | Bibin A Chundatt |
| [YARN-4617](https://issues.apache.org/jira/browse/YARN-4617) | LeafQueue#pendingOrderingPolicy should always use fixed ordering policy instead of using same as active applications ordering policy |  Major | capacity scheduler | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-4428](https://issues.apache.org/jira/browse/YARN-4428) | Redirect RM page to AHS page when AHS turned on and RM page is not available |  Major | . | Chang Li | Chang Li |
| [HDFS-9659](https://issues.apache.org/jira/browse/HDFS-9659) | EditLogTailerThread to Active Namenode RPC should timeout |  Critical | ha, namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [MAPREDUCE-6618](https://issues.apache.org/jira/browse/MAPREDUCE-6618) | YarnClientProtocolProvider leaking the YarnClient thread. |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-9210](https://issues.apache.org/jira/browse/HDFS-9210) | Fix some misuse of %n in VolumeScanner#printStats |  Minor | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-9701](https://issues.apache.org/jira/browse/HDFS-9701) | DN may deadlock when hot-swapping under load |  Major | . | Xiao Chen | Xiao Chen |
| [YARN-3102](https://issues.apache.org/jira/browse/YARN-3102) | Decommisioned Nodes not listed in Web UI |  Minor | resourcemanager | Bibin A Chundatt | Kuhu Shukla |
| [HDFS-9406](https://issues.apache.org/jira/browse/HDFS-9406) | FSImage may get corrupted after deleting snapshot |  Major | namenode | Stanislav Antic | Yongjun Zhang |
| [HDFS-9718](https://issues.apache.org/jira/browse/HDFS-9718) | HAUtil#getConfForOtherNodes should unset independent generic keys before initialize |  Major | namenode | DENG FEI | DENG FEI |
| [HDFS-9708](https://issues.apache.org/jira/browse/HDFS-9708) | FSNamesystem.initAuditLoggers() doesn't trim classnames |  Minor | fs | Steve Loughran | Mingliang Liu |
| [HADOOP-12757](https://issues.apache.org/jira/browse/HADOOP-12757) | Findbug compilation fails for 'Kafka Library support' |  Major | . | Rohith Sharma K S | Akira Ajisaka |
| [MAPREDUCE-6621](https://issues.apache.org/jira/browse/MAPREDUCE-6621) | Memory Leak in JobClient#submitJobInternal() |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-12755](https://issues.apache.org/jira/browse/HADOOP-12755) | Fix typo in defaultFS warning message |  Trivial | . | Andrew Wang | Andrew Wang |
| [MAPREDUCE-6620](https://issues.apache.org/jira/browse/MAPREDUCE-6620) | Jobs that did not start are shown as starting in 1969 in the JHS web UI |  Major | jobhistoryserver | Daniel Templeton | Haibo Chen |
| [HDFS-9739](https://issues.apache.org/jira/browse/HDFS-9739) | DatanodeStorage.isValidStorageId() is broken |  Critical | hdfs-client | Kihwal Lee | Mingliang Liu |
| [HDFS-9740](https://issues.apache.org/jira/browse/HDFS-9740) | Use a reasonable limit in DFSTestUtil.waitForMetric() |  Major | test | Kihwal Lee | Chang Li |
| [YARN-4625](https://issues.apache.org/jira/browse/YARN-4625) | Make ApplicationSubmissionContext and ApplicationSubmissionContextInfo more consistent |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-12761](https://issues.apache.org/jira/browse/HADOOP-12761) | incremental maven build is not really incremental |  Minor | build | Sangjin Lee | Sangjin Lee |
| [YARN-4594](https://issues.apache.org/jira/browse/YARN-4594) | container-executor fails to remove directory tree when chmod required |  Major | nodemanager | Colin P. McCabe | Colin P. McCabe |
| [HDFS-9748](https://issues.apache.org/jira/browse/HDFS-9748) | When addExpectedReplicasToPending is called twice, pendingReplications should avoid duplication |  Minor | . | Walter Su | Walter Su |
| [YARN-4669](https://issues.apache.org/jira/browse/YARN-4669) | Fix logging statements in resource manager's Application class |  Trivial | . | Sidharta Seethana | Sidharta Seethana |
| [HDFS-9730](https://issues.apache.org/jira/browse/HDFS-9730) | Storage ID update does not happen when there is a layout change |  Major | datanode | Kihwal Lee | Tsz Wo Nicholas Sze |
| [HDFS-9724](https://issues.apache.org/jira/browse/HDFS-9724) | Degraded performance in WebHDFS listing as it does not reuse ObjectMapper |  Critical | performance | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12766](https://issues.apache.org/jira/browse/HADOOP-12766) | The default value of "hadoop.workaround.non.threadsafe.getpwuid" is different between core-default.xml and NativeIO.java |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-9761](https://issues.apache.org/jira/browse/HDFS-9761) | Rebalancer sleeps too long between iterations |  Blocker | balancer & mover | Adrian Bridgett | Mingliang Liu |
| [HDFS-9713](https://issues.apache.org/jira/browse/HDFS-9713) | DataXceiver#copyBlock should return if block is pinned |  Major | datanode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-12773](https://issues.apache.org/jira/browse/HADOOP-12773) | HBase classes fail to load with client/job classloader enabled |  Major | util | Sangjin Lee | Sangjin Lee |
| [HDFS-9777](https://issues.apache.org/jira/browse/HDFS-9777) | Fix typos in DFSAdmin command line and documentation |  Trivial | hdfs-client | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9784](https://issues.apache.org/jira/browse/HDFS-9784) | Example usage is not correct in Transparent Encryption document |  Major | documentation | Takashi Ohnishi | Takashi Ohnishi |
| [HDFS-9752](https://issues.apache.org/jira/browse/HDFS-9752) | Permanent write failures may happen to slow writers during datanode rolling upgrades |  Critical | . | Kihwal Lee | Walter Su |
| [HDFS-9760](https://issues.apache.org/jira/browse/HDFS-9760) | WebHDFS AuthFilter cannot be configured with custom AltKerberos auth handler |  Major | webhdfs | Ryan Sasson | Ryan Sasson |
| [HDFS-9779](https://issues.apache.org/jira/browse/HDFS-9779) | TestReplicationPolicyWithNodeGroup NODE variable picks wrong rack value |  Minor | test | Kuhu Shukla | Kuhu Shukla |
| [YARN-4629](https://issues.apache.org/jira/browse/YARN-4629) | Distributed shell breaks under strong security |  Major | applications/distributed-shell, security | Daniel Templeton | Daniel Templeton |
| [HADOOP-12792](https://issues.apache.org/jira/browse/HADOOP-12792) | TestUserGroupInformation#testGetServerSideGroups fails in chroot |  Minor | security, test | Eric Badger | Eric Badger |
| [HDFS-9788](https://issues.apache.org/jira/browse/HDFS-9788) | Incompatible tag renumbering in HeartbeatResponseProto |  Blocker | rolling upgrades | Andrew Wang | Andrew Wang |
| [HADOOP-12795](https://issues.apache.org/jira/browse/HADOOP-12795) | KMS does not log detailed stack trace for unexpected errors. |  Major | kms | Chris Nauroth | Chris Nauroth |
| [HADOOP-12699](https://issues.apache.org/jira/browse/HADOOP-12699) | TestKMS#testKMSProvider intermittently fails during 'test rollover draining' |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-9790](https://issues.apache.org/jira/browse/HDFS-9790) | HDFS Balancer should exit with a proper message if upgrade is not finalized |  Major | . | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-9801](https://issues.apache.org/jira/browse/HDFS-9801) | ReconfigurableBase should update the cached configuration |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-12780](https://issues.apache.org/jira/browse/HADOOP-12780) | During WASB atomic rename handle crash when one directory has been renamed but not file under it. |  Critical | fs/azure | madhumita chakraborty | madhumita chakraborty |
| [HADOOP-12589](https://issues.apache.org/jira/browse/HADOOP-12589) | Fix intermittent test failure of TestCopyPreserveFlag |  Major | test | Tsuyoshi Ozawa | Masatake Iwasaki |
| [HADOOP-12786](https://issues.apache.org/jira/browse/HADOOP-12786) | "hadoop key" command usage is not documented |  Major | documentation | Akira Ajisaka | Xiao Chen |
| [HDFS-9765](https://issues.apache.org/jira/browse/HDFS-9765) | TestBlockScanner#testVolumeIteratorWithCaching fails intermittently |  Major | test | Mingliang Liu | Akira Ajisaka |
| [HDFS-9456](https://issues.apache.org/jira/browse/HDFS-9456) | BlockPlacementPolicyWithNodeGroup should override verifyBlockPlacement |  Major | . | Junping Du | Xiaobing Zhou |
| [HADOOP-12810](https://issues.apache.org/jira/browse/HADOOP-12810) | FileSystem#listLocatedStatus causes unnecessary RPC calls |  Major | fs, fs/s3 | Ryan Blue | Ryan Blue |
| [HDFS-9787](https://issues.apache.org/jira/browse/HDFS-9787) | SNNs stop uploading FSImage to ANN once isPrimaryCheckPointer changed to false. |  Major | ha | Guocui Mi | Guocui Mi |
| [MAPREDUCE-6341](https://issues.apache.org/jira/browse/MAPREDUCE-6341) | Fix typo in mapreduce tutorial |  Trivial | . | John Michael Luy | John Michael Luy |
| [HADOOP-12787](https://issues.apache.org/jira/browse/HADOOP-12787) | KMS SPNEGO sequence does not work with WEBHDFS |  Major | kms, security | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-9815](https://issues.apache.org/jira/browse/HDFS-9815) | Move o.a.h.fs.Hdfs to hadoop-hdfs-client |  Blocker | . | Haohui Mai | Vinayakumar B |
| [HDFS-9608](https://issues.apache.org/jira/browse/HDFS-9608) | Disk IO imbalance in HDFS with heterogeneous storages |  Major | . | Wei Zhou | Wei Zhou |
| [HDFS-9799](https://issues.apache.org/jira/browse/HDFS-9799) | Reimplement getCurrentTrashDir to remove incompatibility |  Blocker | . | Zhe Zhang | Zhe Zhang |
| [YARN-4689](https://issues.apache.org/jira/browse/YARN-4689) | FairScheduler: Cleanup preemptContainer to be more readable |  Trivial | fairscheduler | Karthik Kambatla | Kai Sasaki |
| [YARN-4654](https://issues.apache.org/jira/browse/YARN-4654) | Yarn node label CLI should parse "=" correctly when trying to remove all labels on a node |  Major | . | Wangda Tan | Naganarasimha G R |
| [HDFS-6832](https://issues.apache.org/jira/browse/HDFS-6832) | Fix the usage of 'hdfs namenode' command |  Minor | . | Akira Ajisaka | Manjunath Ballur |
| [HDFS-8923](https://issues.apache.org/jira/browse/HDFS-8923) | Add -source flag to balancer usage message |  Trivial | balancer & mover, documentation | Chris Trezzo | Chris Trezzo |
| [HDFS-9764](https://issues.apache.org/jira/browse/HDFS-9764) | DistCp doesn't print value for several arguments including -numListstatusThreads |  Minor | distcp | Yongjun Zhang | Wei-Chiu Chuang |
| [MAPREDUCE-6637](https://issues.apache.org/jira/browse/MAPREDUCE-6637) | Testcase Failure : TestFileInputFormat.testSplitLocationInfo |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9839](https://issues.apache.org/jira/browse/HDFS-9839) | Reduce verbosity of processReport logging |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7452](https://issues.apache.org/jira/browse/HDFS-7452) | skip StandbyException log for getCorruptFiles() |  Minor | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4707](https://issues.apache.org/jira/browse/YARN-4707) | Remove the extra char (\>) from SecureContainer.md |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4386](https://issues.apache.org/jira/browse/YARN-4386) | refreshNodesGracefully() should send recommission event to active RMNodes only |  Minor | graceful | Kuhu Shukla | Kuhu Shukla |
| [HDFS-9842](https://issues.apache.org/jira/browse/HDFS-9842) | dfs.datanode.balance.bandwidthPerSec should accept friendly size units |  Minor | balancer & mover | Yiqun Lin | Yiqun Lin |
| [YARN-4709](https://issues.apache.org/jira/browse/YARN-4709) | NMWebServices produces incorrect JSON for containers |  Critical | . | Varun Saxena | Varun Saxena |
| [MAPREDUCE-6635](https://issues.apache.org/jira/browse/MAPREDUCE-6635) | Unsafe long to int conversion in UncompressedSplitLineReader and IndexOutOfBoundsException |  Critical | . | Sergey Shelukhin | Junping Du |
| [YARN-4651](https://issues.apache.org/jira/browse/YARN-4651) | movetoqueue option does not documented in 'YARN Commands' |  Major | documentation | Takashi Ohnishi | Takashi Ohnishi |
| [YARN-4648](https://issues.apache.org/jira/browse/YARN-4648) | Move preemption related tests from TestFairScheduler to TestFairSchedulerPreemption |  Major | fairscheduler | Karthik Kambatla | Kai Sasaki |
| [HDFS-9549](https://issues.apache.org/jira/browse/HDFS-9549) | TestCacheDirectives#testExceedsCapacity is flaky |  Major | . | Wei-Chiu Chuang | Xiao Chen |
| [YARN-2046](https://issues.apache.org/jira/browse/YARN-2046) | Out of band heartbeats are sent only on container kill and possibly too early |  Major | nodemanager | Jason Lowe | Ming Ma |
| [HDFS-9844](https://issues.apache.org/jira/browse/HDFS-9844) | Correct path creation in getTrashRoot to handle root dir |  Blocker | encryption | Zhe Zhang | Zhe Zhang |
| [YARN-4722](https://issues.apache.org/jira/browse/YARN-4722) | AsyncDispatcher logs redundant event queue sizes |  Major | . | Jason Lowe | Jason Lowe |
| [HADOOP-12070](https://issues.apache.org/jira/browse/HADOOP-12070) | Some of the bin/hadoop subcommands are not available on Windows |  Major | scripts | Kengo Seki | Kengo Seki |
| [HADOOP-12716](https://issues.apache.org/jira/browse/HADOOP-12716) | KerberosAuthenticator#doSpnegoSequence use incorrect class to determine isKeyTab in JDK8 |  Major | security | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-4729](https://issues.apache.org/jira/browse/YARN-4729) | SchedulerApplicationAttempt#getTotalRequiredResources can throw an NPE |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-4701](https://issues.apache.org/jira/browse/YARN-4701) | When task logs are not available, port 8041 is referenced instead of port 8042 |  Major | yarn | Haibo Chen | Haibo Chen |
| [HDFS-9858](https://issues.apache.org/jira/browse/HDFS-9858) | RollingFileSystemSink can throw an NPE on non-secure clusters |  Major | . | Daniel Templeton | Daniel Templeton |
| [YARN-4723](https://issues.apache.org/jira/browse/YARN-4723) | NodesListManager$UnknownNodeId ClassCastException |  Critical | resourcemanager | Jason Lowe | Kuhu Shukla |
| [HADOOP-12849](https://issues.apache.org/jira/browse/HADOOP-12849) | TestSymlinkLocalFSFileSystem fails intermittently |  Major | test | Mingliang Liu | Mingliang Liu |
| [HADOOP-12831](https://issues.apache.org/jira/browse/HADOOP-12831) | LocalFS/FSOutputSummer NPEs in constructor if bytes per checksum  set to 0 |  Minor | fs | Steve Loughran | Mingliang Liu |
| [HADOOP-12846](https://issues.apache.org/jira/browse/HADOOP-12846) | Credential Provider Recursive Dependencies |  Major | . | Larry McCay | Larry McCay |
| [HDFS-9864](https://issues.apache.org/jira/browse/HDFS-9864) | Correct reference for RENEWDELEGATIONTOKEN and CANCELDELEGATIONTOKEN in webhdfs doc |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4731](https://issues.apache.org/jira/browse/YARN-4731) | container-executor should not follow symlinks in recursive\_unlink\_children |  Blocker | . | Bibin A Chundatt | Colin P. McCabe |
| [HADOOP-12622](https://issues.apache.org/jira/browse/HADOOP-12622) | RetryPolicies (other than FailoverOnNetworkExceptionRetry) should put on retry failed reason or the log from RMProxy's retry could be very misleading. |  Critical | auto-failover | Junping Du | Junping Du |
| [YARN-4748](https://issues.apache.org/jira/browse/YARN-4748) | ApplicationHistoryManagerOnTimelineStore should not swallow exceptions on generateApplicationReport |  Major | timelineserver | Li Lu | Li Lu |
| [HADOOP-10321](https://issues.apache.org/jira/browse/HADOOP-10321) | TestCompositeService should cover all enumerations of adding a service to a parent service |  Major | . | Karthik Kambatla | Ray Chiang |
| [HADOOP-12851](https://issues.apache.org/jira/browse/HADOOP-12851) | S3AFileSystem Uptake of ProviderUtils.excludeIncompatibleCredentialProviders |  Major | fs/s3 | Larry McCay | Larry McCay |
| [HDFS-9870](https://issues.apache.org/jira/browse/HDFS-9870) | Remove unused imports from DFSUtil |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8791](https://issues.apache.org/jira/browse/HDFS-8791) | block ID-based DN storage layout can be very slow for datanode on ext4 |  Blocker | datanode | Nathan Roberts | Chris Trezzo |
| [HDFS-9880](https://issues.apache.org/jira/browse/HDFS-9880) | TestDatanodeRegistration fails occasionally |  Major | test | Kihwal Lee | Kihwal Lee |
| [HDFS-9881](https://issues.apache.org/jira/browse/HDFS-9881) | DistributedFileSystem#getTrashRoot returns incorrect path for encryption zones |  Critical | . | Andrew Wang | Andrew Wang |
| [HDFS-9766](https://issues.apache.org/jira/browse/HDFS-9766) | TestDataNodeMetrics#testDataNodeTimeSpend fails intermittently |  Major | test | Mingliang Liu | Xiao Chen |
| [HDFS-9851](https://issues.apache.org/jira/browse/HDFS-9851) | Name node throws NPE when setPermission is called on a path that does not exist |  Critical | namenode | David Yan | Brahma Reddy Battula |
| [HDFS-9886](https://issues.apache.org/jira/browse/HDFS-9886) | Configuration properties for hedged read is broken |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12870](https://issues.apache.org/jira/browse/HADOOP-12870) | Fix typo admininistration in CommandsManual.md |  Minor | documentation | Akira Ajisaka | John Zhuge |
| [HDFS-9048](https://issues.apache.org/jira/browse/HDFS-9048) | DistCp documentation is out-of-dated |  Major | . | Haohui Mai | Daisuke Kobayashi |
| [HADOOP-12871](https://issues.apache.org/jira/browse/HADOOP-12871) | Fix dead link to NativeLibraries.html in CommandsManual.md |  Minor | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [HADOOP-12872](https://issues.apache.org/jira/browse/HADOOP-12872) | Fix formatting in ServiceLevelAuth.md |  Trivial | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [MAPREDUCE-4785](https://issues.apache.org/jira/browse/MAPREDUCE-4785) | TestMRApp occasionally fails |  Major | mrv2, test | Jason Lowe | Haibo Chen |
| [HADOOP-12717](https://issues.apache.org/jira/browse/HADOOP-12717) | NPE when trying to rename a directory in Windows Azure Storage FileSystem |  Blocker | . | Robert Yokota | Robert Yokota |
| [YARN-4763](https://issues.apache.org/jira/browse/YARN-4763) | RMApps Page crashes with NPE |  Major | webapp | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4761](https://issues.apache.org/jira/browse/YARN-4761) | NMs reconnecting with changed capabilities can lead to wrong cluster resource calculations on fair scheduler |  Major | fairscheduler | Sangjin Lee | Sangjin Lee |
| [YARN-4737](https://issues.apache.org/jira/browse/YARN-4737) | Add CSRF filter support in YARN |  Major | nodemanager, resourcemanager, webapp | Jonathan Maron | Jonathan Maron |
| [YARN-4744](https://issues.apache.org/jira/browse/YARN-4744) | Too many signal to container failure in case of LCE |  Major | . | Bibin A Chundatt | Sidharta Seethana |
| [YARN-4760](https://issues.apache.org/jira/browse/YARN-4760) | proxy redirect to history server uses wrong URL |  Major | webapp | Jason Lowe | Eric Badger |
| [HDFS-9865](https://issues.apache.org/jira/browse/HDFS-9865) | TestBlockReplacement fails intermittently in trunk |  Major | test | Yiqun Lin | Yiqun Lin |
| [YARN-4762](https://issues.apache.org/jira/browse/YARN-4762) | NMs failing on DelegatingLinuxContainerRuntime init with LCE on |  Blocker | . | Vinod Kumar Vavilapalli | Sidharta Seethana |
| [HDFS-9812](https://issues.apache.org/jira/browse/HDFS-9812) | Streamer threads leak if failure happens when closing DFSOutputStream |  Major | hdfs-client | Yiqun Lin | Yiqun Lin |
| [HDFS-9919](https://issues.apache.org/jira/browse/HDFS-9919) | TestStandbyCheckpoints#testNonPrimarySBNUploadFSImage waitForCheckpoint incorrectly |  Minor | test | Yiqun Lin | Yiqun Lin |
| [YARN-4764](https://issues.apache.org/jira/browse/YARN-4764) | Application submission fails when submitted queue is not available in scheduler xml |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-12895](https://issues.apache.org/jira/browse/HADOOP-12895) | SSLFactory#createSSLSocketFactory exception message is wrong |  Trivial | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12688](https://issues.apache.org/jira/browse/HADOOP-12688) | Fix deadlinks in Compatibility.md |  Major | documentation | Akira Ajisaka | Gabor Liptak |
| [HADOOP-12903](https://issues.apache.org/jira/browse/HADOOP-12903) | IPC Server should allow suppressing exception logging by type, not log 'server too busy' messages |  Major | ipc | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-12906](https://issues.apache.org/jira/browse/HADOOP-12906) | AuthenticatedURL should convert a 404/Not Found into an FileNotFoundException. |  Minor | io, security | Steve Loughran | Steve Loughran |
| [HDFS-9934](https://issues.apache.org/jira/browse/HDFS-9934) | ReverseXML oiv processor should bail out if the XML file's layoutVersion doesn't match oiv's |  Major | tools | Colin P. McCabe | Colin P. McCabe |
| [HDFS-9933](https://issues.apache.org/jira/browse/HDFS-9933) | ReverseXML should be capitalized in oiv usage message |  Minor | tools | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-12899](https://issues.apache.org/jira/browse/HADOOP-12899) | External distribution stitching scripts do not work correctly on Windows. |  Blocker | build | Chris Nauroth | Chris Nauroth |
| [HDFS-9953](https://issues.apache.org/jira/browse/HDFS-9953) | Download File from UI broken after pagination |  Blocker | namenode | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9947](https://issues.apache.org/jira/browse/HDFS-9947) | Block#toString should not output information from derived classes |  Minor | . | Colin P. McCabe | Colin P. McCabe |
| [YARN-4816](https://issues.apache.org/jira/browse/YARN-4816) | SystemClock API broken in 2.9.0 |  Major | . | Siddharth Seth | Siddharth Seth |
| [HDFS-9904](https://issues.apache.org/jira/browse/HDFS-9904) | testCheckpointCancellationDuringUpload occasionally fails |  Major | test | Kihwal Lee | Yiqun Lin |
| [MAPREDUCE-6579](https://issues.apache.org/jira/browse/MAPREDUCE-6579) | JobStatus#getFailureInfo should not output diagnostic information when the job is running |  Blocker | test | Rohith Sharma K S | Akira Ajisaka |
| [HDFS-9780](https://issues.apache.org/jira/browse/HDFS-9780) | RollingFileSystemSink doesn't work on secure clusters |  Critical | . | Daniel Templeton | Daniel Templeton |
| [HADOOP-12888](https://issues.apache.org/jira/browse/HADOOP-12888) | Shell to disable bash and setsid support when running under JVM security manager |  Major | security | Costin Leau | Costin Leau |
| [YARN-4593](https://issues.apache.org/jira/browse/YARN-4593) | Deadlock in AbstractService.getConfig() |  Major | yarn | Steve Loughran | Steve Loughran |
| [HDFS-10173](https://issues.apache.org/jira/browse/HDFS-10173) | Typo in DataXceiverServer |  Trivial | datanode | Michael Han | Michael Han |
| [MAPREDUCE-6645](https://issues.apache.org/jira/browse/MAPREDUCE-6645) | TestWordStats outputs logs under directories other than target/test-dir |  Major | test | Akira Ajisaka | Gabor Liptak |
| [YARN-4812](https://issues.apache.org/jira/browse/YARN-4812) | TestFairScheduler#testContinuousScheduling fails intermittently |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HDFS-9874](https://issues.apache.org/jira/browse/HDFS-9874) | Long living DataXceiver threads cause volume shutdown to block. |  Critical | datanode | Rushabh S Shah | Rushabh S Shah |
| [HDFS-3677](https://issues.apache.org/jira/browse/HDFS-3677) | dfs.namenode.edits.dir.required missing from hdfs-default.xml |  Major | documentation, namenode | Todd Lipcon | Mark Yang |
| [HADOOP-12626](https://issues.apache.org/jira/browse/HADOOP-12626) | Intel ISA-L libraries should be added to the Dockerfile |  Blocker | io | Allen Wittenauer | Kai Zheng |
| [HDFS-7166](https://issues.apache.org/jira/browse/HDFS-7166) | SbNN Web UI shows #Under replicated blocks and #pending deletion blocks |  Major | ha | Juan Yu | Wei-Chiu Chuang |
| [YARN-4686](https://issues.apache.org/jira/browse/YARN-4686) | MiniYARNCluster.start() returns before cluster is completely started |  Major | test | Rohith Sharma K S | Eric Badger |
| [MAPREDUCE-6363](https://issues.apache.org/jira/browse/MAPREDUCE-6363) | [NNBench] Lease mismatch error when running with multiple mappers |  Critical | benchmarks | Brahma Reddy Battula | Bibin A Chundatt |
| [MAPREDUCE-6110](https://issues.apache.org/jira/browse/MAPREDUCE-6110) | JobHistoryServer CLI throws NullPointerException with job ids that do not exist |  Minor | jobhistoryserver | Li Lu | Kai Sasaki |
| [HDFS-10189](https://issues.apache.org/jira/browse/HDFS-10189) | PacketResponder#toString should include the downstreams for PacketResponderType.HAS\_DOWNSTREAM\_IN\_PIPELINE |  Minor | datanode | Joe Pallas | Joe Pallas |
| [MAPREDUCE-6580](https://issues.apache.org/jira/browse/MAPREDUCE-6580) | Test failure : TestMRJobsWithProfiler |  Major | . | Rohith Sharma K S | Eric Badger |
| [MAPREDUCE-6656](https://issues.apache.org/jira/browse/MAPREDUCE-6656) | [NNBench] OP\_DELETE operation isn't working after MAPREDUCE-6363 |  Blocker | . | J.Andreina | J.Andreina |
| [HDFS-10193](https://issues.apache.org/jira/browse/HDFS-10193) | fuse\_dfs segfaults if uid cannot be resolved to a username |  Major | fuse-dfs | John Thiltges | John Thiltges |
| [YARN-4820](https://issues.apache.org/jira/browse/YARN-4820) | ResourceManager web redirects in HA mode drops query parameters |  Major | . | Varun Vasudev | Varun Vasudev |
| [YARN-4850](https://issues.apache.org/jira/browse/YARN-4850) | test-fair-scheduler.xml isn't valid xml |  Blocker | fairscheduler, test | Allen Wittenauer | Yufei Gu |
| [MAPREDUCE-6535](https://issues.apache.org/jira/browse/MAPREDUCE-6535) | TaskID default constructor results in NPE on toString() |  Major | mrv2 | Daniel Templeton | Daniel Templeton |
| [HADOOP-12962](https://issues.apache.org/jira/browse/HADOOP-12962) | KMS key names are incorrectly encoded when creating key |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-12958](https://issues.apache.org/jira/browse/HADOOP-12958) | PhantomReference for filesystem statistics can trigger OOM |  Major | . | Jason Lowe | Sangjin Lee |
| [HADOOP-12873](https://issues.apache.org/jira/browse/HADOOP-12873) | Remove MRv1 terms from HttpAuthentication.md |  Major | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [HDFS-10182](https://issues.apache.org/jira/browse/HDFS-10182) | Hedged read might overwrite user's buf |  Major | . | zhouyingchao | zhouyingchao |
| [MAPREDUCE-6655](https://issues.apache.org/jira/browse/MAPREDUCE-6655) | Fix a typo (STRICT\_IE6) in Encrypted Shuffle |  Trivial | documentation | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12954](https://issues.apache.org/jira/browse/HADOOP-12954) | Add a way to change hadoop.security.token.service.use\_ip |  Major | security | Robert Kanter | Robert Kanter |
| [YARN-4773](https://issues.apache.org/jira/browse/YARN-4773) | Log aggregation performs extraneous filesystem operations when rolling log aggregation is disabled |  Minor | nodemanager | Jason Lowe | Jun Gong |
| [MAPREDUCE-6662](https://issues.apache.org/jira/browse/MAPREDUCE-6662) | Clear ASF Warnings on test data files |  Minor | . | Vinayakumar B | Vinayakumar B |
| [HDFS-9871](https://issues.apache.org/jira/browse/HDFS-9871) | "Bytes Being Moved" -ve(-1 B) when cluster was already balanced. |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4863](https://issues.apache.org/jira/browse/YARN-4863) | AHS Security login should be in serviceInit() instead of serviceStart() |  Major | timelineserver | Junping Du | Junping Du |
| [HDFS-10197](https://issues.apache.org/jira/browse/HDFS-10197) | TestFsDatasetCache failing intermittently due to timeout |  Major | test | Yiqun Lin | Yiqun Lin |
| [HDFS-9478](https://issues.apache.org/jira/browse/HDFS-9478) | Reason for failing ipc.FairCallQueue contruction should be thrown |  Minor | . | Archana T | Ajith S |
| [HDFS-10228](https://issues.apache.org/jira/browse/HDFS-10228) | TestHDFSCLI fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-4865](https://issues.apache.org/jira/browse/YARN-4865) | Track Reserved resources in ResourceUsage and QueueCapacities |  Major | resourcemanager | Sunil G | Sunil G |
| [HADOOP-12972](https://issues.apache.org/jira/browse/HADOOP-12972) | Lz4Compressor#getLibraryName returns the wrong version number |  Trivial | native | John Zhuge | Colin P. McCabe |
| [HDFS-5177](https://issues.apache.org/jira/browse/HDFS-5177) | blocksScheduled  count should be decremented for abandoned blocks |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HDFS-10223](https://issues.apache.org/jira/browse/HDFS-10223) | peerFromSocketAndKey performs SASL exchange before setting connection timeouts |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-10221](https://issues.apache.org/jira/browse/HDFS-10221) | Add .json to the rat exclusions |  Blocker | build | Ming Ma | Ming Ma |
| [HADOOP-12902](https://issues.apache.org/jira/browse/HADOOP-12902) | JavaDocs for SignerSecretProvider are out-of-date in AuthenticationFilter |  Major | documentation | Robert Kanter | Gabor Liptak |
| [YARN-4183](https://issues.apache.org/jira/browse/YARN-4183) | Clarify the behavior of timeline service config properties |  Major | . | Mit Desai | Naganarasimha G R |
| [HADOOP-12955](https://issues.apache.org/jira/browse/HADOOP-12955) | Fix bugs in the initialization of the ISA-L library JNI bindings |  Major | native | Kai Zheng | Kai Zheng |
| [YARN-4657](https://issues.apache.org/jira/browse/YARN-4657) | Javadoc comment is broken for Resources.multiplyByAndAddTo() |  Trivial | . | Daniel Templeton | Daniel Templeton |
| [HDFS-10253](https://issues.apache.org/jira/browse/HDFS-10253) | Fix TestRefreshCallQueue failure. |  Major | . | Brahma Reddy Battula | Xiaoyu Yao |
| [YARN-4746](https://issues.apache.org/jira/browse/YARN-4746) | yarn web services should convert parse failures of appId, appAttemptId and containerId to 400 |  Minor | webapp | Steve Loughran | Bibin A Chundatt |
| [HDFS-9599](https://issues.apache.org/jira/browse/HDFS-9599) | TestDecommissioningStatus.testDecommissionStatus occasionally fails |  Major | namenode | Wei-Chiu Chuang | Yiqun Lin |
| [YARN-4706](https://issues.apache.org/jira/browse/YARN-4706) | UI Hosting Configuration in TimelineServer doc is broken |  Critical | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10178](https://issues.apache.org/jira/browse/HDFS-10178) | Permanent write failures can happen if pipeline recoveries occur for the first packet |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-8496](https://issues.apache.org/jira/browse/HDFS-8496) | Calling stopWriter() with FSDatasetImpl lock held may block other threads |  Major | . | zhouyingchao | Colin P. McCabe |
| [HDFS-9917](https://issues.apache.org/jira/browse/HDFS-9917) | IBR accumulate more objects when SNN was down for sometime. |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4880](https://issues.apache.org/jira/browse/YARN-4880) | Running TestZKRMStateStorePerf with real zookeeper cluster throws NPE |  Major | . | Rohith Sharma K S | Sunil G |
| [YARN-4609](https://issues.apache.org/jira/browse/YARN-4609) | RM Nodes list page takes too much time to load |  Major | webapp | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-10239](https://issues.apache.org/jira/browse/HDFS-10239) | Fsshell mv fails if port usage doesn't match in src and destination paths |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-4893](https://issues.apache.org/jira/browse/YARN-4893) | Fix some intermittent test failures in TestRMAdminService |  Blocker | . | Junping Du | Brahma Reddy Battula |
| [YARN-4916](https://issues.apache.org/jira/browse/YARN-4916) | TestNMProxy.tesNMProxyRPCRetry fails. |  Minor | . | Tibor Kiss | Tibor Kiss |
| [YARN-4915](https://issues.apache.org/jira/browse/YARN-4915) | Fix typo in YARN Secure Containers documentation |  Trivial | documentation, yarn | Takashi Ohnishi | Takashi Ohnishi |
| [YARN-4917](https://issues.apache.org/jira/browse/YARN-4917) | Fix typos in documentation of Capacity Scheduler. |  Minor | documentation | Takashi Ohnishi | Takashi Ohnishi |
| [HDFS-10261](https://issues.apache.org/jira/browse/HDFS-10261) | TestBookKeeperHACheckpoints doesn't handle ephemeral HTTP ports |  Major | . | Eric Badger | Eric Badger |
| [YARN-4699](https://issues.apache.org/jira/browse/YARN-4699) | Scheduler UI and REST o/p is not in sync when -replaceLabelsOnNode is used to change label of a node |  Critical | capacity scheduler | Sunil G | Sunil G |
| [YARN-4906](https://issues.apache.org/jira/browse/YARN-4906) | Capture container start/finish time in container metrics |  Major | . | Jian He | Jian He |
| [HADOOP-12022](https://issues.apache.org/jira/browse/HADOOP-12022) | fix site -Pdocs -Pdist in hadoop-project-dist; cleanout remaining forrest bits |  Blocker | build | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6670](https://issues.apache.org/jira/browse/MAPREDUCE-6670) | TestJobListCache#testEviction sometimes fails on Windows with timeout |  Minor | test | Gergely Novák | Gergely Novák |
| [HDFS-10192](https://issues.apache.org/jira/browse/HDFS-10192) | Namenode safemode not coming out during failover |  Major | namenode | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-6520](https://issues.apache.org/jira/browse/HDFS-6520) | hdfs fsck -move passes invalid length value when creating BlockReader |  Major | . | Shengjun Xin | Xiao Chen |
| [MAPREDUCE-6647](https://issues.apache.org/jira/browse/MAPREDUCE-6647) | MR usage counters use the resources requested instead of the resources allocated |  Major | . | Haibo Chen | Haibo Chen |
| [HDFS-10267](https://issues.apache.org/jira/browse/HDFS-10267) | Extra "synchronized" on FsDatasetImpl#recoverAppend and FsDatasetImpl#recoverClose |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-13006](https://issues.apache.org/jira/browse/HADOOP-13006) | FileContextMainOperationsBaseTest.testListStatusThrowsExceptionForNonExistentFile() doesnt run |  Minor | test | Steve Loughran | Kai Sasaki |
| [YARN-4740](https://issues.apache.org/jira/browse/YARN-4740) | AM may not receive the container complete msg when it restarts |  Major | . | sandflee | sandflee |
| [MAPREDUCE-6633](https://issues.apache.org/jira/browse/MAPREDUCE-6633) | AM should retry map attempts if the reduce task encounters commpression related errors. |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [YARN-4927](https://issues.apache.org/jira/browse/YARN-4927) | TestRMHA#testTransitionedToActiveRefreshFail fails with FairScheduler |  Major | test | Karthik Kambatla | Bibin A Chundatt |
| [YARN-4938](https://issues.apache.org/jira/browse/YARN-4938) | MiniYarnCluster should not request transitionToActive to RM on non-HA environment |  Major | test | Akira Ajisaka | Eric Badger |
| [YARN-4562](https://issues.apache.org/jira/browse/YARN-4562) | YARN WebApp ignores the configuration passed to it for keystore settings |  Major | . | Sergey Shelukhin | Sergey Shelukhin |
| [HADOOP-12406](https://issues.apache.org/jira/browse/HADOOP-12406) | AbstractMapWritable.readFields throws ClassNotFoundException with custom writables |  Blocker | io | Nadeem Douba | Nadeem Douba |
| [HADOOP-12993](https://issues.apache.org/jira/browse/HADOOP-12993) | Change ShutdownHookManger complete shutdown log from INFO to DEBUG |  Minor | . | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-4897](https://issues.apache.org/jira/browse/YARN-4897) | dataTables\_wrapper change min height |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-10277](https://issues.apache.org/jira/browse/HDFS-10277) | PositionedReadable test testReadFullyZeroByteFile failing in HDFS |  Major | test | Steve Loughran | Steve Loughran |
| [HDFS-10271](https://issues.apache.org/jira/browse/HDFS-10271) | Extra bytes are getting released from reservedSpace for append |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8356](https://issues.apache.org/jira/browse/HDFS-8356) | Document missing properties in hdfs-default.xml |  Major | documentation | Ray Chiang | Ray Chiang |
| [YARN-4810](https://issues.apache.org/jira/browse/YARN-4810) | NM applicationpage cause internal error 500 |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-10273](https://issues.apache.org/jira/browse/HDFS-10273) | Remove duplicate logSync() and log message in FSN#enterSafemode() |  Minor | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-12964](https://issues.apache.org/jira/browse/HADOOP-12964) | Http server vulnerable to clickjacking |  Major | . | Haibo Chen | Haibo Chen |
| [YARN-4794](https://issues.apache.org/jira/browse/YARN-4794) | Deadlock in NMClientImpl |  Critical | . | Sumana Sathish | Jian He |
| [HDFS-9772](https://issues.apache.org/jira/browse/HDFS-9772) | TestBlockReplacement#testThrottler doesn't work as expected |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-10270](https://issues.apache.org/jira/browse/HDFS-10270) | TestJMXGet:testNameNode() fails |  Minor | test | Andras Bokor | Gergely Novák |
| [HDFS-10282](https://issues.apache.org/jira/browse/HDFS-10282) | The VolumeScanner should warn about replica files which are misplaced |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-10216](https://issues.apache.org/jira/browse/HDFS-10216) | distcp -diff relative path exception |  Major | distcp | John Zhuge | Takashi Ohnishi |
| [YARN-4924](https://issues.apache.org/jira/browse/YARN-4924) | NM recovery race can lead to container not cleaned up |  Major | nodemanager | Nathan Roberts | sandflee |
| [HADOOP-12989](https://issues.apache.org/jira/browse/HADOOP-12989) | Some tests in org.apache.hadoop.fs.shell.find occasionally time out |  Major | test | Akira Ajisaka | Takashi Ohnishi |
| [HADOOP-13026](https://issues.apache.org/jira/browse/HADOOP-13026) | Should not wrap IOExceptions into a AuthenticationException in KerberosAuthenticator |  Critical | . | Xuan Gong | Xuan Gong |
| [HDFS-10283](https://issues.apache.org/jira/browse/HDFS-10283) | o.a.h.hdfs.server.namenode.TestFSImageWithSnapshot#testSaveLoadImageWithAppending fails intermittently |  Major | test | Mingliang Liu | Mingliang Liu |
| [YARN-4940](https://issues.apache.org/jira/browse/YARN-4940) | yarn node -list -all failed if RM start with decommissioned node |  Major | . | sandflee | sandflee |
| [YARN-4965](https://issues.apache.org/jira/browse/YARN-4965) | Distributed shell AM failed due to ClientHandlerException thrown by jersey |  Critical | . | Sumana Sathish | Junping Du |
| [YARN-4934](https://issues.apache.org/jira/browse/YARN-4934) | Reserved Resource for QueueMetrics needs to be handled correctly in few cases |  Major | capacity scheduler | Sunil G | Sunil G |
| [HDFS-10291](https://issues.apache.org/jira/browse/HDFS-10291) | TestShortCircuitLocalRead failing |  Major | test | Steve Loughran | Steve Loughran |
| [HDFS-10275](https://issues.apache.org/jira/browse/HDFS-10275) | TestDataNodeMetrics failing intermittently due to TotalWriteTime counted incorrectly |  Major | test | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-6649](https://issues.apache.org/jira/browse/MAPREDUCE-6649) | getFailureInfo not returning any failure info |  Major | . | Eric Badger | Eric Badger |
| [HDFS-10265](https://issues.apache.org/jira/browse/HDFS-10265) | OEV tool fails to read edit xml file if OP\_UPDATE\_BLOCKS has no BLOCK tag |  Minor | tools | Wan Chang | Wan Chang |
| [HDFS-10306](https://issues.apache.org/jira/browse/HDFS-10306) | SafeModeMonitor should not leave safe mode if name system is starting active service |  Major | namenode | Mingliang Liu | Mingliang Liu |
| [HDFS-9744](https://issues.apache.org/jira/browse/HDFS-9744) | TestDirectoryScanner#testThrottling occasionally time out after 300 seconds |  Minor | datanode | Wei-Chiu Chuang | Yiqun Lin |
| [HDFS-10308](https://issues.apache.org/jira/browse/HDFS-10308) | TestRetryCacheWithHA#testRetryCacheOnStandbyNN failing |  Major | test | Rakesh R | Rakesh R |
| [HDFS-10312](https://issues.apache.org/jira/browse/HDFS-10312) | Large block reports may fail to decode at NameNode due to 64 MB protobuf maximum length restriction. |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6680](https://issues.apache.org/jira/browse/MAPREDUCE-6680) | JHS UserLogDir scan algorithm sometime could skip directory with update in CloudFS (Azure FileSystem, S3, etc.) |  Major | jobhistoryserver | Junping Du | Junping Du |
| [YARN-4935](https://issues.apache.org/jira/browse/YARN-4935) | TestYarnClient#testSubmitIncorrectQueue fails with FairScheduler |  Major | test | Yufei Gu | Yufei Gu |
| [MAPREDUCE-2398](https://issues.apache.org/jira/browse/MAPREDUCE-2398) | MRBench: setting the baseDir parameter has no effect |  Minor | benchmarks | Michael Noll | Wilfred Spiegelenburg |
| [YARN-4976](https://issues.apache.org/jira/browse/YARN-4976) | Missing NullPointer check in ContainerLaunchContextPBImpl causes RM to die |  Major | resourcemanager | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-9670](https://issues.apache.org/jira/browse/HDFS-9670) | DistCp throws NPE when source is root |  Major | distcp | Yongjun Zhang | John Zhuge |
| [HADOOP-13042](https://issues.apache.org/jira/browse/HADOOP-13042) | Restore lost leveldbjni LICENSE and NOTICE changes |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-13043](https://issues.apache.org/jira/browse/HADOOP-13043) | Add LICENSE.txt entries for bundled javascript dependencies |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-10319](https://issues.apache.org/jira/browse/HDFS-10319) | Balancer should not try to pair storages with different types |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-10309](https://issues.apache.org/jira/browse/HDFS-10309) | Balancer doesn't honor dfs.blocksize value defined with suffix k(kilo), m(mega), g(giga) |  Minor | balancer & mover | Amit Anand | Amit Anand |
| [HDFS-9555](https://issues.apache.org/jira/browse/HDFS-9555) | LazyPersistFileScrubber should still sleep if there are errors in the clear progress |  Major | . | Phil Yang | Phil Yang |
| [HADOOP-13052](https://issues.apache.org/jira/browse/HADOOP-13052) | ChecksumFileSystem mishandles crc file permissions |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HDFS-9905](https://issues.apache.org/jira/browse/HDFS-9905) | WebHdfsFileSystem#runWithRetry should display original stack trace on error |  Major | test | Kihwal Lee | Wei-Chiu Chuang |
| [HADOOP-11418](https://issues.apache.org/jira/browse/HADOOP-11418) | Property "io.compression.codec.lzo.class" does not work with other value besides default |  Major | io | fang fang chen | Yuanbo Liu |
| [HDFS-10318](https://issues.apache.org/jira/browse/HDFS-10318) | TestJMXGet hides the real error in case of test failure |  Minor | test | Andras Bokor | Andras Bokor |
| [HDFS-10325](https://issues.apache.org/jira/browse/HDFS-10325) | TestDFSShell#testMoveWithTargetPortEmpty fails |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-4795](https://issues.apache.org/jira/browse/YARN-4795) | ContainerMetrics drops records |  Major | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-4556](https://issues.apache.org/jira/browse/YARN-4556) |  TestFifoScheduler.testResourceOverCommit fails |  Major | scheduler, test | Akihiro Suda | Akihiro Suda |
| [HDFS-10329](https://issues.apache.org/jira/browse/HDFS-10329) | Bad initialisation of StringBuffer in RequestHedgingProxyProvider.java |  Minor | ha | Max Schaefer | Yiqun Lin |
| [HDFS-10313](https://issues.apache.org/jira/browse/HDFS-10313) | Distcp need to enforce the order of snapshot names passed to -diff |  Major | distcp | Yongjun Zhang | Yiqun Lin |
| [YARN-4983](https://issues.apache.org/jira/browse/YARN-4983) | JVM and UGI metrics disappear after RM is once transitioned to standby mode |  Major | . | Li Lu | Li Lu |
| [MAPREDUCE-6286](https://issues.apache.org/jira/browse/MAPREDUCE-6286) | A typo in HistoryViewer makes some code useless, which causes counter limits are not reset correctly. |  Major | client | zhihai xu | zhihai xu |
| [MAPREDUCE-6199](https://issues.apache.org/jira/browse/MAPREDUCE-6199) | AbstractCounters are not reset completely on deserialization |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-13030](https://issues.apache.org/jira/browse/HADOOP-13030) | Handle special characters in passwords in KMS startup script |  Major | kms | Xiao Chen | Xiao Chen |
| [YARN-4955](https://issues.apache.org/jira/browse/YARN-4955) | Add retry for SocketTimeoutException in TimelineClient |  Critical | . | Xuan Gong | Xuan Gong |
| [HDFS-9958](https://issues.apache.org/jira/browse/HDFS-9958) | BlockManager#createLocatedBlocks can throw NPE for corruptBlocks on failed storages. |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-5008](https://issues.apache.org/jira/browse/YARN-5008) | LeveldbRMStateStore database can grow substantially leading to long recovery times |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-5009](https://issues.apache.org/jira/browse/YARN-5009) | NMLeveldbStateStoreService database can grow substantially leading to longer recovery times |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [HADOOP-12378](https://issues.apache.org/jira/browse/HADOOP-12378) | Fix findbugs warnings in hadoop-tools module |  Major | tools | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10260](https://issues.apache.org/jira/browse/HDFS-10260) | TestFsDatasetImpl#testCleanShutdownOfVolume often fails |  Major | datanode, test | Wei-Chiu Chuang | Rushabh S Shah |
| [HDFS-10335](https://issues.apache.org/jira/browse/HDFS-10335) | Mover$Processor#chooseTarget() always chooses the first matching target storage group |  Critical | balancer & mover | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-6672](https://issues.apache.org/jira/browse/MAPREDUCE-6672) | TestTeraSort fails on Windows |  Minor | test | Tibor Kiss | Tibor Kiss |
| [HDFS-10347](https://issues.apache.org/jira/browse/HDFS-10347) | Namenode report bad block method doesn't log the bad block or datanode. |  Minor | namenode | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-13012](https://issues.apache.org/jira/browse/HADOOP-13012) | yetus-wrapper should fail sooner when download fails |  Minor | yetus | Steven K. Wong | Steven K. Wong |
| [HADOOP-13072](https://issues.apache.org/jira/browse/HADOOP-13072) | WindowsGetSpaceUsed constructor should be public |  Major | . | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-6537](https://issues.apache.org/jira/browse/MAPREDUCE-6537) | Include hadoop-pipes examples in the release tarball |  Blocker | pipes | Allen Wittenauer | Kai Sasaki |
| [HDFS-10353](https://issues.apache.org/jira/browse/HDFS-10353) | Fix hadoop-hdfs-native-client compilation on Windows |  Blocker | build | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10344](https://issues.apache.org/jira/browse/HDFS-10344) | DistributedFileSystem#getTrashRoots should skip encryption zone that does not have .Trash |  Major | . | Namit Maheshwari | Xiaoyu Yao |
| [HADOOP-13080](https://issues.apache.org/jira/browse/HADOOP-13080) | Refresh time in SysInfoWindows is in nanoseconds |  Major | util | Inigo Goiri | Inigo Goiri |
| [YARN-4834](https://issues.apache.org/jira/browse/YARN-4834) | ProcfsBasedProcessTree doesn't track daemonized processes |  Major | nodemanager | Nathan Roberts | Nathan Roberts |
| [HADOOP-12469](https://issues.apache.org/jira/browse/HADOOP-12469) | distcp should not ignore the ignoreFailures option |  Critical | tools/distcp | Gera Shegalov | Mingliang Liu |
| [HDFS-10320](https://issues.apache.org/jira/browse/HDFS-10320) | Rack failures may result in NN terminate |  Major | . | Xiao Chen | Xiao Chen |
| [MAPREDUCE-6677](https://issues.apache.org/jira/browse/MAPREDUCE-6677) | LocalContainerAllocator doesn't specify resource of the containers allocated. |  Major | mr-am | Haibo Chen | Haibo Chen |
| [MAPREDUCE-6675](https://issues.apache.org/jira/browse/MAPREDUCE-6675) | TestJobImpl.testUnusableNode failed |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [YARN-4311](https://issues.apache.org/jira/browse/YARN-4311) | Removing nodes from include and exclude lists will not remove them from decommissioned nodes list |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-4984](https://issues.apache.org/jira/browse/YARN-4984) | LogAggregationService shouldn't swallow exception in handling createAppDir() which cause thread leak. |  Critical | log-aggregation | Junping Du | Junping Du |
| [HADOOP-13077](https://issues.apache.org/jira/browse/HADOOP-13077) | Handle special characters in passwords in httpfs.sh |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-13098](https://issues.apache.org/jira/browse/HADOOP-13098) | Dynamic LogLevel setting page should accept case-insensitive log level string |  Major | . | Junping Du | Junping Du |
| [HDFS-10324](https://issues.apache.org/jira/browse/HDFS-10324) | Trash directory in an encryption zone should be pre-created with correct permissions |  Major | encryption | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-6514](https://issues.apache.org/jira/browse/MAPREDUCE-6514) | Job hangs as ask is not updated after ramping down of all reducers |  Blocker | applicationmaster | Varun Saxena | Varun Saxena |
| [HDFS-2043](https://issues.apache.org/jira/browse/HDFS-2043) | TestHFlush failing intermittently |  Major | test | Aaron T. Myers | Yiqun Lin |
| [HADOOP-12751](https://issues.apache.org/jira/browse/HADOOP-12751) | While using kerberos Hadoop incorrectly assumes names with '@' to be non-simple |  Critical | security | Bolke de Bruin | Bolke de Bruin |
| [YARN-5002](https://issues.apache.org/jira/browse/YARN-5002) | getApplicationReport call may raise NPE for removed queues |  Critical | . | Sumana Sathish | Jian He |
| [MAPREDUCE-6689](https://issues.apache.org/jira/browse/MAPREDUCE-6689) | MapReduce job can infinitely increase number of reducer resource requests |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-4747](https://issues.apache.org/jira/browse/YARN-4747) | AHS error 500 due to NPE when container start event is missing |  Major | timelineserver | Jason Lowe | Varun Saxena |
| [HDFS-9939](https://issues.apache.org/jira/browse/HDFS-9939) | Increase DecompressorStream skip buffer size |  Major | . | Yongjun Zhang | John Zhuge |
| [HDFS-10374](https://issues.apache.org/jira/browse/HDFS-10374) | typo in HDFSHighAvailabilityWithQJM doc. |  Minor | documentation | Takashi Ohnishi | Takashi Ohnishi |
| [YARN-5048](https://issues.apache.org/jira/browse/YARN-5048) | DelegationTokenRenewer#skipTokenRenewal may throw NPE |  Major | . | Jian He | Jian He |
| [YARN-4926](https://issues.apache.org/jira/browse/YARN-4926) | Change nodelabel rest API invalid reponse status to 400 |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-10372](https://issues.apache.org/jira/browse/HDFS-10372) | Fix for failing TestFsDatasetImpl#testCleanShutdownOfVolume |  Major | test | Rushabh S Shah | Rushabh S Shah |
| [MAPREDUCE-6684](https://issues.apache.org/jira/browse/MAPREDUCE-6684) | High contention on scanning of user directory under immediate\_done in Job History Server |  Critical | jobhistoryserver | Haibo Chen | Haibo Chen |
| [HADOOP-13118](https://issues.apache.org/jira/browse/HADOOP-13118) | Fix IOUtils#cleanup and IOUtils#closeStream javadoc |  Trivial | io | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4768](https://issues.apache.org/jira/browse/YARN-4768) | getAvailablePhysicalMemorySize can be inaccurate on linux |  Major | nodemanager | Nathan Roberts | Nathan Roberts |
| [HADOOP-10694](https://issues.apache.org/jira/browse/HADOOP-10694) | Remove synchronized input streams from Writable deserialization |  Major | io | Gopal V | Gopal V |
| [HADOOP-13125](https://issues.apache.org/jira/browse/HADOOP-13125) | FS Contract tests don't report FS initialization errors well |  Minor | test | Steve Loughran | Steve Loughran |
| [YARN-5029](https://issues.apache.org/jira/browse/YARN-5029) | RM needs to send update event with YarnApplicationState as Running to ATS/AHS |  Critical | . | Xuan Gong | Xuan Gong |
| [HADOOP-13116](https://issues.apache.org/jira/browse/HADOOP-13116) | Jets3tNativeS3FileSystemContractTest does not run. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6639](https://issues.apache.org/jira/browse/MAPREDUCE-6639) | Process hangs in LocatedFileStatusFetcher if FileSystem.get throws |  Major | mrv2 | Ryan Blue | Ryan Blue |
| [HADOOP-11180](https://issues.apache.org/jira/browse/HADOOP-11180) | Change log message "token.Token: Cannot find class for token kind kms-dt" to debug |  Major | kms, security | Yi Liu | Yi Liu |
| [MAPREDUCE-6558](https://issues.apache.org/jira/browse/MAPREDUCE-6558) | multibyte delimiters with compressed input files generate duplicate records |  Major | mrv1, mrv2 | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-13083](https://issues.apache.org/jira/browse/HADOOP-13083) | The number of javadocs warnings is limited to 100 |  Critical | . | Li Lu | Gergely Novák |
| [MAPREDUCE-6513](https://issues.apache.org/jira/browse/MAPREDUCE-6513) | MR job got hanged forever when one NM unstable for some time |  Critical | applicationmaster, resourcemanager | Bob.zhao | Varun Saxena |
| [HDFS-10333](https://issues.apache.org/jira/browse/HDFS-10333) | Intermittent org.apache.hadoop.hdfs.TestFileAppend failure in trunk |  Major | hdfs | Yongjun Zhang | Yiqun Lin |
| [HADOOP-12942](https://issues.apache.org/jira/browse/HADOOP-12942) | hadoop credential commands non-obviously use password of "none" |  Major | security | Mike Yoder | Mike Yoder |
| [YARN-4325](https://issues.apache.org/jira/browse/YARN-4325) | Nodemanager log handlers fail to send finished/failed events in some cases |  Critical | . | Junping Du | Junping Du |
| [HDFS-10410](https://issues.apache.org/jira/browse/HDFS-10410) | RedundantEditLogInputStream#LOG is set to wrong class |  Minor | . | John Zhuge | John Zhuge |
| [HDFS-10208](https://issues.apache.org/jira/browse/HDFS-10208) | Addendum for HDFS-9579: to handle the case when client machine can't resolve network path |  Major | . | Ming Ma | Ming Ma |
| [HDFS-10242](https://issues.apache.org/jira/browse/HDFS-10242) | Cannot create space quota of zero |  Major | documentation, fs | Takashi Ohnishi | Takashi Ohnishi |
| [MAPREDUCE-6693](https://issues.apache.org/jira/browse/MAPREDUCE-6693) | ArrayIndexOutOfBoundsException occurs when the length of the job name is equal to mapreduce.jobhistory.jobname.limit |  Critical | . | Bibin A Chundatt | Ajith S |
| [HADOOP-13163](https://issues.apache.org/jira/browse/HADOOP-13163) | Reuse pre-computed filestatus in Distcp-CopyMapper |  Minor | tools/distcp | Rajesh Balamohan | Rajesh Balamohan |
| [HDFS-10303](https://issues.apache.org/jira/browse/HDFS-10303) | DataStreamer#ResponseProcessor calculates packet ack latency incorrectly. |  Major | hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [MAPREDUCE-6701](https://issues.apache.org/jira/browse/MAPREDUCE-6701) | application master log can not be available when clicking jobhistory's am logs link |  Critical | jobhistoryserver | chenyukang | Haibo Chen |
| [MAPREDUCE-6698](https://issues.apache.org/jira/browse/MAPREDUCE-6698) | Increase timeout on TestUnnecessaryBlockingOnHistoryFileInfo.testTwoThreadsQueryingDifferentJobOfSameUser |  Major | jobhistoryserver | Haibo Chen | Haibo Chen |
| [HDFS-10404](https://issues.apache.org/jira/browse/HDFS-10404) | Fix formatting of CacheAdmin command usage help text |  Major | caching | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-6657](https://issues.apache.org/jira/browse/MAPREDUCE-6657) | job history server can fail on startup when NameNode is in start phase |  Major | jobhistoryserver | Haibo Chen | Haibo Chen |
| [HADOOP-13159](https://issues.apache.org/jira/browse/HADOOP-13159) | Fix potential NPE in Metrics2 source for DecayRpcScheduler |  Major | ipc | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-10397](https://issues.apache.org/jira/browse/HDFS-10397) | Distcp should ignore -delete option if -diff option is provided instead of exiting |  Major | distcp | Mingliang Liu | Mingliang Liu |
| [HDFS-10381](https://issues.apache.org/jira/browse/HDFS-10381) | DataStreamer DataNode exclusion log message should be warning |  Minor | hdfs-client | John Zhuge | John Zhuge |
| [HDFS-10360](https://issues.apache.org/jira/browse/HDFS-10360) | DataNode may format directory and lose blocks if current/VERSION is missing |  Major | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9226](https://issues.apache.org/jira/browse/HDFS-9226) | MiniDFSCluster leaks dependency Mockito via DataNodeTestUtils |  Major | test | Josh Elser | Josh Elser |
| [HADOOP-13138](https://issues.apache.org/jira/browse/HADOOP-13138) | Unable to append to a SequenceFile with Compression.NONE. |  Critical | . | Gervais Mickaël | Vinayakumar B |
| [YARN-4925](https://issues.apache.org/jira/browse/YARN-4925) | ContainerRequest in AMRMClient, application should be able to specify nodes/racks together with nodeLabelExpression |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-13157](https://issues.apache.org/jira/browse/HADOOP-13157) | Follow-on improvements to hadoop credential commands |  Major | security | Mike Yoder | Mike Yoder |
| [YARN-3840](https://issues.apache.org/jira/browse/YARN-3840) | Resource Manager web ui issue when sorting application by id (with application having id \> 9999) |  Major | resourcemanager | LINTE | Varun Saxena |
| [YARN-5107](https://issues.apache.org/jira/browse/YARN-5107) | TestContainerMetrics fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-13177](https://issues.apache.org/jira/browse/HADOOP-13177) | Native tests fail on OS X, because DYLD\_LIBRARY\_PATH is not defined to include libhadoop.dylib. |  Minor | build | Chris Nauroth | Chris Nauroth |
| [HADOOP-12767](https://issues.apache.org/jira/browse/HADOOP-12767) | update apache httpclient version to 4.5.2; httpcore to 4.4.4 |  Major | build | Artem Aliev | Artem Aliev |
| [YARN-5100](https://issues.apache.org/jira/browse/YARN-5100) | The YarnApplicationState is always running in ATS no matter the application is running or finishes. |  Blocker | . | Xuan Gong | Xuan Gong |
| [YARN-5076](https://issues.apache.org/jira/browse/YARN-5076) | YARN web interfaces lack XFS protection |  Major | nodemanager, resourcemanager, timelineserver | Jonathan Maron | Jonathan Maron |
| [HADOOP-13183](https://issues.apache.org/jira/browse/HADOOP-13183) | S3A proxy tests fail after httpclient/httpcore upgrade. |  Major | fs/s3 | Chris Nauroth | Steve Loughran |
| [YARN-5020](https://issues.apache.org/jira/browse/YARN-5020) | Fix Documentation for Yarn Capacity Scheduler on Resource Calculator |  Minor | . | Jo Desmet | Takashi Ohnishi |
| [HDFS-10424](https://issues.apache.org/jira/browse/HDFS-10424) | DatanodeLifelineProtocol not able to use under security cluster |  Blocker | . | gu-chi | Chris Nauroth |
| [HDFS-10438](https://issues.apache.org/jira/browse/HDFS-10438) | When NameNode HA is configured to use the lifeline RPC server, it should log the address of that server. |  Minor | ha, namenode | KWON BYUNGCHANG | Chris Nauroth |
| [HDFS-10439](https://issues.apache.org/jira/browse/HDFS-10439) | Update setOwner doc in HdfsPermissionsGuide |  Minor | documentation | John Zhuge | John Zhuge |
| [MAPREDUCE-6607](https://issues.apache.org/jira/browse/MAPREDUCE-6607) | Enable regex pattern matching when mapreduce.task.files.preserve.filepattern is set |  Minor | applicationmaster | Maysam Yabandeh | Kai Sasaki |
| [YARN-5112](https://issues.apache.org/jira/browse/YARN-5112) | Excessive log warnings for directory permission issue on NM recovery. |  Major | . | Jian He | Jian He |
| [YARN-5103](https://issues.apache.org/jira/browse/YARN-5103) | With NM recovery enabled, restarting NM multiple times results in AM restart |  Critical | yarn | Sumana Sathish | Junping Du |
| [YARN-5055](https://issues.apache.org/jira/browse/YARN-5055) | max apps per user can be larger than max per queue |  Minor | capacityscheduler, resourcemanager | Jason Lowe | Eric Badger |
| [YARN-4979](https://issues.apache.org/jira/browse/YARN-4979) | FSAppAttempt demand calculation considers demands at multiple locality levels different |  Major | fairscheduler | zhihai xu | zhihai xu |
| [YARN-3971](https://issues.apache.org/jira/browse/YARN-3971) | Skip RMNodeLabelsManager#checkRemoveFromClusterNodeLabelsOfQueue on nodelabel recovery |  Critical | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9365](https://issues.apache.org/jira/browse/HDFS-9365) | Balancer does not work with the HDFS-6376 HA setup |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-6703](https://issues.apache.org/jira/browse/MAPREDUCE-6703) | Add flag to allow MapReduce AM to request for OPPORTUNISTIC containers |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-5131](https://issues.apache.org/jira/browse/YARN-5131) | Distributed shell AM fails when extra container arrives during finishing |  Major | . | Sumana Sathish | Wangda Tan |
| [YARN-3344](https://issues.apache.org/jira/browse/YARN-3344) | Fix warning - procfs stat file is not in the expected format |  Major | . | Jon Bringhurst | Ravindra Kumar Naik |
| [YARN-4459](https://issues.apache.org/jira/browse/YARN-4459) | container-executor should only kill process groups |  Major | nodemanager | Jun Gong | Jun Gong |
| [YARN-4866](https://issues.apache.org/jira/browse/YARN-4866) | FairScheduler: AMs can consume all vcores leading to a livelock when using FAIR policy |  Major | fairscheduler | Karthik Kambatla | Yufei Gu |
| [HDFS-10463](https://issues.apache.org/jira/browse/HDFS-10463) | TestRollingFileSystemSinkWithHdfs needs some cleanup |  Critical | . | Daniel Templeton | Daniel Templeton |
| [YARN-5166](https://issues.apache.org/jira/browse/YARN-5166) | javadoc:javadoc goal fails on hadoop-yarn-client |  Major | . | Andras Bokor | Andras Bokor |
| [HDFS-10276](https://issues.apache.org/jira/browse/HDFS-10276) | HDFS should not expose path info that user has no permission to see. |  Major | . | Kevin Cox | Yuanbo Liu |
| [YARN-5132](https://issues.apache.org/jira/browse/YARN-5132) | Exclude generated protobuf sources from YARN Javadoc build |  Critical | . | Subru Krishnan | Subru Krishnan |
| [HADOOP-13132](https://issues.apache.org/jira/browse/HADOOP-13132) | Handle ClassCastException on AuthenticationException in LoadBalancingKMSClientProvider |  Major | kms | Miklos Szurap | Wei-Chiu Chuang |
| [HDFS-10415](https://issues.apache.org/jira/browse/HDFS-10415) | TestDistributedFileSystem#MyDistributedFileSystem attempts to set up statistics before initialize() is called |  Major | test | Sangjin Lee | Mingliang Liu |
| [HADOOP-13137](https://issues.apache.org/jira/browse/HADOOP-13137) | TraceAdmin should support Kerberized cluster |  Major | tracing | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9476](https://issues.apache.org/jira/browse/HDFS-9476) | TestDFSUpgradeFromImage#testUpgradeFromRel1BBWImage occasionally fail |  Major | . | Wei-Chiu Chuang | Masatake Iwasaki |
| [HDFS-10367](https://issues.apache.org/jira/browse/HDFS-10367) | TestDFSShell.testMoveWithTargetPortEmpty fails with Address bind exception. |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-13224](https://issues.apache.org/jira/browse/HADOOP-13224) | Grep job in Single Cluster document fails |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10471](https://issues.apache.org/jira/browse/HDFS-10471) | DFSAdmin#SetQuotaCommand's help msg is not correct |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-5190](https://issues.apache.org/jira/browse/YARN-5190) | Registering/unregistering container metrics triggered by ContainerEvent and ContainersMonitorEvent are conflict which cause uncaught exception in ContainerMonitorImpl |  Blocker | . | Junping Du | Junping Du |
| [YARN-5098](https://issues.apache.org/jira/browse/YARN-5098) | Yarn Application log Aggreagation fails due to NM can not get correct HDFS delegation token |  Major | yarn | Yesha Vora | Jian He |
| [HADOOP-13155](https://issues.apache.org/jira/browse/HADOOP-13155) | Implement TokenRenewer to renew and cancel delegation tokens in KMS |  Major | kms, security | Xiao Chen | Xiao Chen |
| [HDFS-10481](https://issues.apache.org/jira/browse/HDFS-10481) | HTTPFS server should correctly impersonate as end user to open file |  Major | httpfs | Xiao Chen | Xiao Chen |
| [HDFS-10485](https://issues.apache.org/jira/browse/HDFS-10485) | Fix findbugs warning in FSEditLog.java |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10458](https://issues.apache.org/jira/browse/HDFS-10458) | getFileEncryptionInfo should return quickly for non-encrypted cluster |  Major | encryption, namenode | Zhe Zhang | Zhe Zhang |
| [HDFS-10468](https://issues.apache.org/jira/browse/HDFS-10468) | HDFS read ends up ignoring an interrupt |  Major | . | Siddharth Seth | Jing Zhao |
| [YARN-5206](https://issues.apache.org/jira/browse/YARN-5206) | RegistrySecurity includes id:pass in exception text if considered invalid |  Minor | client, security | Steve Loughran | Steve Loughran |
| [HDFS-10220](https://issues.apache.org/jira/browse/HDFS-10220) | A large number of expired leases can make namenode unresponsive and cause failover |  Major | namenode | Nicolas Fraison | Nicolas Fraison |
| [MAPREDUCE-6240](https://issues.apache.org/jira/browse/MAPREDUCE-6240) | Hadoop client displays confusing error message |  Major | client | Mohammad Kamrul Islam | Gera Shegalov |
| [YARN-4308](https://issues.apache.org/jira/browse/YARN-4308) | ContainersAggregated CPU resource utilization reports negative usage in first few heartbeats |  Major | nodemanager | Sunil G | Sunil G |
| [HDFS-10508](https://issues.apache.org/jira/browse/HDFS-10508) | DFSInputStream should set thread's interrupt status after catching InterruptException from sleep |  Major | . | Jing Zhao | Jing Zhao |
| [HADOOP-13249](https://issues.apache.org/jira/browse/HADOOP-13249) | RetryInvocationHandler need wrap InterruptedException in IOException when call Thread.sleep |  Major | ipc | zhihai xu | zhihai xu |
| [HADOOP-13213](https://issues.apache.org/jira/browse/HADOOP-13213) | Small Documentation bug with AuthenticatedURL in hadoop-auth |  Minor | documentation | Tom Ellis | Tom Ellis |
| [HADOOP-13243](https://issues.apache.org/jira/browse/HADOOP-13243) | TestRollingFileSystemSink.testSetInitialFlushTime() fails intermittently |  Minor | test | Daniel Templeton | Daniel Templeton |
| [HADOOP-13079](https://issues.apache.org/jira/browse/HADOOP-13079) | Add -q option to Ls to print ? instead of non-printable characters |  Major | . | John Zhuge | John Zhuge |
| [HDFS-10516](https://issues.apache.org/jira/browse/HDFS-10516) | Fix bug when warming up EDEK cache of more than one encryption zone |  Major | encryption, namenode | Xiao Chen | Xiao Chen |
| [HADOOP-13270](https://issues.apache.org/jira/browse/HADOOP-13270) | BZip2CompressionInputStream finds the same compression marker twice in corner case, causing duplicate data blocks |  Critical | . | Haibo Chen | Kai Sasaki |
| [HADOOP-13179](https://issues.apache.org/jira/browse/HADOOP-13179) | GenericOptionsParser is not thread-safe because commons-cli OptionBuilder is not thread-safe |  Minor | . | hongbin ma | hongbin ma |
| [HADOOP-13244](https://issues.apache.org/jira/browse/HADOOP-13244) | o.a.h.ipc.Server#Server should honor handlerCount when queueSizePerHandler is specified in consturctor |  Minor | ipc | Xiaoyu Yao | Kai Sasaki |
| [HADOOP-13245](https://issues.apache.org/jira/browse/HADOOP-13245) | Fix up some misc create-release issues |  Blocker | build | Allen Wittenauer | Allen Wittenauer |
| [HDFS-10505](https://issues.apache.org/jira/browse/HDFS-10505) | OIV's ReverseXML processor should support ACLs |  Major | tools | Colin P. McCabe | Surendra Singh Lilhore |
| [HDFS-10525](https://issues.apache.org/jira/browse/HDFS-10525) | Fix NPE in CacheReplicationMonitor#rescanCachedBlockMap |  Major | caching | Xiao Chen | Xiao Chen |
| [YARN-5237](https://issues.apache.org/jira/browse/YARN-5237) | Fix missing log files issue in rolling log aggregation. |  Major | . | Siddharth Seth | Xuan Gong |
| [HDFS-10532](https://issues.apache.org/jira/browse/HDFS-10532) | Typo in RollingUpgrade docs |  Major | documentation | Arpit Agarwal | Yiqun Lin |
| [HADOOP-3733](https://issues.apache.org/jira/browse/HADOOP-3733) | "s3:" URLs break when Secret Key contains a slash, even if encoded |  Minor | fs/s3 | Stuart Sierra | Steve Loughran |
| [HDFS-9466](https://issues.apache.org/jira/browse/HDFS-9466) | TestShortCircuitCache#testDataXceiverCleansUpSlotsOnFailure is flaky |  Major | fs, hdfs-client | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13255](https://issues.apache.org/jira/browse/HADOOP-13255) | KMSClientProvider should check and renew tgt when doing delegation token operations. |  Major | kms | Xiao Chen | Xiao Chen |
| [HADOOP-13242](https://issues.apache.org/jira/browse/HADOOP-13242) | Authenticate to Azure Data Lake using client ID and keys |  Major | fs/azure | Atul Sikaria | Atul Sikaria |
| [HADOOP-13285](https://issues.apache.org/jira/browse/HADOOP-13285) | DecayRpcScheduler MXBean should only report decayed CallVolumeSummary |  Major | ipc | Namit Maheshwari | Xiaoyu Yao |
| [HADOOP-13149](https://issues.apache.org/jira/browse/HADOOP-13149) | Windows distro build fails on dist-copynativelibs. |  Blocker | build | Chris Nauroth | Chris Nauroth |
| [YARN-5077](https://issues.apache.org/jira/browse/YARN-5077) | Fix FSLeafQueue#getFairShare() for queues with zero fairshare |  Major | . | Yufei Gu | Yufei Gu |
| [HDFS-10437](https://issues.apache.org/jira/browse/HDFS-10437) | ReconfigurationProtocol not covered by HDFSPolicyProvider. |  Major | namenode | Chris Nauroth | Arpit Agarwal |
| [YARN-5246](https://issues.apache.org/jira/browse/YARN-5246) | NMWebAppFilter web redirects drop query parameters |  Major | . | Varun Vasudev | Varun Vasudev |
| [HADOOP-13192](https://issues.apache.org/jira/browse/HADOOP-13192) | org.apache.hadoop.util.LineReader cannot handle multibyte delimiters correctly |  Critical | util | binde | binde |
| [HDFS-10423](https://issues.apache.org/jira/browse/HDFS-10423) | Increase default value of httpfs maxHttpHeaderSize |  Minor | hdfs | Nicolae Popa | Nicolae Popa |
| [HDFS-10448](https://issues.apache.org/jira/browse/HDFS-10448) | CacheManager#addInternal tracks bytesNeeded incorrectly when dealing with replication factors other than 1 |  Major | caching | Yiqun Lin | Yiqun Lin |
| [HADOOP-13287](https://issues.apache.org/jira/browse/HADOOP-13287) | TestS3ACredentials#testInstantiateFromURL fails if AWS secret key contains '+'. |  Minor | fs/s3, test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6197](https://issues.apache.org/jira/browse/MAPREDUCE-6197) | Cache MapOutputLocations in ShuffleHandler |  Major | . | Siddharth Seth | Junping Du |
| [MAPREDUCE-6641](https://issues.apache.org/jira/browse/MAPREDUCE-6641) | TestTaskAttempt fails in trunk |  Major | test | Tsuyoshi Ozawa | Haibo Chen |
| [YARN-5266](https://issues.apache.org/jira/browse/YARN-5266) | Wrong exit code while trying to get app logs using regex via CLI |  Critical | yarn | Sumana Sathish | Xuan Gong |
| [HDFS-10561](https://issues.apache.org/jira/browse/HDFS-10561) | test\_native\_mini\_dfs fails by NoClassDefFoundError |  Major | native, test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10555](https://issues.apache.org/jira/browse/HDFS-10555) | Unable to loadFSEdits due to a failure in readCachePoolInfo |  Critical | caching, namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-6725](https://issues.apache.org/jira/browse/MAPREDUCE-6725) | Javadoc for CLI#listEvents() contains no-existent param |  Minor | client, documentation | Shen Yinjie | Shen Yinjie |
| [HDFS-7959](https://issues.apache.org/jira/browse/HDFS-7959) | WebHdfs logging is missing on Datanode |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-10536](https://issues.apache.org/jira/browse/HDFS-10536) | Standby NN can not trigger log roll after EditLogTailer thread failed 3 times in EditLogTailer.triggerActiveLogRoll method. |  Critical | auto-failover | XingFeng Shen | XingFeng Shen |
| [MAPREDUCE-6542](https://issues.apache.org/jira/browse/MAPREDUCE-6542) | HistoryViewer uses SimpleDateFormat, but SimpleDateFormat is not threadsafe |  Major | jobhistoryserver | zhangyubiao | zhangyubiao |
| [HADOOP-13251](https://issues.apache.org/jira/browse/HADOOP-13251) | Authenticate with Kerberos credentials when renewing KMS delegation token |  Major | kms | Xiao Chen | Xiao Chen |
| [HADOOP-13316](https://issues.apache.org/jira/browse/HADOOP-13316) | Enforce Kerberos authentication for required ops in DelegationTokenAuthenticator |  Blocker | kms, security | Xiao Chen | Xiao Chen |
| [MAPREDUCE-5803](https://issues.apache.org/jira/browse/MAPREDUCE-5803) | Counters page display all task neverthless of task type( Map or Reduce) |  Minor | jobhistoryserver | Rohith Sharma K S | Kai Sasaki |
| [HADOOP-9888](https://issues.apache.org/jira/browse/HADOOP-9888) | KerberosName static initialization gets default realm, which is unneeded in non-secure deployment. |  Major | security | Chris Nauroth | Dmytro Kabakchei |
| [HDFS-9852](https://issues.apache.org/jira/browse/HDFS-9852) | hdfs dfs -setfacl error message is misleading |  Minor | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-5262](https://issues.apache.org/jira/browse/YARN-5262) | Optimize sending RMNodeFinishedContainersPulledByAMEvent for every AM heartbeat |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-5182](https://issues.apache.org/jira/browse/YARN-5182) | MockNodes.newNodes creates one more node per rack than requested |  Major | test | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-12345](https://issues.apache.org/jira/browse/HADOOP-12345) | Pad hostname correctly in CredentialsSys.java |  Critical | nfs | Pradeep Nayak Udupi Kadbet | Pradeep Nayak Udupi Kadbet |
| [YARN-5282](https://issues.apache.org/jira/browse/YARN-5282) | Fix typos in CapacityScheduler documentation |  Trivial | documentation | Ray Chiang | Ray Chiang |
| [HADOOP-13314](https://issues.apache.org/jira/browse/HADOOP-13314) | Remove 'package-info.java' from 'test\java\org\apache\hadoop\fs\shell\' to remove eclipse compile error |  Trivial | . | Vinayakumar B | Vinayakumar B |
| [HDFS-10572](https://issues.apache.org/jira/browse/HDFS-10572) | Fix TestOfflineEditsViewer#testGenerated |  Blocker | test | Xiaoyu Yao | Surendra Singh Lilhore |
| [HDFS-10589](https://issues.apache.org/jira/browse/HDFS-10589) | Javadoc for HAState#HAState and HAState#setStateInternal contains non-existent params |  Minor | documentation, hdfs | Shen Yinjie | Shen Yinjie |
| [YARN-5286](https://issues.apache.org/jira/browse/YARN-5286) | Add RPC port info in RM web service's response when getting app status |  Major | . | Jun Gong | Jun Gong |
| [HADOOP-13342](https://issues.apache.org/jira/browse/HADOOP-13342) | ISAL download is breaking the Dockerfile |  Blocker | build | Allen Wittenauer | Allen Wittenauer |
| [YARN-5214](https://issues.apache.org/jira/browse/YARN-5214) | Pending on synchronized method DirectoryCollection#checkDirs can hang NM's NodeStatusUpdater |  Critical | nodemanager | Junping Du | Junping Du |
| [YARN-5296](https://issues.apache.org/jira/browse/YARN-5296) | NMs going OutOfMemory because ContainerMetrics leak in ContainerMonitorImpl |  Major | nodemanager | Karam Singh | Junping Du |
| [YARN-5294](https://issues.apache.org/jira/browse/YARN-5294) | Pass remote ip address down to YarnAuthorizationProvider |  Major | . | Jian He | Jian He |
| [HADOOP-13350](https://issues.apache.org/jira/browse/HADOOP-13350) | Additional fix to LICENSE and NOTICE |  Blocker | build | Xiao Chen | Xiao Chen |
| [HDFS-10592](https://issues.apache.org/jira/browse/HDFS-10592) | Fix intermittent test failure of TestNameNodeResourceChecker#testCheckThatNameNodeResourceMonitorIsRunning |  Major | test | Rakesh R | Rakesh R |
| [HADOOP-13349](https://issues.apache.org/jira/browse/HADOOP-13349) | HADOOP\_CLASSPATH vs HADOOP\_USER\_CLASSPATH |  Blocker | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13320](https://issues.apache.org/jira/browse/HADOOP-13320) | Fix arguments check in documentation for WordCount v2.0 |  Minor | documentation | niccolo becchi | niccolo becchi |
| [YARN-5314](https://issues.apache.org/jira/browse/YARN-5314) | ConcurrentModificationException in ATS v1.5 EntityGroupFSTimelineStore |  Major | timelineserver | Karam Singh | Li Lu |
| [YARN-4939](https://issues.apache.org/jira/browse/YARN-4939) | the decommissioning Node should keep alive  if NM restart |  Major | . | sandflee | sandflee |
| [HADOOP-12893](https://issues.apache.org/jira/browse/HADOOP-12893) | Verify LICENSE.txt and NOTICE.txt |  Blocker | build | Allen Wittenauer | Xiao Chen |
| [HADOOP-13352](https://issues.apache.org/jira/browse/HADOOP-13352) | Make X-FRAME-OPTIONS configurable in HttpServer2 |  Major | net, security | Anu Engineer | Anu Engineer |
| [HDFS-10336](https://issues.apache.org/jira/browse/HDFS-10336) | TestBalancer failing intermittently because of not reseting UserGroupInformation completely |  Major | test | Yiqun Lin | Yiqun Lin |
| [HDFS-10512](https://issues.apache.org/jira/browse/HDFS-10512) | VolumeScanner may terminate due to NPE in DataNode.reportBadBlocks |  Major | datanode | Wei-Chiu Chuang | Yiqun Lin |
| [YARN-5337](https://issues.apache.org/jira/browse/YARN-5337) | Dshell AM failed with "java.lang.OutOfMemoryError: GC overhead limit exceeded" |  Major | . | Sumana Sathish | Jian He |
| [HADOOP-13297](https://issues.apache.org/jira/browse/HADOOP-13297) | Add missing dependency in setting maven-remote-resource-plugin to fix builds |  Major | build | Akira Ajisaka | Sean Busbey |
| [YARN-5270](https://issues.apache.org/jira/browse/YARN-5270) | Solve miscellaneous issues caused by YARN-4844 |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-5354](https://issues.apache.org/jira/browse/YARN-5354) | TestDistributedShell.checkTimelineV2() may fail for concurrent tests |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [MAPREDUCE-6731](https://issues.apache.org/jira/browse/MAPREDUCE-6731) | TestMRTimelineEventHandling.testMRNewTimelineServiceEventHandling() may fail for concurrent tests |  Major | test | Sangjin Lee | Sangjin Lee |
| [HDFS-10579](https://issues.apache.org/jira/browse/HDFS-10579) | HDFS web interfaces lack configs for X-FRAME-OPTIONS protection |  Major | datanode, namenode | Anu Engineer | Anu Engineer |
| [YARN-4366](https://issues.apache.org/jira/browse/YARN-4366) | Fix Lint Warnings in YARN Common |  Major | yarn | Daniel Templeton | Daniel Templeton |
| [MAPREDUCE-6625](https://issues.apache.org/jira/browse/MAPREDUCE-6625) | TestCLI#testGetJob fails occasionally |  Major | test | Jason Lowe | Haibo Chen |
| [HADOOP-13315](https://issues.apache.org/jira/browse/HADOOP-13315) | FileContext#umask is not initialized properly |  Minor | . | John Zhuge | John Zhuge |
| [YARN-5353](https://issues.apache.org/jira/browse/YARN-5353) | ResourceManager can leak delegation tokens when they are shared across apps |  Critical | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-10590](https://issues.apache.org/jira/browse/HDFS-10590) | Fix TestReconstructStripedBlocks.testCountLiveReplicas test failures |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-11361](https://issues.apache.org/jira/browse/HADOOP-11361) | Fix a race condition in MetricsSourceAdapter.updateJmxCache |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-5362](https://issues.apache.org/jira/browse/YARN-5362) | TestRMRestart#testFinishedAppRemovalAfterRMRestart can fail |  Major | . | Jason Lowe | sandflee |
| [YARN-5359](https://issues.apache.org/jira/browse/YARN-5359) | FileSystemTimelineReader/Writer uses unix-specific default storage path |  Minor | . | Sangjin Lee | Sangjin Lee |
| [YARN-5364](https://issues.apache.org/jira/browse/YARN-5364) | timelineservice modules have indirect dependencies on mapreduce artifacts |  Minor | timelineserver | Sangjin Lee | Sangjin Lee |
| [HDFS-10544](https://issues.apache.org/jira/browse/HDFS-10544) | Balancer doesn't work with IPFailoverProxyProvider |  Major | balancer & mover, ha | Zhe Zhang | Zhe Zhang |
| [HDFS-10617](https://issues.apache.org/jira/browse/HDFS-10617) | PendingReconstructionBlocks.size() should be synchronized |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-13351](https://issues.apache.org/jira/browse/HADOOP-13351) | TestDFSClientSocketSize buffer size tests are flaky |  Major | . | Aaron Fabbri | Aaron Fabbri |
| [YARN-5379](https://issues.apache.org/jira/browse/YARN-5379) | TestHBaseTimelineStorage. testWriteApplicationToHBase() fails intermittently |  Minor | test, timelineserver | Sangjin Lee | Vrushali C |
| [YARN-5383](https://issues.apache.org/jira/browse/YARN-5383) | Fix findbugs for nodemanager & checkstyle warnings in nodemanager.ContainerExecutor |  Major | nodemanager | Vrushali C | Vrushali C |
| [MAPREDUCE-6733](https://issues.apache.org/jira/browse/MAPREDUCE-6733) | MapReduce JerseyTest tests failing with "java.net.BindException: Address already in use" |  Critical | test | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-13254](https://issues.apache.org/jira/browse/HADOOP-13254) | Create framework for configurable disk checkers |  Major | util | Yufei Gu | Yufei Gu |
| [YARN-5272](https://issues.apache.org/jira/browse/YARN-5272) | Handle queue names consistently in FairScheduler |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-13202](https://issues.apache.org/jira/browse/HADOOP-13202) | Avoid possible overflow in org.apache.hadoop.util.bloom.BloomFilter#getNBytes |  Major | util | zhengbing li | Kai Sasaki |
| [YARN-5213](https://issues.apache.org/jira/browse/YARN-5213) | Fix a bug in LogCLIHelpers which cause TestLogsCLI#testFetchApplictionLogs fails intermittently |  Major | test | Rohith Sharma K S | Xuan Gong |
| [HDFS-10603](https://issues.apache.org/jira/browse/HDFS-10603) | Fix flaky tests in org.apache.hadoop.hdfs.server.namenode.snapshot.TestOpenFilesWithSnapshot |  Major | hdfs, namenode | Yongjun Zhang | Yiqun Lin |
| [HADOOP-12991](https://issues.apache.org/jira/browse/HADOOP-12991) | Conflicting default ports in DelegateToFileSystem |  Major | fs | Kevin Hogeland | Kai Sasaki |
| [YARN-5309](https://issues.apache.org/jira/browse/YARN-5309) | Fix SSLFactory truststore reloader thread leak in TimelineClientImpl |  Blocker | timelineserver, yarn | Thomas Friedrich | Weiwei Yang |
| [YARN-5340](https://issues.apache.org/jira/browse/YARN-5340) | Race condition in RollingLevelDBTimelineStore#getAndSetStartTime() |  Critical | timelineserver | Sumana Sathish | Li Lu |
| [HDFS-8914](https://issues.apache.org/jira/browse/HDFS-8914) | Document HA support in the HDFS HdfsDesign.md |  Major | documentation | Ravindra Babu | Lars Francke |
| [HADOOP-12588](https://issues.apache.org/jira/browse/HADOOP-12588) | Fix intermittent test failure of TestGangliaMetrics |  Major | . | Tsuyoshi Ozawa | Masatake Iwasaki |
| [HADOOP-13240](https://issues.apache.org/jira/browse/HADOOP-13240) | TestAclCommands.testSetfaclValidations fail |  Minor | test | linbao111 | John Zhuge |
| [HADOOP-13393](https://issues.apache.org/jira/browse/HADOOP-13393) | [Azure Data Lake] Correction needed in fs.defaultFS support in documentation |  Major | fs/azure | Vishwajeet Dusane | Vishwajeet Dusane |
| [HADOOP-13392](https://issues.apache.org/jira/browse/HADOOP-13392) | [Azure Data Lake] OAuth2 configuration should be default set to true when AdlFileSystem is configured. |  Major | fs/azure | Vishwajeet Dusane | Vishwajeet Dusane |
| [HADOOP-13389](https://issues.apache.org/jira/browse/HADOOP-13389) | TestS3ATemporaryCredentials.testSTS error when using IAM credentials |  Major | fs/s3 | Steven K. Wong | Steven K. Wong |
| [HADOOP-13406](https://issues.apache.org/jira/browse/HADOOP-13406) | S3AFileSystem: Consider reusing filestatus in delete() and mkdirs() |  Minor | fs/s3 | Rajesh Balamohan | Rajesh Balamohan |
| [MAPREDUCE-6744](https://issues.apache.org/jira/browse/MAPREDUCE-6744) | Increase timeout on TestDFSIO tests |  Major | . | Eric Badger | Eric Badger |
| [HDFS-10688](https://issues.apache.org/jira/browse/HDFS-10688) | BPServiceActor may run into a tight loop for sending block report when hitting IOException |  Major | datanode | Jing Zhao | Chen Liang |
| [HDFS-10671](https://issues.apache.org/jira/browse/HDFS-10671) | Fix typo in HdfsRollingUpgrade.md |  Trivial | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-13422](https://issues.apache.org/jira/browse/HADOOP-13422) | ZKDelegationTokenSecretManager JaasConfig does not work well with other ZK users in process |  Major | . | Sergey Shelukhin | Sergey Shelukhin |
| [YARN-5195](https://issues.apache.org/jira/browse/YARN-5195) | RM intermittently crashed with NPE while handling APP\_ATTEMPT\_REMOVED event when async-scheduling enabled in CapacityScheduler |  Major | resourcemanager | Karam Singh | sandflee |
| [YARN-5441](https://issues.apache.org/jira/browse/YARN-5441) | Fixing minor Scheduler test case failures |  Major | . | Subru Krishnan | Subru Krishnan |
| [HDFS-10696](https://issues.apache.org/jira/browse/HDFS-10696) | TestHDFSCLI fails |  Major | test | Akira Ajisaka | Kai Sasaki |
| [YARN-5432](https://issues.apache.org/jira/browse/YARN-5432) | Lock already held by another process while LevelDB cache store creation for dag |  Critical | timelineserver | Karam Singh | Li Lu |
| [YARN-5440](https://issues.apache.org/jira/browse/YARN-5440) | Use AHSClient in YarnClient when TimelineServer is running |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5438](https://issues.apache.org/jira/browse/YARN-5438) | TimelineClientImpl leaking FileSystem Instances causing Long running services like HiverServer2 daemon going OOM |  Major | timelineserver | Karam Singh | Rohith Sharma K S |
| [YARN-5203](https://issues.apache.org/jira/browse/YARN-5203) | Return ResourceRequest JAXB object in ResourceManager Cluster Applications REST API |  Major | . | Subru Krishnan | Ellen Hui |
| [HDFS-9276](https://issues.apache.org/jira/browse/HDFS-9276) | Failed to Update HDFS Delegation Token for long running application in HA mode |  Major | fs, ha, security | Liangliang Gu | Liangliang Gu |
| [YARN-5436](https://issues.apache.org/jira/browse/YARN-5436) | Race in AsyncDispatcher can cause random test failures in Tez (probably YARN also) |  Major | . | Zhiyuan Yang | Zhiyuan Yang |
| [HADOOP-13381](https://issues.apache.org/jira/browse/HADOOP-13381) | KMS clients should use KMS Delegation Tokens from current UGI. |  Critical | kms | Xiao Chen | Xiao Chen |
| [HDFS-10691](https://issues.apache.org/jira/browse/HDFS-10691) | FileDistribution fails in hdfs oiv command due to ArrayIndexOutOfBoundsException |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-5444](https://issues.apache.org/jira/browse/YARN-5444) | Fix failing unit tests in TestLinuxContainerExecutorWithMocks |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [MAPREDUCE-6724](https://issues.apache.org/jira/browse/MAPREDUCE-6724) | Single shuffle to memory must not exceed Integer#MAX\_VALUE |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HDFS-5805](https://issues.apache.org/jira/browse/HDFS-5805) | TestCheckpoint.testCheckpoint fails intermittently on branch2 |  Major | . | Mit Desai | Eric Badger |
| [HADOOP-13459](https://issues.apache.org/jira/browse/HADOOP-13459) | hadoop-azure runs several test cases repeatedly, causing unnecessarily long running time. |  Minor | fs/azure, test | Chris Nauroth | Chris Nauroth |
| [HDFS-742](https://issues.apache.org/jira/browse/HDFS-742) | A down DataNode makes Balancer to hang on repeatingly asking NameNode its partial block list |  Minor | balancer & mover | Hairong Kuang | Mit Desai |
| [YARN-4280](https://issues.apache.org/jira/browse/YARN-4280) | CapacityScheduler reservations may not prevent indefinite postponement on a busy cluster |  Major | capacity scheduler | Kuhu Shukla | Kuhu Shukla |
| [YARN-5462](https://issues.apache.org/jira/browse/YARN-5462) | TestNodeStatusUpdater.testNodeStatusUpdaterRetryAndNMShutdown fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [HDFS-10710](https://issues.apache.org/jira/browse/HDFS-10710) | In BlockManager#rescanPostponedMisreplicatedBlocks(), postponed misreplicated block counts should be retrieved with NN lock protection |  Major | namenode | Rui Gao | Rui Gao |
| [YARN-5469](https://issues.apache.org/jira/browse/YARN-5469) | Increase timeout of TestAmFilter.testFilter |  Minor | . | Eric Badger | Eric Badger |
| [HDFS-10569](https://issues.apache.org/jira/browse/HDFS-10569) | A bug causes OutOfIndex error in BlockListAsLongs |  Minor | . | Weiwei Yang | Weiwei Yang |
| [HADOOP-13434](https://issues.apache.org/jira/browse/HADOOP-13434) | Add quoting to Shell class |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-13443](https://issues.apache.org/jira/browse/HADOOP-13443) | KMS should check the type of underlying keyprovider of KeyProviderExtension before falling back to default |  Minor | kms | Anthony Young-Garner | Anthony Young-Garner |
| [HDFS-10716](https://issues.apache.org/jira/browse/HDFS-10716) | In Balancer, the target task should be removed when its size \< 0. |  Minor | balancer & mover | Yiqun Lin | Yiqun Lin |
| [HDFS-10722](https://issues.apache.org/jira/browse/HDFS-10722) | Fix race condition in TestEditLog#testBatchedSyncWithClosedLogs |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HADOOP-13467](https://issues.apache.org/jira/browse/HADOOP-13467) | Shell#getSignalKillCommand should use the bash builtin on Linux |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-10343](https://issues.apache.org/jira/browse/HDFS-10343) | BlockManager#createLocatedBlocks may return blocks on failed storages |  Major | hdfs | Daryn Sharp | Kuhu Shukla |
| [HDFS-10715](https://issues.apache.org/jira/browse/HDFS-10715) | NPE when applying AvailableSpaceBlockPlacementPolicy |  Major | namenode | Guangbin Zhu | Guangbin Zhu |
| [YARN-5333](https://issues.apache.org/jira/browse/YARN-5333) | Some recovered apps are put into default queue when RM HA |  Major | . | Jun Gong | Jun Gong |
| [HADOOP-13353](https://issues.apache.org/jira/browse/HADOOP-13353) | LdapGroupsMapping getPassward shouldn't return null when IOException throws |  Major | security | Zhaohao Liang | Wei-Chiu Chuang |
| [YARN-4624](https://issues.apache.org/jira/browse/YARN-4624) | NPE in PartitionQueueCapacitiesInfo while accessing Schduler UI |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-10823](https://issues.apache.org/jira/browse/HADOOP-10823) | TestReloadingX509TrustManager is flaky |  Major | . | Ratandeep Ratti | Mingliang Liu |
| [HADOOP-13403](https://issues.apache.org/jira/browse/HADOOP-13403) | AzureNativeFileSystem rename/delete performance improvements |  Major | azure | Subramanyam Pattipaka | Subramanyam Pattipaka |
| [HADOOP-13457](https://issues.apache.org/jira/browse/HADOOP-13457) | Remove hardcoded absolute path for shell executable |  Major | util | Arpit Agarwal | Chen Liang |
| [HDFS-10457](https://issues.apache.org/jira/browse/HDFS-10457) | DataNode should not auto-format block pool directory if VERSION is missing |  Major | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-4176](https://issues.apache.org/jira/browse/HDFS-4176) | EditLogTailer should call rollEdits with a timeout |  Major | ha, namenode | Todd Lipcon | Lei (Eddy) Xu |
| [HADOOP-13439](https://issues.apache.org/jira/browse/HADOOP-13439) | Fix race between TestMetricsSystemImpl and TestGangliaMetrics |  Minor | test | Masatake Iwasaki | Chen Liang |
| [YARN-5287](https://issues.apache.org/jira/browse/YARN-5287) | LinuxContainerExecutor fails to set proper permission |  Minor | nodemanager | Ying Zhang | Ying Zhang |
| [YARN-5474](https://issues.apache.org/jira/browse/YARN-5474) | Typo mistake in AMRMClient#getRegisteredTimeineClient API |  Trivial | . | Rohith Sharma K S | Naganarasimha G R |
| [MAPREDUCE-6750](https://issues.apache.org/jira/browse/MAPREDUCE-6750) | TestHSAdminServer.testRefreshSuperUserGroups is failing |  Minor | test | Kihwal Lee | Kihwal Lee |
| [HADOOP-13473](https://issues.apache.org/jira/browse/HADOOP-13473) | Tracing in IPC Server is broken |  Major | . | Wei-Chiu Chuang | Daryn Sharp |
| [HDFS-10738](https://issues.apache.org/jira/browse/HDFS-10738) | Fix TestRefreshUserMappings.testRefreshSuperUserGroupsConfiguration test failure |  Major | test | Rakesh R | Rakesh R |
| [HADOOP-13299](https://issues.apache.org/jira/browse/HADOOP-13299) | JMXJsonServlet is vulnerable to TRACE |  Minor | . | Haibo Chen | Haibo Chen |
| [HADOOP-13476](https://issues.apache.org/jira/browse/HADOOP-13476) | CredentialProviderFactory fails at class loading from libhdfs (JNI) |  Major | fs/s3 | Aaron Fabbri | Aaron Fabbri |
| [HDFS-8224](https://issues.apache.org/jira/browse/HDFS-8224) | Schedule a block for scanning if its metadata file is corrupt |  Major | datanode | Rushabh S Shah | Rushabh S Shah |
| [YARN-5382](https://issues.apache.org/jira/browse/YARN-5382) | RM does not audit log kill request for active applications |  Major | resourcemanager | Jason Lowe | Vrushali C |
| [HDFS-10643](https://issues.apache.org/jira/browse/HDFS-10643) | Namenode should use loginUser(hdfs) to generateEncryptedKey |  Major | encryption, namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-13461](https://issues.apache.org/jira/browse/HADOOP-13461) | NPE in KeyProvider.rollNewVersion |  Minor | . | Colm O hEigeartaigh | Colm O hEigeartaigh |
| [HDFS-10720](https://issues.apache.org/jira/browse/HDFS-10720) | Fix intermittent test failure of TestDataNodeErasureCodingMetrics |  Major | . | Rakesh R | Rakesh R |
| [HDFS-8897](https://issues.apache.org/jira/browse/HDFS-8897) | Balancer should handle fs.defaultFS trailing slash in HA |  Major | balancer & mover | LINTE | John Zhuge |
| [HADOOP-13441](https://issues.apache.org/jira/browse/HADOOP-13441) | Document LdapGroupsMapping keystore password properties |  Minor | security | Wei-Chiu Chuang | Yuanbo Liu |
| [HDFS-10731](https://issues.apache.org/jira/browse/HDFS-10731) | FSDirectory#verifyMaxDirItems does not log path name |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4833](https://issues.apache.org/jira/browse/YARN-4833) | For Queue AccessControlException client retries multiple times on both RM |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5476](https://issues.apache.org/jira/browse/YARN-5476) | Not existed application reported as ACCEPTED state by YarnClientImpl |  Critical | yarn | Yesha Vora | Junping Du |
| [YARN-5491](https://issues.apache.org/jira/browse/YARN-5491) | Random Failure TestCapacityScheduler#testCSQueueBlocked |  Major | test | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5521](https://issues.apache.org/jira/browse/YARN-5521) | TestCapacityScheduler#testKillAllAppsInQueue fails randomly |  Major | . | Varun Saxena | sandflee |
| [HADOOP-13333](https://issues.apache.org/jira/browse/HADOOP-13333) | testConf.xml ls comparators in wrong order |  Trivial | fs | John Zhuge | Vrushali C |
| [HADOOP-13437](https://issues.apache.org/jira/browse/HADOOP-13437) | KMS should reload whitelist and default key ACLs when hot-reloading |  Major | kms | Xiao Chen | Xiao Chen |
| [YARN-5514](https://issues.apache.org/jira/browse/YARN-5514) | Clarify DecommissionType.FORCEFUL comment |  Minor | documentation | Robert Kanter | Vrushali C |
| [YARN-5475](https://issues.apache.org/jira/browse/YARN-5475) | Test failed for TestAggregatedLogFormat on trunk |  Major | . | Junping Du | Jun Gong |
| [HADOOP-13470](https://issues.apache.org/jira/browse/HADOOP-13470) | GenericTestUtils$LogCapturer is flaky |  Major | test, util | Mingliang Liu | Mingliang Liu |
| [YARN-5523](https://issues.apache.org/jira/browse/YARN-5523) | Yarn running container log fetching causes OutOfMemoryError |  Major | log-aggregation | Prasanth Jayachandran | Xuan Gong |
| [HADOOP-13494](https://issues.apache.org/jira/browse/HADOOP-13494) | ReconfigurableBase can log sensitive information |  Major | security | Sean Mackrory | Sean Mackrory |
| [HDFS-9530](https://issues.apache.org/jira/browse/HDFS-9530) | ReservedSpace is not cleared for abandoned Blocks |  Critical | datanode | Fei Hui | Brahma Reddy Battula |
| [HDFS-10549](https://issues.apache.org/jira/browse/HDFS-10549) | Correctly revoke file leases when closing files |  Major | hdfs-client | Yiqun Lin | Yiqun Lin |
| [HADOOP-13513](https://issues.apache.org/jira/browse/HADOOP-13513) | Java 1.7 support for org.apache.hadoop.fs.azure testcases |  Minor | azure | Tibor Kiss | Tibor Kiss |
| [YARN-4307](https://issues.apache.org/jira/browse/YARN-4307) | Display blacklisted nodes for AM container in the RM web UI |  Major | resourcemanager, webapp | Naganarasimha G R | Naganarasimha G R |
| [YARN-5533](https://issues.apache.org/jira/browse/YARN-5533) | JMX AM Used metrics for queue wrong when app submited to nodelabel partition |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [MAPREDUCE-6763](https://issues.apache.org/jira/browse/MAPREDUCE-6763) | Shuffle server listen queue is too small |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [YARN-4837](https://issues.apache.org/jira/browse/YARN-4837) | User facing aspects of 'AM blacklisting' feature need fixing |  Critical | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-6310](https://issues.apache.org/jira/browse/MAPREDUCE-6310) | Add jdiff support to MapReduce |  Blocker | . | Li Lu | Li Lu |
| [YARN-3388](https://issues.apache.org/jira/browse/YARN-3388) | Allocation in LeafQueue could get stuck because DRF calculator isn't well supported when computing user-limit |  Major | capacityscheduler | Nathan Roberts | Nathan Roberts |
| [YARN-4685](https://issues.apache.org/jira/browse/YARN-4685) | Disable AM blacklisting by default to mitigate situations that application get hanged |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-13428](https://issues.apache.org/jira/browse/HADOOP-13428) | Fix hadoop-common to generate jdiff |  Blocker | . | Wangda Tan | Wangda Tan |
| [HDFS-10764](https://issues.apache.org/jira/browse/HDFS-10764) | Fix INodeFile#getBlocks to not return null |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-6762](https://issues.apache.org/jira/browse/MAPREDUCE-6762) | ControlledJob#toString failed with NPE when job status is not successfully updated |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-10692](https://issues.apache.org/jira/browse/HDFS-10692) | Point JDiff base version for HDFS from 2.6.0 to 2.7.2 |  Blocker | . | Wangda Tan | Wangda Tan |
| [HADOOP-13487](https://issues.apache.org/jira/browse/HADOOP-13487) | Hadoop KMS should load old delegation tokens from Zookeeper on startup |  Major | kms | Alex Ivanov | Xiao Chen |
| [HADOOP-13526](https://issues.apache.org/jira/browse/HADOOP-13526) | Add detailed logging in KMS for the authentication failure of proxy user |  Minor | kms | Suraj Acharya | Suraj Acharya |
| [HDFS-8312](https://issues.apache.org/jira/browse/HDFS-8312) | Trash does not descent into child directories to check for permissions |  Critical | fs, security | Eric Yang | Weiwei Yang |
| [HDFS-10783](https://issues.apache.org/jira/browse/HDFS-10783) | The option '-maxSize' and '-step' fail in OfflineImageViewer |  Major | tools | Yiqun Lin | Yiqun Lin |
| [HADOOP-13524](https://issues.apache.org/jira/browse/HADOOP-13524) | mvn eclipse:eclipse generates .gitignore'able files |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-13497](https://issues.apache.org/jira/browse/HADOOP-13497) | fix wrong command in CredentialProviderAPI.md |  Trivial | documentation | Yuanbo Liu | Yuanbo Liu |
| [MAPREDUCE-6761](https://issues.apache.org/jira/browse/MAPREDUCE-6761) | Regression when handling providers - invalid configuration ServiceConfiguration causes Cluster initialization failure |  Major | mrv2 | Peter Vary | Peter Vary |
| [YARN-5537](https://issues.apache.org/jira/browse/YARN-5537) | Intermittent test failure of TestAMRMClient#testAMRMClientWithContainerResourceChange |  Major | . | Varun Saxena | Bibin A Chundatt |
| [HDFS-10748](https://issues.apache.org/jira/browse/HDFS-10748) | TestFileTruncate#testTruncateWithDataNodesRestart runs sometimes timeout |  Major | test | Xiaoyu Yao | Yiqun Lin |
| [YARN-5430](https://issues.apache.org/jira/browse/YARN-5430) | Return container's ip and host from NM ContainerStatus call |  Major | . | Jian He | Jian He |
| [YARN-5373](https://issues.apache.org/jira/browse/YARN-5373) | NPE listing wildcard directory in containerLaunch |  Blocker | nodemanager | Haibo Chen | Daniel Templeton |
| [HDFS-10793](https://issues.apache.org/jira/browse/HDFS-10793) | Fix HdfsAuditLogger binary incompatibility introduced by HDFS-9184 |  Blocker | . | Andrew Wang | Manoj Govindassamy |
| [HDFS-10652](https://issues.apache.org/jira/browse/HDFS-10652) | Add a unit test for HDFS-4660 |  Major | datanode, hdfs | Yongjun Zhang | Vinayakumar B |
| [HADOOP-13552](https://issues.apache.org/jira/browse/HADOOP-13552) | RetryInvocationHandler logs all remote exceptions |  Blocker | ipc | Jason Lowe | Jason Lowe |
| [HADOOP-12765](https://issues.apache.org/jira/browse/HADOOP-12765) | HttpServer2 should switch to using the non-blocking SslSelectChannelConnector to prevent performance degradation when handling SSL connections |  Major | . | Min Shen | Min Shen |
| [MAPREDUCE-6768](https://issues.apache.org/jira/browse/MAPREDUCE-6768) | TestRecovery.testSpeculative failed with NPE |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HADOOP-13559](https://issues.apache.org/jira/browse/HADOOP-13559) | Remove close() within try-with-resources in ChecksumFileSystem/ChecksumFs classes |  Minor | fs | Aaron Fabbri | Aaron Fabbri |
| [HDFS-4210](https://issues.apache.org/jira/browse/HDFS-4210) | Throw helpful exception when DNS entry for JournalNode cannot be resolved |  Trivial | ha, journal-node, namenode | Damien Hardy | John Zhuge |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-2669](https://issues.apache.org/jira/browse/MAPREDUCE-2669) | Some new examples and test cases for them. |  Minor | examples | Plamen Jeliazkov | Plamen Jeliazkov |
| [HDFS-3121](https://issues.apache.org/jira/browse/HDFS-3121) | hdfs tests for HADOOP-8014 |  Major | test | John George | John George |
| [HADOOP-8434](https://issues.apache.org/jira/browse/HADOOP-8434) | TestConfiguration currently has no tests for direct setter methods |  Major | test | Harsh J | madhukara phatak |
| [HDFS-2127](https://issues.apache.org/jira/browse/HDFS-2127) | Add a test that ensure AccessControlExceptions contain a full path |  Major | namenode | Eli Collins | Stephen Chu |
| [HADOOP-8844](https://issues.apache.org/jira/browse/HADOOP-8844) | Add a plaintext fs -text test-case |  Major | fs | Harsh J | Akira Ajisaka |
| [HADOOP-11084](https://issues.apache.org/jira/browse/HADOOP-11084) | jenkins patchprocess links are broken |  Major | scripts | Colin P. McCabe | Arpit Agarwal |
| [MAPREDUCE-6191](https://issues.apache.org/jira/browse/MAPREDUCE-6191) | TestJavaSerialization fails with getting incorrect MR job result |  Minor | test | sam liu | sam liu |
| [HADOOP-10668](https://issues.apache.org/jira/browse/HADOOP-10668) | TestZKFailoverControllerStress#testExpireBackAndForth occasionally fails |  Major | test | Ted Yu | Ming Ma |
| [YARN-3339](https://issues.apache.org/jira/browse/YARN-3339) | TestDockerContainerExecutor should pull a single image and not the entire centos repository |  Minor | test | Ravindra Kumar Naik | Ravindra Kumar Naik |
| [YARN-1880](https://issues.apache.org/jira/browse/YARN-1880) | Cleanup TestApplicationClientProtocolOnHA |  Trivial | test | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-6263](https://issues.apache.org/jira/browse/HDFS-6263) | Remove DRFA.MaxBackupIndex config from log4j.properties |  Minor | . | Akira Ajisaka | Abhiraj Butala |
| [HDFS-6408](https://issues.apache.org/jira/browse/HDFS-6408) | Remove redundant definitions in log4j.properties |  Minor | test | Abhiraj Butala | Abhiraj Butala |
| [YARN-2666](https://issues.apache.org/jira/browse/YARN-2666) | TestFairScheduler.testContinuousScheduling fails Intermittently |  Major | scheduler | Tsuyoshi Ozawa | zhihai xu |
| [HADOOP-11781](https://issues.apache.org/jira/browse/HADOOP-11781) | fix race conditions and add URL support to smart-apply-patch.sh |  Major | test | Allen Wittenauer | Raymie Stata |
| [HDFS-8247](https://issues.apache.org/jira/browse/HDFS-8247) | TestDiskspaceQuotaUpdate#testAppendOverTypeQuota is failing |  Major | test | Anu Engineer | Xiaoyu Yao |
| [HADOOP-11881](https://issues.apache.org/jira/browse/HADOOP-11881) | test-patch.sh javac result is wildly wrong |  Major | build, test | Allen Wittenauer | Kengo Seki |
| [HADOOP-11904](https://issues.apache.org/jira/browse/HADOOP-11904) | test-patch.sh goes into an infinite loop on non-maven builds |  Critical | test | Allen Wittenauer | Allen Wittenauer |
| [YARN-3343](https://issues.apache.org/jira/browse/YARN-3343) | TestCapacitySchedulerNodeLabelUpdate.testNodeUpdate sometime fails in trunk |  Minor | . | Xuan Gong | Rohith Sharma K S |
| [YARN-3580](https://issues.apache.org/jira/browse/YARN-3580) | [JDK 8] TestClientRMService.testGetLabelsToNodes fails |  Major | test | Robert Kanter | Robert Kanter |
| [HDFS-7559](https://issues.apache.org/jira/browse/HDFS-7559) | Create unit test to automatically compare HDFS related classes and hdfs-default.xml |  Minor | . | Ray Chiang | Ray Chiang |
| [HADOOP-11906](https://issues.apache.org/jira/browse/HADOOP-11906) | test-patch.sh should use 'file' command for patch determinism |  Major | . | Allen Wittenauer | Sean Busbey |
| [YARN-3602](https://issues.apache.org/jira/browse/YARN-3602) | TestResourceLocalizationService.testPublicResourceInitializesLocalDir fails Intermittently due to IOException from cleanup |  Minor | test | zhihai xu | zhihai xu |
| [HDFS-8243](https://issues.apache.org/jira/browse/HDFS-8243) | Files written by TestHostsFiles and TestNameNodeMXBean are causing Release Audit Warnings. |  Minor | test | Ruth Wisniewski | Ruth Wisniewski |
| [HADOOP-11884](https://issues.apache.org/jira/browse/HADOOP-11884) | test-patch.sh should pull the real findbugs version |  Minor | test | Allen Wittenauer | Kengo Seki |
| [HADOOP-11944](https://issues.apache.org/jira/browse/HADOOP-11944) | add option to test-patch to avoid relocating patch process directory |  Minor | . | Sean Busbey | Sean Busbey |
| [HADOOP-11949](https://issues.apache.org/jira/browse/HADOOP-11949) | Add user-provided plugins to test-patch |  Major | . | Sean Busbey | Sean Busbey |
| [HADOOP-12000](https://issues.apache.org/jira/browse/HADOOP-12000) | cannot use --java-home in test-patch |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6204](https://issues.apache.org/jira/browse/MAPREDUCE-6204) | TestJobCounters should use new properties instead of JobConf.MAPRED\_TASK\_JAVA\_OPTS |  Minor | test | sam liu | sam liu |
| [HADOOP-12035](https://issues.apache.org/jira/browse/HADOOP-12035) | shellcheck plugin displays a wrong version potentially |  Trivial | build | Kengo Seki | Kengo Seki |
| [HADOOP-12030](https://issues.apache.org/jira/browse/HADOOP-12030) | test-patch should only report on newly introduced findbugs warnings. |  Major | . | Sean Busbey | Sean Busbey |
| [HADOOP-11930](https://issues.apache.org/jira/browse/HADOOP-11930) | test-patch in offline mode should tell maven to be in offline mode |  Major | . | Sean Busbey | Sean Busbey |
| [HADOOP-11965](https://issues.apache.org/jira/browse/HADOOP-11965) | determine-flaky-tests needs a summary mode |  Minor | . | Allen Wittenauer | Yufei Gu |
| [YARN-2871](https://issues.apache.org/jira/browse/YARN-2871) | TestRMRestart#testRMRestartGetApplicationList sometime fails in trunk |  Minor | . | Ted Yu | zhihai xu |
| [YARN-3956](https://issues.apache.org/jira/browse/YARN-3956) | Fix TestNodeManagerHardwareUtils fails on Mac |  Minor | nodemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-8834](https://issues.apache.org/jira/browse/HDFS-8834) | TestReplication#testReplicationWhenBlockCorruption is not valid after HDFS-6482 |  Minor | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-10854](https://issues.apache.org/jira/browse/HADOOP-10854) | unit tests for the shell scripts |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-3992](https://issues.apache.org/jira/browse/YARN-3992) | TestApplicationPriority.testApplicationPriorityAllocation fails intermittently |  Major | . | Zhijie Shen | Sunil G |
| [HDFS-2070](https://issues.apache.org/jira/browse/HDFS-2070) | Add more unit tests for FsShell getmerge |  Major | test | XieXianshan | Daniel Templeton |
| [MAPREDUCE-5045](https://issues.apache.org/jira/browse/MAPREDUCE-5045) | UtilTest#isCygwin method appears to be unused |  Trivial | contrib/streaming, test | Chris Nauroth | Neelesh Srinivas Salian |
| [YARN-3573](https://issues.apache.org/jira/browse/YARN-3573) | MiniMRYarnCluster constructor that starts the timeline server using a boolean should be marked deprecated |  Major | timelineserver | Mit Desai | Brahma Reddy Battula |
| [HDFS-9295](https://issues.apache.org/jira/browse/HDFS-9295) | Add a thorough test of the full KMS code path |  Critical | security, test | Daniel Templeton | Daniel Templeton |
| [HDFS-9339](https://issues.apache.org/jira/browse/HDFS-9339) | Extend full test of KMS ACLs |  Major | test | Daniel Templeton | Daniel Templeton |
| [HDFS-9354](https://issues.apache.org/jira/browse/HDFS-9354) | Fix TestBalancer#testBalancerWithZeroThreadsForMove on Windows |  Major | test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-9410](https://issues.apache.org/jira/browse/HDFS-9410) | Some tests should always reset sysout and syserr |  Minor | test | Xiao Chen | Xiao Chen |
| [HADOOP-12564](https://issues.apache.org/jira/browse/HADOOP-12564) |  Upgrade JUnit3 TestCase to JUnit 4 in org.apache.hadoop.io package |  Trivial | test | Dustin Cote | Dustin Cote |
| [HDFS-9153](https://issues.apache.org/jira/browse/HDFS-9153) | Pretty-format the output for DFSIO |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-9429](https://issues.apache.org/jira/browse/HDFS-9429) | Tests in TestDFSAdminWithHA intermittently fail with EOFException |  Major | test | Xiao Chen | Xiao Chen |
| [HADOOP-10729](https://issues.apache.org/jira/browse/HADOOP-10729) | Add tests for PB RPC in case version mismatch of client and server |  Major | ipc | Junping Du | Junping Du |
| [HDFS-9300](https://issues.apache.org/jira/browse/HDFS-9300) | TestDirectoryScanner.testThrottle() is still a little flakey |  Major | balancer & mover, test | Daniel Templeton | Daniel Templeton |
| [HDFS-7553](https://issues.apache.org/jira/browse/HDFS-7553) | fix the TestDFSUpgradeWithHA due to BindException |  Major | test | Liang Xie | Xiao Chen |
| [HDFS-9626](https://issues.apache.org/jira/browse/HDFS-9626) | TestBlockReplacement#testBlockReplacement fails occasionally |  Minor | test | Xiao Chen | Xiao Chen |
| [HADOOP-12696](https://issues.apache.org/jira/browse/HADOOP-12696) | Add Tests for S3FileSystem Contract |  Major | tools | Matthew Paduano | Matthew Paduano |
| [HADOOP-12736](https://issues.apache.org/jira/browse/HADOOP-12736) | TestTimedOutTestsListener#testThreadDumpAndDeadlocks sometimes times out |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-12715](https://issues.apache.org/jira/browse/HADOOP-12715) | TestValueQueue#testgetAtMostPolicyALL fails intermittently |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-9773](https://issues.apache.org/jira/browse/HDFS-9773) | Remove dead code related to SimulatedFSDataset in tests |  Minor | test | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-4704](https://issues.apache.org/jira/browse/YARN-4704) | TestResourceManager#testResourceAllocation() fails when using FairScheduler |  Major | fairscheduler, test | Ray Chiang | Yufei Gu |
| [HDFS-9888](https://issues.apache.org/jira/browse/HDFS-9888) | Allow reseting KerberosName in unit tests |  Minor | . | Xiao Chen | Xiao Chen |
| [YARN-4717](https://issues.apache.org/jira/browse/YARN-4717) | TestResourceLocalizationService.testPublicResourceInitializesLocalDir fails Intermittently due to IllegalArgumentException from cleanup |  Minor | nodemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-9949](https://issues.apache.org/jira/browse/HDFS-9949) | Add a test case to ensure that the DataNode does not regenerate its UUID when a storage directory is cleared |  Minor | . | Harsh J | Harsh J |
| [HADOOP-12738](https://issues.apache.org/jira/browse/HADOOP-12738) | Create unit test to automatically compare Common related classes and core-default.xml |  Minor | . | Ray Chiang | Ray Chiang |
| [HADOOP-12101](https://issues.apache.org/jira/browse/HADOOP-12101) | Add automatic search of default Configuration variables to TestConfigurationFieldsBase |  Major | test | Ray Chiang | Ray Chiang |
| [HADOOP-13051](https://issues.apache.org/jira/browse/HADOOP-13051) | Add Glob unit test for special characters |  Minor | fs | Harsh J | Harsh J |
| [YARN-4947](https://issues.apache.org/jira/browse/YARN-4947) | Test timeout is happening for TestRMWebServicesNodes |  Major | test | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-12701](https://issues.apache.org/jira/browse/HADOOP-12701) | Run checkstyle on test source files |  Minor | . | John Zhuge | John Zhuge |
| [YARN-5069](https://issues.apache.org/jira/browse/YARN-5069) | TestFifoScheduler.testResourceOverCommit race condition |  Major | test | Eric Badger | Eric Badger |
| [HADOOP-13178](https://issues.apache.org/jira/browse/HADOOP-13178) | TestShellBasedIdMapping.testStaticMapUpdate doesn't work on OS X |  Major | test | Allen Wittenauer | Kai Sasaki |
| [YARN-5114](https://issues.apache.org/jira/browse/YARN-5114) | Add additional tests in TestRMWebServicesApps and rectify testInvalidAppAttempts failure in 2.8 |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5024](https://issues.apache.org/jira/browse/YARN-5024) | TestContainerResourceUsage#testUsageAfterAMRestartWithMultipleContainers random failure |  Major | test | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5005](https://issues.apache.org/jira/browse/YARN-5005) | TestRMWebServices#testDumpingSchedulerLogs fails randomly |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5118](https://issues.apache.org/jira/browse/YARN-5118) | Tests fails with localizer port bind exception. |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10375](https://issues.apache.org/jira/browse/HDFS-10375) | Remove redundant TestMiniDFSCluster.testDualClusters |  Trivial | test | John Zhuge | Jiayi Zhou |
| [YARN-4989](https://issues.apache.org/jira/browse/YARN-4989) | TestWorkPreservingRMRestart#testCapacitySchedulerRecovery fails intermittently |  Major | test | Rohith Sharma K S | Ajith S |
| [YARN-5208](https://issues.apache.org/jira/browse/YARN-5208) | Run TestAMRMClient TestNMClient TestYarnClient TestClientRMTokens TestAMAuthorization tests with hadoop.security.token.service.use\_ip enabled |  Blocker | test | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-12875](https://issues.apache.org/jira/browse/HADOOP-12875) | [Azure Data Lake] Support for contract test and unit test cases |  Major | fs, fs/azure, tools | Vishwajeet Dusane | Vishwajeet Dusane |
| [HADOOP-9321](https://issues.apache.org/jira/browse/HADOOP-9321) | fix coverage  org.apache.hadoop.net |  Major | . | Aleksey Gorshkov | Ivan A. Veselovsky |
| [HADOOP-9330](https://issues.apache.org/jira/browse/HADOOP-9330) | Add custom JUnit4 test runner with configurable timeout |  Major | test | Steve Loughran | Steve Loughran |
| [YARN-5023](https://issues.apache.org/jira/browse/YARN-5023) | TestAMRestart#testShouldNotCountFailureToMaxAttemptRetry random failure |  Major | . | Bibin A Chundatt | sandflee |
| [YARN-5318](https://issues.apache.org/jira/browse/YARN-5318) | TestRMAdminService#testRefreshNodesResourceWithFileSystemBasedConfigurationProvider fails intermittently. |  Minor | . | sandflee | Jun Gong |
| [YARN-5037](https://issues.apache.org/jira/browse/YARN-5037) | TestRMRestart#testQueueMetricsOnRMRestart random failure |  Major | . | sandflee | sandflee |
| [YARN-5317](https://issues.apache.org/jira/browse/YARN-5317) | testAMRestartNotLostContainerCompleteMsg may fail |  Minor | . | sandflee | sandflee |
| [YARN-5159](https://issues.apache.org/jira/browse/YARN-5159) | Wrong Javadoc tag in MiniYarnCluster |  Major | documentation | Andras Bokor | Andras Bokor |
| [MAPREDUCE-6738](https://issues.apache.org/jira/browse/MAPREDUCE-6738) | TestJobListCache.testAddExisting failed intermittently in slow VM testbed |  Minor | . | Junping Du | Junping Du |
| [YARN-5092](https://issues.apache.org/jira/browse/YARN-5092) | TestRMDelegationTokens fails intermittently |  Major | test | Rohith Sharma K S | Jason Lowe |
| [HADOOP-10980](https://issues.apache.org/jira/browse/HADOOP-10980) | TestActiveStandbyElector fails occasionally in trunk |  Minor | . | Ted Yu | Eric Badger |
| [HADOOP-13395](https://issues.apache.org/jira/browse/HADOOP-13395) | Enhance TestKMSAudit |  Minor | kms | Xiao Chen | Xiao Chen |
| [YARN-5343](https://issues.apache.org/jira/browse/YARN-5343) | TestContinuousScheduling#testSortedNodes fails intermittently |  Minor | . | sandflee | Yufei Gu |
| [YARN-2398](https://issues.apache.org/jira/browse/YARN-2398) | TestResourceTrackerOnHA crashes |  Major | test | Jason Lowe | Ajith S |
| [YARN-5492](https://issues.apache.org/jira/browse/YARN-5492) | TestSubmitApplicationWithRMHA is failing sporadically during precommit builds |  Major | test | Jason Lowe | Vrushali C |
| [YARN-5544](https://issues.apache.org/jira/browse/YARN-5544) | TestNodeBlacklistingOnAMFailures fails on trunk |  Major | test | Varun Saxena | Sunil G |
| [HDFS-9745](https://issues.apache.org/jira/browse/HDFS-9745) | TestSecureNNWithQJM#testSecureMode sometimes fails with timeouts |  Minor | . | Xiao Chen | Xiao Chen |
| [YARN-5389](https://issues.apache.org/jira/browse/YARN-5389) | TestYarnClient#testReservationDelete fails |  Major | . | Rohith Sharma K S | Sean Po |
| [YARN-5560](https://issues.apache.org/jira/browse/YARN-5560) | Clean up bad exception catching practices in TestYarnClient |  Major | . | Sean Po | Sean Po |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-3178](https://issues.apache.org/jira/browse/HDFS-3178) | Add states for journal synchronization in journal daemon |  Major | ha, namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3273](https://issues.apache.org/jira/browse/HDFS-3273) | Refactor BackupImage and FSEditLog |  Major | ha, namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3292](https://issues.apache.org/jira/browse/HDFS-3292) | Remove the deprecated DistributedFileSystem.DiskStatus and the related methods |  Major | hdfs-client | Tsz Wo Nicholas Sze | Arpit Gupta |
| [HADOOP-9258](https://issues.apache.org/jira/browse/HADOOP-9258) | Add stricter tests to FileSystemContractTestBase |  Major | test | Steve Loughran | Steve Loughran |
| [MAPREDUCE-5189](https://issues.apache.org/jira/browse/MAPREDUCE-5189) | Basic AM changes to support preemption requests (per YARN-45) |  Major | mr-am, mrv2 | Carlo Curino | Carlo Curino |
| [MAPREDUCE-5867](https://issues.apache.org/jira/browse/MAPREDUCE-5867) | Possible NPE in KillAMPreemptionPolicy related to ProportionalCapacityPreemptionPolicy |  Major | resourcemanager | Sunil G | Sunil G |
| [HDFS-6922](https://issues.apache.org/jira/browse/HDFS-6922) | Add LazyPersist flag to INodeFile, save it in FsImage and edit logs |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7071](https://issues.apache.org/jira/browse/HDFS-7071) | Updated editsStored and editsStored.xml to bump layout version and add LazyPersist flag |  Major | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [MAPREDUCE-6078](https://issues.apache.org/jira/browse/MAPREDUCE-6078) | native-task: fix gtest build on macosx |  Trivial | task | Binglin Chang | Binglin Chang |
| [HDFS-7184](https://issues.apache.org/jira/browse/HDFS-7184) | Allow data migration tool to run as a daemon |  Minor | balancer & mover, scripts | Benoy Antony | Benoy Antony |
| [HDFS-7035](https://issues.apache.org/jira/browse/HDFS-7035) | Make adding a new data directory to the DataNode an atomic operation and improve error handling |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-2766](https://issues.apache.org/jira/browse/YARN-2766) |  ApplicationHistoryManager is expected to return a sorted list of apps/attempts/containers |  Major | timelineserver | Robert Kanter | Robert Kanter |
| [YARN-1984](https://issues.apache.org/jira/browse/YARN-1984) | LeveldbTimelineStore does not handle db exceptions properly |  Major | . | Jason Lowe | Varun Saxena |
| [YARN-2920](https://issues.apache.org/jira/browse/YARN-2920) | CapacityScheduler should be notified when labels on nodes changed |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-3063](https://issues.apache.org/jira/browse/YARN-3063) | Bootstrap TimelineServer Next Gen Module |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-3092](https://issues.apache.org/jira/browse/YARN-3092) | Create common ResourceUsage class to track labeled resource usages in Capacity Scheduler |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3030](https://issues.apache.org/jira/browse/YARN-3030) | [Collector wireup] Set up timeline collector with basic request serving structure and lifecycle |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-3011](https://issues.apache.org/jira/browse/YARN-3011) | NM dies because of the failure of resource localization |  Major | nodemanager | Wang Hao | Varun Saxena |
| [YARN-3099](https://issues.apache.org/jira/browse/YARN-3099) | Capacity Scheduler LeafQueue/ParentQueue should use ResourceUsage to track used-resources-by-label. |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3098](https://issues.apache.org/jira/browse/YARN-3098) | Create common QueueCapacities class in Capacity Scheduler to track capacities-by-labels of queues |  Major | capacityscheduler | Wangda Tan | Wangda Tan |
| [YARN-2694](https://issues.apache.org/jira/browse/YARN-2694) | Ensure only single node labels specified in resource request / host, and node label expression only specified when resourceName=ANY |  Major | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [HDFS-7751](https://issues.apache.org/jira/browse/HDFS-7751) | Fix TestHDFSCLI for quota with storage type |  Minor | test | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-3124](https://issues.apache.org/jira/browse/YARN-3124) | Capacity Scheduler LeafQueue/ParentQueue should use QueueCapacities to track capacities-by-label |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3041](https://issues.apache.org/jira/browse/YARN-3041) | [Data Model] create overall data objects of TS next gen |  Major | timelineserver | Sangjin Lee | Zhijie Shen |
| [YARN-3240](https://issues.apache.org/jira/browse/YARN-3240) | [Data Mode] Implement client API to put generic entities |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-3087](https://issues.apache.org/jira/browse/YARN-3087) | [Collector implementation] the REST server (web server) for per-node collector does not work if it runs inside node manager |  Major | timelineserver | Sangjin Lee | Li Lu |
| [YARN-3125](https://issues.apache.org/jira/browse/YARN-3125) | [Event producers] Change distributed shell to use new timeline service |  Major | timelineserver | Zhijie Shen | Junping Du |
| [YARN-3210](https://issues.apache.org/jira/browse/YARN-3210) | [Source organization] Refactor timeline collector according to new code organization |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-3264](https://issues.apache.org/jira/browse/YARN-3264) | [Storage implementation] Create backing storage write interface and  a POC only file based storage implementation |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-1809](https://issues.apache.org/jira/browse/YARN-1809) | Synchronize RM and Generic History Service Web-UIs |  Major | . | Zhijie Shen | Xuan Gong |
| [YARN-1884](https://issues.apache.org/jira/browse/YARN-1884) | ContainerReport should have nodeHttpAddress |  Major | . | Zhijie Shen | Xuan Gong |
| [HADOOP-11710](https://issues.apache.org/jira/browse/HADOOP-11710) | Make CryptoOutputStream behave like DFSOutputStream wrt synchronization |  Critical | fs | Sean Busbey | Sean Busbey |
| [YARN-3171](https://issues.apache.org/jira/browse/YARN-3171) | Sort by Application id, AppAttempt & ContainerID doesn't work in ATS / RM web ui |  Minor | timelineserver | Jeff Zhang | Naganarasimha G R |
| [YARN-3039](https://issues.apache.org/jira/browse/YARN-3039) | [Collector wireup] Implement timeline app-level collector service discovery |  Major | timelineserver | Sangjin Lee | Junping Du |
| [YARN-3377](https://issues.apache.org/jira/browse/YARN-3377) | TestTimelineServiceClientIntegration fails |  Minor | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-3356](https://issues.apache.org/jira/browse/YARN-3356) | Capacity Scheduler FiCaSchedulerApp should use ResourceUsage to track used-resources-by-label. |  Major | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3345](https://issues.apache.org/jira/browse/YARN-3345) | Add non-exclusive node label API to RMAdmin protocol and NodeLabelsManager |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [HDFS-7854](https://issues.apache.org/jira/browse/HDFS-7854) | Separate class DataStreamer out of DFSOutputStream |  Major | hdfs-client | Li Bo | Li Bo |
| [YARN-3034](https://issues.apache.org/jira/browse/YARN-3034) | [Collector wireup] Implement RM starting its timeline collector |  Major | timelineserver | Sangjin Lee | Naganarasimha G R |
| [HDFS-7713](https://issues.apache.org/jira/browse/HDFS-7713) | Implement mkdirs in the HDFS Web UI |  Major | ui | Ravi Prakash | Ravi Prakash |
| [YARN-3040](https://issues.apache.org/jira/browse/YARN-3040) | [Data Model] Make putEntities operation be aware of the app's context |  Major | timelineserver | Sangjin Lee | Zhijie Shen |
| [YARN-2495](https://issues.apache.org/jira/browse/YARN-2495) | Allow admin specify labels from each NM (Distributed configuration) |  Major | resourcemanager | Wangda Tan | Naganarasimha G R |
| [YARN-3374](https://issues.apache.org/jira/browse/YARN-3374) | Collector's web server should randomly bind an available port |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7893](https://issues.apache.org/jira/browse/HDFS-7893) | Update the POM to create a separate hdfs-client jar |  Major | build | Haohui Mai | Haohui Mai |
| [YARN-3365](https://issues.apache.org/jira/browse/YARN-3365) | Add support for using the 'tc' tool via container-executor |  Major | nodemanager | Sidharta Seethana | Sidharta Seethana |
| [HDFS-8034](https://issues.apache.org/jira/browse/HDFS-8034) | Fix TestDFSClientRetries#testDFSClientConfigurationLocateFollowingBlockInitialDelay for Windows |  Minor | test | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-3334](https://issues.apache.org/jira/browse/YARN-3334) | [Event Producers] NM TimelineClient container metrics posting to new timeline service. |  Major | nodemanager | Junping Du | Junping Du |
| [YARN-3110](https://issues.apache.org/jira/browse/YARN-3110) | Few issues in ApplicationHistory web ui |  Minor | applications, timelineserver | Bibin A Chundatt | Naganarasimha G R |
| [HDFS-8049](https://issues.apache.org/jira/browse/HDFS-8049) | Annotation client implementation as private |  Major | hdfs-client | Tsz Wo Nicholas Sze | Takuya Fukudome |
| [HDFS-8079](https://issues.apache.org/jira/browse/HDFS-8079) | Separate the client retry conf from DFSConfigKeys |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-8080](https://issues.apache.org/jira/browse/HDFS-8080) | Separate JSON related routines used by WebHdfsFileSystem to a package local class |  Minor | hdfs-client | Haohui Mai | Haohui Mai |
| [HDFS-8085](https://issues.apache.org/jira/browse/HDFS-8085) | Move CorruptFileBlockIterator to the hdfs.client.impl package |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-8089](https://issues.apache.org/jira/browse/HDFS-8089) | Move o.a.h.hdfs.web.resources.\* to the client jars |  Minor | build | Haohui Mai | Haohui Mai |
| [HDFS-8102](https://issues.apache.org/jira/browse/HDFS-8102) | Separate webhdfs retry configuration keys from DFSConfigKeys |  Minor | hdfs-client | Haohui Mai | Haohui Mai |
| [YARN-3391](https://issues.apache.org/jira/browse/YARN-3391) | Clearly define flow ID/ flow run / flow version in API and storage |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-1376](https://issues.apache.org/jira/browse/YARN-1376) | NM need to notify the log aggregation status to RM through Node heartbeat |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-8100](https://issues.apache.org/jira/browse/HDFS-8100) | Refactor DFSClient.Conf to a standalone class |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-8103](https://issues.apache.org/jira/browse/HDFS-8103) | Move BlockTokenSecretManager.AccessMode into BlockTokenIdentifier |  Minor | security | Haohui Mai | Haohui Mai |
| [HDFS-8084](https://issues.apache.org/jira/browse/HDFS-8084) | Separate the client failover conf from DFSConfigKeys |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-8083](https://issues.apache.org/jira/browse/HDFS-8083) | Separate the client write conf from DFSConfigKeys |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-3347](https://issues.apache.org/jira/browse/YARN-3347) | Improve YARN log command to get AMContainer logs as well as running containers logs |  Major | log-aggregation | Xuan Gong | Xuan Gong |
| [YARN-3443](https://issues.apache.org/jira/browse/YARN-3443) | Create a 'ResourceHandler' subsystem to ease addition of support for new resource types on the NM |  Major | nodemanager | Sidharta Seethana | Sidharta Seethana |
| [HDFS-7701](https://issues.apache.org/jira/browse/HDFS-7701) | Support reporting per storage type quota and usage with hadoop/hdfs shell |  Major | datanode, namenode | Xiaoyu Yao | Peter Shi |
| [YARN-3361](https://issues.apache.org/jira/browse/YARN-3361) | CapacityScheduler side changes to support non-exclusive node labels |  Major | capacityscheduler | Wangda Tan | Wangda Tan |
| [YARN-3318](https://issues.apache.org/jira/browse/YARN-3318) | Create Initial OrderingPolicy Framework and FifoOrderingPolicy |  Major | scheduler | Craig Welch | Craig Welch |
| [YARN-3326](https://issues.apache.org/jira/browse/YARN-3326) | Support RESTful API for getLabelsToNodes |  Minor | resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [YARN-3354](https://issues.apache.org/jira/browse/YARN-3354) | Container should contains node-labels asked by original ResourceRequests |  Major | api, capacityscheduler, nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [HDFS-8082](https://issues.apache.org/jira/browse/HDFS-8082) | Separate the client read conf from DFSConfigKeys |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-8165](https://issues.apache.org/jira/browse/HDFS-8165) | Move GRANDFATHER\_GENERATION\_STAMP and GRANDFATER\_INODE\_ID to hdfs-client |  Major | build | Haohui Mai | Haohui Mai |
| [YARN-1402](https://issues.apache.org/jira/browse/YARN-1402) | Related Web UI, CLI changes on exposing client API to check log aggregation status |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-2696](https://issues.apache.org/jira/browse/YARN-2696) | Queue sorting in CapacityScheduler should consider node label |  Major | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3487](https://issues.apache.org/jira/browse/YARN-3487) | CapacityScheduler scheduler lock obtained unnecessarily when calling getQueue |  Critical | capacityscheduler | Jason Lowe | Jason Lowe |
| [YARN-3136](https://issues.apache.org/jira/browse/YARN-3136) | getTransferredContainers can be a bottleneck during AM registration |  Major | scheduler | Jason Lowe | Sunil G |
| [HDFS-8169](https://issues.apache.org/jira/browse/HDFS-8169) | Move LocatedBlocks and related classes to hdfs-client |  Major | build, hdfs-client | Haohui Mai | Haohui Mai |
| [YARN-3463](https://issues.apache.org/jira/browse/YARN-3463) | Integrate OrderingPolicy Framework with CapacityScheduler |  Major | capacityscheduler | Craig Welch | Craig Welch |
| [MAPREDUCE-6327](https://issues.apache.org/jira/browse/MAPREDUCE-6327) | [Event producers] Implement MapReduce AM writing MR events/counters to v2 ATS |  Major | . | Sangjin Lee | Junping Du |
| [HDFS-8185](https://issues.apache.org/jira/browse/HDFS-8185) | Separate client related routines in HAUtil into a new class |  Major | build, hdfs-client | Haohui Mai | Haohui Mai |
| [YARN-3225](https://issues.apache.org/jira/browse/YARN-3225) | New parameter or CLI for decommissioning node gracefully in RMAdmin CLI |  Major | graceful | Junping Du | Devaraj K |
| [HDFS-8218](https://issues.apache.org/jira/browse/HDFS-8218) | Move classes that used by ClientProtocol into hdfs-client |  Major | build | Haohui Mai | Haohui Mai |
| [HADOOP-11850](https://issues.apache.org/jira/browse/HADOOP-11850) | Typos in hadoop-common java docs |  Minor | . | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-3366](https://issues.apache.org/jira/browse/YARN-3366) | Outbound network bandwidth : classify/shape traffic originating from YARN containers |  Major | . | Sidharta Seethana | Sidharta Seethana |
| [YARN-2605](https://issues.apache.org/jira/browse/YARN-2605) | [RM HA] Rest api endpoints doing redirect incorrectly |  Major | resourcemanager | bc Wong | Xuan Gong |
| [HADOOP-10597](https://issues.apache.org/jira/browse/HADOOP-10597) | RPC Server signals backoff to clients when all request queues are full |  Major | . | Ming Ma | Ming Ma |
| [YARN-3319](https://issues.apache.org/jira/browse/YARN-3319) | Implement a FairOrderingPolicy |  Major | scheduler | Craig Welch | Craig Welch |
| [YARN-3413](https://issues.apache.org/jira/browse/YARN-3413) | Node label attributes (like exclusivity) should settable via addToClusterNodeLabels but shouldn't be changeable at runtime |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [HADOOP-11852](https://issues.apache.org/jira/browse/HADOOP-11852) | Disable symlinks in trunk |  Major | . | Andrew Wang | Andrew Wang |
| [YARN-3390](https://issues.apache.org/jira/browse/YARN-3390) | Reuse TimelineCollectorManager for RM |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2498](https://issues.apache.org/jira/browse/YARN-2498) | Respect labels in preemption policy of capacity scheduler for inter-queue preemption |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3431](https://issues.apache.org/jira/browse/YARN-3431) | Sub resources of timeline entity needs to be passed to a separate endpoint. |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2740](https://issues.apache.org/jira/browse/YARN-2740) | Fix NodeLabelsManager to properly handle node label modifications when distributed node label configuration enabled |  Major | resourcemanager | Wangda Tan | Naganarasimha G R |
| [MAPREDUCE-6335](https://issues.apache.org/jira/browse/MAPREDUCE-6335) | convert load test driver to timeline service v.2 |  Major | . | Sangjin Lee | Sangjin Lee |
| [YARN-3544](https://issues.apache.org/jira/browse/YARN-3544) | AM logs link missing in the RM UI for a completed app |  Blocker | . | Hitesh Shah | Xuan Gong |
| [YARN-2619](https://issues.apache.org/jira/browse/YARN-2619) | NodeManager: Add cgroups support for disk I/O isolation |  Major | . | Wei Yan | Varun Vasudev |
| [HDFS-8086](https://issues.apache.org/jira/browse/HDFS-8086) | Move LeaseRenewer to the hdfs.client.impl package |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Takanobu Asanuma |
| [YARN-3006](https://issues.apache.org/jira/browse/YARN-3006) | Improve the error message when attempting manual failover with auto-failover enabled |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-8249](https://issues.apache.org/jira/browse/HDFS-8249) | Separate HdfsConstants into the client and the server side class |  Major | hdfs-client | Haohui Mai | Haohui Mai |
| [HDFS-8309](https://issues.apache.org/jira/browse/HDFS-8309) | Skip unit test using DataNodeTestUtils#injectDataDirFailure() on Windows |  Minor | test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-8237](https://issues.apache.org/jira/browse/HDFS-8237) | Move all protocol classes used by ClientProtocol to hdfs-client |  Major | build | Haohui Mai | Haohui Mai |
| [YARN-3551](https://issues.apache.org/jira/browse/YARN-3551) | Consolidate data model change according to the backend implementation |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-8314](https://issues.apache.org/jira/browse/HDFS-8314) | Move HdfsServerConstants#IO\_FILE\_BUFFER\_SIZE and SMALL\_BUFFER\_SIZE to the users |  Major | . | Haohui Mai | Li Lu |
| [HDFS-8310](https://issues.apache.org/jira/browse/HDFS-8310) | Fix TestCLI.testAll 'help: help for find' on Windows |  Minor | test | Xiaoyu Yao | Kiran Kumar M R |
| [YARN-3301](https://issues.apache.org/jira/browse/YARN-3301) | Fix the format issue of the new RM web UI and AHS web UI after YARN-3272 / YARN-3262 |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-3562](https://issues.apache.org/jira/browse/YARN-3562) | unit tests failures and issues found from findbug from earlier ATS checkins |  Minor | timelineserver | Sangjin Lee | Naganarasimha G R |
| [YARN-3448](https://issues.apache.org/jira/browse/YARN-3448) | Add Rolling Time To Lives Level DB Plugin Capabilities |  Major | timelineserver | Jonathan Eagles | Jonathan Eagles |
| [YARN-2918](https://issues.apache.org/jira/browse/YARN-2918) | Don't fail RM if queue's configured labels are not existed in cluster-node-labels |  Major | resourcemanager | Rohith Sharma K S | Wangda Tan |
| [YARN-644](https://issues.apache.org/jira/browse/YARN-644) | Basic null check is not performed on passed in arguments before using them in ContainerManagerImpl.startContainer |  Minor | nodemanager | Omkar Vinit Joshi | Varun Saxena |
| [YARN-3593](https://issues.apache.org/jira/browse/YARN-3593) | Add label-type and Improve "DEFAULT\_PARTITION" in Node Labels Page |  Major | webapp | Naganarasimha G R | Naganarasimha G R |
| [YARN-2331](https://issues.apache.org/jira/browse/YARN-2331) | Distinguish shutdown during supervision vs. shutdown for rolling upgrade |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-3134](https://issues.apache.org/jira/browse/YARN-3134) | [Storage implementation] Exploiting the option of using Phoenix to access HBase backend |  Major | timelineserver | Zhijie Shen | Li Lu |
| [YARN-3529](https://issues.apache.org/jira/browse/YARN-3529) | Add miniHBase cluster and Phoenix support to ATS v2 unit tests |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-3634](https://issues.apache.org/jira/browse/YARN-3634) | TestMRTimelineEventHandling and TestApplication are broken |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-3579](https://issues.apache.org/jira/browse/YARN-3579) | CommonNodeLabelsManager should support NodeLabel instead of string label name when getting node-to-label/label-to-label mappings |  Minor | resourcemanager | Sunil G | Sunil G |
| [MAPREDUCE-6337](https://issues.apache.org/jira/browse/MAPREDUCE-6337) | add a mode to replay MR job history files to the timeline service |  Major | . | Sangjin Lee | Sangjin Lee |
| [YARN-3505](https://issues.apache.org/jira/browse/YARN-3505) | Node's Log Aggregation Report with SUCCEED should not cached in RMApps |  Critical | log-aggregation | Junping Du | Xuan Gong |
| [HDFS-8403](https://issues.apache.org/jira/browse/HDFS-8403) | Eliminate retries in TestFileCreation#testOverwriteOpenForWrite |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8157](https://issues.apache.org/jira/browse/HDFS-8157) | Writes to RAM DISK reserve locked memory for block files |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [YARN-3541](https://issues.apache.org/jira/browse/YARN-3541) | Add version info on timeline service / generic history web UI and REST API |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-3565](https://issues.apache.org/jira/browse/YARN-3565) | NodeHeartbeatRequest/RegisterNodeManagerRequest should use NodeLabel object instead of String |  Blocker | api, client, resourcemanager | Wangda Tan | Naganarasimha G R |
| [YARN-3583](https://issues.apache.org/jira/browse/YARN-3583) | Support of NodeLabel object instead of plain String in YarnClient side. |  Major | client | Sunil G | Sunil G |
| [YARN-3609](https://issues.apache.org/jira/browse/YARN-3609) | Move load labels from storage from serviceInit to serviceStart to make it works with RM HA case. |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3684](https://issues.apache.org/jira/browse/YARN-3684) | Change ContainerExecutor's primary lifecycle methods to use a more extensible mechanism for passing information. |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [YARN-3411](https://issues.apache.org/jira/browse/YARN-3411) | [Storage implementation] explore the native HBase write schema for storage |  Critical | timelineserver | Sangjin Lee | Vrushali C |
| [HDFS-8454](https://issues.apache.org/jira/browse/HDFS-8454) | Remove unnecessary throttling in TestDatanodeDeath |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-6370](https://issues.apache.org/jira/browse/MAPREDUCE-6370) | Timeline service v2 load generator needs to write event id |  Major | . | Li Lu | Li Lu |
| [HDFS-8377](https://issues.apache.org/jira/browse/HDFS-8377) | Support HTTP/2 in datanode |  Major | . | Duo Zhang | Duo Zhang |
| [YARN-3632](https://issues.apache.org/jira/browse/YARN-3632) | Ordering policy should be allowed to reorder an application when demand changes |  Major | capacityscheduler | Craig Welch | Craig Welch |
| [YARN-3686](https://issues.apache.org/jira/browse/YARN-3686) | CapacityScheduler should trim default\_node\_label\_expression |  Critical | api, client, resourcemanager | Wangda Tan | Sunil G |
| [YARN-3647](https://issues.apache.org/jira/browse/YARN-3647) | RMWebServices api's should use updated api from CommonNodeLabelsManager to get NodeLabel object |  Major | resourcemanager | Sunil G | Sunil G |
| [YARN-3581](https://issues.apache.org/jira/browse/YARN-3581) | Deprecate -directlyAccessNodeLabelStore in RMAdminCLI |  Major | api, client, resourcemanager | Wangda Tan | Naganarasimha G R |
| [HDFS-8482](https://issues.apache.org/jira/browse/HDFS-8482) | Rename BlockInfoContiguous to BlockInfo |  Major | . | Zhe Zhang | Zhe Zhang |
| [YARN-3700](https://issues.apache.org/jira/browse/YARN-3700) | ATS Web Performance issue at load time when large number of jobs |  Major | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [YARN-3726](https://issues.apache.org/jira/browse/YARN-3726) | Fix TestHBaseTimelineWriterImpl unit test failure by fixing it's test data |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-3721](https://issues.apache.org/jira/browse/YARN-3721) | build is broken on YARN-2928 branch due to possible dependency cycle |  Blocker | timelineserver | Sangjin Lee | Li Lu |
| [YARN-3716](https://issues.apache.org/jira/browse/YARN-3716) | Node-label-expression should be included by ResourceRequestPBImpl.toString |  Minor | api | Xianyin Xin | Xianyin Xin |
| [YARN-3740](https://issues.apache.org/jira/browse/YARN-3740) | Fixed the typo with the configuration name: APPLICATION\_HISTORY\_PREFIX\_MAX\_APPS |  Major | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [YARN-2900](https://issues.apache.org/jira/browse/YARN-2900) | Application (Attempt and Container) Not Found in AHS results in Internal Server Error (500) |  Major | timelineserver | Jonathan Eagles | Mit Desai |
| [HDFS-8489](https://issues.apache.org/jira/browse/HDFS-8489) | Subclass BlockInfo to represent contiguous blocks |  Major | namenode | Zhe Zhang | Zhe Zhang |
| [YARN-3276](https://issues.apache.org/jira/browse/YARN-3276) | Refactor and fix null casting in some map cast for TimelineEntity (old and new) and fix findbug warnings |  Major | timelineserver | Junping Du | Junping Du |
| [YARN-2392](https://issues.apache.org/jira/browse/YARN-2392) | add more diags about app retry limits on AM failures |  Minor | resourcemanager | Steve Loughran | Steve Loughran |
| [YARN-3766](https://issues.apache.org/jira/browse/YARN-3766) | ATS Web UI breaks because of YARN-3467 |  Blocker | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [YARN-1462](https://issues.apache.org/jira/browse/YARN-1462) | AHS API and other AHS changes to handle tags for completed MR jobs |  Major | . | Karthik Kambatla | Xuan Gong |
| [YARN-1279](https://issues.apache.org/jira/browse/YARN-1279) | Expose a client API to allow clients to figure if log aggregation is complete |  Major | . | Arun C Murthy | Xuan Gong |
| [YARN-3787](https://issues.apache.org/jira/browse/YARN-3787) | loading applications by filtering appstartedTime period for ATS Web UI |  Major | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [HDFS-7923](https://issues.apache.org/jira/browse/HDFS-7923) | The DataNodes should rate-limit their full block reports by asking the NN on heartbeat messages |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [YARN-3044](https://issues.apache.org/jira/browse/YARN-3044) | [Event producers] Implement RM writing app lifecycle events to ATS |  Major | timelineserver | Sangjin Lee | Naganarasimha G R |
| [HDFS-8540](https://issues.apache.org/jira/browse/HDFS-8540) | Mover should exit with NO\_MOVE\_BLOCK if no block can be moved |  Major | balancer & mover | Tsz Wo Nicholas Sze | Surendra Singh Lilhore |
| [YARN-3711](https://issues.apache.org/jira/browse/YARN-3711) | Documentation of ResourceManager HA should explain configurations about listen addresses |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3801](https://issues.apache.org/jira/browse/YARN-3801) | [JDK-8][YARN-2928] Exclude jdk.tools from hbase-client and hbase-testing-util |  Major | timelineserver | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-8597](https://issues.apache.org/jira/browse/HDFS-8597) | Fix TestFSImage#testZeroBlockSize on Windows |  Major | datanode, test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7164](https://issues.apache.org/jira/browse/HDFS-7164) | Feature documentation for HDFS-6581 |  Major | documentation | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8238](https://issues.apache.org/jira/browse/HDFS-8238) | Move ClientProtocol to the hdfs-client |  Major | build | Haohui Mai | Takanobu Asanuma |
| [HDFS-6249](https://issues.apache.org/jira/browse/HDFS-6249) | Output AclEntry in PBImageXmlWriter |  Minor | tools | Akira Ajisaka | Surendra Singh Lilhore |
| [YARN-3706](https://issues.apache.org/jira/browse/YARN-3706) | Generalize native HBase writer for additional tables |  Minor | timelineserver | Joep Rottinghuis | Joep Rottinghuis |
| [YARN-3521](https://issues.apache.org/jira/browse/YARN-3521) | Support return structured NodeLabel objects in REST API |  Major | api, client, resourcemanager | Wangda Tan | Sunil G |
| [HDFS-8192](https://issues.apache.org/jira/browse/HDFS-8192) | Eviction should key off used locked memory instead of ram disk free space |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [YARN-3792](https://issues.apache.org/jira/browse/YARN-3792) | Test case failures in TestDistributedShell and some issue fixes related to ATSV2 |  Major | timelineserver | Naganarasimha G R | Naganarasimha G R |
| [HDFS-8651](https://issues.apache.org/jira/browse/HDFS-8651) | Make hadoop-hdfs-project Native code -Wall-clean |  Major | native | Alan Burlison | Alan Burlison |
| [HADOOP-12036](https://issues.apache.org/jira/browse/HADOOP-12036) | Consolidate all of the cmake extensions in one directory |  Major | . | Allen Wittenauer | Alan Burlison |
| [HDFS-7390](https://issues.apache.org/jira/browse/HDFS-7390) | Provide JMX metrics per storage type |  Major | . | Benoy Antony | Benoy Antony |
| [HADOOP-12104](https://issues.apache.org/jira/browse/HADOOP-12104) | Migrate Hadoop Pipes native build to new CMake framework |  Major | build | Alan Burlison | Alan Burlison |
| [HADOOP-12112](https://issues.apache.org/jira/browse/HADOOP-12112) | Make hadoop-common-project Native code -Wall-clean |  Major | native | Alan Burlison | Alan Burlison |
| [HDFS-8493](https://issues.apache.org/jira/browse/HDFS-8493) | Consolidate truncate() related implementation in a single class |  Major | . | Haohui Mai | Rakesh R |
| [HDFS-8635](https://issues.apache.org/jira/browse/HDFS-8635) | Migrate HDFS native build to new CMake framework |  Major | build | Alan Burlison | Alan Burlison |
| [MAPREDUCE-6407](https://issues.apache.org/jira/browse/MAPREDUCE-6407) | Migrate MAPREDUCE nativetask build to new CMake framework |  Major | build | Alan Burlison | Alan Burlison |
| [YARN-3827](https://issues.apache.org/jira/browse/YARN-3827) | Migrate YARN native build to new CMake framework |  Major | build | Alan Burlison | Alan Burlison |
| [MAPREDUCE-6376](https://issues.apache.org/jira/browse/MAPREDUCE-6376) | Add avro binary support for jhist files |  Major | jobhistoryserver | Ray Chiang | Ray Chiang |
| [YARN-3051](https://issues.apache.org/jira/browse/YARN-3051) | [Storage abstraction] Create backing storage read interface for ATS readers |  Major | timelineserver | Sangjin Lee | Varun Saxena |
| [HDFS-8620](https://issues.apache.org/jira/browse/HDFS-8620) | Clean up the checkstyle warinings about ClientProtocol |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-8726](https://issues.apache.org/jira/browse/HDFS-8726) | Move protobuf files that define the client-sever protocols to hdfs-client |  Major | build | Haohui Mai | Haohui Mai |
| [YARN-3047](https://issues.apache.org/jira/browse/YARN-3047) | [Data Serving] Set up ATS reader with basic request serving structure and lifecycle |  Major | timelineserver | Sangjin Lee | Varun Saxena |
| [YARN-1012](https://issues.apache.org/jira/browse/YARN-1012) | Report NM aggregated container resource utilization in heartbeat |  Major | nodemanager | Arun C Murthy | Inigo Goiri |
| [YARN-3800](https://issues.apache.org/jira/browse/YARN-3800) | Reduce storage footprint for ReservationAllocation |  Major | capacityscheduler, fairscheduler, resourcemanager | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-3836](https://issues.apache.org/jira/browse/YARN-3836) | add equals and hashCode to TimelineEntity and other classes in the data model |  Major | timelineserver | Sangjin Lee | Li Lu |
| [YARN-3445](https://issues.apache.org/jira/browse/YARN-3445) | Cache runningApps in RMNode for getting running apps on given NodeId |  Major | nodemanager, resourcemanager | Junping Du | Junping Du |
| [YARN-3116](https://issues.apache.org/jira/browse/YARN-3116) | [Collector wireup] We need an assured way to determine if a container is an AM container on NM |  Major | nodemanager, timelineserver | Zhijie Shen | Giovanni Matteo Fumarola |
| [HDFS-8541](https://issues.apache.org/jira/browse/HDFS-8541) | Mover should exit with NO\_MOVE\_PROGRESS if there is no move progress |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Surendra Singh Lilhore |
| [YARN-3866](https://issues.apache.org/jira/browse/YARN-3866) | AM-RM protocol changes to support container resizing |  Major | api | MENG DING | MENG DING |
| [HDFS-8742](https://issues.apache.org/jira/browse/HDFS-8742) | Inotify: Support event for OP\_TRUNCATE |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-1449](https://issues.apache.org/jira/browse/YARN-1449) | AM-NM protocol changes to support container resizing |  Major | api | Wangda Tan (No longer used) | MENG DING |
| [HADOOP-11974](https://issues.apache.org/jira/browse/HADOOP-11974) | Fix FIONREAD #include on Solaris |  Minor | net | Alan Burlison | Alan Burlison |
| [YARN-3930](https://issues.apache.org/jira/browse/YARN-3930) | FileSystemNodeLabelsStore should make sure edit log file closed when exception is thrown |  Major | api, client, resourcemanager | Dian Fu | Dian Fu |
| [YARN-3844](https://issues.apache.org/jira/browse/YARN-3844) | Make hadoop-yarn-project Native code -Wall-clean |  Major | build | Alan Burlison | Alan Burlison |
| [HDFS-8794](https://issues.apache.org/jira/browse/HDFS-8794) | Improve CorruptReplicasMap#corruptReplicasMap |  Major | . | Yi Liu | Yi Liu |
| [HDFS-7483](https://issues.apache.org/jira/browse/HDFS-7483) | Display information per tier on the Namenode UI |  Major | . | Benoy Antony | Benoy Antony |
| [YARN-2003](https://issues.apache.org/jira/browse/YARN-2003) | Support for Application priority : Changes in RM and Capacity Scheduler |  Major | resourcemanager | Sunil G | Sunil G |
| [HDFS-8721](https://issues.apache.org/jira/browse/HDFS-8721) | Add a metric for number of encryption zones |  Major | encryption | Rakesh R | Rakesh R |
| [YARN-1645](https://issues.apache.org/jira/browse/YARN-1645) | ContainerManager implementation to support container resizing |  Major | nodemanager | Wangda Tan | MENG DING |
| [HDFS-8495](https://issues.apache.org/jira/browse/HDFS-8495) | Consolidate append() related implementation into a single class |  Major | namenode | Rakesh R | Rakesh R |
| [HDFS-8795](https://issues.apache.org/jira/browse/HDFS-8795) | Improve InvalidateBlocks#node2blocks |  Major | . | Yi Liu | Yi Liu |
| [HADOOP-12184](https://issues.apache.org/jira/browse/HADOOP-12184) | Remove unused Linux-specific constants in NativeIO |  Major | native | Martin Walsh | Martin Walsh |
| [HDFS-8730](https://issues.apache.org/jira/browse/HDFS-8730) | Clean up the import statements in ClientProtocol |  Minor | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-3969](https://issues.apache.org/jira/browse/YARN-3969) | Allow jobs to be submitted to reservation that is active but does not have any allocations |  Major | capacityscheduler, fairscheduler, resourcemanager | Subru Krishnan | Subru Krishnan |
| [HADOOP-12170](https://issues.apache.org/jira/browse/HADOOP-12170) | hadoop-common's JNIFlags.cmake is redundant and can be removed |  Minor | native | Alan Burlison | Alan Burlison |
| [YARN-3656](https://issues.apache.org/jira/browse/YARN-3656) | LowCost: A Cost-Based Placement Agent for YARN Reservations |  Major | capacityscheduler, resourcemanager | Ishai Menache | Jonathan Yaniv |
| [YARN-3949](https://issues.apache.org/jira/browse/YARN-3949) | ensure timely flush of timeline writes |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-3852](https://issues.apache.org/jira/browse/YARN-3852) | Add docker container support to container-executor |  Major | yarn | Sidharta Seethana | Abin Shahab |
| [YARN-3853](https://issues.apache.org/jira/browse/YARN-3853) | Add docker container runtime support to LinuxContainterExecutor |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [YARN-3908](https://issues.apache.org/jira/browse/YARN-3908) | Bugs in HBaseTimelineWriterImpl |  Major | timelineserver | Zhijie Shen | Vrushali C |
| [YARN-3867](https://issues.apache.org/jira/browse/YARN-3867) | ContainerImpl changes to support container resizing |  Major | nodemanager | MENG DING | MENG DING |
| [HDFS-7192](https://issues.apache.org/jira/browse/HDFS-7192) | DN should ignore lazyPersist hint if the writer is not local |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [YARN-433](https://issues.apache.org/jira/browse/YARN-433) | When RM is catching up with node updates then it should not expire acquired containers |  Major | resourcemanager | Bikas Saha | Xuan Gong |
| [MAPREDUCE-6394](https://issues.apache.org/jira/browse/MAPREDUCE-6394) | Speed up Task processing loop in HsTasksBlock#render() |  Major | jobhistoryserver | Ray Chiang | Ray Chiang |
| [HADOOP-7824](https://issues.apache.org/jira/browse/HADOOP-7824) | NativeIO.java flags and identifiers must be set correctly for each platform, not hardcoded to their Linux values |  Major | native | Dmytro Shteflyuk | Martin Walsh |
| [YARN-3543](https://issues.apache.org/jira/browse/YARN-3543) | ApplicationReport should be able to tell whether the Application is AM managed or not. |  Major | api | Spandan Dutta | Rohith Sharma K S |
| [YARN-3993](https://issues.apache.org/jira/browse/YARN-3993) | Change to use the AM flag in ContainerContext determine AM container |  Major | timelineserver | Zhijie Shen | Sunil G |
| [YARN-4004](https://issues.apache.org/jira/browse/YARN-4004) | container-executor should print output of docker logs if the docker container exits with non-0 exit status |  Major | nodemanager | Varun Vasudev | Varun Vasudev |
| [YARN-3736](https://issues.apache.org/jira/browse/YARN-3736) | Add RMStateStore apis to store and load accepted reservations for failover |  Major | capacityscheduler, fairscheduler, resourcemanager | Subru Krishnan | Anubhav Dhoot |
| [YARN-1643](https://issues.apache.org/jira/browse/YARN-1643) | Make ContainersMonitor can support change monitoring size of an allocated container in NM side |  Major | nodemanager | Wangda Tan | MENG DING |
| [YARN-3984](https://issues.apache.org/jira/browse/YARN-3984) | Rethink event column key issue |  Major | timelineserver | Zhijie Shen | Vrushali C |
| [YARN-3974](https://issues.apache.org/jira/browse/YARN-3974) | Refactor the reservation system test cases to use parameterized base test |  Major | capacityscheduler, fairscheduler | Subru Krishnan | Subru Krishnan |
| [YARN-3948](https://issues.apache.org/jira/browse/YARN-3948) | Display Application Priority in RM Web UI |  Major | webapp | Sunil G | Sunil G |
| [YARN-3049](https://issues.apache.org/jira/browse/YARN-3049) | [Storage Implementation] Implement storage reader interface to fetch raw data from HBase backend |  Major | timelineserver | Sangjin Lee | Zhijie Shen |
| [YARN-3873](https://issues.apache.org/jira/browse/YARN-3873) | pendingApplications in LeafQueue should also use OrderingPolicy |  Major | capacityscheduler | Sunil G | Sunil G |
| [HDFS-8818](https://issues.apache.org/jira/browse/HDFS-8818) | Allow Balancer to run faster |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-3887](https://issues.apache.org/jira/browse/YARN-3887) | Support for changing Application priority during runtime |  Major | capacityscheduler, resourcemanager | Sunil G | Sunil G |
| [HDFS-8805](https://issues.apache.org/jira/browse/HDFS-8805) | Archival Storage: getStoragePolicy should not need superuser privilege |  Major | balancer & mover, namenode | Hui Zheng | Brahma Reddy Battula |
| [HDFS-8052](https://issues.apache.org/jira/browse/HDFS-8052) | Move WebHdfsFileSystem into hadoop-hdfs-client |  Major | build | Haohui Mai | Haohui Mai |
| [YARN-3906](https://issues.apache.org/jira/browse/YARN-3906) | split the application table from the entity table |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-4023](https://issues.apache.org/jira/browse/YARN-4023) | Publish Application Priority to TimelineServer |  Major | timelineserver | Sunil G | Sunil G |
| [HDFS-8824](https://issues.apache.org/jira/browse/HDFS-8824) | Do not use small blocks for balancing the cluster |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-3534](https://issues.apache.org/jira/browse/YARN-3534) | Collect memory/cpu usage on the node |  Major | nodemanager, resourcemanager | Inigo Goiri | Inigo Goiri |
| [HDFS-8801](https://issues.apache.org/jira/browse/HDFS-8801) | Convert BlockInfoUnderConstruction as a feature |  Major | namenode | Zhe Zhang | Jing Zhao |
| [HDFS-8792](https://issues.apache.org/jira/browse/HDFS-8792) | BlockManager#postponedMisreplicatedBlocks should use a LightWeightHashSet to save memory |  Major | . | Yi Liu | Yi Liu |
| [YARN-3904](https://issues.apache.org/jira/browse/YARN-3904) | Refactor timelineservice.storage to add support to online and offline aggregation writers |  Major | timelineserver | Li Lu | Li Lu |
| [HDFS-8862](https://issues.apache.org/jira/browse/HDFS-8862) | BlockManager#excessReplicateMap should use a HashMap |  Major | namenode | Yi Liu | Yi Liu |
| [HDFS-8278](https://issues.apache.org/jira/browse/HDFS-8278) | HDFS Balancer should consider remaining storage % when checking for under-utilized machines |  Major | balancer & mover | Gopal V | Tsz Wo Nicholas Sze |
| [YARN-3045](https://issues.apache.org/jira/browse/YARN-3045) | [Event producers] Implement NM writing container lifecycle events to ATS |  Major | timelineserver | Sangjin Lee | Naganarasimha G R |
| [HDFS-8826](https://issues.apache.org/jira/browse/HDFS-8826) | Balancer may not move blocks efficiently in some cases |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-4025](https://issues.apache.org/jira/browse/YARN-4025) | Deal with byte representations of Longs in writer code |  Major | timelineserver | Vrushali C | Sangjin Lee |
| [HDFS-8803](https://issues.apache.org/jira/browse/HDFS-8803) | Move DfsClientConf to hdfs-client |  Major | build | Haohui Mai | Mingliang Liu |
| [YARN-4064](https://issues.apache.org/jira/browse/YARN-4064) | build is broken at TestHBaseTimelineWriterImpl.java |  Blocker | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-2923](https://issues.apache.org/jira/browse/YARN-2923) | Support configuration based NodeLabelsProvider Service in Distributed Node Label Configuration Setup |  Major | nodemanager | Naganarasimha G R | Naganarasimha G R |
| [YARN-1644](https://issues.apache.org/jira/browse/YARN-1644) | RM-NM protocol changes and NodeStatusUpdater implementation to support container resizing |  Major | nodemanager | Wangda Tan | MENG DING |
| [YARN-3868](https://issues.apache.org/jira/browse/YARN-3868) | ContainerManager recovery for container resizing |  Major | nodemanager | MENG DING | MENG DING |
| [YARN-3814](https://issues.apache.org/jira/browse/YARN-3814) | REST API implementation for getting raw entities in TimelineReader |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [HDFS-8823](https://issues.apache.org/jira/browse/HDFS-8823) | Move replication factor into individual blocks |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-221](https://issues.apache.org/jira/browse/YARN-221) | NM should provide a way for AM to tell it not to aggregate logs. |  Major | log-aggregation, nodemanager | Robert Joseph Evans | Ming Ma |
| [HDFS-8934](https://issues.apache.org/jira/browse/HDFS-8934) | Move ShortCircuitShm to hdfs-client |  Major | build | Mingliang Liu | Mingliang Liu |
| [HDFS-8948](https://issues.apache.org/jira/browse/HDFS-8948) | Use GenericTestUtils to set log levels in TestPread and TestReplaceDatanodeOnFailure |  Major | build | Mingliang Liu | Mingliang Liu |
| [YARN-4058](https://issues.apache.org/jira/browse/YARN-4058) | Miscellaneous issues in NodeManager project |  Minor | timelineserver | Naganarasimha G R | Naganarasimha G R |
| [YARN-4014](https://issues.apache.org/jira/browse/YARN-4014) | Support user cli interface in for Application Priority |  Major | client, resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-8951](https://issues.apache.org/jira/browse/HDFS-8951) | Move the shortcircuit package to hdfs-client |  Major | build | Mingliang Liu | Mingliang Liu |
| [HDFS-8248](https://issues.apache.org/jira/browse/HDFS-8248) | Store INodeId instead of the INodeFile object in BlockInfoContiguous |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-8962](https://issues.apache.org/jira/browse/HDFS-8962) | Clean up checkstyle warnings in o.a.h.hdfs.DfsClientConf |  Major | build | Mingliang Liu | Mingliang Liu |
| [YARN-3250](https://issues.apache.org/jira/browse/YARN-3250) | Support admin cli interface in for Application Priority |  Major | resourcemanager | Sunil G | Rohith Sharma K S |
| [HDFS-8925](https://issues.apache.org/jira/browse/HDFS-8925) | Move BlockReaderLocal to hdfs-client |  Major | build | Mingliang Liu | Mingliang Liu |
| [HDFS-8980](https://issues.apache.org/jira/browse/HDFS-8980) | Remove unnecessary block replacement in INodeFile |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-8990](https://issues.apache.org/jira/browse/HDFS-8990) | Move RemoteBlockReader to hdfs-client module |  Major | build | Mingliang Liu | Mingliang Liu |
| [YARN-4092](https://issues.apache.org/jira/browse/YARN-4092) | RM HA UI redirection needs to be fixed when both RMs are in standby mode |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-4082](https://issues.apache.org/jira/browse/YARN-4082) | Container shouldn't be killed when node's label updated. |  Major | capacity scheduler | Wangda Tan | Wangda Tan |
| [YARN-2801](https://issues.apache.org/jira/browse/YARN-2801) | Add documentation for node labels feature |  Major | documentation | Gururaj Shetty | Wangda Tan |
| [YARN-3893](https://issues.apache.org/jira/browse/YARN-3893) | Both RM in active state when Admin#transitionToActive failure from refeshAll() |  Critical | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-8890](https://issues.apache.org/jira/browse/HDFS-8890) | Allow admin to specify which blockpools the balancer should run on |  Major | balancer & mover | Chris Trezzo | Chris Trezzo |
| [YARN-4101](https://issues.apache.org/jira/browse/YARN-4101) | RM should print alert messages if Zookeeper and Resourcemanager gets connection issue |  Critical | yarn | Yesha Vora | Xuan Gong |
| [YARN-3970](https://issues.apache.org/jira/browse/YARN-3970) | REST api support for Application Priority |  Major | webapp | Sunil G | Naganarasimha G R |
| [HDFS-9002](https://issues.apache.org/jira/browse/HDFS-9002) | Move o.a.h.hdfs.net/\*Peer classes to hdfs-client |  Major | build | Mingliang Liu | Mingliang Liu |
| [HDFS-9012](https://issues.apache.org/jira/browse/HDFS-9012) | Move o.a.h.hdfs.protocol.datatransfer.PipelineAck class to hadoop-hdfs-client module |  Major | build | Mingliang Liu | Mingliang Liu |
| [HDFS-8984](https://issues.apache.org/jira/browse/HDFS-8984) | Move replication queues related methods in FSNamesystem to BlockManager |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2884](https://issues.apache.org/jira/browse/YARN-2884) | Proxying all AM-RM communications |  Major | nodemanager, resourcemanager | Carlo Curino | Kishore Chaliparambil |
| [YARN-4136](https://issues.apache.org/jira/browse/YARN-4136) | LinuxContainerExecutor loses info when forwarding ResourceHandlerException |  Trivial | nodemanager | Steve Loughran | Bibin A Chundatt |
| [YARN-4102](https://issues.apache.org/jira/browse/YARN-4102) | Add a "skip existing table" mode for timeline schema creator |  Major | timelineserver | Li Lu | Li Lu |
| [HDFS-9041](https://issues.apache.org/jira/browse/HDFS-9041) | Move entries in META-INF/services/o.a.h.fs.FileSystem to hdfs-client |  Major | build | Haohui Mai | Mingliang Liu |
| [HDFS-9010](https://issues.apache.org/jira/browse/HDFS-9010) | Replace NameNode.DEFAULT\_PORT with HdfsClientConfigKeys.DFS\_NAMENODE\_RPC\_PORT\_DEFAULT config key |  Major | build | Mingliang Liu | Mingliang Liu |
| [YARN-1651](https://issues.apache.org/jira/browse/YARN-1651) | CapacityScheduler side changes to support increase/decrease container resource. |  Major | resourcemanager, scheduler | Wangda Tan | Wangda Tan |
| [YARN-313](https://issues.apache.org/jira/browse/YARN-313) | Add Admin API for supporting node resource configuration in command line |  Critical | client, graceful | Junping Du | Inigo Goiri |
| [HDFS-9008](https://issues.apache.org/jira/browse/HDFS-9008) | Balancer#Parameters class could use a builder pattern |  Minor | balancer & mover | Chris Trezzo | Chris Trezzo |
| [YARN-3635](https://issues.apache.org/jira/browse/YARN-3635) | Get-queue-mapping should be a common interface of YarnScheduler |  Major | scheduler | Wangda Tan | Tan, Wangda |
| [YARN-3717](https://issues.apache.org/jira/browse/YARN-3717) | Expose app/am/queue's node-label-expression to RM web UI / CLI / REST-API |  Major | . | Naganarasimha G R | Naganarasimha G R |
| [HDFS-7986](https://issues.apache.org/jira/browse/HDFS-7986) | Allow files / directories to be deleted from the NameNode UI |  Major | ui | Ravi Prakash | Ravi Prakash |
| [HDFS-7995](https://issues.apache.org/jira/browse/HDFS-7995) | Implement chmod in the HDFS Web UI |  Major | ui | Ravi Prakash | Ravi Prakash |
| [YARN-4034](https://issues.apache.org/jira/browse/YARN-4034) | Render cluster Max Priority in scheduler metrics in RM web UI |  Minor | resourcemanager, webapp | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-3901](https://issues.apache.org/jira/browse/YARN-3901) | Populate flow run data in the flow\_run & flow activity tables |  Major | timelineserver | Vrushali C | Vrushali C |
| [HDFS-9022](https://issues.apache.org/jira/browse/HDFS-9022) | Move NameNode.getAddress() and NameNode.getUri() to hadoop-hdfs-client |  Major | hdfs-client | Mingliang Liu | Mingliang Liu |
| [YARN-4171](https://issues.apache.org/jira/browse/YARN-4171) | Resolve findbugs/javac warnings in YARN-1197 branch |  Major | api, nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3212](https://issues.apache.org/jira/browse/YARN-3212) | RMNode State Transition Update with DECOMMISSIONING state |  Major | graceful, resourcemanager | Junping Du | Junping Du |
| [HDFS-9101](https://issues.apache.org/jira/browse/HDFS-9101) | Remove deprecated NameNode.getUri() static helper method |  Major | . | Mingliang Liu | Mingliang Liu |
| [HDFS-9004](https://issues.apache.org/jira/browse/HDFS-9004) | Add upgrade domain to DatanodeInfo |  Major | . | Ming Ma | Ming Ma |
| [HDFS-9111](https://issues.apache.org/jira/browse/HDFS-9111) | Move hdfs-client protobuf convert methods from PBHelper to PBHelperClient |  Major | . | Mingliang Liu | Mingliang Liu |
| [HDFS-9039](https://issues.apache.org/jira/browse/HDFS-9039) | Separate client and server side methods of o.a.h.hdfs.NameNodeProxies |  Major | . | Mingliang Liu | Mingliang Liu |
| [HDFS-8733](https://issues.apache.org/jira/browse/HDFS-8733) | Keep server related definition in hdfs.proto on server side |  Major | . | Yi Liu | Mingliang Liu |
| [HDFS-9131](https://issues.apache.org/jira/browse/HDFS-9131) | Move config keys used by hdfs-client to HdfsClientConfigKeys |  Major | . | Mingliang Liu | Mingliang Liu |
| [HADOOP-12244](https://issues.apache.org/jira/browse/HADOOP-12244) | recover broken rebase during precommit |  Critical | yetus | Allen Wittenauer | Allen Wittenauer |
| [HDFS-7529](https://issues.apache.org/jira/browse/HDFS-7529) | Consolidate encryption zone related implementation into a single class |  Major | . | Haohui Mai | Rakesh R |
| [HDFS-9134](https://issues.apache.org/jira/browse/HDFS-9134) | Move LEASE\_{SOFTLIMIT,HARDLIMIT}\_PERIOD constants from HdfsServerConstants to HdfsConstants |  Major | . | Mingliang Liu | Mingliang Liu |
| [YARN-4075](https://issues.apache.org/jira/browse/YARN-4075) | [reader REST API] implement support for querying for flows and flow runs |  Major | timelineserver | Sangjin Lee | Varun Saxena |
| [HADOOP-11918](https://issues.apache.org/jira/browse/HADOOP-11918) | Listing an empty s3a root directory throws FileNotFound. |  Minor | fs/s3 | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-8053](https://issues.apache.org/jira/browse/HDFS-8053) | Move DFSIn/OutputStream and related classes to hadoop-hdfs-client |  Major | build | Haohui Mai | Mingliang Liu |
| [HDFS-8740](https://issues.apache.org/jira/browse/HDFS-8740) | Move DistributedFileSystem to hadoop-hdfs-client |  Major | build | Yi Liu | Mingliang Liu |
| [YARN-4141](https://issues.apache.org/jira/browse/YARN-4141) | Runtime Application Priority change should not throw exception for applications at finishing states |  Major | resourcemanager | Sunil G | Sunil G |
| [HDFS-9165](https://issues.apache.org/jira/browse/HDFS-9165) | Move entries in META-INF/services/o.a.h.fs.FileSystem to hdfs-client |  Major | build | Haohui Mai | Mingliang Liu |
| [HDFS-8971](https://issues.apache.org/jira/browse/HDFS-8971) | Remove guards when calling LOG.debug() and LOG.trace() in client package |  Major | build | Mingliang Liu | Mingliang Liu |
| [YARN-4203](https://issues.apache.org/jira/browse/YARN-4203) | Add request/response logging & timing for each REST endpoint call |  Minor | timelineserver | Vrushali C | Varun Saxena |
| [YARN-4210](https://issues.apache.org/jira/browse/YARN-4210) | HBase reader throws NPE if Get returns no rows |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [HDFS-9015](https://issues.apache.org/jira/browse/HDFS-9015) | Refactor TestReplicationPolicy to test different block placement policies |  Major | . | Ming Ma | Ming Ma |
| [YARN-1897](https://issues.apache.org/jira/browse/YARN-1897) | CLI and core support for signal container functionality |  Major | api | Ming Ma | Ming Ma |
| [HDFS-9158](https://issues.apache.org/jira/browse/HDFS-9158) | [OEV-Doc] : Document does not mention about "-f" and "-r" options |  Major | . | nijel | nijel |
| [HDFS-9155](https://issues.apache.org/jira/browse/HDFS-9155) | OEV should treat .XML files as XML even when the file name extension is uppercase |  Major | . | nijel | nijel |
| [YARN-3864](https://issues.apache.org/jira/browse/YARN-3864) | Implement support for querying single app and all apps for a flow run |  Blocker | timelineserver | Varun Saxena | Varun Saxena |
| [HDFS-9180](https://issues.apache.org/jira/browse/HDFS-9180) | Update excluded DataNodes in DFSStripedOutputStream based on failures in data streamers |  Major | erasure-coding | Jing Zhao | Jing Zhao |
| [YARN-4215](https://issues.apache.org/jira/browse/YARN-4215) | RMNodeLabels Manager Need to verify and replace node labels for the only modified Node Label Mappings in the request |  Major | resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [YARN-4178](https://issues.apache.org/jira/browse/YARN-4178) | [storage implementation] app id as string in row keys can cause incorrect ordering |  Major | timelineserver | Sangjin Lee | Varun Saxena |
| [HDFS-9206](https://issues.apache.org/jira/browse/HDFS-9206) | Inconsistent default value of dfs.datanode.stripedread.buffer.size |  Minor | erasure-coding | Walter Su | Walter Su |
| [HDFS-9170](https://issues.apache.org/jira/browse/HDFS-9170) | Move libhdfs / fuse-dfs / libwebhdfs to hdfs-client |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-9159](https://issues.apache.org/jira/browse/HDFS-9159) | [OIV] : return value of the command is not correct if invalid value specified in "-p (processor)" option |  Major | . | nijel | nijel |
| [HDFS-9209](https://issues.apache.org/jira/browse/HDFS-9209) | Erasure coding: Add apache license header in TestFileStatusWithECPolicy.java |  Major | . | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-8632](https://issues.apache.org/jira/browse/HDFS-8632) | Add InterfaceAudience annotation to the erasure coding classes |  Major | . | Rakesh R | Rakesh R |
| [YARN-4140](https://issues.apache.org/jira/browse/YARN-4140) | RM container allocation delayed incase of app submitted to Nodelabel partition |  Major | scheduler | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3964](https://issues.apache.org/jira/browse/YARN-3964) | Support NodeLabelsProvider at Resource Manager side |  Major | . | Dian Fu | Dian Fu |
| [HADOOP-10300](https://issues.apache.org/jira/browse/HADOOP-10300) | Allowed deferred sending of call responses |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [YARN-4230](https://issues.apache.org/jira/browse/YARN-4230) | Increasing container resource while there is no headroom left will cause ResourceManager to crash |  Critical | resourcemanager | MENG DING | MENG DING |
| [HDFS-9006](https://issues.apache.org/jira/browse/HDFS-9006) | Provide BlockPlacementPolicy that supports upgrade domain |  Major | . | Ming Ma | Ming Ma |
| [HDFS-9160](https://issues.apache.org/jira/browse/HDFS-9160) | [OIV-Doc] : Missing details of "delimited" for processor options |  Major | . | nijel | nijel |
| [HDFS-8438](https://issues.apache.org/jira/browse/HDFS-8438) | Erasure Coding: Allow concat striped files if they have the same ErasureCodingPolicy |  Major | . | Walter Su | Walter Su |
| [HDFS-9167](https://issues.apache.org/jira/browse/HDFS-9167) | Update pom.xml in other modules to depend on hdfs-client instead of hdfs |  Major | build | Haohui Mai | Mingliang Liu |
| [YARN-4255](https://issues.apache.org/jira/browse/YARN-4255) | container-executor does not clean up docker operation command files. |  Minor | . | Sidharta Seethana | Sidharta Seethana |
| [HDFS-9223](https://issues.apache.org/jira/browse/HDFS-9223) | Code cleanup for DatanodeDescriptor and HeartbeatManager |  Minor | namenode | Jing Zhao | Jing Zhao |
| [YARN-4258](https://issues.apache.org/jira/browse/YARN-4258) | Add support for controlling capabilities for docker containers |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [HDFS-9157](https://issues.apache.org/jira/browse/HDFS-9157) | [OEV and OIV] : Unnecessary parsing for mandatory arguements if "-h" option is specified as the only option |  Major | . | nijel | nijel |
| [YARN-4237](https://issues.apache.org/jira/browse/YARN-4237) | Support additional queries for ATSv2 Web UI |  Major | . | Varun Saxena | Varun Saxena |
| [HADOOP-12475](https://issues.apache.org/jira/browse/HADOOP-12475) | Replace guava Cache with ConcurrentHashMap for caching Connection in ipc Client |  Major | conf, io, ipc | Walter Su | Walter Su |
| [YARN-4162](https://issues.apache.org/jira/browse/YARN-4162) | CapacityScheduler: Add resource usage by partition and queue capacity by partition to REST API |  Major | api, client, resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [YARN-4170](https://issues.apache.org/jira/browse/YARN-4170) | AM need to be notified with priority in AllocateResponse |  Major | resourcemanager | Sunil G | Sunil G |
| [YARN-2556](https://issues.apache.org/jira/browse/YARN-2556) | Tool to measure the performance of the timeline server |  Major | timelineserver | Jonathan Eagles | Chang Li |
| [YARN-4262](https://issues.apache.org/jira/browse/YARN-4262) | Allow whitelisted users to run privileged docker containers. |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [YARN-4267](https://issues.apache.org/jira/browse/YARN-4267) | Add additional logging to container launch implementations in container-executor |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [YARN-3985](https://issues.apache.org/jira/browse/YARN-3985) | Make ReservationSystem persist state using RMStateStore reservation APIs |  Major | resourcemanager | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-2513](https://issues.apache.org/jira/browse/YARN-2513) | Host framework UIs in YARN for use with the ATS |  Major | timelineserver | Jonathan Eagles | Jonathan Eagles |
| [YARN-3739](https://issues.apache.org/jira/browse/YARN-3739) | Add reservation system recovery to RM recovery process |  Major | capacityscheduler, fairscheduler, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-4243](https://issues.apache.org/jira/browse/YARN-4243) | Add retry on establishing Zookeeper conenction in EmbeddedElectorService#serviceInit |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-4179](https://issues.apache.org/jira/browse/YARN-4179) | [reader implementation] support flow activity queries based on time |  Minor | timelineserver | Sangjin Lee | Varun Saxena |
| [YARN-4129](https://issues.apache.org/jira/browse/YARN-4129) | Refactor the SystemMetricPublisher in RM to better support newer events |  Major | resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [YARN-4221](https://issues.apache.org/jira/browse/YARN-4221) | Store user in app to flow table |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-3738](https://issues.apache.org/jira/browse/YARN-3738) | Add support for recovery of reserved apps running under dynamic queues |  Major | capacityscheduler, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-3724](https://issues.apache.org/jira/browse/YARN-3724) | Use POSIX nftw(3) instead of fts(3) |  Major | . | Malcolm Kavalsky | Alan Burlison |
| [YARN-2729](https://issues.apache.org/jira/browse/YARN-2729) | Support script based NodeLabelsProvider Interface in Distributed Node Label Configuration Setup |  Major | nodemanager | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9304](https://issues.apache.org/jira/browse/HDFS-9304) | Add HdfsClientConfigKeys class to TestHdfsConfigFields#configurationClasses |  Major | build | Mingliang Liu | Mingliang Liu |
| [YARN-3216](https://issues.apache.org/jira/browse/YARN-3216) | Max-AM-Resource-Percentage should respect node labels |  Critical | resourcemanager | Wangda Tan | Sunil G |
| [HADOOP-12457](https://issues.apache.org/jira/browse/HADOOP-12457) | [JDK8] Fix a failure of compiling common by javadoc |  Major | . | Tsuyoshi Ozawa | Akira Ajisaka |
| [MAPREDUCE-6391](https://issues.apache.org/jira/browse/MAPREDUCE-6391) | util/Timer.cc completely misunderstands \_POSIX\_CPUTIME |  Major | build | Alan Burlison | Alan Burlison |
| [MAPREDUCE-6412](https://issues.apache.org/jira/browse/MAPREDUCE-6412) | Make hadoop-mapreduce-client Native code -Wall-clean |  Major | build | Alan Burlison | Alan Burlison |
| [MAPREDUCE-6416](https://issues.apache.org/jira/browse/MAPREDUCE-6416) | Not all platforms have d\_type in struct dirent |  Major | build | Alan Burlison | Alan Burlison |
| [MAPREDUCE-6435](https://issues.apache.org/jira/browse/MAPREDUCE-6435) | MapReduce client assumes the world is x86 |  Major | client | Alan Burlison | Alan Burlison |
| [HADOOP-12040](https://issues.apache.org/jira/browse/HADOOP-12040) | Adjust inputs order for the decode API in raw erasure coder |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-9168](https://issues.apache.org/jira/browse/HDFS-9168) | Move client side unit test to hadoop-hdfs-client |  Major | build | Haohui Mai | Haohui Mai |
| [HADOOP-12327](https://issues.apache.org/jira/browse/HADOOP-12327) | Initialize output buffers with ZERO bytes in erasure coder |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-9261](https://issues.apache.org/jira/browse/HDFS-9261) | Erasure Coding: Skip encoding the data cells if all the parity data streamers are failed for the current block group |  Minor | . | Rakesh R | Rakesh R |
| [HDFS-9343](https://issues.apache.org/jira/browse/HDFS-9343) | Empty caller context considered invalid |  Major | . | Mingliang Liu | Mingliang Liu |
| [HADOOP-12047](https://issues.apache.org/jira/browse/HADOOP-12047) | Indicate preference not to affect input buffers during coding in erasure coder |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-8777](https://issues.apache.org/jira/browse/HDFS-8777) | Erasure Coding: add tests for taking snapshots on EC files |  Major | erasure-coding | Jing Zhao | Rakesh R |
| [HDFS-9275](https://issues.apache.org/jira/browse/HDFS-9275) | Wait previous ErasureCodingWork to finish before schedule another one |  Major | . | Walter Su | Walter Su |
| [HDFS-9362](https://issues.apache.org/jira/browse/HDFS-9362) | TestAuditLogger#testAuditLoggerWithCallContext assumes Unix line endings, fails on Windows. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HADOOP-12544](https://issues.apache.org/jira/browse/HADOOP-12544) | Erasure Coding: create dummy raw coder to isolate performance issues in testing |  Major | . | Rui Li | Rui Li |
| [HDFS-9007](https://issues.apache.org/jira/browse/HDFS-9007) | Fix HDFS Balancer to honor upgrade domain policy |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-11887](https://issues.apache.org/jira/browse/HADOOP-11887) | Introduce Intel ISA-L erasure coding library for native erasure encoding support |  Major | io | Kai Zheng | Kai Zheng |
| [HADOOP-11684](https://issues.apache.org/jira/browse/HADOOP-11684) | S3a to use thread pool that blocks clients |  Major | fs/s3 | Thomas Demoor | Thomas Demoor |
| [HDFS-9379](https://issues.apache.org/jira/browse/HDFS-9379) | Make NNThroughputBenchmark$BlockReportStats support more than 10 datanodes |  Major | test | Mingliang Liu | Mingliang Liu |
| [YARN-1510](https://issues.apache.org/jira/browse/YARN-1510) | Make NMClient support change container resources |  Major | nodemanager | Wangda Tan (No longer used) | MENG DING |
| [YARN-4345](https://issues.apache.org/jira/browse/YARN-4345) | yarn rmadmin -updateNodeResource doesn't work |  Critical | graceful, resourcemanager | Sushmitha Sreenivasan | Junping Du |
| [HDFS-8968](https://issues.apache.org/jira/browse/HDFS-8968) | Erasure coding: a comprehensive I/O throughput benchmark tool |  Major | erasure-coding, test | Kai Zheng | Rui Li |
| [YARN-1509](https://issues.apache.org/jira/browse/YARN-1509) | Make AMRMClient support send increase container request and get increased/decreased containers |  Major | resourcemanager | Wangda Tan (No longer used) | MENG DING |
| [HDFS-9387](https://issues.apache.org/jira/browse/HDFS-9387) | Fix namenodeUri parameter parsing in NNThroughputBenchmark |  Major | test | Mingliang Liu | Mingliang Liu |
| [HDFS-9421](https://issues.apache.org/jira/browse/HDFS-9421) | NNThroughputBenchmark replication test NPE with -namenode option |  Major | benchmarks | Xiaoyu Yao | Mingliang Liu |
| [YARN-4184](https://issues.apache.org/jira/browse/YARN-4184) | Remove update reservation state api from state store as its not used by ReservationSystem |  Major | capacityscheduler, fairscheduler, resourcemanager | Anubhav Dhoot | Sean Po |
| [HADOOP-12582](https://issues.apache.org/jira/browse/HADOOP-12582) | Using BytesWritable's getLength() and getBytes() instead of get() and getSize() |  Major | . | Tsuyoshi Ozawa | Akira Ajisaka |
| [YARN-4053](https://issues.apache.org/jira/browse/YARN-4053) | Change the way metric values are stored in HBase Storage |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-3454](https://issues.apache.org/jira/browse/YARN-3454) | Add efficient merge operation to RLESparseResourceAllocation |  Major | resourcemanager | Carlo Curino | Carlo Curino |
| [HDFS-9348](https://issues.apache.org/jira/browse/HDFS-9348) | Erasure Coding: DFS GetErasureCodingPolicy API on a non-existent file should be handled properly |  Minor | erasure-coding | Rakesh R | Rakesh R |
| [HDFS-7796](https://issues.apache.org/jira/browse/HDFS-7796) | Include X-editable for slick contenteditable fields in the web UI |  Major | ui | Ravi Prakash | Ravi Prakash |
| [HDFS-9433](https://issues.apache.org/jira/browse/HDFS-9433) | DFS getEZForPath API on a non-existent file should throw FileNotFoundException |  Major | encryption | Rakesh R | Rakesh R |
| [YARN-3980](https://issues.apache.org/jira/browse/YARN-3980) | Plumb resource-utilization info in node heartbeat through to the scheduler |  Major | resourcemanager, scheduler | Karthik Kambatla | Inigo Goiri |
| [HADOOP-11954](https://issues.apache.org/jira/browse/HADOOP-11954) | Solaris does not support RLIMIT\_MEMLOCK as in Linux |  Major | . | Malcolm Kavalsky | Alan Burlison |
| [YARN-4384](https://issues.apache.org/jira/browse/YARN-4384) | updateNodeResource CLI should not accept negative values for resource |  Major | graceful, resourcemanager | Sushmitha Sreenivasan | Junping Du |
| [HDFS-9438](https://issues.apache.org/jira/browse/HDFS-9438) | TestPipelinesFailover assumes Linux ifconfig |  Minor | test | Alan Burlison | John Zhuge |
| [YARN-4297](https://issues.apache.org/jira/browse/YARN-4297) | TestJobHistoryEventHandler and TestRMContainerAllocator failing on YARN-2928 branch |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-3862](https://issues.apache.org/jira/browse/YARN-3862) | Support for fetching specific configs and metrics based on prefixes |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-4292](https://issues.apache.org/jira/browse/YARN-4292) | ResourceUtilization should be a part of NodeInfo REST API |  Major | . | Wangda Tan | Sunil G |
| [HDFS-9436](https://issues.apache.org/jira/browse/HDFS-9436) | Make NNThroughputBenchmark$BlockReportStats run with 10 datanodes by default |  Minor | test | Mingliang Liu | Mingliang Liu |
| [HDFS-9484](https://issues.apache.org/jira/browse/HDFS-9484) | NNThroughputBenchmark$BlockReportStats should not send empty block reports |  Major | test | Mingliang Liu | Mingliang Liu |
| [YARN-4405](https://issues.apache.org/jira/browse/YARN-4405) | Support node label store in non-appendable file system |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [HDFS-9214](https://issues.apache.org/jira/browse/HDFS-9214) | Support reconfiguring dfs.datanode.balance.max.concurrent.moves without DN restart |  Major | datanode | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-9129](https://issues.apache.org/jira/browse/HDFS-9129) | Move the safemode block count into BlockManager |  Major | namenode | Haohui Mai | Mingliang Liu |
| [YARN-4248](https://issues.apache.org/jira/browse/YARN-4248) | REST API for submit/update/delete Reservations |  Major | resourcemanager | Carlo Curino | Carlo Curino |
| [HDFS-9414](https://issues.apache.org/jira/browse/HDFS-9414) | Refactor reconfiguration of ClientDatanodeProtocol for reusability |  Major | . | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-4356](https://issues.apache.org/jira/browse/YARN-4356) | ensure the timeline service v.2 is disabled cleanly and has no impact when it's turned off |  Critical | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-4358](https://issues.apache.org/jira/browse/YARN-4358) | Improve relationship between SharingPolicy and ReservationAgent |  Major | capacityscheduler, fairscheduler, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-4450](https://issues.apache.org/jira/browse/YARN-4450) | TestTimelineAuthenticationFilter and TestYarnConfigurationFields fail |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-3946](https://issues.apache.org/jira/browse/YARN-3946) | Update exact reason as to why a submitted app is in ACCEPTED state to app's diagnostic message |  Major | capacity scheduler, resourcemanager | Sumit Nigam | Naganarasimha G R |
| [YARN-4309](https://issues.apache.org/jira/browse/YARN-4309) | Add container launch related debug information to container logs when a container fails |  Major | nodemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-9371](https://issues.apache.org/jira/browse/HDFS-9371) | Code cleanup for DatanodeManager |  Major | namenode | Jing Zhao | Jing Zhao |
| [YARN-4460](https://issues.apache.org/jira/browse/YARN-4460) | [Bug fix] RM fails to start when SMP is enabled |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-4445](https://issues.apache.org/jira/browse/YARN-4445) | Unify the term flowId and flowName in timeline v2 codebase |  Major | timelineserver | Li Lu | Zhan Zhang |
| [YARN-4293](https://issues.apache.org/jira/browse/YARN-4293) | ResourceUtilization should be a part of yarn node CLI |  Major | . | Wangda Tan | Sunil G |
| [YARN-4416](https://issues.apache.org/jira/browse/YARN-4416) | Deadlock due to synchronised get Methods in AbstractCSQueue |  Minor | capacity scheduler, resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [YARN-1856](https://issues.apache.org/jira/browse/YARN-1856) | cgroups based memory monitoring for containers |  Major | nodemanager | Karthik Kambatla | Varun Vasudev |
| [HDFS-9373](https://issues.apache.org/jira/browse/HDFS-9373) | Erasure coding: friendly log information for write operations with some failed streamers |  Major | erasure-coding | Li Bo | Li Bo |
| [YARN-3226](https://issues.apache.org/jira/browse/YARN-3226) | UI changes for decommissioning node |  Major | graceful | Junping Du | Sunil G |
| [YARN-4164](https://issues.apache.org/jira/browse/YARN-4164) | Retrospect update ApplicationPriority API return type |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-4350](https://issues.apache.org/jira/browse/YARN-4350) | TestDistributedShell fails for V2 scenarios |  Major | timelineserver | Sangjin Lee | Naganarasimha G R |
| [HDFS-9173](https://issues.apache.org/jira/browse/HDFS-9173) | Erasure Coding: Lease recovery for striped file |  Major | erasure-coding | Walter Su | Walter Su |
| [HDFS-9575](https://issues.apache.org/jira/browse/HDFS-9575) | Use byte array for internal block indices in a striped block |  Major | erasure-coding | Jing Zhao | Jing Zhao |
| [YARN-3586](https://issues.apache.org/jira/browse/YARN-3586) | RM only get back addresses of Collectors that NM needs to know. |  Critical | resourcemanager, timelineserver | Junping Du | Junping Du |
| [YARN-4234](https://issues.apache.org/jira/browse/YARN-4234) | New put APIs in TimelineClient for ats v1.5 |  Major | timelineserver | Xuan Gong | Xuan Gong |
| [YARN-2882](https://issues.apache.org/jira/browse/YARN-2882) | Add an OPPORTUNISTIC ExecutionType |  Major | nodemanager | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-3480](https://issues.apache.org/jira/browse/YARN-3480) | Recovery may get very slow with lots of services with lots of app-attempts |  Major | resourcemanager | Jun Gong | Jun Gong |
| [YARN-4098](https://issues.apache.org/jira/browse/YARN-4098) | Document ApplicationPriority feature |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-7779](https://issues.apache.org/jira/browse/HDFS-7779) | Support changing ownership, group and replication in HDFS Web UI |  Major | ui | Ravi Prakash | Ravi Prakash |
| [HADOOP-12685](https://issues.apache.org/jira/browse/HADOOP-12685) | Input buffer position after encode/decode not consistent between different kinds of buffers |  Minor | . | Rui Li | Rui Li |
| [HDFS-9498](https://issues.apache.org/jira/browse/HDFS-9498) | Move code that tracks blocks with future generation stamps to BlockManagerSafeMode |  Major | namenode | Mingliang Liu | Mingliang Liu |
| [YARN-2902](https://issues.apache.org/jira/browse/YARN-2902) | Killing a container that is localizing can orphan resources in the DOWNLOADING state |  Major | nodemanager | Jason Lowe | Varun Saxena |
| [YARN-4393](https://issues.apache.org/jira/browse/YARN-4393) | TestResourceLocalizationService#testFailedDirsResourceRelease fails intermittently |  Major | test | Varun Saxena | Varun Saxena |
| [YARN-4550](https://issues.apache.org/jira/browse/YARN-4550) | some tests in TestContainerLanch fails on non-english locale environment |  Minor | nodemanager, test | Takashi Ohnishi | Takashi Ohnishi |
| [YARN-4335](https://issues.apache.org/jira/browse/YARN-4335) | Allow ResourceRequests to specify ExecutionType of a request ask |  Major | nodemanager, resourcemanager | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-4479](https://issues.apache.org/jira/browse/YARN-4479) | Retrospect app-priority in pendingOrderingPolicy during recovering applications |  Major | api, resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-3995](https://issues.apache.org/jira/browse/YARN-3995) | Some of the NM events are not getting published due race condition when AM container finishes in NM |  Major | nodemanager, timelineserver | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9621](https://issues.apache.org/jira/browse/HDFS-9621) | getListing wrongly associates Erasure Coding policy to pre-existing replicated files under an EC directory |  Critical | erasure-coding | Sushmitha Sreenivasan | Jing Zhao |
| [YARN-4537](https://issues.apache.org/jira/browse/YARN-4537) | Pull out priority comparison from fifocomparator and use compound comparator for FifoOrdering policy |  Major | capacity scheduler | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11262](https://issues.apache.org/jira/browse/HADOOP-11262) | Enable YARN to use S3A |  Major | fs/s3 | Thomas Demoor | Pieter Reuse |
| [YARN-4553](https://issues.apache.org/jira/browse/YARN-4553) | Add cgroups support for docker containers |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [YARN-4265](https://issues.apache.org/jira/browse/YARN-4265) | Provide new timeline plugin storage to support fine-grained entity caching |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-4304](https://issues.apache.org/jira/browse/YARN-4304) | AM max resource configuration per partition to be displayed/updated correctly in UI and in various partition related metrics |  Major | webapp | Sunil G | Sunil G |
| [YARN-4526](https://issues.apache.org/jira/browse/YARN-4526) | Make SystemClock singleton so AppSchedulingInfo could use it |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-4557](https://issues.apache.org/jira/browse/YARN-4557) | Fix improper Queues sorting in PartitionedQueueComparator when accessible-node-labels=\* |  Major | resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [HADOOP-11828](https://issues.apache.org/jira/browse/HADOOP-11828) | Implement the Hitchhiker erasure coding algorithm |  Major | . | Zhe Zhang | jack liuquan |
| [HDFS-9542](https://issues.apache.org/jira/browse/HDFS-9542) | Move BlockIdManager from FSNamesystem to BlockManager |  Major | namenode | Jing Zhao | Jing Zhao |
| [YARN-4622](https://issues.apache.org/jira/browse/YARN-4622) | TestDistributedShell fails for v2 test cases after modifications for 1.5 |  Major | timelineserver | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9601](https://issues.apache.org/jira/browse/HDFS-9601) | NNThroughputBenchmark.BlockReportStats should handle NotReplicatedYetException on adding block |  Major | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-4578](https://issues.apache.org/jira/browse/YARN-4578) | Directories that are mounted in docker containers need to be more restrictive/container-specific |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [YARN-4574](https://issues.apache.org/jira/browse/YARN-4574) | TestAMRMClientOnRMRestart fails on trunk |  Major | client, test | Takashi Ohnishi | Takashi Ohnishi |
| [HDFS-9646](https://issues.apache.org/jira/browse/HDFS-9646) | ErasureCodingWorker may fail when recovering data blocks with length less than the first internal block |  Critical | erasure-coding | Takuya Fukudome | Jing Zhao |
| [YARN-4614](https://issues.apache.org/jira/browse/YARN-4614) | TestApplicationPriority#testApplicationPriorityAllocationWithChangeInPriority fails occasionally |  Major | test | Jason Lowe | Sunil G |
| [YARN-4613](https://issues.apache.org/jira/browse/YARN-4613) | TestClientRMService#testGetClusterNodes fails occasionally |  Major | test | Jason Lowe | Takashi Ohnishi |
| [HDFS-9094](https://issues.apache.org/jira/browse/HDFS-9094) | Add command line option to ask NameNode reload configuration. |  Major | namenode | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-9672](https://issues.apache.org/jira/browse/HDFS-9672) | o.a.h.hdfs.TestLeaseRecovery2 fails intermittently |  Major | test | Mingliang Liu | Mingliang Liu |
| [YARN-4573](https://issues.apache.org/jira/browse/YARN-4573) | TestRMAppTransitions.testAppRunningKill and testAppKilledKilled fail on trunk |  Major | resourcemanager, test | Takashi Ohnishi | Takashi Ohnishi |
| [YARN-4238](https://issues.apache.org/jira/browse/YARN-4238) | correctly set createdTime and remove modifiedTime when publishing entities |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-4224](https://issues.apache.org/jira/browse/YARN-4224) | Support fetching entities by UID and change the REST interface to conform to current REST APIs' in YARN |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-4644](https://issues.apache.org/jira/browse/YARN-4644) | TestRMRestart fails and findbugs issue in YARN-2928 branch |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-4633](https://issues.apache.org/jira/browse/YARN-4633) | TestRMRestart.testRMRestartAfterPreemption fails intermittently in trunk |  Major | test | Rohith Sharma K S | Bibin A Chundatt |
| [YARN-4643](https://issues.apache.org/jira/browse/YARN-4643) | Container recovery is broken with delegating container runtime |  Critical | yarn | Sidharta Seethana | Sidharta Seethana |
| [YARN-4219](https://issues.apache.org/jira/browse/YARN-4219) | New levelDB cache storage for timeline v1.5 |  Major | . | Li Lu | Li Lu |
| [YARN-4543](https://issues.apache.org/jira/browse/YARN-4543) | TestNodeStatusUpdater.testStopReentrant fails + JUnit misusage |  Minor | nodemanager | Akihiro Suda | Akihiro Suda |
| [YARN-4615](https://issues.apache.org/jira/browse/YARN-4615) | TestAbstractYarnScheduler#testResourceRequestRecoveryToTheRightAppAttempt fails occasionally |  Major | test | Jason Lowe | Sunil G |
| [HDFS-9494](https://issues.apache.org/jira/browse/HDFS-9494) | Parallel optimization of DFSStripedOutputStream#flushAllInternals( ) |  Minor | hdfs-client | Rui Gao | Rui Gao |
| [YARN-4340](https://issues.apache.org/jira/browse/YARN-4340) | Add "list" API to reservation system |  Major | capacityscheduler, fairscheduler, resourcemanager | Carlo Curino | Sean Po |
| [YARN-4100](https://issues.apache.org/jira/browse/YARN-4100) | Add Documentation for Distributed and Delegated-Centralized Node Labels feature |  Major | api, client, resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9731](https://issues.apache.org/jira/browse/HDFS-9731) | Erasure Coding: Rename BlockECRecoveryCommand to BlockECReconstructionCommand |  Major | erasure-coding | Rakesh R | Rakesh R |
| [HDFS-9403](https://issues.apache.org/jira/browse/HDFS-9403) | Erasure coding: some EC tests are missing timeout |  Minor | erasure-coding, test | Zhe Zhang | Rui Gao |
| [HDFS-9658](https://issues.apache.org/jira/browse/HDFS-9658) | Erasure Coding: allow to use multiple EC policies in striping related tests |  Major | . | Rui Li | Rui Li |
| [HADOOP-12041](https://issues.apache.org/jira/browse/HADOOP-12041) | Implement another Reed-Solomon coder in pure Java |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-9503](https://issues.apache.org/jira/browse/HDFS-9503) | Replace -namenode option with -fs for NNThroughputBenchmark |  Major | test | Konstantin Shvachko | Mingliang Liu |
| [YARN-4446](https://issues.apache.org/jira/browse/YARN-4446) | Refactor reader API for better extensibility |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [HADOOP-12292](https://issues.apache.org/jira/browse/HADOOP-12292) | Make use of DeleteObjects optional |  Major | fs/s3 | Thomas Demoor | Thomas Demoor |
| [YARN-4667](https://issues.apache.org/jira/browse/YARN-4667) | RM Admin CLI for refreshNodesResources throws NPE when nothing is configured |  Critical | client | Naganarasimha G R | Naganarasimha G R |
| [HADOOP-12752](https://issues.apache.org/jira/browse/HADOOP-12752) | Improve diagnostics/use of envvar/sysprop credential propagation |  Minor | security | Steve Loughran | Steve Loughran |
| [YARN-4409](https://issues.apache.org/jira/browse/YARN-4409) | Fix javadoc and checkstyle issues in timelineservice code |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-3367](https://issues.apache.org/jira/browse/YARN-3367) | Replace starting a separate thread for post entity with event loop in TimelineClient |  Major | timelineserver | Junping Du | Naganarasimha G R |
| [HDFS-9775](https://issues.apache.org/jira/browse/HDFS-9775) | Erasure Coding : Rename BlockRecoveryWork to BlockReconstructionWork |  Major | namenode | Rakesh R | Rakesh R |
| [YARN-4138](https://issues.apache.org/jira/browse/YARN-4138) | Roll back container resource allocation after resource increase token expires |  Major | api, nodemanager, resourcemanager | MENG DING | MENG DING |
| [HDFS-9789](https://issues.apache.org/jira/browse/HDFS-9789) | Correctly update DataNode's scheduled block size when writing small EC file |  Major | namenode | Namit Maheshwari | Jing Zhao |
| [HDFS-9755](https://issues.apache.org/jira/browse/HDFS-9755) | Erasure Coding: allow to use multiple EC policies in striping related tests [Part 2] |  Major | erasure-coding, test | Rui Li | Rui Li |
| [YARN-2575](https://issues.apache.org/jira/browse/YARN-2575) | Create separate ACLs for Reservation create/update/delete/list ops |  Major | capacityscheduler, fairscheduler, resourcemanager | Subru Krishnan | Sean Po |
| [YARN-4684](https://issues.apache.org/jira/browse/YARN-4684) | TestYarnCLI#testGetContainers failing in CN locale |  Major | yarn | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9794](https://issues.apache.org/jira/browse/HDFS-9794) | Streamer threads may leak if failure happens when closing the striped outputstream |  Critical | hdfs-client | Namit Maheshwari | Jing Zhao |
| [HDFS-9754](https://issues.apache.org/jira/browse/HDFS-9754) | Avoid unnecessary getBlockCollection calls in BlockManager |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-9691](https://issues.apache.org/jira/browse/HDFS-9691) | TestBlockManagerSafeMode#testCheckSafeMode fails intermittently |  Major | test | Mingliang Liu | Mingliang Liu |
| [HADOOP-12710](https://issues.apache.org/jira/browse/HADOOP-12710) | Remove dependency on commons-httpclient for TestHttpServerLogs |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9830](https://issues.apache.org/jira/browse/HDFS-9830) | Remove references to hftp in ViewFs documentation in trunk |  Minor | documentation, fs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9829](https://issues.apache.org/jira/browse/HDFS-9829) | Erasure Coding: Improve few exception handling logic of ErasureCodingWorker |  Minor | erasure-coding | Rakesh R | Rakesh R |
| [HDFS-9818](https://issues.apache.org/jira/browse/HDFS-9818) | Correctly handle EC reconstruction work caused by not enough racks |  Major | datanode, namenode | Takuya Fukudome | Jing Zhao |
| [HADOOP-11613](https://issues.apache.org/jira/browse/HADOOP-11613) | Remove commons-httpclient dependency from hadoop-azure |  Major | . | Akira Ajisaka | Masatake Iwasaki |
| [HDFS-9084](https://issues.apache.org/jira/browse/HDFS-9084) | Pagination, sorting and filtering of files/directories in the HDFS Web UI |  Major | ui | Ravi Prakash | Ravi Prakash |
| [YARN-3223](https://issues.apache.org/jira/browse/YARN-3223) | Resource update during NM graceful decommission |  Major | graceful, nodemanager, resourcemanager | Junping Du | Brook Zhou |
| [HDFS-9816](https://issues.apache.org/jira/browse/HDFS-9816) | Erasure Coding: allow to use multiple EC policies in striping related tests [Part 3] |  Major | erasure-coding, test | Rui Li | Rui Li |
| [YARN-4680](https://issues.apache.org/jira/browse/YARN-4680) | TimerTasks leak in ATS V1.5 Writer |  Major | timelineserver | Xuan Gong | Xuan Gong |
| [HADOOP-12808](https://issues.apache.org/jira/browse/HADOOP-12808) | Rename the RS coder from HDFS-RAID as legacy |  Major | . | Rui Li | Rui Li |
| [HDFS-9837](https://issues.apache.org/jira/browse/HDFS-9837) | BlockManager#countNodes should be able to detect duplicated internal blocks |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-9866](https://issues.apache.org/jira/browse/HDFS-9866) | BlockManager#chooseExcessReplicasStriped may weaken rack fault tolerance |  Major | namenode | Takuya Fukudome | Jing Zhao |
| [HDFS-9867](https://issues.apache.org/jira/browse/HDFS-9867) | Missing block exception should carry locatedBlocks information |  Minor | erasure-coding, hdfs-client | Mingliang Liu | Mingliang Liu |
| [YARN-4566](https://issues.apache.org/jira/browse/YARN-4566) | TestMiniYarnClusterNodeUtilization sometimes fails on trunk |  Major | test | Takashi Ohnishi | Takashi Ohnishi |
| [HADOOP-12813](https://issues.apache.org/jira/browse/HADOOP-12813) | Migrate TestRPC and related codes to rebase on ProtobufRpcEngine |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-7964](https://issues.apache.org/jira/browse/HDFS-7964) | Add support for async edit logging |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-9876](https://issues.apache.org/jira/browse/HDFS-9876) | shouldProcessOverReplicated should not count number of pending replicas |  Major | erasure-coding, namenode | Takuya Fukudome | Jing Zhao |
| [YARN-4700](https://issues.apache.org/jira/browse/YARN-4700) | ATS storage has one extra record each time the RM got restarted |  Major | timelineserver | Li Lu | Naganarasimha G R |
| [YARN-4749](https://issues.apache.org/jira/browse/YARN-4749) | Generalize config file handling in container-executor |  Major | nodemanager | Sidharta Seethana | Sidharta Seethana |
| [HDFS-8786](https://issues.apache.org/jira/browse/HDFS-8786) | Erasure coding: use simple replication for internal blocks on decommissioning datanodes |  Major | . | Zhe Zhang | Rakesh R |
| [MAPREDUCE-6546](https://issues.apache.org/jira/browse/MAPREDUCE-6546) | reconcile the two versions of the timeline service performance tests |  Minor | . | Sangjin Lee | Sangjin Lee |
| [HDFS-7866](https://issues.apache.org/jira/browse/HDFS-7866) | Erasure coding: NameNode manages multiple erasure coding policies |  Major | . | Kai Zheng | Rui Li |
| [YARN-4696](https://issues.apache.org/jira/browse/YARN-4696) | Improving EntityGroupFSTimelineStore on exception handling, test setup, and concurrency |  Major | timelineserver | Steve Loughran | Steve Loughran |
| [HDFS-1477](https://issues.apache.org/jira/browse/HDFS-1477) | Support reconfiguring dfs.heartbeat.interval and dfs.namenode.heartbeat.recheck-interval without NN restart |  Major | namenode | Patrick Kling | Xiaobing Zhou |
| [MAPREDUCE-6520](https://issues.apache.org/jira/browse/MAPREDUCE-6520) | Migrate MR Client test cases part 1 |  Trivial | test | Dustin Cote | Dustin Cote |
| [HADOOP-11996](https://issues.apache.org/jira/browse/HADOOP-11996) | Improve and restructure native ISAL support |  Major | io | Kai Zheng | Kai Zheng |
| [YARN-4545](https://issues.apache.org/jira/browse/YARN-4545) | Allow YARN distributed shell to use ATS v1.5 APIs |  Major | timelineserver | Li Lu | Li Lu |
| [HADOOP-12826](https://issues.apache.org/jira/browse/HADOOP-12826) | Rename the new Java coder and make it default |  Major | . | Rui Li | Rui Li |
| [YARN-4817](https://issues.apache.org/jira/browse/YARN-4817) | Change Log Level to DEBUG for putDomain call in ATS 1.5 |  Trivial | timelineserver | Xuan Gong | Xuan Gong |
| [HDFS-9857](https://issues.apache.org/jira/browse/HDFS-9857) | Erasure Coding: Rename replication-based names in BlockManager to more generic [part-1] |  Major | erasure-coding | Rakesh R | Rakesh R |
| [YARN-4108](https://issues.apache.org/jira/browse/YARN-4108) | CapacityScheduler: Improve preemption to only kill containers that would satisfy the incoming request |  Major | capacity scheduler | Wangda Tan | Wangda Tan |
| [YARN-4062](https://issues.apache.org/jira/browse/YARN-4062) | Add the flush and compaction functionality via coprocessors and scanners for flow run table |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-4712](https://issues.apache.org/jira/browse/YARN-4712) | CPU Usage Metric is not captured properly in YARN-2928 |  Major | timelineserver | Naganarasimha G R | Naganarasimha G R |
| [HADOOP-12926](https://issues.apache.org/jira/browse/HADOOP-12926) | lz4.c does not detect 64-bit mode properly |  Major | native | Alan Burlison | Alan Burlison |
| [HADOOP-12819](https://issues.apache.org/jira/browse/HADOOP-12819) | Migrate TestSaslRPC and related codes to rebase on ProtobufRpcEngine |  Major | . | Kai Zheng | Kai Zheng |
| [YARN-4815](https://issues.apache.org/jira/browse/YARN-4815) | ATS 1.5 timelineclient impl try to create attempt directory for every event call |  Major | timelineserver | Xuan Gong | Xuan Gong |
| [YARN-4814](https://issues.apache.org/jira/browse/YARN-4814) | ATS 1.5 timelineclient impl call flush after every event write |  Major | timelineserver | Xuan Gong | Xuan Gong |
| [HDFS-9005](https://issues.apache.org/jira/browse/HDFS-9005) | Provide configuration support for upgrade domain |  Major | . | Ming Ma | Ming Ma |
| [HDFS-9694](https://issues.apache.org/jira/browse/HDFS-9694) | Make existing DFSClient#getFileChecksum() work for striped blocks |  Major | . | Kai Zheng | Kai Zheng |
| [YARN-4805](https://issues.apache.org/jira/browse/YARN-4805) | Don't go through all schedulers in ParameterizedTestBase |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HDFS-9640](https://issues.apache.org/jira/browse/HDFS-9640) | Remove hsftp from DistCp in trunk |  Major | distcp | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-998](https://issues.apache.org/jira/browse/YARN-998) | Keep NM resource updated through dynamic resource config for RM/NM restart |  Major | graceful, nodemanager, scheduler | Junping Du | Junping Du |
| [YARN-4711](https://issues.apache.org/jira/browse/YARN-4711) | NM is going down with NPE's due to single thread processing of events by Timeline client |  Critical | timelineserver | Naganarasimha G R | Naganarasimha G R |
| [MAPREDUCE-6543](https://issues.apache.org/jira/browse/MAPREDUCE-6543) | Migrate MR Client test cases part 2 |  Trivial | test | Dustin Cote | Dustin Cote |
| [HDFS-9349](https://issues.apache.org/jira/browse/HDFS-9349) | Support reconfiguring fs.protected.directories without NN restart |  Major | namenode | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-4822](https://issues.apache.org/jira/browse/YARN-4822) | Refactor existing Preemption Policy of CS for easier adding new approach to select preemption candidates |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-4811](https://issues.apache.org/jira/browse/YARN-4811) | Generate histograms in ContainerMetrics for actual container resource usage |  Major | . | Varun Vasudev | Varun Vasudev |
| [YARN-4634](https://issues.apache.org/jira/browse/YARN-4634) | Scheduler UI/Metrics need to consider cases like non-queue label mappings |  Major | . | Sunil G | Sunil G |
| [HADOOP-12169](https://issues.apache.org/jira/browse/HADOOP-12169) | ListStatus on empty dir in S3A lists itself instead of returning an empty list |  Major | fs/s3 | Pieter Reuse | Pieter Reuse |
| [HADOOP-12959](https://issues.apache.org/jira/browse/HADOOP-12959) | Add additional github web site for ISA-L library |  Major | . | Li Bo | Li Bo |
| [HDFS-10186](https://issues.apache.org/jira/browse/HDFS-10186) | DirectoryScanner: Improve logs by adding full path of both actual and expected block directories |  Minor | datanode | Rakesh R | Rakesh R |
| [HDFS-9719](https://issues.apache.org/jira/browse/HDFS-9719) | Refactoring ErasureCodingWorker into smaller reusable constructs |  Major | . | Kai Zheng | Kai Zheng |
| [YARN-3461](https://issues.apache.org/jira/browse/YARN-3461) | Consolidate flow name/version/run defaults |  Major | timelineserver | Zhijie Shen | Sangjin Lee |
| [HADOOP-12753](https://issues.apache.org/jira/browse/HADOOP-12753) | S3A JUnit tests failing if using HTTP proxy |  Minor | fs/s3 | Zoran Rajic | Zoran Rajic |
| [YARN-4826](https://issues.apache.org/jira/browse/YARN-4826) | Document configuration of ReservationSystem for CapacityScheduler |  Minor | capacity scheduler | Subru Krishnan | Subru Krishnan |
| [HADOOP-12444](https://issues.apache.org/jira/browse/HADOOP-12444) | Support lazy seek in S3AInputStream |  Major | fs/s3 | Rajesh Balamohan | Rajesh Balamohan |
| [YARN-4928](https://issues.apache.org/jira/browse/YARN-4928) | Some yarn.server.timeline.\* tests fail on Windows attempting to use a test root path containing a colon |  Minor | test | Gergely Novák | Gergely Novák |
| [YARN-4168](https://issues.apache.org/jira/browse/YARN-4168) | Test TestLogAggregationService.testLocalFileDeletionOnDiskFull failing |  Critical | test | Steve Loughran | Takashi Ohnishi |
| [YARN-3863](https://issues.apache.org/jira/browse/YARN-3863) | Support complex filters in TimelineReader |  Major | . | Varun Saxena | Varun Saxena |
| [HDFS-9918](https://issues.apache.org/jira/browse/HDFS-9918) | Erasure Coding: Sort located striped blocks based on decommissioned states |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-12973](https://issues.apache.org/jira/browse/HADOOP-12973) | make DU pluggable |  Major | . | Elliott Clark | Elliott Clark |
| [YARN-4886](https://issues.apache.org/jira/browse/YARN-4886) | Add HDFS caller context for EntityGroupFSTimelineStore |  Major | timelineserver | Li Lu | Li Lu |
| [HDFS-10209](https://issues.apache.org/jira/browse/HDFS-10209) | Support enable caller context in HDFS namenode audit log without restart namenode |  Major | . | Xiaoyu Yao | Xiaobing Zhou |
| [HDFS-10286](https://issues.apache.org/jira/browse/HDFS-10286) | Fix TestDFSAdmin#testNameNodeGetReconfigurableProperties |  Major | . | Xiaoyu Yao | Xiaobing Zhou |
| [HDFS-10281](https://issues.apache.org/jira/browse/HDFS-10281) | o.a.h.hdfs.server.namenode.ha.TestPendingCorruptDnMessages fails intermittently |  Major | test | Mingliang Liu | Mingliang Liu |
| [HDFS-10293](https://issues.apache.org/jira/browse/HDFS-10293) | StripedFileTestUtil#readAll flaky |  Major | erasure-coding, test | Mingliang Liu | Mingliang Liu |
| [YARN-4909](https://issues.apache.org/jira/browse/YARN-4909) | Fix intermittent failures of TestRMWebServices And TestRMWithCSRFFilter |  Blocker | . | Brahma Reddy Battula | Bibin A Chundatt |
| [YARN-4468](https://issues.apache.org/jira/browse/YARN-4468) | Document the general ReservationSystem functionality, and the REST API |  Major | capacityscheduler, fairscheduler, resourcemanager | Carlo Curino | Carlo Curino |
| [HADOOP-12924](https://issues.apache.org/jira/browse/HADOOP-12924) | Configure raw erasure coders for supported codecs |  Minor | . | Rui Li | Rui Li |
| [HDFS-10284](https://issues.apache.org/jira/browse/HDFS-10284) | o.a.h.hdfs.server.blockmanagement.TestBlockManagerSafeMode.testCheckSafeMode fails intermittently |  Major | test | Mingliang Liu | Mingliang Liu |
| [YARN-2883](https://issues.apache.org/jira/browse/YARN-2883) | Queuing of container requests in the NM |  Major | nodemanager, resourcemanager | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-4890](https://issues.apache.org/jira/browse/YARN-4890) | Unit test intermittent failure: TestNodeLabelContainerAllocation#testQueueUsedCapacitiesUpdate |  Major | . | Wangda Tan | Sunil G |
| [HDFS-10207](https://issues.apache.org/jira/browse/HDFS-10207) | Support enable Hadoop IPC backoff without namenode restart |  Major | . | Xiaoyu Yao | Xiaobing Zhou |
| [YARN-4968](https://issues.apache.org/jira/browse/YARN-4968) | A couple of AM retry unit tests need to wait SchedulerApplicationAttempt stopped. |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-13011](https://issues.apache.org/jira/browse/HADOOP-13011) | Clearly Document the Password Details for Keystore-based Credential Providers |  Major | documentation | Larry McCay | Larry McCay |
| [YARN-3816](https://issues.apache.org/jira/browse/YARN-3816) | [Aggregation] App-level aggregation and accumulation for YARN system metrics |  Major | timelineserver | Junping Du | Li Lu |
| [YARN-4846](https://issues.apache.org/jira/browse/YARN-4846) | Random failures for TestCapacitySchedulerPreemption#testPreemptionPolicyShouldRespectAlreadyMarkedKillableContainers |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3215](https://issues.apache.org/jira/browse/YARN-3215) | Respect labels in CapacityScheduler when computing headroom |  Major | capacityscheduler | Wangda Tan | Naganarasimha G R |
| [YARN-2885](https://issues.apache.org/jira/browse/YARN-2885) | Create AMRMProxy request interceptor for distributed scheduling decisions for queueable containers |  Major | nodemanager, resourcemanager | Konstantinos Karanasos | Arun Suresh |
| [HADOOP-12935](https://issues.apache.org/jira/browse/HADOOP-12935) | API documentation for dynamic subcommands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HDFS-9869](https://issues.apache.org/jira/browse/HDFS-9869) | Erasure Coding: Rename replication-based names in BlockManager to more generic [part-2] |  Major | erasure-coding | Rakesh R | Rakesh R |
| [YARN-4991](https://issues.apache.org/jira/browse/YARN-4991) | Fix ContainerRequest Constructor to set nodelabelExpression correctly |  Major | test | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-10224](https://issues.apache.org/jira/browse/HDFS-10224) | Implement asynchronous rename for DistributedFileSystem |  Major | fs, hdfs-client | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-4966](https://issues.apache.org/jira/browse/YARN-4966) | Improve yarn logs to fetch container logs without specifying nodeId |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-4807](https://issues.apache.org/jira/browse/YARN-4807) | MockAM#waitForState sleep duration is too long |  Major | . | Karthik Kambatla | Yufei Gu |
| [YARN-4956](https://issues.apache.org/jira/browse/YARN-4956) | findbug issue on LevelDBCacheTimelineStore |  Major | timelineserver | Xuan Gong | Zhiyuan Yang |
| [YARN-3998](https://issues.apache.org/jira/browse/YARN-3998) | Add support in the NodeManager to re-launch containers |  Major | . | Jun Gong | Jun Gong |
| [YARN-4986](https://issues.apache.org/jira/browse/YARN-4986) | Add a check in the coprocessor for table to operated on |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-5014](https://issues.apache.org/jira/browse/YARN-5014) | Ensure non-metric values are returned as is for flow run table from the coprocessor |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-3150](https://issues.apache.org/jira/browse/YARN-3150) | [Documentation] Documenting the timeline service v2 |  Major | . | Zhijie Shen | Sangjin Lee |
| [MAPREDUCE-6424](https://issues.apache.org/jira/browse/MAPREDUCE-6424) | Store MR counters as timeline metrics instead of event |  Major | . | Junping Du | Naganarasimha G R |
| [YARN-4447](https://issues.apache.org/jira/browse/YARN-4447) | Provide a mechanism to represent complex filters and parse them at the REST layer |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-4851](https://issues.apache.org/jira/browse/YARN-4851) | Metric improvements for ATS v1.5 storage components |  Major | timelineserver | Li Lu | Li Lu |
| [MAPREDUCE-6688](https://issues.apache.org/jira/browse/MAPREDUCE-6688) | Store job configurations in Timeline Service v2 |  Major | applicationmaster | Junping Du | Varun Saxena |
| [HADOOP-12931](https://issues.apache.org/jira/browse/HADOOP-12931) | bin/hadoop work for dynamic subcommands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12934](https://issues.apache.org/jira/browse/HADOOP-12934) | bin/mapred work for dynamic subcommands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12933](https://issues.apache.org/jira/browse/HADOOP-12933) | bin/hdfs work for dynamic subcommands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12932](https://issues.apache.org/jira/browse/HADOOP-12932) | bin/yarn work for dynamic subcommands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12936](https://issues.apache.org/jira/browse/HADOOP-12936) | modify hadoop-tools to take advantage of dynamic subcommands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-4920](https://issues.apache.org/jira/browse/YARN-4920) | ATS/NM should support a link to dowload/get the logs in text format |  Major | yarn | Xuan Gong | Xuan Gong |
| [HADOOP-13086](https://issues.apache.org/jira/browse/HADOOP-13086) | enable daemonization of dynamic commands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13089](https://issues.apache.org/jira/browse/HADOOP-13089) | hadoop distcp adds client opts twice when dynamic |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13087](https://issues.apache.org/jira/browse/HADOOP-13087) | env var doc update for dynamic commands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13088](https://issues.apache.org/jira/browse/HADOOP-13088) | fix shellprofiles in hadoop-tools to allow replacement |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-4905](https://issues.apache.org/jira/browse/YARN-4905) | Improve "yarn logs" command-line to optionally show log metadata also |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-13094](https://issues.apache.org/jira/browse/HADOOP-13094) | hadoop-common unit tests for dynamic commands |  Major | scripts, test | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13095](https://issues.apache.org/jira/browse/HADOOP-13095) | hadoop-hdfs unit tests for dynamic commands |  Major | scripts, test | Allen Wittenauer | Allen Wittenauer |
| [YARN-4595](https://issues.apache.org/jira/browse/YARN-4595) | Add support for configurable read-only mounts when launching Docker containers |  Major | yarn | Billie Rinaldi | Billie Rinaldi |
| [YARN-4390](https://issues.apache.org/jira/browse/YARN-4390) | Do surgical preemption based on reserved container in CapacityScheduler |  Major | capacity scheduler | Eric Payne | Wangda Tan |
| [HADOOP-13107](https://issues.apache.org/jira/browse/HADOOP-13107) | clean up how rumen is executed |  Major | scripts, tools | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13108](https://issues.apache.org/jira/browse/HADOOP-13108) | dynamic subcommands need a way to manipulate arguments |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13110](https://issues.apache.org/jira/browse/HADOOP-13110) | add a streaming subcommand to mapred |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [YARN-5045](https://issues.apache.org/jira/browse/YARN-5045) | hbase unit tests fail due to dependency issues |  Blocker | timelineserver | Sangjin Lee | Sangjin Lee |
| [HADOOP-13111](https://issues.apache.org/jira/browse/HADOOP-13111) | convert hadoop gridmix to be dynamic |  Major | scripts, tools | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13115](https://issues.apache.org/jira/browse/HADOOP-13115) | dynamic subcommand docs should talk about exit vs. continue program flow |  Major | documentation, scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13117](https://issues.apache.org/jira/browse/HADOOP-13117) | clarify daemonization and security vars for dynamic commands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13121](https://issues.apache.org/jira/browse/HADOOP-13121) | rename sub-project shellprofiles to match the rest of Hadoop |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13120](https://issues.apache.org/jira/browse/HADOOP-13120) | add a --debug message when dynamic commands have been used |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-4778](https://issues.apache.org/jira/browse/YARN-4778) | Support specifying resources for task containers in SLS |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-4842](https://issues.apache.org/jira/browse/YARN-4842) | "yarn logs" command should not require the appOwner argument |  Major | . | Ram Venkatesh | Xuan Gong |
| [YARN-5073](https://issues.apache.org/jira/browse/YARN-5073) | Refactor startContainerInternal() in ContainerManager to remove unused parameter |  Minor | nodemanager, resourcemanager | Konstantinos Karanasos | Konstantinos Karanasos |
| [HADOOP-12749](https://issues.apache.org/jira/browse/HADOOP-12749) | Create a threadpoolexecutor that overrides afterExecute to log uncaught exceptions/errors |  Major | . | Sidharta Seethana | Sidharta Seethana |
| [HDFS-10346](https://issues.apache.org/jira/browse/HDFS-10346) | Implement asynchronous setPermission/setOwner for DistributedFileSystem |  Major | hdfs, hdfs-client | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-13122](https://issues.apache.org/jira/browse/HADOOP-13122) | Customize User-Agent header sent in HTTP requests by S3A. |  Minor | fs/s3 | Chris Nauroth | Chris Nauroth |
| [HADOOP-12844](https://issues.apache.org/jira/browse/HADOOP-12844) | Recover when S3A fails on IOException in read() |  Major | fs/s3 | Pieter Reuse | Pieter Reuse |
| [HADOOP-13028](https://issues.apache.org/jira/browse/HADOOP-13028) | add low level counter metrics for S3A; use in read performance tests |  Major | fs/s3, metrics | Steve Loughran | Steve Loughran |
| [HADOOP-12581](https://issues.apache.org/jira/browse/HADOOP-12581) | ShellBasedIdMapping needs suport for Solaris |  Major | security | Alan Burlison | Alan Burlison |
| [HDFS-9389](https://issues.apache.org/jira/browse/HDFS-9389) | Add maintenance states to AdminStates |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-13113](https://issues.apache.org/jira/browse/HADOOP-13113) | Enable parallel test execution for hadoop-aws. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [YARN-2888](https://issues.apache.org/jira/browse/YARN-2888) | Corrective mechanisms for rebalancing NM container queues |  Major | nodemanager, resourcemanager | Konstantinos Karanasos | Arun Suresh |
| [HADOOP-13129](https://issues.apache.org/jira/browse/HADOOP-13129) | fix typo in dynamic subcommand docs |  Trivial | scripts | Sean Busbey | Allen Wittenauer |
| [HADOOP-13151](https://issues.apache.org/jira/browse/HADOOP-13151) | Underscores should be escaped in dynamic subcommands document |  Minor | documentation | Akira Ajisaka | Allen Wittenauer |
| [HADOOP-13153](https://issues.apache.org/jira/browse/HADOOP-13153) | fix typo in debug statement for dynamic subcommands |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-4738](https://issues.apache.org/jira/browse/YARN-4738) | Notify the RM about the status of OPPORTUNISTIC containers |  Major | nodemanager, resourcemanager | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-5075](https://issues.apache.org/jira/browse/YARN-5075) | Fix findbugs warning in hadoop-yarn-common module |  Major | . | Akira Ajisaka | Arun Suresh |
| [YARN-4412](https://issues.apache.org/jira/browse/YARN-4412) | Create ClusterMonitor to compute ordered list of preferred NMs for OPPORTUNITIC containers |  Major | nodemanager, resourcemanager | Arun Suresh | Arun Suresh |
| [HADOOP-13158](https://issues.apache.org/jira/browse/HADOOP-13158) | S3AFileSystem#toString might throw NullPointerException due to null cannedACL. |  Minor | fs/s3 | Chris Nauroth | Chris Nauroth |
| [YARN-5102](https://issues.apache.org/jira/browse/YARN-5102) | timeline service build fails with java 8 |  Blocker | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-4832](https://issues.apache.org/jira/browse/YARN-4832) | NM side resource value should get updated if change applied in RM side |  Critical | nodemanager, resourcemanager | Junping Du | Junping Du |
| [YARN-5090](https://issues.apache.org/jira/browse/YARN-5090) | Add End-to-End test-cases for DistributedScheduling using MiniYarnCluster |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-5096](https://issues.apache.org/jira/browse/YARN-5096) | timelinereader has a lot of logging that's not useful |  Minor | timelineserver | Sangjin Lee | Sangjin Lee |
| [HADOOP-13140](https://issues.apache.org/jira/browse/HADOOP-13140) | FileSystem#initialize must not attempt to create StorageStatistics objects with null or empty schemes |  Major | fs | Brahma Reddy Battula | Mingliang Liu |
| [YARN-4913](https://issues.apache.org/jira/browse/YARN-4913) | Yarn logs should take a -out option to write to a directory |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-2173](https://issues.apache.org/jira/browse/HDFS-2173) | saveNamespace should not throw IOE when only one storage directory fails to write VERSION file |  Major | . | Todd Lipcon | Andras Bokor |
| [YARN-5110](https://issues.apache.org/jira/browse/YARN-5110) | Fix OpportunisticContainerAllocator to insert complete HostAddress in issued ContainerTokenIds |  Major | . | Arun Suresh | Konstantinos Karanasos |
| [YARN-5050](https://issues.apache.org/jira/browse/YARN-5050) | Code cleanup for TestDistributedShell |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-5016](https://issues.apache.org/jira/browse/YARN-5016) | Add support for a minimum retry interval for container retries |  Major | . | Varun Vasudev | Jun Gong |
| [HADOOP-13130](https://issues.apache.org/jira/browse/HADOOP-13130) | s3a failures can surface as RTEs, not IOEs |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-7766](https://issues.apache.org/jira/browse/HDFS-7766) | Add a flag to WebHDFS op=CREATE to not respond with a 307 redirect |  Major | ui, webhdfs | Ravi Prakash | Ravi Prakash |
| [YARN-5093](https://issues.apache.org/jira/browse/YARN-5093) | created time shows 0 in most REST output |  Critical | timelineserver | Sangjin Lee | Varun Saxena |
| [YARN-5018](https://issues.apache.org/jira/browse/YARN-5018) | Online aggregation logic should not run immediately after collectors got started |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-3362](https://issues.apache.org/jira/browse/YARN-3362) | Add node label usage in RM CapacityScheduler web UI |  Major | capacityscheduler, resourcemanager, webapp | Wangda Tan | Naganarasimha G R |
| [HDFS-10390](https://issues.apache.org/jira/browse/HDFS-10390) | Implement asynchronous setAcl/getAclStatus for DistributedFileSystem |  Major | fs | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-5115](https://issues.apache.org/jira/browse/YARN-5115) | Avoid setting CONTENT-DISPOSITION header in the container-logs web-service |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-857](https://issues.apache.org/jira/browse/YARN-857) | Localization failures should be available in container diagnostics |  Critical | . | Hitesh Shah | Vinod Kumar Vavilapalli |
| [HDFS-8057](https://issues.apache.org/jira/browse/HDFS-8057) | Move BlockReader implementation to the client implementation package |  Major | hdfs-client | Tsz Wo Nicholas Sze | Takanobu Asanuma |
| [HDFS-10434](https://issues.apache.org/jira/browse/HDFS-10434) | Fix intermittent test failure of TestDataNodeErasureCodingMetrics |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-12925](https://issues.apache.org/jira/browse/HADOOP-12925) | Checks for SPARC architecture need to include 64-bit SPARC |  Major | conf | Alan Burlison | Alan Burlison |
| [YARN-5097](https://issues.apache.org/jira/browse/YARN-5097) | NPE in Separator.joinEncoded() |  Critical | timelineserver | Sangjin Lee | Vrushali C |
| [YARN-5095](https://issues.apache.org/jira/browse/YARN-5095) | flow activities and flow runs are populated with wrong timestamp when RM restarts w/ recovery enabled |  Critical | timelineserver | Sangjin Lee | Varun Saxena |
| [YARN-4957](https://issues.apache.org/jira/browse/YARN-4957) | Add getNewReservation in ApplicationClientProtocol |  Major | applications, client, resourcemanager | Subru Krishnan | Sean Po |
| [HADOOP-13010](https://issues.apache.org/jira/browse/HADOOP-13010) | Refactor raw erasure coders |  Major | . | Kai Zheng | Kai Zheng |
| [HADOOP-12727](https://issues.apache.org/jira/browse/HADOOP-12727) | Minor cleanups needed for CMake 3.X |  Minor | native | Alan Burlison | Alan Burlison |
| [YARN-5138](https://issues.apache.org/jira/browse/YARN-5138) | fix "no findbugs output file" error for hadoop-yarn-server-timelineservice-hbase-tests |  Major | timelineserver | Vrushali C | Vrushali C |
| [HDFS-10431](https://issues.apache.org/jira/browse/HDFS-10431) | Refactor and speedup TestAsyncDFSRename |  Minor | test | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-10236](https://issues.apache.org/jira/browse/HDFS-10236) | Erasure Coding: Rename replication-based names in BlockManager to more generic [part-3] |  Major | erasure-coding | Rakesh R | Rakesh R |
| [YARN-5109](https://issues.apache.org/jira/browse/YARN-5109) | timestamps are stored unencoded causing parse errors |  Blocker | timelineserver | Sangjin Lee | Varun Saxena |
| [YARN-5089](https://issues.apache.org/jira/browse/YARN-5089) | Improve "yarn log" command-line "logFiles" option to support regex |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-4987](https://issues.apache.org/jira/browse/YARN-4987) | Read cache concurrency issue between read and evict in EntityGroupFS timeline store |  Critical | . | Li Lu | Li Lu |
| [YARN-5127](https://issues.apache.org/jira/browse/YARN-5127) | Expose ExecutionType in Container api record |  Major | . | Arun Suresh | Hitesh Sharma |
| [HDFS-10430](https://issues.apache.org/jira/browse/HDFS-10430) | Reuse FileSystem#access in TestAsyncDFS |  Major | hdfs | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-5117](https://issues.apache.org/jira/browse/YARN-5117) | QueuingContainerManager does not start GUARANTEED Container even if Resources are available |  Major | . | Arun Suresh | Konstantinos Karanasos |
| [YARN-5111](https://issues.apache.org/jira/browse/YARN-5111) | YARN container system metrics are not aggregated to application |  Critical | timelineserver | Sangjin Lee | Naganarasimha G R |
| [YARN-4007](https://issues.apache.org/jira/browse/YARN-4007) | Add support for different network setups when launching the docker container |  Major | nodemanager | Varun Vasudev | Sidharta Seethana |
| [YARN-5141](https://issues.apache.org/jira/browse/YARN-5141) | Get Container logs for the Running application from Yarn Logs CommandLine |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5105](https://issues.apache.org/jira/browse/YARN-5105) | entire time series is returned for YARN container system metrics (CPU and memory) |  Major | timelineserver | Sangjin Lee | Varun Saxena |
| [HDFS-9833](https://issues.apache.org/jira/browse/HDFS-9833) | Erasure coding: recomputing block checksum on the fly by reconstructing the missed/corrupt block data |  Major | . | Kai Zheng | Rakesh R |
| [HADOOP-13162](https://issues.apache.org/jira/browse/HADOOP-13162) | Consider reducing number of getFileStatus calls in S3AFileSystem.mkdirs |  Minor | fs/s3 | Rajesh Balamohan | Rajesh Balamohan |
| [YARN-5088](https://issues.apache.org/jira/browse/YARN-5088) | Improve "yarn log" command-line to read the last K bytes for the log files |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-13131](https://issues.apache.org/jira/browse/HADOOP-13131) | Add tests to verify that S3A supports SSE-S3 encryption |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-5162](https://issues.apache.org/jira/browse/YARN-5162) | Fix Exceptions thrown during registerAM call when Distributed Scheduling is Enabled |  Major | . | Arun Suresh | Hitesh Sharma |
| [HADOOP-13171](https://issues.apache.org/jira/browse/HADOOP-13171) | Add StorageStatistics to S3A; instrument some more operations |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-1815](https://issues.apache.org/jira/browse/YARN-1815) | Work preserving recovery of Unmanged AMs |  Critical | resourcemanager | Karthik Kambatla | Subru Krishnan |
| [YARN-5165](https://issues.apache.org/jira/browse/YARN-5165) | Fix NoOvercommitPolicy to take advantage of RLE representation of plan |  Major | capacityscheduler, fairscheduler, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-5189](https://issues.apache.org/jira/browse/YARN-5189) | Make HBaseTimeline[Reader\|Writer]Impl default and move FileSystemTimeline\*Impl |  Major | timelineserver | Joep Rottinghuis | Joep Rottinghuis |
| [HDFS-9877](https://issues.apache.org/jira/browse/HDFS-9877) | HDFS Namenode UI: Fix browsing directories that need to be encoded |  Major | ui | Ravi Prakash | Ravi Prakash |
| [HDFS-7767](https://issues.apache.org/jira/browse/HDFS-7767) | Use the noredirect flag in WebHDFS to allow web browsers to upload files via the NN UI |  Major | ui, webhdfs | Ravi Prakash | Ravi Prakash |
| [YARN-5142](https://issues.apache.org/jira/browse/YARN-5142) | fix findbugs warnings/errors for hadoop-yarn-server-timelineservice-hbase-tests |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-5167](https://issues.apache.org/jira/browse/YARN-5167) | Escaping occurences of encodedValues |  Critical | timelineserver | Joep Rottinghuis | Sangjin Lee |
| [YARN-5185](https://issues.apache.org/jira/browse/YARN-5185) | StageAllocaterGreedyRLE: Fix NPE in corner case |  Major | capacityscheduler, fairscheduler, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-4525](https://issues.apache.org/jira/browse/YARN-4525) | Fix bug in RLESparseResourceAllocation.getRangeOverlapping(...) |  Major | . | Ishai Menache | Ishai Menache |
| [YARN-5180](https://issues.apache.org/jira/browse/YARN-5180) | Allow ResourceRequest to specify an enforceExecutionType flag |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-5176](https://issues.apache.org/jira/browse/YARN-5176) | More test cases for queuing of containers at the NM |  Major | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-5204](https://issues.apache.org/jira/browse/YARN-5204) | Properly report status of killed/stopped queued containers |  Major | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [HADOOP-13237](https://issues.apache.org/jira/browse/HADOOP-13237) | s3a initialization against public bucket fails if caller lacks any credentials |  Minor | fs/s3 | Steve Loughran | Chris Nauroth |
| [YARN-5210](https://issues.apache.org/jira/browse/YARN-5210) | NPE in Distributed Shell while publishing DS\_CONTAINER\_START event and other miscellaneous issues |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-5199](https://issues.apache.org/jira/browse/YARN-5199) | Close LogReader in in AHSWebServices#getStreamingOutput and FileInputStream in NMWebServices#getLogs |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5191](https://issues.apache.org/jira/browse/YARN-5191) | Rename the “download=true” option for getLogs in NMWebServices and AHSWebServices |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5170](https://issues.apache.org/jira/browse/YARN-5170) | Eliminate singleton converters and static method access |  Major | timelineserver | Joep Rottinghuis | Joep Rottinghuis |
| [HDFS-7987](https://issues.apache.org/jira/browse/HDFS-7987) | Allow files / directories to be moved |  Major | ui | Ravi Prakash | Ravi Prakash |
| [YARN-3426](https://issues.apache.org/jira/browse/YARN-3426) | Add jdiff support to YARN |  Blocker | . | Li Lu | Li Lu |
| [YARN-5052](https://issues.apache.org/jira/browse/YARN-5052) | [Documentation] Update timeline service v2 documentation to capture information about filters |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-5124](https://issues.apache.org/jira/browse/YARN-5124) | Modify AMRMClient to set the ExecutionType in the ResourceRequest |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-5212](https://issues.apache.org/jira/browse/YARN-5212) | Run existing ContainerManager tests using QueuingContainerManagerImpl |  Major | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-4887](https://issues.apache.org/jira/browse/YARN-4887) | AM-RM protocol changes for identifying resource-requests explicitly |  Major | applications, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-1942](https://issues.apache.org/jira/browse/YARN-1942) | Deprecate toString/fromString methods from ConverterUtils and move them to records classes like ContainerId/ApplicationId, etc. |  Critical | api | Thomas Graves | Wangda Tan |
| [HDFS-9016](https://issues.apache.org/jira/browse/HDFS-9016) | Display upgrade domain information in fsck |  Major | . | Ming Ma | Ming Ma |
| [YARN-5223](https://issues.apache.org/jira/browse/YARN-5223) | Container line in yarn logs output for a live application should include the hostname for the container |  Major | . | Siddharth Seth | Xuan Gong |
| [YARN-5122](https://issues.apache.org/jira/browse/YARN-5122) | "yarn logs" for running containers should print an explicit footer saying that the log may be incomplete |  Major | . | Vinod Kumar Vavilapalli | Jian He |
| [YARN-5243](https://issues.apache.org/jira/browse/YARN-5243) | fix several rebase and other miscellaneous issues before merge |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-5070](https://issues.apache.org/jira/browse/YARN-5070) | upgrade HBase version for first merge |  Critical | timelineserver | Sangjin Lee | Vrushali C |
| [HDFS-9922](https://issues.apache.org/jira/browse/HDFS-9922) | Upgrade Domain placement policy status marks a good block in violation when there are decommissioned nodes |  Minor | . | Chris Trezzo | Chris Trezzo |
| [HADOOP-13241](https://issues.apache.org/jira/browse/HADOOP-13241) | document s3a better |  Minor | documentation, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-12975](https://issues.apache.org/jira/browse/HADOOP-12975) | Add jitter to CachingGetSpaceUsed's thread |  Major | . | Elliott Clark | Elliott Clark |
| [YARN-4844](https://issues.apache.org/jira/browse/YARN-4844) | Add getMemorySize/getVirtualCoresSize to o.a.h.y.api.records.Resource |  Blocker | api | Wangda Tan | Wangda Tan |
| [YARN-5251](https://issues.apache.org/jira/browse/YARN-5251) | Yarn CLI to obtain App logs for last 'n' bytes fails with 'java.io.IOException' and for 'n' bytes fails with NumberFormatException |  Blocker | . | Sumana Sathish | Xuan Gong |
| [HADOOP-13280](https://issues.apache.org/jira/browse/HADOOP-13280) | FileSystemStorageStatistics#getLong(“readOps“) should return readOps + largeReadOps |  Major | fs | Mingliang Liu | Mingliang Liu |
| [HADOOP-13288](https://issues.apache.org/jira/browse/HADOOP-13288) | Guard null stats key in FileSystemStorageStatistics |  Major | fs | Mingliang Liu | Mingliang Liu |
| [HADOOP-13291](https://issues.apache.org/jira/browse/HADOOP-13291) | Probing stats in DFSOpsCountStatistics/S3AStorageStatistics should be correctly implemented |  Major | fs | Mingliang Liu | Mingliang Liu |
| [HDFS-10538](https://issues.apache.org/jira/browse/HDFS-10538) | Remove AsyncDistributedFileSystem API |  Major | hdfs-client | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-10328](https://issues.apache.org/jira/browse/HDFS-10328) | Add per-cache-pool default replication num configuration |  Minor | caching | xupeng | xupeng |
| [HADOOP-13203](https://issues.apache.org/jira/browse/HADOOP-13203) | S3A: Support fadvise "random" mode for high performance readPositioned() reads |  Major | fs/s3 | Rajesh Balamohan | Rajesh Balamohan |
| [HDFS-10473](https://issues.apache.org/jira/browse/HDFS-10473) | Allow only suitable storage policies to be set on striped files |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-5171](https://issues.apache.org/jira/browse/YARN-5171) | Extend DistributedSchedulerProtocol to notify RM of containers allocated by the Node |  Major | . | Arun Suresh | Inigo Goiri |
| [MAPREDUCE-6720](https://issues.apache.org/jira/browse/MAPREDUCE-6720) | Inconsistent values of counters across tasks and job reported to timeline service. |  Major | applicationmaster | Varun Saxena | Varun Saxena |
| [HDFS-10460](https://issues.apache.org/jira/browse/HDFS-10460) | Erasure Coding: Recompute block checksum for a particular range less than file size on the fly by reconstructing missed block |  Major | datanode | Rakesh R | Rakesh R |
| [HADOOP-13019](https://issues.apache.org/jira/browse/HADOOP-13019) | Implement ErasureCodec for HitchHiker XOR coding |  Major | io | Kai Sasaki | Kai Sasaki |
| [HADOOP-12229](https://issues.apache.org/jira/browse/HADOOP-12229) | Fix inconsistent subsection titles in filesystem.md |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-12242](https://issues.apache.org/jira/browse/HADOOP-12242) | Add in-page TOC to filesystem specification pages |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-13305](https://issues.apache.org/jira/browse/HADOOP-13305) | Define common statistics names across schemes |  Major | fs | Mingliang Liu | Mingliang Liu |
| [YARN-5174](https://issues.apache.org/jira/browse/YARN-5174) | [documentation] several updates/corrections to timeline service documentation |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-5227](https://issues.apache.org/jira/browse/YARN-5227) | yarn logs command: no need to specify -applicationId when specifying containerId |  Major | . | Jian He | Gergely Novák |
| [YARN-5224](https://issues.apache.org/jira/browse/YARN-5224) | Logs for a completed container are not available in the yarn logs output for a live application |  Major | . | Siddharth Seth | Xuan Gong |
| [YARN-5316](https://issues.apache.org/jira/browse/YARN-5316) | fix hadoop-aws pom not to do the exclusion |  Major | timelineserver | Sangjin Lee | Sangjin Lee |
| [HADOOP-11993](https://issues.apache.org/jira/browse/HADOOP-11993) | maven enforcer plugin to ban java 8 incompatible dependencies |  Minor | . | Steve Loughran | Tsuyoshi Ozawa |
| [HADOOP-13283](https://issues.apache.org/jira/browse/HADOOP-13283) | Support reset operation for new global storage statistics and per FS storage stats |  Major | fs | Mingliang Liu | Mingliang Liu |
| [YARN-5233](https://issues.apache.org/jira/browse/YARN-5233) | Support for specifying a path for ATS plugin jars |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-5080](https://issues.apache.org/jira/browse/YARN-5080) | Cannot obtain logs using YARN CLI -am for either KILLED or RUNNING AM |  Critical | yarn | Sumana Sathish | Xuan Gong |
| [YARN-5200](https://issues.apache.org/jira/browse/YARN-5200) | Improve yarn logs to get Container List |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-13366](https://issues.apache.org/jira/browse/HADOOP-13366) | Fix dead link in o.a.h.fs.CommonConfigurationKeysPublic javadoc |  Minor | documentation | Rakesh R | Rakesh R |
| [YARN-5299](https://issues.apache.org/jira/browse/YARN-5299) | Log Docker run command when container fails |  Major | yarn | Varun Vasudev | Varun Vasudev |
| [YARN-4759](https://issues.apache.org/jira/browse/YARN-4759) | Fix signal handling for docker containers |  Major | yarn | Sidharta Seethana | Shane Kumpf |
| [YARN-5363](https://issues.apache.org/jira/browse/YARN-5363) | For AM containers, or for containers of running-apps, "yarn logs" incorrectly only (tries to) shows syslog file-type by default |  Major | log-aggregation | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HDFS-10600](https://issues.apache.org/jira/browse/HDFS-10600) | PlanCommand#getThrsholdPercentage should not use throughput value. |  Major | diskbalancer | Lei (Eddy) Xu | Yiqun Lin |
| [YARN-5298](https://issues.apache.org/jira/browse/YARN-5298) | Mount usercache and NM filecache directories into Docker container |  Major | yarn | Varun Vasudev | Sidharta Seethana |
| [YARN-5361](https://issues.apache.org/jira/browse/YARN-5361) | Obtaining logs for completed container says 'file belongs to a running container ' at the end |  Critical | . | Sumana Sathish | Xuan Gong |
| [YARN-4484](https://issues.apache.org/jira/browse/YARN-4484) | Available Resource calculation for a queue is not correct when used with labels |  Major | capacity scheduler | Sunil G | Sunil G |
| [HADOOP-13368](https://issues.apache.org/jira/browse/HADOOP-13368) | DFSOpsCountStatistics$OpType#fromSymbol and s3a.Statistic#fromSymbol should be O(1) operation |  Major | fs | Mingliang Liu | Mingliang Liu |
| [YARN-5350](https://issues.apache.org/jira/browse/YARN-5350) | Distributed Scheduling: Ensure sort order of allocatable nodes returned by the RM is not lost |  Major | . | Arun Suresh | Arun Suresh |
| [HADOOP-13212](https://issues.apache.org/jira/browse/HADOOP-13212) | Provide an option to set the socket buffers in S3AFileSystem |  Minor | fs/s3 | Rajesh Balamohan | Rajesh Balamohan |
| [HDFS-10653](https://issues.apache.org/jira/browse/HDFS-10653) | Optimize conversion from path string to components |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10565](https://issues.apache.org/jira/browse/HDFS-10565) | Erasure Coding: Document about the current allowed storage policies for EC Striped mode files |  Major | documentation | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-13207](https://issues.apache.org/jira/browse/HADOOP-13207) | Specify FileSystem listStatus, listFiles and RemoteIterator |  Major | documentation, fs | Steve Loughran | Steve Loughran |
| [HADOOP-13188](https://issues.apache.org/jira/browse/HADOOP-13188) | S3A file-create should throw error rather than overwrite directories |  Minor | fs/s3 | Raymie Stata | Steve Loughran |
| [YARN-5164](https://issues.apache.org/jira/browse/YARN-5164) | Use plan RLE to improve CapacityOverTimePolicy efficiency |  Major | capacityscheduler, fairscheduler, resourcemanager | Carlo Curino | Carlo Curino |
| [HADOOP-13041](https://issues.apache.org/jira/browse/HADOOP-13041) | Adding tests for coder utilities |  Major | . | Kai Sasaki | Kai Sasaki |
| [HDFS-10642](https://issues.apache.org/jira/browse/HDFS-10642) | TestLazyPersistReplicaRecovery#testDnRestartWithSavedReplicas fails intermittently |  Major | datanode, test | Mingliang Liu | Mingliang Liu |
| [YARN-5392](https://issues.apache.org/jira/browse/YARN-5392) | Replace use of Priority in the Scheduling infrastructure with an opaque ShedulerRequestKey |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-5351](https://issues.apache.org/jira/browse/YARN-5351) | ResourceRequest should take ExecutionType into account during comparison |  Major | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [HDFS-10668](https://issues.apache.org/jira/browse/HDFS-10668) | TestDataNodeMXBean#testDataNodeMXBeanBlockCount fails intermittently |  Major | test | Mingliang Liu | Mingliang Liu |
| [YARN-5434](https://issues.apache.org/jira/browse/YARN-5434) | Add -client\|server argument for graceful decom |  Blocker | graceful | Robert Kanter | Robert Kanter |
| [YARN-5113](https://issues.apache.org/jira/browse/YARN-5113) | Refactoring and other clean-up for distributed scheduling |  Major | . | Arun Suresh | Konstantinos Karanasos |
| [HADOOP-11540](https://issues.apache.org/jira/browse/HADOOP-11540) | Raw Reed-Solomon coder using Intel ISA-L library |  Major | . | Zhe Zhang | Kai Zheng |
| [HDFS-10655](https://issues.apache.org/jira/browse/HDFS-10655) | Fix path related byte array conversion bugs |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [YARN-5458](https://issues.apache.org/jira/browse/YARN-5458) | Rename DockerStopCommandTest to TestDockerStopCommand |  Trivial | . | Shane Kumpf | Shane Kumpf |
| [YARN-5443](https://issues.apache.org/jira/browse/YARN-5443) | Add support for docker inspect command |  Major | yarn | Shane Kumpf | Shane Kumpf |
| [HADOOP-13429](https://issues.apache.org/jira/browse/HADOOP-13429) | Dispose of unnecessary SASL servers |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [YARN-5226](https://issues.apache.org/jira/browse/YARN-5226) | remove AHS enable check from LogsCLI#fetchAMContainerLogs |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-13426](https://issues.apache.org/jira/browse/HADOOP-13426) | More efficiently build IPC responses |  Major | . | Daryn Sharp | Daryn Sharp |
| [HDFS-10656](https://issues.apache.org/jira/browse/HDFS-10656) | Optimize conversion of byte arrays back to path string |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10674](https://issues.apache.org/jira/browse/HDFS-10674) | Optimize creating a full path from an inode |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [YARN-5342](https://issues.apache.org/jira/browse/YARN-5342) | Improve non-exclusive node partition resource allocation in Capacity Scheduler |  Major | . | Wangda Tan | Sunil G |
| [HADOOP-13438](https://issues.apache.org/jira/browse/HADOOP-13438) | Optimize IPC server protobuf decoding |  Major | . | Daryn Sharp | Daryn Sharp |
| [YARN-5459](https://issues.apache.org/jira/browse/YARN-5459) | Add support for docker rm |  Minor | yarn | Shane Kumpf | Shane Kumpf |
| [HDFS-10662](https://issues.apache.org/jira/browse/HDFS-10662) | Optimize UTF8 string/byte conversions |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10673](https://issues.apache.org/jira/browse/HDFS-10673) | Optimize FSPermissionChecker's internal path usage |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [YARN-5429](https://issues.apache.org/jira/browse/YARN-5429) | Fix @return related javadoc warnings in yarn-api |  Major | . | Vrushali C | Vrushali C |
| [YARN-4888](https://issues.apache.org/jira/browse/YARN-4888) | Changes in scheduler to identify resource-requests explicitly by allocation-id |  Major | resourcemanager | Subru Krishnan | Subru Krishnan |
| [HADOOP-13418](https://issues.apache.org/jira/browse/HADOOP-13418) | Fix javadoc warnings by JDK8 in hadoop-nfs package |  Major | . | Kai Sasaki | Kai Sasaki |
| [YARN-5470](https://issues.apache.org/jira/browse/YARN-5470) | Differentiate exactly match with regex in yarn log CLI |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5394](https://issues.apache.org/jira/browse/YARN-5394) | Remove bind-mount /etc/passwd for Docker containers |  Major | yarn | Zhankun Tang | Zhankun Tang |
| [YARN-5137](https://issues.apache.org/jira/browse/YARN-5137) | Make DiskChecker pluggable in NodeManager |  Major | nodemanager | Ray Chiang | Yufei Gu |
| [HADOOP-11588](https://issues.apache.org/jira/browse/HADOOP-11588) | Benchmark framework and test for erasure coders |  Major | io | Kai Zheng | Rui Li |
| [HDFS-8668](https://issues.apache.org/jira/browse/HDFS-8668) | Erasure Coding: revisit buffer used for encoding and decoding. |  Major | . | Yi Liu | SammiChen |
| [HDFS-10744](https://issues.apache.org/jira/browse/HDFS-10744) | Internally optimize path component resolution |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10724](https://issues.apache.org/jira/browse/HDFS-10724) | Document the caller context config keys |  Minor | ipc, namenode | Mingliang Liu | Mingliang Liu |
| [HDFS-10678](https://issues.apache.org/jira/browse/HDFS-10678) | Documenting NNThroughputBenchmark tool |  Major | benchmarks, test | Mingliang Liu | Mingliang Liu |
| [HDFS-10641](https://issues.apache.org/jira/browse/HDFS-10641) | TestBlockManager#testBlockReportQueueing fails intermittently |  Major | namenode, test | Mingliang Liu | Daryn Sharp |
| [HADOOP-13324](https://issues.apache.org/jira/browse/HADOOP-13324) | s3a tests don't authenticate with S3 frankfurt (or other V4 auth only endpoints) |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-13208](https://issues.apache.org/jira/browse/HADOOP-13208) | S3A listFiles(recursive=true) to do a bulk listObjects instead of walking the pseudo-tree of directories |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-13405](https://issues.apache.org/jira/browse/HADOOP-13405) | doc for “fs.s3a.acl.default” indicates incorrect values |  Minor | fs/s3 | Shen Yinjie | Shen Yinjie |
| [YARN-4676](https://issues.apache.org/jira/browse/YARN-4676) | Automatic and Asynchronous Decommissioning Nodes Status Tracking |  Major | resourcemanager | Daniel Zhi | Daniel Zhi |
| [HDFS-10745](https://issues.apache.org/jira/browse/HDFS-10745) | Directly resolve paths into INodesInPath |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HADOOP-13504](https://issues.apache.org/jira/browse/HADOOP-13504) | Refactor jni\_common to conform to C89 restrictions imposed by Visual Studio 2010 |  Major | io | SammiChen | SammiChen |
| [HDFS-10711](https://issues.apache.org/jira/browse/HDFS-10711) | Optimize FSPermissionChecker group membership check |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HADOOP-13252](https://issues.apache.org/jira/browse/HADOOP-13252) | Tune S3A provider plugin mechanism |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-13446](https://issues.apache.org/jira/browse/HADOOP-13446) | Support running isolated unit tests separate from AWS integration tests. |  Major | fs/s3 | Chris Nauroth | Chris Nauroth |
| [HDFS-10762](https://issues.apache.org/jira/browse/HDFS-10762) | Pass IIP for file status related methods |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-8905](https://issues.apache.org/jira/browse/HDFS-8905) | Refactor DFSInputStream#ReaderStrategy |  Major | erasure-coding | Kai Zheng | SammiChen |
| [HDFS-10772](https://issues.apache.org/jira/browse/HDFS-10772) | Reduce byte/string conversions for get listing |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [YARN-5042](https://issues.apache.org/jira/browse/YARN-5042) | Mount /sys/fs/cgroup into Docker containers as read only mount |  Major | yarn | Varun Vasudev | luhuichun |
| [YARN-5564](https://issues.apache.org/jira/browse/YARN-5564) | Fix typo in RM\_SCHEDULER\_RESERVATION\_THRESHOLD\_INCREMENT\_MULTIPLE |  Trivial | fairscheduler | Ray Chiang | Ray Chiang |
| [YARN-3940](https://issues.apache.org/jira/browse/YARN-3940) | Application moveToQueue should check NodeLabel permission |  Major | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5557](https://issues.apache.org/jira/browse/YARN-5557) | Add localize API to the ContainerManagementProtocol |  Major | . | Jian He | Jian He |
| [HDFS-10768](https://issues.apache.org/jira/browse/HDFS-10768) | Optimize mkdir ops |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [YARN-5327](https://issues.apache.org/jira/browse/YARN-5327) | API changes required to support recurring reservations in the YARN ReservationSystem |  Major | resourcemanager | Subru Krishnan | Sangeetha Abdu Jyothi |
| [YARN-4889](https://issues.apache.org/jira/browse/YARN-4889) | Changes in AMRMClient for identifying resource-requests explicitly |  Major | resourcemanager | Subru Krishnan | Arun Suresh |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2776](https://issues.apache.org/jira/browse/HDFS-2776) | Missing interface annotation on JournalSet |  Trivial | namenode | Todd Lipcon | Brandon Li |
| [HADOOP-8308](https://issues.apache.org/jira/browse/HADOOP-8308) | Support cross-project Jenkins builds |  Major | build | Tom White | Tom White |
| [YARN-3357](https://issues.apache.org/jira/browse/YARN-3357) | Move TestFifoScheduler to FIFO package |  Major | scheduler, test | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11814](https://issues.apache.org/jira/browse/HADOOP-11814) | Reformat hadoop-annotations, o.a.h.classification.tools |  Minor | . | Li Lu | Li Lu |
| [MAPREDUCE-6388](https://issues.apache.org/jira/browse/MAPREDUCE-6388) | Remove deprecation warnings from JobHistoryServer classes |  Minor | jobhistoryserver | Ray Chiang | Ray Chiang |
| [YARN-3026](https://issues.apache.org/jira/browse/YARN-3026) | Move application-specific container allocation logic from LeafQueue to FiCaSchedulerApp |  Major | capacityscheduler | Wangda Tan | Wangda Tan |
| [HDFS-8938](https://issues.apache.org/jira/browse/HDFS-8938) | Extract BlockToMarkCorrupt and ReplicationWork as standalone classes from BlockManager |  Major | . | Mingliang Liu | Mingliang Liu |
| [HDFS-9027](https://issues.apache.org/jira/browse/HDFS-9027) | Refactor o.a.h.hdfs.DataStreamer#isLazyPersist() method |  Major | . | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-6477](https://issues.apache.org/jira/browse/MAPREDUCE-6477) | Replace usage of deprecated NameNode.DEFAULT\_PORT in TestFileSystem |  Major | . | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-6483](https://issues.apache.org/jira/browse/MAPREDUCE-6483) | Replace deprecated method NameNode.getUri() with DFSUtilClient.getNNUri() in TestMRCredentials |  Major | test | Mingliang Liu | Mingliang Liu |
| [HDFS-9130](https://issues.apache.org/jira/browse/HDFS-9130) | Use GenericTestUtils#setLogLevel to the logging level |  Major | . | Mingliang Liu | Mingliang Liu |
| [HADOOP-12446](https://issues.apache.org/jira/browse/HADOOP-12446) | Undeprecate createNonRecursive() |  Major | . | Ted Yu | Ted Yu |
| [HDFS-8979](https://issues.apache.org/jira/browse/HDFS-8979) | Clean up checkstyle warnings in hadoop-hdfs-client module |  Major | . | Mingliang Liu | Mingliang Liu |
| [HADOOP-11791](https://issues.apache.org/jira/browse/HADOOP-11791) | Update src/site/markdown/releases to include old versions of Hadoop |  Major | build, documentation | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-12514](https://issues.apache.org/jira/browse/HADOOP-12514) | Make static fields in GenericTestUtils for assertExceptionContains() package-private and final |  Minor | test | Mingliang Liu | Mingliang Liu |
| [HDFS-9377](https://issues.apache.org/jira/browse/HDFS-9377) | Fix findbugs warnings in FSDirSnapshotOp |  Major | namenode | Mingliang Liu | Mingliang Liu |
| [HADOOP-12567](https://issues.apache.org/jira/browse/HADOOP-12567) | NPE in SaslRpcServer |  Major | . | Sergey Shelukhin | Sergey Shelukhin |
| [YARN-4535](https://issues.apache.org/jira/browse/YARN-4535) | Fix checkstyle error in CapacityScheduler.java |  Trivial | . | Rohith Sharma K S | Naganarasimha G R |
| [YARN-4653](https://issues.apache.org/jira/browse/YARN-4653) | Document YARN security model from the perspective of Application Developers |  Major | site | Steve Loughran | Steve Loughran |
| [HDFS-10200](https://issues.apache.org/jira/browse/HDFS-10200) | Docs for WebHDFS still describe GETDELEGATIONTOKENS operation |  Trivial | documentation | Wellington Chevreuil | Wellington Chevreuil |
| [YARN-5297](https://issues.apache.org/jira/browse/YARN-5297) | Avoid printing a stack trace when recovering an app after the RM restarts |  Major | . | Siddharth Seth | Junping Du |
| [HDFS-10651](https://issues.apache.org/jira/browse/HDFS-10651) | Clean up some configuration related codes about legacy block reader |  Minor | . | Kai Zheng | Youwei Wang |
| [HDFS-9353](https://issues.apache.org/jira/browse/HDFS-9353) | Code and comment mismatch in  JavaKeyStoreProvider |  Trivial | . | nijel | Andras Bokor |
| [HDFS-10718](https://issues.apache.org/jira/browse/HDFS-10718) | Prefer direct ByteBuffer in native RS encoder and decoder |  Major | . | SammiChen | SammiChen |


