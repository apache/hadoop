
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

## Release 3.0.0-alpha2 - 2017-01-25

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13361](https://issues.apache.org/jira/browse/HADOOP-13361) | Modify hadoop\_verify\_user to be consistent with hadoop\_subcommand\_opts (ie more granularity) |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HDFS-6962](https://issues.apache.org/jira/browse/HDFS-6962) | ACL inheritance conflicts with umaskmode |  Critical | security | LINTE | John Zhuge |
| [HADOOP-13341](https://issues.apache.org/jira/browse/HADOOP-13341) | Deprecate HADOOP\_SERVERNAME\_OPTS; replace with (command)\_(subcommand)\_OPTS |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13588](https://issues.apache.org/jira/browse/HADOOP-13588) | ConfServlet should respect Accept request header |  Major | conf | Weiwei Yang | Weiwei Yang |
| [HDFS-10636](https://issues.apache.org/jira/browse/HDFS-10636) | Modify ReplicaInfo to remove the assumption that replica metadata and data are stored in java.io.File. |  Major | datanode, fs | Virajith Jalaparti | Virajith Jalaparti |
| [HADOOP-13218](https://issues.apache.org/jira/browse/HADOOP-13218) | Migrate other Hadoop side tests to prepare for removing WritableRPCEngine |  Major | test | Kai Zheng | Wei Zhou |
| [HDFS-10877](https://issues.apache.org/jira/browse/HDFS-10877) | Make RemoteEditLogManifest.committedTxnId optional in Protocol Buffers |  Major | qjm | Sean Mackrory | Sean Mackrory |
| [HADOOP-13681](https://issues.apache.org/jira/browse/HADOOP-13681) | Reduce Kafka dependencies in hadoop-kafka module |  Major | metrics | Grant Henke | Grant Henke |
| [HADOOP-13678](https://issues.apache.org/jira/browse/HADOOP-13678) | Update jackson from 1.9.13 to 2.x in hadoop-tools |  Major | tools | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-6776](https://issues.apache.org/jira/browse/MAPREDUCE-6776) | yarn.app.mapreduce.client.job.max-retries should have a more useful default |  Major | client | Daniel Templeton | Miklos Szegedi |
| [HADOOP-13699](https://issues.apache.org/jira/browse/HADOOP-13699) | Configuration does not substitute multiple references to the same var |  Critical | conf | Andrew Wang | Andrew Wang |
| [HDFS-10637](https://issues.apache.org/jira/browse/HDFS-10637) | Modifications to remove the assumption that FsVolumes are backed by java.io.File. |  Major | datanode, fs | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-10916](https://issues.apache.org/jira/browse/HDFS-10916) | Switch from "raw" to "system" xattr namespace for erasure coding policy |  Major | erasure-coding | Andrew Wang | Andrew Wang |
| [YARN-4464](https://issues.apache.org/jira/browse/YARN-4464) | Lower the default max applications stored in the RM and store |  Blocker | resourcemanager | KWON BYUNGCHANG | Daniel Templeton |
| [HADOOP-13721](https://issues.apache.org/jira/browse/HADOOP-13721) | Remove stale method ViewFileSystem#getTrashCanLocation |  Minor | viewfs | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-10957](https://issues.apache.org/jira/browse/HDFS-10957) | Retire BKJM from trunk |  Major | ha | Vinayakumar B | Vinayakumar B |
| [YARN-5718](https://issues.apache.org/jira/browse/YARN-5718) | TimelineClient (and other places in YARN) shouldn't over-write HDFS client retry settings which could cause unexpected behavior |  Major | resourcemanager, timelineclient | Junping Du | Junping Du |
| [HADOOP-13560](https://issues.apache.org/jira/browse/HADOOP-13560) | S3ABlockOutputStream to support huge (many GB) file writes |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [MAPREDUCE-6791](https://issues.apache.org/jira/browse/MAPREDUCE-6791) | remove unnecessary dependency from hadoop-mapreduce-client-jobclient to hadoop-mapreduce-client-shuffle |  Minor | mrv2 | Haibo Chen | Haibo Chen |
| [HADOOP-7352](https://issues.apache.org/jira/browse/HADOOP-7352) | FileSystem#listStatus should throw IOE upon access error |  Major | fs | Matt Foley | John Zhuge |
| [HADOOP-13693](https://issues.apache.org/jira/browse/HADOOP-13693) | Remove the message about HTTP OPTIONS in SPNEGO initialization message from kms audit log |  Minor | kms | Xiao Chen | Xiao Chen |
| [YARN-5388](https://issues.apache.org/jira/browse/YARN-5388) | Deprecate and remove DockerContainerExecutor |  Critical | nodemanager | Daniel Templeton | Daniel Templeton |
| [YARN-3732](https://issues.apache.org/jira/browse/YARN-3732) | Change NodeHeartbeatResponse.java and RegisterNodeManagerResponse.java as abstract classes |  Minor | . | Devaraj K | Devaraj K |
| [HDFS-11048](https://issues.apache.org/jira/browse/HDFS-11048) | Audit Log should escape control characters |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-13792](https://issues.apache.org/jira/browse/HADOOP-13792) | Stackoverflow for schemeless defaultFS with trailing slash |  Major | fs | Darius Murawski | John Zhuge |
| [HDFS-10970](https://issues.apache.org/jira/browse/HDFS-10970) | Update jackson from 1.9.13 to 2.x in hadoop-hdfs |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11116](https://issues.apache.org/jira/browse/HDFS-11116) | Fix javac warnings caused by deprecation of APIs in TestViewFsDefaultValue |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-12718](https://issues.apache.org/jira/browse/HADOOP-12718) | Incorrect error message by fs -put local dir without permission |  Major | . | John Zhuge | John Zhuge |
| [HADOOP-13660](https://issues.apache.org/jira/browse/HADOOP-13660) | Upgrade commons-configuration version to 2.1 |  Major | build | Sean Mackrory | Sean Mackrory |
| [HADOOP-12705](https://issues.apache.org/jira/browse/HADOOP-12705) | Upgrade Jackson 2.2.3 to 2.7.8 |  Major | build | Steve Loughran | Sean Mackrory |
| [YARN-5713](https://issues.apache.org/jira/browse/YARN-5713) | Update jackson from 1.9.13 to 2.x in hadoop-yarn |  Major | build, timelineserver | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-13050](https://issues.apache.org/jira/browse/HADOOP-13050) | Upgrade to AWS SDK 1.11.45 |  Blocker | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-13706](https://issues.apache.org/jira/browse/HADOOP-13706) | Update jackson from 1.9.13 to 2.x in hadoop-common-project |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-13812](https://issues.apache.org/jira/browse/HADOOP-13812) | Upgrade Tomcat to 6.0.48 |  Blocker | kms | John Zhuge | John Zhuge |
| [HDFS-5517](https://issues.apache.org/jira/browse/HDFS-5517) | Lower the default maximum number of blocks per file |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-13842](https://issues.apache.org/jira/browse/HADOOP-13842) | Update jackson from 1.9.13 to 2.x in hadoop-maven-plugins |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-4683](https://issues.apache.org/jira/browse/MAPREDUCE-4683) | Create and distribute hadoop-mapreduce-client-core-tests.jar |  Critical | build | Arun C Murthy | Akira Ajisaka |
| [HADOOP-13597](https://issues.apache.org/jira/browse/HADOOP-13597) | Switch KMS from Tomcat to Jetty |  Major | kms | John Zhuge | John Zhuge |
| [YARN-6071](https://issues.apache.org/jira/browse/YARN-6071) | Fix incompatible API change on AM-RM protocol due to YARN-3866 (trunk only) |  Blocker | . | Junping Du | Wangda Tan |
| [HADOOP-13964](https://issues.apache.org/jira/browse/HADOOP-13964) | Remove vestigal templates directories creation |  Major | build | Allen Wittenauer | Allen Wittenauer |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-12756](https://issues.apache.org/jira/browse/HADOOP-12756) | Incorporate Aliyun OSS file system implementation |  Major | fs, fs/oss | shimingfei | mingfei.shi |
| [MAPREDUCE-6774](https://issues.apache.org/jira/browse/MAPREDUCE-6774) | Add support for HDFS erasure code policy to TestDFSIO |  Major | . | Sammi Chen | Sammi Chen |
| [HDFS-10918](https://issues.apache.org/jira/browse/HDFS-10918) | Add a tool to get FileEncryptionInfo from CLI |  Major | encryption | Xiao Chen | Xiao Chen |
| [HADOOP-13584](https://issues.apache.org/jira/browse/HADOOP-13584) | hadoop-aliyun: merge HADOOP-12756 branch back |  Major | fs | shimingfei | Genmao Yu |
| [HDFS-9820](https://issues.apache.org/jira/browse/HDFS-9820) | Improve distcp to support efficient restore to an earlier snapshot |  Major | distcp | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-13716](https://issues.apache.org/jira/browse/HADOOP-13716) | Add LambdaTestUtils class for tests; fix eventual consistency problem in contract test setup |  Major | test | Steve Loughran | Steve Loughran |
| [YARN-4597](https://issues.apache.org/jira/browse/YARN-4597) | Introduce ContainerScheduler and a SCHEDULED state to NodeManager container lifecycle |  Major | nodemanager | Chris Douglas | Arun Suresh |
| [HADOOP-13852](https://issues.apache.org/jira/browse/HADOOP-13852) | hadoop build to allow hadoop version property to be explicitly set |  Minor | build | Steve Loughran | Steve Loughran |
| [HADOOP-13578](https://issues.apache.org/jira/browse/HADOOP-13578) | Add Codec for ZStandard Compression |  Major | . | churro morales | churro morales |
| [HADOOP-13933](https://issues.apache.org/jira/browse/HADOOP-13933) | Add haadmin -getAllServiceState option to get the HA state of all the NameNodes/ResourceManagers |  Major | tools | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-13673](https://issues.apache.org/jira/browse/HADOOP-13673) | Update scripts to be smarter when running with privilege |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-2877](https://issues.apache.org/jira/browse/YARN-2877) | Extend YARN to support distributed scheduling |  Major | nodemanager, resourcemanager | Sriram Rao | Konstantinos Karanasos |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-10814](https://issues.apache.org/jira/browse/HDFS-10814) | Add assertion for getNumEncryptionZones when no EZ is created |  Minor | test | Vinitha Reddy Gankidi | Vinitha Reddy Gankidi |
| [HDFS-10784](https://issues.apache.org/jira/browse/HDFS-10784) | Implement WebHdfsFileSystem#listStatusIterator |  Major | webhdfs | Andrew Wang | Andrew Wang |
| [HDFS-10817](https://issues.apache.org/jira/browse/HDFS-10817) | Add Logging for Long-held NN Read Locks |  Major | logging, namenode | Erik Krogen | Erik Krogen |
| [HADOOP-13465](https://issues.apache.org/jira/browse/HADOOP-13465) | Design Server.Call to be extensible for unified call queue |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-10822](https://issues.apache.org/jira/browse/HDFS-10822) | Log DataNodes in the write pipeline |  Trivial | hdfs-client | John Zhuge | John Zhuge |
| [HDFS-10833](https://issues.apache.org/jira/browse/HDFS-10833) | Fix JSON errors in WebHDFS.md examples |  Trivial | documentation | Andrew Wang | Andrew Wang |
| [YARN-5616](https://issues.apache.org/jira/browse/YARN-5616) | Clean up WeightAdjuster |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [HDFS-10778](https://issues.apache.org/jira/browse/HDFS-10778) | Add -format option to make the output of FileDistribution processor human-readable in OfflineImageViewer |  Major | tools | Yiqun Lin | Yiqun Lin |
| [HDFS-10847](https://issues.apache.org/jira/browse/HDFS-10847) | Complete the document for FileDistribution processor in OfflineImageViewer |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-13519](https://issues.apache.org/jira/browse/HADOOP-13519) | Make Path serializable |  Minor | io | Steve Loughran | Steve Loughran |
| [HDFS-10742](https://issues.apache.org/jira/browse/HDFS-10742) | Measure lock time in FsDatasetImpl |  Major | datanode | Chen Liang | Chen Liang |
| [HDFS-10831](https://issues.apache.org/jira/browse/HDFS-10831) | Add log when URLConnectionFactory.openConnection failed |  Minor | webhdfs | yunjiong zhao | yunjiong zhao |
| [HDFS-10855](https://issues.apache.org/jira/browse/HDFS-10855) | Fix typos in HDFS documents |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-10837](https://issues.apache.org/jira/browse/HDFS-10837) | Standardize serializiation of WebHDFS DirectoryListing |  Major | webhdfs | Andrew Wang | Andrew Wang |
| [HADOOP-13598](https://issues.apache.org/jira/browse/HADOOP-13598) | Add eol=lf for unix format files in .gitattributes |  Major | . | Akira Ajisaka | Yiqun Lin |
| [HADOOP-13411](https://issues.apache.org/jira/browse/HADOOP-13411) | Checkstyle suppression by annotation or comment |  Minor | . | John Zhuge | John Zhuge |
| [HADOOP-13412](https://issues.apache.org/jira/browse/HADOOP-13412) | Move dev-support/checkstyle/suppressions.xml to hadoop-build-tools |  Trivial | . | John Zhuge | John Zhuge |
| [HADOOP-13580](https://issues.apache.org/jira/browse/HADOOP-13580) | If user is unauthorized, log "unauthorized" instead of "Invalid signed text:" |  Minor | security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-10823](https://issues.apache.org/jira/browse/HDFS-10823) | Implement HttpFSFileSystem#listStatusIterator |  Major | httpfs | Andrew Wang | Andrew Wang |
| [HDFS-10489](https://issues.apache.org/jira/browse/HDFS-10489) | Deprecate dfs.encryption.key.provider.uri for HDFS encryption zones |  Minor | . | Xiao Chen | Xiao Chen |
| [HDFS-10868](https://issues.apache.org/jira/browse/HDFS-10868) | Remove stray references to DFS\_HDFS\_BLOCKS\_METADATA\_ENABLED |  Trivial | . | Andrew Wang | Andrew Wang |
| [YARN-5540](https://issues.apache.org/jira/browse/YARN-5540) | scheduler spends too much time looking at empty priorities |  Major | capacity scheduler, fairscheduler, resourcemanager | Nathan Roberts | Jason Lowe |
| [HDFS-10875](https://issues.apache.org/jira/browse/HDFS-10875) | Optimize du -x to cache intermediate result |  Major | snapshots | Xiao Chen | Xiao Chen |
| [YARN-4591](https://issues.apache.org/jira/browse/YARN-4591) | YARN Web UIs should provide a robots.txt |  Trivial | . | Lars Francke | Sidharta Seethana |
| [MAPREDUCE-6632](https://issues.apache.org/jira/browse/MAPREDUCE-6632) | Master.getMasterAddress() should be updated to use YARN-4629 |  Minor | applicationmaster | Daniel Templeton | Daniel Templeton |
| [YARN-5622](https://issues.apache.org/jira/browse/YARN-5622) | TestYarnCLI.testGetContainers fails due to mismatched date formats |  Minor | . | Eric Badger | Eric Badger |
| [HDFS-10876](https://issues.apache.org/jira/browse/HDFS-10876) | Dispatcher#dispatch should log IOException stacktrace |  Trivial | balancer & mover | Wei-Chiu Chuang | Manoj Govindassamy |
| [YARN-3692](https://issues.apache.org/jira/browse/YARN-3692) | Allow REST API to set a user generated message when killing an application |  Major | . | Rajat Jain | Rohith Sharma K S |
| [HDFS-10869](https://issues.apache.org/jira/browse/HDFS-10869) | Remove the unused method InodeId#checkId() |  Major | namenode | Jagadesh Kiran N | Jagadesh Kiran N |
| [YARN-3877](https://issues.apache.org/jira/browse/YARN-3877) | YarnClientImpl.submitApplication swallows exceptions |  Minor | client | Steve Loughran | Varun Saxena |
| [HADOOP-13658](https://issues.apache.org/jira/browse/HADOOP-13658) | Replace config key literal strings with config key names I: hadoop common |  Minor | conf | Chen Liang | Chen Liang |
| [YARN-5400](https://issues.apache.org/jira/browse/YARN-5400) | Light cleanup in ZKRMStateStore |  Trivial | resourcemanager | Daniel Templeton | Daniel Templeton |
| [MAPREDUCE-6718](https://issues.apache.org/jira/browse/MAPREDUCE-6718) | add progress log to JHS during startup |  Minor | jobhistoryserver | Haibo Chen | Haibo Chen |
| [HADOOP-13537](https://issues.apache.org/jira/browse/HADOOP-13537) | Support external calls in the RPC call queue |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-10908](https://issues.apache.org/jira/browse/HDFS-10908) | Improve StripedBlockReader#createBlockReader error logging |  Minor | erasure-coding | Wei-Chiu Chuang | Manoj Govindassamy |
| [HDFS-10910](https://issues.apache.org/jira/browse/HDFS-10910) | HDFS Erasure Coding doc should state its currently supported erasure coding policies |  Major | documentation, erasure-coding | Wei-Chiu Chuang | Yiqun Lin |
| [HADOOP-13317](https://issues.apache.org/jira/browse/HADOOP-13317) | Add logs to KMS server-side to improve supportability |  Minor | kms | Xiao Chen | Suraj Acharya |
| [HDFS-10940](https://issues.apache.org/jira/browse/HDFS-10940) | Reduce performance penalty of block caching when not used |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-6638](https://issues.apache.org/jira/browse/MAPREDUCE-6638) | Do not attempt to recover progress from previous job attempts if spill encryption is enabled |  Major | applicationmaster | Karthik Kambatla | Haibo Chen |
| [YARN-4855](https://issues.apache.org/jira/browse/YARN-4855) | Should check if node exists when replace nodelabels |  Minor | . | Tao Jie | Tao Jie |
| [HDFS-10690](https://issues.apache.org/jira/browse/HDFS-10690) | Optimize insertion/removal of replica in ShortCircuitCache |  Major | hdfs-client | Fenghua Hu | Fenghua Hu |
| [HADOOP-13685](https://issues.apache.org/jira/browse/HADOOP-13685) | Document -safely option of rm command. |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-13628](https://issues.apache.org/jira/browse/HADOOP-13628) | Support to retrieve specific property from configuration via REST API |  Major | conf | Weiwei Yang | Weiwei Yang |
| [HDFS-10683](https://issues.apache.org/jira/browse/HDFS-10683) | Make class Token$PrivateToken private |  Minor | . | John Zhuge | John Zhuge |
| [HDFS-10963](https://issues.apache.org/jira/browse/HDFS-10963) | Reduce log level when network topology cannot find enough datanodes. |  Minor | . | Xiao Chen | Xiao Chen |
| [HADOOP-13323](https://issues.apache.org/jira/browse/HADOOP-13323) | Downgrade stack trace on FS load from Warn to debug |  Minor | fs | Steve Loughran | Steve Loughran |
| [HADOOP-13688](https://issues.apache.org/jira/browse/HADOOP-13688) | Stop bundling HTML source code in javadoc JARs |  Major | build | Andrew Wang | Andrew Wang |
| [HADOOP-13150](https://issues.apache.org/jira/browse/HADOOP-13150) | Avoid use of toString() in output of HDFS ACL shell commands. |  Minor | . | Chris Nauroth | Chris Nauroth |
| [HADOOP-13689](https://issues.apache.org/jira/browse/HADOOP-13689) | Do not attach javadoc and sources jars during non-dist build |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-12579](https://issues.apache.org/jira/browse/HADOOP-12579) | Deprecate WriteableRPCEngine |  Major | . | Haohui Mai | Wei Zhou |
| [HADOOP-13641](https://issues.apache.org/jira/browse/HADOOP-13641) | Update UGI#spawnAutoRenewalThreadForUserCreds to reduce indentation |  Minor | . | Xiao Chen | Huafeng Wang |
| [YARN-5551](https://issues.apache.org/jira/browse/YARN-5551) | Ignore file backed pages from memory computation when smaps is enabled |  Minor | . | Rajesh Balamohan | Rajesh Balamohan |
| [HDFS-10933](https://issues.apache.org/jira/browse/HDFS-10933) | Refactor TestFsck |  Minor | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-13684](https://issues.apache.org/jira/browse/HADOOP-13684) | Snappy may complain Hadoop is built without snappy if libhadoop is not found. |  Minor | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13705](https://issues.apache.org/jira/browse/HADOOP-13705) | Revert HADOOP-13534 Remove unused TrashPolicy#getInstance and initialize code |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-13698](https://issues.apache.org/jira/browse/HADOOP-13698) | Document caveat for KeyShell when underlying KeyProvider does not delete a key |  Minor | documentation, kms | Xiao Chen | Xiao Chen |
| [HDFS-10789](https://issues.apache.org/jira/browse/HDFS-10789) | Route webhdfs through the RPC call queue |  Major | ipc, webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10903](https://issues.apache.org/jira/browse/HDFS-10903) | Replace config key literal strings with config key names II: hadoop hdfs |  Minor | . | Mingliang Liu | Chen Liang |
| [HADOOP-13710](https://issues.apache.org/jira/browse/HADOOP-13710) | Supress CachingGetSpaceUsed from logging interrupted exception stacktrace |  Minor | fs | Wei-Chiu Chuang | Hanisha Koneru |
| [YARN-5599](https://issues.apache.org/jira/browse/YARN-5599) | Publish AM launch command to ATS |  Major | . | Daniel Templeton | Rohith Sharma K S |
| [HDFS-11012](https://issues.apache.org/jira/browse/HDFS-11012) | Unnecessary INFO logging on DFSClients for InvalidToken |  Minor | fs | Harsh J | Harsh J |
| [HDFS-11003](https://issues.apache.org/jira/browse/HDFS-11003) | Expose "XmitsInProgress" through DataNodeMXBean |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-13661](https://issues.apache.org/jira/browse/HADOOP-13661) | Upgrade HTrace version |  Major | . | Sean Mackrory | Sean Mackrory |
| [HADOOP-13722](https://issues.apache.org/jira/browse/HADOOP-13722) | Code cleanup -- ViewFileSystem and InodeTree |  Minor | viewfs | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-13724](https://issues.apache.org/jira/browse/HADOOP-13724) | Fix a few typos in site markdown documents |  Minor | documentation | Andrew Wang | Ding Fei |
| [YARN-5466](https://issues.apache.org/jira/browse/YARN-5466) | DefaultContainerExecutor needs JavaDocs |  Minor | nodemanager | Daniel Templeton | Daniel Templeton |
| [HADOOP-13522](https://issues.apache.org/jira/browse/HADOOP-13522) | Add %A and %a formats for fs -stat command to print permissions |  Major | fs | Alex Garbarini | Alex Garbarini |
| [HDFS-11009](https://issues.apache.org/jira/browse/HDFS-11009) | Add a tool to reconstruct block meta file from CLI |  Major | datanode | Xiao Chen | Xiao Chen |
| [HDFS-9480](https://issues.apache.org/jira/browse/HDFS-9480) |  Expose nonDfsUsed via StorageTypeStats |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-13737](https://issues.apache.org/jira/browse/HADOOP-13737) | Cleanup DiskChecker interface |  Major | util | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-13702](https://issues.apache.org/jira/browse/HADOOP-13702) | Add a new instrumented read-write lock |  Major | common | Jingcheng Du | Jingcheng Du |
| [HADOOP-13732](https://issues.apache.org/jira/browse/HADOOP-13732) | Upgrade OWASP dependency-check plugin version |  Minor | security | Mike Yoder | Mike Yoder |
| [HADOOP-12082](https://issues.apache.org/jira/browse/HADOOP-12082) | Support multiple authentication schemes via AuthenticationFilter |  Major | security | Hrishikesh Gadre | Hrishikesh Gadre |
| [HADOOP-13669](https://issues.apache.org/jira/browse/HADOOP-13669) | KMS Server should log exceptions before throwing |  Major | kms | Xiao Chen | Suraj Acharya |
| [MAPREDUCE-6792](https://issues.apache.org/jira/browse/MAPREDUCE-6792) | Allow user's full principal name as owner of MapReduce staging directory in JobSubmissionFiles#JobStagingDir() |  Major | client | Santhosh G Nayak | Santhosh G Nayak |
| [HDFS-5684](https://issues.apache.org/jira/browse/HDFS-5684) | Annotate o.a.h.fs.viewfs.ViewFileSystem.MountPoint as VisibleForTesting |  Minor | hdfs-client | Keith Turner | Manoj Govindassamy |
| [YARN-5575](https://issues.apache.org/jira/browse/YARN-5575) | Many classes use bare yarn. properties instead of the defined constants |  Major | . | Daniel Templeton | Daniel Templeton |
| [HADOOP-13659](https://issues.apache.org/jira/browse/HADOOP-13659) | Upgrade jaxb-api version |  Major | build | Sean Mackrory | Sean Mackrory |
| [HADOOP-13502](https://issues.apache.org/jira/browse/HADOOP-13502) | Split fs.contract.is-blobstore flag into more descriptive flags for use by contract tests. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-11049](https://issues.apache.org/jira/browse/HDFS-11049) | The description of dfs.block.replicator.classname is not clear |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-11069](https://issues.apache.org/jira/browse/HDFS-11069) | Tighten the authorization of datanode RPC |  Major | datanode, security | Kihwal Lee | Kihwal Lee |
| [YARN-4456](https://issues.apache.org/jira/browse/YARN-4456) | Clean up Lint warnings in nodemanager |  Minor | nodemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-11055](https://issues.apache.org/jira/browse/HDFS-11055) | Update default-log4j.properties for httpfs to imporve test logging |  Major | httpfs, test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4710](https://issues.apache.org/jira/browse/YARN-4710) | Reduce logging application reserved debug info in FSAppAttempt#assignContainer |  Minor | fairscheduler | Yiqun Lin | Yiqun Lin |
| [YARN-4668](https://issues.apache.org/jira/browse/YARN-4668) | Reuse objectMapper instance in Yarn |  Major | timelineclient | Yiqun Lin | Yiqun Lin |
| [HDFS-11064](https://issues.apache.org/jira/browse/HDFS-11064) | Mention the default NN rpc ports in hdfs-default.xml |  Minor | documentation | Andrew Wang | Yiqun Lin |
| [HDFS-10926](https://issues.apache.org/jira/browse/HDFS-10926) | Update staled configuration properties related to erasure coding |  Major | . | Sammi Chen | Sammi Chen |
| [YARN-4963](https://issues.apache.org/jira/browse/YARN-4963) | capacity scheduler: Make number of OFF\_SWITCH assignments per heartbeat configurable |  Major | capacityscheduler | Nathan Roberts | Nathan Roberts |
| [HDFS-11047](https://issues.apache.org/jira/browse/HDFS-11047) | Remove deep copies of FinalizedReplica to alleviate heap consumption on DataNode |  Major | datanode | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-10075](https://issues.apache.org/jira/browse/HADOOP-10075) | Update jetty dependency to version 9 |  Critical | . | Robert Rati | Robert Kanter |
| [MAPREDUCE-6799](https://issues.apache.org/jira/browse/MAPREDUCE-6799) | Document mapreduce.jobhistory.webapp.https.address in mapred-default.xml |  Minor | documentation, jobhistoryserver | Akira Ajisaka | Yiqun Lin |
| [HDFS-11074](https://issues.apache.org/jira/browse/HDFS-11074) | Remove unused method FsDatasetSpi#getFinalizedBlocksOnPersistentStorage |  Major | datanode | Arpit Agarwal | Hanisha Koneru |
| [YARN-4907](https://issues.apache.org/jira/browse/YARN-4907) | Make all MockRM#waitForState consistent. |  Major | resourcemanager | Yufei Gu | Yufei Gu |
| [YARN-4396](https://issues.apache.org/jira/browse/YARN-4396) | Log the trace information on FSAppAttempt#assignContainer |  Major | applications, fairscheduler | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-6795](https://issues.apache.org/jira/browse/MAPREDUCE-6795) | Update the document for JobConf#setNumReduceTasks |  Major | documentation | Akira Ajisaka | Yiqun Lin |
| [HADOOP-13603](https://issues.apache.org/jira/browse/HADOOP-13603) | Ignore package line length checkstyle rule |  Major | build | Shane Kumpf | Shane Kumpf |
| [HADOOP-13583](https://issues.apache.org/jira/browse/HADOOP-13583) | Incorporate checkcompatibility script which runs Java API Compliance Checker |  Major | scripts | Andrew Wang | Andrew Wang |
| [HADOOP-13667](https://issues.apache.org/jira/browse/HADOOP-13667) | Fix typing mistake of inline document in hadoop-metrics2.properties |  Major | documentation | Rui Gao | Rui Gao |
| [HDFS-10909](https://issues.apache.org/jira/browse/HDFS-10909) | De-duplicate code in ErasureCodingWorker#initializeStripedReadThreadPool and DFSClient#initThreadsNumForStripedReads |  Minor | . | Wei-Chiu Chuang | Manoj Govindassamy |
| [HADOOP-13784](https://issues.apache.org/jira/browse/HADOOP-13784) | Output javadoc inside the target directory |  Major | documentation | Andrew Wang | Andrew Wang |
| [HDFS-11080](https://issues.apache.org/jira/browse/HDFS-11080) | Update HttpFS to use ConfigRedactor |  Major | . | Sean Mackrory | Sean Mackrory |
| [YARN-5697](https://issues.apache.org/jira/browse/YARN-5697) | Use CliParser to parse options in RMAdminCLI |  Major | resourcemanager | Tao Jie | Tao Jie |
| [HADOOP-12453](https://issues.apache.org/jira/browse/HADOOP-12453) | Support decoding KMS Delegation Token with its own Identifier |  Major | kms, security | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-11088](https://issues.apache.org/jira/browse/HDFS-11088) | Quash unnecessary safemode WARN message during NameNode startup |  Trivial | . | Andrew Wang | Yiqun Lin |
| [MAPREDUCE-6796](https://issues.apache.org/jira/browse/MAPREDUCE-6796) | Remove unused properties from JTConfig.java |  Major | . | Akira Ajisaka | Haibo Chen |
| [YARN-4998](https://issues.apache.org/jira/browse/YARN-4998) | Minor cleanup to UGI use in AdminService |  Trivial | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-10756](https://issues.apache.org/jira/browse/HDFS-10756) | Expose getTrashRoot to HTTPFS and WebHDFS |  Major | encryption, httpfs, webhdfs | Xiao Chen | Yuanbo Liu |
| [MAPREDUCE-6790](https://issues.apache.org/jira/browse/MAPREDUCE-6790) | Update jackson from 1.9.13 to 2.x in hadoop-mapreduce |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-5720](https://issues.apache.org/jira/browse/YARN-5720) | Update document for "rmadmin -replaceLabelOnNode" |  Minor | . | Tao Jie | Tao Jie |
| [YARN-5356](https://issues.apache.org/jira/browse/YARN-5356) | NodeManager should communicate physical resource capability to ResourceManager |  Major | nodemanager, resourcemanager | Nathan Roberts | Íñigo Goiri |
| [HADOOP-13802](https://issues.apache.org/jira/browse/HADOOP-13802) | Make generic options help more consistent, and aligned |  Minor | . | Grant Sohn | Grant Sohn |
| [HADOOP-13782](https://issues.apache.org/jira/browse/HADOOP-13782) | Make MutableRates metrics thread-local write, aggregate-on-read |  Major | metrics | Erik Krogen | Erik Krogen |
| [HDFS-9482](https://issues.apache.org/jira/browse/HDFS-9482) | Replace DatanodeInfo constructors with a builder pattern |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-13800](https://issues.apache.org/jira/browse/HADOOP-13800) | Remove unused HADOOP\_AUDIT\_LOGGER from hadoop-env.sh |  Minor | scripts | Akira Ajisaka | Yiqun Lin |
| [HADOOP-13590](https://issues.apache.org/jira/browse/HADOOP-13590) | Retry until TGT expires even if the UGI renewal thread encountered exception |  Major | security | Xiao Chen | Xiao Chen |
| [HDFS-11120](https://issues.apache.org/jira/browse/HDFS-11120) | TestEncryptionZones should waitActive |  Minor | test | Xiao Chen | John Zhuge |
| [HADOOP-13687](https://issues.apache.org/jira/browse/HADOOP-13687) | Provide a unified dependency artifact that transitively includes the cloud storage modules shipped with Hadoop. |  Major | build | Chris Nauroth | Chris Nauroth |
| [YARN-5552](https://issues.apache.org/jira/browse/YARN-5552) | Add Builder methods for common yarn API records |  Major | . | Arun Suresh | Tao Jie |
| [HDFS-10941](https://issues.apache.org/jira/browse/HDFS-10941) | Improve BlockManager#processMisReplicatesAsync log |  Major | namenode | Xiaoyu Yao | Chen Liang |
| [YARN-4033](https://issues.apache.org/jira/browse/YARN-4033) | In FairScheduler, parent queues should also display queue status |  Major | fairscheduler | Siqi Li | Siqi Li |
| [HADOOP-13810](https://issues.apache.org/jira/browse/HADOOP-13810) | Add a test to verify that Configuration handles &-encoded characters |  Minor | test | Steve Loughran | Steve Loughran |
| [HADOOP-13427](https://issues.apache.org/jira/browse/HADOOP-13427) | Eliminate needless uses of FileSystem#{exists(), isFile(), isDirectory()} |  Major | fs | Steve Loughran | Mingliang Liu |
| [YARN-5736](https://issues.apache.org/jira/browse/YARN-5736) | YARN container executor config does not handle white space |  Trivial | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-5375](https://issues.apache.org/jira/browse/YARN-5375) | invoke MockRM#drainEvents implicitly in MockRM methods to reduce test failures |  Major | resourcemanager | sandflee | sandflee |
| [HDFS-11147](https://issues.apache.org/jira/browse/HDFS-11147) | Remove confusing log output in FsDatasetImpl#getInitialVolumeFailureInfos |  Minor | datanode | Chen Liang | Chen Liang |
| [HADOOP-13742](https://issues.apache.org/jira/browse/HADOOP-13742) | Expose "NumOpenConnectionsPerUser" as a metric |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11117](https://issues.apache.org/jira/browse/HDFS-11117) | Refactor striped file tests to allow flexibly test erasure coding policy |  Major | . | Sammi Chen | Sammi Chen |
| [HADOOP-13646](https://issues.apache.org/jira/browse/HADOOP-13646) | Remove outdated overview.html |  Minor | . | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-5722](https://issues.apache.org/jira/browse/YARN-5722) | FairScheduler hides group resolution exceptions when assigning queue |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-13166](https://issues.apache.org/jira/browse/HADOOP-13166) | add getFileStatus("/") test to AbstractContractGetFileStatusTest |  Minor | fs, test | Steve Loughran | Steve Loughran |
| [HADOOP-11552](https://issues.apache.org/jira/browse/HADOOP-11552) | Allow handoff on the server side for RPC requests |  Major | ipc | Siddharth Seth | Siddharth Seth |
| [HADOOP-13605](https://issues.apache.org/jira/browse/HADOOP-13605) | Clean up FileSystem javadocs, logging; improve diagnostics on FS load |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-13018](https://issues.apache.org/jira/browse/HADOOP-13018) | Make Kdiag check whether hadoop.token.files points to existent and valid files |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-11603](https://issues.apache.org/jira/browse/HADOOP-11603) | Metric Snapshot log can be changed #MetricsSystemImpl.java since all the services will be initialized |  Minor | metrics | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-10776](https://issues.apache.org/jira/browse/HADOOP-10776) | Open up already widely-used APIs for delegation-token fetching & renewal to ecosystem projects |  Blocker | . | Robert Joseph Evans | Vinod Kumar Vavilapalli |
| [HDFS-11175](https://issues.apache.org/jira/browse/HDFS-11175) | Document uppercase key names are not supported in TransparentEncryption.md |  Minor | documentation | Yuanbo Liu | Yiqun Lin |
| [HADOOP-1381](https://issues.apache.org/jira/browse/HADOOP-1381) | The distance between sync blocks in SequenceFiles should be configurable |  Major | io | Owen O'Malley | Harsh J |
| [HADOOP-13506](https://issues.apache.org/jira/browse/HADOOP-13506) | Redundant groupid warning in child projects |  Minor | . | Kai Sasaki | Kai Sasaki |
| [YARN-5890](https://issues.apache.org/jira/browse/YARN-5890) | FairScheduler should log information about AM-resource-usage and max-AM-share for queues |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-4997](https://issues.apache.org/jira/browse/YARN-4997) | Update fair scheduler to use pluggable auth provider |  Major | fairscheduler | Daniel Templeton | Tao Jie |
| [HADOOP-13790](https://issues.apache.org/jira/browse/HADOOP-13790) | Make qbt script executable |  Trivial | scripts | Andrew Wang | Andrew Wang |
| [HDFS-8674](https://issues.apache.org/jira/browse/HDFS-8674) | Improve performance of postponed block scans |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-6787](https://issues.apache.org/jira/browse/MAPREDUCE-6787) | Allow job\_conf.xml to be downloadable on the job overview page in JHS |  Major | jobhistoryserver | Haibo Chen | Haibo Chen |
| [HDFS-10581](https://issues.apache.org/jira/browse/HDFS-10581) | Hide redundant table on NameNode WebUI when no nodes are decomissioning |  Trivial | hdfs, ui | Weiwei Yang | Weiwei Yang |
| [HDFS-11211](https://issues.apache.org/jira/browse/HDFS-11211) | Add a time unit to the DataNode client trace format |  Minor | datanode | Akira Ajisaka | Jagadesh Kiran N |
| [HADOOP-13827](https://issues.apache.org/jira/browse/HADOOP-13827) | Add reencryptEncryptedKey interface to KMS |  Major | kms | Xiao Chen | Xiao Chen |
| [HDFS-10206](https://issues.apache.org/jira/browse/HDFS-10206) | Datanodes not sorted properly by distance when the reader isn't a datanode |  Major | . | Ming Ma | Nanda kumar |
| [HDFS-11217](https://issues.apache.org/jira/browse/HDFS-11217) | Annotate NameNode and DataNode MXBean interfaces as Private/Stable |  Major | . | Akira Ajisaka | Jagadesh Kiran N |
| [YARN-4457](https://issues.apache.org/jira/browse/YARN-4457) | Cleanup unchecked types for EventHandler |  Major | . | Daniel Templeton | Daniel Templeton |
| [HADOOP-13900](https://issues.apache.org/jira/browse/HADOOP-13900) | Remove snapshot version of SDK dependency from Azure Data Lake Store File System |  Major | fs/adl | Vishwajeet Dusane | Vishwajeet Dusane |
| [HDFS-10958](https://issues.apache.org/jira/browse/HDFS-10958) | Add instrumentation hooks around Datanode disk IO |  Major | datanode | Xiaoyu Yao | Arpit Agarwal |
| [HDFS-11249](https://issues.apache.org/jira/browse/HDFS-11249) | Redundant toString() in DFSConfigKeys.java |  Trivial | . | Akira Ajisaka | Jagadesh Kiran N |
| [YARN-5882](https://issues.apache.org/jira/browse/YARN-5882) | Test only changes from YARN-4126 |  Major | . | Andrew Wang | Jian He |
| [HADOOP-13709](https://issues.apache.org/jira/browse/HADOOP-13709) | Ability to clean up subprocesses spawned by Shell when the process exits |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11262](https://issues.apache.org/jira/browse/HDFS-11262) | Remove unused variables in FSImage.java |  Trivial | . | Akira Ajisaka | Jagadesh Kiran N |
| [HDFS-10930](https://issues.apache.org/jira/browse/HDFS-10930) | Refactor: Wrap Datanode IO related operations |  Major | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-10959](https://issues.apache.org/jira/browse/HDFS-10959) | Adding per disk IO statistics and metrics in DataNode. |  Major | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-10913](https://issues.apache.org/jira/browse/HDFS-10913) | Introduce fault injectors to simulate slow mirrors |  Major | datanode, test | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-13911](https://issues.apache.org/jira/browse/HADOOP-13911) | Remove TRUSTSTORE\_PASSWORD related scripts from KMS |  Minor | kms | Xiao Chen | John Zhuge |
| [YARN-4994](https://issues.apache.org/jira/browse/YARN-4994) | Use MiniYARNCluster with try-with-resources in tests |  Trivial | test | Andras Bokor | Andras Bokor |
| [HADOOP-13863](https://issues.apache.org/jira/browse/HADOOP-13863) | Azure: Add a new SAS key mode for WASB. |  Major | fs/azure | Dushyanth | Dushyanth |
| [HDFS-10917](https://issues.apache.org/jira/browse/HDFS-10917) | Collect peer performance statistics on DataNode. |  Major | datanode | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-5969](https://issues.apache.org/jira/browse/YARN-5969) | FairShareComparator: Cache value of getResourceUsage for better performance |  Major | fairscheduler | zhangshilong | zhangshilong |
| [HDFS-11275](https://issues.apache.org/jira/browse/HDFS-11275) | Check groupEntryIndex and throw a helpful exception on failures when removing ACL. |  Major | namenode | Xiao Chen | Xiao Chen |
| [YARN-5709](https://issues.apache.org/jira/browse/YARN-5709) | Cleanup leader election configs and pluggability |  Critical | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-11279](https://issues.apache.org/jira/browse/HDFS-11279) | Cleanup unused DataNode#checkDiskErrorAsync() |  Minor | . | Xiaoyu Yao | Hanisha Koneru |
| [HDFS-9483](https://issues.apache.org/jira/browse/HDFS-9483) | Documentation does not cover use of "swebhdfs" as URL scheme for SSL-secured WebHDFS. |  Major | documentation | Chris Nauroth | Surendra Singh Lilhore |
| [YARN-5991](https://issues.apache.org/jira/browse/YARN-5991) | Yarn Distributed Shell does not print throwable t to App Master When failed to start container |  Minor | . | dashwang | Jim Frankola |
| [HDFS-11292](https://issues.apache.org/jira/browse/HDFS-11292) | log lastWrittenTxId etc info in logSyncAll |  Major | hdfs | Yongjun Zhang | Yongjun Zhang |
| [HDFS-11273](https://issues.apache.org/jira/browse/HDFS-11273) | Move TransferFsImage#doGetUrl function to a Util class |  Major | . | Hanisha Koneru | Hanisha Koneru |
| [YARN-6015](https://issues.apache.org/jira/browse/YARN-6015) | AsyncDispatcher thread name can be set to improved debugging |  Major | . | Ajith S | Ajith S |
| [HADOOP-13953](https://issues.apache.org/jira/browse/HADOOP-13953) | Make FTPFileSystem's data connection mode and transfer mode configurable |  Major | fs | Xiao Chen | Xiao Chen |
| [HDFS-11299](https://issues.apache.org/jira/browse/HDFS-11299) | Support multiple Datanode File IO hooks |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [YARN-5849](https://issues.apache.org/jira/browse/YARN-5849) | Automatically create YARN control group for pre-mounted cgroups |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-11339](https://issues.apache.org/jira/browse/HDFS-11339) | Support File IO sampling for Datanode IO profiling hooks |  Major | datanode | Hanisha Koneru | Hanisha Koneru |
| [HDFS-11274](https://issues.apache.org/jira/browse/HDFS-11274) | Datanode should only check the failed volume upon IO errors |  Major | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [MAPREDUCE-6819](https://issues.apache.org/jira/browse/MAPREDUCE-6819) | Replace UTF8 with Text in MRBench |  Minor | test | Akira Ajisaka | Peter Bacsko |
| [HDFS-11306](https://issues.apache.org/jira/browse/HDFS-11306) | Print remaining edit logs from buffer if edit log can't be rolled. |  Major | ha, namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13978](https://issues.apache.org/jira/browse/HADOOP-13978) | Update project release notes for 3.0.0-alpha2 |  Major | documentation | Andrew Wang | Andrew Wang |
| [HADOOP-13955](https://issues.apache.org/jira/browse/HADOOP-13955) | Replace deprecated HttpServer2 and SSLFactory constants |  Minor | . | John Zhuge | John Zhuge |
| [HADOOP-13496](https://issues.apache.org/jira/browse/HADOOP-13496) | Include file lengths in Mismatch in length error for distcp |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-13999](https://issues.apache.org/jira/browse/HADOOP-13999) | Add -DskipShade maven profile to disable jar shading to reduce compile time |  Minor | build | Arun Suresh | Arun Suresh |
| [YARN-6028](https://issues.apache.org/jira/browse/YARN-6028) | Add document for container metrics |  Major | documentation, nodemanager | Weiwei Yang | Weiwei Yang |
| [MAPREDUCE-6728](https://issues.apache.org/jira/browse/MAPREDUCE-6728) | Give fetchers hint when ShuffleHandler rejects a shuffling connection |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HADOOP-13606](https://issues.apache.org/jira/browse/HADOOP-13606) | swift FS to add a service load metadata file |  Major | fs/swift | Steve Loughran | Steve Loughran |
| [HADOOP-13037](https://issues.apache.org/jira/browse/HADOOP-13037) | Refactor Azure Data Lake Store as an independent FileSystem |  Major | fs/adl | Shrikant Naidu | Vishwajeet Dusane |
| [HDFS-11156](https://issues.apache.org/jira/browse/HDFS-11156) | Add new op GETFILEBLOCKLOCATIONS to WebHDFS REST API |  Major | webhdfs | Weiwei Yang | Weiwei Yang |
| [HADOOP-13738](https://issues.apache.org/jira/browse/HADOOP-13738) | DiskChecker should perform some disk IO |  Major | . | Arpit Agarwal | Arpit Agarwal |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-9034](https://issues.apache.org/jira/browse/HDFS-9034) | "StorageTypeStats" Metric should not count failed storage. |  Major | namenode | Archana T | Surendra Singh Lilhore |
| [MAPREDUCE-4784](https://issues.apache.org/jira/browse/MAPREDUCE-4784) | TestRecovery occasionally fails |  Major | mrv2, test | Jason Lowe | Haibo Chen |
| [HDFS-10760](https://issues.apache.org/jira/browse/HDFS-10760) | DataXceiver#run() should not log InvalidToken exception as an error |  Major | . | Pan Yuxuan | Pan Yuxuan |
| [HDFS-10729](https://issues.apache.org/jira/browse/HDFS-10729) | Improve log message for edit loading failures caused by FS limit checks. |  Major | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13375](https://issues.apache.org/jira/browse/HADOOP-13375) | o.a.h.security.TestGroupsCaching.testBackgroundRefreshCounters seems flaky |  Major | security, test | Mingliang Liu | Weiwei Yang |
| [HDFS-10820](https://issues.apache.org/jira/browse/HDFS-10820) | Reuse closeResponder to reset the response variable in DataStreamer#run |  Minor | . | Yiqun Lin | Yiqun Lin |
| [YARN-5555](https://issues.apache.org/jira/browse/YARN-5555) | Scheduler UI: "% of Queue" is inaccurate if leaf queue is hierarchically nested. |  Minor | . | Eric Payne | Eric Payne |
| [YARN-5549](https://issues.apache.org/jira/browse/YARN-5549) | AMLauncher#createAMContainerLaunchContext() should not log the command to be launched indiscriminately |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-10835](https://issues.apache.org/jira/browse/HDFS-10835) | Fix typos in httpfs.sh |  Trivial | httpfs | John Zhuge | John Zhuge |
| [HDFS-10841](https://issues.apache.org/jira/browse/HDFS-10841) | Remove duplicate or unused variable in appendFile() |  Minor | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-13558](https://issues.apache.org/jira/browse/HADOOP-13558) | UserGroupInformation created from a Subject incorrectly tries to renew the Kerberos ticket |  Major | security | Alejandro Abdelnur | Xiao Chen |
| [HADOOP-13388](https://issues.apache.org/jira/browse/HADOOP-13388) | Clean up TestLocalFileSystemPermission |  Trivial | fs | Andras Bokor | Andras Bokor |
| [HDFS-10844](https://issues.apache.org/jira/browse/HDFS-10844) | test\_libhdfs\_threaded\_hdfs\_static and test\_libhdfs\_zerocopy\_hdfs\_static are failing |  Major | libhdfs | Akira Ajisaka | Akira Ajisaka |
| [HDFS-9038](https://issues.apache.org/jira/browse/HDFS-9038) | DFS reserved space is erroneously counted towards non-DFS used. |  Major | datanode | Chris Nauroth | Brahma Reddy Battula |
| [MAPREDUCE-6628](https://issues.apache.org/jira/browse/MAPREDUCE-6628) | Potential memory leak in CryptoOutputStream |  Major | security | Mariappan Asokan | Mariappan Asokan |
| [HDFS-10832](https://issues.apache.org/jira/browse/HDFS-10832) | Propagate ACL bit and isEncrypted bit in HttpFS FileStatus permissions |  Critical | httpfs | Andrew Wang | Andrew Wang |
| [HDFS-9781](https://issues.apache.org/jira/browse/HDFS-9781) | FsDatasetImpl#getBlockReports can occasionally throw NullPointerException |  Major | datanode | Wei-Chiu Chuang | Manoj Govindassamy |
| [HDFS-10830](https://issues.apache.org/jira/browse/HDFS-10830) | FsDatasetImpl#removeVolumes crashes with IllegalMonitorStateException when vol being removed is in use |  Major | hdfs | Manoj Govindassamy | Arpit Agarwal |
| [HADOOP-13587](https://issues.apache.org/jira/browse/HADOOP-13587) | distcp.map.bandwidth.mb is overwritten even when -bandwidth flag isn't set |  Minor | tools/distcp | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [HDFS-10856](https://issues.apache.org/jira/browse/HDFS-10856) | Update the comment of BPServiceActor$Scheduler#scheduleNextBlockReport |  Minor | documentation | Akira Ajisaka | Yiqun Lin |
| [YARN-5630](https://issues.apache.org/jira/browse/YARN-5630) | NM fails to start after downgrade from 2.8 to 2.7 |  Blocker | nodemanager | Jason Lowe | Jason Lowe |
| [HADOOP-13616](https://issues.apache.org/jira/browse/HADOOP-13616) | Broken code snippet area in Hadoop Benchmarking |  Minor | documentation | Kai Sasaki | Kai Sasaki |
| [HDFS-10862](https://issues.apache.org/jira/browse/HDFS-10862) | Typos in 4 log messages |  Trivial | . | Mehran Hassani | Mehran Hassani |
| [YARN-5163](https://issues.apache.org/jira/browse/YARN-5163) | Migrate TestClientToAMTokens and TestClientRMTokens tests from the old RPC engine |  Major | test | Arun Suresh | Wei Zhou |
| [YARN-4232](https://issues.apache.org/jira/browse/YARN-4232) | TopCLI console support for HA mode |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [MAPREDUCE-6777](https://issues.apache.org/jira/browse/MAPREDUCE-6777) | Typos in 4 log messages |  Trivial | . | Mehran Hassani | Mehran Hassani |
| [YARN-5642](https://issues.apache.org/jira/browse/YARN-5642) | Typos in 9 log messages |  Trivial | . | Mehran Hassani | Mehran Hassani |
| [YARN-5657](https://issues.apache.org/jira/browse/YARN-5657) | Fix TestDefaultContainerExecutor |  Major | test | Akira Ajisaka | Arun Suresh |
| [YARN-5577](https://issues.apache.org/jira/browse/YARN-5577) | [Atsv2] Document object passing in infofilters with an example |  Major | timelinereader, timelineserver | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-5655](https://issues.apache.org/jira/browse/YARN-5655) | TestContainerManagerSecurity#testNMTokens is asserting |  Major | . | Jason Lowe | Robert Kanter |
| [HADOOP-13601](https://issues.apache.org/jira/browse/HADOOP-13601) | Fix typo in a log messages of AbstractDelegationTokenSecretManager |  Trivial | . | Mehran Hassani | Mehran Hassani |
| [HDFS-10879](https://issues.apache.org/jira/browse/HDFS-10879) | TestEncryptionZonesWithKMS#testReadWrite fails intermittently |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-13602](https://issues.apache.org/jira/browse/HADOOP-13602) | Fix some warnings by findbugs in hadoop-maven-plugin |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-4973](https://issues.apache.org/jira/browse/YARN-4973) | YarnWebParams next.fresh.interval should be next.refresh.interval |  Minor | webapp | Daniel Templeton | Daniel Templeton |
| [YARN-5539](https://issues.apache.org/jira/browse/YARN-5539) | TimelineClient failed to retry on "java.net.SocketTimeoutException: Read timed out" |  Critical | yarn | Sumana Sathish | Junping Du |
| [HADOOP-13643](https://issues.apache.org/jira/browse/HADOOP-13643) | Math error in AbstractContractDistCpTest |  Minor | . | Aaron Fabbri | Aaron Fabbri |
| [HDFS-10894](https://issues.apache.org/jira/browse/HDFS-10894) | Remove the redundant charactors for command -saveNamespace in HDFSCommands.md |  Trivial | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-10886](https://issues.apache.org/jira/browse/HDFS-10886) | Replace "fs.default.name" with "fs.defaultFS" in viewfs document |  Minor | documentation, federation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10843](https://issues.apache.org/jira/browse/HDFS-10843) | Update space quota when a UC block is completed rather than committed. |  Major | hdfs, namenode | Erik Krogen | Erik Krogen |
| [HDFS-10866](https://issues.apache.org/jira/browse/HDFS-10866) | Fix Eclipse Java 8 compile errors related to generic parameters. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-5664](https://issues.apache.org/jira/browse/YARN-5664) | Fix Yarn documentation to link to correct versions. |  Minor | . | Xiao Chen | Xiao Chen |
| [YARN-5663](https://issues.apache.org/jira/browse/YARN-5663) | Small refactor in ZKRMStateStore |  Minor | resourcemanager | Oleksii Dymytrov | Oleksii Dymytrov |
| [HADOOP-13638](https://issues.apache.org/jira/browse/HADOOP-13638) | KMS should set UGI's Configuration object properly |  Major | kms | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-9885](https://issues.apache.org/jira/browse/HDFS-9885) | Correct the distcp counters name while displaying counters |  Minor | distcp | Archana T | Surendra Singh Lilhore |
| [YARN-5660](https://issues.apache.org/jira/browse/YARN-5660) | Wrong audit constants are used in Get/Put of priority in RMWebService |  Trivial | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-10889](https://issues.apache.org/jira/browse/HDFS-10889) | Remove outdated Fault Injection Framework documentaion |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10713](https://issues.apache.org/jira/browse/HDFS-10713) | Throttle FsNameSystem lock warnings |  Major | logging, namenode | Arpit Agarwal | Hanisha Koneru |
| [HDFS-10426](https://issues.apache.org/jira/browse/HDFS-10426) | TestPendingInvalidateBlock failed in trunk |  Major | test | Yiqun Lin | Yiqun Lin |
| [HDFS-10828](https://issues.apache.org/jira/browse/HDFS-10828) | Fix usage of FsDatasetImpl object lock in ReplicaMap |  Blocker | . | Arpit Agarwal | Arpit Agarwal |
| [YARN-5631](https://issues.apache.org/jira/browse/YARN-5631) | Missing refreshClusterMaxPriority usage in rmadmin help message |  Minor | . | Kai Sasaki | Kai Sasaki |
| [HDFS-10376](https://issues.apache.org/jira/browse/HDFS-10376) | Enhance setOwner testing |  Major | . | Yongjun Zhang | John Zhuge |
| [HDFS-10915](https://issues.apache.org/jira/browse/HDFS-10915) | Fix time measurement bug in TestDatanodeRestart#testWaitForRegistrationOnRestart |  Minor | test | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-9444](https://issues.apache.org/jira/browse/HDFS-9444) | Add utility to find set of available ephemeral ports to ServerSocketUtil |  Major | . | Brahma Reddy Battula | Masatake Iwasaki |
| [YARN-5662](https://issues.apache.org/jira/browse/YARN-5662) | Provide an option to enable ContainerMonitor |  Major | . | Jian He | Jian He |
| [HADOOP-11780](https://issues.apache.org/jira/browse/HADOOP-11780) | Prevent IPC reader thread death |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-10824](https://issues.apache.org/jira/browse/HDFS-10824) | MiniDFSCluster#storageCapacities has no effects on real capacity |  Major | . | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-10914](https://issues.apache.org/jira/browse/HDFS-10914) | Move remnants of oah.hdfs.client to hadoop-hdfs-client |  Critical | hdfs-client | Andrew Wang | Andrew Wang |
| [HADOOP-13164](https://issues.apache.org/jira/browse/HADOOP-13164) | Optimize S3AFileSystem::deleteUnnecessaryFakeDirectories |  Minor | fs/s3 | Rajesh Balamohan | Rajesh Balamohan |
| [MAPREDUCE-6771](https://issues.apache.org/jira/browse/MAPREDUCE-6771) | RMContainerAllocator sends container diagnostics event after corresponding completion event |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HADOOP-13640](https://issues.apache.org/jira/browse/HADOOP-13640) | Fix findbugs warning in VersionInfoMojo.java |  Major | . | Tsuyoshi Ozawa | Yuanbo Liu |
| [HDFS-10850](https://issues.apache.org/jira/browse/HDFS-10850) | getEZForPath should NOT throw FNF |  Blocker | hdfs | Daryn Sharp | Andrew Wang |
| [HADOOP-13671](https://issues.apache.org/jira/browse/HADOOP-13671) | Fix ClassFormatException in trunk build. |  Major | build | Kihwal Lee | Kihwal Lee |
| [HDFS-10907](https://issues.apache.org/jira/browse/HDFS-10907) | Fix Erasure Coding documentation |  Trivial | documentation, erasure-coding | Wei-Chiu Chuang | Manoj Govindassamy |
| [YARN-5693](https://issues.apache.org/jira/browse/YARN-5693) | Reduce loglevel to Debug in ContainerManagementProtocolProxy and AMRMClientImpl |  Major | yarn | Yufei Gu | Yufei Gu |
| [YARN-5678](https://issues.apache.org/jira/browse/YARN-5678) | Log demand as demand in FSLeafQueue and FSParentQueue |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-5672](https://issues.apache.org/jira/browse/YARN-5672) | FairScheduler: wrong queue name in log when adding application |  Minor | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-4767](https://issues.apache.org/jira/browse/YARN-4767) | Network issues can cause persistent RM UI outage |  Critical | webapp | Daniel Templeton | Daniel Templeton |
| [HDFS-10810](https://issues.apache.org/jira/browse/HDFS-10810) |  Setreplication removing block from underconstrcution temporarily when batch IBR is enabled. |  Major | namenode | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10944](https://issues.apache.org/jira/browse/HDFS-10944) | Correct the javadoc of dfsadmin#disallowSnapshot |  Minor | documentation | Jagadesh Kiran N | Jagadesh Kiran N |
| [HDFS-10947](https://issues.apache.org/jira/browse/HDFS-10947) | Correct the API name for truncate in webhdfs document |  Major | documentation | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-10878](https://issues.apache.org/jira/browse/HDFS-10878) | TestDFSClientRetries#testIdempotentAllocateBlockAndClose throws ConcurrentModificationException |  Major | hdfs-client | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-12667](https://issues.apache.org/jira/browse/HADOOP-12667) | s3a: Support createNonRecursive API |  Major | fs/s3 | Sean Mackrory | Sean Mackrory |
| [HDFS-10609](https://issues.apache.org/jira/browse/HDFS-10609) | Uncaught InvalidEncryptionKeyException during pipeline recovery may abort downstream applications |  Major | encryption | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-10962](https://issues.apache.org/jira/browse/HDFS-10962) | TestRequestHedgingProxyProvider is flaky |  Major | test | Andrew Wang | Andrew Wang |
| [MAPREDUCE-6789](https://issues.apache.org/jira/browse/MAPREDUCE-6789) | Fix TestAMWebApp failure |  Major | test | Akira Ajisaka | Daniel Templeton |
| [MAPREDUCE-6740](https://issues.apache.org/jira/browse/MAPREDUCE-6740) | Enforce mapreduce.task.timeout to be at least mapreduce.task.progress-report.interval |  Minor | mr-am | Haibo Chen | Haibo Chen |
| [HADOOP-13690](https://issues.apache.org/jira/browse/HADOOP-13690) | Fix typos in core-default.xml |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-5101](https://issues.apache.org/jira/browse/YARN-5101) | YARN\_APPLICATION\_UPDATED event is parsed in ApplicationHistoryManagerOnTimelineStore#convertToApplicationReport with reversed order |  Major | . | Xuan Gong | Sunil Govindan |
| [YARN-5659](https://issues.apache.org/jira/browse/YARN-5659) | getPathFromYarnURL should use standard methods |  Major | . | Sergey Shelukhin | Sergey Shelukhin |
| [HADOOP-12611](https://issues.apache.org/jira/browse/HADOOP-12611) | TestZKSignerSecretProvider#testMultipleInit occasionally fail |  Major | . | Wei-Chiu Chuang | Eric Badger |
| [HDFS-10969](https://issues.apache.org/jira/browse/HDFS-10969) | Fix typos in hdfs-default.xml |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-10797](https://issues.apache.org/jira/browse/HDFS-10797) | Disk usage summary of snapshots causes renamed blocks to get counted twice |  Major | snapshots | Sean Mackrory | Sean Mackrory |
| [YARN-5057](https://issues.apache.org/jira/browse/YARN-5057) | resourcemanager.security.TestDelegationTokenRenewer fails in trunk |  Major | . | Yongjun Zhang | Jason Lowe |
| [HADOOP-13697](https://issues.apache.org/jira/browse/HADOOP-13697) | LogLevel#main throws exception if no arguments provided |  Major | . | Mingliang Liu | Mingliang Liu |
| [HDFS-10991](https://issues.apache.org/jira/browse/HDFS-10991) | Export hdfsTruncateFile symbol in libhdfs |  Blocker | libhdfs | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-13700](https://issues.apache.org/jira/browse/HADOOP-13700) | Remove unthrown IOException from TrashPolicy#initialize and #getInstance signatures |  Critical | fs | Haibo Chen | Andrew Wang |
| [HDFS-11002](https://issues.apache.org/jira/browse/HDFS-11002) | Fix broken attr/getfattr/setfattr links in ExtendedAttributes.md |  Major | documentation | Mingliang Liu | Mingliang Liu |
| [HDFS-11000](https://issues.apache.org/jira/browse/HDFS-11000) | webhdfs PUT does not work if requests are routed to call queue. |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-10987](https://issues.apache.org/jira/browse/HDFS-10987) | Make Decommission less expensive when lot of blocks present. |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-13024](https://issues.apache.org/jira/browse/HADOOP-13024) | Distcp with -delete feature on raw data not implemented |  Major | tools/distcp | Mavin Martin | Mavin Martin |
| [HDFS-10986](https://issues.apache.org/jira/browse/HDFS-10986) | DFSAdmin should log detailed error message if any |  Major | tools | Mingliang Liu | Mingliang Liu |
| [HADOOP-13723](https://issues.apache.org/jira/browse/HADOOP-13723) | AliyunOSSInputStream#read() should update read bytes stat correctly |  Major | tools | Mingliang Liu | Mingliang Liu |
| [HDFS-10990](https://issues.apache.org/jira/browse/HDFS-10990) | TestPendingInvalidateBlock should wait for IBRs |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-10735](https://issues.apache.org/jira/browse/HDFS-10735) | Distcp using webhdfs on secure HA clusters fails with StandbyException |  Major | webhdfs | Benoy Antony | Benoy Antony |
| [HDFS-10883](https://issues.apache.org/jira/browse/HDFS-10883) | \`getTrashRoot\`'s behavior is not consistent in DFS after enabling EZ. |  Major | . | Yuanbo Liu | Yuanbo Liu |
| [HADOOP-13707](https://issues.apache.org/jira/browse/HADOOP-13707) | If kerberos is enabled while HTTP SPNEGO is not configured, some links cannot be accessed |  Major | . | Yuanbo Liu | Yuanbo Liu |
| [HDFS-10301](https://issues.apache.org/jira/browse/HDFS-10301) | BlockReport retransmissions may lead to storages falsely being declared zombie if storage report processing happens out of order |  Critical | namenode | Konstantin Shvachko | Vinitha Reddy Gankidi |
| [HDFS-10920](https://issues.apache.org/jira/browse/HDFS-10920) | TestStorageMover#testNoSpaceDisk is failing intermittently |  Major | test | Rakesh R | Rakesh R |
| [YARN-5743](https://issues.apache.org/jira/browse/YARN-5743) | [Atsv2] Publish queue name and RMAppMetrics to ATS |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-10960](https://issues.apache.org/jira/browse/HDFS-10960) | TestDataNodeHotSwapVolumes#testRemoveVolumeBeingWritten fails at disk error verification after volume remove |  Minor | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-10752](https://issues.apache.org/jira/browse/HDFS-10752) | Several log refactoring/improvement suggestion in HDFS |  Major | . | Nemo Chen | Hanisha Koneru |
| [HDFS-11025](https://issues.apache.org/jira/browse/HDFS-11025) | TestDiskspaceQuotaUpdate fails in trunk due to Bind exception |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-10627](https://issues.apache.org/jira/browse/HDFS-10627) | Volume Scanner marks a block as "suspect" even if the exception is network-related |  Major | hdfs | Rushabh S Shah | Rushabh S Shah |
| [HDFS-10699](https://issues.apache.org/jira/browse/HDFS-10699) | Log object instance get incorrectly in TestDFSAdmin |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-11018](https://issues.apache.org/jira/browse/HDFS-11018) | Incorrect check and message in FsDatasetImpl#invalidate |  Major | datanode | Wei-Chiu Chuang | Yiqun Lin |
| [HADOOP-13236](https://issues.apache.org/jira/browse/HADOOP-13236) | truncate will fail when we use viewfilesystem |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10730](https://issues.apache.org/jira/browse/HDFS-10730) | Fix some failed tests due to BindException |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-5747](https://issues.apache.org/jira/browse/YARN-5747) | Application timeline metric aggregation in timeline v2 will lose last round aggregation when an application finishes |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-5679](https://issues.apache.org/jira/browse/YARN-5679) | TestAHSWebServices is failing |  Major | timelineserver | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-13749](https://issues.apache.org/jira/browse/HADOOP-13749) | KMSClientProvider combined with KeyProviderCache can result in wrong UGI being used |  Critical | . | Sergey Shelukhin | Xiaoyu Yao |
| [HDFS-11042](https://issues.apache.org/jira/browse/HDFS-11042) | Add missing cleanupSSLConfig() call for tests that use setupSSLConfig() |  Major | test | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-13626](https://issues.apache.org/jira/browse/HADOOP-13626) | Remove distcp dependency on FileStatus serialization |  Major | tools/distcp | Chris Douglas | Chris Douglas |
| [HDFS-11046](https://issues.apache.org/jira/browse/HDFS-11046) | Duplicate '-' in the daemon log name |  Major | logging | Akira Ajisaka | Akira Ajisaka |
| [YARN-5711](https://issues.apache.org/jira/browse/YARN-5711) | Propogate exceptions back to client when using hedging RM failover provider |  Critical | applications, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-5754](https://issues.apache.org/jira/browse/YARN-5754) | Null check missing for earliest in FifoPolicy |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-5777](https://issues.apache.org/jira/browse/YARN-5777) | TestLogsCLI#testFetchApplictionLogsAsAnotherUser fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11015](https://issues.apache.org/jira/browse/HDFS-11015) | Enforce timeout in balancer |  Major | balancer & mover | Kihwal Lee | Kihwal Lee |
| [HDFS-11040](https://issues.apache.org/jira/browse/HDFS-11040) | Add documentation for HDFS-9820 distcp improvement |  Major | distcp | Yongjun Zhang | Yongjun Zhang |
| [YARN-5677](https://issues.apache.org/jira/browse/YARN-5677) | RM should transition to standby when connection is lost for an extended period |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-11054](https://issues.apache.org/jira/browse/HDFS-11054) | Suppress verbose log message in BlockPlacementPolicyDefault |  Major | . | Arpit Agarwal | Chen Liang |
| [HDFS-10935](https://issues.apache.org/jira/browse/HDFS-10935) | TestFileChecksum fails in some cases |  Major | . | Wei-Chiu Chuang | Sammi Chen |
| [YARN-5753](https://issues.apache.org/jira/browse/YARN-5753) | fix NPE in AMRMClientImpl.getMatchingRequests() |  Major | yarn | Haibo Chen | Haibo Chen |
| [HDFS-11050](https://issues.apache.org/jira/browse/HDFS-11050) | Change log level to 'warn' when ssl initialization fails and defaults to DEFAULT\_TIMEOUT\_CONN\_CONFIGURATOR |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-11053](https://issues.apache.org/jira/browse/HDFS-11053) | Unnecessary superuser check in versionRequest() |  Major | namenode, security | Kihwal Lee | Kihwal Lee |
| [YARN-5433](https://issues.apache.org/jira/browse/YARN-5433) | Audit dependencies for Category-X |  Blocker | timelineserver | Sean Busbey | Sangjin Lee |
| [HDFS-10921](https://issues.apache.org/jira/browse/HDFS-10921) | TestDiskspaceQuotaUpdate doesn't wait for NN to get out of safe mode |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-8299](https://issues.apache.org/jira/browse/HADOOP-8299) | ViewFileSystem link slash mount point crashes with IndexOutOfBoundsException |  Major | viewfs | Eli Collins | Manoj Govindassamy |
| [HDFS-9929](https://issues.apache.org/jira/browse/HDFS-9929) | Duplicate keys in NAMENODE\_SPECIFIC\_KEYS |  Minor | namenode | Akira Ajisaka | Akira Ajisaka |
| [YARN-5752](https://issues.apache.org/jira/browse/YARN-5752) | TestLocalResourcesTrackerImpl#testLocalResourceCache times out |  Major | . | Eric Badger | Eric Badger |
| [YARN-5710](https://issues.apache.org/jira/browse/YARN-5710) | Fix inconsistent naming in class ResourceRequest |  Trivial | yarn | Yufei Gu | Yufei Gu |
| [YARN-5686](https://issues.apache.org/jira/browse/YARN-5686) | DefaultContainerExecutor random working dir algorigthm skews results |  Minor | . | Miklos Szegedi | Vrushali C |
| [HDFS-10769](https://issues.apache.org/jira/browse/HDFS-10769) | BlockIdManager.clear doesn't reset the counter for blockGroupIdGenerator |  Minor | hdfs | Ewan Higgs | Yiqun Lin |
| [MAPREDUCE-6798](https://issues.apache.org/jira/browse/MAPREDUCE-6798) | Fix intermittent failure of TestJobHistoryParsing.testJobHistoryMethods() |  Major | jobhistoryserver | Haibo Chen | Haibo Chen |
| [HDFS-8492](https://issues.apache.org/jira/browse/HDFS-8492) | DN should notify NN when client requests a missing block |  Major | . | Daryn Sharp | Walter Su |
| [YARN-5757](https://issues.apache.org/jira/browse/YARN-5757) | RM REST API documentation is not up to date |  Trivial | resourcemanager, yarn | Miklos Szegedi | Miklos Szegedi |
| [MAPREDUCE-6541](https://issues.apache.org/jira/browse/MAPREDUCE-6541) | Exclude scheduled reducer memory when calculating available mapper slots from headroom to avoid deadlock |  Major | . | Wangda Tan | Varun Saxena |
| [YARN-3848](https://issues.apache.org/jira/browse/YARN-3848) | TestNodeLabelContainerAllocation is not timing out |  Major | test | Jason Lowe | Varun Saxena |
| [YARN-5420](https://issues.apache.org/jira/browse/YARN-5420) | Delete org.apache.hadoop.yarn.server.resourcemanager.resource.Priority as its not necessary |  Minor | resourcemanager | Sunil Govindan | Sunil Govindan |
| [HADOOP-13201](https://issues.apache.org/jira/browse/HADOOP-13201) | Print the directory paths when ViewFs denies the rename operation on internal dirs |  Major | viewfs | Tianyin Xu | Rakesh R |
| [YARN-5172](https://issues.apache.org/jira/browse/YARN-5172) | Update yarn daemonlog documentation due to HADOOP-12847 |  Trivial | documentation | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4831](https://issues.apache.org/jira/browse/YARN-4831) | Recovered containers will be killed after NM stateful restart |  Major | nodemanager | Siqi Li | Siqi Li |
| [YARN-4388](https://issues.apache.org/jira/browse/YARN-4388) | Cleanup "mapreduce.job.hdfs-servers" from yarn-default.xml |  Minor | yarn | Junping Du | Junping Du |
| [YARN-5776](https://issues.apache.org/jira/browse/YARN-5776) | Checkstyle: MonitoringThread.Run method length is too long |  Trivial | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-3432](https://issues.apache.org/jira/browse/YARN-3432) | Cluster metrics have wrong Total Memory when there is reserved memory on CS |  Major | capacityscheduler, resourcemanager | Thomas Graves | Brahma Reddy Battula |
| [HDFS-9500](https://issues.apache.org/jira/browse/HDFS-9500) | datanodesSoftwareVersions map may counting wrong when rolling upgrade |  Major | . | Phil Yang | Erik Krogen |
| [MAPREDUCE-2631](https://issues.apache.org/jira/browse/MAPREDUCE-2631) | Potential resource leaks in BinaryProtocol$TeeOutputStream.java |  Major | . | Ravi Teja Ch N V | Sunil Govindan |
| [YARN-2306](https://issues.apache.org/jira/browse/YARN-2306) | Add test for leakage of reservation metrics in fair scheduler |  Minor | fairscheduler | Hong Zhiguo | Hong Zhiguo |
| [YARN-4743](https://issues.apache.org/jira/browse/YARN-4743) | FairSharePolicy breaks TimSort assumption |  Major | fairscheduler | Zephyr Guo | Zephyr Guo |
| [HADOOP-13763](https://issues.apache.org/jira/browse/HADOOP-13763) | KMS REST API Documentation Decrypt URL typo |  Minor | documentation, kms | Jeffrey E  Rodriguez | Jeffrey E  Rodriguez |
| [YARN-5794](https://issues.apache.org/jira/browse/YARN-5794) | Fix the asflicense warnings |  Major | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-13770](https://issues.apache.org/jira/browse/HADOOP-13770) | Shell.checkIsBashSupported swallowed an interrupted exception |  Minor | util | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-5027](https://issues.apache.org/jira/browse/YARN-5027) | NM should clean up app log dirs after NM restart |  Major | nodemanager | sandflee | sandflee |
| [YARN-5767](https://issues.apache.org/jira/browse/YARN-5767) | Fix the order that resources are cleaned up from the local Public/Private caches |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-11061](https://issues.apache.org/jira/browse/HDFS-11061) | Update dfs -count -t command line help and documentation |  Minor | documentation, fs | Wei-Chiu Chuang | Yiqun Lin |
| [YARN-5773](https://issues.apache.org/jira/browse/YARN-5773) | RM recovery too slow due to LeafQueue#activateApplication() |  Critical | capacity scheduler, rolling upgrade | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5800](https://issues.apache.org/jira/browse/YARN-5800) | Delete LinuxContainerExecutor comment from yarn-default.xml |  Trivial | yarn | Daniel Templeton | Jan Hentschel |
| [YARN-5809](https://issues.apache.org/jira/browse/YARN-5809) | AsyncDispatcher possibly invokes multiple shutdown thread when handling exception |  Major | . | Jian He | Jian He |
| [HADOOP-8500](https://issues.apache.org/jira/browse/HADOOP-8500) | Fix javadoc jars to not contain entire target directory |  Minor | build | EJ Ciramella | Andrew Wang |
| [HDFS-10455](https://issues.apache.org/jira/browse/HDFS-10455) | Logging the username when deny the setOwner operation |  Minor | namenode | Tianyin Xu | Rakesh R |
| [YARN-5805](https://issues.apache.org/jira/browse/YARN-5805) | Add isDebugEnabled check for debug logs in nodemanager |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5788](https://issues.apache.org/jira/browse/YARN-5788) | Apps not activiated and AM limit resource in UI and REST not updated after -replaceLabelsOnNode |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5001](https://issues.apache.org/jira/browse/YARN-5001) | Aggregated Logs root directory is created with wrong group if nonexistent |  Major | log-aggregation, nodemanager, security | Haibo Chen | Haibo Chen |
| [MAPREDUCE-6765](https://issues.apache.org/jira/browse/MAPREDUCE-6765) | MR should not schedule container requests in cases where reducer or mapper containers demand resource larger than the maximum supported |  Minor | mr-am | Haibo Chen | Haibo Chen |
| [YARN-5815](https://issues.apache.org/jira/browse/YARN-5815) | Random failure of TestApplicationPriority.testOrderOfActivatingThePriorityApplicationOnRMRestart |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-11095](https://issues.apache.org/jira/browse/HDFS-11095) | BlockManagerSafeMode should respect extension period default config value (30s) |  Minor | namenode | Mingliang Liu | Mingliang Liu |
| [YARN-4862](https://issues.apache.org/jira/browse/YARN-4862) | Handle duplicate completed containers in RMNodeImpl |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-11097](https://issues.apache.org/jira/browse/HDFS-11097) | Fix the jenkins warning related to the deprecated method StorageReceivedDeletedBlocks |  Major | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-13787](https://issues.apache.org/jira/browse/HADOOP-13787) | Azure testGlobStatusThrowsExceptionForUnreadableDir fails |  Minor | fs/azure | John Zhuge | John Zhuge |
| [HDFS-11098](https://issues.apache.org/jira/browse/HDFS-11098) | Datanode in tests cannot start in Windows after HDFS-10638 |  Blocker | datanode | Vinayakumar B | Vinayakumar B |
| [YARN-5837](https://issues.apache.org/jira/browse/YARN-5837) | NPE when getting node status of a decommissioned node after an RM restart |  Major | . | Robert Kanter | Robert Kanter |
| [HADOOP-13798](https://issues.apache.org/jira/browse/HADOOP-13798) | TestHadoopArchives times out |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-13797](https://issues.apache.org/jira/browse/HADOOP-13797) | Remove hardcoded absolute path for ls |  Major | util | Christine Koppelt | Christine Koppelt |
| [HADOOP-13795](https://issues.apache.org/jira/browse/HADOOP-13795) | Skip testGlobStatusThrowsExceptionForUnreadableDir in TestFSMainOperationsSwift |  Major | fs/swift, test | John Zhuge | John Zhuge |
| [HADOOP-13803](https://issues.apache.org/jira/browse/HADOOP-13803) | Revert HADOOP-13081 add the ability to create multiple UGIs/subjects from one kerberos login |  Major | security | Andrew Wang | Chris Nauroth |
| [YARN-5847](https://issues.apache.org/jira/browse/YARN-5847) | Revert health check exit code check |  Major | nodemanager | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13804](https://issues.apache.org/jira/browse/HADOOP-13804) | MutableStat mean loses accuracy if add(long, long) is used |  Minor | metrics | Erik Krogen | Erik Krogen |
| [YARN-5377](https://issues.apache.org/jira/browse/YARN-5377) | Fix TestQueuingContainerManager.testKillMultipleOpportunisticContainers |  Major | . | Rohith Sharma K S | Konstantinos Karanasos |
| [MAPREDUCE-6782](https://issues.apache.org/jira/browse/MAPREDUCE-6782) | JHS task page search based on each individual column not working |  Major | jobhistoryserver | Bibin A Chundatt | Ajith S |
| [HADOOP-13789](https://issues.apache.org/jira/browse/HADOOP-13789) | Hadoop Common includes generated test protos in both jar and test-jar |  Major | build, common | Sean Busbey | Sean Busbey |
| [YARN-5823](https://issues.apache.org/jira/browse/YARN-5823) | Update NMTokens in case of requests with only opportunistic containers |  Blocker | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [HADOOP-13346](https://issues.apache.org/jira/browse/HADOOP-13346) | DelegationTokenAuthenticationHandler writes via closed writer |  Minor | security | Gregory Chanan | Hrishikesh Gadre |
| [YARN-5856](https://issues.apache.org/jira/browse/YARN-5856) | Unnecessary duplicate start container request sent to NM State store |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-5862](https://issues.apache.org/jira/browse/YARN-5862) | TestDiskFailures.testLocalDirsFailures failed |  Major | . | Yufei Gu | Yufei Gu |
| [YARN-5453](https://issues.apache.org/jira/browse/YARN-5453) | FairScheduler#update may skip update demand resource of child queue/app if current demand reached maxResource |  Major | fairscheduler | sandflee | sandflee |
| [YARN-5843](https://issues.apache.org/jira/browse/YARN-5843) | Incorrect documentation for timeline service entityType/events REST end points |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9337](https://issues.apache.org/jira/browse/HDFS-9337) | Validate required params for WebHDFS requests |  Major | . | Jagadesh Kiran N | Jagadesh Kiran N |
| [YARN-5834](https://issues.apache.org/jira/browse/YARN-5834) | TestNodeStatusUpdater.testNMRMConnectionConf compares nodemanager wait time to the incorrect value |  Trivial | . | Miklos Szegedi | Chang Li |
| [HDFS-11128](https://issues.apache.org/jira/browse/HDFS-11128) | CreateEditsLog throws NullPointerException |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [YARN-5868](https://issues.apache.org/jira/browse/YARN-5868) | Update npm to latest version in Dockerfile to avoid random failures of npm while run maven build |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-5545](https://issues.apache.org/jira/browse/YARN-5545) | Fix issues related to Max App in capacity scheduler |  Major | capacity scheduler | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5825](https://issues.apache.org/jira/browse/YARN-5825) | ProportionalPreemptionalPolicy could use readLock over LeafQueue instead of synchronized block |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [HDFS-11129](https://issues.apache.org/jira/browse/HDFS-11129) | TestAppendSnapshotTruncate fails with bind exception |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-13813](https://issues.apache.org/jira/browse/HADOOP-13813) | TestDelegationTokenFetcher#testDelegationTokenWithoutRenewer is failing |  Major | security, test | Mingliang Liu | Mingliang Liu |
| [HDFS-11135](https://issues.apache.org/jira/browse/HDFS-11135) | The tests in TestBalancer run fails due to NPE |  Major | test | Yiqun Lin | Yiqun Lin |
| [HDFS-11056](https://issues.apache.org/jira/browse/HDFS-11056) | Concurrent append and read operations lead to checksum error |  Major | datanode, httpfs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-6797](https://issues.apache.org/jira/browse/MAPREDUCE-6797) | Job history server scans can become blocked on a single, slow entry |  Critical | jobhistoryserver | Prabhu Joseph | Prabhu Joseph |
| [YARN-5874](https://issues.apache.org/jira/browse/YARN-5874) | RM -format-state-store and -remove-application-from-state-store commands fail with NPE |  Critical | resourcemanager | Varun Saxena | Varun Saxena |
| [YARN-5873](https://issues.apache.org/jira/browse/YARN-5873) | RM crashes with NPE if generic application history is enabled |  Critical | resourcemanager | Varun Saxena | Varun Saxena |
| [YARN-4355](https://issues.apache.org/jira/browse/YARN-4355) | NPE while processing localizer heartbeat |  Major | nodemanager | Jason Lowe | Varun Saxena |
| [YARN-4218](https://issues.apache.org/jira/browse/YARN-4218) | Metric for resource\*time that was preempted |  Major | resourcemanager | Chang Li | Chang Li |
| [YARN-5875](https://issues.apache.org/jira/browse/YARN-5875) | TestTokenClientRMService#testTokenRenewalWrongUser fails |  Major | . | Varun Saxena | Gergely Novák |
| [HADOOP-13815](https://issues.apache.org/jira/browse/HADOOP-13815) | TestKMS#testDelegationTokensOpsSimple and TestKMS#testDelegationTokensOpsKerberized Fails in Trunk |  Major | test | Brahma Reddy Battula | Xiao Chen |
| [YARN-5765](https://issues.apache.org/jira/browse/YARN-5765) | Revert CHMOD on the new dirs created-LinuxContainerExecutor creates appcache and its subdirectories with wrong group owner. |  Blocker | . | Haibo Chen | Naganarasimha G R |
| [MAPREDUCE-6811](https://issues.apache.org/jira/browse/MAPREDUCE-6811) | TestPipeApplication#testSubmitter fails after HADOOP-13802 |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-5891](https://issues.apache.org/jira/browse/YARN-5891) | yarn rmadmin -help contains a misspelled ResourceManager |  Trivial | resourcemanager | Grant Sohn | Grant Sohn |
| [YARN-5836](https://issues.apache.org/jira/browse/YARN-5836) | Malicious AM can kill containers of other apps running in any node its containers are running |  Minor | nodemanager | Botong Huang | Botong Huang |
| [YARN-5870](https://issues.apache.org/jira/browse/YARN-5870) | Expose getApplications API in YarnClient with GetApplicationsRequest parameter |  Major | client | Gour Saha | Jian He |
| [HDFS-11134](https://issues.apache.org/jira/browse/HDFS-11134) | Fix bind exception threw in TestRenameWhileOpen |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-3538](https://issues.apache.org/jira/browse/YARN-3538) | TimelineServer doesn't catch/translate all exceptions raised |  Minor | timelineserver | Steve Loughran | Steve Loughran |
| [YARN-5904](https://issues.apache.org/jira/browse/YARN-5904) | Reduce the number of default server threads for AMRMProxyService |  Minor | nodemanager | Subru Krishnan | Subru Krishnan |
| [MAPREDUCE-6801](https://issues.apache.org/jira/browse/MAPREDUCE-6801) | Fix flaky TestKill.testKillJob() |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HADOOP-13814](https://issues.apache.org/jira/browse/HADOOP-13814) | Sample configuration of KMS HTTP Authentication signature is misleading |  Minor | conf, documentation, kms | Masahiro Tanaka | Masahiro Tanaka |
| [HDFS-11144](https://issues.apache.org/jira/browse/HDFS-11144) | TestFileCreationDelete#testFileCreationDeleteParent fails wind bind exception |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11101](https://issues.apache.org/jira/browse/HDFS-11101) | TestDFSShell#testMoveWithTargetPortEmpty fails intermittently |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-5859](https://issues.apache.org/jira/browse/YARN-5859) | TestResourceLocalizationService#testParallelDownloadAttemptsForPublicResource sometimes fails |  Major | test | Jason Lowe | Eric Badger |
| [MAPREDUCE-6793](https://issues.apache.org/jira/browse/MAPREDUCE-6793) | io.sort.factor code default and mapred-default.xml values inconsistent |  Trivial | task | Gera Shegalov | Prabhu Joseph |
| [HDFS-10966](https://issues.apache.org/jira/browse/HDFS-10966) | Enhance Dispatcher logic on deciding when to give up a source DataNode |  Major | balancer & mover | Zhe Zhang | Mark Wagner |
| [HADOOP-13663](https://issues.apache.org/jira/browse/HADOOP-13663) | Index out of range in SysInfoWindows |  Major | native, util | Íñigo Goiri | Íñigo Goiri |
| [YARN-5911](https://issues.apache.org/jira/browse/YARN-5911) | DrainDispatcher does not drain all events on stop even if setDrainEventsOnStop is true |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-5918](https://issues.apache.org/jira/browse/YARN-5918) | Handle Opportunistic scheduling allocate request failure when NM is lost |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-13766](https://issues.apache.org/jira/browse/HADOOP-13766) | Fix a typo in the comments of RPC.getProtocolVersion |  Trivial | common | Ethan Li | Ethan Li |
| [YARN-5920](https://issues.apache.org/jira/browse/YARN-5920) | Fix deadlock in TestRMHA.testTransitionedToStandbyShouldNotHang |  Major | test | Rohith Sharma K S | Varun Saxena |
| [HADOOP-13833](https://issues.apache.org/jira/browse/HADOOP-13833) | TestSymlinkHdfsFileSystem#testCreateLinkUsingPartQualPath2 fails after HADOOP13605 |  Critical | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11174](https://issues.apache.org/jira/browse/HDFS-11174) | Wrong HttpFS test command in doc |  Minor | documentation, httpfs | John Zhuge | John Zhuge |
| [HADOOP-13820](https://issues.apache.org/jira/browse/HADOOP-13820) | Replace ugi.getUsername() with ugi.getShortUserName() in viewFS |  Minor | viewfs | Archana T | Brahma Reddy Battula |
| [YARN-5572](https://issues.apache.org/jira/browse/YARN-5572) | HBaseTimelineWriterImpl appears to reference a bad property name |  Major | . | Daniel Templeton | Varun Saxena |
| [HADOOP-13816](https://issues.apache.org/jira/browse/HADOOP-13816) | Ambiguous plugin version warning from maven build. |  Minor | . | Kai Sasaki | Kai Sasaki |
| [HADOOP-13838](https://issues.apache.org/jira/browse/HADOOP-13838) | KMSTokenRenewer should close providers |  Critical | kms | Xiao Chen | Xiao Chen |
| [YARN-5725](https://issues.apache.org/jira/browse/YARN-5725) | Test uncaught exception in TestContainersMonitorResourceChange.testContainersResourceChange when setting IP and host |  Minor | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-5851](https://issues.apache.org/jira/browse/YARN-5851) | TestContainerManagerSecurity testContainerManager[1] failed |  Major | nodemanager | Haibo Chen | Haibo Chen |
| [MAPREDUCE-6565](https://issues.apache.org/jira/browse/MAPREDUCE-6565) | Configuration to use host name in delegation token service is not read from job.xml during MapReduce job execution. |  Major | . | Chris Nauroth | Li Lu |
| [YARN-5942](https://issues.apache.org/jira/browse/YARN-5942) | "Overridden" is misspelled as "overriden" in FairScheduler.md |  Trivial | site | Daniel Templeton | Heather Sutherland |
| [HADOOP-13830](https://issues.apache.org/jira/browse/HADOOP-13830) | Intermittent failure of ITestS3NContractRootDir#testRecursiveRootListing: "Can not create a Path from an empty string" |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-5915](https://issues.apache.org/jira/browse/YARN-5915) | ATS 1.5 FileSystemTimelineWriter causes flush() to be called after every event write |  Major | timelineserver | Atul Sikaria | Atul Sikaria |
| [MAPREDUCE-6815](https://issues.apache.org/jira/browse/MAPREDUCE-6815) | Fix flaky TestKill.testKillTask() |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [YARN-5901](https://issues.apache.org/jira/browse/YARN-5901) | Fix race condition in TestGetGroups beforeclass setup() |  Major | yarn | Haibo Chen | Haibo Chen |
| [YARN-5929](https://issues.apache.org/jira/browse/YARN-5929) | Missing scheduling policy in the FS queue metric. |  Major | . | Yufei Gu | Yufei Gu |
| [HDFS-11181](https://issues.apache.org/jira/browse/HDFS-11181) | Fuse wrapper has a typo |  Trivial | fuse-dfs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13847](https://issues.apache.org/jira/browse/HADOOP-13847) | KMSWebApp should close KeyProviderCryptoExtension |  Major | kms | Anthony Young-Garner | John Zhuge |
| [YARN-5559](https://issues.apache.org/jira/browse/YARN-5559) | Analyse 2.8.0/3.0.0 jdiff reports and fix any issues |  Blocker | resourcemanager | Wangda Tan | Akira Ajisaka |
| [HADOOP-13675](https://issues.apache.org/jira/browse/HADOOP-13675) | Bug in return value for delete() calls in WASB |  Major | fs/azure | Dushyanth | Dushyanth |
| [HADOOP-13864](https://issues.apache.org/jira/browse/HADOOP-13864) | KMS should not require truststore password |  Major | kms, security | Mike Yoder | Mike Yoder |
| [HADOOP-13861](https://issues.apache.org/jira/browse/HADOOP-13861) | Spelling errors in logging and exceptions for code |  Major | common, fs, io, security | Grant Sohn | Grant Sohn |
| [HDFS-11180](https://issues.apache.org/jira/browse/HDFS-11180) | Intermittent deadlock in NameNode when failover happens. |  Blocker | namenode | Abhishek Modi | Akira Ajisaka |
| [HDFS-11198](https://issues.apache.org/jira/browse/HDFS-11198) | NN UI should link DN web address using hostnames |  Critical | . | Kihwal Lee | Weiwei Yang |
| [MAPREDUCE-6571](https://issues.apache.org/jira/browse/MAPREDUCE-6571) | JobEndNotification info logs are missing in AM container syslog |  Minor | applicationmaster | Prabhu Joseph | Haibo Chen |
| [MAPREDUCE-6816](https://issues.apache.org/jira/browse/MAPREDUCE-6816) | Progress bars in Web UI always at 100% |  Blocker | webapps | Shen Yinjie | Shen Yinjie |
| [HADOOP-13859](https://issues.apache.org/jira/browse/HADOOP-13859) | TestConfigurationFieldsBase fails for fields that are DEFAULT values of skipped properties. |  Major | common | Haibo Chen | Haibo Chen |
| [YARN-5184](https://issues.apache.org/jira/browse/YARN-5184) | Fix up incompatible changes introduced on ContainerStatus and NodeReport |  Blocker | api | Karthik Kambatla | Sangjin Lee |
| [YARN-5932](https://issues.apache.org/jira/browse/YARN-5932) | Retrospect moveApplicationToQueue in align with YARN-5611 |  Major | capacity scheduler, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-5136](https://issues.apache.org/jira/browse/YARN-5136) | Error in handling event type APP\_ATTEMPT\_REMOVED to the scheduler |  Major | . | tangshangwen | Wilfred Spiegelenburg |
| [MAPREDUCE-6817](https://issues.apache.org/jira/browse/MAPREDUCE-6817) | The format of job start time in JHS is different from those of submit and finish time |  Major | jobhistoryserver | Haibo Chen | Haibo Chen |
| [YARN-5963](https://issues.apache.org/jira/browse/YARN-5963) | Spelling errors in logging and exceptions for node manager, client, web-proxy, common, and app history code |  Trivial | client, nodemanager | Grant Sohn | Grant Sohn |
| [YARN-5921](https://issues.apache.org/jira/browse/YARN-5921) | Incorrect synchronization in RMContextImpl#setHAServiceState/getHAServiceState |  Major | . | Varun Saxena | Varun Saxena |
| [HDFS-11140](https://issues.apache.org/jira/browse/HDFS-11140) | Directory Scanner should log startup message time correctly |  Minor | . | Xiao Chen | Yiqun Lin |
| [HDFS-11223](https://issues.apache.org/jira/browse/HDFS-11223) | Fix typos in HttpFs documentations |  Trivial | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-13867](https://issues.apache.org/jira/browse/HADOOP-13867) | FilterFileSystem should override rename(.., options) to take effect of Rename options called via FilterFileSystem implementations |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-11197](https://issues.apache.org/jira/browse/HDFS-11197) | Listing encryption zones fails when deleting a EZ that is on a snapshotted directory |  Minor | hdfs | Wellington Chevreuil | Wellington Chevreuil |
| [HDFS-11224](https://issues.apache.org/jira/browse/HDFS-11224) | Lifeline message should be ignored for dead nodes |  Critical | . | Vinayakumar B | Vinayakumar B |
| [YARN-4752](https://issues.apache.org/jira/browse/YARN-4752) | FairScheduler should preempt for a ResourceRequest and all preempted containers should be on the same node |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-13824](https://issues.apache.org/jira/browse/HADOOP-13824) | FsShell can suppress the real error if no error message is present |  Major | fs | Rob Vesse | John Zhuge |
| [MAPREDUCE-6820](https://issues.apache.org/jira/browse/MAPREDUCE-6820) | Fix dead links in Job relevant classes |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-8870](https://issues.apache.org/jira/browse/HDFS-8870) | Lease is leaked on write failure |  Major | hdfs-client | Rushabh S Shah | Kuhu Shukla |
| [HADOOP-13565](https://issues.apache.org/jira/browse/HADOOP-13565) | KerberosAuthenticationHandler#authenticate should not rebuild SPN based on client request |  Major | security | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-11229](https://issues.apache.org/jira/browse/HDFS-11229) | HDFS-11056 failed to close meta file |  Blocker | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11226](https://issues.apache.org/jira/browse/HDFS-11226) | cacheadmin,cryptoadmin and storagepolicyadmin should support generic options |  Minor | tools | Archana T | Brahma Reddy Battula |
| [HDFS-11233](https://issues.apache.org/jira/browse/HDFS-11233) | Fix javac warnings related to the deprecated APIs after upgrading Jackson |  Minor | . | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-6821](https://issues.apache.org/jira/browse/MAPREDUCE-6821) | Fix javac warning related to the deprecated APIs after upgrading Jackson |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-11164](https://issues.apache.org/jira/browse/HDFS-11164) | Mover should avoid unnecessary retries if the block is pinned |  Major | balancer & mover | Rakesh R | Rakesh R |
| [HDFS-10684](https://issues.apache.org/jira/browse/HDFS-10684) | WebHDFS DataNode calls fail without parameter createparent |  Blocker | webhdfs | Samuel Low | John Zhuge |
| [HDFS-11204](https://issues.apache.org/jira/browse/HDFS-11204) | Document the missing options of hdfs zkfc command |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-13890](https://issues.apache.org/jira/browse/HADOOP-13890) | Maintain HTTP/host as SPNEGO SPN support and fix KerberosName parsing |  Major | test | Brahma Reddy Battula | Xiaoyu Yao |
| [YARN-5999](https://issues.apache.org/jira/browse/YARN-5999) | AMRMClientAsync will stop if any exceptions thrown on allocate call |  Major | . | Jian He | Jian He |
| [HADOOP-13831](https://issues.apache.org/jira/browse/HADOOP-13831) | Correct check for error code to detect Azure Storage Throttling and provide retries |  Major | fs/azure | Gaurav Kanade | Gaurav Kanade |
| [HADOOP-13508](https://issues.apache.org/jira/browse/HADOOP-13508) | FsPermission string constructor does not recognize sticky bit |  Major | . | Atul Sikaria | Atul Sikaria |
| [HDFS-11094](https://issues.apache.org/jira/browse/HDFS-11094) | Send back HAState along with NamespaceInfo during a versionRequest as an optional parameter |  Major | datanode | Eric Badger | Eric Badger |
| [HDFS-11253](https://issues.apache.org/jira/browse/HDFS-11253) | FileInputStream leak on failure path in BlockSender |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-9911](https://issues.apache.org/jira/browse/HDFS-9911) | TestDataNodeLifeline  Fails intermittently |  Major | datanode | Anu Engineer | Yiqun Lin |
| [HDFS-11160](https://issues.apache.org/jira/browse/HDFS-11160) | VolumeScanner reports write-in-progress replicas as corrupt incorrectly |  Major | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13913](https://issues.apache.org/jira/browse/HADOOP-13913) | maven issuing pom.version being deprecation warnings |  Minor | build | Joe Pallas | Steve Loughran |
| [YARN-4330](https://issues.apache.org/jira/browse/YARN-4330) | MiniYARNCluster is showing multiple  Failed to instantiate default resource calculator warning messages. |  Blocker | test, yarn | Steve Loughran | Varun Saxena |
| [YARN-5877](https://issues.apache.org/jira/browse/YARN-5877) | Allow all env's from yarn.nodemanager.env-whitelist to get overridden during launch |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-11263](https://issues.apache.org/jira/browse/HDFS-11263) | ClassCastException when we use Bzipcodec for Fsimage compression |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11195](https://issues.apache.org/jira/browse/HDFS-11195) | Return error when appending files by webhdfs rest api fails |  Major | . | Yuanbo Liu | Yuanbo Liu |
| [HDFS-11247](https://issues.apache.org/jira/browse/HDFS-11247) | Add a test to verify NameNodeMXBean#getDecomNodes() and Live/Dead Decom Nodes shown in NameNode WebUI |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-11261](https://issues.apache.org/jira/browse/HDFS-11261) | Document missing NameNode metrics |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-5903](https://issues.apache.org/jira/browse/YARN-5903) | Fix race condition in TestResourceManagerAdministrationProtocolPBClientImpl beforeclass setup method |  Major | yarn | Haibo Chen | Haibo Chen |
| [YARN-5774](https://issues.apache.org/jira/browse/YARN-5774) | MR Job stuck in ACCEPTED status without any progress in Fair Scheduler if set yarn.scheduler.minimum-allocation-mb to 0. |  Blocker | resourcemanager | Yufei Gu | Yufei Gu |
| [HDFS-11258](https://issues.apache.org/jira/browse/HDFS-11258) | File mtime change could not save to editlog |  Critical | . | Jimmy Xiang | Jimmy Xiang |
| [MAPREDUCE-6704](https://issues.apache.org/jira/browse/MAPREDUCE-6704) | Update the documents to run MapReduce application |  Blocker | documentation | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-6000](https://issues.apache.org/jira/browse/YARN-6000) | Make AllocationFileLoaderService.Listener public |  Major | fairscheduler, yarn | Tao Jie | Tao Jie |
| [HDFS-11271](https://issues.apache.org/jira/browse/HDFS-11271) | Typo in NameNode UI |  Trivial | namenode, ui | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11250](https://issues.apache.org/jira/browse/HDFS-11250) | Fix a typo in ReplicaUnderRecovery#setRecoveryID |  Trivial | . | Yiqun Lin | Yiqun Lin |
| [YARN-6026](https://issues.apache.org/jira/browse/YARN-6026) | A couple of spelling errors in the docs |  Trivial | documentation | Grant Sohn | Grant Sohn |
| [HADOOP-13940](https://issues.apache.org/jira/browse/HADOOP-13940) | Document the missing envvars commands |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-13943](https://issues.apache.org/jira/browse/HADOOP-13943) | TestCommonConfigurationFields#testCompareXmlAgainstConfigurationClass fails after HADOOP-13863 |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11270](https://issues.apache.org/jira/browse/HDFS-11270) | Document the missing options of NameNode bootstrap command |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-13932](https://issues.apache.org/jira/browse/HADOOP-13932) | Fix indefinite article in comments |  Minor | . | LiXin Ge | LiXin Ge |
| [YARN-5962](https://issues.apache.org/jira/browse/YARN-5962) | Spelling errors in logging and exceptions for resource manager code |  Trivial | resourcemanager | Grant Sohn | Grant Sohn |
| [YARN-5257](https://issues.apache.org/jira/browse/YARN-5257) | Fix unreleased resources and null dereferences |  Major | . | Yufei Gu | Yufei Gu |
| [HDFS-11252](https://issues.apache.org/jira/browse/HDFS-11252) | TestFileTruncate#testTruncateWithDataNodesRestartImmediately can fail with BindException |  Major | . | Jason Lowe | Yiqun Lin |
| [YARN-6001](https://issues.apache.org/jira/browse/YARN-6001) | Improve moveApplicationQueues command line |  Major | client | Sunil Govindan | Sunil Govindan |
| [YARN-6024](https://issues.apache.org/jira/browse/YARN-6024) | Capacity Scheduler 'continuous reservation looking' doesn't work when sum of queue's used and reserved resources is equal to max |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-4882](https://issues.apache.org/jira/browse/YARN-4882) | Change the log level to DEBUG for recovering completed applications |  Major | resourcemanager | Rohith Sharma K S | Daniel Templeton |
| [HDFS-11251](https://issues.apache.org/jira/browse/HDFS-11251) | ConcurrentModificationException during DataNode#refreshVolumes |  Major | . | Jason Lowe | Manoj Govindassamy |
| [HDFS-11267](https://issues.apache.org/jira/browse/HDFS-11267) | Avoid redefinition of storageDirs in NNStorage and cleanup its accessors in Storage |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [MAPREDUCE-5155](https://issues.apache.org/jira/browse/MAPREDUCE-5155) | Race condition in test case TestFetchFailure cause it to fail |  Minor | test | Nemon Lou | Haibo Chen |
| [HADOOP-13942](https://issues.apache.org/jira/browse/HADOOP-13942) | Build failure due to errors of javadoc build in hadoop-azure |  Major | fs/azure | Kai Sasaki | Kai Sasaki |
| [HADOOP-13883](https://issues.apache.org/jira/browse/HADOOP-13883) | Add description of -fs option in generic command usage |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-13922](https://issues.apache.org/jira/browse/HADOOP-13922) | Some modules have dependencies on hadoop-client jar removed by HADOOP-11804 |  Blocker | build | Joe Pallas | Sean Busbey |
| [HADOOP-12733](https://issues.apache.org/jira/browse/HADOOP-12733) | Remove references to obsolete io.seqfile configuration variables |  Minor | . | Ray Chiang | Ray Chiang |
| [YARN-5988](https://issues.apache.org/jira/browse/YARN-5988) | RM unable to start in secure setup |  Blocker | . | Ajith S | Ajith S |
| [HDFS-11280](https://issues.apache.org/jira/browse/HDFS-11280) | Allow WebHDFS to reuse HTTP connections to NN |  Major | hdfs | Zheng Shao | Zheng Shao |
| [HADOOP-13896](https://issues.apache.org/jira/browse/HADOOP-13896) | Invoke assembly plugin after building jars |  Blocker | build | Allen Wittenauer | Andrew Wang |
| [HADOOP-13780](https://issues.apache.org/jira/browse/HADOOP-13780) | LICENSE/NOTICE are out of date for source artifacts |  Blocker | common | Sean Busbey | Xiao Chen |
| [MAPREDUCE-6715](https://issues.apache.org/jira/browse/MAPREDUCE-6715) | Fix Several Unsafe Practices |  Major | . | Yufei Gu | Yufei Gu |
| [HDFS-11282](https://issues.apache.org/jira/browse/HDFS-11282) | Document the missing metrics of DataNode Volume IO operations |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-6711](https://issues.apache.org/jira/browse/MAPREDUCE-6711) | JobImpl fails to handle preemption events on state COMMITTING |  Major | . | Li Lu | Prabhu Joseph |
| [YARN-6066](https://issues.apache.org/jira/browse/YARN-6066) | Opportunistic containers minor fixes: API annotations and config parameter changes |  Minor | . | Arun Suresh | Arun Suresh |
| [HADOOP-13958](https://issues.apache.org/jira/browse/HADOOP-13958) | Bump up release year to 2017 |  Blocker | . | Junping Du | Junping Du |
| [YARN-6068](https://issues.apache.org/jira/browse/YARN-6068) | Log aggregation get failed when NM restart even with recovery |  Blocker | . | Junping Du | Junping Du |
| [HDFS-11301](https://issues.apache.org/jira/browse/HDFS-11301) | Double wrapping over RandomAccessFile in LocalReplicaInPipeline#createStreams |  Minor | hdfs | Hanisha Koneru | Hanisha Koneru |
| [YARN-6073](https://issues.apache.org/jira/browse/YARN-6073) | Misuse of format specifier in Preconditions.checkArgument |  Trivial | . | Yongjun Zhang | Yuanbo Liu |
| [YARN-5899](https://issues.apache.org/jira/browse/YARN-5899) | Debug log in AbstractCSQueue#canAssignToThisQueue needs improvement |  Trivial | capacity scheduler | Ying Zhang | Ying Zhang |
| [YARN-6054](https://issues.apache.org/jira/browse/YARN-6054) | TimelineServer fails to start when some LevelDb state files are missing. |  Critical | . | Ravi Prakash | Ravi Prakash |
| [YARN-5937](https://issues.apache.org/jira/browse/YARN-5937) | stop-yarn.sh is not able to gracefully stop node managers |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-6022](https://issues.apache.org/jira/browse/YARN-6022) | Revert changes of AbstractResourceRequest |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-4148](https://issues.apache.org/jira/browse/YARN-4148) | When killing app, RM releases app's resource before they are released by NM |  Major | resourcemanager | Jun Gong | Jason Lowe |
| [YARN-6079](https://issues.apache.org/jira/browse/YARN-6079) | simple spelling errors in yarn test code |  Trivial | test | Grant Sohn | vijay |
| [HADOOP-13903](https://issues.apache.org/jira/browse/HADOOP-13903) | Improvements to KMS logging to help debug authorization errors |  Minor | kms | Tristan Stevens | Tristan Stevens |
| [HDFS-9935](https://issues.apache.org/jira/browse/HDFS-9935) | Remove LEASE\_{SOFTLIMIT,HARDLIMIT}\_PERIOD and unused import from HdfsServerConstants |  Minor | . | Yiqun Lin | Yiqun Lin |
| [YARN-5554](https://issues.apache.org/jira/browse/YARN-5554) | MoveApplicationAcrossQueues does not check user permission on the target queue |  Major | resourcemanager | Haibo Chen | Wilfred Spiegelenburg |
| [HDFS-11312](https://issues.apache.org/jira/browse/HDFS-11312) | Fix incompatible tag number change for nonDfsUsed in DatanodeInfoProto |  Blocker | . | Sean Mackrory | Sean Mackrory |
| [HADOOP-13961](https://issues.apache.org/jira/browse/HADOOP-13961) | Fix compilation failure from missing hadoop-kms test jar |  Blocker | build | Karthik Kambatla | Andrew Wang |
| [YARN-6072](https://issues.apache.org/jira/browse/YARN-6072) | RM unable to start in secure mode |  Blocker | resourcemanager | Bibin A Chundatt | Ajith S |
| [YARN-6081](https://issues.apache.org/jira/browse/YARN-6081) | LeafQueue#getTotalPendingResourcesConsideringUserLimit should deduct reserved from pending to avoid unnecessary preemption of reserved container |  Critical | . | Wangda Tan | Wangda Tan |
| [HDFS-11344](https://issues.apache.org/jira/browse/HDFS-11344) | The default value of the setting dfs.disk.balancer.block.tolerance.percent is different |  Trivial | diskbalancer | Yiqun Lin | Yiqun Lin |
| [HADOOP-13928](https://issues.apache.org/jira/browse/HADOOP-13928) | TestAdlFileContextMainOperationsLive.testGetFileContext1 runtime error |  Major | fs/adl, test | John Zhuge | John Zhuge |
| [HDFS-11342](https://issues.apache.org/jira/browse/HDFS-11342) | Fix FileInputStream leak in loadLastPartialChunkChecksum |  Major | datanode | Arpit Agarwal | Chen Liang |
| [HDFS-11307](https://issues.apache.org/jira/browse/HDFS-11307) | The rpc to portmap service for NFS has hardcoded timeout. |  Major | . | Jitendra Nath Pandey | Mukul Kumar Singh |
| [YARN-6057](https://issues.apache.org/jira/browse/YARN-6057) | yarn.scheduler.minimum-allocation-\* descriptions are incorrect about behavior when a request is out of bounds |  Minor | . | Bibin A Chundatt | Julia Sommer |
| [HADOOP-13976](https://issues.apache.org/jira/browse/HADOOP-13976) | Path globbing does not match newlines |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11209](https://issues.apache.org/jira/browse/HDFS-11209) | SNN can't checkpoint when rolling upgrade is not finalized |  Critical | rolling upgrades | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-10733](https://issues.apache.org/jira/browse/HDFS-10733) | NameNode terminated after full GC thinking QJM is unresponsive. |  Major | namenode, qjm | Konstantin Shvachko | Vinitha Reddy Gankidi |
| [HDFS-10759](https://issues.apache.org/jira/browse/HDFS-10759) | Change fsimage bool isStriped from boolean to an enum |  Major | hdfs | Ewan Higgs | Ewan Higgs |
| [HADOOP-13965](https://issues.apache.org/jira/browse/HADOOP-13965) | Groups should be consistent in using default group mapping class |  Minor | security | Yiqun Lin | Yiqun Lin |
| [HDFS-11316](https://issues.apache.org/jira/browse/HDFS-11316) | TestDataNodeVolumeFailure#testUnderReplicationAfterVolFailure fails in trunk |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-11287](https://issues.apache.org/jira/browse/HDFS-11287) | Storage class member storageDirs should be private to avoid unprotected access by derived classes |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-11268](https://issues.apache.org/jira/browse/HDFS-11268) | Correctly reconstruct erasure coding file from FSImage |  Critical | erasure-coding | Sammi Chen | Sammi Chen |
| [HADOOP-14001](https://issues.apache.org/jira/browse/HADOOP-14001) | Improve delegation token validity checking |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-6110](https://issues.apache.org/jira/browse/YARN-6110) | Fix opportunistic containers documentation |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-6104](https://issues.apache.org/jira/browse/YARN-6104) | RegistrySecurity overrides zookeeper sasl system properties |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HADOOP-14004](https://issues.apache.org/jira/browse/HADOOP-14004) | Missing hadoop-cloud-storage-project module in pom.xml |  Critical | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-5271](https://issues.apache.org/jira/browse/YARN-5271) | ATS client doesn't work with Jersey 2 on the classpath |  Major | client, timelineserver | Steve Loughran | Weiwei Yang |
| [HDFS-11132](https://issues.apache.org/jira/browse/HDFS-11132) | Allow AccessControlException in contract tests when getFileStatus on subdirectory of existing files |  Major | fs/adl, test | Vishwajeet Dusane | Vishwajeet Dusane |
| [HADOOP-13996](https://issues.apache.org/jira/browse/HADOOP-13996) | Fix some release build issues |  Blocker | build | Andrew Wang | Andrew Wang |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-10657](https://issues.apache.org/jira/browse/HDFS-10657) | testAclCLI.xml setfacl test should expect mask r-x |  Minor | . | John Zhuge | John Zhuge |
| [YARN-5656](https://issues.apache.org/jira/browse/YARN-5656) | Fix ReservationACLsTestBase |  Major | . | Sean Po | Sean Po |
| [HDFS-9333](https://issues.apache.org/jira/browse/HDFS-9333) | Some tests using MiniDFSCluster errored complaining port in use |  Minor | test | Kai Zheng | Masatake Iwasaki |
| [HADOOP-13686](https://issues.apache.org/jira/browse/HADOOP-13686) | Adding additional unit test for Trash (I) |  Major | . | Xiaoyu Yao | Weiwei Yang |
| [YARN-4555](https://issues.apache.org/jira/browse/YARN-4555) | TestDefaultContainerExecutor#testContainerLaunchError fails on non-english locale environment |  Minor | nodemanager, test | Takashi Ohnishi | Takashi Ohnishi |
| [YARN-4363](https://issues.apache.org/jira/browse/YARN-4363) | In TestFairScheduler, testcase should not create FairScheduler redundantly |  Trivial | fairscheduler | Tao Jie | Tao Jie |
| [YARN-3568](https://issues.apache.org/jira/browse/YARN-3568) | TestAMRMTokens should use some random port |  Major | test | Gera Shegalov | Takashi Ohnishi |
| [YARN-3460](https://issues.apache.org/jira/browse/YARN-3460) | TestSecureRMRegistryOperations fails with IBM\_JAVA |  Major | . | pascal oliva | pascal oliva |
| [MAPREDUCE-6804](https://issues.apache.org/jira/browse/MAPREDUCE-6804) | Add timeout when starting JobHistoryServer in MiniMRYarnCluster |  Minor | test | Andras Bokor | Andras Bokor |
| [MAPREDUCE-6743](https://issues.apache.org/jira/browse/MAPREDUCE-6743) | nativetask unit tests need to provide usable output; fix link errors during mvn test |  Major | nativetask | Allen Wittenauer | Allen Wittenauer |
| [HDFS-11272](https://issues.apache.org/jira/browse/HDFS-11272) | Refine the assert messages in TestFSDirAttrOp |  Minor | test | Akira Ajisaka | Jimmy Xiang |
| [HDFS-11278](https://issues.apache.org/jira/browse/HDFS-11278) | Add missing @Test annotation for TestSafeMode.testSafeModeUtils() |  Trivial | namenode | Lukas Majercak | Lukas Majercak |
| [MAPREDUCE-6831](https://issues.apache.org/jira/browse/MAPREDUCE-6831) | Flaky test TestJobImpl.testKilledDuringKillAbort |  Major | mrv2 | Peter Bacsko | Peter Bacsko |
| [HDFS-11290](https://issues.apache.org/jira/browse/HDFS-11290) | TestFSNameSystemMBean should wait until JMX cache is cleared |  Major | test | Akira Ajisaka | Erik Krogen |
| [YARN-5608](https://issues.apache.org/jira/browse/YARN-5608) | TestAMRMClient.setup() fails with ArrayOutOfBoundsException |  Major | test | Daniel Templeton | Daniel Templeton |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4733](https://issues.apache.org/jira/browse/YARN-4733) | [YARN-3368] Initial commit of new YARN web UI |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-4517](https://issues.apache.org/jira/browse/YARN-4517) | [YARN-3368] Add nodes page |  Major | yarn | Wangda Tan | Varun Saxena |
| [YARN-4514](https://issues.apache.org/jira/browse/YARN-4514) | [YARN-3368] Cleanup hardcoded configurations, such as RM/ATS addresses |  Major | . | Wangda Tan | Sunil Govindan |
| [YARN-5019](https://issues.apache.org/jira/browse/YARN-5019) | [YARN-3368] Change urls in new YARN ui from camel casing to hyphens |  Major | . | Varun Vasudev | Sunil Govindan |
| [YARN-5000](https://issues.apache.org/jira/browse/YARN-5000) | [YARN-3368] App attempt page is not loading when timeline server is not started |  Major | . | Sunil Govindan | Sunil Govindan |
| [YARN-5038](https://issues.apache.org/jira/browse/YARN-5038) | [YARN-3368] Application and Container pages shows wrong values when RM is stopped |  Major | . | Sunil Govindan | Sunil Govindan |
| [YARN-4515](https://issues.apache.org/jira/browse/YARN-4515) | [YARN-3368] Support hosting web UI framework inside YARN RM |  Major | . | Wangda Tan | Sunil Govindan |
| [YARN-5183](https://issues.apache.org/jira/browse/YARN-5183) | [YARN-3368] Support for responsive navbar when window is resized |  Major | . | Kai Sasaki | Kai Sasaki |
| [YARN-5161](https://issues.apache.org/jira/browse/YARN-5161) | [YARN-3368] Add Apache Hadoop logo in YarnUI home page |  Major | webapp | Sunil Govindan | Kai Sasaki |
| [YARN-5344](https://issues.apache.org/jira/browse/YARN-5344) | [YARN-3368] Generic UI improvements |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [YARN-5345](https://issues.apache.org/jira/browse/YARN-5345) | [YARN-3368] Cluster overview page improvements |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [YARN-5346](https://issues.apache.org/jira/browse/YARN-5346) | [YARN-3368] Queues page improvements |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [YARN-5347](https://issues.apache.org/jira/browse/YARN-5347) | [YARN-3368] Applications page improvements |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [YARN-5348](https://issues.apache.org/jira/browse/YARN-5348) | [YARN-3368] Node details page improvements |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [YARN-5321](https://issues.apache.org/jira/browse/YARN-5321) | [YARN-3368] Add resource usage for application by node managers |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-5322](https://issues.apache.org/jira/browse/YARN-5322) | [YARN-3368] Add a node heat chart map |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-5334](https://issues.apache.org/jira/browse/YARN-5334) | [YARN-3368] Introduce REFRESH button in various UI pages |  Major | webapp | Sunil Govindan | Sreenath Somarajapuram |
| [YARN-5509](https://issues.apache.org/jira/browse/YARN-5509) | Build error due to preparing 3.0.0-alpha2 deployment |  Major | yarn | Kai Sasaki | Kai Sasaki |
| [YARN-5488](https://issues.apache.org/jira/browse/YARN-5488) | Applications table overflows beyond the page boundary |  Major | . | Harish Jaiprakash | Harish Jaiprakash |
| [YARN-5504](https://issues.apache.org/jira/browse/YARN-5504) | [YARN-3368] Fix YARN UI build pom.xml |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [HADOOP-13355](https://issues.apache.org/jira/browse/HADOOP-13355) | Handle HADOOP\_CLIENT\_OPTS in a function |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-5583](https://issues.apache.org/jira/browse/YARN-5583) | [YARN-3368] Fix wrong paths in .gitignore |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [HADOOP-13554](https://issues.apache.org/jira/browse/HADOOP-13554) | Add an equivalent of hadoop\_subcmd\_opts for secure opts |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-5503](https://issues.apache.org/jira/browse/YARN-5503) | [YARN-3368] Add missing hidden files in webapp folder for deployment |  Major | . | Sreenath Somarajapuram | Sreenath Somarajapuram |
| [HADOOP-13562](https://issues.apache.org/jira/browse/HADOOP-13562) | Change hadoop\_subcommand\_opts to use only uppercase |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13358](https://issues.apache.org/jira/browse/HADOOP-13358) | Modify HDFS to use hadoop\_subcommand\_opts |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13356](https://issues.apache.org/jira/browse/HADOOP-13356) | Add a function to handle command\_subcommand\_OPTS |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13357](https://issues.apache.org/jira/browse/HADOOP-13357) | Modify common to use hadoop\_subcommand\_opts |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13359](https://issues.apache.org/jira/browse/HADOOP-13359) | Modify YARN to use hadoop\_subcommand\_opts |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HDFS-9392](https://issues.apache.org/jira/browse/HDFS-9392) | Admins support for maintenance state |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-13564](https://issues.apache.org/jira/browse/HADOOP-13564) | modify mapred to use hadoop\_subcommand\_opts |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HDFS-10813](https://issues.apache.org/jira/browse/HDFS-10813) | DiskBalancer: Add the getNodeList method in Command |  Minor | balancer & mover | Yiqun Lin | Yiqun Lin |
| [HADOOP-13563](https://issues.apache.org/jira/browse/HADOOP-13563) | hadoop\_subcommand\_opts should print name not actual content during debug |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-13360](https://issues.apache.org/jira/browse/HADOOP-13360) | Documentation for HADOOP\_subcommand\_OPTS |  Major | scripts | Allen Wittenauer | Allen Wittenauer |
| [YARN-5221](https://issues.apache.org/jira/browse/YARN-5221) | Expose UpdateResourceRequest API to allow AM to request for change in container properties |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-5596](https://issues.apache.org/jira/browse/YARN-5596) | Fix failing unit test in TestDockerContainerRuntime |  Minor | nodemanager, yarn | Sidharta Seethana | Sidharta Seethana |
| [HADOOP-13547](https://issues.apache.org/jira/browse/HADOOP-13547) | Optimize IPC client protobuf decoding |  Major | . | Daryn Sharp | Daryn Sharp |
| [YARN-5264](https://issues.apache.org/jira/browse/YARN-5264) | Store all queue-specific information in FSQueue |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [YARN-5576](https://issues.apache.org/jira/browse/YARN-5576) | Allow resource localization while container is running |  Major | . | Jian He | Jian He |
| [HADOOP-13549](https://issues.apache.org/jira/browse/HADOOP-13549) | Eliminate intermediate buffer for server-side PB encoding |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-13447](https://issues.apache.org/jira/browse/HADOOP-13447) | Refactor S3AFileSystem to support introduction of separate metadata repository and tests. |  Major | fs/s3 | Chris Nauroth | Chris Nauroth |
| [YARN-5598](https://issues.apache.org/jira/browse/YARN-5598) | [YARN-3368] Fix create-release to be able to generate bits for the new yarn-ui |  Major | yarn, yarn-ui-v2 | Wangda Tan | Wangda Tan |
| [HDFS-9847](https://issues.apache.org/jira/browse/HDFS-9847) | HDFS configuration should accept time units |  Major | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-13541](https://issues.apache.org/jira/browse/HADOOP-13541) | explicitly declare the Joda time version S3A depends on |  Minor | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-8901](https://issues.apache.org/jira/browse/HDFS-8901) | Use ByteBuffer in striping positional read |  Major | erasure-coding | Kai Zheng | Sammi Chen |
| [HDFS-10845](https://issues.apache.org/jira/browse/HDFS-10845) | Change defaults in hdfs-site.xml to match timeunit type |  Minor | datanode, namenode | Yiqun Lin | Yiqun Lin |
| [HDFS-10553](https://issues.apache.org/jira/browse/HDFS-10553) | DiskBalancer: Rename Tools/DiskBalancer class to Tools/DiskBalancerCLI |  Minor | balancer & mover | Anu Engineer | Manoj Govindassamy |
| [HDFS-9849](https://issues.apache.org/jira/browse/HDFS-9849) | DiskBalancer : reduce lock path in shutdown code |  Major | balancer & mover | Anu Engineer | Yuanbo Liu |
| [YARN-5566](https://issues.apache.org/jira/browse/YARN-5566) | Client-side NM graceful decom is not triggered when jobs finish |  Major | nodemanager | Robert Kanter | Robert Kanter |
| [HADOOP-10940](https://issues.apache.org/jira/browse/HADOOP-10940) | RPC client does no bounds checking of responses |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-13540](https://issues.apache.org/jira/browse/HADOOP-13540) | improve section on troubleshooting s3a auth problems |  Minor | documentation, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-10808](https://issues.apache.org/jira/browse/HDFS-10808) | DiskBalancer does not execute multi-steps plan-redux |  Major | balancer & mover | Anu Engineer | Anu Engineer |
| [HDFS-10821](https://issues.apache.org/jira/browse/HDFS-10821) | DiskBalancer: Report command support with multiple nodes |  Major | balancer & mover | Yiqun Lin | Yiqun Lin |
| [HDFS-10858](https://issues.apache.org/jira/browse/HDFS-10858) | FBR processing may generate incorrect reportedBlock-blockGroup mapping |  Blocker | erasure-coding | Jing Zhao | Jing Zhao |
| [HDFS-10599](https://issues.apache.org/jira/browse/HDFS-10599) | DiskBalancer: Execute CLI via Shell |  Major | balancer & mover | Anu Engineer | Manoj Govindassamy |
| [HADOOP-13546](https://issues.apache.org/jira/browse/HADOOP-13546) | Override equals and hashCode to avoid connection leakage |  Major | ipc | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-10562](https://issues.apache.org/jira/browse/HDFS-10562) | DiskBalancer: update documentation on how to report issues and debug |  Minor | balancer & mover | Anu Engineer | Anu Engineer |
| [HDFS-10805](https://issues.apache.org/jira/browse/HDFS-10805) | Reduce runtime for append test |  Minor | test | Gergely Novák | Gergely Novák |
| [YARN-5620](https://issues.apache.org/jira/browse/YARN-5620) | Core changes in NodeManager to support re-initialization of Containers with new launchContext |  Major | . | Arun Suresh | Arun Suresh |
| [HADOOP-13621](https://issues.apache.org/jira/browse/HADOOP-13621) | s3:// should have been fully cut off from trunk |  Major | documentation, fs/s3 | Mingliang Liu | Mingliang Liu |
| [YARN-5637](https://issues.apache.org/jira/browse/YARN-5637) | Changes in NodeManager to support Container rollback and commit |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-3140](https://issues.apache.org/jira/browse/YARN-3140) | Improve locks in AbstractCSQueue/LeafQueue/ParentQueue |  Major | resourcemanager, scheduler | Wangda Tan | Wangda Tan |
| [YARN-3141](https://issues.apache.org/jira/browse/YARN-3141) | Improve locks in SchedulerApplicationAttempt/FSAppAttempt/FiCaSchedulerApp |  Major | resourcemanager, scheduler | Wangda Tan | Wangda Tan |
| [HDFS-10871](https://issues.apache.org/jira/browse/HDFS-10871) | DiskBalancerWorkItem should not import jackson relocated by htrace |  Major | hdfs-client | Masatake Iwasaki | Manoj Govindassamy |
| [YARN-5609](https://issues.apache.org/jira/browse/YARN-5609) | Expose upgrade and restart API in ContainerManagementProtocol |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-9895](https://issues.apache.org/jira/browse/HDFS-9895) | Remove unnecessary conf cache from DataNode |  Major | datanode | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-13544](https://issues.apache.org/jira/browse/HADOOP-13544) | JDiff reports unncessarily show unannotated APIs and cause confusion while our javadocs only show annotated and public APIs |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-3142](https://issues.apache.org/jira/browse/YARN-3142) | Improve locks in AppSchedulingInfo |  Major | resourcemanager, scheduler | Wangda Tan | Varun Saxena |
| [HDFS-10900](https://issues.apache.org/jira/browse/HDFS-10900) | DiskBalancer: Complete the documents for the report command |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-9850](https://issues.apache.org/jira/browse/HDFS-9850) | DiskBalancer : Explore removing references to FsVolumeSpi |  Major | balancer & mover | Anu Engineer | Manoj Govindassamy |
| [HDFS-10779](https://issues.apache.org/jira/browse/HDFS-10779) | Rename does not need to re-solve destination |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10892](https://issues.apache.org/jira/browse/HDFS-10892) | Add unit tests for HDFS command 'dfs -tail' and 'dfs -stat' |  Major | fs, shell, test | Mingliang Liu | Mingliang Liu |
| [HADOOP-13599](https://issues.apache.org/jira/browse/HADOOP-13599) | s3a close() to be non-synchronized, so avoid risk of deadlock on shutdown |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-4205](https://issues.apache.org/jira/browse/YARN-4205) | Add a service for monitoring application life time out |  Major | scheduler | nijel | Rohith Sharma K S |
| [YARN-5486](https://issues.apache.org/jira/browse/YARN-5486) | Update OpportunisticContainerAllocatorAMService::allocate method to handle OPPORTUNISTIC container requests |  Major | resourcemanager | Arun Suresh | Konstantinos Karanasos |
| [HADOOP-12974](https://issues.apache.org/jira/browse/HADOOP-12974) | Create a CachingGetSpaceUsed implementation that uses df |  Major | . | Elliott Clark | Elliott Clark |
| [HDFS-10851](https://issues.apache.org/jira/browse/HDFS-10851) | FSDirStatAndListingOp: stop passing path as string |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [YARN-5384](https://issues.apache.org/jira/browse/YARN-5384) | Expose priority in ReservationSystem submission APIs |  Major | capacity scheduler, fairscheduler, resourcemanager | Sean Po | Sean Po |
| [HDFS-10619](https://issues.apache.org/jira/browse/HDFS-10619) | Cache path in InodesInPath |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10934](https://issues.apache.org/jira/browse/HDFS-10934) | TestDFSShell.testStat fails intermittently |  Major | test | Eric Badger | Eric Badger |
| [YARN-5682](https://issues.apache.org/jira/browse/YARN-5682) | [YARN-3368] Fix maven build to keep all generated or downloaded files in target folder |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-13674](https://issues.apache.org/jira/browse/HADOOP-13674) | S3A can provide a more detailed error message when accessing a bucket through an incorrect S3 endpoint. |  Minor | fs/s3 | Chris Nauroth | Chris Nauroth |
| [HDFS-10956](https://issues.apache.org/jira/browse/HDFS-10956) | Remove rename/delete performance penalty when not using snapshots |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10896](https://issues.apache.org/jira/browse/HDFS-10896) | Move lock logging logic from FSNamesystem into FSNamesystemLock |  Major | namenode | Erik Krogen | Erik Krogen |
| [YARN-5702](https://issues.apache.org/jira/browse/YARN-5702) | Refactor TestPBImplRecords so that we can reuse for testing protocol records in other YARN modules |  Major | . | Subru Krishnan | Subru Krishnan |
| [YARN-3139](https://issues.apache.org/jira/browse/YARN-3139) | Improve locks in AbstractYarnScheduler/CapacityScheduler/FairScheduler |  Major | resourcemanager, scheduler | Wangda Tan | Wangda Tan |
| [HDFS-10893](https://issues.apache.org/jira/browse/HDFS-10893) | Refactor TestDFSShell by setting up MiniDFSCluser once for all commands test |  Major | test | Mingliang Liu | Mingliang Liu |
| [HDFS-10955](https://issues.apache.org/jira/browse/HDFS-10955) | Pass IIP for FSDirAttr methods |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10826](https://issues.apache.org/jira/browse/HDFS-10826) | Correctly report missing EC blocks in FSCK |  Major | erasure-coding | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-12977](https://issues.apache.org/jira/browse/HADOOP-12977) | s3a to handle delete("/", true) robustly |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-10939](https://issues.apache.org/jira/browse/HDFS-10939) | Reduce performance penalty of encryption zones |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HADOOP-13692](https://issues.apache.org/jira/browse/HADOOP-13692) | hadoop-aws should declare explicit dependency on Jackson 2 jars to prevent classpath conflicts. |  Minor | fs/s3 | Chris Nauroth | Chris Nauroth |
| [HDFS-10979](https://issues.apache.org/jira/browse/HDFS-10979) | Pass IIP for FSDirDeleteOp methods |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HADOOP-13627](https://issues.apache.org/jira/browse/HADOOP-13627) | Have an explicit KerberosAuthException for UGI to throw, text from public constants |  Major | security | Steve Loughran | Xiao Chen |
| [HDFS-10980](https://issues.apache.org/jira/browse/HDFS-10980) | Optimize check for existence of parent directory |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10968](https://issues.apache.org/jira/browse/HDFS-10968) | BlockManager#isInNewRack should consider decommissioning nodes |  Major | erasure-coding, namenode | Jing Zhao | Jing Zhao |
| [HDFS-10988](https://issues.apache.org/jira/browse/HDFS-10988) | Refactor TestBalancerBandwidth |  Major | balancer & mover, test | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10985](https://issues.apache.org/jira/browse/HDFS-10985) | o.a.h.ha.TestZKFailoverController should not use fixed time sleep before assertions |  Minor | ha, test | Mingliang Liu | Mingliang Liu |
| [HDFS-10972](https://issues.apache.org/jira/browse/HDFS-10972) | Add unit test for HDFS command 'dfsadmin -getDatanodeInfo' |  Major | fs, shell, test | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-10965](https://issues.apache.org/jira/browse/HDFS-10965) | Add unit test for HDFS command 'dfsadmin -printTopology' |  Major | fs, shell, test | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-5698](https://issues.apache.org/jira/browse/YARN-5698) | [YARN-3368] Launch new YARN UI under hadoop web app port |  Major | . | Sunil Govindan | Sunil Govindan |
| [HDFS-10949](https://issues.apache.org/jira/browse/HDFS-10949) | DiskBalancer: deprecate TestDiskBalancer#setVolumeCapacity |  Minor | balancer & mover | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-13417](https://issues.apache.org/jira/browse/HADOOP-13417) | Fix javac and checkstyle warnings in hadoop-auth package |  Major | . | Kai Sasaki | Kai Sasaki |
| [HDFS-10827](https://issues.apache.org/jira/browse/HDFS-10827) | When there are unrecoverable ec block groups, Namenode Web UI shows "There are X missing blocks." but doesn't show the block names. |  Major | erasure-coding | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-11008](https://issues.apache.org/jira/browse/HDFS-11008) | Change unit test for testing parsing "-source" parameter in Balancer CLI |  Major | test | Mingliang Liu | Mingliang Liu |
| [HDFS-10558](https://issues.apache.org/jira/browse/HDFS-10558) | DiskBalancer: Print the full path to  plan file |  Minor | balancer & mover | Anu Engineer | Xiaobing Zhou |
| [YARN-5699](https://issues.apache.org/jira/browse/YARN-5699) | Retrospect yarn entity fields which are publishing in events info fields. |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-5145](https://issues.apache.org/jira/browse/YARN-5145) | [YARN-3368] Move new YARN UI configuration to HADOOP\_CONF\_DIR |  Major | . | Wangda Tan | Sunil Govindan |
| [HDFS-11013](https://issues.apache.org/jira/browse/HDFS-11013) | Correct typos in native erasure coding dump code |  Trivial | erasure-coding, native | László Bence Nagy | László Bence Nagy |
| [HDFS-10922](https://issues.apache.org/jira/browse/HDFS-10922) | Adding additional unit tests for Trash (II) |  Major | test | Xiaoyu Yao | Weiwei Yang |
| [HDFS-9390](https://issues.apache.org/jira/browse/HDFS-9390) | Block management for maintenance states |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-13061](https://issues.apache.org/jira/browse/HADOOP-13061) | Refactor erasure coders |  Major | . | Rui Li | Kai Sasaki |
| [YARN-5741](https://issues.apache.org/jira/browse/YARN-5741) | [YARN-3368] Update UI2 documentation for new UI2 path |  Major | . | Kai Sasaki | Kai Sasaki |
| [HDFS-10906](https://issues.apache.org/jira/browse/HDFS-10906) | Add unit tests for Trash with HDFS encryption zones |  Major | encryption | Xiaoyu Yao | Hanisha Koneru |
| [YARN-5561](https://issues.apache.org/jira/browse/YARN-5561) | [Atsv2] : Support for ability to retrieve apps/app-attempt/containers and entities via REST |  Major | timelinereader | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-13735](https://issues.apache.org/jira/browse/HADOOP-13735) | ITestS3AFileContextStatistics.testStatistics() failing |  Minor | fs/s3 | Steve Loughran | Pieter Reuse |
| [HDFS-10976](https://issues.apache.org/jira/browse/HDFS-10976) | Report erasure coding policy of EC files in Fsck |  Major | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-10998](https://issues.apache.org/jira/browse/HDFS-10998) | Add unit tests for HDFS command 'dfsadmin -fetchImage' in HA |  Major | test | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-4911](https://issues.apache.org/jira/browse/YARN-4911) | Bad placement policy in FairScheduler causes the RM to crash |  Major | fairscheduler | Ray Chiang | Ray Chiang |
| [YARN-5047](https://issues.apache.org/jira/browse/YARN-5047) | Refactor nodeUpdate across schedulers |  Major | capacityscheduler, fairscheduler, scheduler | Ray Chiang | Ray Chiang |
| [HDFS-8410](https://issues.apache.org/jira/browse/HDFS-8410) | Add computation time metrics to datanode for ECWorker |  Major | . | Li Bo | Sammi Chen |
| [HDFS-10975](https://issues.apache.org/jira/browse/HDFS-10975) | fsck -list-corruptfileblocks does not report corrupt EC files |  Major | . | Wei-Chiu Chuang | Takanobu Asanuma |
| [HADOOP-13727](https://issues.apache.org/jira/browse/HADOOP-13727) | S3A: Reduce high number of connections to EC2 Instance Metadata Service caused by InstanceProfileCredentialsProvider. |  Minor | fs/s3 | Rajesh Balamohan | Chris Nauroth |
| [HADOOP-12774](https://issues.apache.org/jira/browse/HADOOP-12774) | s3a should use UGI.getCurrentUser.getShortname() for username |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-13309](https://issues.apache.org/jira/browse/HADOOP-13309) | Document S3A known limitations in file ownership and permission model. |  Minor | fs/s3 | Chris Nauroth | Chris Nauroth |
| [HDFS-11011](https://issues.apache.org/jira/browse/HDFS-11011) | Add unit tests for HDFS command 'dfsadmin -set/clrSpaceQuota' |  Major | hdfs-client | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-10638](https://issues.apache.org/jira/browse/HDFS-10638) | Modifications to remove the assumption that StorageLocation is associated with java.io.File in Datanode. |  Major | datanode, fs | Virajith Jalaparti | Virajith Jalaparti |
| [HADOOP-11798](https://issues.apache.org/jira/browse/HADOOP-11798) | Native raw erasure coder in XOR codes |  Major | io | Kai Zheng | Sammi Chen |
| [HADOOP-13614](https://issues.apache.org/jira/browse/HADOOP-13614) | Purge some superfluous/obsolete S3 FS tests that are slowing test runs down |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HDFS-11038](https://issues.apache.org/jira/browse/HDFS-11038) | DiskBalancer: support running multiple commands in single test |  Major | balancer & mover | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-5772](https://issues.apache.org/jira/browse/YARN-5772) | Replace old Hadoop logo with new one |  Major | yarn-ui-v2 | Akira Ajisaka | Akhil PB |
| [YARN-5500](https://issues.apache.org/jira/browse/YARN-5500) | 'Master node' link under application tab is broken |  Critical | . | Sumana Sathish | Akhil PB |
| [YARN-5497](https://issues.apache.org/jira/browse/YARN-5497) | Use different color for Undefined and Succeeded for Final State in applications page |  Trivial | . | Yesha Vora | Akhil PB |
| [YARN-5490](https://issues.apache.org/jira/browse/YARN-5490) | [YARN-3368] Fix various alignment issues and broken breadcrumb link in Node page |  Major | . | Sunil Govindan | Akhil PB |
| [YARN-5779](https://issues.apache.org/jira/browse/YARN-5779) | [YARN-3368] Document limits/notes of the new YARN UI |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-5785](https://issues.apache.org/jira/browse/YARN-5785) | [YARN-3368] Accessing applications and containers list from Node page is throwing few exceptions in console |  Major | yarn-ui-v2 | Sunil Govindan | Akhil PB |
| [YARN-5799](https://issues.apache.org/jira/browse/YARN-5799) | Fix Opportunistic Allocation to set the correct value of Node Http Address |  Major | resourcemanager | Arun Suresh | Arun Suresh |
| [YARN-4765](https://issues.apache.org/jira/browse/YARN-4765) | Split TestHBaseTimelineStorage into multiple test classes |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-5804](https://issues.apache.org/jira/browse/YARN-5804) | New UI2 is not able to launch with jetty 9 upgrade post HADOOP-10075 |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [HADOOP-13680](https://issues.apache.org/jira/browse/HADOOP-13680) | fs.s3a.readahead.range to use getLongBytes |  Major | fs/s3 | Steve Loughran | Abhishek Modi |
| [YARN-5793](https://issues.apache.org/jira/browse/YARN-5793) | Trim configuration values in DockerLinuxContainerRuntime |  Minor | nodemanager | Tianyin Xu | Tianyin Xu |
| [HDFS-11030](https://issues.apache.org/jira/browse/HDFS-11030) | TestDataNodeVolumeFailure#testVolumeFailure is flaky (though passing) |  Major | datanode, test | Mingliang Liu | Mingliang Liu |
| [HDFS-10997](https://issues.apache.org/jira/browse/HDFS-10997) | Reduce number of path resolving methods |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-10499](https://issues.apache.org/jira/browse/HDFS-10499) | TestNameNodeMetadataConsistency#testGenerationStampInFuture Fails Intermittently |  Major | namenode, test | Hanisha Koneru | Yiqun Lin |
| [HDFS-10633](https://issues.apache.org/jira/browse/HDFS-10633) | DiskBalancer : Add the description for the new setting dfs.disk.balancer.plan.threshold.percent in HDFSDiskbalancer.md |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-11065](https://issues.apache.org/jira/browse/HDFS-11065) | Add space quota tests for heterogenous storages |  Major | hdfs | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-11031](https://issues.apache.org/jira/browse/HDFS-11031) | Add additional unit test for DataNode startup behavior when volumes fail |  Major | datanode, test | Mingliang Liu | Mingliang Liu |
| [HDFS-10566](https://issues.apache.org/jira/browse/HDFS-10566) | Submit plan request should throw exception if Datanode is in non-REGULAR status. |  Major | datanode | Jitendra Nath Pandey | Xiaobing Zhou |
| [HDFS-11076](https://issues.apache.org/jira/browse/HDFS-11076) | Add unit test for extended Acls |  Major | test | Chen Liang | Chen Liang |
| [YARN-2995](https://issues.apache.org/jira/browse/YARN-2995) | Enhance UI to show cluster resource utilization of various container Execution types |  Blocker | resourcemanager | Sriram Rao | Konstantinos Karanasos |
| [YARN-4849](https://issues.apache.org/jira/browse/YARN-4849) | [YARN-3368] cleanup code base, integrate web UI related build to mvn, and fix licenses. |  Major | . | Wangda Tan | Wangda Tan |
| [HDFS-11085](https://issues.apache.org/jira/browse/HDFS-11085) | Add unit test for NameNode failing to start when name dir is unwritable |  Major | namenode, test | Mingliang Liu | Xiaobing Zhou |
| [YARN-5802](https://issues.apache.org/jira/browse/YARN-5802) | updateApplicationPriority api in scheduler should ensure to re-insert app to correct ordering policy |  Critical | capacity scheduler | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5716](https://issues.apache.org/jira/browse/YARN-5716) | Add global scheduler interface definition and update CapacityScheduler to use it. |  Major | capacityscheduler | Wangda Tan | Wangda Tan |
| [YARN-5833](https://issues.apache.org/jira/browse/YARN-5833) | Add validation to ensure default ports are unique in Configuration |  Major | yarn | Konstantinos Karanasos | Konstantinos Karanasos |
| [HDFS-11083](https://issues.apache.org/jira/browse/HDFS-11083) | Add unit test for DFSAdmin -report command |  Major | shell, test | Mingliang Liu | Xiaobing Zhou |
| [YARN-4329](https://issues.apache.org/jira/browse/YARN-4329) | Allow fetching exact reason as to why a submitted app is in ACCEPTED state in Fair Scheduler |  Major | fairscheduler, resourcemanager | Naganarasimha G R | Yufei Gu |
| [YARN-4498](https://issues.apache.org/jira/browse/YARN-4498) | Application level node labels stats to be available in REST |  Major | api, client, resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-5611](https://issues.apache.org/jira/browse/YARN-5611) | Provide an API to update lifetime of an application. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-13720](https://issues.apache.org/jira/browse/HADOOP-13720) | Add more info to the msgs printed in AbstractDelegationTokenSecretManager for better supportability |  Trivial | common, security | Yongjun Zhang | Yongjun Zhang |
| [HDFS-11122](https://issues.apache.org/jira/browse/HDFS-11122) | TestDFSAdmin#testReportCommand fails due to timed out |  Minor | test | Yiqun Lin | Yiqun Lin |
| [HDFS-11119](https://issues.apache.org/jira/browse/HDFS-11119) | Support for parallel checking of StorageLocations on DataNode startup |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-10872](https://issues.apache.org/jira/browse/HDFS-10872) | Add MutableRate metrics for FSNamesystemLock operations |  Major | namenode | Erik Krogen | Erik Krogen |
| [HDFS-11105](https://issues.apache.org/jira/browse/HDFS-11105) | TestRBWBlockInvalidation#testRWRInvalidation fails intermittently |  Major | namenode, test | Yiqun Lin | Yiqun Lin |
| [HDFS-11114](https://issues.apache.org/jira/browse/HDFS-11114) | Support for running async disk checks in DataNode |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-13822](https://issues.apache.org/jira/browse/HADOOP-13822) | Use GlobalStorageStatistics.INSTANCE.reset() at FileSystem#clearStatistics() |  Major | fs | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-11601](https://issues.apache.org/jira/browse/HADOOP-11601) | Enhance FS spec & tests to mandate FileStatus.getBlocksize() \>0 for non-empty files |  Minor | fs, test | Steve Loughran | Steve Loughran |
| [HDFS-11113](https://issues.apache.org/jira/browse/HDFS-11113) | Document dfs.client.read.striped configuration in hdfs-default.xml |  Minor | documentation, hdfs-client | Rakesh R | Rakesh R |
| [HDFS-11148](https://issues.apache.org/jira/browse/HDFS-11148) | Update DataNode to use StorageLocationChecker at startup |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-13655](https://issues.apache.org/jira/browse/HADOOP-13655) | document object store use with fs shell and distcp |  Major | documentation, fs, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-5865](https://issues.apache.org/jira/browse/YARN-5865) | Retrospect updateApplicationPriority api to handle state store exception in align with YARN-5611 |  Major | . | Sunil Govindan | Sunil Govindan |
| [HADOOP-13801](https://issues.apache.org/jira/browse/HADOOP-13801) | regression: ITestS3AMiniYarnCluster failing |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-10368](https://issues.apache.org/jira/browse/HDFS-10368) | Erasure Coding: Deprecate replication-related config keys |  Major | erasure-coding | Rakesh R | Rakesh R |
| [YARN-5649](https://issues.apache.org/jira/browse/YARN-5649) | Add REST endpoints for updating application timeouts |  Major | scheduler | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-4206](https://issues.apache.org/jira/browse/YARN-4206) | Add Application timeouts in Application report and CLI |  Major | scheduler | nijel | Rohith Sharma K S |
| [HDFS-10994](https://issues.apache.org/jira/browse/HDFS-10994) | Support an XOR policy XOR-2-1-64k in HDFS |  Major | erasure-coding | Sammi Chen | Sammi Chen |
| [HADOOP-13823](https://issues.apache.org/jira/browse/HADOOP-13823) | s3a rename: fail if dest file exists |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-11149](https://issues.apache.org/jira/browse/HDFS-11149) | Support for parallel checking of FsVolumes |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8678](https://issues.apache.org/jira/browse/HDFS-8678) | Bring back the feature to view chunks of files in the HDFS file browser |  Major | ui | Ravi Prakash | Ravi Prakash |
| [YARN-5761](https://issues.apache.org/jira/browse/YARN-5761) | Separate QueueManager from Scheduler |  Major | capacityscheduler | Xuan Gong | Xuan Gong |
| [HADOOP-13857](https://issues.apache.org/jira/browse/HADOOP-13857) | S3AUtils.translateException to map (wrapped) InterruptedExceptions to InterruptedIOEs |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-13855](https://issues.apache.org/jira/browse/HADOOP-13855) | Fix a couple of the s3a statistic names to be consistent with the rest |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-13257](https://issues.apache.org/jira/browse/HADOOP-13257) | Improve Azure Data Lake contract tests. |  Major | fs/adl | Chris Nauroth | Vishwajeet Dusane |
| [YARN-5746](https://issues.apache.org/jira/browse/YARN-5746) | The state of the parentQueue and its childQueues should be synchronized. |  Major | capacity scheduler, resourcemanager | Xuan Gong | Xuan Gong |
| [HDFS-11178](https://issues.apache.org/jira/browse/HDFS-11178) | TestAddStripedBlockInFBR#testAddBlockInFullBlockReport fails frequently in trunk |  Major | test | Yiqun Lin | Yiqun Lin |
| [HDFS-11172](https://issues.apache.org/jira/browse/HDFS-11172) | Support an erasure coding policy using RS 10 + 4 |  Major | erasure-coding | Sammi Chen | Wei Zhou |
| [YARN-5965](https://issues.apache.org/jira/browse/YARN-5965) | Retrospect ApplicationReport#getApplicationTimeouts |  Major | scheduler | Jian He | Rohith Sharma K S |
| [YARN-5922](https://issues.apache.org/jira/browse/YARN-5922) | Remove direct references of HBaseTimelineWriter/Reader in core ATS classes |  Major | yarn | Haibo Chen | Haibo Chen |
| [HDFS-8630](https://issues.apache.org/jira/browse/HDFS-8630) | WebHDFS : Support get/set/unset StoragePolicy |  Major | webhdfs | nijel | Surendra Singh Lilhore |
| [YARN-5925](https://issues.apache.org/jira/browse/YARN-5925) | Extract hbase-backend-exclusive utility methods from TimelineStorageUtil |  Major | yarn | Haibo Chen | Haibo Chen |
| [YARN-5982](https://issues.apache.org/jira/browse/YARN-5982) | Simplify opportunistic container parameters and metrics |  Major | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-5970](https://issues.apache.org/jira/browse/YARN-5970) | Validate application update timeout request parameters |  Major | scheduler | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-13871](https://issues.apache.org/jira/browse/HADOOP-13871) | ITestS3AInputStreamPerformance.testTimeToOpenAndReadWholeFileBlocks performance awful |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-2009](https://issues.apache.org/jira/browse/YARN-2009) | CapacityScheduler: Add intra-queue preemption for app priority support |  Major | capacityscheduler | Devaraj K | Sunil Govindan |
| [HDFS-8411](https://issues.apache.org/jira/browse/HDFS-8411) | Add bytes count metrics to datanode for ECWorker |  Major | . | Li Bo | Sammi Chen |
| [HADOOP-11804](https://issues.apache.org/jira/browse/HADOOP-11804) | Shaded Hadoop client artifacts and minicluster |  Major | build | Sean Busbey | Sean Busbey |
| [HDFS-11188](https://issues.apache.org/jira/browse/HDFS-11188) | Change min supported DN and NN versions back to 2.x |  Critical | rolling upgrades | Andrew Wang | Andrew Wang |
| [YARN-5524](https://issues.apache.org/jira/browse/YARN-5524) | Yarn live log aggregation does not throw if command line arg is wrong |  Major | log-aggregation | Prasanth Jayachandran | Xuan Gong |
| [YARN-5650](https://issues.apache.org/jira/browse/YARN-5650) | Render Application Timeout value in web UI |  Major | scheduler | Rohith Sharma K S | Akhil PB |
| [HDFS-11182](https://issues.apache.org/jira/browse/HDFS-11182) | Update DataNode to use DatasetVolumeChecker |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [YARN-5976](https://issues.apache.org/jira/browse/YARN-5976) | Update hbase version to 1.2 (removes phoenix dependencies) |  Major | . | Vrushali C | Vrushali C |
| [YARN-4990](https://issues.apache.org/jira/browse/YARN-4990) | Re-direction of a particular log file within in a container in NM UI does not redirect properly to Log Server ( history ) on container completion |  Major | . | Hitesh Shah | Xuan Gong |
| [YARN-5706](https://issues.apache.org/jira/browse/YARN-5706) | Fail to launch SLSRunner due to NPE |  Major | scheduler-load-simulator | Kai Sasaki | Kai Sasaki |
| [YARN-5938](https://issues.apache.org/jira/browse/YARN-5938) | Refactoring OpportunisticContainerAllocator to use SchedulerRequestKey instead of Priority and other misc fixes |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-5756](https://issues.apache.org/jira/browse/YARN-5756) | Add state-machine implementation for scheduler queues |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-5931](https://issues.apache.org/jira/browse/YARN-5931) | Document timeout interfaces CLI and REST APIs |  Major | scheduler | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-5529](https://issues.apache.org/jira/browse/YARN-5529) | Create new DiskValidator class with metrics |  Major | nodemanager | Ray Chiang | Yufei Gu |
| [YARN-6025](https://issues.apache.org/jira/browse/YARN-6025) | Fix synchronization issues of AbstractYarnScheduler#nodeUpdate and its implementations |  Major | capacity scheduler, scheduler | Naganarasimha G R | Naganarasimha G R |
| [YARN-5923](https://issues.apache.org/jira/browse/YARN-5923) | Unable to access logs for a running application if YARN\_ACL\_ENABLE is enabled |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-4899](https://issues.apache.org/jira/browse/YARN-4899) | Queue metrics of SLS capacity scheduler only activated after app submit to the queue |  Major | capacity scheduler | Wangda Tan | Jonathan Hung |
| [YARN-5906](https://issues.apache.org/jira/browse/YARN-5906) | Update AppSchedulingInfo to use SchedulingPlacementSet |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-6009](https://issues.apache.org/jira/browse/YARN-6009) | RM fails to start during an upgrade - Failed to load/recover state (YarnException: Invalid application timeout, value=0 for type=LIFETIME) |  Critical | resourcemanager | Gour Saha | Rohith Sharma K S |
| [YARN-6074](https://issues.apache.org/jira/browse/YARN-6074) | FlowRunEntity does not deserialize long values correctly |  Major | timelinereader | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-3955](https://issues.apache.org/jira/browse/YARN-3955) | Support for application priority ACLs in queues of CapacityScheduler |  Major | capacityscheduler | Sunil Govindan | Sunil Govindan |
| [HDFS-11072](https://issues.apache.org/jira/browse/HDFS-11072) | Add ability to unset and change directory EC policy |  Major | erasure-coding | Andrew Wang | Sammi Chen |
| [HDFS-9391](https://issues.apache.org/jira/browse/HDFS-9391) | Update webUI/JMX to display maintenance state info |  Major | . | Ming Ma | Manoj Govindassamy |
| [YARN-5416](https://issues.apache.org/jira/browse/YARN-5416) | TestRMRestart#testRMRestartWaitForPreviousAMToFinish failed intermittently due to not wait SchedulerApplicationAttempt to be stopped |  Minor | test, yarn | Junping Du | Junping Du |
| [HADOOP-13336](https://issues.apache.org/jira/browse/HADOOP-13336) | S3A to support per-bucket configuration |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-6011](https://issues.apache.org/jira/browse/YARN-6011) | Add a new web service to list the files on a container in AHSWebService |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-6016](https://issues.apache.org/jira/browse/YARN-6016) | Fix minor bugs in handling of local AMRMToken in AMRMProxy |  Minor | federation | Botong Huang | Botong Huang |
| [YARN-5556](https://issues.apache.org/jira/browse/YARN-5556) | CapacityScheduler: Support deleting queues without requiring a RM restart |  Major | capacity scheduler | Xuan Gong | Naganarasimha G R |
| [HDFS-11259](https://issues.apache.org/jira/browse/HDFS-11259) | Update fsck to display maintenance state info |  Major | datanode, namenode | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-5831](https://issues.apache.org/jira/browse/YARN-5831) | Propagate allowPreemptionFrom flag all the way down to the app |  Major | fairscheduler | Karthik Kambatla | Yufei Gu |
| [YARN-5928](https://issues.apache.org/jira/browse/YARN-5928) | Move ATSv2 HBase backend code into a new module that is only dependent at runtime by yarn servers |  Major | yarn | Haibo Chen | Haibo Chen |
| [YARN-5959](https://issues.apache.org/jira/browse/YARN-5959) | RM changes to support change of container ExecutionType |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-6040](https://issues.apache.org/jira/browse/YARN-6040) | Introduce api independent PendingAsk to replace usage of ResourceRequest within Scheduler classes |  Major | . | Wangda Tan | Wangda Tan |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-10861](https://issues.apache.org/jira/browse/HDFS-10861) | Refactor StripeReaders and use ECChunk version decode API |  Major | . | Sammi Chen | Sammi Chen |
| [MAPREDUCE-6780](https://issues.apache.org/jira/browse/MAPREDUCE-6780) | Add support for striping files in benchmarking of TeraGen and TeraSort |  Major | . | Sammi Chen | Sammi Chen |
| [HDFS-10895](https://issues.apache.org/jira/browse/HDFS-10895) | Update HDFS Erasure Coding doc to add how to use ISA-L based coder |  Major | . | Sammi Chen | Sammi Chen |
| [HDFS-10984](https://issues.apache.org/jira/browse/HDFS-10984) | Expose nntop output as metrics |  Major | namenode | Siddharth Wagle | Siddharth Wagle |
| [YARN-5717](https://issues.apache.org/jira/browse/YARN-5717) | Add tests for container-executor's is\_feature\_enabled function |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [YARN-5704](https://issues.apache.org/jira/browse/YARN-5704) | Provide config knobs to control enabling/disabling new/work in progress features in container-executor |  Major | yarn | Sidharta Seethana | Sidharta Seethana |
| [HDFS-11033](https://issues.apache.org/jira/browse/HDFS-11033) | Add documents for native raw erasure coder in XOR codes |  Major | documentation, erasure-coding | Sammi Chen | Sammi Chen |
| [YARN-5308](https://issues.apache.org/jira/browse/YARN-5308) | FairScheduler: Move continuous scheduling related tests to TestContinuousScheduling |  Major | fairscheduler, test | Karthik Kambatla | Kai Sasaki |
| [YARN-5822](https://issues.apache.org/jira/browse/YARN-5822) | Log ContainerRuntime initialization error in LinuxContainerExecutor |  Trivial | nodemanager | Sidharta Seethana | Sidharta Seethana |
| [HDFS-11145](https://issues.apache.org/jira/browse/HDFS-11145) | Implement getTrashRoot() for ViewFileSystem |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-11058](https://issues.apache.org/jira/browse/HDFS-11058) | Implement 'hadoop fs -df' command for ViewFileSystem |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-13828](https://issues.apache.org/jira/browse/HADOOP-13828) | Implement getFileChecksum(path, length) for ViewFileSystem |  Major | viewfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-4395](https://issues.apache.org/jira/browse/YARN-4395) | Typo in comment in ClientServiceDelegate |  Trivial | . | Daniel Templeton | Alison Yu |
| [MAPREDUCE-6810](https://issues.apache.org/jira/browse/MAPREDUCE-6810) | hadoop-mapreduce-client-nativetask compilation broken on GCC-6.2.1 |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-13840](https://issues.apache.org/jira/browse/HADOOP-13840) | Implement getUsed() for ViewFileSystem |  Major | viewfs | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-11216](https://issues.apache.org/jira/browse/HDFS-11216) | Add remoteBytesRead counter metrics for erasure coding reconstruction task |  Major | . | Sammi Chen | Sammi Chen |
| [YARN-5719](https://issues.apache.org/jira/browse/YARN-5719) | Enforce a C standard for native container-executor |  Major | nodemanager | Chris Douglas | Chris Douglas |
| [HADOOP-13885](https://issues.apache.org/jira/browse/HADOOP-13885) | Implement getLinkTarget for ViewFileSystem |  Major | viewfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-5646](https://issues.apache.org/jira/browse/YARN-5646) | Add documentation and update config parameter names for scheduling of OPPORTUNISTIC containers |  Blocker | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [HDFS-9809](https://issues.apache.org/jira/browse/HDFS-9809) | Abstract implementation-specific details from the datanode |  Major | datanode, fs | Virajith Jalaparti | Virajith Jalaparti |
| [HADOOP-13835](https://issues.apache.org/jira/browse/HADOOP-13835) | Move Google Test Framework code from mapreduce to hadoop-common |  Major | test | Varun Vasudev | Varun Vasudev |


