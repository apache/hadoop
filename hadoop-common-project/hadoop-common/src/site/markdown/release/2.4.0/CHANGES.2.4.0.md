
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

## Release 2.4.0 - 2014-04-07

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-5804](https://issues.apache.org/jira/browse/HDFS-5804) | HDFS NFS Gateway fails to mount and proxy when using Kerberos |  Major | nfs | Abin Shahab | Abin Shahab |
| [HADOOP-8691](https://issues.apache.org/jira/browse/HADOOP-8691) | FsShell can print "Found xxx items" unnecessarily often |  Minor | fs | Jason Lowe | Daryn Sharp |
| [HDFS-5321](https://issues.apache.org/jira/browse/HDFS-5321) | Clean up the HTTP-related configuration in HDFS |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-6055](https://issues.apache.org/jira/browse/HDFS-6055) | Change default configuration to limit file name length in HDFS |  Major | namenode | Suresh Srinivas | Chris Nauroth |
| [HDFS-6102](https://issues.apache.org/jira/browse/HDFS-6102) | Lower the default maximum items per directory to fix PB fsimage loading |  Blocker | namenode | Andrew Wang | Andrew Wang |
| [HDFS-5138](https://issues.apache.org/jira/browse/HDFS-5138) | Support HDFS upgrade in HA |  Blocker | . | Kihwal Lee | Aaron T. Myers |
| [MAPREDUCE-5036](https://issues.apache.org/jira/browse/MAPREDUCE-5036) | Default shuffle handler port should not be 8080 |  Major | . | Sandy Ryza | Sandy Ryza |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-5535](https://issues.apache.org/jira/browse/HDFS-5535) | Umbrella jira for improved HDFS rolling upgrades |  Major | datanode, ha, hdfs-client, namenode | Nathan Roberts | Tsz Wo Nicholas Sze |
| [HADOOP-10184](https://issues.apache.org/jira/browse/HADOOP-10184) | Hadoop Common changes required to support HDFS ACLs. |  Major | fs, security | Chris Nauroth | Chris Nauroth |
| [HDFS-4685](https://issues.apache.org/jira/browse/HDFS-4685) | Implementation of ACLs in HDFS |  Major | hdfs-client, namenode, security | Sachin Jose | Chris Nauroth |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-5768](https://issues.apache.org/jira/browse/HDFS-5768) | Consolidate the serialization code in DelegationTokenSecretManager |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-5775](https://issues.apache.org/jira/browse/HDFS-5775) | Consolidate the code for serialization in CacheManager |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-5781](https://issues.apache.org/jira/browse/HDFS-5781) | Use an array to record the mapping between FSEditLogOpCode and the corresponding byte value |  Minor | namenode | Jing Zhao | Jing Zhao |
| [MAPREDUCE-5732](https://issues.apache.org/jira/browse/MAPREDUCE-5732) | Report proper queue when job has been automatically placed |  Major | . | Sandy Ryza | Sandy Ryza |
| [HADOOP-10139](https://issues.apache.org/jira/browse/HADOOP-10139) | Update and improve the Single Cluster Setup document |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10295](https://issues.apache.org/jira/browse/HADOOP-10295) | Allow distcp to automatically identify the checksum type of source files and use it for the target |  Major | tools/distcp | Jing Zhao | Jing Zhao |
| [HDFS-5153](https://issues.apache.org/jira/browse/HDFS-5153) | Datanode should send block reports for each storage in a separate message |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-5709](https://issues.apache.org/jira/browse/HDFS-5709) | Improve NameNode upgrade with existing reserved paths and path components |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-4911](https://issues.apache.org/jira/browse/HDFS-4911) | Reduce PeerCache timeout to be commensurate with dfs.datanode.socket.reuse.keepalive |  Minor | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5698](https://issues.apache.org/jira/browse/HDFS-5698) | Use protobuf to serialize / deserialize FSImage |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-4370](https://issues.apache.org/jira/browse/HDFS-4370) | Fix typo Blanacer in DataNode |  Major | datanode | Konstantin Shvachko | Chu Tong |
| [HADOOP-10333](https://issues.apache.org/jira/browse/HADOOP-10333) | Fix grammatical error in overview.html document |  Trivial | . | René Nyffenegger | René Nyffenegger |
| [HDFS-5929](https://issues.apache.org/jira/browse/HDFS-5929) | Add Block pool % usage to HDFS federated nn page |  Major | federation | Siqi Li | Siqi Li |
| [HADOOP-10343](https://issues.apache.org/jira/browse/HADOOP-10343) | Change info to debug log in LossyRetryInvocationHandler |  Minor | . | Arpit Gupta | Arpit Gupta |
| [YARN-1171](https://issues.apache.org/jira/browse/YARN-1171) | Add default queue properties to Fair Scheduler documentation |  Major | documentation, scheduler | Sandy Ryza | Naren Koneru |
| [HDFS-5318](https://issues.apache.org/jira/browse/HDFS-5318) | Support read-only and read-write paths to shared replicas |  Major | namenode | Eric Sirianni |  |
| [HDFS-5979](https://issues.apache.org/jira/browse/HDFS-5979) | Typo and logger fix for fsimage PB code |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-10348](https://issues.apache.org/jira/browse/HADOOP-10348) | Deprecate hadoop.ssl.configuration in branch-2, and remove it in trunk |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5935](https://issues.apache.org/jira/browse/HDFS-5935) | New Namenode UI FS browser should throw smarter error messages |  Minor | namenode | Travis Thompson | Travis Thompson |
| [HDFS-5776](https://issues.apache.org/jira/browse/HDFS-5776) | Support 'hedged' reads in DFSClient |  Major | hdfs-client | Liang Xie | Liang Xie |
| [MAPREDUCE-5761](https://issues.apache.org/jira/browse/MAPREDUCE-5761) | Add a log message like "encrypted shuffle is ON" in nodemanager logs |  Trivial | . | Yesha Vora | Jian He |
| [HDFS-5939](https://issues.apache.org/jira/browse/HDFS-5939) | WebHdfs returns misleading error code and logs nothing if trying to create a file with no DNs in cluster |  Major | hdfs-client | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6006](https://issues.apache.org/jira/browse/HDFS-6006) | Remove duplicate code in FSNameSystem#getFileInfo |  Trivial | namenode | Akira Ajisaka | Akira Ajisaka |
| [HDFS-5908](https://issues.apache.org/jira/browse/HDFS-5908) | Change AclFeature to capture list of ACL entries in an ImmutableList. |  Minor | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-6018](https://issues.apache.org/jira/browse/HDFS-6018) | Exception recorded in LOG when IPCLoggerChannel#close is called |  Trivial | . | Jing Zhao | Jing Zhao |
| [HADOOP-9454](https://issues.apache.org/jira/browse/HADOOP-9454) | Support multipart uploads for s3native |  Major | fs/s3 | Jordan Mendelson | Akira Ajisaka |
| [MAPREDUCE-5754](https://issues.apache.org/jira/browse/MAPREDUCE-5754) | Preserve Job diagnostics in history |  Major | jobhistoryserver, mr-am | Gera Shegalov | Gera Shegalov |
| [HADOOP-10374](https://issues.apache.org/jira/browse/HADOOP-10374) | InterfaceAudience annotations should have RetentionPolicy.RUNTIME |  Major | . | Enis Soztutar | Enis Soztutar |
| [HDFS-4200](https://issues.apache.org/jira/browse/HDFS-4200) | Reduce the size of synchronized sections in PacketResponder |  Major | datanode | Suresh Srinivas | Andrew Wang |
| [MAPREDUCE-5773](https://issues.apache.org/jira/browse/MAPREDUCE-5773) | Provide dedicated MRAppMaster syslog length limit |  Blocker | mr-am | Gera Shegalov | Gera Shegalov |
| [HADOOP-10379](https://issues.apache.org/jira/browse/HADOOP-10379) | Protect authentication cookies with the HttpOnly and Secure flags |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-6043](https://issues.apache.org/jira/browse/HDFS-6043) | Give HDFS daemons NFS3 and Portmap their own OPTS |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-6044](https://issues.apache.org/jira/browse/HDFS-6044) | Add property for setting the NFS look up time for users |  Minor | nfs | Brandon Li | Brandon Li |
| [HADOOP-10211](https://issues.apache.org/jira/browse/HADOOP-10211) | Enable RPC protocol to negotiate SASL-QOP values between clients and servers |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10386](https://issues.apache.org/jira/browse/HADOOP-10386) | Log proxy hostname in various exceptions being thrown in a HA setup |  Minor | ha | Arpit Gupta | Haohui Mai |
| [HDFS-6069](https://issues.apache.org/jira/browse/HDFS-6069) | Quash stack traces when ACLs are disabled |  Trivial | namenode | Andrew Wang | Chris Nauroth |
| [HDFS-6070](https://issues.apache.org/jira/browse/HDFS-6070) | Cleanup use of ReadStatistics in DFSInputStream |  Trivial | . | Andrew Wang | Andrew Wang |
| [HDFS-3405](https://issues.apache.org/jira/browse/HDFS-3405) | Checkpointing should use HTTP POST or PUT instead of GET-GET to send merged fsimages |  Major | . | Aaron T. Myers | Vinayakumar B |
| [HDFS-6085](https://issues.apache.org/jira/browse/HDFS-6085) | Improve CacheReplicationMonitor log messages a bit |  Major | namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-6072](https://issues.apache.org/jira/browse/HDFS-6072) | Clean up dead code of FSImage |  Major | . | Haohui Mai | Haohui Mai |
| [MAPREDUCE-5553](https://issues.apache.org/jira/browse/MAPREDUCE-5553) | Add task state filters on Application/MRJob page for MR Application master |  Minor | applicationmaster | Paul Han | Paul Han |
| [YARN-1789](https://issues.apache.org/jira/browse/YARN-1789) | ApplicationSummary does not escape newlines in the app name |  Minor | resourcemanager | Akira Ajisaka | Tsuyoshi Ozawa |
| [HDFS-6080](https://issues.apache.org/jira/browse/HDFS-6080) | Improve NFS gateway performance by making rtmax and wtmax configurable |  Major | nfs, performance | Abin Shahab | Abin Shahab |
| [YARN-1771](https://issues.apache.org/jira/browse/YARN-1771) | many getFileStatus calls made from node manager for localizing a public distributed cache resource |  Critical | nodemanager | Sangjin Lee | Sangjin Lee |
| [HDFS-6084](https://issues.apache.org/jira/browse/HDFS-6084) | Namenode UI - "Hadoop" logo link shouldn't go to hadoop homepage |  Minor | namenode | Travis Thompson | Travis Thompson |
| [HDFS-6090](https://issues.apache.org/jira/browse/HDFS-6090) | Use MiniDFSCluster.Builder instead of deprecated constructors |  Minor | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10383](https://issues.apache.org/jira/browse/HADOOP-10383) | InterfaceStability annotations should have RetentionPolicy.RUNTIME |  Major | . | Enis Soztutar | Enis Soztutar |
| [YARN-1512](https://issues.apache.org/jira/browse/YARN-1512) | Enhance CS to decouple scheduling from node heartbeats |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-10221](https://issues.apache.org/jira/browse/HADOOP-10221) | Add a plugin to specify SaslProperties for RPC protocol based on connection properties |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-6123](https://issues.apache.org/jira/browse/HDFS-6123) | Improve datanode error messages |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2349](https://issues.apache.org/jira/browse/MAPREDUCE-2349) | speed up list[located]status calls from input formats |  Major | task | Joydeep Sen Sarma | Siddharth Seth |
| [YARN-1570](https://issues.apache.org/jira/browse/YARN-1570) | Formatting the lines within 80 chars in YarnCommands.apt.vm |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-6138](https://issues.apache.org/jira/browse/HDFS-6138) | User Guide for how to use viewfs with federation |  Minor | documentation | Sanjay Radia | Sanjay Radia |
| [HDFS-6120](https://issues.apache.org/jira/browse/HDFS-6120) | Fix and improve safe mode log messages |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-1536](https://issues.apache.org/jira/browse/YARN-1536) | Cleanup: Get rid of ResourceManager#get\*SecretManager() methods and use the RMContext methods instead |  Minor | resourcemanager | Karthik Kambatla | Anubhav Dhoot |
| [HADOOP-10423](https://issues.apache.org/jira/browse/HADOOP-10423) | Clarify compatibility policy document for combination of new client and old server. |  Minor | documentation | Chris Nauroth | Chris Nauroth |
| [HDFS-5910](https://issues.apache.org/jira/browse/HDFS-5910) | Enhance DataTransferProtocol to allow per-connection choice of encryption/plain-text |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-6150](https://issues.apache.org/jira/browse/HDFS-6150) | Add inode id information in the logs to make debugging easier |  Minor | namenode | Suresh Srinivas | Suresh Srinivas |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-5626](https://issues.apache.org/jira/browse/HDFS-5626) | dfsadmin -report shows incorrect cache values |  Major | caching | Stephen Chu | Colin P. McCabe |
| [HDFS-5705](https://issues.apache.org/jira/browse/HDFS-5705) | TestSecondaryNameNodeUpgrade#testChangeNsIDFails may fail due to ConcurrentModificationException |  Major | datanode | Ted Yu | Ted Yu |
| [YARN-1166](https://issues.apache.org/jira/browse/YARN-1166) | YARN 'appsFailed' metric should be of type 'counter' |  Blocker | resourcemanager | Srimanth Gunturi | Zhijie Shen |
| [HDFS-5492](https://issues.apache.org/jira/browse/HDFS-5492) | Port HDFS-2069 (Incorrect default trash interval in the docs) to trunk |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-5843](https://issues.apache.org/jira/browse/HDFS-5843) | DFSClient.getFileChecksum() throws IOException if checksum is disabled |  Major | datanode | Laurent Goujon | Laurent Goujon |
| [HDFS-5790](https://issues.apache.org/jira/browse/HDFS-5790) | LeaseManager.findPath is very slow when many leases need recovery |  Major | namenode, performance | Todd Lipcon | Todd Lipcon |
| [YARN-1617](https://issues.apache.org/jira/browse/YARN-1617) | Remove ancient comment and surround LOG.debug in AppSchedulingInfo.allocate |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [HDFS-5856](https://issues.apache.org/jira/browse/HDFS-5856) | DataNode.checkDiskError might throw NPE |  Minor | datanode | Josh Elser | Josh Elser |
| [YARN-1632](https://issues.apache.org/jira/browse/YARN-1632) | TestApplicationMasterServices should be under org.apache.hadoop.yarn.server.resourcemanager package |  Minor | . | Chen He | Chen He |
| [HADOOP-10320](https://issues.apache.org/jira/browse/HADOOP-10320) | Javadoc in InterfaceStability.java lacks final \</ul\> |  Trivial | documentation | René Nyffenegger | René Nyffenegger |
| [HDFS-5859](https://issues.apache.org/jira/browse/HDFS-5859) | DataNode#checkBlockToken should check block tokens even if security is not enabled |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5828](https://issues.apache.org/jira/browse/HDFS-5828) | BlockPlacementPolicyWithNodeGroup can place multiple replicas on the same node group when dfs.namenode.avoid.write.stale.datanode is true |  Major | namenode | Taylor, Buddy | Taylor, Buddy |
| [HADOOP-10085](https://issues.apache.org/jira/browse/HADOOP-10085) | CompositeService should allow adding services while being inited |  Blocker | . | Karthik Kambatla | Steve Loughran |
| [HDFS-5767](https://issues.apache.org/jira/browse/HDFS-5767) | NFS implementation assumes userName userId mapping to be unique, which is not true sometimes |  Blocker | nfs | Yongjun Zhang | Yongjun Zhang |
| [YARN-1673](https://issues.apache.org/jira/browse/YARN-1673) | Valid yarn kill application prints out help message. |  Blocker | client | Tassapol Athiapinya | Mayank Bansal |
| [YARN-1285](https://issues.apache.org/jira/browse/YARN-1285) | Inconsistency of default "yarn.acl.enable" value |  Major | . | Zhijie Shen | Kenji Kikushima |
| [HDFS-5791](https://issues.apache.org/jira/browse/HDFS-5791) | TestHttpsFileSystem should use a random port to avoid binding error during testing |  Major | test | Brandon Li | Haohui Mai |
| [HDFS-5881](https://issues.apache.org/jira/browse/HDFS-5881) | Fix skip() of the short-circuit local reader (legacy). |  Critical | . | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-5699](https://issues.apache.org/jira/browse/MAPREDUCE-5699) | Allow setting tags on MR jobs |  Major | applicationmaster | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-10327](https://issues.apache.org/jira/browse/HADOOP-10327) | Trunk windows build broken after HDFS-5746 |  Blocker | native | Vinayakumar B | Vinayakumar B |
| [YARN-1661](https://issues.apache.org/jira/browse/YARN-1661) | AppMaster logs says failing even if an application does succeed. |  Major | applications/distributed-shell | Tassapol Athiapinya | Vinod Kumar Vavilapalli |
| [HDFS-5895](https://issues.apache.org/jira/browse/HDFS-5895) | HDFS cacheadmin -listPools has exit\_code of 1 when the command returns 0 result. |  Major | tools | Tassapol Athiapinya | Tassapol Athiapinya |
| [YARN-1689](https://issues.apache.org/jira/browse/YARN-1689) | RMAppAttempt is not killed when RMApp is at ACCEPTED |  Critical | resourcemanager | Deepesh Khandelwal | Vinod Kumar Vavilapalli |
| [HADOOP-10330](https://issues.apache.org/jira/browse/HADOOP-10330) | TestFrameDecoder fails if it cannot bind port 12345 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-5900](https://issues.apache.org/jira/browse/HDFS-5900) | Cannot set cache pool limit of "unlimited" via CacheAdmin |  Major | caching | Tassapol Athiapinya | Andrew Wang |
| [YARN-1672](https://issues.apache.org/jira/browse/YARN-1672) | YarnConfiguration is missing a default for yarn.nodemanager.log.retain-seconds |  Trivial | nodemanager | Karthik Kambatla | Naren Koneru |
| [HDFS-5886](https://issues.apache.org/jira/browse/HDFS-5886) | Potential null pointer deference in RpcProgramNfs3#readlink() |  Major | nfs | Ted Yu | Brandon Li |
| [HDFS-5915](https://issues.apache.org/jira/browse/HDFS-5915) | Refactor FSImageFormatProtobuf to simplify cross section reads |  Major | namenode | Haohui Mai | Haohui Mai |
| [HADOOP-10326](https://issues.apache.org/jira/browse/HADOOP-10326) | M/R jobs can not access S3 if Kerberos is enabled |  Major | security | Manuel DE FERRAN | bc Wong |
| [YARN-1697](https://issues.apache.org/jira/browse/YARN-1697) | NodeManager reports negative running containers |  Major | nodemanager | Sandy Ryza | Sandy Ryza |
| [HADOOP-10338](https://issues.apache.org/jira/browse/HADOOP-10338) | Cannot get the FileStatus of the root inode from the new Globber |  Major | . | Andrew Wang | Colin P. McCabe |
| [HDFS-4858](https://issues.apache.org/jira/browse/HDFS-4858) | HDFS DataNode to NameNode RPC should timeout |  Minor | datanode | Jagane Sundar | Henry Wang |
| [MAPREDUCE-5746](https://issues.apache.org/jira/browse/MAPREDUCE-5746) | Job diagnostics can implicate wrong task for a failed job |  Major | jobhistoryserver | Jason Lowe | Jason Lowe |
| [HDFS-5879](https://issues.apache.org/jira/browse/HDFS-5879) | Some TestHftpFileSystem tests do not close streams |  Major | test | Gera Shegalov | Gera Shegalov |
| [YARN-1531](https://issues.apache.org/jira/browse/YARN-1531) | True up yarn command documentation |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-1692](https://issues.apache.org/jira/browse/YARN-1692) | ConcurrentModificationException in fair scheduler AppSchedulable |  Major | scheduler | Sangjin Lee | Sangjin Lee |
| [HDFS-5891](https://issues.apache.org/jira/browse/HDFS-5891) | webhdfs should not try connecting the DN during redirection |  Major | namenode, webhdfs | Haohui Mai | Haohui Mai |
| [HDFS-5904](https://issues.apache.org/jira/browse/HDFS-5904) | TestFileStatus fails intermittently on trunk and branch2 |  Major | . | Mit Desai | Mit Desai |
| [HDFS-5941](https://issues.apache.org/jira/browse/HDFS-5941) | add dfs.namenode.secondary.https-address and dfs.namenode.secondary.https-address in hdfs-default.xml |  Major | documentation, namenode | Haohui Mai | Haohui Mai |
| [YARN-1417](https://issues.apache.org/jira/browse/YARN-1417) | RM may issue expired container tokens to AM while issuing new containers. |  Blocker | . | Omkar Vinit Joshi | Jian He |
| [HDFS-5913](https://issues.apache.org/jira/browse/HDFS-5913) | Nfs3Utils#getWccAttr() should check attr parameter against null |  Minor | nfs | Ted Yu | Brandon Li |
| [MAPREDUCE-5670](https://issues.apache.org/jira/browse/MAPREDUCE-5670) | CombineFileRecordReader should report progress when moving to the next file |  Minor | mrv2 | Jason Lowe | Chen He |
| [HADOOP-10249](https://issues.apache.org/jira/browse/HADOOP-10249) | LdapGroupsMapping should trim ldap password read from file |  Major | . | Dilli Arumugam | Dilli Arumugam |
| [HDFS-5934](https://issues.apache.org/jira/browse/HDFS-5934) | New Namenode UI back button doesn't work as expected |  Minor | namenode | Travis Thompson | Travis Thompson |
| [HDFS-5901](https://issues.apache.org/jira/browse/HDFS-5901) | NameNode new UI doesn't support IE8 and IE9 on windows 7 |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HDFS-5943](https://issues.apache.org/jira/browse/HDFS-5943) | 'dfs.namenode.https-address.ns1' property is not used in federation setup |  Major | . | Yesha Vora | Suresh Srinivas |
| [HDFS-3128](https://issues.apache.org/jira/browse/HDFS-3128) | Unit tests should not use a test root in /tmp |  Minor | test | Eli Collins | Andrew Wang |
| [HDFS-5948](https://issues.apache.org/jira/browse/HDFS-5948) | TestBackupNode flakes with port in use error |  Major | . | Andrew Wang | Haohui Mai |
| [HDFS-5949](https://issues.apache.org/jira/browse/HDFS-5949) | New Namenode UI when trying to download a file, the browser doesn't know the file name |  Minor | namenode | Travis Thompson | Travis Thompson |
| [YARN-1553](https://issues.apache.org/jira/browse/YARN-1553) | Do not use HttpConfig.isSecure() in YARN |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5716](https://issues.apache.org/jira/browse/HDFS-5716) | Allow WebHDFS to use pluggable authentication filter |  Major | webhdfs | Haohui Mai | Haohui Mai |
| [HDFS-5759](https://issues.apache.org/jira/browse/HDFS-5759) | Web UI does not show up during the period of loading FSImage |  Major | . | Haohui Mai | Haohui Mai |
| [MAPREDUCE-5757](https://issues.apache.org/jira/browse/MAPREDUCE-5757) | ConcurrentModificationException in JobControl.toList |  Major | client | Jason Lowe | Jason Lowe |
| [HDFS-5959](https://issues.apache.org/jira/browse/HDFS-5959) | Fix typo at section name in FSImageFormatProtobuf.java |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-5780](https://issues.apache.org/jira/browse/HDFS-5780) | TestRBWBlockInvalidation times out intemittently on branch-2 |  Major | . | Mit Desai | Mit Desai |
| [HADOOP-10346](https://issues.apache.org/jira/browse/HADOOP-10346) | Deadlock while logging tokens |  Blocker | security | Jason Lowe | Jason Lowe |
| [HDFS-5803](https://issues.apache.org/jira/browse/HDFS-5803) | TestBalancer.testBalancer0 fails |  Major | . | Mit Desai | Chen He |
| [YARN-1721](https://issues.apache.org/jira/browse/YARN-1721) | When moving app between queues in Fair Scheduler, grab lock on FSSchedulerApp |  Critical | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-1724](https://issues.apache.org/jira/browse/YARN-1724) | Race condition in Fair Scheduler when continuous scheduling is turned on |  Critical | scheduler | Sandy Ryza | Sandy Ryza |
| [HDFS-5893](https://issues.apache.org/jira/browse/HDFS-5893) | HftpFileSystem.RangeHeaderUrlOpener uses the default URLConnectionFactory which does not import SSL certificates |  Major | . | Yesha Vora | Haohui Mai |
| [YARN-1590](https://issues.apache.org/jira/browse/YARN-1590) | \_HOST doesn't expand properly for RM, NM, ProxyServer and JHS |  Major | resourcemanager | Mohammad Kamrul Islam | Mohammad Kamrul Islam |
| [HDFS-5961](https://issues.apache.org/jira/browse/HDFS-5961) | OIV cannot load fsimages containing a symbolic link |  Critical | . | Kihwal Lee | Kihwal Lee |
| [YARN-713](https://issues.apache.org/jira/browse/YARN-713) | ResourceManager can exit unexpectedly if DNS is unavailable |  Critical | resourcemanager | Jason Lowe | Jian He |
| [HDFS-5742](https://issues.apache.org/jira/browse/HDFS-5742) | DatanodeCluster (mini cluster of DNs) fails to start |  Minor | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-5962](https://issues.apache.org/jira/browse/HDFS-5962) | Mtime and atime are not persisted for symbolic links |  Critical | . | Kihwal Lee | Akira Ajisaka |
| [HADOOP-10328](https://issues.apache.org/jira/browse/HADOOP-10328) | loadGenerator exit code is not reliable |  Major | tools | Arpit Gupta | Haohui Mai |
| [HDFS-5944](https://issues.apache.org/jira/browse/HDFS-5944) | LeaseManager:findLeaseWithPrefixPath can't handle path like /a/b/ right and cause SecondaryNameNode failed do checkpoint |  Major | namenode | yunjiong zhao | yunjiong zhao |
| [HDFS-5982](https://issues.apache.org/jira/browse/HDFS-5982) | Need to update snapshot manager when applying editlog for deleting a snapshottable directory |  Critical | namenode | Tassapol Athiapinya | Jing Zhao |
| [YARN-1398](https://issues.apache.org/jira/browse/YARN-1398) | Deadlock in capacity scheduler leaf queue and parent queue for getQueueInfo and completedContainer call |  Blocker | resourcemanager | Sunil G | Vinod Kumar Vavilapalli |
| [HDFS-5988](https://issues.apache.org/jira/browse/HDFS-5988) | Bad fsimage always generated after upgrade |  Blocker | namenode | Andrew Wang | Andrew Wang |
| [HADOOP-10355](https://issues.apache.org/jira/browse/HADOOP-10355) | TestLoadGenerator#testLoadGenerator fails |  Major | . | Akira Ajisaka | Haohui Mai |
| [HADOOP-10352](https://issues.apache.org/jira/browse/HADOOP-10352) | Recursive setfacl erroneously attempts to apply default ACL to files. |  Major | fs | Chris Nauroth | Chris Nauroth |
| [YARN-1071](https://issues.apache.org/jira/browse/YARN-1071) | ResourceManager's decommissioned and lost node count is 0 after restart |  Major | resourcemanager | Srimanth Gunturi | Jian He |
| [HDFS-5981](https://issues.apache.org/jira/browse/HDFS-5981) | PBImageXmlWriter generates malformed XML |  Minor | tools | Haohui Mai | Haohui Mai |
| [HADOOP-10354](https://issues.apache.org/jira/browse/HADOOP-10354) | TestWebHDFS fails after merge of HDFS-4685 to trunk |  Major | fs | Yongjun Zhang | Chris Nauroth |
| [MAPREDUCE-5688](https://issues.apache.org/jira/browse/MAPREDUCE-5688) | TestStagingCleanup fails intermittently with JDK7 |  Major | . | Mit Desai | Mit Desai |
| [YARN-1470](https://issues.apache.org/jira/browse/YARN-1470) | Add audience annotation to MiniYARNCluster |  Major | . | Sandy Ryza | Anubhav Dhoot |
| [YARN-1742](https://issues.apache.org/jira/browse/YARN-1742) | Fix javadoc of parameter DEFAULT\_NM\_MIN\_HEALTHY\_DISKS\_FRACTION |  Trivial | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-1686](https://issues.apache.org/jira/browse/YARN-1686) | NodeManager.resyncWithRM() does not handle exception which cause NodeManger to Hang. |  Major | nodemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-5922](https://issues.apache.org/jira/browse/HDFS-5922) | DN heartbeat thread can get stuck in tight loop |  Major | datanode | Aaron T. Myers | Arpit Agarwal |
| [HADOOP-10361](https://issues.apache.org/jira/browse/HADOOP-10361) | Correct alignment in CLI output for ACLs. |  Minor | fs | Chris Nauroth | Chris Nauroth |
| [HDFS-6008](https://issues.apache.org/jira/browse/HDFS-6008) | Namenode dead node link is giving HTTP error 500 |  Minor | namenode | Benoy Antony | Benoy Antony |
| [HADOOP-10368](https://issues.apache.org/jira/browse/HADOOP-10368) | InputStream is not closed in VersionInfo ctor |  Minor | util | Ted Yu | Tsuyoshi Ozawa |
| [HDFS-3969](https://issues.apache.org/jira/browse/HDFS-3969) | Small bug fixes and improvements for disk locations API |  Major | hdfs-client | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-5766](https://issues.apache.org/jira/browse/MAPREDUCE-5766) | Ping messages from attempts should be moved to DEBUG |  Minor | applicationmaster | Ramya Sunil | Jian He |
| [YARN-1301](https://issues.apache.org/jira/browse/YARN-1301) | Need to log the blacklist additions/removals when YarnSchedule#allocate |  Minor | . | Zhijie Shen | Tsuyoshi Ozawa |
| [HADOOP-10353](https://issues.apache.org/jira/browse/HADOOP-10353) | FsUrlStreamHandlerFactory is not thread safe |  Major | fs | Tudor Scurtu | Tudor Scurtu |
| [MAPREDUCE-5770](https://issues.apache.org/jira/browse/MAPREDUCE-5770) | Redirection from AM-URL is broken with HTTPS\_ONLY policy |  Major | . | Yesha Vora | Jian He |
| [HDFS-6028](https://issues.apache.org/jira/browse/HDFS-6028) | Print clearer error message when user attempts to delete required mask entry from ACL. |  Trivial | namenode | Chris Nauroth | Chris Nauroth |
| [YARN-1528](https://issues.apache.org/jira/browse/YARN-1528) | Allow setting auth for ZK connections |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5339](https://issues.apache.org/jira/browse/HDFS-5339) | WebHDFS URI does not accept logical nameservices when security is enabled |  Major | webhdfs | Stephen Chu | Haohui Mai |
| [MAPREDUCE-5768](https://issues.apache.org/jira/browse/MAPREDUCE-5768) | TestMRJobs.testContainerRollingLog fails on trunk |  Major | . | Zhijie Shen | Gera Shegalov |
| [HDFS-6033](https://issues.apache.org/jira/browse/HDFS-6033) | PBImageXmlWriter incorrectly handles processing cache directives |  Major | caching | Aaron T. Myers | Aaron T. Myers |
| [HDFS-5821](https://issues.apache.org/jira/browse/HDFS-5821) | TestHDFSCLI fails for user names with the dash character |  Major | test | Gera Shegalov | Gera Shegalov |
| [YARN-1760](https://issues.apache.org/jira/browse/YARN-1760) | TestRMAdminService assumes CapacityScheduler |  Trivial | . | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-10314](https://issues.apache.org/jira/browse/HADOOP-10314) | The ls command help still shows outdated 0.16 format. |  Major | . | Kihwal Lee | Rushabh S Shah |
| [YARN-1758](https://issues.apache.org/jira/browse/YARN-1758) | MiniYARNCluster broken post YARN-1666 |  Blocker | . | Hitesh Shah | Xuan Gong |
| [YARN-1748](https://issues.apache.org/jira/browse/YARN-1748) | hadoop-yarn-server-tests packages core-site.xml breaking downstream tests |  Blocker | . | Sravya Tirukkovalur | Sravya Tirukkovalur |
| [HDFS-6039](https://issues.apache.org/jira/browse/HDFS-6039) | Uploading a File under a Dir with default acls throws "Duplicated ACLFeature" |  Major | namenode | Yesha Vora | Chris Nauroth |
| [HADOOP-10070](https://issues.apache.org/jira/browse/HADOOP-10070) | RPC client doesn't use per-connection conf to determine server's expected Kerberos principal name |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6053](https://issues.apache.org/jira/browse/HDFS-6053) | Fix TestDecommissioningStatus and TestDecommission in branch-2 |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-6047](https://issues.apache.org/jira/browse/HDFS-6047) | TestPread NPE inside in DFSInputStream hedgedFetchBlockByteRange |  Major | . | stack | stack |
| [HDFS-6051](https://issues.apache.org/jira/browse/HDFS-6051) | HDFS cannot run on Windows since short-circuit shared memory segment changes. |  Blocker | hdfs-client | Chris Nauroth | Colin P. McCabe |
| [YARN-1768](https://issues.apache.org/jira/browse/YARN-1768) | yarn kill non-existent application is too verbose |  Minor | client | Hitesh Shah | Tsuyoshi Ozawa |
| [YARN-1785](https://issues.apache.org/jira/browse/YARN-1785) | FairScheduler treats app lookup failures as ERRORs |  Major | . | bc Wong | bc Wong |
| [YARN-1752](https://issues.apache.org/jira/browse/YARN-1752) | Unexpected Unregistered event at Attempt Launched state |  Major | . | Jian He | Rohith Sharma K S |
| [HDFS-5857](https://issues.apache.org/jira/browse/HDFS-5857) | TestWebHDFS#testNamenodeRestart fails intermittently with NPE |  Major | . | Mit Desai | Mit Desai |
| [HDFS-6057](https://issues.apache.org/jira/browse/HDFS-6057) | DomainSocketWatcher.watcherThread should be marked as daemon thread |  Blocker | hdfs-client | Eric Sirianni | Colin P. McCabe |
| [HDFS-6058](https://issues.apache.org/jira/browse/HDFS-6058) | Fix TestHDFSCLI failures after HADOOP-8691 change |  Major | . | Vinayakumar B | Haohui Mai |
| [HDFS-6059](https://issues.apache.org/jira/browse/HDFS-6059) | TestBlockReaderLocal fails if native library is not available |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-6062](https://issues.apache.org/jira/browse/HDFS-6062) | TestRetryCacheWithHA#testConcat is flaky |  Minor | . | Jing Zhao | Jing Zhao |
| [MAPREDUCE-5780](https://issues.apache.org/jira/browse/MAPREDUCE-5780) | SliveTest always uses default FileSystem |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6064](https://issues.apache.org/jira/browse/HDFS-6064) | DFSConfigKeys.DFS\_BLOCKREPORT\_INTERVAL\_MSEC\_DEFAULT is not updated with latest block report interval of 6 hrs |  Minor | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-6067](https://issues.apache.org/jira/browse/HDFS-6067) | TestPread.testMaxOutHedgedReadPool is flaky |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-6065](https://issues.apache.org/jira/browse/HDFS-6065) | HDFS zero-copy reads should return null on EOF when doing ZCR |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [YARN-1774](https://issues.apache.org/jira/browse/YARN-1774) | FS: Submitting to non-leaf queue throws NPE |  Blocker | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-1783](https://issues.apache.org/jira/browse/YARN-1783) | yarn application does not make any progress even when no other application is running when RM is being restarted in the background |  Critical | . | Arpit Gupta | Jian He |
| [HDFS-5064](https://issues.apache.org/jira/browse/HDFS-5064) | Standby checkpoints should not block concurrent readers |  Major | ha, namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6078](https://issues.apache.org/jira/browse/HDFS-6078) | TestIncrementalBlockReports is flaky |  Minor | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6071](https://issues.apache.org/jira/browse/HDFS-6071) | BlockReaderLocal doesn't return -1 on EOF when doing a zero-length read on a short file |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [YARN-1793](https://issues.apache.org/jira/browse/YARN-1793) | yarn application -kill doesn't kill UnmanagedAMs |  Critical | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-1788](https://issues.apache.org/jira/browse/YARN-1788) | AppsCompleted/AppsKilled metric is incorrect when MR job is killed with yarn application -kill |  Critical | resourcemanager | Tassapol Athiapinya | Varun Vasudev |
| [HDFS-6077](https://issues.apache.org/jira/browse/HDFS-6077) | running slive with webhdfs on secure HA cluster fails with unkown host exception |  Major | . | Arpit Gupta | Jing Zhao |
| [HADOOP-10395](https://issues.apache.org/jira/browse/HADOOP-10395) | TestCallQueueManager is flaky |  Minor | test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-10394](https://issues.apache.org/jira/browse/HADOOP-10394) | TestAuthenticationFilter is flaky |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-5028](https://issues.apache.org/jira/browse/MAPREDUCE-5028) | Maps fail when io.sort.mb is set to high value |  Critical | . | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-10337](https://issues.apache.org/jira/browse/HADOOP-10337) | ConcurrentModificationException from MetricsDynamicMBeanBase.createMBeanInfo() |  Major | metrics | Liang Xie | Liang Xie |
| [YARN-1444](https://issues.apache.org/jira/browse/YARN-1444) | RM crashes when node resource request sent without corresponding off-switch request |  Blocker | client, resourcemanager | Robert Grandl | Wangda Tan |
| [MAPREDUCE-5778](https://issues.apache.org/jira/browse/MAPREDUCE-5778) | JobSummary does not escape newlines in the job name |  Major | jobhistoryserver | Jason Lowe | Akira Ajisaka |
| [HDFS-6079](https://issues.apache.org/jira/browse/HDFS-6079) | Timeout for getFileBlockStorageLocations does not work |  Major | hdfs-client | Andrew Wang | Andrew Wang |
| [HDFS-6096](https://issues.apache.org/jira/browse/HDFS-6096) | TestWebHdfsTokens may timeout |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-5789](https://issues.apache.org/jira/browse/MAPREDUCE-5789) | Average Reduce time is incorrect on Job Overview page |  Major | jobhistoryserver, webapps | Rushabh S Shah | Rushabh S Shah |
| [HDFS-5244](https://issues.apache.org/jira/browse/HDFS-5244) | TestNNStorageRetentionManager#testPurgeMultipleDirs fails |  Major | test | Jinghui Wang | Jinghui Wang |
| [MAPREDUCE-5794](https://issues.apache.org/jira/browse/MAPREDUCE-5794) | SliveMapper always uses default FileSystem. |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6097](https://issues.apache.org/jira/browse/HDFS-6097) | zero-copy reads are incorrectly disabled on file offsets above 2GB |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-5751](https://issues.apache.org/jira/browse/MAPREDUCE-5751) | MR app master fails to start in some cases if mapreduce.job.classloader is true |  Major | . | Sangjin Lee | Sangjin Lee |
| [MAPREDUCE-5769](https://issues.apache.org/jira/browse/MAPREDUCE-5769) | Unregistration to RM should not be called if AM is crashed before registering with RM |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [MAPREDUCE-5570](https://issues.apache.org/jira/browse/MAPREDUCE-5570) | Map task attempt with fetch failure has incorrect attempt finish time |  Major | mr-am, mrv2 | Jason Lowe | Rushabh S Shah |
| [YARN-1833](https://issues.apache.org/jira/browse/YARN-1833) | TestRMAdminService Fails in trunk and branch-2 : Assert Fails due to different count of UserGroups for currentUser() |  Major | . | Mit Desai | Mit Desai |
| [HDFS-6106](https://issues.apache.org/jira/browse/HDFS-6106) | Reduce default for dfs.namenode.path.based.cache.refresh.interval.ms |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-4052](https://issues.apache.org/jira/browse/MAPREDUCE-4052) | Windows eclipse cannot submit job from Windows client to Linux/Unix Hadoop cluster. |  Major | job submission | xieguiming | Jian He |
| [YARN-1824](https://issues.apache.org/jira/browse/YARN-1824) | Make Windows client work with Linux/Unix cluster |  Major | . | Jian He | Jian He |
| [HDFS-6094](https://issues.apache.org/jira/browse/HDFS-6094) | The same block can be counted twice towards safe mode threshold |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6107](https://issues.apache.org/jira/browse/HDFS-6107) | When a block can't be cached due to limited space on the DataNode, that block becomes uncacheable |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5516](https://issues.apache.org/jira/browse/HDFS-5516) | WebHDFS does not require user name when anonymous http requests are disallowed. |  Major | webhdfs | Chris Nauroth | Miodrag Radulovic |
| [YARN-1206](https://issues.apache.org/jira/browse/YARN-1206) | AM container log link broken on NM web page |  Blocker | . | Jian He | Rohith Sharma K S |
| [YARN-1591](https://issues.apache.org/jira/browse/YARN-1591) | TestResourceTrackerService fails randomly on trunk |  Major | . | Vinod Kumar Vavilapalli | Tsuyoshi Ozawa |
| [YARN-1839](https://issues.apache.org/jira/browse/YARN-1839) | Capacity scheduler preempts an AM out. AM attempt 2 fails to launch task container with SecretManager$InvalidToken: No NMToken sent |  Critical | applications, capacityscheduler | Tassapol Athiapinya | Jian He |
| [YARN-1846](https://issues.apache.org/jira/browse/YARN-1846) | TestRM#testNMTokenSentForNormalContainer assumes CapacityScheduler |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-6068](https://issues.apache.org/jira/browse/HDFS-6068) | Disallow snapshot names that are also invalid directory names |  Major | snapshots | Andrew Wang | sathish |
| [HDFS-6117](https://issues.apache.org/jira/browse/HDFS-6117) | Print file path information in FileNotFoundException |  Minor | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-6099](https://issues.apache.org/jira/browse/HDFS-6099) | HDFS file system limits not enforced on renames. |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-6100](https://issues.apache.org/jira/browse/HDFS-6100) | DataNodeWebHdfsMethods does not failover in HA mode |  Major | ha | Arpit Gupta | Haohui Mai |
| [HDFS-6105](https://issues.apache.org/jira/browse/HDFS-6105) | NN web UI for DN list loads the same jmx page multiple times. |  Major | . | Kihwal Lee | Haohui Mai |
| [HDFS-6127](https://issues.apache.org/jira/browse/HDFS-6127) | WebHDFS tokens cannot be renewed in HA setup |  Major | ha | Arpit Gupta | Haohui Mai |
| [HDFS-6129](https://issues.apache.org/jira/browse/HDFS-6129) | When a replica is not found for deletion, do not throw exception. |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6089](https://issues.apache.org/jira/browse/HDFS-6089) | Standby NN while transitioning to active throws a connection refused error when the prior active NN process is suspended |  Major | ha | Arpit Gupta | Jing Zhao |
| [HDFS-6131](https://issues.apache.org/jira/browse/HDFS-6131) | Move HDFSHighAvailabilityWithNFS.apt.vm and HDFSHighAvailabilityWithQJM.apt.vm from Yarn to HDFS |  Major | documentation | Jing Zhao | Jing Zhao |
| [YARN-1859](https://issues.apache.org/jira/browse/YARN-1859) | WebAppProxyServlet will throw ApplicationNotFoundException if the app is no longer cached in RM |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1849](https://issues.apache.org/jira/browse/YARN-1849) | NPE in ResourceTrackerService#registerNodeManager for UAM |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5806](https://issues.apache.org/jira/browse/MAPREDUCE-5806) | Log4j settings in container-log4j.properties cannot be overridden |  Major | . | Eugene Koifman | Varun Vasudev |
| [HADOOP-10191](https://issues.apache.org/jira/browse/HADOOP-10191) | Missing executable permission on viewfs internal dirs |  Blocker | viewfs | Gera Shegalov | Gera Shegalov |
| [HDFS-6140](https://issues.apache.org/jira/browse/HDFS-6140) | WebHDFS cannot create a file with spaces in the name after HA failover changes. |  Major | webhdfs | Chris Nauroth | Chris Nauroth |
| [YARN-1670](https://issues.apache.org/jira/browse/YARN-1670) | aggregated log writer can write more log data then it says is the log length |  Critical | . | Thomas Graves | Mit Desai |
| [HADOOP-10015](https://issues.apache.org/jira/browse/HADOOP-10015) | UserGroupInformation prints out excessive ERROR warnings |  Minor | security | Haohui Mai | Nicolas Liochon |
| [YARN-1852](https://issues.apache.org/jira/browse/YARN-1852) | Application recovery throws InvalidStateTransitonException for FAILED and KILLED jobs |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [MAPREDUCE-5791](https://issues.apache.org/jira/browse/MAPREDUCE-5791) | Shuffle phase is slow in Windows - FadviseFileRegion::transferTo does not read disks efficiently |  Major | client | Nikola Vujic | Nikola Vujic |
| [HDFS-6135](https://issues.apache.org/jira/browse/HDFS-6135) | In HDFS upgrade with HA setup, JournalNode cannot handle layout version bump when rolling back |  Blocker | journal-node | Jing Zhao | Jing Zhao |
| [HDFS-5846](https://issues.apache.org/jira/browse/HDFS-5846) | Assigning DEFAULT\_RACK in resolveNetworkLocation method can break data resiliency |  Major | namenode | Nikola Vujic | Nikola Vujic |
| [HADOOP-10422](https://issues.apache.org/jira/browse/HADOOP-10422) | Remove redundant logging of RPC retry attempts. |  Minor | ipc | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5795](https://issues.apache.org/jira/browse/MAPREDUCE-5795) | Job should be marked as Failed if it is recovered from commit. |  Major | . | Yesha Vora | Xuan Gong |
| [HADOOP-10425](https://issues.apache.org/jira/browse/HADOOP-10425) | Incompatible behavior of LocalFileSystem:getContentSummary |  Critical | fs | Brandon Li | Tsz Wo Nicholas Sze |
| [HDFS-5840](https://issues.apache.org/jira/browse/HDFS-5840) | Follow-up to HDFS-5138 to improve error handling during partial upgrade failures |  Blocker | ha, journal-node, namenode | Aaron T. Myers | Jing Zhao |
| [HDFS-5807](https://issues.apache.org/jira/browse/HDFS-5807) | TestBalancerWithNodeGroup.testBalancerWithNodeGroup fails intermittently on Branch-2 |  Major | test | Mit Desai | Chen He |
| [YARN-1866](https://issues.apache.org/jira/browse/YARN-1866) | YARN RM fails to load state store with delegation token parsing error |  Blocker | . | Arpit Gupta | Jian He |
| [YARN-1867](https://issues.apache.org/jira/browse/YARN-1867) | NPE while fetching apps via the REST API |  Blocker | resourcemanager | Karthik Kambatla | Vinod Kumar Vavilapalli |
| [HDFS-6130](https://issues.apache.org/jira/browse/HDFS-6130) | NPE when upgrading namenode from fsimages older than -32 |  Blocker | namenode | Fengdong Yu | Haohui Mai |
| [HDFS-6115](https://issues.apache.org/jira/browse/HDFS-6115) | flush() should be called for every append on block scan verification log |  Minor | datanode | Vinayakumar B | Vinayakumar B |
| [HADOOP-10440](https://issues.apache.org/jira/browse/HADOOP-10440) | HarFsInputStream of HarFileSystem, when reading data, computing the position has bug |  Major | fs | guodongdong | guodongdong |
| [HADOOP-10441](https://issues.apache.org/jira/browse/HADOOP-10441) | Namenode metric "rpc.RetryCache/NameNodeRetryCache.CacheHit" can't be correctly processed by Ganglia |  Blocker | metrics | Jing Zhao | Jing Zhao |
| [YARN-1873](https://issues.apache.org/jira/browse/YARN-1873) | TestDistributedShell#testDSShell fails when the test cases are out of order |  Major | . | Mit Desai | Mit Desai |
| [HDFS-6157](https://issues.apache.org/jira/browse/HDFS-6157) | Fix the entry point of OfflineImageViewer for hdfs.cmd |  Major | . | Haohui Mai | Haohui Mai |
| [MAPREDUCE-5805](https://issues.apache.org/jira/browse/MAPREDUCE-5805) | Unable to parse launch time from job history file |  Major | jobhistoryserver | Fengdong Yu | Akira Ajisaka |
| [HDFS-6163](https://issues.apache.org/jira/browse/HDFS-6163) | Fix a minor bug in the HA upgrade document |  Minor | documentation | Fengdong Yu | Fengdong Yu |
| [HADOOP-10442](https://issues.apache.org/jira/browse/HADOOP-10442) | Group look-up can cause segmentation fault when certain JNI-based mapping module is used. |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10450](https://issues.apache.org/jira/browse/HADOOP-10450) | Build zlib native code bindings in hadoop.dll for Windows. |  Major | io, native | Chris Nauroth | Chris Nauroth |
| [HADOOP-10301](https://issues.apache.org/jira/browse/HADOOP-10301) | AuthenticationFilter should return Forbidden for failed authentication |  Blocker | security | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-5810](https://issues.apache.org/jira/browse/MAPREDUCE-5810) | TestStreamingTaskLog#testStreamingTaskLogWithHadoopCmd is failing |  Major | contrib/streaming | Mit Desai | Akira Ajisaka |
| [HDFS-6166](https://issues.apache.org/jira/browse/HDFS-6166) | revisit balancer so\_timeout |  Blocker | balancer & mover | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-5813](https://issues.apache.org/jira/browse/MAPREDUCE-5813) | YarnChild does not load job.xml with mapreduce.job.classloader=true |  Blocker | mrv2, task | Gera Shegalov | Gera Shegalov |
| [YARN-1830](https://issues.apache.org/jira/browse/YARN-1830) | TestRMRestart.testQueueMetricsOnRMRestart failure |  Major | resourcemanager | Karthik Kambatla | Zhijie Shen |
| [HDFS-6237](https://issues.apache.org/jira/browse/HDFS-6237) | TestDFSShell#testGet fails on Windows due to invalid file system path. |  Trivial | hdfs-client, test | Chris Nauroth | Chris Nauroth |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-5882](https://issues.apache.org/jira/browse/HDFS-5882) | TestAuditLogs is flaky |  Minor | . | Jimmy Xiang | Jimmy Xiang |
| [HDFS-5953](https://issues.apache.org/jira/browse/HDFS-5953) | TestBlockReaderFactory fails if libhadoop.so has not been built |  Major | . | Ted Yu | Akira Ajisaka |
| [HDFS-5936](https://issues.apache.org/jira/browse/HDFS-5936) | MiniDFSCluster does not clean data left behind by SecondaryNameNode. |  Major | namenode, test | Andrew Wang | Binglin Chang |
| [HDFS-6063](https://issues.apache.org/jira/browse/HDFS-6063) | TestAclCLI fails intermittently when running test 24: copyFromLocal |  Minor | test, tools | Colin P. McCabe | Chris Nauroth |
| [YARN-1855](https://issues.apache.org/jira/browse/YARN-1855) | TestRMFailover#testRMWebAppRedirect fails in trunk |  Critical | . | Ted Yu | Zhijie Shen |
| [YARN-1863](https://issues.apache.org/jira/browse/YARN-1863) | TestRMFailover fails with 'AssertionError: null' |  Blocker | . | Ted Yu | Xuan Gong |
| [YARN-1854](https://issues.apache.org/jira/browse/YARN-1854) | Race condition in TestRMHA#testStartAndTransitions |  Blocker | . | Mit Desai | Rohith Sharma K S |
| [HDFS-5672](https://issues.apache.org/jira/browse/HDFS-5672) | TestHASafeMode#testSafeBlockTracking fails in trunk |  Major | namenode | Ted Yu | Jing Zhao |
| [HADOOP-9525](https://issues.apache.org/jira/browse/HADOOP-9525) | Add tests that validate winutils chmod behavior on folders |  Major | test, util | Ivan Mitic | Ivan Mitic |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-930](https://issues.apache.org/jira/browse/YARN-930) | Bootstrap ApplicationHistoryService module |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-935](https://issues.apache.org/jira/browse/YARN-935) | YARN-321 branch is broken due to applicationhistoryserver module's pom.xml |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-962](https://issues.apache.org/jira/browse/YARN-962) | Update application\_history\_service.proto |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-984](https://issues.apache.org/jira/browse/YARN-984) | [YARN-321] Move classes from applicationhistoryservice.records.pb.impl package to applicationhistoryservice.records.impl.pb |  Major | . | Devaraj K | Devaraj K |
| [YARN-1007](https://issues.apache.org/jira/browse/YARN-1007) | [YARN-321] Enhance History Reader interface for Containers |  Major | . | Devaraj K | Mayank Bansal |
| [YARN-1191](https://issues.apache.org/jira/browse/YARN-1191) | [YARN-321] Update artifact versions for application history service |  Major | . | Mayank Bansal | Mayank Bansal |
| [YARN-934](https://issues.apache.org/jira/browse/YARN-934) | HistoryStorage writer interface for Application History Server |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-947](https://issues.apache.org/jira/browse/YARN-947) | Defining the history data classes for the implementation of the reading/writing interface |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-956](https://issues.apache.org/jira/browse/YARN-956) | [YARN-321] Add a testable in-memory HistoryStorage |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [YARN-975](https://issues.apache.org/jira/browse/YARN-975) | Add a file-system implementation for history-storage |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1379](https://issues.apache.org/jira/browse/YARN-1379) | [YARN-321] AHS protocols need to be in yarn proto package name after YARN-1170 |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1123](https://issues.apache.org/jira/browse/YARN-1123) | [YARN-321] Adding ContainerReport and Protobuf implementation |  Major | . | Zhijie Shen | Mayank Bansal |
| [YARN-978](https://issues.apache.org/jira/browse/YARN-978) | [YARN-321] Adding ApplicationAttemptReport and Protobuf implementation |  Major | . | Mayank Bansal | Mayank Bansal |
| [YARN-979](https://issues.apache.org/jira/browse/YARN-979) | [YARN-321] Add more APIs related to ApplicationAttempt and Container in ApplicationHistoryProtocol |  Major | . | Mayank Bansal | Mayank Bansal |
| [YARN-974](https://issues.apache.org/jira/browse/YARN-974) | RMContainer should collect more useful information to be recorded in Application-History |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-987](https://issues.apache.org/jira/browse/YARN-987) | Adding ApplicationHistoryManager responsible for exposing reports to all clients |  Major | . | Mayank Bansal | Mayank Bansal |
| [YARN-953](https://issues.apache.org/jira/browse/YARN-953) | [YARN-321] Enable ResourceManager to write history data |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [HDFS-5531](https://issues.apache.org/jira/browse/HDFS-5531) | Combine the getNsQuota() and getDsQuota() methods in INode |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-5285](https://issues.apache.org/jira/browse/HDFS-5285) | Flatten INodeFile hierarchy: Add UnderContruction Feature |  Major | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [YARN-1266](https://issues.apache.org/jira/browse/YARN-1266) | Implement PB service and client wrappers for ApplicationHistoryProtocol |  Major | . | Mayank Bansal | Mayank Bansal |
| [YARN-955](https://issues.apache.org/jira/browse/YARN-955) | [YARN-321] Implementation of ApplicationHistoryProtocol |  Major | . | Vinod Kumar Vavilapalli | Mayank Bansal |
| [YARN-1242](https://issues.apache.org/jira/browse/YARN-1242) | Script changes to start AHS as an individual process |  Major | . | Zhijie Shen | Mayank Bansal |
| [HDFS-5286](https://issues.apache.org/jira/browse/HDFS-5286) | Flatten INodeDirectory hierarchy: add DirectoryWithQuotaFeature |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-954](https://issues.apache.org/jira/browse/YARN-954) | [YARN-321] History Service should create the webUI and wire it to HistoryStorage |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [HDFS-5537](https://issues.apache.org/jira/browse/HDFS-5537) | Remove FileWithSnapshot interface |  Major | namenode, snapshots | Jing Zhao | Jing Zhao |
| [YARN-967](https://issues.apache.org/jira/browse/YARN-967) | [YARN-321] Command Line Interface(CLI) for Reading Application History Storage Data |  Major | . | Devaraj K | Mayank Bansal |
| [HDFS-5554](https://issues.apache.org/jira/browse/HDFS-5554) | Add Snapshot Feature to INodeFile |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-5647](https://issues.apache.org/jira/browse/HDFS-5647) | Merge INodeDirectory.Feature and INodeFile.Feature |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-5632](https://issues.apache.org/jira/browse/HDFS-5632) | Add Snapshot feature to INodeDirectory |  Major | namenode | Jing Zhao | Jing Zhao |
| [YARN-1023](https://issues.apache.org/jira/browse/YARN-1023) | [YARN-321] Webservices REST API's support for Application History |  Major | . | Devaraj K | Zhijie Shen |
| [YARN-1534](https://issues.apache.org/jira/browse/YARN-1534) | TestAHSWebApp failed in YARN-321 branch |  Major | . | Shinichi Yamashita | Shinichi Yamashita |
| [YARN-1493](https://issues.apache.org/jira/browse/YARN-1493) | Schedulers don't recognize apps separately from app-attempts |  Major | . | Jian He | Jian He |
| [YARN-1555](https://issues.apache.org/jira/browse/YARN-1555) | [YARN-321] Failing tests in org.apache.hadoop.yarn.server.applicationhistoryservice.\* |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HDFS-5715](https://issues.apache.org/jira/browse/HDFS-5715) | Use Snapshot ID to indicate the corresponding Snapshot for a FileDiff/DirectoryDiff |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-5726](https://issues.apache.org/jira/browse/HDFS-5726) | Fix compilation error in AbstractINodeDiff for JDK7 |  Minor | namenode | Jing Zhao | Jing Zhao |
| [YARN-1490](https://issues.apache.org/jira/browse/YARN-1490) | RM should optionally not kill all containers when an ApplicationMaster exits |  Major | . | Vinod Kumar Vavilapalli | Jian He |
| [YARN-1041](https://issues.apache.org/jira/browse/YARN-1041) | Protocol changes for RM to bind and notify a restarted AM of existing containers |  Major | resourcemanager | Steve Loughran | Jian He |
| [YARN-1566](https://issues.apache.org/jira/browse/YARN-1566) | Change distributed-shell to retain containers from previous AppAttempt |  Major | . | Jian He | Jian He |
| [YARN-1594](https://issues.apache.org/jira/browse/YARN-1594) | YARN-321 branch needs to be updated after YARN-888 pom changes |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1596](https://issues.apache.org/jira/browse/YARN-1596) | Javadoc failures on YARN-321 branch |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1597](https://issues.apache.org/jira/browse/YARN-1597) | FindBugs warnings on YARN-321 branch |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1595](https://issues.apache.org/jira/browse/YARN-1595) | Test failures on YARN-321 branch |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1605](https://issues.apache.org/jira/browse/YARN-1605) | Fix formatting issues with new module in YARN-321 branch |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1496](https://issues.apache.org/jira/browse/YARN-1496) | Protocol additions to allow moving apps between queues |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-1587](https://issues.apache.org/jira/browse/YARN-1587) | [YARN-321] Merge Patch for YARN-321 |  Major | . | Mayank Bansal | Vinod Kumar Vavilapalli |
| [YARN-1625](https://issues.apache.org/jira/browse/YARN-1625) | mvn apache-rat:check outputs warning message in YARN-321 branch |  Trivial | . | Shinichi Yamashita | Shinichi Yamashita |
| [YARN-1613](https://issues.apache.org/jira/browse/YARN-1613) | Fix config name YARN\_HISTORY\_SERVICE\_ENABLED |  Major | . | Zhijie Shen | Akira Ajisaka |
| [YARN-1633](https://issues.apache.org/jira/browse/YARN-1633) | Define user-faced entity, entity-info and event objects |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [HDFS-5746](https://issues.apache.org/jira/browse/HDFS-5746) | add ShortCircuitSharedMemorySegment |  Major | datanode, hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [YARN-1504](https://issues.apache.org/jira/browse/YARN-1504) | RM changes for moving apps between queues |  Major | resourcemanager | Sandy Ryza | Sandy Ryza |
| [YARN-1611](https://issues.apache.org/jira/browse/YARN-1611) | Make admin refresh of capacity scheduler configuration work across RM failover |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-1498](https://issues.apache.org/jira/browse/YARN-1498) | Common scheduler changes for moving apps between queues |  Major | resourcemanager | Sandy Ryza | Sandy Ryza |
| [YARN-1639](https://issues.apache.org/jira/browse/YARN-1639) | YARM RM HA requires different configs on different RM hosts |  Major | resourcemanager | Arpit Gupta | Xuan Gong |
| [YARN-1659](https://issues.apache.org/jira/browse/YARN-1659) | Define the ApplicationTimelineStore store as an abstraction for implementing different storage impls for storing timeline information |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-1668](https://issues.apache.org/jira/browse/YARN-1668) | Make admin refreshAdminAcls work across RM failover |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-1667](https://issues.apache.org/jira/browse/YARN-1667) | Make admin refreshSuperUserGroupsConfiguration work across RM failover |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-1684](https://issues.apache.org/jira/browse/YARN-1684) | Fix history server heap size in yarn script |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-1669](https://issues.apache.org/jira/browse/YARN-1669) | Make admin refreshServiceAcls work across RM failover |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-1634](https://issues.apache.org/jira/browse/YARN-1634) | Define an in-memory implementation of ApplicationTimelineStore |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [YARN-1461](https://issues.apache.org/jira/browse/YARN-1461) | RM API and RM changes to handle tags for running jobs |  Major | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-1636](https://issues.apache.org/jira/browse/YARN-1636) | Implement timeline related web-services inside AHS for storing and retrieving entities+events |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [YARN-1499](https://issues.apache.org/jira/browse/YARN-1499) | Fair Scheduler changes for moving apps between queues |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-1665](https://issues.apache.org/jira/browse/YARN-1665) | Set better defaults for HA configs for automatic failover |  Major | resourcemanager | Arpit Gupta | Xuan Gong |
| [YARN-1660](https://issues.apache.org/jira/browse/YARN-1660) | add the ability to set yarn.resourcemanager.hostname.rm-id instead of setting all the various host:port properties for RM |  Major | resourcemanager | Arpit Gupta | Xuan Gong |
| [YARN-1497](https://issues.apache.org/jira/browse/YARN-1497) | Expose moving apps between queues on the command line |  Major | client | Sandy Ryza | Sandy Ryza |
| [YARN-1635](https://issues.apache.org/jira/browse/YARN-1635) | Implement a Leveldb based ApplicationTimelineStore |  Major | . | Vinod Kumar Vavilapalli | Billie Rinaldi |
| [YARN-1637](https://issues.apache.org/jira/browse/YARN-1637) | Implement a client library for java users to post entities+events |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [YARN-1459](https://issues.apache.org/jira/browse/YARN-1459) | RM services should depend on ConfigurationProvider during startup too |  Major | resourcemanager | Karthik Kambatla | Xuan Gong |
| [YARN-1698](https://issues.apache.org/jira/browse/YARN-1698) | Replace MemoryApplicationTimelineStore with LeveldbApplicationTimelineStore as default |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1706](https://issues.apache.org/jira/browse/YARN-1706) | Create an utility function to dump timeline records to json |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1719](https://issues.apache.org/jira/browse/YARN-1719) | ATSWebServices produces jersey warnings |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-1641](https://issues.apache.org/jira/browse/YARN-1641) | ZK store should attempt a write periodically to ensure it is still Active |  Major | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5810](https://issues.apache.org/jira/browse/HDFS-5810) | Unify mmap cache and short-circuit file descriptor cache |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5938](https://issues.apache.org/jira/browse/HDFS-5938) | Make BlockReaderFactory#BlockReaderPeer a static class |  Trivial | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5847](https://issues.apache.org/jira/browse/HDFS-5847) | Consolidate INodeReference into a separate section |  Major | . | Haohui Mai | Jing Zhao |
| [YARN-1578](https://issues.apache.org/jira/browse/YARN-1578) | Fix how to read history file in FileSystemApplicationHistoryStore |  Major | . | Shinichi Yamashita | Shinichi Yamashita |
| [YARN-1345](https://issues.apache.org/jira/browse/YARN-1345) | Removing FINAL\_SAVING from YarnApplicationAttemptState |  Major | . | Zhijie Shen | Zhijie Shen |
| [HDFS-5940](https://issues.apache.org/jira/browse/HDFS-5940) | Minor cleanups to ShortCircuitReplica, FsDatasetCache, and DomainSocketWatcher |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [YARN-1676](https://issues.apache.org/jira/browse/YARN-1676) | Make admin refreshUserToGroupsMappings of configuration work across RM failover |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-5942](https://issues.apache.org/jira/browse/HDFS-5942) | Fix javadoc in OfflineImageViewer |  Minor | documentation, tools | Akira Ajisaka | Akira Ajisaka |
| [YARN-1428](https://issues.apache.org/jira/browse/YARN-1428) | RM cannot write the final state of RMApp/RMAppAttempt to the application history store in the transition to the final state |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1666](https://issues.apache.org/jira/browse/YARN-1666) | Make admin refreshNodes work across RM failover |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-5973](https://issues.apache.org/jira/browse/HDFS-5973) | add DomainSocket#shutdown method |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5868](https://issues.apache.org/jira/browse/HDFS-5868) | Make hsync implementation pluggable |  Major | datanode | Taylor, Buddy | Taylor, Buddy |
| [HDFS-5483](https://issues.apache.org/jira/browse/HDFS-5483) | NN should gracefully handle multiple block replicas on same DN |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-10278](https://issues.apache.org/jira/browse/HADOOP-10278) | Refactor to make CallQueue pluggable |  Major | ipc | Chris Li | Chris Li |
| [YARN-1732](https://issues.apache.org/jira/browse/YARN-1732) | Change types of related entities and primary filters in ATSEntity |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-1687](https://issues.apache.org/jira/browse/YARN-1687) | Refactoring timeline classes to remove "app" related words |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1749](https://issues.apache.org/jira/browse/YARN-1749) | Review AHS configs and sync them up with the timeline-service configs |  Major | . | Zhijie Shen | Zhijie Shen |
| [HDFS-5623](https://issues.apache.org/jira/browse/HDFS-5623) | NameNode: add tests for skipping ACL enforcement when permission checks are disabled, user is superuser or user is member of supergroup. |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [YARN-1588](https://issues.apache.org/jira/browse/YARN-1588) | Rebind NM tokens for previous attempt's running containers to the new attempt |  Major | . | Jian He | Jian He |
| [HADOOP-10285](https://issues.apache.org/jira/browse/HADOOP-10285) | Admin interface to swap callqueue at runtime |  Major | . | Chris Li | Chris Li |
| [HDFS-5956](https://issues.apache.org/jira/browse/HDFS-5956) | A file size is multiplied by the replication factor in 'hdfs oiv -p FileDistribution' option |  Major | tools | Akira Ajisaka | Akira Ajisaka |
| [YARN-1734](https://issues.apache.org/jira/browse/YARN-1734) | RM should get the updated Configurations when it transits from Standby to Active |  Critical | . | Xuan Gong | Xuan Gong |
| [HDFS-5950](https://issues.apache.org/jira/browse/HDFS-5950) | The DFSClient and DataNode should use shared memory segments to communicate short-circuit information |  Major | datanode, hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5866](https://issues.apache.org/jira/browse/HDFS-5866) | '-maxSize' and '-step' option fail in OfflineImageViewer |  Major | tools | Akira Ajisaka | Akira Ajisaka |
| [YARN-1704](https://issues.apache.org/jira/browse/YARN-1704) | Review LICENSE and NOTICE to reflect new levelDB releated libraries being used |  Blocker | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-1765](https://issues.apache.org/jira/browse/YARN-1765) | Write test cases to verify that killApplication API works in RM HA |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-1729](https://issues.apache.org/jira/browse/YARN-1729) | TimelineWebServices always passes primary and secondary filters as strings |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-6046](https://issues.apache.org/jira/browse/HDFS-6046) | add dfs.client.mmap.enabled |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [YARN-1730](https://issues.apache.org/jira/browse/YARN-1730) | Leveldb timeline store needs simple write locking |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-6040](https://issues.apache.org/jira/browse/HDFS-6040) | fix DFSClient issue without libhadoop.so and some other ShortCircuitShm cleanups |  Blocker | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [YARN-986](https://issues.apache.org/jira/browse/YARN-986) | RM DT token service should have service addresses of both RMs |  Blocker | . | Vinod Kumar Vavilapalli | Karthik Kambatla |
| [YARN-1766](https://issues.apache.org/jira/browse/YARN-1766) | When RM does the initiation, it should use loaded Configuration instead of bootstrap configuration. |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-5167](https://issues.apache.org/jira/browse/HDFS-5167) | Add metrics about the NameNode retry cache |  Minor | ha, namenode | Jing Zhao | Tsuyoshi Ozawa |
| [YARN-1761](https://issues.apache.org/jira/browse/YARN-1761) | RMAdminCLI should check whether HA is enabled before executes transitionToActive/transitionToStandby |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-5898](https://issues.apache.org/jira/browse/HDFS-5898) | Allow NFS gateway to login/relogin from its kerberos keytab |  Major | nfs | Jing Zhao | Abin Shahab |
| [HDFS-6061](https://issues.apache.org/jira/browse/HDFS-6061) | Allow dfs.datanode.shared.file.descriptor.path to contain multiple entries and fall back when needed |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-6060](https://issues.apache.org/jira/browse/HDFS-6060) | NameNode should not check DataNode layout version |  Major | namenode | Brandon Li | Brandon Li |
| [YARN-1780](https://issues.apache.org/jira/browse/YARN-1780) | Improve logging in timeline service |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1525](https://issues.apache.org/jira/browse/YARN-1525) | Web UI should redirect to active RM when HA is enabled. |  Major | . | Xuan Gong | Cindy Li |
| [HDFS-5986](https://issues.apache.org/jira/browse/HDFS-5986) | Capture the number of blocks pending deletion on namenode webUI |  Major | namenode | Suresh Srinivas | Chris Nauroth |
| [HDFS-6076](https://issues.apache.org/jira/browse/HDFS-6076) | SimulatedDataSet should not create DatanodeRegistration with namenode layout version and type |  Minor | datanode, test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-1781](https://issues.apache.org/jira/browse/YARN-1781) | NM should allow users to specify max disk utilization for local disks |  Major | nodemanager | Varun Vasudev | Varun Vasudev |
| [HADOOP-10393](https://issues.apache.org/jira/browse/HADOOP-10393) | Fix hadoop-auth javac warnings |  Minor | security | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-1410](https://issues.apache.org/jira/browse/YARN-1410) | Handle RM fails over after getApplicationID() and before submitApplication(). |  Major | . | Bikas Saha | Xuan Gong |
| [YARN-1787](https://issues.apache.org/jira/browse/YARN-1787) | yarn applicationattempt/container print wrong usage information |  Major | . | Zhijie Shen | Zhijie Shen |
| [HADOOP-10399](https://issues.apache.org/jira/browse/HADOOP-10399) | FileContext API for ACLs. |  Major | fs | Chris Nauroth | Vinayakumar B |
| [YARN-1764](https://issues.apache.org/jira/browse/YARN-1764) | Handle RM fail overs after the submitApplication call. |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-5638](https://issues.apache.org/jira/browse/HDFS-5638) | HDFS implementation of FileContext API for ACLs. |  Major | hdfs-client | Chris Nauroth | Vinayakumar B |
| [YARN-1821](https://issues.apache.org/jira/browse/YARN-1821) | NPE on registerNodeManager if the request has containers for UnmanagedAMs |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-6086](https://issues.apache.org/jira/browse/HDFS-6086) | Fix a case where zero-copy or no-checksum reads were not allowed even when the block was cached |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [YARN-1800](https://issues.apache.org/jira/browse/YARN-1800) | YARN NodeManager with java.util.concurrent.RejectedExecutionException |  Critical | nodemanager | Paul Isaychuk | Varun Vasudev |
| [YARN-1812](https://issues.apache.org/jira/browse/YARN-1812) | Job stays in PREP state for long time after RM Restarts |  Major | . | Yesha Vora | Jian He |
| [YARN-1816](https://issues.apache.org/jira/browse/YARN-1816) | Succeeded application remains in accepted after RM restart |  Major | . | Arpit Gupta | Jian He |
| [YARN-1389](https://issues.apache.org/jira/browse/YARN-1389) | ApplicationClientProtocol and ApplicationHistoryProtocol should expose analogous APIs |  Major | . | Mayank Bansal | Mayank Bansal |
| [YARN-1658](https://issues.apache.org/jira/browse/YARN-1658) | Webservice should redirect to active RM when HA is enabled. |  Major | . | Cindy Li | Cindy Li |
| [YARN-1717](https://issues.apache.org/jira/browse/YARN-1717) | Enable offline deletion of entries in leveldb timeline store |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HADOOP-10407](https://issues.apache.org/jira/browse/HADOOP-10407) | Fix the javac warnings in the ipc package. |  Minor | ipc | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-1685](https://issues.apache.org/jira/browse/YARN-1685) | Bugs around log URL |  Major | . | Mayank Bansal | Zhijie Shen |
| [YARN-1705](https://issues.apache.org/jira/browse/YARN-1705) | Reset cluster-metrics on transition to standby |  Major | resourcemanager | Karthik Kambatla | Rohith Sharma K S |
| [YARN-1690](https://issues.apache.org/jira/browse/YARN-1690) | Sending timeline entities+events from Distributed shell |  Major | . | Mayank Bansal | Mayank Bansal |
| [YARN-1640](https://issues.apache.org/jira/browse/YARN-1640) | Manual Failover does not work in secure clusters |  Blocker | . | Xuan Gong | Xuan Gong |
| [HDFS-6038](https://issues.apache.org/jira/browse/HDFS-6038) | Allow JournalNode to handle editlog produced by new release with future layoutversion |  Major | journal-node, namenode | Haohui Mai | Jing Zhao |
| [YARN-1811](https://issues.apache.org/jira/browse/YARN-1811) | RM HA: AM link broken if the AM is on nodes other than RM |  Major | resourcemanager | Robert Kanter | Robert Kanter |
| [MAPREDUCE-5787](https://issues.apache.org/jira/browse/MAPREDUCE-5787) | Modify ShuffleHandler to support Keep-Alive |  Critical | nodemanager | Rajesh Balamohan | Rajesh Balamohan |
| [YARN-1775](https://issues.apache.org/jira/browse/YARN-1775) | Create SMAPBasedProcessTree to get PSS information |  Major | nodemanager | Rajesh Balamohan | Rajesh Balamohan |
| [YARN-1776](https://issues.apache.org/jira/browse/YARN-1776) | renewDelegationToken should survive RM failover |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1577](https://issues.apache.org/jira/browse/YARN-1577) | Unmanaged AM is broken because of YARN-1493 |  Blocker | . | Jian He | Jian He |
| [YARN-1838](https://issues.apache.org/jira/browse/YARN-1838) | Timeline service getEntities API should provide ability to get entities from given id |  Major | . | Srimanth Gunturi | Billie Rinaldi |
| [HDFS-6124](https://issues.apache.org/jira/browse/HDFS-6124) | Add final modifier to class members |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [YARN-1850](https://issues.apache.org/jira/browse/YARN-1850) | Make enabling timeline service configurable |  Major | . | Zhijie Shen | Zhijie Shen |
| [HADOOP-10280](https://issues.apache.org/jira/browse/HADOOP-10280) | Make Schedulables return a configurable identity of user or group |  Major | . | Chris Li | Chris Li |
| [YARN-1521](https://issues.apache.org/jira/browse/YARN-1521) | Mark appropriate protocol methods with the idempotent annotation or AtMostOnce annotation |  Blocker | . | Xuan Gong | Xuan Gong |
| [HADOOP-10437](https://issues.apache.org/jira/browse/HADOOP-10437) | Fix the javac warnings in the conf and the util package |  Minor | conf, util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10449](https://issues.apache.org/jira/browse/HADOOP-10449) | Fix the javac warnings in the security packages. |  Minor | security | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-1017](https://issues.apache.org/jira/browse/YARN-1017) | Document RM Restart feature |  Blocker | resourcemanager | Jian He | Jian He |
| [YARN-1893](https://issues.apache.org/jira/browse/YARN-1893) | Make ApplicationMasterProtocol#allocate AtMostOnce |  Blocker | resourcemanager | Xuan Gong | Xuan Gong |
| [HDFS-4564](https://issues.apache.org/jira/browse/HDFS-4564) | Webhdfs returns incorrect http response codes for denied operations |  Blocker | webhdfs | Daryn Sharp | Daryn Sharp |
| [YARN-925](https://issues.apache.org/jira/browse/YARN-925) | HistoryStorage Reader Interface for Application History Server |  Major | . | Mayank Bansal | Mayank Bansal |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-6025](https://issues.apache.org/jira/browse/HDFS-6025) | Update findbugsExcludeFile.xml |  Minor | build | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6030](https://issues.apache.org/jira/browse/HDFS-6030) | Remove an unused constructor in INode.java |  Trivial | . | Yongjun Zhang | Yongjun Zhang |
| [YARN-1452](https://issues.apache.org/jira/browse/YARN-1452) | Document the usage of the generic application history and the timeline data service |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1891](https://issues.apache.org/jira/browse/YARN-1891) | Document NodeManager health-monitoring |  Minor | . | Varun Vasudev | Varun Vasudev |


