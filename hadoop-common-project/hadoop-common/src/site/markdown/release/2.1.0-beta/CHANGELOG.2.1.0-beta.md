
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

## Release 2.1.0-beta - 2013-08-22

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9832](https://issues.apache.org/jira/browse/HADOOP-9832) | Add RPC header to client ping |  Blocker | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9820](https://issues.apache.org/jira/browse/HADOOP-9820) | RPCv9 wire protocol is insufficient to support multiplexing |  Blocker | ipc, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-9698](https://issues.apache.org/jira/browse/HADOOP-9698) | RPCv9 client must honor server's SASL negotiate response |  Blocker | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9683](https://issues.apache.org/jira/browse/HADOOP-9683) | Wrap IpcConnectionContext in RPC headers |  Blocker | ipc | Luke Lu | Daryn Sharp |
| [HADOOP-9649](https://issues.apache.org/jira/browse/HADOOP-9649) | Promote YARN service life-cycle libraries into Hadoop Common |  Blocker | . | Zhijie Shen | Zhijie Shen |
| [HADOOP-9630](https://issues.apache.org/jira/browse/HADOOP-9630) | Remove IpcSerializationType |  Major | ipc | Luke Lu | Junping Du |
| [HADOOP-9425](https://issues.apache.org/jira/browse/HADOOP-9425) | Add error codes to rpc-response |  Major | ipc | Sanjay Radia | Sanjay Radia |
| [HADOOP-9421](https://issues.apache.org/jira/browse/HADOOP-9421) | Convert SASL to use ProtoBuf and provide negotiation capabilities |  Blocker | . | Sanjay Radia | Daryn Sharp |
| [HADOOP-9380](https://issues.apache.org/jira/browse/HADOOP-9380) | Add totalLength to rpc response |  Major | ipc | Sanjay Radia | Sanjay Radia |
| [HADOOP-9194](https://issues.apache.org/jira/browse/HADOOP-9194) | RPC Support for QoS |  Major | ipc | Luke Lu | Junping Du |
| [HADOOP-9163](https://issues.apache.org/jira/browse/HADOOP-9163) | The rpc msg in  ProtobufRpcEngine.proto should be moved out to avoid an extra copy |  Major | ipc | Sanjay Radia | Sanjay Radia |
| [HADOOP-9151](https://issues.apache.org/jira/browse/HADOOP-9151) | Include RPC error info in RpcResponseHeader instead of sending it separately |  Major | ipc | Sanjay Radia | Sanjay Radia |
| [HADOOP-8886](https://issues.apache.org/jira/browse/HADOOP-8886) | Remove KFS support |  Major | fs | Eli Collins | Eli Collins |
| [HDFS-5083](https://issues.apache.org/jira/browse/HDFS-5083) | Update the HDFS compatibility version range |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-4866](https://issues.apache.org/jira/browse/HDFS-4866) | Protocol buffer support cannot compile under C |  Blocker | namenode | Ralph Castain | Arpit Agarwal |
| [HDFS-4659](https://issues.apache.org/jira/browse/HDFS-4659) | Support setting execution bit for regular files |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-4434](https://issues.apache.org/jira/browse/HDFS-4434) | Provide a mapping from INodeId to INode |  Major | namenode | Brandon Li | Suresh Srinivas |
| [HDFS-4305](https://issues.apache.org/jira/browse/HDFS-4305) | Add a configurable limit on number of blocks per file, and min block size |  Minor | namenode | Todd Lipcon | Andrew Wang |
| [HDFS-2802](https://issues.apache.org/jira/browse/HDFS-2802) | Support for RW/RO snapshots in HDFS |  Major | namenode | Hari Mankude | Tsz Wo Nicholas Sze |
| [MAPREDUCE-5304](https://issues.apache.org/jira/browse/MAPREDUCE-5304) | mapreduce.Job killTask/failTask/getTaskCompletionEvents methods have incompatible signature changes |  Blocker | . | Alejandro Abdelnur | Karthik Kambatla |
| [MAPREDUCE-5300](https://issues.apache.org/jira/browse/MAPREDUCE-5300) | Two function signature changes in filecache.DistributedCache |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5237](https://issues.apache.org/jira/browse/MAPREDUCE-5237) | ClusterStatus incompatiblity issues with MR1 |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5234](https://issues.apache.org/jira/browse/MAPREDUCE-5234) | Signature changes for getTaskId of TaskReport in mapred |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5233](https://issues.apache.org/jira/browse/MAPREDUCE-5233) | Functions are changed or removed from Job in jobcontrol |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5176](https://issues.apache.org/jira/browse/MAPREDUCE-5176) | Preemptable annotations (to support preemption in MR) |  Major | mrv2 | Carlo Curino | Carlo Curino |
| [MAPREDUCE-5156](https://issues.apache.org/jira/browse/MAPREDUCE-5156) | Hadoop-examples-1.x.x.jar cannot run on Yarn |  Blocker | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-4942](https://issues.apache.org/jira/browse/MAPREDUCE-4942) | mapreduce.Job has a bunch of methods that throw InterruptedException so its incompatible with MR1 |  Major | mrv2 | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4737](https://issues.apache.org/jira/browse/MAPREDUCE-4737) |  Hadoop does not close output file / does not call Mapper.cleanup if exception in map |  Major | . | Daniel Dai | Arun C Murthy |
| [MAPREDUCE-4067](https://issues.apache.org/jira/browse/MAPREDUCE-4067) | Replace YarnRemoteException with IOException in MRv2 APIs |  Critical | . | Jitendra Nath Pandey | Xuan Gong |
| [YARN-1056](https://issues.apache.org/jira/browse/YARN-1056) | Fix configs yarn.resourcemanager.resourcemanager.connect.{max.wait.secs\|retry\_interval.secs} |  Trivial | . | Karthik Kambatla | Karthik Kambatla |
| [YARN-926](https://issues.apache.org/jira/browse/YARN-926) | ContainerManagerProtcol APIs should take in requests for multiple containers |  Blocker | . | Vinod Kumar Vavilapalli | Jian He |
| [YARN-918](https://issues.apache.org/jira/browse/YARN-918) | ApplicationMasterProtocol doesn't need ApplicationAttemptId in the payload after YARN-701 |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-869](https://issues.apache.org/jira/browse/YARN-869) | ResourceManagerAdministrationProtocol should neither be public(yet) nor in yarn.api |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-851](https://issues.apache.org/jira/browse/YARN-851) | Share NMTokens using NMTokenCache (api-based) instead of memory based approach which is used currently. |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-841](https://issues.apache.org/jira/browse/YARN-841) | Annotate and document AuxService APIs |  Major | . | Siddharth Seth | Vinod Kumar Vavilapalli |
| [YARN-840](https://issues.apache.org/jira/browse/YARN-840) | Move ProtoUtils to  yarn.api.records.pb.impl |  Major | . | Jian He | Jian He |
| [YARN-837](https://issues.apache.org/jira/browse/YARN-837) | ClusterInfo.java doesn't seem to belong to org.apache.hadoop.yarn |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-834](https://issues.apache.org/jira/browse/YARN-834) | Review/fix annotations for yarn-client module and clearly differentiate \*Async apis |  Blocker | . | Arun C Murthy | Zhijie Shen |
| [YARN-831](https://issues.apache.org/jira/browse/YARN-831) | Remove resource min from GetNewApplicationResponse |  Blocker | . | Jian He | Jian He |
| [YARN-829](https://issues.apache.org/jira/browse/YARN-829) | Rename RMTokenSelector to be RMDelegationTokenSelector |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-828](https://issues.apache.org/jira/browse/YARN-828) | Remove YarnVersionAnnotation |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-826](https://issues.apache.org/jira/browse/YARN-826) | Move Clock/SystemClock to util package |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-824](https://issues.apache.org/jira/browse/YARN-824) | Add  static factory to yarn client lib interface and change it to abstract class |  Major | . | Jian He | Jian He |
| [YARN-823](https://issues.apache.org/jira/browse/YARN-823) | Move RMAdmin from yarn.client to yarn.client.cli and rename as RMAdminCLI |  Major | . | Jian He | Jian He |
| [YARN-822](https://issues.apache.org/jira/browse/YARN-822) | Rename ApplicationToken to AMRMToken |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-821](https://issues.apache.org/jira/browse/YARN-821) | Rename FinishApplicationMasterRequest.setFinishApplicationStatus to setFinalApplicationStatus to be consistent with getter |  Major | . | Jian He | Jian He |
| [YARN-806](https://issues.apache.org/jira/browse/YARN-806) | Move ContainerExitStatus from yarn.api to yarn.api.records |  Major | . | Jian He | Jian He |
| [YARN-792](https://issues.apache.org/jira/browse/YARN-792) | Move NodeHealthStatus from yarn.api.record to yarn.server.api.record |  Major | . | Jian He | Jian He |
| [YARN-791](https://issues.apache.org/jira/browse/YARN-791) | Ensure that RM RPC APIs that return nodes are consistent with /nodes REST API |  Blocker | api, resourcemanager | Sandy Ryza | Sandy Ryza |
| [YARN-787](https://issues.apache.org/jira/browse/YARN-787) | Remove resource min from Yarn client API |  Blocker | api | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-777](https://issues.apache.org/jira/browse/YARN-777) | Remove unreferenced objects from proto |  Major | . | Jian He | Jian He |
| [YARN-756](https://issues.apache.org/jira/browse/YARN-756) | Move PreemptionContainer/PremptionContract/PreemptionMessage/StrictPreemptionContract/PreemptionResourceRequest to api.records |  Major | . | Jian He | Jian He |
| [YARN-755](https://issues.apache.org/jira/browse/YARN-755) | Rename AllocateResponse.reboot to AllocateResponse.resync |  Major | . | Bikas Saha | Bikas Saha |
| [YARN-753](https://issues.apache.org/jira/browse/YARN-753) | Add individual factory method for api protocol records |  Major | . | Jian He | Jian He |
| [YARN-749](https://issues.apache.org/jira/browse/YARN-749) | Rename ResourceRequest (get,set)HostName to (get,set)ResourceName |  Major | . | Arun C Murthy | Arun C Murthy |
| [YARN-748](https://issues.apache.org/jira/browse/YARN-748) | Move BuilderUtils from yarn-common to yarn-server-common |  Major | . | Jian He | Jian He |
| [YARN-746](https://issues.apache.org/jira/browse/YARN-746) | rename Service.register() and Service.unregister() to registerServiceListener() & unregisterServiceListener() respectively |  Major | . | Steve Loughran | Steve Loughran |
| [YARN-735](https://issues.apache.org/jira/browse/YARN-735) | Make ApplicationAttemptID, ContainerID, NodeID immutable |  Major | . | Jian He | Jian He |
| [YARN-724](https://issues.apache.org/jira/browse/YARN-724) | Move ProtoBase from api.records to api.records.impl.pb |  Major | . | Jian He | Jian He |
| [YARN-720](https://issues.apache.org/jira/browse/YARN-720) | container-log4j.properties should not refer to mapreduce properties |  Major | . | Siddharth Seth | Zhijie Shen |
| [YARN-716](https://issues.apache.org/jira/browse/YARN-716) | Make ApplicationID immutable |  Major | . | Siddharth Seth | Siddharth Seth |
| [YARN-701](https://issues.apache.org/jira/browse/YARN-701) | ApplicationTokens should be used irrespective of kerberos |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-694](https://issues.apache.org/jira/browse/YARN-694) | Start using NMTokens to authenticate all communication with NM |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-684](https://issues.apache.org/jira/browse/YARN-684) | ContainerManager.startContainer needs to only have ContainerTokenIdentifier instead of the whole Container |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-642](https://issues.apache.org/jira/browse/YARN-642) | Fix up /nodes REST API to have 1 param and be consistent with the Java API |  Major | api, resourcemanager | Sandy Ryza | Sandy Ryza |
| [YARN-635](https://issues.apache.org/jira/browse/YARN-635) | Rename YarnRemoteException to YarnException |  Major | . | Xuan Gong | Siddharth Seth |
| [YARN-633](https://issues.apache.org/jira/browse/YARN-633) | Change RMAdminProtocol api to throw IOException and YarnRemoteException |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-632](https://issues.apache.org/jira/browse/YARN-632) | Change ContainerManager api to throw IOException and YarnRemoteException |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-631](https://issues.apache.org/jira/browse/YARN-631) | Change ClientRMProtocol api to throw IOException and YarnRemoteException |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-630](https://issues.apache.org/jira/browse/YARN-630) | Change AMRMProtocol api to throw IOException and YarnRemoteException |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-629](https://issues.apache.org/jira/browse/YARN-629) | Make YarnRemoteException not be rooted at IOException |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-615](https://issues.apache.org/jira/browse/YARN-615) | ContainerLaunchContext.containerTokens should simply be called tokens |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-610](https://issues.apache.org/jira/browse/YARN-610) | ClientToken (ClientToAMToken) should not be set in the environment |  Blocker | . | Siddharth Seth | Omkar Vinit Joshi |
| [YARN-579](https://issues.apache.org/jira/browse/YARN-579) | Make ApplicationToken part of Container's token list to help RM-restart |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-571](https://issues.apache.org/jira/browse/YARN-571) | User should not be part of ContainerLaunchContext |  Major | . | Hitesh Shah | Omkar Vinit Joshi |
| [YARN-561](https://issues.apache.org/jira/browse/YARN-561) | Nodemanager should set some key information into the environment of every container that it launches. |  Major | . | Hitesh Shah | Xuan Gong |
| [YARN-553](https://issues.apache.org/jira/browse/YARN-553) | Have YarnClient generate a directly usable ApplicationSubmissionContext |  Minor | client | Harsh J | Karthik Kambatla |
| [YARN-536](https://issues.apache.org/jira/browse/YARN-536) | Remove ContainerStatus, ContainerState from Container api interface as they will not be called by the container object |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-530](https://issues.apache.org/jira/browse/YARN-530) | Define Service model strictly, implement AbstractService for robust subclassing, migrate yarn-common services |  Major | . | Steve Loughran | Steve Loughran |
| [YARN-440](https://issues.apache.org/jira/browse/YARN-440) | Flatten RegisterNodeManagerResponse |  Major | . | Siddharth Seth | Xuan Gong |
| [YARN-439](https://issues.apache.org/jira/browse/YARN-439) | Flatten NodeHeartbeatResponse |  Major | . | Siddharth Seth | Xuan Gong |
| [YARN-396](https://issues.apache.org/jira/browse/YARN-396) | Rationalize AllocateResponse in RM scheduler API |  Major | . | Bikas Saha | Zhijie Shen |
| [YARN-387](https://issues.apache.org/jira/browse/YARN-387) | Fix inconsistent protocol naming |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9763](https://issues.apache.org/jira/browse/HADOOP-9763) | Extends LightWeightGSet to support eviction of expired elements |  Major | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-9515](https://issues.apache.org/jira/browse/HADOOP-9515) | Add general interface for NFS and Mount |  Major | . | Brandon Li | Brandon Li |
| [HADOOP-9509](https://issues.apache.org/jira/browse/HADOOP-9509) | Implement ONCRPC and XDR |  Major | . | Brandon Li | Brandon Li |
| [HADOOP-9283](https://issues.apache.org/jira/browse/HADOOP-9283) | Add support for running the Hadoop client on AIX |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-9209](https://issues.apache.org/jira/browse/HADOOP-9209) | Add shell command to dump file checksums |  Major | fs, tools | Todd Lipcon | Todd Lipcon |
| [HADOOP-8562](https://issues.apache.org/jira/browse/HADOOP-8562) | Enhancements to support Hadoop on Windows Server and Windows Azure environments |  Major | . | Bikas Saha | Bikas Saha |
| [HADOOP-8040](https://issues.apache.org/jira/browse/HADOOP-8040) | Add symlink support to FileSystem |  Major | fs | Eli Collins | Andrew Wang |
| [HDFS-4249](https://issues.apache.org/jira/browse/HDFS-4249) | Add status NameNode startup to webUI |  Major | namenode | Suresh Srinivas | Chris Nauroth |
| [HDFS-4124](https://issues.apache.org/jira/browse/HDFS-4124) | Refactor INodeDirectory#getExistingPathINodes() to enable returning more than INode array |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-3601](https://issues.apache.org/jira/browse/HDFS-3601) | Implementation of ReplicaPlacementPolicyNodeGroup to support 4-layer network topology |  Major | namenode | Junping Du | Junping Du |
| [HDFS-3495](https://issues.apache.org/jira/browse/HDFS-3495) | Update Balancer to support new NetworkTopology with NodeGroup |  Major | balancer & mover | Junping Du | Junping Du |
| [HDFS-3278](https://issues.apache.org/jira/browse/HDFS-3278) | Umbrella Jira for HDFS-HA Phase 2 |  Major | . | Sanjay Radia | Todd Lipcon |
| [HDFS-2576](https://issues.apache.org/jira/browse/HDFS-2576) | Namenode should have a favored nodes hint to enable clients to have control over block placement. |  Major | hdfs-client, namenode | Pritam Damania | Devaraj Das |
| [HDFS-1804](https://issues.apache.org/jira/browse/HDFS-1804) | Add a new block-volume device choosing policy that looks at free space |  Minor | datanode | Harsh J | Aaron T. Myers |
| [MAPREDUCE-5298](https://issues.apache.org/jira/browse/MAPREDUCE-5298) | Move MapReduce services to YARN-117 stricter lifecycle |  Major | applicationmaster | Steve Loughran | Steve Loughran |
| [MAPREDUCE-5129](https://issues.apache.org/jira/browse/MAPREDUCE-5129) | Add tag info to JH files |  Minor | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-326](https://issues.apache.org/jira/browse/YARN-326) | Add multi-resource scheduling to the fair scheduler |  Major | scheduler | Sandy Ryza | Sandy Ryza |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9845](https://issues.apache.org/jira/browse/HADOOP-9845) | Update protobuf to 2.5 from 2.4.x |  Blocker | performance | stack | Alejandro Abdelnur |
| [HADOOP-9792](https://issues.apache.org/jira/browse/HADOOP-9792) | Retry the methods that are tagged @AtMostOnce along with @Idempotent |  Major | ipc | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9770](https://issues.apache.org/jira/browse/HADOOP-9770) | Make RetryCache#state non volatile |  Minor | util | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9762](https://issues.apache.org/jira/browse/HADOOP-9762) | RetryCache utility for implementing RPC retries |  Major | util | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9760](https://issues.apache.org/jira/browse/HADOOP-9760) | Move GSet and LightWeightGSet to hadoop-common |  Major | util | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9756](https://issues.apache.org/jira/browse/HADOOP-9756) | Additional cleanup RPC code |  Minor | ipc | Junping Du | Junping Du |
| [HADOOP-9754](https://issues.apache.org/jira/browse/HADOOP-9754) | Clean up RPC code |  Minor | ipc | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-9751](https://issues.apache.org/jira/browse/HADOOP-9751) | Add clientId and retryCount to RpcResponseHeaderProto |  Major | ipc | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-9734](https://issues.apache.org/jira/browse/HADOOP-9734) | Common protobuf definitions for GetUserMappingsProtocol, RefreshAuthorizationPolicyProtocol and RefreshUserMappingsProtocol |  Minor | ipc | Jason Lowe | Jason Lowe |
| [HADOOP-9717](https://issues.apache.org/jira/browse/HADOOP-9717) | Add retry attempt count to the RPC requests |  Major | ipc | Suresh Srinivas | Jing Zhao |
| [HADOOP-9716](https://issues.apache.org/jira/browse/HADOOP-9716) | Move the Rpc request call ID generation to client side InvocationHandler |  Major | ipc | Suresh Srinivas | Tsz Wo Nicholas Sze |
| [HADOOP-9691](https://issues.apache.org/jira/browse/HADOOP-9691) | RPC clients can generate call ID using AtomicInteger instead of synchronizing on the Client instance. |  Minor | ipc | Chris Nauroth | Chris Nauroth |
| [HADOOP-9688](https://issues.apache.org/jira/browse/HADOOP-9688) | Add globally unique Client ID to RPC requests |  Blocker | ipc | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9676](https://issues.apache.org/jira/browse/HADOOP-9676) | make maximum RPC buffer size configurable |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-9673](https://issues.apache.org/jira/browse/HADOOP-9673) | NetworkTopology: when a node can't be added, print out its location for diagnostic purposes |  Trivial | net | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-9661](https://issues.apache.org/jira/browse/HADOOP-9661) | Allow metrics sources to be extended |  Major | metrics | Sandy Ryza | Sandy Ryza |
| [HADOOP-9625](https://issues.apache.org/jira/browse/HADOOP-9625) | HADOOP\_OPTS not picked up by hadoop command |  Minor | bin, conf | Paul Han | Paul Han |
| [HADOOP-9605](https://issues.apache.org/jira/browse/HADOOP-9605) | Update junit dependency |  Major | build | Timothy St. Clair |  |
| [HADOOP-9604](https://issues.apache.org/jira/browse/HADOOP-9604) | Wrong Javadoc of FSDataOutputStream |  Minor | fs | Jingguo Yao | Jingguo Yao |
| [HADOOP-9560](https://issues.apache.org/jira/browse/HADOOP-9560) | metrics2#JvmMetrics should have max memory size of JVM |  Minor | metrics | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-9523](https://issues.apache.org/jira/browse/HADOOP-9523) | Provide a generic IBM java vendor flag in PlatformName.java to support non-Sun JREs |  Major | . | Tian Hong Wang | Tian Hong Wang |
| [HADOOP-9511](https://issues.apache.org/jira/browse/HADOOP-9511) | Adding support for additional input streams (FSDataInputStream and RandomAccessFile) in SecureIOUtils. |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [HADOOP-9503](https://issues.apache.org/jira/browse/HADOOP-9503) | Remove sleep between IPC client connect timeouts |  Minor | ipc | Varun Sharma | Varun Sharma |
| [HADOOP-9483](https://issues.apache.org/jira/browse/HADOOP-9483) | winutils support for readlink command |  Major | util | Chris Nauroth | Arpit Agarwal |
| [HADOOP-9450](https://issues.apache.org/jira/browse/HADOOP-9450) | HADOOP\_USER\_CLASSPATH\_FIRST is not honored; CLASSPATH is PREpended instead of APpended |  Major | scripts | Mitch Wyle | Harsh J |
| [HADOOP-9401](https://issues.apache.org/jira/browse/HADOOP-9401) | CodecPool: Add counters for number of (de)compressors leased out |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9379](https://issues.apache.org/jira/browse/HADOOP-9379) | capture the ulimit info after printing the log to the console |  Trivial | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-9352](https://issues.apache.org/jira/browse/HADOOP-9352) | Expose UGI.setLoginUser for tests |  Major | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-9343](https://issues.apache.org/jira/browse/HADOOP-9343) | Allow additional exceptions through the RPC layer |  Major | . | Siddharth Seth | Siddharth Seth |
| [HADOOP-9338](https://issues.apache.org/jira/browse/HADOOP-9338) | FsShell Copy Commands Should Optionally Preserve File Attributes |  Major | fs | Nick White | Nick White |
| [HADOOP-9336](https://issues.apache.org/jira/browse/HADOOP-9336) | Allow UGI of current connection to be queried |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9334](https://issues.apache.org/jira/browse/HADOOP-9334) | Update netty version |  Minor | build | Nicolas Liochon | Nicolas Liochon |
| [HADOOP-9322](https://issues.apache.org/jira/browse/HADOOP-9322) | LdapGroupsMapping doesn't seem to set a timeout for its directory search |  Minor | security | Harsh J | Harsh J |
| [HADOOP-9318](https://issues.apache.org/jira/browse/HADOOP-9318) | when exiting on a signal, print the signal name first |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-9279](https://issues.apache.org/jira/browse/HADOOP-9279) | Document the need to build hadoop-maven-plugins for eclipse and separate project builds |  Major | build, documentation | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-9253](https://issues.apache.org/jira/browse/HADOOP-9253) | Capture ulimit info in the logs at service start time |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-9227](https://issues.apache.org/jira/browse/HADOOP-9227) | FileSystemContractBaseTest doesn't test filesystem's mkdir/isDirectory() logic rigorously enough |  Trivial | fs | Steve Loughran | Steve Loughran |
| [HADOOP-9164](https://issues.apache.org/jira/browse/HADOOP-9164) | Print paths of loaded native libraries in NativeLibraryChecker |  Minor | native | Binglin Chang | Binglin Chang |
| [HADOOP-9117](https://issues.apache.org/jira/browse/HADOOP-9117) | replace protoc ant plugin exec with a maven plugin |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8924](https://issues.apache.org/jira/browse/HADOOP-8924) | Add maven plugin alternative to shell script to save package-info.java |  Major | build | Chris Nauroth | Chris Nauroth |
| [HADOOP-8711](https://issues.apache.org/jira/browse/HADOOP-8711) | provide an option for IPC server users to avoid printing stack information for certain exceptions |  Major | ipc | Brandon Li | Brandon Li |
| [HADOOP-8608](https://issues.apache.org/jira/browse/HADOOP-8608) | Add Configuration API for parsing time durations |  Minor | conf | Todd Lipcon | Chris Douglas |
| [HADOOP-8462](https://issues.apache.org/jira/browse/HADOOP-8462) | Native-code implementation of bzip2 codec |  Major | io | Govind Kamat | Govind Kamat |
| [HADOOP-8415](https://issues.apache.org/jira/browse/HADOOP-8415) | getDouble() and setDouble() in org.apache.hadoop.conf.Configuration |  Minor | conf | Jan van der Lugt | Jan van der Lugt |
| [HDFS-5027](https://issues.apache.org/jira/browse/HDFS-5027) | On startup, DN should scan volumes in parallel |  Major | datanode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-5020](https://issues.apache.org/jira/browse/HDFS-5020) | Make DatanodeProtocol#blockReceivedAndDeleted idempotent |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-5010](https://issues.apache.org/jira/browse/HDFS-5010) | Reduce the frequency of getCurrentUser() calls from namenode |  Major | namenode, performance | Kihwal Lee | Kihwal Lee |
| [HDFS-5008](https://issues.apache.org/jira/browse/HDFS-5008) | Make ClientProtocol#abandonBlock() idempotent |  Major | namenode | Suresh Srinivas | Jing Zhao |
| [HDFS-5007](https://issues.apache.org/jira/browse/HDFS-5007) | Replace hard-coded property keys with DFSConfigKeys fields |  Minor | . | Kousuke Saruta | Kousuke Saruta |
| [HDFS-4996](https://issues.apache.org/jira/browse/HDFS-4996) | ClientProtocol#metaSave can be made idempotent by overwriting the output file instead of appending to it |  Minor | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-4992](https://issues.apache.org/jira/browse/HDFS-4992) | Make balancer's thread count configurable |  Major | balancer & mover | Max Lapan | Max Lapan |
| [HDFS-4978](https://issues.apache.org/jira/browse/HDFS-4978) | Make disallowSnapshot idempotent |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-4942](https://issues.apache.org/jira/browse/HDFS-4942) | Add retry cache support in Namenode |  Major | ha, namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4932](https://issues.apache.org/jira/browse/HDFS-4932) | Avoid a wide line on the name node webUI if we have more Journal nodes |  Minor | ha, namenode | Fengdong Yu | Fengdong Yu |
| [HDFS-4914](https://issues.apache.org/jira/browse/HDFS-4914) | When possible, Use DFSClient.Conf instead of Configuration |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4912](https://issues.apache.org/jira/browse/HDFS-4912) | Cleanup FSNamesystem#startFileInternal |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4903](https://issues.apache.org/jira/browse/HDFS-4903) | Print trash configuration and trash emptier state in namenode log |  Minor | namenode | Suresh Srinivas | Arpit Agarwal |
| [HDFS-4848](https://issues.apache.org/jira/browse/HDFS-4848) | copyFromLocal and renaming a file to ".snapshot" should output that ".snapshot" is a reserved name |  Minor | snapshots | Stephen Chu | Jing Zhao |
| [HDFS-4804](https://issues.apache.org/jira/browse/HDFS-4804) | WARN when users set the block balanced preference percent below 0.5 or above 1.0 |  Minor | . | Stephen Chu | Stephen Chu |
| [HDFS-4787](https://issues.apache.org/jira/browse/HDFS-4787) | Create a new HdfsConfiguration before each TestDFSClientRetries testcases |  Major | . | Tian Hong Wang | Tian Hong Wang |
| [HDFS-4772](https://issues.apache.org/jira/browse/HDFS-4772) | Add number of children in HdfsFileStatus |  Minor | namenode | Brandon Li | Brandon Li |
| [HDFS-4721](https://issues.apache.org/jira/browse/HDFS-4721) | Speed up lease/block recovery when DN fails and a block goes into recovery |  Major | namenode | Varun Sharma | Varun Sharma |
| [HDFS-4698](https://issues.apache.org/jira/browse/HDFS-4698) | provide client-side metrics for remote reads, local reads, and short-circuit reads |  Minor | hdfs-client | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4679](https://issues.apache.org/jira/browse/HDFS-4679) | Namenode operation checks should be done in a consistent manner |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4645](https://issues.apache.org/jira/browse/HDFS-4645) | Move from randomly generated block ID to sequentially generated block ID |  Major | namenode | Suresh Srinivas | Arpit Agarwal |
| [HDFS-4635](https://issues.apache.org/jira/browse/HDFS-4635) | Move BlockManager#computeCapacity to LightWeightGSet |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4569](https://issues.apache.org/jira/browse/HDFS-4569) | Small image transfer related cleanups. |  Trivial | . | Andrew Wang | Andrew Wang |
| [HDFS-4565](https://issues.apache.org/jira/browse/HDFS-4565) | use DFSUtil.getSpnegoKeytabKey() to get the spnego keytab key in secondary namenode and namenode http server |  Minor | security | Arpit Gupta | Arpit Gupta |
| [HDFS-4521](https://issues.apache.org/jira/browse/HDFS-4521) | invalid network topologies should not be cached |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4465](https://issues.apache.org/jira/browse/HDFS-4465) | Optimize datanode ReplicasMap and ReplicaInfo |  Major | datanode | Suresh Srinivas | Aaron T. Myers |
| [HDFS-4461](https://issues.apache.org/jira/browse/HDFS-4461) | DirectoryScanner: volume path prefix takes up memory for every block that is scanned |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4304](https://issues.apache.org/jira/browse/HDFS-4304) | Make FSEditLogOp.MAX\_OP\_SIZE configurable |  Major | namenode | Todd Lipcon | Colin Patrick McCabe |
| [HDFS-4246](https://issues.apache.org/jira/browse/HDFS-4246) | The exclude node list should be more forgiving, for each output stream |  Minor | hdfs-client | Harsh J | Harsh J |
| [HDFS-4234](https://issues.apache.org/jira/browse/HDFS-4234) | Use the generic code for choosing datanode in Balancer |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4215](https://issues.apache.org/jira/browse/HDFS-4215) | Improvements on INode and image loading |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4206](https://issues.apache.org/jira/browse/HDFS-4206) | Change the fields in INode and its subclasses to private |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4152](https://issues.apache.org/jira/browse/HDFS-4152) | Add a new class for the parameter in INode.collectSubtreeBlocksAndClear(..) |  Minor | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-4151](https://issues.apache.org/jira/browse/HDFS-4151) | Passing INodesInPath instead of INode[] in FSDirectory |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4053](https://issues.apache.org/jira/browse/HDFS-4053) | Increase the default block size |  Major | . | Eli Collins | Eli Collins |
| [HDFS-3940](https://issues.apache.org/jira/browse/HDFS-3940) | Add Gset#clear method and clear the block map when namenode is shutdown |  Minor | . | Eli Collins | Suresh Srinivas |
| [HDFS-3880](https://issues.apache.org/jira/browse/HDFS-3880) | Use Builder to get RPC server in HDFS |  Minor | datanode, ha, namenode, security | Brandon Li | Brandon Li |
| [HDFS-3817](https://issues.apache.org/jira/browse/HDFS-3817) | avoid printing stack information for SafeModeException |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-3498](https://issues.apache.org/jira/browse/HDFS-3498) | Make Replica Removal Policy pluggable and ReplicaPlacementPolicyDefault extensible for reusing code in subclass |  Major | namenode | Junping Du | Junping Du |
| [HDFS-3163](https://issues.apache.org/jira/browse/HDFS-3163) | TestHDFSCLI.testAll fails if the user name is not all lowercase |  Trivial | test | Brandon Li | Brandon Li |
| [HDFS-2857](https://issues.apache.org/jira/browse/HDFS-2857) | Cleanup BlockInfo class |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-2572](https://issues.apache.org/jira/browse/HDFS-2572) | Unnecessary double-check in DN#getHostName |  Trivial | datanode | Harsh J | Harsh J |
| [HDFS-2042](https://issues.apache.org/jira/browse/HDFS-2042) | Require c99 when building libhdfs |  Minor | libhdfs | Eli Collins |  |
| [HDFS-347](https://issues.apache.org/jira/browse/HDFS-347) | DFS read performance suboptimal when client co-located on nodes with data |  Major | datanode, hdfs-client, performance | George Porter | Colin Patrick McCabe |
| [MAPREDUCE-5398](https://issues.apache.org/jira/browse/MAPREDUCE-5398) | MR changes for YARN-513 |  Major | . | Bikas Saha | Jian He |
| [MAPREDUCE-5352](https://issues.apache.org/jira/browse/MAPREDUCE-5352) | Optimize node local splits generated by CombineFileInputFormat |  Major | . | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-5283](https://issues.apache.org/jira/browse/MAPREDUCE-5283) | Over 10 different tests have near identical implementations of AppContext |  Major | applicationmaster, test | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5268](https://issues.apache.org/jira/browse/MAPREDUCE-5268) | Improve history server startup performance |  Major | jobhistoryserver | Jason Lowe | Karthik Kambatla |
| [MAPREDUCE-5246](https://issues.apache.org/jira/browse/MAPREDUCE-5246) | Adding application type to submission context |  Major | . | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-5171](https://issues.apache.org/jira/browse/MAPREDUCE-5171) | Expose blacklisted nodes from the MR AM REST API |  Major | applicationmaster | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5128](https://issues.apache.org/jira/browse/MAPREDUCE-5128) | mapred-default.xml is missing a bunch of history server configs |  Major | documentation, jobhistoryserver | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5079](https://issues.apache.org/jira/browse/MAPREDUCE-5079) | Recovery should restore task state from job history info directly |  Critical | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5069](https://issues.apache.org/jira/browse/MAPREDUCE-5069) | add concrete common implementations of CombineFileInputFormat |  Minor | mrv1, mrv2 | Sangjin Lee | Sangjin Lee |
| [MAPREDUCE-5033](https://issues.apache.org/jira/browse/MAPREDUCE-5033) | mapred shell script should respect usage flags (--help -help -h) |  Minor | . | Andrew Wang | Andrew Wang |
| [MAPREDUCE-4990](https://issues.apache.org/jira/browse/MAPREDUCE-4990) | Construct debug strings conditionally in ShuffleHandler.Shuffle#sendMapOutput() |  Trivial | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4989](https://issues.apache.org/jira/browse/MAPREDUCE-4989) | JSONify DataTables input data for Attempts page |  Major | jobhistoryserver, mr-am | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4974](https://issues.apache.org/jira/browse/MAPREDUCE-4974) | Optimising the LineRecordReader initialize() method |  Major | mrv1, mrv2, performance | Arun A K | Gelesh |
| [MAPREDUCE-4846](https://issues.apache.org/jira/browse/MAPREDUCE-4846) | Some JobQueueInfo methods are public in MR1 but protected in MR2 |  Major | client | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-3787](https://issues.apache.org/jira/browse/MAPREDUCE-3787) | [Gridmix] Improve STRESS mode |  Major | contrib/gridmix | Amar Kamat | Amar Kamat |
| [MAPREDUCE-3533](https://issues.apache.org/jira/browse/MAPREDUCE-3533) | have the service interface extend Closeable and use close() as its shutdown operation |  Minor | mrv2 | Steve Loughran |  |
| [YARN-1045](https://issues.apache.org/jira/browse/YARN-1045) | Improve toString implementation for PBImpls |  Major | . | Siddharth Seth | Jian He |
| [YARN-883](https://issues.apache.org/jira/browse/YARN-883) | Expose Fair Scheduler-specific queue metrics |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-865](https://issues.apache.org/jira/browse/YARN-865) | RM webservices can't query based on application Types |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-803](https://issues.apache.org/jira/browse/YARN-803) | factor out scheduler config validation from the ResourceManager to each scheduler implementation |  Major | resourcemanager, scheduler | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-789](https://issues.apache.org/jira/browse/YARN-789) | Enable zero capabilities resource requests in fair scheduler |  Major | scheduler | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-782](https://issues.apache.org/jira/browse/YARN-782) | vcores-pcores ratio functions differently from vmem-pmem ratio in misleading way |  Critical | nodemanager | Sandy Ryza | Sandy Ryza |
| [YARN-752](https://issues.apache.org/jira/browse/YARN-752) | In AMRMClient, automatically add corresponding rack requests for requested nodes |  Major | api, applications | Sandy Ryza | Sandy Ryza |
| [YARN-736](https://issues.apache.org/jira/browse/YARN-736) | Add a multi-resource fair sharing metric |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-600](https://issues.apache.org/jira/browse/YARN-600) | Hook up cgroups CPU settings to the number of virtual cores allocated |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-598](https://issues.apache.org/jira/browse/YARN-598) | Add virtual cores to queue metrics |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-590](https://issues.apache.org/jira/browse/YARN-590) | Add an optional mesage to RegisterNodeManagerResponse as to why NM is being asked to resync or shutdown |  Major | . | Vinod Kumar Vavilapalli | Mayank Bansal |
| [YARN-538](https://issues.apache.org/jira/browse/YARN-538) | RM address DNS lookup can cause unnecessary slowness on every JHS page load |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-525](https://issues.apache.org/jira/browse/YARN-525) | make CS node-locality-delay refreshable |  Major | capacityscheduler | Thomas Graves | Thomas Graves |
| [YARN-518](https://issues.apache.org/jira/browse/YARN-518) | Fair Scheduler's document link could be added to the hadoop 2.x main doc page |  Major | documentation | Dapeng Sun | Sandy Ryza |
| [YARN-447](https://issues.apache.org/jira/browse/YARN-447) | applicationComparator improvement for CS |  Minor | scheduler | Nemon Lou | Nemon Lou |
| [YARN-406](https://issues.apache.org/jira/browse/YARN-406) | TestRackResolver fails when local network resolves "host1" to a valid host |  Minor | . | Hitesh Shah | Hitesh Shah |
| [YARN-391](https://issues.apache.org/jira/browse/YARN-391) | detabify LCEResourcesHandler classes |  Trivial | nodemanager | Steve Loughran | Steve Loughran |
| [YARN-385](https://issues.apache.org/jira/browse/YARN-385) | ResourceRequestPBImpl's toString() is missing location and # containers |  Major | api | Sandy Ryza | Sandy Ryza |
| [YARN-382](https://issues.apache.org/jira/browse/YARN-382) | SchedulerUtils improve way normalizeRequest sets the resource capabilities |  Major | scheduler | Thomas Graves | Zhijie Shen |
| [YARN-381](https://issues.apache.org/jira/browse/YARN-381) | Improve FS docs |  Minor | documentation | Eli Collins | Sandy Ryza |
| [YARN-347](https://issues.apache.org/jira/browse/YARN-347) | YARN CLI should show CPU info besides memory info in node status |  Major | client | Junping Du | Junping Du |
| [YARN-297](https://issues.apache.org/jira/browse/YARN-297) | Improve hashCode implementations for PB records |  Major | . | Arun C Murthy | Xuan Gong |
| [YARN-249](https://issues.apache.org/jira/browse/YARN-249) | Capacity Scheduler web page should show list of active users per queue like it used to (in 1.x) |  Major | capacityscheduler | Ravi Prakash | Ravi Prakash |
| [YARN-237](https://issues.apache.org/jira/browse/YARN-237) | Refreshing the RM page forgets how many rows I had in my Datatables |  Major | resourcemanager | Ravi Prakash | Jian He |
| [YARN-198](https://issues.apache.org/jira/browse/YARN-198) | If we are navigating to Nodemanager UI from Resourcemanager,then there is not link to navigate back to Resource manager |  Minor | nodemanager | Ramgopal N | Jian He |
| [YARN-117](https://issues.apache.org/jira/browse/YARN-117) | Enhance YARN service model |  Major | . | Steve Loughran | Steve Loughran |
| [YARN-84](https://issues.apache.org/jira/browse/YARN-84) | Use Builder to get RPC server in YARN |  Minor | . | Brandon Li | Brandon Li |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9872](https://issues.apache.org/jira/browse/HADOOP-9872) | Improve protoc version handling and detection |  Blocker | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-9850](https://issues.apache.org/jira/browse/HADOOP-9850) | RPC kerberos errors don't trigger relogin |  Blocker | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9816](https://issues.apache.org/jira/browse/HADOOP-9816) | RPC Sasl QOP is broken |  Blocker | ipc, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-9787](https://issues.apache.org/jira/browse/HADOOP-9787) | ShutdownHelper util to shutdown threads and threadpools |  Major | util | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9786](https://issues.apache.org/jira/browse/HADOOP-9786) | RetryInvocationHandler#isRpcInvocation should support ProtocolTranslator |  Major | . | Jing Zhao | Jing Zhao |
| [HADOOP-9773](https://issues.apache.org/jira/browse/HADOOP-9773) | TestLightWeightCache fails |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-9759](https://issues.apache.org/jira/browse/HADOOP-9759) | Add support for NativeCodeLoader#getLibraryName on Windows |  Critical | . | Chuan Liu | Chuan Liu |
| [HADOOP-9738](https://issues.apache.org/jira/browse/HADOOP-9738) | TestDistCh fails |  Major | tools | Kihwal Lee | Jing Zhao |
| [HADOOP-9707](https://issues.apache.org/jira/browse/HADOOP-9707) | Fix register lists for crc32c inline assembly |  Minor | util | Todd Lipcon | Todd Lipcon |
| [HADOOP-9701](https://issues.apache.org/jira/browse/HADOOP-9701) | mvn site ambiguous links in hadoop-common |  Minor | documentation | Steve Loughran | Karthik Kambatla |
| [HADOOP-9681](https://issues.apache.org/jira/browse/HADOOP-9681) | FileUtil.unTarUsingJava() should close the InputStream upon finishing |  Minor | . | Chuan Liu | Chuan Liu |
| [HADOOP-9678](https://issues.apache.org/jira/browse/HADOOP-9678) | TestRPC#testStopsAllThreads intermittently fails on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [HADOOP-9665](https://issues.apache.org/jira/browse/HADOOP-9665) | BlockDecompressorStream#decompress will throw EOFException instead of return -1 when EOF |  Critical | . | Zhijie Shen | Zhijie Shen |
| [HADOOP-9656](https://issues.apache.org/jira/browse/HADOOP-9656) | Gridmix unit tests fail on Windows and Linux |  Minor | test, tools | Chuan Liu | Chuan Liu |
| [HADOOP-9643](https://issues.apache.org/jira/browse/HADOOP-9643) | org.apache.hadoop.security.SecurityUtil calls toUpperCase(Locale.getDefault()) as well as toLowerCase(Locale.getDefault()) on hadoop.security.authentication value. |  Minor | security | Mark Miller | Mark Miller |
| [HADOOP-9638](https://issues.apache.org/jira/browse/HADOOP-9638) | parallel test changes caused invalid test path for several HDFS tests on Windows |  Major | test | Chris Nauroth | Andrey Klochkov |
| [HADOOP-9637](https://issues.apache.org/jira/browse/HADOOP-9637) | Adding Native Fstat for Windows as needed by YARN |  Major | . | Chuan Liu | Chuan Liu |
| [HADOOP-9632](https://issues.apache.org/jira/browse/HADOOP-9632) | TestShellCommandFencer will fail if there is a 'host' machine in the network |  Minor | . | Chuan Liu | Chuan Liu |
| [HADOOP-9607](https://issues.apache.org/jira/browse/HADOOP-9607) | Fixes in Javadoc build |  Minor | documentation | Timothy St. Clair |  |
| [HADOOP-9599](https://issues.apache.org/jira/browse/HADOOP-9599) | hadoop-config.cmd doesn't set JAVA\_LIBRARY\_PATH correctly |  Major | . | Mostafa Elhemali | Mostafa Elhemali |
| [HADOOP-9593](https://issues.apache.org/jira/browse/HADOOP-9593) | stack trace printed at ERROR for all yarn clients without hadoop.home set |  Major | util | Steve Loughran | Steve Loughran |
| [HADOOP-9581](https://issues.apache.org/jira/browse/HADOOP-9581) | hadoop --config non-existent directory should result in error |  Major | scripts | Ashwin Shankar | Ashwin Shankar |
| [HADOOP-9574](https://issues.apache.org/jira/browse/HADOOP-9574) | Add new methods in AbstractDelegationTokenSecretManager for restoring RMDelegationTokens on RMRestart |  Major | . | Jian He | Jian He |
| [HADOOP-9566](https://issues.apache.org/jira/browse/HADOOP-9566) | Performing direct read using libhdfs sometimes raises SIGPIPE (which in turn throws SIGABRT) causing client crashes |  Major | native | Lenni Kuff | Colin Patrick McCabe |
| [HADOOP-9563](https://issues.apache.org/jira/browse/HADOOP-9563) | Fix incompatibility introduced by HADOOP-9523 |  Major | util | Kihwal Lee | Tian Hong Wang |
| [HADOOP-9556](https://issues.apache.org/jira/browse/HADOOP-9556) | disable HA tests on Windows that fail due to ZooKeeper client connection management bug |  Major | ha, test | Chris Nauroth | Chris Nauroth |
| [HADOOP-9553](https://issues.apache.org/jira/browse/HADOOP-9553) | TestAuthenticationToken fails on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9550](https://issues.apache.org/jira/browse/HADOOP-9550) | Remove aspectj dependency |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9549](https://issues.apache.org/jira/browse/HADOOP-9549) | WebHdfsFileSystem hangs on close() |  Blocker | security | Kihwal Lee | Daryn Sharp |
| [HADOOP-9532](https://issues.apache.org/jira/browse/HADOOP-9532) | HADOOP\_CLIENT\_OPTS is appended twice by Windows cmd scripts |  Minor | bin | Chris Nauroth | Chris Nauroth |
| [HADOOP-9527](https://issues.apache.org/jira/browse/HADOOP-9527) | Add symlink support to LocalFileSystem on Windows |  Major | fs, test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9526](https://issues.apache.org/jira/browse/HADOOP-9526) | TestShellCommandFencer and TestShell fail on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9524](https://issues.apache.org/jira/browse/HADOOP-9524) | Fix ShellCommandFencer to work on Windows |  Major | ha | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9517](https://issues.apache.org/jira/browse/HADOOP-9517) | Document Hadoop Compatibility |  Blocker | documentation | Arun C Murthy | Karthik Kambatla |
| [HADOOP-9507](https://issues.apache.org/jira/browse/HADOOP-9507) | LocalFileSystem rename() is broken in some cases when destination exists |  Minor | fs | Mostafa Elhemali | Chris Nauroth |
| [HADOOP-9504](https://issues.apache.org/jira/browse/HADOOP-9504) | MetricsDynamicMBeanBase has concurrency issues in createMBeanInfo |  Critical | metrics | Liang Xie | Liang Xie |
| [HADOOP-9500](https://issues.apache.org/jira/browse/HADOOP-9500) | TestUserGroupInformation#testGetServerSideGroups fails on Windows due to failure to find winutils.exe |  Major | test | Chris Nauroth | Chris Nauroth |
| [HADOOP-9496](https://issues.apache.org/jira/browse/HADOOP-9496) | Bad merge of HADOOP-9450 on branch-2 breaks all bin/hadoop calls that need HADOOP\_CLASSPATH |  Critical | bin | Gopal V | Harsh J |
| [HADOOP-9490](https://issues.apache.org/jira/browse/HADOOP-9490) | LocalFileSystem#reportChecksumFailure not closing the checksum file handle before rename |  Major | fs | Ivan Mitic | Ivan Mitic |
| [HADOOP-9488](https://issues.apache.org/jira/browse/HADOOP-9488) | FileUtil#createJarWithClassPath only substitutes environment variables from current process environment/does not support overriding when launching new process |  Major | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-9486](https://issues.apache.org/jira/browse/HADOOP-9486) | Promote Windows and Shell related utils from YARN to Hadoop Common |  Major | . | Vinod Kumar Vavilapalli | Chris Nauroth |
| [HADOOP-9485](https://issues.apache.org/jira/browse/HADOOP-9485) | No default value in the code for hadoop.rpc.socket.factory.class.default |  Minor | net | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-9481](https://issues.apache.org/jira/browse/HADOOP-9481) | Broken conditional logic with HADOOP\_SNAPPY\_LIBRARY |  Minor | . | Vadim Bondarev | Vadim Bondarev |
| [HADOOP-9473](https://issues.apache.org/jira/browse/HADOOP-9473) | typo in FileUtil copy() method |  Trivial | fs | Glen Mazza |  |
| [HADOOP-9469](https://issues.apache.org/jira/browse/HADOOP-9469) | mapreduce/yarn source jars not included in dist tarball |  Major | . | Thomas Graves | Robert Parker |
| [HADOOP-9459](https://issues.apache.org/jira/browse/HADOOP-9459) | ActiveStandbyElector can join election even before Service HEALTHY, and results in null data at ActiveBreadCrumb |  Critical | ha | Vinayakumar B | Vinayakumar B |
| [HADOOP-9455](https://issues.apache.org/jira/browse/HADOOP-9455) | HADOOP\_CLIENT\_OPTS appended twice causes JVM failures |  Minor | bin | Sangjin Lee | Chris Nauroth |
| [HADOOP-9451](https://issues.apache.org/jira/browse/HADOOP-9451) | Node with one topology layer should be handled as fault topology when NodeGroup layer is enabled |  Major | net | Junping Du | Junping Du |
| [HADOOP-9443](https://issues.apache.org/jira/browse/HADOOP-9443) | Port winutils static code analysis change to trunk |  Major | . | Chuan Liu | Chuan Liu |
| [HADOOP-9439](https://issues.apache.org/jira/browse/HADOOP-9439) | JniBasedUnixGroupsMapping: fix some crash bugs |  Minor | native | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-9437](https://issues.apache.org/jira/browse/HADOOP-9437) | TestNativeIO#testRenameTo fails on Windows due to assumption that POSIX errno is embedded in NativeIOException |  Major | test | Chris Nauroth | Chris Nauroth |
| [HADOOP-9430](https://issues.apache.org/jira/browse/HADOOP-9430) | TestSSLFactory fails on IBM JVM |  Major | security | Amir Sanjar |  |
| [HADOOP-9429](https://issues.apache.org/jira/browse/HADOOP-9429) | TestConfiguration fails with IBM JAVA |  Major | test | Amir Sanjar |  |
| [HADOOP-9413](https://issues.apache.org/jira/browse/HADOOP-9413) | Introduce common utils for File#setReadable/Writable/Executable and File#canRead/Write/Execute that work cross-platform |  Major | . | Ivan Mitic | Ivan Mitic |
| [HADOOP-9408](https://issues.apache.org/jira/browse/HADOOP-9408) | misleading description for net.topology.table.file.name property in core-default.xml |  Minor | conf | rajeshbabu | rajeshbabu |
| [HADOOP-9407](https://issues.apache.org/jira/browse/HADOOP-9407) | commons-daemon 1.0.3 dependency has bad group id causing build issues |  Major | build | Sangjin Lee | Sangjin Lee |
| [HADOOP-9397](https://issues.apache.org/jira/browse/HADOOP-9397) | Incremental dist tar build fails |  Major | build | Jason Lowe | Chris Nauroth |
| [HADOOP-9388](https://issues.apache.org/jira/browse/HADOOP-9388) | TestFsShellCopy fails on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [HADOOP-9387](https://issues.apache.org/jira/browse/HADOOP-9387) | TestDFVariations fails on Windows after the merge |  Minor | fs | Ivan Mitic | Ivan Mitic |
| [HADOOP-9376](https://issues.apache.org/jira/browse/HADOOP-9376) | TestProxyUserFromEnv fails on a Windows domain joined machine |  Major | . | Ivan Mitic | Ivan Mitic |
| [HADOOP-9373](https://issues.apache.org/jira/browse/HADOOP-9373) | Merge CHANGES.branch-trunk-win.txt to CHANGES.txt |  Minor | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9372](https://issues.apache.org/jira/browse/HADOOP-9372) | Fix bad timeout annotations on tests |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9369](https://issues.apache.org/jira/browse/HADOOP-9369) | DNS#reverseDns() can return hostname with . appended at the end |  Major | net | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9365](https://issues.apache.org/jira/browse/HADOOP-9365) | TestHAZKUtil fails on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [HADOOP-9364](https://issues.apache.org/jira/browse/HADOOP-9364) | PathData#expandAsGlob does not return correct results for absolute paths on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [HADOOP-9358](https://issues.apache.org/jira/browse/HADOOP-9358) | "Auth failed" log should include exception string |  Major | ipc, security | Todd Lipcon | Todd Lipcon |
| [HADOOP-9353](https://issues.apache.org/jira/browse/HADOOP-9353) | Activate native-win profile by default on Windows |  Major | build | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9349](https://issues.apache.org/jira/browse/HADOOP-9349) | Confusing output when running hadoop version from one hadoop installation when HADOOP\_HOME points to another |  Major | tools | Sandy Ryza | Sandy Ryza |
| [HADOOP-9342](https://issues.apache.org/jira/browse/HADOOP-9342) | Remove jline from distribution |  Major | build | Thomas Weise | Thomas Weise |
| [HADOOP-9339](https://issues.apache.org/jira/browse/HADOOP-9339) | IPC.Server incorrectly sets UGI auth type |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9337](https://issues.apache.org/jira/browse/HADOOP-9337) | org.apache.hadoop.fs.DF.getMount() does not work on Mac OS |  Major | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-9323](https://issues.apache.org/jira/browse/HADOOP-9323) | Typos in API documentation |  Minor | documentation, fs, io, record | Hao Zhong | Suresh Srinivas |
| [HADOOP-9307](https://issues.apache.org/jira/browse/HADOOP-9307) | BufferedFSInputStream.read returns wrong results after certain seeks |  Major | fs | Todd Lipcon | Todd Lipcon |
| [HADOOP-9305](https://issues.apache.org/jira/browse/HADOOP-9305) | Add support for running the Hadoop client on 64-bit AIX |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-9304](https://issues.apache.org/jira/browse/HADOOP-9304) | remove addition of avro genreated-sources dirs to build |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-9303](https://issues.apache.org/jira/browse/HADOOP-9303) | command manual dfsadmin missing entry for restoreFailedStorage option |  Major | . | Thomas Graves | Andy Isaacson |
| [HADOOP-9302](https://issues.apache.org/jira/browse/HADOOP-9302) | HDFS docs not linked from top level |  Major | documentation | Thomas Graves | Andy Isaacson |
| [HADOOP-9297](https://issues.apache.org/jira/browse/HADOOP-9297) | remove old record IO generation and tests |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-9294](https://issues.apache.org/jira/browse/HADOOP-9294) | GetGroupsTestBase fails on Windows |  Major | test | Chris Nauroth | Chris Nauroth |
| [HADOOP-9290](https://issues.apache.org/jira/browse/HADOOP-9290) | Some tests cannot load native library |  Major | build, native | Arpit Agarwal | Chris Nauroth |
| [HADOOP-9267](https://issues.apache.org/jira/browse/HADOOP-9267) | hadoop -help, -h, --help should show usage instructions |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-9264](https://issues.apache.org/jira/browse/HADOOP-9264) | port change to use Java untar API on Windows from branch-1-win to trunk |  Major | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-9246](https://issues.apache.org/jira/browse/HADOOP-9246) | Execution phase for hadoop-maven-plugin should be process-resources |  Major | build | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9245](https://issues.apache.org/jira/browse/HADOOP-9245) | mvn clean without running mvn install before fails |  Major | build | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9230](https://issues.apache.org/jira/browse/HADOOP-9230) | TestUniformSizeInputFormat fails intermittently |  Major | test | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9228](https://issues.apache.org/jira/browse/HADOOP-9228) | FileSystemContractTestBase never verifies that files are files |  Minor | fs | Steve Loughran | Steve Loughran |
| [HADOOP-9220](https://issues.apache.org/jira/browse/HADOOP-9220) | Unnecessary transition to standby in ActiveStandbyElector |  Critical | ha | Tom White | Tom White |
| [HADOOP-9211](https://issues.apache.org/jira/browse/HADOOP-9211) | HADOOP\_CLIENT\_OPTS default setting fixes max heap size at 128m, disregards HADOOP\_HEAPSIZE |  Major | conf | Sarah Weissman | Plamen Jeliazkov |
| [HADOOP-9154](https://issues.apache.org/jira/browse/HADOOP-9154) | SortedMapWritable#putAll() doesn't add key/value classes to the map |  Major | io | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9150](https://issues.apache.org/jira/browse/HADOOP-9150) | Unnecessary DNS resolution attempts for logical URIs |  Critical | fs/s3, ha, performance, viewfs | Todd Lipcon | Todd Lipcon |
| [HADOOP-9131](https://issues.apache.org/jira/browse/HADOOP-9131) | TestLocalFileSystem#testListStatusWithColons cannot run on Windows |  Major | test | Chris Nauroth | Chris Nauroth |
| [HADOOP-9125](https://issues.apache.org/jira/browse/HADOOP-9125) | LdapGroupsMapping threw CommunicationException after some idle time |  Major | security | Kai Zheng | Kai Zheng |
| [HADOOP-9043](https://issues.apache.org/jira/browse/HADOOP-9043) | disallow in winutils creating symlinks with forwards slashes |  Major | util | Chris Nauroth | Chris Nauroth |
| [HADOOP-8982](https://issues.apache.org/jira/browse/HADOOP-8982) | TestSocketIOWithTimeout fails on Windows |  Major | net | Chris Nauroth | Chris Nauroth |
| [HADOOP-8973](https://issues.apache.org/jira/browse/HADOOP-8973) | DiskChecker cannot reliably detect an inaccessible disk on Windows with NTFS ACLs |  Major | util | Chris Nauroth | Chris Nauroth |
| [HADOOP-8958](https://issues.apache.org/jira/browse/HADOOP-8958) | ViewFs:Non absolute mount name failures when running multiple tests on Windows |  Major | viewfs | Chris Nauroth | Chris Nauroth |
| [HADOOP-8957](https://issues.apache.org/jira/browse/HADOOP-8957) | AbstractFileSystem#IsValidName should be overridden for embedded file systems like ViewFs |  Major | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-8917](https://issues.apache.org/jira/browse/HADOOP-8917) | add LOCALE.US to toLowerCase in SecurityUtil.replacePattern |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-8569](https://issues.apache.org/jira/browse/HADOOP-8569) | CMakeLists.txt: define \_GNU\_SOURCE and \_LARGEFILE\_SOURCE |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8440](https://issues.apache.org/jira/browse/HADOOP-8440) | HarFileSystem.decodeHarURI fails for URIs whose host contains numbers |  Minor | fs | Ivan Mitic | Ivan Mitic |
| [HADOOP-7487](https://issues.apache.org/jira/browse/HADOOP-7487) | DF should throw a more reasonable exception when mount cannot be determined |  Major | fs | Todd Lipcon | Andrew Wang |
| [HADOOP-7391](https://issues.apache.org/jira/browse/HADOOP-7391) | Document Interface Classification from HADOOP-5073 |  Major | . | Sanjay Radia | Sanjay Radia |
| [HDFS-5024](https://issues.apache.org/jira/browse/HDFS-5024) | Make DatanodeProtocol#commitBlockSynchronization idempotent |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-5018](https://issues.apache.org/jira/browse/HDFS-5018) | Misspelled DFSConfigKeys#DFS\_NAMENODE\_STALE\_DATANODE\_INTERVAL\_DEFAULT in javadoc of DatanodeInfo#isStale() |  Minor | . | Ted Yu | Ted Yu |
| [HDFS-5016](https://issues.apache.org/jira/browse/HDFS-5016) | Deadlock in pipeline recovery causes Datanode to be marked dead |  Blocker | . | Devaraj Das | Suresh Srinivas |
| [HDFS-5005](https://issues.apache.org/jira/browse/HDFS-5005) | Move SnapshotException and SnapshotAccessControlException to o.a.h.hdfs.protocol |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-5003](https://issues.apache.org/jira/browse/HDFS-5003) | TestNNThroughputBenchmark failed caused by existing directories |  Minor | test | Xi Fang | Xi Fang |
| [HDFS-4999](https://issues.apache.org/jira/browse/HDFS-4999) | fix TestShortCircuitLocalRead on branch-2 |  Major | . | Kihwal Lee | Colin Patrick McCabe |
| [HDFS-4998](https://issues.apache.org/jira/browse/HDFS-4998) | TestUnderReplicatedBlocks fails intermittently |  Major | test | Kihwal Lee | Kihwal Lee |
| [HDFS-4982](https://issues.apache.org/jira/browse/HDFS-4982) | JournalNode should relogin from keytab before fetching logs from other JNs |  Major | journal-node, security | Todd Lipcon | Todd Lipcon |
| [HDFS-4980](https://issues.apache.org/jira/browse/HDFS-4980) | Incorrect logging.properties file for hadoop-httpfs |  Major | build | Mark Grover | Mark Grover |
| [HDFS-4969](https://issues.apache.org/jira/browse/HDFS-4969) | WebhdfsFileSystem expects non-standard WEBHDFS Json element |  Blocker | test, webhdfs | Robert Kanter | Robert Kanter |
| [HDFS-4954](https://issues.apache.org/jira/browse/HDFS-4954) | compile failure in branch-2: getFlushedOffset should catch or rethrow IOException |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-4951](https://issues.apache.org/jira/browse/HDFS-4951) | FsShell commands using secure httpfs throw exceptions due to missing TokenRenewer |  Major | security | Robert Kanter | Robert Kanter |
| [HDFS-4948](https://issues.apache.org/jira/browse/HDFS-4948) | mvn site for hadoop-hdfs-nfs fails |  Major | . | Robert Joseph Evans | Brandon Li |
| [HDFS-4944](https://issues.apache.org/jira/browse/HDFS-4944) | WebHDFS cannot create a file path containing characters that must be URI-encoded, such as space. |  Major | webhdfs | Chris Nauroth | Chris Nauroth |
| [HDFS-4943](https://issues.apache.org/jira/browse/HDFS-4943) | WebHdfsFileSystem does not work when original file path has encoded chars |  Minor | webhdfs | Jerry He | Jerry He |
| [HDFS-4927](https://issues.apache.org/jira/browse/HDFS-4927) | CreateEditsLog creates inodes with an invalid inode ID, which then cannot be loaded by a namenode. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4917](https://issues.apache.org/jira/browse/HDFS-4917) | Start-dfs.sh cannot pass the parameters correctly |  Major | datanode, namenode | Fengdong Yu | Fengdong Yu |
| [HDFS-4910](https://issues.apache.org/jira/browse/HDFS-4910) | TestPermission failed in branch-2 |  Major | . | Chuan Liu | Chuan Liu |
| [HDFS-4906](https://issues.apache.org/jira/browse/HDFS-4906) | HDFS Output streams should not accept writes after being closed |  Major | hdfs-client | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4902](https://issues.apache.org/jira/browse/HDFS-4902) | DFSClient.getSnapshotDiffReport should use string path rather than o.a.h.fs.Path |  Major | snapshots | Binglin Chang | Binglin Chang |
| [HDFS-4888](https://issues.apache.org/jira/browse/HDFS-4888) | Refactor and fix FSNamesystem.getTurnOffTip to sanity |  Major | . | Ravi Prakash | Ravi Prakash |
| [HDFS-4887](https://issues.apache.org/jira/browse/HDFS-4887) | TestNNThroughputBenchmark exits abruptly |  Blocker | benchmarks, test | Kihwal Lee | Kihwal Lee |
| [HDFS-4883](https://issues.apache.org/jira/browse/HDFS-4883) | complete() should verify fileId |  Major | namenode | Konstantin Shvachko | Tao Luo |
| [HDFS-4880](https://issues.apache.org/jira/browse/HDFS-4880) | Diagnostic logging while loading name/edits files |  Major | namenode | Arpit Agarwal | Suresh Srinivas |
| [HDFS-4878](https://issues.apache.org/jira/browse/HDFS-4878) | On Remove Block, Block is not Removed from neededReplications queue |  Major | namenode | Tao Luo | Tao Luo |
| [HDFS-4877](https://issues.apache.org/jira/browse/HDFS-4877) | Snapshot: fix the scenario where a directory is renamed under its prior descendant |  Blocker | snapshots | Jing Zhao | Jing Zhao |
| [HDFS-4873](https://issues.apache.org/jira/browse/HDFS-4873) | callGetBlockLocations returns incorrect number of blocks for snapshotted files |  Major | snapshots | Hari Mankude | Jing Zhao |
| [HDFS-4867](https://issues.apache.org/jira/browse/HDFS-4867) | metaSave NPEs when there are invalid blocks in repl queue. |  Major | namenode | Kihwal Lee | Plamen Jeliazkov |
| [HDFS-4865](https://issues.apache.org/jira/browse/HDFS-4865) | Remove sub resource warning from httpfs log at startup time |  Major | . | Wei Yan | Wei Yan |
| [HDFS-4863](https://issues.apache.org/jira/browse/HDFS-4863) | The root directory should be added to the snapshottable directory list while loading fsimage |  Major | snapshots | Jing Zhao | Jing Zhao |
| [HDFS-4862](https://issues.apache.org/jira/browse/HDFS-4862) | SafeModeInfo.isManual() returns true when resources are low even if it wasn't entered into manually |  Major | . | Ravi Prakash | Ravi Prakash |
| [HDFS-4857](https://issues.apache.org/jira/browse/HDFS-4857) | Snapshot.Root and AbstractINodeDiff#snapshotINode should not be put into INodeMap when loading FSImage |  Major | snapshots | Jing Zhao | Jing Zhao |
| [HDFS-4850](https://issues.apache.org/jira/browse/HDFS-4850) | fix OfflineImageViewer to work on fsimages with empty files or snapshots |  Major | tools | Stephen Chu | Jing Zhao |
| [HDFS-4846](https://issues.apache.org/jira/browse/HDFS-4846) | Clean up snapshot CLI commands output stacktrace for invalid arguments |  Minor | snapshots | Stephen Chu | Jing Zhao |
| [HDFS-4845](https://issues.apache.org/jira/browse/HDFS-4845) | FSEditLogLoader gets NPE while accessing INodeMap in TestEditLogRace |  Critical | namenode | Kihwal Lee | Arpit Agarwal |
| [HDFS-4841](https://issues.apache.org/jira/browse/HDFS-4841) | FsShell commands using secure webhfds fail ClientFinalizer shutdown hook |  Major | security, webhdfs | Stephen Chu | Robert Kanter |
| [HDFS-4840](https://issues.apache.org/jira/browse/HDFS-4840) | ReplicationMonitor gets NPE during shutdown |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-4832](https://issues.apache.org/jira/browse/HDFS-4832) | Namenode doesn't change the number of missing blocks in safemode when DNs rejoin or leave |  Critical | . | Ravi Prakash | Ravi Prakash |
| [HDFS-4830](https://issues.apache.org/jira/browse/HDFS-4830) | Typo in config settings for AvailableSpaceVolumeChoosingPolicy in hdfs-default.xml |  Minor | . | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4827](https://issues.apache.org/jira/browse/HDFS-4827) | Slight update to the implementation of API for handling favored nodes in DFSClient |  Major | . | Devaraj Das | Devaraj Das |
| [HDFS-4826](https://issues.apache.org/jira/browse/HDFS-4826) | TestNestedSnapshots times out due to repeated slow edit log flushes when running on virtualized disk |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4825](https://issues.apache.org/jira/browse/HDFS-4825) | webhdfs / httpfs tests broken because of min block size change |  Major | webhdfs | Andrew Wang | Andrew Wang |
| [HDFS-4824](https://issues.apache.org/jira/browse/HDFS-4824) | FileInputStreamCache.close leaves dangling reference to FileInputStreamCache.cacheCleaner |  Major | hdfs-client | Henry Robinson | Colin Patrick McCabe |
| [HDFS-4818](https://issues.apache.org/jira/browse/HDFS-4818) | several HDFS tests that attempt to make directories unusable do not work correctly on Windows |  Minor | namenode, test | Chris Nauroth | Chris Nauroth |
| [HDFS-4815](https://issues.apache.org/jira/browse/HDFS-4815) | TestRBWBlockInvalidation#testBlockInvalidationWhenRBWReplicaMissedInDN: Double call countReplicas() to fetch corruptReplicas and liveReplicas is not needed |  Major | datanode, test | Tian Hong Wang | Tian Hong Wang |
| [HDFS-4813](https://issues.apache.org/jira/browse/HDFS-4813) | BlocksMap may throw NullPointerException during shutdown |  Minor | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-4810](https://issues.apache.org/jira/browse/HDFS-4810) | several HDFS HA tests have timeouts that are too short |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4807](https://issues.apache.org/jira/browse/HDFS-4807) | DFSOutputStream.createSocketForPipeline() should not include timeout extension on connect |  Major | . | Kihwal Lee | Cristina L. Abad |
| [HDFS-4805](https://issues.apache.org/jira/browse/HDFS-4805) | Webhdfs client is fragile to token renewal errors |  Critical | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-4799](https://issues.apache.org/jira/browse/HDFS-4799) | Corrupt replica can be prematurely removed from corruptReplicas map |  Blocker | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-4797](https://issues.apache.org/jira/browse/HDFS-4797) | BlockScanInfo does not override equals(..) and hashCode() consistently |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4783](https://issues.apache.org/jira/browse/HDFS-4783) | TestDelegationTokensWithHA#testHAUtilClonesDelegationTokens fails on Windows |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4780](https://issues.apache.org/jira/browse/HDFS-4780) | Use the correct relogin method for services |  Minor | namenode | Kihwal Lee | Robert Parker |
| [HDFS-4778](https://issues.apache.org/jira/browse/HDFS-4778) | Invoke getPipeline in the chooseTarget implementation that has favoredNodes |  Major | namenode | Devaraj Das | Devaraj Das |
| [HDFS-4768](https://issues.apache.org/jira/browse/HDFS-4768) | File handle leak in datanode when a block pool is removed |  Major | datanode | Chris Nauroth | Chris Nauroth |
| [HDFS-4765](https://issues.apache.org/jira/browse/HDFS-4765) | Permission check of symlink deletion incorrectly throws UnresolvedLinkException |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-4751](https://issues.apache.org/jira/browse/HDFS-4751) | TestLeaseRenewer#testThreadName flakes |  Minor | test | Andrew Wang | Andrew Wang |
| [HDFS-4748](https://issues.apache.org/jira/browse/HDFS-4748) | MiniJournalCluster#restartJournalNode leaks resources, which causes sporadic test failures |  Major | qjm, test | Chris Nauroth | Chris Nauroth |
| [HDFS-4745](https://issues.apache.org/jira/browse/HDFS-4745) | TestDataTransferKeepalive#testSlowReader has race condition that causes sporadic failure |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4743](https://issues.apache.org/jira/browse/HDFS-4743) | TestNNStorageRetentionManager fails on Windows |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4741](https://issues.apache.org/jira/browse/HDFS-4741) | TestStorageRestore#testStorageRestoreFailure fails on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4740](https://issues.apache.org/jira/browse/HDFS-4740) | Fixes for a few test failures on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4739](https://issues.apache.org/jira/browse/HDFS-4739) | NN can miscalculate the number of extra edit log segments to retain |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4737](https://issues.apache.org/jira/browse/HDFS-4737) | JVM path embedded in fuse binaries |  Major | . | Sean Mackrory | Sean Mackrory |
| [HDFS-4734](https://issues.apache.org/jira/browse/HDFS-4734) | HDFS Tests that use ShellCommandFencer are broken on Windows |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4733](https://issues.apache.org/jira/browse/HDFS-4733) | Make HttpFS username pattern configurable |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-4732](https://issues.apache.org/jira/browse/HDFS-4732) | TestDFSUpgradeFromImage fails on Windows due to failure to unpack old image tarball that contains hard links |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4725](https://issues.apache.org/jira/browse/HDFS-4725) | fix HDFS file handle leaks |  Major | namenode, test, tools | Chris Nauroth | Chris Nauroth |
| [HDFS-4722](https://issues.apache.org/jira/browse/HDFS-4722) | TestGetConf#testFederation times out on Windows |  Major | test | Ivan Mitic | Ivan Mitic |
| [HDFS-4714](https://issues.apache.org/jira/browse/HDFS-4714) | Log short messages in Namenode RPC server for exceptions meant for clients |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-4705](https://issues.apache.org/jira/browse/HDFS-4705) | Address HDFS test failures on Windows because of invalid dfs.namenode.name.dir |  Minor | . | Ivan Mitic | Ivan Mitic |
| [HDFS-4699](https://issues.apache.org/jira/browse/HDFS-4699) | TestPipelinesFailover#testPipelineRecoveryStress fails sporadically |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4695](https://issues.apache.org/jira/browse/HDFS-4695) | TestEditLog leaks open file handles between tests |  Major | test | Ivan Mitic | Ivan Mitic |
| [HDFS-4693](https://issues.apache.org/jira/browse/HDFS-4693) | Some test cases in TestCheckpoint do not clean up after themselves |  Minor | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4687](https://issues.apache.org/jira/browse/HDFS-4687) | TestDelegationTokenForProxyUser#testWebHdfsDoAs is flaky with JDK7 |  Minor | test | Andrew Wang | Andrew Wang |
| [HDFS-4677](https://issues.apache.org/jira/browse/HDFS-4677) | Editlog should support synchronous writes |  Major | . | Ivan Mitic | Ivan Mitic |
| [HDFS-4676](https://issues.apache.org/jira/browse/HDFS-4676) | TestHDFSFileSystemContract should set MiniDFSCluster variable to null to free up memory |  Minor | test | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4674](https://issues.apache.org/jira/browse/HDFS-4674) | TestBPOfferService fails on Windows due to failure parsing datanode data directory as URI |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-4669](https://issues.apache.org/jira/browse/HDFS-4669) | TestBlockPoolManager fails using IBM java |  Major | test | Tian Hong Wang | Tian Hong Wang |
| [HDFS-4658](https://issues.apache.org/jira/browse/HDFS-4658) | Standby NN will log that it has received a block report "after becoming active" |  Trivial | ha, namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4655](https://issues.apache.org/jira/browse/HDFS-4655) | DNA\_FINALIZE is logged as being an unknown command by the DN when received from the standby NN |  Minor | datanode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4643](https://issues.apache.org/jira/browse/HDFS-4643) | Fix flakiness in TestQuorumJournalManager |  Trivial | qjm, test | Todd Lipcon | Todd Lipcon |
| [HDFS-4639](https://issues.apache.org/jira/browse/HDFS-4639) | startFileInternal() should not increment generation stamp |  Major | namenode | Konstantin Shvachko | Plamen Jeliazkov |
| [HDFS-4625](https://issues.apache.org/jira/browse/HDFS-4625) | Make TestNNWithQJM#testNewNamenodeTakesOverWriter work on Windows |  Minor | test | Arpit Agarwal | Ivan Mitic |
| [HDFS-4621](https://issues.apache.org/jira/browse/HDFS-4621) | additional logging to help diagnose slow QJM logSync |  Minor | ha, qjm | Todd Lipcon | Todd Lipcon |
| [HDFS-4620](https://issues.apache.org/jira/browse/HDFS-4620) | Documentation for dfs.namenode.rpc-address specifies wrong format |  Major | documentation | Sandy Ryza | Sandy Ryza |
| [HDFS-4618](https://issues.apache.org/jira/browse/HDFS-4618) | default for checkpoint txn interval is too low |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-4615](https://issues.apache.org/jira/browse/HDFS-4615) | Fix TestDFSShell failures on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4614](https://issues.apache.org/jira/browse/HDFS-4614) | FSNamesystem#getContentSummary should use getPermissionChecker helper method |  Trivial | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4610](https://issues.apache.org/jira/browse/HDFS-4610) | Move to using common utils FileUtil#setReadable/Writable/Executable and FileUtil#canRead/Write/Execute |  Major | . | Ivan Mitic | Ivan Mitic |
| [HDFS-4609](https://issues.apache.org/jira/browse/HDFS-4609) | TestAuditLogs should release log handles between tests |  Minor | test | Ivan Mitic | Ivan Mitic |
| [HDFS-4607](https://issues.apache.org/jira/browse/HDFS-4607) | TestGetConf#testGetSpecificKey fails on Windows |  Minor | test | Ivan Mitic | Ivan Mitic |
| [HDFS-4604](https://issues.apache.org/jira/browse/HDFS-4604) | TestJournalNode fails on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [HDFS-4603](https://issues.apache.org/jira/browse/HDFS-4603) | TestMiniDFSCluster fails on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [HDFS-4598](https://issues.apache.org/jira/browse/HDFS-4598) | WebHDFS concat: the default value of sources in the code does not match the doc |  Minor | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4596](https://issues.apache.org/jira/browse/HDFS-4596) | Shutting down namenode during checkpointing can lead to md5sum error |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-4595](https://issues.apache.org/jira/browse/HDFS-4595) | When short circuit read is fails, DFSClient does not fallback to regular reads |  Major | hdfs-client | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4593](https://issues.apache.org/jira/browse/HDFS-4593) | TestSaveNamespace fails on Windows |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4592](https://issues.apache.org/jira/browse/HDFS-4592) | Default values for access time precision are out of sync between hdfs-default.xml and the code |  Minor | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4591](https://issues.apache.org/jira/browse/HDFS-4591) | HA clients can fail to fail over while Standby NN is performing long checkpoint |  Major | ha, namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4586](https://issues.apache.org/jira/browse/HDFS-4586) | TestDataDirs.testGetDataDirsFromURIs fails with all directories in dfs.datanode.data.dir are invalid |  Major | . | Ivan Mitic | Ivan Mitic |
| [HDFS-4584](https://issues.apache.org/jira/browse/HDFS-4584) | Fix TestNNWithQJM failures on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4583](https://issues.apache.org/jira/browse/HDFS-4583) | TestNodeCount fails |  Major | . | Ivan Mitic | Ivan Mitic |
| [HDFS-4582](https://issues.apache.org/jira/browse/HDFS-4582) | TestHostsFiles fails on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [HDFS-4573](https://issues.apache.org/jira/browse/HDFS-4573) | Fix TestINodeFile on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4572](https://issues.apache.org/jira/browse/HDFS-4572) | Fix TestJournal failures on Windows |  Major | namenode, test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4544](https://issues.apache.org/jira/browse/HDFS-4544) | Error in deleting blocks should not do check disk, for all types of errors |  Major | . | Amareshwari Sriramadasu | Arpit Agarwal |
| [HDFS-4541](https://issues.apache.org/jira/browse/HDFS-4541) | set hadoop.log.dir and hadoop.id.str when starting secure datanode so it writes the logs to the correct dir by default |  Major | datanode, security | Arpit Gupta | Arpit Gupta |
| [HDFS-4540](https://issues.apache.org/jira/browse/HDFS-4540) | namenode http server should use the web authentication keytab for spnego principal |  Major | security | Arpit Gupta | Arpit Gupta |
| [HDFS-4533](https://issues.apache.org/jira/browse/HDFS-4533) | start-dfs.sh ignored additional parameters besides -upgrade |  Major | datanode, namenode | Fengdong Yu | Fengdong Yu |
| [HDFS-4532](https://issues.apache.org/jira/browse/HDFS-4532) | RPC call queue may fill due to current user lookup |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-4522](https://issues.apache.org/jira/browse/HDFS-4522) | LightWeightGSet expects incrementing a volatile to be atomic |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4519](https://issues.apache.org/jira/browse/HDFS-4519) | Support override of jsvc binary and log file locations when launching secure datanode. |  Major | datanode, scripts | Chris Nauroth | Chris Nauroth |
| [HDFS-4518](https://issues.apache.org/jira/browse/HDFS-4518) | Finer grained metrics for HDFS capacity |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4495](https://issues.apache.org/jira/browse/HDFS-4495) | Allow client-side lease renewal to be retried beyond soft-limit |  Major | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-4489](https://issues.apache.org/jira/browse/HDFS-4489) | Use InodeID as as an identifier of a file in HDFS protocols and APIs |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-4484](https://issues.apache.org/jira/browse/HDFS-4484) | libwebhdfs compilation broken with gcc 4.6.2 |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4477](https://issues.apache.org/jira/browse/HDFS-4477) | Secondary namenode may retain old tokens |  Critical | security | Kihwal Lee | Daryn Sharp |
| [HDFS-4471](https://issues.apache.org/jira/browse/HDFS-4471) | Namenode WebUI file browsing does not work with wildcard addresses configured |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-4470](https://issues.apache.org/jira/browse/HDFS-4470) | several HDFS tests attempt file operations on invalid HDFS paths when running on Windows |  Major | . | Chris Nauroth | Chris Nauroth |
| [HDFS-4382](https://issues.apache.org/jira/browse/HDFS-4382) | Fix typo MAX\_NOT\_CHANGED\_INTERATIONS |  Major | . | Ted Yu | Ted Yu |
| [HDFS-4342](https://issues.apache.org/jira/browse/HDFS-4342) | Edits dir in dfs.namenode.edits.dir.required will be silently ignored if it is not in dfs.namenode.edits.dir |  Major | namenode | Mark Yang | Arpit Agarwal |
| [HDFS-4300](https://issues.apache.org/jira/browse/HDFS-4300) | TransferFsImage.downloadEditsToStorage should use a tmp file for destination |  Critical | . | Todd Lipcon | Andrew Wang |
| [HDFS-4298](https://issues.apache.org/jira/browse/HDFS-4298) | StorageRetentionManager spews warnings when used with QJM |  Major | namenode | Todd Lipcon | Aaron T. Myers |
| [HDFS-4296](https://issues.apache.org/jira/browse/HDFS-4296) | Add layout version for HDFS-4256 for release 1.2.0 |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4287](https://issues.apache.org/jira/browse/HDFS-4287) | HTTPFS tests fail on Windows |  Major | webhdfs | Chris Nauroth | Chris Nauroth |
| [HDFS-4269](https://issues.apache.org/jira/browse/HDFS-4269) | DatanodeManager#registerDatanode rejects all datanode registrations from localhost in single-node developer setup |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-4261](https://issues.apache.org/jira/browse/HDFS-4261) | TestBalancerWithNodeGroup times out |  Major | balancer & mover | Tsz Wo Nicholas Sze | Junping Du |
| [HDFS-4243](https://issues.apache.org/jira/browse/HDFS-4243) | INodeDirectory.replaceChild(..) does not update parent |  Major | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-4240](https://issues.apache.org/jira/browse/HDFS-4240) | In nodegroup-aware case, make sure nodes are avoided to place replica if some replica are already under the same nodegroup |  Major | namenode | Junping Du | Junping Du |
| [HDFS-4235](https://issues.apache.org/jira/browse/HDFS-4235) | when outputting XML, OfflineEditsViewer can't handle some edits containing non-ASCII strings |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4222](https://issues.apache.org/jira/browse/HDFS-4222) | NN is unresponsive and loses heartbeats of DNs when Hadoop is configured to use LDAP and LDAP has issues |  Minor | namenode | Xiaobo Peng | Xiaobo Peng |
| [HDFS-4209](https://issues.apache.org/jira/browse/HDFS-4209) | Clean up the addNode/addChild/addChildNoQuotaCheck methods in FSDirectory |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4205](https://issues.apache.org/jira/browse/HDFS-4205) | fsck fails with symlinks |  Major | hdfs-client | Andy Isaacson | Jason Lowe |
| [HDFS-4128](https://issues.apache.org/jira/browse/HDFS-4128) | 2NN gets stuck in inconsistent state if edit log replay fails in the middle |  Major | namenode | Todd Lipcon | Kihwal Lee |
| [HDFS-4013](https://issues.apache.org/jira/browse/HDFS-4013) | TestHftpURLTimeouts throws NPE |  Trivial | hdfs-client | Chao Shi | Chao Shi |
| [HDFS-3934](https://issues.apache.org/jira/browse/HDFS-3934) | duplicative dfs\_hosts entries handled wrong |  Minor | . | Andy Isaacson | Colin Patrick McCabe |
| [HDFS-3875](https://issues.apache.org/jira/browse/HDFS-3875) | Issue handling checksum errors in write pipeline |  Critical | datanode, hdfs-client | Todd Lipcon | Kihwal Lee |
| [HDFS-3792](https://issues.apache.org/jira/browse/HDFS-3792) | Fix two findbugs introduced by HDFS-3695 |  Trivial | build, namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3499](https://issues.apache.org/jira/browse/HDFS-3499) | Make NetworkTopology support user specified topology class |  Major | datanode | Junping Du | Junping Du |
| [HDFS-3277](https://issues.apache.org/jira/browse/HDFS-3277) | fail over to loading a different FSImage if the first one we try to load is corrupt |  Major | . | Colin Patrick McCabe | Andrew Wang |
| [HDFS-3180](https://issues.apache.org/jira/browse/HDFS-3180) | Add socket timeouts to webhdfs |  Major | webhdfs | Daryn Sharp | Chris Nauroth |
| [HDFS-3009](https://issues.apache.org/jira/browse/HDFS-3009) | DFSClient islocaladdress() can use similar routine in netutils |  Trivial | hdfs-client | Hari Mankude | Hari Mankude |
| [MAPREDUCE-5421](https://issues.apache.org/jira/browse/MAPREDUCE-5421) | TestNonExistentJob is failed due to recent changes in YARN |  Blocker | test | Junping Du | Junping Du |
| [MAPREDUCE-5419](https://issues.apache.org/jira/browse/MAPREDUCE-5419) | TestSlive is getting FileNotFound Exception |  Major | mrv2 | Robert Parker | Robert Parker |
| [MAPREDUCE-5412](https://issues.apache.org/jira/browse/MAPREDUCE-5412) | Change MR to use multiple containers API of ContainerManager after YARN-926 |  Major | . | Jian He | Jian He |
| [MAPREDUCE-5399](https://issues.apache.org/jira/browse/MAPREDUCE-5399) | Unnecessary Configuration instantiation in IFileInputStream slows down merge |  Blocker | mrv1, mrv2 | Stanislav Barton | Stanislav Barton |
| [MAPREDUCE-5366](https://issues.apache.org/jira/browse/MAPREDUCE-5366) | TestMRAsyncDiskService fails on Windows |  Minor | test | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5360](https://issues.apache.org/jira/browse/MAPREDUCE-5360) | TestMRJobClient fails on Windows due to path format |  Minor | test | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5359](https://issues.apache.org/jira/browse/MAPREDUCE-5359) | JobHistory should not use File.separator to match timestamp in path |  Minor | . | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5357](https://issues.apache.org/jira/browse/MAPREDUCE-5357) | Job staging directory owner checking could fail on Windows |  Minor | . | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5355](https://issues.apache.org/jira/browse/MAPREDUCE-5355) | MiniMRYarnCluster with localFs does not work on Windows |  Minor | . | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5349](https://issues.apache.org/jira/browse/MAPREDUCE-5349) | TestClusterMapReduceTestCase and TestJobName fail on Windows in branch-2 |  Minor | . | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5334](https://issues.apache.org/jira/browse/MAPREDUCE-5334) | TestContainerLauncherImpl is failing |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5328](https://issues.apache.org/jira/browse/MAPREDUCE-5328) | ClientToken should not be set in the environment |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [MAPREDUCE-5326](https://issues.apache.org/jira/browse/MAPREDUCE-5326) | Add version to shuffle header |  Blocker | . | Arun C Murthy | Zhijie Shen |
| [MAPREDUCE-5325](https://issues.apache.org/jira/browse/MAPREDUCE-5325) | ClientRMProtocol.getAllApplications should accept ApplicationType as a parameter---MR changes |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-5319](https://issues.apache.org/jira/browse/MAPREDUCE-5319) | Job.xml file does not has 'user.name' property for Hadoop2 |  Major | . | Yesha Vora | Xuan Gong |
| [MAPREDUCE-5315](https://issues.apache.org/jira/browse/MAPREDUCE-5315) | DistCp reports success even on failure. |  Critical | distcp | Mithun Radhakrishnan | Mithun Radhakrishnan |
| [MAPREDUCE-5312](https://issues.apache.org/jira/browse/MAPREDUCE-5312) | TestRMNMInfo is failing |  Major | . | Alejandro Abdelnur | Sandy Ryza |
| [MAPREDUCE-5310](https://issues.apache.org/jira/browse/MAPREDUCE-5310) | MRAM should not normalize allocation request capabilities |  Major | applicationmaster | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-5308](https://issues.apache.org/jira/browse/MAPREDUCE-5308) | Shuffling to memory can get out-of-sync when fetching multiple compressed map outputs |  Major | . | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-5303](https://issues.apache.org/jira/browse/MAPREDUCE-5303) | Changes on MR after moving ProtoBase to package impl.pb on YARN-724 |  Major | . | Jian He | Jian He |
| [MAPREDUCE-5301](https://issues.apache.org/jira/browse/MAPREDUCE-5301) | Update MR code to work with YARN-635 changes |  Major | . | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-5297](https://issues.apache.org/jira/browse/MAPREDUCE-5297) | Update MR App  since BuilderUtils is moved to yarn-server-common after YARN-748 |  Major | . | Jian He | Jian He |
| [MAPREDUCE-5291](https://issues.apache.org/jira/browse/MAPREDUCE-5291) | Change MR App to use update property names in container-log4j.properties |  Major | . | Siddharth Seth | Zhijie Shen |
| [MAPREDUCE-5289](https://issues.apache.org/jira/browse/MAPREDUCE-5289) | Update MR App to use Token directly after YARN-717 |  Major | . | Vinod Kumar Vavilapalli | Jian He |
| [MAPREDUCE-5285](https://issues.apache.org/jira/browse/MAPREDUCE-5285) | Update MR App to use immutable ApplicationAttemptID, ContainerID, NodeID after YARN-735 |  Major | . | Jian He |  |
| [MAPREDUCE-5282](https://issues.apache.org/jira/browse/MAPREDUCE-5282) | Update MR App to use immutable ApplicationID after YARN-716 |  Major | . | Vinod Kumar Vavilapalli | Siddharth Seth |
| [MAPREDUCE-5270](https://issues.apache.org/jira/browse/MAPREDUCE-5270) | Migrate from using BuilderUtil factory methods to individual record factory method on MapReduce side |  Major | . | Jian He | Jian He |
| [MAPREDUCE-5259](https://issues.apache.org/jira/browse/MAPREDUCE-5259) | TestTaskLog fails on Windows because of path separators missmatch |  Major | test | Ivan Mitic | Ivan Mitic |
| [MAPREDUCE-5257](https://issues.apache.org/jira/browse/MAPREDUCE-5257) | TestContainerLauncherImpl fails |  Major | mr-am, mrv2 | Jason Lowe | Omkar Vinit Joshi |
| [MAPREDUCE-5240](https://issues.apache.org/jira/browse/MAPREDUCE-5240) | inside of FileOutputCommitter the initialized Credentials cache appears to be empty |  Blocker | mrv2 | Roman Shaposhnik | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5239](https://issues.apache.org/jira/browse/MAPREDUCE-5239) | Update MR App to reflect YarnRemoteException changes after YARN-634 |  Major | . | Vinod Kumar Vavilapalli | Siddharth Seth |
| [MAPREDUCE-5226](https://issues.apache.org/jira/browse/MAPREDUCE-5226) | Handle exception related changes in YARN's AMRMProtocol api after YARN-630 |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-5213](https://issues.apache.org/jira/browse/MAPREDUCE-5213) | Re-assess TokenCache methods marked @Private |  Minor | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5212](https://issues.apache.org/jira/browse/MAPREDUCE-5212) | Handle exception related changes in YARN's ClientRMProtocol api after YARN-631 |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-5209](https://issues.apache.org/jira/browse/MAPREDUCE-5209) | ShuffleScheduler log message incorrect |  Minor | mrv2 | Radim Kolar | Tsuyoshi Ozawa |
| [MAPREDUCE-5208](https://issues.apache.org/jira/browse/MAPREDUCE-5208) | SpillRecord and ShuffleHandler should use SecureIOUtils for reading index file and map output |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [MAPREDUCE-5205](https://issues.apache.org/jira/browse/MAPREDUCE-5205) | Apps fail in secure cluster setup |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5204](https://issues.apache.org/jira/browse/MAPREDUCE-5204) | Handle YarnRemoteException separately from IOException in MR api |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-5193](https://issues.apache.org/jira/browse/MAPREDUCE-5193) | A few MR tests use block sizes which are smaller than the default minimum block size |  Major | test | Aaron T. Myers | Andrew Wang |
| [MAPREDUCE-5191](https://issues.apache.org/jira/browse/MAPREDUCE-5191) | TestQueue#testQueue fails with timeout on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [MAPREDUCE-5187](https://issues.apache.org/jira/browse/MAPREDUCE-5187) | Create mapreduce command scripts on Windows |  Major | mrv2 | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5181](https://issues.apache.org/jira/browse/MAPREDUCE-5181) | RMCommunicator should not use AMToken from the env |  Major | applicationmaster | Siddharth Seth | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5179](https://issues.apache.org/jira/browse/MAPREDUCE-5179) | Change TestHSWebServices to do string equal check on hadoop build version similar to YARN-605 |  Major | . | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-5178](https://issues.apache.org/jira/browse/MAPREDUCE-5178) | Fix use of BuilderUtils#newApplicationReport as a result of YARN-577. |  Major | . | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-5177](https://issues.apache.org/jira/browse/MAPREDUCE-5177) | Move to common utils FileUtil#setReadable/Writable/Executable and FileUtil#canRead/Write/Execute |  Major | . | Ivan Mitic | Ivan Mitic |
| [MAPREDUCE-5175](https://issues.apache.org/jira/browse/MAPREDUCE-5175) | Update MR App to not set envs that will be set by NMs anyways after YARN-561 |  Major | . | Vinod Kumar Vavilapalli | Xuan Gong |
| [MAPREDUCE-5167](https://issues.apache.org/jira/browse/MAPREDUCE-5167) | Update MR App after YARN-562 |  Major | . | Vinod Kumar Vavilapalli | Jian He |
| [MAPREDUCE-5166](https://issues.apache.org/jira/browse/MAPREDUCE-5166) | ConcurrentModificationException in LocalJobRunner |  Blocker | . | Gunther Hagleitner | Sandy Ryza |
| [MAPREDUCE-5163](https://issues.apache.org/jira/browse/MAPREDUCE-5163) | Update MR App after YARN-441 |  Major | . | Vinod Kumar Vavilapalli | Xuan Gong |
| [MAPREDUCE-5152](https://issues.apache.org/jira/browse/MAPREDUCE-5152) | MR App is not using Container from RM |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5151](https://issues.apache.org/jira/browse/MAPREDUCE-5151) | Update MR App after YARN-444 |  Major | . | Vinod Kumar Vavilapalli | Sandy Ryza |
| [MAPREDUCE-5147](https://issues.apache.org/jira/browse/MAPREDUCE-5147) | Maven build should create hadoop-mapreduce-client-app-VERSION.jar directly |  Major | mrv2 | Robert Parker | Robert Parker |
| [MAPREDUCE-5146](https://issues.apache.org/jira/browse/MAPREDUCE-5146) | application classloader may be used too early to load classes |  Minor | task | Sangjin Lee | Sangjin Lee |
| [MAPREDUCE-5145](https://issues.apache.org/jira/browse/MAPREDUCE-5145) | Change default max-attempts to be more than one for MR jobs as well |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5140](https://issues.apache.org/jira/browse/MAPREDUCE-5140) | MR part of YARN-514 |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5139](https://issues.apache.org/jira/browse/MAPREDUCE-5139) | Update MR App after YARN-486 |  Major | . | Vinod Kumar Vavilapalli | Xuan Gong |
| [MAPREDUCE-5138](https://issues.apache.org/jira/browse/MAPREDUCE-5138) | Fix LocalDistributedCacheManager after YARN-112 |  Major | . | Vinod Kumar Vavilapalli | Omkar Vinit Joshi |
| [MAPREDUCE-5137](https://issues.apache.org/jira/browse/MAPREDUCE-5137) | AM web UI: clicking on Map Task results in 500 error |  Major | applicationmaster | Thomas Graves | Thomas Graves |
| [MAPREDUCE-5136](https://issues.apache.org/jira/browse/MAPREDUCE-5136) | TestJobImpl-\>testJobNoTasks fails with IBM JAVA |  Major | . | Amir Sanjar | Amir Sanjar |
| [MAPREDUCE-5113](https://issues.apache.org/jira/browse/MAPREDUCE-5113) | Streaming input/output types are ignored with java mapper/reducer |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5098](https://issues.apache.org/jira/browse/MAPREDUCE-5098) | Fix findbugs warnings in gridmix |  Major | contrib/gridmix | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5078](https://issues.apache.org/jira/browse/MAPREDUCE-5078) | TestMRAppMaster fails on Windows due to mismatched path separators |  Major | client | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5077](https://issues.apache.org/jira/browse/MAPREDUCE-5077) | Cleanup: mapreduce.util.ResourceCalculatorPlugin and related code should be removed |  Minor | mrv2 | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5075](https://issues.apache.org/jira/browse/MAPREDUCE-5075) | DistCp leaks input file handles |  Major | distcp | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5066](https://issues.apache.org/jira/browse/MAPREDUCE-5066) | JobTracker should set a timeout when calling into job.end.notification.url |  Major | . | Ivan Mitic | Ivan Mitic |
| [MAPREDUCE-5065](https://issues.apache.org/jira/browse/MAPREDUCE-5065) | DistCp should skip checksum comparisons if block-sizes are different on source/target. |  Major | distcp | Mithun Radhakrishnan | Mithun Radhakrishnan |
| [MAPREDUCE-5062](https://issues.apache.org/jira/browse/MAPREDUCE-5062) | MR AM should read max-retries information from the RM |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [MAPREDUCE-5060](https://issues.apache.org/jira/browse/MAPREDUCE-5060) | Fetch failures that time out only count against the first map task |  Critical | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-5059](https://issues.apache.org/jira/browse/MAPREDUCE-5059) | Job overview shows average merge time larger than for any reduce attempt |  Major | jobhistoryserver, webapps | Jason Lowe | Omkar Vinit Joshi |
| [MAPREDUCE-5043](https://issues.apache.org/jira/browse/MAPREDUCE-5043) | Fetch failure processing can cause AM event queue to backup and eventually OOM |  Blocker | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5042](https://issues.apache.org/jira/browse/MAPREDUCE-5042) | Reducer unable to fetch for a map task that was recovered |  Blocker | mr-am, security | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5027](https://issues.apache.org/jira/browse/MAPREDUCE-5027) | Shuffle does not limit number of outstanding connections |  Major | . | Jason Lowe | Robert Parker |
| [MAPREDUCE-5013](https://issues.apache.org/jira/browse/MAPREDUCE-5013) | mapred.JobStatus compatibility: MR2 missing constructors from MR1 |  Major | client | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5009](https://issues.apache.org/jira/browse/MAPREDUCE-5009) | Killing the Task Attempt slated for commit does not clear the value from the Task commitAttempt member |  Critical | mrv1 | Robert Parker | Robert Parker |
| [MAPREDUCE-5008](https://issues.apache.org/jira/browse/MAPREDUCE-5008) | Merger progress miscounts with respect to EOF\_MARKER |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5000](https://issues.apache.org/jira/browse/MAPREDUCE-5000) | TaskImpl.getCounters() can return the counters for the wrong task attempt when task is speculating |  Critical | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4994](https://issues.apache.org/jira/browse/MAPREDUCE-4994) | -jt generic command line option does not work |  Major | client | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4992](https://issues.apache.org/jira/browse/MAPREDUCE-4992) | AM hangs in RecoveryService when recovering tasks with speculative attempts |  Critical | mr-am | Robert Parker | Robert Parker |
| [MAPREDUCE-4987](https://issues.apache.org/jira/browse/MAPREDUCE-4987) | TestMRJobs#testDistributedCache fails on Windows due to classpath problems and unexpected behavior of symlinks |  Major | distributed-cache, nodemanager | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-4985](https://issues.apache.org/jira/browse/MAPREDUCE-4985) | TestDFSIO supports compression but usages doesn't reflect |  Trivial | . | Plamen Jeliazkov | Plamen Jeliazkov |
| [MAPREDUCE-4981](https://issues.apache.org/jira/browse/MAPREDUCE-4981) | WordMean, WordMedian, WordStandardDeviation missing from ExamplesDriver |  Minor | . | Plamen Jeliazkov | Plamen Jeliazkov |
| [MAPREDUCE-4932](https://issues.apache.org/jira/browse/MAPREDUCE-4932) | mapreduce.job#getTaskCompletionEvents incompatible with Hadoop 1 |  Major | mrv2 | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4927](https://issues.apache.org/jira/browse/MAPREDUCE-4927) | Historyserver 500 error due to NPE when accessing specific counters page for failed job |  Major | jobhistoryserver | Jason Lowe | Ashwin Shankar |
| [MAPREDUCE-4898](https://issues.apache.org/jira/browse/MAPREDUCE-4898) | FileOutputFormat.checkOutputSpecs and FileOutputFormat.setOutputPath incompatible with MR1 |  Major | mrv2 | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4896](https://issues.apache.org/jira/browse/MAPREDUCE-4896) | "mapred queue -info" spits out ugly exception when queue does not exist |  Major | client, scheduler | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4892](https://issues.apache.org/jira/browse/MAPREDUCE-4892) | CombineFileInputFormat node input split can be skewed on small clusters |  Major | . | Bikas Saha | Bikas Saha |
| [MAPREDUCE-4885](https://issues.apache.org/jira/browse/MAPREDUCE-4885) | Streaming tests have multiple failures on Windows |  Major | contrib/streaming, test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-4871](https://issues.apache.org/jira/browse/MAPREDUCE-4871) | AM uses mapreduce.jobtracker.split.metainfo.maxsize but mapred-default has mapreduce.job.split.metainfo.maxsize |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4794](https://issues.apache.org/jira/browse/MAPREDUCE-4794) | DefaultSpeculator generates error messages on normal shutdown |  Major | applicationmaster | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4716](https://issues.apache.org/jira/browse/MAPREDUCE-4716) | TestHsWebServicesJobsQuery.testJobsQueryStateInvalid fails with jdk7 |  Major | jobhistoryserver | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4693](https://issues.apache.org/jira/browse/MAPREDUCE-4693) | Historyserver should provide counters for failed tasks |  Major | jobhistoryserver, mrv2 | Jason Lowe | Xuan Gong |
| [MAPREDUCE-4671](https://issues.apache.org/jira/browse/MAPREDUCE-4671) | AM does not tell the RM about container requests that are no longer needed |  Major | . | Bikas Saha | Bikas Saha |
| [MAPREDUCE-4571](https://issues.apache.org/jira/browse/MAPREDUCE-4571) | TestHsWebServicesJobs fails on jdk7 |  Major | webapps | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4374](https://issues.apache.org/jira/browse/MAPREDUCE-4374) | Fix child task environment variable config and add support for Windows |  Minor | mrv2 | Chuan Liu | Chuan Liu |
| [MAPREDUCE-4356](https://issues.apache.org/jira/browse/MAPREDUCE-4356) | Provide access to ParsedTask.obtainTaskAttempts() |  Major | tools/rumen | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-4149](https://issues.apache.org/jira/browse/MAPREDUCE-4149) | Rumen fails to parse certain counter strings |  Major | tools/rumen | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-4100](https://issues.apache.org/jira/browse/MAPREDUCE-4100) | Sometimes gridmix emulates data larger much larger then acutal counter for map only jobs |  Minor | contrib/gridmix | Karam Singh | Amar Kamat |
| [MAPREDUCE-4083](https://issues.apache.org/jira/browse/MAPREDUCE-4083) | GridMix emulated job tasks.resource-usage emulator for CPU usage throws NPE when Trace contains cumulativeCpuUsage value of 0 at attempt level |  Major | contrib/gridmix | Karam Singh | Amar Kamat |
| [MAPREDUCE-4019](https://issues.apache.org/jira/browse/MAPREDUCE-4019) | -list-attempt-ids  is not working |  Minor | client | B Anil Kumar | Ashwin Shankar |
| [MAPREDUCE-3953](https://issues.apache.org/jira/browse/MAPREDUCE-3953) | Gridmix throws NPE and does not simulate a job if the trace contains null taskStatus for a task |  Major | . | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-3872](https://issues.apache.org/jira/browse/MAPREDUCE-3872) | event handling races in ContainerLauncherImpl and TestContainerLauncher |  Major | client, mrv2 | Patrick Hunt | Robert Kanter |
| [MAPREDUCE-3829](https://issues.apache.org/jira/browse/MAPREDUCE-3829) | [Gridmix] Gridmix should give better error message when input-data directory already exists and -generate option is given |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-3757](https://issues.apache.org/jira/browse/MAPREDUCE-3757) | Rumen Folder is not adjusting the shuffleFinished and sortFinished times of reduce task attempts |  Major | tools/rumen | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-3685](https://issues.apache.org/jira/browse/MAPREDUCE-3685) | There are some bugs in implementation of MergeManager |  Critical | mrv2 | anty.rao | anty |
| [MAPREDUCE-2722](https://issues.apache.org/jira/browse/MAPREDUCE-2722) | Gridmix simulated job's map's hdfsBytesRead counter is wrong when compressed input is used |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [YARN-1046](https://issues.apache.org/jira/browse/YARN-1046) | Disable mem monitoring by default in MiniYARNCluster |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [YARN-1043](https://issues.apache.org/jira/browse/YARN-1043) | YARN Queue metrics are getting pushed to neither file nor Ganglia |  Major | . | Yusaku Sako | Jian He |
| [YARN-968](https://issues.apache.org/jira/browse/YARN-968) | RM admin commands don't work |  Blocker | . | Kihwal Lee | Vinod Kumar Vavilapalli |
| [YARN-960](https://issues.apache.org/jira/browse/YARN-960) | TestMRCredentials and  TestBinaryTokenFile are failing on trunk |  Blocker | . | Alejandro Abdelnur | Daryn Sharp |
| [YARN-945](https://issues.apache.org/jira/browse/YARN-945) | AM register failing after AMRMToken |  Blocker | . | Bikas Saha | Vinod Kumar Vavilapalli |
| [YARN-937](https://issues.apache.org/jira/browse/YARN-937) | Fix unmanaged AM in non-secure/secure setup post YARN-701 |  Blocker | applications/unmanaged-AM-launcher | Arun C Murthy | Alejandro Abdelnur |
| [YARN-932](https://issues.apache.org/jira/browse/YARN-932) | TestResourceLocalizationService.testLocalizationInit can fail on JDK7 |  Major | . | Sandy Ryza | Karthik Kambatla |
| [YARN-919](https://issues.apache.org/jira/browse/YARN-919) | Document setting default heap sizes in yarn env |  Minor | . | Mayank Bansal | Mayank Bansal |
| [YARN-912](https://issues.apache.org/jira/browse/YARN-912) | Create exceptions package in common/api for yarn and move client facing exceptions to them |  Major | . | Bikas Saha | Mayank Bansal |
| [YARN-909](https://issues.apache.org/jira/browse/YARN-909) | Disable TestLinuxContainerExecutorWithMocks on Windows |  Minor | nodemanager | Chuan Liu | Chuan Liu |
| [YARN-897](https://issues.apache.org/jira/browse/YARN-897) | CapacityScheduler wrongly sorted queues |  Blocker | capacityscheduler | Djellel Eddine Difallah | Djellel Eddine Difallah |
| [YARN-894](https://issues.apache.org/jira/browse/YARN-894) | NodeHealthScriptRunner timeout checking is inaccurate on Windows |  Minor | nodemanager | Chuan Liu | Chuan Liu |
| [YARN-875](https://issues.apache.org/jira/browse/YARN-875) | Application can hang if AMRMClientAsync callback thread has exception |  Major | . | Bikas Saha | Xuan Gong |
| [YARN-874](https://issues.apache.org/jira/browse/YARN-874) | Tracking YARN/MR test failures after HADOOP-9421 and YARN-827 |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-861](https://issues.apache.org/jira/browse/YARN-861) | TestContainerManager is failing |  Critical | nodemanager | Devaraj K | Vinod Kumar Vavilapalli |
| [YARN-854](https://issues.apache.org/jira/browse/YARN-854) | App submission fails on secure deploy |  Blocker | . | Ramya Sunil | Omkar Vinit Joshi |
| [YARN-853](https://issues.apache.org/jira/browse/YARN-853) | maximum-am-resource-percent doesn't work after refreshQueues command |  Major | capacityscheduler | Devaraj K | Devaraj K |
| [YARN-852](https://issues.apache.org/jira/browse/YARN-852) | TestAggregatedLogFormat.testContainerLogsFileAccess fails on Windows |  Minor | . | Chuan Liu | Chuan Liu |
| [YARN-848](https://issues.apache.org/jira/browse/YARN-848) | Nodemanager does not register with RM using the fully qualified hostname |  Major | . | Hitesh Shah | Hitesh Shah |
| [YARN-839](https://issues.apache.org/jira/browse/YARN-839) | TestContainerLaunch.testContainerEnvVariables fails on Windows |  Minor | . | Chuan Liu | Chuan Liu |
| [YARN-833](https://issues.apache.org/jira/browse/YARN-833) | Move Graph and VisualizeStateMachine into yarn.state package |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-812](https://issues.apache.org/jira/browse/YARN-812) | Enabling app summary logs causes 'FileNotFound' errors |  Major | . | Ramya Sunil | Siddharth Seth |
| [YARN-799](https://issues.apache.org/jira/browse/YARN-799) | CgroupsLCEResourcesHandler tries to write to cgroup.procs |  Major | nodemanager | Chris Riccomini | Chris Riccomini |
| [YARN-795](https://issues.apache.org/jira/browse/YARN-795) | Fair scheduler queue metrics should subtract allocated vCores from available vCores |  Major | scheduler | Wei Yan | Wei Yan |
| [YARN-767](https://issues.apache.org/jira/browse/YARN-767) | Initialize Application status metrics  when QueueMetrics is initialized |  Major | . | Jian He | Jian He |
| [YARN-764](https://issues.apache.org/jira/browse/YARN-764) | blank Used Resources on Capacity Scheduler page |  Major | resourcemanager | Nemon Lou | Nemon Lou |
| [YARN-763](https://issues.apache.org/jira/browse/YARN-763) | AMRMClientAsync should stop heartbeating after receiving shutdown from RM |  Major | . | Bikas Saha | Xuan Gong |
| [YARN-761](https://issues.apache.org/jira/browse/YARN-761) | TestNMClientAsync fails sometimes |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [YARN-760](https://issues.apache.org/jira/browse/YARN-760) | NodeManager throws AvroRuntimeException on failed start |  Major | nodemanager | Sandy Ryza | Niranjan Singh |
| [YARN-757](https://issues.apache.org/jira/browse/YARN-757) | TestRMRestart failing/stuck on trunk |  Blocker | . | Bikas Saha | Bikas Saha |
| [YARN-742](https://issues.apache.org/jira/browse/YARN-742) | Log aggregation causes a lot of redundant setPermission calls |  Major | nodemanager | Kihwal Lee | Jason Lowe |
| [YARN-733](https://issues.apache.org/jira/browse/YARN-733) | TestNMClient fails occasionally |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-726](https://issues.apache.org/jira/browse/YARN-726) | Queue, FinishTime fields broken on RM UI |  Critical | . | Siddharth Seth | Mayank Bansal |
| [YARN-715](https://issues.apache.org/jira/browse/YARN-715) | TestDistributedShell and TestUnmanagedAMLauncher are failing |  Major | . | Siddharth Seth | Vinod Kumar Vavilapalli |
| [YARN-706](https://issues.apache.org/jira/browse/YARN-706) | Race Condition in TestFSDownload |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-700](https://issues.apache.org/jira/browse/YARN-700) | TestInfoBlock fails on Windows because of line ending missmatch |  Major | . | Ivan Mitic | Ivan Mitic |
| [YARN-690](https://issues.apache.org/jira/browse/YARN-690) | RM exits on token cancel/renew problems |  Blocker | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-688](https://issues.apache.org/jira/browse/YARN-688) | Containers not cleaned up when NM received SHUTDOWN event from NodeStatusUpdater |  Major | . | Jian He | Jian He |
| [YARN-661](https://issues.apache.org/jira/browse/YARN-661) | NM fails to cleanup local directories for users |  Major | nodemanager | Jason Lowe | Omkar Vinit Joshi |
| [YARN-656](https://issues.apache.org/jira/browse/YARN-656) | In scheduler UI, including reserved memory in "Memory Total" can make it exceed cluster capacity. |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-655](https://issues.apache.org/jira/browse/YARN-655) | Fair scheduler metrics should subtract allocated memory from available memory |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-654](https://issues.apache.org/jira/browse/YARN-654) | AMRMClient: Perform sanity checks for parameters of public methods |  Major | . | Bikas Saha | Xuan Gong |
| [YARN-648](https://issues.apache.org/jira/browse/YARN-648) | FS: Add documentation for pluggable policy |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-646](https://issues.apache.org/jira/browse/YARN-646) | Some issues in Fair Scheduler's document |  Major | documentation | Dapeng Sun | Dapeng Sun |
| [YARN-645](https://issues.apache.org/jira/browse/YARN-645) | Move RMDelegationTokenSecretManager from yarn-server-common to yarn-server-resourcemanager |  Major | . | Jian He | Jian He |
| [YARN-639](https://issues.apache.org/jira/browse/YARN-639) | Make AM of Distributed Shell Use NMClient |  Major | applications/distributed-shell | Zhijie Shen | Zhijie Shen |
| [YARN-637](https://issues.apache.org/jira/browse/YARN-637) | FS: maxAssign is not honored |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-618](https://issues.apache.org/jira/browse/YARN-618) | Modify RM\_INVALID\_IDENTIFIER to  a -ve number |  Major | . | Jian He | Jian He |
| [YARN-605](https://issues.apache.org/jira/browse/YARN-605) | Failing unit test in TestNMWebServices when using git for source control |  Major | . | Hitesh Shah | Hitesh Shah |
| [YARN-599](https://issues.apache.org/jira/browse/YARN-599) | Refactoring submitApplication in ClientRMService and RMAppManager |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-597](https://issues.apache.org/jira/browse/YARN-597) | TestFSDownload fails on Windows because of dependencies on tar/gzip/jar tools |  Major | . | Ivan Mitic | Ivan Mitic |
| [YARN-594](https://issues.apache.org/jira/browse/YARN-594) | Update test and add comments in YARN-534 |  Major | . | Jian He | Jian He |
| [YARN-593](https://issues.apache.org/jira/browse/YARN-593) | container launch on Windows does not correctly populate classpath with new process's environment variables and localized resources |  Major | nodemanager | Chris Nauroth | Chris Nauroth |
| [YARN-586](https://issues.apache.org/jira/browse/YARN-586) | Typo in ApplicationSubmissionContext#setApplicationId |  Trivial | . | Zhijie Shen | Zhijie Shen |
| [YARN-585](https://issues.apache.org/jira/browse/YARN-585) | TestFairScheduler#testNotAllowSubmitApplication is broken due to YARN-514 |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-576](https://issues.apache.org/jira/browse/YARN-576) | RM should not allow registrations from NMs that do not satisfy minimum scheduler allocations |  Major | . | Hitesh Shah | Kenji Kikushima |
| [YARN-557](https://issues.apache.org/jira/browse/YARN-557) | TestUnmanagedAMLauncher fails on Windows |  Major | applications | Chris Nauroth | Chris Nauroth |
| [YARN-542](https://issues.apache.org/jira/browse/YARN-542) | Change the default global AM max-attempts value to be not one |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [YARN-541](https://issues.apache.org/jira/browse/YARN-541) | getAllocatedContainers() is not returning all the allocated containers |  Blocker | resourcemanager | Krishna Kishore Bonagiri | Bikas Saha |
| [YARN-532](https://issues.apache.org/jira/browse/YARN-532) | RMAdminProtocolPBClientImpl should implement Closeable |  Major | . | Siddharth Seth | Siddharth Seth |
| [YARN-515](https://issues.apache.org/jira/browse/YARN-515) | Node Manager not getting the master key |  Blocker | . | Robert Joseph Evans | Robert Joseph Evans |
| [YARN-512](https://issues.apache.org/jira/browse/YARN-512) | Log aggregation root directory check is more expensive than it needs to be |  Minor | nodemanager | Jason Lowe | Maysam Yabandeh |
| [YARN-507](https://issues.apache.org/jira/browse/YARN-507) | Add interface visibility and stability annotations to FS interfaces/classes |  Minor | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-506](https://issues.apache.org/jira/browse/YARN-506) | Move to common utils FileUtil#setReadable/Writable/Executable and FileUtil#canRead/Write/Execute |  Major | . | Ivan Mitic | Ivan Mitic |
| [YARN-500](https://issues.apache.org/jira/browse/YARN-500) | ResourceManager webapp is using next port if configured port is already in use |  Major | resourcemanager | Nishan Shetty | Kenji Kikushima |
| [YARN-496](https://issues.apache.org/jira/browse/YARN-496) | Fair scheduler configs are refreshed inconsistently in reinitialize |  Minor | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-495](https://issues.apache.org/jira/browse/YARN-495) | Change NM behavior of reboot to resync |  Major | . | Jian He | Jian He |
| [YARN-493](https://issues.apache.org/jira/browse/YARN-493) | NodeManager job control logic flaws on Windows |  Major | nodemanager | Chris Nauroth | Chris Nauroth |
| [YARN-491](https://issues.apache.org/jira/browse/YARN-491) | TestContainerLogsPage fails on Windows |  Major | nodemanager | Chris Nauroth | Chris Nauroth |
| [YARN-490](https://issues.apache.org/jira/browse/YARN-490) | TestDistributedShell fails on Windows |  Major | applications/distributed-shell | Chris Nauroth | Chris Nauroth |
| [YARN-488](https://issues.apache.org/jira/browse/YARN-488) | TestContainerManagerSecurity fails on Windows |  Major | nodemanager | Chris Nauroth | Chris Nauroth |
| [YARN-487](https://issues.apache.org/jira/browse/YARN-487) | TestDiskFailures fails on Windows due to path mishandling |  Major | nodemanager | Chris Nauroth | Chris Nauroth |
| [YARN-485](https://issues.apache.org/jira/browse/YARN-485) | TestProcfsProcessTree#testProcessTree() doesn't wait long enough for the process to die |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [YARN-481](https://issues.apache.org/jira/browse/YARN-481) | Add AM Host and RPC Port to ApplicationCLI Status Output |  Major | client | Chris Riccomini | Chris Riccomini |
| [YARN-479](https://issues.apache.org/jira/browse/YARN-479) | NM retry behavior for connection to RM should be similar for lost heartbeats |  Major | . | Hitesh Shah | Jian He |
| [YARN-476](https://issues.apache.org/jira/browse/YARN-476) | ProcfsBasedProcessTree info message confuses users |  Minor | . | Jason Lowe | Sandy Ryza |
| [YARN-474](https://issues.apache.org/jira/browse/YARN-474) | CapacityScheduler does not activate applications when maximum-am-resource-percent configuration is refreshed |  Major | capacityscheduler | Hitesh Shah | Zhijie Shen |
| [YARN-460](https://issues.apache.org/jira/browse/YARN-460) | CS user left in list of active users for the queue even when application finished |  Blocker | capacityscheduler | Thomas Graves | Thomas Graves |
| [YARN-458](https://issues.apache.org/jira/browse/YARN-458) | YARN daemon addresses must be placed in many different configs |  Major | nodemanager, resourcemanager | Sandy Ryza | Sandy Ryza |
| [YARN-448](https://issues.apache.org/jira/browse/YARN-448) | Remove unnecessary hflush from log aggregation |  Major | nodemanager | Kihwal Lee | Kihwal Lee |
| [YARN-426](https://issues.apache.org/jira/browse/YARN-426) | Failure to download a public resource on a node prevents further downloads of the resource from that node |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-412](https://issues.apache.org/jira/browse/YARN-412) | FifoScheduler incorrectly checking for node locality |  Minor | scheduler | Roger Hoover | Roger Hoover |
| [YARN-410](https://issues.apache.org/jira/browse/YARN-410) | New lines in diagnostics for a failed app on the per-application page make it hard to read |  Major | . | Vinod Kumar Vavilapalli | Omkar Vinit Joshi |
| [YARN-400](https://issues.apache.org/jira/browse/YARN-400) | RM can return null application resource usage report leading to NPE in client |  Critical | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-390](https://issues.apache.org/jira/browse/YARN-390) | ApplicationCLI and NodeCLI use hard-coded platform-specific line separator, which causes test failures on Windows |  Major | client | Chris Nauroth | Chris Nauroth |
| [YARN-383](https://issues.apache.org/jira/browse/YARN-383) | AMRMClientImpl should handle null rmClient in stop() |  Minor | . | Hitesh Shah | Hitesh Shah |
| [YARN-380](https://issues.apache.org/jira/browse/YARN-380) | yarn node -status prints Last-Last-Health-Update |  Major | client | Thomas Graves | Omkar Vinit Joshi |
| [YARN-377](https://issues.apache.org/jira/browse/YARN-377) | Fix TestContainersMonitor for HADOOP-9252 |  Minor | . | Tsz Wo Nicholas Sze | Chris Nauroth |
| [YARN-376](https://issues.apache.org/jira/browse/YARN-376) | Apps that have completed can appear as RUNNING on the NM UI |  Blocker | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-368](https://issues.apache.org/jira/browse/YARN-368) | Fix typo "defiend" should be "defined" in error output |  Trivial | . | Albert Chu | Albert Chu |
| [YARN-363](https://issues.apache.org/jira/browse/YARN-363) | yarn proxyserver fails to find webapps/proxy directory on startup |  Major | . | Jason Lowe | Kenji Kikushima |
| [YARN-362](https://issues.apache.org/jira/browse/YARN-362) | Unexpected extra results when using webUI table search |  Minor | . | Jason Lowe | Ravi Prakash |
| [YARN-345](https://issues.apache.org/jira/browse/YARN-345) | Many InvalidStateTransitonException errors for ApplicationImpl in Node Manager |  Critical | nodemanager | Devaraj K | Robert Parker |
| [YARN-333](https://issues.apache.org/jira/browse/YARN-333) | Schedulers cannot control the queue-name of an application |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-319](https://issues.apache.org/jira/browse/YARN-319) | Submit a job to a queue that not allowed in fairScheduler, client will hold forever. |  Major | resourcemanager, scheduler | Hong Shen | Hong Shen |
| [YARN-289](https://issues.apache.org/jira/browse/YARN-289) | Fair scheduler allows reservations that won't fit on node |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-269](https://issues.apache.org/jira/browse/YARN-269) | Resource Manager not logging the health\_check\_script result when taking it out |  Major | resourcemanager | Thomas Graves | Jason Lowe |
| [YARN-236](https://issues.apache.org/jira/browse/YARN-236) | RM should point tracking URL to RM web page when app fails to start |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-227](https://issues.apache.org/jira/browse/YARN-227) | Application expiration difficult to debug for end-users |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-209](https://issues.apache.org/jira/browse/YARN-209) | Capacity scheduler doesn't trigger app-activation after adding nodes |  Major | capacityscheduler | Bikas Saha | Zhijie Shen |
| [YARN-196](https://issues.apache.org/jira/browse/YARN-196) | Nodemanager should be more robust in handling connection failure  to ResourceManager when a cluster is started |  Major | nodemanager | Ramgopal N | Xuan Gong |
| [YARN-193](https://issues.apache.org/jira/browse/YARN-193) | Scheduler.normalizeRequest does not account for allocation requests that exceed maximumAllocation limits |  Major | resourcemanager | Hitesh Shah | Zhijie Shen |
| [YARN-109](https://issues.apache.org/jira/browse/YARN-109) | .tmp file is not deleted for localized archives |  Major | nodemanager | Jason Lowe | Mayank Bansal |
| [YARN-101](https://issues.apache.org/jira/browse/YARN-101) | If  the heartbeat message loss, the nodestatus info of complete container will loss too. |  Minor | nodemanager | xieguiming | Xuan Gong |
| [YARN-71](https://issues.apache.org/jira/browse/YARN-71) | Ensure/confirm that the NodeManager cleans up local-dirs on restart |  Critical | nodemanager | Vinod Kumar Vavilapalli | Xuan Gong |
| [YARN-45](https://issues.apache.org/jira/browse/YARN-45) | [Preemption] Scheduler feedback to AM to release containers |  Major | resourcemanager | Chris Douglas | Carlo Curino |
| [YARN-24](https://issues.apache.org/jira/browse/YARN-24) | Nodemanager fails to start if log aggregation enabled and namenode unavailable |  Major | nodemanager | Jason Lowe | Sandy Ryza |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9624](https://issues.apache.org/jira/browse/HADOOP-9624) | TestFSMainOperationsLocalFileSystem failed when the Hadoop test root path has "X" in its name |  Minor | test | Xi Fang | Xi Fang |
| [HADOOP-9287](https://issues.apache.org/jira/browse/HADOOP-9287) | Parallel testing hadoop-common |  Major | test | Tsuyoshi Ozawa | Andrey Klochkov |
| [HADOOP-9233](https://issues.apache.org/jira/browse/HADOOP-9233) | Cover package org.apache.hadoop.io.compress.zlib with unit tests |  Major | . | Vadim Bondarev | Vadim Bondarev |
| [HADOOP-9222](https://issues.apache.org/jira/browse/HADOOP-9222) | Cover package with org.apache.hadoop.io.lz4 unit tests |  Major | . | Vadim Bondarev | Vadim Bondarev |
| [HDFS-4129](https://issues.apache.org/jira/browse/HDFS-4129) | Add utility methods to dump NameNode in memory tree for testing |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-5333](https://issues.apache.org/jira/browse/MAPREDUCE-5333) | Add test that verifies MRAM works correctly when sending requests with non-normalized capabilities |  Major | mr-am | Alejandro Abdelnur | Wei Yan |
| [MAPREDUCE-5015](https://issues.apache.org/jira/browse/MAPREDUCE-5015) | Coverage fix for org.apache.hadoop.mapreduce.tools.CLI |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [MAPREDUCE-5007](https://issues.apache.org/jira/browse/MAPREDUCE-5007) | fix coverage org.apache.hadoop.mapreduce.v2.hs |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [MAPREDUCE-4991](https://issues.apache.org/jira/browse/MAPREDUCE-4991) | coverage for gridmix |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [MAPREDUCE-4972](https://issues.apache.org/jira/browse/MAPREDUCE-4972) | Coverage fixing for org.apache.hadoop.mapreduce.jobhistory |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [MAPREDUCE-4875](https://issues.apache.org/jira/browse/MAPREDUCE-4875) | coverage fixing for org.apache.hadoop.mapred |  Major | test | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-866](https://issues.apache.org/jira/browse/YARN-866) | Add test for class ResourceWeights |  Major | . | Wei Yan | Wei Yan |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9720](https://issues.apache.org/jira/browse/HADOOP-9720) | Rename Client#uuid to Client#clientId |  Major | . | Suresh Srinivas | Arpit Agarwal |
| [HADOOP-9619](https://issues.apache.org/jira/browse/HADOOP-9619) | Mark stability of .proto files |  Major | documentation | Sanjay Radia | Sanjay Radia |
| [HADOOP-9418](https://issues.apache.org/jira/browse/HADOOP-9418) | Add symlink resolution support to DistributedFileSystem |  Major | fs | Andrew Wang | Andrew Wang |
| [HADOOP-9416](https://issues.apache.org/jira/browse/HADOOP-9416) | Add new symlink resolution methods in FileSystem and FileSystemLinkResolver |  Major | fs | Andrew Wang | Andrew Wang |
| [HADOOP-9414](https://issues.apache.org/jira/browse/HADOOP-9414) | Refactor out FSLinkResolver and relevant helper methods |  Major | fs | Andrew Wang | Andrew Wang |
| [HADOOP-9355](https://issues.apache.org/jira/browse/HADOOP-9355) | Abstract symlink tests to use either FileContext or FileSystem |  Major | fs | Andrew Wang | Andrew Wang |
| [HADOOP-9258](https://issues.apache.org/jira/browse/HADOOP-9258) | Add stricter tests to FileSystemContractTestBase |  Major | test | Steve Loughran | Steve Loughran |
| [HADOOP-9218](https://issues.apache.org/jira/browse/HADOOP-9218) | Document the Rpc-wrappers used internally |  Major | ipc | Sanjay Radia | Sanjay Radia |
| [HADOOP-9140](https://issues.apache.org/jira/browse/HADOOP-9140) | Cleanup rpc PB protos |  Major | ipc | Sanjay Radia | Sanjay Radia |
| [HADOOP-8470](https://issues.apache.org/jira/browse/HADOOP-8470) | Implementation of 4-layer subclass of NetworkTopology (NetworkTopologyWithNodeGroup) |  Major | . | Junping Du | Junping Du |
| [HADOOP-8469](https://issues.apache.org/jira/browse/HADOOP-8469) | Make NetworkTopology class pluggable |  Major | . | Junping Du | Junping Du |
| [HDFS-5025](https://issues.apache.org/jira/browse/HDFS-5025) | Record ClientId and CallId in EditLog to enable rebuilding retry cache in case of HA failover |  Major | ha, namenode | Jing Zhao | Jing Zhao |
| [HDFS-4979](https://issues.apache.org/jira/browse/HDFS-4979) | Implement retry cache on the namenode |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4974](https://issues.apache.org/jira/browse/HDFS-4974) | Analyze and add annotations to Namenode protocol methods and enable retry |  Major | ha, namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4908](https://issues.apache.org/jira/browse/HDFS-4908) | Reduce snapshot inode memory usage |  Major | namenode, snapshots | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4876](https://issues.apache.org/jira/browse/HDFS-4876) | The javadoc of FileWithSnapshot is incorrect |  Minor | snapshots | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4875](https://issues.apache.org/jira/browse/HDFS-4875) | Add a test for testing snapshot file length |  Minor | snapshots, test | Tsz Wo Nicholas Sze | Arpit Agarwal |
| [HDFS-4842](https://issues.apache.org/jira/browse/HDFS-4842) | Snapshot: identify the correct prior snapshot when deleting a snapshot under a renamed subtree |  Major | snapshots | Jing Zhao | Jing Zhao |
| [HDFS-4819](https://issues.apache.org/jira/browse/HDFS-4819) | Update Snapshot doc for HDFS-4758 |  Minor | documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4785](https://issues.apache.org/jira/browse/HDFS-4785) | Concat operation does not remove concatenated files from InodeMap |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4784](https://issues.apache.org/jira/browse/HDFS-4784) | NPE in FSDirectory.resolvePath() |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-4762](https://issues.apache.org/jira/browse/HDFS-4762) | Provide HDFS based NFSv3 and Mountd implementation |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-4661](https://issues.apache.org/jira/browse/HDFS-4661) | fix various bugs in short circuit read |  Major | datanode, hdfs-client | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4602](https://issues.apache.org/jira/browse/HDFS-4602) | TestBookKeeperHACheckpoints fails |  Major | . | Suresh Srinivas | Uma Maheswara Rao G |
| [HDFS-4542](https://issues.apache.org/jira/browse/HDFS-4542) | Webhdfs doesn't support secure proxy users |  Blocker | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-4525](https://issues.apache.org/jira/browse/HDFS-4525) | Provide an API for knowing that whether file is closed or not. |  Major | namenode | Uma Maheswara Rao G | SreeHari |
| [HDFS-4502](https://issues.apache.org/jira/browse/HDFS-4502) | WebHdfsFileSystem handling of fileld breaks compatibility |  Blocker | webhdfs | Alejandro Abdelnur | Brandon Li |
| [HDFS-4485](https://issues.apache.org/jira/browse/HDFS-4485) | HDFS-347: DN should chmod socket path a+w |  Critical | datanode | Todd Lipcon | Colin Patrick McCabe |
| [HDFS-4374](https://issues.apache.org/jira/browse/HDFS-4374) | Display NameNode startup progress in UI |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-4373](https://issues.apache.org/jira/browse/HDFS-4373) | Add HTTP API for querying NameNode startup progress |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-4372](https://issues.apache.org/jira/browse/HDFS-4372) | Track NameNode startup progress |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-4346](https://issues.apache.org/jira/browse/HDFS-4346) | Refactor INodeId and GenerationStamp |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4340](https://issues.apache.org/jira/browse/HDFS-4340) | Update addBlock() to inculde inode id as additional argument |  Major | hdfs-client, namenode | Brandon Li | Brandon Li |
| [HDFS-4339](https://issues.apache.org/jira/browse/HDFS-4339) | Persist inode id in fsimage and editlog |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-4334](https://issues.apache.org/jira/browse/HDFS-4334) | Add a unique id to each INode |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-3769](https://issues.apache.org/jira/browse/HDFS-3769) | standby namenode become active fails because starting log segment fail on shared storage |  Critical | ha | liaowenrui |  |
| [MAPREDUCE-5299](https://issues.apache.org/jira/browse/MAPREDUCE-5299) | Mapred API: void setTaskID(TaskAttemptID) is missing in TaskCompletionEvent |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5296](https://issues.apache.org/jira/browse/MAPREDUCE-5296) | Mapred API: Function signature change in JobControl |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5280](https://issues.apache.org/jira/browse/MAPREDUCE-5280) | Mapreduce API: ClusterMetrics incompatibility issues with MR1 |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5275](https://issues.apache.org/jira/browse/MAPREDUCE-5275) | Mapreduce API: TokenCache incompatibility issues with MR1 |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5274](https://issues.apache.org/jira/browse/MAPREDUCE-5274) | Mapreduce API: String toHex(byte[]) is removed from SecureShuffleUtils |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5273](https://issues.apache.org/jira/browse/MAPREDUCE-5273) | Protected variables are removed from CombineFileRecordReader in both mapred and mapreduce |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5263](https://issues.apache.org/jira/browse/MAPREDUCE-5263) | filecache.DistributedCache incompatiblity issues with MR1 |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5245](https://issues.apache.org/jira/browse/MAPREDUCE-5245) | A number of public static variables are removed from JobConf |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5244](https://issues.apache.org/jira/browse/MAPREDUCE-5244) | Two functions changed their visibility in JobStatus |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5235](https://issues.apache.org/jira/browse/MAPREDUCE-5235) | mapred.Counters incompatiblity issues with MR1 |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5231](https://issues.apache.org/jira/browse/MAPREDUCE-5231) | Constructor of DBInputFormat.DBRecordReader in mapred is changed |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5230](https://issues.apache.org/jira/browse/MAPREDUCE-5230) | createFileSplit is removed from NLineInputFormat of mapred |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5229](https://issues.apache.org/jira/browse/MAPREDUCE-5229) | TEMP\_DIR\_NAME is removed from of FileOutputCommitter of mapreduce |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5228](https://issues.apache.org/jira/browse/MAPREDUCE-5228) | Enum Counter is removed from FileInputFormat and FileOutputFormat of both mapred and mapreduce |  Major | . | Zhijie Shen | Mayank Bansal |
| [MAPREDUCE-5222](https://issues.apache.org/jira/browse/MAPREDUCE-5222) | Fix JobClient incompatibilities with MR1 |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5220](https://issues.apache.org/jira/browse/MAPREDUCE-5220) | Mapred API: TaskCompletionEvent incompatibility issues with MR1 |  Major | client | Sandy Ryza | Zhijie Shen |
| [MAPREDUCE-5199](https://issues.apache.org/jira/browse/MAPREDUCE-5199) | AppTokens file can/should be removed |  Blocker | security | Vinod Kumar Vavilapalli | Daryn Sharp |
| [MAPREDUCE-5184](https://issues.apache.org/jira/browse/MAPREDUCE-5184) | Document MR Binary Compatibility vis-a-vis hadoop-1 and hadoop-2 |  Major | documentation | Arun C Murthy | Zhijie Shen |
| [MAPREDUCE-5159](https://issues.apache.org/jira/browse/MAPREDUCE-5159) | Aggregatewordcount and aggregatewordhist in hadoop-1 examples are not binary compatible with hadoop-2 mapred.lib.aggregate |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5157](https://issues.apache.org/jira/browse/MAPREDUCE-5157) | Sort in hadoop-1 examples is not binary compatible with hadoop-2 mapred.lib |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5086](https://issues.apache.org/jira/browse/MAPREDUCE-5086) | MR app master deletes staging dir when sent a reboot command from the RM |  Major | . | Jian He | Jian He |
| [MAPREDUCE-4951](https://issues.apache.org/jira/browse/MAPREDUCE-4951) | Container preemption interpreted as task failure |  Major | applicationmaster, mr-am, mrv2 | Sandy Ryza | Sandy Ryza |
| [YARN-961](https://issues.apache.org/jira/browse/YARN-961) | ContainerManagerImpl should enforce token on server. Today it is [TOKEN, SIMPLE] |  Blocker | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-922](https://issues.apache.org/jira/browse/YARN-922) | Change FileSystemRMStateStore to use directories |  Major | resourcemanager | Jian He | Jian He |
| [YARN-877](https://issues.apache.org/jira/browse/YARN-877) | Allow for black-listing resources in FifoScheduler |  Major | scheduler | Junping Du | Junping Du |
| [YARN-873](https://issues.apache.org/jira/browse/YARN-873) | YARNClient.getApplicationReport(unknownAppId) returns a null report |  Major | . | Bikas Saha | Xuan Gong |
| [YARN-850](https://issues.apache.org/jira/browse/YARN-850) | Rename getClusterAvailableResources to getAvailableResources in AMRMClients |  Major | . | Jian He | Jian He |
| [YARN-846](https://issues.apache.org/jira/browse/YARN-846) | Move pb Impl from yarn-api to yarn-common |  Major | . | Jian He | Jian He |
| [YARN-845](https://issues.apache.org/jira/browse/YARN-845) | RM crash with NPE on NODE\_UPDATE |  Major | resourcemanager | Arpit Gupta | Mayank Bansal |
| [YARN-827](https://issues.apache.org/jira/browse/YARN-827) | Need to make Resource arithmetic methods accessible |  Critical | . | Bikas Saha | Jian He |
| [YARN-825](https://issues.apache.org/jira/browse/YARN-825) | Fix yarn-common javadoc annotations |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-820](https://issues.apache.org/jira/browse/YARN-820) | NodeManager has invalid state transition after error in resource localization |  Major | . | Bikas Saha | Mayank Bansal |
| [YARN-814](https://issues.apache.org/jira/browse/YARN-814) | Difficult to diagnose a failed container launch when error due to invalid environment variable |  Major | . | Hitesh Shah | Jian He |
| [YARN-805](https://issues.apache.org/jira/browse/YARN-805) | Fix yarn-api javadoc annotations |  Blocker | . | Jian He | Jian He |
| [YARN-781](https://issues.apache.org/jira/browse/YARN-781) | Expose LOGDIR that containers should use for logging |  Major | . | Devaraj Das | Jian He |
| [YARN-773](https://issues.apache.org/jira/browse/YARN-773) | Move YarnRuntimeException from package api.yarn to api.yarn.exceptions |  Major | . | Jian He | Jian He |
| [YARN-759](https://issues.apache.org/jira/browse/YARN-759) | Create Command enum in AllocateResponse |  Major | . | Bikas Saha | Bikas Saha |
| [YARN-750](https://issues.apache.org/jira/browse/YARN-750) | Allow for black-listing resources in YARN API and Impl in CS |  Major | . | Arun C Murthy | Arun C Murthy |
| [YARN-739](https://issues.apache.org/jira/browse/YARN-739) | NM startContainer should validate the NodeId |  Major | . | Siddharth Seth | Omkar Vinit Joshi |
| [YARN-737](https://issues.apache.org/jira/browse/YARN-737) | Some Exceptions no longer need to be wrapped by YarnException and can be directly thrown out after YARN-142 |  Major | . | Jian He | Jian He |
| [YARN-731](https://issues.apache.org/jira/browse/YARN-731) | RPCUtil.unwrapAndThrowException should unwrap remote RuntimeExceptions |  Major | . | Siddharth Seth | Zhijie Shen |
| [YARN-727](https://issues.apache.org/jira/browse/YARN-727) | ClientRMProtocol.getAllApplications should accept ApplicationType as a parameter |  Blocker | . | Siddharth Seth | Xuan Gong |
| [YARN-719](https://issues.apache.org/jira/browse/YARN-719) | Move RMIdentifier from Container to ContainerTokenIdentifier |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-717](https://issues.apache.org/jira/browse/YARN-717) | Copy BuilderUtil methods into token-related records |  Major | . | Jian He | Jian He |
| [YARN-714](https://issues.apache.org/jira/browse/YARN-714) | AMRM protocol changes for sending NMToken list |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-711](https://issues.apache.org/jira/browse/YARN-711) | Copy BuilderUtil methods into individual records |  Major | . | Vinod Kumar Vavilapalli | Jian He |
| [YARN-695](https://issues.apache.org/jira/browse/YARN-695) | masterContainer and status are in ApplicationReportProto but not in ApplicationReport |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-693](https://issues.apache.org/jira/browse/YARN-693) | Sending NMToken to AM on allocate call |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-692](https://issues.apache.org/jira/browse/YARN-692) | Creating NMToken master key on RM and sharing it with NM as a part of RM-NM heartbeat. |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-686](https://issues.apache.org/jira/browse/YARN-686) | Flatten NodeReport |  Major | api | Sandy Ryza | Sandy Ryza |
| [YARN-663](https://issues.apache.org/jira/browse/YARN-663) | Change ResourceTracker API and LocalizationProtocol API to throw YarnRemoteException and IOException |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-660](https://issues.apache.org/jira/browse/YARN-660) | Improve AMRMClient with matching requests |  Major | . | Bikas Saha | Bikas Saha |
| [YARN-651](https://issues.apache.org/jira/browse/YARN-651) | Change ContainerManagerPBClientImpl and RMAdminProtocolPBClientImpl to throw IOException and YarnRemoteException |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-638](https://issues.apache.org/jira/browse/YARN-638) | Restore RMDelegationTokens after RM Restart |  Major | resourcemanager | Jian He | Jian He |
| [YARN-634](https://issues.apache.org/jira/browse/YARN-634) | Make YarnRemoteException not backed by PB and introduce a SerializedException |  Major | . | Siddharth Seth | Siddharth Seth |
| [YARN-628](https://issues.apache.org/jira/browse/YARN-628) | Fix YarnException unwrapping |  Major | . | Siddharth Seth | Siddharth Seth |
| [YARN-625](https://issues.apache.org/jira/browse/YARN-625) | Move unwrapAndThrowException from YarnRemoteExceptionPBImpl to RPCUtil |  Major | . | Siddharth Seth | Siddharth Seth |
| [YARN-617](https://issues.apache.org/jira/browse/YARN-617) | In unsercure mode, AM can fake resource requirements |  Minor | . | Vinod Kumar Vavilapalli | Omkar Vinit Joshi |
| [YARN-613](https://issues.apache.org/jira/browse/YARN-613) | Create NM proxy per NM instead of per container |  Major | . | Bikas Saha | Omkar Vinit Joshi |
| [YARN-595](https://issues.apache.org/jira/browse/YARN-595) | Refactor fair scheduler to use common Resources |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-591](https://issues.apache.org/jira/browse/YARN-591) | RM recovery related records do not belong to the API |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-583](https://issues.apache.org/jira/browse/YARN-583) | Application cache files should be localized under local-dir/usercache/userid/appcache/appid/filecache |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-582](https://issues.apache.org/jira/browse/YARN-582) | Restore appToken and clientToken for app attempt after RM restart |  Major | resourcemanager | Bikas Saha | Jian He |
| [YARN-581](https://issues.apache.org/jira/browse/YARN-581) | Test and verify that app delegation tokens are added to tokenRenewer after RM restart |  Major | resourcemanager | Bikas Saha | Jian He |
| [YARN-578](https://issues.apache.org/jira/browse/YARN-578) | NodeManager should use SecureIOUtils for serving and aggregating logs |  Major | nodemanager | Vinod Kumar Vavilapalli | Omkar Vinit Joshi |
| [YARN-577](https://issues.apache.org/jira/browse/YARN-577) | ApplicationReport does not provide progress value of application |  Major | . | Hitesh Shah | Hitesh Shah |
| [YARN-569](https://issues.apache.org/jira/browse/YARN-569) | CapacityScheduler: support for preemption (using a capacity monitor) |  Major | capacityscheduler | Carlo Curino | Carlo Curino |
| [YARN-568](https://issues.apache.org/jira/browse/YARN-568) | FairScheduler: support for work-preserving preemption |  Major | scheduler | Carlo Curino | Carlo Curino |
| [YARN-567](https://issues.apache.org/jira/browse/YARN-567) | RM changes to support preemption for FairScheduler and CapacityScheduler |  Major | resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-563](https://issues.apache.org/jira/browse/YARN-563) | Add application type to ApplicationReport |  Major | . | Thomas Weise | Mayank Bansal |
| [YARN-562](https://issues.apache.org/jira/browse/YARN-562) | NM should reject containers allocated by previous RM |  Major | resourcemanager | Jian He | Jian He |
| [YARN-549](https://issues.apache.org/jira/browse/YARN-549) | YarnClient.submitApplication should wait for application to be accepted by the RM |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-548](https://issues.apache.org/jira/browse/YARN-548) | Add tests for YarnUncaughtExceptionHandler |  Major | . | Vadim Bondarev | Vadim Bondarev |
| [YARN-547](https://issues.apache.org/jira/browse/YARN-547) | Race condition in Public / Private Localizer may result into resource getting downloaded again |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-539](https://issues.apache.org/jira/browse/YARN-539) | LocalizedResources are leaked in memory in case resource localization fails |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-534](https://issues.apache.org/jira/browse/YARN-534) | AM max attempts is not checked when RM restart and try to recover attempts |  Major | resourcemanager | Jian He | Jian He |
| [YARN-523](https://issues.apache.org/jira/browse/YARN-523) | Container localization failures aren't reported from NM to RM |  Major | . | Vinod Kumar Vavilapalli | Jian He |
| [YARN-521](https://issues.apache.org/jira/browse/YARN-521) | Augment AM - RM client module to be able to request containers only at specific locations |  Major | api | Sandy Ryza | Sandy Ryza |
| [YARN-514](https://issues.apache.org/jira/browse/YARN-514) | Delayed store operations should not result in RM unavailability for app submission |  Major | resourcemanager | Bikas Saha | Zhijie Shen |
| [YARN-513](https://issues.apache.org/jira/browse/YARN-513) | Create common proxy client for communicating with RM |  Major | resourcemanager | Bikas Saha | Jian He |
| [YARN-486](https://issues.apache.org/jira/browse/YARN-486) | Change startContainer NM API to accept Container as a parameter and make ContainerLaunchContext user land |  Major | . | Bikas Saha | Xuan Gong |
| [YARN-482](https://issues.apache.org/jira/browse/YARN-482) | FS: Extend SchedulingMode to intermediate queues |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-475](https://issues.apache.org/jira/browse/YARN-475) | Remove ApplicationConstants.AM\_APP\_ATTEMPT\_ID\_ENV as it is no longer set in an AM's environment |  Major | . | Hitesh Shah | Hitesh Shah |
| [YARN-469](https://issues.apache.org/jira/browse/YARN-469) | Make scheduling mode in FS pluggable |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-468](https://issues.apache.org/jira/browse/YARN-468) | coverage fix for org.apache.hadoop.yarn.server.webproxy.amfilter |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-467](https://issues.apache.org/jira/browse/YARN-467) | Jobs fail during resource localization when public distributed-cache hits unix directory limits |  Major | nodemanager | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-450](https://issues.apache.org/jira/browse/YARN-450) | Define value for \* in the scheduling protocol |  Major | . | Bikas Saha | Zhijie Shen |
| [YARN-444](https://issues.apache.org/jira/browse/YARN-444) | Move special container exit codes from YarnConfiguration to API |  Major | api, applications/distributed-shell | Sandy Ryza | Sandy Ryza |
| [YARN-441](https://issues.apache.org/jira/browse/YARN-441) | Clean up unused collection methods in various APIs |  Major | . | Siddharth Seth | Xuan Gong |
| [YARN-422](https://issues.apache.org/jira/browse/YARN-422) | Add NM client library |  Major | . | Bikas Saha | Zhijie Shen |
| [YARN-417](https://issues.apache.org/jira/browse/YARN-417) | Create AMRMClient wrapper that provides asynchronous callbacks |  Major | api, applications | Sandy Ryza | Sandy Ryza |
| [YARN-398](https://issues.apache.org/jira/browse/YARN-398) | Enhance CS to allow for white-list of resources |  Major | . | Arun C Murthy | Arun C Murthy |
| [YARN-392](https://issues.apache.org/jira/browse/YARN-392) | Make it possible to specify hard locality constraints in resource requests |  Major | resourcemanager | Bikas Saha | Sandy Ryza |
| [YARN-378](https://issues.apache.org/jira/browse/YARN-378) | ApplicationMaster retry times should be set by Client |  Major | client, resourcemanager | xieguiming | Zhijie Shen |
| [YARN-369](https://issues.apache.org/jira/browse/YARN-369) | Handle ( or throw a proper error when receiving) status updates from application masters that have not registered |  Major | resourcemanager | Hitesh Shah | Mayank Bansal |
| [YARN-365](https://issues.apache.org/jira/browse/YARN-365) | Each NM heartbeat should not generate an event for the Scheduler |  Major | resourcemanager, scheduler | Siddharth Seth | Xuan Gong |
| [YARN-309](https://issues.apache.org/jira/browse/YARN-309) | Make RM provide heartbeat interval to NM |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-295](https://issues.apache.org/jira/browse/YARN-295) | Resource Manager throws InvalidStateTransitonException: Invalid event: CONTAINER\_FINISHED at ALLOCATED for RMAppAttemptImpl |  Major | resourcemanager | Devaraj K | Mayank Bansal |
| [YARN-200](https://issues.apache.org/jira/browse/YARN-200) | yarn log does not output all needed information, and is in a binary format |  Major | . | Robert Joseph Evans | Ravi Prakash |
| [YARN-125](https://issues.apache.org/jira/browse/YARN-125) | Make Yarn Client service shutdown operations robust |  Minor | . | Steve Loughran | Steve Loughran |
| [YARN-124](https://issues.apache.org/jira/browse/YARN-124) | Make Yarn Node Manager services robust against shutdown |  Minor | . | Steve Loughran | Steve Loughran |
| [YARN-123](https://issues.apache.org/jira/browse/YARN-123) | Make yarn Resource Manager services robust against shutdown |  Minor | . | Steve Loughran | Steve Loughran |
| [YARN-112](https://issues.apache.org/jira/browse/YARN-112) | Race in localization can cause containers to fail |  Major | nodemanager | Jason Lowe | Omkar Vinit Joshi |
| [YARN-99](https://issues.apache.org/jira/browse/YARN-99) | Jobs fail during resource localization when private distributed-cache hits unix directory limits |  Major | nodemanager | Devaraj K | Omkar Vinit Joshi |
| [YARN-62](https://issues.apache.org/jira/browse/YARN-62) | AM should not be able to abuse container tokens for repetitive container launches |  Major | . | Vinod Kumar Vavilapalli | Omkar Vinit Joshi |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-5286](https://issues.apache.org/jira/browse/MAPREDUCE-5286) | startContainer call should use the ContainerToken instead of Container [YARN-684] |  Major | . | Siddharth Seth | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5194](https://issues.apache.org/jira/browse/MAPREDUCE-5194) | Heed interrupts during Fetcher shutdown |  Minor | task | Chris Douglas | Chris Douglas |
| [MAPREDUCE-5192](https://issues.apache.org/jira/browse/MAPREDUCE-5192) | Separate TCE resolution from fetch |  Minor | task | Chris Douglas | Chris Douglas |
| [MAPREDUCE-3502](https://issues.apache.org/jira/browse/MAPREDUCE-3502) | Review all Service.stop() operations and make sure that they work before a service is started |  Major | mrv2 | Steve Loughran | Steve Loughran |
| [YARN-927](https://issues.apache.org/jira/browse/YARN-927) | Change ContainerRequest to not have more than 1 container count and remove StoreContainerRequest |  Major | . | Bikas Saha | Bikas Saha |
| [YARN-708](https://issues.apache.org/jira/browse/YARN-708) | Move RecordFactory classes to hadoop-yarn-api, miscellaneous fixes to the interfaces |  Major | . | Siddharth Seth | Siddharth Seth |
| [YARN-142](https://issues.apache.org/jira/browse/YARN-142) | [Umbrella] Cleanup YARN APIs w.r.t exceptions |  Blocker | . | Siddharth Seth |  |


