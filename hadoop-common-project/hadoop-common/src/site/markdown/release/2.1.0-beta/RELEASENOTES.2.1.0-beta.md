
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
# Apache Hadoop  2.1.0-beta Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-9832](https://issues.apache.org/jira/browse/HADOOP-9832) | *Blocker* | **Add RPC header to client ping**

Client ping will be sent as a RPC header with a reserved callId instead of as a sentinel RPC packet length.


---

* [HADOOP-9820](https://issues.apache.org/jira/browse/HADOOP-9820) | *Blocker* | **RPCv9 wire protocol is insufficient to support multiplexing**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-9698](https://issues.apache.org/jira/browse/HADOOP-9698) | *Blocker* | **RPCv9 client must honor server's SASL negotiate response**

The RPC client now waits for the Server's SASL negotiate response before instantiating its SASL client.


---

* [HADOOP-9683](https://issues.apache.org/jira/browse/HADOOP-9683) | *Blocker* | **Wrap IpcConnectionContext in RPC headers**

Connection context is now sent as a rpc header wrapped protobuf.


---

* [HADOOP-9649](https://issues.apache.org/jira/browse/HADOOP-9649) | *Blocker* | **Promote YARN service life-cycle libraries into Hadoop Common**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-9630](https://issues.apache.org/jira/browse/HADOOP-9630) | *Major* | **Remove IpcSerializationType**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-9425](https://issues.apache.org/jira/browse/HADOOP-9425) | *Major* | **Add error codes to rpc-response**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-9421](https://issues.apache.org/jira/browse/HADOOP-9421) | *Blocker* | **Convert SASL to use ProtoBuf and provide negotiation capabilities**

Raw SASL protocol now uses protobufs wrapped with RPC headers.
The negotiation sequence incorporates the state of the exchange.
The server now has the ability to advertise its supported auth types.


---

* [HADOOP-9380](https://issues.apache.org/jira/browse/HADOOP-9380) | *Major* | **Add totalLength to rpc response**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-9228](https://issues.apache.org/jira/browse/HADOOP-9228) | *Minor* | **FileSystemContractTestBase never verifies that files are files**

fixed in HADOOP-9258


---

* [HADOOP-9227](https://issues.apache.org/jira/browse/HADOOP-9227) | *Trivial* | **FileSystemContractBaseTest doesn't test filesystem's mkdir/isDirectory() logic rigorously enough**

fixed in HADOOP-9258


---

* [HADOOP-9194](https://issues.apache.org/jira/browse/HADOOP-9194) | *Major* | **RPC Support for QoS**

Part of the RPC version 9 change. A service class byte is added after the version byte.


---

* [HADOOP-9163](https://issues.apache.org/jira/browse/HADOOP-9163) | *Major* | **The rpc msg in  ProtobufRpcEngine.proto should be moved out to avoid an extra copy**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-9151](https://issues.apache.org/jira/browse/HADOOP-9151) | *Major* | **Include RPC error info in RpcResponseHeader instead of sending it separately**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-8886](https://issues.apache.org/jira/browse/HADOOP-8886) | *Major* | **Remove KFS support**

Kosmos FS (KFS) is no longer maintained and Hadoop support has been removed. KFS has been replaced by QFS (HADOOP-8885).


---

* [HADOOP-8562](https://issues.apache.org/jira/browse/HADOOP-8562) | *Major* | **Enhancements to support Hadoop on Windows Server and Windows Azure environments**

This umbrella jira makes enhancements to support Hadoop natively on Windows Server and Windows Azure environments.


---

* [HADOOP-8470](https://issues.apache.org/jira/browse/HADOOP-8470) | *Major* | **Implementation of 4-layer subclass of NetworkTopology (NetworkTopologyWithNodeGroup)**

This patch should be checked in together (or after) with JIRA Hadoop-8469: https://issues.apache.org/jira/browse/HADOOP-8469


---

* [HDFS-5083](https://issues.apache.org/jira/browse/HDFS-5083) | *Blocker* | **Update the HDFS compatibility version range**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-4996](https://issues.apache.org/jira/browse/HDFS-4996) | *Minor* | **ClientProtocol#metaSave can be made idempotent by overwriting the output file instead of appending to it**

The dfsadmin -metasave command has been changed to overwrite the output file.  Previously, this command would append to the output file if it already existed.


---

* [HDFS-4866](https://issues.apache.org/jira/browse/HDFS-4866) | *Blocker* | **Protocol buffer support cannot compile under C**

The Protocol Buffers definition of the inter-namenode protocol required a change for compatibility with compiled C clients.  This is a backwards-incompatible change.  A namenode prior to this change will not be able to communicate with a namenode after this change.


---

* [HDFS-4832](https://issues.apache.org/jira/browse/HDFS-4832) | *Critical* | **Namenode doesn't change the number of missing blocks in safemode when DNs rejoin or leave**

This change makes name node keep its internal replication queues and data node state updated in manual safe mode. This allows metrics and UI to present up-to-date information while in safe mode. The behavior during start-up safe mode is unchanged.


---

* [HDFS-4659](https://issues.apache.org/jira/browse/HDFS-4659) | *Major* | **Support setting execution bit for regular files**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-4519](https://issues.apache.org/jira/browse/HDFS-4519) | *Major* | **Support override of jsvc binary and log file locations when launching secure datanode.**

With this improvement the following options are available in release 1.2.0 and later on 1.x release stream:
1. jsvc location can be overridden by setting environment variable JSVC\_HOME. Defaults to jsvc binary packaged within the Hadoop distro.
2. jsvc log output is directed to the file defined by JSVC\_OUTFILE. Defaults to $HADOOP\_LOG\_DIR/jsvc.out.
3. jsvc error output is directed to the file defined by JSVC\_ERRFILE file.  Defaults to $HADOOP\_LOG\_DIR/jsvc.err.

With this improvement the following options are available in release 2.0.4 and later on 2.x release stream:
1. jsvc log output is directed to the file defined by JSVC\_OUTFILE. Defaults to $HADOOP\_LOG\_DIR/jsvc.out.
2. jsvc error output is directed to the file defined by JSVC\_ERRFILE file.  Defaults to $HADOOP\_LOG\_DIR/jsvc.err.

For overriding jsvc location on 2.x releases, here is the release notes from HDFS-2303:
To run secure Datanodes users must install jsvc for their platform and set JSVC\_HOME to point to the location of jsvc in their environment.


---

* [HDFS-4434](https://issues.apache.org/jira/browse/HDFS-4434) | *Major* | **Provide a mapping from INodeId to INode**

This change adds support for referencing files and directories based on fileID/inodeID using a path /.reserved/.inodes/\<inodeid\>. With this change creating a file or directory /.reserved is not longer allowed. Before upgrading to a release with this change, files /.reserved needs to be renamed to another name.


---

* [HDFS-4305](https://issues.apache.org/jira/browse/HDFS-4305) | *Minor* | **Add a configurable limit on number of blocks per file, and min block size**

This change introduces a maximum number of blocks per file, by default one million, and a minimum block size, by default 1MB. These can optionally be changed via the configuration settings "dfs.namenode.fs-limits.max-blocks-per-file" and "dfs.namenode.fs-limits.min-block-size", respectively.


---

* [HDFS-4053](https://issues.apache.org/jira/browse/HDFS-4053) | *Major* | **Increase the default block size**

The default blocks size prior to this change was 64MB. This jira changes the default block size to 128MB. To go back to previous behavior, please configure the in hdfs-site.xml, the configuration parameter "dfs.blocksize" to 67108864.


---

* [HDFS-2802](https://issues.apache.org/jira/browse/HDFS-2802) | *Major* | **Support for RW/RO snapshots in HDFS**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-1804](https://issues.apache.org/jira/browse/HDFS-1804) | *Minor* | **Add a new block-volume device choosing policy that looks at free space**

There is now a new option to have the DN take into account available disk space on each volume when choosing where to place a replica when performing an HDFS write. This can be enabled by setting the config "dfs.datanode.fsdataset.volume.choosing.policy" to the value "org.apache.hadoop.hdfs.server.datanode.fsdataset.AvailableSpaceVolumeChoosingPolicy".


---

* [MAPREDUCE-5399](https://issues.apache.org/jira/browse/MAPREDUCE-5399) | *Blocker* | **Unnecessary Configuration instantiation in IFileInputStream slows down merge**

Fixes blank Configuration object creation overhead by reusing the Job configuration in InMemoryReader.


---

* [MAPREDUCE-5304](https://issues.apache.org/jira/browse/MAPREDUCE-5304) | *Blocker* | **mapreduce.Job killTask/failTask/getTaskCompletionEvents methods have incompatible signature changes**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-5300](https://issues.apache.org/jira/browse/MAPREDUCE-5300) | *Major* | **Two function signature changes in filecache.DistributedCache**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-5237](https://issues.apache.org/jira/browse/MAPREDUCE-5237) | *Major* | **ClusterStatus incompatiblity issues with MR1**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-5234](https://issues.apache.org/jira/browse/MAPREDUCE-5234) | *Major* | **Signature changes for getTaskId of TaskReport in mapred**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-5233](https://issues.apache.org/jira/browse/MAPREDUCE-5233) | *Major* | **Functions are changed or removed from Job in jobcontrol**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-5184](https://issues.apache.org/jira/browse/MAPREDUCE-5184) | *Major* | **Document MR Binary Compatibility vis-a-vis hadoop-1 and hadoop-2**

Document MR Binary Compatibility vis-a-vis hadoop-1 and hadoop-2 for end-users.


---

* [MAPREDUCE-5176](https://issues.apache.org/jira/browse/MAPREDUCE-5176) | *Major* | **Preemptable annotations (to support preemption in MR)**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-5156](https://issues.apache.org/jira/browse/MAPREDUCE-5156) | *Blocker* | **Hadoop-examples-1.x.x.jar cannot run on Yarn**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-4942](https://issues.apache.org/jira/browse/MAPREDUCE-4942) | *Major* | **mapreduce.Job has a bunch of methods that throw InterruptedException so its incompatible with MR1**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-4737](https://issues.apache.org/jira/browse/MAPREDUCE-4737) | *Major* | ** Hadoop does not close output file / does not call Mapper.cleanup if exception in map**

Ensure that mapreduce APIs are semantically consistent with mapred API w.r.t Mapper.cleanup and Reducer.cleanup; in the sense that cleanup is now called even if there is an error. The old mapred API already ensures that Mapper.close and Reducer.close are invoked during error handling. Note that it is an incompatible change, however end-users can override Mapper.run and Reducer.run to get the old (inconsistent) behaviour.


---

* [MAPREDUCE-4356](https://issues.apache.org/jira/browse/MAPREDUCE-4356) | *Major* | **Provide access to ParsedTask.obtainTaskAttempts()**

Made the method ParsedTask.obtainTaskAttempts() public.


---

* [MAPREDUCE-4149](https://issues.apache.org/jira/browse/MAPREDUCE-4149) | *Major* | **Rumen fails to parse certain counter strings**

Fixes Rumen to parse counter strings containing the special characters "{" and "}".


---

* [MAPREDUCE-4100](https://issues.apache.org/jira/browse/MAPREDUCE-4100) | *Minor* | **Sometimes gridmix emulates data larger much larger then acutal counter for map only jobs**

Bug fixed in compression emulation feature for map only jobs.


---

* [MAPREDUCE-4083](https://issues.apache.org/jira/browse/MAPREDUCE-4083) | *Major* | **GridMix emulated job tasks.resource-usage emulator for CPU usage throws NPE when Trace contains cumulativeCpuUsage value of 0 at attempt level**

Fixes NPE in cpu emulation in Gridmix


---

* [MAPREDUCE-4067](https://issues.apache.org/jira/browse/MAPREDUCE-4067) | *Critical* | **Replace YarnRemoteException with IOException in MRv2 APIs**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-3953](https://issues.apache.org/jira/browse/MAPREDUCE-3953) | *Major* | **Gridmix throws NPE and does not simulate a job if the trace contains null taskStatus for a task**

Fixes NPE and makes Gridmix simulate succeeded-jobs-with-failed-tasks. All tasks of such simulated jobs(including the failed ones of original job) will succeed.


---

* [MAPREDUCE-3829](https://issues.apache.org/jira/browse/MAPREDUCE-3829) | *Major* | **[Gridmix] Gridmix should give better error message when input-data directory already exists and -generate option is given**

Makes Gridmix emit out correct error message when the input data directory already exists and -generate option is used. Makes Gridmix exit with proper exit codes when Gridmix fails in args-processing, startup/setup.


---

* [MAPREDUCE-3787](https://issues.apache.org/jira/browse/MAPREDUCE-3787) | *Major* | **[Gridmix] Improve STRESS mode**

JobMonitor can now deploy multiple threads for faster job-status polling. Use 'gridmix.job-monitor.thread-count' to set the number of threads. Stress mode now relies on the updates from the job monitor instead of polling for job status. Failures in job submission now get reported to the statistics module and ultimately reported to the user via summary.


---

* [MAPREDUCE-3757](https://issues.apache.org/jira/browse/MAPREDUCE-3757) | *Major* | **Rumen Folder is not adjusting the shuffleFinished and sortFinished times of reduce task attempts**

Fixed the sortFinishTime and shuffleFinishTime adjustments in Rumen Folder.


---

* [MAPREDUCE-2722](https://issues.apache.org/jira/browse/MAPREDUCE-2722) | *Major* | **Gridmix simulated job's map's hdfsBytesRead counter is wrong when compressed input is used**

Makes Gridmix use the uncompressed input data size while simulating map tasks in the case where compressed input data was used in original job.


---

* [YARN-1056](https://issues.apache.org/jira/browse/YARN-1056) | *Trivial* | **Fix configs yarn.resourcemanager.resourcemanager.connect.{max.wait.secs\|retry\_interval.secs}**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-926](https://issues.apache.org/jira/browse/YARN-926) | *Blocker* | **ContainerManagerProtcol APIs should take in requests for multiple containers**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-918](https://issues.apache.org/jira/browse/YARN-918) | *Blocker* | **ApplicationMasterProtocol doesn't need ApplicationAttemptId in the payload after YARN-701**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-869](https://issues.apache.org/jira/browse/YARN-869) | *Blocker* | **ResourceManagerAdministrationProtocol should neither be public(yet) nor in yarn.api**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-851](https://issues.apache.org/jira/browse/YARN-851) | *Major* | **Share NMTokens using NMTokenCache (api-based) instead of memory based approach which is used currently.**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-841](https://issues.apache.org/jira/browse/YARN-841) | *Major* | **Annotate and document AuxService APIs**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-840](https://issues.apache.org/jira/browse/YARN-840) | *Major* | **Move ProtoUtils to  yarn.api.records.pb.impl**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-837](https://issues.apache.org/jira/browse/YARN-837) | *Major* | **ClusterInfo.java doesn't seem to belong to org.apache.hadoop.yarn**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-834](https://issues.apache.org/jira/browse/YARN-834) | *Blocker* | **Review/fix annotations for yarn-client module and clearly differentiate \*Async apis**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-831](https://issues.apache.org/jira/browse/YARN-831) | *Blocker* | **Remove resource min from GetNewApplicationResponse**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-829](https://issues.apache.org/jira/browse/YARN-829) | *Major* | **Rename RMTokenSelector to be RMDelegationTokenSelector**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-828](https://issues.apache.org/jira/browse/YARN-828) | *Major* | **Remove YarnVersionAnnotation**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-826](https://issues.apache.org/jira/browse/YARN-826) | *Major* | **Move Clock/SystemClock to util package**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-824](https://issues.apache.org/jira/browse/YARN-824) | *Major* | **Add  static factory to yarn client lib interface and change it to abstract class**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-823](https://issues.apache.org/jira/browse/YARN-823) | *Major* | **Move RMAdmin from yarn.client to yarn.client.cli and rename as RMAdminCLI**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-822](https://issues.apache.org/jira/browse/YARN-822) | *Major* | **Rename ApplicationToken to AMRMToken**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-821](https://issues.apache.org/jira/browse/YARN-821) | *Major* | **Rename FinishApplicationMasterRequest.setFinishApplicationStatus to setFinalApplicationStatus to be consistent with getter**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-806](https://issues.apache.org/jira/browse/YARN-806) | *Major* | **Move ContainerExitStatus from yarn.api to yarn.api.records**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-792](https://issues.apache.org/jira/browse/YARN-792) | *Major* | **Move NodeHealthStatus from yarn.api.record to yarn.server.api.record**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-791](https://issues.apache.org/jira/browse/YARN-791) | *Blocker* | **Ensure that RM RPC APIs that return nodes are consistent with /nodes REST API**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-787](https://issues.apache.org/jira/browse/YARN-787) | *Blocker* | **Remove resource min from Yarn client API**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-777](https://issues.apache.org/jira/browse/YARN-777) | *Major* | **Remove unreferenced objects from proto**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-756](https://issues.apache.org/jira/browse/YARN-756) | *Major* | **Move PreemptionContainer/PremptionContract/PreemptionMessage/StrictPreemptionContract/PreemptionResourceRequest to api.records**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-755](https://issues.apache.org/jira/browse/YARN-755) | *Major* | **Rename AllocateResponse.reboot to AllocateResponse.resync**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-753](https://issues.apache.org/jira/browse/YARN-753) | *Major* | **Add individual factory method for api protocol records**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-749](https://issues.apache.org/jira/browse/YARN-749) | *Major* | **Rename ResourceRequest (get,set)HostName to (get,set)ResourceName**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-748](https://issues.apache.org/jira/browse/YARN-748) | *Major* | **Move BuilderUtils from yarn-common to yarn-server-common**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-746](https://issues.apache.org/jira/browse/YARN-746) | *Major* | **rename Service.register() and Service.unregister() to registerServiceListener() & unregisterServiceListener() respectively**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-735](https://issues.apache.org/jira/browse/YARN-735) | *Major* | **Make ApplicationAttemptID, ContainerID, NodeID immutable**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-724](https://issues.apache.org/jira/browse/YARN-724) | *Major* | **Move ProtoBase from api.records to api.records.impl.pb**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-720](https://issues.apache.org/jira/browse/YARN-720) | *Major* | **container-log4j.properties should not refer to mapreduce properties**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-716](https://issues.apache.org/jira/browse/YARN-716) | *Major* | **Make ApplicationID immutable**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-701](https://issues.apache.org/jira/browse/YARN-701) | *Blocker* | **ApplicationTokens should be used irrespective of kerberos**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-694](https://issues.apache.org/jira/browse/YARN-694) | *Major* | **Start using NMTokens to authenticate all communication with NM**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-684](https://issues.apache.org/jira/browse/YARN-684) | *Major* | **ContainerManager.startContainer needs to only have ContainerTokenIdentifier instead of the whole Container**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-642](https://issues.apache.org/jira/browse/YARN-642) | *Major* | **Fix up /nodes REST API to have 1 param and be consistent with the Java API**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-635](https://issues.apache.org/jira/browse/YARN-635) | *Major* | **Rename YarnRemoteException to YarnException**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-633](https://issues.apache.org/jira/browse/YARN-633) | *Major* | **Change RMAdminProtocol api to throw IOException and YarnRemoteException**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-632](https://issues.apache.org/jira/browse/YARN-632) | *Major* | **Change ContainerManager api to throw IOException and YarnRemoteException**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-631](https://issues.apache.org/jira/browse/YARN-631) | *Major* | **Change ClientRMProtocol api to throw IOException and YarnRemoteException**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-630](https://issues.apache.org/jira/browse/YARN-630) | *Major* | **Change AMRMProtocol api to throw IOException and YarnRemoteException**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-629](https://issues.apache.org/jira/browse/YARN-629) | *Major* | **Make YarnRemoteException not be rooted at IOException**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-615](https://issues.apache.org/jira/browse/YARN-615) | *Major* | **ContainerLaunchContext.containerTokens should simply be called tokens**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-610](https://issues.apache.org/jira/browse/YARN-610) | *Blocker* | **ClientToken (ClientToAMToken) should not be set in the environment**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-579](https://issues.apache.org/jira/browse/YARN-579) | *Major* | **Make ApplicationToken part of Container's token list to help RM-restart**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-571](https://issues.apache.org/jira/browse/YARN-571) | *Major* | **User should not be part of ContainerLaunchContext**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-561](https://issues.apache.org/jira/browse/YARN-561) | *Major* | **Nodemanager should set some key information into the environment of every container that it launches.**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-553](https://issues.apache.org/jira/browse/YARN-553) | *Minor* | **Have YarnClient generate a directly usable ApplicationSubmissionContext**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-536](https://issues.apache.org/jira/browse/YARN-536) | *Major* | **Remove ContainerStatus, ContainerState from Container api interface as they will not be called by the container object**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-530](https://issues.apache.org/jira/browse/YARN-530) | *Major* | **Define Service model strictly, implement AbstractService for robust subclassing, migrate yarn-common services**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-440](https://issues.apache.org/jira/browse/YARN-440) | *Major* | **Flatten RegisterNodeManagerResponse**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-439](https://issues.apache.org/jira/browse/YARN-439) | *Major* | **Flatten NodeHeartbeatResponse**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-396](https://issues.apache.org/jira/browse/YARN-396) | *Major* | **Rationalize AllocateResponse in RM scheduler API**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-387](https://issues.apache.org/jira/browse/YARN-387) | *Blocker* | **Fix inconsistent protocol naming**

**WARNING: No release note provided for this incompatible change.**



