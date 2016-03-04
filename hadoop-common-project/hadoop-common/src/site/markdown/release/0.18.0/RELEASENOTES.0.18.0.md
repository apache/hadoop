
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
# Apache Hadoop  0.18.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-3837](https://issues.apache.org/jira/browse/HADOOP-3837) | *Major* | **hadop streaming does not use progress reporting to detect hung tasks**

Changed streaming tasks to adhere to task timeout value specified in the job configuration.


---

* [HADOOP-3808](https://issues.apache.org/jira/browse/HADOOP-3808) | *Blocker* | **[HOD] Include job tracker RPC in notes attribute after job submission**

Modified HOD to include the RPC port of the JobTracker in the 'notes' attribute of the resource manager. The RPC port is included as the string 'Mapred RPC Port:\<port number\>'. Tools that depend on the value of the notes attribute must change to parse this new value.


---

* [HADOOP-3703](https://issues.apache.org/jira/browse/HADOOP-3703) | *Blocker* | **[HOD] logcondense needs to use the new pattern of output in hadoop dfs -lsr**

Modified logcondense.py to use the new format of hadoop dfs -lsr output. This version of logcondense would not work with previous versions of Hadoop and hence is incompatible.


---

* [HADOOP-3683](https://issues.apache.org/jira/browse/HADOOP-3683) | *Major* | **Hadoop dfs metric FilesListed shows number of files listed instead of operations**

Change FileListed to getNumGetListingOps and add CreateFileOps, DeleteFileOps and AddBlockOps metrics.


---

* [HADOOP-3677](https://issues.apache.org/jira/browse/HADOOP-3677) | *Blocker* | **Problems with generation stamp upgrade**

Simplify generation stamp upgrade by making is a local upgrade on datandodes. Deleted distributed upgrade.


---

* [HADOOP-3665](https://issues.apache.org/jira/browse/HADOOP-3665) | *Minor* | **WritableComparator newKey() fails for NullWritable**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-3610](https://issues.apache.org/jira/browse/HADOOP-3610) | *Blocker* | **[HOD] HOD does not automatically create a cluster directory for the script option**

Modified HOD to automatically create a cluster directory if the one specified with the script command does not exist.


---

* [HADOOP-3598](https://issues.apache.org/jira/browse/HADOOP-3598) | *Blocker* | **Map-Reduce framework needlessly creates temporary \_${taskid} directories for Maps**

Changed Map-Reduce framework to no longer create temporary task output directories for staging outputs if staging outputs isn't necessary. ${mapred.out.dir}/\_temporary/\_${taskid}


---

* [HADOOP-3569](https://issues.apache.org/jira/browse/HADOOP-3569) | *Minor* | **KFS input stream read() returns 4 bytes instead of 1**

Fixed KFS to have read() read and return 1 byte instead of 4.


---

* [HADOOP-3564](https://issues.apache.org/jira/browse/HADOOP-3564) | *Blocker* | **Sometime after successful  hod allocation datanode fails to come up with java.net.BindException for dfs.datanode.ipc.address**

Modifed HOD to generate the dfs.datanode.ipc.address parameter in the hadoop-site.xml of datanodes that it launches.


---

* [HADOOP-3512](https://issues.apache.org/jira/browse/HADOOP-3512) | *Major* | **Split map/reduce tools into separate jars**

Separated Distcp, Logalyzer and Archiver  into a tools jar.


---

* [HADOOP-3486](https://issues.apache.org/jira/browse/HADOOP-3486) | *Major* | **Change default for initial block report to 0 sec and document it in hadoop-defaults.xml**

Changed the default value of dfs.blockreport.initialDelay to be 0 seconds.


---

* [HADOOP-3483](https://issues.apache.org/jira/browse/HADOOP-3483) | *Major* | **[HOD] Improvements with cluster directory handling**

Modified HOD to create a cluster directory if one does not exist and to auto-deallocate a cluster while reallocating it, if it is already dead.


---

* [HADOOP-3464](https://issues.apache.org/jira/browse/HADOOP-3464) | *Major* | **[HOD] HOD can improve error messages by reporting failures on compute nodes back to hod client**

Implemented a mechanism to transfer HOD errors that occur on compute nodes to the submit node running the HOD client, so users have good feedback on why an allocation failed.


---

* [HADOOP-3460](https://issues.apache.org/jira/browse/HADOOP-3460) | *Minor* | **SequenceFileAsBinaryOutputFormat**

Created SequenceFileAsBinaryOutputFormat to write raw bytes as keys and values to a SequenceFile.


---

* [HADOOP-3459](https://issues.apache.org/jira/browse/HADOOP-3459) | *Major* | **Change dfs -ls listing to closely match format on Linux**

Changed the output of the "fs -ls" command to more closely match familiar Linux format. Applications that parse the command output should be reviewed.


---

* [HADOOP-3452](https://issues.apache.org/jira/browse/HADOOP-3452) | *Minor* | **fsck exit code would be better if non-zero when FS corrupt**

Changed exit status of fsck to report whether the files system is healthy or corrupt.


---

* [HADOOP-3429](https://issues.apache.org/jira/browse/HADOOP-3429) | *Major* | **Increase the buffersize for the streaming parent java process's streams**

Increased the size of the buffer used in the communication between the Java task and the Streaming process to 128KB.


---

* [HADOOP-3427](https://issues.apache.org/jira/browse/HADOOP-3427) | *Major* | **In ReduceTask::fetchOutputs, wait for result can be improved slightly**

Changed shuffle scheduler policy to wait for notifications from shuffle threads before scheduling more.


---

* [HADOOP-3417](https://issues.apache.org/jira/browse/HADOOP-3417) | *Major* | **JobClient should not have a static configuration for cli parsing**

Removed the public class org.apache.hadoop.mapred.JobShell.
Command line options -libjars, -files and -archives are moved to GenericCommands. Thus applications have to implement org.apache.hadoop.util.Tool to use the options.


---

* [HADOOP-3405](https://issues.apache.org/jira/browse/HADOOP-3405) | *Major* | **Make mapred internal classes package-local**

Refactored previously public classes MapTaskStatus, ReduceTaskStatus, JobSubmissionProtocol, CompletedJobStatusStore to be package local.


---

* [HADOOP-3390](https://issues.apache.org/jira/browse/HADOOP-3390) | *Major* | **Remove deprecated ClientProtocol.abandonFileInProgress()**

Removed deprecated ClientProtocol.abandonFileInProgress().


---

* [HADOOP-3379](https://issues.apache.org/jira/browse/HADOOP-3379) | *Blocker* | **Document the "stream.non.zero.exit.status.is.failure" knob for streaming**

Set default value for configuration property "stream.non.zero.exit.status.is.failure" to be "true".


---

* [HADOOP-3376](https://issues.apache.org/jira/browse/HADOOP-3376) | *Major* | **[HOD] HOD should have a way to detect and deal with clusters that violate/exceed resource manager limits**

Modified HOD client to look for specific messages related to resource limit overruns and take appropriate actions - such as either failing to allocate the cluster, or issuing a warning to the user. A tool is provided, specific to Maui and Torque, that will set these specific messages.


---

* [HADOOP-3366](https://issues.apache.org/jira/browse/HADOOP-3366) | *Major* | **Shuffle/Merge improvements**

Improved shuffle so that all fetched map-outputs are kept in-memory before being merged by stalling the shuffle so that the in-memory merge executes and frees up memory for the shuffle.


---

* [HADOOP-3355](https://issues.apache.org/jira/browse/HADOOP-3355) | *Major* | **Configuration should accept decimal and hexadecimal values**

Added support for hexadecimal values in Configuration


---

* [HADOOP-3339](https://issues.apache.org/jira/browse/HADOOP-3339) | *Major* | **DFS Write pipeline does not detect defective datanode correctly if it times out.**

Improved failure handling of last Data Node in write pipeline.


---

* [HADOOP-3336](https://issues.apache.org/jira/browse/HADOOP-3336) | *Major* | **Direct a subset of namenode RPC events for audit logging**

Added a log4j appender that emits events from FSNamesystem for audit logging


---

* [HADOOP-3329](https://issues.apache.org/jira/browse/HADOOP-3329) | *Major* | **DatanodeDescriptor objects stored in FSImage may be out dated.**

Changed format of file system image to not store locations of last block.


---

* [HADOOP-3326](https://issues.apache.org/jira/browse/HADOOP-3326) | *Major* | **ReduceTask should not sleep for 200 ms while waiting for merge to finish**

Changed fetchOutputs() so that LocalFSMerger and InMemFSMergeThread threads are spawned only once. The thread gets notified when something is ready for merge. The merge happens when thresholds are met.


---

* [HADOOP-3317](https://issues.apache.org/jira/browse/HADOOP-3317) | *Minor* | **add default port for hdfs namenode**

Changed the default port for  "hdfs:" URIs to be 8020, so that one may simply use URIs of the form "hdfs\://example.com/dir/file".


---

* [HADOOP-3310](https://issues.apache.org/jira/browse/HADOOP-3310) | *Major* | **Lease recovery for append**

Implemented Lease Recovery to sync the last bock of a file.  Added ClientDatanodeProtocol for client trigging block recovery. Changed DatanodeProtocol to support block synchronization. Changed InterDatanodeProtocol to support block update.


---

* [HADOOP-3307](https://issues.apache.org/jira/browse/HADOOP-3307) | *Major* | **Archives in Hadoop.**

Introduced archive feature to Hadoop. A Map/Reduce job can be run to create an archive with indexes. A FileSystem abstraction is provided over the archive.


---

* [HADOOP-3299](https://issues.apache.org/jira/browse/HADOOP-3299) | *Major* | **org.apache.hadoop.mapred.join.CompositeInputFormat does not initialize  TextInput format files with the configuration resulting in an NullPointerException**

Changed the TextInputFormat and KeyValueTextInput classes to initialize the compressionCodecs member variable before dereferencing it.


---

* [HADOOP-3283](https://issues.apache.org/jira/browse/HADOOP-3283) | *Major* | **Need a mechanism for data nodes to update generation stamps.**

Added an IPC server in DataNode and a new IPC protocol InterDatanodeProtocol.  Added conf properties dfs.datanode.ipc.address and dfs.datanode.handler.count with defaults "0.0.0.0:50020" and 3, respectively.
Changed the serialization in DatanodeRegistration and DatanodeInfo, and therefore, updated the versionID in ClientProtocol, DatanodeProtocol, NamenodeProtocol.


---

* [HADOOP-3265](https://issues.apache.org/jira/browse/HADOOP-3265) | *Major* | **Remove deprecated API getFileCacheHints**

Removed deprecated API getFileCacheHints


---

* [HADOOP-3246](https://issues.apache.org/jira/browse/HADOOP-3246) | *Major* | **FTP client over HDFS**

Introduced an FTPFileSystem backed by Apache Commons FTPClient to directly store data into HDFS.


---

* [HADOOP-3232](https://issues.apache.org/jira/browse/HADOOP-3232) | *Critical* | **Datanodes time out**

Changed 'du' command to run in a seperate thread so that it does not block user.


---

* [HADOOP-3230](https://issues.apache.org/jira/browse/HADOOP-3230) | *Major* | **Add command line access to named counters**

Added command line tool "job -counter \<job-id\> \<group-name\> \<counter-name\>" to access counters.


---

* [HADOOP-3226](https://issues.apache.org/jira/browse/HADOOP-3226) | *Major* | **Run combiner when merging spills from map output**

Changed policy for running combiner. The combiner may be run multiple times as the map's output is sorted and merged. Additionally, it may be run on the reduce side as data is merged. The old semantics are available in Hadoop 0.18 if the user calls: 
job.setCombineOnlyOnce(true);


---

* [HADOOP-3221](https://issues.apache.org/jira/browse/HADOOP-3221) | *Major* | **Need a "LineBasedTextInputFormat"**

Added org.apache.hadoop.mapred.lib.NLineInputFormat ,which splits N lines of input as one split. N can be specified by configuration property "mapred.line.input.format.linespermap", which defaults to 1.


---

* [HADOOP-3193](https://issues.apache.org/jira/browse/HADOOP-3193) | *Minor* | **Discovery of corrupt block reported in name node log**

Added reporter to FSNamesystem stateChangeLog, and a new metric to track the number of corrupted replicas.


---

* [HADOOP-3187](https://issues.apache.org/jira/browse/HADOOP-3187) | *Major* | **Quotas for name space management**

Introduced directory quota as hard limits on the number of names in the tree rooted at that directory. An administrator may set quotas on individual directories explicitly. Newly created directories have no associated quota. File/directory creations fault if the quota would be exceeded. The attempt to set a quota faults if the directory would be in violation of the new quota.


---

* [HADOOP-3184](https://issues.apache.org/jira/browse/HADOOP-3184) | *Major* | **HOD gracefully exclude "bad" nodes during ring formation**

Modified HOD to handle master (NameNode or JobTracker) failures on bad nodes by trying to bring them up on another node in the ring. Introduced new property ringmaster.max-master-failures to specify the maximum number of times a master is allowed to fail.


---

* [HADOOP-3177](https://issues.apache.org/jira/browse/HADOOP-3177) | *Major* | **Expose DFSOutputStream.fsync API though the FileSystem interface**

Added a new public interface Syncable which declares the sync() operation.  FSDataOutputStream implements Syncable.  If the wrappedStream in FSDataOutputStream is Syncalbe, calling FSDataOutputStream.sync() is equivalent to call wrappedStream.sync().  Otherwise, FSDataOutputStream.sync() is a no-op.  Both DistributedFileSystem and LocalFileSystem support the sync() operation.


---

* [HADOOP-3164](https://issues.apache.org/jira/browse/HADOOP-3164) | *Major* | **Use FileChannel.transferTo() when data is read from DataNode.**

Changed data node to use FileChannel.tranferTo() to transfer block data.


---

* [HADOOP-3135](https://issues.apache.org/jira/browse/HADOOP-3135) | *Critical* | **if the 'mapred.system.dir' in the client jobconf is different from the JobTracker's value job submission fails**

Changed job submission protocol to not allow submission if the client's value of mapred.system.dir does not match the job tracker's. Deprecated JobConf.getSystemDir(); use JobClient.getSystemDir().


---

* [HADOOP-3113](https://issues.apache.org/jira/browse/HADOOP-3113) | *Major* | **DFSOututStream.flush() should flush data to real block file on DataNode.**

Added sync() method to FSDataOutputStream to really, really persist data in HDFS. InterDatanodeProtocol to implement this feature.


---

* [HADOOP-3095](https://issues.apache.org/jira/browse/HADOOP-3095) | *Major* | **Validating input paths and creating splits is slow on S3**

Added overloaded method getFileBlockLocations(FileStatus, long, long). This is an incompatible change for FileSystem implementations which override getFileBlockLocations(Path, long, long). They should have the signature of this method changed to getFileBlockLocations(FileStatus, long, long) to work correctly.


---

* [HADOOP-3061](https://issues.apache.org/jira/browse/HADOOP-3061) | *Major* | **Writable for single byte and double**

Introduced ByteWritable and DoubleWritable (implementing WritableComparable) implementations for Byte and Double.


---

* [HADOOP-3058](https://issues.apache.org/jira/browse/HADOOP-3058) | *Minor* | **Hadoop DFS to report more replication metrics**

Added FSNamesystem status metrics.


---

* [HADOOP-3035](https://issues.apache.org/jira/browse/HADOOP-3035) | *Major* | **Data nodes should inform the name-node about block crc errors.**

Changed protocol for transferring blocks between data nodes to report corrupt blocks to data node for re-replication from a good replica.


---

* [HADOOP-3013](https://issues.apache.org/jira/browse/HADOOP-3013) | *Major* | **fsck to show (checksum) corrupted files**

fsck reports corrupt blocks in the system.


---

* [HADOOP-2909](https://issues.apache.org/jira/browse/HADOOP-2909) | *Major* | **Improve IPC idle connection management**

Removed property ipc.client.maxidletime from the default configuration. The allowed idle time is  twice ipc.client.connection.maxidletime.


---

* [HADOOP-2867](https://issues.apache.org/jira/browse/HADOOP-2867) | *Major* | **Add a task's cwd to it's LD\_LIBRARY\_PATH**

Added task's cwd to its LD\_LIBRARY\_PATH.


---

* [HADOOP-2865](https://issues.apache.org/jira/browse/HADOOP-2865) | *Major* | **FsShell.ls() should print file attributes first then the path name.**

Changed the output of the "fs -ls" command to more closely match familiar Linux format. Additional changes were made by HADOOP-3459. Applications that parse the command output should be reviewed.


---

* [HADOOP-2797](https://issues.apache.org/jira/browse/HADOOP-2797) | *Critical* | **Withdraw CRC upgrade from HDFS**

Withdrew the upgrade-to-CRC facility. HDFS will no longer support upgrades from versions without CRCs for block data. Users upgrading from version 0.13 or earlier must first upgrade to an intermediate (0.14, 0.15, 0.16, 0.17) version before doing upgrade to version 0.18 or later.


---

* [HADOOP-2703](https://issues.apache.org/jira/browse/HADOOP-2703) | *Minor* | **New files under lease (before close) still shows up as MISSING files/blocks in fsck**

Changed fsck to ignore files opened for writing. Introduced new option "-openforwrite" to explicitly show open files.


---

* [HADOOP-2656](https://issues.apache.org/jira/browse/HADOOP-2656) | *Major* | **Support for upgrading existing cluster to facilitate appends to HDFS files**

Associated a generation stamp with each block. On data nodes, the generation stamp is stored as part of the file name of the block's meta-data file.


---

* [HADOOP-2585](https://issues.apache.org/jira/browse/HADOOP-2585) | *Major* | **Automatic namespace recovery from the secondary image.**

Improved management of replicas of the name space image. If all replicas on the Name Node are lost, the latest check point can be loaded from the secondary Name Node. Use parameter "-importCheckpoint" and specify the location with "fs.checkpoint.dir." The directory structure on the secondary Name Node has changed to match the primary Name Node.


---

* [HADOOP-2427](https://issues.apache.org/jira/browse/HADOOP-2427) | *Major* | **Cleanup of mapred.local.dir after maptask is complete**

The current working directory of a task, i.e. ${mapred.local.dir}/taskTracker/jobcache/\<jobid\>/\<task\_dir\>/work is cleanedup, as soon as the task is finished.


---

* [HADOOP-2188](https://issues.apache.org/jira/browse/HADOOP-2188) | *Major* | **RPC should send a ping rather than use client timeouts**

Replaced timeouts with pings to check that client connection is alive. Removed the property ipc.client.timeout from the default Hadoop configuration. Removed the metric RpcOpsDiscardedOPsNum.


---

* [HADOOP-2181](https://issues.apache.org/jira/browse/HADOOP-2181) | *Minor* | **Input Split details for maps should be logged**

Added logging for input splits in job tracker log and job history log. Added web UI for viewing input splits in the job UI and history UI.


---

* [HADOOP-2132](https://issues.apache.org/jira/browse/HADOOP-2132) | *Critical* | **Killing successfully completed jobs moves them to failed**

Change "job -kill" to only allow a job that is in the RUNNING or PREP state to be killed.


---

* [HADOOP-2095](https://issues.apache.org/jira/browse/HADOOP-2095) | *Major* | **Reducer failed due to Out ofMemory**

Reduced in-memory copies of keys and values as they flow through the Map-Reduce framework. Changed the storage of intermediate map outputs to use new IFile instead of SequenceFile for better compression.


---

* [HADOOP-2065](https://issues.apache.org/jira/browse/HADOOP-2065) | *Major* | **Replication policy for corrupted block**

Added "corrupt" flag to LocatedBlock to indicate that all replicas of the block thought to be corrupt.


---

* [HADOOP-2019](https://issues.apache.org/jira/browse/HADOOP-2019) | *Major* | **DistributedFileCache should support .tgz files in addition to jars and zip files**

Added support for .tar, .tgz and .tar.gz files in DistributedCache. File sizes are limited to 2GB.


---

* [HADOOP-1915](https://issues.apache.org/jira/browse/HADOOP-1915) | *Minor* | **adding counters methods using String (as opposed to Enum)**

Provided a new method to update counters. "incrCounter(String group, String counter, long amount)"


---

* [HADOOP-1702](https://issues.apache.org/jira/browse/HADOOP-1702) | *Major* | **Reduce buffer copies when data is written to DFS**

Reduced buffer copies as data is written to HDFS. The order of sending data bytes and control information has changed, but this will not be observed by client applications.


---

* [HADOOP-1328](https://issues.apache.org/jira/browse/HADOOP-1328) | *Major* | **Hadoop Streaming needs to provide a way for the stream plugin to update global counters**

Introduced a way for a streaming process to update global counters and status using stderr stream to emit information. Use "reporter:counter:\<group\>,\<counter\>,\<amount\> " to update a counter. Use "reporter:status:\<message\>" to update status.


---

* [HADOOP-930](https://issues.apache.org/jira/browse/HADOOP-930) | *Major* | **Add support for reading regular (non-block-based) files from S3 in S3FileSystem**

Added support for reading and writing native S3 files. Native S3 files are referenced using s3n URIs. See http://wiki.apache.org/hadoop/AmazonS3 for more details.


---

* [HADOOP-544](https://issues.apache.org/jira/browse/HADOOP-544) | *Major* | **Replace the job, tip and task ids with objects.**

Introduced new classes JobID, TaskID and TaskAttemptID, which should be used instead of their string counterparts. Deprecated functions in JobClient, TaskReport, RunningJob, jobcontrol.Job and TaskCompletionEvent that use string arguments. Applications can use xxxID.toString() and xxxID.forName() methods to convert/restore objects to/from strings.


---

* [HADOOP-236](https://issues.apache.org/jira/browse/HADOOP-236) | *Major* | **job tracker should refuse connection from a task tracker with a different version number**

Changed connection protocol job tracker and task tracker so that task tracker will not connect to a job tracker with a different build version.


---

* [HADOOP-4](https://issues.apache.org/jira/browse/HADOOP-4) | *Major* | **tool to mount dfs on linux**

Introduced FUSE module for HDFS. Module allows mount of HDFS as a Unix filesystem,  and optionally the export of that mount point to other machines. Writes are disabled. rmdir, mv, mkdir, rm are  supported, but not cp, touch, and the like. Usage information is attached to the Jira record.



