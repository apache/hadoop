
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
# Apache Hadoop  0.22.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-7302](https://issues.apache.org/jira/browse/HADOOP-7302) | *Major* | **webinterface.private.actions should not be in common**

Option webinterface.private.actions has been renamed to mapreduce.jobtracker.webinterface.trusted and should be specified in mapred-site.xml instead of core-site.xml


---

* [HADOOP-7229](https://issues.apache.org/jira/browse/HADOOP-7229) | *Major* | **Absolute path to kinit in auto-renewal thread**

When Hadoop's Kerberos integration is enabled, it is now required that either {{kinit}} be on the path for user accounts running the Hadoop client, or that the {{hadoop.kerberos.kinit.command}} configuration option be manually set to the absolute path to {{kinit}}.


---

* [HADOOP-7193](https://issues.apache.org/jira/browse/HADOOP-7193) | *Minor* | **Help message is wrong for touchz command.**

Updated the help for the touchz command.


---

* [HADOOP-7192](https://issues.apache.org/jira/browse/HADOOP-7192) | *Trivial* | **fs -stat docs aren't updated to reflect the format features**

Updated the web documentation to reflect the formatting abilities of 'fs -stat'.


---

* [HADOOP-7156](https://issues.apache.org/jira/browse/HADOOP-7156) | *Critical* | **getpwuid\_r is not thread-safe on RHEL6**

Adds a new configuration hadoop.work.around.non.threadsafe.getpwuid which can be used to enable a mutex around this call to workaround thread-unsafe implementations of getpwuid\_r. Users should consult http://wiki.apache.org/hadoop/KnownBrokenPwuidImplementations for a list of such systems.


---

* [HADOOP-7137](https://issues.apache.org/jira/browse/HADOOP-7137) | *Major* | **Remove hod contrib**

Removed contrib related build targets.


---

* [HADOOP-7134](https://issues.apache.org/jira/browse/HADOOP-7134) | *Major* | **configure files that are generated as part of the released tarball need to have executable bit set**

I have just committed this to trunk and branch-0.22. Thanks Roman!


---

* [HADOOP-7117](https://issues.apache.org/jira/browse/HADOOP-7117) | *Major* | **Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml**

Removed references to the older fs.checkpoint.\* properties that resided in core-site.xml


---

* [HADOOP-7089](https://issues.apache.org/jira/browse/HADOOP-7089) | *Minor* | **Fix link resolution logic in hadoop-config.sh**

Updates hadoop-config.sh to always resolve symlinks when determining HADOOP\_HOME. Bash built-ins or POSIX:2001 compliant cmds are now required.


---

* [HADOOP-7013](https://issues.apache.org/jira/browse/HADOOP-7013) | *Major* | **Add boolean field isCorrupt to BlockLocation**

This patch has changed the serialization format of BlockLocation.


---

* [HADOOP-7005](https://issues.apache.org/jira/browse/HADOOP-7005) | *Major* | **Update test-patch.sh to remove callback to Hudson master**

N/A


---

* [HADOOP-6949](https://issues.apache.org/jira/browse/HADOOP-6949) | *Major* | **Reduces RPC packet size for primitive arrays, especially long[], which is used at block reporting**

Increments the RPC protocol version in org.apache.hadoop.ipc.Server from 4 to 5.
Introduces ArrayPrimitiveWritable for a much more efficient wire format to transmit arrays of primitives over RPC. ObjectWritable uses the new writable for array of primitives for RPC and continues to use existing format for on-disk data.


---

* [HADOOP-6922](https://issues.apache.org/jira/browse/HADOOP-6922) | *Major* | **COMMON part of MAPREDUCE-1664**

Makes AccessControlList a writable and updates documentation for Job ACLs.


---

* [HADOOP-6905](https://issues.apache.org/jira/browse/HADOOP-6905) | *Major* | **Better logging messages when a delegation token is invalid**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-6835](https://issues.apache.org/jira/browse/HADOOP-6835) | *Major* | **Support concatenated gzip files**

Processing of concatenated gzip files formerly stopped (quietly) at the end of the first substream/"member"; now processing will continue to the end of the concatenated stream, like gzip(1) does.  (bzip2 support is unaffected by this patch.)


---

* [HADOOP-6787](https://issues.apache.org/jira/browse/HADOOP-6787) | *Major* | **Factor out glob pattern code from FileContext and Filesystem**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-6730](https://issues.apache.org/jira/browse/HADOOP-6730) | *Major* | **Bug in FileContext#copy and provide base class for FileContext tests**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-6693](https://issues.apache.org/jira/browse/HADOOP-6693) | *Major* | **Add metrics to track kerberos login activity**

New metrics "login" of type MetricTimeVaryingRate is added under new metrics context name "ugi" and metrics record name "ugi".


---

* [HADOOP-6683](https://issues.apache.org/jira/browse/HADOOP-6683) | *Minor* | **the first optimization: ZlibCompressor does not fully utilize the buffer**

Improve the buffer utilization of ZlibCompressor to avoid invoking a JNI per write request.


---

* [HADOOP-6663](https://issues.apache.org/jira/browse/HADOOP-6663) | *Major* | **BlockDecompressorStream get EOF exception when decompressing the file compressed from empty file**

Fix EOF exception in BlockDecompressorStream when decompressing previous compressed empty file


---

* [HADOOP-6599](https://issues.apache.org/jira/browse/HADOOP-6599) | *Major* | **Split RPC metrics into summary and detailed metrics**

Split existing RpcMetrics into RpcMetrics and RpcDetailedMetrics. The new RpcDetailedMetrics has per method usage details and is available under context name "rpc" and record name "detailed-metrics"


---

* [HADOOP-6436](https://issues.apache.org/jira/browse/HADOOP-6436) | *Major* | **Remove auto-generated native build files**

The native build run when from trunk now requires autotools, libtool and openssl dev libraries.


---

* [HADOOP-6344](https://issues.apache.org/jira/browse/HADOOP-6344) | *Major* | **rm and rmr fail to correctly move the user's files to the trash prior to deleting when they are over quota.**

Trash feature notifies user of over-quota condition rather than silently deleting files/directories; deletion can be compelled with "rm -skiptrash".


---

* [HADOOP-4675](https://issues.apache.org/jira/browse/HADOOP-4675) | *Major* | **Current Ganglia metrics implementation is incompatible with Ganglia 3.1**

Support for reporting metrics to Ganglia 3.1 servers


---

* [HDFS-1948](https://issues.apache.org/jira/browse/HDFS-1948) | *Major* | **Forward port 'hdfs-1520 lightweight namenode operation to trigger lease reccovery'**

Adds method to NameNode/ClientProtocol that allows for rude revoke of lease on current lease holder


---

* [HDFS-1825](https://issues.apache.org/jira/browse/HDFS-1825) | *Major* | **Remove thriftfs contrib**

Removed thriftfs contrib component.


---

* [HDFS-1596](https://issues.apache.org/jira/browse/HDFS-1596) | *Major* | **Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml**

Removed references to the older fs.checkpoint.\* properties that resided in core-site.xml


---

* [HDFS-1582](https://issues.apache.org/jira/browse/HDFS-1582) | *Major* | **Remove auto-generated native build files**

The native build run when from trunk now requires autotools, libtool and openssl dev libraries.


---

* [HDFS-1560](https://issues.apache.org/jira/browse/HDFS-1560) | *Minor* | **dfs.data.dir permissions should default to 700**

The permissions on datanode data directories (configured by dfs.datanode.data.dir.perm) now default to 0700. Upon startup, the datanode will automatically change the permissions to match the configured value.


---

* [HDFS-1457](https://issues.apache.org/jira/browse/HDFS-1457) | *Major* | **Limit transmission rate when transfering image between primary and secondary NNs**

Add a configuration variable dfs.image.transfer.bandwidthPerSec to allow the user to specify the amount of bandwidth for transferring image and edits. Its default value is 0 indicating no throttling.


---

* [HDFS-1435](https://issues.apache.org/jira/browse/HDFS-1435) | *Major* | **Provide an option to store fsimage compressed**

This provides an option to store fsimage compressed. The layout version is bumped to -25. The user could configure if s/he wants the fsimage to be compressed or not and which codec to use. By default the fsimage is not compressed.


---

* [HDFS-1318](https://issues.apache.org/jira/browse/HDFS-1318) | *Major* | **HDFS Namenode and Datanode WebUI information needs to be accessible programmatically for scripts**

resubmit the patch for HDFS1318 as Hudson was down last week.


---

* [HDFS-1315](https://issues.apache.org/jira/browse/HDFS-1315) | *Major* | **Add fsck event to audit log and remove other audit log events corresponding to FSCK listStatus and open calls**

When running fsck, audit log events are not logged for listStatus and open are not logged. A new event with cmd=fsck is logged with ugi field set to the user requesting fsck and src field set to the fsck path.


---

* [HDFS-1109](https://issues.apache.org/jira/browse/HDFS-1109) | *Major* | **HFTP and URL Encoding**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-1096](https://issues.apache.org/jira/browse/HDFS-1096) | *Major* | **allow dfsadmin/mradmin refresh of superuser proxy group mappings**

changed protocol name (may be used in hadoop-policy.xml)
from security.refresh.usertogroups.mappings.protocol.acl to security.refresh.user.mappings.protocol.acl


---

* [HDFS-1080](https://issues.apache.org/jira/browse/HDFS-1080) | *Major* | **SecondaryNameNode image transfer should use the defined http address rather than local ip address**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-1079](https://issues.apache.org/jira/browse/HDFS-1079) | *Major* | **HDFS implementation should throw exceptions defined in AbstractFileSystem**

Specific exceptions are thrown from HDFS implementation and protocol per the interface defined in AbstractFileSystem. The compatibility is not affected as the applications catch IOException and will be able to handle specific exceptions that are subclasses of IOException.


---

* [HDFS-1061](https://issues.apache.org/jira/browse/HDFS-1061) | *Minor* | **Memory footprint optimization for INodeFile object.**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-1035](https://issues.apache.org/jira/browse/HDFS-1035) | *Major* | **Generate Eclipse's .classpath file from Ivy config**

Added support to auto-generate the Eclipse .classpath file from ivy.


---

* [HDFS-903](https://issues.apache.org/jira/browse/HDFS-903) | *Critical* | **NN should verify images and edit logs on startup**

Store fsimage MD5 checksum in VERSION file. Validate checksum when loading a fsimage. Layout version bumped.


---

* [HDFS-712](https://issues.apache.org/jira/browse/HDFS-712) | *Major* | **Move libhdfs from mr to hdfs**

Moved the libhdfs package to the HDFS subproject.


---

* [HDFS-708](https://issues.apache.org/jira/browse/HDFS-708) | *Major* | **A stress-test tool for HDFS.**

Does not currently provide anything but uniform distribution. 
Uses some older depreciated class interfaces (for mapper and reducer)
This was tested on 0.20 and 0.22 (locally) so it should be fairly backwards compatible.


---

* [HDFS-330](https://issues.apache.org/jira/browse/HDFS-330) | *Trivial* | **Datanode Web UIs should provide robots.txt**

A robots.txt is now in place which will prevent well behaved crawlers from perusing Hadoop web interfaces.


---

* [HDFS-202](https://issues.apache.org/jira/browse/HDFS-202) | *Major* | **Add a bulk FIleSystem.getFileBlockLocations**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-3151](https://issues.apache.org/jira/browse/MAPREDUCE-3151) | *Major* | **Contrib tests failing**

Confirmed that problem of finding ivy file occurs w/o patch with ant 1.7, and not with patch (with either ant 1.7 or 1.8).
Other unit tests are still failing the test steps themselves on my laptop, but that is not due not finding the ivy file.


---

* [MAPREDUCE-2516](https://issues.apache.org/jira/browse/MAPREDUCE-2516) | *Minor* | **option to control sensitive web actions**

Configuration option webinterface.private.actions has been renamed to mapreduce.jobtracker.webinterface.trusted


---

* [MAPREDUCE-2410](https://issues.apache.org/jira/browse/MAPREDUCE-2410) | *Minor* | **document multiple keys per reducer oddity in hadoop streaming FAQ**

Add an FAQ entry regarding the differences between Java API and Streaming development of MR programs.


---

* [MAPREDUCE-2272](https://issues.apache.org/jira/browse/MAPREDUCE-2272) | *Trivial* | **Job ACL file should not be executable**

Job ACL files now have permissions set to 600 (previously 700).


---

* [MAPREDUCE-2260](https://issues.apache.org/jira/browse/MAPREDUCE-2260) | *Major* | **Remove auto-generated native build files**

The native build run when from trunk now requires autotools, libtool and openssl dev libraries.


---

* [MAPREDUCE-2251](https://issues.apache.org/jira/browse/MAPREDUCE-2251) | *Major* | **Remove mapreduce.job.userhistorylocation config**

Remove the now defunct property `mapreduce.job.userhistorylocation`.


---

* [MAPREDUCE-2147](https://issues.apache.org/jira/browse/MAPREDUCE-2147) | *Trivial* | **JobInProgress has some redundant lines in its ctor**

Remove some redundant lines from JobInProgress's constructor which was re-initializing things unnecessarily.


---

* [MAPREDUCE-2096](https://issues.apache.org/jira/browse/MAPREDUCE-2096) | *Blocker* | **Secure local filesystem IO from symlink vulnerabilities**

The TaskTracker now uses the libhadoop JNI library to operate securely on local files when security is enabled. Secure clusters must ensure that libhadoop.so is available to the TaskTracker.


---

* [MAPREDUCE-2054](https://issues.apache.org/jira/browse/MAPREDUCE-2054) | *Major* | **Hierarchical queue implementation broke dynamic queue addition in Dynamic Scheduler**

Fix Dynamic Priority Scheduler to work with hierarchical queue names


---

* [MAPREDUCE-2032](https://issues.apache.org/jira/browse/MAPREDUCE-2032) | *Major* | **TestJobOutputCommitter fails in ant test run**

Clears a problem that {{TestJobCleanup}} leaves behind files that cause {{TestJobOutputCommitter}} to error out.


---

* [MAPREDUCE-1996](https://issues.apache.org/jira/browse/MAPREDUCE-1996) | *Trivial* | **API: Reducer.reduce() method detail misstatement**

Fix a misleading documentation note about the usage of Reporter objects in Reducers.


---

* [MAPREDUCE-1905](https://issues.apache.org/jira/browse/MAPREDUCE-1905) | *Blocker* | **Context.setStatus() and progress() api are ignored**

Moved the api public Counter getCounter(Enum\<?\> counterName), public Counter getCounter(String groupName, String counterName) from org.apache.hadoop.mapreduce.TaskInputOutputContext to org.apache.hadoop.mapreduce.TaskAttemptContext


---

* [MAPREDUCE-1887](https://issues.apache.org/jira/browse/MAPREDUCE-1887) | *Major* | **MRAsyncDiskService does not properly absolutize volume root paths**

MAPREDUCE-1887. MRAsyncDiskService now properly absolutizes volume root paths. (Aaron Kimball via zshao)


---

* [MAPREDUCE-1866](https://issues.apache.org/jira/browse/MAPREDUCE-1866) | *Minor* | **Remove deprecated class org.apache.hadoop.streaming.UTF8ByteArrayUtils**

Removed public deprecated class org.apache.hadoop.streaming.UTF8ByteArrayUtils.


---

* [MAPREDUCE-1836](https://issues.apache.org/jira/browse/MAPREDUCE-1836) | *Major* | **Refresh for proxy superuser config (mr part for HDFS-1096)**

changing name of the protocol (may be used in hadoop-policy.xml) 
from 
security.refresh.usertogroups.mappings.protocol.acl
to 
security.refresh.user.mappings.protocol.acl


---

* [MAPREDUCE-1829](https://issues.apache.org/jira/browse/MAPREDUCE-1829) | *Major* | **JobInProgress.findSpeculativeTask should use min() to find the candidate instead of sort()**

Improved performance of the method JobInProgress.findSpeculativeTask() which is in the critical heartbeat code path.


---

* [MAPREDUCE-1813](https://issues.apache.org/jira/browse/MAPREDUCE-1813) | *Major* | **NPE in PipeMapred.MRErrorThread**

Fixed an NPE in streaming that occurs when there is no input to reduce and the streaming reducer sends status updates by writing "reporter:status: xxx" statements to stderr.


---

* [MAPREDUCE-1785](https://issues.apache.org/jira/browse/MAPREDUCE-1785) | *Minor* | **Add streaming config option for not emitting the key**

Added a configuration property "stream.map.input.ignoreKey" to specify whether to ignore key or not while writing input for the mapper. This configuration parameter is valid only if stream.map.input.writer.class is org.apache.hadoop.streaming.io.TextInputWriter.class. For all other InputWriter's, key is always written.


---

* [MAPREDUCE-1780](https://issues.apache.org/jira/browse/MAPREDUCE-1780) | *Major* | **AccessControlList.toString() is used for serialization of ACL in JobStatus.java**

Fixes serialization of job-acls in JobStatus to use AccessControlList.write() instead of AccessControlList.toString().


---

* [MAPREDUCE-1773](https://issues.apache.org/jira/browse/MAPREDUCE-1773) | *Major* | **streaming doesn't support jobclient.output.filter**

Improved console messaging for streaming jobs by using the generic JobClient API itself instead of the existing streaming-specific code.


---

* [MAPREDUCE-1733](https://issues.apache.org/jira/browse/MAPREDUCE-1733) | *Major* | **Authentication between pipes processes and java counterparts.**

This jira introduces backward incompatibility. Existing pipes applications  MUST be recompiled with new hadoop pipes library once the changes in this jira are deployed.


---

* [MAPREDUCE-1707](https://issues.apache.org/jira/browse/MAPREDUCE-1707) | *Major* | **TaskRunner can get NPE in getting ugi from TaskTracker**

Fixed a bug that causes TaskRunner to get NPE in getting ugi from TaskTracker and subsequently crashes it resulting in a failing task after task-timeout period.


---

* [MAPREDUCE-1683](https://issues.apache.org/jira/browse/MAPREDUCE-1683) | *Major* | **Remove JNI calls from ClusterStatus cstr**

Removes JNI calls to get jvm current/max heap usage in ClusterStatus. Any instances of ClusterStatus serialized in a prior version will not be correctly deserialized using the updated class.


---

* [MAPREDUCE-1680](https://issues.apache.org/jira/browse/MAPREDUCE-1680) | *Major* | **Add a metrics to track the number of heartbeats processed**

Added a metric to track number of heartbeats processed by the JobTracker.


---

* [MAPREDUCE-1664](https://issues.apache.org/jira/browse/MAPREDUCE-1664) | *Major* | **Job Acls affect Queue Acls**

<!-- markdown -->
* Removed aclsEnabled flag from queues configuration files.
* Removed the configuration property mapreduce.cluster.job-authorization-enabled.
* Added mapreduce.cluster.acls.enabled as the single configuration property in mapred-default.xml that enables the authorization checks for all job level and queue level operations.
* To enable authorization of users to do job level and queue level operations, mapreduce.cluster.acls.enabled is to be set to true in JobTracker's configuration and in all TaskTrackers' configurations.
* To get access to a job, it is enough for a user to be part of one of the access lists i.e. either job-acl or queue-admins-acl(unlike before, when, one has to be part of both the lists).
* Queue administrators(configured via acl-administer-jobs) of a queue can do all view-job and modify-job operations on all jobs submitted to that queue. 
* ClusterOwner(who started the mapreduce cluster) and cluster administrators(configured via mapreduce.cluster.permissions.supergroup) can do all job level operations and queue level operations on all jobs on all queues in that cluster irrespective of job-acls and queue-acls configured.
* JobOwner(who submitted job to a queue) can do all view-job and modify-job operations on his/her job irrespective of job-acls and queue-acls.
* Since aclsEnabled flag is removed from queues configuration files, "refresh of queues configuration" will not change mapreduce.cluster.acls.enabled on the fly. mapreduce.cluster.acls.enabled can be modified only when restarting the mapreduce cluster.


---

* [MAPREDUCE-1592](https://issues.apache.org/jira/browse/MAPREDUCE-1592) | *Major* | **Generate Eclipse's .classpath file from Ivy config**

Added support to auto-generate the Eclipse .classpath file from ivy.


---

* [MAPREDUCE-1558](https://issues.apache.org/jira/browse/MAPREDUCE-1558) | *Major* | **specify correct server principal for RefreshAuthorizationPolicyProtocol and RefreshUserToGroupMappingsProtocol protocols in MRAdmin (for HADOOP-6612)**

new config: 
hadoop.security.service.user.name.key
this setting points to the server principal for RefreshUserToGroupMappingsProtocol.
The value should be either NN or JT principal depending if it is used in DFAdmin or MRAdmin. The value is set by the application. No need for default value.


---

* [MAPREDUCE-1543](https://issues.apache.org/jira/browse/MAPREDUCE-1543) | *Major* | **Log messages of JobACLsManager should use security logging of HADOOP-6586**

Adds the audit logging facility to MapReduce. All authorization/authentication events are logged to audit log. Audit log entries are stored as key=value.


---

* [MAPREDUCE-1533](https://issues.apache.org/jira/browse/MAPREDUCE-1533) | *Major* | **Reduce or remove usage of String.format() usage in CapacityTaskScheduler.updateQSIObjects and Counters.makeEscapedString()**

Incremental enhancements to the JobTracker to optimize heartbeat handling.


---

* [MAPREDUCE-1517](https://issues.apache.org/jira/browse/MAPREDUCE-1517) | *Major* | **streaming should support running on background**

Adds -background option to run a streaming job in background.


---

* [MAPREDUCE-1505](https://issues.apache.org/jira/browse/MAPREDUCE-1505) | *Major* | **Cluster class should create the rpc client only when needed**

Lazily construct a connection to the JobTracker from the job-submission client.


---

* [MAPREDUCE-1354](https://issues.apache.org/jira/browse/MAPREDUCE-1354) | *Critical* | **Incremental enhancements to the JobTracker for better scalability**

Incremental enhancements to the JobTracker include a no-lock version of JT.getTaskCompletion events, no lock on the JT while doing i/o during job-submission and several fixes to cut down configuration parsing during heartbeat-handling.


---

* [MAPREDUCE-1159](https://issues.apache.org/jira/browse/MAPREDUCE-1159) | *Trivial* | **Limit Job name on jobtracker.jsp to be 80 char long**

Job names on jobtracker.jsp should be 80 characters long at most.


---

* [MAPREDUCE-1118](https://issues.apache.org/jira/browse/MAPREDUCE-1118) | *Major* | **Capacity Scheduler scheduling information is hard to read / should be tabular format**

Add CapacityScheduler servlet to enhance web UI for queue information.


---

* [MAPREDUCE-927](https://issues.apache.org/jira/browse/MAPREDUCE-927) | *Major* | **Cleanup of task-logs should happen in TaskTracker instead of the Child**

Moved Task log cleanup into a separate thread in TaskTracker.  
Added configuration "mapreduce.job.userlog.retain.hours" to specify the time(in hours) for which the user-logs are to be retained after the job completion.


---

* [MAPREDUCE-572](https://issues.apache.org/jira/browse/MAPREDUCE-572) | *Minor* | **If #link is missing from uri format of -cacheArchive then streaming does not throw error.**

Improved streaming job failure when #link is missing from uri format of -cacheArchive. Earlier it used to fail when launching individual tasks, now it fails during job submission itself.


---

* [MAPREDUCE-478](https://issues.apache.org/jira/browse/MAPREDUCE-478) | *Minor* | **separate jvm param for mapper and reducer**

Allow map and reduce jvm parameters, environment variables and ulimit to be set separately.

Configuration changes:
      add mapred.map.child.java.opts
      add mapred.reduce.child.java.opts
      add mapred.map.child.env
      add mapred.reduce.child.ulimit
      add mapred.map.child.env
      add mapred.reduce.child.ulimit
      deprecated mapred.child.java.opts
      deprecated mapred.child.env
      deprecated mapred.child.ulimit


---

* [MAPREDUCE-220](https://issues.apache.org/jira/browse/MAPREDUCE-220) | *Major* | **Collecting cpu and memory usage for MapReduce tasks**

Collect cpu and memory statistics per task.



