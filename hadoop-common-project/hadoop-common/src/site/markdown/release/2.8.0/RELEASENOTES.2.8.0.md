
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
# Apache Hadoop  2.8.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-7713](https://issues.apache.org/jira/browse/HADOOP-7713) | *Trivial* | **dfs -count -q should label output column**

Added -v option to fs -count command to display a header record in the report.


---

* [HADOOP-8934](https://issues.apache.org/jira/browse/HADOOP-8934) | *Minor* | **Shell command ls should include sort options**

Options to sort output of fs -ls comment: -t (mtime), -S (size), -u (atime), -r (reverse)


---

* [HADOOP-11226](https://issues.apache.org/jira/browse/HADOOP-11226) | *Major* | **Add a configuration to set ipc.Client's traffic class with IPTOS\_LOWDELAY\|IPTOS\_RELIABILITY**

Use low latency TCP connections for hadoop IPC


---

* [HADOOP-9477](https://issues.apache.org/jira/browse/HADOOP-9477) | *Major* | **Add posixGroups support for LDAP groups mapping service**

Add posixGroups support for LDAP groups mapping service. The change in LDAPGroupMapping is compatible with previous scenario. In LDAP, the group mapping between {{posixAccount}} and {{posixGroup}} is different from the general LDAPGroupMapping, one of the differences is the {{"memberUid"}} will be used to mapping {{posixAccount}} and {{posixGroup}}. The feature will handle the mapping in internal when configuration {{hadoop.security.group.mapping.ldap.search.filter.user}} is set as "posixAccount" and {{hadoop.security.group.mapping.ldap.search.filter.group}} is "posixGroup".


---

* [YARN-3241](https://issues.apache.org/jira/browse/YARN-3241) | *Major* | **FairScheduler handles "invalid" queue names inconsistently**

FairScheduler does not allow queue names with leading or tailing spaces or empty sub-queue names anymore.


---

* [HDFS-7501](https://issues.apache.org/jira/browse/HDFS-7501) | *Major* | **TransactionsSinceLastCheckpoint can be negative on SBNs**

Fixed a bug where the StandbyNameNode's TransactionsSinceLastCheckpoint metric may slide into a negative number after every subsequent checkpoint.


---

* [HADOOP-11660](https://issues.apache.org/jira/browse/HADOOP-11660) | *Minor* | **Add support for hardware crc of HDFS checksums on ARM aarch64 architecture**

Add support for aarch64 CRC instructions


---

* [HADOOP-11731](https://issues.apache.org/jira/browse/HADOOP-11731) | *Major* | **Rework the changelog and releasenotes**

<!-- markdown -->
* The release notes now only contains JIRA issues with incompatible changes and actual release notes.  The generated format has been changed from HTML to markdown.

* The changelog is now automatically generated from data stored in JIRA rather than manually maintained. The format has been changed from pure text to markdown as well as containing more of the information that was previously stored in the release notes.

* In order to generate the changes file, python must be installed.

* New -Preleasedocs profile added to maven in order to trigger this functionality.


---

* [YARN-3365](https://issues.apache.org/jira/browse/YARN-3365) | *Major* | **Add support for using the 'tc' tool via container-executor**

Adding support for using the 'tc' tool in batch mode via container-executor. This is a prerequisite for traffic-shaping functionality that is necessary to support outbound bandwidth as a resource in YARN.


---

* [YARN-3443](https://issues.apache.org/jira/browse/YARN-3443) | *Major* | **Create a 'ResourceHandler' subsystem to ease addition of support for new resource types on the NM**

The current cgroups implementation is closely tied to supporting CPU as a resource . This patch separates out CGroups implementation into a reusable class as well as provides a simple ResourceHandler subsystem that will enable us to add support for new resource types on the NM - e.g Network, Disk etc.


---

* [HDFS-6666](https://issues.apache.org/jira/browse/HDFS-6666) | *Minor* | **Abort NameNode and DataNode startup if security is enabled but block access token is not enabled.**

NameNode and DataNode now abort during startup if attempting to run in secure mode, but block access tokens are not enabled by setting configuration property dfs.block.access.token.enable to true in hdfs-site.xml.  Previously, this case logged a warning, because this would be an insecure configuration.


---

* [YARN-3021](https://issues.apache.org/jira/browse/YARN-3021) | *Major* | **YARN's delegation-token handling disallows certain trust setups to operate properly over DistCp**

ResourceManager renews delegation tokens for applications. This behavior has been changed to renew tokens only if the token's renewer is a non-empty string. MapReduce jobs can instruct ResourceManager to skip renewal of tokens obtained from certain hosts by specifying the hosts with configuration mapreduce.job.hdfs-servers.token-renewal.exclude=\<host1\>,\<host2\>,..,\<hostN\>.


---

* [HADOOP-11746](https://issues.apache.org/jira/browse/HADOOP-11746) | *Major* | **rewrite test-patch.sh**

<!-- markdown -->
* test-patch.sh now has new output that is different than the previous versions
* test-patch.sh is now pluggable via the test-patch.d directory, with checkstyle and shellcheck tests included
* JIRA comments now use much more markup to improve readability
* test-patch.sh now supports either a file name, a URL, or a JIRA issue as input in developer mode
* If part of the patch testing code is changed, test-patch.sh will now attempt to re-executing itself using the new version.
* Some logic to try and reduce the amount of unnecessary tests.  For example, patches that only modify markdown should not run the Java compilation tests.
* Plugins for checkstyle, shellcheck, and whitespace now execute as necessary.
* New test code for mvn site
* A breakdown of the times needed to execute certain blocks as well as a total runtime is now reported to assist in fixing long running tests and optimize the entire process.
* Several new options
  * --resetrepo will put test-patch.sh in destructive mode, similar to a normal Jenkins run
  * --testlist allows one to provide a comma delimited list of test subsystems to forcibly execute
  * --modulelist to provide a comma delimited list of module tests to execute in addition to the ones that are automatically detected
  * --offline mode to attempt to stop connecting to the Internet for certain operations
* test-patch.sh now defaults to the POSIX equivalents on Solaris and Illumos-based operating systems
* shelldocs.py may be used to generate test-patch.sh API information
* FindBugs output is now listed on the JIRA comment
* lots of general code cleanup, including attempts to remove any local state files to reduce potential race conditions
* Some logic to determine if a patch is for a given major branch using several strategies as well as a particular git ref (using git+ref as part of the name).
* Some logic to determine if a patch references a particular JIRA issue.
* Unit tests are only flagged as necessary with native or Java code, since Hadoop has no framework in place yet for other types of unit tests.
* test-patch now exits with a failure status if problems arise trying to do git checkouts.  Previously the exit code was success.


---

* [YARN-3366](https://issues.apache.org/jira/browse/YARN-3366) | *Major* | **Outbound network bandwidth : classify/shape traffic originating from YARN containers**

1) A TrafficController class that provides an implementation for traffic shaping using tc. 
2) A ResourceHandler implementation for OutboundBandwidth as a resource - isolation/enforcement using cgroups and tc.


---

* [HADOOP-11861](https://issues.apache.org/jira/browse/HADOOP-11861) | *Major* | **test-patch.sh rewrite addendum patch**

<!-- markdown -->
* --build-native=false should work now
* --branch option lets one specify a branch to test against on the command line
* On certain Jenkins machines, the artifact directory sometimes gets deleted from outside the test-patch script.  There is now some code to try to detect, alert, and quick exit if that happens.
* Various semi-critical output and bug fixes


---

* [HADOOP-11843](https://issues.apache.org/jira/browse/HADOOP-11843) | *Major* | **Make setting up the build environment easier**

Includes a docker based solution for setting up a build environment with minimal effort.


---

* [HADOOP-11813](https://issues.apache.org/jira/browse/HADOOP-11813) | *Minor* | **releasedocmaker.py should use today's date instead of unreleased**

Use today instead of 'Unreleased' in releasedocmaker.py when --usetoday is given as an option.


---

* [HDFS-8226](https://issues.apache.org/jira/browse/HDFS-8226) | *Blocker* | **Non-HA rollback compatibility broken**

Non-HA rollback steps have been changed. Run the rollback command on the namenode (\`bin/hdfs namenode -rollback\`) before starting cluster with '-rollback' option using (sbin/start-dfs.sh -rollback).


---

* [HDFS-6888](https://issues.apache.org/jira/browse/HDFS-6888) | *Major* | **Allow selectively audit logging ops**

Specific HDFS ops can be selectively excluded from audit logging via 'dfs.namenode.audit.log.debug.cmdlist' configuration.


---

* [HDFS-8157](https://issues.apache.org/jira/browse/HDFS-8157) | *Major* | **Writes to RAM DISK reserve locked memory for block files**

This change requires setting the dfs.datanode.max.locked.memory configuration key to use the HDFS Lazy Persist feature. Its value limits the combined off-heap memory for blocks in RAM via caching and lazy persist writes.


---

* [HADOOP-11772](https://issues.apache.org/jira/browse/HADOOP-11772) | *Major* | **RPC Invoker relies on static ClientCache which has synchronized(this) blocks**

The Client#call() methods that are deprecated since 0.23 have been removed.


---

* [YARN-3684](https://issues.apache.org/jira/browse/YARN-3684) | *Major* | **Change ContainerExecutor's primary lifecycle methods to use a more extensible mechanism for passing information.**

Modifying key methods in ContainerExecutor to use context objects instead of an argument list. This is more extensible and less brittle.


---

* [YARN-2336](https://issues.apache.org/jira/browse/YARN-2336) | *Major* | **Fair scheduler REST api returns a missing '[' bracket JSON for deep queue tree**

Fix FairScheduler's REST api returns a missing '[' blacket JSON for childQueues.


---

* [HDFS-8486](https://issues.apache.org/jira/browse/HDFS-8486) | *Blocker* | **DN startup may cause severe data loss**

<!-- markdown -->
Public service notice:
* Every restart of a 2.6.x or 2.7.0 DN incurs a risk of unwanted block deletion.
* Apply this patch if you are running a pre-2.7.1 release.


---

* [HDFS-8270](https://issues.apache.org/jira/browse/HDFS-8270) | *Major* | **create() always retried with hardcoded timeout when file already exists with open lease**

Proxy level retries will not be done on AlreadyBeingCreatedExeption for create() op.


---

* [YARN-41](https://issues.apache.org/jira/browse/YARN-41) | *Major* | **The RM should handle the graceful shutdown of the NM.**

The behavior of shutdown a NM could be different (if NM work preserving is not enabled): NM will unregister to RM immediately rather than waiting for timeout to be LOST. A new status of NodeStatus - SHUTDOWN is involved which could affect UI, CLI and ClusterMetrics for node's status.


---

* [HADOOP-7139](https://issues.apache.org/jira/browse/HADOOP-7139) | *Major* | **Allow appending to existing SequenceFiles**

Existing sequence files can be appended.


---

* [HDFS-8582](https://issues.apache.org/jira/browse/HDFS-8582) | *Minor* | **Support getting a list of reconfigurable config properties and do not generate spurious reconfig warnings**

Add a new option "properties" to the "dfsadmin -reconfig" command to get a list of reconfigurable properties.


---

* [HDFS-6564](https://issues.apache.org/jira/browse/HDFS-6564) | *Major* | **Use slf4j instead of common-logging in hdfs-client**

Users may need special attention for this change while upgrading to this version. Previously hdfs client was using commons-logging as the logging framework. With this change it will use slf4j framework. For more details about slf4j, please see: http://www.slf4j.org/manual.html. Also, org.apache.hadoop.hdfs.protocol.CachePoolInfo#LOG public static member variable has been removed as it is not used anywhere. Users need to correct their code if any one has a reference to this variable. One can retrieve the named logger via the logging framework of their choice directly like, org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(org.apache.hadoop.hdfs.protocol.CachePoolInfo.class);


---

* [YARN-3823](https://issues.apache.org/jira/browse/YARN-3823) | *Minor* | **Fix mismatch in default values for yarn.scheduler.maximum-allocation-vcores property**

Default value for 'yarn.scheduler.maximum-allocation-vcores' changed from 32 to 4.


---

* [HADOOP-5732](https://issues.apache.org/jira/browse/HADOOP-5732) | *Minor* | **Add SFTP FileSystem**

Added SFTP filesystem by using the JSch library.


---

* [YARN-3069](https://issues.apache.org/jira/browse/YARN-3069) | *Major* | **Document missing properties in yarn-default.xml**

Documented missing properties and added the regression test to verify that there are no missing properties in yarn-default.xml.


---

* [MAPREDUCE-6427](https://issues.apache.org/jira/browse/MAPREDUCE-6427) | *Minor* | **Fix typo in JobHistoryEventHandler**

There is a typo in the event string "WORKFLOW\_ID" (as "WORKLFOW\_ID").  The branch-2 change will publish both event strings for compatibility with consumers, but the misspelled metric will be removed in trunk.


---

* [HADOOP-12209](https://issues.apache.org/jira/browse/HADOOP-12209) | *Minor* | **Comparable type should be in FileStatus**

**WARNING: No release note provided for this change.**


---

* [HDFS-7582](https://issues.apache.org/jira/browse/HDFS-7582) | *Major* | **Enforce maximum number of ACL entries separately per access and default.**

Limit on Maximum number of ACL entries(32) will be enforced separately on access and default ACLs. So in total, max. 64 ACL entries can be present in a ACL spec.


---

* [HADOOP-12269](https://issues.apache.org/jira/browse/HADOOP-12269) | *Major* | **Update aws-sdk dependency to 1.10.6; move to aws-sdk-s3**

The Maven dependency on aws-sdk has been changed to aws-sdk-s3 and the version bumped. Applications depending on transitive dependencies pulled in by aws-sdk and not aws-sdk-s3 might not work.


---

* [HADOOP-12352](https://issues.apache.org/jira/browse/HADOOP-12352) | *Trivial* | **Delay in checkpointing Trash can leave trash for 2 intervals before deleting**

Fixes an Trash related issue wherein a delay in the periodic checkpointing of one user's directory causes the subsequent user directory checkpoints to carry a newer timestamp, thereby delaying their eventual deletion.


---

* [HDFS-8900](https://issues.apache.org/jira/browse/HDFS-8900) | *Major* | **Compact XAttrs to optimize memory footprint.**

The config key "dfs.namenode.fs-limits.max-xattr-size" can no longer be set to a value of 0 (previously used to indicate unlimited) or a value greater than 32KB. This is a constraint on xattr size similar to many local filesystems.


---

* [HDFS-8890](https://issues.apache.org/jira/browse/HDFS-8890) | *Major* | **Allow admin to specify which blockpools the balancer should run on**

Adds a new blockpools flag to the balancer. This allows admins to specify which blockpools the balancer will run on.
Usage:
-blockpools \<comma-separated list of blockpool ids\>
The balancer will only run on blockpools included in this list.


---

* [YARN-4087](https://issues.apache.org/jira/browse/YARN-4087) | *Major* | **Followup fixes after YARN-2019 regarding RM behavior when state-store error occurs**

Set YARN\_FAIL\_FAST to be false by default. If HA is enabled and if there's any state-store error, after the retry operation failed, we always transition RM to standby state.


---

* [HADOOP-12384](https://issues.apache.org/jira/browse/HADOOP-12384) | *Major* | **Add "-direct" flag option for fs copy so that user can choose not to create ".\_COPYING\_" file**

An option '-d' added for all command-line copy commands to skip intermediate '.COPYING' file creation.


---

* [HDFS-8929](https://issues.apache.org/jira/browse/HDFS-8929) | *Major* | **Add a metric to expose the timestamp of the last journal**

Exposed a metric 'LastJournalTimestamp' for JournalNode


---

* [HDFS-7116](https://issues.apache.org/jira/browse/HDFS-7116) | *Major* | **Add a command to get the balancer bandwidth**

Exposed command "-getBalancerBandwidth" in dfsadmin to get the bandwidth of balancer.


---

* [HDFS-8829](https://issues.apache.org/jira/browse/HDFS-8829) | *Major* | **Make SO\_RCVBUF and SO\_SNDBUF size configurable for DataTransferProtocol sockets and allow configuring auto-tuning**

HDFS-8829 introduces two new configuration settings: dfs.datanode.transfer.socket.send.buffer.size and dfs.datanode.transfer.socket.recv.buffer.size. These settings can be used to control the socket send buffer and receive buffer sizes respectively on the DataNode for client-DataNode and DataNode-DataNode connections. The default values of both settings are 128KB for backwards compatibility. For optimum performance it is recommended to set these values to zero to enable the OS networking stack to auto-tune buffer sizes.


---

* [YARN-313](https://issues.apache.org/jira/browse/YARN-313) | *Critical* | **Add Admin API for supporting node resource configuration in command line**

After this patch, the feature to support NM resource dynamically configuration is completed, so that user can configure NM with new resource without bring NM down or decommissioned.
Two CLIs are provided to support update resources on individual node or a batch of nodes:
1. Update resource on single node: yarn rmadmin -updateNodeResource [NodeID] [MemSize] [vCores] 
2. Update resource on a batch of nodes: yarn rmadmin -refreshNodesResources, that reflect nodes' resource configuration defined in dynamic-resources.xml which is loaded by RM dynamically (like capacity-scheduler.xml or fair-scheduler.xml). 
The first version of configuration format is:
\<configuration\>
  \<property\>
    \<name\>yarn.resource.dynamic.nodes\</name\>
    \<value\>h1:1234\</value\>
  \</property\>
  \<property\>
    \<name\>yarn.resource.dynamic.h1:1234.vcores\</name\>
    \<value\>16\</value\>
  \</property\>
  \<property\>
    \<name\>yarn.resource.dynamic.h1:1234.memory\</name\>
    \<value\>1024\</value\>
  \</property\>
\</configuration\>


---

* [HADOOP-12416](https://issues.apache.org/jira/browse/HADOOP-12416) | *Major* | **Trash messages should be handled by Logger instead of being delivered on System.out**

Now trash message is not printed to System.out. It is handled by Logger instead.


---

* [HDFS-9063](https://issues.apache.org/jira/browse/HDFS-9063) | *Major* | **Correctly handle snapshot path for getContentSummary**

The jira made the following changes:
1. Fix a bug to exclude newly-created files from quota usage calculation for a snapshot path. 
2. Number of snapshots is no longer counted as directory number in getContentSummary result.


---

* [HADOOP-12360](https://issues.apache.org/jira/browse/HADOOP-12360) | *Minor* | **Create StatsD metrics2 sink**

Added StatsD metrics2 sink


---

* [HDFS-9013](https://issues.apache.org/jira/browse/HDFS-9013) | *Major* | **Deprecate NameNodeMXBean#getNNStarted in branch2 and remove from trunk**

NameNodeMXBean#getNNStarted()  metric is deprecated in branch-2 and removed from trunk.


---

* [HADOOP-12437](https://issues.apache.org/jira/browse/HADOOP-12437) | *Major* | **Allow SecurityUtil to lookup alternate hostnames**

HADOOP-12437 introduces two new configuration settings: hadoop.security.dns.interface and hadoop.security.dns.nameserver. These settings can be used to control how Hadoop service instances look up their own hostname and may be required in some multi-homed environments where hosts are configured with multiple hostnames in DNS or hosts files. They supersede the existing settings dfs.datanode.dns.interface and dfs.datanode.dns.nameserver.


---

* [HADOOP-12446](https://issues.apache.org/jira/browse/HADOOP-12446) | *Major* | **Undeprecate createNonRecursive()**

FileSystem#createNonRecursive() is undeprecated.


---

* [HDFS-8696](https://issues.apache.org/jira/browse/HDFS-8696) | *Major* | **Make the lower and higher watermark in the DN Netty server configurable**

Introduced two new configuration dfs.webhdfs.netty.low.watermark and dfs.webhdfs.netty.high.watermark to enable tuning the size of the buffers of the Netty server inside Datanodes.


---

* [HDFS-9184](https://issues.apache.org/jira/browse/HDFS-9184) | *Major* | **Logging HDFS operation's caller context into audit logs**

The feature needs to enabled by setting "hadoop.caller.context.enabled" to true. When the feature is used, additional fields are written into namenode audit log records.


---

* [HDFS-9259](https://issues.apache.org/jira/browse/HDFS-9259) | *Major* | **Make SO\_SNDBUF size configurable at DFSClient side for hdfs write scenario**

Introduces a new configuration setting dfs.client.socket.send.buffer.size to control the socket send buffer size for writes. Setting it to zero enables TCP auto-tuning on systems that support it.


---

* [HDFS-9311](https://issues.apache.org/jira/browse/HDFS-9311) | *Major* | **Support optional offload of NameNode HA service health checks to a separate RPC server.**

There is now support for offloading HA health check RPC activity to a separate RPC server endpoint running within the NameNode process.  This may improve reliability of HA health checks and prevent spurious failovers in highly overloaded conditions.  For more details, please refer to the hdfs-default.xml documentation for properties dfs.namenode.lifeline.rpc-address, dfs.namenode.lifeline.rpc-bind-host and dfs.namenode.lifeline.handler.count.


---

* [HDFS-6200](https://issues.apache.org/jira/browse/HDFS-6200) | *Major* | **Create a separate jar for hdfs-client**

Projects that access HDFS can depend on the hadoop-hdfs-client module instead of the hadoop-hdfs module to avoid pulling in unnecessary dependency.
Please note that hadoop-hdfs-client module could miss class like ConfiguredFailoverProxyProvider. So if a cluster is in HA deployment, we should still use hadoop-hdfs instead.


---

* [HDFS-9057](https://issues.apache.org/jira/browse/HDFS-9057) | *Major* | **allow/disallow snapshots via webhdfs**

Snapshots can be allowed/disallowed on a directory via WebHdfs from users with superuser privilege.


---

* [MAPREDUCE-5485](https://issues.apache.org/jira/browse/MAPREDUCE-5485) | *Critical* | **Allow repeating job commit by extending OutputCommitter API**

Previously, the MR job will get failed if AM get restarted for some reason (like node failure, etc.) during its doing commit job no matter if AM attempts reach to the maximum attempts. 
In this improvement, we add a new API isCommitJobRepeatable() to OutputCommitter interface which to indicate if job's committer can do commitJob again if previous commit work is interrupted by NM/AM failures, etc. The instance of OutputCommitter, which support repeatable job commit (like FileOutputCommitter in algorithm 2), can allow AM to continue the commitJob() after AM restart as a new attempt.


---

* [HADOOP-12313](https://issues.apache.org/jira/browse/HADOOP-12313) | *Critical* | **NPE in JvmPauseMonitor when calling stop() before start()**

Allow stop() before start() completed in JvmPauseMonitor


---

* [HDFS-9433](https://issues.apache.org/jira/browse/HDFS-9433) | *Major* | **DFS getEZForPath API on a non-existent file should throw FileNotFoundException**

Unify the behavior of dfs.getEZForPath() API when getting a non-existent normal file and non-existent ezone file by throwing FileNotFoundException


---

* [HDFS-8335](https://issues.apache.org/jira/browse/HDFS-8335) | *Major* | **FSNamesystem should construct FSPermissionChecker only if permission is enabled**

Only check permissions when permissions enabled in FSDirStatAndListingOp.getFileInfo() and getListingInt()


---

* [HDFS-8831](https://issues.apache.org/jira/browse/HDFS-8831) | *Major* | **Trash Support for deletion in HDFS encryption zone**

Add Trash support for deleting files within encryption zones. Deleted files will remain encrypted and they will be moved to a “.Trash” subdirectory under the root of the encryption zone, prefixed by $USER/current. Checkpoint and expunge continue to work like the existing Trash.


---

* [HDFS-9214](https://issues.apache.org/jira/browse/HDFS-9214) | *Major* | **Support reconfiguring dfs.datanode.balance.max.concurrent.moves without DN restart**

Steps to reconfigure:
1. change value of the parameter in corresponding xml configuration file
2. to reconfigure, run
    hdfs dfsadmin -reconfig datanode \<dn\_addr\>:\<ipc\_port\> start
3. repeat step 2 until all DNs are reconfigured
4. to check status of the most recent reconfigure operation, run
    hdfs dfsadmin -reconfig datanode \<dn\_addr\>:\<ipc\_port\> status
5. to query a list reconfigurable properties on DN, run
    hdfs dfsadmin -reconfig datanode \<dn\_addr\>:\<ipc\_port\> properties


---

* [YARN-3623](https://issues.apache.org/jira/browse/YARN-3623) | *Major* | **We should have a config to indicate the Timeline Service version**

Add a new configuration "yarn.timeline-service.version" to indicate what is the current version of the running timeline service. For example, if "yarn.timeline-service.version" is 1.5, and "yarn.timeline-service.enabled" is true, it means the cluster will and should bring up the timeline service v.1.5. On the client side, if the client uses the same version of timeline service, it should succeed. If the client chooses to use a smaller version in spite of this, then depending on how robust the compatibility story is between versions, the results may vary.


---

* [YARN-4207](https://issues.apache.org/jira/browse/YARN-4207) | *Major* | **Add a non-judgemental YARN app completion status**

Adds the ENDED attribute to o.a.h.yarn.api.records.FinalApplicationStatus


---

* [HADOOP-12657](https://issues.apache.org/jira/browse/HADOOP-12657) | *Minor* | **Add a option to skip newline on empty files with getMerge -nl**

Added -skip-empty-file option to hadoop fs -getmerge command. With the option, delimiter (LF) is not printed for empty files even if -nl option is used.


---

* [HADOOP-11252](https://issues.apache.org/jira/browse/HADOOP-11252) | *Critical* | **RPC client does not time out by default**

This fix includes public method interface change.
A follow-up JIRA issue for this incompatibility for branch-2.7 is HADOOP-13579.


---

* [HDFS-9047](https://issues.apache.org/jira/browse/HDFS-9047) | *Major* | **Retire libwebhdfs**

libwebhdfs has been retired in 2.8.0 due to the lack of maintenance.


---

* [HADOOP-11262](https://issues.apache.org/jira/browse/HADOOP-11262) | *Major* | **Enable YARN to use S3A**

S3A has been made accessible through the FileContext API.


---

* [HADOOP-12635](https://issues.apache.org/jira/browse/HADOOP-12635) | *Major* | **Adding Append API support for WASB**

The Azure Blob Storage file system (WASB) now includes optional support for use of the append API by a single writer on a path.  Please note that the implementation differs from the semantics of HDFS append.  HDFS append internally guarantees that only a single writer may append to a path at a given time.  WASB does not enforce this guarantee internally.  Instead, the application must enforce access by a single writer, such as by running single-threaded or relying on some external locking mechanism to coordinate concurrent processes.  Refer to the Azure Blob Storage documentation page for more details on enabling append in configuration.


---

* [HADOOP-12651](https://issues.apache.org/jira/browse/HADOOP-12651) | *Major* | **Replace dev-support with wrappers to Yetus**

<!-- markdown -->

* Major portions of dev-support have been replaced with wrappers to Apache Yetus:
  * releasedocmaker.py is now dev-support/bin/releasedocmaker
  * shelldocs.py is now dev-support/bin/shelldocs
  * smart-apply-patch.sh is now dev-support/bin/smart-apply-patch
  * test-patch.sh is now dev-support/bin/test-patch
* See the dev-support/README.md file for more details on how to control the wrappers to various degrees.


---

* [HDFS-9503](https://issues.apache.org/jira/browse/HDFS-9503) | *Major* | **Replace -namenode option with -fs for NNThroughputBenchmark**

The patch replaces -namenode option with -fs for specifying the remote name node against which the benchmark is running. Before this patch, if '-namenode' was not given, the benchmark would run in standalone mode, ignoring the 'fs.defaultFS' in config file even if it's remote. With this patch, the benchmark, as other tools, will rely on the 'fs.defaultFS' config, which is overridable by -fs command option, to run standalone mode or remote mode.


---

* [HADOOP-12426](https://issues.apache.org/jira/browse/HADOOP-12426) | *Minor* | **Add Entry point for Kerberos health check**

Hadoop now includes a shell command named KDiag that helps with diagnosis of Kerberos misconfiguration problems.  Please refer to the Secure Mode documentation for full details on usage of the command.


---

* [HADOOP-12805](https://issues.apache.org/jira/browse/HADOOP-12805) | *Major* | **Annotate CanUnbuffer with @InterfaceAudience.Public**

Made CanBuffer interface public for use in client applications.


---

* [HADOOP-12548](https://issues.apache.org/jira/browse/HADOOP-12548) | *Major* | **Read s3a creds from a Credential Provider**

The S3A Hadoop-compatible file system now support reading its S3 credentials from the Hadoop Credential Provider API in addition to XML configuration files.


---

* [HDFS-9711](https://issues.apache.org/jira/browse/HDFS-9711) | *Major* | **Integrate CSRF prevention filter in WebHDFS.**

WebHDFS now supports options to enforce cross-site request forgery (CSRF) prevention for HTTP requests to both the NameNode and the DataNode.  Please refer to the updated WebHDFS documentation for a description of this feature and further details on how to configure it.


---

* [HADOOP-12794](https://issues.apache.org/jira/browse/HADOOP-12794) | *Major* | **Support additional compression levels for GzipCodec**

Added New compression levels for GzipCodec that can be set in zlib.compress.level


---

* [HDFS-9425](https://issues.apache.org/jira/browse/HDFS-9425) | *Major* | **Expose number of blocks per volume as a metric**

Number of blocks per volume is made available as a metric.


---

* [HADOOP-12668](https://issues.apache.org/jira/browse/HADOOP-12668) | *Critical* | **Support excluding weak Ciphers in HttpServer2 through ssl-server.xml**

The Code Changes include following:
- Modified DFSUtil.java in Apache HDFS project for supplying new parameter ssl.server.exclude.cipher.list
- Modified HttpServer2.java in Apache Hadoop-common project to work with new parameter and exclude ciphers using jetty setExcludeCihers method.
- Modfied associated test classes to owrk with existing code and also cover the newfunctionality in junit


---

* [HADOOP-12555](https://issues.apache.org/jira/browse/HADOOP-12555) | *Minor* | **WASB to read credentials from a credential provider**

The hadoop-azure file system now supports configuration of the Azure Storage account credentials using the standard Hadoop Credential Provider API.  For details, please refer to the documentation on hadoop-azure and the Credential Provider API.


---

* [MAPREDUCE-6622](https://issues.apache.org/jira/browse/MAPREDUCE-6622) | *Critical* | **Add capability to set JHS job cache to a task-based limit**

Two recommendations for the mapreduce.jobhistory.loadedtasks.cache.size property:
1) For every 100k of cache size, set the heap size of the Job History Server to 1.2GB.  For example, mapreduce.jobhistory.loadedtasks.cache.size=500000, heap size=6GB.
2) Make sure that the cache size is larger than the number of tasks required for the largest job run on the cluster.  It might be a good idea to set the value slightly higher (say, 20%) in order to allow for job size growth.


---

* [HADOOP-12552](https://issues.apache.org/jira/browse/HADOOP-12552) | *Minor* | **Fix undeclared/unused dependency to httpclient**

Dependency on commons-httpclient::commons-httpclient was removed from hadoop-common. Downstream projects using commons-httpclient transitively provided by hadoop-common need to add explicit dependency to their pom. Since commons-httpclient is EOL, it is recommended to migrate to org.apache.httpcomponents:httpclient which is the successor.


---

* [HDFS-8791](https://issues.apache.org/jira/browse/HDFS-8791) | *Blocker* | **block ID-based DN storage layout can be very slow for datanode on ext4**

HDFS-8791 introduces a new datanode layout format. This layout is identical to the previous block id based layout except it has a smaller 32x32 sub-directory structure in each data storage. On startup, the datanode will automatically upgrade it's storages to this new layout. Currently, datanode layout changes support rolling upgrades, on the other hand downgrading is not supported between datanode layout changes and a rollback would be required.


---

* [HDFS-9887](https://issues.apache.org/jira/browse/HDFS-9887) | *Major* | **WebHdfs socket timeouts should be configurable**

Added new configuration options: dfs.webhdfs.socket.connect-timeout and dfs.webhdfs.socket.read-timeout both defaulting to 60s.


---

* [HADOOP-11792](https://issues.apache.org/jira/browse/HADOOP-11792) | *Major* | **Remove all of the CHANGES.txt files**

With the introduction of the markdown-formatted and automatically built changes file, the CHANGES.txt files have been eliminated.


---

* [HDFS-9239](https://issues.apache.org/jira/browse/HDFS-9239) | *Major* | **DataNode Lifeline Protocol: an alternative protocol for reporting DataNode liveness**

This release adds a new feature called the DataNode Lifeline Protocol.  If configured, then DataNodes can report that they are still alive to the NameNode via a fallback protocol, separate from the existing heartbeat messages.  This can prevent the NameNode from incorrectly marking DataNodes as stale or dead in highly overloaded clusters where heartbeat processing is suffering delays.  For more information, please refer to the hdfs-default.xml documentation for several new configuration properties: dfs.namenode.lifeline.rpc-address, dfs.namenode.lifeline.rpc-bind-host, dfs.datanode.lifeline.interval.seconds, dfs.namenode.lifeline.handler.ratio and dfs.namenode.lifeline.handler.count.


---

* [YARN-4785](https://issues.apache.org/jira/browse/YARN-4785) | *Major* | **inconsistent value type of the "type" field for LeafQueueInfo in response of RM REST API - cluster/scheduler**

Fix inconsistent value type ( String and Array ) of the "type" field for LeafQueueInfo in response of RM REST API


---

* [MAPREDUCE-6670](https://issues.apache.org/jira/browse/MAPREDUCE-6670) | *Minor* | **TestJobListCache#testEviction sometimes fails on Windows with timeout**

Backport the fix to 2.7 and 2.8


---

* [HDFS-9945](https://issues.apache.org/jira/browse/HDFS-9945) | *Major* | **Datanode command for evicting writers**

This new dfsadmin command, evictWriters, stops active block writing activities on a data node. The affected writes will continue without the node after a write pipeline recovery. This is useful when data node decommissioning is blocked by slow writers. If issued against a non-decommissioing data node, all current writers will be stopped, but new write requests will continue to be served.


---

* [HADOOP-12963](https://issues.apache.org/jira/browse/HADOOP-12963) | *Minor* | **Allow using path style addressing for accessing the s3 endpoint**

Add new flag to allow supporting path style addressing for s3a


---

* [HDFS-3702](https://issues.apache.org/jira/browse/HDFS-3702) | *Minor* | **Add an option for NOT writing the blocks locally if there is a datanode on the same box as the client**

This patch will attempt to allocate all replicas to remote DataNodes, by adding local DataNode to the excluded DataNodes. If no sufficient replicas can be obtained, it will fall back to default block placement policy, which writes one replica to local DataNode.


---

* [HDFS-9902](https://issues.apache.org/jira/browse/HDFS-9902) | *Major* | **Support different values of dfs.datanode.du.reserved per storage type**

Reserved space can be configured independently for different storage types for clusters with heterogeneous storage. The 'dfs.datanode.du.reserved' property name can be suffixed with a storage types (i.e. one of ssd, disk, archival or ram\_disk). e.g. reserved space for RAM\_DISK storage can be configured using the property 'dfs.datanode.du.reserved.ram\_disk'. If specific storage type reservation is not configured then the value specified by 'dfs.datanode.du.reserved' will be used for all volumes.


---

* [HDFS-10324](https://issues.apache.org/jira/browse/HDFS-10324) | *Major* | **Trash directory in an encryption zone should be pre-created with correct permissions**

HDFS will create a ".Trash" subdirectory when creating a new encryption zone to support soft delete for files deleted within the encryption zone. A new "crypto -provisionTrash" command has been introduced to provision trash directories for encryption zones created with Apache Hadoop minor releases prior to 2.8.0.


---

* [HADOOP-13122](https://issues.apache.org/jira/browse/HADOOP-13122) | *Minor* | **Customize User-Agent header sent in HTTP requests by S3A.**

S3A now includes the current Hadoop version in the User-Agent string passed through the AWS SDK to the S3 service.  Users also may include optional additional information to identify their application.  See the documentation of configuration property fs.s3a.user.agent.prefix for further details.


---

* [HADOOP-12723](https://issues.apache.org/jira/browse/HADOOP-12723) | *Major* | **S3A: Add ability to plug in any AWSCredentialsProvider**

Users can integrate a custom credential provider with S3A.  See documentation of configuration property fs.s3a.aws.credentials.provider for further details.


---

* [MAPREDUCE-6607](https://issues.apache.org/jira/browse/MAPREDUCE-6607) | *Minor* | **Enable regex pattern matching when mapreduce.task.files.preserve.filepattern is set**

Before this fix, the files in .staging directory are always preserved when mapreduce.task.files.preserve.filepattern is set. After this fix, the files in .staging directory are preserved if the name of the directory matches the regex pattern specified by mapreduce.task.files.preserve.filepattern.


---

* [YARN-5035](https://issues.apache.org/jira/browse/YARN-5035) | *Major* | **FairScheduler: Adjust maxAssign dynamically when assignMultiple is turned on**

Introducing a new configuration "yarn.scheduler.fair.dynamic.max.assign" to dynamically determine the resources to assign per heartbeat when assignmultiple is turned on. When turned on, the scheduler allocates roughly half of the remaining resources overriding any max.assign settings configured. This is turned ON by default.


---

* [YARN-5132](https://issues.apache.org/jira/browse/YARN-5132) | *Critical* | **Exclude generated protobuf sources from YARN Javadoc build**

Exclude javadocs for proto-generated java classes.


---

* [HADOOP-13105](https://issues.apache.org/jira/browse/HADOOP-13105) | *Major* | **Support timeouts in LDAP queries in LdapGroupsMapping.**

This patch adds two new config keys for supporting timeouts in LDAP query operations. The property "hadoop.security.group.mapping.ldap.connection.timeout.ms" is the connection timeout (in milliseconds), within which period if the LDAP provider doesn't establish a connection, it will abort the connect attempt. The property "hadoop.security.group.mapping.ldap.read.timeout.ms" is the read timeout (in milliseconds), within which period if the LDAP provider doesn't get a LDAP response, it will abort the read attempt.


---

* [HADOOP-13155](https://issues.apache.org/jira/browse/HADOOP-13155) | *Major* | **Implement TokenRenewer to renew and cancel delegation tokens in KMS**

Enables renewal and cancellation of KMS delegation tokens. hadoop.security.key.provider.path needs to be configured to reach the key provider.


---

* [HADOOP-12807](https://issues.apache.org/jira/browse/HADOOP-12807) | *Minor* | **S3AFileSystem should read AWS credentials from environment variables**

Adds support to S3AFileSystem for reading AWS credentials from environment variables.


---

* [HDFS-10375](https://issues.apache.org/jira/browse/HDFS-10375) | *Trivial* | **Remove redundant TestMiniDFSCluster.testDualClusters**

Remove redundent TestMiniDFSCluster.testDualClusters to save time.


---

* [HDFS-10220](https://issues.apache.org/jira/browse/HDFS-10220) | *Major* | **A large number of expired leases can make namenode unresponsive and cause failover**

Two new configuration have been added "dfs.namenode.lease-recheck-interval-ms" and "dfs.namenode.max-lock-hold-to-release-lease-ms" to fine tune the duty cycle with which the Namenode recovers old leases.


---

* [HADOOP-13237](https://issues.apache.org/jira/browse/HADOOP-13237) | *Minor* | **s3a initialization against public bucket fails if caller lacks any credentials**

S3A now supports read access to a public S3 bucket even if the client does not configure any AWS credentials.  See the documentation of configuration property fs.s3a.aws.credentials.provider for further details.


---

* [HADOOP-12537](https://issues.apache.org/jira/browse/HADOOP-12537) | *Minor* | **S3A to support Amazon STS temporary credentials**

S3A now supports use of AWS Security Token Service temporary credentials for authentication to S3.  Refer to the documentation of configuration property fs.s3a.session.token for further details.


---

* [HADOOP-12892](https://issues.apache.org/jira/browse/HADOOP-12892) | *Blocker* | **fix/rewrite create-release**

This rewrites the release process with a new dev-support/bin/create-release script.  See http://wiki.apache.org/hadoop/HowToRelease for updated instructions on how to use it.


---

* [HADOOP-3733](https://issues.apache.org/jira/browse/HADOOP-3733) | *Minor* | **"s3:" URLs break when Secret Key contains a slash, even if encoded**

Allows userinfo component of URI authority to contain a slash (escaped as %2F).  Especially useful for accessing AWS S3 with distcp or hadoop fs.


---

* [HADOOP-13203](https://issues.apache.org/jira/browse/HADOOP-13203) | *Major* | **S3A: Support fadvise "random" mode for high performance readPositioned() reads**

S3A has added support for configurable input policies.  Similar to fadvise, this configuration provides applications with a way to specify their expected access pattern (sequential or random) while reading a file.  S3A then performs optimizations tailored to that access pattern.  See site documentation of the fs.s3a.experimental.input.fadvise configuration property for more details.  Please be advised that this feature is experimental and subject to backward-incompatible changes in future releases.


---

* [HADOOP-13263](https://issues.apache.org/jira/browse/HADOOP-13263) | *Major* | **Reload cached groups in background after expiry**

hadoop.security.groups.cache.background.reload can be set to true to enable background reload of expired groups cache entries. This setting can improve the performance of services that use Groups.java (e.g. the NameNode) when group lookups are slow. The setting is disabled by default.


---

* [HDFS-10440](https://issues.apache.org/jira/browse/HDFS-10440) | *Major* | **Improve DataNode web UI**

DataNode Web UI has been improved with new HTML5 page, showing useful information.


---

* [HADOOP-13139](https://issues.apache.org/jira/browse/HADOOP-13139) | *Major* | **Branch-2: S3a to use thread pool that blocks clients**

The configuration option 'fs.s3a.threads.core' is no longer supported. The string is still defined in org.apache.hadoop.fs.s3a.Constants.CORE\_THREADS, however its value is ignored. If it is set, a warning message will be printed when initializing the S3A filesystem


---

* [HADOOP-13382](https://issues.apache.org/jira/browse/HADOOP-13382) | *Major* | **remove unneeded commons-httpclient dependencies from POM files in Hadoop and sub-projects**

Dependencies on commons-httpclient have been removed. Projects with undeclared transitive dependencies on commons-httpclient, previously provided via hadoop-common or hadoop-client, may find this to be an incompatible change. Such project are also potentially exposed to the commons-httpclient CVE, and should be fixed for that reason as well.


---

* [HDFS-7933](https://issues.apache.org/jira/browse/HDFS-7933) | *Major* | **fsck should also report decommissioning replicas.**

The output of hdfs fsck now also contains information about decommissioning replicas.


---

* [HADOOP-13208](https://issues.apache.org/jira/browse/HADOOP-13208) | *Minor* | **S3A listFiles(recursive=true) to do a bulk listObjects instead of walking the pseudo-tree of directories**

S3A has optimized the listFiles method by doing a bulk listing of all entries under a path in a single S3 operation instead of recursively walking the directory tree.  The listLocatedStatus method has been optimized by fetching results from S3 lazily as the caller traverses the returned iterator instead of doing an eager fetch of all possible results.


---

* [HADOOP-13252](https://issues.apache.org/jira/browse/HADOOP-13252) | *Minor* | **Tune S3A provider plugin mechanism**

S3A now supports configuration of multiple credential provider classes for authenticating to S3.  These are loaded and queried in sequence for a valid set of credentials.  For more details, refer to the description of the fs.s3a.aws.credentials.provider configuration property or the S3A documentation page.


---

* [HDFS-8986](https://issues.apache.org/jira/browse/HDFS-8986) | *Major* | **Add option to -du to calculate directory space usage excluding snapshots**

Add a -x option for "hdfs -du" and "hdfs -count" commands to exclude snapshots from being calculated.


---

* [HDFS-10760](https://issues.apache.org/jira/browse/HDFS-10760) | *Major* | **DataXceiver#run() should not log InvalidToken exception as an error**

Log InvalidTokenException at trace level in DataXceiver#run().


---

* [YARN-5549](https://issues.apache.org/jira/browse/YARN-5549) | *Critical* | **AMLauncher#createAMContainerLaunchContext() should not log the command to be launched indiscriminately**

Introduces a new configuration property, yarn.resourcemanager.amlauncher.log.command.  If this property is set to true, then the AM command being launched will be masked in the RM log.


---

* [HDFS-10489](https://issues.apache.org/jira/browse/HDFS-10489) | *Minor* | **Deprecate dfs.encryption.key.provider.uri for HDFS encryption zones**

The configuration dfs.encryption.key.provider.uri is deprecated. To configure key provider in HDFS, please use hadoop.security.key.provider.path.


---

* [HDFS-10914](https://issues.apache.org/jira/browse/HDFS-10914) | *Critical* | **Move remnants of oah.hdfs.client to hadoop-hdfs-client**

The remaining classes in the org.apache.hadoop.hdfs.client package have been moved from hadoop-hdfs to hadoop-hdfs-client.


---

* [HADOOP-12667](https://issues.apache.org/jira/browse/HADOOP-12667) | *Major* | **s3a: Support createNonRecursive API**

S3A now provides a working implementation of the FileSystem#createNonRecursive method.


---

* [HDFS-10609](https://issues.apache.org/jira/browse/HDFS-10609) | *Major* | **Uncaught InvalidEncryptionKeyException during pipeline recovery may abort downstream applications**

If pipeline recovery fails due to expired encryption key, attempt to refresh the key and retry.


---

* [HDFS-10797](https://issues.apache.org/jira/browse/HDFS-10797) | *Major* | **Disk usage summary of snapshots causes renamed blocks to get counted twice**

Disk usage summaries previously incorrectly counted files twice if they had been renamed (including files moved to Trash) since being snapshotted. Summaries now include current data plus snapshotted data that is no longer under the directory either due to deletion or being moved outside of the directory.


---

* [HDFS-10883](https://issues.apache.org/jira/browse/HDFS-10883) | *Major* | **\`getTrashRoot\`'s behavior is not consistent in DFS after enabling EZ.**

If root path / is an encryption zone, the old DistributedFileSystem#getTrashRoot(new Path("/")) returns
/user/$USER/.Trash
which is a wrong behavior. The correct value should be
/.Trash/$USER


---

* [HADOOP-13560](https://issues.apache.org/jira/browse/HADOOP-13560) | *Major* | **S3ABlockOutputStream to support huge (many GB) file writes**

This mechanism replaces the (experimental) fast output stream of Hadoop 2.7.x, combining better scalability options with instrumentation. Consult the S3A documentation to see the extra configuration operations.


---

* [HDFS-11018](https://issues.apache.org/jira/browse/HDFS-11018) | *Major* | **Incorrect check and message in FsDatasetImpl#invalidate**

Improves the error message when datanode removes a replica which is not found.


---

* [YARN-5767](https://issues.apache.org/jira/browse/YARN-5767) | *Major* | **Fix the order that resources are cleaned up from the local Public/Private caches**

This issue fixes a bug in how resources are evicted from the PUBLIC and PRIVATE yarn local caches used by the node manager for resource localization. In summary, the caches are now properly cleaned based on an LRU policy across both the public and private caches.


---

* [HDFS-11048](https://issues.apache.org/jira/browse/HDFS-11048) | *Major* | **Audit Log should escape control characters**

HDFS audit logs are formatted as individual lines, each of which has a few of key-value pair fields. Some of the values come from client request (e.g. src, dst). Before this patch the control characters including \\t \\n etc are not escaped in audit logs. That may break lines unexpectedly or introduce additional table character (in the worst case, both) within a field. Tools that parse audit logs had to deal with this case carefully. After this patch, the control characters in the src/dst fields are escaped.


---

* [HADOOP-10597](https://issues.apache.org/jira/browse/HADOOP-10597) | *Major* | **RPC Server signals backoff to clients when all request queues are full**

This change introduces a new configuration key used by RPC server to decide whether to send backoff signal to RPC Client when RPC call queue is full. When the feature is enabled, RPC server will no longer block on the processing of RPC requests when RPC call queue is full. It helps to improve quality of service when the service is under heavy load. The configuration key is in the format of "ipc.#port#.backoff.enable" where #port# is the port number that RPC server listens on. For example, if you want to enable the feature for the RPC server that listens on 8020, set ipc.8020.backoff.enable to true.


---

* [HDFS-11056](https://issues.apache.org/jira/browse/HDFS-11056) | *Major* | **Concurrent append and read operations lead to checksum error**

Load last partial chunk checksum properly into memory when converting a finalized/temporary replica to rbw replica. This ensures concurrent reader reads the correct checksum that matches the data before the update.


---

* [HADOOP-13812](https://issues.apache.org/jira/browse/HADOOP-13812) | *Blocker* | **Upgrade Tomcat to 6.0.48**

Tomcat 6.0.46 starts to filter weak ciphers. Some old SSL clients may be affected. It is recommended to upgrade the SSL client. Run the SSL client against https://www.howsmyssl.com/a/check to find out its TLS version and cipher suites.


---

* [HDFS-11217](https://issues.apache.org/jira/browse/HDFS-11217) | *Major* | **Annotate NameNode and DataNode MXBean interfaces as Private/Stable**

The DataNode and NameNode MXBean interfaces have been marked as Private and Stable to indicate that although users should not be implementing these interfaces directly, the information exposed by these interfaces is part of the HDFS public API.


---

* [HDFS-11229](https://issues.apache.org/jira/browse/HDFS-11229) | *Blocker* | **HDFS-11056 failed to close meta file**

The fix for HDFS-11056 reads meta file to load last partial chunk checksum when a block is converted from finalized/temporary to rbw. However, it did not close the file explicitly, which may cause number of open files reaching system limit. This jira fixes it by closing the file explicitly after the meta file is read.


---

* [HDFS-11160](https://issues.apache.org/jira/browse/HDFS-11160) | *Major* | **VolumeScanner reports write-in-progress replicas as corrupt incorrectly**

Fixed a race condition that caused VolumeScanner to recognize a good replica as a bad one if the replica is also being written concurrently.


---

* [HADOOP-13956](https://issues.apache.org/jira/browse/HADOOP-13956) | *Critical* | **Read ADLS credentials from Credential Provider**

The hadoop-azure-datalake file system now supports configuration of the Azure Data Lake Store account credentials using the standard Hadoop Credential Provider API. For details, please refer to the documentation on hadoop-azure-datalake and the Credential Provider API.


---

* [YARN-5271](https://issues.apache.org/jira/browse/YARN-5271) | *Major* | **ATS client doesn't work with Jersey 2 on the classpath**

A workaround to avoid dependency conflict with Spark2, before a full classpath isolation solution is implemented.
Skip instantiating a Timeline Service client if encountering NoClassDefFoundError.


---

* [HADOOP-13929](https://issues.apache.org/jira/browse/HADOOP-13929) | *Major* | **ADLS connector should not check in contract-test-options.xml**

To run live unit tests, create src/test/resources/auth-keys.xml with the same properties as in the deprecated contract-test-options.xml.


---

* [YARN-6177](https://issues.apache.org/jira/browse/YARN-6177) | *Major* | **Yarn client should exit with an informative error message if an incompatible Jersey library is used at client**

Let yarn client exit with an informative error message if an incompatible Jersey library is used from client side.


---

* [HADOOP-14138](https://issues.apache.org/jira/browse/HADOOP-14138) | *Critical* | **Remove S3A ref from META-INF service discovery, rely on existing core-default entry**

The classpath implementing the s3a filesystem is now defined in core-default.xml. Attempting to instantiate an S3A filesystem instance using a Configuration instance which has not included the default resorts will fail. Applications should not be doing this anyway, as it will lose other critical  configuration options needed by the filesystem.


---

* [HDFS-11498](https://issues.apache.org/jira/browse/HDFS-11498) | *Major* | **Make RestCsrfPreventionHandler and WebHdfsHandler compatible with Netty 4.0**

This JIRA sets the Netty 4 dependency to 4.0.23. This is an incompatible change for the 3.0 release line, as 3.0.0-alpha1 and 3.0.0-alpha2 depended on Netty 4.1.0.Beta5.


---

* [HADOOP-13037](https://issues.apache.org/jira/browse/HADOOP-13037) | *Major* | **Refactor Azure Data Lake Store as an independent FileSystem**

Hadoop now supports integration with Azure Data Lake as an alternative Hadoop-compatible file system. Please refer to the Hadoop site documentation of Azure Data Lake for details on usage and configuration.


---

* [HDFS-11431](https://issues.apache.org/jira/browse/HDFS-11431) | *Blocker* | **hadoop-hdfs-client JAR does not include ConfiguredFailoverProxyProvider**

The hadoop-client POM now includes a leaner hdfs-client, stripping out all the transitive dependencies on JARs only needed for the Hadoop HDFS daemon itself. The specific jars now excluded are: leveldbjni-all, jetty-util, commons-daemon, xercesImpl, netty and servlet-api.

This should make downstream projects dependent JARs smaller, and avoid version conflict problems with the specific JARs now excluded.

Applications may encounter build problems if they did depend on these JARs, and which didn't explicitly include them. There are two fixes for this

\* explicitly include the JARs, stating which version of them you want.
\* add a dependency on hadoop-hdfs. For Hadoop 2.8+, this will add the missing dependencies. For builds against older versions of Hadoop, this will be harmless, as hadoop-hdfs and all its dependencies are already pulled in by the hadoop-client POM.


---

* [HDFS-8818](https://issues.apache.org/jira/browse/HDFS-8818) | *Major* | **Allow Balancer to run faster**

Add a new conf "dfs.balancer.max-size-to-move" so that Balancer.MAX\_SIZE\_TO\_MOVE becomes configurable.


---

* [YARN-6959](https://issues.apache.org/jira/browse/YARN-6959) | *Major* | **RM may allocate wrong AM Container for new attempt**

ResourceManager will now record ResourceRequests from different attempts into different objects.

