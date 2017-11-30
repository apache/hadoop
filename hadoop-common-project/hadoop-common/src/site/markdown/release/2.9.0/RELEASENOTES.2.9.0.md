
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
# Apache Hadoop  2.9.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-9624](https://issues.apache.org/jira/browse/HDFS-9624) | *Major* | **DataNode start slowly due to the initial DU command operations**

Make it configurable how long the cached du file is valid. Useful for rolling upgrade.


---

* [HDFS-9525](https://issues.apache.org/jira/browse/HDFS-9525) | *Blocker* | **hadoop utilities need to support provided delegation tokens**

If hadoop.token.files property is defined and configured to one or more comma-delimited delegation token files, Hadoop will use those token files to connect to the services as named in the token.


---

* [YARN-4762](https://issues.apache.org/jira/browse/YARN-4762) | *Blocker* | **NMs failing on DelegatingLinuxContainerRuntime init with LCE on**

Fixed CgroupHandler's creation and usage to avoid NodeManagers crashing when LinuxContainerExecutor is enabled.


---

* [HDFS-1477](https://issues.apache.org/jira/browse/HDFS-1477) | *Major* | **Support reconfiguring dfs.heartbeat.interval and dfs.namenode.heartbeat.recheck-interval without NN restart**

Steps to reconfigure:
1. change value of the parameter in corresponding xml configuration file
2. to reconfigure, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> start
3. to check status of the most recent reconfigure operation, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> status
4. to query a list reconfigurable properties on NN, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> properties


---

* [YARN-4732](https://issues.apache.org/jira/browse/YARN-4732) | *Trivial* | **\*ProcessTree classes have too many whitespace issues**




---

* [HDFS-9349](https://issues.apache.org/jira/browse/HDFS-9349) | *Major* | **Support reconfiguring fs.protected.directories without NN restart**

Steps to reconfigure:
1. change value of the parameter in corresponding xml configuration file
2. to reconfigure, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> start
3. to check status of the most recent reconfigure operation, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> status
4. to query a list reconfigurable properties on NN, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> properties


---

* [YARN-4784](https://issues.apache.org/jira/browse/YARN-4784) | *Major* | **Fairscheduler: defaultQueueSchedulingPolicy should not accept FIFO**

Clusters cannot use FIFO policy as the defaultQueueSchedulingPolicy. Clusters with a single level of queues will have to explicitly set the policy to FIFO if that is desired.


---

* [HADOOP-10694](https://issues.apache.org/jira/browse/HADOOP-10694) | *Major* | **Remove synchronized input streams from Writable deserialization**

Remove invisible synchronization primitives from DataInputBuffer


---

* [HADOOP-12782](https://issues.apache.org/jira/browse/HADOOP-12782) | *Major* | **Faster LDAP group name resolution with ActiveDirectory**

If the user object returned by LDAP server has the user's group object DN (supported by Active Directory), Hadoop can reduce LDAP group mapping latency by setting hadoop.security.group.mapping.ldap.search.attr.memberof to memberOf.


---

* [HDFS-10328](https://issues.apache.org/jira/browse/HDFS-10328) | *Minor* | **Add per-cache-pool default replication num configuration**

Add per-cache-pool default replication num configuration


---

* [HADOOP-13354](https://issues.apache.org/jira/browse/HADOOP-13354) | *Major* | **Update WASB driver to use the latest version (4.2.0) of SDK for Microsoft Azure Storage Clients**

The WASB FileSystem now uses version 4.2.0 of the Azure Storage SDK.


---

* [HADOOP-13403](https://issues.apache.org/jira/browse/HADOOP-13403) | *Major* | **AzureNativeFileSystem rename/delete performance improvements**

WASB has added an optional capability to execute certain FileSystem operations in parallel on multiple threads for improved performance.  Please refer to the Azure Blob Storage documentation page for more information on how to enable and control the feature.


---

* [HADOOP-12747](https://issues.apache.org/jira/browse/HADOOP-12747) | *Major* | **support wildcard in libjars argument**

It is now possible to specify multiple jar files for the libjars argument using a wildcard. For example, you can specify "-libjars 'libs/\*'" as a shorthand for all jars in the libs directory.


---

* [YARN-5137](https://issues.apache.org/jira/browse/YARN-5137) | *Major* | **Make DiskChecker pluggable in NodeManager**

Added new plugin property yarn.nodemanager.disk-validator to allow the NodeManager to use an alternate class for checking whether a disk is good or not.


---

* [HDFS-8312](https://issues.apache.org/jira/browse/HDFS-8312) | *Critical* | **Trash does not descent into child directories to check for permissions**

Permissions are now checked when moving a file to Trash.


---

* [HADOOP-13588](https://issues.apache.org/jira/browse/HADOOP-13588) | *Major* | **ConfServlet should respect Accept request header**

Conf HTTP service should set response's content type according to the Accept header in the request.


---

* [MAPREDUCE-6776](https://issues.apache.org/jira/browse/MAPREDUCE-6776) | *Major* | **yarn.app.mapreduce.client.job.max-retries should have a more useful default**

The default value of yarn.app.mapreduce.client.job.max-retries has been changed from 0 to 3.  This will help protect clients from failures that are transient.  True failures may take slightly longer now due to the retries.


---

* [HADOOP-13522](https://issues.apache.org/jira/browse/HADOOP-13522) | *Major* | **Add %A and %a formats for fs -stat command to print permissions**

Added permissions to the fs stat command. They are now available as symbolic (%A) and octal (%a) formats, which are in line with Linux.


---

* [YARN-5388](https://issues.apache.org/jira/browse/YARN-5388) | *Critical* | **Deprecate and remove DockerContainerExecutor**

DockerContainerExecutor is deprecated starting 2.9.0 and removed from 3.0.0. Please use LinuxContainerExecutor with the DockerRuntime to run Docker containers on YARN clusters.


---

* [HDFS-10756](https://issues.apache.org/jira/browse/HDFS-10756) | *Major* | **Expose getTrashRoot to HTTPFS and WebHDFS**

"getTrashRoot" returns a trash root for a path. Currently in DFS if the path "/foo" is a normal path, it returns "/user/$USER/.Trash" for "/foo" and if "/foo" is an encrypted zone, it returns "/foo/.Trash/$USER" for the child file/dir of "/foo". This patch is about to override the old "getTrashRoot" of httpfs and webhdfs, so that the behavior of returning trash root in httpfs and webhdfs are consistent with DFS.


---

* [HADOOP-12705](https://issues.apache.org/jira/browse/HADOOP-12705) | *Major* | **Upgrade Jackson 2.2.3 to 2.7.8**

We are sorry for causing pain for everyone for whom this Jackson update causes problems, but it was proving impossible to stay on the older version: too much code had moved past it, and by staying back we were limiting what Hadoop could do, and giving everyone who wanted an up to date version of Jackson a different set of problems. We've selected Jackson 2.7.8 as it fixed fix a security issue in XML parsing, yet proved compatible at the API level with the Hadoop codebase --and hopefully everything downstream.


---

* [HADOOP-13050](https://issues.apache.org/jira/browse/HADOOP-13050) | *Blocker* | **Upgrade to AWS SDK 1.11.45**

The dependency on the AWS SDK has been bumped to 1.11.45.


---

* [HADOOP-13953](https://issues.apache.org/jira/browse/HADOOP-13953) | *Major* | **Make FTPFileSystem's data connection mode and transfer mode configurable**

Added two configuration key fs.ftp.data.connection.mode and fs.ftp.transfer.mode, and configure FTP data connection mode and transfer mode accordingly.


---

* [HADOOP-14003](https://issues.apache.org/jira/browse/HADOOP-14003) | *Major* | **Make additional KMS tomcat settings configurable**

<!-- markdown -->

The KMS can now be configured with the additional environment variables `KMS_PROTOCOL`, `KMS_ACCEPT_COUNT`, and `KMS_ACCEPTOR_THREAD_COUNT`. See `kms-env.sh` for more information about these variables.


---

* [MAPREDUCE-6404](https://issues.apache.org/jira/browse/MAPREDUCE-6404) | *Major* | **Allow AM to specify a port range for starting its webapp**

Add a new configuration - "yarn.app.mapreduce.am.webapp.port-range" to specify port-range for webapp launched by AM.


---

* [HADOOP-13075](https://issues.apache.org/jira/browse/HADOOP-13075) | *Major* | **Add support for SSE-KMS and SSE-C in s3a filesystem**

The new encryption options SSE-KMS and especially SSE-C must be considered experimental at present. If you are using SSE-C, problems may arise if the bucket mixes encrypted and unencrypted files. For SSE-KMS, there may be extra throttling of IO, especially with the fadvise=random option. You may wish to request an increase in your KMS IOPs limits.


---

* [YARN-6177](https://issues.apache.org/jira/browse/YARN-6177) | *Major* | **Yarn client should exit with an informative error message if an incompatible Jersey library is used at client**

Let yarn client exit with an informative error message if an incompatible Jersey library is used from client side.


---

* [HADOOP-13817](https://issues.apache.org/jira/browse/HADOOP-13817) | *Minor* | **Add a finite shell command timeout to ShellBasedUnixGroupsMapping**

A new introduced configuration key "hadoop.security.groups.shell.command.timeout" allows applying a finite wait timeout over the 'id' commands launched by the ShellBasedUnixGroupsMapping plugin. Values specified can be in any valid time duration units: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/conf/Configuration.html#getTimeDuration-java.lang.String-long-java.util.concurrent.TimeUnit-

Value defaults to 0, indicating infinite wait (preserving existing behaviour).


---

* [HADOOP-6801](https://issues.apache.org/jira/browse/HADOOP-6801) | *Minor* | **io.sort.mb and io.sort.factor were renamed and moved to mapreduce but are still in CommonConfigurationKeysPublic.java and used in SequenceFile.java**

Two new configuration keys, seq.io.sort.mb and seq.io.sort.factor have been introduced for the SequenceFile's Sorter feature to replace older, deprecated property keys of io.sort.mb and io.sort.factor.

This only affects direct users of the org.apache.hadoop.io.SequenceFile.Sorter Java class. For controlling MR2's internal sorting instead, use the existing config keys of mapreduce.task.io.sort.mb and mapreduce.task.io.sort.factor.


---

* [HDFS-11499](https://issues.apache.org/jira/browse/HDFS-11499) | *Major* | **Decommissioning stuck because of failing recovery**

Allow a block to complete if the number of replicas on live nodes, decommissioning nodes and nodes in maintenance mode satisfies minimum replication factor.
The fix prevents block recovery failure if replica of last block is being decommissioned. Vice versa, the decommissioning will be stuck, waiting for the last block to be completed. In addition, file close() operation will not fail due to last block being decommissioned.


---

* [HADOOP-14213](https://issues.apache.org/jira/browse/HADOOP-14213) | *Major* | **Move Configuration runtime check for hadoop-site.xml to initialization**

Move the check for hadoop-site.xml to static initialization of the Configuration class.


---

* [HADOOP-14038](https://issues.apache.org/jira/browse/HADOOP-14038) | *Minor* | **Rename ADLS credential properties**

<!-- markdown -->

* Properties {{dfs.adls.*}} are renamed {{fs.adl.*}}
* Property {{adl.dfs.enable.client.latency.tracker}} is renamed {{adl.enable.client.latency.tracker}}
* Old properties are still supported


---

* [HADOOP-14174](https://issues.apache.org/jira/browse/HADOOP-14174) | *Major* | **Set default ADLS access token provider type to ClientCredential**

Switch the default ADLS access token provider type from Custom to ClientCredential.


---

* [HADOOP-14301](https://issues.apache.org/jira/browse/HADOOP-14301) | *Major* | **Deprecate SharedInstanceProfileCredentialsProvider in branch-2.**

SharedInstanceProfileCredentialsProvider has been deprecated. Users should use InstanceProfileCredentialsProvider provided by AWS SDK instead, which itself enforces a singleton instance to reduce calls to AWS EC2 Instance Metadata Service.  SharedInstanceProfileCredentialsProvider will be removed permanently in a future release.


---

* [HADOOP-11794](https://issues.apache.org/jira/browse/HADOOP-11794) | *Major* | **Enable distcp to copy blocks in parallel**

If  a positive value is passed to command line switch -blocksperchunk, files with more blocks than this value will be split into chunks of \`\<blocksperchunk\>\` blocks to be transferred in parallel, and reassembled on the destination. By default, \`\<blocksperchunk\>\` is 0 and the files will be transmitted in their entirety without splitting. This switch is only applicable when both the source file system supports getBlockLocations and target supports concat.


---

* [HDFS-11402](https://issues.apache.org/jira/browse/HDFS-11402) | *Major* | **HDFS Snapshots should capture point-in-time copies of OPEN files**

When the config param "dfs.namenode.snapshot.capture.openfiles" is enabled, HDFS snapshots taken will additionally capture point-in-time copies of the open files that have valid leases. Even when the current version open files grow or shrink in size, the snapshot will always retain the immutable versions of these open files, just as in for all other closed files. Note: The file length captured for open files in the snapshot was the one recorded in NameNode at the time of snapshot and it may be shorter than what the client has written till then. In order to capture the latest length, the client can call hflush/hsync with the flag SyncFlag.UPDATE\_LENGTH on the open files handles.


---

* [YARN-2962](https://issues.apache.org/jira/browse/YARN-2962) | *Critical* | **ZKRMStateStore: Limit the number of znodes under a znode**

**WARNING: No release note provided for this change.**


---

* [HDFS-9016](https://issues.apache.org/jira/browse/HDFS-9016) | *Major* | **Display upgrade domain information in fsck**

New fsck option "-upgradedomains" has been added to display upgrade domains of any block.


---

* [HADOOP-14419](https://issues.apache.org/jira/browse/HADOOP-14419) | *Minor* | **Remove findbugs report from docs profile**

Findbugs report is no longer part of the documentation.


---

* [HDFS-11661](https://issues.apache.org/jira/browse/HDFS-11661) | *Blocker* | **GetContentSummary uses excessive amounts of memory**

Reverted HDFS-10797 to fix a scalability regression brought by the commit.


---

* [HADOOP-14407](https://issues.apache.org/jira/browse/HADOOP-14407) | *Major* | **DistCp - Introduce a configurable copy buffer size**

The copy buffer size can be configured via the new parameter \<copybuffersize\>. By default the \<copybuffersize\> is set to 8KB.


---

* [YARN-6127](https://issues.apache.org/jira/browse/YARN-6127) | *Major* | **Add support for work preserving NM restart when AMRMProxy is enabled**

This breaks rolling upgrades because it changes the major version of the NM state store schema. Therefore when a new NM comes up on an old state store it crashes.

The state store versions for this change have been updated in YARN-6798.


---

* [HADOOP-14536](https://issues.apache.org/jira/browse/HADOOP-14536) | *Major* | **Update azure-storage sdk to version 5.3.0**

The WASB FileSystem now uses version 5.3.0 of the Azure Storage SDK.


---

* [HADOOP-14546](https://issues.apache.org/jira/browse/HADOOP-14546) | *Major* | **Azure: Concurrent I/O does not work when secure.mode is enabled**

Fix to wasb:// (Azure) file system that allows the concurrent I/O feature to be used with the secure mode feature.


---

* [HADOOP-14535](https://issues.apache.org/jira/browse/HADOOP-14535) | *Major* | **wasb: implement high-performance random access and seek of block blobs**

Random access and seek improvements for the wasb:// (Azure) file system.


---

* [YARN-5049](https://issues.apache.org/jira/browse/YARN-5049) | *Major* | **Extend NMStateStore to save queued container information**

This breaks rolling upgrades because it changes the major version of the NM state store schema. Therefore when a new NM comes up on an old state store it crashes.

The state store versions for this change have been updated in YARN-6798.


---

* [YARN-6798](https://issues.apache.org/jira/browse/YARN-6798) | *Major* | **Fix NM startup failure with old state store due to version mismatch**

<!-- markdown -->

This fixes the LevelDB state store for the NodeManager.  As of this patch, the state store versions now correspond to the following table.

* Previous Patch: YARN-5049
  * LevelDB Key: queued
  * Hadoop Versions: 2.9.0, 3.0.0-alpha1
  * Corresponding LevelDB Version: 1.2
* Previous Patch: YARN-6127
  * LevelDB Key: AMRMProxy/NextMasterKey
  * Hadoop Versions: 2.9.0, 3.0.0-alpha4
  * Corresponding LevelDB Version: 1.1


---

* [HADOOP-14539](https://issues.apache.org/jira/browse/HADOOP-14539) | *Major* | **Move commons logging APIs over to slf4j in hadoop-common**

In Hadoop common, fatal log level is changed to error because slf4j API does not support fatal log level.


---

* [HADOOP-14518](https://issues.apache.org/jira/browse/HADOOP-14518) | *Minor* | **Customize User-Agent header sent in HTTP/HTTPS requests by WASB.**

WASB now includes the current Apache Hadoop version in the User-Agent string passed to Azure Blob service. Users also may include optional additional information to identify their application. See the documentation of configuration property fs.wasb.user.agent.id for further details.


---

* [HADOOP-14722](https://issues.apache.org/jira/browse/HADOOP-14722) | *Major* | **Azure: BlockBlobInputStream position incorrect after seek**

Bug fix to Azure Filesystem related to HADOOP-14535.


---

* [HADOOP-14680](https://issues.apache.org/jira/browse/HADOOP-14680) | *Minor* | **Azure: IndexOutOfBoundsException in BlockBlobInputStream**

Bug fix to Azure Filesystem related to HADOOP-14535


---

* [HADOOP-14260](https://issues.apache.org/jira/browse/HADOOP-14260) | *Major* | **Configuration.dumpConfiguration should redact sensitive information**

<!-- markdown -->
Configuration.dumpConfiguration no longer prints out the clear text values for the sensitive keys listed in `hadoop.security.sensitive-config-keys`. Callers can override the default list of sensitive keys either to redact more keys or print the clear text values for a few extra keys for debugging purpose.


---

* [HADOOP-14660](https://issues.apache.org/jira/browse/HADOOP-14660) | *Major* | **wasb: improve throughput by 34% when account limit exceeded**

Up to 34% throughput improvement for the wasb:// (Azure) file system when fs.azure.selfthrottling.enable is false fs.azure.autothrottling.enable is true.


---

* [HADOOP-14769](https://issues.apache.org/jira/browse/HADOOP-14769) | *Major* | **WASB: delete recursive should not fail if a file is deleted**

Recursive directory delete improvement for the wasb filesystem.
