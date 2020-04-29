
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
# Apache Hadoop  3.0.0-alpha2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-12756](https://issues.apache.org/jira/browse/HADOOP-12756) | *Major* | **Incorporate Aliyun OSS file system implementation**

Aliyun OSS is widely used among China’s cloud users and this work implemented a new Hadoop compatible filesystem AliyunOSSFileSystem with oss scheme, similar to the s3a and azure support.


---

* [HDFS-10760](https://issues.apache.org/jira/browse/HDFS-10760) | *Major* | **DataXceiver#run() should not log InvalidToken exception as an error**

Log InvalidTokenException at trace level in DataXceiver#run().


---

* [HADOOP-13361](https://issues.apache.org/jira/browse/HADOOP-13361) | *Major* | **Modify hadoop\_verify\_user to be consistent with hadoop\_subcommand\_opts (ie more granularity)**

Users:

In Apache Hadoop 3.0.0-alpha1, verification required environment variables with the format of HADOOP\_(subcommand)\_USER where subcommand was lowercase applied globally.  This changes the format to be (command)\_(subcommand)\_USER where all are uppercase to be consistent with the \_OPTS functionality as well as being able to set per-command options.  Additionally, the check is now happening sooner, which should make it faster to fail.

Developers:

This changes hadoop\_verify\_user to require the program's name as part of the function call.  This is incompatible with Apache Hadoop 3.0.0-alpha1.


---

* [YARN-5549](https://issues.apache.org/jira/browse/YARN-5549) | *Critical* | **AMLauncher#createAMContainerLaunchContext() should not log the command to be launched indiscriminately**

Introduces a new configuration property, yarn.resourcemanager.amlauncher.log.command.  If this property is set to true, then the AM command being launched will be masked in the RM log.


---

* [HDFS-6962](https://issues.apache.org/jira/browse/HDFS-6962) | *Critical* | **ACL inheritance conflicts with umaskmode**

<!-- markdown -->
The original implementation of HDFS ACLs applied the client's umask to the permissions when
inheriting a default ACL defined on a parent directory.  This behavior is a deviation from the
POSIX ACL specification, which states that the umask has no influence when a default ACL
propagates from parent to child.  HDFS now offers the capability to ignore the umask in this
case for improved compliance with POSIX.  This change is considered backward-incompatible,
so the new behavior is off by default and must be explicitly configured by setting
dfs.namenode.posix.acl.inheritance.enabled to true in hdfs-site.xml.
Please see the HDFS Permissions Guide for further details.


---

* [HADOOP-13341](https://issues.apache.org/jira/browse/HADOOP-13341) | *Major* | **Deprecate HADOOP\_SERVERNAME\_OPTS; replace with (command)\_(subcommand)\_OPTS**

<!-- markdown -->
Users:
* Ability to set per-command+sub-command options from the command line.
* Makes daemon environment variable options consistent across the project. (See deprecation list below)
* HADOOP\_CLIENT\_OPTS is now honored for every non-daemon sub-command. Prior to this change, many sub-commands did not use it.

Developers:
* No longer need to do custom handling for options in the case section of the shell scripts.
* Consolidates all \_OPTS handling into hadoop-functions.sh to enable future projects.
* All daemons running with secure mode features now get \_SECURE\_EXTRA\_OPTS support.

\_OPTS Changes:

| Old | New |
|:---- |:---- |
| HADOOP\_BALANCER\_OPTS | HDFS\_BALANCER\_OPTS |
| HADOOP\_DATANODE\_OPTS | HDFS\_DATANODE\_OPTS |
| HADOOP\_DN\_SECURE_EXTRA_OPTS | HDFS\_DATANODE\_SECURE\_EXTRA\_OPTS |
| HADOOP\_JOB\_HISTORYSERVER\_OPTS | MAPRED\_HISTORYSERVER\_OPTS |
| HADOOP\_JOURNALNODE\_OPTS | HDFS\_JOURNALNODE\_OPTS |
| HADOOP\_MOVER\_OPTS | HDFS\_MOVER\_OPTS |
| HADOOP\_NAMENODE\_OPTS | HDFS\_NAMENODE\_OPTS |
| HADOOP\_NFS3\_OPTS | HDFS\_NFS3\_OPTS |
| HADOOP\_NFS3\_SECURE\_EXTRA\_OPTS | HDFS\_NFS3\_SECURE\_EXTRA\_OPTS |
| HADOOP\_PORTMAP\_OPTS | HDFS\_PORTMAP\_OPTS |
| HADOOP\_SECONDARYNAMENODE\_OPTS | HDFS\_SECONDARYNAMENODE\_OPTS |
| HADOOP\_ZKFC\_OPTS | HDFS\_ZKFC\_OPTS |


---

* [HADOOP-13588](https://issues.apache.org/jira/browse/HADOOP-13588) | *Major* | **ConfServlet should respect Accept request header**

Conf HTTP service should set response's content type according to the Accept header in the request.


---

* [HDFS-10636](https://issues.apache.org/jira/browse/HDFS-10636) | *Major* | **Modify ReplicaInfo to remove the assumption that replica metadata and data are stored in java.io.File.**

**WARNING: No release note provided for this change.**


---

* [HADOOP-13218](https://issues.apache.org/jira/browse/HADOOP-13218) | *Major* | **Migrate other Hadoop side tests to prepare for removing WritableRPCEngine**

**WARNING: No release note provided for this change.**


---

* [HDFS-10489](https://issues.apache.org/jira/browse/HDFS-10489) | *Minor* | **Deprecate dfs.encryption.key.provider.uri for HDFS encryption zones**

The configuration dfs.encryption.key.provider.uri is deprecated. To configure key provider in HDFS, please use hadoop.security.key.provider.path.


---

* [HDFS-10877](https://issues.apache.org/jira/browse/HDFS-10877) | *Major* | **Make RemoteEditLogManifest.committedTxnId optional in Protocol Buffers**

A new protobuf field added to RemoteEditLogManifest was mistakenly marked as required. This changes the field to optional, preserving compatibility with 2.x releases but breaking compatibility with 3.0.0-alpha1.


---

* [HDFS-10914](https://issues.apache.org/jira/browse/HDFS-10914) | *Critical* | **Move remnants of oah.hdfs.client to hadoop-hdfs-client**

The remaining classes in the org.apache.hadoop.hdfs.client package have been moved from hadoop-hdfs to hadoop-hdfs-client.


---

* [HADOOP-13681](https://issues.apache.org/jira/browse/HADOOP-13681) | *Major* | **Reduce Kafka dependencies in hadoop-kafka module**

Changed Apache Kafka dependency from kafka-2.10 to kafka-clients in hadoop-kafka module.


---

* [HADOOP-12667](https://issues.apache.org/jira/browse/HADOOP-12667) | *Major* | **s3a: Support createNonRecursive API**

S3A now provides a working implementation of the FileSystem#createNonRecursive method.


---

* [HDFS-10609](https://issues.apache.org/jira/browse/HDFS-10609) | *Major* | **Uncaught InvalidEncryptionKeyException during pipeline recovery may abort downstream applications**

If pipeline recovery fails due to expired encryption key, attempt to refresh the key and retry.


---

* [HADOOP-13678](https://issues.apache.org/jira/browse/HADOOP-13678) | *Major* | **Update jackson from 1.9.13 to 2.x in hadoop-tools**

Jackson 1.9.13 dependency was removed from hadoop-tools module.


---

* [MAPREDUCE-6776](https://issues.apache.org/jira/browse/MAPREDUCE-6776) | *Major* | **yarn.app.mapreduce.client.job.max-retries should have a more useful default**

The default value of yarn.app.mapreduce.client.job.max-retries has been changed from 0 to 3.  This will help protect clients from failures that are transient.  True failures may take slightly longer now due to the retries.


---

* [HDFS-10797](https://issues.apache.org/jira/browse/HDFS-10797) | *Major* | **Disk usage summary of snapshots causes renamed blocks to get counted twice**

Disk usage summaries previously incorrectly counted files twice if they had been renamed (including files moved to Trash) since being snapshotted. Summaries now include current data plus snapshotted data that is no longer under the directory either due to deletion or being moved outside of the directory.


---

* [HADOOP-13699](https://issues.apache.org/jira/browse/HADOOP-13699) | *Critical* | **Configuration does not substitute multiple references to the same var**

This changes the config var cycle detection introduced in 3.0.0-alpha1 by HADOOP-6871 such that it detects single-variable but not multi-variable loops. This also fixes resolution of multiple specifications of the same variable in a config value.


---

* [HDFS-10637](https://issues.apache.org/jira/browse/HDFS-10637) | *Major* | **Modifications to remove the assumption that FsVolumes are backed by java.io.File.**

**WARNING: No release note provided for this change.**


---

* [HDFS-10916](https://issues.apache.org/jira/browse/HDFS-10916) | *Major* | **Switch from "raw" to "system" xattr namespace for erasure coding policy**

EC policy is now stored in the "system" extended attribute namespace rather than "raw". This means the EC policy extended attribute is no longer directly accessible by users or preserved across a distcp that preserves raw extended attributes.

Users can instead use HdfsAdmin#setErasureCodingPolicy and HdfsAdmin#getErasureCodingPolicy to set and get the EC policy for a path.


---

* [YARN-4464](https://issues.apache.org/jira/browse/YARN-4464) | *Blocker* | **Lower the default max applications stored in the RM and store**

The maximum applications the RM stores in memory and in the state-store by default has been lowered from 10,000 to 1,000. This should ease the pressure on the state-store. However, installations relying on the default to be 10,000 are affected.


---

* [HDFS-10883](https://issues.apache.org/jira/browse/HDFS-10883) | *Major* | **\`getTrashRoot\`'s behavior is not consistent in DFS after enabling EZ.**

If root path / is an encryption zone, the old DistributedFileSystem#getTrashRoot(new Path("/")) returns
/user/$USER/.Trash
which is a wrong behavior. The correct value should be
/.Trash/$USER


---

* [HADOOP-13721](https://issues.apache.org/jira/browse/HADOOP-13721) | *Minor* | **Remove stale method ViewFileSystem#getTrashCanLocation**

The unused method getTrashCanLocation has been removed. This method has long been superceded by FileSystem#getTrashRoot.


---

* [HADOOP-13661](https://issues.apache.org/jira/browse/HADOOP-13661) | *Major* | **Upgrade HTrace version**

Bump HTrace version from 4.0.1-incubating to 4.1.0-incubating.


---

* [HDFS-10957](https://issues.apache.org/jira/browse/HDFS-10957) | *Major* | **Retire BKJM from trunk**

The BookkeeperJournalManager implementation has been removed. Users are encouraged to use QuorumJournalManager instead.


---

* [HADOOP-13522](https://issues.apache.org/jira/browse/HADOOP-13522) | *Major* | **Add %A and %a formats for fs -stat command to print permissions**

Added permissions to the fs stat command. They are now available as symbolic (%A) and octal (%a) formats, which are in line with Linux.


---

* [YARN-5718](https://issues.apache.org/jira/browse/YARN-5718) | *Major* | **TimelineClient (and other places in YARN) shouldn't over-write HDFS client retry settings which could cause unexpected behavior**

**WARNING: No release note provided for this change.**


---

* [HADOOP-13560](https://issues.apache.org/jira/browse/HADOOP-13560) | *Major* | **S3ABlockOutputStream to support huge (many GB) file writes**

This mechanism replaces the (experimental) fast output stream of Hadoop 2.7.x, combining better scalability options with instrumentation. Consult the S3A documentation to see the extra configuration operations.


---

* [MAPREDUCE-6791](https://issues.apache.org/jira/browse/MAPREDUCE-6791) | *Minor* | **remove unnecessary dependency from hadoop-mapreduce-client-jobclient to hadoop-mapreduce-client-shuffle**

An unnecessary dependency on hadoop-mapreduce-client-shuffle in hadoop-mapreduce-client-jobclient has been removed.


---

* [HADOOP-7352](https://issues.apache.org/jira/browse/HADOOP-7352) | *Major* | **FileSystem#listStatus should throw IOE upon access error**

Change FileSystem#listStatus contract to never return null. Local filesystems prior to 3.0.0 returned null upon access error. It is considered erroneous. We should expect FileSystem#listStatus to throw IOException upon access error.


---

* [HADOOP-13693](https://issues.apache.org/jira/browse/HADOOP-13693) | *Minor* | **Remove the message about HTTP OPTIONS in SPNEGO initialization message from kms audit log**

kms-audit.log used to show an UNAUTHENTICATED message even for successful operations, because of the OPTIONS HTTP request during SPNEGO initial handshake. This message brings more confusion than help, and has hence been removed.


---

* [HDFS-11018](https://issues.apache.org/jira/browse/HDFS-11018) | *Major* | **Incorrect check and message in FsDatasetImpl#invalidate**

Improves the error message when datanode removes a replica which is not found.


---

* [HDFS-10976](https://issues.apache.org/jira/browse/HDFS-10976) | *Major* | **Report erasure coding policy of EC files in Fsck**

Fsck now reports whether a file is replicated and erasure-coded. If it is replicated, fsck reports replication factor of the file. If it is erasure coded, fsck reports the erasure coding policy of the file.


---

* [HDFS-10975](https://issues.apache.org/jira/browse/HDFS-10975) | *Major* | **fsck -list-corruptfileblocks does not report corrupt EC files**

Fixed a bug that made fsck -list-corruptfileblocks counts corrupt erasure coded files incorrectly.


---

* [YARN-5388](https://issues.apache.org/jira/browse/YARN-5388) | *Critical* | **Deprecate and remove DockerContainerExecutor**

DockerContainerExecutor is deprecated starting 2.9.0 and removed from 3.0.0. Please use LinuxContainerExecutor with the DockerRuntime to run Docker containers on YARN clusters.


---

* [HADOOP-11798](https://issues.apache.org/jira/browse/HADOOP-11798) | *Major* | **Native raw erasure coder in XOR codes**

This provides a native implementation of XOR codec by leveraging Intel ISA-L library function to achieve a better performance.


---

* [HADOOP-13659](https://issues.apache.org/jira/browse/HADOOP-13659) | *Major* | **Upgrade jaxb-api version**

Bump the version of third party dependency jaxb-api to 2.2.11.


---

* [YARN-3732](https://issues.apache.org/jira/browse/YARN-3732) | *Minor* | **Change NodeHeartbeatResponse.java and RegisterNodeManagerResponse.java as abstract classes**

Interface classes has been changed to Abstract class to maintain consistency across all other protos.


---

* [YARN-5767](https://issues.apache.org/jira/browse/YARN-5767) | *Major* | **Fix the order that resources are cleaned up from the local Public/Private caches**

This issue fixes a bug in how resources are evicted from the PUBLIC and PRIVATE yarn local caches used by the node manager for resource localization. In summary, the caches are now properly cleaned based on an LRU policy across both the public and private caches.


---

* [HDFS-11048](https://issues.apache.org/jira/browse/HDFS-11048) | *Major* | **Audit Log should escape control characters**

HDFS audit logs are formatted as individual lines, each of which has a few of key-value pair fields. Some of the values come from client request (e.g. src, dst). Before this patch the control characters including \\t \\n etc are not escaped in audit logs. That may break lines unexpectedly or introduce additional table character (in the worst case, both) within a field. Tools that parse audit logs had to deal with this case carefully. After this patch, the control characters in the src/dst fields are escaped.


---

* [HADOOP-8500](https://issues.apache.org/jira/browse/HADOOP-8500) | *Minor* | **Fix javadoc jars to not contain entire target directory**

Hadoop's javadoc jars should be significantly smaller, and contain only javadoc.

As a related cleanup, the dummy hadoop-dist-\* jars are no longer generated as part of the build.


---

* [HADOOP-13792](https://issues.apache.org/jira/browse/HADOOP-13792) | *Major* | **Stackoverflow for schemeless defaultFS with trailing slash**

FileSystem#getDefaultUri will throw IllegalArgumentException if default FS has no scheme and can not be fixed.


---

* [HDFS-10756](https://issues.apache.org/jira/browse/HDFS-10756) | *Major* | **Expose getTrashRoot to HTTPFS and WebHDFS**

"getTrashRoot" returns a trash root for a path. Currently in DFS if the path "/foo" is a normal path, it returns "/user/$USER/.Trash" for "/foo" and if "/foo" is an encrypted zone, it returns "/foo/.Trash/$USER" for the child file/dir of "/foo". This patch is about to override the old "getTrashRoot" of httpfs and webhdfs, so that the behavior of returning trash root in httpfs and webhdfs are consistent with DFS.


---

* [HDFS-10970](https://issues.apache.org/jira/browse/HDFS-10970) | *Major* | **Update jackson from 1.9.13 to 2.x in hadoop-hdfs**

Removed jackson 1.9.13 dependency from hadoop-hdfs-project module.


---

* [YARN-5847](https://issues.apache.org/jira/browse/YARN-5847) | *Major* | **Revert health check exit code check**

This change reverts YARN-5567 from 3.0.0-alpha1. The exit codes of the health check script are once again ignored.


---

* [HDFS-9337](https://issues.apache.org/jira/browse/HDFS-9337) | *Major* | **Validate required params for WebHDFS requests**

Strict validations will be done for mandatory parameters for WebHDFS REST requests.


---

* [HDFS-11116](https://issues.apache.org/jira/browse/HDFS-11116) | *Minor* | **Fix javac warnings caused by deprecation of APIs in TestViewFsDefaultValue**

ViewFileSystem#getServerDefaults(Path) throws NotInMountException instead of FileNotFoundException for unmounted path.


---

* [HADOOP-12718](https://issues.apache.org/jira/browse/HADOOP-12718) | *Major* | **Incorrect error message by fs -put local dir without permission**

<!-- markdown -->

The `hadoop fs -ls` command now prints "Permission denied" rather than "No such file or directory" when the user doesn't have permission to traverse the path.


---

* [HDFS-11056](https://issues.apache.org/jira/browse/HDFS-11056) | *Major* | **Concurrent append and read operations lead to checksum error**

Load last partial chunk checksum properly into memory when converting a finalized/temporary replica to rbw replica. This ensures concurrent reader reads the correct checksum that matches the data before the update.


---

* [YARN-5765](https://issues.apache.org/jira/browse/YARN-5765) | *Blocker* | **Revert CHMOD on the new dirs created-LinuxContainerExecutor creates appcache and its subdirectories with wrong group owner.**

This change reverts YARN-5287 from 3.0.0-alpha1. chmod clears the set-group-ID bit of a regular file hence folder was getting reset with the rights.


---

* [HADOOP-13660](https://issues.apache.org/jira/browse/HADOOP-13660) | *Major* | **Upgrade commons-configuration version to 2.1**

Bump commons-configuration version from 1.6 to 2.1


---

* [HADOOP-12705](https://issues.apache.org/jira/browse/HADOOP-12705) | *Major* | **Upgrade Jackson 2.2.3 to 2.7.8**

We are sorry for causing pain for everyone for whom this Jackson update causes problems, but it was proving impossible to stay on the older version: too much code had moved past it, and by staying back we were limiting what Hadoop could do, and giving everyone who wanted an up to date version of Jackson a different set of problems. We've selected Jackson 2.7.8 as it fixed fix a security issue in XML parsing, yet proved compatible at the API level with the Hadoop codebase --and hopefully everything downstream.


---

* [YARN-5713](https://issues.apache.org/jira/browse/YARN-5713) | *Major* | **Update jackson from 1.9.13 to 2.x in hadoop-yarn**

Jackson 1.9.13 dependency was removed from hadoop-yarn-project.


---

* [HADOOP-13050](https://issues.apache.org/jira/browse/HADOOP-13050) | *Blocker* | **Upgrade to AWS SDK 1.11.45**

The dependency on the AWS SDK has been bumped to 1.11.45.


---

* [HADOOP-1381](https://issues.apache.org/jira/browse/HADOOP-1381) | *Major* | **The distance between sync blocks in SequenceFiles should be configurable**

The default sync interval within new SequenceFile writes is now 100KB, up from the older default of 2000B. The sync interval is now also manually configurable via the SequenceFile.Writer API.


---

* [HDFS-10994](https://issues.apache.org/jira/browse/HDFS-10994) | *Major* | **Support an XOR policy XOR-2-1-64k in HDFS**

This introduced a new erasure coding policy named XOR-2-1-64k using the simple XOR codec, and it can be used to evaluate HDFS erasure coding feature in a small cluster (only 2 + 1 datanodes needed). The policy isn't recommended to be used in a production cluster.


---

* [MAPREDUCE-6743](https://issues.apache.org/jira/browse/MAPREDUCE-6743) | *Major* | **nativetask unit tests need to provide usable output; fix link errors during mvn test**

As part of this patch, the Google test framework code was updated to v1.8.0


---

* [HADOOP-13706](https://issues.apache.org/jira/browse/HADOOP-13706) | *Major* | **Update jackson from 1.9.13 to 2.x in hadoop-common-project**

Removed Jackson 1.9.13 dependency from hadoop-common module.


---

* [HADOOP-13812](https://issues.apache.org/jira/browse/HADOOP-13812) | *Blocker* | **Upgrade Tomcat to 6.0.48**

Tomcat 6.0.46 starts to filter weak ciphers. Some old SSL clients may be affected. It is recommended to upgrade the SSL client. Run the SSL client against https://www.howsmyssl.com/a/check to find out its TLS version and cipher suites.


---

* [HDFS-5517](https://issues.apache.org/jira/browse/HDFS-5517) | *Major* | **Lower the default maximum number of blocks per file**

The default value of "dfs.namenode.fs-limits.max-blocks-per-file" has been reduced from 1M to 10K.


---

* [HADOOP-13827](https://issues.apache.org/jira/browse/HADOOP-13827) | *Major* | **Add reencryptEncryptedKey interface to KMS**

A reencryptEncryptedKey interface is added to the KMS, to re-encrypt an encrypted key with the latest version of encryption key.


---

* [HADOOP-13842](https://issues.apache.org/jira/browse/HADOOP-13842) | *Minor* | **Update jackson from 1.9.13 to 2.x in hadoop-maven-plugins**

Jackson 1.9.13 dependency was removed from hadoop-maven-plugins module.


---

* [MAPREDUCE-4683](https://issues.apache.org/jira/browse/MAPREDUCE-4683) | *Critical* | **Create and distribute hadoop-mapreduce-client-core-tests.jar**

hadoop-mapreduce-client-core module now creates and distributes test jar.


---

* [HDFS-11217](https://issues.apache.org/jira/browse/HDFS-11217) | *Major* | **Annotate NameNode and DataNode MXBean interfaces as Private/Stable**

The DataNode and NameNode MXBean interfaces have been marked as Private and Stable to indicate that although users should not be implementing these interfaces directly, the information exposed by these interfaces is part of the HDFS public API.


---

* [HDFS-11229](https://issues.apache.org/jira/browse/HDFS-11229) | *Blocker* | **HDFS-11056 failed to close meta file**

The fix for HDFS-11056 reads meta file to load last partial chunk checksum when a block is converted from finalized/temporary to rbw. However, it did not close the file explicitly, which may cause number of open files reaching system limit. This jira fixes it by closing the file explicitly after the meta file is read.


---

* [HADOOP-11804](https://issues.apache.org/jira/browse/HADOOP-11804) | *Major* | **Shaded Hadoop client artifacts and minicluster**

<!-- markdown -->

The `hadoop-client` Maven artifact available in 2.x releases pulls
Hadoop's transitive dependencies onto a Hadoop application's classpath.
This can be problematic if the versions of these transitive dependencies
conflict with the versions used by the application.

[HADOOP-11804](https://issues.apache.org/jira/browse/HADOOP-11804) adds
new `hadoop-client-api` and `hadoop-client-runtime` artifacts that
shade Hadoop's dependencies into a single jar. This avoids leaking
Hadoop's dependencies onto the application's classpath.


---

* [HDFS-11160](https://issues.apache.org/jira/browse/HDFS-11160) | *Major* | **VolumeScanner reports write-in-progress replicas as corrupt incorrectly**

Fixed a race condition that caused VolumeScanner to recognize a good replica as a bad one if the replica is also being written concurrently.


---

* [HADOOP-13597](https://issues.apache.org/jira/browse/HADOOP-13597) | *Major* | **Switch KMS from Tomcat to Jetty**

<!-- markdown -->

The following environment variables are deprecated. Set the corresponding
configuration properties instead.

Environment Variable     | Configuration Property       | Configuration File
-------------------------|------------------------------|--------------------
KMS_HTTP_PORT            | hadoop.kms.http.port         | kms-site.xml
KMS_MAX_HTTP_HEADER_SIZE | hadoop.http.max.request.header.size and hadoop.http.max.response.header.size | kms-site.xml
KMS_MAX_THREADS          | hadoop.http.max.threads      | kms-site.xml
KMS_SSL_ENABLED          | hadoop.kms.ssl.enabled       | kms-site.xml
KMS_SSL_KEYSTORE_FILE    | ssl.server.keystore.location | ssl-server.xml
KMS_SSL_KEYSTORE_PASS    | ssl.server.keystore.password | ssl-server.xml
KMS_TEMP                 | hadoop.http.temp.dir         | kms-site.xml

These default HTTP Services have been added.

Name               | Description
-------------------|------------------------------------
/conf              | Display configuration properties
/jmx               | Java JMX management interface
/logLevel          | Get or set log level per class
/logs              | Display log files
/stacks            | Display JVM stacks
/static/index.html | The static home page

The JMX path has been changed from /kms/jmx to /jmx.

Script kms.sh has been deprecated, use `hadoop kms` instead. The new scripts are based on the Hadoop shell scripting framework. `hadoop daemonlog` is supported. SSL configurations are read from ssl-server.xml.


---

* [HADOOP-13953](https://issues.apache.org/jira/browse/HADOOP-13953) | *Major* | **Make FTPFileSystem's data connection mode and transfer mode configurable**

Added two configuration key fs.ftp.data.connection.mode and fs.ftp.transfer.mode, and configure FTP data connection mode and transfer mode accordingly.


---

* [YARN-6071](https://issues.apache.org/jira/browse/YARN-6071) | *Blocker* | **Fix incompatible API change on AM-RM protocol due to YARN-3866 (trunk only)**

**WARNING: No release note provided for this change.**


---

* [HADOOP-13673](https://issues.apache.org/jira/browse/HADOOP-13673) | *Major* | **Update scripts to be smarter when running with privilege**

Apache Hadoop is now able to switch to the appropriate user prior to launching commands so long as the command is being run with a privileged user and the appropriate set of \_USER variables are defined.  This re-enables sbin/start-all.sh and sbin/stop-all.sh as well as fixes the sbin/start-dfs.sh and sbin/stop-dfs.sh to work with both secure and unsecure systems.


---

* [HADOOP-13964](https://issues.apache.org/jira/browse/HADOOP-13964) | *Major* | **Remove vestigal templates directories creation**

This patch removes share/hadoop/{hadoop,hdfs,mapred,yarn}/templates directories and contents.


---

* [YARN-5271](https://issues.apache.org/jira/browse/YARN-5271) | *Major* | **ATS client doesn't work with Jersey 2 on the classpath**

A workaround to avoid dependency conflict with Spark2, before a full classpath isolation solution is implemented.
Skip instantiating a Timeline Service client if encountering NoClassDefFoundError.


---

* [HADOOP-13037](https://issues.apache.org/jira/browse/HADOOP-13037) | *Major* | **Refactor Azure Data Lake Store as an independent FileSystem**

Hadoop now supports integration with Azure Data Lake as an alternative Hadoop-compatible file system. Please refer to the Hadoop site documentation of Azure Data Lake for details on usage and configuration.


---

* [YARN-2877](https://issues.apache.org/jira/browse/YARN-2877) | *Major* | **Extend YARN to support distributed scheduling**

With this JIRA we are introducing distributed scheduling in YARN.
In particular, we make the following contributions:
- Introduce the notion of container types. GUARANTEED containers follow the semantics of the existing YARN containers. OPPORTUNISTIC ones can be seen as lower priority containers, and can be preempted in order to make space for GUARANTEED containers to run.
- Queuing of tasks at the NMs. This enables us to send more containers in an NM than its available resources. At the moment we are allowing queuing of OPPORTUNISTIC containers. Once resources become available at the NM, such containers can immediately start their execution.
- Introduce the AMRMProxy. This is a service running at each node, intercepting the requests between the AM and the RM. It is instrumental for both distributed scheduling and YARN Federation (YARN-2915).
- Enable distributed scheduling. To minimize their allocation latency, OPPORTUNISTIC containers are dispatched immediately to NMs in a distributed fashion by using the AMRMProxy of the node where the corresponding AM resides, without needing to go through the ResourceManager.

All the functionality introduced in this JIRA is disabled by default, so it will not affect the behavior of existing applications.
We have introduced parameters in YarnConfiguration to enable NM queuing (yarn.nodemanager.container-queuing-enabled), distributed scheduling (yarn.distributed-scheduling.enabled) and the AMRMProxy service (yarn.nodemanager.amrmproxy.enable).
AMs currently need to specify the type of container to be requested for each task. We are in the process of adding in the MapReduce AM the ability to randomly request OPPORTUNISTIC containers for a specified percentage of a job's tasks, so that users can experiment with the new features.
