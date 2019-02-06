
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
# Apache Hadoop  3.0.0-alpha4 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-13956](https://issues.apache.org/jira/browse/HADOOP-13956) | *Critical* | **Read ADLS credentials from Credential Provider**

The hadoop-azure-datalake file system now supports configuration of the Azure Data Lake Store account credentials using the standard Hadoop Credential Provider API. For details, please refer to the documentation on hadoop-azure-datalake and the Credential Provider API.


---

* [MAPREDUCE-6404](https://issues.apache.org/jira/browse/MAPREDUCE-6404) | *Major* | **Allow AM to specify a port range for starting its webapp**

Add a new configuration - "yarn.app.mapreduce.am.webapp.port-range" to specify port-range for webapp launched by AM.


---

* [HDFS-10860](https://issues.apache.org/jira/browse/HDFS-10860) | *Blocker* | **Switch HttpFS from Tomcat to Jetty**

<!-- markdown -->

The following environment variables are deprecated. Set the corresponding
configuration properties instead.

Environment Variable        | Configuration Property       | Configuration File
----------------------------|------------------------------|--------------------
HTTPFS_TEMP                 | hadoop.http.temp.dir         | httpfs-site.xml
HTTPFS_HTTP_PORT            | hadoop.httpfs.http.port      | httpfs-site.xml
HTTPFS_MAX_HTTP_HEADER_SIZE | hadoop.http.max.request.header.size and hadoop.http.max.response.header.size | httpfs-site.xml
HTTPFS_MAX_THREADS          | hadoop.http.max.threads      | httpfs-site.xml
HTTPFS_SSL_ENABLED          | hadoop.httpfs.ssl.enabled    | httpfs-site.xml
HTTPFS_SSL_KEYSTORE_FILE    | ssl.server.keystore.location | ssl-server.xml
HTTPFS_SSL_KEYSTORE_PASS    | ssl.server.keystore.password | ssl-server.xml

These default HTTP Services have been added.

Name               | Description
-------------------|------------------------------------
/conf              | Display configuration properties
/jmx               | Java JMX management interface
/logLevel          | Get or set log level per class
/logs              | Display log files
/stacks            | Display JVM stacks
/static/index.html | The static home page

Script httpfs.sh has been deprecated, use `hdfs httpfs` instead. The new scripts are based on the Hadoop shell scripting framework. `hadoop daemonlog` is supported. SSL configurations are read from ssl-server.xml.


---

* [HDFS-11210](https://issues.apache.org/jira/browse/HDFS-11210) | *Major* | **Enhance key rolling to guarantee new KeyVersion is returned from generateEncryptedKeys after a key is rolled**

<!-- markdown --> 

An `invalidateCache` command has been added to the KMS.
The `rollNewVersion` semantics of the KMS has been improved so that after a key's version is rolled, `generateEncryptedKey` of that key guarantees to return the `EncryptedKeyVersion` based on the new key version.


---

* [HADOOP-13119](https://issues.apache.org/jira/browse/HADOOP-13119) | *Major* | **Add ability to secure log servlet using proxy users**

**WARNING: No release note provided for this change.**


---

* [HADOOP-13075](https://issues.apache.org/jira/browse/HADOOP-13075) | *Major* | **Add support for SSE-KMS and SSE-C in s3a filesystem**

The new encryption options SSE-KMS and especially SSE-C must be considered experimental at present. If you are using SSE-C, problems may arise if the bucket mixes encrypted and unencrypted files. For SSE-KMS, there may be extra throttling of IO, especially with the fadvise=random option. You may wish to request an increase in your KMS IOPs limits.


---

* [HDFS-11026](https://issues.apache.org/jira/browse/HDFS-11026) | *Major* | **Convert BlockTokenIdentifier to use Protobuf**

Changed the serialized format of BlockTokenIdentifier to protocol buffers. Includes logic to decode both the old Writable format and the new PB format to support existing clients. Client implementations in other languages will require similar functionality.


---

* [HADOOP-13929](https://issues.apache.org/jira/browse/HADOOP-13929) | *Major* | **ADLS connector should not check in contract-test-options.xml**

To run live unit tests, create src/test/resources/auth-keys.xml with the same properties as in the deprecated contract-test-options.xml.


---

* [HDFS-11100](https://issues.apache.org/jira/browse/HDFS-11100) | *Critical* | **Recursively deleting file protected by sticky bit should fail**

Changed the behavior of removing directories with sticky bits, so that it is closer to what most Unix/Linux users would expect.


---

* [YARN-6177](https://issues.apache.org/jira/browse/YARN-6177) | *Major* | **Yarn client should exit with an informative error message if an incompatible Jersey library is used at client**

Let yarn client exit with an informative error message if an incompatible Jersey library is used from client side.


---

* [HADOOP-13805](https://issues.apache.org/jira/browse/HADOOP-13805) | *Major* | **UGI.getCurrentUser() fails if user does not have a keytab associated**

Due to a remaining issue after HADOOP-13558, an UGI may still try to renew the TGT even though the UGI is created from an existing Subject. The renewal would fail because of non-existing keytab. 

Fixing the issue means different behavior which is incompatible, however,  configuration property "hadoop.treat.subject.external" is introduced to enable the fix (disabled by default). The behavior is the same as before when the fix is not enabled.


---

* [HDFS-11405](https://issues.apache.org/jira/browse/HDFS-11405) | *Blocker* | **Rename "erasurecode" CLI subcommand to "ec"**

The "hdfs erasurecode" CLI command has been renamed to "hdfs ec" for ease-of-use.


---

* [HDFS-11426](https://issues.apache.org/jira/browse/HDFS-11426) | *Major* | **Refactor EC CLI to be similar to storage policies CLI**

The \`hdfs ec\` CLI command has been substantially reworked to make the calling patterns more similar to the \`hdfs storagepolicies\` command. See \`hdfs ec -help\` and the HDFS erasure coding documentation for more information.


---

* [HADOOP-13817](https://issues.apache.org/jira/browse/HADOOP-13817) | *Minor* | **Add a finite shell command timeout to ShellBasedUnixGroupsMapping**

A new introduced configuration key "hadoop.security.groups.shell.command.timeout" allows applying a finite wait timeout over the 'id' commands launched by the ShellBasedUnixGroupsMapping plugin. Values specified can be in any valid time duration units: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/conf/Configuration.html#getTimeDuration-java.lang.String-long-java.util.concurrent.TimeUnit-

Value defaults to 0, indicating infinite wait (preserving existing behaviour).


---

* [HDFS-11427](https://issues.apache.org/jira/browse/HDFS-11427) | *Major* | **Rename "rs-default" to "rs"**

The "rs-default" codec has been renamed to simply "rs" for simplicity. Previous configuration keys like "io.erasurecode.codec.rs-default" have also been renamed to match.


---

* [HDFS-11382](https://issues.apache.org/jira/browse/HDFS-11382) | *Major* | **Persist Erasure Coding Policy ID in a new optional field in INodeFile in FSImage**

The FSImage on-disk format for INodeFile is changed to additionally include a field for Erasure Coded files. This optional field 'erasureCodingPolicyID' which is unit32 type is available for all Erasure Coded files and represents the Erasure Coding Policy ID. Previously, the 'replication' field in INodeFile disk format was overloaded  to represent the same Erasure Coding Policy ID.


---

* [HDFS-11428](https://issues.apache.org/jira/browse/HDFS-11428) | *Major* | **Change setErasureCodingPolicy to take a required string EC policy name**

{{HdfsAdmin#setErasureCodingPolicy}} now takes a String {{ecPolicyName}} rather than an ErasureCodingPolicy object. The corresponding RPC's wire format has also been modified.


---

* [HADOOP-14138](https://issues.apache.org/jira/browse/HADOOP-14138) | *Critical* | **Remove S3A ref from META-INF service discovery, rely on existing core-default entry**

The classpath implementing the s3a filesystem is now defined in core-default.xml. Attempting to instantiate an S3A filesystem instance using a Configuration instance which has not included the default resorts will fail. Applications should not be doing this anyway, as it will lose other critical  configuration options needed by the filesystem.


---

* [HADOOP-6801](https://issues.apache.org/jira/browse/HADOOP-6801) | *Minor* | **io.sort.mb and io.sort.factor were renamed and moved to mapreduce but are still in CommonConfigurationKeysPublic.java and used in SequenceFile.java**

Two new configuration keys, seq.io.sort.mb and seq.io.sort.factor have been introduced for the SequenceFile's Sorter feature to replace older, deprecated property keys of io.sort.mb and io.sort.factor.

This only affects direct users of the org.apache.hadoop.io.SequenceFile.Sorter Java class. For controlling MR2's internal sorting instead, use the existing config keys of mapreduce.task.io.sort.mb and mapreduce.task.io.sort.factor.


---

* [HDFS-8112](https://issues.apache.org/jira/browse/HDFS-8112) | *Blocker* | **Relax permission checking for EC related operations**

The HdfsAdmin erasure coding APIs (set, unset, get) are now usable by non-superusers based on appropriate file and directory permissions.


---

* [HDFS-11498](https://issues.apache.org/jira/browse/HDFS-11498) | *Major* | **Make RestCsrfPreventionHandler and WebHdfsHandler compatible with Netty 4.0**

This JIRA sets the Netty 4 dependency to 4.0.23. This is an incompatible change for the 3.0 release line, as 3.0.0-alpha1 and 3.0.0-alpha2 depended on Netty 4.1.0.Beta5.


---

* [HDFS-11152](https://issues.apache.org/jira/browse/HDFS-11152) | *Blocker* | **Start erasure coding policy ID number from 1 instead of 0 to void potential unexpected errors**

The NameNode metadata for storing erasure coding policies has changed.


---

* [HDFS-11314](https://issues.apache.org/jira/browse/HDFS-11314) | *Blocker* | **Enforce set of enabled EC policies on the NameNode**

HDFS will now restrict the set of erasure coding policies that can be set by users. The set of allowed policies can be configured via "dfs.namenode.ec.policies.enabled" on the NameNode. Please see the documentation for more details.


---

* [HDFS-11499](https://issues.apache.org/jira/browse/HDFS-11499) | *Major* | **Decommissioning stuck because of failing recovery**

Allow a block to complete if the number of replicas on live nodes, decommissioning nodes and nodes in maintenance mode satisfies minimum replication factor.
The fix prevents block recovery failure if replica of last block is being decommissioned. Vice versa, the decommissioning will be stuck, waiting for the last block to be completed. In addition, file close() operation will not fail due to last block being decommissioned.


---

* [HDFS-11505](https://issues.apache.org/jira/browse/HDFS-11505) | *Major* | **Do not enable any erasure coding policies by default**

By default, none of the built-in erasure coding policies are enabled. Users have to explicitly enable the erasure coding policy via the hdfs configuration 'dfs.namenode.ec.policies.enabled' before setting the policy on any directories.


---

* [HADOOP-14213](https://issues.apache.org/jira/browse/HADOOP-14213) | *Major* | **Move Configuration runtime check for hadoop-site.xml to initialization**

Move the check for hadoop-site.xml to static initialization of the Configuration class.


---

* [HADOOP-10101](https://issues.apache.org/jira/browse/HADOOP-10101) | *Major* | **Update guava dependency to the latest version**

Guava is updated to version 21.0. 

In the background of merging this patch into trunk, there is a work, shaded Hadoop client artifacts and minicluster, on HADOOP-11804. hadoop-client has its own Guava which is shaded, so we can update dependency with minimum effect compare to previous HADOOP-11804. 

See also HADOOP-14238 as related problem.


---

* [HADOOP-14038](https://issues.apache.org/jira/browse/HADOOP-14038) | *Minor* | **Rename ADLS credential properties**

<!-- markdown -->

* Properties {{dfs.adls.*}} are renamed {{fs.adl.*}}
* Property {{adl.dfs.enable.client.latency.tracker}} is renamed {{adl.enable.client.latency.tracker}}
* Old properties are still supported


---

* [HADOOP-14267](https://issues.apache.org/jira/browse/HADOOP-14267) | *Major* | **Make DistCpOptions class immutable**

DistCpOptions has been changed to be constructed with a Builder pattern. This potentially affects applications that invoke DistCp with the Java API.


---

* [HDFS-11596](https://issues.apache.org/jira/browse/HDFS-11596) | *Critical* | **hadoop-hdfs-client jar is in the wrong directory in release tarball**

The scope of hadoop-hdfs's dependency on hadoop-hdfs-client has changed from "compile" to "provided". This may affect users who directly consume hadoop-hdfs, which is a private API. These users need to add a new dependency on hadoop-hdfs-client, or better yet, switch from hadoop-hdfs to hadoop-hdfs-client.


---

* [HADOOP-14202](https://issues.apache.org/jira/browse/HADOOP-14202) | *Major* | **fix jsvc/secure user var inconsistencies**

<!-- markdown -->

The secure user variables have been changed to be consistent with the rest of the environment variable changes:

| Old | New |
|:---- |:---- | 
| HADOOP\_SECURE\_DN\_USER  | HDFS\_DATANODE\_SECURE\_USER |
| HADOO\P_PRIVILEGED\_NFS\_USER | HDFS\_NFS3\_SECURE\_USER |


---

* [HADOOP-14174](https://issues.apache.org/jira/browse/HADOOP-14174) | *Major* | **Set default ADLS access token provider type to ClientCredential**

Switch the default ADLS access token provider type from Custom to ClientCredential.


---

* [YARN-6298](https://issues.apache.org/jira/browse/YARN-6298) | *Blocker* | **Metric preemptCall is not used in new preemption**

Metric preemptCall in FSOpDurations is no longer supported.


---

* [HADOOP-14285](https://issues.apache.org/jira/browse/HADOOP-14285) | *Major* | **Update minimum version of Maven from 3.0 to 3.3**

Minimum version of Apache Maven has been updated from 3.0 to 3.3.


---

* [HADOOP-14225](https://issues.apache.org/jira/browse/HADOOP-14225) | *Minor* | **Remove xmlenc dependency**

xmlenc dependency has been removed. If you rely on the transitive dependency, you need to set the dependency explicitly in your code after this change.


---

* [HADOOP-13665](https://issues.apache.org/jira/browse/HADOOP-13665) | *Blocker* | **Erasure Coding codec should support fallback coder**

Use configuration properties io.erasurecode.codec.{rs-legacy,rs,xor}.rawcoders to control erasure coding codec. These properties support codec fallback in case the previous codec is not loaded.


---

* [HADOOP-14248](https://issues.apache.org/jira/browse/HADOOP-14248) | *Major* | **Retire SharedInstanceProfileCredentialsProvider in trunk.**

SharedInstanceProfileCredentialsProvider is removed after this change. Users should use InstanceProfileCredentialsProvider provided by AWS SDK instead, which itself enforces a singleton instance to reduce calls to AWS EC2 Instance Metadata Service.


---

* [HDFS-11565](https://issues.apache.org/jira/browse/HDFS-11565) | *Blocker* | **Use compact identifiers for built-in ECPolicies in HdfsFileStatus**

Some of the existing fields in ErasureCodingPolicyProto have changed from required to optional. For system EC policies, these fields are populated from hardcoded values.


---

* [HADOOP-11794](https://issues.apache.org/jira/browse/HADOOP-11794) | *Major* | **Enable distcp to copy blocks in parallel**

If  a positive value is passed to command line switch -blocksperchunk, files with more blocks than this value will be split into chunks of \`\<blocksperchunk\>\` blocks to be transferred in parallel, and reassembled on the destination. By default, \`\<blocksperchunk\>\` is 0 and the files will be transmitted in their entirety without splitting. This switch is only applicable when both the source file system supports getBlockLocations and target supports concat.


---

* [YARN-3427](https://issues.apache.org/jira/browse/YARN-3427) | *Blocker* | **Remove deprecated methods from ResourceCalculatorProcessTree**

The deprecated ProcessTree methods getCumulativeVmem
 and getCumulativeRssmem have been removed.


---

* [HDFS-11402](https://issues.apache.org/jira/browse/HDFS-11402) | *Major* | **HDFS Snapshots should capture point-in-time copies of OPEN files**

When the config param "dfs.namenode.snapshot.capture.openfiles" is enabled, HDFS snapshots taken will additionally capture point-in-time copies of the open files that have valid leases. Even when the current version open files grow or shrink in size, the snapshot will always retain the immutable versions of these open files, just as in for all other closed files. Note: The file length captured for open files in the snapshot was the one recorded in NameNode at the time of snapshot and it may be shorter than what the client has written till then. In order to capture the latest length, the client can call hflush/hsync with the flag SyncFlag.UPDATE\_LENGTH on the open files handles.


---

* [HDFS-6708](https://issues.apache.org/jira/browse/HDFS-6708) | *Major* | **StorageType should be encoded in the block token**

StorageTypes are now encoded in the BlockTokenIdentifier to ensure that the intended StorageType for writes is not tampered with on it's way through the Client to the Datanode.


---

* [HADOOP-10105](https://issues.apache.org/jira/browse/HADOOP-10105) | *Blocker* | **remove httpclient dependency**

Apache Httpclient has been removed as a dependency. This library is End of Life: people using it should move to its {{httpcore}} successor. If you cannot do that, you must add an explicit dependency on {{httpclient}} in your classpath.


---

* [HADOOP-13200](https://issues.apache.org/jira/browse/HADOOP-13200) | *Blocker* | **Implement customizable and configurable erasure coders**

CodecRegistry uses ServiceLoader to dynamically load all implementations of RawErasureCoderFactory. In Hadoop 3.0, there are several built-in implementations, and user can also provide self-defined implementations with the corresponding resource files. 
For each codec, user can configure the order of the implementations with the configuration keys:
\`io.erasurecode.codec.rs.rawcoders\` for the default RS codec,
\`io.erasurecode.codec.rs-legacy.rawcoders\` for the legacy RS codec,
\`io.erasurecode.codec.xor.rawcoders\` for the XOR codec.
User can also configure self-defined codec with the configuration key like:
\`io.erasurecode.codec.self-defined.rawcoders\`.
For each codec, Hadoop will use the implementation according to the order configured. If the former implementation fails, it will fall back to call the latter one. The order is defined by a list of coder names separated by commas. The names for the built-in implementations are:
\`rs\_native\` and \`rs\_java\` for the default RS codec, of which  the former is a native implementation which leverages Intel ISA-L library, which is the default implementation and the latter is the implementation in pure Java,
\`rs-legacy\_java\` for the legacy RS codec, which is the default implementation in pure Java,
\`xor\_native\` and \`xor\_java\` for the XOR codec, of which the former is the Intel ISA-L implementation which is the default one and the latter in pure Java.


---

* [YARN-2962](https://issues.apache.org/jira/browse/YARN-2962) | *Critical* | **ZKRMStateStore: Limit the number of znodes under a znode**

**WARNING: No release note provided for this change.**


---

* [HADOOP-14386](https://issues.apache.org/jira/browse/HADOOP-14386) | *Blocker* | **Rewind trunk from Guava 21.0 back to Guava 11.0.2**

YARN application tags can no longer contain non-printable ASCII characters.


---

* [HADOOP-14401](https://issues.apache.org/jira/browse/HADOOP-14401) | *Major* | **maven-project-info-reports-plugin can be removed**

hadoop-auth and hadoop-hdfs-httpfs modules no longer generate dependencies.html via maven-project-info-reports-plugin.


---

* [HADOOP-14375](https://issues.apache.org/jira/browse/HADOOP-14375) | *Minor* | **Remove tomcat support from hadoop-functions.sh**

This change removes the support in the shell scripts for Tomcat that was added in 3.0.0-alpha1.


---

* [HADOOP-14419](https://issues.apache.org/jira/browse/HADOOP-14419) | *Minor* | **Remove findbugs report from docs profile**

Findbugs report is no longer part of the documentation.


---

* [HDFS-11661](https://issues.apache.org/jira/browse/HDFS-11661) | *Blocker* | **GetContentSummary uses excessive amounts of memory**

Reverted HDFS-10797 to fix a scalability regression brought by the commit.


---

* [HADOOP-14426](https://issues.apache.org/jira/browse/HADOOP-14426) | *Blocker* | **Upgrade Kerby version from 1.0.0-RC2 to 1.0.0**

**WARNING: No release note provided for this change.**


---

* [HADOOP-14407](https://issues.apache.org/jira/browse/HADOOP-14407) | *Major* | **DistCp - Introduce a configurable copy buffer size**

The copy buffer size can be configured via the new parameter \<copybuffersize\>. By default the \<copybuffersize\> is set to 8KB.


---

* [HADOOP-13921](https://issues.apache.org/jira/browse/HADOOP-13921) | *Critical* | **Remove Log4j classes from JobConf**

Changes the type of JobConf.DEFAULT\_LOG\_LEVEL from a Log4J Level to a String. Clients that referenced this field will need to be recompiled and may need to alter their source to account for the type change. The level itself remains conceptually at "INFO".


---

* [HADOOP-8143](https://issues.apache.org/jira/browse/HADOOP-8143) | *Minor* | **Change distcp to have -pb on by default**

If -p option of distcp command is unspecified, block size is preserved.


---

* [HADOOP-14502](https://issues.apache.org/jira/browse/HADOOP-14502) | *Minor* | **Confusion/name conflict between NameNodeActivity#BlockReportNumOps and RpcDetailedActivity#BlockReportNumOps**

Remove the BlockReport(NumOps,AvgTime) metrics emitted under the NameNodeActivity context in favor of StorageBlockReport(NumOps,AvgTime) which more accurately represent the metric. Same for the corresponding quantile metrics.


---

* [HDFS-11067](https://issues.apache.org/jira/browse/HDFS-11067) | *Major* | **DFS#listStatusIterator(..) should throw FileNotFoundException if the directory deleted before fetching next batch of entries**

DistributedFileSystem#listStatusIterator(..) throws FileNotFoundException if directory got deleted during iterating over large list beyond ls limit.


---

* [YARN-6127](https://issues.apache.org/jira/browse/YARN-6127) | *Major* | **Add support for work preserving NM restart when AMRMProxy is enabled**

This breaks rolling upgrades because it changes the major version of the NM state store schema. Therefore when a new NM comes up on an old state store it crashes.

The state store versions for this change have been updated in YARN-6798.


---

* [HDFS-11956](https://issues.apache.org/jira/browse/HDFS-11956) | *Blocker* | **Do not require a storage ID or target storage IDs when writing a block**

Hadoop 2.x clients do not pass the storage ID or target storage IDs when writing a block. For backwards compatibility, the DataNode will not require the presence of these fields. This means older clients are unable to write to a particular storage as chosen by the NameNode (e.g. HDFS-9806).


---

* [HADOOP-14536](https://issues.apache.org/jira/browse/HADOOP-14536) | *Major* | **Update azure-storage sdk to version 5.3.0**

The WASB FileSystem now uses version 5.3.0 of the Azure Storage SDK.


---

* [HADOOP-14546](https://issues.apache.org/jira/browse/HADOOP-14546) | *Major* | **Azure: Concurrent I/O does not work when secure.mode is enabled**

Fix to wasb:// (Azure) file system that allows the concurrent I/O feature to be used with the secure mode feature.



