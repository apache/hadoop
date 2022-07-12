
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
# Apache Hadoop  3.3.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [YARN-8587](https://issues.apache.org/jira/browse/YARN-8587) | *Major* | **Delays are noticed to launch docker container**

add "docker inspect" retries to discover container exit code.


---

* [HDFS-14053](https://issues.apache.org/jira/browse/HDFS-14053) | *Major* | **Provide ability for NN to re-replicate based on topology changes.**

A new option (-replicate) is added to fsck command to re-trigger the replication for mis-replicated blocks. This option should be used instead of previous workaround of increasing and then decreasing replication factor (using hadoop fs -setrep command).


---

* [HADOOP-15358](https://issues.apache.org/jira/browse/HADOOP-15358) | *Critical* | **SFTPConnectionPool connections leakage**

Fixed SFTPConnectionPool connections leakage


---

* [YARN-8986](https://issues.apache.org/jira/browse/YARN-8986) | *Minor* | **publish all exposed ports to random ports when using bridge network**

support -p and -P for bridge type network;


---

* [MAPREDUCE-6190](https://issues.apache.org/jira/browse/MAPREDUCE-6190) | *Major* | **If a task stucks before its first heartbeat, it never timeouts and the MR job becomes stuck**

Added "mapreduce.task.stuck.timeout-ms" parameter to timeout a task before sending the task's first heartbeat. The default value is 600000 milliseconds (10 minutes).


---

* [YARN-9071](https://issues.apache.org/jira/browse/YARN-9071) | *Critical* | **NM and service AM don't have updated status for reinitialized containers**

In progress upgrade status may show READY state sooner than actual upgrade operations.  External caller to upgrade API is recommended to wait minimum 30 seconds before querying yarn app -status.


---

* [HADOOP-15428](https://issues.apache.org/jira/browse/HADOOP-15428) | *Major* | **s3guard bucket-info will create s3guard table if FS is set to do this automatically**

If -unguarded flag is passed to \`hadoop s3guard bucket-info\`, it will now proceed with S3Guard disabled instead of failing if S3Guard is not already disabled.


---

* [HADOOP-16000](https://issues.apache.org/jira/browse/HADOOP-16000) | *Major* | **Remove TLSv1 and SSLv2Hello from the default value of hadoop.ssl.enabled.protocols**

TLSv1 and SSLv2Hello were removed from the default value of "hadoop.ssl.enabled.protocols".


---

* [YARN-9084](https://issues.apache.org/jira/browse/YARN-9084) | *Major* | **Service Upgrade: With default readiness check, the status of upgrade is reported to be successful prematurely**

Improve transient container status accuracy for upgrade.


---

* [YARN-8762](https://issues.apache.org/jira/browse/YARN-8762) | *Major* | **[Umbrella] Support Interactive Docker Shell to running Containers**

- Add shell access to YARN containers


---

* [HADOOP-15965](https://issues.apache.org/jira/browse/HADOOP-15965) | *Major* | **Upgrade to ADLS SDK which has major performance improvement for ingress/egress**




---

* [HADOOP-15996](https://issues.apache.org/jira/browse/HADOOP-15996) | *Major* | **Plugin interface to support more complex usernames in Hadoop**

This patch enables "Hadoop" and "MIT" as options for "hadoop.security.auth\_to\_local.mechanism" and defaults to 'hadoop'. This should be backward compatible with pre-HADOOP-12751.

This is basically HADOOP-12751 plus configurable + extended tests.


---

* [YARN-8489](https://issues.apache.org/jira/browse/YARN-8489) | *Major* | **Need to support "dominant" component concept inside YARN service**

- Improved YARN service status report based on dominant component status.


---

* [HADOOP-15922](https://issues.apache.org/jira/browse/HADOOP-15922) | *Major* | **DelegationTokenAuthenticationFilter get wrong doAsUser since it does not decode URL**

- Fix DelegationTokenAuthentication filter for incorrectly double encode doAs user parameter.


---

* [YARN-9116](https://issues.apache.org/jira/browse/YARN-9116) | *Major* | **Capacity Scheduler: implements queue level maximum-allocation inheritance**

After this change, capacity scheduler queue is able to inherit and override max-allocation from its parent queue. The subqueue's max-allocation can be larger or smaller than the parent queue's but can't exceed the global "yarn.scheduler.capacity.maximum-allocation". User can set queue-level max-allocation on any level of queues, using configuration property "yarn.scheduler.capacity.%QUEUE\_PATH%.maximum-allocation". And this property allows user to set max-resource-allocation for customized resource types, e.g "memory=20G,vcores=20,gpu=3".


---

* [HDFS-14158](https://issues.apache.org/jira/browse/HDFS-14158) | *Minor* | **Checkpointer ignores configured time period \> 5 minutes**

Fix the Checkpointer not to ignore the configured "dfs.namenode.checkpoint.period" \> 5 minutes


---

* [YARN-8761](https://issues.apache.org/jira/browse/YARN-8761) | *Major* | **Service AM support for decommissioning component instances**

- Component instance number is not linear increment when decommission feature is used.  Application with assumption of linear increment component instance number maybe impacted by introduction of this feature.


---

* [HDFS-14273](https://issues.apache.org/jira/browse/HDFS-14273) | *Trivial* | **Fix checkstyle issues in BlockLocation's method javadoc**

Thanks for the patch, [~shwetayakkali], and review, [~knanasi]. Committed to trunk.


---

* [HDFS-14118](https://issues.apache.org/jira/browse/HDFS-14118) | *Major* | **Support using DNS to resolve nameservices to IP addresses**

HDFS clients can use a single domain name to discover servers (namenodes/routers/observers) instead of explicitly listing out all hosts in the config


---

* [HDFS-7133](https://issues.apache.org/jira/browse/HDFS-7133) | *Major* | **Support clearing namespace quota on "/"**

Namespace Quota on root can be cleared now.


---

* [HADOOP-16011](https://issues.apache.org/jira/browse/HADOOP-16011) | *Major* | **OsSecureRandom very slow compared to other SecureRandom implementations**

The default RNG is now OpensslSecureRandom instead of OsSecureRandom.The high-performance hardware random number generator (RDRAND instruction) will be used if available. If not, it will fall back to OpenSSL secure random generator.
If you insist on using OsSecureRandom, set hadoop.security.secure.random.impl in core-site.xml to org.apache.hadoop.crypto.random.OsSecureRandom.


---

* [HADOOP-16210](https://issues.apache.org/jira/browse/HADOOP-16210) | *Critical* | **Update guava to 27.0-jre in hadoop-project trunk**

Guava has been updated to 27.0. Code built against this new version  may not link against older releases because of new overloads of methods like Preconditions.checkArgument().


---

* [HADOOP-16085](https://issues.apache.org/jira/browse/HADOOP-16085) | *Major* | **S3Guard: use object version or etags to protect against inconsistent read after replace/overwrite**

S3Guard will now track the etag of uploaded files and, if an S3 bucket is versioned, the object version. You can then control how to react to a mismatch between the data in the DynamoDB table and that in the store: warn, fail, or, when using versions, return the original value.

This adds two new columns to the table: etag and version. This is transparent to older S3A clients -but when such clients add/update data to the S3Guard table, they will not add these values. As a result, the etag/version checks will not work with files uploaded by older clients.

For a consistent experience, upgrade all clients to use the latest hadoop version.


---

* [HADOOP-15563](https://issues.apache.org/jira/browse/HADOOP-15563) | *Major* | **S3Guard to support creating on-demand DDB tables**

S3Guard now defaults to creating DynamoDB tables as "On-Demand", rather than with a prepaid IO capacity. This reduces costs when idle to only the storage of the metadata entries, while delivering significantly faster performance during query planning and other bursts of IO. Consult the S3Guard documentation for further details.


---

* [YARN-9578](https://issues.apache.org/jira/browse/YARN-9578) | *Major* | **Add limit/actions/summarize options for app activities REST API**

This patch changes the format of app activities query URL to http://{rm-host}:{port}/ws/v1/cluster/scheduler/app-activities/{app-id}/, app-id now is a path parameter instead of a query parameter.


---

* [HDFS-14339](https://issues.apache.org/jira/browse/HDFS-14339) | *Major* | **Inconsistent log level practices in RpcProgramNfs3.java**

**WARNING: No release note provided for this change.**


---

* [HDFS-14403](https://issues.apache.org/jira/browse/HDFS-14403) | *Major* | **Cost-Based RPC FairCallQueue**

This adds an extension to the IPC FairCallQueue which allows for the consideration of the \*cost\* of a user's operations when deciding how they should be prioritized, as opposed to the number of operations. This can be helpful for protecting the NameNode from clients which submit very expensive operations (e.g. large listStatus operations or recursive getContentSummary operations).

This can be enabled by setting the \`ipc.\<port\>.costprovder.impl\` configuration to \`org.apache.hadoop.ipc.WeightedTimeCostProvider\`.


---

* [HADOOP-16460](https://issues.apache.org/jira/browse/HADOOP-16460) | *Major* | **ABFS: fix for Sever Name Indication (SNI)**

ABFS: Bug fix to support Server Name Indication (SNI).


---

* [HADOOP-16452](https://issues.apache.org/jira/browse/HADOOP-16452) | *Major* | **Increase ipc.maximum.data.length default from 64MB to 128MB**

Default ipc.maximum.data.length is now 128 MB in order to accommodate huge block reports.


---

* [HDFS-13783](https://issues.apache.org/jira/browse/HDFS-13783) | *Major* | **Balancer: make balancer to be a long service process for easy to monitor it.**

Adds a new parameter to the Balancer CLI, "-asService", to enable the process to be long-running.


---

* [HADOOP-16398](https://issues.apache.org/jira/browse/HADOOP-16398) | *Major* | **Exports Hadoop metrics to Prometheus**

If "hadoop.prometheus.endpoint.enabled" is set to true, Prometheus-friendly formatted metrics can be obtained from '/prom' endpoint of Hadoop daemons. The default value of the property is false.


---

* [HADOOP-16479](https://issues.apache.org/jira/browse/HADOOP-16479) | *Major* | **ABFS FileStatus.getModificationTime returns localized time instead of UTC**

private long parseLastModifiedTime(final String lastModifiedTime)

Timezone is part of lastModifiedTime String as it's last field. But when parsed it ignores timezone field and assumes JVM timezone.
Fix: Made timezone field considered in lastModifiedTime when the same is parsed


---

* [HADOOP-16499](https://issues.apache.org/jira/browse/HADOOP-16499) | *Critical* | **S3A retry policy to be exponential**

The S3A filesystem now backs off exponentially on failures. if you have customized the fs.s3a.retry.limit and fs.s3a.retry.interval options you may wish to review these settings


---

* [HDFS-13505](https://issues.apache.org/jira/browse/HDFS-13505) | *Major* | **Turn on HDFS ACLs by default.**

By default, dfs.namenode.acls.enabled is now set to true. If you haven't set dfs.namenode.acls.enabled=false before and want to keep ACLs feature disabled, you must explicitly set it to false in hdfs-site.xml. i.e.
 \<property\>
   \<name\>dfs.namenode.acls.enabled\</name\>
   \<value\>false\</value\>
 \</property\>


---

* [YARN-2599](https://issues.apache.org/jira/browse/YARN-2599) | *Major* | **Standby RM should expose jmx endpoint**

YARN /jmx URL end points will be accessible per resource manager process. Hence there will not be any redirection to active resource manager while accessing /jmx endpoints.


---

* [HDFS-13101](https://issues.apache.org/jira/browse/HDFS-13101) | *Critical* | **Yet another fsimage corruption related to snapshot**

Fix a corner case in deleting HDFS snapshots. A regression was later found and fixed by HDFS-15012.


---

* [HDFS-14675](https://issues.apache.org/jira/browse/HDFS-14675) | *Major* | **Increase Balancer Defaults Further**

Increase default bandwidth limit for rebalancing per DataNode (dfs.datanode.balance.bandwidthPerSec) from 10MB/s to 100MB/s.

Increase default maximum threads of DataNode balancer (dfs.datanode.balance.max.concurrent.moves) from 50 to 100.


---

* [HDFS-14617](https://issues.apache.org/jira/browse/HDFS-14617) | *Major* | **Improve fsimage load time by writing sub-sections to the fsimage index**

This change allows the inode and inode directory sections of the fsimage to be loaded in parallel. Tests on large images have shown this change to reduce the image load time to about 50% of the pre-change run time.

It works by writing sub-section entries to the image index, effectively splitting each image section into many sub-sections which can be processed in parallel. By default 12 sub-sections per image section are created when the image is saved, and 4 threads are used to load the image at startup.

This is disabled by default for any image with more than 1M inodes (dfs.image.parallel.inode.threshold) and can be enabled by setting dfs.image.parallel.load to true. When the feature is enabled, the next HDFS checkpoint will write the image sub-sections and subsequent namenode restarts can load the image in parallel.

A image with the parallel sections can be read even if the feature is disabled, but HDFS versions without this Jira cannot load an image with parallel sections. OIV can process a parallel enabled image without issues.

Key configuration parameters are:

dfs.image.parallel.load=false - enable or disable the feature

dfs.image.parallel.target.sections = 12 - The target number of subsections. Aim for 2 to 3 times the number of dfs.image.parallel.threads.

dfs.image.parallel.inode.threshold = 1000000 - Only save and load in parallel if the image has more than this number of inodes.

dfs.image.parallel.threads = 4 - The number of threads used to load the image. Testing has shown 4 to be optimal, but this may depends on the environment


---

* [HDFS-14396](https://issues.apache.org/jira/browse/HDFS-14396) | *Blocker* | **Failed to load image from FSImageFile when downgrade from 3.x to 2.x**

During a rolling upgrade from Hadoop 2.x to 3.x, NameNode cannot persist erasure coding information, and therefore a user cannot start using erasure coding feature until finalize is done.


---

* [HADOOP-16554](https://issues.apache.org/jira/browse/HADOOP-16554) | *Major* | **mvn javadoc:javadoc fails in hadoop-aws**

Fixed the link for javadoc of hadoop-aws CommitOperations


---

* [HADOOP-16557](https://issues.apache.org/jira/browse/HADOOP-16557) | *Major* | **[pb-upgrade] Upgrade protobuf.version to 3.7.1**

Upgraded the protobuf to 3.7.1.


---

* [HDFS-14845](https://issues.apache.org/jira/browse/HDFS-14845) | *Critical* | **Ignore AuthenticationFilterInitializer for HttpFSServerWebServer and honor hadoop.http.authentication configs**

httpfs.authentication.\* configs become deprecated and hadoop.http.authentication.\* configs are honored in HttpFS. If the both configs are set, httpfs.authentication.\* configs are effective for compatibility.


---

* [HADOOP-15616](https://issues.apache.org/jira/browse/HADOOP-15616) | *Major* | **Incorporate Tencent Cloud COS File System Implementation**

Tencent cloud is top 2 cloud vendors in China market and the object store COS is widely used among China’s cloud users.  This task implements a COSN filesytem to support Tencent cloud COS natively in Hadoop.  With simple configuration, Hadoop applications, like Spark and Hive,  can read/write data from COS without any code change.


---

* [HDFS-13762](https://issues.apache.org/jira/browse/HDFS-13762) | *Major* | **Support non-volatile storage class memory(SCM) in HDFS cache directives**

Non-volatile storage class memory (SCM, also known as persistent memory) is supported in HDFS cache. To enable SCM cache, user just needs to configure SCM volume for property “dfs.datanode.cache.pmem.dirs” in hdfs-site.xml. And all HDFS cache directives keep unchanged. There are two implementations for HDFS SCM Cache, one is pure java code implementation and the other is native PMDK based implementation. The latter implementation can bring user better performance gain in cache write and cache read. If PMDK native libs could be loaded, it will use PMDK based implementation otherwise it will fallback to java code implementation. To enable PMDK based implementation, user should install PMDK library by referring to the official site http://pmem.io/. Then, build Hadoop with PMDK support by referring to "PMDK library build options" section in \`BUILDING.txt\` in the source code. If multiple SCM volumes are configured, a round-robin policy is used to select an available volume for caching a block. Consistent with DRAM cache, SCM cache also has no cache eviction mechanism. When DataNode receives a data read request from a client, if the corresponding block is cached into SCM, DataNode will instantiate an InputStream with the block location path on SCM (pure java implementation) or cache address on SCM (PMDK based implementation). Once the InputStream is created, DataNode will send the cached data to the client. Please refer "Centralized Cache Management" guide for more details.


---

* [HDFS-14890](https://issues.apache.org/jira/browse/HDFS-14890) | *Blocker* | **Setting permissions on name directory fails on non posix compliant filesystems**

- Fixed namenode/journal startup on Windows.


---

* [HADOOP-16152](https://issues.apache.org/jira/browse/HADOOP-16152) | *Major* | **Upgrade Eclipse Jetty version to 9.4.x**

Upgraded Jetty to 9.4.20.v20190813.
Downstream applications that still depend on Jetty 9.3.x may break.


---

* [HDFS-12943](https://issues.apache.org/jira/browse/HDFS-12943) | *Major* | **Consistent Reads from Standby Node**

Observer is a new type of a NameNode in addition to Active and Standby Nodes in HA settings. An Observer Node maintains a replica of the namespace same as a Standby Node. It additionally allows execution of clients read requests.

To ensure read-after-write consistency within a single client, a state ID is introduced in RPC headers. The Observer responds to the client request only after its own state has caught up with the client’s state ID, which it previously received from the Active NameNode.

Clients can explicitly invoke a new client protocol call msync(), which ensures that subsequent reads by this client from an Observer are consistent.

A new client-side ObserverReadProxyProvider is introduced to provide automatic switching between Active and Observer NameNodes for submitting respectively write and read requests.


---

* [HDFS-13571](https://issues.apache.org/jira/browse/HDFS-13571) | *Major* | **Deadnode detection**

When dead node blocks DFSInputStream, Deadnode detection can find it and share this information to other DFSInputStreams in the same DFSClient. Thus, these DFSInputStreams will not read from the dead node and be blocked by this dead node.


---

* [HADOOP-16771](https://issues.apache.org/jira/browse/HADOOP-16771) | *Major* | **Update checkstyle to 8.26 and maven-checkstyle-plugin to 3.1.0**

Updated checkstyle to 8.26 and updated maven-checkstyle-plugin to 3.1.0.


---

* [YARN-10036](https://issues.apache.org/jira/browse/YARN-10036) | *Major* | **Install yarnpkg and upgrade nodejs in Dockerfile**

In the Dockerfile, nodejs is upgraded to 8.17.0 and yarn 1.12.1 is installed.


---

* [HADOOP-16621](https://issues.apache.org/jira/browse/HADOOP-16621) | *Critical* | **[pb-upgrade] Remove Protobuf classes from signatures of Public APIs**

Following APIs have been removed from Token.java to avoid protobuf classes in signature.
1.   o.a.h.security.token.Token(TokenProto tokenPB)
2.   o.a.h.security.token.Token.toTokenProto()


---

* [HADOOP-16670](https://issues.apache.org/jira/browse/HADOOP-16670) | *Blocker* | **Stripping Submarine code from Hadoop codebase.**

The Submarine subproject is moved to its own Apache Top Level Project. Therefore, the Submarine code is removed from Hadoop 3.3.0 and forward. Please visit https://submarine.apache.org/ for more information.


---

* [YARN-8472](https://issues.apache.org/jira/browse/YARN-8472) | *Major* | **YARN Container Phase 2**

- Improved debugging Docker container on YARN
- Improved security for running Docker containers
- Improved cgroup management for docker container.


---

* [HADOOP-16732](https://issues.apache.org/jira/browse/HADOOP-16732) | *Major* | **S3Guard to support encrypted DynamoDB table**

Support server-side encrypted DynamoDB table for S3Guard. Users don't need to do anything (provide any configuration or change application code) if they don't want to enable server side encryption. Existing tables and the default configuration values will keep existing behavior, which is encrypted using Amazon owned customer master key (CMK).

To enable server side encryption, users can set "fs.s3a.s3guard.ddb.table.sse.enabled" as true. This uses Amazon managed CMK "alias/aws/dynamodb". When it's enabled, a user can also specify her own custom KMS CMK with config "fs.s3a.s3guard.ddb.table.sse.cmk".


---

* [HADOOP-16596](https://issues.apache.org/jira/browse/HADOOP-16596) | *Major* | **[pb-upgrade] Use shaded protobuf classes from hadoop-thirdparty dependency**

All protobuf classes will be used from hadooop-shaded-protobuf\_3\_7 artifact with package prefix as 'org.apache.hadoop.thirdparty.protobuf' instead of 'com.google.protobuf'


---

* [HADOOP-16823](https://issues.apache.org/jira/browse/HADOOP-16823) | *Minor* | **Large DeleteObject requests are their own Thundering Herd**

The page size for bulk delete operations has been reduced from 1000 to 250 to reduce the likelihood of overloading an S3 partition, especially because the retry policy on throttling is simply to try again.

The page size can be set in  "fs.s3a.bulk.delete.page.size"

There is also an option to control whether or not the AWS client retries requests, or whether it is handled exclusively in the S3A code. This option "fs.s3a.experimental.aws.s3.throttling" is true by default. If set to false: everything is handled in the S3A client. While this means that metrics may be more accurate, it may mean that throttling failures in helper threads of the AWS SDK (especially those used in copy/rename) may not be handled properly. This is experimental, and should be left at "true" except when seeking more detail about throttling rates.


---

* [HADOOP-16850](https://issues.apache.org/jira/browse/HADOOP-16850) | *Major* | **Support getting thread info from thread group for JvmMetrics to improve the performance**

Add a new configuration "hadoop.metrics.jvm.use-thread-mxbean". If set to true, ThreadMXBean is used for getting thread info in JvmMetrics, otherwise, ThreadGroup is used. The default value is false (use ThreadGroup for better performance).


---

* [HADOOP-16711](https://issues.apache.org/jira/browse/HADOOP-16711) | *Minor* | **S3A bucket existence checks to support v2 API and "no checks at all"**

The probe for an S3 bucket existing now uses the V2 API, which fails fast if the caller lacks access to the bucket.
The property fs.s3a.bucket.probe can be used to change this. If using a third party library which doesn't support this API call, change it to "1".

to skip the probe entirely, use "0". This will make filesystem instantiation slightly faster, at a cost of postponing all issues related to bucket existence and client authentication until the filesystem API calls are first used.


---

* [HDFS-15186](https://issues.apache.org/jira/browse/HDFS-15186) | *Critical* | **Erasure Coding: Decommission may generate the parity block's content with all 0 in some case**

**WARNING: No release note provided for this change.**


---

* [HDFS-14743](https://issues.apache.org/jira/browse/HDFS-14743) | *Critical* | **Enhance INodeAttributeProvider/ AccessControlEnforcer Interface in HDFS to support Authorization of mkdir, rm, rmdir, copy, move etc...**

A new INodeAttributeProvider API checkPermissionWithContext(AuthorizationContext) is added. Authorization provider implementations may implement this API to get additional context (operation name and caller context) of an authorization request.


---

* [HDFS-14820](https://issues.apache.org/jira/browse/HDFS-14820) | *Major* | ** The default 8KB buffer of BlockReaderRemote#newBlockReader#BufferedOutputStream is too big**

Reduce the output stream buffer size of a DFSClient remote read from 8KB to 512 bytes.


---

* [HADOOP-16661](https://issues.apache.org/jira/browse/HADOOP-16661) | *Major* | **Support TLS 1.3**

Starting with Hadoop 3.3.0, TLS 1.3 is supported on Java 11 Runtime (11.0.3 and above).
This support does not include cloud connectors.

To enable TLSv1.3, add to the core-site.xml:
\<property\>
  \<name\>hadoop.ssl.enabled.protocols\</name\>
  \<value\>TLSv1.3\</value\>
\</property\>


---

* [HADOOP-15620](https://issues.apache.org/jira/browse/HADOOP-15620) | *Major* | **Über-jira: S3A phase VI: Hadoop 3.3 features**

Lots of enhancements to the S3A code, including
\* Delegation Token support
\* better handling of 404 caching
\* S3guard performance, resilience improvements


---

* [HADOOP-16986](https://issues.apache.org/jira/browse/HADOOP-16986) | *Major* | **s3a to not need wildfly on the classpath**

hadoop-aws can use native openssl libraries for better HTTPS performance -consult the S3A performance document for details.

To enable this, wildfly.jar is declared as a compile-time dependency of the hadoop-aws module, so ensuring it ends up on the classpath of the hadoop command line, distribution packages and downstream modules. 

It is however, still optional, unless fs.s3a.ssl.channel.mode is set to openssl



