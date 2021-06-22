
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
# Apache Hadoop  3.3.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-16054](https://issues.apache.org/jira/browse/HADOOP-16054) | *Major* | **Update Dockerfile to use Bionic**

The build image has been upgraded to Bionic.


---

* [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | *Major* | **ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address**

ZKFC binds host address to "dfs.namenode.servicerpc-bind-host", if configured. Otherwise, it binds to "dfs.namenode.rpc-bind-host". If neither of those is configured, ZKFC binds itself to NameNode RPC server address (effectively "dfs.namenode.rpc-address").


---

* [HADOOP-16916](https://issues.apache.org/jira/browse/HADOOP-16916) | *Minor* | **ABFS: Delegation SAS generator for integration with Ranger**

Azure ABFS support for Shared Access Signatures (SAS)


---

* [HADOOP-17044](https://issues.apache.org/jira/browse/HADOOP-17044) | *Major* | **Revert "HADOOP-8143. Change distcp to have -pb on by default"**

Distcp block size is not preserved by default, unless -pb is specified. This restores the behavior prior to Hadoop 3.


---

* [HADOOP-17024](https://issues.apache.org/jira/browse/HADOOP-17024) | *Major* | **ListStatus on ViewFS root (ls "/") should list the linkFallBack root (configured target root).**

ViewFS#listStatus on root("/") considers listing from fallbackLink if available. If the same directory name is present in configured mount path as well as in fallback link, then only the configured mount path will be listed in the returned result.


---

* [HDFS-13183](https://issues.apache.org/jira/browse/HDFS-13183) | *Major* | **Standby NameNode process getBlocks request to reduce Active load**

Enable balancer to redirect getBlocks request to a Standby Namenode, thus reducing the performance impact of balancer to the Active NameNode.

The feature is disabled by default. To enable it, configure the hdfs-site.xml of balancer: 
dfs.ha.allow.stale.reads = true.


---

* [HADOOP-17076](https://issues.apache.org/jira/browse/HADOOP-17076) | *Minor* | **ABFS: Delegation SAS Generator Updates**

Azure Blob File System (ABFS) SAS Generator Update


---

* [HADOOP-17089](https://issues.apache.org/jira/browse/HADOOP-17089) | *Critical* | **WASB: Update azure-storage-java SDK**

Azure WASB bug fix that can cause list results to appear empty.


---

* [HADOOP-17105](https://issues.apache.org/jira/browse/HADOOP-17105) | *Minor* | **S3AFS globStatus attempts to resolve symlinks**

Remove unnecessary symlink resolution in S3AFileSystem globStatus


---

* [HADOOP-13230](https://issues.apache.org/jira/browse/HADOOP-13230) | *Major* | **S3A to optionally retain directory markers**

The S3A connector now has an option to stop deleting directory markers as files are written. This eliminates the IO throttling the operations can cause, and avoids creating tombstone markers on versioned S3 buckets.

This feature is incompatible with all versions of Hadoop which lack the HADOOP-17199 change to list and getFileStatus calls.

Consult the S3A documentation for further details


---

* [HADOOP-17215](https://issues.apache.org/jira/browse/HADOOP-17215) | *Major* | **ABFS: Support for conditional overwrite**

ABFS: Support for conditional overwrite.


---

* [YARN-9809](https://issues.apache.org/jira/browse/YARN-9809) | *Major* | **NMs should supply a health status when registering with RM**

Improved node registration with node health status.


---

* [HADOOP-17125](https://issues.apache.org/jira/browse/HADOOP-17125) | *Major* | **Using snappy-java in SnappyCodec**

The SnappyCodec uses the snappy-java compression library, rather than explicitly referencing native binaries.  It contains the native libraries for many operating systems and instruction sets, falling back to a pure java implementation. It does requires the snappy-java.jar is on the classpath. It can be found in hadoop-common/lib, and has already been present as part of the avro dependencies


---

* [HDFS-15253](https://issues.apache.org/jira/browse/HDFS-15253) | *Major* | **Set default throttle value on dfs.image.transfer.bandwidthPerSec**

The configuration dfs.image.transfer.bandwidthPerSec which defines the maximum bandwidth available for fsimage transfer is changed from 0 (meaning no throttle at all) to 50MB/s.


---

* [HADOOP-17021](https://issues.apache.org/jira/browse/HADOOP-17021) | *Minor* | **Add concat fs command**

"hadoop fs" has a concat command. Available on all filesystems which support the concat API including HDFS and WebHDFS


---

* [HADOOP-17292](https://issues.apache.org/jira/browse/HADOOP-17292) | *Major* | **Using lz4-java in Lz4Codec**

The Hadoop's LZ4 compression codec now depends on lz4-java. The native LZ4 is performed by the encapsulated JNI and it is no longer necessary to install and configure the lz4 system package.

The lz4-java is declared in provided scope. Applications that wish to use lz4 codec must declare dependency on lz4-java explicitly.


---

* [HADOOP-17313](https://issues.apache.org/jira/browse/HADOOP-17313) | *Major* | **FileSystem.get to support slow-to-instantiate FS clients**

The option "fs.creation.parallel.count" sets a a semaphore to throttle the number of FileSystem instances which
can be created simultaneously.

This is designed to reduce the impact of many threads in an application calling
FileSystem.get() on a filesystem which takes time to instantiate -for example
to an object where HTTPS connections are set up during initialization.
Many threads trying to do this may create spurious delays by conflicting
for access to synchronized blocks, when simply limiting the parallelism
diminishes the conflict, so speeds up all threads trying to access
the store.

The default value, 64, is larger than is likely to deliver any speedup -but
it does mean that there should be no adverse effects from the change.

If a service appears to be blocking on all threads initializing connections to
abfs, s3a or store, try a smaller (possibly significantly smaller) value.


---

* [HADOOP-17338](https://issues.apache.org/jira/browse/HADOOP-17338) | *Major* | **Intermittent S3AInputStream failures: Premature end of Content-Length delimited message body etc**

**WARNING: No release note provided for this change.**


---

* [HDFS-15380](https://issues.apache.org/jira/browse/HDFS-15380) | *Major* | **RBF: Could not fetch real remote IP in RouterWebHdfsMethods**

**WARNING: No release note provided for this change.**


---

* [HADOOP-17422](https://issues.apache.org/jira/browse/HADOOP-17422) | *Major* | **ABFS: Set default ListMaxResults to max server limit**

ABFS: The default value for "fs.azure.list.max.results" was changed from 500 to 5000.


---

* [HDFS-15719](https://issues.apache.org/jira/browse/HDFS-15719) | *Critical* | **[Hadoop 3] Both NameNodes can crash simultaneously due to the short JN socket timeout**

The default value of the configuration hadoop.http.idle\_timeout.ms (how long does Jetty disconnect an idle connection) is changed from 10000 to 60000. 
This property is inlined during compile time, so an application that references this property must be recompiled in order for it to take effect.


---

* [HADOOP-17454](https://issues.apache.org/jira/browse/HADOOP-17454) | *Major* | **[s3a] Disable bucket existence check - set fs.s3a.bucket.probe to 0**

S3A bucket existence check is disabled (fs.s3a.bucket.probe is 0), so there will be no existence check on the bucket during the S3AFileSystem initialization. The first operation which attempts to interact with the bucket which will fail if the bucket does not exist.


---

* [HADOOP-17337](https://issues.apache.org/jira/browse/HADOOP-17337) | *Blocker* | **S3A NetworkBinding has a runtime class dependency on a third-party shaded class**

the s3a filesystem will link against the unshaded AWS s3 SDK. Making an application's dependencies consistent with that SDK is left as exercise. Note: native openssl is not supported as a socket factory in unshaded deployments.


---

* [HADOOP-16748](https://issues.apache.org/jira/browse/HADOOP-16748) | *Major* | **Migrate to Python 3 and upgrade Yetus to 0.13.0**

<!-- markdown -->
- Upgraded Yetus to 0.13.0.
- Removed determine-flaky-tests-hadoop.py.
- Temporarily disabled shelldocs check in the Jenkins jobs due to YETUS-1099.


---

* [HADOOP-16721](https://issues.apache.org/jira/browse/HADOOP-16721) | *Blocker* | **Improve S3A rename resilience**

The S3A connector's rename() operation now raises FileNotFoundException if the source doesn't exist; FileAlreadyExistsException if the destination is unsuitable. It no longer checks for a parent directory existing -instead it simply verifies that there is no file immediately above the destination path.


---

* [HADOOP-17531](https://issues.apache.org/jira/browse/HADOOP-17531) | *Critical* | **DistCp: Reduce memory usage on copying huge directories**

Added a -useiterator option in distcp which uses listStatusIterator for building the listing. Primarily to reduce memory usage at client for building listing.


---

* [HADOOP-16870](https://issues.apache.org/jira/browse/HADOOP-16870) | *Major* | **Use spotbugs-maven-plugin instead of findbugs-maven-plugin**

Removed findbugs from the hadoop build images and added spotbugs instead.
Upgraded SpotBugs to 4.2.2 and spotbugs-maven-plugin to 4.2.0.


---

* [HADOOP-17222](https://issues.apache.org/jira/browse/HADOOP-17222) | *Major* | ** Create socket address leveraging URI cache**

DFS client can use the newly added URI cache when creating socket address for read operations. By default it is disabled. When enabled, creating socket address will use cached URI object based on host:port to reduce the frequency of URI object creation.

To enable it, set the following config key to true:
\<property\>
  \<name\>dfs.client.read.uri.cache.enabled\</name\>
  \<value\>true\</value\>
\</property\>


---

* [HADOOP-16524](https://issues.apache.org/jira/browse/HADOOP-16524) | *Major* | **Automatic keystore reloading for HttpServer2**

Adds auto-reload of keystore.

Adds below new config (default 10 seconds):

 ssl.{0}.stores.reload.interval

The refresh interval used to check if either of the truststore or keystore certificate file has changed.


---

* [HDFS-15942](https://issues.apache.org/jira/browse/HDFS-15942) | *Major* | **Increase Quota initialization threads**

The default quota initialization thread count during the NameNode startup process (dfs.namenode.quota.init-threads) is increased from 4 to 12.


---

* [HDFS-15975](https://issues.apache.org/jira/browse/HDFS-15975) | *Minor* | **Use LongAdder instead of AtomicLong**

This JIRA changes public fields in DFSHedgedReadMetrics. If you are using the public member variables of DFSHedgedReadMetrics, you need to use them through the public API.


---

* [HADOOP-17597](https://issues.apache.org/jira/browse/HADOOP-17597) | *Minor* | **Add option to downgrade S3A rejection of Syncable to warning**

The S3A output streams now raise UnsupportedOperationException on calls to Syncable.hsync() or Syncable.hflush(). This is to make absolutely clear to programs trying to use the syncable API that the stream doesn't save any data at all until close. Programs which use this to flush their write ahead logs will fail immediately, rather than appear to succeed but without saving any data.

To downgrade the API calls to simply printing a warning, set fs.s3a.downgrade.syncable.exceptions" to true. This will not change the other behaviour: no data is saved.

Object stores are not filesystems.



