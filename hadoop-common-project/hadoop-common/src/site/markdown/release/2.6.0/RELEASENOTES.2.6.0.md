
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
# Apache Hadoop  2.6.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-10244](https://issues.apache.org/jira/browse/HADOOP-10244) | *Major* | **TestKeyShell improperly tests the results of a Delete**

Fix of inappropriate test of delete functionality.


---

* [HADOOP-10583](https://issues.apache.org/jira/browse/HADOOP-10583) | *Minor* | **bin/hadoop key throws NPE with no args and assorted other fixups**

bin/hadoop key
with no args would throw an NPE.


---

* [HDFS-2856](https://issues.apache.org/jira/browse/HDFS-2856) | *Major* | **Fix block protocol so that Datanodes don't require root or jsvc**

SASL now can be used to secure the DataTransferProtocol, which transfers file block content between HDFS clients and DataNodes.  In this configuration, it is no longer required for secured clusters to start the DataNode as root and bind to privileged ports.


---

* [HADOOP-10839](https://issues.apache.org/jira/browse/HADOOP-10839) | *Major* | **Add unregisterSource() to MetricsSystem API**

The MetricsSystem abstract class has added a new abstract method, unregisterSource, for unregistering a previously registered metrics source.  Custom subclasses of MetricsSystem must be updated to provide an implementation of this method.


---

* [HADOOP-8069](https://issues.apache.org/jira/browse/HADOOP-8069) | *Major* | **Enable TCP\_NODELAY by default for IPC**

This change enables the TCP\_NODELAY flag for all Hadoop IPC connections, hence bypassing TCP Nagling. Nagling interacts poorly with TCP delayed ACKs especially for request-response protocols.


---

* [HDFS-6482](https://issues.apache.org/jira/browse/HDFS-6482) | *Major* | **Use block ID-based block layout on datanodes**

The directory structure for finalized replicas on DNs has been changed. Now, the directory that a finalized replica goes in is determined uniquely by its ID. Specifically, we use a two-level directory structure, with the 24th through 17th bits identifying the correct directory at the first level and the 16th through 8th bits identifying the correct directory at the second level.


---

* [HADOOP-10903](https://issues.apache.org/jira/browse/HADOOP-10903) | *Major* | **Enhance hadoop classpath command to expand wildcards or write classpath into jar manifest.**

The "hadoop classpath" command has been enhanced to support options for automatic expansion of wildcards in classpath elements and writing the classpath to a jar file manifest.  These options make it easier to construct a correct classpath for libhdfs applications.


---

* [HDFS-573](https://issues.apache.org/jira/browse/HDFS-573) | *Major* | **Porting libhdfs to Windows**

The libhdfs C API is now supported on Windows.


---

* [HADOOP-8944](https://issues.apache.org/jira/browse/HADOOP-8944) | *Trivial* | **Shell command fs -count should include human readable option**

Implements -h option for fs -count to show file sizes in human readable format. Additionally, ContentSummary.getHeader() now returns a different string that is incompatible with previous releases.


---

* [HDFS-6376](https://issues.apache.org/jira/browse/HDFS-6376) | *Major* | **Distcp data between two HA clusters requires another configuration**

Allow distcp to copy data between HA clusters. Users can use a new configuration property "dfs.internal.nameservices" to explicitly specify the name services belonging to the local cluster, while continue using the configuration property "dfs.nameservices" to specify all the name services in the local and remote clusters.


---

* [YARN-668](https://issues.apache.org/jira/browse/YARN-668) | *Blocker* | **TokenIdentifier serialization should consider Unknown fields**

**WARNING: No release note provided for this change.**


---

* [YARN-1051](https://issues.apache.org/jira/browse/YARN-1051) | *Major* | **YARN Admission Control/Planner: enhancing the resource allocation model with time.**

**WARNING: No release note provided for this change.**


---

* [HADOOP-10681](https://issues.apache.org/jira/browse/HADOOP-10681) | *Major* | **Remove synchronized blocks from SnappyCodec and ZlibCodec buffering inner loop**

Remove unnecessary synchronized blocks from Snappy/Zlib codecs.


---

* [YARN-2615](https://issues.apache.org/jira/browse/YARN-2615) | *Blocker* | **ClientToAMTokenIdentifier and DelegationTokenIdentifier should allow extended fields**

**WARNING: No release note provided for this change.**


---

* [HDFS-6606](https://issues.apache.org/jira/browse/HDFS-6606) | *Major* | **Optimize HDFS Encrypted Transport performance**

HDFS now supports the option to configure AES encryption for block data transfer.  AES offers improved cryptographic strength and performance over the prior options of 3DES and RC4.


---

* [HDFS-7276](https://issues.apache.org/jira/browse/HDFS-7276) | *Major* | **Limit the number of byte arrays used by DFSOutputStream**

The following configuration properties are added.

- dfs.client.write.byte-array-manager.enabled:
for enabling/disabling byte array manger.  Default is false.

- dfs.client.write.byte-array-manager.count-threshold:
The count threshold for each array length so that a manager is created only after the allocation count exceeds the threshold.  In other words, the particular array length is not managed until the allocation count exceeds the threshold.  Default is 128.

- dfs.client.write.byte-array-manager.count-limit:
The maximum number of arrays allowed for each array length.  Default is 2048.

- dfs.client.write.byte-array-manager.count-reset-time-period-ms:
The time period in milliseconds that the allocation count for each array length is reset to zero if there is no increment.  Default is 10,000ms, i.e. 10 seconds.



