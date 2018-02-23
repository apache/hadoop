
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
# Apache Hadoop  2.5.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-10342](https://issues.apache.org/jira/browse/HADOOP-10342) | *Major* | **Extend UserGroupInformation to return a UGI given a preauthenticated kerberos Subject**

Add getUGIFromSubject to leverage an external kerberos authentication


---

* [HDFS-6164](https://issues.apache.org/jira/browse/HDFS-6164) | *Major* | **Remove lsr in OfflineImageViewer**

The offlineimageviewer no longer generates lsr-style outputs. The functionality has been superseded by a tool that takes the fsimage and exposes WebHDFS-like API for user queries.


---

* [HDFS-6168](https://issues.apache.org/jira/browse/HDFS-6168) | *Major* | **Remove deprecated methods in DistributedFileSystem**

**WARNING: No release note provided for this change.**


---

* [HADOOP-10451](https://issues.apache.org/jira/browse/HADOOP-10451) | *Trivial* | **Remove unused field and imports from SaslRpcServer**

SaslRpcServer.SASL\_PROPS is removed.
Any use of this variable  should be replaced with the following code: 
SaslPropertiesResolver saslPropsResolver = SaslPropertiesResolver.getInstance(conf); 
Map\<String, String\> sasl\_props = saslPropsResolver.getDefaultProperties();


---

* [HDFS-6153](https://issues.apache.org/jira/browse/HDFS-6153) | *Minor* | **Document "fileId" and "childrenNum" fields in the FileStatus Json schema**

**WARNING: No release note provided for this change.**


---

* [HADOOP-9919](https://issues.apache.org/jira/browse/HADOOP-9919) | *Major* | **Update hadoop-metrics2.properties examples to Yarn**

Remove MRv1 settings from hadoop-metrics2.properties, add YARN settings instead.


---

* [HDFS-6273](https://issues.apache.org/jira/browse/HDFS-6273) | *Major* | **Config options to allow wildcard endpoints for namenode HTTP and HTTPS servers**

HDFS-6273 introduces two new HDFS configuration keys: 
- dfs.namenode.http-bind-host
- dfs.namenode.https-bind-host

The most common use case for these keys is to have the NameNode HTTP (or HTTPS) endpoints listen on all interfaces on multi-homed systems by setting the keys to 0.0.0.0 i.e. INADDR\_ANY.

For the systems background on this usage of INADDR\_ANY please refer to ip(7) in the Linux Programmer's Manual (web link: http://man7.org/linux/man-pages/man7/ip.7.html).

These keys complement the existing NameNode options:
- dfs.namenode.rpc-bind-host
- dfs.namenode.servicerpc-bind-host


---

* [HADOOP-10568](https://issues.apache.org/jira/browse/HADOOP-10568) | *Major* | **Add s3 server-side encryption**

s3 server-side encryption is now supported.

To enable this feature, specify the following in your client-side configuration:

name: fs.s3n.server-side-encryption-algorithm
value: AES256


---

* [HDFS-6293](https://issues.apache.org/jira/browse/HDFS-6293) | *Blocker* | **Issues with OIV processing PB-based fsimages**

Set "dfs.namenode.legacy-oiv-image.dir" to an appropriate directory to make standby name node or secondary name node save its file system state in the old fsimage format during checkpointing. This image can be used for offline analysis using the OfflineImageViewer.  Use the "hdfs oiv\_legacy" command to process the old fsimage format.


---

* [HDFS-6110](https://issues.apache.org/jira/browse/HDFS-6110) | *Major* | **adding more slow action log in critical write path**

Log slow i/o.  Set log thresholds in dfsclient and datanode via the below  new configs:

dfs.client.slow.io.warning.threshold.ms (Default 30 seconds)
dfs.datanode.slow.io.warning.threshold.ms (Default 300ms)


---

* [YARN-2107](https://issues.apache.org/jira/browse/YARN-2107) | *Major* | **Refactor timeline classes into server.timeline package**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-5777](https://issues.apache.org/jira/browse/MAPREDUCE-5777) | *Major* | **Support utf-8 text with BOM (byte order marker)**

**WARNING: No release note provided for this change.**



