
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
# Apache Hadoop  2.4.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-5790](https://issues.apache.org/jira/browse/HDFS-5790) | *Major* | **LeaseManager.findPath is very slow when many leases need recovery**

Committed to branch-2 and trunk.


---

* [HADOOP-10295](https://issues.apache.org/jira/browse/HADOOP-10295) | *Major* | **Allow distcp to automatically identify the checksum type of source files and use it for the target**

Add option for distcp to preserve the checksum type of the source files. Users can use "-pc" as distcp command option to preserve the checksum type.


---

* [HDFS-5804](https://issues.apache.org/jira/browse/HDFS-5804) | *Major* | **HDFS NFS Gateway fails to mount and proxy when using Kerberos**

Fixes NFS on Kerberized cluster.


---

* [HDFS-5698](https://issues.apache.org/jira/browse/HDFS-5698) | *Major* | **Use protobuf to serialize / deserialize FSImage**

Use protobuf to serialize/deserialize the FSImage.


---

* [HDFS-4370](https://issues.apache.org/jira/browse/HDFS-4370) | *Major* | **Fix typo Blanacer in DataNode**

I just committed this. Thank you Chu.


---

* [HDFS-5776](https://issues.apache.org/jira/browse/HDFS-5776) | *Major* | **Support 'hedged' reads in DFSClient**

If a read from a block is slow, start up another parallel, 'hedged' read against a different block replica.  We then take the result of which ever read returns first (the outstanding read is cancelled).  This 'hedged' read feature will help rein in the outliers, the odd read that takes a long time because it hit a bad patch on the disc, etc.

This feature is off by default.  To enable this feature, set \<code\>dfs.client.hedged.read.threadpool.size\</code\> to a positive number.  The threadpool size is how many threads to dedicate to the running of these 'hedged', concurrent reads in your client.

Then set \<code\>dfs.client.hedged.read.threshold.millis\</code\> to the number of milliseconds to wait before starting up a 'hedged' read.  For example, if you set this property to 10, then if a read has not returned within 10 milliseconds, we will start up a new read against a different block replica.

This feature emits new metrics:

+ hedgedReadOps
+ hedgeReadOpsWin -- how many times the hedged read 'beat' the original read
+ hedgedReadOpsInCurThread -- how many times we went to do a hedged read but we had to run it in the current thread because dfs.client.hedged.read.threadpool.size was at a maximum.


---

* [HADOOP-8691](https://issues.apache.org/jira/browse/HADOOP-8691) | *Minor* | **FsShell can print "Found xxx items" unnecessarily often**

The \`ls\` command only prints "Found foo items" once when listing the directories recursively.


---

* [HDFS-5321](https://issues.apache.org/jira/browse/HDFS-5321) | *Major* | **Clean up the HTTP-related configuration in HDFS**

dfs.http.port and dfs.https.port are removed. Filesystem clients, such as WebHdfsFileSystem, now have fixed instead of configurable default ports (i.e., 50070 for http and 50470 for https).

Users can explicitly specify the port in the URI to access the file system which runs on non-default ports.


---

* [HADOOP-10211](https://issues.apache.org/jira/browse/HADOOP-10211) | *Major* | **Enable RPC protocol to negotiate SASL-QOP values between clients and servers**

The hadoop.rpc.protection configuration property previously supported specifying a single value: one of authentication, integrity or privacy.  An unrecognized value was silently assumed to mean authentication.  This configuration property now accepts a comma-separated list of any of the 3 values, and unrecognized values are rejected with an error. Existing configurations containing an invalid value must be corrected. If the property is empty or not specified, authentication is assumed.


---

* [HDFS-6055](https://issues.apache.org/jira/browse/HDFS-6055) | *Major* | **Change default configuration to limit file name length in HDFS**

The default configuration of HDFS now sets dfs.namenode.fs-limits.max-component-length to 255 for improved interoperability with other file system implementations.  This limits each component of a file system path to a maximum of 255 bytes in UTF-8 encoding.  Attempts to create new files that violate this rule will fail with an error.  Existing files that violate the rule are not effected.  Previously, dfs.namenode.fs-limits.max-component-length was set to 0 (ignored).  If necessary, it is possible to set the value back to 0 in the cluster's configuration to restore the old behavior.


---

* [HDFS-6102](https://issues.apache.org/jira/browse/HDFS-6102) | *Blocker* | **Lower the default maximum items per directory to fix PB fsimage loading**

**WARNING: No release note provided for this change.**


---

* [HADOOP-10221](https://issues.apache.org/jira/browse/HADOOP-10221) | *Major* | **Add a plugin to specify SaslProperties for RPC protocol based on connection properties**

SaslPropertiesResolver  or its subclass is used to resolve the QOP used for a connection. The subclass can be specified via "hadoop.security.saslproperties.resolver.class" configuration property. If not specified, the full set of values specified in hadoop.rpc.protection is used while determining the QOP used for the  connection. If a class is specified, then the QOP values returned by the class will be used while determining the QOP used for the connection.

Note that this change, effectively removes SaslRpcServer.SASL\_PROPS which was a public field. Any use of this variable  should be replaced with the following code:
SaslPropertiesResolver saslPropsResolver = SaslPropertiesResolver.getInstance(conf);
Map\<String, String\> sasl\_props = saslPropsResolver.getDefaultProperties();


---

* [HDFS-4685](https://issues.apache.org/jira/browse/HDFS-4685) | *Major* | **Implementation of ACLs in HDFS**

HDFS now supports ACLs (Access Control Lists).  ACLs can specify fine-grained file permissions for specific named users or named groups.


---

* [HDFS-5138](https://issues.apache.org/jira/browse/HDFS-5138) | *Blocker* | **Support HDFS upgrade in HA**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-5036](https://issues.apache.org/jira/browse/MAPREDUCE-5036) | *Major* | **Default shuffle handler port should not be 8080**

**WARNING: No release note provided for this change.**



