
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
# Apache Hadoop  1.0.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-617](https://issues.apache.org/jira/browse/HDFS-617) | *Major* | **Support for non-recursive create() in HDFS**

New DFSClient.create(...) allows option of not creating missing parent(s).


---

* [HADOOP-7728](https://issues.apache.org/jira/browse/HADOOP-7728) | *Major* | **hadoop-setup-conf.sh should be modified to enable task memory manager**

Enable task memory management to be configurable via hadoop config setup script.


---

* [HADOOP-7740](https://issues.apache.org/jira/browse/HADOOP-7740) | *Minor* | **security audit logger is not on by default, fix the log4j properties to enable the logger**

Fixed security audit logger configuration. (Arpit Gupta via Eric Yang)


---

* [HDFS-2246](https://issues.apache.org/jira/browse/HDFS-2246) | *Major* | **Shortcut a local client reads to a Datanodes files directly**

1. New configurations
a. dfs.block.local-path-access.user is the key in datanode configuration to specify the user allowed to do short circuit read.
b. dfs.client.read.shortcircuit is the key to enable short circuit read at the client side configuration.
c. dfs.client.read.shortcircuit.skip.checksum is the key to bypass checksum check at the client side.
2. By default none of the above are enabled and short circuit read will not kick in.
3. If security is on, the feature can be used only for user that has kerberos credentials at the client, therefore map reduce tasks cannot benefit from it in general.


---

* [HADOOP-7923](https://issues.apache.org/jira/browse/HADOOP-7923) | *Major* | **Automatically update doc versions**

Docs version number is now automatically updated by reference to the build number.


---

* [HDFS-2316](https://issues.apache.org/jira/browse/HDFS-2316) | *Major* | **[umbrella] WebHDFS: a complete FileSystem implementation for accessing HDFS over HTTP**

Provide WebHDFS as a complete FileSystem implementation for accessing HDFS over HTTP.
Previous hftp feature was a read-only FileSystem and does not provide "write" accesses.



