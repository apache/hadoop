
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
# Apache Hadoop  2.0.0-alpha Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-395](https://issues.apache.org/jira/browse/HDFS-395) | *Major* | **DFS Scalability: Incremental block reports**

**WARNING: No release note provided for this change.**


---

* [HADOOP-7524](https://issues.apache.org/jira/browse/HADOOP-7524) | *Major* | **Change RPC to allow multiple protocols including multiple versions of the same protocol**

**WARNING: No release note provided for this change.**


---

* [HADOOP-7704](https://issues.apache.org/jira/browse/HADOOP-7704) | *Minor* | **JsonFactory can be created only once and used for every next request to create JsonGenerator inside JMXJsonServlet**

Reduce number of object created by JMXJsonServlet. (Devaraj K via Eric Yang)


---

* [MAPREDUCE-3818](https://issues.apache.org/jira/browse/MAPREDUCE-3818) | *Blocker* | **Trunk MRV1 compilation is broken.**

Fixed broken compilation in TestSubmitJob after the patch for HDFS-2895.


---

* [HDFS-2731](https://issues.apache.org/jira/browse/HDFS-2731) | *Major* | **HA: Autopopulate standby name dirs if they're empty**

The HA NameNode may now be started with the "-bootstrapStandby" flag. This causes it to copy the namespace information and most recent checkpoint from its HA pair, and save it to local storage, allowing an HA setup to be bootstrapped without use of rsync or external tools.


---

* [HDFS-2303](https://issues.apache.org/jira/browse/HDFS-2303) | *Major* | **Unbundle jsvc**

To run secure Datanodes users must install jsvc for their platform and set JSVC\_HOME to point to the location of jsvc in their environment.


---

* [HDFS-3044](https://issues.apache.org/jira/browse/HDFS-3044) | *Major* | **fsck move should be non-destructive by default**

The fsck "move" option is no longer destructive. It copies the accessible blocks of corrupt files to lost and found as before, but no longer deletes the corrupt files after copying the blocks. The original, destructive behavior can be enabled by specifying both the "move" and "delete" options.


---

* [HADOOP-8154](https://issues.apache.org/jira/browse/HADOOP-8154) | *Major* | **DNS#getIPs shouldn't silently return the local host IP for bogus interface names**

**WARNING: No release note provided for this change.**


---

* [HADOOP-8184](https://issues.apache.org/jira/browse/HADOOP-8184) | *Major* | **ProtoBuf RPC engine does not need it own reply packet - it can use the IPC layer reply packet.**

This change will affect the output of errors for some Hadoop CLI commands. Specifically, the name of the exception class will no longer appear, and instead only the text of the exception message will appear.


---

* [HADOOP-8149](https://issues.apache.org/jira/browse/HADOOP-8149) | *Major* | **cap space usage of default log4j rolling policy**

Hadoop log files are now rolled by size instead of date (daily) by default. Tools that depend on the log file name format will need to be updated. Users who would like to maintain the previous settings of hadoop.root.logger and hadoop.security.logger can use their current log4j.properties files and update the HADOOP\_ROOT\_LOGGER and HADOOP\_SECURITY\_LOGGER environment variables to use DRFA and DRFAS respectively.


---

* [HDFS-3137](https://issues.apache.org/jira/browse/HDFS-3137) | *Major* | **Bump LAST\_UPGRADABLE\_LAYOUT\_VERSION to -16**

Upgrade from Hadoop versions earlier than 0.18 is not supported as of 2.0. To upgrade from an earlier release, first upgrade to 0.18, and then upgrade again from there.


---

* [HDFS-3138](https://issues.apache.org/jira/browse/HDFS-3138) | *Major* | **Move DatanodeInfo#ipcPort to DatanodeID**

This change modifies DatanodeID, which is part of the client to server protocol, therefore clients must be upgraded with servers.


---

* [HDFS-3164](https://issues.apache.org/jira/browse/HDFS-3164) | *Major* | **Move DatanodeInfo#hostName to DatanodeID**

This change modifies DatanodeID, which is part of the client to server protocol, therefore clients must be upgraded with servers.


---

* [HDFS-3144](https://issues.apache.org/jira/browse/HDFS-3144) | *Major* | **Refactor DatanodeID#getName by use**

This change modifies DatanodeID, which is part of the client to server protocol, therefore clients must be upgraded with servers.


---

* [HDFS-3004](https://issues.apache.org/jira/browse/HDFS-3004) | *Major* | **Implement Recovery Mode**

This is a new feature.  It is documented in hdfs\_user\_guide.xml.


---

* [HADOOP-8270](https://issues.apache.org/jira/browse/HADOOP-8270) | *Minor* | **hadoop-daemon.sh stop action should return 0 for an already stopped service**

The daemon stop action no longer returns failure when stopping an already stopped service.


---

* [HDFS-3094](https://issues.apache.org/jira/browse/HDFS-3094) | *Major* | **add -nonInteractive and -force option to namenode -format command**

The 'namenode -format' command now supports the flags '-nonInteractive' and '-force' to improve usefulness without user input.


---

* [HADOOP-8314](https://issues.apache.org/jira/browse/HADOOP-8314) | *Major* | **HttpServer#hasAdminAccess should return false if authorization is enabled but user is not authenticated**

**WARNING: No release note provided for this change.**


---

* [HDFS-3286](https://issues.apache.org/jira/browse/HDFS-3286) | *Major* | **When the threshold value for balancer is 0(zero) ,unexpected output is displayed**

**WARNING: No release note provided for this change.**



