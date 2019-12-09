
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
# Apache Hadoop  0.20.205.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-630](https://issues.apache.org/jira/browse/HDFS-630) | *Major* | **In DFSOutputStream.nextBlockOutputStream(), the client can exclude specific datanodes when locating the next block.**

**WARNING: No release note provided for this change.**


---

* [HDFS-1554](https://issues.apache.org/jira/browse/HDFS-1554) | *Major* | **Append 0.20: New semantics for recoverLease**

Change recoverLease API to return if the file is closed or not. It also change the semantics of recoverLease to start lease recovery immediately.


---

* [HDFS-2202](https://issues.apache.org/jira/browse/HDFS-2202) | *Major* | **Changes to balancer bandwidth should not require datanode restart.**

New dfsadmin command added: [-setBalancerBandwidth \<bandwidth\>] where bandwidth is max network bandwidth in bytes per second that the balancer is allowed to use on each datanode during balacing.

This is an incompatible change in 0.23.  The versions of ClientProtocol and DatanodeProtocol are changed.


---

* [MAPREDUCE-2494](https://issues.apache.org/jira/browse/MAPREDUCE-2494) | *Major* | **Make the distributed cache delete entires using LRU priority**

Added config option mapreduce.tasktracker.cache.local.keep.pct to the TaskTracker.  It is the target percentage of the local distributed cache that should be kept in between garbage collection runs.  In practice it will delete unused distributed cache entries in LRU order until the size of the cache is less than mapreduce.tasktracker.cache.local.keep.pct of the maximum cache size.  This is a floating point value between 0.0 and 1.0.  The default is 0.95.


---

* [MAPREDUCE-2187](https://issues.apache.org/jira/browse/MAPREDUCE-2187) | *Major* | **map tasks timeout during sorting**

I just committed this. Thanks Anupam!


---

* [HADOOP-7119](https://issues.apache.org/jira/browse/HADOOP-7119) | *Major* | **add Kerberos HTTP SPNEGO authentication support to Hadoop JT/NN/DN/TT web-consoles**

Adding support for Kerberos HTTP SPNEGO authentication to the Hadoop web-consoles


---

* [HDFS-2338](https://issues.apache.org/jira/browse/HDFS-2338) | *Major* | **Configuration option to enable/disable webhdfs.**

Added a conf property dfs.webhdfs.enabled for enabling/disabling webhdfs.


---

* [HDFS-2318](https://issues.apache.org/jira/browse/HDFS-2318) | *Major* | **Provide authentication to webhdfs using SPNEGO**

Added two new conf properties dfs.web.authentication.kerberos.principal and dfs.web.authentication.kerberos.keytab for the SPNEGO servlet filter.


---

* [MAPREDUCE-3081](https://issues.apache.org/jira/browse/MAPREDUCE-3081) | *Major* | **Change the name format for hadoop core and vaidya jar to be hadoop-{core/vaidya}-{version}.jar in vaidya.sh**

contrib/vaidya/bin/vaidya.sh script fixed to use appropriate jars and classpath


---

* [HADOOP-7691](https://issues.apache.org/jira/browse/HADOOP-7691) | *Major* | **hadoop deb pkg should take a diff group id**

Fixed conflict uid for install packages. (Eric Yang)


---

* [HADOOP-7603](https://issues.apache.org/jira/browse/HADOOP-7603) | *Major* | **Set default hdfs, mapred uid, and hadoop group gid for RPM packages**

Set hdfs uid, mapred uid, and hadoop gid to fixed numbers (201, 202, and 123, respectively).


---

* [HADOOP-7684](https://issues.apache.org/jira/browse/HADOOP-7684) | *Major* | **jobhistory server and secondarynamenode should have init.d script**

Added init.d script for jobhistory server and secondary namenode. (Eric Yang)


---

* [MAPREDUCE-3112](https://issues.apache.org/jira/browse/MAPREDUCE-3112) | *Major* | **Calling hadoop cli inside mapreduce job leads to errors**

Removed inheritance of certain server environment variables (HADOOP\_OPTS and HADOOP\_ROOT\_LOGGER) in task attempt process.


---

* [HADOOP-7715](https://issues.apache.org/jira/browse/HADOOP-7715) | *Major* | **see log4j Error when running mr jobs and certain dfs calls**

Removed unnecessary security logger configuration. (Eric Yang)


---

* [HADOOP-7711](https://issues.apache.org/jira/browse/HADOOP-7711) | *Major* | **hadoop-env.sh generated from templates has duplicate info**

Fixed recursive sourcing of HADOOP\_OPTS environment variables (Arpit Gupta via Eric Yang)


---

* [HADOOP-7681](https://issues.apache.org/jira/browse/HADOOP-7681) | *Minor* | **log4j.properties is missing properties for security audit and hdfs audit should be changed to info**

HADOOP-7681. Fixed security and hdfs audit log4j properties
(Arpit Gupta via Eric Yang)


---

* [HADOOP-7708](https://issues.apache.org/jira/browse/HADOOP-7708) | *Critical* | **config generator does not update the properties file if on exists already**

Fixed hadoop-setup-conf.sh to handle config file consistently.  (Eric Yang)


---

* [HADOOP-7707](https://issues.apache.org/jira/browse/HADOOP-7707) | *Major* | **improve config generator to allow users to specify proxy user, turn append on or off, turn webhdfs on or off**

Added toggle for dfs.support.append, webhdfs and hadoop proxy user to setup config script. (Arpit Gupta via Eric Yang)


---

* [HADOOP-7720](https://issues.apache.org/jira/browse/HADOOP-7720) | *Major* | **improve the hadoop-setup-conf.sh to read in the hbase user and setup the configs**

Added parameter for HBase user to setup config script. (Arpit Gupta via Eric Yang)


---

* [MAPREDUCE-2777](https://issues.apache.org/jira/browse/MAPREDUCE-2777) | *Major* | **Backport MAPREDUCE-220 to Hadoop 20 security branch**

Adds cumulative cpu usage and total heap usage to task counters. This is a backport of MAPREDUCE-220 and MAPREDUCE-2469.


---

* [HDFS-2358](https://issues.apache.org/jira/browse/HDFS-2358) | *Major* | **NPE when the default filesystem's uri has no authority**

Give meaningful error message instead of NPE.


---

* [HADOOP-7724](https://issues.apache.org/jira/browse/HADOOP-7724) | *Major* | **hadoop-setup-conf.sh should put proxy user info into the core-site.xml**

Fixed hadoop-setup-conf.sh to put proxy user in core-site.xml.  (Arpit Gupta via Eric Yang)


---

* [MAPREDUCE-2764](https://issues.apache.org/jira/browse/MAPREDUCE-2764) | *Major* | **Fix renewal of dfs delegation tokens**

Generalizes token renewal and canceling to a common interface and provides a plugin interface for adding renewers for new kinds of tokens. Hftp changed to store the tokens as HFTP and renew them over http.


---

* [HADOOP-7655](https://issues.apache.org/jira/browse/HADOOP-7655) | *Major* | **provide a small validation script that smoke tests the installed cluster**

Committed to trunk and v23, since code reviewed by Eric.



