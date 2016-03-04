
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
# Apache Hadoop Changelog

## Release 0.20.203.0 - 2011-05-11

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7108](https://issues.apache.org/jira/browse/HADOOP-7108) | hadoop-0.20.100 |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-4343](https://issues.apache.org/jira/browse/HADOOP-4343) | Adding user and service-to-service authentication to Hadoop |  Blocker | . | Kan Zhang | Kan Zhang |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7247](https://issues.apache.org/jira/browse/HADOOP-7247) | Fix documentation to reflect new jar names |  Major | . | Owen O'Malley | Owen O'Malley |
| [HDFS-1626](https://issues.apache.org/jira/browse/HDFS-1626) | Make BLOCK\_INVALIDATE\_LIMIT configurable |  Minor | namenode | Arun C Murthy | Tsz Wo Nicholas Sze |
| [HDFS-457](https://issues.apache.org/jira/browse/HDFS-457) | better handling of volume failure in Data Node storage |  Major | datanode | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-2355](https://issues.apache.org/jira/browse/MAPREDUCE-2355) | Add an out of band heartbeat damper |  Major | jobtracker | Owen O'Malley | Arun C Murthy |
| [MAPREDUCE-2316](https://issues.apache.org/jira/browse/MAPREDUCE-2316) | Update docs for CapacityScheduler |  Major | capacity-sched, documentation | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-1943](https://issues.apache.org/jira/browse/MAPREDUCE-1943) | Implement limits on per-job JobConf, Counters, StatusReport, Split-Sizes |  Major | . | Mahadev konar | Mahadev konar |
| [MAPREDUCE-478](https://issues.apache.org/jira/browse/MAPREDUCE-478) | separate jvm param for mapper and reducer |  Minor | . | Koji Noguchi | Arun C Murthy |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7259](https://issues.apache.org/jira/browse/HADOOP-7259) | contrib modules should include build.properties from parent. |  Major | build | Owen O'Malley | Owen O'Malley |
| [HADOOP-7258](https://issues.apache.org/jira/browse/HADOOP-7258) | Gzip codec should not return null decompressors |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-7253](https://issues.apache.org/jira/browse/HADOOP-7253) | Fix default config |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-7246](https://issues.apache.org/jira/browse/HADOOP-7246) | The default log4j configuration causes warnings about EventCounter |  Major | . | Owen O'Malley | Luke Lu |
| [HADOOP-7243](https://issues.apache.org/jira/browse/HADOOP-7243) | Fix contrib unit tests (fairshare, hdfsproxy, datajoin, streaming) |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-7232](https://issues.apache.org/jira/browse/HADOOP-7232) | Fix javadoc warnings |  Blocker | documentation | Owen O'Malley | Owen O'Malley |
| [HADOOP-7215](https://issues.apache.org/jira/browse/HADOOP-7215) | RPC clients must connect over a network interface corresponding to the host name in the client's kerberos principal key |  Blocker | security | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7190](https://issues.apache.org/jira/browse/HADOOP-7190) | Put metrics v1 back into the hadoop-20-security branch |  Major | metrics | Owen O'Malley | Owen O'Malley |
| [HADOOP-7163](https://issues.apache.org/jira/browse/HADOOP-7163) | "java.net.SocketTimeoutException: 60000 millis timeout" happens a lot |  Major | ipc | Owen O'Malley | Devaraj Das |
| [HADOOP-7143](https://issues.apache.org/jira/browse/HADOOP-7143) | Hive Hadoop20SShims depends on removed HadoopArchives |  Major | fs | Joep Rottinghuis | Joep Rottinghuis |
| [HADOOP-7040](https://issues.apache.org/jira/browse/HADOOP-7040) | DiskChecker:mkdirsWithExistsCheck swallows FileNotFoundException. |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6907](https://issues.apache.org/jira/browse/HADOOP-6907) | Rpc client doesn't use the per-connection conf to figure out server's Kerberos principal |  Major | ipc, security | Kan Zhang | Kan Zhang |
| [HADOOP-5647](https://issues.apache.org/jira/browse/HADOOP-5647) | TestJobHistory fails if /tmp/\_logs is not writable to. Testcase should not depend on /tmp |  Major | test | Ravi Gummadi | Ravi Gummadi |
| [HDFS-1822](https://issues.apache.org/jira/browse/HDFS-1822) | Editlog opcodes overlap between 20 security and later releases |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1022](https://issues.apache.org/jira/browse/HDFS-1022) | Merge under-10-min tests specs into one file |  Major | test | Erik Steffl | Erik Steffl |
| [MAPREDUCE-2365](https://issues.apache.org/jira/browse/MAPREDUCE-2365) | Add counters for FileInputFormat (BYTES\_READ) and FileOutputFormat (BYTES\_WRITTEN) |  Major | . | Owen O'Malley | Siddharth Seth |
| [MAPREDUCE-2278](https://issues.apache.org/jira/browse/MAPREDUCE-2278) | DistributedCache shouldn't hold a ref to JobConf |  Major | distributed-cache, tasktracker | Arun C Murthy | Chris Douglas |
| [MAPREDUCE-1699](https://issues.apache.org/jira/browse/MAPREDUCE-1699) | JobHistory shouldn't be disabled for any reason |  Major | jobtracker | Arun C Murthy | Krishna Ramachandran |
| [MAPREDUCE-1280](https://issues.apache.org/jira/browse/MAPREDUCE-1280) | Eclipse Plugin does not work with Eclipse Ganymede (3.4) |  Major | . | Aaron Kimball | Alex Kozlov |
| [MAPREDUCE-1233](https://issues.apache.org/jira/browse/MAPREDUCE-1233) | Incorrect Waiting maps/reduces in Jobtracker metrics |  Major | jobtracker | V.Karthikeyan | Luke Lu |
| [MAPREDUCE-1118](https://issues.apache.org/jira/browse/MAPREDUCE-1118) | Capacity Scheduler scheduling information is hard to read / should be tabular format |  Major | capacity-sched | Allen Wittenauer | Krishna Ramachandran |
| [MAPREDUCE-323](https://issues.apache.org/jira/browse/MAPREDUCE-323) | Improve the way job history files are managed |  Critical | jobtracker | Amar Kamat | Dick King |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


