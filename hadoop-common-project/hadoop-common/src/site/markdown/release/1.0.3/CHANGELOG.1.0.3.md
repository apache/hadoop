
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

## Release 1.0.3 - 2012-05-07



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5528](https://issues.apache.org/jira/browse/HADOOP-5528) | Binary partitioner |  Major | . | Klaas Bosteels | Klaas Bosteels |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8027](https://issues.apache.org/jira/browse/HADOOP-8027) | Visiting /jmx on the daemon web interfaces may print unnecessary error in logs |  Minor | metrics | Harsh J | Aaron T. Myers |
| [HADOOP-8188](https://issues.apache.org/jira/browse/HADOOP-8188) | Fix the build process to do with jsvc, with IBM's JDK as the underlying jdk |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-8352](https://issues.apache.org/jira/browse/HADOOP-8352) | We should always generate a new configure script for the c++ code |  Major | . | Owen O'Malley | Owen O'Malley |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-119](https://issues.apache.org/jira/browse/HDFS-119) | logSync() may block NameNode forever. |  Major | namenode | Konstantin Shvachko | Suresh Srinivas |
| [HDFS-1041](https://issues.apache.org/jira/browse/HDFS-1041) | DFSClient does not retry in getFileChecksum(..) |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-6924](https://issues.apache.org/jira/browse/HADOOP-6924) | Build fails with non-Sun JREs due to different pathing to the operating system architecture shared libraries |  Major | . | Stephen Watt | Devaraj Das |
| [HADOOP-6941](https://issues.apache.org/jira/browse/HADOOP-6941) | Support non-SUN JREs in UserGroupInformation |  Major | . | Stephen Watt | Devaraj Das |
| [HDFS-3127](https://issues.apache.org/jira/browse/HDFS-3127) | failure in recovering removed storage directories should not stop checkpoint process |  Major | namenode | Brandon Li | Brandon Li |
| [MAPREDUCE-3377](https://issues.apache.org/jira/browse/MAPREDUCE-3377) | Compatibility issue with 0.20.203. |  Major | . | Jane Chen | Jane Chen |
| [HADOOP-8251](https://issues.apache.org/jira/browse/HADOOP-8251) | SecurityUtil.fetchServiceTicket broken after HADOOP-6941 |  Blocker | security | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-1238](https://issues.apache.org/jira/browse/MAPREDUCE-1238) | mapred metrics shows negative count of waiting maps and reduces |  Major | jobtracker | Ramya Sunil | Thomas Graves |
| [MAPREDUCE-4003](https://issues.apache.org/jira/browse/MAPREDUCE-4003) | log.index (No such file or directory) AND Task process exit with nonzero status of 126 |  Major | task-controller, tasktracker | toughman | Koji Noguchi |
| [MAPREDUCE-4154](https://issues.apache.org/jira/browse/MAPREDUCE-4154) | streaming MR job succeeds even if the streaming command fails |  Major | . | Thejas M Nair | Devaraj Das |
| [HADOOP-8293](https://issues.apache.org/jira/browse/HADOOP-8293) | The native library's Makefile.am doesn't include JNI path |  Major | build | Owen O'Malley | Owen O'Malley |
| [HADOOP-8294](https://issues.apache.org/jira/browse/HADOOP-8294) | IPC Connection becomes unusable even if server address was temporarilly unresolvable |  Critical | ipc | Kihwal Lee | Kihwal Lee |
| [HDFS-3310](https://issues.apache.org/jira/browse/HDFS-3310) | Make sure that we abort when no edit log directories are left |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-4207](https://issues.apache.org/jira/browse/MAPREDUCE-4207) | Remove System.out.println() in FileInputFormat |  Major | mrv1 | Kihwal Lee | Kihwal Lee |
| [HDFS-3265](https://issues.apache.org/jira/browse/HDFS-3265) | PowerPc Build error. |  Major | build | Kumar Ravi | Kumar Ravi |
| [HADOOP-8338](https://issues.apache.org/jira/browse/HADOOP-8338) | Can't renew or cancel HDFS delegation tokens over secure RPC |  Major | security | Owen O'Malley | Owen O'Malley |
| [HADOOP-8346](https://issues.apache.org/jira/browse/HADOOP-8346) | Changes to support Kerberos with non Sun JVM (HADOOP-6941) broke SPNEGO |  Blocker | security | Alejandro Abdelnur | Devaraj Das |
| [HDFS-3061](https://issues.apache.org/jira/browse/HDFS-3061) | Backport HDFS-1487 to branch-1 |  Blocker | namenode | Alex Holmes | Kihwal Lee |
| [HADOOP-7381](https://issues.apache.org/jira/browse/HADOOP-7381) | FindBugs OutOfMemoryError |  Major | build | Joep Rottinghuis | Joep Rottinghuis |
| [MAPREDUCE-3857](https://issues.apache.org/jira/browse/MAPREDUCE-3857) | Grep example ignores mapred.job.queue.name |  Major | examples | Jonathan Eagles | Jonathan Eagles |
| [HDFS-3374](https://issues.apache.org/jira/browse/HDFS-3374) | hdfs' TestDelegationToken fails intermittently with a race condition |  Major | namenode | Owen O'Malley | Owen O'Malley |
| [HADOOP-8151](https://issues.apache.org/jira/browse/HADOOP-8151) | Error handling in snappy decompressor throws invalid exceptions |  Major | io, native | Todd Lipcon | Matt Foley |


