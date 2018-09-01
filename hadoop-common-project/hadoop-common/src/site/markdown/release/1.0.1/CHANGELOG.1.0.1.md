
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

## Release 1.0.1 - 2012-02-22

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7470](https://issues.apache.org/jira/browse/HADOOP-7470) | move up to Jackson 1.8.8 |  Minor | util | Steve Loughran | Enis Soztutar |
| [HADOOP-8037](https://issues.apache.org/jira/browse/HADOOP-8037) | Binary tarball does not preserve platform info for native builds, and RPMs fail to provide needed symlinks for libhadoop.so |  Blocker | build | Matt Foley | Matt Foley |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-3184](https://issues.apache.org/jira/browse/MAPREDUCE-3184) | Improve handling of fetch failures when a tasktracker is not responding on HTTP |  Major | jobtracker | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3607](https://issues.apache.org/jira/browse/MAPREDUCE-3607) | Port missing new API mapreduce lib classes to 1.x |  Major | client | Tom White | Tom White |
| [HADOOP-7987](https://issues.apache.org/jira/browse/HADOOP-7987) | Support setting the run-as user in unsecure mode |  Major | security | Devaraj Das | Jitendra Nath Pandey |
| [HDFS-2814](https://issues.apache.org/jira/browse/HDFS-2814) | NamenodeMXBean does not account for svn revision in the version information |  Minor | . | Hitesh Shah | Hitesh Shah |
| [HADOOP-8009](https://issues.apache.org/jira/browse/HADOOP-8009) | Create hadoop-client and hadoop-minicluster artifacts for downstream projects |  Critical | build | Alejandro Abdelnur | Alejandro Abdelnur |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-3343](https://issues.apache.org/jira/browse/MAPREDUCE-3343) | TaskTracker Out of Memory because of distributed cache |  Major | mrv1 | Ahmed Radwan | yunjiong zhao |
| [HADOOP-7960](https://issues.apache.org/jira/browse/HADOOP-7960) | Port HADOOP-5203 to branch-1, build version comparison is too restrictive |  Major | . | Giridharan Kesavan | Matt Foley |
| [HADOOP-7964](https://issues.apache.org/jira/browse/HADOOP-7964) | Deadlock in class init. |  Blocker | security, util | Kihwal Lee | Daryn Sharp |
| [HADOOP-7988](https://issues.apache.org/jira/browse/HADOOP-7988) | Upper case in hostname part of the principals doesn't work with kerberos. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-8010](https://issues.apache.org/jira/browse/HADOOP-8010) | hadoop-config.sh spews error message when HADOOP\_HOME\_WARN\_SUPPRESS is set to true and HADOOP\_HOME is present |  Minor | scripts | Roman Shaposhnik | Roman Shaposhnik |
| [HDFS-2379](https://issues.apache.org/jira/browse/HDFS-2379) | 0.20: Allow block reports to proceed without holding FSDataset lock |  Critical | datanode | Todd Lipcon | Todd Lipcon |
| [HADOOP-8052](https://issues.apache.org/jira/browse/HADOOP-8052) | Hadoop Metrics2 should emit Float.MAX\_VALUE (instead of Double.MAX\_VALUE) to avoid making Ganglia's gmetad core |  Major | metrics | Varun Kapoor | Varun Kapoor |


