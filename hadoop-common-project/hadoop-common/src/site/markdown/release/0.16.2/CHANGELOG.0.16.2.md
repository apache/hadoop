
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

## Release 0.16.2 - 2008-04-02



### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2833](https://issues.apache.org/jira/browse/HADOOP-2833) | JobClient.submitJob(...) should not use "Dr Who" as a default username |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3011](https://issues.apache.org/jira/browse/HADOOP-3011) | Distcp deleting target directory |  Blocker | util | Koji Noguchi | Chris Douglas |
| [HADOOP-3033](https://issues.apache.org/jira/browse/HADOOP-3033) | Datanode fails write to DFS file with exception message "Trying to change block file offset" |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2978](https://issues.apache.org/jira/browse/HADOOP-2978) | JobHistory log format for COUNTER is ambigurous |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-2985](https://issues.apache.org/jira/browse/HADOOP-2985) | LocalJobRunner gets NullPointerException if there is no output directory |  Critical | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-3003](https://issues.apache.org/jira/browse/HADOOP-3003) | FileSystem cache key should be updated after a FileSystem object is created |  Major | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3042](https://issues.apache.org/jira/browse/HADOOP-3042) | Update the Javadoc in JobConf.getOutputPath to reflect the actual temporary path |  Major | documentation | Devaraj Das | Amareshwari Sriramadasu |
| [HADOOP-3007](https://issues.apache.org/jira/browse/HADOOP-3007) | DataNode pipelining : failure on mirror results in failure on upstream datanode |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2944](https://issues.apache.org/jira/browse/HADOOP-2944) | redesigned plugin has missing functionality |  Major | contrib/eclipse-plugin | Chris Dyer | Christophe Taton |
| [HADOOP-3049](https://issues.apache.org/jira/browse/HADOOP-3049) | MultithreadedMapRunner eats RuntimeExceptions |  Blocker | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-3039](https://issues.apache.org/jira/browse/HADOOP-3039) | Runtime exceptions not killing job |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3027](https://issues.apache.org/jira/browse/HADOOP-3027) | JobTracker shuts down during initialization if the NameNode is down |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3056](https://issues.apache.org/jira/browse/HADOOP-3056) | distcp seems to be broken in 0.16.1 |  Blocker | util | Christian Kunz | Chris Douglas |
| [HADOOP-3070](https://issues.apache.org/jira/browse/HADOOP-3070) | Trash not being expunged, Trash Emptier thread gone by NPE |  Blocker | fs | Koji Noguchi | Koji Noguchi |
| [HADOOP-3084](https://issues.apache.org/jira/browse/HADOOP-3084) | distcp fails for files with zero length |  Blocker | util | Mukund Madhugiri | Chris Douglas |
| [HADOOP-3107](https://issues.apache.org/jira/browse/HADOOP-3107) | fsck failing with NPE |  Blocker | . | Koji Noguchi | dhruba borthakur |
| [HADOOP-3105](https://issues.apache.org/jira/browse/HADOOP-3105) | compile-core-test fails for branch 0.16 |  Blocker | . | Amareshwari Sriramadasu | Alejandro Abdelnur |
| [HADOOP-3103](https://issues.apache.org/jira/browse/HADOOP-3103) | [HOD] Hadoop.tmp.dir should not be set to cluster directory |  Blocker | contrib/hod | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-3098](https://issues.apache.org/jira/browse/HADOOP-3098) | dfs -chown does not like "\_" underscore in user name |  Blocker | fs | Koji Noguchi | Raghu Angadi |
| [HADOOP-3108](https://issues.apache.org/jira/browse/HADOOP-3108) | NPE in FSDirectory.unprotectedSetPermission |  Blocker | . | Koji Noguchi | Konstantin Shvachko |
| [HADOOP-3104](https://issues.apache.org/jira/browse/HADOOP-3104) | MultithreadMapRunner keeps consuming records even if trheads are not available |  Critical | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-3128](https://issues.apache.org/jira/browse/HADOOP-3128) | TestDFSPermission due to not throwing exception. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3111](https://issues.apache.org/jira/browse/HADOOP-3111) | Remove HBase from Hadoop contrib |  Major | . | Jim Kellerman | Jim Kellerman |


