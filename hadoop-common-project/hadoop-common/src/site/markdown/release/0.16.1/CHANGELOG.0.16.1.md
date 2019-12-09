
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

## Release 0.16.1 - 2008-03-14



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2735](https://issues.apache.org/jira/browse/HADOOP-2735) | Setting default tmp directory for java createTempFile (java.io.tmpdir) |  Critical | . | Koji Noguchi | Amareshwari Sriramadasu |
| [HADOOP-2371](https://issues.apache.org/jira/browse/HADOOP-2371) | Candidate user guide for permissions feature of Hadoop DFS |  Major | . | Robert Chansler | Robert Chansler |
| [HADOOP-2923](https://issues.apache.org/jira/browse/HADOOP-2923) | Check in missing files from HADOOP-2603 |  Major | . | Owen O'Malley | Chris Douglas |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2730](https://issues.apache.org/jira/browse/HADOOP-2730) | Update HOD documentation |  Major | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-2861](https://issues.apache.org/jira/browse/HADOOP-2861) | [HOD] Improve the user interface for the HOD commands |  Major | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-2911](https://issues.apache.org/jira/browse/HADOOP-2911) | [HOD] Make the information printed by allocate and info commands less verbose and clearer |  Major | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2754](https://issues.apache.org/jira/browse/HADOOP-2754) | Path filter for Local file system list .crc files |  Major | . | Amareshwari Sriramadasu | Hairong Kuang |
| [HADOOP-1188](https://issues.apache.org/jira/browse/HADOOP-1188) | processIOError() should update fstime file |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2787](https://issues.apache.org/jira/browse/HADOOP-2787) | The constant org.apache.hadoop.fs.permission.FsPermission.UMASK\_LABEL should be "dfs.umask", instead of "hadoop.dfs.umask" |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2788](https://issues.apache.org/jira/browse/HADOOP-2788) | chgrp missing from hadoop dfs options |  Critical | . | Mukund Madhugiri | Raghu Angadi |
| [HADOOP-2785](https://issues.apache.org/jira/browse/HADOOP-2785) | Typo in peridioc block verification patch |  Trivial | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2780](https://issues.apache.org/jira/browse/HADOOP-2780) | Socket receive buffer size on datanode too large |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2716](https://issues.apache.org/jira/browse/HADOOP-2716) | Balancer should require superuser privilege |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2733](https://issues.apache.org/jira/browse/HADOOP-2733) | Compiler warnings in TestClusterMapReduceTestCase and TestJobStatusPersistency |  Major | test | Konstantin Shvachko | Tsz Wo Nicholas Sze |
| [HADOOP-2725](https://issues.apache.org/jira/browse/HADOOP-2725) | Distcp truncates some files when copying |  Critical | util | Murtaza A. Basrai | Tsz Wo Nicholas Sze |
| [HADOOP-2789](https://issues.apache.org/jira/browse/HADOOP-2789) | Race condition in ipc.Server prevents responce being written back to client. |  Critical | ipc | Clint Morgan | Raghu Angadi |
| [HADOOP-2391](https://issues.apache.org/jira/browse/HADOOP-2391) | Speculative Execution race condition with output paths |  Major | . | Dennis Kubes | Amareshwari Sriramadasu |
| [HADOOP-2808](https://issues.apache.org/jira/browse/HADOOP-2808) | FileUtil::copy ignores "overwrite" formal |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-2683](https://issues.apache.org/jira/browse/HADOOP-2683) | Provide a way to specifiy login out side an RPC |  Blocker | . | Raghu Angadi | Tsz Wo Nicholas Sze |
| [HADOOP-2814](https://issues.apache.org/jira/browse/HADOOP-2814) | NPE in datanode during TestDataTransferProtocol. |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2811](https://issues.apache.org/jira/browse/HADOOP-2811) | method Counters.makeCompactString() does not insert separator char ',' between the counters of different groups. |  Critical | . | Runping Qi | Runping Qi |
| [HADOOP-2843](https://issues.apache.org/jira/browse/HADOOP-2843) | mapred.join access control is overly restrictive |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-2813](https://issues.apache.org/jira/browse/HADOOP-2813) | Unit test fails on Linux: org.apache.hadoop.fs.TestDU.testDU |  Blocker | fs | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2840](https://issues.apache.org/jira/browse/HADOOP-2840) | Gridmix test script fails to run java sort tests |  Major | test | Mukund Madhugiri | Mukund Madhugiri |
| [HADOOP-2766](https://issues.apache.org/jira/browse/HADOOP-2766) | [HOD] No way to set HADOOP\_OPTS environment variable to the Hadoop daemons through HOD |  Critical | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-2769](https://issues.apache.org/jira/browse/HADOOP-2769) | TestNNThroughputBenchmark should not used a fixed http port |  Major | test | Owen O'Malley | Owen O'Malley |
| [HADOOP-2894](https://issues.apache.org/jira/browse/HADOOP-2894) | task trackers can't survive a job tracker bounce |  Blocker | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2904](https://issues.apache.org/jira/browse/HADOOP-2904) | 3 minor fixes in the rpc metrics area. |  Major | . | girish vaitheeswaran | dhruba borthakur |
| [HADOOP-2903](https://issues.apache.org/jira/browse/HADOOP-2903) | Data type mismatch exception raised from pushMetric |  Major | metrics | girish vaitheeswaran | girish vaitheeswaran |
| [HADOOP-2847](https://issues.apache.org/jira/browse/HADOOP-2847) | [HOD] Idle cluster cleanup does not work if the JobTracker becomes unresponsive to RPC calls |  Blocker | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-2809](https://issues.apache.org/jira/browse/HADOOP-2809) | [HOD] Syslog configuration, syslog-address, does not work in HOD 0.4 |  Critical | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-2931](https://issues.apache.org/jira/browse/HADOOP-2931) | exception in DFSClient.create: Stream closed |  Major | . | Michael Bieniosek | Michael Bieniosek |
| [HADOOP-2883](https://issues.apache.org/jira/browse/HADOOP-2883) | Extensive write failures |  Blocker | . | Christian Kunz | dhruba borthakur |
| [HADOOP-2925](https://issues.apache.org/jira/browse/HADOOP-2925) | [HOD] Create mapred system directory using a naming convention that will avoid clashes in multi-user shared cluster scenario. |  Major | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-2756](https://issues.apache.org/jira/browse/HADOOP-2756) | NPE in DFSClient in hbase under load |  Minor | . | stack | Raghu Angadi |
| [HADOOP-2869](https://issues.apache.org/jira/browse/HADOOP-2869) | Deprecate and remove SequenceFile.setCompressionType |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-2958](https://issues.apache.org/jira/browse/HADOOP-2958) | Test utility no longer works in trunk |  Minor | test | Chris Douglas | Chris Douglas |
| [HADOOP-2915](https://issues.apache.org/jira/browse/HADOOP-2915) | mapred output files and directories should be created as the job submitter, not tasktracker or jobtracker |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2852](https://issues.apache.org/jira/browse/HADOOP-2852) | Update gridmix to avoid artificially long tail |  Major | test | Chris Douglas | Chris Douglas |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2918](https://issues.apache.org/jira/browse/HADOOP-2918) | Enhance log messages to better debug "No lease on file" message |  Major | . | dhruba borthakur | dhruba borthakur |


