
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

## Release 0.15.2 - 2008-01-08



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2160](https://issues.apache.org/jira/browse/HADOOP-2160) | separate website from user documentation |  Major | documentation | Doug Cutting | Doug Cutting |
| [HADOOP-1327](https://issues.apache.org/jira/browse/HADOOP-1327) | Doc on Streaming |  Major | documentation | Runping Qi | Rob Weltman |
| [HADOOP-2382](https://issues.apache.org/jira/browse/HADOOP-2382) | include hadoop-default.html in subversion |  Minor | documentation | Doug Cutting |  |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2246](https://issues.apache.org/jira/browse/HADOOP-2246) | In CHANGES.txt, move HADOOP-1851 & HADOOP-1231 to INCOMPATIBLE CHANGES section |  Blocker | documentation | Devaraj Das | Arun C Murthy |
| [HADOOP-2238](https://issues.apache.org/jira/browse/HADOOP-2238) | TaskGraphServlet does not set Content-Type |  Major | . | Paul Saab |  |
| [HADOOP-2129](https://issues.apache.org/jira/browse/HADOOP-2129) | distcp between two clusters does not work if it is run on the target cluster |  Critical | util | Murtaza A. Basrai | Doug Cutting |
| [HADOOP-2158](https://issues.apache.org/jira/browse/HADOOP-2158) | hdfsListDirectory in libhdfs does not scale |  Blocker | . | Christian Kunz | Christian Kunz |
| [HADOOP-2378](https://issues.apache.org/jira/browse/HADOOP-2378) | last TaskCompletionEvent gets added to the job after the job is marked as completed |  Blocker | . | Alejandro Abdelnur | Devaraj Das |
| [HADOOP-2228](https://issues.apache.org/jira/browse/HADOOP-2228) | Jobs fail because job.xml exists |  Major | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-2422](https://issues.apache.org/jira/browse/HADOOP-2422) | dfs -cat multiple files fail with 'Unable to write to output stream.' |  Blocker | . | Koji Noguchi | Raghu Angadi |
| [HADOOP-2460](https://issues.apache.org/jira/browse/HADOOP-2460) | NameNode could delete wrong edits file when there is an error |  Major | . | Raghu Angadi | dhruba borthakur |
| [HADOOP-2227](https://issues.apache.org/jira/browse/HADOOP-2227) | wrong usage of mapred.local.dir.minspacestart |  Critical | . | Christian Kunz | Amareshwari Sriramadasu |
| [HADOOP-2437](https://issues.apache.org/jira/browse/HADOOP-2437) | final map output not evenly distributed across multiple disks |  Blocker | . | Christian Kunz | Arun C Murthy |
| [HADOOP-2486](https://issues.apache.org/jira/browse/HADOOP-2486) | Dropping records at reducer.  InMemoryFileSystem NPE. |  Blocker | . | Koji Noguchi | Devaraj Das |
| [HADOOP-2456](https://issues.apache.org/jira/browse/HADOOP-2456) | German locale makes NameNode web interface crash |  Minor | . | Matthias Friedrich | Matthias Friedrich |


