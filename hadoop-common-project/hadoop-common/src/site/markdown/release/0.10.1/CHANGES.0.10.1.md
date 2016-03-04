
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

## Release 0.10.1 - 2007-01-10

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-851](https://issues.apache.org/jira/browse/HADOOP-851) | Implement the LzoCodec with support for the lzo compression algorithms |  Major | io | Arun C Murthy | Arun C Murthy |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-880](https://issues.apache.org/jira/browse/HADOOP-880) | Recursive delete for an S3 directory does not actually delete files or subdirectories |  Major | fs | Tom White | Tom White |
| [HADOOP-879](https://issues.apache.org/jira/browse/HADOOP-879) | SequenceFileInputFormat can no longer read from data produced by MapFileOutputFormat |  Major | . | Bryan Pendleton | Doug Cutting |
| [HADOOP-873](https://issues.apache.org/jira/browse/HADOOP-873) | native libraries aren't loaded unless the user specifies the java.library.path in the child jvm options |  Major | util | Owen O'Malley | Owen O'Malley |
| [HADOOP-871](https://issues.apache.org/jira/browse/HADOOP-871) | java.library.path is wrongly initialized by bin/hadoop when only pre-built libs are present, but custom-built ones aren't |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-868](https://issues.apache.org/jira/browse/HADOOP-868) | Fix the merge method on Maps to limit the number of open files |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-866](https://issues.apache.org/jira/browse/HADOOP-866) | dfs -get should remove existing crc file if -crc is not specified |  Major | fs | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-865](https://issues.apache.org/jira/browse/HADOOP-865) | Files written to S3 but never closed can't be deleted |  Major | fs | Bryan Pendleton | Tom White |
| [HADOOP-864](https://issues.apache.org/jira/browse/HADOOP-864) | bin/hadoop jar throws file creation exception for temp files |  Minor | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-863](https://issues.apache.org/jira/browse/HADOOP-863) | MapTask prints info log message when the progress-reporting thread starts |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-857](https://issues.apache.org/jira/browse/HADOOP-857) | IOException when running map reduce on S3 filesystem |  Major | fs | Tom White |  |
| [HADOOP-815](https://issues.apache.org/jira/browse/HADOOP-815) | Investigate and fix the extremely large memory-footprint of JobTracker |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-600](https://issues.apache.org/jira/browse/HADOOP-600) | Race condition in JobTracker updating the task tracker's status while declaring it lost |  Major | . | Owen O'Malley | Arun C Murthy |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


