
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

## Release 0.12.3 - 2007-04-06

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1192](https://issues.apache.org/jira/browse/HADOOP-1192) | Du command takes a noticable longer time to execute on a large dfs than the 0.11 release |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1191](https://issues.apache.org/jira/browse/HADOOP-1191) | MapTask should wait for the status reporting thread to die before invoking the TaskUmbilicalProtocol.done(taskid) |  Major | . | Devaraj Das | Doug Cutting |
| [HADOOP-1179](https://issues.apache.org/jira/browse/HADOOP-1179) | task Tracker should be restarted if its jetty http server cannot serve get-map-output files |  Major | . | Runping Qi | Devaraj Das |
| [HADOOP-1177](https://issues.apache.org/jira/browse/HADOOP-1177) | Lack of logging of exceptions in MapOutputLocation.getFile |  Major | . | Owen O'Malley | Devaraj Das |
| [HADOOP-1175](https://issues.apache.org/jira/browse/HADOOP-1175) | the user output/logs are broken from the web ui |  Major | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-1162](https://issues.apache.org/jira/browse/HADOOP-1162) | Record IO: seariliizing a byte buffer to CSV fails if buffer contains bytes less than 16. |  Major | . | David Bowen | Milind Bhandarkar |
| [HADOOP-1123](https://issues.apache.org/jira/browse/HADOOP-1123) | LocalFileSystem gets a NullPointerException when tries to recover from ChecksumError |  Major | fs | Hairong Kuang | Hairong Kuang |
| [HADOOP-1105](https://issues.apache.org/jira/browse/HADOOP-1105) | Reducers don't make "progress" while iterating through values |  Major | . | Owen O'Malley | Owen O'Malley |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


