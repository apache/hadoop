
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

## Release 0.3.2 - 2006-06-09

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-275](https://issues.apache.org/jira/browse/HADOOP-275) | log4j changes for hadoopStreaming |  Major | . | Michel Tourn | Doug Cutting |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-279](https://issues.apache.org/jira/browse/HADOOP-279) | running without the hadoop script causes warnings about log4j not being configured correctly |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-269](https://issues.apache.org/jira/browse/HADOOP-269) | add FAQ to Wiki |  Major | documentation | Doug Cutting | Doug Cutting |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-294](https://issues.apache.org/jira/browse/HADOOP-294) | dfs client error retries aren't happening (already being created and not replicated yet) |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-292](https://issues.apache.org/jira/browse/HADOOP-292) | hadoop dfs commands should not output superfluous data to stdout |  Minor | . | Yoram Arnon | Owen O'Malley |
| [HADOOP-289](https://issues.apache.org/jira/browse/HADOOP-289) | Datanodes need to catch SocketTimeoutException and UnregisteredDatanodeException |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-285](https://issues.apache.org/jira/browse/HADOOP-285) | Data nodes cannot re-join the cluster once connection is lost |  Blocker | . | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-284](https://issues.apache.org/jira/browse/HADOOP-284) | dfs timeout on open |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-282](https://issues.apache.org/jira/browse/HADOOP-282) | the datanode crashes if it starts before the namenode |  Critical | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-280](https://issues.apache.org/jira/browse/HADOOP-280) | AllTestDriver has incorrect class name for DistributedFSCheck test |  Major | . | Konstantin Shvachko |  |
| [HADOOP-277](https://issues.apache.org/jira/browse/HADOOP-277) | Race condition in Configuration.getLocalPath() |  Major | . | p sutter | Sameer Paranjpye |
| [HADOOP-242](https://issues.apache.org/jira/browse/HADOOP-242) | job fails because of "No valid local directories in property: " exception |  Major | . | Yoram Arnon | Owen O'Malley |
| [HADOOP-240](https://issues.apache.org/jira/browse/HADOOP-240) | namenode should not log failed mkdirs at warning level |  Minor | . | Hairong Kuang | Hairong Kuang |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


