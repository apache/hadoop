
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

## Release 0.23.9 - 2013-07-08



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-169](https://issues.apache.org/jira/browse/YARN-169) | Update log4j.appender.EventCounter to use org.apache.hadoop.log.metrics.EventCounter |  Minor | nodemanager | Anthony Rojas | Anthony Rojas |
| [MAPREDUCE-5268](https://issues.apache.org/jira/browse/MAPREDUCE-5268) | Improve history server startup performance |  Major | jobhistoryserver | Jason Lowe | Karthik Kambatla |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-4765](https://issues.apache.org/jira/browse/HDFS-4765) | Permission check of symlink deletion incorrectly throws UnresolvedLinkException |  Major | namenode | Andrew Wang | Andrew Wang |
| [YARN-742](https://issues.apache.org/jira/browse/YARN-742) | Log aggregation causes a lot of redundant setPermission calls |  Major | nodemanager | Kihwal Lee | Jason Lowe |
| [HDFS-4867](https://issues.apache.org/jira/browse/HDFS-4867) | metaSave NPEs when there are invalid blocks in repl queue. |  Major | namenode | Kihwal Lee | Plamen Jeliazkov |
| [HDFS-4862](https://issues.apache.org/jira/browse/HDFS-4862) | SafeModeInfo.isManual() returns true when resources are low even if it wasn't entered into manually |  Major | . | Ravi Prakash | Ravi Prakash |
| [HDFS-4832](https://issues.apache.org/jira/browse/HDFS-4832) | Namenode doesn't change the number of missing blocks in safemode when DNs rejoin or leave |  Critical | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-9581](https://issues.apache.org/jira/browse/HADOOP-9581) | hadoop --config non-existent directory should result in error |  Major | scripts | Ashwin Shankar | Ashwin Shankar |
| [MAPREDUCE-5308](https://issues.apache.org/jira/browse/MAPREDUCE-5308) | Shuffling to memory can get out-of-sync when fetching multiple compressed map outputs |  Major | . | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-5315](https://issues.apache.org/jira/browse/MAPREDUCE-5315) | DistCp reports success even on failure. |  Critical | distcp | Mithun Radhakrishnan | Mithun Radhakrishnan |
| [HDFS-4878](https://issues.apache.org/jira/browse/HDFS-4878) | On Remove Block, Block is not Removed from neededReplications queue |  Major | namenode | Tao Luo | Tao Luo |
| [MAPREDUCE-4019](https://issues.apache.org/jira/browse/MAPREDUCE-4019) | -list-attempt-ids  is not working |  Minor | client | B Anil Kumar | Ashwin Shankar |
| [MAPREDUCE-5316](https://issues.apache.org/jira/browse/MAPREDUCE-5316) | job -list-attempt-ids command does not handle illegal task-state |  Major | client | Ashwin Shankar | Ashwin Shankar |
| [HADOOP-9582](https://issues.apache.org/jira/browse/HADOOP-9582) | Non-existent file to "hadoop fs -conf" doesn't throw error |  Major | conf | Ashwin Shankar | Ashwin Shankar |
| [HDFS-4205](https://issues.apache.org/jira/browse/HDFS-4205) | fsck fails with symlinks |  Major | hdfs-client | Andy Isaacson | Jason Lowe |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-5084](https://issues.apache.org/jira/browse/MAPREDUCE-5084) | fix coverage  org.apache.hadoop.mapreduce.v2.app.webapp and org.apache.hadoop.mapreduce.v2.hs.webapp |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-427](https://issues.apache.org/jira/browse/YARN-427) | Coverage fix for org.apache.hadoop.yarn.server.api.\* |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-478](https://issues.apache.org/jira/browse/YARN-478) | fix coverage org.apache.hadoop.yarn.webapp.log |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |


