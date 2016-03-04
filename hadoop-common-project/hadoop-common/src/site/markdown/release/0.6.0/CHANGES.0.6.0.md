
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

## Release 0.6.0 - 2006-09-08

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-497](https://issues.apache.org/jira/browse/HADOOP-497) | DataNodes and TaskTrackers should be able to report hostnames and ips relative to customizable network interfaces and nameservers |  Minor | util | Lorenzo Thione | Lorenzo Thione |
| [HADOOP-456](https://issues.apache.org/jira/browse/HADOOP-456) | Checkpointing and logging of data node descriptors |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-441](https://issues.apache.org/jira/browse/HADOOP-441) | SequenceFile should support 'custom compressors' |  Major | io | Arun C Murthy | Arun C Murthy |
| [HADOOP-322](https://issues.apache.org/jira/browse/HADOOP-322) | Need a job control utility to submit and monitor a group of jobs which have DAG dependency |  Major | . | Runping Qi | Runping Qi |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-499](https://issues.apache.org/jira/browse/HADOOP-499) | Avoid the use of Strings to improve the  performance of hadoop streaming |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-488](https://issues.apache.org/jira/browse/HADOOP-488) | Change ToolBase.doMain() to return a status code |  Major | . | Andrzej Bialecki |  |
| [HADOOP-486](https://issues.apache.org/jira/browse/HADOOP-486) | adding username to jobstatus |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-483](https://issues.apache.org/jira/browse/HADOOP-483) | Document libhdfs and fix some OS specific stuff in Makefile |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-474](https://issues.apache.org/jira/browse/HADOOP-474) | support compressed text files as input and output |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-468](https://issues.apache.org/jira/browse/HADOOP-468) | Setting scheduling priority in hadoop-env.sh |  Minor | . | Vetle Roeim |  |
| [HADOOP-464](https://issues.apache.org/jira/browse/HADOOP-464) | Troubleshooting message for RunJar (bin/hadoop jar) |  Major | util | Michel Tourn |  |
| [HADOOP-463](https://issues.apache.org/jira/browse/HADOOP-463) | variable expansion in Configuration |  Major | conf | Michel Tourn |  |
| [HADOOP-450](https://issues.apache.org/jira/browse/HADOOP-450) | Remove the need for users to specify the types of the inputs |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-437](https://issues.apache.org/jira/browse/HADOOP-437) | support gzip input files |  Major | . | Michel Tourn |  |
| [HADOOP-312](https://issues.apache.org/jira/browse/HADOOP-312) | Connections should not be cached |  Major | ipc | Devaraj Das | Devaraj Das |
| [HADOOP-64](https://issues.apache.org/jira/browse/HADOOP-64) | DataNode should be capable of managing multiple volumes |  Minor | . | Sameer Paranjpye | Milind Bhandarkar |
| [HADOOP-54](https://issues.apache.org/jira/browse/HADOOP-54) | SequenceFile should compress blocks, not individual entries |  Major | io | Doug Cutting | Arun C Murthy |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-517](https://issues.apache.org/jira/browse/HADOOP-517) | readLine function of UTF8ByteArrayUtils does not handle end of line correctly |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-507](https://issues.apache.org/jira/browse/HADOOP-507) | Runtime exception in org.apache.hadoop.io.WritableFactories.newInstance when trying to startup namenode/datanode |  Major | util | Arun C Murthy | Owen O'Malley |
| [HADOOP-501](https://issues.apache.org/jira/browse/HADOOP-501) | toString(resources, sb) in Configuration.java throws a ClassCastException since resources can be loaded from an URL |  Minor | . | Thomas Friol |  |
| [HADOOP-473](https://issues.apache.org/jira/browse/HADOOP-473) | TextInputFormat does not correctly handle all line endings |  Major | . | Dennis Kubes |  |
| [HADOOP-469](https://issues.apache.org/jira/browse/HADOOP-469) | Portability of hadoop shell scripts for deployment |  Major | . | Jean-Baptiste Quenot |  |
| [HADOOP-460](https://issues.apache.org/jira/browse/HADOOP-460) | Small jobs benchmark fails with current Hadoop due to UTF8 -\> Text ClassCastException |  Major | . | Sanjay Dahiya |  |
| [HADOOP-458](https://issues.apache.org/jira/browse/HADOOP-458) | libhdfs corrupts memory |  Major | . | Christian Kunz | Konstantin Shvachko |
| [HADOOP-455](https://issues.apache.org/jira/browse/HADOOP-455) | Text class should support the DEL character |  Major | io | Hairong Kuang | Hairong Kuang |
| [HADOOP-453](https://issues.apache.org/jira/browse/HADOOP-453) | bug in Text.setCapacity( int len ) |  Minor | . | Sami Siren |  |
| [HADOOP-434](https://issues.apache.org/jira/browse/HADOOP-434) | Use Hadoop scripts to run smallJobsBenchmark to avoid classpath issues. |  Major | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-427](https://issues.apache.org/jira/browse/HADOOP-427) | DatanodeInfo class should be used instead of DatanodeDescriptor in the jsp & related files |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-426](https://issues.apache.org/jira/browse/HADOOP-426) | streaming unit tests failing on SunOS |  Major | . | Doug Cutting | Michel Tourn |
| [HADOOP-424](https://issues.apache.org/jira/browse/HADOOP-424) | mapreduce jobs fail when no split is returned via inputFormat.getSplits |  Major | . | Frédéric Bertin |  |
| [HADOOP-421](https://issues.apache.org/jira/browse/HADOOP-421) | replace String in hadoop record io with the new Text class |  Major | record | Owen O'Malley | Milind Bhandarkar |
| [HADOOP-419](https://issues.apache.org/jira/browse/HADOOP-419) | libdfs doesn't work with application threads |  Major | . | Christian Kunz | Owen O'Malley |
| [HADOOP-400](https://issues.apache.org/jira/browse/HADOOP-400) | the job tracker re-runs failed tasks on the same node |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-324](https://issues.apache.org/jira/browse/HADOOP-324) | "IOException: No space left on device" is handled incorrectly |  Major | . | Konstantin Shvachko | Wendy Chien |
| [HADOOP-320](https://issues.apache.org/jira/browse/HADOOP-320) | bin/hadoop dfs -mv does not mv  source's checksum file if source is a file |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-286](https://issues.apache.org/jira/browse/HADOOP-286) | copyFromLocal throws LeaseExpiredException |  Major | . | Runping Qi | Konstantin Shvachko |
| [HADOOP-281](https://issues.apache.org/jira/browse/HADOOP-281) | dfs.FSDirectory.mkdirs can create sub-directories of a file! |  Major | . | Sameer Paranjpye | Wendy Chien |
| [HADOOP-196](https://issues.apache.org/jira/browse/HADOOP-196) | Fix buggy uselessness of Configuration( Configuration other) constructor |  Major | conf | alan wootton |  |
| [HADOOP-176](https://issues.apache.org/jira/browse/HADOOP-176) | comparators of integral writable types are not transitive for inequalities |  Major | io | Dick King |  |
| [HADOOP-50](https://issues.apache.org/jira/browse/HADOOP-50) | dfs datanode should store blocks in multiple directories |  Major | . | Doug Cutting | Milind Bhandarkar |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-408](https://issues.apache.org/jira/browse/HADOOP-408) | Unit tests take too long to run. |  Minor | . | Doug Cutting |  |


