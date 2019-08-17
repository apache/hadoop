
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

## Release 0.17.1 - 2008-06-23

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3565](https://issues.apache.org/jira/browse/HADOOP-3565) | JavaSerialization can throw java.io.StreamCorruptedException |  Major | . | Tom White | Tom White |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2159](https://issues.apache.org/jira/browse/HADOOP-2159) | Namenode stuck in safemode |  Major | . | Christian Kunz | Hairong Kuang |
| [HADOOP-3472](https://issues.apache.org/jira/browse/HADOOP-3472) | MapFile.Reader getClosest() function returns incorrect results when before is true |  Major | io | Todd Lipcon | stack |
| [HADOOP-3442](https://issues.apache.org/jira/browse/HADOOP-3442) | QuickSort may get into unbounded recursion |  Blocker | . | Runping Qi | Chris Douglas |
| [HADOOP-3477](https://issues.apache.org/jira/browse/HADOOP-3477) | release tar.gz contains duplicate files |  Major | build | Adam Heath | Adam Heath |
| [HADOOP-3475](https://issues.apache.org/jira/browse/HADOOP-3475) | MapOutputBuffer allocates 4x as much space to record capacity as intended |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-3522](https://issues.apache.org/jira/browse/HADOOP-3522) | ValuesIterator.next() doesn't return a new object, thus failing many equals() tests. |  Major | . | Spyros Blanas | Owen O'Malley |
| [HADOOP-3550](https://issues.apache.org/jira/browse/HADOOP-3550) | Reduce tasks failing with OOM |  Blocker | . | Arun C Murthy | Chris Douglas |
| [HADOOP-3526](https://issues.apache.org/jira/browse/HADOOP-3526) | contrib/data\_join doesn't work |  Blocker | . | Spyros Blanas | Spyros Blanas |
| [HADOOP-1979](https://issues.apache.org/jira/browse/HADOOP-1979) | fsck on namenode without datanodes takes too much time |  Minor | . | Koji Noguchi | Lohit Vijayarenu |


