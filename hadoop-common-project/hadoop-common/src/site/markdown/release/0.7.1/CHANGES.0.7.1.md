
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

## Release 0.7.1 - 2006-10-11



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-594](https://issues.apache.org/jira/browse/HADOOP-594) | Safemode default threshold should be 0.999 |  Major | . | Konstantin Shvachko | Konstantin Shvachko |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-593](https://issues.apache.org/jira/browse/HADOOP-593) | NullPointerException in JobTracker's ExireTaskTracker thread |  Critical | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-592](https://issues.apache.org/jira/browse/HADOOP-592) | NullPointerException in toString for RPC connection objects |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-598](https://issues.apache.org/jira/browse/HADOOP-598) | rpc timeout in Task.done kills task |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-597](https://issues.apache.org/jira/browse/HADOOP-597) | transmission errors to the reduce will cause map output to be considered lost |  Major | . | Owen O'Malley | Owen O'Malley |


