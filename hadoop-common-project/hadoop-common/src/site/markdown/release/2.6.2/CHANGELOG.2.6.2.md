
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

## Release 2.6.2 - 2015-10-28

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4087](https://issues.apache.org/jira/browse/YARN-4087) | Followup fixes after YARN-2019 regarding RM behavior when state-store error occurs |  Major | . | Jian He | Jian He |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-3727](https://issues.apache.org/jira/browse/YARN-3727) | For better error recovery, check if the directory exists before using it for localization. |  Major | nodemanager | zhihai xu | zhihai xu |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-3194](https://issues.apache.org/jira/browse/YARN-3194) | RM should handle NMContainerStatuses sent by NM while registering if NM is Reconnected node |  Blocker | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [MAPREDUCE-6334](https://issues.apache.org/jira/browse/MAPREDUCE-6334) | Fetcher#copyMapOutput is leaking usedMemory upon IOException during InMemoryMapOutput shuffle handler |  Blocker | . | Eric Payne | Eric Payne |
| [YARN-3554](https://issues.apache.org/jira/browse/YARN-3554) | Default value for maximum nodemanager connect wait time is too high |  Major | . | Jason Lowe | Naganarasimha G R |
| [YARN-3780](https://issues.apache.org/jira/browse/YARN-3780) | Should use equals when compare Resource in RMNodeImpl#ReconnectNodeTransition |  Minor | resourcemanager | zhihai xu | zhihai xu |
| [YARN-3802](https://issues.apache.org/jira/browse/YARN-3802) | Two RMNodes for the same NodeId are used in RM sometimes after NM is reconnected. |  Major | resourcemanager | zhihai xu | zhihai xu |
| [YARN-2019](https://issues.apache.org/jira/browse/YARN-2019) | Retrospect on decision of making RM crashed if any exception throw in ZKRMStateStore |  Critical | . | Junping Du | Jian He |
| [YARN-4005](https://issues.apache.org/jira/browse/YARN-4005) | Completed container whose app is finished is not removed from NMStateStore |  Major | . | Jun Gong | Jun Gong |
| [MAPREDUCE-6454](https://issues.apache.org/jira/browse/MAPREDUCE-6454) | MapReduce doesn't set the HADOOP\_CLASSPATH for jar lib in distributed cache. |  Critical | . | Junping Du | Junping Du |
| [YARN-3896](https://issues.apache.org/jira/browse/YARN-3896) | RMNode transitioned from RUNNING to REBOOTED because its response id had not been reset synchronously |  Major | resourcemanager | Jun Gong | Jun Gong |
| [MAPREDUCE-6497](https://issues.apache.org/jira/browse/MAPREDUCE-6497) | Fix wrong value of JOB\_FINISHED event in JobHistoryEventHandler |  Major | . | Shinichi Yamashita | Shinichi Yamashita |
| [YARN-3798](https://issues.apache.org/jira/browse/YARN-3798) | ZKRMStateStore shouldn't create new session without occurrance of SESSIONEXPIED |  Blocker | resourcemanager | Bibin A Chundatt | Varun Saxena |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4092](https://issues.apache.org/jira/browse/YARN-4092) | RM HA UI redirection needs to be fixed when both RMs are in standby mode |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-4101](https://issues.apache.org/jira/browse/YARN-4101) | RM should print alert messages if Zookeeper and Resourcemanager gets connection issue |  Critical | yarn | Yesha Vora | Xuan Gong |


