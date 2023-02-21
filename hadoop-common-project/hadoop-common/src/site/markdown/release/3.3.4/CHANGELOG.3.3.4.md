
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

## Release 3.3.4 - 2022-07-29



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-18044](https://issues.apache.org/jira/browse/HADOOP-18044) | Hadoop - Upgrade to JQuery 3.6.0 |  Major | . | Yuan Luo | Yuan Luo |
| [YARN-11195](https://issues.apache.org/jira/browse/YARN-11195) | Document how to configure NUMA in YARN |  Major | documentation | Prabhu Joseph | Samrat Deb |
| [HADOOP-18332](https://issues.apache.org/jira/browse/HADOOP-18332) | Remove rs-api dependency by downgrading jackson to 2.12.7 |  Major | build | PJ Fanning | PJ Fanning |
| [HADOOP-18354](https://issues.apache.org/jira/browse/HADOOP-18354) | Upgrade reload4j to 1.2.22 due to XXE vulnerability |  Major | . | PJ Fanning | PJ Fanning |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-18085](https://issues.apache.org/jira/browse/HADOOP-18085) | S3 SDK Upgrade causes AccessPoint ARN endpoint mistranslation |  Major | fs/s3, test | Bogdan Stolojan | Bogdan Stolojan |
| [YARN-11092](https://issues.apache.org/jira/browse/YARN-11092) | Upgrade jquery ui to 1.13.1 |  Major | . | D M Murali Krishna Reddy | groot |
| [HDFS-16453](https://issues.apache.org/jira/browse/HDFS-16453) | Upgrade okhttp from 2.7.5 to 4.9.3 |  Major | hdfs-client | Ivan Viaznikov | groot |
| [YARN-10974](https://issues.apache.org/jira/browse/YARN-10974) | CS UI: queue filter and openQueues param do not work as expected |  Major | capacity scheduler | Chengbing Liu | Chengbing Liu |
| [HADOOP-18237](https://issues.apache.org/jira/browse/HADOOP-18237) | Upgrade Apache Xerces Java to 2.12.2 |  Major | build | groot | groot |
| [HADOOP-18074](https://issues.apache.org/jira/browse/HADOOP-18074) | Partial/Incomplete groups list can be returned in LDAP groups lookup |  Major | security | Philippe Lanoe | Larry McCay |
| [HADOOP-18079](https://issues.apache.org/jira/browse/HADOOP-18079) | Upgrade Netty to 4.1.77.Final |  Major | build | Renukaprasad C | Wei-Chiu Chuang |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-18068](https://issues.apache.org/jira/browse/HADOOP-18068) | Upgrade AWS SDK to 1.12.132 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18307](https://issues.apache.org/jira/browse/HADOOP-18307) | remove hadoop-cos as a dependency of hadoop-cloud-storage |  Major | bulid, fs | Steve Loughran | Steve Loughran |
| [HADOOP-18344](https://issues.apache.org/jira/browse/HADOOP-18344) | AWS SDK update to 1.12.262 to address jackson  CVE-2018-7489 |  Major | fs/s3 | Steve Loughran | Steve Loughran |


