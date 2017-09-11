
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

## Release 2.8.3 - Unreleased (as of 2017-08-28)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13588](https://issues.apache.org/jira/browse/HADOOP-13588) | ConfServlet should respect Accept request header |  Major | conf | Weiwei Yang | Weiwei Yang |
| [HADOOP-14260](https://issues.apache.org/jira/browse/HADOOP-14260) | Configuration.dumpConfiguration should redact sensitive information |  Major | conf, security | Vihang Karajgaonkar | John Zhuge |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13933](https://issues.apache.org/jira/browse/HADOOP-13933) | Add haadmin -getAllServiceState option to get the HA state of all the NameNodes/ResourceManagers |  Major | tools | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-10480](https://issues.apache.org/jira/browse/HDFS-10480) | Add an admin command to list currently open files |  Major | . | Kihwal Lee | Manoj Govindassamy |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13628](https://issues.apache.org/jira/browse/HADOOP-13628) | Support to retrieve specific property from configuration via REST API |  Major | conf | Weiwei Yang | Weiwei Yang |
| [HDFS-12143](https://issues.apache.org/jira/browse/HDFS-12143) | Improve performance of getting and removing inode features |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-12171](https://issues.apache.org/jira/browse/HDFS-12171) | Reduce IIP object allocations for inode lookup |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-14455](https://issues.apache.org/jira/browse/HADOOP-14455) | ViewFileSystem#rename should support be supported within same nameservice with different mountpoints |  Major | viewfs | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-12131](https://issues.apache.org/jira/browse/HDFS-12131) | Add some of the FSNamesystem JMX values as metrics |  Minor | hdfs, namenode | Erik Krogen | Erik Krogen |
| [HADOOP-14627](https://issues.apache.org/jira/browse/HADOOP-14627) | Support MSI and DeviceCode token provider in ADLS |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HADOOP-14251](https://issues.apache.org/jira/browse/HADOOP-14251) | Credential provider should handle property key deprecation |  Critical | security | John Zhuge | John Zhuge |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10829](https://issues.apache.org/jira/browse/HADOOP-10829) | Iteration on CredentialProviderFactory.serviceLoader  is thread-unsafe |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-14578](https://issues.apache.org/jira/browse/HADOOP-14578) | Bind IPC connections to kerberos UPN host for proxy users |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-11896](https://issues.apache.org/jira/browse/HDFS-11896) | Non-dfsUsed will be doubled on dead node re-registration |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-14677](https://issues.apache.org/jira/browse/HADOOP-14677) | mvn clean compile fails |  Major | build | Andras Bokor | Andras Bokor |
| [HADOOP-14702](https://issues.apache.org/jira/browse/HADOOP-14702) | Fix formatting issue and regression caused by conversion from APT to Markdown |  Minor | documentation | Doris Gu | Doris Gu |
| [YARN-6965](https://issues.apache.org/jira/browse/YARN-6965) | Duplicate instantiation in FairSchedulerQueueInfo |  Minor | fairscheduler | Masahiro Tanaka | Masahiro Tanaka |
| [HDFS-11738](https://issues.apache.org/jira/browse/HDFS-11738) | Hedged pread takes more time when block moved from initial locations |  Major | hdfs-client | Vinayakumar B | Vinayakumar B |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14678](https://issues.apache.org/jira/browse/HADOOP-14678) | AdlFilesystem#initialize swallows exception when getting user name |  Minor | fs/adl | John Zhuge | John Zhuge |
