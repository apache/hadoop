
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
# Apache Hadoop  2.7.5 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-8829](https://issues.apache.org/jira/browse/HDFS-8829) | *Major* | **Make SO\_RCVBUF and SO\_SNDBUF size configurable for DataTransferProtocol sockets and allow configuring auto-tuning**

HDFS-8829 introduces two new configuration settings: dfs.datanode.transfer.socket.send.buffer.size and dfs.datanode.transfer.socket.recv.buffer.size. These settings can be used to control the socket send buffer and receive buffer sizes respectively on the DataNode for client-DataNode and DataNode-DataNode connections. The default values of both settings are 128KB for backwards compatibility. For optimum performance it is recommended to set these values to zero to enable the OS networking stack to auto-tune buffer sizes.


---

* [YARN-6959](https://issues.apache.org/jira/browse/YARN-6959) | *Major* | **RM may allocate wrong AM Container for new attempt**

ResourceManager will now record ResourceRequests from different attempts into different objects.

