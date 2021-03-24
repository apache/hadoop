
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
# Apache Hadoop  3.0.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [YARN-7219](https://issues.apache.org/jira/browse/YARN-7219) | *Critical* | **Make AllocateRequestProto compatible with branch-2/branch-2.8**

Change update\_requests field id to 7.  This matches the current field id in branch-2 and branch-2.8.


---

* [YARN-1492](https://issues.apache.org/jira/browse/YARN-1492) | *Major* | **truly shared cache for jars (jobjar/libjar)**

The YARN Shared Cache provides the facility to upload and manage shared application resources to HDFS in a safe and scalable manner. YARN applications can leverage resources uploaded by other applications or previous runs of the same application without having to re-­upload and localize identical files multiple times. This will save network resources and reduce YARN application startup time.


---

* [HDFS-10467](https://issues.apache.org/jira/browse/HDFS-10467) | *Major* | **Router-based HDFS federation**

HDFS Router-based Federation adds a RPC routing layer that provides a federated view of multiple HDFS namespaces.
This is similar to the existing ViewFS and HDFS federation functionality, except the mount table is managed on the server-side by the routing layer rather than on the client.
This simplifies access to a federated cluster for existing HDFS clients.

See HDFS-10467 and the HDFS Router-based Federation documentation for more details.


---

* [YARN-5734](https://issues.apache.org/jira/browse/YARN-5734) | *Major* | **OrgQueue for easy CapacityScheduler queue configuration management**

<!-- markdown -->

The OrgQueue extension to the capacity scheduler provides a programmatic way to change configurations by providing a REST API that users can call to modify queue configurations. This enables automation of queue configuration management by administrators in the queue's `administer_queue` ACL.


---

* [MAPREDUCE-5951](https://issues.apache.org/jira/browse/MAPREDUCE-5951) | *Major* | **Add support for the YARN Shared Cache**

MapReduce support for the YARN shared cache allows MapReduce jobs to take advantage of additional resource caching. This saves network bandwidth between the job submission client as well as within the YARN cluster itself. This will reduce job submission time and overall job runtime.


---

* [YARN-6623](https://issues.apache.org/jira/browse/YARN-6623) | *Blocker* | **Add support to turn off launching privileged containers in the container-executor**

A change in configuration for launching Docker containers under YARN. Docker container capabilities, mounts, networks and allowing privileged container have to specified in the container-executor.cfg. By default, all of the above are turned off. This change will break existing setups launching Docker containers under YARN. Please refer to the Docker containers under YARN documentation for more information.


---

* [HADOOP-14816](https://issues.apache.org/jira/browse/HADOOP-14816) | *Major* | **Update Dockerfile to use Xenial**

This patch changes the default build and test environment in the following ways:

\* Switch from Ubuntu "Trusty" 14.04 to Ubuntu "Xenial" 16.04
\* Switch from Oracle JDK 8 to OpenJDK 8
\* Adds OpenJDK 9 to the build environment


---

* [HADOOP-14957](https://issues.apache.org/jira/browse/HADOOP-14957) | *Major* | **ReconfigurationTaskStatus is exposing guava Optional in its public api**

ReconfigurationTaskStatus' API scope is reduced to LimitedPrivate, and its dependency on com.google.com.base.Optional is replaced by java.util.Optional.


---

* [HADOOP-14840](https://issues.apache.org/jira/browse/HADOOP-14840) | *Major* | **Tool to estimate resource requirements of an application pipeline based on prior executions**

The first version of Resource Estimator service, a tool that captures the historical resource usage of an app and predicts its future resource requirement.


---

* [MAPREDUCE-6983](https://issues.apache.org/jira/browse/MAPREDUCE-6983) | *Major* | **Moving logging APIs over to slf4j in hadoop-mapreduce-client-core**

In hadoop-mapreduce-client-core module, the type of some public LOG variables were changed from org.apache.commons.logging.Log to org.slf4j.Logger. In the public methods that accepts logger, the logger was changed from org.apache.commons.logging.Log to org.slf4j.Logger.


---

* [HDFS-12682](https://issues.apache.org/jira/browse/HDFS-12682) | *Blocker* | **ECAdmin -listPolicies will always show SystemErasureCodingPolicies state as DISABLED**

**WARNING: No release note provided for this change.**


---

* [YARN-5085](https://issues.apache.org/jira/browse/YARN-5085) | *Major* | **Add support for change of container ExecutionType**

This allows the Application Master to ask the Scheduler to change the ExecutionType of a running/allocated container.


---

* [HDFS-12840](https://issues.apache.org/jira/browse/HDFS-12840) | *Blocker* | **Creating a file with non-default EC policy in a EC zone is not correctly serialized in the editlog**

Add ErasureCodingPolicyId to each OP\_ADD edit log op.


---

* [HADOOP-15059](https://issues.apache.org/jira/browse/HADOOP-15059) | *Blocker* | **3.0 deployment cannot work with old version MR tar ball which breaks rolling upgrade**

This change reverses the default delegation token format implemented by HADOOP-12563, but preserves the capability to read the new delegation token format.  When the new format becomes default, then MR deployment jobs runs will be compatible with releases that contain this change.



