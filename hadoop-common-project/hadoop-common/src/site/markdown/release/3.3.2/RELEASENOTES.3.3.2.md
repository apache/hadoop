
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
# Apache Hadoop  3.3.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-15288](https://issues.apache.org/jira/browse/HDFS-15288) | *Major* | **Add Available Space Rack Fault Tolerant BPP**

Added a new BlockPlacementPolicy: "AvailableSpaceRackFaultTolerantBlockPlacementPolicy" which uses the same optimization logic as the AvailableSpaceBlockPlacementPolicy along with spreading the replicas across maximum number of racks, similar to BlockPlacementPolicyRackFaultTolerant.
The BPP can be configured by setting the blockplacement policy class as org.apache.hadoop.hdfs.server.blockmanagement.AvailableSpaceRackFaultTolerantBlockPlacementPolicy


---

* [HADOOP-17424](https://issues.apache.org/jira/browse/HADOOP-17424) | *Major* | **Replace HTrace with No-Op tracer**

Dependency on HTrace and TraceAdmin protocol/utility were removed. Tracing functionality is no-op until alternative tracer implementation is added.


---

* [HDFS-15814](https://issues.apache.org/jira/browse/HDFS-15814) | *Major* | **Make some parameters configurable for DataNodeDiskMetrics**

**WARNING: No release note provided for this change.**


---

* [YARN-10820](https://issues.apache.org/jira/browse/YARN-10820) | *Major* | **Make GetClusterNodesRequestPBImpl thread safe**

Added syncronization so that the "yarn node list" command does not fail intermittently


---

* [HADOOP-13887](https://issues.apache.org/jira/browse/HADOOP-13887) | *Minor* | **Encrypt S3A data client-side with AWS SDK (S3-CSE)**

Adds support for client side encryption in AWS S3,
with keys managed by AWS-KMS.

Read the documentation in encryption.md very, very carefully before
use and consider it unstable.

S3-CSE is enabled in the existing configuration option
"fs.s3a.server-side-encryption-algorithm":

fs.s3a.server-side-encryption-algorithm=CSE-KMS
fs.s3a.server-side-encryption.key=\<KMS\_KEY\_ID\>

You cannot enable CSE and SSE in the same client, although
you can still enable a default SSE option in the S3 console.

\* Not compatible with S3Guard.   
\* Filesystem list/get status operations subtract 16 bytes from the length
  of all files \>= 16 bytes long to compensate for the padding which CSE
  adds.
\* The SDK always warns about the specific algorithm chosen being
  deprecated. It is critical to use this algorithm for ranged
  GET requests to work (i.e. random IO). Ignore.
\* Unencrypted files CANNOT BE READ.
  The entire bucket SHOULD be encrypted with S3-CSE.
\* Uploading files may be a bit slower as blocks are now
  written sequentially.
\* The Multipart Upload API is disabled when S3-CSE is active.


---

* [YARN-8234](https://issues.apache.org/jira/browse/YARN-8234) | *Critical* | **Improve RM system metrics publisher's performance by pushing events to timeline server in batch**

When Timeline Service V1 or V1.5 is used, if "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.enable-batch" is set to true, ResourceManager sends timeline events in batch. The default value is false. If this functionality is enabled, the maximum number that events published in batch is configured by "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.batch-size". The default value is 1000. The interval of publishing events can be configured by "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.interval-seconds". By default, it is set to 60 seconds.



