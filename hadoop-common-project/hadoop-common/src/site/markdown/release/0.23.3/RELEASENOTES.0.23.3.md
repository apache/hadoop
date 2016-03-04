
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
# Apache Hadoop  0.23.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-8703](https://issues.apache.org/jira/browse/HADOOP-8703) | *Major* | **distcpV2: turn CRC checking off for 0 byte size**

distcp skips CRC on 0 byte files.


---

* [HADOOP-8551](https://issues.apache.org/jira/browse/HADOOP-8551) | *Major* | **fs -mkdir creates parent directories without the -p option**

FsShell's "mkdir" no longer implicitly creates all non-existent parent directories.  The command adopts the posix compliant behavior of requiring the "-p" flag to auto-create parent directories.


---

* [HADOOP-8327](https://issues.apache.org/jira/browse/HADOOP-8327) | *Major* | **distcpv2 and distcpv1 jars should not coexist**

Resolve sporadic distcp issue due to having two DistCp classes (v1 & v2) in the classpath.


---

* [HDFS-3318](https://issues.apache.org/jira/browse/HDFS-3318) | *Blocker* | **Hftp hangs on transfers \>2GB**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-4311](https://issues.apache.org/jira/browse/MAPREDUCE-4311) | *Major* | **Capacity scheduler.xml does not accept decimal values for capacity and maximum-capacity settings**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-4072](https://issues.apache.org/jira/browse/MAPREDUCE-4072) | *Major* | **User set java.library.path seems to overwrite default creating problems native lib loading**

-Djava.library.path in mapred.child.java.opts can cause issues with native libraries.  LD\_LIBRARY\_PATH through mapred.child.env should be used instead.


---

* [MAPREDUCE-4017](https://issues.apache.org/jira/browse/MAPREDUCE-4017) | *Trivial* | **Add jobname to jobsummary log**

The Job Summary log may contain commas in values that are escaped by a '\' character.  This was true before, but is more likely to be exposed now.


---

* [MAPREDUCE-3940](https://issues.apache.org/jira/browse/MAPREDUCE-3940) | *Major* | **ContainerTokens should have an expiry interval**

ContainerTokens now have an expiry interval so that stale tokens cannot be used for launching containers.


---

* [MAPREDUCE-3812](https://issues.apache.org/jira/browse/MAPREDUCE-3812) | *Major* | **Lower default allocation sizes, fix allocation configurations and document them**

Removes two sets of previously available config properties:

1. ( yarn.scheduler.fifo.minimum-allocation-mb and yarn.scheduler.fifo.maximum-allocation-mb ) and,
2. ( yarn.scheduler.capacity.minimum-allocation-mb and yarn.scheduler.capacity.maximum-allocation-mb )

In favor of two new, generically named properties:

1. yarn.scheduler.minimum-allocation-mb - This acts as the floor value of memory resource requests for containers.
2. yarn.scheduler.maximum-allocation-mb - This acts as the ceiling value of memory resource requests for containers.

Both these properties need to be set at the ResourceManager (RM) to take effect, as the RM is where the scheduler resides.

Also changes the default minimum and maximums to 128 MB and 10 GB respectively.


---

* [MAPREDUCE-3543](https://issues.apache.org/jira/browse/MAPREDUCE-3543) | *Critical* | **Mavenize Gridmix.**

Note that to apply this you should first run the script - ./MAPREDUCE-3543v3.sh svn, then apply the patch.

If this is merged to more then trunk, the version inside of hadoop-tools/hadoop-gridmix/pom.xml will need to be udpated accordingly.


---

* [MAPREDUCE-3348](https://issues.apache.org/jira/browse/MAPREDUCE-3348) | *Major* | **mapred job -status fails to give info even if the job is present in History**

Fixed a bug in MR client to redirect to JobHistoryServer correctly when RM forgets the app.



