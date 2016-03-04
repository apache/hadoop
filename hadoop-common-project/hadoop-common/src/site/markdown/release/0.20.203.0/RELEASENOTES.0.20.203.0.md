
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
# Apache Hadoop  0.20.203.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-5647](https://issues.apache.org/jira/browse/HADOOP-5647) | *Major* | **TestJobHistory fails if /tmp/\_logs is not writable to. Testcase should not depend on /tmp**

Removed dependency of testcase on /tmp and made it to use test.build.data directory instead.


---

* [HDFS-1626](https://issues.apache.org/jira/browse/HDFS-1626) | *Minor* | **Make BLOCK\_INVALIDATE\_LIMIT configurable**

Added a new configuration property dfs.block.invalidate.limit for FSNamesystem.blockInvalidateLimit.


---

* [HDFS-457](https://issues.apache.org/jira/browse/HDFS-457) | *Major* | **better handling of volume failure in Data Node storage**

Datanode can continue if a volume for replica storage fails. Previously a datanode resigned if any volume failed.


---

* [MAPREDUCE-1118](https://issues.apache.org/jira/browse/MAPREDUCE-1118) | *Major* | **Capacity Scheduler scheduling information is hard to read / should be tabular format**

Add CapacityScheduler servlet to enhance web UI for queue information.


---

* [MAPREDUCE-478](https://issues.apache.org/jira/browse/MAPREDUCE-478) | *Minor* | **separate jvm param for mapper and reducer**

Allow map and reduce jvm parameters, environment variables and ulimit to be set separately.

Configuration changes:
      add mapred.map.child.java.opts
      add mapred.reduce.child.java.opts
      add mapred.map.child.env
      add mapred.reduce.child.ulimit
      add mapred.map.child.env
      add mapred.reduce.child.ulimit
      deprecated mapred.child.java.opts
      deprecated mapred.child.env
      deprecated mapred.child.ulimit


---

* [MAPREDUCE-323](https://issues.apache.org/jira/browse/MAPREDUCE-323) | *Critical* | **Improve the way job history files are managed**

This patch does four things:

\* it changes the directory structure of the done directory that holds history logs for jobs that are completed,
\* it builds toy databases for completed jobs, so we no longer have to scan 2N files on DFS to find out facts about the N jobs that have completed since the job tracker started [which can be hundreds of thousands of files in practical cases],
\* it changes the job history browser to display more information and allow more filtering criteria, and
\* it creates a new programmatic interface for finding files matching user-chosen criteria. This allows users to no longer be concerned with our methods of storing them, in turn allowing us to change those at will.

The new API described above, which can be used to programmatically obtain history file PATHs given search criteria, is described below:

    package org.apache.hadoop.mapreduce.jobhistory;
    ...

    // this interface is within O.A.H.mapreduce.jobhistory.JobHistory:

    // holds information about one job hostory log in the done 
    //   job history logs
    public static class JobHistoryJobRecord {
       public Path getPath() { ... }
       public String getJobIDString() { ... }
       public long getSubmitTime() { ... }
       public String getUserName() { ... }
       public String getJobName() { ... }
    }

    public class JobHistoryRecordRetriever implements Iterator\<JobHistoryJobRecord\> {
       // usual Interface methods -- remove() throws UnsupportedOperationException
       // returns the number of calls to next() that will succeed
       public int numMatches() { ... }
    }

    // returns a JobHistoryRecordRetriever that delivers all Path's of job matching job history files,
    // in no particular order.  Any criterion that is null or the empty string does not constrain.
    // All criteria that are specified are applied conjunctively, except that if there's more than
    // one date you retrieve all Path's matching ANY date.
    // soughtUser and soughtJobid must match exactly.
    // soughtJobName can match the entire job name or any substring.
    // dates must be in the format exactly MM/DD/YYYY .  
    // Dates' leading digits must be 2's .  We're incubating a Y3K problem.
    public JobHistoryRecordRetriever getMatchingJob
        (String soughtUser, String soughtJobName, String[] dateStrings, String soughtJobid)
      throws IOException



