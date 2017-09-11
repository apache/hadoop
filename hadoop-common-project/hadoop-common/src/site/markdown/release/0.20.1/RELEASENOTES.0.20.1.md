
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
# Apache Hadoop  0.20.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-5210](https://issues.apache.org/jira/browse/HADOOP-5210) | *Minor* | **Reduce Task Progress shows \> 100% when the total size of map outputs (for a single reducer) is high**

This patch resets the variable totalBytesProcessed before the final merge sothat it will be used for calculating the progress of reducePhase(the 3rd phase of reduce task) correctly.


---

* [HADOOP-5726](https://issues.apache.org/jira/browse/HADOOP-5726) | *Major* | **Remove pre-emption from the capacity scheduler code base**

Removed pre-emption from capacity scheduler. The impact of this change is that capacities for queues can no longer be guaranteed within a given span of time. Also changed configuration variables to remove pre-emption related variables and better reflect the absence of guarantees.


---

* [HADOOP-5881](https://issues.apache.org/jira/browse/HADOOP-5881) | *Major* | **Simplify configuration related to task-memory-monitoring and memory-based scheduling**

**WARNING: No release note provided for this change.**


---

* [HADOOP-5924](https://issues.apache.org/jira/browse/HADOOP-5924) | *Major* | **JT fails to recover the jobs after restart after HADOOP:4372**

Post HADOOP-4372, empty job history files caused NPE. This issues fixes that by creating new files if no old file is found.


---

* [HADOOP-5746](https://issues.apache.org/jira/browse/HADOOP-5746) | *Major* | **Errors encountered in MROutputThread after the last map/reduce call can go undetected**

If the child (streaming) process returns successfully and the MROutputThread throws an error, there was no way to detect that as all the IOExceptions was ignored. Such issues can occur when DFS clients were closed etc. Now a check for errors (in threads) is made before finishing off the task and an exception is thrown that fails he task.


---

* [HADOOP-5884](https://issues.apache.org/jira/browse/HADOOP-5884) | *Major* | **Capacity scheduler should account high memory jobs as using more capacity of the queue**

Fixes Capacity scheduler to account more capacity of a queue for a high memory job. Done by considering these jobs to
take more slots proportionally with respect to a slot's default memory size.


---

* [HADOOP-5921](https://issues.apache.org/jira/browse/HADOOP-5921) | *Major* | **JobTracker does not come up because of NotReplicatedYetException**

Jobtracker crashes if it fails to create jobtracker.info file (i.e if sufficient datanodes are not up). With this patch it keeps on retrying on IOExceptions assuming IOExceptions in jobtracker.info creation implies that the hdfs is not in \*ready \*state.


---

* [HADOOP-5920](https://issues.apache.org/jira/browse/HADOOP-5920) | *Major* | **TestJobHistory fails some times.**

TestJobHistory fails as jobtracker is restarted very fast (within a minute) and history files from earlier testcases were not cleaned up. This patch cleans up the history-dir and mapred-system-dir after every test.


---

* [HADOOP-3315](https://issues.apache.org/jira/browse/HADOOP-3315) | *Major* | **New binary file format**

Add a new, binary file format TFile.


---

* [MAPREDUCE-2](https://issues.apache.org/jira/browse/MAPREDUCE-2) | *Major* | **ArrayOutOfIndex error in KeyFieldBasedPartitioner on empty key**

KeyFieldBasedPartitioner throws ArrayOutOfIndex when passed an empty key. This patch hashes empty key to 0 hashcode.


---

* [MAPREDUCE-130](https://issues.apache.org/jira/browse/MAPREDUCE-130) | *Major* | **Delete the jobconf copy from the log directory of the JobTracker when the job is retired**

When a job is initialized, it localizes the job conf to the logs dir. Without this patch I never gets deleted. Now when the job retires, the conf is deleted. This local copy is required to display on the webui.


---

* [MAPREDUCE-657](https://issues.apache.org/jira/browse/MAPREDUCE-657) | *Major* | **CompletedJobStatusStore hardcodes filesystem to hdfs**

CompletedJobStatusStore was hardcored to persist to hdfs. This patch allows to persist to local fs. Just qualify mapred.job.tracker.persist.jobstatus.dir with file://


---

* [HADOOP-6080](https://issues.apache.org/jira/browse/HADOOP-6080) | *Major* | **Handling of  Trash with quota**

Provide a new option to rm and rmr, -skipTrash, which will immediately delete the files specified, rather than moving them to the trash.


---

* [MAPREDUCE-18](https://issues.apache.org/jira/browse/MAPREDUCE-18) | *Blocker* | **Under load the shuffle sometimes gets incorrect data**

This patch adds the mapid and reduceid in the http header of mapoutput when being sent to reduce node. Also validates compressed length, decompressed length, mapid and reduceid from http header at reduce node.


---

* [MAPREDUCE-383](https://issues.apache.org/jira/browse/MAPREDUCE-383) | *Major* | **pipes combiner does not reset properly after a spill**

Fixed a bug in Pipes combiner to reset the spilled bytes count after the spill.


---

* [MAPREDUCE-40](https://issues.apache.org/jira/browse/MAPREDUCE-40) | *Blocker* | **Memory management variables need a backwards compatibility option after HADOOP-5881**

Fixed backwards compatibility by re-introducing and deprecating removed memory monitoring related configuration options.


---

* [MAPREDUCE-796](https://issues.apache.org/jira/browse/MAPREDUCE-796) | *Major* | **Encountered "ClassCastException" on tasktracker while running wordcount with MultithreadedMapRunner**

Multithreaded mapper was modified to create a new Runtime exception (object) from a throwable instead of casting a throwable into a RuntimeException, once the Multithreaded map encounters a fault.


---

* [MAPREDUCE-838](https://issues.apache.org/jira/browse/MAPREDUCE-838) | *Blocker* | **Task succeeds even when committer.commitTask fails with IOException**

Fixed a bug in the way commit of task outputs happens. The bug was that if commit fails with IOException, the task would be declared as successful.


---

* [MAPREDUCE-805](https://issues.apache.org/jira/browse/MAPREDUCE-805) | *Major* | **Deadlock in Jobtracker**

Job initialization process was changed to not change (run) states during initialization. The reason is two fold
- this can lead to deadlock as state changes require circular locking (i.e JobInProgress requires JobTracker lock)
- events were not raised as these state changes were not informed/propogated back to the JobTracker

Now the JobTracker takes care of initializing/failing/killing the job and raising appropriate events. The simple rule that was enforced was that "The JobTracker lock is \*must\* before changing the run-state of a job".


---

* [MAPREDUCE-832](https://issues.apache.org/jira/browse/MAPREDUCE-832) | *Major* | **Too many WARN messages about deprecated memorty config variables in JobTacker log**

Reduced the frequency of log messages printed when a deprecated memory management variable is found in configuration of a job.


---

* [MAPREDUCE-745](https://issues.apache.org/jira/browse/MAPREDUCE-745) | *Major* | **TestRecoveryManager fails sometimes**

JobTracker was changed to take an identifier as an argument. This helps in testcases where the jobtracker/mapred-cluster is (re)started in a short span of time and the chances of jobtracker identifier clashing are high. Also the RecoveryManager was modified to throw an exception if a job fails in init during the recovery process. The reason being that this event will trigger a job failure in the recovery process and will remove the failed job from further initialization and processing.


---

* [MAPREDUCE-834](https://issues.apache.org/jira/browse/MAPREDUCE-834) | *Major* | **When TaskTracker config use old memory management values its memory monitoring is diabled.**

The tasktracker's startup code was modified to use deprecated memory management configuration variables, when specified, and enable memory monitoring of tasks.


---

* [MAPREDUCE-818](https://issues.apache.org/jira/browse/MAPREDUCE-818) | *Minor* | **org.apache.hadoop.mapreduce.Counters.getGroup returns null if the group name doesnt exist.**

Fixed a bug in the new org.apache.hadoop.mapreduce.Counters.getGroup() method to return an empty group if group name doesn't exist, instead of null, thus making sure that it is in sync with the Javadoc.


---

* [MAPREDUCE-807](https://issues.apache.org/jira/browse/MAPREDUCE-807) | *Blocker* | **Stray user files in mapred.system.dir with permissions other than 777 can prevent the jobtracker from starting up.**

The JobTracker tries to delete the mapred.system.dir when it is starting up (with the job recovery disabled). The fix provided by this jira is that JobTracker will fail (bail out) with AccessControlException if it fails to delete files/directories in mapred.system.dir due to access control issues.


---

* [MAPREDUCE-767](https://issues.apache.org/jira/browse/MAPREDUCE-767) | *Major* | **to remove mapreduce dependency on commons-cli2**

Removes the dependency of hadoop-mapred from commons-cli2 and uses commons-cli1.2 for command-line parsing.


---

* [HADOOP-6213](https://issues.apache.org/jira/browse/HADOOP-6213) | *Blocker* | **Remove commons dependency on commons-cli2**

GenericOptionsParser in branch 0.20 depends on commons-cli2. This jira removes the dependency of branch 0.20 on commons-cli2 completely. The problem is seen after 'ant binary' where all the library files are copied to '$hadoop-home/lib' which already has commons-cli2.


---

* [MAPREDUCE-430](https://issues.apache.org/jira/browse/MAPREDUCE-430) | *Major* | **Task stuck in cleanup with OutOfMemoryErrors**

Various code paths in the framework caught Throwable and tried to do inline cleanup. In case of OOM errors, such inline-cleanups can result into hung jvms. With this fix, the TaskTracker provides a api to report fatal errors (any throwable other than FSErrror and Exceptions). On catching a Throwable, Mapper/Reducer tries to inform the TT.



