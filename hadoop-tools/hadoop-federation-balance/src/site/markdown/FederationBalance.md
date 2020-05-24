<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Federation Balance Guide
=====================

---

 - [Overview](#Overview)
 - [Usage](#Usage)
     - [Basic Usage](#Basic_Usage)
 - [Configuration Options](#Configuration_Options)
 - [Architecture of Federation Balance](#Architecture_of_Federation_Balance)

---

Overview
--------

  Federation Balance is a tool used for balancing data across different
  federation namespaces. It uses DistCp to copy data from source to the target.
  First it creates a snapshot at src path and submit the initial distcp. Then it
  uses distcp diff to do the incremental copy. Finally when the source and
  target are the same, it updates the mount table on Router and move the source
  to trash.

  This document aims to describe the design and usage of the Federation Balance.

Usage
-----

### Basic Usage

  Supposing we have a mount entry in Router:

    /foo/src --> hdfs://nn0:8020/foo/src

  Submit a federation balance job locally. The first parameter should be a mount
   entry. The second parameter is the target path in the target cluster.

    bash$ /bin/hadoop fedbalance submit /foo/src hdfs://nn1:8020/foo/dst

  This will copy data from hdfs://nn0:8020/foo/src to hdfs://nn1:8020/foo/dst
  incrementally and finally update the mount entry to:

    /foo/src --> hdfs://nn1:8020/foo/dst

  If the hadoop shell process exits unexpectedly and we want to continue the
  unfinished job, we can use command:

    bash$ /bin/hadoop fedbalance continue

  This will scan the journal to find all the unfinished jobs, recover and
  continue to run them.


Configuration Options
--------------------

| Configuration key              | Description                          |
| ------------------------------ | ------------------------------------ |
| hadoop.hdfs.procedure.work.thread.num | The worker threads number of the BalanceProcedureScheduler |
| hadoop.hdfs.procedure.scheduler.journal.uri | The uri of the journal |
| federation.balance.class | The class used for federation balance |
| distcp.procedure.map.num | The map number of distcp job |
| distcp.procedure.bandwidth.limit | The bandwidth limit of distcp job |
| distcp.procedure.force.close.open.files | When there is no diff between source path and target path but there are still open files in source path, force close all the open files then do the final distcp |
| distcp.procedure.move.to.trash | Move source path to trash after all the data are sync to target |

Architecture of Federation Balance
----------------------

  The components of the Federation Balance may be classified into the following
  categories:

  * Balance Procedure Scheduler
  * DistCpFedBalance

### Balance Procedure Scheduler

  The Balance Procedure Scheduler implements a state machine. It's responsible
  for scheduling a balance job, including submit, run, delay and recover.
  The model is showed below:

  ![Balance Procedure Scheduler](images/BalanceProcedureScheduler.png)

  * After a job is submitted, the job is added to the pendingQueue.
  * Worker thread takes job and run it. Journals are written to storage.
  * If writing journal fails, the job is added to the recoverQueue for later
    recovery. If Worker thread catches a RetryTaskException, it adds the job to
    the delayQueue.
  * Rooster thread takes job from delayQueue and adds it back to pendingQueue.
  * When a scheduler starts, it will scan all the unfinished jobs from
    journal and add them to the recoverQueue. The recover thread will recover
    them from journal and add them back to pendingQueue.

### DistCpFedBalance

  DistCpFedBalance is implemented as a job of the state machine. All the distcp
  balance logic are implemented here. A DistCpFedBalance job is consists of 3
  procedures:

  * DistCpProcedure: This is the first procedure. It handles all the data copy
    works. There are 3 phases:
    * Init Distcp: Creates a snapshot of the source path and distcp it to
      target.
    * Diff Distcp: Submit distcp with `-diff` round by round to sync source and
      target paths.
    * Final Distcp(optional): This phase is only triggered when
      `distcp.procedure.force.close.open.files` is set. If there is no diff
      between source and target and there are still some open files under source
      , it will force close all the open files and submit the final distcp. If
      the force close is not enabled, it will wait until there is no open files.

  * SingleMountTableProcedure: This procedure updates the mount entry in Router.

  * TrashProcedure: This procedure move the source path to trash.

  After all 3 phases finish, the balance job is done.