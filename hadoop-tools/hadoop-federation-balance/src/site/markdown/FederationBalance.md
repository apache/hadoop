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
     - [RBF Mode And Normal Federation Mode](#RBF_Mode_And_Normal_Federation_Mode)     
     - [Command Options](#Command_Options)
     - [Configuration Options](#Configuration_Options)
 - [Architecture of Federation Balance](#Architecture_of_Federation_Balance)
     - [Balance Procedure Scheduler](#Balance_Procedure_Scheduler)
     - [DistCpFedBalance](#DistCpFedBalance)     
---

Overview
--------

  Federation Balance is a tool balancing data across different federation
  namespaces. It uses [DistCp](../hadoop-distcp/DistCp.html) to copy data from
  the source path to the target path. First it creates a snapshot at the source
  path and submits the initial distcp. Then it uses distcp diff to do the
  incremental copy. Finally when the source and the target are the same, it
  updates the mount table in Router and moves the source to trash.

  This document aims to describe the usage and design of the Federation Balance.

Usage
-----

### Basic Usage

  The federation balance tool supports both normal federation cluster and
  router-based federation cluster. Taking rbf for example. Supposing we have a
  mount entry in Router:

    /foo/src --> hdfs://nn0:8020/foo/src

  The command below runs a federation balance job. The first parameter is the
  mount entry. The second one is the target path which must include the target
  cluster.

    bash$ /bin/hadoop fedbalance submit /foo/src hdfs://nn1:8020/foo/dst

  It copies data from hdfs://nn0:8020/foo/src to hdfs://nn1:8020/foo/dst
  incrementally and finally updates the mount entry to:

    /foo/src --> hdfs://nn1:8020/foo/dst

  If the hadoop shell process exits unexpectedly, we can use the command below
  to continue the unfinished job:

    bash$ /bin/hadoop fedbalance continue

  This will scan the journal to find all the unfinished jobs, recover and
  continue to execute them.
  
  If we want to balance in a normal federation cluster, use the command below.
  
    bash$ /bin/hadoop fedbalance -router false submit hdfs://nn0:8020/foo/src hdfs://nn1:8020/foo/dst
    
  The option `-router false` indicates this is not in router-based federation.
  The source path must includes the source cluster.

### RBF Mode And Normal Federation Mode

  The federation balance tool has 2 modes: 
  
  * the router-based federation mode(rbf mode).
  * the normal federation mode.
  
  By default the command runs in the rbf mode. You can specify the rbf mode
  explicitly by using the option `-router true`. The option `-router false`
  specifies the normal federation mode.
  
  In the rbf mode the first parameter is taken as the mount point. It disables
  write by setting the mount point readonly.
  
  In the normal federation mode the first parameter is taken as the full path of
  the source. The first parameter must include the source cluster. It disables
  write by cancelling the execute permission of the source path.
  
  Details about disabling write see [DistCpFedBalance](#DistCpFedBalance).

### Command Options

Command `submit` has 5 options:

| Option key                     | Description                          |
| ------------------------------ | ------------------------------------ |
| -router | This option specifies the mode of the command. `True` indicates the router-based federation mode. `False` indicates the normal federation mode. |
| -forceCloseOpen | If `true`, the DIFF_DISTCP stage forces close all open files when there is no diff. Otherwise it waits until there is no open files. The default value is `false`. |
| -map | Max number of concurrent maps to use for copy. |
| -bandwidth | Specify bandwidth per map in MB. |
| -moveToTrash | If `true` move the source path to trash after the job is done. Otherwise delete the source path directly. |

### Configuration Options
--------------------

| Configuration key              | Description                          |
| ------------------------------ | ------------------------------------ |
| hadoop.hdfs.procedure.work.thread.num | The worker threads number of the BalanceProcedureScheduler. Default is `10`. |
| hadoop.hdfs.procedure.scheduler.journal.uri | The uri of the journal. |
| federation.balance.class | The class used for federation balance. Default is `org.apache.hadoop.tools.DistCpProcedure.` |

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
  * The worker threads take jobs and run them. Journals are written to storage.
  * If writing the journal fails, the job is added to the recoverQueue for later
    recovery. If Worker thread catches a RetryTaskException, it adds the job to
    the delayQueue.
  * Rooster thread takes job from delayQueue and adds it back to pendingQueue.
  * When a scheduler starts, it scans all the unfinished jobs from the journal
    and add them to the recoverQueue. The recover thread will recover them from
    the journal and add them back to the pendingQueue.

### DistCpFedBalance

  DistCpFedBalance is implemented as a job of the state machine. All the distcp
  balance logic are implemented here. A DistCpFedBalance job consists of 3
  procedures:

  * DistCpProcedure: This is the first procedure. It handles all the data copy
    works. There are 6 stages:
    * PRE_CHECK: Do the pre-check of the src and dst path.
    * INIT_DISTCP: Create a snapshot of the source path and distcp it to the
      target.
    * DIFF_DISTCP: Submit distcp with `-diff` round by round to sync source and
      target paths. If `-forceCloseOpen` is set, this stage will finish when
      there is no diff between src and dst. Otherwise this stage only finishes
      when there is no diff and no open files.  
    * DISABLE_WRITE: Disable write operations so the src won't be changed. When
      working in router mode, it is done by making the mount point readonly.
      Otherwise it is done by cancelling the execute permission of the source
      path.
    * FINAL_DISTCP: Force close all the open files and submit the final distcp.
    * FINISH: Cleanup works. If the execute permission is cancelled then
      restoring the permission of the dst path.
    
  * MountTableProcedure: This procedure updates the mount entry in Router. The 
    readonly is unset and the destination is updated of the mount point. This
    procedure is activated only when `-router` is true.

  * TrashProcedure: This procedure moves the source path to trash.

  After all 3 procedures finish, the balance job is done.