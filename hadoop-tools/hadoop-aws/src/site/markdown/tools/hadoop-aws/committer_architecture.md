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

# S3A Committers: Architecture and Implementation

<!-- DISABLEDMACRO{toc|fromDepth=0|toDepth=5} -->

This document covers the architecture and implementation details of the S3A committers.

For information on using the committers, see [the S3A Committers](./committer.html).


## Problem: Efficient, reliable commits of work to consistent S3 buckets


The standard commit algorithms (the `FileOutputCommitter` and its v1 and v2 algorithms)
rely on directory rename being an `O(1)` atomic operation: callers output their
work to temporary directories in the destination filesystem, then
rename these directories to the final destination as way of committing work.
This is the perfect solution for committing work against any filesystem with
consistent listing operations and where the `FileSystem.rename()` command
is an atomic `O(1)` operation.

Using rename allows individual tasks to work in temporary directories, with the
rename as the atomic operation can be used to explicitly commit tasks and
ultimately the entire job. Because the cost of the rename is low, it can be
performed during task and job commits with minimal delays. Note that HDFS
will lock the namenode metadata during the rename operation, so all rename() calls
will be serialized. However, as they only update the metadata of two directory
entries, the duration of the lock is low.

In contrast to a "real" filesystem, Amazon's S3A object store, similar to
most others, does not support `rename()` at all. A hash operation on the filename
determines the location of of the data —there is no separate metadata to change.
To mimic renaming, the Hadoop S3A client has to copy the data to a new object
with the destination filename, then delete the original entry. This copy
can be executed server-side, but as it does not complete until the in-cluster
copy has completed, it takes time proportional to the amount of data.

The rename overhead is the most visible issue, but it is not the most dangerous.
That is the fact that path listings have no consistency guarantees, and may
lag the addition or deletion of files.
If files are not listed, the commit operation will *not* copy them, and
so they will not appear in the final output.

The solution to this problem is closely coupled to the S3 protocol itself:
delayed completion of multi-part PUT operations

That is: tasks write all data as multipart uploads, *but delay the final
commit action until until the final, single job commit action.* Only that
data committed in the job commit action will be made visible; work from speculative
and failed tasks will not be instantiated. As there is no rename, there is no
delay while data is copied from a temporary directory to the final directory.
The duration of the commit will be the time needed to determine which commit operations
to construct, and to execute them.


## Terminology

* *Job*: a potentially parallelized query/operation to execute. The execution
of a job: the division of work into tasks and the management of their completion,
is generally executed in a single process.

The output of a Job is made visible to other stages in a larger operation
sequence or other applications if the job *completes successfully*.

* *Job Driver*. Not sure quite what term to use here. Whatever process schedules
task execution, tracks success/failures and, determines when all the work has been
processed and then commits the output. It may also determine that a job
has failed and cannot be recovered, in which case the job is aborted.
In MR and Tez, this is inside the YARN application master.
In Spark it is the driver, which can run in the AM, the YARN client, or other
places (e.g Livy?).

* *Final directory*: the directory into which the output of a job is placed
so as to be visible.

* *Task* a single operation within a job, on a single process, one which generates
one or more files.
After a successful job completion, the data MUST be visible in the final directory.
A task completes successfully if it generates all the output it expects to without
failing in some way (error in processing; network/process failure).

* *Job Context* an instance of the class `org.apache.hadoop.mapreduce.JobContext`,
which provides a read-only view of the Job for the Job Driver and tasks.

* *Task Attempt Context* an instance of the class
`org.apache.hadoop.mapreduce.TaskAttemptContext extends JobContext, Progressable`,
which provides operations for tasks, such as getting and setting status,
progress and counter values.

* *Task Working Directory*: a directory for exclusive access by a single task,
into which uncommitted work may be placed.

* *Task Commit* The act of taking the output of a task, as found in the
Task Working Directory, and making it visible in the final directory.
This is traditionally implemented via a `FileSystem.rename()` call.

  It is useful to differentiate between a *task-side commit*: an operation performed
  in the task process after its work, and a *driver-side task commit*, in which
  the Job driver performs the commit operation. Any task-side commit work will
  be performed across the cluster, and may take place off the critical part for
  job execution. However, unless the commit protocol requires all tasks to await
  a signal from the job driver, task-side commits cannot instantiate their output
  in the final directory. They may be used to promote the output of a successful
  task into a state ready for the job commit, addressing speculative execution
  and failures.

* *Job Commit* The act of taking all successfully completed tasks of a job,
and committing them. This process is generally non-atomic; as it is often
a serialized operation at the end of a job, its performance can be a bottleneck.

* *Task Abort* To cancel a task such that its data is not committed.

* *Job Abort* To cancel all work in a job: no task's work is committed.

* *Speculative Task Execution/ "Speculation"* Running multiple tasks against the same
input dataset in parallel, with the first task which completes being the one
which is considered successful. Its output SHALL be committed; the other task
SHALL be aborted. There's a requirement that a task can be executed in parallel,
and that the output of a task MUST NOT BE visible until the job is committed,
at the behest of the Job driver. There is the expectation that the output
SHOULD BE the same on each task, though that MAY NOT be the case. What matters
is if any instance of a speculative task is committed, the output MUST BE
considered valid.

There is an expectation that the Job Driver and tasks can communicate: if a task
performs any operations itself during the task commit phase, it shall only do
this when instructed by the Job Driver. Similarly, if a task is unable to
communicate its final status to the Job Driver, it MUST NOT commit is work.
This is very important when working with S3, as some network partitions could
isolate a task from the Job Driver, while the task retains access to S3.

## The execution workflow


**setup**:

* A job is created, assigned a Job ID (YARN?).
* For each attempt, an attempt ID is created, to build the job attempt ID.
* `Driver`: a `JobContext` is created/configured
* A committer instance is instantiated with the `JobContext`; `setupJob()` invoked.


## The `FileOutputCommitter`

The standard commit protocols are implemented in
 `org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter`.

There are two algorithms, the "v1" designed to address failures and restarts
of the MapReduce application master. The V2 algorithm cannot recover from failure
except by re-executing the entire job. It does, however, propagate all its work
to the output directory in the task commit. When working with object stores which
mimic `rename()` by copy and delete, it is more efficient due to the reduced
number of listings, copies and deletes, and, because these are executed in
task commits, eliminates the final `O(data)` pause at the end of all work.
It is still highly inefficient, but the inefficiencies are less visible as
large pauses in execution.


Notes

* The v1 algorithm was implemented to handle MapReduce AM restarts, it was
 not used in Hadoop 1.x, whose JobTracker could not recover from failures.
 Historically then, it is the second version of a file commit algorithm.
* Because the renames are considered to be fast, there is no logging
of renames being in progress, or their duration.
* Speculative execution is supported by having every task attempt write its
uncommitted data to a task attempt directory. When a task is ready to commit,
it must notify that job driver that it is ready to commit -the job driver
will then commit or abort the task.

## Hadoop MR Commit algorithm "1"


The "v1" commit algorithm is the default commit algorithm in Hadoop 2.x;
it was implemented as part of [MAPREDUCE-2702](https://issues.apache.org/jira/browse/MAPREDUCE-2702).

This algorithm is designed to handle a failure and restart of the Job driver,
with the restarted job driver only rerunning the incomplete tasks; the
output of the completed tasks is recovered for commitment when the restarted
job completes.

There is a cost: the time to commit by recursive listing all files in all
committed task directories, and renaming this.

As this is performed sequentially, time to commit is `O(files)`, which is
generally `O(tasks)`.



```python
#Job Attempt Path is `$dest/_temporary/$appAttemptId/`

jobAttemptPath = '$dest/_temporary/$appAttemptId/'

# Task Attempt Path is `$dest/_temporary/$appAttemptId/_temporary/$taskAttemptID`
taskAttemptPath = '$dest/_temporary/$appAttemptId/_temporary/$taskAttemptID'

#Task committed path is `$dest/_temporary/$appAttemptId/$taskAttemptID`
taskCommittedPath = '$dest/_temporary/$appAttemptId/$taskAttemptID'
```

Tasks write in/under the task attempt path.

### Job Setup

```python
fs.mkdir(jobAttemptPath)
```

### Task Setup

None: directories are created on demand.


### Task Commit

Rename task attempt path to task committed path.

```python

def needsTaskCommit(fs, jobAttemptPath, taskAttemptPath, dest):
  return fs.exists(taskAttemptPath)


def commitTask(fs, jobAttemptPath, taskAttemptPath, dest):
  if fs.exists(taskAttemptPath) :
    fs.delete(taskCommittedPath, recursive=True)
    fs.rename(taskAttemptPath, taskCommittedPath)
```

On a genuine filesystem this is an `O(1)` directory rename.

On an object store with a mimiced rename, it is `O(data)` for the copy,
along with overhead for listing and deleting all files (For S3, that's
`(1 + files/500)` lists, and the same number of delete calls.


### Task Abort

Delete task attempt path.

```python
def abortTask(fs, jobAttemptPath, taskAttemptPath, dest):
  fs.delete(taskAttemptPath, recursive=True)
```

On a genuine filesystem this is an `O(1)` operation. On an object store,
proportional to the time to list and delete files, usually in batches.


### Job Commit

Merge all files/directories in all task committed paths into final destination path.
Optionally; create 0-byte `_SUCCESS` file in destination path.

```python
def commitJob(fs, jobAttemptDir, dest):
  for committedTask in fs.listFiles(jobAttemptDir):
    mergePathsV1(fs, committedTask, dest)
  fs.touch("$dest/_SUCCESS")
```

(See below for details on `mergePaths()`)


A failure during job abort cannot be recovered from except by re-executing
the entire query:

```python
def isCommitJobRepeatable() :
  return True
```

Accordingly, it is a failure point in the protocol. With a low number of files
and fast rename/list algorithms, the window of vulnerability is low. At
scale, the vulnerability increases. It could actually be reduced through
parallel execution of the renaming of of committed tasks.


### Job Abort

Delete all data under job attempt path.

```python
def abortJob(fs, jobAttemptDir, dest):
  fs.delete(jobAttemptDir, recursive = True)
```

### Job Cleanup

```python
def cleanupJob(fs, dest):
  fs.delete('$dest/_temporary', recursive = True)
```


### Job Recovery

1. Data under task committed paths is retained
1. All directories under `$dest/_temporary/$appAttemptId/_temporary/` are deleted.

Uncommitted/unexecuted tasks are (re)executed.

This significantly improves time to recover from Job driver (here MR AM) failure.
The only lost work is that of all tasks in progress -those which had generated
data but were not yet committed.

Only the failure of the job driver requires a job restart, not an individual
task. Therefore the probability of this happening is independent of the number
of tasks executed in parallel, instead simply due to the duration of the query.

The longer the task, the higher the risk of failure, the more value there is
in recovering the work in progress.

Fast queries not only have a lower risk of failure, they can recover from
failure simply by rerunning the entire job. This is implicitly the strategy
in Spark, which does not attempt to recover any in-progress jobs. The faster
your queries, the simpler your recovery strategy needs to be.

### `mergePaths(FileSystem fs, FileStatus src, Path dest)` Algorithm

`mergePaths()` is the core algorithm to merge data; it is somewhat confusing
as the implementation mixes the strategies for both algorithms across
two co-recursive routines, `mergePaths()` and `renameOrMerge()`.


Here the two algorithms have been split, and one of the co-recursive methods
inlined.

```python
def mergePathsV1(fs, src, dest) :
  if fs.exists(dest) :
    toStat = fs.getFileStatus(dest)
  else:
    toStat = None

  if src.isFile :
    if not toStat is None :
      fs.delete(dest, recursive = True)
    fs.rename(src.getPath, dest)
  else :
    # destination is directory, choose action on source type
    if src.isDirectory :
      if not toStat is None :
        if not toStat.isDirectory :
          # Destination exists and is not a directory
          fs.delete(dest)
          fs.rename(src.getPath(), dest)
        else :
          # Destination exists and is a directory
          # merge all children under destination directory
          for child in fs.listStatus(src.getPath) :
            mergePathsV1(fs, child, dest + child.getName)
      else :
        # destination does not exist
        fs.rename(src.getPath(), dest)
```

## v2 commit algorithm


The v2 algorithm directly commits task output into the destination directory.
It is essentially a re-implementation of the Hadoop 1.x commit algorithm.

1. During execution, intermediate data becomes visible.
1. On a failure, all output must be deleted and the job restarted.

It implements `mergePaths` differently, as shown below.

```python
def mergePathsV2(fs, src, dest) :
  if fs.exists(dest) :
    toStat = fs.getFileStatus(dest)
  else:
    toStat = None

  if src.isFile :
    if not toStat is None :
      fs.delete(dest, recursive = True)
    fs.rename(src.getPath, dest)
  else :
    # destination is directory, choose action on source type
    if src.isDirectory :
      if not toStat is None :
        if not toStat.isDirectory :
          # Destination exists and is not a directory
          fs.delete(dest)
          fs.mkdirs(dest)                                    #
          for child in fs.listStatus(src.getPath) :          # HERE
            mergePathsV2(fs, child, dest + child.getName)    #

        else :
          # Destination exists and is a directory
          # merge all children under destination directory
          for child in fs.listStatus(src.getPath) :
            mergePathsV2(fs, child, dest + child.getName)
      else :
        # destination does not exist
        fs.mkdirs(dest)                                     #
        for child in fs.listStatus(src.getPath) :           # HERE
          mergePathsV2(fs, child, dest + child.getName)     #
```

Both recurse down any source directory tree, and commit single files
by renaming the files.

A a key difference is that the v1 algorithm commits a source directory to
via a directory rename, which is traditionally an `O(1)` operation.

In contrast, the v2 algorithm lists all direct children of a source directory
and recursively calls `mergePath()` on them, ultimately renaming the individual
files. As such, the number of renames it performs equals the number of source
*files*, rather than the number of source *directories*; the number of directory
listings being `O(depth(src))` , where `depth(path)` is a function returning the
depth of directories under the given path.

On a normal filesystem, the v2 merge algorithm is potentially more expensive
than the v1 algorithm. However, as the merging only takes place in task commit,
it is potentially less of a bottleneck in the entire execution process.

On an object store, it is suboptimal not just from its expectation that `rename()`
is an `O(1)` operation, but from its expectation that a recursive tree walk is
an efficient way to enumerate and act on a tree of data. If the algorithm was
switched to using `FileSystem.listFiles(path, recursive)` for a single call to
enumerate all children under a path, then the listing operation would be significantly
faster, at least on a deep or wide tree. However, for any realistic dataset,
the size of the output files is likely to be the main cause of delays. That
is, if the cost of `mergePathsV2` is `O(depth(src)) + O(data))`, then
generally the `O(data)` value will be more significant than the `depth(src)`.

There is one key exception: tests which work on small amounts of data yet try
to generate realistic output directory structures. In these tests the cost
of listing directories and calling `getFileStatus()` could exceed that of the copy
calls. This is why small-scale tests of the commit algorithms against object stores
must be considered significantly misleading.

### v2 Task Commit

Rename task attempt path to task committed path.

```python
def needsTaskCommit(fs, jobAttemptPath, taskAttemptPath, dest):
  return fs.exists(taskAttemptPath)

def commitTask(fs, jobAttemptPath, taskAttemptPath, dest):
  if fs.exists(taskAttemptPath) :
    mergePathsV2(fs. taskAttemptPath, dest)
```

### v2 Task Abort

Delete task attempt path.

```python
def abortTask(fs, jobAttemptPath, taskAttemptPath, dest):
  fs.delete(taskAttemptPath, recursive=True)
```

Cost: `O(1)` for normal filesystems, `O(files)` for object stores.


### v2 Job Commit

As all the task output is already completed, all that is needed is
to touch the `_SUCCESS` marker.

```python
def commitJob(fs, jobAttemptDir, dest):
  fs.touch("$dest/_SUCCESS")
```
Cost: `O(1)`

A failure during job abort is implicitly repeatable

```python
def isCommitJobRepeatable() :
  return True
```

### v2 Job Abort

Delete all data under job attempt path.

```python
def abortJob(fs, jobAttemptDir, dest):
  fs.delete(jobAttemptDir, recursive=True)
```

Cost: `O(1)` for normal filesystems, `O(files)` for object stores.

### v2 Task Recovery

As no data is written to the destination directory, a task can be cleaned up
by deleting the task attempt directory.

### v2 Job Recovery

Because the data has been renamed into the destination directory, it is nominally
recoverable. However, this assumes that the number and name of generated
files are constant on retried tasks.


## How MapReduce uses the committer in a task

MapReduce runs each Mapper or Reducer in its own container; it
gets its own process. Implicitly, this also means that each task gets
its own instance of every filesystem.

The work is choreographed in `org.apache.hadoop.mapred.Task`, for which there
are specific subclassing for the Map (`MapTask`) and reduce (`ReduceTask`).
There are also cleanup tasks whose role is simpler: clean things up,
and a Job setup task which runs `OutputCommittre.setupJob`. That is: even
the job setup phase is run in a Task.

MapTask uses the committer to write the output of all the mappers into
the filesystem, ready for the reducers. Each partition writes its data
as a `MapFile`, which is actually two `SequenceFile` files in a directory,:
the `data` file of all Key-Value
output, and `index` which contains an index of some of the keys in the file.

This is all written to the local filesystem.

The `ReduceTask` does the final write to the destination filesystem.

Because the Map phase uses a committer to commits intermediate work,
any plug in committer supplied to a process
through any extension mechanism *must* work with the output generated by a mapper.
The staging committers do only if unique filenames are disabled, but as they
and the magic committers are only meant to be used for the final output of work,
it is somewhat moot. What is important is to be able to use different committers
for the map phase as the final reduce. This is implicit if separate committers
are defined for different filesystems: the new committer can be defined for
the final destination FS, while `file://` can retain the default
`FileOutputCommitter`.

### Task Setup

`Task.initialize()`: read in the configuration, instantiate the `JobContextImpl`
and `TaskAttemptContextImpl` instances bonded to the current job & task.

### Task Ccommit

After the work is completed, `Task.done()` is invoked. Which is essentially the following
codepath:

```java
if (committer.needsTaskCommit(taskContext)) {
  // get permission to commit from AM
  int retries = MAX_RETRIES;
  while(true) {
    try {
      umbilical.commitPending(taskId, taskStatus);
      break;
    } catch(IOException ie) {
      if (--retries == 0) {
        // FAIL WITHOUT CALLING ABORT
        System.exit(67);
      }
    }
  }
  // commit the work
  try {
    committer.commitTask(taskContext);
  } catch (IOException iee) {
    // failure: abort
    try {
      committer.abortTask(taskContext);
    } catch (IOException ioe)  {
      LOG.warn("Failure cleaning up: " +
               StringUtils.stringifyException(ioe));
    }
    throw iee;
  }
}
```

That is: if and only if there is data to write, the Task requests clearance
to do so from the AM. This ensures that speculative work is not committed if
another task has written it, *and that even non-speculative work is not committed
if the task has lost contact with the AM*.

That is: task output is not committed if the Job's AM has failed or
is in a separate network partition from the Task.

If permission is granted, the commit call is invoked.
If this doesn't work, `abortTask()` is called; all
failures there are logged and swallowed.


This method actually appears to be limited in that it does not guarantee
that `committer.abortTask()` is always called: if the `umbilical.commitPending()` calls
fail repeatedly and the Task process aborts, `committer.abortTask()` is not called.
If this is due to a network partition, we can hope that the AM invokes
`committer.abortTask()`, and that if it is an AM failure then a restarted AM can
clean up its previous attempts. For the classic FileOutputCommitter, listing and
deleting the previous attempt's data is straightforward. However, for S3 committers
using Multipart Upload as the means of uploading uncommitted data, it is critical
to ensure that pending uploads are always aborted. This can be done by

* Making sure that all task-side failure branches in `Task.done()` call `committer.abortTask()`.
* Having job commit & abort cleaning up all pending multipart writes to the same directory
tree. That is: require that no other jobs are writing to the same tree, and so
list all pending operations and cancel them.
* Add a CLI command to verify that there are no commits under a path; returning
a non-zero exit code if there are. This can be used in testing.
* Add a CLI command to purge commits under a path. This can be used in workflows,
if felt necessary, and in test runs to isolate the output of different tests.
* Document a recommended purge-pending-commit timeout for the system
* Add probes in the integration tests to verify that pending uploads are never
present after a write.



### How MapReduce uses the committer in the Application Master

The AM uses a committer to set up and commit the job. To support failure
and recovery of the AM, `OutputCommitter.isRecoverySupported()`  is used
to declare whether all the output of successful tasks can be used in the final
job, or the entire job needs to be reset and repeated.
`OutputCommitter.isCommitJobRepeatable()` addresses the other question:
can a committer recover from a failure of the commit process itself.

A committer is created in the Application Master, and handed to an instance
of `org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventHandler`,
which then manages the job-level lifecycle.

If the MRv1 API is used, the committer is chosen from the value of
`"mapred.output.committer.class"`; in the MRv2 API the output format
is instantiated, then asked for a committer using a task and task attempt ID of
0. A committer is obtained from the output format via a call to
`Committer.getOutputCommitter(taskContext)`, again using the task attempt context
with the (job, 0, 0) task and attempt IDs. That is: even for the job committers,
a task context is always passed in to the `OutputFormat` when requesting a committer.
*It is critical for all implementations of `OutputCommitter.abortTask()` to
be able to execute from the AM, rather than the container and host running
the task. Furthermore, all information needed for the abort (paths, filesystem instances
&c) *must* be retrieved from the `TaskAttemptContext` passed to the method,
rather than relying on fields initiated from the context passed to the constructor.


#### AM: Job setup: `OutputCommitter.setupJob()`

This is initiated in `org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl.StartTransition`.
It is queued for asynchronous execution in `org.apache.hadoop.mapreduce.v2.app.MRAppMaster.startJobs()`,
which is invoked when the service is started. Thus: the job is set up when the
AM is started.


#### AM: Job Commit: `OutputCommitter.commitJob()`

In the "default"/cluster filesystem, the `CommitterEventHandler` uses data in
the staging area defined in `yarn.app.mapreduce.am.staging-dir`
(default `/tmp/hadoop-yarn/staging/$user/.staging`), in a subdirectory
named from the job ID.

Three paths are built up for using the filesystem for tracking the state
of the commit process, with a goal of making the commit operation recoverable,
where supported.


| name |  role |
|------|--------|
| `COMMIT_STARTED` | mark the commencement of the job commit |
| `COMMIT_SUCCESS` | mark the successful completion of the job commit |
| `COMMIT_FAIL` | mark the failure of the job commit |

These markers are used to manage job restart/failure of they happen during
the job commit itself.

When an AM starts up, it looks in its staging area for these files. as a way
to determine the previous state of the job. If there are no `COMMIT_` marker files,
the job is considered not to have attempted to commit itself yet.


The presence of `COMMIT_SUCCESS` or `COMMIT_FAIL` are taken as evidence
that the previous job completed successfully or unsuccessfully; the AM
then completes with a success/failure error code, without attempting to rerun
the job.

If `COMMIT_STARTED` exists but not either of the completion markers, then,
if the committer declares that its job commit operation is repeatable
(`Committer.isCommitJobRepeatable(jobContext) == true`), then an attempt
is made to recommit the job, deleting the `COMMIT_STARTED` and commencing
the commit process again.

These `COMMIT_STARTED` files are simply 0-byte files, but are created
with the overwrite bit only set to true if the job commit is considered
repeatable:

```java
private void touchz(Path p, boolean overwrite) throws IOException {
  fs.create(p, overwrite).close();
}
```
That is: the atomicity of the `create(path, overwrite=false)` on the cluster
filesystem is used to guarantee that only one process will attempt to commit
a specific job.

```java
boolean commitJobIsRepeatable = committer.isCommitJobRepeatable(
      event.getJobContext());
try {
  touchz(startCommitFile, commitJobIsRepeatable);
  waitForValidCommitWindow();
  committer.commitJob(event.getJobContext());
  touchz(endCommitSuccessFile, commitJobIsRepeatable);
} catch (Exception e) {
  touchz(endCommitFailureFile, commitJobIsRepeatable);
}
```

The `waitForValidCommitWindow()` operation is important: it declares that
the committer must not commit unless there has been communication with the YARN
Resource Manager with in `yarn.app.mapreduce.am.job.committer.commit-window` milliseconds
(default: 10,000). It does this by waiting until the next heartbeat it received.
There's a possible bug here: if the interval is set too small the thread may
permanently spin waiting a callback within the window. Ignoring that, this algorithm
guarantees that

1. As only one call can create a file with `overwrite=false`,
   only one process's attempt to commit a non-repeatable job will proceed

1. Only a process with contact with the YARN within the configured window
   may commit a job.

1. If the AM is partitioned from the rest of the network, provided that its clock
is monotonically increasing at the same rate as the rest of the cluster, then
the rest of the cluster can be confident that
`yarn.app.mapreduce.am.ob.committer.commit-window` milliseconds after the AM
successfully heartbeated to the YARN RM, then the output of this job attempt
will *never* be committed. This permits 1 job to run simultaneously, but
helps ensure that only one of them will attempt to commit.

1. Provided YARN heartbeats are only sent to the AM which successfully created
the `COMMIT_STARTED` file, it will initiate the commit operation.

Possible issues with this algorithm:

* The `COMMIT_STARTED` file is created before waiting to get a heartbeat. It
may be that this AM has lost contact with YARN, but doesn't know it yet. When the
YARN liveness protocols eventually time out, the AM will correctly terminate,
but as the `COMMIT_STARTED` file has been created at this point, no other launched
AM will be able to commit.

* If two committers attempt to create `COMMIT_STARTED` files on a no-repeatable
commit, one will succeed, wait for a heartbeat then attempt a (possibly slow) commit.
The second committer will fail, and will *immediately* create a `COMMIT_FAILED` file.
As a result, the state of the staging area will imply that the commit has failed,
when really it is in progress, and that only the second process failed.

It would seem safer to address this through
1. Waiting for a heartbeat before creating the `COMMIT_STARTED` file.
1. Maybe: not creating the `COMMIT_FAILED` file if the failure happens when
trying to create the `COMMIT_STARTED` file. That is: only a process which
successfully created the `COMMIT_STARTED` file may indicate that a commit has failed.


### AM: Cancelling job commit

The thread performing the commit is interrupted; the `CommitterEventHandler`
awaits for it to finish. (set in `yarn.app.mapreduce.am.job.committer.cancel-timeout`
as milliseconds).

### AM: Task Abort

The AM may call the `OutputCommitter.taskAbort()` with a task attempt context,
when handling the failure/loss of a container. That is: on container failure,
the task abort operation is executed in the AM, using the AM's committer.
This avoids the need to create a new container, and means that the "best-effort"
task abort does cope with container failures.

A partition between the AM and the task container means that this AM-executed
task abort may take place while a task in the partitioned container is still
executing. Unless output writing operations will fail after the abort operation,
the partitioned task may not become aware of the partition until it's own task
commit sequence in `Task.done()`, when `talkToAMTGetPermissionToCommit()`

# Requirements of an S3A Committer

1. Support an eventually consistent S3 object store as a reliable direct
destination of work through the S3A filesystem client.
1. Efficient: implies no rename, and a minimal amount of delay in the job driver's
task and job commit phases,
1. Support task failure and speculation.
1. Can be used by existing code: Hadoop MapReduce, Spark, Hive.
1. Retrofittable to existing subclasses of FileOutputFormat and/or compatible
with committers which expect a specific FileOutputFormat.
1. Clean up uncommitted data from all task attempts, all previous attempts of
the job, and any previous incompleted jobs.
1. Security: not to permit privilege escalation from other users with
write access to the same file system(s).


## Features of S3 and the S3A Client


A core problem is that
[object stores are not filesystems](../../../hadoop-project-dist/hadoop-common/filesystem/introduction.html);
how `rename()` has been emulated in the S3A client means that both the existing
MR committer algorithms have significant performance problems.

1. Single-object renames are implemented as a copy and delete sequence.
1. COPY is atomic, but overwrites cannot be prevented.
1. Amazon S3 is eventually consistent on listings, deletes and updates.
1. Amazon S3 has create consistency, however, the negative response of a HEAD/GET
performed on a path before an object was created can be cached, unintentionally
creating a create inconsistency. The S3A client library does perform such a check,
on `create()` and `rename()` to check the state of the destination path, and
so, whether the operation is permitted.
1. Multi-object renames are sequential or parallel single object COPY+DELETE operations:
non atomic, `O(data)` and, on failure, can leave the filesystem in an unknown
state.
1. There is a PUT operation, capable of uploading 5GB of data in one HTTP request.
1. The PUT operation is atomic, but there is no PUT-no-overwrite option.
1. There is a multipart POST/PUT sequence for uploading larger amounts of data
in a sequence of PUT requests.


The Hadoop S3A Filesystem client supports PUT and multipart PUT for uploading
data, with the `S3ABlockOutputStream` of HADOOP-13560 uploading written data
as parts of a multipart PUT once the threshold set in the configuration
parameter `fs.s3a.multipart.size` (default: 100MB).

[S3Guard](./s3guard.html) adds an option of consistent view of the filesystem
to all processes using the shared DynamoDB table as the authoritative store of
metadata. Some S3-compatible object stores are fully consistent; the
proposed algorithm is designed to work with such object stores without the
need for any DynamoDB tables.

## Related work: Spark's `DirectOutputCommitter`

One implementation to look at is the
[`DirectOutputCommitter` of Spark 1.6](https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/DirectParquetOutputCommitter.scala).

This implements a zero rename commit by subclassing the `ParquetOutputCommitter`
and then

1. Returning the destination directory as the task working directory.
1. Subclassing all the task commit/abort operations to be no-ops.

With the working directory as the destination directory, there is no need
to move/rename the task output on a successful commit. However, it is flawed.
There is no notion of "committing" or "aborting" a task, hence no ability to
handle speculative execution or failures. This is why the committer
was removed from Spark 2 [SPARK-10063](https://issues.apache.org/jira/browse/SPARK-10063)

There is also the issue that work-in-progress data is visible; this may or may
not be a problem.

## Related work: IBM's "Stocator" committer

IBM's [Stocator](https://github.com/SparkTC/stocator) can transform indirect
writes of V1/V2 committers into direct writes to the destination directory.

Hpw does it do this? It's a special Hadoop `FileSystem` implementation which
recognizes writes to `_temporary` paths and translate them to writes to the
base directory. As well as translating the write operation, it also supports
a `getFileStatus()` call on the original path, returning details on the file
at the final destination. This allows for committing applications to verify
the creation/existence/size of the written files (in contrast to the magic
committer covered below).

The FS targets Openstack Swift, though other object stores are supportable through
different backends.

This solution is innovative in that it appears to deliver the same semantics
(and hence failure modes) as the Spark Direct OutputCommitter, but which
does not need any change in either Spark *or* the Hadoop committers. In contrast,
the committers proposed here combines changing the Hadoop MR committers for
ease of pluggability, and offers a new committer exclusively for S3, one
strongly dependent upon and tightly integrated with the S3A Filesystem.

The simplicity of the Stocator committer is something to appreciate.

## Background: The S3 multi-part PUT mechanism

In the [S3 REST API](http://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html),
multipart uploads allow clients to upload a series of "Parts" of a file,
then commit the upload with a final call.

1. Caller initiates a multipart request, including the destination bucket, key
and metadata.

        POST bucket.s3.aws.com/path?uploads

    An UploadId is returned

1. Caller uploads one or more parts.

        PUT bucket.s3.aws.com/path?partNumber=PartNumber&uploadId=UploadId

    The part number is used to declare the ordering of the PUTs; they
    can be uploaded in parallel and out of order.
    All parts *excluding the final part* must be 5MB or larger.
    Every upload completes with an etag returned

1. Caller completes the operation

        POST /ObjectName?uploadId=UploadId
        <CompleteMultipartUpload>
          <Part><PartNumber>(number)<PartNumber><ETag>(Tag)</ETag></Part>
          ...
        </CompleteMultipartUpload>

    This final call lists the etags of all uploaded parts and the actual ordering
    of the parts within the object.

The completion operation is apparently `O(1)`; presumably the PUT requests
have already uploaded the data to the server(s) which will eventually be
serving up the data for the final path. All that is needed to complete
the upload is to construct an object by linking together the files in
the server's local filesystem and update an entry the index table of the
object store.

In the S3A client, all PUT calls in the sequence and the final commit are
initiated by the same process. *This does not have to be the case*.
It is that fact, that a different process may perform different parts
of the upload, which make this algorithm viable.


## The Netfix "Staging" committer

Ryan Blue, of Netflix, has submitted an alternate committer, one which has a
number of appealing features

* Doesn't have any requirements of the destination object store,
* Known to work.

The final point is not to be underestimated, es not even
a need for a consistency layer.
* Overall a simpler design. Especially given the need to
be resilient to the various failure modes which may arise.


The committer writes task outputs to a temporary directory on the local FS.
Task outputs are directed to the local FS by `getTaskAttemptPath` and `getWorkPath`.
On task commit, the committer enumerates files in the task attempt directory (ignoring hidden files).
Each file is uploaded to S3 using the [multi-part upload API](http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html),

The information needed to commit the upload is saved into HDFS and committed
via that protocol: when the job commits, the pending uploads of the successful
tasks are all committed.


### Commit logic

The core algorithm is as follows:

1. The destination directory for output (e.g. `FileOutputFormat` and subclasses)
is a local `file://` reference.
1. Task commit initiates the multipart PUT to the destination object store.
1. A list of every pending PUT for task is persisted to a single file
within a consistent, cluster-wide filesystem. For Netflix, that is HDFS.
1. The Standard `FileOutputCommitter` (algorithm 1) is used to manage the commit/abort of these
files. That is: it copies only those lists of files to commit from successful tasks
into a (transient) job commit directory.
1. The S3 job committer reads the pending file list for every task committed
in HDFS, and completes those put requests.

By using `FileOutputCommmitter` to manage the propagation of the lists of files
to commit, the existing commit algorithm implicitly becomes that defining which
files will be committed at the end of the job.


The Netflix contribution has Hadoop `OutputCommitter` implementations for S3.

There are 3 main classes:
* `S3MultipartOutputCommitter` is a base committer class that handles commit logic. This should not be used directly.
* `S3DirectoryOutputCommitter` for writing unpartitioned data to S3 with conflict resolution.
* `S3PartitionedOutputCommitter` for writing partitioned data to S3 with conflict resolution.

Callers should use `S3DirectoryOutputCommitter` for single-directory outputs,
or `S3PartitionedOutputCommitter` for partitioned data.


These S3 committers work by writing task outputs to a temporary directory on the local FS.
Task outputs are directed to the local FS by `getTaskAttemptPath` and `getWorkPath`.


### Conflict Resolution

The single-directory and partitioned committers handle conflict resolution by
checking whether target paths exist in S3 before uploading any data.
There are 3 conflict resolution modes, controlled by setting `fs.s3a.committer.staging.conflict-mode`:

* `fail`: Fail a task if an output directory or partition already exists. (Default)
* `append`: Upload data files without checking whether directories or partitions already exist.
* `replace`: If an output directory exists, delete it so the new data replaces the current content.

The partitioned committer enforces the conflict mode when a conflict is detected with output data, not before the job runs.
Conflict resolution differs from an output mode because it does not enforce the mode when there is no conflict.
For example, overwriting a partition should remove all sub-partitions and data it contains, whether or not new output is created.
Conflict resolution will only replace partitions that have output data.

When the conflict mode is `replace`, conflicting directories are removed during
job commit. Data is only deleted if all tasks have completed successfully.

A UUID that identifies a write is added to filenames that are uploaded to S3.
This allows rolling back data from jobs that fail during job commit (see failure cases below) and avoids
file-level conflicts when appending data to existing directories.


*Note* the checks for existence are made via `S3AFileSystem.getFileStatus()` requests of the destination paths.
Unless the view of the S3 store is consistent, it may be that a newly-deleted object
is still discovered in the probe, so a commit fail, even when there is no longer any actual conflict.

### Performance

Compared to the previous proposal, henceforth the "magic" committer, this
committer, the "staging committer", adds the extra overhead of uploading
each file at the end of every task. This is an `O(data)` operation; it can be
parallelized, but is bounded by the bandwidth from compute node to S3, as
well as the write/IOP capacity of the destination shard of S3. If many tasks
complete at or near the same time, there may be a peak of bandwidth load
slowing down the upload.

Time to commit will be the same, and, given the Netflix committer has already
implemented the parallelization logic here, a time of `O(files/threads)`.

### Resilience

There's already a lot of code in the task and job commits to handle failure.

Any failure in a commit triggers a best-effort abort/revert of the commit
actions for a task/job.

Task commits delegate to the `FileOutputCommitter` to ensure that only one task's
output reaches the job commit.

Similarly, if a task is aborted, temporary output on the local FS is removed.

If a task dies while the committer is running, it is possible for data to be
eft on the local FS or as unfinished parts in S3.
Unfinished upload parts in S3 are not visible to table readers and are cleaned
up following the rules in the target bucket's life-cycle policy.

Failures during job commit are handled by deleting any files that have already
been completed and aborting the remaining uploads.
Because uploads are completed individually, the files that are deleted were visible to readers.

If the process dies while the job committer is running, there are two possible failures:

1. Some directories that would be replaced have been deleted, but no new data is visible.
2. Some new data is visible but is not complete, and all replaced directories have been removed.
 Only complete files are visible.

If the process dies during job commit, cleaning up is a manual process.
File names include a UUID for each write so that files can be identified and removed.


**Failure during task execution**

All data is written to local temporary files; these need to be cleaned up.

The job must ensure that the local (pending) data is purged.

**Failure during task commit**


A process failure during the upload process will result in the
list of pending multipart PUTs to *not* be persisted to the cluster filesystem.
This window is smaller than the entire task execution, but still potentially
significant, at least for large uploads.

Per-file persistence, or incremental overwrites of the upload list may
reduce the problems here, but there would still be a small risk of
an outstanding multipart upload not being recorded

**Explicit Task abort before task commit**

Task will delete all local data; no uploads will be initiated.

**Failure to communicate with S3 during data upload**

If an upload fails, tasks will:

* Retry using the retry policies implemented in the S3AFileSystem classes
and the AWS libraries.
* Eventually: attempt to abort outstanding multipart uploads already initiated.
* Remove temporary files on the local FS.


**Explicit Job Abort**

All in-progress tasks are aborted and cleaned up. The pending commit data
of all completed tasks can be loaded, outstanding multipart PUT requests aborted.


This is done by

1. Listing all local files, with a best effort read attempt followed by
an abort of all successfully read files.
1. List and abort all pending multipart uploads.

Because of action #2, action #1 is superfluous. It is retained so as to leave
open the option of making action #2 a configurable option -which would be
required to handle the use case of >1 partitioned commit running simultaneously/

**Job Driver failure before Job Commit**

Because the local data is managed with the v1 commit algorithm, the
second attempt of the job will recover all the outstanding commit data
of the first attempt; those tasks will not be rerun.

This also ensures that on a job abort, the individual tasks' .pendingset
files can be read and used to initiate the abort of those uploads.
That is: a recovered job can clean up the pending writes of the previous job

If the query engine does not support multiple job attempts, then the
pending commit data will not be recovered; an explicit abort operation will
need to be initiated (we will add a CLI command for this), or the S3 bucket
must be configured to automatically delete the pending request.

**Job Driver failure during Job Commit**

Those uploads already executed by a failed job commit will persist; those
yet to execute will remain outstanding.

The committer currently declares itself as non-recoverable, but that
may not actually hold, as the recovery process could be one of:

1. Enumerate all job commits from the .pendingset files (*:= Commits*).
1. List all outstanding uploads under the destination path (*:= Outstandings*)..
1. List all written files (for better consistency, via a GET call on the known
filenames)
1. Identify all files which are yet to be completed (*Commits - Written*)/
1. Verify that the set of pending uploads matches (*Outstanding = (Commits - Written)*)

The main problem here is the progress of the job-commit-time conflict resolution
process: how to determine if it completed, as the only way to be confident
that all files in the destination directory are to be retained is by knowing
that the pre-commit phase completed. This could be implicitly determined
based on the rule "no uploads are committed until precommit is completed".
If it can be determined that 1+ upload has completed, then it could be inferred
that precommit had completed and so the job could be repeated.

This is dangerous territory to delve into. For now, the committer declares
itself as unrecoverable.

**Entire application failure before any task commit**

Data is left on local systems, in the temporary directories. This may
not be cleaned up.

**Entire application failure after one or more task commits, before job commit**

* A multipart PUT request will be outstanding for every pending write.
* A temporary directory in HDFS will list all known pending requests.

**Job complete/abort after >1 task failure**

1. All pending put data listed in the job completion directory needs to be loaded
and then cancelled.
1. Any other pending writes to the dest dir need to be enumerated and aborted.
This catches the situation of a task failure before the output is written.
1. All pending data in local dirs need to be deleted.

Issue: what about the destination directory: overwrite or not? It could well
depend upon the merge policy.


#### Overall Resilience

1. The only time that incomplete work will appear in the destination directory
is if the job commit operation fails partway through.
1. There's a risk of leakage of local filesystem data; this will need to
be managed in the response to a task failure.
1. There's a risk of uncommitted multipart PUT operations remaining outstanding,
operations which will run up bills until cancelled. (as indeed, so does the Magic Committer).


For cleaning up PUT commits, as well as scheduled GC of uncommitted writes, we
may want to consider having job setup list and cancel all pending commits
to the destination directory, on the assumption that these are from a previous
incomplete operation.

We should add a "commit" command to the S3guard CLI to probe for, list and abort pending requests under
a path, e.g. `--has-pending <path>`, `--list-pending <path>`, `--abort-pending <path>`.



## The "Magic" Committer

Development on this committer began before Netflix donated their committer.

By making changes to the `S3AFileSystem` and the `S3ABlockOutputStream`, this committer
manages to postpone the completion of writes of all files written to special
("magic") directories; the final destination of the write being altered to
that of the final job destination. When the job is committed, the pending
writes are instantiated.

With the addition of the Netflix Staging committer, the actual committer
code now shares common formats for the persistent metadata and shared routines
for parallel committing of work, including all the error handling based on
the Netflix experience.

It differs in that it directly streams data to S3 (there is no staging),
and it also stores the lists of pending commits in S3 too. That mandates
consistent metadata on S3, which S3Guard provides.


### Core concept: A new/modified output stream for delayed PUT commits


This algorithm uses a modified `S3ABlockOutputStream`  Output stream, which, rather
than commit any active multipart upload in the final `close()` operation,
it instead saves enough information into the S3 repository for an independent
process to be able to complete or abort the upload.

Originally, in `OutputStream.close()`, it chose whether to perform a single PUT or to
complete an ongoing multipart write.

If a multipart PUT is in progress, then the stream waits for the ongoing uploads
to complete (including any final block submitted), and then builds and PUTs
the final multipart commit operation. The list of parts (and their ordering)
has been built up during the opt

In contrast, when writing to a delayed-commit file:

1. A multipart write MUST always be initiated, even for small writes. This write
MAY be initiated during the creation of the stream.

1. Instead of committing the write in the `close()` call, perform a PUT to
a path in the S3A repository with all the information needed to commit the operation.
That is: the final path, the multipart upload ID, and the ordered list of etags
for the uploaded parts.


Recognising when a file is "special" is problematic; the normal `create(Path, Boolean)`
call must recognize when the file being created is to be a delayed-commit file,
so returning the special new stream.



This is done with a "magic" temporary directory name, `__magic`, to indicate that all files
created under this path are not to be completed during the stream write process.
Directories created under the path will still be created —this allows job- and
task-specific directories to be created for individual job and task attempts.

For example, the pattern `__magic/${jobID}/${taskId}` could be used to
store pending commits to the final directory for that specific task. If that
task is committed, all pending commit files stored in that path will be loaded
and used to commit the final uploads.

Consider a job with the final directory `/results/latest`

 The intermediate directory for the task 01 attempt 01 of job `job_400_1` would be

    /results/latest/__magic/job_400_1/_task_01_01

This would be returned as the temp directory.

When a client attempted to create the file
`/results/latest/__magic/job_400_1/task_01_01/latest.orc.lzo` , the S3A FS would initiate
a multipart request with the final destination of `/results/latest/latest.orc.lzo`.

As data was written to the output stream, it would be incrementally uploaded as
individual multipart PUT operations

On `close()`, summary data would be written to the file
`/results/latest/__magic/job400_1/task_01_01/latest.orc.lzo.pending`.
This would contain the upload ID and all the parts and etags of uploaded data.


#### Task commit

The information needed to commit a task is moved from the task attempt
to the job attempt.

1. The task commit operation lists all `.pending` files in its attempt directory.
1. The contents are loaded into a list of single pending uploads.
1. These are merged into to a single `Pendingset` structure.
1. Which is saved to a `.pendingset` file in the job attempt directory.
1. Finally, the task attempt directory is deleted. In the example, this
would be to `/results/latest/__magic/job400_1/task_01_01.pendingset`;


A failure to load any of the single pending upload files (i.e. the file
could not load or was considered invalid, the task is considered to
have failed. All successfully loaded pending commits will be aborted, then
the failure reported.

Similarly, a failure to save the `.pendingset` file will trigger an
abort of all its pending uploads.


#### Job Commit

The job committer loads all `.pendingset` files in its job attempt directory.

A failure to load any of these files is considered a job failure; all
pendingsets which could be loaded will be aborted.

If all pendingsets were loaded, then every
pending commit in the job will be committed. If any one of these commits
failed, then all successful commits will be reverted by deleting the destination
file.

#### Supporting directory trees

To allow tasks to generate data in subdirectories, a special filename `__base`
will be used to provide an extra cue as to the final path. When mapping an output
path  `/results/latest/__magic/job_400/task_01_01/__base/2017/2017-01-01.orc.lzo.pending`
to a final destination path, the path will become `/results/latest/2017/2017-01-01.orc.lzo`.
That is: all directories between `__magic` and `__base` inclusive will be ignored.


**Issues**

Q. What if there are some non-`.pending` files in the task attempt directory?

A. This can only happen if the magic committer is being used in an S3A client
which does not have the "magic path" feature enabled. This will be checked for
during job and task committer initialization.


### Failure cases

#### Network Partitioning

The job/task commit protocol is expected to handle this with the task
only committing work when the job driver tells it to. A network partition
should trigger the task committer's cancellation of the work (this is a protocol
above the committers).

#### Job Driver failure

The job will be restarted. When it completes it will delete all
outstanding requests to the destination directory which it has not
committed itself.

#### Task failure

The task will be restarted. Pending work of the task will not be committed;
when the job driver cleans up it will cancel pending writes under the directory.

#### Multiple jobs targeting the same destination directory

This leaves things in an indeterminate state.


#### Failure during task commit

Pending uploads will remain, but no changes will be visible.

If the `.pendingset` file has been saved to the job attempt directory, the
task has effectively committed, it has just failed to report to the
controller. This will cause complications during job commit, as there
may be two task PendingSet committing the same files, or committing
files with

*Proposed*: track task ID in pendingsets, recognise duplicates on load
and then respond by cancelling one set and committing the other. (or fail?)

#### Failure during job commit

The destination will be left in an unknown state.

#### Failure during task/job abort

Failures in the abort process are not well handled in either the committers
or indeed in the applications which use these committers. If an abort
operation fails, what can be done?

While somewhat hypothetical for the use case of a task being aborted due
to the protocol (e.g. speculative jobs being aborted), the abort task/abort job
calls may be made as part of the exception handling logic on a failure to commit.
As such, the caller may assume that the abort does not fail: if it does,
the newly thrown exception may hide the original problem.

Two options present themselves

1. Catch, log and swallow failures in the `abort()`
1. Throw the exceptions, and expect the callers to handle them: review, fix
and test that code as appropriate.

Fixing the calling code does seem to be the best strategy, as it allows the
failure to be explicitly handled in the commit protocol, rather than hidden
in the committer.::OpenFile

#### Preemption

Preemption is the explicit termination of work at the behest of the cluster
scheduler. It's a failure, but a special one: pre-empted tasks must not be counted
as a failure in any code which only allows a limited number of trackers, and the
Job driver can assume that the task was successfully terminated.

Job drivers themselves may be preempted.



#### Cleaning up after complete job failure

One failure case is that the entire execution framework failed; a new process
must identify outstanding jobs with pending work, and abort them, then delete
the appropriate `__magic` directories.

This can be done either by scanning the directory tree for `__magic` directories
and scanning underneath them, or by using the `listMultipartUploads()` call to
list multipart uploads under a path, then cancel them. The most efficient solution
may be to use `listMultipartUploads` to identify all outstanding request, and use that
to identify which requests to cancel, and where to scan for `__magic` directories.
This strategy should address scalability problems when working with repositories
with many millions of objects —rather than list all keys searching for those
with `/__magic/**/*.pending` in their name, work backwards from the active uploads to
the directories with the data.

We may also want to consider having a cleanup operation in the S3 CLI to
do the full tree scan and purge of pending items; give some statistics on
what was found. This will keep costs down and help us identify problems
related to cleanup.

### Performance

The time to upload is that of today's block upload (`s3a.fast.upload=true`)
output stream; ongoing through the write, and in the `close()` operation,
a delay to upload any pending data and await all outstanding uploads to complete.
There wouldn't be any overhead of the final completion request. If no
data had yet been uploaded, the `close()` time would be that of the initiate
multipart request and the final put. This could perhaps be simplified by always
requesting a multipart ID on stream creation.

The time to commit each task is `O(files)`: all `.pending` files in and under the task attempt
directory will be listed, their contents read and then an aggregate `.pendingset`
file PUT to the job attempt directory. The `.pending` files are then deleted.

The time to commit a job will be `O(files/threads)`

Every `.pendingset` file in the job attempt directory must be loaded, and a PUT
request issued for every incomplete upload listed in the files.

Note that it is the bulk listing of all children which is where full consistency
is required. If instead, the list of files to commit could be returned from
tasks to the job committer, as the Spark commit protocol allows, it would be
possible to commit data to an inconsistent object store.

### Cost

Uncommitted data in an incomplete multipart upload is billed at the storage
cost of the S3 bucket. To keep costs down, outstanding data from
failed jobs must be deleted. This can be done through S3 bucket lifecycle policies,
or some command tools which we would need to write.

### Limitations of this algorithm

1. Files will not be visible after the `close()` call, as they will not exist.
Any code which expected pending-commit files to be visible will fail.

1. Failures of tasks and jobs will leave outstanding multipart uploads. These
will need to be garbage collected. S3 now supports automated cleanup; S3A has
the option to do it on startup, and we plan for the `hadoop s3` command to
allow callers to explicitly do it. If tasks were to explicitly write the upload
ID of writes as a write commenced, cleanup by the job committer may be possible.

1. The time to write very small files may be higher than that of PUT and COPY.
We are ignoring this problem as not relevant in production; any attempt at optimizing
small file operations will only complicate development, maintenance and testing.

1. The files containing temporary information could be mistaken for actual
data.

1. It could potentially be harder to diagnose what is causing problems. Lots of
logging can help, especially with debug-level listing of the directory structure
of the temporary directories.

1. To reliably list all PUT requests outstanding, we need list consistency
In the absence of a means to reliably identify when an S3 endpoint is consistent, people
may still use eventually consistent stores, with the consequent loss of data.

1. If there is more than one job simultaneously writing to the same destination
directories, the output may get confused. This appears to hold today with the current
commit algorithms.

1. It is possible to create more than one client writing to the
same destination file within the same S3A client/task, either sequentially or in parallel.

1. Even with a consistent metadata store, if a job overwrites existing
files, then old data may still be visible to clients reading the data, until
the update has propagated to all replicas of the data.

1. If the operation is attempting to completely overwrite the contents of
a directory, then it is not going to work: the existing data will not be cleaned
up. A cleanup operation would need to be included in the job commit, deleting
all files in the destination directory which where not being overwritten.

1. It requires a path element, such as `__magic` which cannot be used
for any purpose other than for the storage of pending commit data.

1. Unless extra code is added to every FS operation, it will still be possible
to manipulate files under the `__magic` tree. That's not bad, it just potentially
confusing.

1. As written data is not materialized until the commit, it will not be possible
for any process to read or manipulated a file which it has just created.




### Changes to `S3ABlockOutputStream`

To avoid having to copy and paste the `S3ABlockOutputStream` it has
been modified to be constructed with a `PutTracker` class to
managed the immediate/delayed completion of uploads.
It will be called at appropriate points.

* Initialization, returning a marker to indicate whether or not multipart
upload is commence immediately.
* Multipart PUT init.
* Single put init (not used in this algorithm, but useful for completeness).
* Block upload init, failure and completion (from the relevant thread).
* `close()` entered; all blocks completed —returning a marker to indicate
whether any outstanding multipart should be committed.
* Multipart abort in `abort()` call (maybe: move core logic elsewhere).

The base implementation would do nothing
except declare that the MPU must be executed in the `close()` call.

The S3A Committer version, would
1. Always initiate a during initialization
1. In `close()` operation save all the data required to commit later.


## Integrating the Committers with Hadoop MapReduce


In order to support the ubiquitous `FileOutputFormat` and subclasses,
S3A Committers will need somehow be accepted as a valid committer by the class,
a class which explicitly expects the output committer to be `FileOutputCommitter`

```java
public Path getDefaultWorkFile(TaskAttemptContext context,
                               String extension) throws IOException{
  PathOutputCommitter committer =
    (PathOutputCommitter) getOutputCommitter(context);
  return new Path(committer.getWorkPath(), getUniqueFile(context,
    getOutputName(context), extension));
}

```

Here are some options which have been considered, explored and discarded

1. Adding more of a factory mechanism to create `FileOutputCommitter` instances;
subclass this for S3A output and return it. The complexity of `FileOutputCommitter`
and of supporting more dynamic construction makes this dangerous from an implementation
and maintenance perspective.

1. Add a new commit algorithm "3", which actually reads in the configured
classname of a committer which it then instantiates and then relays the commit
operations, passing in context information. Ths new committer interface would
add methods for methods and attributes. This is viable, but does still change
the existing Committer code in a way which may be high-maintenance.

1. Allow the `FileOutputFormat` class to take any task/job context committer
which implemented the `getWorkPath()` method —that being the sole
specific feature which it needs from the `FileOutputCommitter`.


Option 3, make `FileOutputFormat` support more generic committers, is the
current design. It relies on the fact that the sole specific method of
`FileOutputCommitter` which `FileOutputFormat` uses is `getWorkPath()`.

This can be pulled up into a new abstract class, `PathOutputCommitter`, which
`FileOutputCommitter` and `S3ACommitter` can implement:

```java
public abstract class PathOutputCommitter extends OutputCommitter {

  /**
   * Get the directory that the task should write results into.
   * @return the work directory
   */
  public abstract Path getWorkPath() throws IOException;
}
```

The sole change needed for `FileOutputFormat`  is to change what it casts
the context committer to:

```java
PathOutputCommitter committer =
  (PathOutputCommitter) getOutputCommitter(context);
```

Provided that `getWorkPath()` remains the sole method which `FileOutputFormat`
uses, these changes will allow an S3A committer to replace the `FileOutputCommitter`,
with minimal changes to the codebase.


Update: There is a cost to this: MRv1 API support is lost.


### MRv1 support via `org.apache.hadoop.mapred.FileOutputFormat`

A price of not subclassing `FileOutputCommitter` is that the code used
to wrap and relay the MRv1 API calls protocol to the `FileOutputCommitter`
will not work: the new committer will not be picked up.

This is visible in Spark, where the V1 API is exported from the `RDD` class
(`RDD.saveAsHadoopFile()`)). The successor code, `PairRDDFunctions.saveAsNewAPIHadoopFile()`
does work: *To get high performance commits in Object Stores, the MRv2 commit protocol
must be used, which means: the V2 classes.



#### Resolved issues


**Magic Committer: Name of directory**

The design proposes the name `__magic` for the directory. HDFS and
the various scanning routines always treat files and directories starting with `_`
as temporary/excluded data.


**Magic Committer: Subdirectories**

It is legal to create subdirectories in a task work directory, which
will then be moved into the destination directory, retaining that directory
tree.

That is, a if the task working dir is `dest/__magic/app1/task1/`, all files
under `dest/__magic/app1/task1/part-0000/` must end up under the path
`dest/part-0000/`.

This behavior is relied upon for the writing of intermediate map data in an MR
job.

This means it is not simply enough to strip off all elements of under `__magic`,
it is critical to determine the base path.

Proposed: use the special name `__base` as a marker of the base element for
committing. Under task attempts a `__base` dir is created and turned into the
working dir. All files created under this path will be committed to the destination
with a path relative to the base dir.

More formally: the last parent element of a path which is `__base` sets the
base for relative paths created underneath it.


## Testing

The committers can only be tested against an S3-compatible object store.

Although a consistent object store is a requirement for a production deployment
of the magic committer an inconsistent one has appeared to work during testing, simply by
adding some delays to the operations: a task commit does not succeed until
all the objects which it has PUT are visible in the LIST operation. Assuming
that further listings from the same process also show the objects, the job
committer will be able to list and commit the uploads.


The committers have some unit tests, and integration tests based on
the protocol integration test lifted from `org.apache.hadoop.mapreduce.lib.output.TestFileOutputCommitter`
to test various state transitions of the commit mechanism has been extended
to support the variants of the staging committer.

There is an abstract integration test, `AbstractITCommitMRJob` which creates
a MiniYARN cluster bonded to a MiniHDFS cluster, then submits a simple
MR job using the relevant committer. This verifies that the committer actually
works, rather than just "appears to follow the protocol"

One feature added during this testing is that the `_SUCCESS` marker file saved is
no-longer a 0-byte file, it is a JSON manifest file, as implemented in
`org.apache.hadoop.fs.s3a.commit.files.SuccessData`. This file includes
the committer used, the hostname performing the commit, timestamp data and
a list of paths committed.

```
SuccessData{
  committer='PartitionedStagingCommitter',
  hostname='devbox.local',
  description='Task committer attempt_1493832493956_0001_m_000000_0',
  date='Wed May 03 18:28:41 BST 2017',
  filenames=[/test/testMRJob/part-m-00000, /test/testMRJob/part-m-00002, /test/testMRJob/part-m-00001]
}
```

This was useful a means of verifying that the correct
committer had in fact been invoked in those forked processes: a 0-byte `_SUCCESS`
marker implied the classic `FileOutputCommitter` had been used; if it could be read
then it provides some details on the commit operation which are then used
in assertions in the test suite.

It has since been extended to collect metrics and other values, and has proven
equally useful in Spark integration testing.

## Integrating the Committers with Apache Spark


Spark defines a commit protocol `org.apache.spark.internal.io.FileCommitProtocol`,
implementing it in `HadoopMapReduceCommitProtocol` a subclass `SQLHadoopMapReduceCommitProtocol`
which supports the configurable declaration of the underlying Hadoop committer class,
and the `ManifestFileCommitProtocol` for Structured Streaming. The latter
is best defined as "a complication" —but without support for it, S3 cannot be used
as a reliable destination of stream checkpoints.

One aspect of the Spark commit protocol is that alongside the Hadoop file committer,
there's an API to request an absolute path as a target for a commit operation,
`newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String`;
each task's mapping of temp-> absolute files is passed to the Spark driver
in the `TaskCommitMessage` returned after a task performs its local
commit operations (which includes requesting permission to commit from the executor).
These temporary paths are renamed to the final absolute paths are renamed
in `FileCommitProtocol.commitJob()`. This is currently a serialized rename sequence
at the end of all other work. This use of absolute paths is used in writing
data into a destination directory tree whose directory names is driven by
partition names (year, month, etc).

Supporting that feature is going to be challenging; either we allow each directory in the partition tree to
have its own staging directory documenting pending PUT operations, or (better) a staging directory
tree is built off the base path, with all pending commits tracked in a matching directory
tree.

Alternatively, the fact that Spark tasks provide data to the job committer on their
completion means that a list of pending PUT commands could be built up, with the commit
operations being executed by an S3A-specific implementation of the `FileCommitProtocol`.
As noted earlier, this may permit the requirement for a consistent list operation
to be bypassed. It would still be important to list what was being written, as
it is needed to aid aborting work in failed tasks, but the list of files
created by successful tasks could be passed directly from the task to committer,
avoid that potentially-inconsistent list.


#### Spark, Parquet and the Spark SQL Commit mechanism

Spark's `org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat`
Parquet output format wants a subclass of `org.apache.parquet.hadoop.ParquetOutputCommitter`,
the option being defined by the classname in the configuration
key `spark.sql.parquet.output.committer.class`;
this is then patched in to the value `spark.sql.sources.outputCommitterClass`
where it is picked up by `SQLHadoopMapReduceCommitProtocol` and instantiated
as the committer for the work.

This is presumably done so the user has the option of requesting a metadata
summary file by setting the option `"parquet.enable.summary-metadata"`.
Creating the summary file requires scanning every single file in the destination
directory on the job commit, so is *very* expensive, and not something which
we recommend when working with S3.


To use a S3Guard committer, it must also be identified as the Parquet committer.
The fact that instances are dynamically instantiated somewhat complicates the process.

In early tests; we can switch committers for ORC output without making any changes
to the Spark code or configuration other than configuring the factory
for Path output committers.  For Parquet support, it may be sufficient to also declare
the classname of the specific committer (i.e not the factory).

This is unfortunate as it complicates dynamically selecting a committer protocol
based on the destination filesystem type or any per-bucket configuration.



The solution as implemented in the [initial prototype](https://github.com/hortonworks-spark/cloud-integration)
consists of two things

1. A class `PathOutputCommitProtocol extends HadoopMapReduceCommitProtocol`
which always creates the committer using the `PathOutputCommitterFactory`
mechanism. This ensures that output format's own committers are replaced
with an output factory mechanism.

1. A class `org.apache.hadoop.mapreduce.lib.output.BindingPathOutputCommitter`
which is a directly instantiable output committer that then creates a
committer through the factory mechanism and delegates all operations to
that committer. This allows a committer to be declared in any configuration
option which takes a committer class, but still use the factory mechanism
underneath.

1. Add a patch to Spark 2.3 [SPARK-21762], which allows any output committer
to be used for `ParquetFileOutput`.

Overall, its a bit convoluted to implement, document and use. Users must
declare two spark SQL options as well as three spark.hadoop ones

```properties
spark.sql.sources.commitProtocolClass=com.hortonworks.spark.cloud.commit.PathOutputCommitProtocol
spark.sql.parquet.output.committer.class=org.apache.hadoop.mapreduce.lib.output.BindingPathOutputCommitter
spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory
spark.hadoop.fs.s3a.committer.name=magic
spark.hadoop.fs.s3a.committer.magic.enabled=true
```

We could actually simplify this by adding a new algorithm "3" to the existing
`FileOutputFormat`, telling `FileOutputFormat` itself to use the factory
to create committers. This would then automatically pick up the factory
Avoiding loops in this situation would be "challenging": If instantiated
via a factory, the file committer must not attempt to use the factory
itself.


## Security

What are the obvious possible security risks which need to be covered
and which code reviews should check for?

* Leakage of AWS credentials in jobs/logging.
* Exposure of data to unauthorized users.
* Exposure of workflow to unauthorized users (e.g. paths & filenames, sequence
of queries).
* Silent tampering of data by unauthed users.
* Attacks on jobs which run up (large) bills by leaking pending datasets.
* DoS attacks with malicious users sabotaging/hindering progress of other
users' work.

#### Security Risks of the Staging Committers

Staged data is on local FS, in directories listed by `fs.s3a.buffer.dir`,
falling back to the directory in `hadoop.tmp.dir`.
These are the same directories already
used to buffer blocks of data being written in output streams, prior to
PUT/POST to S3. Therefore: there is more risk than before. We should
clearly document the security aspects of the temp dirs to ensure this.

As all files written by a task are not uploaded until task commit, more
local storage is needed. A malicious user executing work on one system
could potentially fill up the temp disk space. Mitigation: storage
quotas in local FS, keeping temp dirs on different mounted FS from root.

The intermediate `.pendingset` files are saved in HDFS under the directory in
`fs.s3a.committer.staging.tmp.path`; defaulting to  `/tmp`. This data can
disclose the workflow (it contains the destination paths & amount of data
generated), and if deleted, breaks the job. If malicious code were to edit
the file, by, for example, reordering the ordered etag list, the generated
data would be committed out of order, creating invalid files. As this is
the (usually transient) cluster FS, any user in the cluster has the potential
to do this.

*Locking down the temporary directories is critical to prevent malicious
cluster users damaging the workflow and output.*

#### Security Risks of the Magic Committer

The directory defined by `fs.s3a.buffer.dir` is used to buffer blocks
before upload, unless the job is configured to buffer the blocks in memory.
This is as before: no incremental risk. As blocks are deleted from the filesystem
after upload, the amount of storage needed is determined by the data generation
bandwidth and the data upload bandwidth.

No use is made of the cluster filesystem; there are no risks there.

A consistent store is required, which, for Amazon's infrastructure, means S3Guard.
This is covered below.

A malicious user with write access to the `__magic` directory could manipulate
or delete the metadata of pending uploads, or potentially inject new work int
the commit. Having access to the `__magic` directory implies write access
to the parent destination directory: a malicious user could just as easily
manipulate the final output, without needing to attack the committer's intermediate
files.


### Security Risks of all committers


#### Visibility

* If S3Guard is used for storing metadata, then the metadata is visible to
all users with read access. A malicious user with write access could delete
entries of newly generated files, so they would not be visible.


#### Malicious Serialized Data

The internal metadata summary files (`.pending` and `.pendingset`)
could be tampered by someone malicious, and, when parsed or interpreted by
the trusted account, used to execute untrusted code, fail, etc.
The formats are all JSON, parsed with Jackson; we expect invalid JSON
to result in parse errors, and fail the job. Aborting the job triggers a
best-effort attempt to load the pending files, ignoring those which cannot
be loaded or parsed, and aborts all pending uploads identified by the loaded
files.

* None of the strings in the parsed dataset are passed through any interpreter
(e.g used in SQL queries, shell commands or similar). Therefore (SQL) injection
attacks are not possible.

* Some of the data *may* be printed in the log files generated during
process execution. For example, commit Id, destination Key, etc. These
are all treated as plain text, and should be served up as such in any browser-
hosted view of the logs.

* Some of the data is returned in the `toString()` values of the loaded
classes. This may also be logged, observed in IDEs.

* None of the content in the serialized data is displayed in any web UI.
The vulnerability there is what if this happened in the future in some
downstream application: would it be possible to inject Javascript into
any of the text fields, script which could then be executed in some XSS
attack. We may wish to consider sanitizing this data on load.

* Paths in tampered data could be modified in an attempt to commit an upload across
an existing file, or the MPU ID alterated to prematurely commit a different upload.
These attempts will not going to succeed, because the destination
path of the upload is declared on the initial POST to initiate the MPU, and
operations associated with the MPU must also declare the path: if the path and
ID are inconsistent, the operation will fail. If a valid (path, MPU) tuple
were inserted in a tampered-with file, the commit or abort could complete or abort
the upload prematurely. As the malicious party must already have enough access to the
target S3 store to obtain this information, they are unlikely to need to
tamper with the JSON files to perform such actions.


#### Outstanding uncommitted data and its cost

* Killed jobs will leak uncommitted uploads, which will run up bills.
A restarted job will automatically purge
all pending uploads under the destination path on job commit, so if the job
is rerun it will cancel the pending writes of the previous job attempt.

We shall also provide a CLI tool to list and delete pending uploads under a path.

* configuring a store to automatically clean pending uploads after a
time period such as 24h will guarantee that pending upload data will always
be deleted, even without a rerun of the job or use of the CLI tool.

AWS document [the permissions for MPU](http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html).
There are special permissions `s3:ListBucketMultipartUploads`,
`s3:ListMultipartUploadParts` and `s3:AbortMultipartUpload`. By default
bucket owner has all these permissions, MPU initiator will be granted
the permissions to list the upload, list hte parts and abort their own uploads.
Bucket owner may grant/deny perms to either (this may permit a user to be
able to initiate & complete MPU, but not delete and abort).

#### Proposed security settings & recommendations

* Bucket access restricted to specific IAM roles.
* `fs.s3a.buffer.dir` set to location under `/tmp` with read & write access
restricted to the active user.
* `fs.s3a.committer.staging.tmp.path` should be isolated to the active
each user. Proposed: make the default an unqualified path, `tmp/staging`,
which will made absolute relative to the current user. In filesystems in
which access under user's home directories are restricted, this final, absolute
path, will not be visible to untrusted accounts.

* Maybe: define the for valid characters in a text strings, and a regex for
 validating, e,g, `[a-zA-Z0-9 \.\,\(\) \-\+]+` and then validate any free text
 JSON fields on load and save.
