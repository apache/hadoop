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


# Manifest Committer Architecture

This document describes the architecture and other implementation/correctness
aspects of the [Manifest Committer](manifest_committer.html)

The _Intermediate Manifest_ committer is a new committer for
work which should deliver performance on ABFS
for "real world" queries, and performance and correctness on GCS.

This committer uses the extension point which came in for the S3A committers.
Users can declare a new committer factory for `abfs://` and `gcs://` URLs.
A suitably configured spark deployment will pick up the new committer.

## Background

### Terminology

| Term | Meaning|
|------|--------|
| Committer |  A class which can be invoked by MR Spark to perform the task and job commit operations. |
| Spark Driver | The spark process scheduling the work and choreographing the commit operation.|
| Job: in MapReduce | The entire application. In spark, this is a single stage in a chain of work |
| Job Attempt | A single attempt at a job. MR supports multiple Job attempts with recovery on partial job failure. Spark says "start again from scratch" |
| Task | a subsection of a job, such as processing one file, or one part of a file |
| Task ID |  ID of the task, unique within this job. Usually starts at 0 and is used in filenames (part-0000, part-001, etc.) |
| Task attempt (TA) | An attempt to perform a task. It may fail, in which case MR/spark will schedule another. |
| Task Attempt ID | A unique ID for the task attempt. The Task ID + an attempt counter.|
|  Destination directory | The final destination of work.|
| Job Attempt Directory | A temporary directory used by the job attempt. This is always _underneath_ the destination directory, so as to ensure it is in the same encryption zone as HDFS, storage volume in other filesystems, etc.|
| Task Attempt directory | (also known as "Task Attempt Working Directory"). Directory exclusive for each task attempt under which files are written |
| Task Commit | Taking the output of a Task Attempt and making it the final/exclusive result of that "successful" Task.|
| Job Commit | aggregating all the outputs of all committed tasks and producing the final results of the job. |

For Hive's classic hierarchical-directory-structured tables, job committing requires
the output of all committed tasks to be put into the correct location in the
directory tree.

### Correctness

The purpose of a committer is to ensure that the complete output of
a job ends up in the destination, even in the presence of failures of tasks.

- _Complete:_ the output includes the work of all successful tasks.
- _Exclusive:_ the output of unsuccessful tasks is not present.
- _Concurrent:_ When multiple tasks are committed in parallel the output is the same as when
  the task commits are serialized. This is not a requirement of Job Commit.
- _Abortable:_  jobs and tasks may be aborted prior to job commit, after which their output is not visible.
- _Continuity of correctness:_ once a job is committed, the output of any failed,
  aborted, or unsuccessful task MUST NO appear at some point in the future.

The v1 committer meets these requirements, relying on the task commit operation
being a single atomic directory rename of the task attempt's attempt dir into the job attempt dir.


It has two algorithms, v1 and v2. The v2 algorithm is not considered safe because it is
not _exclusive_.

The v1 algorithm is resilient to all forms of task failure, but slow
when committing the final aggregate output as it renames each newly created file
to the correct place in the table one by one.

## File Output Committer V1 and V2

### File Output Committer V1 and V2 Commit algorithms

#### Task attempt execution (V1 and V2)

job attempt directory in `$dest/__temporary/$jobAttemptId/` contains all output
of the job in progress every task attempt is allocated its own task attempt dir
`$dest/__temporary/$jobAttemptId/__temporary/$taskAttemptId`

All work for a task is written under the task attempt directory. If the output
is a deep tree with files at the root, the task attempt dir will end up with a
similar structure, with the files it has generated and the directories above
them.

### MapReduce V1 algorithm:

#### v1 Task commit

The task attempt dir is renamed directly underneath the job attempt dir

```
rename(
  $dest/__temporary/$jobAttemptId/__temporary/$taskAttemptId
  $dest/__temporary/$jobAttemptId/$taskId)
```

#### V1 Job Commit

For each committed task, all files underneath are renamed into the destination
directory, with a filename relative from the base directory of the task remapped
to that of the dest dir.

That is, everything under `$dest/__temporary/$jobAttemptId/$taskId` is converted
to a path under `$dest`.

A recursive treewalk identifies the paths to rename in each TA directory.
There's some optimisation if the task directory tree contains a subdirectory
directory which does not exist under the destination: in this case the whole
directory can be renamed. If the directory already exists, a file-by-file merge
takes place for that dir, with the action for subdirectories again depending on
the presence of the destination.

As a result, if the output of each task goes a separate final directory (e.g the
final partition is unique to a single task), the rename is O(1) for the dir,
irrespective of children. If the output is to be in the same dir as other
tasks (or updating existing directories), then the rename performance becomes O(
files).

Finally, a 0-byte `_SUCCESS` file is written.

### MapReduce V2 algorithm:

#### V2 Task commit

The files under the task attempt dir are renamed one by one into the destination
directory. There's no attempt at optimising directory renaming, because other
tasks may be committing their work at the same time. It is therefore `O(files)` +
the cost of listing the directory tree. Again: done with a recursive treewalk,
not a deep `listFiles(path, recursive=true)` API, which would be faster on HDFS
and (though not relevant here) S3.

#### V2 Job Commit

A 0-byte `_SUCCESS` file is written.

V2's job commit algorithm is clearly a lot faster —all the rename has already
taken place.

There is one small flaw with the v2 algorithm: it cannot cope with failures
during task commit.

### Why the V2 committer is incorrect/unsafe

If, for a Task T1, Task Attempt 1 (T1A1) fails before committing, the driver
will schedule a new attempt "T1A2", and commit it. All is good.

But: if T1A1 was given permission to commit and it failed during the commit
process, some of its output may have been written to the destination directory.

If attempt T1A2 was then told to commit, then if and only if its output had the
exact set of file names would any already-renamed files be overwritten. If
different filenames were generated, then the output would contain files of T1A1
and T1A2.

If T1A1 became partitioned during the commit process, then the job committer
would schedule another attempt and commit its work. However, if T1A1 still had
connectivity to the filesystem, it could still be renaming files. The output of
the two tasks could be intermingled even if the same filenames were used.

## The S3A Committers

The paper, [_A Zero-Rename Committer_](https://github.com/steveloughran/zero-rename-committer/releases/),
Loughran et. al., covers these committers

It also describes the commit problem, defines correctness, and describes the
algorithms of the v1 and v2 committers, as well as those of the S3A committers,
IBM Stocator committer and what we know of EMR's Spark committer.

The `hadoop-aws` JAR contains a pair of committers, "Staging" and "Magic". Both
of these are implementations of the same problem: safely and rapidly committing
work to an S3 object store.

The committers take advantage of the fact that S3 offers an atomic way to create
a file: the PUT request.

Files either exist or they don't. A file can be uploaded direct to its
destination, and it is only when the upload completes that the file is manifest
-overwriting any existing copy.

For large files, a multipart upload allows this upload operation to be split
into a series of POST requests

1 `initiate-upload (path -> upload ID)`
1. `upload part(path, upload ID, data[]) -> checksum.`
   This can be parallelised. Up to 10,000 parts can be uploaded to a single
   object. All but the final part must be >= 5MB.
1. `complete-upload (path, upload ID, List<checksum>)`
   this manifests the file, building it from the parts in the sequence of blocks
   defined by the ordering of the checksums.

The secret for the S3A committers is that the final POST request can be delayed
until the job commit phase, even though the files are uploaded during task
attempt execution/commit. The task attempts need to determine the final
destination of each file, upload the data as part of a multipart operation, then
save the information needed to complete the upload in a file which is later read
by the job committer and used in a POST request.

### Staging Committer

The Staging committer is based on the contribution by Ryan Blue of Netflix.
it  relies on HDFS to be the consistent store to propagate the `.pendingset` files.

The working directory of each task attempt is in the local filesystem, "the
staging directory". The information needed to complete the uploads is passed
from Task Attempts to the Job Committer by using a v1 FileOutputCommitter
working with the cluster HDFS filesystem. This ensures that the committer has
the same correctness guarantees as the v1 algorithm.

1. Task commit consists of uploading all files under the local filesystem's task
   attempt working directory to their final destination path, holding back on
   the final manifestation POST.
1. A JSON file containing all information needed to complete the upload of all
   files in the task attempt is written to the Job Attempt directory of of the
   wrapped committer working with HDFS.
1. Job commit: load in all the manifest files in the HDFS job attempt directory,
   then issued the POST request to complete the uploads. These are parallelised.


### The Magic Committer

The magic committer is purely-S3A and takes advantage and of
the fact the authorts could make changes within the file system client itself.

"Magic" paths are defined which, when opened for writing under, initiate a
multi-party upload to the final destination directory. When the output stream is
`close()`d, a zero byte marker file is written to the magic path, and a JSON
.pending file containing all the information needed to complete the upload is
saved.

Task commit:
1. list all `.pending` files under each task attempt's magic directory;
1. aggregate to a `.pendingset` file
1. save to the job attempt directory with the task ID.

Job commit:

1. list `.pendingset` files in the job attempt directory
1. complete the uploads with POST requests.

The Magic committer absolutely requires a consistent S3 Store -originally with
S3Guard. Now that S3 is consistent, raw S3 can be used. It does not need HDFS
or any other filesystem with `rename()`.

## Performance Correctness of the S3A committers

## Performance and Scalability.

The two S3A committers are different in task commit performance; job commit is
nearly identical.

_Staging_: files created in a task attempt are uploaded from the staging
directory to S3 in task commit. Although parallelized, this is limited to the
bandwidth of the VM., so its time is data-generated/bandwidth. Until task
commit, however, all IO is to the local FS, which is significantly faster.

_Magic_: files created in a task attempt are written directly and incrementally
to the object store. Task commit is a deep list of the magic directory `O(files)`,
a read of each `.pending` file (again, `O(files)`), and a write of the pendingset
file. For small files, this is ~ `O(1)`.

Job commit for both is the act of listing the job attempt directory (faster on
HDFS), then, in parallel, loading each .pendingset file and completing all
uploads. This is `O(files/threads)`.

In terasort benchmarks with third party S3-compatible object stores, the magic
committer is a clear winner. For real-world applications it is less clear cut
-task commit is generally not the bottleneck. Where the magic committer does
have an advantage is that it doesn't need much local storage (only enough for
buffering blocks being uploaded), so individual tasks can generate large amounts
of data.

Both committers use the same JSON `.pendingset` files containing the manifest of
work to complete: the list of files and the checksums of the parts. We are not
aware of the time to load these files being a bottleneck, nor has any task
generated so many files that the use of a non-streamable format has created
scale problems.

Terasort benchmarks at hundred of TB did trigger scale problems when the job
committer loaded all task's manifests into data structures for validation prior
to completing any upload. Incremental loading of manifest files in different
threads and immediate completion of uploads addressed this.
Terasorting, however, is not a real world use case. The scale problems encountered
there have never surfaced in the wild.

### Correctness

The S3A committer is considered correct because

1. Nothing is materialized until job commit.
1. Only one task attempt's manifest can be saved to the job attempt directory.
   Hence: only of the TA's files of the same task ID are exclusively committed.
1. The staging committer's use of HDFS to pass manifests from TAs to the Job
   committer ensures that S3's eventual consistency would not cause manifests to
   be missed.
1. Until S3 was consistent, the magic committer relied on S3Guard to provide the
   list consistency needed during both task- and job- commit.
1. The authors and wider community fixed all the issues related to the committers
   which have surfaced in production.

Significant issues which were fixed include:

* [HADOOP-15961](https://issues.apache.org/jira/browse/HADOOP-15961).
  S3A committers: make sure there's regular progress() calls.
* [HADOOP-16570](https://issues.apache.org/jira/browse/HADOOP-16570).
  S3A committers encounter scale issues.
* [HADOOP-16798](https://issues.apache.org/jira/browse/HADOOP-16798).
  S3A Committer thread pool shutdown problems.
* [HADOOP-17112](https://issues.apache.org/jira/browse/HADOOP-17112).
  S3A committers can't handle whitespace in paths.
* [HADOOP-17318](https://issues.apache.org/jira/browse/HADOOP-17318).
  Support concurrent S3A commit jobs with same app attempt ID.
* [HADOOP-17258](https://issues.apache.org/jira/browse/HADOOP-17258).
  MagicS3GuardCommitter fails with `pendingset` already exists
* [HADOOP-17414](https://issues.apache.org/jira/browse/HADOOP-17414]).
  Magic committer files don't have the count of bytes written collected by spark
* [SPARK-33230](https://issues.apache.org/jira/browse/SPARK-33230)
  Hadoop committers to get unique job ID in `spark.sql.sources.writeJobUUID`
* [SPARK-33402](https://issues.apache.org/jira/browse/SPARK-33402)
  Jobs launched in same second have duplicate MapReduce JobIDs
* [SPARK-33739](https://issues.apache.org/jira/browse/SPARK-33739]).
  Jobs committed through the S3A Magic committer don't report
  the bytes written (depends on HADOOP-17414)

Of those which affected the correctness rather than scale/performance/UX:
HADOOP-17258 involved the recovery from a failure after TA1 task commit had
completed —but had failed to report in. SPARK-33402, SPARK-33230 and
HADOOP-17318 are all related: if two spark jobs/stages started in the
same second, they had the same job ID. This caused the HDFS directories used by
the staging committers to be intermingled.

What is notable is this: these are all problems which the minimal integration
test suites did not or discover.

The good news: we now know of these issues and are better placed to avoid
replicating them again. And know what to write tests for.

## The V1 committer: slow in Azure and slow and unsafe on GCS.

The V1 committer underperforms on ABFS because:

1. Directory listing and file renaming is somewhat slower with ABFS than it is
   with HDFS.
1. The v1 committer sequentially commits the output of each task through a
   listing of each committed task's output, moving directories when none exist
   in the destination, merging files into extant directories.

The V2 committer is much faster in the job commit because it performs the list
and rename process in the task commit. Which, because it is non-atomic, is why
it is considered dangerous to use. What the V2 task commit algorithm does show is
that it is possible to parallelise committing the output of different tasks by
using file-by-file rename exclusively.

The V1 committer underperforms on GCS because even the task commit operation,
—directory rename—, is a non-atomic `O(files)` operation.
This also means that it is unsafe.

If the task attempt has partitioned and the spark driver schedules/commits another TA, then,
the task dir may contain 1+ file from the first attempt.

# The manifest Committer: A high performance committer for Spark on ABFS and GCS

Here then is a proposal for a higher performance committer for abfs and gcs storage
Note: it will also work well on `hdfs://` and indeed, `file://` URLs.

Although it will work with MapReduce
there is no handling of multiple job attempts with recovery from previous failed
attempts. (Plan: fail on MR AM restart)

A Manifest file is designed which contains (along with IOStatistics and some
other things)

1. A list of destination directories which must be created if they do not exist.
1. A list of files to rename as (absolute source, absolute destination,
   file-size) entries.

### Task Commit

Task attempts are committed by:

1. Recursively listing the task attempt working dir to build lists of
   directories to create and files to rename.
1. Saving this information in a manifest file in the job attempt directory with
   a filename derived from the Task ID. The task attempt ID is not used in the
   filename -only one task attempt may successfully write a manifest.

No renaming takes place —the files are left in their original location.

### Job Commit

Job Commit becomes:

1. List all manifest files in the job attempt directory.
1. Load each manifest file, create directories which do not yet exist, then
   rename each file in the rename list.
1. Save a JSON `_SUCCESS` file with the same format as the S3A committer (for
   testing; use write and rename for atomic save)

The job commit phase supports parallelization for many tasks and many files
per task, specifically:

1. Manifest tasks are loaded and processed in a pool of "manifest processor"
   threads.
1. Directory creation and file rename operations are processed in a pool of "
   executor" threads: many renames can execute in parallel as they use minimal
   network IO.
1. Each manifest processor thread reads in its manifest file, submits all
   directory creation operations to the "executor thread" pool; once those have
   completed the final renames themselves are submitted. Finally, a TA dir
   deletion operation can be submitted to the executors. Without even waiting
   for completion, the manifest processor thread can start to process the next
   manifest

Directory creation can be optimised by building a map of directories which have
already been created and their parents —there is no need to replicate work.

For each directory then:

1. Probe shared directory map for directory existing. If found: operation is
   complete.
1. if the map is empty, call `getFileStatus()` on the path. Not found: create
   directory, add entry and those of all parent paths Found and is directory:
   add entry and those of all parent paths Found and is file: delete. then
   create as before.

Handling concurrent creation of directories (or delete+create) is going to be a
troublespot. It's notable that duplicate operations are not harmful if, when
mkdir() returns false, the path is probed to see if it now exists. If it does,
another thread has created it.

### Benefits

* Pushes the source tree list operations into the task commit phase, which is
  generally off the critical path of execution
* Provides an atomic task commit to GCS, as there is no expectation that
  directory rename is atomic
* It is possible to pass IOStatistics from workers in manifest.
* Allows for some pre-rename operations similar to the S3A "Partitioned Staging
  committer". This can be configured to delete all existing entries in
  directories scheduled to be created -or fail if those partitions are
  non-empty.
  See [Partitioned Staging Committer](../../hadoop-aws/tools/hadoop-aws/committers.html#The_.E2.80.9CPartitioned.E2.80.9D_Staging_Committer)
* Allows for an optional preflight validation check (verify no duplicate files created by different tasks)
* Manifests can be viewed, size of output determined, etc, during
  development/debugging.

### Disadvantages

* Needs a new manifest file format.
* May makes task commit more complex.

This solution is necessary for GCS and should be beneficial on ABFS as listing
overheads are paid for in the task committers.

## Implementation Details

### Constraints

A key goal is to keep the manifest committer isolated and neither
touch the existing committer code nor other parts of the hadoop codebase.

It must plug directly into MR and Spark without needing any changes
other than already implemented for the S3A Committers

* Self-contained: MUST NOT require changes to hadoop-common, etc.
* Isolated: MUST NOT make changes to existing committers
* Integrated: MUST bind via `PathOutputCommitterFactory`.

As a result of this there's a bit of copy and paste from elsewhere,
e.g. `org.apache.hadoop.util.functional.TaskPool`
is based on S3ACommitter's `org.apache.hadoop.fs.s3a.commit.Tasks`.

The` _SUCCESS` file MUST be compatible with the S3A JSON file.
This is to ensure any existing test suites which validate
S3A committer output can be retargeted at jobs executed
by the manifest committer without any changes.


#### Progress callbacks in job commit.

When? Proposed: heartbeat until renaming finally finishes.

#### Error handling and aborting in job commit.

We would want to stop the entire job commit. Some atomic boolean "abort job"
would need to be checked in the processing of each task committer thread's
iteraton through a directory (or processing of each file?)
Failures in listing or renaming will need to be escalated to halting the entire
job commit. This implies that any IOE raised in asynchronous rename operation or
in a task committer thread must:

1. be caught
1. be stored in a shared field/variable
1. trigger the abort
1. be rethrown at the end of the `commitJob()` call

#### Avoiding deadlocks

If a job commit stage is using a thread pool for per-task operations, e.g. loading
files, that same thread pool MUST NOT be used for parallel operations within
the per-task stage.

As every `JobStage` is executed in sequence within task or job commit, it
is safe to share the same thread pool across stages.

In the current implementation, there is no parallel "per manifest" operation
in job commit other than for actually loading the files.
The operations to create directories and to rename files are actually
performed without performing parallel processing of individual manifests.

Directory Preparation: merge the directory lists of all manifests,
then queue for creation the (hopefully very much smaller) set of unique
directories.

Rname: iterate through all manifests and queue their renames into a pool for
renaming.
There's likely little benefit from parallelising the queueing of each
manifest, so it isn't.

#### Thread pool lifetimes

Review the issues related to the S3A committer and avoid (HADOOP-16798).

#### Scale issues similar to S3A HADOOP-16570.

This was a failure in terasorting where many tasks each generated many files;
the full list of files to commit (and the etag info) was built up in memory and
validated prior to execution. The fix: be more incremental, even if stopped a
preflight check of the entire job before committing any single file.

#### Concurrent jobs to same destination path

* [SPARK-33402](https://issues.apache.org/jira/browse/SPARK-33402) _Jobs
  launched in same second have duplicate MapReduce JobIDs_
* [HADOOP-17318](https://issues.apache.org/jira/browse/HADOOP-17318) _S3A committer to support concurrent jobs with same app attempt ID and dest
  dir_
* [SPARK-24552](https://issues.apache.org/jira/browse/SPARK-24552) _Task attempt
  numbers are reused when stages are retried_
* [SPARK-24589](https://issues.apache.org/jira/browse/SPARK-24589)
  _OutputCommitCoordinator may allow duplicate commits_
* [SPARK-33230](https://issues.apache.org/jira/browse/SPARK-33230)
  _FileOutputWriter jobs have duplicate JobIDs if launched in
  same second_


Unless the spark build has SPARK-33402 in, there's a risk of problems if >1 job
is launched targeting the same dest dir in the same second. We believe we have
also seen this surfacing in the original FileOutputCommitter against HDFS.

HADOOP-17318 was an emergency fix in the s3a committers to support spark builds
without the SPARK- fixes: job setup to create an ID, set it in the job conf and
rely on that updated conf being passed to the task attempts. Done to allow
existing Spark releases to work properly.


* Use `spark.sql.sources.writeJobUUID` if set as the ID of the job (not the
generated MR job attempt ID). Fallback to the YARN job attempt ID if
writeJobUUID unset
* Declare that job IDs passed down by spark MUST be unique and so each job attempt
dir being unique. (i.e. SPARK-33402 MUST be in, SPARK-33230 SHOULD be in)
* Job setup and task setup MUST fail if the job attempt temp paths exist (and are
non-empty?). This will at least ensure that problems surface early.

Spark releases also need SPARK-24552/SPARK-24589. This is a prerequisite out of scope
of this committer.

* [SPARK-24552](https://issues.apache.org/jira/browse/SPARK-24552) _Task attempt
  numbers are reused when stages are retried_
* [SPARK-24589](https://issues.apache.org/jira/browse/SPARK-24589) _
  OutputCommitCoordinator may allow duplicate commits_


#### Duplicate creation of directories in the dest dir

Combine all lists of directories to create and eliminate duplicates.

## Implementation Architecture

The implementation architecture reflects lessons from the S3A Connector.

* Isolate the commit stages from the MR commit class, as that's got a complex lifecycle.
* Instead, break up into series of _stages_ which can be tested in isolation
  and chained to provided the final protocol.
* Don't pass in MR data types (taskID etc) down into the stages -pass down a configuration
  with general types (string etc).
* Also pass in a callback for store operations, for ease of implementing a fake store.
* For each stage: define preconditions and postconditions, failure modes. Test in isolation.

#### Instrumentation/Statistics

1. Instrument using IOStatistics, tracking performance of every FS call made
   ( list, mkdir, rename...)
1. execution of every stage to be collected in a statistic too.
1. collect and aggregate IOStatistics published by FS class instances unique to
   individual threads (listing iterators in particular)
1. include counts of files per task, number of tasks, number of directories
   created
1. Save these statistics to the `_SUCCESS` file, as the S3A committers already do.
1. And log to the console
1. Use in assertions where there's perceived value.

## Auditing


When invoking the `ManifestCommitter` via the `PathOutputCommitter` API, the following
attributes are added to the active (thread) context

| Key | Value |
|-----|-------|
| `ji` | Job ID |
| `tai` | Task Attempt ID |
| `st` |  Stage |

These are also all set in all the helper threads performing work
as part of a stage's execution.

Any store/FS which supports auditing is able to collect this data
and include in their logs.

To ease backporting, all audit integration SHALL BE in its own isolated class;
easy to replace with no-op.