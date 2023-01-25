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

The protocol and its correctness are covered in [Manifest Committer Protocol](manifest_committer_protocol.html).
<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

The _Manifest_ committer is a committer for work which provides performance on ABFS for "real world"
queries, and performance and correctness on GCS.

This committer uses the extension point which came in for the S3A committers.
Users can declare a new committer factory for `abfs://` and `gcs://` URLs.
It can be used through Hadoop MapReduce and Apache Spark.

## Background

### Terminology

| Term | Meaning|
|------|--------|
| Committer |  A class which can be invoked by MR/Spark to perform the task and job commit operations. |
| Spark Driver | The spark process scheduling the work and choreographing the commit operation.|
| Job  | In MapReduce. the entire application. In spark, this is a single stage in a chain of work |
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



The purpose of a committer is to ensure that the complete output of
a job ends up in the destination, even in the presence of failures of tasks.

- _Complete:_ the output includes the work of all successful tasks.
- _Exclusive:_ the output of unsuccessful tasks is not present.
- _Concurrent:_ When multiple tasks are committed in parallel the output is the same as when
  the task commits are serialized. This is not a requirement of Job Commit.
- _Abortable:_  jobs and tasks may be aborted prior to job commit, after which their output is not visible.
- _Continuity of correctness:_ once a job is committed, the output of any failed,
  aborted, or unsuccessful task MUST NO appear at some point in the future.

For Hive's classic hierarchical-directory-structured tables, job committing
requires the output of all committed tasks to be put into the correct location
in the directory tree.

The committer built into `hadoop-mapreduce-client-core` module is the `FileOutputCommitter`.



## The Manifest Committer: A high performance committer for Spark on Azure and Google storage.

The Manifest Committer is a higher performance committer for ABFS and GCS storage
for jobs which create file across deep directory trees through many tasks.

It will also work on `hdfs://` and indeed, `file://` URLs, but
it is optimized to address listing and renaming performance and throttling
issues in cloud storage.

It *will not* work correctly with S3, because it relies on an atomic rename-no-overwrite
operation to commit the manifest file. It will also have the performance
problems of copying rather than moving all the generated data.

Although it will work with MapReduce
there is no handling of multiple job attempts with recovery from previous failed
attempts.

### The Manifest

A Manifest file is designed which contains (along with IOStatistics and some
other things)

1. A list of destination directories which must be created if they do not exist.
1. A list of files to rename, recorded as (absolute source, absolute destination,
   file-size) entries.

### Task Commit

Task attempts are committed by:

1. Recursively listing the task attempt working dir to build
   1. A list of directories under which files are renamed.
   2. A list of files to rename: source, destination, size and optionally, etag.
2. Saving this information in a manifest file in the job attempt directory with
   a filename derived from the Task ID.
   Note: writing to a temp file and then renaming to the final path will be used
   to ensure the manifest creation is atomic.


No renaming takes place —the files are left in their original location.

The directory treewalk is single-threaded, then it is `O(directories)`,
with each directory listing using one or more paged LIST calls.

This is simple, and for most tasks, the scan is off the critical path of the job.

Statistics analysis may justify moving to parallel scans in future.


### Job Commit

Job Commit consists of:

1. List all manifest files in the job attempt directory.
1. Load each manifest file, create directories which do not yet exist, then
   rename each file in the rename list.
1. Save a JSON `_SUCCESS` file with the same format as the S3A committer (for
   testing; use write and rename for atomic save)

The job commit phase supports parallelization for many tasks and many files
per task, specifically:

1. Manifest tasks are loaded and processed in a pool of "manifest processor"
   threads.
2. Directory creation and file rename operations are each processed in a pool of "
   executor" threads: many renames can execute in parallel as they use minimal
   network IO.
3. job cleanup can parallelize deletion of task attempt directories. This
   is relevant as directory deletion is `O(files)` on Google cloud storage,
   and also on ABFS when OAuth authentication is used.


### Ancestor directory preparation

Optional scan of all ancestors ...if any are files, delete.


### Parent directory creation

1. Probe shared directory map for directory existing. If found: operation is
   complete.
1. if the map is empty, call `getFileStatus()` on the path. Not found: create
   directory, add entry and those of all parent paths Found and is directory:
   add entry and those of all parent paths Found and is file: delete. then
   create as before.

Efficiently handling concurrent creation of directories (or delete+create) is going to be a
troublespot; some effort is invested there to build the set of directories to
create.

### File Rename

Files are renamed in parallel.

A pre-rename check for anything being at that path (and deleting it) will be optional.
With spark creating new UUIDs for each file, this isn't going to happen, and
saves HTTP requests.


### Validation

Optional scan of all committed files and verify length and, if known,
etag. For testing and diagnostics.

## Benefits

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

# Implementation Details

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

Rename: iterate through all manifests and queue their renames into a pool for
renaming.

#### Thread pool lifetimes

The lifespan of thread pools is constrained to that of the stage configuration,
which will be limited to within each of the `PathOutputCommitter` methods
to setup, commit, abort and cleanup.

This avoids the thread pool lifecycle problems of the S3A Committers.

#### Scale issues similar to S3A HADOOP-16570.

This was a failure in terasorting where many tasks each generated many files;
the full list of files to commit (and the etag of every block) was built up in memory and
validated prior to execution.

The manifest committer assumes that the amount of data being stored in memory is less,
because there is no longer the need to store an etag for every block of every
file being committed.


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

#### Statistics

The committer collects duration statistics on all the operations it performs/invokes
against filesystems.
* Those collected during task commit are saved to the manifest (excluding the time to
save and rename that file)
* When these manifests are loaded during job commit, these statistics are merged to
form aggregate statistics of the whole job.
* Which are saved to the `_SUCCESS` file
* and to any copy of that file in the directory specified by
  `mapreduce.manifest.committer.summary.report.directory`, if set.
  to be saved.
* The class `org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestPrinter`
  can load and print these.

IO statistics from filsystems and input and output streams used in a query are not
collected.


## Auditing

When invoking the `ManifestCommitter` via the `PathOutputCommitter` API, the following
attributes are added to the active (thread) context

| Key   | Value           |
|-------|-----------------|
| `ji`  | Job ID          |
| `tai` | Task Attempt ID |
| `st`  | Stage           |

These are also all set in all the helper threads performing work
as part of a stage's execution.

Any store/FS which supports auditing is able to collect this data
and include in their logs.

To ease backporting, all audit integration is in the single class
`org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration`.
