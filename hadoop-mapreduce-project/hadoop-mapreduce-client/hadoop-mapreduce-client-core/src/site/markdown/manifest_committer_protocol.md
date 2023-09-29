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


# Manifest Committer Protocol

This document describes the commit protocol
 of the [Manifest Committer](manifest_committer.html)

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

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
| Task Attempt directory | Directory under the Job Attempt Directory where task attempts create subdiretories for their own work |
| Task Attempt Working Directory| Directory exclusive for each task attempt under which files are written  |
| Task Commit | Taking the output of a Task Attempt and making it the final/exclusive result of that "successful" Task.  |
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


It has two algorithms, v1 and v2.

The v1 algorithm is resilient to all forms of task failure, but slow
when committing the final aggregate output as it renames each newly created file
to the correct place in the table one by one.

The v2 algorithm is not considered safe because the output is visible when individual
tasks commit, rather than being delayed until job commit.
It is possible for multiple task attempts to get their data into the output
directory tree, and if a job fails/is aborted before the job is committed,
thie output is visible.

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

Finally, a 0-byte `_SUCCESS` file is written iff `mapreduce.fileoutputcommitter.marksuccessfuljobs` is true.

### MapReduce V2 algorithm:

#### V2 Task commit

The files under the task attempt dir are renamed one by one into the destination
directory. There's no attempt at optimising directory renaming, because other
tasks may be committing their work at the same time. It is therefore `O(files)` +
the cost of listing the directory tree. Again: done with a recursive treewalk,
not a deep `listFiles(path, recursive=true)` API, which would be faster on HDFS
and (though not relevant here) S3.

#### V2 Job Commit

A 0-byte `_SUCCESS` file is written iff `mapreduce.fileoutputcommitter.marksuccessfuljobs`
is true.



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

## Background: the S3A Committers

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

The _Staging Committer_ is based on the contribution by Ryan Blue of Netflix.
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
   files in the task attempt is written to the Job Attempt directory of the
   wrapped committer working with HDFS.
1. Job commit: load in all the manifest files in the HDFS job attempt directory,
   then issued the POST request to complete the uploads. These are parallelised.


### The Magic Committer

The _Magic Committer_ is purely-S3A and takes advantage and of
the fact the authorts could make changes within the file system client itself.

"Magic" paths are defined which, when opened for writing under, initiate a
multi-party upload to the final destination directory. When the output stream is
`close()`d, a zero byte marker file is written to the magic path, and a JSON
.pending file containing all the information needed to complete the upload is
saved.

Task commit:
1. List all `.pending` files under each task attempt's magic directory;
1. Aggregate to a `.pendingset` file
1. Save to the job attempt directory with the task ID.

Job commit:

1. List `.pendingset` files in the job attempt directory
1. Complete the uploads with POST requests.

The Magic committer absolutely requires a consistent S3 Store -originally with
S3Guard. Now that S3 is consistent, raw S3 can be used. It does not need HDFS
or any other filesystem with `rename()`.

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
test suites did not discover.

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

----------------------------------------------------------------------------------------

# The Manifest Committer Protocol

## Requirements of the Store

Stores/filesystems supported by this committer MUST:

* Have consistent listings.
* Have an atomic `O(1)` file rename operation.

Stores/filesystems supported by this committer SHOULD:

* Rename files successfully, even under load. ABFS does not do this,
  so special recovery is provided there.
* Implement the `EtagSource` interface of HADOOP-17979.
  This is used for ABFS rename recovery, and for optional
  validation of the final output.

Stores/filesystems supported by this committer MAY:

* Have list operations with high latency.
* Reject calls under load with throttling responses,
  which MUST be handled in the filesystem connector.

Stores/filesystems supported by this committer MAY NOT:

* Support atomic directory rename. This is never used except optionally in cleanup.
* Support `O(1)` directory deletion. The `CleanupJobStage` assumes this is not
  the case and so deletes task attempt directories in parallel.
* Support an atomic `create(Path, overwrite=false)` operation.
  The manifests are committed by writing to a path including the task attempt ID,
  then renamed to their final path.
* Support fast `listFiles(path, recursive=true)` calls.
  This API call is not used.

When compared with the `FileOutputCommitter`, the requirements
which have been removed are:

* Atomic directory rename.
* `O(1)` directory deletion.
* Fast directory listings.
* The implicit absence of throttling behaviors.

HDFS meets all those requirements, so does not benefit significantly from
this committer, though it will still work there.

The S3 store does not meet the rename requirements of this committer,
even now that it is consistent.
This committer is not safe to use on S3.

### Task and Job IDs

Every job MUST have a unique ID.

The implementation expects the Spark runtime to have the relevant patches to
ensure this.

The job ID is used to name temporary directories, rather than using the classic
incrementing natural numbering scheme of `_temporary/0/`.
That scheme comes from MapReduce where job attempts of attempt ID &gt; 1
look for tasks committed by predecessors and incorporate that into their
results.

This committer targets Spark, where there is no attempt at recovery.
By using the job ID in paths, if jobs are configured to _not_ delete
all of `_temporary` in job cleanup/abort, then multiple jobs
MAY be executed using the same table as their destination.

Task IDs and Task Attempt IDs will be derived from Job IDs as usual.

It is expected that filenames of written files SHALL be unique.
This is done in Spark for ORC and Parquet files, and allows for
checks for destination files to be omitted by default.


## Directory Structure

Given a destination directory `destDir: Path`

A job of id `jobID: String` and attempt number `jobAttemptNumber:int`
will use the directory:

```
$destDir/_temporary/manifest_$jobID/$jobAttemptNumber/
```

For its work (note: it will actually format that final subdir with `%02d`).

This is termed the _Job Attempt Directory_

Under the Job Attempt Directory, a subdirectory `tasks` is
created. This is termed the _Task Attempt Directory_.
Every task attempt will have its own subdirectory of this,
into which its work will be saved.

Under the Job Attempt Directory, a subdirectory `manifests` is created.
This is termed the _y_.

The manifests of all committed tasks will be saved to this
directory with the filename of
`$taskId-manifest.json`

The full path

```
$destDir/_temporary/manifest_$jobID/$jobAttemptNumber/manifests/$taskId-manifest.json
```

Is the final location for the manifest of all files created by
a committed task. It is termed the _Manifest Path of a Committed Task_.

Task attempts will save their manifest into this directory with
a temporary filename
`$taskAttemptId-manifest.json.tmp`.

This is termed the _Temporary Path of a Task Attempt's Manifest_.

For the job and task operations then, the following paths are
defined.
```
let jobDirectory = "$destDir/_temporary/manifest_$jobID/"
let jobAttemptDirectory = jobDirectory + "$jobAttemptNumber/"
let manifestDirectory = jobAttemptDirectory + "manifests/"
let taskAttemptDirectory = jobAttemptDirectory + "tasks/"
```

And for each task attempt, the following paths are also defined

```
let taskAttemptWorkingDirectory = taskAttemptDirectory + "$taskAttemptId"
let taskManifestPath = manifestDirectory + "$taskId-manifest.json"
let taskAttemptTemporaryManifestPath = manifestDirectory + "$taskAttemptId-manifest.json"
```

## Core Algorithm of the Protocol

1. Each Task attempt writes all its files to a unique directory tree under the
   Task Attempt Directory.
2. Task Commit consists of a recursive scan of the directory for that task attempt,
   creating a list of directories and a list of files.
3. These lists are saved as a JSON manifest file.
4. Job commit consists of listing all of the JSON manifest files,
   loading their contents, creating the aggregate set of destination directories
   and renaming all files into their final destinations.


### The Intermediate Manifest

This is JSON file is designed which contains (along with IOStatistics and some diagnostics)

1. A list of destination directories which must be created if they do not exist.
1. A list of files to rename as (absolute source, absolute destination,
   file-size) entries.

### Job Setup

```
mkdir(jobAttemptDirectory)
mkdir(manifestDirectory)
mkdir(taskAttemptDirectory)
```

### Task Setup

```
mkdir(taskAttemptWorkingDirectory)
```

### Task Commit

Task attempts are committed by:

1. Recursively listing the task attempt working dir to build
   1. A list of destination directories under which files will be renamed,
      and their status (exists, not_found, file)
   2. A list of files to rename: source, destination, size and optionally, etag.
2. These lists populate a JSON file, the _Intermediate Manifest_.
3. The task attempt saves this file to its _Temporary Path of a Task Attempt's
   Manifest_.
4. The task attempt then deletes the _Manifest Path of a Committed Task_ and
   renames its own manifest file to that path.
5. If the rename succeeeds, the task commit is considered a success.

No renaming takes place at this point.: the files are left in their original location until
renamed in job commit.

```
let (renames, directories) = scan(taskAttemptWorkingDirectory)
let manifest = new Manifest(renames, directories)

manifest.save(taskAttemptTemporaryManifestPath)
rename(taskAttemptTemporaryManifestPath, taskManifestPath)
```

### Task Abort/cleanup

```
delete(taskAttemptWorkingDirectory)
```

### Job Commit

Job Commit consists of:

1. List all manifest files in the job attempt directory.
2. Load each manifest file, create directories which do not yet exist, then
   rename each file in the rename list.
3. Optionally save a JSON `_SUCCESS` file with the same format as the S3A committer (for
   testing; use write and rename for atomic save)

The job commit phase supports parallelization for many tasks and many files
per task, specifically there is a thread pool for parallel store IO

1. Manifest tasks are loaded and processed in parallel.
1. Deletion of files where directories are intended to be created.
1. Creation of leaf directories.
1. File rename.
1. In cleanup and abort: deletion of task attempt directories
1. If validation of output is enabled for testing/debugging: getFileStatus calls
   to compare file length and, if possible etags.

```
let manifestPaths = list("$manifestDirectory/*-manifest.json")
let manifests = manifestPaths.map(p -> loadManifest(p))
let directoriesToCreate = merge(manifests.directories)
let filesToRename = concat(manifests.files)

directoriesToCreate.map(p -> mkdirs(p))
filesToRename.map((src, dest, etag) -> rename(src, dest, etag))

if mapreduce.fileoutputcommitter.marksuccessfuljobs then
  success.save("$destDir/_SUCCESS")

```

Implementation Note:

To aid debugging and development, the summary be saved to a location
in the same _or different_ filesystem; the intermediate
manifests may be renamed to a location in the target filesystem.

```
if summary.report.directory != "" then
  success.save("${summary.report.directory}/$jobID.json")
if diagnostics.manifest.directory != null then
  rename($manifestDirectory, "${diagnostics.manifest.directory}/$jobID")
```

The summary report is saved even if job commit fails for any reason

### Job Abort/cleanup

Job cleanup is nominally one of deleting the job directory
```
delete(jobDirectory)
```

To address scale issues with the object stores, this SHALL be preceeded by
a (parallelized) deletion of all task attempt working directories

```
let taskAttemptWorkingDirectories = list("taskAttemptDirectory")
taskAttemptWorkingDirectories.map(p -> delete(p))
```


## Benefits of the new protocol

* Pushes the source tree list operations into the task commit phase, which is
  generally off the critical path of execution.
* Reduces the number of directories probed/created to the aggregate set of
  output directories, with all duplicates eliminated.
* File rename can be parallelized, with the limits being that of configured
  thread pool sizes and/or any rate limiting constraints.
* Provides an atomic task commit to GCS, as there is no expectation that
  directory rename is atomic.
* Permits pass IOStatistics from tasks attempts to the job committer via the manifests.
* Allows for some pre-rename operations in the Job Committer
  similar to the S3A "Partitioned Staging committer".
  This can be configured to delete all existing entries in
  directories scheduled to be created -or fail if those partitions are
  non-empty.
  See [Partitioned Staging Committer](../../hadoop-aws/tools/hadoop-aws/committers.html#The_.E2.80.9CPartitioned.E2.80.9D_Staging_Committer)
* Allows for an optional preflight validation check (verify no duplicate files created by different tasks).
* Manifests can be viewed, size of output determined, etc., during development/debugging.

## Disadvantages of the new protocol compared to the v1 algorithm

* Needs a new manifest file format.
* Manifests may get large if tasks create many files and/or subdirectories, or if
  etags are collected and the length of these tags is significant.
  The HTTP protocol limits each etag to 8 KiB, so the cost may be 8 KiB per file.
* Makes task commit more complex than the v1 algorithm.
* Possibly suboptimal on jobs where individual tasks create unique output directories,
  as directory rename will never be used to commit a directory.
