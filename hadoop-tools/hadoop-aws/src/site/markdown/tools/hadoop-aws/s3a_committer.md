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

# The S3Guard Committers

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

This page covers the S3Guard Committers, which can commit work directly
to an S3 object store.

These committers are designed to solve a fundamental problem which
the standard committers of work cannot do to S3: consistent, high performance,
reliable commitment of work done by individual workers into the final set of
results of a job.

For details on their internal design, see
[S3A Committers: Architecture and Implementation](s3a_committer_architecture.html).



## Introduction: Using the "classic" committers to write to Amazon S3 is dangerous

Normally, Hadoop uses the `FileOutputFormatCommitter` to manage the
promotion of files created in a single task attempt to the final output of
a query. This is done in a way to handle failures of tasks and jobs, and to
support speculative execution. It does that by listing directories and renaming
their content into the final destination when tasks and then jobs are committed.

This has some key requirement of the underlying filesystem

1. When you list a directory, you see all the files which have been created in it,
and no files which are not in it (i.e. have been deleted).
1. When you rename a directory, it is an `O(1)` atomic transaction. No other
process across the cluster may rename a file or directory to the same path.
If the rename fails for any reason, either the data is at the original location,
or it is at the destination, -in which case the rename actually successed.

The `s3a://` filesystem client cannot meet these requirements.

1. Amazon S3 has inconsistent directory listings unless S3Guard is enabled.
1. The S3A mimics `rename()` by copying files and then deleting the originals.
This can fail partway through, and there is nothing to prevent any other process
in the cluster attempting a rename at the same time

As a result,

* Files my not be listed, hence not renamed into place.
* Deleted files may still be discovered, confusing the rename process to the point
of failure.
* If a rename fails, the data is left in an unknown state.
* If more than one process attempts to commit work simultaneously, the output
directory may contain the results of both processes: it is no longer an exclusive
operation.

To address this problem there is now explicit support in the `hadop-aws`
module for committing work to Amazon S3 via the S3A filesystem client.



### Background : the "commit protocol"

How exactly is work written to its final destination? That is accomplished by
a "commit protocol" between the workers and the job manager.

This protocol is implemented in Hadoop MapReduce, with a similar but extended
version in Apache Spark

1. A "Job" is the entire query, with inputs to outputs
1. The "Job Manager" is the process in charge of choreographing the execution
of the job. It may perform some of the actual computation too.  
1. The job has "workers", which are processes which work the actual data
and write the results.
1. Workers execute "Tasks", which are fractiona of the job, a job whose
input has been *partitioned* into units of work which can be executed independently.
1. The Job Manager directs workers to execute "tasks", usually trying to schedule
the work close to the data (if the filesystem provides locality information).
1. Workers can fail: the Job manager needs to detect this and reschedule their active tasks.
1. The output of a failed task must not be visible; this is to avoid its
data getting into the final output.
1. Multiple workers can be instructed to evaluate the same partition of the work;
this "speculation" delivers speedup as it can address the "straggler problem".
When multiple workers are working on the same data, only one worker is allowed
to write the final output.
1. The entire job may fail (often from the failure of the Job Manager (MR Master, Spark Driver, ...)).
1. Restarted jobs may recover from a failure by reusing the output of all
completed tasks (MapReduce with the "v1" algorithm), or just by rerunning everything
(The "v2" algorithm and Spark).

What is "the commit protocol" then? It is the requirements on workers as to
when their data is made visible, where, for a filesystem, "visible" means "can
be seen in the destination directory of the query."

* There is a destination directory of work, "the output directory."
* The final output of tasks must be in this directory *or paths underneath it*. 
* The intermediate output of a task must not be visible in the destination directory.
That is: they must not write directly to the destination.
* The final output of a task *may* be visible under the destination. 
* The Job Manager makes the decision as to whether a task's data is to be "committed",
be it directly to the final directory or to some intermediate store..
* Individual workers communicate with the Job manager to manage the commit process:
whether the output is to be *committed* or *aborted* 
* When a worker commits the output of a task, it somehow promotes its intermediate work to becoming
final.
* When a worker aborts a task's output, that output must not become visible
(i.e. it is not committed).
* Jobs themselves may be committed/aborted (the nature of "when" is not covered here).
* After a Job is committed, all its work must be visible.
* And a file `_SUCCESS` may be written to the output directory.
* After a Job is aborted, all its intermediate data is lost.
* Jobs may also fail. When restarted, the successor job must be able to clean up
all the intermediate and committed work of its predecessor(s).

That's "essentially" it. When working with HDFS and similar filesystems, 
directory `rename()` is the mechanism used to commit the work of tasks and
jobs.
* Tasks write data to task attempt directories under the directory `_temporary`
underneath the final destination directory.
* When a task is committed, these files are renamed to the destination directory
(v2 algorithm) or a job attempt directory under `_temporary` (v1 algorithm).
* When a job is committed, for the v2 algorithm the `_SUCCESS` file is created,
and the `_temporary` deleted.
* For the v1 algorithm, when a job is committed, all the tasks committed under
the job attempt directory will have their output renamed into the destination
directory.
* The v2 algorithm recovers from failure by deleting the destination directory
and restarting the job.
* The v1 algorithm recovers from failure by discovering all committed tasks
whose output is in the job attempt directory, *and only rerunning all uncommitted tasks*.


None of this algorithm works safely or swiftly when working with "raw" AWS S3
* directory listing can be inconsistent: the tasks and jobs may not list all work to
be committed. 
* renames go from being fast, atomic operations to slow operations which can fail partway through.


This then is the problem which the S3Guard committers address: how to safely
and reliably commit work to Amazon S3 or compatible object store.


## Meet the S3Guard Commmitters


There are two new commit mechanisms for writing data, *the staging committer*
and *the magic committer*.

The underlying architecture of these committers is quite complex, and
covered in [the committer architecture documentation](./s3a_committer_architecture.html).

The key concept to know of is S3's "Multipart upload mechanism", which allows
an S3 client to write data to S3 in multiple HTTP POST requests, only completing
the write operation with a final POST to complete the upload; this final POST
consisting of a short list of the etags of the uploaded blocks.
This multipart upload mechanism is already automatically used when writing large
amounts of data to S3; an implementation detail of the S3A output stream.

The S3Guard committers make explicit use of this multipart upload mechanism:

1. The individual *tasks* in a job write their data to S3 as POST operations
within multipart uploads, yet do not issue the final POST to complete the upload.
1. The multipart uploads are committed in the job commit process.

The committers primarily vary in how data is written during task execution, how 
the pending commit information is passed to the job manager, and, as a result,
what requirements they have of the S3 filesystem.
 


| feature | staging | magic |
|--------|---------|---|
| task output destination | local disk | S3A *without completing the write* |
| task commit process | upload data from disk to S3 | list all pending uploads on s3 and write details to job attempt directory |
| task abort process | delete local disk data | list all pending uploads and abort them |
| job commit | list & complete pending uploads | list & complete pending uploads |




### The Staging Committer

This is based on work from Netflix. It "stages" data into the local filesystem.
It also requires the cluster to have HDFS, so that 

Tasks write to URLs with "file://" schemas. When a task is committed,
its files are listed, uploaded to S3 as incompleted Multipart Uploads.
The information needed to complete the uploads is saved to HDFS where
it is committed through the standard "v1" commit algorithm.

When the Job is committed, the Job Manager reads the lists of pending writes from its
HDFS Job destination directory and completes those uploads.

Cancelling a task is straighforward: the local directory is deleted with
its staged data. Cancelling a job is achieved by reading in the lists of
pending writes from the HDFS job attempt directory, and aborting those
uploads. For extra safety, all outstanding multipart writes to the destination directory
are aborted.

The staging committer comes in two slightly different forms, depending
on what kind of output is to be generated


* **Directory**: the data is written directly into the directory.
* **Partitioned**: special handling of partitioned directory trees of the form
`YEAR=2017/MONTH=09/DAY=19`, where the directory tree may already exist.

These two variants address the 


### The Magic Committer

The "Magic" committer does its work through "magic" in the filesystem: 
attempts to write to specifci "magic" paths are interpreted as writes
to a parent directory *which are not to be completed*. When the output stream
is closed, the information needed to complete the write is saved in the magic
directory. The task committer saves the list of these to a directory for the
job committers use, or, if aborting, lists the pending writes and aborts them.

The job committer reads in the list of pending commits, and commits them as
the Staging Committer does.

Compared to the Staging Committer, the Magic Committer offers faster write
times: output is uploaded to S3 as it is written, rather than in the
task commit.

However, it has extra requirements of the filesystem

1. It requires a consistent object store, which for Amazon S3, 
means that [S3Guard](./s3guard.html) must be enabled. For third-party stores,
consult the documentation.
1. The S3A client must be configured to recognize interactions
with the magic directories and treat them specially.


It's also not been field tested to the extent of Netflix's committer; consider
it the least mature of the committers.


## Enabling a Committer

The choice of which committer a job will use for writing data in `FileOutputFormat`
classes is set in the configuration option `mapreduce.pathoutputcommitter.factory.class`.
This declares a the classname of a class which creates committers for jobs and tasks.


| factory | description |
|--------|---------|
| `org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory` |  The default file output committer |
| `org.apache.hadoop.fs.s3a.commit.DynamicCommitterFactory` | Dynamically choose the committer on a per-bucket basis |
| `org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory` |  Use the Directory Staging Committer|
| `org.apache.hadoop.fs.s3a.commit.staging.PartitonedStagingCommitterFactory` |  Partitioned Staging Committer|
| `org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory` | Use the magic committer (not yet ready for production use) |

All of the S3A committers revert to provide a classic FileOutputCommitter instance
when a commmitter is requested for any filesystem other than an S3A one.
This allows any of these committers to be declared in an application configuration,
without worrying about the job failing for any queries using `hdfs://` paths as
the final destination of work.

The Dynamic committer factory is different in that it allows any of the other committer
factories to be used to create a committer, based on the specific value of theoption `fs.s3a.committer.name`:

| value of `fs.s3a.committer.name` |  meaning |
|--------|---------|
| `file` | the original File committer; (not safe for use with S3 storage unless S3Guard is enabled) |
| `directory` | directory staging committer |
| `partition` | partition staging committer |
| `magic` | the "magic" committer |

The dynamic committer lets you choose different committers for different
filesystems, such as only using the magic committer with an S3 bucket known
to have S3Guard enabled. 



## Using the Staging Committer 





The initial option set:

| option | meaning |
|--------|---------|
| `fs.s3a.committer.staging.conflict-mode` | how to resolve directory conflicts during commit: `fail`, `append`, or `replace`; defaults to `fail`. |
| `fs.s3a.committer.staging.unique-filenames` | Should the committer generate unique filenames by including a unique ID in the name of each created file? |
| `fs.s3a.committer.staging.uuid` | a UUID that identifies a write; `spark.sql.sources.writeJobUUID` is used if not set |
| `fs.s3a.committer.staging.upload.size` | size, in bytes, to use for parts of the upload to S3; defaults: `10M` |
| `fs.s3a.committer.tmp.path` | Directory in the cluster filesystem used for storing information on the uncommitted files. |
| `fs.s3a.committer.threads` | number of threads to use to complete S3 uploads during job commit; default: `8` |
| `mapreduce.fileoutputcommitter.marksuccessfuljobs` | flag to control creation of `_SUCCESS` marker file on job completion. Default: `true` |
| `fs.s3a.multipart.size` | Size in bytes of each part of a multipart upload. Default: `100M` |
| `fs.s3a.buffer.dir` | Directory in local filesystem under which data is saved before being uploaded. Example: `/tmp/hadoop/s3a/` |

Generated files are initially written to a local directory underneath one of the temporary
directories listed in `fs.s3a.buffer.dir`.

Temporary files are saved in HDFS (or other cluster filesystem )under the path
`${fs.s3a.committer.tmp.path}/${user}` where `user` is the name of the user running the job.
The default value of `fs.s3a.committer.tmp.path` is `/tmp`, so the temporary directory
for any application attempt will be a path `/tmp/${user}.
In the special case in which the local `file:` filesystem is the cluster filesystem, the
location of the temporary directory is that of the JVM system property
`java.io.tmpdir`.

The application attempt ID is used to create a unique path under this directory,
resulting in a path `/tmp/${user}/${application-attempt-id}/` under which
summary data of each task's pending commits are managed using the standard
`FileOutputFormat` committer.

## Using the Magic committer

This is less mature than the Staging Committer




## Troubleshooting

### `Filesystem does not have support for 'magic' committer`

```
org.apache.hadoop.fs.s3a.commit.PathCommitException: `s3a://landsat-pds': Filesystem does not have support for 'magic' committer enabled
in configuration option fs.s3a.committer.magic.enabled
```

The Job is configured to use the magic committer, but the S3A bucket has not been explicitly
called out as supporting it,

The destination bucket **must** be declared as supporting the magic committer.


This can be done for those buckets which are known to be consistent, either
because the [S3Guard](s3guard.html) is used to provide consistency,

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.committer.magic.enabled</name>
  <value>true</value>
</property>

```

*IMPORTANT*: only enable the magic committer against object stores which
offer consistent listings. By default, Amazon S3 does not do this -which is
why the option `fs.s3a.committer.magic.enabled` is disabled by default.

### `Directory for intermediate work cannot be on S3`

`org.apache.hadoop.fs.PathIOException: s3a://landsat-pds/tmp/alice/local-1495211753375/staging-uploads': Directory for intermediate work cannot be on S3`

The Staging committer uses Hadoop's original committer to manage the commit/abort
protocol for the files listing the pending write operations. Tt uses
the cluster filesystem for this. This must be HDFS or a similar distributed
filesystem with consistent data and renames as O(1) atomic renames.
