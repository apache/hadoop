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

# Committing work to S3 with the "S3A Committers"

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

This page covers the S3A Committers, which can commit work directly
to an S3 object store.

These committers are designed to solve a fundamental problem which
the standard committers of work cannot do to S3: consistent, high performance,
and reliable commitment of output to S3.

For details on their internal design, see
[S3A Committers: Architecture and Implementation](./committer_architecture.html).


## Introduction: The Commit Problem


Apache Hadoop MapReduce (and behind the scenes, Apache Spark) often write
the output of their work to filesystems

Normally, Hadoop uses the `FileOutputFormatCommitter` to manage the
promotion of files created in a single task attempt to the final output of
a query. This is done in a way to handle failures of tasks and jobs, and to
support speculative execution. It does that by listing directories and renaming
their content into the final destination when tasks and then jobs are committed.

This has some key requirement of the underlying filesystem:

1. When you list a directory, you see all the files which have been created in it,
and no files which are not in it (i.e. have been deleted).
1. When you rename a directory, it is an `O(1)` atomic transaction. No other
process across the cluster may rename a file or directory to the same path.
If the rename fails for any reason, either the data is at the original location,
or it is at the destination, -in which case the rename actually succeeded.

**The S3 object store and the `s3a://` filesystem client cannot meet these requirements.*

1. Amazon S3 has inconsistent directory listings unless S3Guard is enabled.
1. The S3A mimics `rename()` by copying files and then deleting the originals.
This can fail partway through, and there is nothing to prevent any other process
in the cluster attempting a rename at the same time.

As a result,

* Files my not be listed, hence not renamed into place.
* Deleted files may still be discovered, confusing the rename process to the point
of failure.
* If a rename fails, the data is left in an unknown state.
* If more than one process attempts to commit work simultaneously, the output
directory may contain the results of both processes: it is no longer an exclusive
operation.
*. While S3Guard may deliver the listing consistency, commit time is still
proportional to the amount of data created. It still can't handle task failure.

**Using the "classic" `FileOutputCommmitter` to commit work to Amazon S3 risks
loss or corruption of generated data**


To address these problems there is now explicit support in the `hadop-aws`
module for committing work to Amazon S3 via the S3A filesystem client,
*the S3A Committers*


For safe, as well as high-performance output of work to S3,
we need use "a committer" explicitly written to work with S3, treating it as
an object store with special features.


### Background : Hadoop's "Commit Protocol"

How exactly is work written to its final destination? That is accomplished by
a "commit protocol" between the workers and the job manager.

This protocol is implemented in Hadoop MapReduce, with a similar but extended
version in Apache Spark:

1. A "Job" is the entire query, with inputs to outputs
1. The "Job Manager" is the process in charge of choreographing the execution
of the job. It may perform some of the actual computation too.
1. The job has "workers", which are processes which work the actual data
and write the results.
1. Workers execute "Tasks", which are fractions of the job, a job whose
input has been *partitioned* into units of work which can be executed independently.
1. The Job Manager directs workers to execute "tasks", usually trying to schedule
the work close to the data (if the filesystem provides locality information).
1. Workers can fail: the Job manager needs to detect this and reschedule their active tasks.
1. Workers can also become separated from the Job Manager, a "network partition".
It is (provably) impossible for the Job Manager to distinguish a running-but-unreachable
worker from a failed one.
1. The output of a failed task must not be visible; this is to avoid its
data getting into the final output.
1. Multiple workers can be instructed to evaluate the same partition of the work;
this "speculation" delivers speedup as it can address the "straggler problem".
When multiple workers are working on the same data, only one worker is allowed
to write the final output.
1. The entire job may fail (often from the failure of the Job Manager (MR Master, Spark Driver, ...)).
1, The network may partition, with workers isolated from each other or
the process managing the entire commit.
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
* Task and Job processes measure the intervals between communications with their
Application Master and YARN respectively.
When the interval has grown too large they must conclude
that the network has partitioned and that they must abort their work.


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


None of this algorithm works safely or swiftly when working with "raw" AWS S3 storage:
* Directory listing can be inconsistent: the tasks and jobs may not list all work to
be committed.
* Renames go from being fast, atomic operations to slow operations which can fail partway through.

This then is the problem which the S3A committers address:

*How to safely and reliably commit work to Amazon S3 or compatible object store*


## Meet the S3A Commmitters

Since Hadoop 3.1, the S3A FileSystem has been accompanied by classes
designed to integrate with the Hadoop and Spark job commit protocols, classes
which interact with the S3A filesystem to reliably commit work work to S3:
*The S3A Committers*

The underlying architecture of this process is very complex, and
covered in [the committer architecture documentation](./committer_architecture.html).

The key concept to know of is S3's "Multipart Upload" mechanism. This allows
an S3 client to write data to S3 in multiple HTTP POST requests, only completing
the write operation with a final POST to complete the upload; this final POST
consisting of a short list of the etags of the uploaded blocks.
This multipart upload mechanism is already automatically used when writing large
amounts of data to S3; an implementation detail of the S3A output stream.

The S3A committers make explicit use of this multipart upload ("MPU") mechanism:

1. The individual *tasks* in a job write their data to S3 as POST operations
within multipart uploads, yet do not issue the final POST to complete the upload.
1. The multipart uploads are committed in the job commit process.

There are two different S3A committer types, *staging*
and *magic*. The committers primarily vary in how data is written during task execution,
how  the pending commit information is passed to the job manager, and in how
conflict with existing files is resolved.


| feature | staging | magic |
|--------|---------|---|
| task output destination | local disk | S3A *without completing the write* |
| task commit process | upload data from disk to S3 | list all pending uploads on s3 and write details to job attempt directory |
| task abort process | delete local disk data | list all pending uploads and abort them |
| job commit | list & complete pending uploads | list & complete pending uploads |

The other metric is "maturity". There, the fact that the Staging committers
are based on Netflix's production code counts in its favor.


### The Staging Committer

This is based on work from Netflix. It "stages" data into the local filesystem.
It also requires the cluster to have HDFS, so that

Tasks write to URLs with `file://` schemas. When a task is committed,
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

The staging committer comes in two slightly different forms, with slightly
diffrent conflict resolution policies:


* **Directory**: the entire directory tree of data is written or overwritten,
as normal.

* **Partitioned**: special handling of partitioned directory trees of the form
`YEAR=2017/MONTH=09/DAY=19`: conflict resolution is limited to the partitions
being updated.


The Partitioned Committer is intended to allow jobs updating a partitioned
directory tree to restrict the conflict resolution to only those partition
directories containing new data. It is intended for use with Apache Spark
only.


## Conflict Resolution in the Staging Committers

The Staging committers offer the ability to replace the conflict policy
of the execution engine with policy designed to work with the tree of data.
This is based on the experience and needs of Netflix, where efficiently adding
new data to an existing partitioned directory tree is a common operation.

```xml
<property>
  <name>fs.s3a.committer.staging.conflict-mode</name>
  <value>fail</value>
  <description>
    Staging committer conflict resolution policy: {@value}.
    Supported: fail, append, replace.
  </description>
</property>
```

**replace** : when the job is committed (and not before), delete files in
directories into which new data will be written.

**fail**: when there are existing files in the destination, fail the job.

**append**: Add new data to the directories at the destination; overwriting
any with the same name. Reliable use requires unique names for generated files,
which the committers generate
by default.

The difference between the two staging ommitters are as follows:

The Directory Committer uses the entire directory tree for conflict resolution.
If any file exists at the destination it will fail in job setup; if the resolution
mechanism is "replace" then all existing files will be deleted.

The partitioned committer calculates the partitions into which files are added,
the final directories in the tree, and uses that in its conflict resolution
process:


**replace** : delete all data in the destination partition before committing
the new files.

**fail**: fail if there is data in the destination partition, ignoring the state
of any parallel partitions.

**append**: add the new data.

It's intended for use in Apache Spark Dataset operations, rather
than Hadoop's original MapReduce engine, and only in jobs
where adding new data to an existing dataset is the desired goal.

Preequisites for successful work

1. The output is written into partitions via `PARTITIONED BY` or `partitionedBy()`
instructions.
2. There is no data written directly to the root path (all files there are
ignored; it's implicitly "append").

Here's an example in Spark, assuming that `sourceDataset` is a dataset
whose columns include "year" and "month":

```scala
sourceDataset
  .write
  .partitionBy("year", "month")
  .mode(SaveMode.Append)
  .opt("fs.s3a.committer.name", "partitioned")
  .opt("fs.s3a.committer.staging.conflict-mode", "replace")
  .format("orc")
  .save("s3a://examples/statistics")
```


### The Magic Committer

The "Magic" committer does its work through "magic" in the filesystem:
attempts to write to specific "magic" paths are interpreted as writes
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


#### Which Committer to Use?

1. If you want to create or update existing partitioned data trees in Spark, use thee
Partitioned Committer. Make sure you have enough hard disk capacity for all staged data.
Do not use it in other situations.

1. If you know that your object store is consistent, or that the processes
writing data use S3Guard, use the Magic Committer for higher performance
writing of large amounts of data.

1. Otherwise: use the directory committer, making sure you have enough
hard disk capacity for all staged data.

Put differently: start with the Directory Committer.

## Switching to an S3A Committer

To use an S3A committer, the property `mapreduce.outputcommitter.factory.scheme.s3a`
must be set to the S3A committer factory, `org.apache.hadoop.fs.s3a.commit.staging.S3ACommitterFactory`.
This is done in `core-default.xml`

```xml
<property>
  <name>mapreduce.outputcommitter.factory.scheme.s3a</name>
  <value>org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory</value>
  <description>
    The committer factory to use when writing data to S3A filesystems.
  </description>
</property>
```

What is missing is an explicit choice of committer to use in the property
`fs.s3a.committer.name`; so the classic (and unsafe) file committer is used.

| `fs.s3a.committer.name` |  Committer |
|--------|---------|
| `directory` | directory staging committer |
| `partitioned` | partition staging committer (for use in Spark only) |
| `magic` | the "magic" committer |
| `file` | the original and unsafe File committer; (default) |



## Using the Directory and Partitioned Staging Committers

Generated files are initially written to a local directory underneath one of the temporary
directories listed in `fs.s3a.buffer.dir`.


The staging commmitter needs a path in the cluster filesystem
(e.g. HDFS). This must be declared in `fs.s3a.committer.staging.tmp.path`.

Temporary files are saved in HDFS (or other cluster filesystem) under the path
`${fs.s3a.committer.staging.tmp.path}/${user}` where `user` is the name of the user running the job.
The default value of `fs.s3a.committer.staging.tmp.path` is `tmp/staging`,
Which will be converted at run time to a path under the current user's home directory,
essentially `~/tmp/staging`
 so the temporary directory

The application attempt ID is used to create a unique path under this directory,
resulting in a path `~/tmp/staging/${user}/${application-attempt-id}/` under which
summary data of each task's pending commits are managed using the standard
`FileOutputFormat` committer.

When a task is committed the data is uploaded under the destination directory.
The policy of how to react if the destination exists is defined by
the `fs.s3a.committer.staging.conflict-mode` setting.

| `fs.s3a.committer.staging.conflict-mode` | Meaning |
| -----------------------------------------|---------|
| `fail` | Fail if the destination directory exists |
| `replace` | Delete all existing files before committing the new data |
| `append` | Add the new files to the existing directory tree |


## The "Partitioned" Staging Committer

This committer an extension of the "Directory" committer which has a special conflict resolution
policy designed to support operations which insert new data into a directory tree structured
using Hive's partitioning strategy: different levels of the tree represent different columns.

For example, log data could be partitioned by `YEAR` and then by `MONTH`, with different
entries underneath.

```
logs/YEAR=2017/MONTH=01/
  log-20170101.avro
  log-20170102.avro
  ...
  log-20170131.avro

logs/YEAR=2017/MONTH=02/
  log-20170201.avro
  log-20170202.avro
  ...
  log-20170227.avro

logs/YEAR=2017/MONTH=03/
logs/YEAR=2017/MONTH=04/
```

A partitioned structure like this allows for queries using Hive or Spark to filter out
files which do not contain relevant data.

What the partitioned committer does is, where the tooling permits, allows callers
to add data to an existing partitioned layout*.

More specifically, it does this by having a conflict resolution options which
only act on invididual partitions, rather than across the entire output tree.

| `fs.s3a.committer.staging.conflict-mode` | Meaning |
| -----------------------------------------|---------|
| `fail` | Fail if the destination partition(s) exist |
| `replace` | Delete the existing data partitions before committing the new data |
| `append` | Add the new data to the existing partitions |


As an example, if a job was writing the file
`logs/YEAR=2017/MONTH=02/log-20170228.avro`, then with a policy of `fail`,
the job would fail. With a policy of `replace`, then entire directory
`logs/YEAR=2017/MONTH=02/` would be deleted before the new file `log-20170228.avro`
was written. With the policy of `append`, the new file would be added to
the existing set of files.


### Notes

1. A deep partition tree can itself be a performance problem in S3 and the s3a client,
or, more specifically. a problem with applications which use recursive directory tree
walks to work with data.

1. The outcome if you have more than one job trying simultaneously to write data
to the same destination with any policy other than "append" is undefined.

1. In the `append` operation, there is no check for conflict with file names.
If, in the example above, the file `log-20170228.avro` already existed,
it would be overridden. Set `fs.s3a.committer.staging.unique-filenames` to `true`
to ensure that a UUID is included in every filename to avoid this.


## Using the Magic committer

This is less mature than the Staging Committer, but promises higher
performance.

### FileSystem client setup

1. Use a *consistent* S3 object store. For Amazon S3, this means enabling
[S3Guard](./s3guard.html). For S3-compatible filesystems, consult the filesystem
documentation to see if it is consistent, hence compatible "out of the box".
1. Turn the magic on by `fs.s3a.committer.magic.enabled"`

```xml
<property>
  <name>fs.s3a.committer.magic.enabled</name>
  <description>
  Enable support in the filesystem for the S3 "Magic" committter.
  </description>
  <value>true</value>
</property>
```

*Do not use the Magic Committer on an inconsistent S3 object store. For
Amazon S3, that means S3Guard must *always* be enabled.


### Enabling the committer

```xml
<property>
  <name>fs.s3a.committer.name</name>
  <value>magic</value>
</property>

```

Conflict management is left to the execution engine itself.

## Committer Configuration Options


| Option | Magic | Directory | Partitioned | Meaning | Default |
|--------|-------|-----------|-------------|---------|---------|
| `mapreduce.fileoutputcommitter.marksuccessfuljobs` | X | X | X | Write a `_SUCCESS` file  at the end of each job | `true` |
| `fs.s3a.committer.threads` | X | X | X | Number of threads in committers for parallel operations on files. | 8 |
| `fs.s3a.committer.staging.conflict-mode` |  | X | X | Conflict resolution: `fail`, `abort` or `overwrite`| `fail` |
| `fs.s3a.committer.staging.unique-filenames` |  | X | X | Generate unique filenames | `true` |

| `fs.s3a.committer.magic.enabled` | X |  | | Enable "magic committer" support in the filesystem | `false` |




| Option | Magic | Directory | Partitioned | Meaning | Default |
|--------|-------|-----------|-------------|---------|---------|
| `fs.s3a.buffer.dir` | X | X | X | Local filesystem directory for data being written and/or staged. | |
| `fs.s3a.committer.staging.tmp.path` |  | X | X | Path in the cluster filesystem for temporary data | `tmp/staging` |


```xml
<property>
  <name>fs.s3a.committer.name</name>
  <value>file</value>
  <description>
    Committer to create for output to S3A, one of:
    "file", "directory", "partitioned", "magic".
  </description>
</property>

<property>
  <name>fs.s3a.committer.magic.enabled</name>
  <value>false</value>
  <description>
    Enable support in the filesystem for the S3 "Magic" committer.
    When working with AWS S3, S3Guard must be enabled for the destination
    bucket, as consistent metadata listings are required.
  </description>
</property>

<property>
  <name>fs.s3a.committer.threads</name>
  <value>8</value>
  <description>
    Number of threads in committers for parallel operations on files
    (upload, commit, abort, delete...)
  </description>
</property>

<property>
  <name>fs.s3a.committer.staging.tmp.path</name>
  <value>tmp/staging</value>
  <description>
    Path in the cluster filesystem for temporary data.
    This is for HDFS, not the local filesystem.
    It is only for the summary data of each file, not the actual
    data being committed.
    Using an unqualified path guarantees that the full path will be
    generated relative to the home directory of the user creating the job,
    hence private (assuming home directory permissions are secure).
  </description>
</property>

<property>
  <name>fs.s3a.committer.staging.unique-filenames</name>
  <value>true</value>
  <description>
    Option for final files to have a unique name through job attempt info,
    or the value of fs.s3a.committer.staging.uuid
    When writing data with the "append" conflict option, this guarantees
    that new data will not overwrite any existing data.
  </description>
</property>

<property>
  <name>fs.s3a.committer.staging.conflict-mode</name>
  <value>fail</value>
  <description>
    Staging committer conflict resolution policy.
    Supported: "fail", "append", "replace".
  </description>
</property>

<property>
  <name>s.s3a.committer.staging.abort.pending.uploads</name>
  <value>true</value>
  <description>
    Should the staging committers abort all pending uploads to the destination
    directory?

    Changing this if more than one partitioned committer is
    writing to the same destination tree simultaneously; otherwise
    the first job to complete will cancel all outstanding uploads from the
    others. However, it may lead to leaked outstanding uploads from failed
    tasks. If disabled, configure the bucket lifecycle to remove uploads
    after a time period, and/or set up a workflow to explicitly delete
    entries. Otherwise there is a risk that uncommitted uploads may run up
    bills.
  </description>
</property>

<property>
  <name>mapreduce.outputcommitter.factory.scheme.s3a</name>
  <value>org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory</value>
  <description>
    The committer factory to use when writing data to S3A filesystems.
    If mapreduce.outputcommitter.factory.class is set, it will
    override this property.

    (This property is set in mapred-default.xml)
  </description>
</property>

```


## Troubleshooting

### `Filesystem does not have support for 'magic' committer`

```
org.apache.hadoop.fs.s3a.commit.PathCommitException: `s3a://landsat-pds': Filesystem does not have support for 'magic' committer enabled
in configuration option fs.s3a.committer.magic.enabled
```

The Job is configured to use the magic committer, but the S3A bucket has not been explicitly
declared as supporting it.

The destination bucket **must** be declared as supporting the magic committer.

This can be done for those buckets which are known to be consistent, either
because [S3Guard](s3guard.html) is used to provide consistency,
or because the S3-compatible filesystem is known to be strongly consistent.

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.committer.magic.enabled</name>
  <value>true</value>
</property>
```

*IMPORTANT*: only enable the magic committer against object stores which
offer consistent listings. By default, Amazon S3 does not do this -which is
why the option `fs.s3a.committer.magic.enabled` is disabled by default.


Tip: you can verify that a bucket supports the magic committer through the
`hadoop s3guard bucket-info` command:


```
> hadoop s3guard bucket-info -magic s3a://landsat-pds/

Filesystem s3a://landsat-pds
Location: us-west-2
Filesystem s3a://landsat-pds is not using S3Guard
The "magic" committer is not supported

S3A Client
  Endpoint: fs.s3a.endpoint=(unset)
  Encryption: fs.s3a.server-side-encryption-algorithm=none
  Input seek policy: fs.s3a.experimental.input.fadvise=normal
2017-09-27 19:18:57,917 INFO util.ExitUtil: Exiting with status 46: 46: The magic committer is not enabled for s3a://landsat-pds
```

## Error message: "File being created has a magic path, but the filesystem has magic file support disabled:

A file is being written to a path which is used for "magic" files,
files which are actually written to a different destination than their stated path
*but the filesystem doesn't support "magic" files*

This message should not appear through the committer itself &mdash;it will
fail with the error message in the previous section, but may arise
if other applications are attempting to create files under the path `/__magic/`.

Make sure the filesytem meets the requirements of the magic committer
(a consistent S3A filesystem through S3Guard or the S3 service itself),
and set the `fs.s3a.committer.magic.enabled` flag to indicate that magic file
writes are supported.


### `FileOutputCommitter` appears to be still used (from logs or delays in commits)

The Staging committers use the original `FileOutputCommitter` to manage
the propagation of commit information: do not worry if it the logs show `FileOutputCommitter`
work with data in the cluster filesystem (e.g. HDFS).

One way to make sure that the `FileOutputCommitter` is not being used to write
the data to S3 is to set the option `mapreduce.fileoutputcommitter.algorithm.version`
to a value such as "10". Because the only supported algorithms are "1" and "2",
any erroneously created `FileOutputCommitter` will raise an exception in its constructor
when instantiated:

```
java.io.IOException: Only 1 or 2 algorithm version is supported
at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.<init>(FileOutputCommitter.java:130)
at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.<init>(FileOutputCommitter.java:104)
at org.apache.parquet.hadoop.ParquetOutputCommitter.<init>(ParquetOutputCommitter.java:42)
at org.apache.parquet.hadoop.ParquetOutputFormat.getOutputCommitter(ParquetOutputFormat.java:395)
at org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.setupCommitter(HadoopMapReduceCommitProtocol.scala:67)
at com.hortonworks.spark.cloud.commit.PathOutputCommitProtocol.setupCommitter(PathOutputCommitProtocol.scala:62)
at org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.setupJob(HadoopMapReduceCommitProtocol.scala:124)
at com.hortonworks.spark.cloud.commit.PathOutputCommitProtocol.setupJob(PathOutputCommitProtocol.scala:152)
at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:175)
at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:145)
```

While that will not make the problem go away, it will at least make
the failure happen at the start of a job.

(Setting this option will not interfer with the Staging Committers' use of HDFS,
as it explicitly sets the algorithm to "2" for that part of its work).

The other way to check which committer to use is to examine the `_SUCCESS` file.
If it is 0-bytes long, the classic `FileOutputCommitter` committed the job.
The S3A committers all write a non-empty JSON file; the `committer` field lists
the committer used.


*Common causes*

1. The property `fs.s3a.committer.name` is set to "file". Fix: change.
1. The job has overridden the property `mapreduce.outputcommitter.factory.class`
with a new factory class for all committers. This takes priority over
all committers registered for the s3a:// schema.
1. The property `mapreduce.outputcommitter.factory.scheme.s3a` is unset.
1. The output format has overridden `FileOutputFormat.getOutputCommitter()`
and is returning its own committer -one which is a subclass of `FileOutputCommitter`.

That final cause. *the output format is returning its own committer*, is not
easily fixed; it may be that the custom committer performs critical work
during its lifecycle, and contains assumptions about the state of the written
data during task and job commit (i.e. it is in the destination filesystem).
Consult with the authors/maintainers of the output format
to see whether it would be possible to integrate with the new committer factory
mechanism and object-store-specific commit algorithms.

Parquet is a special case here: its committer does no extra work
other than add the option to read all newly-created files then write a schema
summary. The Spark integration has explicit handling for Parquet to enable it
to support the new committers, removing this (slow on S3) option.

If you have subclassed `FileOutputCommitter` and want to move to the
factory model, please get in touch.


## Job/Task fails with PathExistsException: Destination path exists and committer conflict resolution mode is "fail"

This surfaces when either of two conditions are met.

1. The Directory committer is used with `fs.s3a.committer.staging.conflict-mode` set to
`fail` and the output/destination directory exists.
The job will fail in the driver during job setup.
1. The Partitioned Committer is used with `fs.s3a.committer.staging.conflict-mode` set to
`fail`  and one of the partitions. The specific task(s) generating conflicting data will fail
during task commit, which will cause the entire job to fail.

If you are trying to write data and want write conflicts to be rejected, this is the correct
behavior: there was data at the destination so the job was aborted.

## Staging committer task fails with IOException: No space left on device

There's not enough space on the local hard disk (real or virtual)
to store all the uncommitted data of the active tasks on that host.
Because the staging committers write all output to the local disk
and only upload the data on task commits, enough local temporary
storage is needed to store all output generated by all uncommitted
tasks running on the single host. Small EC2 VMs may run out of disk.

1. Make sure that `fs.s3a.buffer.dir` includes a temporary directory on
every available hard disk; this spreads load better.

1. Add more disk space. In EC2: request instances with more local storage.
There is no need for EMR storage; this is just for temporary data.

1. Purge the directories listed in `fs.s3a.buffer.dir` of old data.
Failed tasks may not clean up all old files.

1. Reduce the number of worker threads/process in the host.

1. Consider partitioning the job into more tasks. This *may* result in more tasks
generating less data each.

1. Use the magic committer. This only needs enough disk storage to buffer
blocks of the currently being written file during their upload process,
so can use a lot less disk space.
