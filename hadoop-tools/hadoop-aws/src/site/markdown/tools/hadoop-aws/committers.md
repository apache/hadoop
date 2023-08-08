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

### January 2021 Update

Now that S3 is fully consistent, problems related to inconsistent directory
listings have gone. However the rename problem exists: committing work by
renaming directories is unsafe as well as horribly slow.

This architecture document, and the committers, were written at a time when S3
was inconsistent. The two committers addressed this problem differently

* Staging Committer: rely on a cluster HDFS filesystem for safely propagating
  the lists of files to commit from workers to the job manager/driver.
* Magic Committer: require S3Guard to offer consistent directory listings on the
  object store.

With consistent S3, the Magic Committer can be safely used with any S3 bucket.
The choice of which to use, then, is matter for experimentation.

This document was written in 2017, a time when S3 was only
consistent when an extra consistency layer such as S3Guard was used. The
document indicates where requirements/constraints which existed then are now
obsolete.

## Introduction: The Commit Problem

Apache Hadoop MapReduce (and behind the scenes, Apache Spark) often write
the output of their work to filesystems.

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

_The S3 object store and the `s3a://` filesystem client cannot meet these requirements._

Although S3 is (now) consistent, the S3A client still mimics `rename()`
by copying files and then deleting the originals.
This can fail partway through, and there is nothing to prevent any other process
in the cluster attempting a rename at the same time.

As a result,

* If a 'rename' fails, the data is left in an unknown state.
* If more than one process attempts to commit work simultaneously, the output
directory may contain the results of both processes: it is no longer an exclusive
operation.
* Commit time is still proportional to the amount of data created.
It still can't handle task failure.

**Using the "classic" `FileOutputCommmitter` to commit work to Amazon S3 risks
loss or corruption of generated data**.


To address these problems there is now explicit support in the `hadoop-aws`
module for committing work to Amazon S3 via the S3A filesystem client:
*the S3A Committers*.


For safe, as well as high-performance output of work to S3,
we need to use "a committer" explicitly written to work with S3,
treating it as an object store with special features.


### Background: Hadoop's "Commit Protocol"

How exactly is work written to its final destination? That is accomplished by
a "commit protocol" between the workers and the job manager.

This protocol is implemented in Hadoop MapReduce, with a similar but extended
version in Apache Spark:

1. The "Job" is the entire query. It takes a given set of input and produces some output.
1. The "Job Manager" is the process in charge of choreographing the execution
of the job. It may perform some of the actual computation too.
1. The job has "workers", which are processes which work with the actual data
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
1. The network may partition, with workers isolated from each other or
the process managing the entire commit.
1. Restarted jobs may recover from a failure by reusing the output of all
completed tasks (MapReduce with the "v1" algorithm), or just by rerunning everything
(The "v2" algorithm and Spark).


What is "the commit protocol" then? It is the requirements on workers as to
when their data is made visible, where, for a filesystem, "visible" means "can
be seen in the destination directory of the query."

* There is a destination directory of work: "the output directory".
The final output of tasks must be in this directory *or paths underneath it*.
* The intermediate output of a task must not be visible in the destination directory.
That is: they must not write directly to the destination.
* The final output of a task *may* be visible under the destination.
* Individual workers communicate with the Job manager to manage the commit process.
* The Job Manager makes the decision on if a task's output data is to be "committed",
be it directly to the final directory or to some intermediate store.
* When a worker commits the output of a task, it somehow promotes its intermediate work to becoming
final.
* When a worker aborts a task's output, that output must not become visible
(i.e. it is not committed).
* Jobs themselves may be committed/aborted (the nature of "when" is not covered here).
* After a Job is committed, all its work must be visible.
A file named `_SUCCESS` may be written to the output directory.
* After a Job is aborted, all its intermediate data is lost.
* Jobs may also fail. When restarted, the successor job must be able to clean up
all the intermediate and committed work of its predecessor(s).
* Task and Job processes measure the intervals between communications with their
Application Master and YARN respectively.
When the interval has grown too large, they must conclude
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


This algorithm does not work safely or swiftly with AWS S3 storage because
renames go from being fast, atomic operations to slow operations which can fail partway through.

This then is the problem which the S3A committers address:
*How to safely and reliably commit work to Amazon S3 or compatible object store.*


## Meet the S3A Committers

Since Hadoop 3.1, the S3A FileSystem has been accompanied by classes
designed to integrate with the Hadoop and Spark job commit protocols,
classes which interact with the S3A filesystem to reliably commit work to S3:
*The S3A Committers*.

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
| task output destination | write to local disk | upload to S3 *without completing the write* |
| task commit process | upload data from disk to S3 *without completing the write* | list all pending uploads on S3 and write details to job attempt directory |
| task abort process | delete local disk data | list all pending uploads and abort them |
| job commit | list & complete pending uploads | list & complete pending uploads |

The other metric is "maturity". There, the fact that the Staging committers
are based on Netflix's production code counts in its favor.


### The Staging Committers

This is based on work from Netflix.
It "stages" data into the local filesystem, using URLs with `file://` schemas.

When a task is committed, its files are listed and uploaded to S3 as incomplete Multipart Uploads.
The information needed to complete the uploads is saved to HDFS where
it is committed through the standard "v1" commit algorithm.

When the Job is committed, the Job Manager reads the lists of pending writes from its
HDFS Job destination directory and completes those uploads.

Canceling a _task_ is straightforward: the local directory is deleted with its staged data.
Canceling a _job_ is achieved by reading in the lists of
pending writes from the HDFS job attempt directory, and aborting those
uploads. For extra safety, all outstanding multipart writes to the destination directory
are aborted.

There are two staging committers with slightly different conflict resolution behaviors:

* **Directory Committer**: the entire directory tree of data is written or overwritten,
as normal.

* **Partitioned Committer**: special handling of partitioned directory trees of the form
`YEAR=2017/MONTH=09/DAY=19`: conflict resolution is limited to the partitions
being updated.


The Partitioned Committer is intended to allow jobs updating a partitioned
directory tree to restrict the conflict resolution to only those partition
directories containing new data. It is intended for use with Apache Spark
only.


#### Conflict Resolution in the Staging Committers

The Staging committers offer the ability to replace the conflict policy
of the execution engine with policy designed to work with the tree of data.
This is based on the experience and needs of Netflix, where efficiently adding
new data to an existing partitioned directory tree is a common operation.

An XML configuration is shown below.
The default conflict mode if unset would be `append`.

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

The _Directory Committer_ uses the entire directory tree for conflict resolution.
For this committer, the behavior of each conflict mode is shown below:

- **replace**: When the job is committed (and not before), delete files in
directories into which new data will be written.

- **fail**: When there are existing files in the destination, fail the job.

- **append**: Add new data to the directories at the destination; overwriting
any with the same name. Reliable use requires unique names for generated files,
which the committers generate
by default.

The _Partitioned Committer_ calculates the partitions into which files are added,
the final directories in the tree, and uses that in its conflict resolution process.
For the _Partitioned Committer_, the behavior of each mode is as follows:

- **replace**: Delete all data in the destination _partition_ before committing
the new files.

- **fail**: Fail if there is data in the destination _partition_, ignoring the state
of any parallel partitions.

- **append**: Add the new data to the destination _partition_,
  overwriting any files with the same name.

The _Partitioned Committer_ is intended for use in Apache Spark Dataset operations, rather
than Hadoop's original MapReduce engine, and only in jobs
where adding new data to an existing dataset is the desired goal.

Prerequisites for success with the _Partitioned Committer_:

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

1. The object store must be consistent.
1. The S3A client must be configured to recognize interactions
with the magic directories and treat them as a special case.

Now that [Amazon S3 is consistent](https://aws.amazon.com/s3/consistency/),
the magic directory path rewriting is enabled by default.

The Magic Committer has not been field tested to the extent of Netflix's committer;
consider it the least mature of the committers.


### Which Committer to Use?

1. If you want to create or update existing partitioned data trees in Spark, use the
Partitioned Committer. Make sure you have enough hard disk capacity for all staged data.
Do not use it in other situations.

1. If you do not have a shared cluster store: use the Magic Committer.
   
1. If you are writing large amounts of data: use the Magic Committer.

1. Otherwise: use the directory committer, making sure you have enough
hard disk capacity for all staged data.

Now that S3 is consistent, there are fewer reasons not to use the Magic Committer.
Experiment with both to see which works best for your work.

## Switching to an S3A Committer

To use an S3A committer, the property `mapreduce.outputcommitter.factory.scheme.s3a`
must be set to the S3A committer factory, `org.apache.hadoop.fs.s3a.commit.staging.S3ACommitterFactory`.
This is done in `mapred-default.xml`

```xml
<property>
  <name>mapreduce.outputcommitter.factory.scheme.s3a</name>
  <value>org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory</value>
  <description>
    The committer factory to use when writing data to S3A filesystems.
  </description>
</property>
```

You must also choose which of the S3A committers to use with the `fs.s3a.committer.name` property.
Otherwise, the classic (and unsafe) file committer is used.

| `fs.s3a.committer.name` |  Committer |
|--------|---------|
| `directory` | directory staging committer |
| `partitioned` | partition staging committer (for use in Spark only) |
| `magic` | the "magic" committer |
| `file` | the original and unsafe File committer; (default) |

## Using the Staging Committers

Generated files are initially written to a local directory underneath one of the temporary
directories listed in `fs.s3a.buffer.dir`.


The staging committer needs a path in the cluster filesystem
(e.g. HDFS). This must be declared in `fs.s3a.committer.staging.tmp.path`.

Temporary files are saved in HDFS (or other cluster filesystem) under the path
`${fs.s3a.committer.staging.tmp.path}/${user}` where `user` is the name of the user running the job.
The default value of `fs.s3a.committer.staging.tmp.path` is `tmp/staging`,
resulting in the HDFS directory `~/tmp/staging/${user}`.

The application attempt ID is used to create a unique path under this directory,
resulting in a path `~/tmp/staging/${user}/${application-attempt-id}/` under which
summary data of each task's pending commits are managed using the standard
`FileOutputFormat` committer.

When a task is committed, the data is uploaded under the destination directory.
The policy of how to react if the destination exists is defined by
the `fs.s3a.committer.staging.conflict-mode` setting.

| `fs.s3a.committer.staging.conflict-mode` | Meaning |
| -----------------------------------------|---------|
| `fail` | Fail if the destination directory exists |
| `replace` | Delete all existing files before committing the new data |
| `append` | Add the new files to the existing directory tree |


### The "Partitioned" Staging Committer

This committer is an extension of the "Directory" committer which has a special conflict resolution
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

The partitioned committer allows callers to add new data to an existing partitioned layout,
where the application supports it.

More specifically, it does this by reducing the scope of conflict resolution to
only act on individual partitions, rather than across the entire output tree.

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


### Notes on using Staging Committers

1. A deep partition tree can itself be a performance problem in S3 and the s3a client,
or more specifically a problem with applications which use recursive directory tree
walks to work with data.

1. The outcome if you have more than one job trying simultaneously to write data
to the same destination with any policy other than "append" is undefined.

1. In the `append` operation, there is no check for conflict with file names.
If the file `log-20170228.avro` in the example above already existed, it would be overwritten.
Set `fs.s3a.committer.staging.unique-filenames` to `true`
to ensure that a UUID is included in every filename to avoid this.


## Using the Magic committer

This is less mature than the Staging Committer, but promises higher
performance.

### FileSystem client setup

The S3A connector can recognize files created under paths with `__magic/` as a parent directory.
This allows it to handle those files in a special way, such as uploading to a different location
and storing the information needed to complete pending multipart uploads.

Turn the magic on by setting `fs.s3a.committer.magic.enabled` to `true`:

```xml
<property>
  <name>fs.s3a.committer.magic.enabled</name>
  <description>
  Enable support in the filesystem for the S3 "Magic" committer.
  </description>
  <value>true</value>
</property>
```

### Enabling the committer

Set the committer used by S3A's committer factory to `magic`:

```xml
<property>
  <name>fs.s3a.committer.name</name>
  <value>magic</value>
</property>
```

Conflict management is left to the execution engine itself.

## Committer Options Reference

### Common S3A Committer Options

The table below provides a summary of each option.

| Option | Meaning | Default |
|--------|---------|---------|
| `mapreduce.fileoutputcommitter.marksuccessfuljobs` | Write a `_SUCCESS` file on the successful completion of the job. | `true` |
| `fs.s3a.buffer.dir` | Local filesystem directory for data being written and/or staged. | `${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/s3a` |
| `fs.s3a.committer.magic.enabled` | Enable "magic committer" support in the filesystem. | `true` |
| `fs.s3a.committer.abort.pending.uploads` | list and abort all pending uploads under the destination path when the job is committed or aborted. | `true` |
| `fs.s3a.committer.threads` | Number of threads in committers for parallel operations on files.| -4 |
| `fs.s3a.committer.generate.uuid` | Generate a Job UUID if none is passed down from Spark | `false` |
| `fs.s3a.committer.require.uuid` |Require the Job UUID to be passed down from Spark | `false` |

The examples below shows how these options can be configured in XML.

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
  <value>true</value>
  <description>
    Enable support in the filesystem for the S3 "Magic" committer.
  </description>
</property>

<property>
  <name>fs.s3a.committer.threads</name>
  <value>-4</value>
  <description>
    Number of threads in committers for parallel operations on files
    (upload, commit, abort, delete...).
    Two thread pools this size are created, one for the outer
    task-level parallelism, and one for parallel execution
    within tasks (POSTs to commit individual uploads)
    If the value is negative, it is inverted and then multiplied
    by the number of cores in the CPU.
  </description>
</property>

<property>
  <name>fs.s3a.committer.abort.pending.uploads</name>
  <value>true</value>
  <description>
    Should the committers abort all pending uploads to the destination
    directory?

    Set to false if more than one job is writing to the same directory tree.
    Was:  "fs.s3a.committer.staging.abort.pending.uploads" when only used
    by the staging committers.
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

<property>
  <name>fs.s3a.committer.require.uuid</name>
  <value>false</value>
  <description>
    Require the committer fail to initialize if a unique ID is not set in
    "spark.sql.sources.writeJobUUID" or "fs.s3a.committer.uuid".
    This helps guarantee that unique IDs for jobs are being
    passed down in spark applications.
  
    Setting this option outside of spark will stop the S3A committer
    in job setup. In MapReduce workloads the job attempt ID is unique
    and so no unique ID need be passed down.
  </description>
</property>

<property>
  <name>fs.s3a.committer.generate.uuid</name>
  <value>false</value>
  <description>
    Generate a Job UUID if none is passed down from Spark.
    This uuid is only generated if the fs.s3a.committer.require.uuid flag
    is false. 
  </description>
</property>
```

### Staging committer (Directory and Partitioned) options

| Option | Meaning | Default |
|--------|---------|---------|
| `fs.s3a.committer.staging.conflict-mode` | Conflict resolution: `fail`, `append`, or `replace`.| `append` |
| `fs.s3a.committer.staging.tmp.path` | Path in the cluster filesystem for temporary data. | `tmp/staging` |
| `fs.s3a.committer.staging.unique-filenames` | Generate unique filenames. | `true` |
| `fs.s3a.committer.staging.abort.pending.uploads` | Deprecated; replaced by `fs.s3a.committer.abort.pending.uploads`. |  `(false)` |

```xml
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
    or the value of fs.s3a.committer.uuid.
    When writing data with the "append" conflict option, this guarantees
    that new data will not overwrite any existing data.
  </description>
</property>

<property>
  <name>fs.s3a.committer.staging.conflict-mode</name>
  <value>append</value>
  <description>
    Staging committer conflict resolution policy.
    Supported: "fail", "append", "replace".
  </description>
</property>


```

### Disabling magic committer path rewriting

The magic committer recognizes when files are created under paths with `__magic/` as a parent directory
and redirects the upload to a different location, adding the information needed to complete the upload
in the job commit operation.

If, for some reason, you *do not* want these paths to be redirected and completed later,
the feature can be disabled by setting `fs.s3a.committer.magic.enabled` to false.
By default, it is enabled.

```xml
<property>
  <name>fs.s3a.committer.magic.enabled</name>
  <value>true</value>
  <description>
    Enable support in the S3A filesystem for the "Magic" committer.
  </description>
</property>
```

You will not be able to use the Magic Committer if this option is disabled.

## <a name="concurrent-jobs"></a> Concurrent Jobs writing to the same destination

It is sometimes possible for multiple jobs to simultaneously write to the same destination path.

Before attempting this, the committers must be set to not delete all incomplete uploads on job commit,
by setting `fs.s3a.committer.abort.pending.uploads` to `false`

```xml
<property>
  <name>fs.s3a.committer.abort.pending.uploads</name>
  <value>false</value>
</property>
```

If more than one job is writing to the same destination path then every task MUST
be creating files with paths/filenames unique to the specific job.
It is not enough for them to be unique by task `part-00000.snappy.parquet`,
because each job will have tasks with the same name, so generate files with conflicting operations.

For the staging committers, enable `fs.s3a.committer.staging.unique-filenames` to ensure unique names are
generated during the upload. Otherwise, use what configuration options are available in the specific `FileOutputFormat`.

Note: by default, the option `mapreduce.output.basename` sets the base name for files;
changing that from the default `part` value to something unique for each job may achieve this.

For example, for any job executed through Hadoop MapReduce, the Job ID can be used in the filename.

```xml
<property>
  <name>mapreduce.output.basename</name>
  <value>part-${mapreduce.job.id}</value>
</property>
```

Even with these settings, the outcome of concurrent jobs to the same destination is
inherently nondeterministic -use with caution.

## Troubleshooting

### `Filesystem does not have support for 'magic' committer`

```
org.apache.hadoop.fs.s3a.commit.PathCommitException: `s3a://landsat-pds': Filesystem does not have support for 'magic' committer enabled
in configuration option fs.s3a.committer.magic.enabled
```

The Job is configured to use the magic committer,
but the S3A bucket has not been explicitly declared as supporting it.

Magic Committer support within the S3A filesystem has been enabled by default since Hadoop 3.3.1.
This error will only surface with a configuration which has explicitly disabled it.
Remove all global/per-bucket declarations of `fs.s3a.bucket.magic.enabled` or set them to `true`.

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.committer.magic.enabled</name>
  <value>true</value>
</property>
```

Tip: you can verify that a bucket supports the magic committer through the
`hadoop s3guard bucket-info` command:


```
> hadoop s3guard bucket-info -magic s3a://landsat-pds/
Location: us-west-2

S3A Client
        Signing Algorithm: fs.s3a.signing-algorithm=(unset)
        Endpoint: fs.s3a.endpoint=s3.amazonaws.com
        Encryption: fs.s3a.encryption.algorithm=none
        Input seek policy: fs.s3a.experimental.input.fadvise=normal
        Change Detection Source: fs.s3a.change.detection.source=etag
        Change Detection Mode: fs.s3a.change.detection.mode=server

S3A Committers
        The "magic" committer is supported in the filesystem
        S3A Committer factory class: mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory
        S3A Committer name: fs.s3a.committer.name=magic
        Store magic committer integration: fs.s3a.committer.magic.enabled=true

Security
        Delegation token support is disabled

Directory Markers
        The directory marker policy is "keep"
        Available Policies: delete, keep, authoritative
        Authoritative paths: fs.s3a.authoritative.path=```
```

### Error message: "File being created has a magic path, but the filesystem has magic file support disabled"

A file is being written to a path which is used for "magic" files,
files which are actually written to a different destination than their stated path
*but the filesystem doesn't support "magic" files*

This message should not appear through the committer itself &mdash;it will
fail with the error message in the previous section, but may arise
if other applications are attempting to create files under the path `/__magic/`.


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

(Setting this option will not interfere with the Staging Committers' use of HDFS,
as it explicitly sets the algorithm to "2" for that part of its work).

The other way to check which committer was used is to examine the `_SUCCESS` file.
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

The final cause "the output format is returning its own committer" is not
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


### Job/Task fails with PathExistsException: Destination path exists and committer conflict resolution mode is "fail"

This surfaces when either of two conditions are met.

1. The Directory committer is used with `fs.s3a.committer.staging.conflict-mode` set to
`fail` and the output/destination directory exists.
The job will fail in the driver during job setup.
1. The Partitioned Committer is used with `fs.s3a.committer.staging.conflict-mode` set to
`fail` and one of the partitions exist. The specific task(s) generating conflicting data will fail
during task commit, which will cause the entire job to fail.

If you are trying to write data and want write conflicts to be rejected, this is the correct
behavior: there was data at the destination so the job was aborted.

### Staging committer task fails with IOException: No space left on device

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

### Jobs run with directory/partitioned committers complete but the output is empty.

Make sure that `fs.s3a.committer.staging.tmp.path` is set to a path on the shared cluster
filesystem (usually HDFS). It MUST NOT be set to a local directory, as then the job committer,
running on a different host *will not see the lists of pending uploads to commit*.

### Magic output committer task fails "The specified upload does not exist" "Error Code: NoSuchUpload"

The magic committer is being used and a task writing data to the S3 store fails
with an error message about the upload not existing.

```
java.io.FileNotFoundException: upload part #1 upload
    YWHTRqBaxlsutujKYS3eZHfdp6INCNXbk0JVtydX_qzL5fZcoznxRbbBZRfswOjomddy3ghRyguOqywJTfGG1Eq6wOW2gitP4fqWrBYMroasAygkmXNYF7XmUyFHYzja
    on test/ITestMagicCommitProtocol-testParallelJobsToSameDestPaths/part-m-00000:
    com.amazonaws.services.s3.model.AmazonS3Exception: The specified upload does not
    exist. The upload ID may be invalid, or the upload may have been aborted or
    completed. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchUpload;
    Request ID: EBE6A0C9F8213AC3; S3 Extended Request ID:
    cQFm2N+666V/1HehZYRPTHX9tFK3ppvHSX2a8Oy3qVDyTpOFlJZQqJpSixMVyMI1D0dZkHHOI+E=),
    S3 Extended Request ID:
    cQFm2N+666V/1HehZYRPTHX9tFK3ppvHSX2a8Oy3qVDyTpOFlJZQqJpSixMVyMI1D0dZkHHOI+E=:NoSuchUpload

    at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:259)
    at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:112)
    at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$4(Invoker.java:315)
    at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:407)
    at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:311)
    at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:286)
    at org.apache.hadoop.fs.s3a.WriteOperationHelper.retry(WriteOperationHelper.java:154)
    at org.apache.hadoop.fs.s3a.WriteOperationHelper.uploadPart(WriteOperationHelper.java:590)
    at org.apache.hadoop.fs.s3a.S3ABlockOutputStream$MultiPartUpload.lambda$uploadBlockAsync$0(S3ABlockOutputStream.java:652)

Caused by: com.amazonaws.services.s3.model.AmazonS3Exception:
    The specified upload does not exist.
    The upload ID may be invalid, or the upload may have been aborted or completed.
    (Service: Amazon S3; Status Code: 404; Error Code: NoSuchUpload; Request ID: EBE6A0C9F8213AC3; S3 Extended Request ID:
    cQFm2N+666V/1HehZYRPTHX9tFK3ppvHSX2a8Oy3qVDyTpOFlJZQqJpSixMVyMI1D0dZkHHOI+E=),
    S3 Extended Request ID: cQFm2N+666V/1HehZYRPTHX9tFK3ppvHSX2a8Oy3qVDyTpOFlJZQqJpSixMVyMI1D0dZkHHOI+E=
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1712)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1367)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1113)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:770)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:744)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:726)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:686)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:668)
    at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:532)
    at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:512)
    at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4920)
    at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4866)
    at com.amazonaws.services.s3.AmazonS3Client.doUploadPart(AmazonS3Client.java:3715)
    at com.amazonaws.services.s3.AmazonS3Client.uploadPart(AmazonS3Client.java:3700)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.uploadPart(S3AFileSystem.java:2343)
    at org.apache.hadoop.fs.s3a.WriteOperationHelper.lambda$uploadPart$8(WriteOperationHelper.java:594)
    at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:110)
    ... 15 more
```

The block write failed because the previously created upload was aborted before the data could be written.

Causes

1. Another job has written to the same directory tree with an S3A committer
   -and when that job was committed, all incomplete uploads were aborted.
1. The `hadoop s3guard uploads --abort` command has being called on/above the directory.
1. Some other program is cancelling uploads to that bucket/path under it.
1. The job is lasting over 24h and a bucket lifecycle policy is aborting the uploads.

The `_SUCCESS` file from the previous job may provide diagnostics.

If the cause is Concurrent Jobs, see [Concurrent Jobs writing to the same destination](#concurrent-jobs).

### Job commit fails "java.io.FileNotFoundException: Completing multipart upload" "The specified upload does not exist"

The job commit fails with an error about the specified upload not existing.

```
java.io.FileNotFoundException: Completing multipart upload on
    test/DELAY_LISTING_ME/ITestDirectoryCommitProtocol-testParallelJobsToSameDestPaths/part-m-00001:
    com.amazonaws.services.s3.model.AmazonS3Exception:
    The specified upload does not exist.
    The upload ID may be invalid, or the upload may have been aborted or completed.
    (Service: Amazon S3; Status Code: 404; Error Code: NoSuchUpload;
    Request ID: 8E6173241D2970CB; S3 Extended Request ID:
    Pg6x75Q60UrbSJgfShCFX7czFTZAHR1Cy7W0Kh+o1uj60CG9jw7hL40tSa+wa7BRLbaz3rhX8Ds=),
    S3 Extended Request ID:
    Pg6x75Q60UrbSJgfShCFX7czFTZAHR1Cy7W0Kh+o1uj60CG9jw7hL40tSa+wa7BRLbaz3rhX8Ds=:NoSuchUpload

    at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:259)
    at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:112)
    at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$4(Invoker.java:315)
    at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:407)
    at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:311)
    at org.apache.hadoop.fs.s3a.WriteOperationHelper.finalizeMultipartUpload(WriteOperationHelper.java:261)
    at org.apache.hadoop.fs.s3a.WriteOperationHelper.commitUpload(WriteOperationHelper.java:549)
    at org.apache.hadoop.fs.s3a.commit.CommitOperations.innerCommit(CommitOperations.java:199)
    at org.apache.hadoop.fs.s3a.commit.CommitOperations.commit(CommitOperations.java:168)
    at org.apache.hadoop.fs.s3a.commit.CommitOperations.commitOrFail(CommitOperations.java:144)
    at org.apache.hadoop.fs.s3a.commit.CommitOperations.access$100(CommitOperations.java:74)
    at org.apache.hadoop.fs.s3a.commit.CommitOperations$CommitContext.commitOrFail(CommitOperations.java:612)
    at org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter.lambda$loadAndCommit$5(AbstractS3ACommitter.java:535)
    at org.apache.hadoop.fs.s3a.commit.Tasks$Builder.runSingleThreaded(Tasks.java:164)
    at org.apache.hadoop.fs.s3a.commit.Tasks$Builder.run(Tasks.java:149)
    at org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter.loadAndCommit(AbstractS3ACommitter.java:534)
    at org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter.lambda$commitPendingUploads$2(AbstractS3ACommitter.java:482)
    at org.apache.hadoop.fs.s3a.commit.Tasks$Builder$1.run(Tasks.java:253)

Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: The specified upload does not exist.
    The upload ID may be invalid, or the upload may have been aborted or completed.
    (Service: Amazon S3; Status Code: 404; Error Code: NoSuchUpload; Request ID: 8E6173241D2970CB;
    S3 Extended Request ID: Pg6x75Q60UrbSJgfShCFX7czFTZAHR1Cy7W0Kh+o1uj60CG9jw7hL40tSa+wa7BRLbaz3rhX8Ds=),

    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1712)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1367)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1113)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:770)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:744)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:726)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:686)
    at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:668)
    at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:532)
    at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:512)
    at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4920)
    at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4866)
    at com.amazonaws.services.s3.AmazonS3Client.completeMultipartUpload(AmazonS3Client.java:3464)
    at org.apache.hadoop.fs.s3a.WriteOperationHelper.lambda$finalizeMultipartUpload$1(WriteOperationHelper.java:267)
    at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:110)
```

The problem is likely to be that of the previous one: concurrent jobs are writing the same output directory,
or another program has cancelled all pending uploads.

See [Concurrent Jobs writing to the same destination](#concurrent-jobs).

### Job commit fails `java.io.FileNotFoundException` "File hdfs://.../staging-uploads/_temporary/0 does not exist"

The Staging committer will fail in job commit if the intermediate directory on the cluster FS is missing during job commit.

This is possible if another job used the same staging upload directory and,
 after committing its work, it deleted the directory.

A unique Job ID is required for each spark job run by a specific user.
Spark generates job IDs for its committers using the current timestamp,
and if two jobs/stages are started in the same second, they will have the same job ID.

See [SPARK-33230](https://issues.apache.org/jira/browse/SPARK-33230).

This is fixed in all spark releases which have the patch applied.

You can set the property `fs.s3a.committer.staging.require.uuid` to fail
the staging committers fast if a unique Job ID isn't found in
`spark.sql.sources.writeJobUUID`.

### Job setup fails `Job/task context does not contain a unique ID in spark.sql.sources.writeJobUUID`

This will surface in job setup if the option `fs.s3a.committer.require.uuid` is `true`, and
one of the following conditions are met

1. The committer is being used in a Hadoop MapReduce job, whose job attempt ID is unique
   -there is no need to add this requirement.
   Fix: unset `fs.s3a.committer.require.uuid`.
1. The committer is being used in spark, and the version of spark being used does not
   set the `spark.sql.sources.writeJobUUID` property.
   Either upgrade to a new spark release, or set `fs.s3a.committer.generate.uuid` to true.
