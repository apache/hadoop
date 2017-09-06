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

# The S3A Committers

<!-- DISABLEDMACRO{toc|fromDepth=0|toDepth=5} -->

This page covers the S3A Committers, which can commit work directly
to an S3 object store which supports consistent metadata.

These committers are designed to solve a fundamental problem which
the standard committers of work cannot do to S3: consistent, high performance,
reliable commitment of work done by individual workers into the final set of
results of a job.

For details on their internal design, see
[S3A Committers: Architecture and Implementation](s3a_committer_architecture.html).



### Why you Must Not Use the normal committers to write to Amazon S3

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

1. It has inconsistent directory listings
1. It mimics `rename()` by copying files and then deleting the originals.
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






## Choosing a committer

The choice of which committer to use for writing data via `FileOutputFormat`
is set in the configuration option `mapreduce.pathoutputcommitter.factory.class`.
This declares a the classname of a class which creates committers for jobs and tasks.


| factory | description |
|--------|---------|
| `org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory` |  The default file output committer |
| `org.apache.hadoop.fs.s3a.commit.DynamicCommitterFactory` | Dynamically choose the committer on a per-bucket basis |
| `org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory` |  Use the Directory Staging Committer|
| `org.apache.hadoop.fs.s3a.commit.staging.PartitonedStagingCommitterFactory` |  Partitioned Staging Committer|
| `org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory` | Use the magic committer (not yet ready for production use) |

All of the s3a committers revert to provide a classic FileOutputCommitter instance
when a commmitter is requested for any filesystem other than an S3A one.
This allows any of these committers to be declared in an application configuration,
without worrying about the job failing for any queries using `hdfs://` paths as
the final destination of work.

The Dynamic committer factory is different in that it allows any of the other committer
factories to be used to create a committer, based on the specific value of theoption `fs.s3a.committer.name`:

| value of `fs.s3a.committer.name` |  meaning |
|--------|---------|
| `file` | the original File committer; (not safe for use with S3 storage) |
| `directory` | directory staging committer |
| `partition` | partition staging committer |
| `magic` | the "magic" committer |

The dynamic committer was originally written to allow for easier performance
testing of the committers from applications such as Apache Zeppelin notebooks:
different buckets can be given a different committer with S3A's per-bucket
configuration, and the performance of the operations compared.




### Staging Committer Options

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

## The Magic committer

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
