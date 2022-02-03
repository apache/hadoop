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


# "Intermediate Manifest" Committer for Azure and GCS

## Problem:

The only committer of work from Spark to Azure ADLS Gen 2 "abfs://" storage
which is safe to use is the "v1 file committer".

This is "correct" in that if a task attempt fails, its output is guaranteed not
to be included in the final out. The "v2" commit algorithm cannot meet that
guarantee, which is why it is no longer the default.

But: it is slow, especially on jobs where deep directory trees of output are used.
Why is it slow? It's hard to point at a particular cause, primarily because of
the lack of any instrumentation in the `FileOutputCommitter`.
Stack traces of running jobs generally show `rename()`, though list operations
do surface too.

On Google GCS, neither the v1 nor v2 algorithm are _safe_ because the google
filesystem doesn't have the atomic directory rename which the v1 algorithm
requires.

A further issue is that both Azure and GCS storage may encounter scale issues
with deleting directories with many descendants.
This can trigger timeouts because the FileOutputCommitter assumes that
cleaning up after the job is a fast call to `delete("_temporary", true)`.

## Solution.

The _Intermediate Manifest_ committer is a new committer for
work which should deliver performance on ABFS
for "real world" queries, and performance and correctness on GCS.

This committer uses the extension point which came in for the S3A committers.
Users can declare a new committer factory for abfs:// and gcs:// URLs.
A suitably configured spark deployment will pick up the new committer.

Directory performance issues in job cleanup can be addressed by two options
1. The committer can be configured to move the temporary directory under `~/.trash`
in the cluster FS. This may benefit azure, but will not benefit GCS.
1. The committer will parallelize deletion of task attempt directories before
   deleting the `_temporary` directory.
   This is highly beneficial on GCS; may or may not be beneficial on ABFS.
   (More specifically: use it if you are using OAuth to authenticate).

Suitably configured MR and Spark deployments will pick up the new committer.

The committer can be used with any filesystem client which has a "real" file rename where
only one process may rename a file, and if it exists, then the caller is notified.



# How it works

The full details are covered in [Manifest Committer Architecture](manifest_committer_architecture.html).

#### Switching to the committer

The hooks put in to support the S3A committers were designed to allow every
filesystem schema to provide their own committer.
See [Switching To an S3A Committer](../../hadoop-aws/tools/hadoop-aws/committers.html#Switching_to_an_S3A_Committer)

A factory for the abfs schema would be defined in
`mapreduce.outputcommitter.factory.scheme.abfs` ; and a similar one for `gcs`.

Some matching spark configuration changes, especially for parquet binding, will be required.
These can be done in `core-site.xml`, if it is not defined in the `mapred-default.xml` JAR.


```xml
<property>
  <name>mapreduce.outputcommitter.factory.scheme.abfs</name>
  <value>org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory</value>
  <description>
    The default committer factory for ABFS is for the manifest committer.
  </description>
</property>
<property>
  <name>mapreduce.outputcommitter.factory.scheme.gs</name>
  <value>org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory</value>
  <description>
    The default committer factory for GCS is for the manifest committer.
  </description>
</property>
```

In `spark-default`

```
spark.hadoop.mapreduce.outputcommitter.factory.scheme.abfs org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory
spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory
spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol

```

The hadoop committer settings can be validated in a recent build of [cloudstore](https://github.com/steveloughran/cloudstore)
and its `committerinfo` command.
This command instantiates a committer for that path through the same factory mechanism as MR and spark jobs use,
then prints its toString value.

```
hadoop jar cloudstore-1.0.jar committerinfo abfs://testing@ukwest.dfs.core.windows.net/

2021-09-16 19:42:59,731 [main] INFO  commands.CommitterInfo (StoreDurationInfo.java:<init>(53)) - Starting: Create committer
Committer factory for path abfs://testing@ukwest.dfs.core.windows.net/ is
 org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory@3315d2d7
  (classname org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory)
2021-09-16 19:43:00,897 [main] INFO  manifest.ManifestCommitter (ManifestCommitter.java:<init>(144)) - Created ManifestCommitter with
   JobID job__0000, Task Attempt attempt__0000_r_000000_1 and destination abfs://testing@ukwest.dfs.core.windows.net/
Created committer of class org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter:
 ManifestCommitter{ManifestCommitterConfig{destinationDir=abfs://testing@ukwest.dfs.core.windows.net/,
   role='task committer',
   taskAttemptDir=abfs://testing@ukwest.dfs.core.windows.net/_temporary/manifest_job__0000/0/_temporary/attempt__0000_r_000000_1,
   createJobMarker=true,
   jobUniqueId='job__0000',
   jobUniqueIdSource='JobID',
   jobAttemptNumber=0,
   jobAttemptId='job__0000_0',
   taskId='task__0000_r_000000',
   taskAttemptId='attempt__0000_r_000000_1'},
   iostatistics=counters=();

gauges=();

minimums=();

maximums=();

means=();
}

```


## Verifying that the committer was used

The new committer will write a JSON summary of the operation, including statistics, in the `_SUCCESS` file.

If this file exists and is zero bytes long: the classic `FileOutputCommitter` was used.

If this file exists and is greater than zero bytes wrong, either the manifest committer was used,
or in the case of S3A filesystems, one of the S3A committers. They all use the same JSON format.

## Configuration options

| Option | Meaning | Default Value |
|--------|---------|---------------|
| `mapreduce.manifest.committer.io.rate` | Rate limit in operations/second for store operations. | `10000` |
| `mapreduce.manifest.committer.io.thread.count` | Thread count for parallel operations | `64` |
| `mapreduce.manifest.committer.store.operations.classname` | Classname for Store Operations | `""` |
| `mapreduce.manifest.committer.prepare.parent.directories` | Prepare parent directories? | `false` |
| `mapreduce.manifest.committer.prepare.target.files` | Delete target files? | `false` |
| `mapreduce.manifest.committer.validate.output` | Perform output validation? | `false` |
| `mapreduce.manifest.committer.summary.report.directory` | directory to save reports. | `""` |
| `mapreduce.manifest.committer.cleanup.move.to.trash` | Move the `_temporary` directory to `~/.trash` | `false` |
| `mapreduce.manifest.committer.cleanup.parallel.delete.attempt.directories` | Delete task attempt directories in parallel | `true` |
| `mapreduce.fileoutputcommitter.cleanup.skipped` | Skip cleanup of `_temporary` directory| `false` |
| `mapreduce.fileoutputcommitter.cleanup-failures.ignored` | Ignore errors during cleanup | `false` |
| `mapreduce.fileoutputcommitter.marksuccessfuljobs` | Create a `_SUCCESS` marker file on successful completion. (and delete any existing one in job setup) | `true` |

## Job Commit Preparation options `mapreduce.manifest.committer.prepare`

Two committer options enable behaviors found in the classic FileOutputCommitter.

Setting these options to `false` increases performance with a risk of job failures in
jobs which update directories without any initial cleanup, and either of two situations
arise

1. A previous job has created a file at a path which is now a parent/ancestor directory
   of a file being committed.
2. A previous job has created a file *or directory* at a path which the current job
   generated a file.

Problem 1 is rare and unusual. If it does arise, set
`mapreduce.manifest.committer.prepare.parent.directories` to `true`
This adds the overhead of probing every parent directory in the table,
which is done across multiple threads.

```
spark.hadoop.mapreduce.manifest.committer.prepare.parent.directories true
```

Problem 2, "existing files" may happen in jobs which appends data to existing
tables _and do not generate unique names_.

Apache Spark does generate unique filenames for ORC and Parquet
since
[SPARK-8406](https://issues.apache.org/jira/browse/SPARK-8406)
_Adding UUID to output file name to avoid accidental overwriting_

Avoiding checks for/deleting target files saves one delete call per file being committed,
so can save a significant amount of store IO.

When appending to existing tables, using formats other than ORC and parquet,
unless confident that unique identifiers
are added to each filename, enable deletion of the target files.

```
spark.hadoop.mapreduce.manifest.committer.prepare.target.files true
```

## Collecting Job Summaries into a report directory

The committer can be configured to save the `_SUCCESS` summary files to a report directory,
Irrespective of whether the job succeed or failed.

`mapreduce.manifest.committer.summary.report.directory`

If this is set to a path in the cluster FS/object store then after job commit
succeeds/fails or after an `abort()` operation, a JSON file is created using the Job ID
in the filename.

This allows for the statistics of jobs to be collected irrespective of their outcome,
Whether or not saving the `_SUCCESS` marker is enabled, and without problems
caused by a chain of queries overwriting the markers.

# Viewing Success/Summary files through the `ManifestPrinter` command.

The summary files are JSON, and can be viewed in any text editor.

For a more succinct summary, including better display of statistics, use the `ManifestPrinter` tool.

```
hadoop org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestPrinter <path>
```


## Testing only: Validating output

The option `mapreduce.manifest.committer.validate.output` triggers a check
of every renamed file to verify it has the expected length.

This adds the overhead of a `HEAD` request per file, and so is
recommended for testing only.

There is no verification of the actual contents.

## Cleanup

Job cleanup is convoluted as it is designed to address a number of issues which
may surface in cloud storage.

* Slow performance for deletion of directories (GCS, where it is file-by-file)
* Service timeout when deleting very deep and wide directory trees.
  (Azure with OAuth auth)
* General resilience to cleanup issues escalating to job failures.


| Option | Meaning | Default Value |
|--------|---------|---------------|
| `mapreduce.fileoutputcommitter.cleanup.skipped` | Skip cleanup of `_temporary` directory| `false` |
| `mapreduce.fileoutputcommitter.cleanup-failures.ignored` | Ignore errors during cleanup | `false` |
| `mapreduce.manifest.committer.cleanup.parallel.delete.attempt.directories` | Delete task attempt directories in parallel | `true` |
| `mapreduce.manifest.committer.cleanup.move.to.trash` | Move the `_temporary` directory to `~/.trash` | `false` |

The algorithm is:

1. If `mapreduce.fileoutputcommitter.cleanup.skipped` is true, skip all cleanup.
2. if `mapreduce.manifest.committer.cleanup.move.to.trash` is true, jump to step #5
3. Attempt parallel task attempt directory delete unless
   `mapreduce.manifest.committer.cleanup.parallel.delete.attempt.directories` is false.
   Any error here is swallowed.
4. Attempt delete of base `_temporary` directory. Any error here is cached.
5. Delete failed in step #4 or was skipped, and if trash is enabled for the filesystem
   attempt to rename  `_temporary` directory under trash dir.

If the dir could not be deleted/renamed:
1. if `mapreduce.fileoutputcommitter.cleanup-failures.ignored`
   is true then the stage warns and continues.
2. Else the last raised exception is rethown.

It's complicated, but the goal is to perform a fast/scalable delete
fall back to a rename to trash if that fails, and
throw a meaningful exception if that didn't work.

# Rate Limiting

To avoid triggering store throttling and backoff delays, the committer rate-limits read- and write
operations per second.

| Option | Meaning |
|--------|---------|
| `mapreduce.manifest.committer.io.rate` | Rate limit in operations/second for IO operations. |

Set the option to `0` remove all rate limiting.

If rate limiting is imposed, The statistics `io_acquire_write_permit` and `io_acquire_read_permit` report
the time to acquire permits for read and write and so identify any delays.

The need for any throttling can be determined by looking at job logs to see if
throttling events and retries took place, or, (often easier) the store service's
logs and their throttling status codes (usually 503 or 500).

# Working with Azure Storage


The option `mapreduce.manifest.committer.store.operations.classname` should be set to the Azure Specific store binding,  `org.apache.hadoop.fs.azurebfs.commit.AbfsManifestStoreOperations`.

This allows for ABFS-specific performance and consistency logic to be used from within the committer.
In particular, the [Etag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) header
can be collected in listings and used in the job commit phase.

```xml
<property>
  <name>mapreduce.outputcommitter.factory.scheme.abfs</name>
  <value>org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory</value>
</property>
```

The core set of Azure-optimized options becomes

```xml
<property>
<name>mapreduce.outputcommitter.factory.scheme.abfs</name>
<value>org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory</value>
</property>

<property>
  <name>mapreduce.manifest.committer.store.operations.classname</name>
  <value>org.apache.hadoop.fs.azurebfs.commit.AbfsManifestStoreOperations</value>
</property>

<property>
  <name>mapreduce.manifest.committer.cleanup.parallel.delete.attempt.directories</name>
  <value>true/value>
  <description>Parallel directory deletion to address scale-related timeouts.</description>
</property>


<property>
<name>mapreduce.manifest.committer.io.rate</name>
<value>10000</value>
<description>Rate limit for a store, unless increased by Microsoft</description>
</property>

```

And optional settings for debugging/performance analysis

```xml

<property>
  <name>mapreduce.manifest.committer.summary.report.directory/name>
  <value>abfs:// Path within same store/separate store</value>
  <description>Optional: path to where job summaries are saved</description>
</property>

<property>
<name>mapreduce.manifest.committer.validate.output</name>
<value>true</value>
<description>Validate the output</description>
</property>

        ``
```