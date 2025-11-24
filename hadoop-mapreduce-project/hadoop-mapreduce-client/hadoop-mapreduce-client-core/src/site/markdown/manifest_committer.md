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


# The Manifest Committer for Azure and Google Cloud Storage

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

This documents how to use the _Manifest Committer_.

The _Manifest_ committer is a committer for work which provides
performance on ABFS for "real world" queries,
and performance and correctness on GCS.
It also works with other filesystems, including HDFS.
However, the design is optimized for object stores where
listing operations are slow and expensive.

The architecture and implementation of the committer is covered in
[Manifest Committer Architecture](manifest_committer_architecture.html).


The protocol and its correctness are covered in
[Manifest Committer Protocol](manifest_committer_protocol.html).

It was added in March 2022.
As of April 2024, the problems which surfaced have been
* Memory use at scale.
* Directory deletion scalability.
* Resilience to task commit to rename failures.

That is: the core algorithms is correct, but task commit
robustness was insufficient to some failure conditions.
And scale is always a challenge, even with components tested through
large TPC-DS test runs.

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

Directory performance issues in job cleanup can be addressed by some options
1. The committer will parallelize deletion of task attempt directories before
   deleting the `_temporary` directory.
2. An initial attempt to delete the  `_temporary` directory before the parallel
   attempt is made.
3. Exceptions can be supressed, so that cleanup failures do not fail the job
4. Cleanup can be disabled.

The committer can be used with any filesystem client which has a "real" file rename()
operation.
It has been optimised for remote object stores where listing and file probes
are expensive -the design is less likely to offer such signifcant speedup
on HDFS -though the parallel renaming operations will speed up jobs
there compared to the classic v1 algorithm.

# <a name="how"></a> How it works

The full details are covered in [Manifest Committer Architecture](manifest_committer_architecture.html).

# <a name="use"></a> Using the committer

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
  <value>org.apache.hadoop.fs.azurebfs.commit.AzureManifestCommitterFactory</value>
</property>
<property>
  <name>mapreduce.outputcommitter.factory.scheme.gs</name>
  <value>org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory</value>
</property>
```

## Binding to the manifest committer in Spark.

In Apache Spark, the configuration can be done either with command line options (after the `--conf`) or by using the `spark-defaults.conf` file.
The following is an example of using `spark-defaults.conf` also including the configuration for Parquet with a subclass of the parquet committer which uses the factory mechanism internally.

```
spark.hadoop.mapreduce.outputcommitter.factory.scheme.abfs org.apache.hadoop.fs.azurebfs.commit.AzureManifestCommitterFactory
spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory
spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
```


### <a name="committerinfo"></a> Using the Cloudstore `committerinfo` command to probe committer bindings.

The hadoop committer settings can be validated in a recent build of [cloudstore](https://github.com/steveloughran/cloudstore)
and its `committerinfo` command.
This command instantiates a committer for that path through the same factory mechanism as MR and spark jobs use,
then prints its `toString` value.

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

If this file exists and is greater than zero bytes long, either the manifest committer was used,
or in the case of S3A filesystems, one of the S3A committers. They all use the same JSON format.

# <a name="configuration"></a> Configuration options

Here are the main configuration options of the committer.


| Option | Meaning | Default Value |
|--------|---------|---------------|
| `mapreduce.manifest.committer.delete.target.files` | Delete target files? | `false` |
| `mapreduce.manifest.committer.io.threads` | Thread count for parallel operations | `64` |
| `mapreduce.manifest.committer.summary.report.directory` | directory to save reports. | `""` |
| `mapreduce.manifest.committer.cleanup.parallel.delete` | Delete temporary directories in parallel | `true` |
| `mapreduce.manifest.committer.cleanup.parallel.delete.base.first` | Attempt to delete the base directory before parallel task attempts | `false` |
| `mapreduce.fileoutputcommitter.cleanup.skipped` | Skip cleanup of `_temporary` directory| `false` |
| `mapreduce.fileoutputcommitter.cleanup-failures.ignored` | Ignore errors during cleanup | `false` |
| `mapreduce.fileoutputcommitter.marksuccessfuljobs` | Create a `_SUCCESS` marker file on successful completion. (and delete any existing one in job setup) | `true` |

There are some more, as covered in the (Advanced)[#advanced] section.

WARNING: setting `mapreduce.fileoutputcommitter.cleanup.skipped` to `true` is not compatible with version 1 of the committer and can cause unexpected behaviors.

## <a name="scaling"></a> Scaling jobs `mapreduce.manifest.committer.io.threads`

The core reason that this committer is faster than the classic `FileOutputCommitter`
is that it tries to parallelize as much file IO as it can during job commit, specifically:

* task manifest loading
* deletion of files where directories will be created
* directory creation
* file-by-file renaming
* deletion of task attempt directories in job cleanup

These operations are all performed in the same thread pool, whose size is set
in the option `mapreduce.manifest.committer.io.threads`.

Larger values may be used.

Hadoop XML configuration
```xml
<property>
  <name>mapreduce.manifest.committer.io.threads</name>
  <value>32</value>
</property>
```

In `spark-defaults.conf`

```properties
spark.hadoop.mapreduce.manifest.committer.io.threads 32
```

A larger value than that of the number of cores allocated to
the MapReduce AM or Spark Driver does not directly overload
the CPUs, as the threads are normally waiting for (slow) IO
against the object store/filesystem to complete.

Manifest loading in job commit may be memory intensive;
the larger the number of threads, the more manifests which
will be loaded simultaneously.

Caveats
* In Spark, multiple jobs may be committed in the same process,
  each of which will create their own thread pool during job
  commit or cleanup.
* Azure rate throttling may be triggered if too many IO requests
  are made against the store. The rate throttling option
  `mapreduce.manifest.committer.io.rate` can help avoid this.

## <a name="deleting"></a> Optional: deleting target files in Job Commit

The classic `FileOutputCommitter` deletes files at the destination paths
before renaming the job's files into place.

This is optional in the manifest committers, set in the option
`mapreduce.manifest.committer.delete.target.files` with a default value of `false`.

This increases performance and is safe to use when all files created by a job
have unique filenames.

Apache Spark does generate unique filenames for ORC and Parquet since
[SPARK-8406](https://issues.apache.org/jira/browse/SPARK-8406)
_Adding UUID to output file name to avoid accidental overwriting_

Avoiding checks for/deleting target files saves one delete call per file being committed, so can
save a significant amount of store IO.

When appending to existing tables, using formats other than ORC and parquet,
unless confident that unique identifiers
are added to each filename, enable deletion of the target files.

```
spark.hadoop.mapreduce.manifest.committer.delete.target.files true
```

*Note 1:* the committer will skip deletion operations when it
created the directory into which a file is to be renamed.
This makes it slightly more efficient, at least if jobs
appending data are creating and writing into new partitions.

*Note 2:* the committer still requires tasks within a single
job to create unique files. This is foundational for
any job to generate correct data.

# <a name="dynamic"></a> Spark Dynamic Partition overwriting

Spark has a feature called "Dynamic Partition Overwrites",

This can be initiated in SQL
```SQL
INSERT OVERWRITE TABLE ...
```
Or through DataSet writes where the mode is `overwrite` and the partitioning matches
that of the existing table
```scala
sparkConf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
// followed by an overwrite of a Dataset into an existing partitioned table.
eventData2
  .write
  .mode("overwrite")
  .partitionBy("year", "month")
  .format("parquet")
  .save(existingDir)
```

This feature is implemented in Spark, which
1. Directs the job to write its new data to a temporary directory
1. After job commit completes, scans the output to identify the leaf directories "partitions" into which data was written.
1. Deletes the content of those directories in the destination table
1. Renames the new files into the partitions.

This is all done in spark, which takes over the tasks of scanning
the intermediate output tree, deleting partitions and of
renaming the new files.

This feature also adds the ability for a job to write data entirely outside
the destination table, which is done by
1. writing new files into the working directory
1. spark moving them to the final destination in job commit


The manifest committer is compatible with dynamic partition overwrites
on Azure and Google cloud storage as together they meet the core requirements of
the extension:
1. The working directory returned in `getWorkPath()` is in the same filesystem
  as the final output.
2. `rename()` is an `O(1)` operation which is safe and fast to use when committing a job.

None of the S3A committers support this. Condition (1) is not met by
the staging committers, while (2) is not met by S3 itself.

To use the manifest committer with dynamic partition overwrites, the
spark version must contain
[SPARK-40034](https://issues.apache.org/jira/browse/SPARK-40034)
_PathOutputCommitters to work with dynamic partition overwrite_.

Be aware that the rename phase of the operation will be slow
if many files are renamed -this is done sequentially.
Parallel renaming would speed this up, *but could trigger the abfs overload
problems the manifest committer is designed to both minimize the risk
of and support recovery from*

The spark side of the commit operation will be listing/treewalking
the temporary output directory (some overhead), followed by
the file promotion, done with a classic filesystem `rename()`
call. There will be no explicit rate limiting here.

*What does this mean?*

It means that _dynamic partitioning should not be used on Azure Storage
for SQL queries/Spark DataSet operations where many thousands of files are created.
The fact that these will suffer from performance problems before
throttling scale issues surface, should be considered a warning.

# <a name="SUCCESS"></a> Job Summaries in `_SUCCESS` files

The original hadoop committer creates a zero byte `_SUCCESS` file in the root of the output directory
unless disabled.

This committer writes a JSON summary which includes
* The name of the committer.
* Diagnostics information.
* A list of some of the files created (for testing; a full list is excluded as it can get big).
* IO Statistics.

If, after running a query, this `_SUCCESS` file is zero bytes long,
*the new committer has not been used*

If it is not empty, then it can be examined.

## <a name="printer"></a> Viewing `_SUCCESS` file files through the `ManifestPrinter` tool.

The summary files are JSON, and can be viewed in any text editor.

For a more succinct summary, including better display of statistics, use the `ManifestPrinter` tool.

```bash
hadoop org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestPrinter <path>
```

This works for the files saved at the base of an output directory, and
any reports saved to a report directory.

Example from a run of the `ITestAbfsTerasort` MapReduce terasort.

```
bin/mapred successfile abfs://testing@ukwest.dfs.core.windows.net/terasort/_SUCCESS

Manifest file: abfs://testing@ukwest.dfs.core.windows.net/terasort/_SUCCESS
succeeded: true
created: 2024-04-18T18:34:34.003+01:00[Europe/London]
committer: org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter
hostname: pi5
jobId: job_1713461587013_0003
jobIdSource: JobID
Diagnostics
  mapreduce.manifest.committer.io.threads = 192
  principal = alice
  stage = committer_commit_job

Statistics:
counters=((commit_file_rename=1)
(committer_bytes_committed=21)
(committer_commit_job=1)
(committer_files_committed=1)
(committer_task_directory_depth=2)
(committer_task_file_count=2)
(committer_task_file_size=21)
(committer_task_manifest_file_size=37157)
(job_stage_cleanup=1)
(job_stage_create_target_dirs=1)
(job_stage_load_manifests=1)
(job_stage_optional_validate_output=1)
(job_stage_rename_files=1)
(job_stage_save_success_marker=1)
(job_stage_setup=1)
(op_create_directories=1)
(op_delete=3)
(op_delete_dir=1)
(op_get_file_status=9)
(op_get_file_status.failures=6)
(op_list_status=3)
(op_load_all_manifests=1)
(op_load_manifest=2)
(op_mkdirs=4)
(op_msync=1)
(op_rename=2)
(op_rename.failures=1)
(task_stage_commit=2)
(task_stage_save_task_manifest=1)
(task_stage_scan_directory=2)
(task_stage_setup=2));

gauges=();

minimums=((commit_file_rename.min=141)
(committer_commit_job.min=2306)
(committer_task_directory_count=0)
(committer_task_directory_depth=1)
(committer_task_file_count=0)
(committer_task_file_size=0)
(committer_task_manifest_file_size=18402)
(job_stage_cleanup.min=196)
(job_stage_create_target_dirs.min=2)
(job_stage_load_manifests.min=687)
(job_stage_optional_validate_output.min=66)
(job_stage_rename_files.min=161)
(job_stage_save_success_marker.min=653)
(job_stage_setup.min=571)
(op_create_directories.min=1)
(op_delete.min=57)
(op_delete_dir.min=129)
(op_get_file_status.failures.min=57)
(op_get_file_status.min=55)
(op_list_status.min=202)
(op_load_all_manifests.min=445)
(op_load_manifest.min=171)
(op_mkdirs.min=67)
(op_msync.min=0)
(op_rename.failures.min=266)
(op_rename.min=139)
(task_stage_commit.min=206)
(task_stage_save_task_manifest.min=651)
(task_stage_scan_directory.min=206)
(task_stage_setup.min=127));

maximums=((commit_file_rename.max=141)
(committer_commit_job.max=2306)
(committer_task_directory_count=0)
(committer_task_directory_depth=1)
(committer_task_file_count=1)
(committer_task_file_size=21)
(committer_task_manifest_file_size=18755)
(job_stage_cleanup.max=196)
(job_stage_create_target_dirs.max=2)
(job_stage_load_manifests.max=687)
(job_stage_optional_validate_output.max=66)
(job_stage_rename_files.max=161)
(job_stage_save_success_marker.max=653)
(job_stage_setup.max=571)
(op_create_directories.max=1)
(op_delete.max=113)
(op_delete_dir.max=129)
(op_get_file_status.failures.max=231)
(op_get_file_status.max=61)
(op_list_status.max=300)
(op_load_all_manifests.max=445)
(op_load_manifest.max=436)
(op_mkdirs.max=123)
(op_msync.max=0)
(op_rename.failures.max=266)
(op_rename.max=139)
(task_stage_commit.max=302)
(task_stage_save_task_manifest.max=651)
(task_stage_scan_directory.max=302)
(task_stage_setup.max=157));

means=((commit_file_rename.mean=(samples=1, sum=141, mean=141.0000))
(committer_commit_job.mean=(samples=1, sum=2306, mean=2306.0000))
(committer_task_directory_count=(samples=4, sum=0, mean=0.0000))
(committer_task_directory_depth=(samples=2, sum=2, mean=1.0000))
(committer_task_file_count=(samples=4, sum=2, mean=0.5000))
(committer_task_file_size=(samples=2, sum=21, mean=10.5000))
(committer_task_manifest_file_size=(samples=2, sum=37157, mean=18578.5000))
(job_stage_cleanup.mean=(samples=1, sum=196, mean=196.0000))
(job_stage_create_target_dirs.mean=(samples=1, sum=2, mean=2.0000))
(job_stage_load_manifests.mean=(samples=1, sum=687, mean=687.0000))
(job_stage_optional_validate_output.mean=(samples=1, sum=66, mean=66.0000))
(job_stage_rename_files.mean=(samples=1, sum=161, mean=161.0000))
(job_stage_save_success_marker.mean=(samples=1, sum=653, mean=653.0000))
(job_stage_setup.mean=(samples=1, sum=571, mean=571.0000))
(op_create_directories.mean=(samples=1, sum=1, mean=1.0000))
(op_delete.mean=(samples=3, sum=240, mean=80.0000))
(op_delete_dir.mean=(samples=1, sum=129, mean=129.0000))
(op_get_file_status.failures.mean=(samples=6, sum=614, mean=102.3333))
(op_get_file_status.mean=(samples=3, sum=175, mean=58.3333))
(op_list_status.mean=(samples=3, sum=671, mean=223.6667))
(op_load_all_manifests.mean=(samples=1, sum=445, mean=445.0000))
(op_load_manifest.mean=(samples=2, sum=607, mean=303.5000))
(op_mkdirs.mean=(samples=4, sum=361, mean=90.2500))
(op_msync.mean=(samples=1, sum=0, mean=0.0000))
(op_rename.failures.mean=(samples=1, sum=266, mean=266.0000))
(op_rename.mean=(samples=1, sum=139, mean=139.0000))
(task_stage_commit.mean=(samples=2, sum=508, mean=254.0000))
(task_stage_save_task_manifest.mean=(samples=1, sum=651, mean=651.0000))
(task_stage_scan_directory.mean=(samples=2, sum=508, mean=254.0000))
(task_stage_setup.mean=(samples=2, sum=284, mean=142.0000)));

```

## <a name="summaries"></a> Collecting Job Summaries `mapreduce.manifest.committer.summary.report.directory`

The committer can be configured to save the `_SUCCESS` summary files to a report directory,
irrespective of whether the job succeed or failed, by setting a filesystem path in
the option `mapreduce.manifest.committer.summary.report.directory`.

The path does not have to be on the same
store/filesystem as the destination of work. For example, a local filesystem could be used.

XML

```xml
<property>
  <name>mapreduce.manifest.committer.summary.report.directory</name>
  <value>file:///tmp/reports</value>
</property>
```

spark-defaults.conf

```
spark.hadoop.mapreduce.manifest.committer.summary.report.directory file:///tmp/reports
```

This allows for the statistics of jobs to be collected irrespective of their outcome, Whether or not
saving the `_SUCCESS` marker is enabled, and without problems caused by a chain of queries
overwriting the markers.

The `mapred successfile` operation can be used to print these reports.

# <a name="cleanup"></a> Cleanup

Job cleanup is convoluted as it is designed to address a number of issues which
may surface in cloud storage.

* Slow performance for deletion of directories (GCS).
* Timeout when deleting very deep and wide directory trees (Azure).
* General resilience to cleanup issues escalating to job failures.


| Option                                                            | Meaning                                                            | Default Value |
|-------------------------------------------------------------------|--------------------------------------------------------------------|---------------|
| `mapreduce.fileoutputcommitter.cleanup.skipped`                   | Skip cleanup of `_temporary` directory                             | `false`       |
| `mapreduce.fileoutputcommitter.cleanup-failures.ignored`          | Ignore errors during cleanup                                       | `false`       |
| `mapreduce.manifest.committer.cleanup.parallel.delete`            | Delete task attempt directories in parallel                        | `true`        |
| `mapreduce.manifest.committer.cleanup.parallel.delete.base.first` | Attempt to delete the base directory before parallel task attempts | `false`       |

The algorithm is:

```python
if "mapreduce.fileoutputcommitter.cleanup.skipped":
  return
if "mapreduce.manifest.committer.cleanup.parallel.delete":
  if "mapreduce.manifest.committer.cleanup.parallel.delete.base.first" :
    if delete("_temporary"):
      return
  delete(list("$task-directories")) catch any exception
if not "mapreduce.fileoutputcommitter.cleanup.skipped":
  delete("_temporary"); catch any exception
if caught-exception and not "mapreduce.fileoutputcommitter.cleanup-failures.ignored":
  raise caught-exception
```

It's a bit complicated, but the goal is to perform a fast/scalable delete and
throw a meaningful exception if that didn't work.

For ABFS set `mapreduce.manifest.committer.cleanup.parallel.delete.base.first` to `true`
which should normally result in less network IO and a faster cleanup.

```
spark.hadoop.mapreduce.manifest.committer.cleanup.parallel.delete.base.first true
```

For GCS, setting `mapreduce.manifest.committer.cleanup.parallel.delete.base.first`
to `false` may speed up cleanup.

If somehow errors surface during cleanup, ignoring failures will ensure the job
is still considered a success.
`mapreduce.fileoutputcommitter.cleanup-failures.ignored = true`

Disabling cleanup even avoids the overhead of cleanup, but
requires a workflow or manual operation to clean up all
`_temporary` directories on a regular basis:
`mapreduce.fileoutputcommitter.cleanup.skipped = true`.

# <a name="abfs"></a> Working with Azure ADLS Gen2 Storage

To switch to the manifest committer, the factory for committers for destinations with `abfs://` URLs must
be switched to the manifest committer factory, either for the application or
the entire cluster.

```xml
<property>
  <name>mapreduce.outputcommitter.factory.scheme.abfs</name>
  <value>org.apache.hadoop.fs.azurebfs.commit.AzureManifestCommitterFactory</value>
</property>
```

This allows for ADLS Gen2 -specific performance and consistency logic to be used from within the committer.
In particular:
* the [Etag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) header
can be collected in listings and used in the job commit phase.
* IO rename operations are rate limited
* recovery is attempted when throttling triggers rename failures.

*Warning* This committer is not compatible with older Azure storage services
(WASB or ADLS Gen 1).

The core set of Azure-optimized options becomes

```xml
<property>
  <name>mapreduce.outputcommitter.factory.scheme.abfs</name>
  <value>org.apache.hadoop.fs.azurebfs.commit.AzureManifestCommitterFactory</value>
</property>

<property>
  <name>fs.azure.io.rate.limit</name>
  <value>1000</value>
</property>

<property>
  <name>mapreduce.manifest.committer.cleanup.parallel.delete.base.first</name>
  <value>true</value>
</property>

```

And optional settings for debugging/performance analysis

```xml
<property>
  <name>mapreduce.manifest.committer.summary.report.directory</name>
  <value>Path within same store/separate store</value>
  <description>Optional: path to where job summaries are saved</description>
</property>
```

## <a name="abfs-options"></a> Full set of ABFS options for spark

```
spark.hadoop.mapreduce.outputcommitter.factory.scheme.abfs org.apache.hadoop.fs.azurebfs.commit.AzureManifestCommitterFactory
spark.hadoop.fs.azure.io.rate.limit 1000
spark.hadoop.mapreduce.manifest.committer.cleanup.parallel.delete.base.first true
spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol

spark.hadoop.mapreduce.manifest.committer.summary.report.directory  (optional: URI of a directory for job summaries)
```

## <a name="abfs-rate-limit"></a> ABFS Rename Rate Limiting `fs.azure.io.rate.limit`

To avoid triggering store throttling and backoff delays, as well as other
throttling-related failure conditions file renames during job commit
are throttled through a "rate limiter" which limits the number of
rename operations per second a single instance of the ABFS FileSystem client
may issue.

| Option | Meaning |
|--------|---------|
| `fs.azure.io.rate.limit` | Rate limit in operations/second for IO operations. |

Set the option to `0` remove all rate limiting.

The default value of this is set to 1000.

```xml
<property>
  <name>fs.azure.io.rate.limit</name>
  <value>1000</value>
  <description>maximum number of renames attempted per second</description>
</property>
```

This capacity is set at the level of the filesystem client, and so not
shared across all processes within a single application, let
alone other applications sharing the same storage account.

It will be shared with all jobs being committed by the same
Spark driver, as these do share that filesystem connector.

If rate limiting is imposed, the statistic `store_io_rate_limited` will
report the time to acquire permits for committing files.

If server-side throttling took place, signs of this can be seen in
* The store service's logs and their throttling status codes (usually 503 or 500).
* The job statistic `commit_file_rename_recovered`. This statistic indicates that
  ADLS throttling manifested as failures in renames, failures which were recovered
  from in the committer.

If these are seen -or other applications running at the same time experience
throttling/throttling-triggered problems, consider reducing the value of
`fs.azure.io.rate.limit`, and/or requesting a higher IO capacity from Microsoft.

*Important* if you do get extra capacity from Microsoft and you want to use
it to speed up job commits, increase the value of `fs.azure.io.rate.limit`
either across the cluster, or specifically for those jobs which you wish
to allocate extra priority to.

This is still a work in progress; it may be expanded to support
all IO operations performed by a single filesystem instance.

# <a name="gcs"></a> Working with Google Cloud Storage

The manifest committer is compatible with and tested against Google cloud storage through
the gcs-connector library from google, which provides a Hadoop filesystem client for the
schema `gs`.

Google cloud storage has the semantics needed for the commit protocol
to work safely.

The Spark settings to switch to this committer are

```
spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory
spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
spark.hadoop.mapreduce.manifest.committer.cleanup.parallel.delete.base.first false
spark.hadoop.mapreduce.manifest.committer.summary.report.directory  (optional: URI of a directory for job summaries)
```

The store's directory delete operations are `O(files)` so the value
of `mapreduce.manifest.committer.cleanup.parallel.delete`
SHOULD be left at the default of `true`, but
`mapreduce.manifest.committer.cleanup.parallel.delete.base.first` changed to `false`

For mapreduce, declare the binding in `core-site.xml`or `mapred-site.xml`
```xml
<property>
  <name>mapreduce.outputcommitter.factory.scheme.gcs</name>
  <value>org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory</value>
</property>
```


# <a name="hdfs"></a> Working with HDFS

This committer _does_ work with HDFS, it has just been targeted at object stores with
reduced performance on some operations, especially listing and renaming,
and semantics too reduced for the classic `FileOutputCommitter` to rely on
(specifically GCS).

To use on HDFS, set the `ManifestCommitterFactory` as the committer factory for `hdfs://` URLs.

Because HDFS does fast directory deletion, there is no need to parallelize deletion
of task attempt directories during cleanup, so set
`mapreduce.manifest.committer.cleanup.parallel.delete` to `false`

The final spark bindings becomes

```
spark.hadoop.mapreduce.outputcommitter.factory.scheme.hdfs org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory
spark.hadoop.mapreduce.manifest.committer.cleanup.parallel.delete false
spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol

spark.hadoop.mapreduce.manifest.committer.summary.report.directory  (optional: URI of a directory for job summaries)
```

# <a name="advanced"></a> Advanced Configuration options

There are some advanced options which are intended for development and testing,
rather than production use.

| Option                                                    | Meaning                                                     | Default Value |
|-----------------------------------------------------------|-------------------------------------------------------------|---------------|
| `mapreduce.manifest.committer.manifest.save.attempts`     | How many attempts should be made to commit a task manifest? | `5`           |
| `mapreduce.manifest.committer.store.operations.classname` | Classname for Manifest Store Operations                     | `""`          |
| `mapreduce.manifest.committer.validate.output`            | Perform output validation?                                  | `false`       |
| `mapreduce.manifest.committer.writer.queue.capacity`      | Queue capacity for writing intermediate file                | `32`          |

### `mapreduce.manifest.committer.manifest.save.attempts`

The number of attempts which should be made to save a task attempt manifest, which is done by
1. Writing the file to a temporary file in the job attempt directory.
2. Deleting any existing task manifest
3. Renaming the temporary file to the final filename.

This may fail for unrecoverable reasons (permissions, permanent loss of network, service down,...) or it may be
a transient problem which may not reoccur if another attempt is made to write the data.

The number of attempts to make is set by `mapreduce.manifest.committer.manifest.save.attempts`;
the sleep time increases with each attempt.

Consider increasing the default value if task attempts fail to commit their work
and fail to recover from network problems.

### Validating output  `mapreduce.manifest.committer.validate.output`

The option `mapreduce.manifest.committer.validate.output` triggers a check of every renamed file to
verify it has the expected length.

This adds the overhead of a `HEAD` request per file, and so is recommended for testing only.

There is no verification of the actual contents.

### Controlling storage integration `mapreduce.manifest.committer.store.operations.classname`

The manifest committer interacts with filesystems through implementations of the interface
`ManifestStoreOperations`.
It is possible to provide custom implementations for store-specific features.
There is one of these for ABFS; when the abfs-specific committer factory is used this
is automatically set.

It can be explicitly set.
```xml
<property>
  <name>mapreduce.manifest.committer.store.operations.classname</name>
  <value>org.apache.hadoop.fs.azurebfs.commit.AbfsManifestStoreOperations</value>
</property>
```

The default implementation may also be configured.

```xml
<property>
  <name>mapreduce.manifest.committer.store.operations.classname</name>
  <value>org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperationsThroughFileSystem</value>
</property>
```

There is no need to alter these values, except when writing new implementations for other stores,
something which is only needed if the store provides extra integration support for the
committer.

### `mapreduce.manifest.committer.writer.queue.capacity`

This is a secondary scale option.
It controls the size of the queue for storing lists of files to rename from
the manifests loaded from the target filesystem, manifests loaded
from a pool of worker threads, and the single thread which saves
the entries from each manifest to an intermediate file in the local filesystem.

Once the queue is full, all manifest loading threads will block.

```xml
<property>
  <name>mapreduce.manifest.committer.writer.queue.capacity</name>
  <value>32</value>
</property>
```

As the local filesystem is usually much faster to write to than any cloud store,
this queue size should not be a limit on manifest load performance.

It can help limit the amount of memory consumed during manifest load during
job commit.
The maximum number of loaded manifests will be:

```
mapreduce.manifest.committer.writer.queue.capacity + mapreduce.manifest.committer.io.threads
```

## <a name="concurrent"></a> Support for concurrent jobs to the same directory

It *may* be possible to run multiple jobs targeting the same directory tree.

For this to work, a number of conditions must be met:

* When using spark, unique job IDs must be set. This meangs the Spark distribution
  MUST contain the patches for
  [SPARK-33402](https://issues.apache.org/jira/browse/SPARK-33402)
  and
  [SPARK-33230](https://issues.apache.org/jira/browse/SPARK-33230).
* Cleanup of the `_temporary` directory must be disabled by setting
  `mapreduce.fileoutputcommitter.cleanup.skipped` to `true`.
* All jobs/tasks must create files with unique filenames.
* All jobs must create output with the same directory partition structure.
* The job/queries MUST NOT be using Spark Dynamic Partitioning "INSERT OVERWRITE TABLE"; data may be lost.
  This holds for *all* committers, not just the manifest committer.
* Remember to delete the `_temporary` directory later!

This has *NOT BEEN TESTED*
