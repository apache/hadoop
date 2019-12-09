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

# Maximizing Performance when working with the S3A Connector

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->


## <a name="introduction"></a> Introduction

S3 is slower to work with than HDFS, even on virtual clusters running on
Amazon EC2.

That's because its a very different system, as you can see:


| Feature | HDFS | S3 through the S3A connector |
|---------|------|------------------------------|
| communication | RPC | HTTP GET/PUT/HEAD/LIST/COPY requests |
| data locality | local storage | remote S3 servers |
| replication | multiple datanodes | asynchronous after upload |
| consistency | consistent data and listings | eventual consistent for listings, deletes and updates |
| bandwidth | best: local IO, worst: datacenter network | bandwidth between servers and S3 |
| latency | low | high, especially for "low cost" directory operations |
| rename | fast, atomic | slow faked rename through COPY & DELETE|
| delete | fast, atomic | fast for a file, slow & non-atomic for directories |
| writing| incremental | in blocks; not visible until the writer is closed |
| reading | seek() is fast | seek() is slow and expensive |
| IOPs | limited only by hardware | callers are throttled to shards in an s3 bucket |
| Security | Posix user+group; ACLs | AWS Roles and policies |

From a performance perspective, key points to remember are:

* S3 throttles bucket access across all callers: adding workers can make things worse.
* EC2 VMs have network IO throttled based on the VM type.
* Directory rename and copy operations take *much* longer the more objects and data there is.
The slow performance of `rename()` surfaces during the commit phase of jobs,
applications like `DistCP`, and elsewhere.
* seek() calls when reading a file can force new HTTP requests.
This can make reading columnar Parquet/ORC data expensive.

Overall, although the S3A connector makes S3 look like a file system,
it isn't, and some attempts to preserve the metaphor are "aggressively suboptimal".

To make most efficient use of S3, care is needed.

## <a name="s3guard"></a> Speeding up directory listing operations through S3Guard

[S3Guard](s3guard.html) provides significant speedups for operations which
list files a lot. This includes the setup of all queries against data:
MapReduce, Hive and Spark, as well as DistCP.


Experiment with using it to see what speedup it delivers.


## <a name="fadvise"></a> Improving data input performance through fadvise

The S3A Filesystem client supports the notion of input policies, similar
to that of the Posix `fadvise()` API call. This tunes the behavior of the S3A
client to optimise HTTP GET requests for the different use cases.

### fadvise `sequential`

Read through the file, possibly with some short forward seeks.

The whole document is requested in a single HTTP request; forward seeks
within the readahead range are supported by skipping over the intermediate
data.

This delivers maximum sequential throughput —but with very expensive
backward seeks.

Applications reading a file in bulk (DistCP, any copy operations) should use
sequential access, as should those reading data from gzipped `.gz` files.
Because the "normal" fadvise policy starts off in sequential IO mode,
there is rarely any need to explicit request this policy.

### fadvise `random`

Optimised for random IO, specifically the Hadoop `PositionedReadable`
operations —though `seek(offset); read(byte_buffer)` also benefits.

Rather than ask for the whole file, the range of the HTTP request is
set to that that of the length of data desired in the `read` operation
(Rounded up to the readahead value set in `setReadahead()` if necessary).

By reducing the cost of closing existing HTTP requests, this is
highly efficient for file IO accessing a binary file
through a series of `PositionedReadable.read()` and `PositionedReadable.readFully()`
calls. Sequential reading of a file is expensive, as now many HTTP requests must
be made to read through the file: there's a delay between each GET operation.


Random IO is best for IO with seek-heavy characteristics:

* Data is read using the `PositionedReadable` API.
* Long distance (many MB) forward seeks
* Backward seeks as likely as forward seeks.
* Little or no use of single character `read()` calls or small `read(buffer)`
calls.
* Applications running close to the S3 data store. That is: in EC2 VMs in
the same datacenter as the S3 instance.

The desired fadvise policy must be set in the configuration option
`fs.s3a.experimental.input.fadvise` when the filesystem instance is created.
That is: it can only be set on a per-filesystem basis, not on a per-file-read
basis.

```xml
<property>
  <name>fs.s3a.experimental.input.fadvise</name>
  <value>random</value>
  <description>
  Policy for reading files.
  Values: 'random', 'sequential' or 'normal'
   </description>
</property>
```

[HDFS-2744](https://issues.apache.org/jira/browse/HDFS-2744),
*Extend FSDataInputStream to allow fadvise* proposes adding a public API
to set fadvise policies on input streams. Once implemented,
this will become the supported mechanism used for configuring the input IO policy.

### fadvise `normal` (default)

The `normal` policy starts off reading a file  in `sequential` mode,
but if the caller seeks backwards in the stream, it switches from
sequential to `random`.

This policy essentially recognizes the initial read pattern of columnar
storage formats (e.g. Apache ORC and Apache Parquet), which seek to the end
of a file, read in index data and then seek backwards to selectively read
columns. The first seeks may be be expensive compared to the random policy,
however the overall process is much less expensive than either sequentially
reading through a file with the `random` policy, or reading columnar data
with the `sequential` policy.


## <a name="commit"></a> Committing Work in MapReduce and Spark

Hadoop MapReduce, Apache Hive and Apache Spark all write their work
to HDFS and similar filesystems.
When using S3 as a destination, this is slow because of the way `rename()`
is mimicked with copy and delete.

If committing output takes a long time, it is because you are using the standard
`FileOutputCommitter`. If you are doing this on any S3 endpoint which lacks
list consistency (Amazon S3 without [S3Guard](s3guard.html)), this committer
is at risk of losing data!

*Your problem may appear to be performance, but that is a symptom
of the underlying problem: the way S3A fakes rename operations means that
the rename cannot be safely be used in output-commit algorithms.*

Fix: Use one of the dedicated [S3A Committers](committers.md).

## <a name="tuning"></a> Options to Tune

### <a name="pooling"></a> Thread and connection pool sizes.

Each S3A client interacting with a single bucket, as a single user, has its
own dedicated pool of open HTTP 1.1 connections alongside a pool of threads used
for upload and copy operations.
The default pool sizes are intended to strike a balance between performance
and memory/thread use.

You can have a larger pool of (reused) HTTP connections and threads
for parallel IO (especially uploads) by setting the properties


| property | meaning | default |
|----------|---------|---------|
| `fs.s3a.threads.max`| Threads in the AWS transfer manager| 10 |
| `fs.s3a.connection.maximum`| Maximum number of HTTP connections | 10|

We recommend using larger values for processes which perform
a lot of IO: `DistCp`, Spark Workers and similar.

```xml
<property>
  <name>fs.s3a.threads.max</name>
  <value>20</value>
</property>
<property>
  <name>fs.s3a.connection.maximum</name>
  <value>20</value>
</property>
```

Be aware, however, that processes which perform many parallel queries
may consume large amounts of resources if each query is working with
a different set of s3 buckets, or are acting on behalf of different users.

### For large data uploads, tune the block size: `fs.s3a.block.size`

When uploading data, it is uploaded in blocks set by the option
`fs.s3a.block.size`; default value "32M" for 32 Megabytes.

If a larger value is used, then more data is buffered before the upload
begins:

```xml
<property>
  <name>fs.s3a.block.size</name>
  <value>128M</value>
</property>
```

This means that fewer PUT/POST requests are made of S3 to upload data,
which reduces the likelihood that S3 will throttle the client(s)

### Maybe: Buffer Write Data in Memory

When large files are being uploaded, blocks are saved to disk and then
queued for uploading, with multiple threads uploading different blocks
in parallel.

The blocks can be buffered in memory by setting the option
`fs.s3a.fast.upload.buffer` to `bytebuffer`, or, for on-heap storage
`array`.

1. Switching to in memory-IO reduces disk IO, and can be faster if the bandwidth
to the S3 store is so high that the disk IO becomes the bottleneck.
This can have a tangible benefit when working with on-premise S3-compatible
object stores with very high bandwidth to servers.

It is very easy to run out of memory when buffering to it; the option
`fs.s3a.fast.upload.active.blocks"` exists to tune how many active blocks
a single output stream writing to S3 may have queued at a time.

As the size of each buffered block is determined by the value of `fs.s3a.block.size`,
the larger the block size, the more likely you will run out of memory.

## <a name="distcp"></a> DistCP

DistCP can be slow, especially if the parameters and options for the operation
are not tuned for working with S3.

To exacerbate the issue, DistCP invariably puts heavy load against the
bucket being worked with, which will cause S3 to throttle requests.
It will throttle: directory operations, uploads of new data, and delete operations,
amongst other things

### DistCP: Options to Tune

* `-numListstatusThreads <threads>` : set to something higher than the default (1).
* `-bandwidth <mb>` : use to limit the upload bandwidth per worker
* `-m <maps>` : limit the number of mappers, hence the load on the S3 bucket.

Adding more maps with the `-m` option does not guarantee better performance;
it may just increase the amount of throttling which takes place.
A smaller number of maps with a higher bandwidth per map can be more efficient.

### DistCP: Options to Avoid.

DistCp's `-atomic` option copies up data into a directory, then renames
it into place, which is the where the copy takes place. This is a performance
killer.

* Do not use the `-atomic` option.
* The `-append` operation is not supported on S3; avoid.
* `-p` S3 does not have a POSIX-style permission model; this will fail.


### DistCP: Parameters to Tune

1. As discussed [earlier](#pooling), use large values for
`fs.s3a.threads.max` and `fs.s3a.connection.maximum`.

1. Make sure that the bucket is using `sequential` or `normal` fadvise seek policies,
that is, `fs.s3a.experimental.fadvise` is not set to `random`

1. Perform listings in parallel by setting `-numListstatusThreads`
to a higher number. Make sure that `fs.s3a.connection.maximum`
is equal to or greater than the value used.

1. If using `-delete`, set `fs.trash.interval` to 0 to avoid the deleted
objects from being copied to a trash directory.

*DO NOT* switch `fs.s3a.fast.upload.buffer` to buffer in memory.
If one distcp mapper runs out of memory it will fail,
and that runs the risk of failing the entire job.
It is safer to keep the default value, `disk`.

What is potentially useful is uploading in bigger blocks; this is more
efficient in terms of HTTP connection use, and reduce the IOP rate against
the S3 bucket/shard.

```xml
<property>
  <name>fs.s3a.threads.max</name>
  <value>20</value>
</property>

<property>
  <name>fs.s3a.connection.maximum</name>
  <value>30</value>
  <descriptiom>
   Make greater than both fs.s3a.threads.max and -numListstatusThreads
   </descriptiom>
</property>

<property>
  <name>fs.s3a.experimental.fadvise</name>
  <value>normal</value>
</property>

<property>
  <name>fs.s3a.block.size</name>
  <value>128M</value>
</property>

<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>disk</value>
</property>

<property>
  <name>fs.trash.interval</name>
  <value>0</value>
</property>
```

## <a name="rm"></a> hadoop shell commands `fs -rm`

The `hadoop fs -rm` command can rename the file under `.Trash` rather than
deleting it. Use `-skipTrash` to eliminate that step.


This can be set in the property `fs.trash.interval`; while the default is 0,
most HDFS deployments have it set to a non-zero value to reduce the risk of
data loss.

```xml
<property>
  <name>fs.trash.interval</name>
  <value>0</value>
</property>
```


## <a name="load balancing"></a> Improving S3 load-balancing behavior

Amazon S3 uses a set of front-end servers to provide access to the underlying data.
The choice of which front-end server to use is handled via load-balancing DNS
service: when the IP address of an S3 bucket is looked up, the choice of which
IP address to return to the client is made based on the the current load
of the front-end servers.

Over time, the load across the front-end changes, so those servers considered
"lightly loaded" will change. If the DNS value is cached for any length of time,
your application may end up talking to an overloaded server. Or, in the case
of failures, trying to talk to a server that is no longer there.

And by default, for historical security reasons in the era of applets,
the DNS TTL of a JVM is "infinity".

To work with AWS better, set the DNS time-to-live of an application which
works with S3 to something lower.
See [AWS documentation](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-jvm-ttl.html).

## <a name="network_performance"></a> Troubleshooting network performance

An example of this is covered in [HADOOP-13871](https://issues.apache.org/jira/browse/HADOOP-13871).

1. For public data, use `curl`:

        curl -O https://landsat-pds.s3.amazonaws.com/scene_list.gz
1. Use `nettop` to monitor a processes connections.


## <a name="throttling"></a> Throttling

When many requests are made of a specific S3 bucket (or shard inside it),
S3 will respond with a 503 "throttled" response.
Throttling can be recovered from, provided overall load decreases.
Furthermore, because it is sent before any changes are made to the object store,
is inherently idempotent. For this reason, the client will always attempt to
retry throttled requests.

The limit of the number of times a throttled request can be retried,
and the exponential interval increase between attempts, can be configured
independently of the other retry limits.

```xml
<property>
  <name>fs.s3a.retry.throttle.limit</name>
  <value>20</value>
  <description>
    Number of times to retry any throttled request.
  </description>
</property>

<property>
  <name>fs.s3a.retry.throttle.interval</name>
  <value>500ms</value>
  <description>
    Interval between retry attempts on throttled requests.
  </description>
</property>
```

If a client is failing due to `AWSServiceThrottledException` failures,
increasing the interval and limit *may* address this. However, it
it is a sign of AWS services being overloaded by the sheer number of clients
and rate of requests. Spreading data across different buckets, and/or using
a more balanced directory structure may be beneficial.
Consult [the AWS documentation](http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html).

Reading or writing data encrypted with SSE-KMS forces S3 to make calls of
the AWS KMS Key Management Service, which comes with its own
[Request Rate Limits](http://docs.aws.amazon.com/kms/latest/developerguide/limits.html).
These default to 1200/second for an account, across all keys and all uses of
them, which, for S3 means: across all buckets with data encrypted with SSE-KMS.

### <a name="minimizing_throttling"></a> Tips to Keep Throttling down

If you are seeing a lot of throttling responses on a large scale
operation like a `distcp` copy, *reduce* the number of processes trying
to work with the bucket (for distcp: reduce the number of mappers with the
`-m` option).

If you are reading or writing lists of files, if you can randomize
the list so they are not processed in a simple sorted order, you may
reduce load on a specific shard of S3 data, so potentially increase throughput.

An S3 Bucket is throttled by requests coming from all
simultaneous clients. Different applications and jobs may interfere with
each other: consider that when troubleshooting.
Partitioning data into different buckets may help isolate load here.

If you are using data encrypted with SSE-KMS, then the
will also apply: these are stricter than the S3 numbers.
If you believe that you are reaching these limits, you may be able to
get them increased.
Consult [the KMS Rate Limit documentation](http://docs.aws.amazon.com/kms/latest/developerguide/limits.html).

### <a name="s3guard_throttling"></a> S3Guard and Throttling


S3Guard uses DynamoDB for directory and file lookups;
it is rate limited to the amount of (guaranteed) IO purchased for a
table.

To see the allocated capacity of a bucket, the `hadoop s3guard bucket-info s3a://bucket`
command will print out the allocated capacity.


If significant throttling events/rate is observed here, the pre-allocated
IOPs can be increased with the `hadoop s3guard set-capacity` command, or
through the AWS Console. Throttling events in S3Guard are noted in logs, and
also in the S3A metrics `s3guard_metadatastore_throttle_rate` and
`s3guard_metadatastore_throttled`.

If you are using DistCP for a large backup to/from a S3Guarded bucket, it is
actually possible to increase the capacity for the duration of the operation.


## <a name="coding"></a> Best Practises for Code

Here are some best practises if you are writing applications to work with
S3 or any other object store through the Hadoop APIs.

Use `listFiles(path, recursive)` over `listStatus(path)`.
The recursive `listFiles()` call can enumerate all dependents of a path
in a single LIST call, irrespective of how deep the path is.
In contrast, any directory tree-walk implemented in the client is issuing
multiple HTTP requests to scan each directory, all the way down.

Cache the outcome of `getFileStats()`, rather than repeatedly ask for it.
That includes using `isFile()`, `isDirectory()`, which are simply wrappers
around `getFileStatus()`.

Don't immediately look for a file with a `getFileStatus()` or listing call
after creating it, or try to read it immediately.
This is where eventual consistency problems surface: the data may not yet be visible.

Rely on `FileNotFoundException` being raised if the source of an operation is
missing, rather than implementing your own probe for the file before
conditionally calling the operation.

### `rename()`

Avoid any algorithm which uploads data into a temporary file and then uses
`rename()` to commit it into place with a final path.
On HDFS this offers a fast commit operation.
With S3, Wasb and other object stores, you can write straight to the destination,
knowing that the file isn't visible until you close the write: the write itself
is atomic.

The `rename()` operation may return `false` if the source is missing; this
is a weakness in the API. Consider a check before calling rename, and if/when
a new rename() call is made public, switch to it.


### `delete(path, recursive)`

Keep in mind that `delete(path, recursive)` is a no-op if the path does not exist, so
there's no need to have a check for the path existing before you call it.

`delete()` is often used as a cleanup operation.
With an object store this is slow, and may cause problems if the caller
expects an immediate response. For example, a thread may block so long
that other liveness checks start to fail.
Consider spawning off an executor thread to do these background cleanup operations.
