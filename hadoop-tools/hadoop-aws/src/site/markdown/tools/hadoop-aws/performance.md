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
| consistency | consistent data and listings | consistent since November 2020|
| bandwidth | best: local IO, worst: datacenter network | bandwidth between servers and S3 |
| latency | low | high, especially for "low cost" directory operations |
| rename | fast, atomic | slow faked rename through COPY and DELETE|
| delete | fast, atomic | fast for a file, slow and non-atomic for directories |
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

## <a name="vectoredIO"></a> Improving read performance using Vectored IO
The S3A FileSystem supports implementation of vectored read api using which
a client can provide a list of file ranges to read returning a future read
object associated with each range. For full api specification please see
[FSDataInputStream](../../../../../../hadoop-common-project/hadoop-common/target/site/filesystem/fsdatainputstream.html).

The following properties can be configured to optimise vectored reads based
on the client requirements.

```xml
<property>
  <name>fs.s3a.vectored.read.min.seek.size</name>
  <value>128K</value>
  <description>
     What is the smallest reasonable seek in bytes such
     that we group ranges together during vectored
     read operation.
   </description>
</property>
<property>
   <name>fs.s3a.vectored.read.max.merged.size</name>
   <value>2M</value>
   <description>
      What is the largest merged read size in bytes such
      that we group ranges together during vectored read.
      Setting this value to 0 will disable merging of ranges.
   </description>
<property>
   <name>fs.s3a.vectored.active.ranged.reads</name>
   <value>4</value>
   <description>
      Maximum number of range reads a single input stream can have
      active (downloading, or queued) to the central FileSystem
      instance's pool of queued operations.
      This stops a single stream overloading the shared thread pool.
   </description>
</property>
```

## <a name="bulkdelete"></a> Improving delete performance through bulkdelete API.

For bulk delete API spec refer to File System specification. [BulkDelete](../../../../../../hadoop-common-project/hadoop-common/target/site/filesystem/bulkdelete.html)

The S3A client exports this API.

### S3A Implementation of Bulk Delete.
If multi-object delete is enabled (`fs.s3a.multiobjectdelete.enable` = true), as
it is by default, then the page size is limited to that defined in
`fs.s3a.bulk.delete.page.size`, which MUST be less than or equal to 1000.
* The entire list of paths to delete is aggregated into a single bulk delete request,
  issued to the store.
* Provided the caller has the correct permissions, every entry in the list
  will, if the path references an object, cause that object to be deleted.
* If the path does not reference an object: the path will not be deleted
  "This is for deleting objects, not directories"
* No probes for the existence of parent directories will take place; no
  parent directory markers will be created.
  "If you need parent directories, call mkdir() yourself"
* The list of failed keys listed in the `DeleteObjectsResponse` response
  are converted into paths and returned along with their error messages.
* Network and other IO errors are raised as exceptions.

If multi-object delete is disabled (or the list of size 1)
* A single `DELETE` call is issued
* Any `AccessDeniedException` raised is converted to a result in the error list.
* Any 404 response from a (non-AWS) store will be ignored.
* Network and other IO errors are raised as exceptions.

Because there are no probes to ensure the call does not overwrite a directory,
or to see if a parentDirectory marker needs to be created,
this API is still faster than issuing a normal `FileSystem.delete(path)` call.

That is: all the overhead normally undertaken to preserve the Posix System model are omitted.


### S3 Scalability and Performance

Every entry in a bulk delete request counts as one write operation
against AWS S3 storage.
With the default write rate under a prefix on AWS S3 Standard storage
restricted to 3,500 writes/second, it is very easy to overload
the store by issuing a few bulk delete requests simultaneously.

* If throttling is triggered then all clients interacting with
  the store may observe performance issues.
* The write quota applies even for paths which do not exist.
* The S3A client *may* perform rate throttling as well as page size limiting.

What does that mean? it means that attempting to issue multiple
bulk delete calls in parallel can be counterproductive.

When overloaded, the S3 store returns a 403 throttle response.
This will trigger it back off and retry of posting the request.
However, the repeated request will still include the same number of objects and
*so generate the same load*.

This can lead to a pathological situation where the repeated requests will
never be satisfied because the request itself is sufficient to overload the store.
See [HADOOP-16823.Large DeleteObject requests are their own Thundering Herd]
(https://issues.apache.org/jira/browse/HADOOP-16823)
for an example of where this did actually surface in production.

This is why the default page size of S3A clients is 250 paths, not the store limit of 1000 entries.
It is also why the S3A delete/rename operations do not attempt to do massive parallel deletions,
Instead bulk delete requests are queued for a single blocking thread to issue.
Consider a similar design.


When working with versioned S3 buckets, every path deleted will add a tombstone marker
to the store at that location, even if there was no object at that path.
While this has no negative performance impact on the bulk delete call,
it will slow down list requests subsequently made against that path.
That is: bulk delete requests of paths which do not exist will hurt future queries.

Avoid this. Note also that TPC-DS Benchmark do not create the right load to make the
performance problems observable -but they can surface in production.
* Configure buckets to have a limited number of days for tombstones to be preserved.
* Do not delete paths which you know reference nonexistent files or directories.

## <a name="fadvise"></a> Improving data input performance through fadvise

The S3A Filesystem client supports the notion of input policies, similar
to that of the Posix `fadvise()` API call. This tunes the behavior of the S3A
client to optimise HTTP GET requests for the different use cases.

The list of supported options is found in
[FSDataInputStream](../../../../../../hadoop-common-project/hadoop-common/target/site/filesystem/fsdatainputstreambuilder.html).


### fadvise `sequential`, `whole-file`

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

Distcp will automatically request `whole-file` access, even on deployments
where the cluster configuration is for `random` IO.

### fadvise `random`

Optimised for random IO, specifically the Hadoop `PositionedReadable`
operations —though `seek(offset); read(byte_buffer)` also benefits.

Rather than ask for the whole file, the range of the HTTP request is
set to the length of data desired in the `read` operation
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

### fadvise `normal` or `adaptive` (default)

The `normal` policy starts off reading a file  in `sequential` mode,
but if the caller seeks backwards in the stream, it switches from
sequential to `random`.

This policy essentially recognizes the initial read pattern of columnar
storage formats (e.g. Apache ORC and Apache Parquet), which seek to the end
of a file, read in index data and then seek backwards to selectively read
columns. The first seeks may be expensive compared to the random policy,
however the overall process is much less expensive than either sequentially
reading through a file with the `random` policy, or reading columnar data
with the `sequential` policy.


## <a name="commit"></a> Committing Work in MapReduce and Spark

Hadoop MapReduce, Apache Hive and Apache Spark all write their work
to HDFS and similar filesystems.
When using S3 as a destination, this is slow because of the way `rename()`
is mimicked with copy and delete.

If committing output takes a long time, it is because you are using the standard
`FileOutputCommitter`.

*Your problem may appear to be performance, but that is a symptom
of the underlying problem: the way S3A fakes rename operations means that
the rename cannot be safely be used in output-commit algorithms.*

Fix: Use one of the dedicated [S3A Committers](committers.md).

## <a name="tuning"></a> Options to Tune

### <a name="flags"></a> Performance Flags: `fs.s3a.performance.flags`

This option takes a comma separated list of performance flags.
View it as the equivalent of the `-O` compiler optimization list C/C++ compilers offer.
That is a complicated list of options which deliver speed if the person setting them
understands the risks.

* The list of flags MAY change across releases
* The semantics of specific flags SHOULD NOT change across releases.
* If an option is to be tuned which may relax semantics, a new option MUST be defined.
* Unknown flags are ignored; this is to avoid compatibility.
* The option `*` means "turn everything on". This is implicitly unstable across releases.

| *Option* | *Meaning*          | Since |
|----------|--------------------|:------|
| `create` | Create Performance | 3.4.1 |
| `mkdir`  | Mkdir Performance  | 3.4.1 |


* The `create` flag has the same semantics as [`fs.s3a.create.performance`](#create-performance)
* The `mkdir` flag semantics are explained in [Mkdir Performance](#mkdir-performance)


### <a name="create-performance"></a> Create Performance `fs.s3a.create.performance`


The configuration option `fs.s3a.create.performance` has the same behavior as
the `fs.s3a.performance.flag` flag option `create`:

* No overwrite checks are made when creating a file, even if overwrite is set to `false` in the application/library code.
  Instead S3 conditional creation will be used to perform an atomic overwrite check _when the file write completes_.
* No checks are made for an object being written above a path containing other objects (i.e. a "directory")
* No checks are made for a parent path containing an object which is not a directory marker (i.e. a "file")

This saves multiple probes per operation, especially a `LIST` call.

It may however result in creation of directory structures which can no longer be navigated through filesystem APIs.

Use with care, and, ideally, enable versioning on the S3 store.

Note that S3 Conditional creation may not be supported on third party stores,
in which case no overwrite checks are performed at all.


### <a name="mkdir-performance"></a> Mkdir Performance

`fs.s3a.performance.flag` flag option `mkdir`:

* Mkdir does not check whether the parent is directory or file.

This avoids the verification of the file status of the parent file
or the closest ancestor. Unlike the default mkdir operation, if the
parent is not a directory, the mkdir operation does not throw any
error.

This option can help with mkdir performance improvement but must be used
only if the person setting them understands the above-mentioned risk.


### <a name="threads"></a> Thread and connection pool settings.

Each S3A client interacting with a single bucket, as a single user, has its
own dedicated pool of open HTTP connections alongside a pool of threads used
for background/parallel operations in addition to the worker threads of the
actual application.

The default pool sizes are intended to strike a balance between performance
and memory/thread use.

You can have a larger pool of (reused) HTTP connections and threads
for parallel IO (especially uploads, prefetching and vector reads) by setting the appropriate
properties. Note: S3A Connectors have their own thread pools for job commit, but
everything uses the same HTTP connection pool.

| Property                       | Default | Meaning                                                          |
|--------------------------------|---------|------------------------------------------------------------------|
| `fs.s3a.threads.max`           | `96`    | Threads in the thread pool                                       |
| `fs.s3a.threads.keepalivetime` | `60s`   | Expiry time for idle threads in the thread pool                  |
| `fs.s3a.executor.capacity`     | `16`    | Maximum threads for any single operation                         |
| `fs.s3a.max.total.tasks`       | `16`    | Extra tasks which can be queued excluding prefetching operations |

### <a name="timeouts"></a> Timeouts.

Network timeout options can be tuned to make the client fail faster *or* retry more.
The choice is yours. Generally recovery is better, but sometimes fail-fast is more useful.


| Property                                | Default | V2  | Meaning                                               |
|-----------------------------------------|---------|:----|-------------------------------------------------------|
| `fs.s3a.connection.maximum`             | `500`   |     | Connection pool size                                  |
| `fs.s3a.connection.keepalive`           | `false` | `*` | Use TCP keepalive on open channels                    |
| `fs.s3a.connection.acquisition.timeout` | `60s`   | `*` | Timeout for waiting for a connection from the pool.   |
| `fs.s3a.connection.establish.timeout`   | `30s`   |     | Time to establish the TCP/TLS connection              |
| `fs.s3a.connection.idle.time`           | `60s`   | `*` | Maximum time for idle HTTP connections in the pool    |
| `fs.s3a.connection.request.timeout`     | `60s`   |     | If greater than zero, maximum time for a response     |
| `fs.s3a.connection.timeout`             | `200s`  |     | Timeout for socket problems on a TCP channel          |
| `fs.s3a.connection.ttl`                 | `5m`    |     | Lifetime of HTTP connections from the pool            |


Units:
1. The default unit for all these options except for `fs.s3a.threads.keepalivetime` is milliseconds, unless a time suffix is declared.
2. Versions of Hadoop built with the AWS V1 SDK *only* support milliseconds rather than suffix values.
   If configurations are intended to apply across hadoop releases, you MUST use milliseconds without a suffix.
3. `fs.s3a.threads.keepalivetime` has a default unit of seconds on all hadoop releases.
4. Options flagged as "V2" are new with the AWS V2 SDK; they are ignored on V1 releases.

---

There are some hard tuning decisions related to pool size and expiry.
As servers add more cores and services add many more worker threads, a larger pool size is more and more important:
the default values in `core-default.xml` have been slowly increased over time but should be treated as
"the best", simply what is considered a good starting case.
With Vectored IO adding multiple GET requests per Spark/Hive worker thread,
and stream prefetching performing background block prefetch, larger pool and thread sizes are even more important.

In large hive deployments, thread and connection pools of thousands have been known to have been set.

Small pool: small value in `fs.s3a.connection.maximum`.
* Keeps network/memory cost of having many S3A instances in the same process low.
* But: limit on how many connections can be open at at a time.

* Large Pool. More HTTP connections can be created and kept, but cost of keeping network connections increases
unless idle time is reduced through `fs.s3a.connection.idle.time`.

If exceptions are raised with about timeouts acquiring connections from the pool, this can be a symptom of
* Heavy load. Increase pool size and acquisition timeout `fs.s3a.connection.acquisition.timeout`
* Process failing to close open input streams from the S3 store.
  Fix: Find uses of `open()`/`openFile()` and make sure that the streams are being `close()d`

*Retirement of HTTP Connections.*

Connections are retired from the pool by `fs.s3a.connection.idle.time`, the maximum time for idle connections,
and `fs.s3a.connection.ttl`, the maximum life of any connection in the pool, even if it repeatedly reused.

Limiting idle time saves on network connections, at the cost of requiring new connections on subsequent S3 operations.

Limiting connection TTL is useful to spread across load balancers and recover from some network
connection problems, including those caused by proxies.

*Request timeout*: `fs.s3a.connection.request.timeout`

If set, this sets an upper limit on any non-streaming API call (i.e. everything but `GET`).

A timeout is good to detect and recover from failures.
However, it also sets a limit on the duration of a POST/PUT of data
-which, if after a timeout, will only be repeated, ultimately to failure.


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

1. Perform listings in parallel by setting `-numListstatusThreads`
to a higher number. Make sure that `fs.s3a.connection.maximum`
is equal to or greater than the value used.

1. If using `-delete`, set `fs.trash.interval` to 0 to avoid the deleted
objects from being copied to a trash directory.

1. If using distcp to upload to a new path where no existing data exists,
   consider adding the option `create` to the flags in `fs.s3a.performance.flag`.

*DO NOT* switch `fs.s3a.fast.upload.buffer` to buffer in memory.
If one distcp mapper runs out of memory it will fail,
and that runs the risk of failing the entire job.
It is safer to keep the default value, `disk`.

What is potentially useful is uploading in bigger blocks; this is more
efficient in terms of HTTP connection use, and reduce the IOP rate against
the S3 bucket/shard.

```xml
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

<!-- maybe -->
<property>
  <name>fs.s3a.create.performance</name>
  <value>create</value>
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
IP address to return to the client is made based on the current load
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

        curl -O https://noaa-cors-pds.s3.amazonaws.com/raw/2023/001/akse/AKSE001a.23_.gz

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

Reading or writing data encrypted with SSE-KMS or DSSE-KMS forces S3 to make calls of
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

## <a name="ssl"></a> Tuning SSL Performance

By default, S3A uses HTTPS to communicate with AWS Services. This means that all
communication with S3 is encrypted using SSL. The overhead of this encryption
can significantly slow down applications. The configuration option
`fs.s3a.ssl.channel.mode` allows applications to trigger certain SSL
optimizations.

By default, `fs.s3a.ssl.channel.mode` is set to `default_jsse`, which uses
the Java Secure Socket Extension implementation of SSL (this is the default
implementation when running Java). However, there is one difference, the GCM
cipher is removed from the list of enabled cipher suites when running on Java 8.
The GCM cipher has known performance issues when running on Java 8, see
HADOOP-15669 and HADOOP-16050 for details. It is important to note that the
GCM cipher is only disabled on Java 8. GCM performance has been improved
in Java 9, so if `default_jsse` is specified and applications run on Java
9, they should see no difference compared to running with the vanilla JSSE.

`fs.s3a.ssl.channel.mode` can be set to `default_jsse_with_gcm`. This option
includes GCM in the list of cipher suites on Java 8, so it is equivalent to
running with the vanilla JSSE.

### <a name="openssl"></a> OpenSSL Acceleration

As of HADOOP-16050 and HADOOP-16346, `fs.s3a.ssl.channel.mode` can be set to
either `default` or `openssl` to enable native OpenSSL acceleration of HTTPS
requests. OpenSSL implements the SSL and TLS protocols using native code. For
users reading a large amount of data over HTTPS, OpenSSL can provide a
significant performance benefit over the JSSE.

S3A uses the
[WildFly OpenSSL](https://github.com/wildfly-security/wildfly-openssl) library
to bind OpenSSL to the Java JSSE APIs. This library allows S3A to
transparently read data using OpenSSL. The `wildfly-openssl` library is an
optional runtime dependency of S3A and contains native libraries for binding the Java
JSSE to OpenSSL.

WildFly OpenSSL must load OpenSSL itself. This can be done using the system
property `org.wildfly.openssl.path`. For example,
`HADOOP_OPTS="-Dorg.wildfly.openssl.path=<path to OpenSSL libraries>
${HADOOP_OPTS}"`. See WildFly OpenSSL documentation for more details.

When `fs.s3a.ssl.channel.mode` is set to `default`, S3A will attempt to load
the OpenSSL libraries using the WildFly library. If it is unsuccessful, it
will fall back to the `default_jsse` behavior.

When `fs.s3a.ssl.channel.mode` is set to `openssl`, S3A will attempt to load
the OpenSSL libraries using WildFly. If it is unsuccessful, it will throw an
exception and S3A initialization will fail.

### `fs.s3a.ssl.channel.mode` Configuration

`fs.s3a.ssl.channel.mode` can be configured as follows:

```xml
<property>
  <name>fs.s3a.ssl.channel.mode</name>
  <value>default_jsse</value>
  <description>
    If secure connections to S3 are enabled, configures the SSL
    implementation used to encrypt connections to S3. Supported values are:
    "default_jsse", "default_jsse_with_gcm", "default", and "openssl".
    "default_jsse" uses the Java Secure Socket Extension package (JSSE).
    However, when running on Java 8, the GCM cipher is removed from the list
    of enabled ciphers. This is due to performance issues with GCM in Java 8.
    "default_jsse_with_gcm" uses the JSSE with the default list of cipher
    suites. "default_jsse_with_gcm" is equivalent to the behavior prior to
    this feature being introduced. "default" attempts to use OpenSSL rather
    than the JSSE for SSL encryption, if OpenSSL libraries cannot be loaded,
    it falls back to the "default_jsse" behavior. "openssl" attempts to use
    OpenSSL as well, but fails if OpenSSL libraries cannot be loaded.
  </description>
</property>
```

Supported values for `fs.s3a.ssl.channel.mode`:

| `fs.s3a.ssl.channel.mode` Value | Description                                                            |
|---------------------------------|------------------------------------------------------------------------|
| `default_jsse`                  | Uses Java JSSE without GCM on Java 8                                   |
| `default_jsse_with_gcm`         | Uses Java JSSE                                                         |
| `default`                       | Uses OpenSSL, falls back to `default_jsse` if OpenSSL cannot be loaded |
| `openssl`                       | Uses OpenSSL, fails if OpenSSL cannot be loaded                        |

The naming convention is setup in order to preserve backwards compatibility
with the ABFS support of [HADOOP-15669](https://issues.apache.org/jira/browse/HADOOP-15669).

Other options may be added to `fs.s3a.ssl.channel.mode` in the future as
further SSL optimizations are made.

### WildFly classpath and SSL library requirements

For OpenSSL acceleration to work, a compatible version of the
wildfly JAR must be on the classpath. This is not explicitly declared
in the dependencies of the published `hadoop-aws` module, as it is
optional.

If the wildfly JAR is not found, the network acceleration will fall back
to the JVM, always.

Similarly, the `libssl` library must be compatibile with wildfly.

Wildfly requires this native library to be part of an `openssl` installation.
Third party implementations may not work correctly.
This can be an isse in FIPS-compliant deployments, where the `libssl` library
is a third-party implementation built with restricted TLS protocols.


There have been compatibility problems with wildfly JARs and openSSL
releases in the past: version 1.0.4.Final is not compatible with openssl 1.1.1.
An extra complication was older versions of the `azure-data-lake-store-sdk`
JAR used in `hadoop-azure-datalake` contained an unshaded copy of the 1.0.4.Final
classes, causing binding problems even when a later version was explicitly
being placed on the classpath.

## <a name="initilization"></a> Tuning FileSystem Initialization.

### Bucket existence checks

When an S3A Filesystem instance is created and initialized, the client
can be checks if the bucket provided is valid. This can be slow, which is why
it is disabled by default.

```xml
<property>
  <name>fs.s3a.bucket.probe</name>
  <value>0</value>
</property>
```

If the bucket does not exist, this issue will surface when operations are performed
on the filesystem; you will see `UnknownStoreException` stack traces.

Re-enabling the probe will force an early check but but is generally not needed.

### Rate limiting parallel FileSystem creation operations

Applications normally ask for filesystems from the shared cache,
via `FileSystem.get()` or `Path.getFileSystem()`.
The cache, `FileSystem.CACHE` will, for each user, cachec one instance of a filesystem
for a given URI.
All calls to `FileSystem.get` for a cached FS for a URI such
as `s3a://noaa-isd-pds/` will return that singe single instance.

FileSystem instances are created on-demand for the cache,
and will be done in each thread which requests an instance.
This is done outside of any synchronisation block.
Once a task has an initialized FileSystem instance, it will, in a synchronized block
add it to the cache.
If it turns out that the cache now already has an instance for that URI, it will
revert the cached copy to it, and close the FS instance it has just created.

If a FileSystem takes time to be initialized, and many threads are trying to
retrieve a FileSystem instance for the same S3 bucket in parallel,
All but one of the threads will be doing useless work, and may unintentionally
be creating lock contention on shared objects.

There is an option, `fs.creation.parallel.count`, which uses a semaphore
to limit the number of FS instances which may be created in parallel.

Setting this to a low number will reduce the amount of wasted work,
at the expense of limiting the number of FileSystem clients which
can be created simultaneously for different object stores/distributed
filesystems.

For example, a value of four would put an upper limit on the number
of wasted instantiations of a connector for the `s3a://noaa-isd-pds/`
bucket.

```xml
<property>
  <name>fs.creation.parallel.count</name>
  <value>4</value>
</property>
```

It would also mean that if four threads were in the process
of creating such connectors, all threads trying to create
connectors for other buckets, would end up blocking too.

Consider experimenting with this when running applications
where many threads may try to simultaneously interact
with the same slow-to-initialize object stores.
