t<!---
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

# Reading Data From S3 Storage.

One of the most important --and performance sensitive-- parts
of the S3A connector is reading data from storage.
This is always evolving, based on experience, and benchmarking,
and in collaboration with other projects.

## Key concepts

* Data is read from S3 through an instance of an `ObjectInputStream`.
* There are different implementations of this in the codebase:
  `classic`, `analytics` and `prefetch`; these are called _stream types_
* The choice of which stream type to use is made in the hadoop configuration.

Configuration Options


| Property                   | Permitted Values                                        | Default   | Meaning                    |
|----------------------------|---------------------------------------------------------|-----------|----------------------------|
| `fs.s3a.input.stream.type` | `default`, `classic`, `analytics`, `prefetch`, `custom` | `classic` | Name of stream type to use |

### Stream type `default`

The default implementation for this release of Hadoop.

```xml
<property>
  <name>fs.s3a.input.stream.type</name>
  <value>default</value>
</property>
```

The choice of which stream type to use by default may change in future releases.

It is currently `classic`.

### Stream type `classic`

This is the classic S3A input stream, present since the original addition of the S3A connector
to the Hadoop codebase.

```xml
<property>
  <name>fs.s3a.input.stream.type</name>
  <value>classic</value>
</property>
```

Strengths
* Stable
* Petabytes of data are read through the connector a day,  so well tested in production.
* Resilience to network and service failures acquired "a stack trace at at time"
* Implements Vector IO through parallel HTTP requests.

Weaknesses
* Takes effort to tune for different file formats/read strategies (sequential, random etc),
  and suboptimal if not correctly tuned for the workloads.
* Non-vectored reads are blocking, with the sole buffering being that from
  the http client library and layers beneath.
* Can keep HTTP connections open too long/not returned to the connection pool.
  This can consume both network resources and can consume all connections
  in the pool if streams are not correctly closed by applications.

### Stream type `analytics`

An input stream aware-of and adapted-to the columnar storage
formats used in production, currently with specific support for
Apache Parquet.

```xml
<property>
  <name>fs.s3a.input.stream.type</name>
  <value>analytics</value>
</property>
```

Strengths
* Significant speedup measured when reading Parquet files through Spark.
* Prefetching can also help other read patterns/file formats.
* Parquet V1 Footer caching reduces HEAD/GET requests when opening
  and reading files.

Weaknesses
* Requires an extra library.
* Currently considered in "stabilization".
* Likely to need more tuning on failure handling, either in the S3A code or
  (better) the underlying library.
* Not yet benchmarked with other applications (Apache Hive or ORC).
* Vector IO API falls back to a series of sequential read calls.
  For Parquet the format-aware prefetching will satisfy the requests,
  but for ORC this may be be very inefficient.

It delivers tangible speedup for reading Parquet files where the reader
is deployed within AWS infrastructure, it will just take time to encounter
all the failure conditions which the classic connectors have encountered
and had to address.

This library is where all future feature development is focused,
including benchmark-based tuning for other file formats.


### Stream type `prefetch`

This input stream prefetches data in multi-MB blocks and caches these
on the local disk's buffer directory.

```xml
<property>
  <name>fs.s3a.input.stream.type</name>
  <value>prefetch</value>
</property>
```

Strengths
* Format agnostic.
* Asynchronous, parallel pre-fetching of blocks.
* Blocking on-demand reads of any blocks which are required and not cached.
  This may be done in multiple parallel reads, as required.
* Blocks are cached, so that backwards and random IO is very
  efficient if using data already read/prefetched.

Weaknesses
* Caching of blocks is for the duration of the filesystem instance,
  this works for transient worker processes, but not for
  long-lived processes such as Spark or HBase workers.
* No prediction of which blocks are to be prefetched next,
  so can be wasteful of prefetch reads, while still blocking on application
  read operations.



## Vector IO and Stream Types

All streams support VectorIO to some degree.

| Stream      | Support                                                     |
|-------------|-------------------------------------------------------------|
| `classic`   | Parallel issuing of GET request with range coalescing       |
| `prefetch`  | Sequential reads, using prefetched blocks as appropriate    |
| `analytics` | Sequential reads, using prefetched blocks as where possible |

Because the analytics streams is doing parquet-aware RowGroup prefetch, its
prefetched blocks should align with Parquet read sequences through vectored
reads, as well the unvectored reads.

This does not hold for ORC.
When reading ORC files with a version of the ORC library which is
configured to use the vector IO API, it is likely to be significantly
faster to use the classic stream and its parallel reads.


## Developer Topics

### Stream IOStatistics

Some of the streams support detailed IOStatistics, which will get aggregated into
the filesystem IOStatistics when the stream is closed(), or possibly after `unbuffer()`.

The filesystem aggregation can be displayed when the instance is closed, which happens
in process termination, if not earlier:
```xml
  <property>
    <name>fs.thread.level.iostatistics.enabled</name>
    <value>true</value>
  </property>
```

### Capabilities Probe for stream type and features.

`StreamCapabilities.hasCapability()` can be used to probe for the active
stream type and its capabilities.

### Unbuffer() support

The `unbuffer()` operation requires the stream to release all client-side
resources: buffer, connections to remote servers, cached files etc.
This is used in some query engines, including Apache Impala, to keep
streams open for rapid re-use, avoiding the overhead of re-opening files.

Only the classic stream supports `CanUnbuffer.unbuffer()`;
the other streams must be closed rather than kept open for an extended
period of time.

### Stream Leak alerts

All input streams MUST be closed via a `close()` call once no-longer needed
-this is the only way to guarantee a timely release of HTTP connections
and local resources.

Some applications/libraries neglect to close the stram

### Custom Stream Types

There is a special stream type `custom`.
This is primarily used internally for testing, however it may also be used by
anyone who wishes to experiment with alternative input stream implementations.

If it is requested, then the name of the _factory_ for streams must be set in the
property `fs.s3a.input.stream.custom.factory`.

This must be a classname to an implementation of the factory service,
`org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamFactory`.
Consult the source and javadocs of the package `org.apache.hadoop.fs.s3a.impl.streams` for
details.

*Note* this is very much internal code and unstable: any use of this should be considered
experimental, unstable -and is not recommended for production use.



| Property                             | Permitted Values                       | Meaning                     |
|--------------------------------------|----------------------------------------|-----------------------------|
| `fs.s3a.input.stream.custom.factory` | name of factory class on the classpath | classname of custom factory |

