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

<!--  ============================================================= -->
<!--  CLASS: FutureDataInputStreamBuilder -->
<!--  ============================================================= -->

# class `org.apache.hadoop.fs.FutureDataInputStreamBuilder`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

An interface offering of the Builder pattern for creating Java `Future`
references to `FSDataInputStream` and its subclasses.
It is used to initate a (potentially asynchronous) operation to open an existing
file for reading.


## <a name="History"></a> History

### Hadoop 3.3.0: API introduced

[HADOOP-15229](https://issues.apache.org/jira/browse/HADOOP-15229)
_Add FileSystem builder-based openFile() API to match createFile()_

* No `opt(String key, long value)` method was available.
* the `withFileStatus(status)` call required a non-null parameter.
* Sole Filesystem to process options and file status was S3A;
* Only the s3a specific options were the S3 select and `fs.s3a.experimental.input.fadvise`
* S3A Filesystem raised `IllegalArgumentException` if a file status was passed in
  and the path of the filestatus did not match the path of the `openFile(path)` call.

This is the baseline implementation. To write code guaranteed to compile against this version,
use the `opt(String, String)` and `must(String, String)` methods, converting numbers to
string explicitly.

```java
fs.open("s3a://bucket/file")
  .opt("fs.option.openfile.length", Long.toString(length))
  .build().get()
```

### Hadoop 3.3.5: standardization and expansion

[HADOOP-16202](https://issues.apache.org/jira/browse/HADOOP-16202)
_Enhance openFile() for better read performance against object stores_

* `withFileStatus(null)` required to be accepted (and ignored)
* only the filename part of any supplied FileStatus path must match the
  filename passed in on `openFile(path)`.
* An `opt(String key, long value)` option was added. *This is now deprecated as it
caused regression
* Standard `fs.option.openfile` options defined.
* S3A FS to use openfile length option, seek start/end options not _yet_ used.
* Azure ABFS connector takes a supplied `VersionedFileStatus` and omits any
  HEAD probe for the object.

### Hadoop 3.3.6: API change to address operator overload bugs.

new `optLong()`, `optDouble()`, `mustLong()` and `mustDouble()` builder methods.

* See [HADOOP-18724](https://issues.apache.org/jira/browse/HADOOP-18724) _Open file fails with NumberFormatException for S3AFileSystem_,
  which was somehow caused by the overloaded `opt(long)`.
* Specification updated to declare that unparseable numbers MUST be treated as "unset" and the default
  value used instead.

## Invariants

The `FutureDataInputStreamBuilder` interface does not require parameters or
or the state of `FileSystem` until [`build()`](#build) is
invoked and/or during the asynchronous open operation itself.

Some aspects of the state of the filesystem, MAY be checked in the initial
`openFile()` call, provided they are known to be invariants which will not
change between `openFile()` and the `build().get()` sequence. For example,
path validation.

## <a name="parameters"></a> `Implementation-agnostic parameters.


### <a name="Builder.bufferSize"></a> `FutureDataInputStreamBuilder bufferSize(int bufSize)`

Set the size of the buffer to be used.

### <a name="Builder.withFileStatus"></a> `FutureDataInputStreamBuilder withFileStatus(FileStatus status)`

A `FileStatus` instance which refers to the file being opened.

This MAY be used by implementations to short-circuit checks for the file,
So potentially saving on remote calls especially to object stores.

Requirements:

* `status != null`
* `status.getPath().getName()` == the name of the file being opened.

The path validation MUST take place if the store uses the `FileStatus` when
it opens files, and MAY be performed otherwise. The validation
SHOULD be postponed until the `build()` operation.

This operation should be considered a hint to the filesystem.

If a filesystem implementation extends the `FileStatus` returned in its
implementation MAY use this information when opening the file.

This is relevant with those stores which return version/etag information,
-they MAY use this to guarantee that the file they opened
is exactly the one returned in the listing.


The final `status.getPath().getName()` element of the supplied status MUST equal
the name value of the path supplied to the  `openFile(path)` call.

Filesystems MUST NOT validate the rest of the path.
This is needed to support viewfs and other mount-point wrapper filesystems
where schemas and paths are different. These often create their own FileStatus results

Preconditions

```python
status == null or status.getPath().getName() == path.getName()

```

Filesystems MUST NOT require the class of `status` to equal
that of any specific subclass their implementation returns in filestatus/list
operations. This is to support wrapper filesystems and serialization/deserialization
of the status.


### <a name="optional"></a> Set optional or mandatory parameters

```java
FutureDataInputStreamBuilder opt(String key, String value)
FutureDataInputStreamBuilder opt(String key, int value)
FutureDataInputStreamBuilder opt(String key, boolean value)
FutureDataInputStreamBuilder optLong(String key, long value)
FutureDataInputStreamBuilder optDouble(String key, double value)
FutureDataInputStreamBuilder must(String key, String value)
FutureDataInputStreamBuilder must(String key, int value)
FutureDataInputStreamBuilder must(String key, boolean value)
FutureDataInputStreamBuilder mustLong(String key, long value)
FutureDataInputStreamBuilder mustDouble(String key, double value)
```

Set optional or mandatory parameters to the builder. Using `opt()` or `must()`,
client can specify FS-specific parameters without inspecting the concrete type
of `FileSystem`.

Example:

```java
out = fs.openFile(path)
    .must("fs.option.openfile.read.policy", "random")
    .optLong("fs.http.connection.timeout", 30_000L)
    .withFileStatus(statusFromListing)
    .build()
    .get();
```

Here the read policy of `random` has been specified,
with the requirement that the filesystem implementation must understand the option.
An http-specific option has been supplied which may be interpreted by any store;
If the filesystem opening the file does not recognize the option, it can safely be
ignored.

### <a name="usage"></a> When to use `opt` versus `must`

The difference between `opt` versus `must` is how the FileSystem opening
the file must react to an option which it does not recognize.

```python

def must(name, value):
  if not name in known_keys:
    raise IllegalArgumentException
  if not name in supported_keys:
    raise UnsupportedException


def opt(name, value):
  if not name in known_keys:
     # ignore option

```

For any known key, the validation of the `value` argument MUST be the same
irrespective of how the (key, value) pair was declared.

1. For a filesystem-specific option, it is the choice of the implementation
   how to validate the entry.
1. For standard options, the specification of what is a valid `value` is
   defined in this filesystem specification, validated through contract
   tests.

## <a name="implementation"></a> Implementation Notes

Checking for supported options must be performed in the `build()` operation.

1. If a mandatory parameter declared via `must(key, value)`) is not recognized,
`IllegalArgumentException` MUST be thrown.

1. If a mandatory parameter declared via `must(key, value)` relies on
a feature which is recognized but not supported in the specific
`FileSystem`/`FileContext` instance `UnsupportedException` MUST be thrown.

Parsing of numeric values SHOULD trim any string and if the value
cannot be parsed as a number, downgrade to any default value supplied.
This is to address [HADOOP-18724](https://issues.apache.org/jira/browse/HADOOP-18724)
_Open file fails with NumberFormatException for S3AFileSystem_, which was cause by the overloaded `opt()`
builder parameter binding to `opt(String, double)` rather than `opt(String, long)` when a long
value was passed in.

The behavior of resolving the conflicts between the parameters set by
builder methods (i.e., `bufferSize()`) and `opt()`/`must()` is as follows:

> The last option specified defines the value and its optional/mandatory state.

If the `FileStatus` option passed in `withFileStatus()` is used, implementations
MUST accept all subclasses of `FileStatus`, including `LocatedFileStatus`,
rather than just any FS-specific subclass implemented by the implementation
(e.g `S3AFileStatus`). They MAY simply ignore those which are not the 
custom subclasses.

This is critical to ensure safe use of the feature: directory listing/
status serialization/deserialization can result in the `withFileStatus()`
argument not being the custom subclass returned by the Filesystem instance's
own `getFileStatus()`, `listFiles()`, `listLocatedStatus()` calls, etc.

In such a situation the implementations must:

1. Verify that `status.getPath().getName()` matches the current `path.getName()`
   value. The rest of the path MUST NOT be validated.
1. Use any status fields as desired -for example the file length.

Even if not values of the status are used, the presence of the argument
can be interpreted as the caller declaring that they believe the file
to be present and of the given size.

## <a name="builder"></a> Builder interface

### <a name="build"></a> `CompletableFuture<FSDataInputStream> build()`


Return an `CompletableFuture<FSDataInputStream>` which, when successfully
completed, returns an input stream which can read data from the filesystem.

The `build()` operation MAY perform the validation of the file's existence,
its kind, so rejecting attempts to read from a directory or non-existent
file. Alternatively
* file existence/status checks MAY be performed asynchronously within the returned
    `CompletableFuture<>`.
* file existence/status checks MAY be postponed until the first byte is read in
  any of the read such as `read()` or `PositionedRead`.

That is, the precondition  `exists(FS, path)` and `isFile(FS, path)` are
only guaranteed to have been met after the `get()` called on returned future
and an attempt has been made to read the stream.

Thus, if even when file does not exist, or is a directory rather than a file,
the following call MUST succeed, returning a `CompletableFuture` to be evaluated.

```java
Path p = new Path("file://tmp/file-which-does-not-exist");

CompletableFuture<FSDataInputStream> future = p.getFileSystem(conf)
      .openFile(p)
      .build();
```

The inability to access/read a file MUST raise an `IOException`or subclass
in either the future's `get()` call, or, for late binding operations,
when an operation to read data is invoked.

Therefore the following sequence SHALL fail when invoked on the
`future` returned by the previous example.

```java
  future.get().read();
```

Access permission checks have the same visibility requirements: permission failures
MUST be delayed until the `get()` call and MAY be delayed into subsequent operations.

Note: some operations on the input stream, such as `seek()` may not attempt any IO
at all. Such operations MAY NOT raise exceotions when interacting with
nonexistent/unreadable files.

## <a name="options"></a> Standard `openFile()` options since hadoop branch-3.3

These are options which `FileSystem` and `FileContext` implementation
MUST recognise and MAY support by changing the behavior of
their input streams as appropriate.

Hadoop 3.3.0 added the `openFile()` API; these standard options were defined in
a later release. Therefore, although they are "well known", unless confident that
the application will only be executed against releases of Hadoop which knows of
the options -applications SHOULD set the options via `opt()` calls rather than `must()`.

When opening a file through the `openFile()` builder API, callers MAY use
both `.opt(key, value)` and `.must(key, value)` calls to set standard and
filesystem-specific options.

If set as an `opt()` parameter, unsupported "standard" options MUST be ignored,
as MUST unrecognized standard options.

If set as a `must()` parameter, unsupported "standard" options MUST be ignored.
unrecognized standard options MUST be rejected.

The standard `openFile()` options are defined
in `org.apache.hadoop.fs.OpenFileOptions`; they all SHALL start
with `fs.option.openfile.`.

Note that while all `FileSystem`/`FileContext` instances SHALL support these
options to the extent that `must()` declarations SHALL NOT fail, the
implementations MAY support them to the extent of interpreting the values. This
means that it is not a requirement for the stores to actually read the read
policy or file length values and use them when opening files.

Unless otherwise stated, they SHOULD be viewed as hints.

Note: if a standard option is added such that if set but not
supported would be an error, then implementations SHALL reject it. For example,
the S3A filesystem client supports the ability to push down SQL commands. If
something like that were ever standardized, then the use of the option, either
in `opt()` or `must()` argument MUST be rejected for filesystems which don't
support the feature.

### <a name="buffer.size"></a>  Option: `fs.option.openfile.buffer.size`

Read buffer size in bytes.

This overrides the default value set in the configuration with the option
`io.file.buffer.size`.

It is supported by all filesystem clients which allow for stream-specific buffer
sizes to be set via `FileSystem.open(path, buffersize)`.

### <a name="read.policy"></a> Option: `fs.option.openfile.read.policy`

Declare the read policy of the input stream. This is a hint as to what the
expected read pattern of an input stream will be. This MAY control readahead,
buffering and other optimizations.

Sequential reads may be optimized with prefetching data and/or reading data in
larger blocks. Some applications (e.g. distCp) perform sequential IO even over
columnar data.

In contrast, random IO reads data in different parts of the file using a
sequence of `seek()/read()`
or via the `PositionedReadable` or `ByteBufferPositionedReadable` APIs.

Random IO performance may be best if little/no prefetching takes place, along
with other possible optimizations

Queries over columnar formats such as Apache ORC and Apache Parquet perform such
random IO; other data formats may be best read with sequential or whole-file
policies.

What is key is that optimizing reads for seqential reads may impair random
performance -and vice versa.

1. The seek policy is a hint; even if declared as a `must()` option, the
   filesystem MAY ignore it.
1. The interpretation/implementation of a policy is a filesystem specific
   behavior -and it may change with Hadoop releases and/or specific storage
   subsystems.
1. If a policy is not recognized, the filesystem client MUST ignore it.

| Policy       | Meaning                                                  |
|--------------|----------------------------------------------------------|
| `adaptive`   | Any adaptive policy implemented by the store.            |
| `default`    | The default policy for this store. Generally "adaptive". |
| `random`     | Optimize for random access.                              |
| `sequential` | Optimize for sequential access.                          |
| `vector`     | The Vectored IO API is intended to be used.              |
| `whole-file` | The whole file will be read.                             |

Choosing the wrong read policy for an input source may be inefficient.

A list of read policies MAY be supplied; the first one recognized/supported by
the filesystem SHALL be the one used. This allows for custom policies to be
supported, for example an `hbase-hfile` policy optimized for HBase HFiles.

The S3A and ABFS input streams both implement
the [IOStatisticsSource](iostatistics.html) API, and can be queried for their IO
Performance.

*Tip:* log the `toString()` value of input streams at `DEBUG`. The S3A and ABFS
Input Streams log read statistics, which can provide insight about whether reads
are being performed efficiently or not.

_Futher reading_

* [Linux fadvise()](https://linux.die.net/man/2/fadvise).
* [Windows `CreateFile()`](https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea#caching-behavior)

#### <a name="read.policy.adaptive"></a> Read Policy `adaptive`

Try to adapt the seek policy to the read pattern of the application.

The `normal` policy of the S3A client and the sole policy supported by
the `wasb:` client are both adaptive -they assume sequential IO, but once a
backwards seek/positioned read call is made the stream switches to random IO.

Other filesystem implementations may wish to adopt similar strategies, and/or
extend the algorithms to detect forward seeks and/or switch from random to
sequential IO if that is considered more efficient.

Adaptive read policies are the absence of the ability to
declare the seek policy in the `open()` API, so requiring it to be declared, if
configurable, in the cluster/application configuration. However, the switch from
sequential to random seek policies may be exensive.

When applications explicitly set the `fs.option.openfile.read.policy` option, if
they know their read plan, they SHOULD declare which policy is most appropriate.

#### <a name="read.policy.default"></a> Read Policy ``

The default policy for the filesystem instance.
Implementation/installation-specific.

#### <a name="read.policy.sequential"></a> Read Policy `sequential`

Expect sequential reads from the first byte read to the end of the file/until
the stream is closed.

#### <a name="read.policy.random"></a> Read Policy `random`

Expect `seek()/read()` sequences, or use of `PositionedReadable`
or `ByteBufferPositionedReadable` APIs.


#### <a name="read.policy.vector"></a> Read Policy `vector`

This declares that the caller intends to use the Vectored read API of
[HADOOP-11867](https://issues.apache.org/jira/browse/HADOOP-11867)
_Add a high-performance vectored read API_.

This is a hint: it is not a requirement when using the API.
It does inform the implemenations that the stream should be
configured for optimal vectored IO performance, if such a
feature has been implemented.

It is *not* exclusive: the same stream may still be used for
classic `InputStream` and `PositionedRead` API calls.
Implementations SHOULD use the `random` read policy
with these operations.

#### <a name="read.policy.whole-file"></a> Read Policy `whole-file`


This declares that the whole file is to be read end-to-end; the file system client is free to enable
whatever strategies maximise performance for this. In particular, larger ranged reads/GETs can
deliver high bandwidth by reducing socket/TLS setup costs and providing a connection long-lived
enough for TCP flow control to determine the optimal download rate.

Strategies can include:

* Initiate an HTTP GET of the entire file in `openFile()` operation.
* Prefech data in large blocks, possibly in parallel read operations.

Applications which know that the entire file is to be read from an opened stream SHOULD declare this
read policy.

### <a name="openfile.length"></a> Option: `fs.option.openfile.length`

Declare the length of a file.

This can be used by clients to skip querying a remote store for the size
of/existence of a file when opening it, similar to declaring a file status
through the `withFileStatus()` option.

If supported by a filesystem connector, this option MUST be interpreted as
declaring the minimum length of the file:

1. If the value is negative, the option SHALL be considered unset.
2. It SHALL NOT be an error if the actual length of the file is greater than
   this value.
3. `read()`, `seek()` and positioned read calls MAY use a position across/beyond
   this length but below the actual length of the file. Implementations MAY
   raise `EOFExceptions` in such cases, or they MAY return data.

If this option is used by the FileSystem implementation

*Implementor's Notes*

* A value of `fs.option.openfile.length` &lt; 0 MUST be ignored.
* If a file status is supplied along with a value in `fs.opt.openfile.length`;
  the file status values take precedence.

### <a name="split.start"></a> Options: `fs.option.openfile.split.start` and `fs.option.openfile.split.end`

Declare the start and end of the split when a file has been split for processing
in pieces.

1. If a value is negative, the option SHALL be considered unset.
1. Filesystems MAY assume that the length of the file is greater than or equal
   to the value of `fs.option.openfile.split.end`.
1. And that they MAY raise an exception if the client application reads past the
   value set in `fs.option.openfile.split.end`.
1. The pair of options MAY be used to optimise the read plan, such as setting
   the content range for GET requests, or using the split end as an implicit
   declaration of the guaranteed minimum length of the file.
1. If both options are set, and the split start is declared as greater than the
   split end, then the split start SHOULD just be reset to zero, rather than
   rejecting the operation.

The split end value can provide a hint as to the end of the input stream. The
split start can be used to optimize any initial read offset for filesystem
clients.

*Note for implementors: applications will read past the end of a split when they
need to read to the end of a record/line which begins before the end of the
split.

Therefore clients MUST be allowed to `seek()`/`read()` past the length
set in `fs.option.openfile.split.end` if the file is actually longer
than that value.

## <a name="s3a"></a> S3A-specific options

The S3A Connector supports custom options for readahead and seek policy.

| Name                                 | Type     | Meaning                                                                   |
|--------------------------------------|----------|---------------------------------------------------------------------------|
| `fs.s3a.readahead.range`             | `long`   | readahead range in bytes                                                  |
| `fs.s3a.experimental.input.fadvise`  | `String` | seek policy. Superceded by `fs.option.openfile.read.policy`               |
| `fs.s3a.input.async.drain.threshold` | `long`   | threshold to switch to asynchronous draining of the stream. (Since 3.3.5) |

If the option set contains a SQL statement in the `fs.s3a.select.sql` statement,
then the file is opened as an S3 Select query.
Consult the S3A documentation for more details.

## <a name="abfs"></a> ABFS-specific options

The ABFS Connector supports custom input stream options.

| Name                              | Type      | Meaning                                            |
|-----------------------------------|-----------|----------------------------------------------------|
| `fs.azure.buffered.pread.disable` | `boolean` | disable caching on the positioned read operations. |


Disables caching on data read through the [PositionedReadable](fsdatainputstream.html#PositionedReadable)
APIs.

Consult the ABFS Documentation for more details.

## <a name="examples"></a> Examples

#### Declaring seek policy and split limits when opening a file.

Here is an example from a proof of
concept `org.apache.parquet.hadoop.util.HadoopInputFile`
reader which uses a (nullable) file status and a split start/end.

The `FileStatus` value is always passed in -but if it is null, then the split
end is used to declare the length of the file.

```java
protected SeekableInputStream newStream(Path path, FileStatus stat,
     long splitStart, long splitEnd)
     throws IOException {

   FutureDataInputStreamBuilder builder = fs.openFile(path)
   .opt("fs.option.openfile.read.policy", "vector, random")
   .withFileStatus(stat);

   builder.optLong("fs.option.openfile.split.start", splitStart);
   builder.optLong("fs.option.openfile.split.end", splitEnd);
   CompletableFuture<FSDataInputStream> streamF = builder.build();
   return HadoopStreams.wrap(FutureIO.awaitFuture(streamF));
}
```

As a result, whether driven directly by a file listing, or when opening a file
from a query plan of `(path, splitStart, splitEnd)`, there is no need to probe
the remote store for the length of the file. When working with remote object
stores, this can save tens to hundreds of milliseconds, even if such a probe is
done asynchronously.

If both the file length and the split end is set, then the file length MUST be
considered "more" authoritative, that is it really SHOULD be defining the file
length. If the split end is set, the caller MAY ot read past it.

The `CompressedSplitLineReader` can read past the end of a split if it is
partway through processing a compressed record. That is: it assumes an
incomplete record read means that the file length is greater than the split
length, and that it MUST read the entirety of the partially read record. Other
readers may behave similarly.

Therefore

1. File length as supplied in a `FileStatus` or in `fs.option.openfile.length`
   SHALL set the strict upper limit on the length of a file
2. The split end as set in `fs.option.openfile.split.end` MUST be viewed as a
   hint, rather than the strict end of the file.

### Opening a file with both standard and non-standard options

Standard and non-standard options MAY be combined in the same `openFile()`
operation.

```java
Future<FSDataInputStream> f = openFile(path)
  .must("fs.option.openfile.read.policy", "random, adaptive")
  .opt("fs.s3a.readahead.range", 1024 * 1024)
  .build();

FSDataInputStream is = f.get();
```

The option set in `must()` MUST be understood, or at least recognized and
ignored by all filesystems. In this example, S3A-specific option MAY be
ignored by all other filesystem clients.

### Opening a file with older releases

Not all hadoop releases recognize the `fs.option.openfile.read.policy` option.

The option can be safely used in application code if it is added via the `opt()`
builder argument, as it will be treated as an unknown optional key which can
then be discarded.

```java
Future<FSDataInputStream> f = openFile(path)
  .opt("fs.option.openfile.read.policy", "vector, random, adaptive")
  .build();

FSDataInputStream is = f.get();
```

*Note 1* if the option name is set by a reference to a constant in
`org.apache.hadoop.fs.Options.OpenFileOptions`, then the program will not link
against versions of Hadoop without the specific option. Therefore for resilient
linking against older releases -use a copy of the value.

*Note 2* as option validation is performed in the FileSystem connector,
a third-party connector designed to work with multiple hadoop versions
MAY NOT support the option.

### Passing options in to MapReduce

Hadoop MapReduce will automatically read MR Job Options with the prefixes
`mapreduce.job.input.file.option.` and `mapreduce.job.input.file.must.`
prefixes, and apply these values as `.opt()` and `must()` respectively, after
remove the mapreduce-specific prefixes.

This makes passing options in to MR jobs straightforward. For example, to
declare that a job should read its data using random IO:

```java
JobConf jobConf = (JobConf) job.getConfiguration()
jobConf.set(
    "mapreduce.job.input.file.option.fs.option.openfile.read.policy",
    "random");
```

### MapReduce input format propagating options

An example of a record reader passing in options to the file it opens.

```java
  public void initialize(InputSplit genericSplit,
                     TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit)genericSplit;
    Configuration job = context.getConfiguration();
    start = split.getStart();
    end = start + split.getLength();
    Path file = split.getPath();

    // open the file and seek to the start of the split
    FutureDataInputStreamBuilder builder =
      file.getFileSystem(job).openFile(file);
    // the start and end of the split may be used to build
    // an input strategy.
    builder.optLong("fs.option.openfile.split.start", start);
    builder.optLong("fs.option.openfile.split.end", end);
    FutureIO.propagateOptions(builder, job,
        "mapreduce.job.input.file.option",
        "mapreduce.job.input.file.must");

    fileIn = FutureIO.awaitFuture(builder.build());
    fileIn.seek(start)
    /* Rest of the operation on the opened stream */
  }
```

### `FileContext.openFile`

From `org.apache.hadoop.fs.AvroFSInput`; a file is opened with sequential input.
Because the file length has already been probed for, the length is passed down

```java
  public AvroFSInput(FileContext fc, Path p) throws IOException {
    FileStatus status = fc.getFileStatus(p);
    this.len = status.getLen();
    this.stream = awaitFuture(fc.openFile(p)
        .opt("fs.option.openfile.read.policy",
            "sequential")
        .optLong("fs.option.openfile.length",
            Long.toString(status.getLen()))
        .build());
    fc.open(p);
  }
```

In this example, the length is passed down as a string (via `Long.toString()`)
rather than directly as a long. This is to ensure that the input format will
link against versions of $Hadoop which do not have the
`opt(String, long)` and `must(String, long)` builder parameters. Similarly, the
values are passed as optional, so that if unrecognized the application will
still succeed.

### Example: reading a whole file

This is from `org.apache.hadoop.util.JsonSerialization`.

Its `load(FileSystem, Path, FileStatus)` method
* declares the whole file is to be read end to end.
* passes down the file status

```java
public T load(FileSystem fs,
        Path path,
        status)
        throws IOException {

 try (FSDataInputStream dataInputStream =
          awaitFuture(fs.openFile(path)
              .opt("fs.option.openfile.read.policy", "whole-file")
              .withFileStatus(status)
              .build())) {
   return fromJsonStream(dataInputStream);
 } catch (JsonProcessingException e) {
   throw new PathIOException(path.toString(),
       "Failed to read JSON file " + e, e);
 }
}
```
