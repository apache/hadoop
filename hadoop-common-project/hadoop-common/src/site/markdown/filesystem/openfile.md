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

# `openFile()`

Create a builder to open a file, supporting options
both standard and filesystem specific. The return
value of the `build()` call is a `Future<FSDataInputStream>`,
which must be waited on. The file opening may be
asynchronous, and it may actually be postponed (including
permission/existence checks) until reads are actually
performed.

This API call was added to `FileSystem` and `FileContext` in
Hadoop 3.3.0; it was tuned in Hadoop 3.3.1 as follows.

* Support `opt(key, long)` and `must(key, long)`. 
* Declare that `withFileStatus(null)` is allowed.
* Declare that `withFileStatus(status)` only checks
  the filename of the path, not the full path.
  This is needed to support passthrough/mounted filesystems.


###  <a name="openfile(path)"></a> `FSDataInputStreamBuilder openFile(Path path)`

Creates a [`FSDataInputStreamBuilder`](fsdatainputstreambuilder.html)
to construct a operation to open the file at `path` for reading.

When `build()` is invoked on the returned `FSDataInputStreamBuilder` instance,
the builder parameters are verified and
`openFileWithOptions(Path, OpenFileParameters)` invoked.

This (protected) operation returns a `CompletableFuture<FSDataInputStream>`
which, when its `get()` method is called, either returns an input
stream of the contents of opened file, or raises an exception.

The base implementation of the `openFileWithOptions(PathHandle, OpenFileParameters)`
ultimately invokes `open(Path, int)`.

Thus the chain `openFile(path).build().get()` has the same preconditions
and postconditions as `open(Path p, int bufferSize)`

However, there is one difference which implementations are free to
take advantage of: 

The returned stream MAY implement a lazy open where file non-existence or
access permission failures may not surface until the first `read()` of the
actual data.

The `openFile()` operation may check the state of the filesystem during its
invocation, but as the state of the filesystem may change betwen this call and
the actual `build()` and `get()` operations, this file-specific
preconditions (file exists, file is readable, etc) MUST NOT be checked here.

FileSystem implementations which do not implement `open(Path, int)`
MAY postpone raising an `UnsupportedOperationException` until either the
`FSDataInputStreamBuilder.build()` or the subsequent `get()` call,
else they MAY fail fast in the `openFile()` call.

### Implementors notes

The base implementation of `openFileWithOptions()` actually executes
the `open(path)` operation synchronously, yet still returns the result
or any failures in the `CompletableFuture<>`, so as to ensure that users
code expecting this.

Any filesystem where the time to open a file may be significant SHOULD
execute it asynchronously by submitting the operation in some executor/thread
pool. This is particularly recommended for object stores and other filesystems
likely to be accessed over long-haul connections.

Arbitrary filesystem-specific options MAY be supported; these MUST
be prefixed with either the filesystem schema, e.g. `hdfs.`
or in the `fs.SCHEMA` format as normal configuration settings `fs.hdfs`. The
latter style allows the same configuration option to be used for both
filesystem configuration and file-specific configuration.

It SHOULD be possible to always open a file without specifying any options,
so as to present a consistent model to users. However, an implementation MAY
opt to require one or more mandatory options to be set.

The returned stream may perform "lazy" evaluation of file access. This is
relevant for object stores where the probes for existence are expensive, and,
even with an asynchronous open, may be considered needless.
 
### <a name="openfile(pathhandle)"></a> `FSDataInputStreamBuilder openFile(PathHandle)`

Creates a `FSDataInputStreamBuilder` to build an operation to open a file.
Creates a [`FSDataInputStreamBuilder`](fsdatainputstreambuilder.html)
to construct a operation to open the file identified by the given `PathHandle` for reading.

When `build()` is invoked on the returned `FSDataInputStreamBuilder` instance,
the builder parameters are verified and
`openFileWithOptions(PathHandle, OpenFileParameters)` invoked.

This (protected) operation returns a `CompletableFuture<FSDataInputStream>`
which, when its `get()` method is called, either returns an input
stream of the contents of opened file, or raises an exception.

The base implementation of the `openFileWithOptions(PathHandle, OpenFileParameters)` method
returns a future which invokes `open(Path, int)`.

Thus the chain `openFile(pathhandle).build().get()` has the same preconditions
and postconditions as `open(Pathhandle, int)`

As with `FSDataInputStreamBuilder openFile(PathHandle)`, the `openFile()`
call must not be where path-specific preconditions are checked -that
is postponed to the `build()` and `get()` calls.

FileSystem implementations which do not implement `open(PathHandle handle, int bufferSize)`
MAY postpone raising an `UnsupportedOperationException` until either the
`FSDataInputStreamBuilder.build()` or the subsequent `get()` call,
else they MAY fail fast in the `openFile()` call.

The base implementation raises this exception in the `build()` operation;
other implementations SHOULD copy this.


## <a name="options"></a> Standard `openFile()` options

The standard `openFile()` options are defined in `org.apache.hadoop.fs.OpenFileOptions`;
they all SHALL start with `fs.option.openfile`.

Note that while all `FileSystem`/`FileContext` instances SHALL support these options
to the extent that `must()` declarations SHALL NOT fail, the implementations
MAY support them to the extent of interpreting the values.
This means that it is not a requirement for the stores to actually read the
the fadvise or file length values and use them when opening files.

They MUST be viewed as hints.

#### `fs.option.openfile.fadvise`

Declare the read strategy of the input stream. This MAY control readahead, buffering
and other optimizations. For example, for an object store which assumes a 
read is sequential, a single `GET` request may be used to read that data.
If a stream is being read in random IO, then ranged `GET` calls to blocks of
data is more efficient -so may be used instead.

| Policy | Meaning |
| -------|---------|
| `normal` | The "Normal" policy for this store. |
| `sequential` | Optimized for sequential access. |
| `random` | Optimized purely for random seek+read/positionedRead operations. |

Choosing the wrong read policy for an input source may be inefficient.

<b>Tip:</b> log the `toString()` value of input streams at `DEBUG`. The S3A and ABFS Input
Streams log read statistics, which can provide insight about whether reads are being
performed efficiently or not.

#### `fs.option.openfile.length`

Declare the length of a file. This MAY be used to skip a probe for the file
length.


If supported by a filesystem connector, this option MUST be interpreted as declaring
the minimum length of the file: 

1. It SHALL NOT be an error if the actual length of the file is greater than this value.
1. `read()`, `seek()` and positioned read calls MAY use a position across/beyond this length
   but below the actual length of the file.
   Implementations MAY raise `EOFExceptions` in such cases, or they MAY return data.


## Example

Here is an example from a proof of concept `org.apache.parquet.hadoop.util.HadoopInputFile`
reader which uses a (nullable) file status and a split start/end.

The `FileStatus` value is always passed in -but if it is null, then the split
end is used to declare the length of the file. 

```java
protected SeekableInputStream newStream(Path path, FileStatus stat,
     long splitStart, long splitEnd)
     throws IOException {
    FutureDataInputStreamBuilder builder = fs.openFile(path)
      .opt("fs.option.openfile.fadvise", "random")
      .withFileStatus(stat);

    // fall back to leng
    if (stat == null && splitEnd > 0) {
      builder.opt("fs.option.openfile.length", splitEnd);
    }
    CompletableFuture<FSDataInputStream> streamF = builder.build();
    return HadoopStreams.wrap(awaitFuture(streamF));
  }
```

As a result, whether driven directly by a file listing,
or when opening a file from a query plan of `(path, splitStart, splitEnd)`,
there is no need to probe the remote store for the length of the file.
When working with remote object stores, this can save tens to hundreds
of milliseconds, even if such a probe is done asynchronously.


## S3A Non-standard options

The S3A Connector supports custom options 


|  Name | Type | Meaning |
|-------|------|---------|
| `fs.s3a.readahead.range` | long | readahead range in bytes  |
| `fs.s3a.experimental.input.fadvise` | String | seek policy. Superceded by `fs.option.openfile.fadvise`   |


If the option set contains a SQL statement in the `fs.s3a.select.sql` statement,
then the file is opened as an S3 Select query. Consult the S3A documentation.

 
## Passing options in to MapReduce

Hadoop MapReduce will automatically read MR Job Options with the prefixes
`mapreduce.job.input.file.option.` and `mapreduce.job.input.file.must.`
prefixes, and apply these values as `.opt()` and `must()` respectively,
after remove the mapreduce-specific prefixes.

This makes passing options in to MR jobs straightforward.
For example, to declare that a job should read its data using random IO:

```java
JobConf jobConf = (JobConf) job.getConfiguration()
jobConf.set(
    "mapreduce.job.input.file.option.fs.option.openfile.fadvise",
    "random"); 
```

