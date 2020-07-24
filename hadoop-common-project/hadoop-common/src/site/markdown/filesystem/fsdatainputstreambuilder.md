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
<!--  CLASS: FSDataInputStreamBuilder -->
<!--  ============================================================= -->

# class `org.apache.hadoop.fs.FSDataInputStreamBuilder`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

An interface offering of the Builder pattern for creating Java `Future`
references to `FSDataInputStream` and its subclasses.
It is used to initate a (potentially asynchronous) operation to open an existing
file for reading.

## Invariants

The `FSDataInputStreamBuilder` interface does not require parameters or
or the state of `FileSystem` until [`build()`](#build) is
invoked and/or during the asynchronous open operation itself.

Some aspects of the state of the filesystem, MAY be checked in the initial
`openFile()` call, provided they are known to be invariants which will not
change between `openFile()` and the `build().get()` sequence. For example,
path validation.

## Implementation-agnostic parameters.


### <a name="Builder.bufferSize"></a> `FSDataInputStreamBuilder bufferSize(int bufSize)`

Set the size of the buffer to be used.

### <a name="Builder.withFileStatus"></a> `FSDataInputStreamBuilder withFileStatus(FileStatus status)`

A `FileStatus` instance which refers to the file being opened.

This MAY be used by implementations to short-circuit checks for the file,
So potentially saving on remote calls especially to object stores.

Requirements:

* `status != null`
* `status.getPath()` == the resolved path of the file being opened.

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


### Set optional or mandatory parameters

    FSDataInputStreamBuilder opt(String key, ...)
    FSDataInputStreamBuilder must(String key, ...)

Set optional or mandatory parameters to the builder. Using `opt()` or `must()`,
client can specify FS-specific parameters without inspecting the concrete type
of `FileSystem`.

Example:

```java
out = fs.openFile(path)
    .must("fs.opt.openfile.fadvise", "random")
    .opt("fs.http.connection.timeout", 30_000L)
    .withFileStatus(statusFromListing)
    .build()
    .get();
```

Here the seek policy of `random` has been specified,
with the requirement that the filesystem implementation must understand the option.
And s3a-specific option has been supplied which may be interpreted by any store;
the expectation is that the S3A connector will recognize it.

#### Implementation Notes

Checking for supported options must be performed in the `build()` operation.

1. If a mandatory parameter declared via `must(key, value)`) is not recognized,
`IllegalArgumentException` MUST be thrown.

1. If a mandatory parameter declared via `must(key, value)`) relies on
a feature which is recognized but not supported in the specific
Filesystem/FileContext instance `UnsupportedException` MUST be thrown.

The behavior of resolving the conflicts between the parameters set by
builder methods (i.e., `bufferSize()`) and `opt()`/`must()` is as follows:

> The last option specified defines the value and its optional/mandatory state.

If the `FileStatus` option passed in `withFileStatus()` is used, implementations
MUST accept all subclasses of `FileStatus`, including `LocatedFileStatus`,
rather than just any FS-specific subclass implemented by the implementation
(e.g `S3AFileStatus`). They MAY simply ignore those which are not the 
custom subclasses.

This is critical to ensure safe use of the feature: directory listing/
status serialization/deserialization can result result in the `withFileStatus()`
argumennt not being the custom subclass returned by the Filesystem instance's
own `getFileStatus()`, `listFiles()`, `listLocatedStatus()` calls, etc.

In such a situation the implementations must:

1. Validate the path (always).
1. Use the status/convert to the custom type, *or* simply discard it.

## Builder interface

### <a name="build"></a> `CompletableFuture<FSDataInputStream> build()`


Return an `CompletableFuture<FSDataInputStream>` which, when successfully
completed, returns an input stream which can read data from the filesystem.

The `build()` operation MAY perform the validation of the file's existence,
its kind, so rejecting attempts to read from a directory or non-existent
file. **Alternatively**, the `build()` operation may delay all checks
until an asynchronous operation whose outcome is provided by the `Future`

That is, the precondition  `exists(FS, path)` and `isFile(FS, path)` are
only guaranteed to have been met after the `get()` on the returned future is successful.

Thus, if even a file does not exist, the following call will still succeed, returning
a future to be evaluated.

```java
Path p = new Path("file://tmp/file-which-does-not-exist");

CompletableFuture<FSDataInputStream> future = p.getFileSystem(conf)
      .openFile(p)
      .build;
```

The preconditions for opening the file are checked during the asynchronous
evaluation, and so will surface when the future is completed:

```java
FSDataInputStream in = future.get();
```

## <a name="options"></a> Standard options

Individual filesystems MAY provide their own set of options for use in the builder.

There are also some standard options with common names, all of which begin with
`fs.opt.openfile`

| Name | Type | Description |
|------|------|-------------|
| `fs.opt.openfile.fadvise` | string | seek policy |
| `fs.opt.openfile.length` | long | file length |

These policies are *not* declared as constants in
public `org.apache.hadoop` classes/interfaces, so as to avoid
applications being unable to link to a specific hadoop release
without that standard option.

(Implementors: use `org.apache.hadoop.fs.impl.OpenFileParameters`.)

### <a name="option.fadvise"></a> Seek Policy Option `fs.opt.openfile.fadvise`

This is a hint as to what the expected read pattern of an input stream will be.
"sequential" -read a file from start to finish. "Random": client will be
reading data in different parts of the file using a sequence of `seek()/read()`
or via the `PositionedReadable` or `ByteBufferPositionedReadable` APIs.

Sequential reads may be optimized with prefetching data and/or reading data in larger blocks.
In contrast, random IO performance may be best if little/no prefetching takes place, along
with other possible optimizations.
Queries over columnar formats such as Apach ORC and Apache Parquet
perform such random IO; other data formats are best for sequential reads.
Some applications (e.g. distCp) perform sequential IO even over columnar data.

What is key is that optimizing reads for seqential reads may impair random performance
-and vice versa.

Allowing applications to hint what their read policy is allows the filesystem clients
to optimize their interaction with the underlying data stores.

1. The seek policy is a hint; even if declared as a `must()` option, the filesystem
MAY ignore it.
1. The interpretation/implementation of a policy is a filesystem specific behavior
-and it may change with Hadoop releases and/or specific storage subsystems.
1. If a policy is not recognized, the FileSystem MUST ignore it and revert to
whichever policy is active for that specific FileSystem instance.

| Policy | Meaning |
|--------|---------|
| `normal` | Default policy for the filesystem |
| `sequential` | Optimize for sequential IO |
| `random` | Optimize for random IO |
| `adaptive` | Adapt seek policy based on read patterns |


#### Seek Policy `normal`

The default policy for the filesystem instance.
Implementation/installation-specific

#### Seek Policy `sequential`

Expect sequential reads from the first byte read to the end of the file/until the stream is closed.


#### Seek Policy `random`

Expect `seek()/read()` sequences, or use of `PositionedReadable` or `ByteBufferPositionedReadable` APIs.

#### Seek Policy `adaptive`

Try to adapt the seek policy to the read pattern of the application.

The `normal` policy of the S3A client and the sole policy supported by the `wasb:` client are both
adaptive -they assume sequential IO, but once a backwards seek/positioned read call is made
the stream switches to random IO.

Other filesystem implementations may wish to adopt similar strategies, and/or extend
the algorithms to detect forward seeks and/or switch from random to sequential IO if
that is considered more efficient.

Adaptive seek policies have proven effective in the absence of the ability to declare
the seek policy in the `open()` API, so requiring it to be declared, if configurable,
in the cluster/application configuration. However, the switch from sequential to
random seek policies may

When applications explicitly set the `fs.opt.openfile.fadvise` option, if they
know their read plan, they SHOULD declare which policy is most appropriate.

_Implementor's Notes_

If a policy is unrecognized/unsupported: SHOULD log it and then MUST ignore the option. This is
to support future evolution of policies and implementations


_Futher reading_

* [Linux fadvise()](https://linux.die.net/man/2/fadvise).
* [Windows `CreateFile()`](https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea#caching-behavior)

### <a name="option.length"></a> File Length hint `fs.opt.openfile.length`

Declare that a file is expected to be *at least as long as the specified length*

This can be used by clients to skip querying a remote store for the size of/existence of a file when
opening it, similar to declaring a file status through the `withFileStatus()` option.

The supplied length MAY be shorter than the actual file.

This allows worker tasks which only know of of their task's split to declare that as the
length of the file.

If this option is used by the FileSystem implementation

* A `length` &lt; 0 MUST be rejected.
* If a file status is supplied along with a value in `fs.opt.openfile.length`; the file status
values take precedence.
* seek/read calls past the length value MAY be rejected, equally: they MAY be accepted. It is simply a hint.
