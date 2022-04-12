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

# `FileSystem.openFile()`/`FileContext.openFile()`

This is a method provided by both FileSystem and FileContext for
advanced file opening options and, where implemented,
an asynchrounous/lazy opening of a file.

Creates a builder to open a file, supporting options
both standard and filesystem specific. The return
value of the `build()` call is a `Future<FSDataInputStream>`,
which must be waited on. The file opening may be
asynchronous, and it may actually be postponed (including
permission/existence checks) until reads are actually
performed.

This API call was added to `FileSystem` and `FileContext` in
Hadoop 3.3.0; it was tuned in Hadoop 3.3.1 as follows.

* Added `opt(key, long)` and `must(key, long)`.
* Declared that `withFileStatus(null)` is allowed.
* Declared that `withFileStatus(status)` only checks
  the filename of the path, not the full path.
  This is needed to support passthrough/mounted filesystems.
* Added standard option keys.

###  <a name="openfile_path_"></a> `FutureDataInputStreamBuilder openFile(Path path)`

Creates a [`FutureDataInputStreamBuilder`](fsdatainputstreambuilder.html)
to construct a operation to open the file at `path` for reading.

When `build()` is invoked on the returned `FutureDataInputStreamBuilder` instance,
the builder parameters are verified and
`FileSystem.openFileWithOptions(Path, OpenFileParameters)` or
`AbstractFileSystem.openFileWithOptions(Path, OpenFileParameters)` invoked.

These protected methods returns a `CompletableFuture<FSDataInputStream>`
which, when its `get()` method is called, either returns an input
stream of the contents of opened file, or raises an exception.

The base implementation of the `FileSystem.openFileWithOptions(PathHandle, OpenFileParameters)`
ultimately invokes `FileSystem.open(Path, int)`.

Thus the chain `FileSystem.openFile(path).build().get()` has the same preconditions
and postconditions as `FileSystem.open(Path p, int bufferSize)`

However, there is one difference which implementations are free to
take advantage of:

The returned stream MAY implement a lazy open where file non-existence or
access permission failures may not surface until the first `read()` of the
actual data.

This saves network IO on object stores.

The `openFile()` operation MAY check the state of the filesystem during its
invocation, but as the state of the filesystem may change between this call and
the actual `build()` and `get()` operations, this file-specific
preconditions (file exists, file is readable, etc) MUST NOT be checked here.

FileSystem implementations which do not implement `open(Path, int)`
MAY postpone raising an `UnsupportedOperationException` until either the
`FutureDataInputStreamBuilder.build()` or the subsequent `get()` call,
else they MAY fail fast in the `openFile()` call.

Consult [`FutureDataInputStreamBuilder`](fsdatainputstreambuilder.html) for details
on how to use the builder, and for standard options which may be passed in.

### <a name="openfile_pathhandle_"></a> `FutureDataInputStreamBuilder openFile(PathHandle)`

Creates a [`FutureDataInputStreamBuilder`](fsdatainputstreambuilder.html)
to construct a operation to open the file identified by the given `PathHandle` for reading.

If implemented by a filesystem, the semantics of  [`openFile(Path)`](#openfile_path_)
Thus the chain `openFile(pathhandle).build().get()` has the same preconditions and postconditions
as `open(Pathhandle, int)`

FileSystem implementations which do not implement `open(PathHandle handle, int bufferSize)`
MAY postpone raising an `UnsupportedOperationException` until either the
`FutureDataInputStreamBuilder.build()` or the subsequent `get()` call, else they MAY fail fast in
the `openFile(PathHandle)` call.

The base implementation raises this exception in the `build()` operation; other implementations
SHOULD copy this.

### Implementors notes

The base implementation of `openFileWithOptions()` actually executes
the `open(path)` operation synchronously, yet still returns the result
or any failures in the `CompletableFuture<>`, so as to provide a consistent
lifecycle across all filesystems.

Any filesystem client where the time to open a file may be significant SHOULD
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
