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

### Set optional or mandatory parameters

    FSDataInputStreamBuilder opt(String key, ...)
    FSDataInputStreamBuilder must(String key, ...)

Set optional or mandatory parameters to the builder. Using `opt()` or `must()`,
client can specify FS-specific parameters without inspecting the concrete type
of `FileSystem`.

```java
out = fs.openFile(path)
    .opt("fs.s3a.experimental.fadvise", "random")
    .must("fs.s3a.readahead.range", 256 * 1024)
    .build()
    .get();
```

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
