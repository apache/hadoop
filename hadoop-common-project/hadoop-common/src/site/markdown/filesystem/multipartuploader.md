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
<!--  INTERFACE: MultipartUploader -->
<!--  ============================================================= -->

# interface `org.apache.hadoop.fs.MultipartUploader`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

The `MultipartUploader` can upload a file
using multiple parts to Hadoop-supported filesystems. The benefits of a
multipart upload is that the file can be uploaded from multiple clients or
processes in parallel and the results will not be visible to other clients until
the `complete` function is called.

When implemented by an object store, uploaded data may incur storage charges,
even before it is visible in the filesystems. Users of this API must be diligent
and always perform best-effort attempts to complete or abort the upload.
The `abortUploadsUnderPath(path)` operation can help here.

## Invariants

All the requirements of a valid `MultipartUploader` are considered implicit
econditions and postconditions:

The operations of a single multipart upload may take place across different
instance of a multipart uploader, across different processes and hosts.
It is therefore a requirement that:

1. All state needed to upload a part, complete an upload or abort an upload
must be contained within or retrievable from an upload handle.

1. That handle MUST be serializable; it MUST be deserializable to different
processes executing the exact same version of Hadoop.

1. different hosts/processes MAY upload different parts, sequentially or
simultaneously. The order in which they are uploaded to the filesystem
MUST NOT constrain the order in which the data is stored in the final file.

1. An upload MAY be completed on a different instance than any which uploaded
parts.

1. The output of an upload MUST NOT be visible at the final destination
until the upload may complete.

1. It is not an error if a single multipart uploader instance initiates
or completes multiple uploads files to the same destination sequentially,
irrespective of whether or not the store supports concurrent uploads.

## Concurrency

Multiple processes may upload parts of a multipart upload simultaneously.

If a call is made to `startUpload(path)` to a destination where an active
upload is in progress, implementations MUST perform one of the two operations.

* Reject the call as a duplicate.
* Permit both to proceed, with the final output of the file being
that of _exactly one of the two uploads_.

Which upload succeeds is undefined. Users must not expect consistent
behavior across filesystems, across filesystem instances *or even
across different requests.

If a multipart upload is completed or aborted while a part upload is in progress,
the in-progress upload, if it has not completed, must not be included in
the final file, in whole or in part. Implementations SHOULD raise an error
in the `putPart()` operation.

# Serialization Compatibility

Users MUST NOT expect that serialized PathHandle versions are compatible across
* different multipart uploader implementations.
* different versions of the same implementation.

That is: all clients MUST use the exact same version of Hadoop.

## Model

A FileSystem/FileContext which supports Multipart Uploads extends the existing model
`(Directories, Files, Symlinks)` to one of `(Directories, Files, Symlinks, Uploads)`
`Uploads` of type `Map[UploadHandle -> Map[PartHandle -> UploadPart]`.


The `Uploads` element of the state tuple is a map of all active uploads.

```python
Uploads: Map[UploadHandle -> Map[PartHandle -> UploadPart]`
```

An UploadHandle is a non-empty list of bytes.
```python
UploadHandle: List[byte]
len(UploadHandle) > 0
```

Clients *MUST* treat this as opaque. What is core to this features design is that the handle is valid from
across clients: the handle may be serialized on host `hostA`, deserialized on `hostB` and still used
to extend or complete the upload.

```python
UploadPart = (Path: path, parts: Map[PartHandle -> byte[]])
```

Similarly, the `PartHandle` type is also a non-empty list of opaque bytes, again, marshallable between hosts.

```python
PartHandle: List[byte]
```

It is implicit that each `UploadHandle` in `FS.Uploads` is unique.
Similarly, each `PartHandle` in the map of `[PartHandle -> UploadPart]` must also be unique.

1. There is no requirement that Part Handles are unique across uploads.
1. There is no requirement that Upload Handles are unique over time.
However, if Part Handles are rapidly recycled, there is a risk that the nominally
idempotent operation `abort(FS, uploadHandle)` could unintentionally cancel a
successor operation which used the same Upload Handle.

## Asynchronous API

All operations return `CompletableFuture<>` types which must be
subsequently evaluated to get their return values.

1. The execution of the operation MAY be a blocking operation in on the call thread.
1. If not, it SHALL be executed in a separate thread and MUST complete by the time the
future evaluation returns.
1. Some/All preconditions MAY be evaluated at the time of initial invocation,
1. All those which are not evaluated at that time, MUST Be evaluated during the execution
of the future.


What this means is that when an implementation interacts with a fast file system/store all preconditions
including the existence of files MAY be evaluated early, whereas and implementation interacting with a
remote object store whose probes are slow MAY verify preconditions in the asynchronous phase -especially
those which interact with the remote store.

Java CompletableFutures do not work well with checked exceptions. The Hadoop codease is still evolving the
details of the exception handling here, as more use is made of the asynchronous APIs. Assume that any
precondition failure which declares that an `IOException` MUST be raised may have that operation wrapped in a
`RuntimeException` of some form if evaluated in the future; this also holds for any other `IOException`
raised during the operations.

### `close()`

Applications MUST call `close()` after using an uploader; this is so it may release other
objects, update statistics, etc.

## State Changing Operations

### `CompletableFuture<UploadHandle> startUpload(Path)`

Starts a Multipart Upload, ultimately returning an `UploadHandle` for use in
subsequent operations.

#### Preconditions

```python
if path == "/" : raise IOException

if exists(FS, path) and not isFile(FS, path) raise PathIsDirectoryException, IOException
```

If a filesystem does not support concurrent uploads to a destination,
then the following precondition is added:

```python
if path in values(FS.Uploads) raise PathExistsException, IOException
```

#### Postconditions

Once the initialization operation completes, the filesystem state is updated with a new
active upload, with a new handle, this handle being returned to the caller.

```python
handle' = UploadHandle where not handle' in keys(FS.Uploads)
FS' = FS where FS'.Uploads(handle') == {}
result = handle'
```

### `CompletableFuture<PartHandle> putPart(UploadHandle uploadHandle, int partNumber, Path filePath, InputStream inputStream, long lengthInBytes)`

Upload a part for the specific multipart upload, eventually being returned an opaque part handle
represting this part of the specified upload.

#### Preconditions


```python
uploadHandle in keys(FS.Uploads)
partNumber >= 1
lengthInBytes >= 0
len(inputStream) >= lengthInBytes
```

#### Postconditions

```python
data' = inputStream(0..lengthInBytes)
partHandle' = byte[] where not partHandle' in keys(FS.uploads(uploadHandle).parts)
FS' = FS where FS'.uploads(uploadHandle).parts(partHandle') == data'
result = partHandle'
```

The data is stored in the filesystem, pending completion. It MUST NOT be visible at the destination path.
It MAY be visible in a temporary path somewhere in the file system;
This is implementation-specific and MUST NOT be relied upon.


### ` CompletableFuture<PathHandle> complete(UploadHandle uploadId, Path filePath, Map<Integer, PartHandle> handles)`

Complete the multipart upload.

A Filesystem may enforce a minimum size of each part, excluding the last part uploaded.

If a part is out of this range, an `IOException` MUST be raised.

#### Preconditions

```python
uploadHandle in keys(FS.Uploads) else raise FileNotFoundException
FS.Uploads(uploadHandle).path == path
if exists(FS, path) and not isFile(FS, path) raise PathIsDirectoryException, IOException
parts.size() > 0
forall k in keys(parts): k > 0
forall k in keys(parts):
  not exists(k2 in keys(parts)) where (parts[k] == parts[k2])
```

All keys MUST be greater than zero, and there MUST not be any duplicate
references to the same parthandle.
These validations MAY be performed at any point during the operation.
After a failure, there is no guarantee that a `complete()` call for this
upload with a valid map of paths will complete.
Callers SHOULD invoke `abort()` after any such failure to ensure cleanup.

if `putPart()` operations For this `uploadHandle` were performed But whose
`PathHandle` Handles were not included in this request -the omitted
parts SHALL NOT be a part of the resulting file.

The MultipartUploader MUST clean up any such outstanding entries.

In the case of backing stores that support directories (local filesystem, HDFS,
etc), if, at the point of completion, there is now a directory at the
destination then a `PathIsDirectoryException` or other `IOException` must be thrown.

#### Postconditions

```python
UploadData' == ordered concatention of all data in the map of parts, ordered by key
exists(FS', path') and result = PathHandle(path')
FS' = FS where FS.Files(path) == UploadData' and not uploadHandle in keys(FS'.uploads)
```

The `PathHandle` is returned by the complete operation so subsequent operations
will be able to identify that the data has not changed in the meantime.

The order of parts in the uploaded by file is that of the natural order of
parts in the map: part 1 is ahead of part 2, etc.


### `CompletableFuture<Void> abort(UploadHandle uploadId, Path filePath)`

Abort a multipart upload. The handle becomes invalid and not subject to reuse.

#### Preconditions


```python
uploadHandle in keys(FS.Uploads) else raise FileNotFoundException
```

#### Postconditions

The upload handle is no longer known.

```python
FS' = FS where not uploadHandle in keys(FS'.uploads)
```
A subsequent call to `abort()` with the same handle will fail, unless
the handle has been recycled.

### `CompletableFuture<Integer> abortUploadsUnderPath(Path path)`

Perform a best-effort cleanup of all uploads under a path.

returns a future which resolves to.

    -1 if unsuppported
    >= 0 if supported

Because it is best effort a strict postcondition isn't possible.
The ideal postcondition is all uploads under the path are aborted,
and the count is the number of uploads aborted:

```python
FS'.uploads forall upload in FS.uploads:
    not isDescendant(FS, path, upload.path)
return len(forall upload in FS.uploads:
               isDescendant(FS, path, upload.path))
```
