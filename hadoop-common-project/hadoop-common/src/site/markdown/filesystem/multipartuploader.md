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
<!--  CLASS: MultipartUploader -->
<!--  ============================================================= -->

# class `org.apache.hadoop.fs.MultipartUploader`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

The abstract `MultipartUploader` class is the original class to upload a file
using multiple parts to Hadoop-supported filesystems. The benefits of a
multipart upload is that the file can be uploaded from multiple clients or
processes in parallel and the results will not be visible to other clients until
the `complete` function is called.

When implemented by an object store, uploaded data may incur storage charges,
even before it is visible in the filesystems. Users of this API must be diligent
and always perform best-effort attempts to complete or abort the upload.

## Invariants

All the requirements of a valid MultipartUploader are considered implicit
econditions and postconditions:
all operations on a valid MultipartUploader MUST result in a new
MultipartUploader that is also valid.

The operations of a single multipart upload may take place across different
instance of a multipart uploader, across different processes and hosts.
It is therefore a requirement that:

1. All state needed to upload a part, complete an upload or abort an upload
must be contained within or retrievable from an upload handle.

1. If an upload handle is marshalled to another process, then, if the
receiving process has the correct permissions, it may participate in the
upload, by uploading one or more parts, by completing an upload, and/or by
aborting the upload.

## Concurrency

Multiple processes may upload parts of a multipart upload simultaneously.

If a call is made to `initialize(path)` to a destination where an active
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

## Model

A File System which supports Multipart Uploads extends the existing model
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

## State Changing Operations

### `UploadHandle initialize(Path path)`

Initialized a Multipart Upload, returning an upload handle for use in
subsequent operations.

#### Preconditions

```python
if path == "/" : raise IOException

if exists(FS, path) and not isFile(FS, path) raise PathIsDirectoryException, IOException
```

If a filesystem does not support concurrent uploads to a destination,
then the following precondition is added

```python
if path in values(FS.Uploads) raise PathExistsException, IOException

```


#### Postconditions

The outcome of this operation is that the filesystem state is updated with a new
active upload, with a new handle, this handle being returned to the caller.

```python
handle' = UploadHandle where not handle' in keys(FS.Uploads)
FS' = FS where FS'.Uploads(handle') == {}
result = handle'
```

### `PartHandle putPart(Path path, InputStream inputStream, int partNumber, UploadHandle uploadHandle, long lengthInBytes)`

Upload a part for the multipart upload.

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

The data is stored in the filesystem, pending completion.


### `PathHandle complete(Path path, Map<Integer, PartHandle> parts, UploadHandle multipartUploadId)`

Complete the multipart upload.

A Filesystem may enforce a minimum size of each part, excluding the last part uploaded.

If a part is out of this range, an `IOException` MUST be raised.

#### Preconditions

```python
uploadHandle in keys(FS.Uploads) else raise FileNotFoundException
FS.Uploads(uploadHandle).path == path
if exists(FS, path) and not isFile(FS, path) raise PathIsDirectoryException, IOException
parts.size() > 0
```

If there are handles in the MPU which aren't included in the map, then the omitted
parts will not be a part of the resulting file. It is up to the implementation
of the MultipartUploader to make sure the leftover parts are cleaned up.

In the case of backing stores that support directories (local filesystem, HDFS,
etc), if, at the point of completion, there is now a directory at the
destination then a `PathIsDirectoryException` or other `IOException` must be thrown.

#### Postconditions

```python
UploadData' == ordered concatention of all data in the map of parts, ordered by key
exists(FS', path') and result = PathHandle(path')
FS' = FS where FS.Files(path) == UploadData' and not uploadHandle in keys(FS'.uploads)
```

The PathHandle is returned by the complete operation so subsequent operations
will be able to identify that the data has not changed in the meantime.

The order of parts in the uploaded by file is that of the natural order of
parts: part 1 is ahead of part 2, etc.


### `void abort(Path path, UploadHandle multipartUploadId)`

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
