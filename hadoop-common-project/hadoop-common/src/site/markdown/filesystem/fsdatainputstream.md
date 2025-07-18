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
<!--  CLASS: FSDataInputStream -->
<!--  ============================================================= -->

# class `org.apache.hadoop.fs.FSDataInputStream`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

## Class `FSDataInputStream extends DataInputStream`

The core behavior of `FSDataInputStream` is defined by `java.io.DataInputStream`,
with extensions that add key assumptions to the system.

1. The source is a local or remote filesystem.
1. The stream being read references a finite array of bytes.
1. The length of the data does not change during the read process.
1. The contents of the data does not change during the process.
1. The source file remains present during the read process.
1. Callers may use `Seekable.seek()` to offsets within the array of bytes, with future
reads starting at this offset.
1. The cost of forward and backward seeks is low.
1. There is no requirement for the stream implementation to be thread-safe.
1. BUT, if a stream implements [PositionedReadable](#PositionedReadable),
 "positioned reads" MUST be thread-safe.

Files are opened via `FileSystem.open(p)`, which, if successful, returns:

    result = FSDataInputStream(0, FS.Files[p])

The stream can be modeled as:

    FSDIS = (pos, data[], isOpen)

with access functions:

    pos(FSDIS)
    data(FSDIS)
    isOpen(FSDIS)

**Implicit invariant**: the size of the data stream equals the size of the
file as returned by `FileSystem.getFileStatus(Path p)`

    forall p in dom(FS.Files[p]) :
        len(data(FSDIS)) == FS.getFileStatus(p).length


### `Closeable.close()`

The semantics of `java.io.Closeable` are defined in the interface definition
within the JRE.

The operation MUST be idempotent; the following sequence is not an error:

    FSDIS.close();
    FSDIS.close();

#### Implementation Notes

* Implementations SHOULD be robust against failure. If an inner stream
is closed, it should be checked for being `null` first.

* Implementations SHOULD NOT raise `IOException` exceptions (or any other exception)
during this operation. Client applications often ignore these, or may fail
unexpectedly.





#### Postconditions


    FSDIS' = ((undefined), (undefined), False)


### <a name="Seekable.getPos"></a>`Seekable.getPos()`

Return the current position. The outcome when a stream is closed is undefined.

#### Preconditions

    isOpen(FSDIS)

#### Postconditions

    result = pos(FSDIS)


### <a name="InputStream.read"></a> `InputStream.read()`

Return the data at the current position.

1. Implementations should fail when a stream is closed.
1. There is no limit on how long `read()` may take to complete.

#### Preconditions

    isOpen(FSDIS)

#### Postconditions

    if ( pos < len(data) ):
       FSDIS' = (pos + 1, data, True)
       result = data[pos]
    else
        result = -1


### <a name="InputStream.read.buffer[]"></a> `InputStream.read(buffer[], offset, length)`

Read `length` bytes of data into the destination buffer, starting at offset
`offset`. The source of the data is the current position of the stream,
as implicitly set in `pos`.

#### Preconditions

    isOpen(FSDIS)
    buffer != null else raise NullPointerException, IllegalArgumentException
    offset >= 0 else raise IndexOutOfBoundsException
    length >= 0 else raise IndexOutOfBoundsException, IllegalArgumentException
    offset < len(buffer) else raise IndexOutOfBoundsException
    length <= len(buffer) - offset else raise IndexOutOfBoundsException
    pos >= 0 else raise EOFException, IOException

Exceptions that may be raised on precondition failure are

    InvalidArgumentException
    ArrayIndexOutOfBoundsException
    RuntimeException

Not all filesystems check the `isOpen` state.

#### Postconditions

    if length == 0 :
      result = 0

    else if pos > len(data):
      result = -1

    else
      let l = min(length, len(data)-length) :
        buffer' = buffer where forall i in [0..l-1]:
           buffer'[o+i] = data[pos+i]
        FSDIS' = (pos+l, data, true)
        result = l

The `java.io` API states that if the amount of data to be read (i.e. `length`)
then the call must block until the amount of data available is greater than
zero —that is, until there is some data. The call is not required to return
when the buffer is full, or indeed block until there is no data left in
the stream.

That is, rather than `l` being simply defined as `min(length, len(data)-length)`,
it strictly is an integer in the range `1..min(length, len(data)-length)`.
While the caller may expect as much of the buffer as possible to be filled
in, it is within the specification for an implementation to always return
a smaller number, perhaps only ever 1 byte.

What is critical is that unless the destination buffer size is 0, the call
must block until at least one byte is returned. Thus, for any data source
of length greater than zero, repeated invocations of this `read()` operation
will eventually read all the data.

#### Implementation Notes

1. If the caller passes a `null` buffer, then an unchecked exception MUST be thrown. The base JDK
`InputStream` implementation throws `NullPointerException`. HDFS historically used
`IllegalArgumentException`. Implementations MAY use either of these.
1. If the caller passes a negative value for `length`, then an unchecked exception MUST be thrown.
The base JDK `InputStream` implementation throws `IndexOutOfBoundsException`. HDFS historically used
`IllegalArgumentException`. Implementations MAY use either of these.
1. Reads through any method MUST return the same data.
1. Callers MAY interleave calls to different read methods (single-byte and multi-byte) on the same
stream. The stream MUST return the same underlying data, regardless of the specific read calls or
their ordering.

### <a name="Seekable.seek"></a>`Seekable.seek(s)`


#### Preconditions

Not all subclasses implement the Seek operation:

    supported(FSDIS, Seekable.seek) else raise [UnsupportedOperationException, IOException]

If the operation is supported, the file SHOULD be open:

    isOpen(FSDIS)

Some filesystems do not perform this check, relying on the `read()` contract
to reject reads on a closed stream (e.g. `RawLocalFileSystem`).

A `seek(0)` MUST always succeed, as  the seek position must be
positive and less than the length of the Stream:

    s > 0 and ((s==0) or ((s < len(data)))) else raise [EOFException, IOException]

Some FileSystems do not raise an exception if this condition is not met. They
instead return -1 on any `read()` operation where, at the time of the read,
`len(data(FSDIS)) < pos(FSDIS)`.

After a failed seek, the value of `pos(FSDIS)` may change.
As an example, seeking past the EOF may move the read position
to the end of the file, *as well as raising an `EOFException`.*

#### Postconditions

    FSDIS' = (s, data, True)

There is an implicit invariant: a seek to the current position is a no-op

    seek(getPos())

Implementations may recognise this operation and bypass all other precondition
checks, leaving the input stream unchanged.

The most recent connectors to object stores all implement some form
of "lazy-seek": the `seek()` call may appear to update the stream, and the value
of `getPos()` is updated, but the file is not opened/reopenend until
data is actually read. Implementations of lazy seek MUST still validate
the new seek position against the known length of the file.
However the state of the file (i.e. does it exist, what
its current length is) does not need to be refreshed at this point.
The fact that a file has been deleted or truncated may not surface until
that `read()` call.


### `Seekable.seekToNewSource(offset)`

This operation instructs the source to retrieve `data[]` from a different
source from the current source. This is only relevant if the filesystem supports
multiple replicas of a file and there is more than 1 replica of the
data at offset `offset`.


#### Preconditions

Not all subclasses implement this operation, and instead
either raise an exception or return `False`.

    supported(FSDIS, Seekable.seekToNewSource) else raise [UnsupportedOperationException, IOException]

Examples: `CompressionInputStream` , `HttpFSFileSystem`

If supported, the file must be open:

    isOpen(FSDIS)

#### Postconditions

The majority of subclasses that do not implement this operation simply
fail.

    if not supported(FSDIS, Seekable.seekToNewSource(s)):
        result = False

Examples: `RawLocalFileSystem` , `HttpFSFileSystem`

If the operation is supported and there is a new location for the data:

    FSDIS' = (pos, data', true)
    result = True

The new data is the original data (or an updated version of it, as covered
in the Consistency section below), but the block containing the data at `offset`
is sourced from a different replica.

If there is no other copy, `FSDIS` is  not updated; the response indicates this:

    result = False

Outside of test methods, the primary use of this method is in the {{FSInputChecker}}
class, which can react to a checksum error in a read by attempting to source
the data elsewhere. If a new source can be found it attempts to reread and
recheck that portion of the file.

### `CanUnbuffer.unbuffer()`

This operation instructs the source to release any system resources they are
currently holding on to, such as buffers, sockets, file descriptors, etc. Any
subsequent IO operation will likely have to reacquire these resources.
Unbuffering is useful in situation where streams need to remain open, but no IO
operation is expected from the stream in the immediate future (examples include
file handle cacheing).

#### Preconditions

Not all subclasses implement this operation. In addition to implementing
`CanUnbuffer`. Subclasses must implement the `StreamCapabilities` interface and
`StreamCapabilities.hasCapability(UNBUFFER)` must return true. If a subclass
implements `CanUnbuffer` but does not report the functionality via
`StreamCapabilities` then the call to `unbuffer` does nothing. If a subclass
reports that it does implement `UNBUFFER`, but does not implement the
`CanUnbuffer` interface, an `UnsupportedOperationException` is thrown.

    supported(FSDIS, StreamCapabilities.hasCapability && FSDIS.hasCapability(UNBUFFER) && CanUnbuffer.unbuffer)

This method is not thread-safe. If `unbuffer` is called while a `read` is in
progress, the outcome is undefined.

`unbuffer` can be called on a closed file, in which case `unbuffer` will do
nothing.

#### Postconditions

The majority of subclasses that do not implement this operation simply
do nothing.

If the operation is supported, `unbuffer` releases any and all system resources
associated with the stream. The exact list of what these resources are is
generally implementation dependent, however, in general, it may include
buffers, sockets, file descriptors, etc.

## <a name="PositionedReadable"></a> interface `PositionedReadable`

The `PositionedReadable` operations supply "positioned reads" ("pread").
They provide the ability to read data into a buffer from a specific
position in the data stream. Positioned reads equate to a
[`Seekable.seek`](#Seekable.seek) at a particular offset followed by a
[`InputStream.read(buffer[], offset, length)`](#InputStream.read.buffer[]),
only there is a single method invocation, rather than `seek` then
`read`, and two positioned reads can *optionally* run concurrently
over a single instance of a `FSDataInputStream` stream.

The interface declares positioned reads thread-safe (some of the
implementations do not follow this guarantee).

Any positional read run concurrent with a stream operation &mdash; e.g.
[`Seekable.seek`](#Seekable.seek), [`Seekable.getPos()`](#Seekable.getPos),
and [`InputStream.read()`](#InputStream.read) &mdash; MUST run in
isolation; there must not be  mutual interference.

Concurrent positional reads and stream operations MUST be serializable;
one may block the other so they run in series but, for better throughput
and 'liveness', they SHOULD run concurrently.

Given two parallel positional reads, one at `pos1` for `len1` into buffer
`dest1`, and another at `pos2` for `len2` into buffer `dest2`, AND given
a concurrent, stream read run after a seek to `pos3`, the resultant
buffers MUST be filled as follows, even if the reads happen to overlap
on the underlying stream:

    // Positioned read #1
    read(pos1, dest1, ... len1) -> dest1[0..len1 - 1] =
      [data(FS, path, pos1), data(FS, path, pos1 + 1) ... data(FS, path, pos1 + len1 - 1]

    // Positioned read #2
    read(pos2, dest2, ... len2) -> dest2[0..len2 - 1] =
      [data(FS, path, pos2), data(FS, path, pos2 + 1) ... data(FS, path, pos2 + len2 - 1]

    // Stream read
    seek(pos3);
    read(dest3, ... len3) -> dest3[0..len3 - 1] =
      [data(FS, path, pos3), data(FS, path, pos3 + 1) ... data(FS, path, pos3 + len3 - 1]

Note that implementations are not required to be atomic; the intermediate state
of the operation (the change in the value of `getPos()`) may be visible.

### Implementation preconditions

Not all `FSDataInputStream` implementations support these operations. Those that do
not implement `Seekable.seek()` do not implement the `PositionedReadable`
interface.

    supported(FSDIS, Seekable.seek) else raise [UnsupportedOperationException, IOException]

This could be considered obvious: if a stream is not `Seekable`, a client
cannot seek to a location. It is also a side effect of the
base class implementation, which uses `Seekable.seek()`.


**Implicit invariant**: for all `PositionedReadable` operations, the value
of `pos` is unchanged at the end of the operation

    pos(FSDIS') == pos(FSDIS)


### Failure states

For any operations that fail, the contents of the destination
`buffer` are undefined. Implementations may overwrite part
or all of the buffer before reporting a failure.

### `int PositionedReadable.read(position, buffer, offset, length)`

Read as much data as possible into the buffer space allocated for it.

#### Preconditions

    position >= 0 else raise [EOFException, IOException, IllegalArgumentException, RuntimeException]
    len(buffer) - offset >= length else raise [IndexOutOfBoundException, RuntimeException]
    length >= 0
    offset >= 0

#### Postconditions

The amount of data read is the less of the length or the amount
of data available from the specified position:

    let available = min(length, len(data)-position)
    buffer'[offset..(offset+available-1)] = data[position..position+available -1]
    result = available

1. A return value of -1 means that the stream had no more available data.
1. An invocation with `length==0` implicitly does not read any data;
implementations may short-cut the operation and omit any IO. In such instances,
checks for the stream being at the end of the file may be omitted.
1. If an IO exception occurs during the read operation(s),
the final state of `buffer` is undefined.

### `void PositionedReadable.readFully(position, buffer, offset, length)`

Read exactly `length` bytes of data into the buffer, failing if there is not
enough data available.

#### Preconditions

    position >= 0 else raise [EOFException, IOException, IllegalArgumentException, RuntimeException]
    length >= 0
    offset >= 0
    len(buffer) - offset >= length else raise [IndexOutOfBoundException, RuntimeException]
    (position + length) <= len(data) else raise [EOFException, IOException]

If an IO exception occurs during the read operation(s),
the final state of `buffer` is undefined.

If there is not enough data in the input stream to satisfy the requests,
the final state of `buffer` is undefined.

#### Postconditions

The buffer from offset `offset` is filled with the data starting at `position`

    buffer'[offset..(offset+length-1)] = data[position..(position + length -1)]

### `PositionedReadable.readFully(position, buffer)`

The semantics of this are exactly equivalent to

    readFully(position, buffer, 0, len(buffer))

That is, the buffer is filled entirely with the contents of the input source
from position `position`.

### `void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)`

Read fully data for a list of ranges asynchronously. The default implementation
iterates through the ranges, tries to coalesce the ranges based on values of
`minSeekForVectorReads` and `maxReadSizeForVectorReads` and then read each merged
ranges synchronously, but the intent is sub classes can implement efficient
implementation. Reading in both direct and heap byte buffers are supported.
Also, clients are encouraged to use `WeakReferencedElasticByteBufferPool` for
allocating buffers such that even direct buffers are garbage collected when
they are no longer referenced.

The position returned by `getPos()` after `readVectored()` is undefined.

If a file is changed while the `readVectored()` operation is in progress, the output is
undefined. Some ranges may have old data, some may have new, and some may have both.

While a `readVectored()` operation is in progress, normal read API calls MAY block;
the value of `getPos(`) is also undefined. Applications SHOULD NOT make such requests
while waiting for the results of a vectored read.

Note: Don't use direct buffers for reading from `ChecksumFileSystem` as that may
lead to memory fragmentation explained in
[HADOOP-18296](https://issues.apache.org/jira/browse/HADOOP-18296)
_Memory fragmentation in ChecksumFileSystem Vectored IO implementation_

#### Preconditions

No empty lists.

```python
if ranges = null raise NullPointerException
if allocate = null raise NullPointerException
```

For each requested range `range[i]` in the list of ranges `range[0..n]` sorted
on `getOffset()` ascending such that

for all `i where i > 0`:

    range[i].getOffset() > range[i-1].getOffset()

For all ranges `0..i` the preconditions are:

```python
ranges[i] != null else raise IllegalArgumentException
ranges[i].getOffset() >= 0 else raise EOFException
ranges[i].getLength() >= 0 else raise IllegalArgumentException
if i > 0 and ranges[i].getOffset() < (ranges[i-1].getOffset() + ranges[i-1].getLength) :
   raise IllegalArgumentException
```
If the length of the file is known during the validation phase:

```python
if range[i].getOffset + range[i].getLength >= data.length() raise EOFException
```

#### Postconditions

For each requested range `range[i]` in the list of ranges `range[0..n]`

```
ranges[i]'.getData() = CompletableFuture<buffer: ByteBuffer>
```

 and when `getData().get()` completes:
```
let buffer = `getData().get()
let len = ranges[i].getLength()
let data = new byte[len]
(buffer.position() - buffer.limit) = len
buffer.get(data, 0, len) = readFully(ranges[i].getOffset(), data, 0, len)
```

That is: the result of every ranged read is the result of the (possibly asynchronous)
call to `PositionedReadable.readFully()` for the same offset and length

#### `minSeekForVectorReads()`

The smallest reasonable seek. Two ranges won't be merged together if the difference between
end of first and start of next range is more than this value.

#### `maxReadSizeForVectorReads()`

Maximum number of bytes which can be read in one go after merging the ranges.
Two ranges won't be merged if the combined data to be read.
Essentially setting this to 0 will disable the merging of ranges.

#### Concurrency

* When calling `readVectored()` while a separate thread is trying
  to read data through `read()`/`readFully()`, all operations MUST
  complete successfully.
* Invoking a vector read while an existing set of pending vector reads
  are in progress MUST be supported. The order of which ranges across
  the multiple requests complete is undefined.
* Invoking `read()`/`readFully()` while a vector API call is in progress
  MUST be supported. The order of which calls return data is undefined.

The S3A connector closes any open stream when its `synchronized readVectored()`
method is invoked;
It will then switch the read policy from normal to random
so that any future invocations will be for limited ranges.
This is because the expectation is that vector IO and large sequential
reads are not mixed and that holding on to any open HTTP connection is wasteful.

#### Handling of zero-length ranges

Implementations MAY short-circuit reads for any range where `range.getLength() = 0`
and return an empty buffer.

In such circumstances, other validation checks MAY be omitted.

There are no guarantees that such optimizations take place; callers SHOULD NOT
include empty ranges for this reason.

#### Consistency

* All readers, local and remote, of a data stream `FSDIS` provided from a `FileSystem.open(p)`
are expected to receive access to the data of `FS.Files[p]` at the time of opening.
* If the underlying data is changed during the read process, these changes MAY or
MAY NOT be visible.
* Such changes that are visible MAY be partially visible.

At time `t0`

    FSDIS0 = FS'read(p) = (0, data0[])

At time `t1`

    FS' = FS' where FS'.Files[p] = data1

From time `t >= t1`, the value of `FSDIS0` is undefined.

It may be unchanged

    FSDIS0.data == data0

    forall l in len(FSDIS0.data):
      FSDIS0.read() == data0[l]


It may pick up the new data

    FSDIS0.data == data1

    forall l in len(FSDIS0.data):
      FSDIS0.read() == data1[l]

It may be inconsistent, such that a read of an offset returns
data from either of the datasets

    forall l in len(FSDIS0.data):
      (FSDIS0.read(l) == data0[l]) or (FSDIS0.read(l) == data1[l]))

That is, every value read may be from the original or updated file.

It may also be inconsistent on repeated reads of same offset, that is
at time `t2 > t1`:

    r2 = FSDIS0.read(l)

While at time `t3 > t2`:

    r3 = FSDIS0.read(l)

It may be that `r3 != r2`. (That is, some of the data my be cached or replicated,
and on a subsequent read, a different version of the file's contents are returned).

Similarly, if the data at the path `p`, is deleted, this change MAY or MAY
not be visible during read operations performed on `FSDIS0`.

#### API Stabilization Notes

The `readVectored()` API was shipped in Hadoop 3.3.5, with explicit local, raw local and S3A
support -and fallback everywhere else.

*Overlapping ranges*

The restriction "no overlapping ranges" was only initially enforced in
the S3A connector, which would raise `UnsupportedOperationException`.
Adding the range check as a precondition for all implementations (Raw Local
being an exception) guarantees consistent behavior everywhere.
The reason Raw Local doesn't have this precondition is ChecksumFileSystem
creates the chunked ranges based on the checksum chunk size and then calls
readVectored on Raw Local which may lead to overlapping ranges in some cases.
For details see [HADOOP-19291](https://issues.apache.org/jira/browse/HADOOP-19291)

For reliable use with older hadoop releases with the API: sort the list of ranges
and check for overlaps before calling `readVectored()`.

#### Direct Buffer Reads

Releases without [HADOOP-19101](https://issues.apache.org/jira/browse/HADOOP-19101)
_Vectored Read into off-heap buffer broken in fallback implementation_ can read data
from the wrong offset with the default "fallback" implementation if the buffer allocator
function returns off heap "direct" buffers.

The custom implementations in local filesystem and S3A's non-prefetching stream are safe.

Anyone implementing support for the API, unless confident they only run
against releases with the fixed implementation, SHOULD NOT use the API
if the allocator is direct and the input stream does not explicitly declare
support through an explicit `hasCapability()` probe:

```java
Stream.hasCapability("in:readvectored")
```

#### Buffer Slicing

[HADOOP-18296](https://issues.apache.org/jira/browse/HADOOP-18296),
_Memory fragmentation in ChecksumFileSystem Vectored IO implementation_
highlights that `ChecksumFileSystem` (which the default implementation of `file://`
subclasses), may return buffers which are sliced subsets of buffers allocated
through the `allocate()` function passed in.

This will happen during reads with and without range coalescing.

Checksum verification may be disabled by setting the option
`fs.file.checksum.verify` to false (Hadoop 3.4.2 and later).

```xml
<property>
  <name>fs.file.checksum.verify</name>
  <value>false</value>
</property>
```

(As you would expect, disabling checksum verification means that errors
reading data may not be detected during the read operation.
Use with care in production.)

Filesystem instances which split buffers during vector read operations
MUST declare this by returning `true`
to the path capabilities probe `fs.capability.vectoredio.sliced`,
and for the open stream in its `hasCapability()` method.


The local filesystem will not slice buffers if the checksum file
of `filename + ".crc"` is not found. This is not declared in the
filesystem `hasPathCapability(filename, "fs.capability.vectoredio.sliced")`
call, as no checks for the checksum file are made then.
This cannot be relied on in production, but it may be useful when
testing for buffer recycling with Hadoop releases 3.4.1 and earlier.

*Implementors Notes*

* Don't slice buffers. `ChecksumFileSystem` has to be considered an outlier which
  needs to be addressed in future.
* Always free buffers in error handling code paths.
* When handling errors in coalesced ranges, don't release buffers for any sub-ranges
  which have already completed.

Handling failures in coalesced ranges is complicated. Recent implementations, such as
`org.apache.hadoop.fs.s3a.impl.streams.AnalyticsStream` omit range coalescing,
relying solely on parallel HTTP for performance.


## `void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate, Consumer<ByteBuffer> release)`

This is the extension of `readVectored/2` with an additional `release` consumer operation to release buffers.

The specification and rules of this method are exactly those of the other operation, with
the addition of:

Preconditions
```
if release = null raise NullPointerException
```

* If a read operation fails due to an `IOException` or similar, the implementation of `readVectored()`,
  SHOULD call `release(buffer)` with the buffer created by invoking the `allocate()` function into which
  the data was being read.
* Implementations MUST NOT call `release(buffer)` with any non-null buffer _not_ obtained through `allocate()`.
* Implementations MUST only call `release(buffer)` when a failure has occurred and the future is about to have `Future.completedExceptionally()` invoked.

It is an extension to the original Vector Read API -not all versions of Hadoop with the original `readVectored()` call define it.
If used directly in application code, that application is restricting itself to later versions
of the API.

If used via reflection, if this method is not found, fall back to the original method.

