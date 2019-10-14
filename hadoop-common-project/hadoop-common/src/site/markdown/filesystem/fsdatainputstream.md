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
    buffer != null else raise NullPointerException
    length >= 0
    offset < len(buffer)
    length <= len(buffer) - offset
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
from position `position`


## Consistency

* All readers, local and remote, of a data stream FSDIS provided from a `FileSystem.open(p)`
are expected to receive access to the data of `FS.Files[p]` at the time of opening.
* If the underlying data is changed during the read process, these changes MAY or
MAY NOT be visible.
* Such changes that are visible MAY be partially visible.


At time t0

    FSDIS0 = FS'read(p) = (0, data0[])

At time t1

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
