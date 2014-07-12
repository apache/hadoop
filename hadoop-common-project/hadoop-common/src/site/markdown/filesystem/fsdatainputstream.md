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


# Class `FSDataInputStream extends DataInputStream`

The core behavior of `FSDataInputStream` is defined by `java.io.DataInputStream`,
with extensions that add key assumptions to the system.

1. The source is a local or remote filesystem.
1. The stream being read references a finite array of bytes.
1. The length of the data does not change during the read process.
1. The contents of the data does not change during the process.
1. The source file remains present during the read process
1. Callers may use `Seekable.seek()` to offsets within the array of bytes, with future
reads starting at this offset.
1. The cost of forward and backward seeks is low.
1. There is no requirement for the stream implementation to be thread-safe.
 Callers MUST assume that instances are not thread-safe.


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


### `Seekable.getPos()`

Return the current position. The outcome when a stream is closed is undefined.

#### Preconditions

    isOpen(FSDIS)

#### Postconditions

    result = pos(FSDIS)


### `InputStream.read()`

Return the data at the current position.

1. Implementations should fail when a stream is closed
1. There is no limit on how long `read()` may take to complete.

#### Preconditions

    isOpen(FSDIS)

#### Postconditions

    if ( pos < len(data) ):
       FSDIS' = (pos + 1, data, True)
       result = data[pos]
    else
        result = -1


### `InputStream.read(buffer[], offset, length)`

Read `length` bytes of data into the destination buffer, starting at offset
`offset`

#### Preconditions

    isOpen(FSDIS)
    buffer != null else raise NullPointerException
    length >= 0
    offset < len(buffer)
    length <= len(buffer) - offset

Exceptions that may be raised on precondition failure are

    InvalidArgumentException
    ArrayIndexOutOfBoundsException
    RuntimeException

#### Postconditions

    if length == 0 :
      result = 0

    elseif pos > len(data):
      result -1

    else
      let l = min(length, len(data)-length) :
          buffer' = buffer where forall i in [0..l-1]:
              buffer'[o+i] = data[pos+i]
          FSDIS' = (pos+l, data, true)
          result = l

### `Seekable.seek(s)`


#### Preconditions

Not all subclasses implement the Seek operation:

    supported(FSDIS, Seekable.seek) else raise [UnsupportedOperationException, IOException]

If the operation is supported, the file SHOULD be open:

    isOpen(FSDIS)

Some filesystems do not perform this check, relying on the `read()` contract
to reject reads on a closed stream (e.g. `RawLocalFileSystem`).

A `seek(0)` MUST always succeed, as  the seek position must be
positive and less than the length of the Stream's:

    s > 0 and ((s==0) or ((s < len(data)))) else raise [EOFException, IOException]

Some FileSystems do not raise an exception if this condition is not met. They
instead return -1 on any `read()` operation where, at the time of the read,
`len(data(FSDIS)) < pos(FSDIS)`.

#### Postconditions

    FSDIS' = (s, data, True)

There is an implicit invariant: a seek to the current position is a no-op

    seek(getPos())

Implementations may recognise this operation and bypass all other precondition
checks, leaving the input stream unchanged.


### `Seekable.seekToNewSource(offset)`

This operation instructs the source to retrieve `data[]` from a different
source from the current source. This is only relevant if the filesystem supports
multiple replicas of a file and there is more than 1 replica of the
data at offset `offset`.


#### Preconditions

Not all subclasses implement the operation operation, and instead
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
sourced from a different replica.

If there is no other copy, `FSDIS` is  not updated; the response indicates this:

        result = False

Outside of test methods, the primary use of this method is in the {{FSInputChecker}}
class, which can react to a checksum error in a read by attempting to source
the data elsewhere. It a new source can be found it attempts to reread and
recheck that portion of the file.

## interface `PositionedReadable`

The `PositionedReadable` operations provide the ability to
read data into a buffer from a specific position in
the data stream.

Although the interface declares that it must be thread safe,
some of the implementations do not follow this guarantee.

#### Implementation preconditions

Not all `FSDataInputStream` implementations support these operations. Those that do
not implement `Seekable.seek()` do not implement the `PositionedReadable`
interface.

    supported(FSDIS, Seekable.seek) else raise [UnsupportedOperationException, IOException]

This could be considered obvious: if a stream is not Seekable, a client
cannot seek to a location. It is also a side effect of the
base class implementation, which uses `Seekable.seek()`.


**Implicit invariant**: for all `PositionedReadable` operations, the value
of `pos` is unchanged at the end of the operation

    pos(FSDIS') == pos(FSDIS)


There are no guarantees that this holds *during* the operation.


#### Failure states

For any operations that fail, the contents of the destination
`buffer` are undefined. Implementations may overwrite part
or all of the buffer before reporting a failure.



### `int PositionedReadable.read(position, buffer, offset, length)`

#### Preconditions

    position > 0 else raise [IllegalArgumentException, RuntimeException]
    len(buffer) + offset < len(data) else raise [IndexOutOfBoundException, RuntimeException]
    length >= 0
    offset >= 0

#### Postconditions

The amount of data read is the less of the length or the amount
of data available from the specified position:

    let available = min(length, len(data)-position)
    buffer'[offset..(offset+available-1)] = data[position..position+available -1]
    result = available


### `void PositionedReadable.readFully(position, buffer, offset, length)`

#### Preconditions

    position > 0 else raise [IllegalArgumentException, RuntimeException]
    length >= 0
    offset >= 0
    (position + length) <= len(data) else raise [EOFException, IOException]
    len(buffer) + offset < len(data)

#### Postconditions

The amount of data read is the less of the length or the amount
of data available from the specified position:

    let available = min(length, len(data)-position)
    buffer'[offset..(offset+length-1)] = data[position..(position + length -1)]

### `PositionedReadable.readFully(position, buffer)`

The semantics of this are exactly equivalent to

    readFully(position, buffer, 0, len(buffer))


## Consistency

* All readers, local and remote, of a data stream FSDIS provided from a `FileSystem.open(p)`
are expected to receive access to the data of `FS.Files[p]` at the time of opening.
* If the underlying data is changed during the read process, these changes MAY or
MAY NOT be visible.
* Such changes are visible MAY be partially visible.


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
