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
<!--  CLASS: OutputStream -->
<!--  ============================================================= -->


# Class `OutputStream`

The `FileSystem.create()` and `FileSystem.append()` calls return an instance
of a class `FSDataOutputStream`, a subclass of `java.io.OutputStream`.

It wraps an `OutputStream` instance, one which may implement `Streamable`
and `CanSetDropBehind`. This document covers the requirements of such implementations.

A Java `OutputStream` allows applications to write a sequence of bytes to a destination.
In a Hadoop filesystem, that destination is the data under a path in the filesystem.



## Output stream model

An output stream consists of a buffer `buf: List[byte]`


## Model

For this specification, the output stream can be viewed as a cached byte array
alongside the filesystem. After data is flushed, read operations on the filesystem
should be in sync with the data

    Stream = (path, isOpen, cache)

when opening a new file, the initial state of the cache is empty

     Stream = (path, true, [])

When appending to a file, that is the output stream is created using
`FileSystem.append(path, buffersize, progress)`, then the initial state
of the cache is the current contents of the file


    Stream = (path, true, data(FS, path))

After a call to `close()`, the stream is closed for all operations other
than `close()`; they MAY fail with `IOException` or `RuntimeException` instances.

    Stream =  (path, false, [])

The `close()` operation must become a no-op. That is: followon attempts to
persist data after a failure MUST NOT be made. (alternatively: users of the
API MUST NOT expect failing `close()` attemps to succeed if retried) 



Data written to an `FSDataOutputStream` implementation


```java
public class FSDataOutputStream extends DataOutputStream, FilterOutputStream
  implements Syncable, CanSetDropBehind, DataOutput {
 // ...
 }
```

## Durability

1. `OutputStream.write()` MAY persist the data, synchronously or asynchronously
1. `OutputStream.flush()` flushes data to the destination. There
are no strict persistence requirements.
1. `Syncable.hflush()` synchronously sends all local data to the destination
filesystem. After returning to the caller, the data MUST be visible to other readers,
it MAY be durable. That is: it does not have to be persisted, merely guaranteed
to be consistently visible to all clients attempting to open a new stream reading
data at the path.
1. `Syncable.hsync()` MUST flush the data and persist data to the underlying durable
storage.
1. `close()` The first call to `close()` MUST flush out all remaining data in
the buffers, and persist it.


## Concurrency

1. The outcome of more than one process writing to the same file is undefined.

1. An input stream opened to read a file *before the file was opened for writing*
MAY fetch data updated by writes to an OutputStream.
Because of buffering and caching, this is not a requirement
—and if a input stream does pick up updated data, the point at
which the updated data is read is undefined. This surfaces in object stores
where a `seek()` call which closes and re-opens the connection may pick up
updated data, while forward stream reads do not. Similarly, in block-oriented
filesystems, the data may be cached a block at a time —and changes only picked
up when a different block is read.

1. A Filesystem MAY allow the destination path to be manipulated while a stream
is writing to it —for example, `rename()` of the path or a parent; `delete()` of
a path or parent. In such a case, the outcome of future write operations on
the output stream is undefined. Some filesystems MAY implement locking to
prevent conflict. However, this tends to be rare on distributed filesystems,
for reasons well known in the literature.

1. The Java API specification of `java.io.OutputStream` does not require
an instance of the class to be thread safe.
However, `org.apache.hadoop.hdfs.DFSOutputStream`
has a stronger thread safety model (possibly unintentionally). This fact is
relied upon in Apache HBase, as discovered in HADOOP-11708. Implementations
SHOULD be thread safe. *Note*: even the `DFSOutputStream` synchronization
model permits the output stream to `close()`'d while awaiting an acknowledgement
from datanode or namenode writes in an `hsync()` operation.


## Consistency and Visibility

There is no requirement for the data to be immediately visible to other applications
—not until a specific call to flush buffers or persist it to the underlying storage
medium are made.

1. If an output stream is created with `FileSystem.create(path)`, with overwrite=true
and there is an existing file at the path, that is `exists(FS, path)` holds,
then, the existing data is immediately unavailable; the data at the end of the
path MUST consist of an empty byte sequence `[]`, with consistent metadata.
1. The metadata of a file (`length(FS, path)` in particular) SHOULD be consistent
with the contents of the file after `flush()` and `sync()`.
HDFS does not do this except when the write crosses a block boundary; to do
otherwise would overload the Namenode. As a result, while a file is being written
`length(Filesystem, Path)` MAY be less than the length of `data(Filesystem, Path)`.
1. The metadata MUST be consistent with the contents of a file after the `close()`
operation.
1. If the filesystem supports modification timestamps, this
timestamp MAY be updated while a file is being written, especially after a
`Syncable.hsync()` call. The timestamps MUST be updated after the file is closed.
1. After the contents of an output stream have been persisted (`hflush()/hsync()`)
all new `open(FS, Path)` operations MUST return the updated data.
1. After `close()` has been invoked on an output stream returned from a call
to `FileSystem.create(Path,...)`, a call to `getFileStatus(path)` MUST return the
final metadata of the written file, including length and modification time.
The metadata of the file returned in any of the FileSystem `list` operations
MUST be consistent with this metadata.

### HDFS

That HDFS file metadata often lags the content of a file being written
to is not something everyone expects, nor convenient for any program trying
pick up updated data in a file being written. Most visible is the length
of a file returned in the various `list` commands and `getFileStatus` —this
is often out of data.

As HDFS only supports file growth in its output operations, this means
that the size of the file as listed in the metadata may be less than or equal
to the number of available bytes —but never larger. This is a guarantee which
is also held

The standard technique to determine whether a file is updated is:

1. Remember the last read position `pos` in the file, using `0` if this is the initial
read.
1. Use `getFileStatus(FS, Path)` to query the updated length of the file as
recorded in the metadata.
1. If `Status.length &gt pos`, the file has grown.
1. If the number has not changed, then
    1. reopen the file.
    1. `seek(pos)` to that location
    1. If `read() != -1`, there is new data.

This algorithm works for filesystems which are consistent with metadata and
data, as well as HDFS. What is important to know is that even if the 
`getFileStatus(path).getLen()==0` holds, 
in HDFS there may be data, but the metadata is not up to date"

### Local Filesystem, `file:`

`LocalFileSystem`, `file:`, (or any other `FileSystem` implementation based on
`ChecksumFileSystem`) has a different issue. If an output stream
is obtained from `create()` and `FileSystem.setWriteChecksum(false)` has
*not* been called on the filesystem, then the FS only flushes as much
local data as can be written to full checksummed blocks of data.

That is, the flush operations are not guaranteed to write all the pending
data until the file is finally closed.

That is, `sync()` and `hsync()` cannot be guaranteed to persist the data currently
buffered locally.

For anyone thinking "this is a violation of this specification" —they are correct.
The local filesystem is intended for testing, rather than production use.

### Object Stores

Object store implementations historically cache the entire object's data
locally on disk (possibly in memory), until the final `close()` operation
triggers a single `PUT` of the data. Some implementations push out intermediate
blocks of data, synchronously or asynchronously.

Accordingly, they tend not to implement the `Syncable` interface.
However, Exception: Azure's `PageBlobOutputStream` does implement `hsync()`,
blocking until write operations being executed in separate threads have completed.

Equally importantly

1. The object may not be visible at the end of the path until the final `close()`.
is called; this holds for `getFileStatus()`, `open()` and all FileSystem list operations.
1. Any existing data at the end of a path `p`, may remain visible until the final
`close()` operation overwrites this data.
1. The check for existing data in a `create()` call with overwrite=false, may
take place in the `create()` call itself, in the `close()` call prior to/during
the write, or at some point in between. Expect in the special case that the
object store supports an atomic PUT operation, the check for existence of
existing data and the subsequent creation of data at the path contains a race
condition: other clients may create data at the path between the existence check
and the subsequent qrite.




### <a name="write(data)"></a>`write(int data)`

#### Preconditions

    Stream.isOpen else raise IOException

#### Postconditions

The cache has the lower 8 bits of the data argument appended to it.

    Stream'.cache = Stream.cache + [data & 0xff]

There may be an explicit limit on the size of cached data, or an implicit
limit based by the available capacity of the destination filesystem.
When a limit is reached, `write()` MUST fail with an `IOException`.

### <a name="write(buffer,offset,len)"></a>`write(byte[] buffer, int offset, int len)`


#### Preconditions

The preconditions are all defined in `OutputStream.write()`

    Stream.isOpen else raise IOException
    buffer != null else raise NullPointerException
    offset >= 0 else raise IndexOutOfBoundsException
    len >= 0 else raise IndexOutOfBoundsException
    offset < buffer.length else raise IndexOutOfBoundsException
    offset + len < buffer.length else raise IndexOutOfBoundsException


There may be an explicit limit on the size of cached data, or an implicit
limit based by the available capacity of the destination filesystem.
When a limit is reached, `write()` MUST fail with an `IOException`.

After the operation has returned, the buffer may be re-used. The outcome
of updates to the buffer while the `write()` operation is in progress is undefined.

#### Postconditions

    Stream'.cache = Stream.cache + buffer[offset...offset+len]


### <a name="write(buffer)"></a>`write(byte[] buffer)`

This is required to be the equivalent of

    write(buffer, 0, buffer.length)


#### Preconditions

    Stream.isOpen else raise IOException


With the offset of 0 and the length known to be that of the buffer, the
other preconditions of `write(byte[] buffer, int offset, int len)`
can be simplified to

    buffer != null else raise NullPointerException

#### Postconditions

The postconditions become

     Stream'.cache = Stream.cache + buffer[0...buffer.length]

Which is equivalent to

     Stream'.cache = Stream.cache + buffer

### <a name="flush()"></a>`flush()`

Requests that the data is flushed. The specification of `ObjectStream.flush()`
declares that this SHOULD write data to the "intended destination".

It explicitly precludes any guarantees about durability.


#### Preconditions

    Stream.isOpen else raise IOException

#### Postconditions


    FS' = FS where data(path) == cache


### <a name="close"></a>`close()`

The `close()` operation completes the write. It is expected to block
until the write has completed (as with `Syncable.hflush()`), possibly
until it has been written to durable storage (as HDFS does).

After `close()` completes, the data in a file MUST be visible and consistent
with the data most recently written. The metadata of the file MUST be consistent
with the data and the write history itself (i.e. any modification time fields
updated).

Any locking/leaseholding mechanism is also required to release its lock/lease.


    FS' = FS where data(path) == cache
    Stream'.isOpen = false


The `close()` call MAY fail during its operation.

1. Callers of the API MUST expect for some calls to fail and SHOULD code appropriately.
Catching and swallowing exceptions, while common, is not always the ideal solution.
1. Even after a failure, `close()` MUST place the stream into a closed state.
Follow-on calls to `close()` are ignored, and calls to other methods
rejected. That is: caller's cannot be expected to call `close()` repeatedly
until it succeeds.
1. The duration of the `call()` operation is undefined. Operations which rely
on acknowledgements from remote systems to meet the persistence guarantees
implicitly have to await these acknowledgements. Some Object Store output streams
upload the entire data file in the `close()` operation. This can take a large amount
of time. The fact that many user applications assume that `close()` is both fast
and does not fail means that this behavior is dangerous.



## <a name="syncable"></a>`org.apache.hadoop.fs.Syncable`

The purpose of `Syncable` interface is to provide guarantees that data is written
to a filesystem for both visibility and durability.

It's presence is an explicit declaration of an Object Stream's ability to
meet those guarantees.

The interface MUST NOT be declared as implemented by an `OutputStream` unless
those guarantees can be met.

The `Syncable` interface has been implemented by other classes than
subclasses of `OutputStream`. Therefore the fact that
a class implements `Syncable` does not guarantee that `extends OutputStream`
holds.

This specification only covers the required behavior of `ObjectStream` subclasses
which implement `Syncable`.

### <a name="Syncable.hflush"></a>`Syncable.hflush()`

Flush out the data in client's user buffer. After the return of
this call, new readers will see the data.

#### Preconditions

    Stream.isOpen else raise IOException

#### Postconditions


    FS' = FS where data(path) == cache

It's not clear whether this operation is expected to be blocking, that is,
whether, after the call returns, is it guaranteed that the data is
now visible to all


### <a name="Syncable.hsync"></a>`Syncable.hsync()`

Similar to POSIX fsync, save the data in client's user buffer
all the way to the disk device (but the disk may have it in its cache).

That is, it is a requirement for the underlying FS To save all the data to
the disk hardware itself, where it is expected to be durable.

#### Preconditions

    Stream.isOpen else raise IOException

#### Postconditions


    FS' = FS where data(path) == cache

The reference implementation, `DFSOutputStream` will block
until an acknowledgement is received from the datanodes: That is, all hosts
in the replica write chain have successfully written the file.

That means that the expectation callers may have is that the return of
the method call contains visibility and durability guarantees which other
implementations must maintain.

Note, however, that the reference `DFSOutputStream.hsync()` call only actually syncs/
*the current block*. If there have been a series of writes since the last sync,
such that a block boundary has been crossed. The `hsync()` call claims only
to write the most recent.
From the javadocs of `DFSOutputStream.hsync(EnumSet<SyncFlag> syncFlags)`

> Note that only the current block is flushed to the disk device.
> To guarantee durable sync across block boundaries the stream should
> be created with {@link CreateFlag#SYNC_BLOCK}.


In virtual machines, the notion of "disk hardware" is really that of
another software abstraction. There can be no durability guarantees in such
environments.



### <a name="Syncable.hflush"></a>`Syncable.hflush()`

Deprecated: replaced by `hsync()`

