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

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

# Output: `OutputStream`, `Syncable` and `StreamCapabilities`

## Introduction

This document covers the Output Streams within the context of the
[Hadoop File System Specification](index.html).

It uses the filesystem model defined in [A Model of a Hadoop Filesystem](model.html)
with the notation defined in [notation](Notation.md).

The target audiences are:
1. Users of the APIs. While `java.io.OutputStream` is a standard interfaces,
this document clarifies how it is implemented in HDFS and elsewhere.
The Hadoop-specific interfaces `Syncable` and `StreamCapabilities` are new;
`Syncable` is notable in offering durability and visibility guarantees which
exceed that of `OutputStream`.
1. Implementors of File Systems and clients.

## How data is written to a filesystem

The core mechanism to write data to files through the Hadoop FileSystem APIs
is through `OutputStream` subclasses obtained through calls to
`FileSystem.create()`, `FileSystem.append()`,
or `FSDataOutputStreamBuilder.build()`.

These all return instances of `FSDataOutputStream`, through which data
can be written through various `write()` methods.
After a stream's `close()` method is called, all data written to the
stream MUST BE persisted to the filesystem and visible to oll other
clients attempting to read data from that path via `FileSystem.open()`.

As well as operations to write the data, Hadoop's `OutputStream` implementations
provide methods to flush buffered data back to the filesystem,
so as to ensure that the data is reliably persisted and/or visible
to other callers. This is done via the `Syncable` interface. It was
originally intended that the presence of this interface could be interpreted
as a guarantee that the stream supported its methods. However, this has proven
impossible to guarantee as the static nature of the interface is incompatible
with filesystems whose syncability semantics may vary on a store/path basis.
As an example, erasure coded files in HDFS do not support the Sync operations,
even though they are implemented as subclass of an output stream which is `Syncable`.

A new interface: `StreamCapabilities`. This allows callers
to probe the exact capabilities of a stream, even transitively
through a chain of streams.

## Output Stream Model

For this specification, an output stream can be viewed as a list of bytes
stored in the client; `hsync()` and `hflush()` are operations the actions
which propagate the data to be visible to other readers of the file and/or
made durable.

```python
buffer: List[byte]
```

A flag, `open` tracks whether the stream is open: after the stream
is closed no more data may be written to it:

```python
open: bool
buffer: List[byte]
```

The destination path of the stream, `path`, can be tracked to form a triple
`path, open, buffer`

```python
Stream = (path: Path, open: Boolean, buffer: byte[])
```

#### Visibility of Flushed Data

(Immediately) after `Syncable` operations which flush data to the filesystem,
the data at the stream's destination path MUST match that of
`buffer`. That is, the following condition MUST hold:

```python
FS'.Files(path) == buffer
```

Any client reading the data at the path MUST see the new data.
The `Syncable` operations differ in their durability
guarantees, not visibility of data.

### State of Stream and File System after `Filesystem.create()`

The output stream returned by a `FileSystem.create(path)` or
`FileSystem.createFile(path).build()` within a filesystem `FS`,
can be modeled as a triple containing an empty array of no data:

```python
Stream' = (path, true, [])
```

The filesystem `FS'` MUST contain a 0-byte file at the path:

```python
FS' = FS where data(FS', path) == []
```

Thus, the initial state of `Stream'.buffer` is implicitly
consistent with the data at the filesystem.


*Object Stores*: see caveats in the "Object Stores" section below.

### State of Stream and File System after `Filesystem.append()`

The output stream returned from a call of
 `FileSystem.append(path, buffersize, progress)` within a filesystem `FS`,
can be modelled as a stream whose `buffer` is initialized to that of
the original file:

```python
Stream' = (path, true, data(FS, path))
```

####  Persisting data

When the stream writes data back to its store, be it in any
supported flush operation, in the `close()` operation, or at any other
time the stream chooses to do so, the contents of the file
are replaced with the current buffer

```python
Stream' = (path, true, buffer)
FS' = FS where data(FS', path) == buffer
```

After a call to `close()`, the stream is closed for all operations other
than `close()`; they MAY fail with `IOException` or `RuntimeException`.

```python
Stream' = (path, false, [])
```

The `close()` operation MUST be idempotent with the sole attempt to write the
data made in the first invocation.

1. If `close()` succeeds, subsequent calls are no-ops.
1. If `close()` fails, again, subsequent calls are no-ops. They MAY rethrow
the previous exception, but they MUST NOT retry the write.

<!--  ============================================================= -->
<!--  CLASS: FSDataOutputStream -->
<!--  ============================================================= -->

## <a name="fsdataoutputstream"></a>Class `FSDataOutputStream`

```java
public class FSDataOutputStream
  extends DataOutputStream
  implements Syncable, CanSetDropBehind, StreamCapabilities {
 // ...
}
```

The `FileSystem.create()`, `FileSystem.append()` and
`FSDataOutputStreamBuilder.build()` calls return an instance
of a class `FSDataOutputStream`, a subclass of `java.io.OutputStream`.

The base class wraps an `OutputStream` instance, one which may implement `Syncable`,
`CanSetDropBehind` and `StreamCapabilities`.

This document covers the requirements of such implementations.

HDFS's `FileSystem` implementation, `DistributedFileSystem`, returns an instance
of `HdfsDataOutputStream`. This implementation has at least two behaviors
which are not explicitly declared by the base Java implementation

1. Writes are synchronized: more than one thread can write to the same
output stream. This is a use pattern which HBase relies on.

1. `OutputStream.flush()` is a no-op when the file is closed. Apache Druid
has made such a call on this in the past
[HADOOP-14346](https://issues.apache.org/jira/browse/HADOOP-14346).


As the HDFS implementation is considered the de-facto specification of
the FileSystem APIs, the fact that `write()` is thread-safe is significant.

For compatibility, not only SHOULD other FS clients be thread-safe,
but new HDFS features, such as encryption and Erasure Coding SHOULD also
implement consistent behavior with the core HDFS output stream.

Put differently:

*It isn't enough for Output Streams to implement the core semantics
of `java.io.OutputStream`: they need to implement the extra semantics
of `HdfsDataOutputStream`, especially for HBase to work correctly.*

The concurrent `write()` call is the most significant tightening of
the Java specification.

## <a name="outputstream"></a>Class `java.io.OutputStream`

A Java `OutputStream` allows applications to write a sequence of bytes to a destination.
In a Hadoop filesystem, that destination is the data under a path in the filesystem.

```java
public abstract class OutputStream implements Closeable, Flushable {
  public abstract void write(int b) throws IOException;
  public void write(byte b[]) throws IOException;
  public void write(byte b[], int off, int len) throws IOException;
  public void flush() throws IOException;
  public void close() throws IOException;
}
```
### <a name="write(data: int)"></a>`write(Stream, data)`

Writes a byte of data to the stream.

#### Preconditions

```python
Stream.open else raise ClosedChannelException, PathIOException, IOException
```

The exception `java.nio.channels.ClosedChannelExceptionn` is
raised in the HDFS output streams when trying to write to a closed file.
This exception does not include the destination path; and
`Exception.getMessage()` is `null`. It is therefore of limited value in stack
traces. Implementors may wish to raise exceptions with more detail, such
as a `PathIOException`.


#### Postconditions

The buffer has the lower 8 bits of the data argument appended to it.

```python
Stream'.buffer = Stream.buffer + [data & 0xff]
```

There may be an explicit limit on the size of cached data, or an implicit
limit based by the available capacity of the destination filesystem.
When a limit is reached, `write()` SHOULD fail with an `IOException`.

### <a name="write(buffer,offset,len)"></a>`write(Stream, byte[] data, int offset, int len)`


#### Preconditions

The preconditions are all defined in `OutputStream.write()`

```python
Stream.open else raise ClosedChannelException, PathIOException, IOException
data != null else raise NullPointerException
offset >= 0 else raise IndexOutOfBoundsException
len >= 0 else raise IndexOutOfBoundsException
offset < data.length else raise IndexOutOfBoundsException
offset + len < data.length else raise IndexOutOfBoundsException
```

After the operation has returned, the buffer may be re-used. The outcome
of updates to the buffer while the `write()` operation is in progress is undefined.

#### Postconditions

```python
Stream'.buffer = Stream.buffer + data[offset...(offset + len)]
```

### <a name="write(buffer)"></a>`write(byte[] data)`

This is defined as the equivalent of:

```python
write(data, 0, data.length)
```

### <a name="flush()"></a> `flush()`

Requests that the data is flushed. The specification of `ObjectStream.flush()`
declares that this SHOULD write data to the "intended destination".

It explicitly precludes any guarantees about durability.

For that reason, this document doesn't provide any normative
specifications of behaviour.

#### Preconditions

None.

#### Postconditions

None.

If the implementation chooses to implement a stream-flushing operation,
the data may be saved to the file system such that it becomes visible to
others"

```python
FS' = FS where data(FS', path) == buffer
```

When a stream is closed, `flush()` SHOULD downgrade to being a no-op, if it was not
one already. This is to work with applications and libraries which can invoke
it in exactly this way.


*Issue*: Should `flush()` forward to `hflush()`?

No. Or at least, make it optional.

There's a lot of application code which assumes that `flush()` is low cost
and should be invoked after writing every single line of output, after
writing small 4KB blocks or similar.

Forwarding this to a full flush across a distributed filesystem, or worse,
a distant object store, is very inefficient.
Filesystem clients which convert a `flush()` to an `hflush()` will eventually
have to roll back that feature:
[HADOOP-16548](https://issues.apache.org/jira/browse/HADOOP-16548).

### <a name="close"></a>`close()`

The `close()` operation saves all data to the filesystem and
releases any resources used for writing data.

The `close()` call is expected to block
until the write has completed (as with `Syncable.hflush()`), possibly
until it has been written to durable storage.

After `close()` completes, the data in a file MUST be visible and consistent
with the data most recently written. The metadata of the file MUST be consistent
with the data and the write history itself (i.e. any modification time fields
updated).

After `close()` is invoked, all subsequent `write()` calls on the stream
MUST fail with an `IOException`.

Any locking/leaseholding mechanism MUST release its lock/lease.

```python
Stream'.open = false
FS' = FS where data(FS', path) == buffer
```

The `close()` call MAY fail during its operation.

1. Callers of the API MUST expect for some calls to  `close()` to fail and SHOULD code appropriately.
Catching and swallowing exceptions, while common, is not always the ideal solution.
1. Even after a failure, `close()` MUST place the stream into a closed state.
Follow-on calls to `close()` are ignored, and calls to other methods
rejected. That is: caller's cannot be expected to call `close()` repeatedly
until it succeeds.
1. The duration of the `close()` operation is undefined. Operations which rely
on acknowledgements from remote systems to meet the persistence guarantees
implicitly have to await these acknowledgements. Some Object Store output streams
upload the entire data file in the `close()` operation. This can take a large amount
of time. The fact that many user applications assume that `close()` is both fast
and does not fail means that this behavior is dangerous.

Recommendations for safe use by callers

* Do plan for exceptions being raised, either in catching and logging or
by throwing the exception further up. Catching and silently swallowing exceptions
may hide serious problems.
* Heartbeat operations SHOULD take place on a separate thread, so that a long
delay in `close()` does not block the thread so long that the heartbeat times
out.

Implementors:

* Have a look at [HADOOP-16785](https://issues.apache.org/jira/browse/HADOOP-16785)
to see examples of complications in close.
* Incrementally writing blocks before a close operation results in a behavior which
matches client expectations better: write failures to surface earlier and close
to be more housekeeping than the actual upload.
* If block uploads are executed in separate threads, the output stream `close()`
call MUST block until all the asynchronous uploads have completed; any error raised
MUST be reported.
If multiple errors were raised, the stream can choose which to propagate.
What is important is: when `close()` returns without an error, applications expect
the data to have been successfully written.

### HDFS and `OutputStream.close()`

HDFS does not immediately `sync()` the output of a written file to disk on
`OutputStream.close()` unless configured with `dfs.datanode.synconclose`
is true. This has caused [problems in some applications](https://issues.apache.org/jira/browse/ACCUMULO-1364).

Applications which absolutely require the guarantee that a file has been persisted
MUST call `Syncable.hsync()` *before* the file is closed.


## <a name="syncable"></a>`org.apache.hadoop.fs.Syncable`

```java
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Syncable {


  /** Flush out the data in client's user buffer. After the return of
   * this call, new readers will see the data.
   * @throws IOException if any error occurs
   */
  void hflush() throws IOException;

  /** Similar to posix fsync, flush out the data in client's user buffer
   * all the way to the disk device (but the disk may have it in its cache).
   * @throws IOException if error occurs
   */
  void hsync() throws IOException;
}
```

The purpose of `Syncable` interface is to provide guarantees that data is written
to a filesystem for both visibility and durability.

*SYNC-1*: An `OutputStream` which implements `Syncable` and does not raise
`UnsupportedOperationException` on invocations is
making an explicit declaration that it can meet those guarantees.

*SYNC-2*: If a stream, declares the interface as implemented, but does not
provide durability, the interface's methods MUST raise
`UnsupportedOperationException`.

The `Syncable` interface has been implemented by other classes than
subclasses of `OutputStream`, such as `org.apache.hadoop.io.SequenceFile.Writer`.

*SYNC-3* The fact that a class implements `Syncable` does not guarantee
that `extends OutputStream` holds.

That is, for any class `C`: `(C instanceof Syncable)` does not imply
`(C instanceof OutputStream)`

This specification only covers the required behavior of `OutputStream` subclasses
which implement `Syncable`.


*SYNC-4:* The return value of `FileSystem.create(Path)` is an instance
of `FSDataOutputStream`.

*SYNC-5:* `FSDataOutputStream implements Syncable`


SYNC-5 and SYNC-1 imply that all output streams which can be created
with `FileSystem.create(Path)` must support the semantics of `Syncable`.
This is demonstrably not true: `FSDataOutputStream` simply downgrades
to a `flush()` if its wrapped stream is not `Syncable`.
Therefore the declarations SYNC-1 and SYNC-2 do not hold: you cannot trust `Syncable`.

Put differently: *callers MUST NOT rely on the presence of the interface
as evidence that the semantics of `Syncable` are supported*. Instead
they MUST be dynamically probed for using the `StreamCapabilities`
interface, where available.


### <a name="syncable.hflush"></a>`Syncable.hflush()`

Flush out the data in client's user buffer. After the return of
this call, new readers will see the data. The `hflush()` operation
does not contain any guarantees as to the durability of the data. only
its visibility.

Thus implementations may cache the written data in memory
—visible to all, but not yet persisted.

#### Preconditions

```python
hasCapability(Stream, "hflush")
Stream.open else raise IOException
```


#### Postconditions

```python
FS' = FS where data(path) == cache
```


After the call returns, the data MUST be visible to all new callers
of `FileSystem.open(path)` and `FileSystem.openFile(path).build()`.

There is no requirement or guarantee that clients with an existing
`DataInputStream` created by a call to `(FS, path)` will see the updated
data, nor is there a guarantee that they *will not* in a current or subsequent
read.

Implementation note: as a correct `hsync()` implementation MUST also
offer all the semantics of an `hflush()` call, implementations of `hflush()`
may just invoke `hsync()`:

```java
public void hflush() throws IOException {
  hsync();
}
```

#### `hflush()` Performance

The `hflush()` call MUST block until the store has acknowledge that the
data has been received and is now visible to others. This can be slow,
as it will include the time to upload any outstanding data from the
client, and for the filesystem itself to process it.

Often Filesystems only offer the `Syncable.hsync()` guarantees: persistence as
well as visibility. This means the time to return can be even greater.

Application code MUST NOT call `hflush()` or `hsync()` at the end of every line
or, unless they are writing a WAL, at the end of every record. Use with care.


### <a name="syncable.hsync"></a> `Syncable.hsync()`

Similar to POSIX `fsync()`, this call saves the data in client's user buffer
all the way to the disk device (but the disk may have it in its cache).

That is: it is a requirement for the underlying FS To save all the data to
the disk hardware itself, where it is expected to be durable.

#### Preconditions

```python
hasCapability(Stream, "hsync")
Stream.open else raise IOException
```

#### Postconditions

```python
FS' = FS where data(path) == buffer
```

_Implementations are required to block until that write has been
acknowledged by the store._

This is so the caller can be confident that once the call has
returned successfully, the data has been written.



## <a name="streamcapabilities"></a>Interface `StreamCapabilities`

```java
@InterfaceAudience.Public
@InterfaceStability.Evolving
```

The `org.apache.hadoop.fs.StreamCapabilities` interface exists to allow callers to dynamically
determine the behavior of a stream.

```java
  public boolean hasCapability(String capability) {
    switch (capability.toLowerCase(Locale.ENGLISH)) {
      case StreamCapabilities.HSYNC:
      case StreamCapabilities.HFLUSH:
        return supportFlush;
      default:
        return false;
    }
  }
```

Once a stream has been closed, a `hasCapability()` call MUST do one of

* return the capabilities of the open stream.
* return false.

That is: it MUST NOT raise an exception about the file being closed;

See [pathcapabilities](pathcapabilities.html) for specifics on the `PathCapabilities` API;
the requirements are similar: a stream MUST NOT return true for a capability
for which it lacks support, be it because

* The capability is unknown.
* The capability is known and known to be unsupported.

Standard stream capabilities are defined in `StreamCapabilities`;
consult the javadocs for the complete set of options.

| Name  | Probes for support of |
|-------|---------|
| `dropbehind` | `CanSetDropBehind.setDropBehind()` |
| `hsync` | `Syncable.hsync()` |
| `hflush` | `Syncable.hflush()`. Deprecated: probe for `HSYNC` only. |
| `in:readahead` | `CanSetReadahead.setReadahead()` |
| `in:unbuffer"` | `CanUnbuffer.unbuffer()` |
| `in:readbytebuffer` | `ByteBufferReadable#read(ByteBuffer)` |
| `in:preadbytebuffer` | `ByteBufferPositionedReadable#read(long, ByteBuffer)` |

Stream implementations MAY add their own custom options.
These MUST be prefixed with `fs.SCHEMA.`, where `SCHEMA` is the schema of the filesystem.

## <a name="cansetdropbehind"></a> interface `CanSetDropBehind`

```java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CanSetDropBehind {
  /**
   * Configure whether the stream should drop the cache.
   *
   * @param dropCache     Whether to drop the cache.  null means to use the
   *                      default value.
   * @throws IOException  If there was an error changing the dropBehind
   *                      setting.
   *         UnsupportedOperationException  If this stream doesn't support
   *                                        setting the drop-behind.
   */
  void setDropBehind(Boolean dropCache)
      throws IOException, UnsupportedOperationException;
}
```

This interface allows callers to change policies used inside HDFS.

Implementations MUST return `true` for the call

```java
StreamCapabilities.hasCapability("dropbehind");
```


## <a name="durability-of-output"></a>Durability, Concurrency, Consistency and Visibility of stream output.

These are the aspects of the system behaviour which are not directly
covered in this (very simplistic) filesystem model, but which are visible
in production.


### <a name="durability"></a> Durability

1. `OutputStream.write()` MAY persist the data, synchronously or asynchronously
1. `OutputStream.flush()` flushes data to the destination. There
are no strict persistence requirements.
1. `Syncable.hflush()` synchronously sends all outstanding data to the destination
filesystem. After returning to the caller, the data MUST be visible to other readers,
it MAY be durable. That is: it does not have to be persisted, merely guaranteed
to be consistently visible to all clients attempting to open a new stream reading
data at the path.
1. `Syncable.hsync()` MUST transmit the data as per `hflush` and persist
   that data to the underlying durable storage.
1. `close()` The first call to `close()` MUST flush out all remaining data in
the buffers, and persist it, as a call to `hsync()`.


Many applications call `flush()` far too often -such as at the end of every line written.
If this triggered an update of the data in persistent storage and any accompanying
metadata, distributed stores would overload fast.
Thus: `flush()` is often treated at most as a cue to flush data to the network
buffers -but not commit to writing any data.

It is only the `Syncable` interface which offers guarantees.

The two `Syncable` operations `hsync()` and `hflush()` differ purely by the extra guarantee of `hsync()`: the data must be persisted.
If `hsync()` is implemented, then `hflush()` can be implemented simply
by invoking `hsync()`

```java
public void hflush() throws IOException {
  hsync();
}
```

This is perfectly acceptable as an implementation: the semantics of `hflush()`
are satisfied.
What is not acceptable is downgrading `hsync()` to `hflush()`, as the durability guarantee is no longer met.


### <a name="concurrency"></a> Concurrency

1. The outcome of more than one process writing to the same file is undefined.

1. An input stream opened to read a file *before the file was opened for writing*
MAY fetch data updated by writes to an OutputStream.
Because of buffering and caching, this is not a requirement
—and if an input stream does pick up updated data, the point at
which the updated data is read is undefined. This surfaces in object stores
where a `seek()` call which closes and re-opens the connection may pick up
updated data, while forward stream reads do not. Similarly, in block-oriented
filesystems, the data may be cached a block at a time —and changes only picked
up when a different block is read.

1. A filesystem MAY allow the destination path to be manipulated while a stream
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
model permits the output stream to have `close()` invoked while awaiting an
acknowledgement from datanode or namenode writes in an `hsync()` operation.

### <a name="consistency"></a>Consistency and Visibility

There is no requirement for the data to be immediately visible to other applications
—not until a specific call to flush buffers or persist it to the underlying storage
medium are made.

If an output stream is created with `FileSystem.create(path, overwrite==true)`
and there is an existing file at the path, that is `exists(FS, path)` holds,
then, the existing data is immediately unavailable; the data at the end of the
path MUST consist of an empty byte sequence `[]`, with consistent metadata.


```python
exists(FS, path)
(Stream', FS') = create(FS, path)
exists(FS', path)
getFileStatus(FS', path).getLen() = 0
```

The metadata of a file (`length(FS, path)` in particular) SHOULD be consistent
with the contents of the file after `flush()` and `sync()`.

```python
(Stream', FS') = create(FS, path)
(Stream'', FS'') = write(Stream', data)
(Stream''', FS''') hsync(Stream'')
exists(FS''', path)
getFileStatus(FS''', path).getLen() = len(data)
```

*HDFS does not do this except when the write crosses a block boundary*; to do
otherwise would overload the Namenode. Other stores MAY copy this behavior.

As a result, while a file is being written
`length(Filesystem, Path)` MAY be less than the length of `data(Filesystem, Path)`.

The metadata MUST be consistent with the contents of a file after the `close()`
operation.

After the contents of an output stream have been persisted (`hflush()/hsync()`)
all new `open(FS, Path)` operations MUST return the updated data.

After `close()` has been invoked on an output stream,
a call to `getFileStatus(path)` MUST return the final metadata of the written file,
including length and modification time.
The metadata of the file returned in any of the FileSystem `list` operations
MUST be consistent with this metadata.

The value of `getFileStatus(path).getModificationTime()` is not defined
while a stream is being written to.
The timestamp MAY be updated while a file is being written,
especially after a `Syncable.hsync()` call.
The timestamps MUST be updated after the file is closed
to that of a clock value observed by the server during the `close()` call.
It is *likely* to be in the time and time zone of the filesystem, rather
than that of the client.

Formally, if a `close()` operation triggers an interaction with a server
which starts at server-side time `t1` and completes at time `t2` with a successfully
written file, then the last modification time SHOULD be a time `t` where
`t1 <= t <= t2`

## <a name="issues"></a> Issues with the Hadoop Output Stream model.

There are some known issues with the output stream model as offered by Hadoop,
specifically about the guarantees about when data is written and persisted
—and when the metadata is synchronized.
These are where implementation aspects of HDFS and the "Local" filesystem
do not follow the simple model of the filesystem used in this specification.

### <a name="hdfs-issues"></a> HDFS

#### HDFS: `hsync()` only syncs the latest block

The reference implementation, `DFSOutputStream` will block until an
acknowledgement is received from the datanodes: that is, all hosts in the
replica write chain have successfully written the file.

That means that the expectation callers may have is that the return of the
method call contains visibility and durability guarantees which other
implementations must maintain.

Note, however, that the reference `DFSOutputStream.hsync()` call only actually
persists *the current block*. If there have been a series of writes since the
last sync, such that a block boundary has been crossed. The `hsync()` call
claims only to write the most recent.

From the javadocs of `DFSOutputStream.hsync(EnumSet<SyncFlag> syncFlags)`

> Note that only the current block is flushed to the disk device.
> To guarantee durable sync across block boundaries the stream should
> be created with {@link CreateFlag#SYNC_BLOCK}.


This is an important HDFS implementation detail which must not be ignored by
anyone relying on HDFS to provide a Write-Ahead-Log or other database structure
where the requirement of the application is that
"all preceeding bytes MUST have been persisted before the commit flag in the WAL
is flushed"

See [Stonebraker81], Michael Stonebraker, _Operating System Support for Database Management_,
1981, for a discussion on this topic.

If you do need `hsync()` to have synced every block in a very large write, call
it regularly.

#### HDFS: delayed visibility of metadata updates.

That HDFS file metadata often lags the content of a file being written
to is not something everyone expects, nor convenient for any program trying
to pick up updated data in a file being written. Most visible is the length
of a file returned in the various `list` commands and `getFileStatus` —this
is often out of date.

As HDFS only supports file growth in its output operations, this means
that the size of the file as listed in the metadata may be less than or equal
to the number of available bytes —but never larger. This is a guarantee which
is also held

One algorithm to determine whether a file in HDFS is updated is:

1. Remember the last read position `pos` in the file, using `0` if this is the initial
read.
1. Use `getFileStatus(FS, Path)` to query the updated length of the file as
recorded in the metadata.
1. If `Status.length &gt; pos`, the file has grown.
1. If the number has not changed, then
    1. Reopen the file.
    1. `seek(pos)` to that location
    1. If `read() != -1`, there is new data.

This algorithm works for filesystems which are consistent with metadata and
data, as well as HDFS. What is important to know is that, for an open file
`getFileStatus(FS, path).getLen() == 0` does not imply that `data(FS, path)` is
empty.

When an output stream in HDFS is closed; the newly written data is not immediately
written to disk unless HDFS is deployed with `dfs.datanode.synconclose` set to
true. Otherwise it is cached and written to disk later.

### <a name="local-issues"></a>Local Filesystem, `file:`

`LocalFileSystem`, `file:`, (or any other `FileSystem` implementation based on
`ChecksumFileSystem`) has a different issue. If an output stream
is obtained from `create()` and `FileSystem.setWriteChecksum(false)` has
*not* been called on the filesystem, then the stream only flushes as much
local data as can be written to full checksummed blocks of data.

That is, the hsync/hflush operations are not guaranteed to write all the pending
data until the file is finally closed.

For this reason, the local filesystem accessed via `file://` URLs
does not support `Syncable` unless `setWriteChecksum(false)` was
called on that FileSystem instance so as to disable checksum creation.
After which, obviously, checksums are not generated for any file.
Is
### <a name="checksummed-fs-issues"></a> Checksummed output streams

Because  `org.apache.hadoop.fs.FSOutputSummer` and
`org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSOutputSummer`
implement the underlying checksummed output stream used by HDFS and
other filesystems, it provides some of the core semantics of the output
stream behavior.

1. The `close()` call is unsynchronized, re-entrant and may attempt
to close the stream more than once.
1. It is possible to call `write(int)` on a closed stream (but not
`write(byte[], int, int)`).
1. It is possible to call `flush()` on a closed stream.

Behaviors 1 and 2 really have to be considered bugs to fix, albeit with care.

Behavior 3 has to be considered a defacto standard, for other implementations
to copy.

### <a name="object-store-issues"></a> Object Stores

Object store streams MAY buffer the entire stream's output
until the final `close()` operation triggers a single `PUT` of the data
and materialization of the final output.

This significantly changes their behaviour compared to that of
POSIX filesystems and that specified in this document.

#### Visibility of newly created objects

There is no guarantee that any file will be visible at the path of an output
stream after the output stream is created .

That is: while `create(FS, path, boolean)` returns a new stream

```python
Stream' = (path, true, [])
```

The other postcondition of the operation, `data(FS', path) == []` MAY NOT
hold, in which case:

1. `exists(FS, p)` MAY return false.
1. If a file was created with `overwrite = True`, the existing data MAY still
be visible: `data(FS', path) = data(FS, path)`.

1. The check for existing data in a `create()` call with `overwrite=False`, may
take place in the `create()` call itself, in the `close()` call prior to/during
the write, or at some point in between. In the special case that the
object store supports an atomic `PUT` operation, the check for existence of
existing data and the subsequent creation of data at the path contains a race
condition: other clients may create data at the path between the existence check
and the subsequent write.

1. Calls to `create(FS, Path, overwrite=false)` MAY succeed, returning a new
`OutputStream`, even while another stream is open and writing to the destination
path.

This allows for the following sequence of operations, which would
raise an exception in the second `open()` call if invoked against HDFS:

```python
Stream1 = open(FS, path, false)
sleep(200)
Stream2 = open(FS, path, false)
Stream.write('a')
Stream1.close()
Stream2.close()
```

For anyone wondering why the clients don't create a 0-byte file in the `create()` call,
it would cause problems after `close()` —the marker file could get
returned in `open()` calls instead of the final data.

#### Visibility of the output of a stream after `close()`

One guarantee which Object Stores SHOULD make is the same as those of POSIX
filesystems: After a stream `close()` call returns, the data MUST be persisted
durably and visible to all callers. Unfortunately, even that guarantee is
not always met:

1. Existing data on a path MAY be visible for an indeterminate period of time.

1. If the store has any form of create inconsistency or buffering of negative
existence probes, then even after the stream's `close()` operation has returned,
`getFileStatus(FS, path)` and `open(FS, path)` may fail with a `FileNotFoundException`.

In their favour, the atomicity of the store's PUT operations do offer their
own guarantee: a newly created object is either absent or all of its data
is present: the act of instantiating the object, while potentially exhibiting
create inconsistency, is atomic. Applications may be able to use that fact
to their advantage.

The [Abortable](abortable.html) interface exposes this ability to abort an output
stream before its data is made visible, so can be used for checkpointing and similar
operations.

## <a name="implementors"></a> Implementors notes.

### Always implement `Syncable` -even if just to throw `UnsupportedOperationException`

Because `FSDataOutputStream` silently downgrades `Syncable.hflush()`
and `Syncable.hsync()` to `wrappedStream.flush()`, callers of the 
API MAY be misled into believing that their data has been flushed/synced
after syncing to a stream which does not support the APIs.

Implementations SHOULD implement the API but
throw `UnsupportedOperationException`. 

### `StreamCapabilities`

Implementors of filesystem clients SHOULD implement the `StreamCapabilities`
interface and its `hasCapabilities()` method to declare whether or not
an output streams offer the visibility and durability guarantees of `Syncable`.

Implementors of `StreamCapabilities.hasCapabilities()` MUST NOT declare that
they support the  `hflush` and `hsync` capabilities on streams where this is not true.

Sometimes streams pass their data to store, but the far end may not
sync it all the way to disk. That is not something the client can determine.
Here: if the client code is making the hflush/hsync passes these requests
on to the distributed FS, it SHOULD declare that it supports them.

### Metadata updates

Implementors MAY NOT update a file's metadata (length, date, ...) after
every `hsync()` call. HDFS doesn't, except when the written data crosses
a block boundary.



### Does `close()` synchronize and persist data?

By default, HDFS does not immediately data to disk when a stream is closed; it will
be asynchronously saved to disk.

This does not mean that users do not expect it.

The behavior as implemented is similar to the write-back aspect's of NFS's
[caching](https://docstore.mik.ua/orelly/networking_2ndEd/nfs/ch07_04.htm).
`DFSClient.close()` is performing an `hflush()` to the client to upload
all data to the datanodes.

1. `close()` SHALL return once the guarantees of `hflush()` are met: the data is
   visible to others.
1. For durability guarantees, `hsync()` MUST be called first.
