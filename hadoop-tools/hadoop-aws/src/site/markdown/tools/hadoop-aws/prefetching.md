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

# S3A Prefetching

This document explains the `S3PrefetchingInputStream` and the various components it uses.

This input stream implements prefetching and caching to improve read performance of the input
stream.
A high level overview of this feature was published in
[Pinterest Engineering's blog post titled "Improving efficiency and reducing runtime using S3 read optimization"](https://medium.com/pinterest-engineering/improving-efficiency-and-reducing-runtime-using-s3-read-optimization-b31da4b60fa0).

With prefetching, the input stream divides the remote file into blocks of a fixed size, associates
buffers to these blocks and then reads data into these buffers asynchronously.
It also potentially caches these blocks.

### Basic Concepts

* **Remote File**: A binary blob of data stored on some storage device.
* **Block File**: Local file containing a block of the remote file.
* **Block**: A file is divided into a number of blocks.
The size of the first n-1 blocks is same, and the size of the last block may be same or smaller.
* **Block based reading**: The granularity of read is one block.
That is, either an entire block is read and returned or none at all.
Multiple blocks may be read in parallel.

### Configuring the stream

|Property    |Meaning    |Default    |
|---|---|---|
|`fs.s3a.prefetch.enabled`    |Enable the prefetch input stream    |`false` |
|`fs.s3a.prefetch.block.size`    |Size of a block    |`8M`    |
|`fs.s3a.prefetch.block.count`    |Number of blocks to prefetch    |`8`    |

The default size of a block is 8MB, and the minimum allowed block size is 1 byte.
Decreasing block size will increase the number of blocks to be read for a file.
A smaller block size may negatively impact performance as the number of prefetches required will increase.

### Key Components

`S3PrefetchingInputStream` - When prefetching is enabled, S3AFileSystem will return an instance of
this class as the input stream.
Depending on the remote file size, it will either use
the `S3InMemoryInputStream` or the `S3CachingInputStream` as the underlying input stream.

`S3InMemoryInputStream` - Underlying input stream used when the remote file size < configured block
size.
Will read the entire remote file into memory.

`S3CachingInputStream` - Underlying input stream used when remote file size > configured block size.
Uses asynchronous prefetching of blocks and caching to improve performance.

`BlockData` - Holds information about the blocks in a remote file, such as:

* Number of blocks in the remote file
* Block size
* State of each block (initially all blocks have state *NOT_READY*).
Other states are: Queued, Ready, Cached.

`BufferData` - Holds the buffer and additional information about it such as:

* The block number this buffer is for
* State of the buffer (Unknown, Blank, Prefetching, Caching, Ready, Done).
Initial state of a buffer is blank.

`CachingBlockManager` - Implements reading data into the buffer, prefetching and caching.

`BufferPool` - Manages a fixed sized pool of buffers.
It's used by `CachingBlockManager` to acquire buffers.

`S3File` - Implements operations to interact with S3 such as opening and closing the input stream to
the remote file in S3.

`S3Reader` - Implements reading from the stream opened by `S3File`.
Reads from this input stream in blocks of 64KB.

`FilePosition` - Provides functionality related to tracking the position in the file.
Also gives access to the current buffer in use.

`SingleFilePerBlockCache` - Responsible for caching blocks to the local file system.
Each cache block is stored on the local disk as a separate block file.

### Operation

#### S3InMemoryInputStream

For a remote file with size 5MB, and block size = 8MB, since file size is less than the block size,
the `S3InMemoryInputStream` will be used.

If the caller makes the following read calls:

```
in.read(buffer, 0, 3MB);
in.read(buffer, 0, 2MB);
```

When the first read is issued, there is no buffer in use yet.
The `S3InMemoryInputStream` gets the data in this remote file by calling the `ensureCurrentBuffer()`
method, which ensures that a buffer with data is available to be read from.

The `ensureCurrentBuffer()` then:

* Reads data into a buffer by calling `S3Reader.read(ByteBuffer buffer, long offset, int size)`.
* `S3Reader` uses `S3File` to open an input stream to the remote file in S3 by making
  a `getObject()` request with range as `(0, filesize)`.
* The `S3Reader` reads the entire remote file into the provided buffer, and once reading is complete
  closes the S3 stream and frees all underlying resources.
* Now the entire remote file is in a buffer, set this data in `FilePosition` so it can be accessed
  by the input stream.

The read operation now just gets the required bytes from the buffer in `FilePosition`.

When the second read is issued, there is already a valid buffer which can be used.
Don't do anything else, just read the required bytes from this buffer.

#### S3CachingInputStream

If there is a remote file with size 40MB and block size = 8MB, the `S3CachingInputStream` will be
used.

##### Sequential Reads

If the caller makes the following calls:

```
in.read(buffer, 0, 5MB)
in.read(buffer, 0, 8MB)
```

For the first read call, there is no valid buffer yet.
`ensureCurrentBuffer()` is called, and for the first `read()`, prefetch count is set as 1.

The current block (block 0) is read synchronously, while the blocks to be prefetched (block 1) is
read asynchronously.

The `CachingBlockManager` is responsible for getting buffers from the buffer pool and reading data
into them. This process of acquiring the buffer pool works as follows:

* The buffer pool keeps a map of allocated buffers and a pool of available buffers.
The size of this pool is = prefetch block count + 1.
If the prefetch block count is 8, the buffer pool has a size of 9.
* If the pool is not yet at capacity, create a new buffer and add it to the pool.
* If it's at capacity, check if any buffers with state = done can be released.
Releasing a buffer means removing it from allocated and returning it back to the pool of available
buffers.
* If there are no buffers with state = done currently then nothing will be released, so retry the
  above step at a fixed interval a few times till a buffer becomes available.
* If after multiple retries there are still no available buffers, release a buffer in the ready state.
The buffer for the block furthest from the current block is released.

Once a buffer has been acquired by `CachingBlockManager`, if the buffer is in a *READY* state, it is
returned.
This means that data was already read into this buffer asynchronously by a prefetch.
If its state is *BLANK* then data is read into it using
`S3Reader.read(ByteBuffer buffer, long offset, int size).`

For the second read call, `in.read(buffer, 0, 8MB)`, since the block sizes are of 8MB and only 5MB
of block 0 has been read so far, 3MB of the required data will be read from the current block 0.
Once all data has been read from this block, `S3CachingInputStream` requests the next block (
block 1), which will already have been prefetched and so it can just start reading from it.
Also, while reading from block 1 it will also issue prefetch requests for the next blocks.
The number of blocks to be prefetched is determined by `fs.s3a.prefetch.block.count`.

##### Random Reads

The `CachingInputStream` also caches prefetched blocks. This happens when `read()` is issued
after a `seek()` outside the current block, but the current block still has not been fully read.

For example, consider the following calls:

```
in.read(buffer, 0, 5MB)
in.seek(10MB)
in.read(buffer, 0, 4MB)
in.seek(2MB)
in.read(buffer, 0, 4MB)
```

For the above read sequence, after the `seek(10MB)` call is issued, block 0 has not been read
completely so the subsequent call to `read()` will cache it, on the assumption that the caller
will probably want to read from it again.

After `seek(2MB)` is called, the position is back inside block 0. The next read can now be
satisfied from the locally cached block file, which is typically orders of magnitude faster
than a network based read.

NB: `seek()` is implemented lazily, so it only keeps track of the new position but does not
otherwise affect the internal state of the stream. Only when a `read()` is issued, it will call
the `ensureCurrentBuffer()` method and fetch a new block if required.