/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

/**
 * The input stream for the COS blob store.
 * Optimized sequential read flow based on a forward read-ahead queue
 */
public class CosNInputStream extends FSInputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(CosNInputStream.class);

  /**
   * This class is used by {@link CosNInputStream}
   * and {@link CosNFileReadTask} to buffer data that read from COS blob store.
   */
  public static class ReadBuffer {
    public static final int INIT = 1;
    public static final int SUCCESS = 0;
    public static final int ERROR = -1;

    private final ReentrantLock lock = new ReentrantLock();
    private Condition readyCondition = lock.newCondition();

    private byte[] buffer;
    private int status;
    private long start;
    private long end;

    public ReadBuffer(long start, long end) {
      this.start = start;
      this.end = end;
      this.buffer = new byte[(int) (this.end - this.start) + 1];
      this.status = INIT;
    }

    public void lock() {
      this.lock.lock();
    }

    public void unLock() {
      this.lock.unlock();
    }

    public void await(int waitStatus) throws InterruptedException {
      while (this.status == waitStatus) {
        readyCondition.await();
      }
    }

    public void signalAll() {
      readyCondition.signalAll();
    }

    public byte[] getBuffer() {
      return this.buffer;
    }

    public int getStatus() {
      return this.status;
    }

    public void setStatus(int status) {
      this.status = status;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }
  }

  private FileSystem.Statistics statistics;
  private final Configuration conf;
  private final NativeFileSystemStore store;
  private final String key;
  private long position = 0;
  private long nextPos = 0;
  private long fileSize;
  private long partRemaining;
  private final long preReadPartSize;
  private final int maxReadPartNumber;
  private byte[] buffer;
  private boolean closed;

  private final ExecutorService readAheadExecutorService;
  private final Queue<ReadBuffer> readBufferQueue;

  public CosNInputStream(Configuration conf, NativeFileSystemStore store,
      FileSystem.Statistics statistics, String key, long fileSize,
      ExecutorService readAheadExecutorService) {
    super();
    this.conf = conf;
    this.store = store;
    this.statistics = statistics;
    this.key = key;
    this.fileSize = fileSize;
    this.preReadPartSize = conf.getLong(
        CosNConfigKeys.READ_AHEAD_BLOCK_SIZE_KEY,
        CosNConfigKeys.DEFAULT_READ_AHEAD_BLOCK_SIZE);
    this.maxReadPartNumber = conf.getInt(
        CosNConfigKeys.READ_AHEAD_QUEUE_SIZE,
        CosNConfigKeys.DEFAULT_READ_AHEAD_QUEUE_SIZE);

    this.readAheadExecutorService = readAheadExecutorService;
    this.readBufferQueue = new ArrayDeque<>(this.maxReadPartNumber);
    this.closed = false;
  }

  private synchronized void reopen(long pos) throws IOException {
    long partSize;

    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    } else if (pos > this.fileSize) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
    } else {
      if (pos + this.preReadPartSize > this.fileSize) {
        partSize = this.fileSize - pos;
      } else {
        partSize = this.preReadPartSize;
      }
    }

    this.buffer = null;

    boolean isRandomIO = true;
    if (pos == this.nextPos) {
      isRandomIO = false;
    } else {
      while (this.readBufferQueue.size() != 0) {
        if (this.readBufferQueue.element().getStart() != pos) {
          this.readBufferQueue.poll();
        } else {
          break;
        }
      }
    }

    this.nextPos = pos + partSize;

    int currentBufferQueueSize = this.readBufferQueue.size();
    long lastByteStart;
    if (currentBufferQueueSize == 0) {
      lastByteStart = pos - partSize;
    } else {
      ReadBuffer[] readBuffers =
          this.readBufferQueue.toArray(
              new ReadBuffer[currentBufferQueueSize]);
      lastByteStart = readBuffers[currentBufferQueueSize - 1].getStart();
    }

    int maxLen = this.maxReadPartNumber - currentBufferQueueSize;
    for (int i = 0; i < maxLen && i < (currentBufferQueueSize + 1) * 2; i++) {
      if (lastByteStart + partSize * (i + 1) > this.fileSize) {
        break;
      }

      long byteStart = lastByteStart + partSize * (i + 1);
      long byteEnd = byteStart + partSize - 1;
      if (byteEnd >= this.fileSize) {
        byteEnd = this.fileSize - 1;
      }

      ReadBuffer readBuffer = new ReadBuffer(byteStart, byteEnd);
      if (readBuffer.getBuffer().length == 0) {
        readBuffer.setStatus(ReadBuffer.SUCCESS);
      } else {
        this.readAheadExecutorService.execute(
            new CosNFileReadTask(
                this.conf, this.key, this.store, readBuffer));
      }

      this.readBufferQueue.add(readBuffer);
      if (isRandomIO) {
        break;
      }
    }

    ReadBuffer readBuffer = this.readBufferQueue.poll();
    if (null != readBuffer) {
      readBuffer.lock();
      try {
        readBuffer.await(ReadBuffer.INIT);
        if (readBuffer.getStatus() == ReadBuffer.ERROR) {
          this.buffer = null;
        } else {
          this.buffer = readBuffer.getBuffer();
        }
      } catch (InterruptedException e) {
        LOG.warn("An interrupted exception occurred "
            + "when waiting a read buffer.");
      } finally {
        readBuffer.unLock();
      }
    }

    if (null == this.buffer) {
      throw new IOException("Null IO stream");
    }

    this.position = pos;
    this.partRemaining = partSize;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    if (pos > this.fileSize) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
    }

    if (this.position == pos) {
      return;
    }
    if (pos > position && pos < this.position + partRemaining) {
      long len = pos - this.position;
      this.position = pos;
      this.partRemaining -= len;
    } else {
      this.reopen(pos);
    }
  }

  @Override
  public long getPos() {
    return this.position;
  }

  @Override
  public boolean seekToNewSource(long targetPos) {
    // Currently does not support to seek the offset of a new source
    return false;
  }

  @Override
  public int read() throws IOException {
    if (this.closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    if (this.partRemaining <= 0 && this.position < this.fileSize) {
      this.reopen(this.position);
    }

    int byteRead = -1;
    if (this.partRemaining != 0) {
      byteRead = this.buffer[
          (int) (this.buffer.length - this.partRemaining)] & 0xff;
    }
    if (byteRead >= 0) {
      this.position++;
      this.partRemaining--;
      if (null != this.statistics) {
        this.statistics.incrementBytesRead(byteRead);
      }
    }

    return byteRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (this.closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    if (len == 0) {
      return 0;
    }

    if (off < 0 || len < 0 || len > b.length) {
      throw new IndexOutOfBoundsException();
    }

    int bytesRead = 0;
    while (position < fileSize && bytesRead < len) {
      if (partRemaining <= 0) {
        reopen(position);
      }

      int bytes = 0;
      for (int i = this.buffer.length - (int) partRemaining;
           i < this.buffer.length; i++) {
        b[off + bytesRead] = this.buffer[i];
        bytes++;
        bytesRead++;
        if (off + bytesRead >= len) {
          break;
        }
      }

      if (bytes > 0) {
        this.position += bytes;
        this.partRemaining -= bytes;
      } else if (this.partRemaining != 0) {
        throw new IOException(
            "Failed to read from stream. Remaining: " + this.partRemaining);
      }
    }
    if (null != this.statistics && bytesRead > 0) {
      this.statistics.incrementBytesRead(bytesRead);
    }

    return bytesRead == 0 ? -1 : bytesRead;
  }

  @Override
  public int available() throws IOException {
    if (this.closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    long remaining = this.fileSize - this.position;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int)remaining;
  }

  @Override
  public void close() {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.buffer = null;
  }
}
