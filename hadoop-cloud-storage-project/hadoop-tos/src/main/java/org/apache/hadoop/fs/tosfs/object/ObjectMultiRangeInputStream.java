/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.FSUtils;
import org.apache.hadoop.fs.tosfs.util.Range;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.util.Preconditions;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class ObjectMultiRangeInputStream extends FSInputStream {
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ExecutorService threadPool;
  private final ObjectStorage storage;
  private final String objectKey;
  private final long contentLength;
  private final long rangeSize;

  private volatile ObjectRangeInputStream stream;
  private volatile long nextPos = 0;
  private volatile long currPos = 0;
  // All range streams should have same checksum.
  private final byte[] checksum;

  public ObjectMultiRangeInputStream(
      ExecutorService threadPool,
      ObjectStorage storage,
      Path path,
      long contentLength,
      long rangeSize,
      byte[] checksum) {
    this(threadPool, storage, ObjectUtils.pathToKey(path), contentLength, rangeSize, checksum);
  }

  public ObjectMultiRangeInputStream(
      ExecutorService threadPool,
      ObjectStorage storage,
      String objectKey,
      long contentLength,
      long rangeSize,
      byte[] checksum) {
    this.threadPool = threadPool;
    this.storage = storage;
    this.objectKey = objectKey;
    this.contentLength = contentLength;
    this.rangeSize = rangeSize;
    this.checksum = checksum;

    Preconditions.checkNotNull(checksum, "Checksum should not be null.");
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + pos);
    }

    if (contentLength <= 0) {
      return;
    }

    nextPos = pos;
  }

  @Override
  public synchronized long getPos() {
    return nextPos;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    checkNotClosed();
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    byte[] buf = new byte[1];
    int n = read(buf, 0, buf.length);
    if (n < 0) {
      return -1;
    } else {
      return buf[0] & 0xFF;
    }
  }

  @Override
  public synchronized int read(byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed();
    FSUtils.checkReadParameters(buffer, offset, length);
    if (length == 0) {
      return 0;
    }

    int total = 0;
    while (total < length) {
      if (contentLength == 0 || nextPos >= contentLength) {
        return total == 0 ? -1 : total;
      }

      seekStream();
      int n = stream.read(buffer, offset, length - total);
      if (n < 0) {
        return total == 0 ? -1 : total;
      }

      total += n;
      offset += n;
      currPos += n;
      nextPos += n;
    }

    return total;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed();
    // Check the arguments, according to the HDFS contract.
    if (position < 0) {
      throw new EOFException("position is negative");
    }
    FSUtils.checkReadParameters(buffer, offset, length);
    if (length == 0) {
      return 0;
    }

    if (contentLength == 0 || position >= contentLength) {
      return -1;
    }

    long remaining = contentLength - position;
    int limit = (remaining >= length) ? length : (int) remaining;

    try (InputStream in = storage.get(objectKey, position, limit).verifiedStream(checksum)) {
      return in.read(buffer, offset, limit);
    }
  }

  private void seekStream() throws IOException {
    if (stream != null && stream.include(nextPos)) {
      // Seek to a random position which is still located in the current range of stream.
      if (nextPos != currPos) {
        stream.seek(nextPos);
        currPos = nextPos;
      }
      return;
    }

    // Seek to a position which is located in another range of new stream.
    currPos = nextPos;
    openStream();
  }

  private void openStream() throws IOException {
    closeStream(true);

    long off = (nextPos / rangeSize) * rangeSize;
    Range range = Range.of(off, Math.min(contentLength - off, rangeSize));
    if (nextPos < range.end()) {
      stream = new ObjectRangeInputStream(storage, objectKey, range, checksum);
      stream.seek(nextPos);
    }
  }

  private void closeStream(boolean asyncClose) throws IOException {
    if (stream != null) {
      if (asyncClose) {
        final ObjectRangeInputStream streamToClose = stream;
        threadPool.submit(() -> CommonUtils.runQuietly(streamToClose::close));
      } else {
        stream.close();
      }
      stream = null;
    }
  }

  private void checkNotClosed() throws IOException {
    if (closed.get()) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    if (closed.compareAndSet(false, true)) {
      closeStream(false);
    }
  }

  // for test
  public long nextExpectPos() {
    return currPos;
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();
    return Ints.saturatedCast(contentLength - nextPos);
  }

  @VisibleForTesting
  ObjectRangeInputStream stream() {
    return stream;
  }
}
