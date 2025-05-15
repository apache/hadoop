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

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.util.FSUtils;
import org.apache.hadoop.fs.tosfs.util.Range;
import org.apache.hadoop.thirdparty.com.google.common.io.ByteStreams;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class ObjectRangeInputStream extends FSInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectRangeInputStream.class);
  private static final int MAX_SKIP_SIZE = 1024 * 1024;

  private final ObjectStorage storage;
  private final String objectKey;
  private final Range range;
  private final byte[] checksum;

  private InputStream stream;
  private long nextPos;
  private long currPos;
  private boolean closed = false;

  public ObjectRangeInputStream(ObjectStorage storage, Path path, Range range, byte[] checksum) {
    this(storage, ObjectUtils.pathToKey(path), range, checksum);
  }

  public ObjectRangeInputStream(
      ObjectStorage storage, String objectKey, Range range, byte[] checksum) {
    this.storage = storage;
    this.objectKey = objectKey;
    this.range = range;
    this.checksum = checksum;

    this.stream = null;
    this.nextPos = range.off();
    this.currPos = nextPos;

    Preconditions.checkNotNull(checksum, "Checksum should not be null.");
  }

  @Override
  public int read() throws IOException {
    byte[] buf = new byte[1];
    int n = read(buf, 0, buf.length);
    if (n < 0) {
      return -1;
    } else {
      return buf[0] & 0xFF;
    }
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed();
    FSUtils.checkReadParameters(buffer, offset, length);

    if (length == 0) {
      return 0;
    }

    if (!range.include(nextPos)) {
      return -1;
    }

    seekStream();

    int toRead = Math.min(length, Ints.saturatedCast(range.end() - nextPos));
    int readLen = stream.read(buffer, offset, toRead);
    if (readLen > 0) {
      nextPos += readLen;
      currPos += readLen;
    }
    return readLen;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closeStream();
    closed = true;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed();

    FSUtils.checkReadParameters(buffer, offset, length);
    if (!range.include(position)) {
      return -1;
    }

    int toRead = Math.min(length, Ints.saturatedCast(range.end() - position));
    if (toRead == 0) {
      return 0;
    }

    try (InputStream in = openStream(position, toRead)) {
      return in.read(buffer, offset, toRead);
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    checkNotClosed();
    Preconditions.checkArgument(range.include(pos), "Position %s must be in range %s", pos, range);
    this.nextPos = pos;
  }

  @Override
  public long getPos() throws IOException {
    checkNotClosed();
    return nextPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    checkNotClosed();
    return false;
  }

  private void seekStream() throws IOException {
    // sequential read
    if (stream != null && nextPos == currPos) {
      return;
    }

    // random read
    if (stream != null && nextPos > currPos) {
      long skip = nextPos - currPos;
      // It is not worth skipping because the skip size is too big, or it can't read any bytes
      // after skip.
      if (skip < MAX_SKIP_SIZE) {
        try {
          ByteStreams.skipFully(stream, skip);
          currPos = nextPos;
          return;
        } catch (IOException ignored) {
          LOG.warn("Failed to skip {} bytes in stream, will try to reopen the stream", skip);
        }
      }
    }

    currPos = nextPos;

    closeStream();
    stream = openStream(nextPos, range.end() - nextPos);
  }

  private InputStream openStream(long offset, long limit) throws IOException {
    return storage.get(objectKey, offset, limit).verifiedStream(checksum);
  }

  private void closeStream() throws IOException {
    if (stream != null) {
      stream.close();
    }
    stream = null;
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  public boolean include(long pos) {
    return range.include(pos);
  }

  public Range range() {
    return range;
  }
}
