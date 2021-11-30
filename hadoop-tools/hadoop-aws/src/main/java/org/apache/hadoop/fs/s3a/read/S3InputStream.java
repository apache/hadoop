/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.read;

import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.FilePosition;
import org.apache.hadoop.fs.common.Validate;

import com.amazonaws.services.s3.AmazonS3;
import com.twitter.util.FuturePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Provides an {@link InputStream} that allows reading from an S3 file.
 */
public abstract class S3InputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

  // Asynchronous reads are performed using this pool.
  protected FuturePool futurePool;

  // Size of the internal buffer.
  protected int bufferSize;

  // The S3 file read by this instance.
  protected S3File s3File;

  // Name of this stream. Used only for logging.
  protected final String name;

  // Indicates whether the stream has been closed.
  protected volatile boolean closed;

  // Current position within the file.
  protected FilePosition fpos;

  // The target of the most recent seek operation.
  protected long seekTargetPos;

  // Information about each block of the mapped S3 file.
  protected BlockData blockData;

  public S3InputStream(
      FuturePool futurePool,
      int bufferSize,
      String bucket,
      String key,
      long fileSize,
      AmazonS3 client) {

    Validate.checkNotNull(futurePool, "futurePool");
    if (fileSize == 0) {
      Validate.checkNotNegative(bufferSize, "bufferSize");
    } else {
      Validate.checkPositiveInteger(bufferSize, "bufferSize");
    }
    Validate.checkNotNullAndNotEmpty(bucket, "bucket");
    Validate.checkNotNullAndNotEmpty(key, "key");
    Validate.checkNotNull(client, "client");

    this.futurePool = futurePool;
    this.bufferSize = bufferSize;
    this.blockData = new BlockData(fileSize, this.bufferSize);
    this.fpos = new FilePosition(fileSize, bufferSize);
    this.s3File = this.getS3File(client, bucket, key, fileSize);
    this.name = this.s3File.getPath();
    this.seekTargetPos = 0;

    LOG.info("reading: {} (size = {})", this.name, fileSize);
  }

  public String getName() {
    return this.name;
  }

  /**
   * Returns the number of bytes that can read from this stream without blocking.
   */
  @Override
  public int available() {
    if (!ensureCurrentBuffer()) {
      return 0;
    }

    return this.fpos.buffer().remaining();
  }

  public long getPos() throws IOException {
    if (this.fpos.isValid()) {
      return this.fpos.absolute();
    } else {
      return this.seekTargetPos;
    }
  }

  /**
   * Moves the current read position so that the next read will occur at {@code pos}.
   */
  public void seek(long pos) throws IOException {
    Validate.checkWithinRange(pos, "pos", 0, this.s3File.size() - 1);

    if (!this.fpos.setAbsolute(pos)) {
      this.fpos.invalidate();
      this.seekTargetPos = pos;
    }
  }

  /**
   * Ensures that a non-empty valid buffer is available for immediate reading.
   * It returns true when at least one such buffer is available for reading.
   * It returns false on reaching the end of the stream.
   */
  protected abstract boolean ensureCurrentBuffer();

  @Override
  public int read() {
    if (!ensureCurrentBuffer()) {
      return -1;
    }

    this.fpos.incrementBytesRead(1);

    return this.fpos.buffer().get() & 0xff;
  }

  @Override
  public int read(byte[] b) {
    return this.read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) {
    if (!ensureCurrentBuffer()) {
      return -1;
    }

    int numBytesRead = 0;
    int numBytesRemaining = len;

    while (numBytesRemaining > 0) {
      if (!ensureCurrentBuffer()) {
        break;
      }

      ByteBuffer buffer = this.fpos.buffer();
      int bytesToRead = Math.min(numBytesRemaining, buffer.remaining());
      buffer.get(b, off, bytesToRead);
      this.fpos.incrementBytesRead(bytesToRead);
      off += bytesToRead;
      numBytesRemaining -= bytesToRead;
      numBytesRead += bytesToRead;
    }

    return numBytesRead;
  }

  // @VisibleForTesting
  protected S3File getS3File(AmazonS3 client, String bucket, String key, long fileSize) {
    return new S3File(client, bucket, key, fileSize);
  }

  protected String getOffsetStr(long offset) {
    int blockNumber = -1;

    if (this.blockData.isValidOffset(offset)) {
      blockNumber = this.blockData.getBlockNumber(offset);
    }

    return String.format("%d:%d", blockNumber, offset);
  }

  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    this.closed = true;

    this.blockData = null;
    this.s3File = null;
    this.fpos.invalidate();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  // Unsupported functions.

  @Override
  public void mark(int readlimit) {
    throw new UnsupportedOperationException("mark not supported");
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("reset not supported");
  }

  @Override
  public long skip(long n) {
    throw new UnsupportedOperationException("skip not supported");
  }
}
