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

import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.FilePosition;
import org.apache.hadoop.fs.common.Validate;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Provides an {@link InputStream} that allows reading from an S3 file.
 */
public abstract class S3InputStream
    extends InputStream
    implements CanSetReadahead, StreamCapabilities, IOStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

  // The S3 file read by this instance.
  private S3File s3File;

  // Reading of S3 file takes place through this reader.
  private S3Reader reader;

  // Name of this stream. Used only for logging.
  private final String name;

  // Indicates whether the stream has been closed.
  private volatile boolean closed;

  // Current position within the file.
  private FilePosition fpos;

  // The target of the most recent seek operation.
  private long seekTargetPos;

  // Information about each block of the mapped S3 file.
  private BlockData blockData;

  // Read-specific operation context.
  private S3AReadOpContext context;

  // S3 object attributes.
  private S3ObjectAttributes s3Attributes;

  // Callbacks used for interacting with the underlying S3 client.
  private S3AInputStream.InputStreamCallbacks client;

  // Used for reporting input stream access statistics.
  private final S3AInputStreamStatistics streamStatistics;

  private S3AInputPolicy inputPolicy;
  private final ChangeTracker changeTracker;
  private final IOStatistics ioStatistics;

  public S3InputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client) {

    Validate.checkNotNull(context, "context");
    Validate.checkNotNull(s3Attributes, "s3Attributes");
    Validate.checkNotNull(client, "client");

    this.context = context;
    this.s3Attributes = s3Attributes;
    this.client = client;
    this.streamStatistics = context.getS3AStatisticsContext().newInputStreamStatistics();
    this.ioStatistics = streamStatistics.getIOStatistics();
    this.name = S3File.getPath(s3Attributes);
    this.changeTracker = new ChangeTracker(
        this.name,
        context.getChangeDetectionPolicy(),
        this.streamStatistics.getChangeTrackerStatistics(),
        s3Attributes);

    setInputPolicy(context.getInputPolicy());
    setReadahead(context.getReadahead());

    long fileSize = s3Attributes.getLen();
    int bufferSize = context.getPrefetchBlockSize();

    this.blockData = new BlockData(fileSize, bufferSize);
    this.fpos = new FilePosition(fileSize, bufferSize);
    this.s3File = this.getS3File();
    this.reader = new S3Reader(this.s3File);

    this.seekTargetPos = 0;


    LOG.info("reading: {} (size = {})", this.name, fileSize);
  }

  protected S3File getFile() {
    return this.s3File;
  }

  protected S3Reader getReader() {
    return this.reader;
  }

  protected S3ObjectAttributes getS3ObjectAttributes() {
    return this.s3Attributes;
  }

  public FilePosition getFilePosition() {
    return this.fpos;
  }

  public String getName() {
    return this.name;
  }

  public boolean isClosed() {
    return this.closed;
  }

  protected long getSeekTargetPos() {
    return this.seekTargetPos;
  }

  protected void setSeekTargetPos(long pos) {
    this.seekTargetPos = pos;
  }

  protected BlockData getBlockData() {
    return this.blockData;
  }

  protected S3AReadOpContext getContext() {
    return this.context;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return this.ioStatistics;
  }

  @Override
  public synchronized void setReadahead(Long readahead) {
    // We support read head by prefetching therefore we ignore the supplied value.
  }

  @Override
  public boolean hasCapability(String capability) {
    return capability.equalsIgnoreCase(StreamCapabilities.IOSTATISTICS)
        || capability.equalsIgnoreCase(StreamCapabilities.READAHEAD);
  }

  /**
   * Set/update the input policy of the stream.
   * This updates the stream statistics.
   * @param inputPolicy new input policy.
   */
  private void setInputPolicy(S3AInputPolicy inputPolicy) {
    this.inputPolicy = inputPolicy;
    streamStatistics.inputPolicySet(inputPolicy.ordinal());
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
   *
   * @param pos the absolute position to seek to.
   * @throws IOException if there an error during this operation.
   *
   * @throws IllegalArgumentException if pos is outside of the range [0, file size].
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
   *
   * @return true if at least one such buffer is available for reading, false otherwise.
   */
  protected abstract boolean ensureCurrentBuffer();

  @Override
  public int read() {
    if (!ensureCurrentBuffer()) {
      return -1;
    }

    this.incrementBytesRead(1);

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
      this.incrementBytesRead(bytesToRead);
      off += bytesToRead;
      numBytesRemaining -= bytesToRead;
      numBytesRead += bytesToRead;
    }

    return numBytesRead;
  }

  private void incrementBytesRead(int bytesRead) {
    if (bytesRead > 0) {
      this.streamStatistics.bytesRead(bytesRead);
      if (this.getContext().getStats() != null) {
        this.getContext().getStats().incrementBytesRead(bytesRead);
      }
      this.fpos.incrementBytesRead(bytesRead);
    }
  }

  // @VisibleForTesting
  protected S3File getS3File() {
    return new S3File(
        this.context,
        this.s3Attributes,
        this.client,
        this.streamStatistics,
        this.changeTracker
    );
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
