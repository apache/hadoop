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

package org.apache.hadoop.fs.s3a.prefetch;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * Enhanced {@code InputStream} for reading from S3.
 *
 * This implementation provides improved read throughput by asynchronously prefetching
 * blocks of configurable size from the underlying S3 file.
 */
public class S3APrefetchingInputStream
    extends FSInputStream
    implements CanSetReadahead, StreamCapabilities, IOStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3APrefetchingInputStream.class);

  /**
   * Underlying input stream used for reading S3 file.
   */
  private S3ARemoteInputStream inputStream;

  /**
   * To be only used by synchronized getPos().
   */
  private long lastReadCurrentPos = 0;

  /**
   * To be only used by getIOStatistics().
   */
  private IOStatistics ioStatistics = null;

  /**
   * To be only used by getS3AStreamStatistics().
   */
  private S3AInputStreamStatistics inputStreamStatistics = null;

  /**
   * Initializes a new instance of the {@code S3APrefetchingInputStream} class.
   *
   * @param context read-specific operation context.
   * @param s3Attributes attributes of the S3 object being read.
   * @param client callbacks used for interacting with the underlying S3 client.
   * @param streamStatistics statistics for this stream.
   * @param conf the configuration.
   * @param localDirAllocator the local dir allocator instance retrieved from S3A FS.
   * @throws IllegalArgumentException if context is null.
   * @throws IllegalArgumentException if s3Attributes is null.
   * @throws IllegalArgumentException if client is null.
   */
  public S3APrefetchingInputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client,
      S3AInputStreamStatistics streamStatistics,
      Configuration conf,
      LocalDirAllocator localDirAllocator) {

    Validate.checkNotNull(context, "context");
    Validate.checkNotNull(s3Attributes, "s3Attributes");
    Validate.checkNotNullAndNotEmpty(s3Attributes.getBucket(),
        "s3Attributes.getBucket()");
    Validate.checkNotNullAndNotEmpty(s3Attributes.getKey(),
        "s3Attributes.getKey()");
    Validate.checkNotNegative(s3Attributes.getLen(), "s3Attributes.getLen()");
    Validate.checkNotNull(client, "client");
    Validate.checkNotNull(streamStatistics, "streamStatistics");

    long fileSize = s3Attributes.getLen();
    if (fileSize <= context.getPrefetchBlockSize()) {
      LOG.debug("Creating in memory input stream for {}", context.getPath());
      this.inputStream = new S3AInMemoryInputStream(
          context,
          s3Attributes,
          client,
          streamStatistics);
    } else {
      LOG.debug("Creating in caching input stream for {}", context.getPath());
      this.inputStream = new S3ACachingInputStream(
          context,
          s3Attributes,
          client,
          streamStatistics,
          conf,
          localDirAllocator);
    }
  }

  /**
   * Returns the number of bytes available for reading without blocking.
   *
   * @return the number of bytes available for reading without blocking.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized int available() throws IOException {
    throwIfClosed();
    return inputStream.available();
  }

  /**
   * Gets the current position. If the underlying S3 input stream is closed,
   * it returns last read current position from the underlying steam. If the
   * current position was never read and the underlying input stream is closed,
   * this would return 0.
   *
   * @return the current position.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized long getPos() throws IOException {
    if (!isClosed()) {
      lastReadCurrentPos = inputStream.getPos();
    }
    return lastReadCurrentPos;
  }

  /**
   * Reads and returns one byte from this stream.
   *
   * @return the next byte from this stream.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized int read() throws IOException {
    throwIfClosed();
    return inputStream.read();
  }

  /**
   * Reads up to {@code len} bytes from this stream and copies them into
   * the given {@code buffer} starting at the given {@code offset}.
   * Returns the number of bytes actually copied in to the given buffer.
   *
   * @param buffer the buffer to copy data into.
   * @param offset data is copied starting at this offset.
   * @param len max number of bytes to copy.
   * @return the number of bytes actually copied in to the given buffer.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized int read(byte[] buffer, int offset, int len)
      throws IOException {
    throwIfClosed();
    return inputStream.read(buffer, offset, len);
  }

  /**
   * Closes this stream and releases all acquired resources.
   *
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
      super.close();
    }
  }

  /**
   * Updates internal data such that the next read will take place at the given {@code pos}.
   *
   * @param pos new read position.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    throwIfClosed();
    inputStream.seek(pos);
  }

  /**
   * Sets the number of bytes to read ahead each time.
   *
   * @param readahead the number of bytes to read ahead each time..
   */
  @Override
  public synchronized void setReadahead(Long readahead) {
    if (!isClosed()) {
      inputStream.setReadahead(readahead);
    }
  }

  /**
   * Indicates whether the given {@code capability} is supported by this stream.
   *
   * @param capability the capability to check.
   * @return true if the given {@code capability} is supported by this stream, false otherwise.
   */
  @Override
  public boolean hasCapability(String capability) {
    if (!isClosed()) {
      return inputStream.hasCapability(capability);
    }

    return false;
  }

  /**
   * Access the input stream statistics.
   * This is for internal testing and may be removed without warning.
   * @return the statistics for this input stream
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  public S3AInputStreamStatistics getS3AStreamStatistics() {
    if (!isClosed()) {
      inputStreamStatistics = inputStream.getS3AStreamStatistics();
    }
    return inputStreamStatistics;
  }

  /**
   * Gets the internal IO statistics.
   *
   * @return the internal IO statistics.
   */
  @Override
  public IOStatistics getIOStatistics() {
    if (!isClosed()) {
      ioStatistics = inputStream.getIOStatistics();
    }
    return ioStatistics;
  }

  protected boolean isClosed() {
    return inputStream == null;
  }

  protected void throwIfClosed() throws IOException {
    if (isClosed()) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  // Unsupported functions.

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
