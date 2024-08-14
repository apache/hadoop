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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.impl.prefetch.BlockData;
import org.apache.hadoop.fs.impl.prefetch.FilePosition;
import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static java.util.Objects.requireNonNull;

/**
 * Provides an {@link InputStream} that allows reading from an S3 file.
 */
public abstract class S3ARemoteInputStream
    extends FSInputStream
    implements CanSetReadahead, StreamCapabilities, IOStatisticsSource, CanUnbuffer {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ARemoteInputStream.class);

  /**
   * The S3 file read by this instance.
   */
  private S3ARemoteObject remoteObject;

  /**
   * Reading of S3 file takes place through this reader.
   */
  private S3ARemoteObjectReader reader;

  /**
   * Name of this stream. Used only for logging.
   */
  private final String name;

  /**
   * Indicates whether the stream has been closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Indicates whether resources have been freed, used when the stream is closed
   * via unbuffer.
   */
  private final AtomicBoolean underlyingResourcesClosed = new AtomicBoolean(true);

  /**
   * Internal position within the file. Updated lazily
   * after a seek before a read.
   */
  private FilePosition fpos;

  /**
   * This is the actual position within the file, used by
   * lazy seek to decide whether to seek on the next read or not.
   */
  private long nextReadPos;

  /** Information about each block of the mapped S3 file. */
  private BlockData blockData;

  /** Read-specific operation context. */
  private S3AReadOpContext context;

  /** Attributes of the S3 object being read. */
  private S3ObjectAttributes s3Attributes;

  /** Callbacks used for interacting with the underlying S3 client. */
  private S3AInputStream.InputStreamCallbacks client;

  /** Used for reporting input stream access statistics. */
  private final S3AInputStreamStatistics streamStatistics;

  private S3AInputPolicy inputPolicy;

  private final ChangeTracker changeTracker;

  /**
   * IOStatistics.
   */
  private final IOStatistics ioStatistics;

  /** Aggregator used to aggregate per thread IOStatistics. */
  private final IOStatisticsAggregator threadIOStatistics;

  /**
   * Initializes a new instance of the {@code S3ARemoteInputStream} class.
   *
   * @param context read-specific operation context.
   * @param s3Attributes attributes of the S3 object being read.
   * @param client callbacks used for interacting with the underlying S3 client.
   * @param streamStatistics statistics for this stream.
   *
   * @throws IllegalArgumentException if context is null.
   * @throws IllegalArgumentException if s3Attributes is null.
   * @throws IllegalArgumentException if client is null.
   */
  public S3ARemoteInputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client,
      S3AInputStreamStatistics streamStatistics) {

    this.context = requireNonNull(context);
    this.s3Attributes = requireNonNull(s3Attributes);
    this.client = requireNonNull(client);
    this.streamStatistics = requireNonNull(streamStatistics);
    this.ioStatistics = streamStatistics.getIOStatistics();
    this.name = context.getPath().toUri().toString();
    this.changeTracker = new ChangeTracker(
        this.name,
        context.getChangeDetectionPolicy(),
        this.streamStatistics.getChangeTrackerStatistics(),
        s3Attributes);
    this.threadIOStatistics = requireNonNull(context.getIOStatisticsAggregator());
    this.nextReadPos = 0;

    setInputPolicy(context.getInputPolicy());
    setReadahead(context.getReadahead());

    initializeUnderlyingResources();
  }
  /**
   * Initializes those resources that the stream uses but are released during unbuffer.
   */
  protected final void initializeUnderlyingResources() {
    underlyingResourcesClosed.set(false);
    long fileSize = s3Attributes.getLen();
    int bufferSize = context.getPrefetchBlockSize();
    remoteObject = getS3File();
    reader = new S3ARemoteObjectReader(remoteObject);
    blockData = new BlockData(fileSize, bufferSize);
    fpos = new FilePosition(fileSize, bufferSize);
  }

  /**
   * Gets the internal IO statistics.
   *
   * @return the internal IO statistics.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return ioStatistics;
  }

  /**
   * Access the input stream statistics.
   * This is for internal testing and may be removed without warning.
   * @return the statistics for this input stream
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public S3AInputStreamStatistics getS3AStreamStatistics() {
    return streamStatistics;
  }

  /**
   * Sets the number of bytes to read ahead each time.
   *
   * @param readahead the number of bytes to read ahead each time..
   */
  @Override
  public final void setReadahead(Long readahead) {
    // We support read head by prefetching therefore we ignore the supplied value.
    if (readahead != null) {
      Validate.checkNotNegative(readahead, "readahead");
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
    return capability.equalsIgnoreCase(StreamCapabilities.IOSTATISTICS)
        || capability.equalsIgnoreCase(StreamCapabilities.READAHEAD)
        || capability.equalsIgnoreCase(StreamCapabilities.UNBUFFER);
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
  public int available() throws IOException {
    throwIfClosed();

    // buffer is closed.
    if (underlyingResourcesClosed.get()) {
      return 0;
    }

    // Update the current position in the current buffer, if possible.
    if (!fpos.setAbsolute(nextReadPos)) {
      return 0;
    }

    return fpos.buffer().remaining();
  }

  /**
   * Gets the current position.
   *
   * @return the current position.
   * @throws IOException if there is an IO error during this operation.
   */
  public long getPos() throws IOException {
    throwIfClosed();

    return nextReadPos;
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
    throwIfClosed();
    throwIfInvalidSeek(pos);

    nextReadPos = pos;
  }

  /**
   * Ensures that a non-empty valid buffer is available for immediate reading.
   * It returns true when at least one such buffer is available for reading.
   * It returns false on reaching the end of the stream.
   *
   * @return true if at least one such buffer is available for reading, false otherwise.
   * @throws IOException if there is an IO error during this operation.
   */
  protected abstract boolean ensureCurrentBuffer() throws IOException;

  @Override
  public int read() throws IOException {
    throwIfClosed();

    if (remoteObject.size() == 0) {
      LOG.debug("Rejecting read on empty file");
      return -1;
    }

    if (nextReadPos >= remoteObject.size()) {
      LOG.debug("Rejecting read past EOF");
      return -1;
    }

    if (!ensureCurrentBuffer()) {
      LOG.debug("Empty buffer in cache");
      return -1;
    }

    nextReadPos++;
    incrementBytesRead(1);

    return fpos.buffer().get() & 0xff;
  }

  /**
   * Reads bytes from this stream and copies them into
   * the given {@code buffer} starting at the beginning (offset 0).
   * Returns the number of bytes actually copied in to the given buffer.
   *
   * @param buffer the buffer to copy data into.
   * @return the number of bytes actually copied in to the given buffer.
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public int read(byte[] buffer) throws IOException {
    return read(buffer, 0, buffer.length);
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
  public int read(byte[] buffer, int offset, int len) throws IOException {
    throwIfClosed();
    validatePositionedReadArgs(nextReadPos, buffer, offset, len);
    if (len == 0) {
      return 0;
    }

    if (remoteObject.size() == 0) {
      LOG.debug("Rejecting read on empty file");
      return -1;
    }

    if (nextReadPos >= remoteObject.size()) {
      LOG.debug("Rejecting read past EOF");
      return -1;
    }

    if (!ensureCurrentBuffer()) {
      LOG.debug("Empty buffer in cache");
      return -1;
    }

    int numBytesRead = 0;
    int numBytesRemaining = len;

    while (numBytesRemaining > 0) {
      if (!ensureCurrentBuffer()) {
        break;
      }

      ByteBuffer buf = fpos.buffer();
      int bytesToRead = Math.min(numBytesRemaining, buf.remaining());
      buf.get(buffer, offset, bytesToRead);
      nextReadPos += bytesToRead;
      incrementBytesRead(bytesToRead);
      offset += bytesToRead;
      numBytesRemaining -= bytesToRead;
      numBytesRead += bytesToRead;
    }
    return numBytesRead;
  }

  /**
   * Forward to superclass after updating the {@code readFully()} IOStatistics.
   * {@inheritDoc}
   */
  @Override
  public void readFully(final long position,
      final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    throwIfClosed();
    validatePositionedReadArgs(position, buffer, offset, length);
    streamStatistics.readFullyOperationStarted(position, length);
    super.readFully(position, buffer, offset, length);
  }

  protected final S3ARemoteObject getRemoteObject() {
    return remoteObject;
  }

  protected final S3ARemoteObjectReader getReader() {
    return reader;
  }

  protected final S3ObjectAttributes getS3ObjectAttributes() {
    return s3Attributes;
  }

  protected final FilePosition getFilePosition() {
    return fpos;
  }

  protected final String getName() {
    return name;
  }

  protected final boolean isClosed() {
    return closed.get();
  }

  protected final boolean underlyingResourcesClosed() {
    return underlyingResourcesClosed.get();
  }

  protected final long getNextReadPos() {
    return nextReadPos;
  }

  protected final BlockData getBlockData() {
    return blockData;
  }

  protected final S3AReadOpContext getContext() {
    return context;
  }

  private void incrementBytesRead(int bytesRead) {
    if (bytesRead > 0) {
      streamStatistics.bytesReadFromBuffer(bytesRead);
      if (getContext().getStats() != null) {
        getContext().getStats().incrementBytesRead(bytesRead);
      }
      fpos.incrementBytesRead(bytesRead);
    }
  }

  protected S3ARemoteObject getS3File() {
    return new S3ARemoteObject(
        context,
        s3Attributes,
        client,
        streamStatistics,
        changeTracker
    );
  }

  /**
   * Get string info on offset, mapping to block number:offset.
   * @param offset absolute position
   * @return a string of block number and offset, with a block number of "-1:"
   *          if the offset is invalid.
   */
  protected String getOffsetStr(long offset) {
    int blockNumber = -1;

    if (blockData.isValidOffset(offset)) {
      blockNumber = blockData.getBlockNumber(offset);
    }

    return String.format("%d:%d", blockNumber, offset);
  }

  @Override
  public synchronized void unbuffer() {
    LOG.debug("{}: unbuffered", getName());
    if (closeStream(true)) {
      getS3AStreamStatistics().unbuffered();
    }
  }

  /**
   * Close the stream in close() or unbuffer().
   * @param unbuffer is this an unbuffer operation?
   * @return true if the stream was closed; false means it was already closed.
   */
  protected boolean closeStream(final boolean unbuffer) {

    if (underlyingResourcesClosed.getAndSet(true)) {
      return false;
    }

    // release all the blocks
    blockData = null;

    reader.close();
    reader = null;
    // trigger GC.
    remoteObject = null;
    fpos.invalidate();
    return true;
  }

  /**
   * Closes this stream and releases all acquired resources.
   *
   * @throws IOException if there is an IO error during this operation.
   */
  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      return;
    }
    closeStream(false);

    try {
      client.close();
    } finally {
      streamStatistics.close();
      // Collect ThreadLevel IOStats
      threadIOStatistics.aggregate(streamStatistics.getIOStatistics());
    }
    client = null;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public String toString() {
    if (isClosed()) {
      return "closed";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("nextReadPos = (%d)%n", nextReadPos));
    sb.append(String.format("fpos = (%s)", fpos));
    return sb.toString();
  }

  protected void throwIfClosed() throws IOException {
    if (closed.get()) {
      throw new IOException(
          name + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  protected void throwIfInvalidSeek(long pos) throws EOFException {
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + pos);
    } else if (pos > this.getBlockData().getFileSize()) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF + " " + pos);
    }
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
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}
