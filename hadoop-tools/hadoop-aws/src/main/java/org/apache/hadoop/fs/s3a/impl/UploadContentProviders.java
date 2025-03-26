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

package org.apache.hadoop.fs.s3a.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.ContentStreamProvider;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.store.ByteBufferInputStream;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.Preconditions.checkState;
import static org.apache.hadoop.util.functional.FunctionalIO.uncheckIOExceptions;

/**
 * Implementations of {@code software.amazon.awssdk.http.ContentStreamProvider}.
 * <p>
 * These are required to ensure that retry of multipart uploads are reliable,
 * while also avoiding memory copy/consumption overhead.
 * <p>
 * For these reasons the providers built in to the AWS SDK are not used.
 * <p>
 * See HADOOP-19221 for details.
 */
public final class UploadContentProviders {

  public static final Logger LOG = LoggerFactory.getLogger(UploadContentProviders.class);

  private UploadContentProviders() {
  }

  /**
   * Create a content provider from a file.
   * @param file file to read.
   * @param offset offset in file.
   * @param size of data.
   * @return the provider
   * @throws IllegalArgumentException if the offset is negative.
   */
  public static BaseContentProvider<BufferedInputStream> fileContentProvider(
      File file,
      long offset,
      final int size) {

    return new FileWithOffsetContentProvider(file, offset, size);
  }

  /**
   * Create a content provider from a file.
   * @param file file to read.
   * @param offset offset in file.
   * @param size of data.
   * @param isOpen optional predicate to check if the stream is open.
   * @return the provider
   * @throws IllegalArgumentException if the offset is negative.
   */
  public static BaseContentProvider<BufferedInputStream> fileContentProvider(
      File file,
      long offset,
      final int size,
      final Supplier<Boolean> isOpen) {

    return new FileWithOffsetContentProvider(file, offset, size, isOpen);
  }

  /**
   * Create a content provider from a byte buffer.
   * The buffer is not copied and MUST NOT be modified while
   * the upload is taking place.
   * @param byteBuffer buffer to read.
   * @param size size of the data.
   * @return the provider
   * @throws IllegalArgumentException if the arguments are invalid.
   * @throws NullPointerException if the buffer is null
   */
  public static BaseContentProvider<ByteBufferInputStream> byteBufferContentProvider(
      final ByteBuffer byteBuffer,
      final int size) {
    return new ByteBufferContentProvider(byteBuffer, size);
  }

  /**
   * Create a content provider from a byte buffer.
   * The buffer is not copied and MUST NOT be modified while
   * the upload is taking place.
   * @param byteBuffer buffer to read.
   * @param size size of the data.
   * @param isOpen optional predicate to check if the stream is open.
   * @return the provider
   * @throws IllegalArgumentException if the arguments are invalid.
   * @throws NullPointerException if the buffer is null
   */
  public static BaseContentProvider<ByteBufferInputStream> byteBufferContentProvider(
      final ByteBuffer byteBuffer,
      final int size,
      final @Nullable Supplier<Boolean> isOpen) {

    return new ByteBufferContentProvider(byteBuffer, size, isOpen);
  }

  /**
   * Create a content provider for all or part of a byte array.
   * The buffer is not copied and MUST NOT be modified while
   * the upload is taking place.
   * @param bytes buffer to read.
   * @param offset offset in buffer.
   * @param size size of the data.
   * @return the provider
   * @throws IllegalArgumentException if the arguments are invalid.
   * @throws NullPointerException if the buffer is null.
   */
  public static BaseContentProvider<ByteArrayInputStream> byteArrayContentProvider(
      final byte[] bytes, final int offset, final int size) {
    return new ByteArrayContentProvider(bytes, offset, size);
  }

  /**
   * Create a content provider for all or part of a byte array.
   * The buffer is not copied and MUST NOT be modified while
   * the upload is taking place.
   * @param bytes buffer to read.
   * @param offset offset in buffer.
   * @param size size of the data.
   * @param isOpen optional predicate to check if the stream is open.
   * @return the provider
   * @throws IllegalArgumentException if the arguments are invalid.
   * @throws NullPointerException if the buffer is null.
   */
  public static BaseContentProvider<ByteArrayInputStream> byteArrayContentProvider(
      final byte[] bytes,
      final int offset,
      final int size,
      final @Nullable Supplier<Boolean> isOpen) {
    return new ByteArrayContentProvider(bytes, offset, size, isOpen);
  }

  /**
   * Create a content provider for all of a byte array.
   * @param bytes buffer to read.
   * @return the provider
   * @throws IllegalArgumentException if the arguments are invalid.
   * @throws NullPointerException if the buffer is null.
   */
  public static BaseContentProvider<ByteArrayInputStream> byteArrayContentProvider(
      final byte[] bytes) {
    return byteArrayContentProvider(bytes, 0, bytes.length);
  }

  /**
   * Create a content provider for all of a byte array.
   * @param bytes buffer to read.
   * @param isOpen optional predicate to check if the stream is open.
   * @return the provider
   * @throws IllegalArgumentException if the arguments are invalid.
   * @throws NullPointerException if the buffer is null.
   */
  public static BaseContentProvider<ByteArrayInputStream> byteArrayContentProvider(
      final byte[] bytes,
      final @Nullable Supplier<Boolean> isOpen) {
    return byteArrayContentProvider(bytes, 0, bytes.length, isOpen);
  }

  /**
   * Base class for content providers; tracks the number of times a stream
   * has been opened.
   * @param <T> type of stream created.
   */
  @VisibleForTesting
  public static abstract class BaseContentProvider<T extends InputStream>
      implements ContentStreamProvider, Closeable {

    /**
     * Size of the data.
     */
    private final int size;

    /**
     * Probe to check if the stream is open.
     * Invoked in {@link #checkOpen()}, which is itself
     * invoked in {@link #newStream()}.
     */
    private final Supplier<Boolean> isOpen;

    /**
     * How many times has a stream been created?
     */
    private int streamCreationCount;

    /**
     * Current stream. Null if not opened yet.
     * When {@link #newStream()} is called, this is set to the new value,
     * Note: when the input stream itself is closed, this reference is not updated.
     * Therefore this field not being null does not imply that the stream is open.
     */
    private T currentStream;

    /**
     * When did this upload start?
     * Use in error messages.
     */
    private final LocalDateTime startTime;

    /**
     * Constructor.
     * @param size size of the data. Must be non-negative.
     */
    protected BaseContentProvider(int size) {
      this(size, null);
    }

    /**
     * Constructor.
     * @param size size of the data. Must be non-negative.
     * @param isOpen optional predicate to check if the stream is open.
     */
    protected BaseContentProvider(int size, @Nullable Supplier<Boolean> isOpen) {
      checkArgument(size >= 0, "size is negative: %s", size);
      this.size = size;
      this.isOpen = isOpen;
      this.startTime = LocalDateTime.now();
    }

    /**
     * Check if the stream is open.
     * If the stream is not open, raise an exception
     * @throws IllegalStateException if the stream is not open.
     */
    private void checkOpen() {
      checkState(isOpen == null || isOpen.get(), "Stream is closed: %s", this);
    }

    /**
     * Close the current stream.
     */
    @Override
    public void close() {
      cleanupWithLogger(LOG, getCurrentStream());
      setCurrentStream(null);
    }

    /**
     * Create a new stream.
     * <p>
     * Calls {@link #close()} to ensure that any existing stream is closed,
     * then {@link #checkOpen()} to verify that the data source is still open.
     * Logs if this is a subsequent event as it implies a failure of the first attempt.
     * @return the new stream
     */
    @Override
    public final InputStream newStream() {
      close();
      checkOpen();
      streamCreationCount++;
      if (streamCreationCount == 2) {
        // the stream has been recreated for the first time.
        // notify only once for this stream, so as not to flood
        // the logs.
        // originally logged at info; logs at debug because HADOOP-19516
        // means that this message is very common with S3 Express stores.
        LOG.debug("Stream recreated: {}", this);
      }
      return setCurrentStream(createNewStream());
    }

    /**
     * Override point for subclasses to create their new streams.
     * @return a stream
     */
    protected abstract T createNewStream();

    /**
     * How many times has a stream been created?
     * @return stream creation count
     */
    public int getStreamCreationCount() {
      return streamCreationCount;
    }

    /**
     * Size as set by constructor parameter.
     * @return size of the data
     */
    public int getSize() {
      return size;
    }

    /**
     * When did this upload start?
     * @return start time
     */
    public LocalDateTime getStartTime() {
      return startTime;
    }

    /**
     * Current stream.
     * When {@link #newStream()} is called, this is set to the new value,
     * after closing the previous one.
     * <p>
     * Why? The AWS SDK implementations do this, so there
     * is an implication that it is needed to avoid keeping streams
     * open on retries.
     * @return the current stream, or null if none is open.
     */
    protected T getCurrentStream() {
      return currentStream;
    }

    /**
     * Set the current stream.
     * @param stream the new stream
     * @return the current stream.
     */
    protected T setCurrentStream(T stream) {
      this.currentStream = stream;
      return stream;
    }

    @Override
    public String toString() {
      return "BaseContentProvider{" +
          "size=" + size +
          ", initiated at " + startTime +
          ", streamCreationCount=" + streamCreationCount +
          ", currentStream=" + currentStream +
          '}';
    }
  }

  /**
   * Content provider for a file with an offset.
   */
  private static final class FileWithOffsetContentProvider
      extends BaseContentProvider<BufferedInputStream> {

    /**
     * File to read.
     */
    private final File file;

    /**
     * Offset in file.
     */
    private final long offset;

    /**
     * Constructor.
     * @param file file to read.
     * @param offset offset in file.
     * @param size of data.
     * @param isOpen optional predicate to check if the stream is open.
     * @throws IllegalArgumentException if the offset is negative.
     */
    private FileWithOffsetContentProvider(
        final File file,
        final long offset,
        final int size,
        @Nullable final Supplier<Boolean> isOpen) {
      super(size, isOpen);
      this.file = requireNonNull(file);
      checkArgument(offset >= 0, "Offset is negative: %s", offset);
      this.offset = offset;
    }

    /**
     * Constructor.
     * @param file file to read.
     * @param offset offset in file.
     * @param size of data.
     * @throws IllegalArgumentException if the offset is negative.
     */
    private FileWithOffsetContentProvider(final File file,
        final long offset,
        final int size) {
      this(file, offset, size, null);
    }

    /**
     * Create a new stream.
     * @return a stream at the start of the offset in the file
     * @throws UncheckedIOException on IO failure.
     */
    @Override
    protected BufferedInputStream createNewStream() throws UncheckedIOException {
      // create the stream, seek to the offset.
      final FileInputStream fis = uncheckIOExceptions(() -> {
        final FileInputStream f = new FileInputStream(file);
        f.getChannel().position(offset);
        return f;
      });
      return setCurrentStream(new BufferedInputStream(fis));
    }

    @Override
    public String toString() {
      return "FileWithOffsetContentProvider{" +
          "file=" + file +
          ", offset=" + offset +
          "} " + super.toString();
    }

  }

  /**
   * Create a content provider for a byte buffer.
   * Uses {@link ByteBufferInputStream} to read the data.
   */
  private static final class ByteBufferContentProvider
      extends BaseContentProvider<ByteBufferInputStream> {

    /**
     * The buffer which will be read; on or off heap.
     */
    private final ByteBuffer blockBuffer;

    /**
     * The position in the buffer at the time the provider was created.
     */
    private final int initialPosition;

    /**
     * Constructor.
     * @param blockBuffer buffer to read.
     * @param size size of the data.
     * @throws IllegalArgumentException if the arguments are invalid.
     * @throws NullPointerException if the buffer is null
     */
    private ByteBufferContentProvider(final ByteBuffer blockBuffer, int size) {
      this(blockBuffer, size, null);
    }

    /**
     * Constructor.
     * @param blockBuffer buffer to read.
     * @param size size of the data.
     * @param isOpen optional predicate to check if the stream is open.
     * @throws IllegalArgumentException if the arguments are invalid.
     * @throws NullPointerException if the buffer is null
     */
    private ByteBufferContentProvider(
        final ByteBuffer blockBuffer,
        int size,
        @Nullable final Supplier<Boolean> isOpen) {
      super(size, isOpen);
      this.blockBuffer = blockBuffer;
      this.initialPosition = blockBuffer.position();
    }

    @Override
    protected ByteBufferInputStream createNewStream() {
      // set the buffer up from reading from the beginning
      blockBuffer.limit(initialPosition);
      blockBuffer.position(0);
      return new ByteBufferInputStream(getSize(), blockBuffer);
    }

    @Override
    public String toString() {
      return "ByteBufferContentProvider{" +
          "blockBuffer=" + blockBuffer +
          ", initialPosition=" + initialPosition +
          "} " + super.toString();
    }
  }

  /**
   * Simple byte array content provider.
   * <p>
   * The array is not copied; if it is changed during the write the outcome
   * of the upload is undefined.
   */
  private static final class ByteArrayContentProvider
      extends BaseContentProvider<ByteArrayInputStream> {

    /**
     * The buffer where data is stored.
     */
    private final byte[] bytes;

    /**
     * Offset in the buffer.
     */
    private final int offset;

    /**
     * Constructor.
     * @param bytes buffer to read.
     * @param offset offset in buffer.
     * @param size length of the data.
     * @throws IllegalArgumentException if the arguments are invalid.
     * @throws NullPointerException if the buffer is null
     */
    private ByteArrayContentProvider(
        final byte[] bytes,
        final int offset,
        final int size) {
      this(bytes, offset, size, null);
    }

    /**
     * Constructor.
     * @param bytes buffer to read.
     * @param offset offset in buffer.
     * @param size length of the data.
     * @param isOpen optional predicate to check if the stream is open.
     * @throws IllegalArgumentException if the arguments are invalid.
     * @throws NullPointerException if the buffer is null
     */
    private ByteArrayContentProvider(
        final byte[] bytes,
        final int offset,
        final int size,
        final Supplier<Boolean> isOpen) {

      super(size, isOpen);
      this.bytes = bytes;
      this.offset = offset;
      checkArgument(offset >= 0, "Offset is negative: %s", offset);
      final int length = bytes.length;
      checkArgument((offset + size) <= length,
          "Data to read [%d-%d] is past end of array %s",
          offset,
          offset + size, length);
    }

    @Override
    protected ByteArrayInputStream createNewStream() {
      return new ByteArrayInputStream(bytes, offset, getSize());
    }

    @Override
    public String toString() {
      return "ByteArrayContentProvider{" +
          "buffer with length=" + bytes.length +
          ", offset=" + offset +
          "} " + super.toString();
    }
  }

}
