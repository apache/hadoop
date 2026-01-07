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

package org.apache.hadoop.fs.gs;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkState;
import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.nullToEmpty;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.apache.hadoop.fs.gs.GoogleCloudStorageExceptions.createFileNotFoundException;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/** Provides seekable read access to GCS via java-storage library. */
class GoogleCloudStorageClientReadChannel implements SeekableByteChannel {
  private static final Logger LOG =
      LoggerFactory.getLogger(GoogleCloudStorageClientReadChannel.class);
  private static final String GZIP_ENCODING = "gzip";

  private final StorageResourceId resourceId;
  private final Storage storage;
  private final GoogleHadoopFileSystemConfiguration config;

  // The size of this object generation, in bytes.
  private long objectSize;
  private ContentReadChannel contentReadChannel;
  private boolean gzipEncoded = false;
  private boolean open = true;

  // Current position in this channel, it could be different from contentChannelCurrentPosition if
  // position(long) method calls were made without calls to read(ByteBuffer) method.
  private long currentPosition = 0;

  GoogleCloudStorageClientReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleHadoopFileSystemConfiguration config)
      throws IOException {
    validate(itemInfo);
    this.storage = storage;
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    this.contentReadChannel = new ContentReadChannel(config, resourceId);
    initMetadata(itemInfo.getContentEncoding(), itemInfo.getSize());
    this.config = config;
  }

  protected void initMetadata(@Nullable String encoding, long sizeFromMetadata) throws IOException {
    gzipEncoded = nullToEmpty(encoding).contains(GZIP_ENCODING);
    if (gzipEncoded && !config.isGzipEncodingSupportEnabled()) {
      throw new IOException(
          "Cannot read GZIP encoded files - content encoding support is disabled.");
    }
    objectSize = gzipEncoded ? Long.MAX_VALUE : sizeFromMetadata;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();

    // Don't try to read if the buffer has no space.
    if (dst.remaining() == 0) {
      return 0;
    }
    LOG.trace(
        "Reading {} bytes at {} position from '{}'", dst.remaining(), currentPosition, resourceId);
    if (currentPosition == objectSize) {
      return -1;
    }
    return contentReadChannel.readContent(dst);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    return currentPosition;
  }

  /**
   * Sets this channel's position.
   *
   * <p>This method will throw an exception if {@code newPosition} is greater than object size,
   * which contradicts {@link SeekableByteChannel#position(long) SeekableByteChannel} contract.
   * TODO(user): decide if this needs to be fixed.
   *
   * @param newPosition the new position, counting the number of bytes from the beginning.
   * @return this channel instance
   * @throws FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();

    if (newPosition == currentPosition) {
      return this;
    }

    validatePosition(newPosition);
    LOG.trace(
        "Seek from {} to {} position for '{}'", currentPosition, newPosition, resourceId);
    currentPosition = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    return objectSize;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    if (open) {
      try {
        LOG.trace("Closing channel for '{}'", resourceId);
        contentReadChannel.closeContentChannel();
      } catch (Exception e) {
        throw new IOException(
            String.format("Exception occurred while closing channel '%s'", resourceId), e);
      } finally {
        contentReadChannel = null;
        open = false;
      }
    }
  }

  /**
   * This class own the responsibility of opening up contentChannel. It also implements the Fadvise,
   * which helps in deciding the boundaries of content channel being opened and also caching the
   * footer of an object.
   */
  private class ContentReadChannel {

    // Size of buffer to allocate for skipping bytes in-place when performing in-place seeks.
    private static final int SKIP_BUFFER_SIZE = 8192;
    private final BlobId blobId;

    // This is the actual current position in `contentChannel` from where read can happen.
    // This remains unchanged of position(long) method call.
    private long contentChannelCurrentPosition = -1;
    private long contentChannelEnd = -1;
    // Prefetched footer content.
    private byte[] footerContent;
    // Used as scratch space when reading bytes just to discard them when trying to perform small
    // in-place seeks.
    private byte[] skipBuffer = null;
    private ReadableByteChannel byteChannel = null;
    private final FileAccessPatternManager fileAccessManager;

    ContentReadChannel(
        GoogleHadoopFileSystemConfiguration config, StorageResourceId resourceId) {
      this.blobId =
          BlobId.of(
              resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
      this.fileAccessManager = new FileAccessPatternManager(resourceId, config);
      if (gzipEncoded) {
        fileAccessManager.overrideAccessPattern(false);
      }
    }

    int readContent(ByteBuffer dst) throws IOException {

      performPendingSeeks();

      checkState(
          contentChannelCurrentPosition == currentPosition || byteChannel == null,
          "contentChannelCurrentPosition (%s) should be equal to currentPosition "
              + "(%s) after lazy seek, if channel is open",
          contentChannelCurrentPosition,
          currentPosition);

      int totalBytesRead = 0;
      // We read from a streaming source. We may not get all the bytes we asked for
      // in the first read. Therefore, loop till we either read the required number of
      // bytes or we reach end-of-stream.
      while (dst.hasRemaining()) {
        int remainingBeforeRead = dst.remaining();
        try {
          if (byteChannel == null) {
            byteChannel = openByteChannel(dst.remaining());
            // We adjust the start index of content channel in following cases
            // 1. request range is in footer boundaries --> request the whole footer
            // 2. requested content is gzip encoded -> request always from start of file.
            // Case(1) is handled with reading and caching the extra read bytes, for all other cases
            // we need to skip all the unrequested bytes before start reading from current position.
            if (currentPosition > contentChannelCurrentPosition) {
              skipInPlace();
            }
            // making sure that currentPosition is in alignment with currentReadPosition before
            // actual read starts to avoid read discrepancies.
            checkState(
                contentChannelCurrentPosition == currentPosition,
                "position of read offset isn't in alignment with channel's read offset");
          }
          int bytesRead = byteChannel.read(dst);

          /*
          As we are using the zero copy implementation of byteChannel,
          it can return even zero bytes,
          while reading,
          we should not treat it as an error scenario anymore.
          */
          if (bytesRead == 0) {
            LOG.trace(
                "Read {} from storage-client's byte channel at position: {} with channel "
                    + "ending at: {} for resourceId: {} of size: {}",
                bytesRead, currentPosition, contentChannelEnd, resourceId, objectSize);
          }

          if (bytesRead < 0) {
            // Because we don't know decompressed object size for gzip-encoded objects,
            // assume that this is an object end.
            if (gzipEncoded) {
              objectSize = currentPosition;
              contentChannelEnd = currentPosition;
            }

            if (currentPosition != contentChannelEnd && currentPosition != objectSize) {
              throw new IOException(
                  String.format(
                      "Received end of stream result before all requestedBytes were received;"
                          + "EndOf stream signal received at offset: %d where as stream was "
                          + "suppose to end at: %d for resource: %s of size: %d",
                      currentPosition, contentChannelEnd, resourceId, objectSize));
            }
            // If we have reached an end of a contentChannel but not an end of an object.
            // then close contentChannel and continue reading an object if necessary.
            if (contentChannelEnd != objectSize && currentPosition == contentChannelEnd) {
              closeContentChannel();
              continue;
            } else {
              break;
            }
          }
          totalBytesRead += bytesRead;
          currentPosition += bytesRead;
          contentChannelCurrentPosition += bytesRead;
          checkState(
              contentChannelCurrentPosition == currentPosition,
              "contentChannelPosition (%s) should be equal to currentPosition (%s)"
                  + " after successful read",
              contentChannelCurrentPosition,
              currentPosition);
        } catch (Exception e) {
          int partialBytes = partiallyReadBytes(remainingBeforeRead, dst);
          currentPosition += partialBytes;
          contentChannelCurrentPosition += partialBytes;
          LOG.trace(
              "Closing contentChannel after {} exception for '{}'.", e.getMessage(), resourceId);
          closeContentChannel();
          throw convertError(e);
        }
      }
      return totalBytesRead;
    }

    private int partiallyReadBytes(int remainingBeforeRead, ByteBuffer dst) {
      int partialReadBytes = 0;
      if (remainingBeforeRead != dst.remaining()) {
        partialReadBytes = remainingBeforeRead - dst.remaining();
      }
      return partialReadBytes;
    }

    private ReadableByteChannel openByteChannel(long bytesToRead) throws IOException {
      checkArgument(
          bytesToRead > 0, "bytesToRead should be greater than 0, but was %s", bytesToRead);
      checkState(
          byteChannel == null && contentChannelEnd < 0,
          "contentChannel and contentChannelEnd should be not initialized yet for '%s'",
          resourceId);

      if (footerContent != null && currentPosition >= objectSize - footerContent.length) {
        return serveFooterContent();
      }

      // Should be updated only if content is not served from cached footer
      fileAccessManager.updateAccessPattern(currentPosition);

      setChannelBoundaries(bytesToRead);

      ReadableByteChannel readableByteChannel =
          getStorageReadChannel(contentChannelCurrentPosition, contentChannelEnd);

      if (contentChannelEnd == objectSize
          && (contentChannelEnd - contentChannelCurrentPosition)
              <= config.getMinRangeRequestSize()) {

        if (footerContent == null) {
          cacheFooter(readableByteChannel);
        }
        return serveFooterContent();
      }
      return readableByteChannel;
    }

    private void setChannelBoundaries(long bytesToRead) {
      contentChannelCurrentPosition = getRangeRequestStart();
      contentChannelEnd = getRangeRequestEnd(contentChannelCurrentPosition, bytesToRead);
      checkState(
          contentChannelEnd >= contentChannelCurrentPosition,
          String.format(
              "Start position should be <= endPosition startPosition:%d, endPosition: %d",
              contentChannelCurrentPosition, contentChannelEnd));
    }

    private void cacheFooter(ReadableByteChannel readableByteChannel) throws IOException {
      int footerSize = toIntExact(objectSize - contentChannelCurrentPosition);
      footerContent = new byte[footerSize];
      try (InputStream footerStream = Channels.newInputStream(readableByteChannel)) {
        int totalBytesRead = 0;
        int bytesRead;
        do {
          bytesRead = footerStream.read(footerContent, totalBytesRead, footerSize - totalBytesRead);
          if (bytesRead >= 0) {
            totalBytesRead += bytesRead;
          }
        } while (bytesRead >= 0 && totalBytesRead < footerSize);
        checkState(
            bytesRead >= 0,
            "footerStream shouldn't be empty before reading the footer of size %s, "
                + "totalBytesRead %s, read via last call %s, for '%s'",
            footerSize,
            totalBytesRead,
            bytesRead,
            resourceId);
        checkState(
            totalBytesRead == footerSize,
            "totalBytesRead (%s) should equal footerSize (%s) for '%s'",
            totalBytesRead,
            footerSize,
            resourceId);
      } catch (Exception e) {
        footerContent = null;
        throw e;
      }
      LOG.trace("Prefetched {} bytes footer for '{}'", footerContent.length, resourceId);
    }

    private ReadableByteChannel serveFooterContent() {
      contentChannelCurrentPosition = currentPosition;
      int offset = toIntExact(currentPosition - (objectSize - footerContent.length));
      int length = footerContent.length - offset;
      LOG.trace(
          "Opened channel (prefetched footer) from {} position for '{}'",
          currentPosition, resourceId);
      return Channels.newChannel(new ByteArrayInputStream(footerContent, offset, length));
    }

    private long getRangeRequestStart() {
      if (gzipEncoded) {
        return 0;
      }
      if (config.getFadvise() != Fadvise.SEQUENTIAL
          && isFooterRead()
          && !config.isReadExactRequestedBytesEnabled()) {
        // Prefetch footer and adjust start position to footerStart.
        return max(0, objectSize - config.getMinRangeRequestSize());
      }
      return currentPosition;
    }

    private long getRangeRequestEnd(long startPosition, long bytesToRead) {
      // Always read gzip-encoded files till the end - they do not support range reads.
      if (gzipEncoded) {
        return objectSize;
      }
      long endPosition = objectSize;
      if (fileAccessManager.shouldAdaptToRandomAccess()) {
        // opening a channel for whole object doesn't make sense as anyhow it will not be utilized
        // for further reads.
        endPosition = startPosition + max(bytesToRead, config.getMinRangeRequestSize());
      } else {
        if (config.getFadvise() == Fadvise.AUTO_RANDOM) {
          endPosition = min(startPosition + config.getBlockSize(), objectSize);
        }
      }

      if (footerContent != null) {
        // If footer is cached open just till footerStart.
        // Remaining content ill be served from cached footer itself.
        endPosition = min(endPosition, objectSize - footerContent.length);
      }
      return endPosition;
    }

    void closeContentChannel() {
      if (byteChannel != null) {
        LOG.trace("Closing internal contentChannel for '{}'", resourceId);
        try {
          byteChannel.close();
        } catch (Exception e) {
          LOG.trace(
              "Got an exception on contentChannel.close() for '{}'; ignoring it.", resourceId, e);
        } finally {
          byteChannel = null;
          fileAccessManager.updateLastServedIndex(contentChannelCurrentPosition);
          reset();
        }
      }
    }

    private void reset() {
      checkState(byteChannel == null, "contentChannel should be null for '%s'", resourceId);
      contentChannelCurrentPosition = -1;
      contentChannelEnd = -1;
    }

    private boolean isInRangeSeek() {
      long seekDistance = currentPosition - contentChannelCurrentPosition;
      if (byteChannel != null
          && seekDistance > 0
          // for gzip encoded content always seek in place
          && (gzipEncoded || seekDistance <= config.getInplaceSeekLimit())
          && currentPosition < contentChannelEnd) {
        return true;
      }
      return false;
    }

    private void skipInPlace() {
      if (skipBuffer == null) {
        skipBuffer = new byte[SKIP_BUFFER_SIZE];
      }
      long seekDistance = currentPosition - contentChannelCurrentPosition;
      while (seekDistance > 0 && byteChannel != null) {
        try {
          int bufferSize = toIntExact(min(skipBuffer.length, seekDistance));
          int bytesRead = byteChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
          if (bytesRead < 0) {
            LOG.info(
                "Somehow read {} bytes trying to skip {} bytes to seek to position {}, size: {}",
                bytesRead, seekDistance, currentPosition, objectSize);
            closeContentChannel();
          } else {
            seekDistance -= bytesRead;
            contentChannelCurrentPosition += bytesRead;
          }
        } catch (Exception e) {
          LOG.info(
              "Got an IO exception on contentChannel.read(), a lazy-seek will be pending for '{}'",
              resourceId, e);
          closeContentChannel();
        }
      }
      checkState(
          byteChannel == null || contentChannelCurrentPosition == currentPosition,
          "contentChannelPosition (%s) should be equal to currentPosition (%s)"
              + " after successful in-place skip",
          contentChannelCurrentPosition,
          currentPosition);
    }

    private void performPendingSeeks() {

      // Return quickly if there is no pending seek operation, i.e. position didn't change.
      if (currentPosition == contentChannelCurrentPosition && byteChannel != null) {
        return;
      }

      LOG.trace(
          "Performing lazySeek from {} to {} position '{}'",
          contentChannelCurrentPosition, currentPosition, resourceId);

      if (isInRangeSeek()) {
        skipInPlace();
      } else {
        // close existing contentChannel as requested bytes can't be served from current
        // contentChannel;
        closeContentChannel();
      }
    }

    private ReadableByteChannel getStorageReadChannel(long seek, long limit) throws IOException {
      ReadChannel readChannel = storage.reader(blobId, generateReadOptions());
      try {
        readChannel.seek(seek);
        readChannel.limit(limit);
        // bypass the storage-client caching layer hence eliminates the need to maintain a copy of
        // chunk
        readChannel.setChunkSize(0);
        return readChannel;
      } catch (Exception e) {
        throw new IOException(
            String.format(
                "Unable to update the boundaries/Range of contentChannel %s",
                resourceId.toString()),
            e);
      }
    }

    private BlobSourceOption[] generateReadOptions() {
      List<BlobSourceOption> blobReadOptions = new ArrayList<>();
      // To get decoded content
      blobReadOptions.add(BlobSourceOption.shouldReturnRawInputStream(false));

      if (blobId.getGeneration() != null) {
        blobReadOptions.add(BlobSourceOption.generationMatch(blobId.getGeneration()));
      }

      // TODO: Add support for encryptionKey
      return blobReadOptions.toArray(new BlobSourceOption[blobReadOptions.size()]);
    }

    private boolean isFooterRead() {
      return objectSize - currentPosition <= config.getMinRangeRequestSize();
    }
  }

  private static void validate(GoogleCloudStorageItemInfo itemInfo) throws IOException {
    checkNotNull(itemInfo, "itemInfo cannot be null");
    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Can not open a non-file object for read: %s", resourceId);
    if (!itemInfo.exists()) {
      throw new FileNotFoundException(String.format("Item not found: %s", resourceId));
    }
  }

  private IOException convertError(Exception error) {
    String msg = String.format("Error reading '%s'", resourceId);
    switch (ErrorTypeExtractor.getErrorType(error)) {
    case NOT_FOUND:
      return createFileNotFoundException(
          resourceId.getBucketName(), resourceId.getObjectName(), new IOException(msg, error));
    case OUT_OF_RANGE:
      return (IOException) new EOFException(msg).initCause(error);
    default:
      return new IOException(msg, error);
    }
  }

  /** Validates that the given position is valid for this channel. */
  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              position, resourceId));
    }

    if (objectSize >= 0 && position >= objectSize) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
              position, objectSize, resourceId));
    }
  }

  /** Throws if this channel is not currently open. */
  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }
}
