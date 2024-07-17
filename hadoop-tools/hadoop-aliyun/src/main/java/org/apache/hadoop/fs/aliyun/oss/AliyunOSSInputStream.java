/**
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

package org.apache.hadoop.fs.aliyun.oss;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;

import static org.apache.hadoop.fs.VectoredReadUtils.isOrderedDisjoint;
import static org.apache.hadoop.fs.VectoredReadUtils.mergeSortedRanges;
import static org.apache.hadoop.fs.VectoredReadUtils.validateAndSortRanges;
import static org.apache.hadoop.fs.aliyun.oss.Constants.*;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * The input stream for OSS blob system.
 * The class uses multi-part downloading to read data from the object content
 * stream.
 */
public class AliyunOSSInputStream extends FSInputStream implements StreamCapabilities {
  public static final Logger LOG = LoggerFactory.getLogger(AliyunOSSInputStream.class);
  private final long downloadPartSize;
  private AliyunOSSFileSystemStore store;
  private final String key;
  private Statistics statistics;
  private boolean closed;
  private long contentLength;
  private long position = 0;
  private long partRemaining = 0;
  private byte[] buffer;
  private int maxReadAheadPartNumber;
  private long expectNextPos;
  private long lastByteStart;

  private ExecutorService readAheadExecutorService;
  private Queue<ReadBuffer> readBufferQueue = new ArrayDeque<>();

  /**
   * Atomic boolean variable to stop all ongoing vectored read operation
   * for this input stream. This will be set to true when the stream is closed.
   */
  private final AtomicBoolean stopVectoredIOOperations = new AtomicBoolean(false);

  /** Vectored IO context. */
  private final VectoredIOContext vectoredIOContext;

  private ExecutorService vectorReadExecutorService;

  public AliyunOSSInputStream(Configuration conf,
      ExecutorService readAheadExecutorService, int maxReadAheadPartNumber,
      ExecutorService vectorReadExecutorService,
      VectoredIOContext vectoredIOContext,
      AliyunOSSFileSystemStore store, String key, Long contentLength,
      Statistics statistics) throws IOException {
    this.readAheadExecutorService =
        MoreExecutors.listeningDecorator(readAheadExecutorService);
    this.vectorReadExecutorService =
        MoreExecutors.listeningDecorator(vectorReadExecutorService);
    this.vectoredIOContext = vectoredIOContext;
    this.store = store;
    this.key = key;
    this.statistics = statistics;
    this.contentLength = contentLength;
    downloadPartSize = conf.getLong(MULTIPART_DOWNLOAD_SIZE_KEY,
        MULTIPART_DOWNLOAD_SIZE_DEFAULT);
    this.maxReadAheadPartNumber = maxReadAheadPartNumber;

    this.expectNextPos = 0;
    this.lastByteStart = -1;
    closed = false;
  }

  /**
   * Reopen the wrapped stream at give position, by seeking for
   * data of a part length from object content stream.
   *
   * @param pos position from start of a file
   * @throws IOException if failed to reopen
   */
  private synchronized void reopen(long pos) throws IOException {
    long partSize;

    if (pos < 0) {
      throw new EOFException("Cannot seek at negative position:" + pos);
    } else if (pos > contentLength) {
      throw new EOFException("Cannot seek after EOF, contentLength:" +
          contentLength + " position:" + pos);
    } else if (pos + downloadPartSize > contentLength) {
      partSize = contentLength - pos;
    } else {
      partSize = downloadPartSize;
    }

    if (this.buffer != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Aborting old stream to open at pos " + pos);
      }
      this.buffer = null;
    }

    boolean isRandomIO = true;
    if (pos == this.expectNextPos) {
      isRandomIO = false;
    } else {
      //new seek, remove cache buffers if its byteStart is not equal to pos
      while (readBufferQueue.size() != 0) {
        if (readBufferQueue.element().getByteStart() != pos) {
          readBufferQueue.poll();
        } else {
          break;
        }
      }
    }

    this.expectNextPos = pos + partSize;

    int currentSize = readBufferQueue.size();
    if (currentSize == 0) {
      //init lastByteStart to pos - partSize, used by for loop below
      lastByteStart = pos - partSize;
    } else {
      ReadBuffer[] readBuffers = readBufferQueue.toArray(
          new ReadBuffer[currentSize]);
      lastByteStart = readBuffers[currentSize - 1].getByteStart();
    }

    int maxLen = this.maxReadAheadPartNumber - currentSize;
    for (int i = 0; i < maxLen && i < (currentSize + 1) * 2; i++) {
      if (lastByteStart + partSize * (i + 1) > contentLength) {
        break;
      }

      long byteStart = lastByteStart + partSize * (i + 1);
      long byteEnd = byteStart + partSize -1;
      if (byteEnd >= contentLength) {
        byteEnd = contentLength - 1;
      }

      ReadBuffer readBuffer = new ReadBuffer(byteStart, byteEnd);
      if (readBuffer.getBuffer().length == 0) {
        //EOF
        readBuffer.setStatus(ReadBuffer.STATUS.SUCCESS);
      } else {
        this.readAheadExecutorService.execute(
            new AliyunOSSFileReaderTask(key, store, readBuffer));
      }
      readBufferQueue.add(readBuffer);
      if (isRandomIO) {
        break;
      }
    }

    ReadBuffer readBuffer = readBufferQueue.poll();
    readBuffer.lock();
    try {
      readBuffer.await(ReadBuffer.STATUS.INIT);
      if (readBuffer.getStatus() == ReadBuffer.STATUS.ERROR) {
        this.buffer = null;
      } else {
        this.buffer = readBuffer.getBuffer();
      }
    } catch (InterruptedException e) {
      LOG.warn("interrupted when wait a read buffer");
    } finally {
      readBuffer.unlock();
    }

    if (this.buffer == null) {
      throw new IOException("Null IO stream");
    }
    position = pos;
    partRemaining = partSize;
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();

    if (partRemaining <= 0 && position < contentLength) {
      reopen(position);
    }

    int byteRead = -1;
    if (partRemaining != 0) {
      byteRead = this.buffer[this.buffer.length - (int)partRemaining] & 0xFF;
    }
    if (byteRead >= 0) {
      position++;
      partRemaining--;
    }

    if (statistics != null && byteRead >= 0) {
      statistics.incrementBytesRead(byteRead);
    }
    return byteRead;
  }


  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   *
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len)
      throws IOException {
    checkNotClosed();

    if (buf == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > buf.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int bytesRead = 0;
    // Not EOF, and read not done
    while (position < contentLength && bytesRead < len) {
      if (partRemaining == 0) {
        reopen(position);
      }

      int bytes = 0;
      for (int i = this.buffer.length - (int)partRemaining;
           i < this.buffer.length; i++) {
        buf[off + bytesRead] = this.buffer[i];
        bytes++;
        bytesRead++;
        if (off + bytesRead >= len) {
          break;
        }
      }

      if (bytes > 0) {
        position += bytes;
        partRemaining -= bytes;
      } else if (partRemaining != 0) {
        throw new IOException("Failed to read from stream. Remaining:" +
            partRemaining);
      }
    }

    if (statistics != null && bytesRead > 0) {
      statistics.incrementBytesRead(bytesRead);
    }

    // Read nothing, but attempt to read something
    if (bytesRead == 0 && len > 0) {
      return -1;
    } else {
      return bytesRead;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    stopVectoredIOOperations.set(true);
    this.buffer = null;
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = contentLength - position;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int)remaining;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkNotClosed();
    if (position == pos) {
      return;
    } else if (pos > position && pos < position + partRemaining) {
      long len = pos - position;
      position = pos;
      partRemaining -= len;
    } else {
      reopen(pos);
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    checkNotClosed();
    return position;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    checkNotClosed();
    return false;
  }

  public long getExpectNextPos() {
    return this.expectNextPos;
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (toLowerCase(capability)) {
      case StreamCapabilities.VECTOREDIO:
        return true;
      default:
        return false;
    }
  }

  /**
   * Validates range parameters.
   * In case of OSS we already have contentLength from the first GET request
   * during an open file operation so failing fast here.
   * @param range requested range.
   * @throws EOFException end of file exception.
   */
  private void validateRangeRequest(FileRange range) throws EOFException {
    VectoredReadUtils.validateRangeRequest(range);
    if (range.getOffset() + range.getLength() > contentLength) {
      final String errMsg = String.format(
          "Requested range [%d, %d) is beyond EOF for path %s",
          range.getOffset(), range.getLength(), key);
      LOG.warn(errMsg);
      throw new EOFException(errMsg);
    }
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public int minSeekForVectorReads() {
    return vectoredIOContext.getMinSeekForVectorReads();
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public int maxReadSizeForVectorReads() {
    return vectoredIOContext.getMaxReadSizeForVectorReads();
  }

  /**
   * Read data into destination buffer from OSS object content.
   * @param objectContent result from OSS.
   * @param dest destination buffer.
   * @param offset start offset of dest buffer.
   * @param length number of bytes to fill in dest.
   * @throws IOException any IOE.
   */
  private void readByteArray(InputStream objectContent,
                             byte[] dest,
                             int offset,
                             int length) throws IOException {
    int readBytes = 0;
    while (readBytes < length) {
      int readBytesCurr = objectContent.read(dest,
          offset + readBytes,
          length - readBytes);
      readBytes +=readBytesCurr;
      if (readBytesCurr < 0) {
        throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
      }
    }
  }

  /**
   * Populates the buffer with data from objectContent
   * till length. Handles both direct and heap byte buffers.
   * @param range vector range to populate.
   * @param buffer buffer to fill.
   * @param objectContent result retrieved from OSS store.
   * @throws IOException any IOE.
   */
  private void populateBuffer(FileRange range,
                              ByteBuffer buffer,
                              InputStream objectContent) throws IOException {

    int length = range.getLength();
    if (buffer.isDirect()) {
      VectoredReadUtils.readInDirectBuffer(range, buffer,
          (position, tmp, offset, currentLength) -> {
            readByteArray(objectContent, tmp, offset, currentLength);
            return null;
          });
      buffer.flip();
    } else {
      readByteArray(objectContent, buffer.array(), 0, length);
    }
  }

  /**
   * Drain unnecessary data in between ranges.
   * @param objectContent oss data stream.
   * @param drainQuantity how many bytes to drain.
   * @throws IOException any IOE.
   */
  private void drainUnnecessaryData(InputStream objectContent, long drainQuantity)
      throws IOException {
    int drainBytes = 0;
    int readCount;
    while (drainBytes < drainQuantity) {
      if (drainBytes + Constants.DRAIN_BUFFER_SIZE <= drainQuantity) {
        byte[] drainBuffer = new byte[Constants.DRAIN_BUFFER_SIZE];
        readCount = objectContent.read(drainBuffer);
      } else {
        byte[] drainBuffer = new byte[(int) (drainQuantity - drainBytes)];
        readCount = objectContent.read(drainBuffer);
      }
      if (readCount < 0) {
        // read request failed; often network issues.
        // no attempt is made to recover at this point.
        final String s = String.format(
            "End of stream reached draining data between ranges; expected %,d bytes;"
                + " only drained %,d bytes before -1 returned (position=%,d)",
            drainQuantity, drainBytes, position + drainBytes);
        throw new EOFException(s);
      }
      drainBytes += readCount;
    }
    LOG.debug("{} bytes drained from stream ", drainBytes);
  }

  /**
   * Populate underlying buffers of the child ranges.
   * @param combinedFileRange big combined file range.
   * @param objectContent data from oss.
   * @param allocate method to allocate child byte buffers.
   * @throws IOException any IOE.
   */
  private void populateChildBuffers(CombinedFileRange combinedFileRange,
                                    InputStream objectContent,
                                    IntFunction<ByteBuffer> allocate) throws IOException {
    // If the combined file range just contains a single child
    // range, we only have to fill that one child buffer else
    // we drain the intermediate data between consecutive ranges
    // and fill the buffers one by one.
    if (combinedFileRange.getUnderlying().size() == 1) {
      FileRange child = combinedFileRange.getUnderlying().get(0);
      ByteBuffer buffer = allocate.apply(child.getLength());
      populateBuffer(child, buffer, objectContent);
      child.getData().complete(buffer);
    } else {
      FileRange prev = null;
      for (FileRange child : combinedFileRange.getUnderlying()) {
        if (prev != null && prev.getOffset() + prev.getLength() < child.getOffset()) {
          long drainQuantity = child.getOffset() - prev.getOffset() - prev.getLength();
          drainUnnecessaryData(objectContent, drainQuantity);
        }
        ByteBuffer buffer = allocate.apply(child.getLength());
        populateBuffer(child, buffer, objectContent);
        child.getData().complete(buffer);
        prev = child;
      }
    }
  }

  /**
   * Read data from OSS for this range and populate the buffer.
   * @param range range of data to read.
   * @param buffer buffer to fill.
   */
  private void readSingleRange(FileRange range, ByteBuffer buffer) {
    LOG.debug("Start reading range {} from path {} ", range, key);
    InputStream objectRange = null;
    try {
      long position = range.getOffset();
      int length = range.getLength();
      objectRange = store.retrieve(key, position, position + length - 1);
      populateBuffer(range, buffer, objectRange);
      range.getData().complete(buffer);
    } catch (Exception ex) {
      LOG.warn("Exception while reading a range {} from path {} ", range, key, ex);
      range.getData().completeExceptionally(ex);
    } finally {
      IOUtils.cleanupWithLogger(LOG, objectRange);
    }
  }

  /**
   * Read the data from OSS for the bigger combined file range and update all the
   * underlying ranges.
   * @param combinedFileRange big combined file range.
   * @param allocate method to create byte buffers to hold result data.
   */
  private void readCombinedRangeAndUpdateChildren(CombinedFileRange combinedFileRange,
                                                  IntFunction<ByteBuffer> allocate) {
    LOG.debug("Start reading combined range {} from path {} ", combinedFileRange, key);
    InputStream objectRange = null;
    try {
      long position = combinedFileRange.getOffset();
      int length = combinedFileRange.getLength();
      objectRange = store.retrieve(key, position, position + length - 1);
      populateChildBuffers(combinedFileRange, objectRange, allocate);
    } catch (Exception ex) {
      LOG.debug("Exception while reading a range {} from path {} ", combinedFileRange, key, ex);
      for(FileRange child : combinedFileRange.getUnderlying()) {
        child.getData().completeExceptionally(ex);
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, objectRange);
    }
    LOG.debug("Finished reading range {} from path {} ", combinedFileRange, key);
  }

  /**
   * {@inheritDoc}
   * Vectored read implementation for {@link AliyunOSSInputStream}.
   * @param ranges the byte ranges to read.
   * @param allocate the function to allocate ByteBuffer.
   * @throws IOException IOE if any.
   */
  @Override
  public void readVectored(List<? extends FileRange> ranges,
                           IntFunction<ByteBuffer> allocate) throws IOException {
    LOG.debug("Starting vectored read on path {} for ranges {} ", key, ranges);
    checkNotClosed();
    if (stopVectoredIOOperations.getAndSet(false)) {
      LOG.debug("Reinstating vectored read operation for path {} ", key);
    }
    List<? extends FileRange> sortedRanges = validateAndSortRanges(ranges,
        Optional.of(contentLength));
    for (FileRange range : ranges) {
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      range.setData(result);
    }

    if (isOrderedDisjoint(sortedRanges, 1, minSeekForVectorReads())) {
      LOG.debug("Not merging the ranges as they are disjoint");
      for (FileRange range: sortedRanges) {
        ByteBuffer buffer = allocate.apply(range.getLength());
        vectorReadExecutorService.submit(() -> readSingleRange(range, buffer));
      }
    } else {
      LOG.debug("Trying to merge the ranges as they are not disjoint");
      List<CombinedFileRange> combinedFileRanges = mergeSortedRanges(sortedRanges,
          1, minSeekForVectorReads(),
          maxReadSizeForVectorReads());
      LOG.debug("Number of original ranges size {} , Number of combined ranges {} ",
          ranges.size(), combinedFileRanges.size());
      for (CombinedFileRange combinedFileRange: combinedFileRanges) {
        vectorReadExecutorService.submit(
            () -> readCombinedRangeAndUpdateChildren(combinedFileRange, allocate));
      }
    }

    LOG.debug("Finished submitting vectored read to threadpool" +
        " on path {} for ranges {} ", key, ranges);
  }
}
