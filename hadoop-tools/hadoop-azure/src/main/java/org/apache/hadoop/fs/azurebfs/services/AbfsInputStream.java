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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.CachedSASToken;
import org.apache.hadoop.fs.azurebfs.utils.Listener;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static java.lang.Math.max;
import static java.lang.Math.min;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.STREAM_ID_LEN;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * The AbfsInputStream for AbfsClient.
 */
public class AbfsInputStream extends FSInputStream implements CanUnbuffer,
        StreamCapabilities, IOStatisticsSource {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);
  //  Footer size is set to qualify for both ORC and parquet files
  public static final int FOOTER_SIZE = 16 * ONE_KB;
  public static final int MAX_OPTIMIZED_READ_ATTEMPTS = 2;

  private int readAheadBlockSize;
  private final AbfsClient client;
  private final Statistics statistics;
  private final String path;
  private final long contentLength;
  private final int bufferSize; // default buffer size
  private final int readAheadQueueDepth;         // initialized in constructor
  private final String eTag;                  // eTag of the path when InputStream are created
  private final boolean tolerateOobAppends; // whether tolerate Oob Appends
  private final boolean readAheadEnabled; // whether enable readAhead;
  private final String inputStreamId;
  private final boolean alwaysReadBufferSize;
  /*
   * By default the pread API will do a seek + read as in FSInputStream.
   * The read data will be kept in a buffer. When bufferedPreadDisabled is true,
   * the pread API will read only the specified amount of data from the given
   * offset and the buffer will not come into use at all.
   * @see #read(long, byte[], int, int)
   */
  private final boolean bufferedPreadDisabled;
  // User configured size of read ahead.
  private final int readAheadRange;

  private boolean firstRead = true;
  private long offsetOfFirstRead = 0;
  // SAS tokens can be re-used until they expire
  private CachedSASToken cachedSasToken;
  private byte[] buffer = null;            // will be initialized on first use

  private long fCursor = 0;  // cursor of buffer within file - offset of next byte to read from remote server
  private long fCursorAfterLastRead = -1;
  private int bCursor = 0;   // cursor of read within buffer - offset of next byte to be returned from buffer
  private int limit = 0;     // offset of next byte to be read into buffer from service (i.e., upper marker+1
  //                                                      of valid bytes in buffer)
  private boolean closed = false;
  private TracingContext tracingContext;

  //  Optimisations modify the pointer fields.
  //  For better resilience the following fields are used to save the
  //  existing state before optimization flows.
  private int limitBkp;
  private int bCursorBkp;
  private long fCursorBkp;
  private long fCursorAfterLastReadBkp;
  private final AbfsReadFooterMetrics abfsReadFooterMetrics;
  /** Stream statistics. */
  private final AbfsInputStreamStatistics streamStatistics;
  private long bytesFromReadAhead; // bytes read from readAhead; for testing
  private long bytesFromRemoteRead; // bytes read remotely; for testing
  private Listener listener;
  private boolean collectMetricsForNextRead = false;

  private boolean collectStreamMetrics = false;
  private final AbfsInputStreamContext context;
  private IOStatistics ioStatistics;
  /**
   * This is the actual position within the object, used by
   * lazy seek to decide whether to seek on the next read or not.
   */
  private long nextReadPos;

  public AbfsInputStream(
          final AbfsClient client,
          final Statistics statistics,
          final String path,
          final long contentLength,
          final AbfsInputStreamContext abfsInputStreamContext,
          final String eTag,
          TracingContext tracingContext) {
    this.client = client;
    this.statistics = statistics;
    this.path = path;
    this.contentLength = contentLength;
    this.bufferSize = abfsInputStreamContext.getReadBufferSize();
    this.readAheadQueueDepth = abfsInputStreamContext.getReadAheadQueueDepth();
    this.tolerateOobAppends = abfsInputStreamContext.isTolerateOobAppends();
    this.eTag = eTag;
    this.readAheadRange = abfsInputStreamContext.getReadAheadRange();
    this.readAheadEnabled = true;
    this.alwaysReadBufferSize
        = abfsInputStreamContext.shouldReadBufferSizeAlways();
    this.bufferedPreadDisabled = abfsInputStreamContext
        .isBufferedPreadDisabled();
    this.cachedSasToken = new CachedSASToken(
        abfsInputStreamContext.getSasTokenRenewPeriodForStreamsInSeconds());
    this.streamStatistics = abfsInputStreamContext.getStreamStatistics();
    this.abfsReadFooterMetrics = abfsInputStreamContext.getAbfsReadFooterMetrics();
    this.inputStreamId = createInputStreamId();
    this.tracingContext = new TracingContext(tracingContext);
    this.tracingContext.setOperation(FSOperationType.READ);
    this.tracingContext.setStreamID(inputStreamId);
    this.context = abfsInputStreamContext;
    readAheadBlockSize = abfsInputStreamContext.getReadAheadBlockSize();

    // Propagate the config values to ReadBufferManager so that the first instance
    // to initialize can set the readAheadBlockSize
    ReadBufferManager.setReadBufferManagerConfigs(readAheadBlockSize);
    if (streamStatistics != null) {
      ioStatistics = streamStatistics.getIOStatistics();
    }
  }

  public String getPath() {
    return path;
  }

  private String createInputStreamId() {
    return StringUtils.right(UUID.randomUUID().toString(), STREAM_ID_LEN);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    // When bufferedPreadDisabled = true, this API does not use any shared buffer,
    // cursor position etc. So this is implemented as NOT synchronized. HBase
    // kind of random reads on a shared file input stream will greatly get
    // benefited by such implementation.
    // Strict close check at the begin of the API only not for the entire flow.
    synchronized (this) {
      if (closed) {
        throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
      }
    }
    LOG.debug("pread requested offset = {} len = {} bufferedPreadDisabled = {}",
        offset, length, bufferedPreadDisabled);
    if (!bufferedPreadDisabled) {
      return super.read(position, buffer, offset, length);
    }
    validatePositionedReadArgs(position, buffer, offset, length);
    if (length == 0) {
      return 0;
    }
    if (streamStatistics != null) {
      streamStatistics.readOperationStarted();
    }
    int bytesRead = readRemote(position, buffer, offset, length, tracingContext);
    if (statistics != null) {
      statistics.incrementBytesRead(bytesRead);
    }
    if (streamStatistics != null) {
      streamStatistics.bytesRead(bytesRead);
    }
    return bytesRead;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    int numberOfBytesRead = read(b, 0, 1);
    if (numberOfBytesRead < 0) {
      return -1;
    } else {
      return (b[0] & 0xFF);
    }
  }

  @Override
  public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
    // check if buffer is null before logging the length
    if (b != null) {
      LOG.debug("read requested b.length = {} offset = {} len = {}", b.length,
          off, len);
    } else {
      LOG.debug("read requested b = null offset = {} len = {}", off, len);
    }

    int currentOff = off;
    int currentLen = len;
    int lastReadBytes;
    int totalReadBytes = 0;
    if (streamStatistics != null) {
      streamStatistics.readOperationStarted();
    }
    incrementReadOps();
    do {

      // limit is the maximum amount of data present in buffer.
      // fCursor is the current file pointer. Thus maximum we can
      // go back and read from buffer is fCursor - limit.
      // There maybe case that we read less than requested data.
      long filePosAtStartOfBuffer = fCursor - limit;
      if (firstRead && nextReadPos >= contentLength - 16 * ONE_KB) {
        this.collectStreamMetrics = true;
        this.collectMetricsForNextRead = true;
        this.offsetOfFirstRead = nextReadPos;
        this.abfsReadFooterMetrics.setSizeReadByFirstRead(len+"_"+(Math.abs(contentLength - nextReadPos)));
        this.abfsReadFooterMetrics.getFileLength().set(contentLength);
      }
      if (!firstRead && collectMetricsForNextRead){
        this.abfsReadFooterMetrics.setOffsetDiffBetweenFirstAndSecondRead(len+"_"+(Math.abs(nextReadPos - offsetOfFirstRead)));
        this.collectMetricsForNextRead = false;
      }
      if (nextReadPos >= filePosAtStartOfBuffer && nextReadPos <= fCursor) {
        // Determining position in buffer from where data is to be read.
        bCursor = (int) (nextReadPos - filePosAtStartOfBuffer);

        // When bCursor == limit, buffer will be filled again.
        // So in this case we are not actually reading from buffer.
        if (bCursor != limit && streamStatistics != null) {
          streamStatistics.seekInBuffer();
        }
      } else {
        // Clearing the buffer and setting the file pointer
        // based on previous seek() call.
        fCursor = nextReadPos;
        limit = 0;
        bCursor = 0;
      }
      if (shouldReadFully()) {
        lastReadBytes = readFileCompletely(b, currentOff, currentLen);
      } else if (shouldReadLastBlock()) {
        lastReadBytes = readLastBlock(b, currentOff, currentLen);
      } else {
        lastReadBytes = readOneBlock(b, currentOff, currentLen);
      }
      if (lastReadBytes > 0) {
        currentOff += lastReadBytes;
        currentLen -= lastReadBytes;
        totalReadBytes += lastReadBytes;
      }
      if (currentLen <= 0 || currentLen > b.length - currentOff) {
        break;
      }
    } while (lastReadBytes > 0);
    return totalReadBytes > 0 ? totalReadBytes : lastReadBytes;
  }

  private boolean shouldReadFully() {
    return this.firstRead && this.context.readSmallFilesCompletely()
        && this.contentLength <= this.bufferSize;
  }

  private boolean shouldReadLastBlock() {
    long footerStart = max(0, this.contentLength - FOOTER_SIZE);
    return this.firstRead && this.context.optimizeFooterRead()
        && this.fCursor >= footerStart;
  }

  private int readOneBlock(final byte[] b, final int off, final int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (!validate(b, off, len)) {
      return -1;
    }
    //If buffer is empty, then fill the buffer.
    if (bCursor == limit) {
      //If EOF, then return -1
      if (fCursor >= contentLength) {
        return -1;
      }

      long bytesRead = 0;
      //reset buffer to initial state - i.e., throw away existing data
      bCursor = 0;
      limit = 0;
      if (buffer == null) {
        LOG.debug("created new buffer size {}", bufferSize);
        buffer = new byte[bufferSize];
      }

      if (alwaysReadBufferSize) {
        bytesRead = readInternal(fCursor, buffer, 0, bufferSize, false);
      } else {
        // Enable readAhead when reading sequentially
        if (-1 == fCursorAfterLastRead || fCursorAfterLastRead == fCursor || b.length >= bufferSize) {
          LOG.debug("Sequential read with read ahead size of {}", bufferSize);
          bytesRead = readInternal(fCursor, buffer, 0, bufferSize, false);
        } else {
          // Enabling read ahead for random reads as well to reduce number of remote calls.
          int lengthWithReadAhead = Math.min(b.length + readAheadRange, bufferSize);
          LOG.debug("Random read with read ahead size of {}", lengthWithReadAhead);
          bytesRead = readInternal(fCursor, buffer, 0, lengthWithReadAhead, true);
        }
      }
      if (firstRead) {
        firstRead = false;
      }
      if (bytesRead == -1) {
        return -1;
      }

      limit += bytesRead;
      fCursor += bytesRead;
      fCursorAfterLastRead = fCursor;
    }
    return copyToUserBuffer(b, off, len);
  }

  private int readFileCompletely(final byte[] b, final int off, final int len)
      throws IOException {
    if (len == 0) {
      return 0;
    }
    if (!validate(b, off, len)) {
      return -1;
    }
    savePointerState();
    // data need to be copied to user buffer from index bCursor, bCursor has
    // to be the current fCusor
    bCursor = (int) fCursor;
    return optimisedRead(b, off, len, 0, contentLength);
  }

  private int readLastBlock(final byte[] b, final int off, final int len)
      throws IOException {
    if (len == 0) {
      return 0;
    }
    if (!validate(b, off, len)) {
      return -1;
    }
    savePointerState();
    // data need to be copied to user buffer from index bCursor,
    // AbfsInutStream buffer is going to contain data from last block start. In
    // that case bCursor will be set to fCursor - lastBlockStart
    long lastBlockStart = max(0, contentLength - bufferSize);
    bCursor = (int) (fCursor - lastBlockStart);
    // 0 if contentlength is < buffersize
    long actualLenToRead = min(bufferSize, contentLength);
    return optimisedRead(b, off, len, lastBlockStart, actualLenToRead);
  }

  private int optimisedRead(final byte[] b, final int off, final int len,
      final long readFrom, final long actualLen) throws IOException {
    fCursor = readFrom;
    int totalBytesRead = 0;
    int lastBytesRead = 0;
    try {
      buffer = new byte[bufferSize];
      for (int i = 0;
           i < MAX_OPTIMIZED_READ_ATTEMPTS && fCursor < contentLength; i++) {
        lastBytesRead = readInternal(fCursor, buffer, limit,
            (int) actualLen - limit, true);
        if (lastBytesRead > 0) {
          totalBytesRead += lastBytesRead;
          limit += lastBytesRead;
          fCursor += lastBytesRead;
          fCursorAfterLastRead = fCursor;
        }
      }
    } catch (IOException e) {
      LOG.debug("Optimized read failed. Defaulting to readOneBlock {}", e);
      restorePointerState();
      return readOneBlock(b, off, len);
    } finally {
      firstRead = false;
    }
    if (totalBytesRead < 1) {
      restorePointerState();
      return -1;
    }
    //  If the read was partial and the user requested part of data has
    //  not read then fallback to readoneblock. When limit is smaller than
    //  bCursor that means the user requested data has not been read.
    if (fCursor < contentLength && bCursor > limit) {
      restorePointerState();
      return readOneBlock(b, off, len);
    }
    return copyToUserBuffer(b, off, len);
  }

  private void savePointerState() {
    //  Saving the current state for fall back ifn case optimization fails
    this.limitBkp = this.limit;
    this.fCursorBkp = this.fCursor;
    this.fCursorAfterLastReadBkp = this.fCursorAfterLastRead;
    this.bCursorBkp = this.bCursor;
  }

  private void restorePointerState() {
    //  Saving the current state for fall back ifn case optimization fails
    this.limit = this.limitBkp;
    this.fCursor = this.fCursorBkp;
    this.fCursorAfterLastRead = this.fCursorAfterLastReadBkp;
    this.bCursor = this.bCursorBkp;
  }

  private boolean validate(final byte[] b, final int off, final int len)
      throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    Preconditions.checkNotNull(b);
    LOG.debug("read one block requested b.length = {} off {} len {}", b.length,
        off, len);

    if (this.available() == 0) {
      return false;
    }

    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    return true;
  }

  private int copyToUserBuffer(byte[] b, int off, int len){
    //If there is anything in the buffer, then return lesser of (requested bytes) and (bytes in buffer)
    //(bytes returned may be less than requested)
    int bytesRemaining = limit - bCursor;
    int bytesToRead = min(len, bytesRemaining);
    System.arraycopy(buffer, bCursor, b, off, bytesToRead);
    bCursor += bytesToRead;
    nextReadPos += bytesToRead;
    if (statistics != null) {
      statistics.incrementBytesRead(bytesToRead);
    }
    if (streamStatistics != null) {
      // Bytes read from the local buffer.
      streamStatistics.bytesReadFromBuffer(bytesToRead);
      streamStatistics.bytesRead(bytesToRead);
    }
    return bytesToRead;
  }

  private int readInternal(final long position, final byte[] b, final int offset, final int length,
                           final boolean bypassReadAhead) throws IOException {
    if (readAheadEnabled && !bypassReadAhead) {
      // try reading from read-ahead
      if (offset != 0) {
        throw new IllegalArgumentException("readahead buffers cannot have non-zero buffer offsets");
      }
      int receivedBytes;

      // queue read-aheads
      int numReadAheads = this.readAheadQueueDepth;
      long nextOffset = position;
      // First read to queue needs to be of readBufferSize and later
      // of readAhead Block size
      long nextSize = min((long) bufferSize, contentLength - nextOffset);
      LOG.debug("read ahead enabled issuing readheads num = {}", numReadAheads);
      TracingContext readAheadTracingContext = new TracingContext(tracingContext);
      readAheadTracingContext.setPrimaryRequestID();
      while (numReadAheads > 0 && nextOffset < contentLength) {
        LOG.debug("issuing read ahead requestedOffset = {} requested size {}",
            nextOffset, nextSize);
        ReadBufferManager.getBufferManager().queueReadAhead(this, nextOffset, (int) nextSize,
                new TracingContext(readAheadTracingContext));
        nextOffset = nextOffset + nextSize;
        numReadAheads--;
        // From next round onwards should be of readahead block size.
        nextSize = min((long) readAheadBlockSize, contentLength - nextOffset);
      }

      // try reading from buffers first
      receivedBytes = ReadBufferManager.getBufferManager().getBlock(this, position, length, b);
      bytesFromReadAhead += receivedBytes;
      if (receivedBytes > 0) {
        incrementReadOps();
        LOG.debug("Received data from read ahead, not doing remote read");
        if (streamStatistics != null) {
          streamStatistics.readAheadBytesRead(receivedBytes);
        }
        return receivedBytes;
      }

      // got nothing from read-ahead, do our own read now
      receivedBytes = readRemote(position, b, offset, length, new TracingContext(tracingContext));
      return receivedBytes;
    } else {
      LOG.debug("read ahead disabled, reading remote");
      return readRemote(position, b, offset, length, new TracingContext(tracingContext));
    }
  }

  int readRemote(long position, byte[] b, int offset, int length, TracingContext tracingContext) throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("attempting to read from negative offset");
    }
    if (position >= contentLength) {
      return -1;  // Hadoop prefers -1 to EOFException
    }
    if (b == null) {
      throw new IllegalArgumentException("null byte array passed in to read() method");
    }
    if (offset >= b.length) {
      throw new IllegalArgumentException("offset greater than length of array");
    }
    if (length < 0) {
      throw new IllegalArgumentException("requested read length is less than zero");
    }
    if (length > (b.length - offset)) {
      throw new IllegalArgumentException("requested read length is more than will fit after requested offset in buffer");
    }
    final AbfsRestOperation op;
    AbfsPerfTracker tracker = client.getAbfsPerfTracker();
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker, "readRemote", "read")) {
      if (streamStatistics != null) {
        streamStatistics.remoteReadOperation();
      }
      LOG.trace("Trigger client.read for path={} position={} offset={} length={}", path, position, offset, length);
      op = client.read(path, position, b, offset, length,
          tolerateOobAppends ? "*" : eTag, cachedSasToken.get(), tracingContext);
      cachedSasToken.update(op.getSasToken());
      LOG.debug("issuing HTTP GET request params position = {} b.length = {} "
          + "offset = {} length = {}", position, b.length, offset, length);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
      incrementReadOps();
    } catch (AzureBlobFileSystemException ex) {
      if (ex instanceof AbfsRestOperationException) {
        AbfsRestOperationException ere = (AbfsRestOperationException) ex;
        if (ere.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new FileNotFoundException(ere.getMessage());
        }
      }
      throw new IOException(ex);
    }
    long bytesRead = op.getResult().getBytesReceived();
    if (streamStatistics != null) {
      streamStatistics.remoteBytesRead(bytesRead);
    }
    if (bytesRead > Integer.MAX_VALUE) {
      throw new IOException("Unexpected Content-Length");
    }
    LOG.debug("HTTP request read bytes = {}", bytesRead);
    bytesFromRemoteRead += bytesRead;
    return (int) bytesRead;
  }

  /**
   * Increment Read Operations.
   */
  private void incrementReadOps() {
    if (statistics != null) {
      statistics.incrementReadOps(1);
    }
  }

  /**
   * Seek to given position in stream.
   * @param n position to seek to
   * @throws IOException if there is an error
   * @throws EOFException if attempting to seek past end of file
   */
  @Override
  public synchronized void seek(long n) throws IOException {
    LOG.debug("requested seek to position {}", n);
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    if (n < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    if (n > contentLength) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
    }

    if (streamStatistics != null) {
      streamStatistics.seek(n, fCursor);
    }

    // next read will read from here
    nextReadPos = n;
    LOG.debug("set nextReadPos to {}", nextReadPos);
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    long currentPos = getPos();
    if (currentPos == contentLength) {
      if (n > 0) {
        throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
      }
    }
    long newPos = currentPos + n;
    if (newPos < 0) {
      newPos = 0;
      n = newPos - currentPos;
    }
    if (newPos > contentLength) {
      newPos = contentLength;
      n = newPos - currentPos;
    }
    seek(newPos);
    return n;
  }

  /**
   * Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   *
   * This is to match the behavior of DFSInputStream.available(),
   * which some clients may rely on (HBase write-ahead log reading in
   * particular).
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException(
          FSExceptionMessages.STREAM_IS_CLOSED);
    }
    final long remaining = this.contentLength - this.getPos();
    return remaining <= Integer.MAX_VALUE
        ? (int) remaining : Integer.MAX_VALUE;
  }

  /**
   * Returns the length of the file that this stream refers to. Note that the length returned is the length
   * as of the time the Stream was opened. Specifically, if there have been subsequent appends to the file,
   * they wont be reflected in the returned length.
   *
   * @return length of the file.
   * @throws IOException if the stream is closed
   */
  public long length() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    return contentLength;
  }

  /**
   * Return the current offset from the start of the file
   * @throws IOException throws {@link IOException} if there is an error
   */
  @Override
  public synchronized long getPos() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    return nextReadPos < 0 ? 0 : nextReadPos;
  }

  public TracingContext getTracingContext() {
    return tracingContext;
  }

  /**
   * Seeks a different copy of the data.  Returns true if
   * found a new source, false otherwise.
   * @throws IOException throws {@link IOException} if there is an error
   */
  @Override
  public boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public synchronized void close() throws IOException {
    LOG.debug("Closing {}", this);
    closed = true;
    buffer = null; // de-reference the buffer so it can be GC'ed sooner
    if(this.collectStreamMetrics) {
      checkIsParquet(abfsReadFooterMetrics);
      this.client.getAbfsCounters()
          .getAbfsReadFooterMetrics()
          .add(abfsReadFooterMetrics);
    }
    ReadBufferManager.getBufferManager().purgeBuffersForStream(this);
  }

  private void checkIsParquet(AbfsReadFooterMetrics abfsReadFooterMetrics) {
    String[] firstReadSize = abfsReadFooterMetrics.getSizeReadByFirstRead().split("_");
    String[] offDiffFirstSecondRead  = abfsReadFooterMetrics.getOffsetDiffBetweenFirstAndSecondRead().split("_");
    if((firstReadSize[0].equals(firstReadSize[1])) && (offDiffFirstSecondRead[0].equals(offDiffFirstSecondRead[1]))){
      abfsReadFooterMetrics.setParquetFile(true);
      abfsReadFooterMetrics.setSizeReadByFirstRead(firstReadSize[0]);
      abfsReadFooterMetrics.setOffsetDiffBetweenFirstAndSecondRead(offDiffFirstSecondRead[0]);
    }
  }

  /**
   * Not supported by this stream. Throws {@link UnsupportedOperationException}
   * @param readlimit ignored
   */
  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
  }

  /**
   * Not supported by this stream. Throws {@link UnsupportedOperationException}
   */
  @Override
  public synchronized void reset() throws IOException {
    throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
  }

  /**
   * gets whether mark and reset are supported by {@code ADLFileInputStream}. Always returns false.
   *
   * @return always {@code false}
   */
  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public synchronized void unbuffer() {
    buffer = null;
    // Preserve the original position returned by getPos()
    fCursor = fCursor - limit + bCursor;
    fCursorAfterLastRead = -1;
    bCursor = 0;
    limit = 0;
  }

  @Override
  public boolean hasCapability(String capability) {
    return StreamCapabilities.UNBUFFER.equals(toLowerCase(capability));
  }

  byte[] getBuffer() {
    return buffer;
  }

  @VisibleForTesting
  public int getReadAheadRange() {
    return readAheadRange;
  }

  @VisibleForTesting
  protected void setCachedSasToken(final CachedSASToken cachedSasToken) {
    this.cachedSasToken = cachedSasToken;
  }

  @VisibleForTesting
  public String getStreamID() {
    return inputStreamId;
  }

  /**
   * Getter for AbfsInputStreamStatistics.
   *
   * @return an instance of AbfsInputStreamStatistics.
   */
  @VisibleForTesting
  public AbfsInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  @VisibleForTesting
  public void registerListener(Listener listener1) {
    listener = listener1;
    tracingContext.setListener(listener);
  }

  /**
   * Getter for bytes read from readAhead buffer that fills asynchronously.
   *
   * @return value of the counter in long.
   */
  @VisibleForTesting
  public long getBytesFromReadAhead() {
    return bytesFromReadAhead;
  }

  /**
   * Getter for bytes read remotely from the data store.
   *
   * @return value of the counter in long.
   */
  @VisibleForTesting
  public long getBytesFromRemoteRead() {
    return bytesFromRemoteRead;
  }

  @VisibleForTesting
  public int getBufferSize() {
    return bufferSize;
  }

  @VisibleForTesting
  public int getReadAheadQueueDepth() {
    return readAheadQueueDepth;
  }

  @VisibleForTesting
  public boolean shouldAlwaysReadBufferSize() {
    return alwaysReadBufferSize;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return ioStatistics;
  }

  /**
   * Get the statistics of the stream.
   * @return a string value.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(super.toString());
    if (streamStatistics != null) {
      sb.append("AbfsInputStream@(").append(this.hashCode()).append("){");
      sb.append(streamStatistics.toString());
      sb.append("}");
    }
    return sb.toString();
  }

  @VisibleForTesting
  int getBCursor() {
    return this.bCursor;
  }

  @VisibleForTesting
  long getFCursor() {
    return this.fCursor;
  }

  @VisibleForTesting
  long getFCursorAfterLastRead() {
    return this.fCursorAfterLastRead;
  }

  @VisibleForTesting
  long getLimit() {
    return this.limit;
  }
}
