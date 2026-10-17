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

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsStatistic;
import org.apache.hadoop.fs.azurebfs.constants.ReadType;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutXmlParser;
import org.apache.hadoop.fs.impl.BackReference;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.CachedSASToken;
import org.apache.hadoop.fs.azurebfs.utils.Listener;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.getRelativePath;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.STREAM_ID_LEN;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.CAPABILITY_SAFE_READAHEAD;
import static org.apache.hadoop.fs.azurebfs.services.BlobLayoutCache.getFetchExecutor;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.INVALID_RANGE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_READ_ON_DIRECTORY;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * The AbfsInputStream for AbfsClient.
 */
public abstract class AbfsInputStream extends FSInputStream implements CanUnbuffer,
        StreamCapabilities, IOStatisticsSource {
  protected static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);
  //  Footer size is set to qualify for both ORC and parquet files
  public static final int FOOTER_SIZE = 16 * ONE_KB;
  public static final int MAX_OPTIMIZED_READ_ATTEMPTS = 2;

  private final int readAheadBlockSize;
  private final AbfsClient client;
  private final Statistics statistics;
  private final String path;

  private volatile long contentLength;
  private final int bufferSize; // default buffer size
  private final int footerReadSize; // default buffer size to read when reading footer
  private final int readAheadQueueDepth;         // initialized in constructor
  private String eTag;                  // eTag of the path when InputStream is created
  private final boolean tolerateOobAppends; // whether tolerate Oob Appends
  private final boolean readAheadEnabled; // whether enable readAhead;
  private final boolean readAheadV2Enabled; // whether enable readAhead V2;
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

  private boolean firstRead = true; // to identify first read for optimizations

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
  private final ContextEncryptionAdapter contextEncryptionAdapter;

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
  private final AbfsInputStreamContext context;
  private IOStatistics ioStatistics;
  private String filePathIdentifier;

  /**
   * This is the actual position within the object, used by
   * lazy seek to decide whether to seek on the next read or not.
   */
  private long nextReadPos;

  /** ABFS instance to be held by the input stream to avoid GC close. */
  private final BackReference fsBackRef;
  private final ReadBufferManager readBufferManager;

  private BlobLayoutCache layoutCache;

  private static final long MAX_FETCH_LIMIT = 64 * ONE_MB;

  private final boolean isDataLocalityCheckEnabled;

  /**
   * Constructor for AbfsInputStream.
   * @param client the ABFS client
   * @param statistics the statistics
   * @param path the file path
   * @param contentLength the content length
   * @param abfsInputStreamContext the input stream context
   * @param eTag the eTag of the file
   * @param tracingContext the tracing context
   */
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
    this.footerReadSize = Math.min(bufferSize, abfsInputStreamContext.getFooterReadBufferSize());
    this.readAheadQueueDepth = abfsInputStreamContext.getReadAheadQueueDepth();
    this.tolerateOobAppends = abfsInputStreamContext.isTolerateOobAppends();
    this.eTag = eTag;
    this.readAheadRange = abfsInputStreamContext.getReadAheadRange();
    this.readAheadEnabled = abfsInputStreamContext.isReadAheadEnabled();
    this.readAheadV2Enabled = abfsInputStreamContext.isReadAheadV2Enabled();
    this.alwaysReadBufferSize
        = abfsInputStreamContext.shouldReadBufferSizeAlways();
    this.bufferedPreadDisabled = abfsInputStreamContext
        .isBufferedPreadDisabled();
    this.cachedSasToken = new CachedSASToken(
        abfsInputStreamContext.getSasTokenRenewPeriodForStreamsInSeconds());
    this.streamStatistics = abfsInputStreamContext.getStreamStatistics();
    this.abfsReadFooterMetrics = client.getAbfsCounters().getAbfsReadFooterMetrics();
    this.inputStreamId = createInputStreamId();
    this.tracingContext = new TracingContext(tracingContext);
    this.tracingContext.setOperation(FSOperationType.READ);
    this.tracingContext.setStreamID(inputStreamId);
    this.tracingContext.setReadType(ReadType.UNKNOWN_READ);
    this.context = abfsInputStreamContext;
    readAheadBlockSize = abfsInputStreamContext.getReadAheadBlockSize();
    if (abfsReadFooterMetrics != null) {
      this.filePathIdentifier = eTag + path;
      synchronized (this) {
        abfsReadFooterMetrics.updateMap(filePathIdentifier);
      }
    }
    this.fsBackRef = abfsInputStreamContext.getFsBackRef();
    contextEncryptionAdapter = abfsInputStreamContext.getEncryptionAdapter();

    /*
     * Initialize the ReadBufferManager based on whether readAheadV2 is enabled or not.
     * Precedence is given to ReadBufferManagerV2.
     * If none of the V1 and V2 are enabled, then no read ahead will be done.
     */
    if (readAheadV2Enabled) {
      ReadBufferManagerV2.setReadBufferManagerConfigs(
          readAheadBlockSize, client.getAbfsConfiguration());
      readBufferManager = ReadBufferManagerV2.getBufferManager(client.getAbfsCounters());
    } else {
      ReadBufferManagerV1.setReadBufferManagerConfigs(readAheadBlockSize, client.getAbfsConfiguration());
      readBufferManager = ReadBufferManagerV1.getBufferManager();
    }

    if (streamStatistics != null) {
      ioStatistics = streamStatistics.getIOStatistics();
    }

    this.isDataLocalityCheckEnabled = client.getAbfsConfiguration() != null
        && client.getAbfsConfiguration().isDataLocalityEnabled()
        && eTag != null && client instanceof AbfsBlobClient;
    if (isDataLocalityCheckEnabled) {
      this.layoutCache = BlobLayoutCache.getInstance(
          client.getAbfsConfiguration().getBlobLayoutCacheEvictionMins(),
          client.getAbfsConfiguration().getBlobLayoutCacheMaxCount());
      this.layoutCache.registerStream(eTag, contentLength);
    }
  }

  /**
   * Returns the path of file associated with this stream.
   * @return the path of the file
   */
  public String getPath() {
    return path;
  }

  private String createInputStreamId() {
    return StringUtils.right(UUID.randomUUID().toString(), STREAM_ID_LEN);
  }

  /**
   * Retrieves the handler responsible for processing vectored read requests.
   * @return the {@link VectoredReadHandler} instance associated with the buffer manager.
   */
  VectoredReadHandler getVectoredReadHandler() {
    return getReadBufferManager().getVectoredReadHandler();
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
    TracingContext tc = new TracingContext(tracingContext);
    tc.setReadType(ReadType.DIRECT_READ);

    String endpoint = findEndpointForPosition(position, length);
    int bytesRead = readRemote(position, buffer, offset, length, tc, endpoint);
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

      long nextReadPosForMetric = nextReadPos;

      if (shouldReadFully()) {
        lastReadBytes = readFileCompletely(b, currentOff, currentLen);
      } else if (shouldReadLastBlock()) {
        lastReadBytes = readLastBlock(b, currentOff, currentLen);
      } else {
        lastReadBytes = readOneBlock(b, currentOff, currentLen);
      }

      if (abfsReadFooterMetrics != null) {
        abfsReadFooterMetrics.updateReadMetrics(filePathIdentifier, len, contentLength, nextReadPosForMetric);
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

  /**
   * {@inheritDoc}
   * Vectored read implementation for AbfsInputStream.
   *
   * @param ranges the byte ranges to read.
   * @param allocate the function to allocate ByteBuffer.
   */
  @Override
  public void readVectored(List<? extends FileRange> ranges,
      IntFunction<ByteBuffer> allocate) throws EOFException {
    getVectoredReadHandler().readVectored(this, ranges, allocate);
  }

  private boolean shouldReadFully() {
    return this.firstRead && !shouldRestrictGpsOnOpenFile() && this.context.readSmallFilesCompletely()
        && this.contentLength <= this.bufferSize;
  }

  private boolean shouldReadLastBlock() {
    long footerStart = max(0, this.contentLength - FOOTER_SIZE);
    return this.firstRead && !shouldRestrictGpsOnOpenFile() && this.context.optimizeFooterRead()
        && this.fCursor >= footerStart;
  }

  /**
   * Read one block of data into buffer.
   * @param b buffer
   * @param off offset
   * @param len length
   * @return number of bytes read
   * @throws IOException if there is an error
   */
  protected abstract int readOneBlock(byte[] b, int off, int len) throws IOException;

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
    tracingContext.setReadType(ReadType.SMALLFILE_READ);
    return optimisedRead(b, off, len, 0, contentLength);
  }

  // To do footer read of files when enabled.
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
    long lastBlockStart = max(0, contentLength - footerReadSize);
    bCursor = (int) (fCursor - lastBlockStart);
    // 0 if contentlength is < buffersize
    long actualLenToRead = min(footerReadSize, contentLength);
    tracingContext.setReadType(ReadType.FOOTER_READ);
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

  /**
   * Validate the read parameters.
   * @param b buffer byte array
   * @param off offset in buffer
   * @param len length to read
   * @return true if valid else false
   * @throws IOException if there is an error
   */
  protected boolean validate(final byte[] b, final int off, final int len)
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

  /**
   * Copy data from internal buffer to user buffer.
   * @param b user buffer
   * @param off offset
   * @param len length
   * @return number of bytes copied
   */
  protected int copyToUserBuffer(byte[] b, int off, int len){
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

  /**
     * Finds the read endpoint for the given position and length based on blob layout.
     *
     * @param position the file position to read from
     * @param length the number of bytes to read
     * @return the read endpoint URL, or {@code null} if blob layout is not available
     */
  private String findEndpointForPosition(long position, int length) {
    if (!isDataLocalityCheckEnabled) {
      return null;
    }
    List<BlobLayout.BlobRange> blobRanges = getBlobRanges(position,
        position + length - 1, tracingContext);
    if (blobRanges == null || blobRanges.isEmpty()) {
      return null;
    }
    return blobRanges.get(0).host();
  }

  /**
   * Internal read method which handles read-ahead logic.
   * @param position to read from
   * @param b buffer
   * @param offset in buffer
   * @param length to read
   * @param bypassReadAhead whether to bypass read-ahead
   * @return number of bytes read
   * @throws IOException if there is an error
   */
  protected int readInternal(final long position, final byte[] b,
      final int offset, final int length, final boolean bypassReadAhead)
      throws IOException {
    if (isReadAheadEnabled() && !bypassReadAhead) {
      // try reading from read-ahead
      if (offset != 0) {
        throw new IllegalArgumentException(
            "readahead buffers cannot have non-zero buffer offsets");
      }
      int receivedBytes;

      // queue read-aheads
      int numReadAheads = this.readAheadQueueDepth;
      long nextOffset = position;
      // First read to queue needs to be of readBufferSize and later
      // of readAhead Block size
      long nextSize = min((long) bufferSize, contentLength - nextOffset);
      LOG.debug("read ahead enabled issuing readheads num = {}", numReadAheads);
      TracingContext readAheadTracingContext = new TracingContext(
          tracingContext);
      readAheadTracingContext.setPrimaryRequestID();
      readAheadTracingContext.setReadType(ReadType.PREFETCH_READ);

      while (numReadAheads > 0 && nextOffset < contentLength) {

        List<BlobLayout.BlobRange> blobRangeList = getBlobRanges(nextOffset,
            nextOffset + nextSize - 1, tracingContext);
        if (blobRangeList == null || blobRangeList.isEmpty()) {
          LOG.debug("Read ranges not found. Issuing read ahead requestedOffset = {} requested size {}",
              nextOffset, nextSize);
          getReadBufferManager().queueReadAhead(this, nextOffset, (int) nextSize,
                  new TracingContext(readAheadTracingContext), null);
        } else if(!readAheadV2Enabled) {
            String endpoint = findEndpointForPosition(position, length);
            getReadBufferManager().queueReadAhead(this, nextOffset, (int) nextSize,
                    new TracingContext(readAheadTracingContext), endpoint);
        }
        else{
          LOG.debug(
              "Queuing all child buffers with requestedStart={}, requestedEnd={}, ranges=[{}]",
              nextOffset, nextOffset + nextSize - 1,
              blobRangeList.stream()
                  .map(s -> String.format("%d-%d:%s", s.start(), s.end(),
                      s.host()))
                  .collect(Collectors.joining(", ")));

          getReadBufferManager().queueReadAhead(
              this,
              nextOffset,
              (int) nextSize,
              blobRangeList,
              readAheadTracingContext
          );
        }
        nextOffset = nextOffset + nextSize;
        numReadAheads--;
        // From next round onwards should be of readahead block size.
        nextSize = min((long) readAheadBlockSize, contentLength - nextOffset);
      }

      // try reading from buffers first
      receivedBytes = getReadBufferManager().getBlock(this, position, length,
          b);
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
      TracingContext tc = new TracingContext(tracingContext);
      tc.setReadType(ReadType.MISSEDCACHE_READ);

      String endpoint = findEndpointForPosition(position, length);
      receivedBytes = readRemote(position, b, offset, length, tc, endpoint);
      return receivedBytes;
    } else {
      LOG.debug("read ahead disabled, reading remote");
      return readRemote(position, b, offset, length,
          new TracingContext(tracingContext),
          findEndpointForPosition(position, length));
    }
  }

  /**
   * Get blob layout for a file based on start and end position.
   * In case the layout is present in multiple pages, this method will internally
   * fetch all the pages and construct the layout.
   *
   * @param start start position of the read
   * @param end end position of the read
   * @return blob layout for the file
   * @throws IllegalStateException if the client is not an AbfsBlobClient
   */
  private BlobLayoutResponse getBlobLayout(final long start, final long end,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    BlobLayoutResponse fullLayout = new BlobLayoutResponse();
    TracingContext context = new TracingContext(tracingContext);
    context.setOperation(FSOperationType.GET_BLOB_LAYOUT);
    String nextMarker = null;
    do {
      AbfsRestOperation op = ((AbfsBlobClient) client).getBlobLayout(path,
          start, end, eTag, nextMarker, context);
      try {
        InputStream stream = op.getResult().getListResultStream();
        stream.reset();

        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser parser = factory.newSAXParser();
        BlobLayoutXmlParser handler = new BlobLayoutXmlParser();
        parser.parse(stream, handler);

        BlobLayoutResponse currPage = handler.getResponse();
        fullLayout.addBlobLayoutResponse(currPage);
        nextMarker = currPage.getNextMarker();
      } catch (Exception ex) {
        throw new AbfsRestOperationException(-1, "",
            "Failed to parse blob layout response", ex);
      }
    }
    while (!StringUtils.isEmpty(nextMarker));
    return fullLayout;
  }

  /**
   * Get Blob layout for the range. This method will first try to get the layout
   * from cache and if not present, it will call get blob layout API and update
   * the cache and return the request data to the caller method. This method fetch
   * additional layout to save future calls to get blob layout API.
   * @param start start position
   * @param end end position
   * @param tracingContext tracing context
   * @return List of blob ranges for the requested range
   */
  private List<BlobLayout.BlobRange> getBlobRanges(long start, long end,
      TracingContext tracingContext) {
    if (!isDataLocalityCheckEnabled) {
      return null;
    }

    TracingContext tracingContext1 = new TracingContext(tracingContext);
    tracingContext1.setOperation(FSOperationType.GET_BLOB_LAYOUT);
    List<BlobLayout.BlobRange> gaps = layoutCache.getGaps(eTag, start, end);
    AbfsCounters abfsCounters = client.getAbfsCounters();

    Set<CompletableFuture<Void>> dependencies = new HashSet<>();
    if (gaps == null) {
      // This case will come when data is not distributed across layouts.
      // In this case, we need to use host URL to fetch the data instead of
      // iterating through layouts.
      if (abfsCounters != null) {
        abfsCounters.incrementCounter(AbfsStatistic.LAYOUT_NOT_PRESENT, 1);
      }
      return null;
    } else if (!gaps.isEmpty()) {
      if (abfsCounters != null) {
        abfsCounters.incrementCounter(AbfsStatistic.LAYOUT_CACHE_MISS, 1);
      }
      for (BlobLayout.BlobRange gap : gaps) {
        // Determine the optimal range to fetch using bridge logic
        BlobLayout.BlobRange bridge = layoutCache.getBridgeGap(
            eTag, gap.start(), MAX_FETCH_LIMIT);

        long fStart = (bridge != null) ? bridge.start() : gap.start();
        long fEnd = (bridge != null) ? bridge.end() :
            Math.min(contentLength, gap.start() + MAX_FETCH_LIMIT) - 1;

        // Atomic operation: registers if absent, returns existing if present
        dependencies.add(
            registerAndFetch(start, end, fStart, fEnd, tracingContext1));
      }
    } else {
      if (abfsCounters != null) {
        abfsCounters.incrementCounter(AbfsStatistic.LAYOUT_CACHE_HIT, 1);
      }
    }

    if (!dependencies.isEmpty()) {
      try {
        CompletableFuture.allOf(dependencies.toArray(new CompletableFuture[0]))
            .get(client.getAbfsConfiguration()
                .getBlobLayoutFetchTimeoutInMillis(), TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // Transient failure (timeout/exception) waiting on in-flight fetches.
        // Do not poison the whole entry; the failed gaps' promises are already
        // removed, so a subsequent read will retry them.
        LOG.debug("Timed out or failed waiting for blob layout fetches for {}: {}",
            eTag, e.getMessage());
      }
    }

    if (abfsCounters != null) {
      abfsCounters.incrementCounter(AbfsStatistic.GET_LAYOUT_FROM_CACHE, 1);
    }
    return layoutCache.getBlobLayout(eTag, start, end);
  }

  /**
   * Checks the existing in-progress calls -  If the given range is being fetched
   * by any other thread, instead of creating another request it will wait on the
   * same request to complete and if not, it registers the range for fetch and
   * calls get blob layout API to fetch the layout for the given range and updates
   * the cache.
   * @param originalStart actual requested start
   * @param originalEnd actual requested end
   * @param start start in case of gap or bridge
   * @param end end in case of gap or bridge
   * @param tracingContext tracing context
   * @return computable future which will be completed once the layout is fetched and cache is updated
   */
  @VisibleForTesting
  CompletableFuture<Void> registerAndFetch(long originalStart,
      long originalEnd,
      long start,
      long end,
      TracingContext tracingContext) {

    // The cache handles the Map.compute and gives us the final future.
    return layoutCache.processInFlightPromises(eTag, (promiseList) -> {

      // 1. Check if already covered
      boolean isAlreadyCovered = promiseList.stream()
          .anyMatch(p -> p.start() <= originalStart && p.end() >= originalEnd);

      if (isAlreadyCovered) {
        if (client.getAbfsCounters() != null) {
          client.getAbfsCounters()
              .incrementCounter(AbfsStatistic.LAYOUT_SHARED_CALLS, 1);
        }
        return buildCompositeFuture(promiseList.stream()
            .filter(p -> p.start() <= originalEnd && p.end() >= originalStart)
            .map(BlobLayoutCache.InFlightPromise::future)
            .collect(Collectors.toSet()));
      }

      // 2. Interval Subtraction (Gaps)
      Deque<BlobLayout.BlobRange> gapsToProcess = new ArrayDeque<>();
      gapsToProcess.add(new BlobLayout.BlobRange(start, end, null));
      Set<CompletableFuture<Void>> dependencies = new HashSet<>();

      for (BlobLayoutCache.InFlightPromise p : promiseList) {
        int size = gapsToProcess.size();
        for (int i = 0; i < size; i++) {
          BlobLayout.BlobRange gap = gapsToProcess.pollFirst();
          if (gap == null) {break;}

          if (gap.start() <= p.end() && gap.end() >= p.start()) {
            dependencies.add(p.future());
            if (gap.start() < p.start()) {
              gapsToProcess.addLast(
                  new BlobLayout.BlobRange(gap.start(), p.start() - 1, null));
            }
            if (gap.end() > p.end()) {
              gapsToProcess.addLast(
                  new BlobLayout.BlobRange(p.end() + 1, gap.end(), null));
            }
          } else {
            gapsToProcess.addLast(gap);
          }
        }
        if (gapsToProcess.isEmpty()) {break;}
      }

      // 3. Registration & Execution
      for (BlobLayout.BlobRange remainingGap : gapsToProcess) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        BlobLayoutCache.InFlightPromise promise =
            new BlobLayoutCache.InFlightPromise(remainingGap.start(),
                remainingGap.end(), f);
        promiseList.add(promise);

        if (executeFetch(remainingGap.start(), remainingGap.end(), f,
            tracingContext)) {
          dependencies.add(f);
        } else {
          // The fetch pool rejected this task (saturated). Drop the promise we
          // just registered so a later read can retry this gap. This is a safe,
          // direct mutation of the local list since we hold the compute lock for
          // this eTag; using removePromise() here would re-enter the same
          // ConcurrentHashMap.compute and is not allowed. The gap is skipped for
          // this round (best-effort prefetch) rather than failing the whole read.
          promiseList.remove(promise);
        }
      }

      return buildCompositeFuture(dependencies);
    });
  }

  /**
   * Helper to handle the "Short-Circuit" logic for promises. If any of the
   * dependencies is already completed exceptionally, we short-circuit and
   * complete the returned future exceptionally without waiting for other dependencies.
   * @param dependencies set of dependent futures
   * @return a composite future that completes when all dependencies complete,
   * or completes exceptionally if any dependency fails
   */
  private CompletableFuture<Void> buildCompositeFuture(Set<CompletableFuture<Void>> dependencies) {
    CompletableFuture<Void> allOf = CompletableFuture.allOf(
        dependencies.toArray(new CompletableFuture[0]));
    for (CompletableFuture<Void> dep : dependencies) {
      dep.exceptionally(ex -> {
        allOf.completeExceptionally(ex);
        return null;
      });
    }
    return allOf;
  }

  /**
   * This method calls client's get blob API asynchronously and put the data in the cache.
   * <p>The fetch pool is non-blocking: when it is saturated the submission is
   * rejected with {@link RejectedExecutionException} instead of running the fetch
   * inline on the read thread. On rejection the supplied future is completed
   * exceptionally and {@code false} is returned so the caller can unregister the
   * in-flight promise (the task body that would otherwise remove it never runs).
   * @param start start position
   * @param end end position
   * @param future future which will be completed once the layout is fetched and cache is updated
   * @param tracingContext tracing context
   * @return {@code true} if the fetch task was accepted by the pool, {@code false}
   *         if it was rejected because the pool was saturated
   */
  private boolean executeFetch(long start, long end,
      CompletableFuture<Void> future, TracingContext tracingContext) {
    try {
      CompletableFuture.runAsync(() -> {
        try {
          // High-throughput network call
          BlobLayoutResponse response = getBlobLayout(start, end, tracingContext);
          if (client.getAbfsCounters() != null) {
            client.getAbfsCounters().incrementCounter(AbfsStatistic.PUT_LAYOUT_TO_CACHE, 1);
          }
          layoutCache.putBlobLayout(eTag, response, contentLength);

          // Success
          future.complete(null);
        } catch (Throwable e) {
          // Signal failure immediately to all dependent futures
          future.completeExceptionally(e);
        } finally {
          try {
            // Ensure promise is removed so future requests can retry the gap
            layoutCache.removePromise(eTag, start, end);
          } catch (Exception cleanupEx) {
            // Log cleanup failure but don't allow it to hang the thread
            LOG.error("Failed to remove promise for {}-{}: {}", start, end,
                cleanupEx.getMessage());
          }
        }
      }, getFetchExecutor());
      return true;
    } catch (RejectedExecutionException rejected) {
      // Pool saturated: do not run the fetch on the read thread. The task body
      // (which normally removes the promise) never runs, so completion and
      // promise cleanup are handled here at the submission site.
      LOG.debug("Blob layout fetch rejected (pool saturated) for {}-{}: {}",
          start, end, rejected.getMessage());
      future.completeExceptionally(rejected);
      return false;
    }
  }

  /**
   * Creates an exception indicating that a read operation was attempted on a directory.
   *
   * @return an {@link UnsupportedOperationException} indicating the operation is not permitted on a directory
   */
  private UnsupportedOperationException directoryReadException() {
    return new UnsupportedOperationException(
        ERR_READ_ON_DIRECTORY,
        new AbfsRestOperationException(
            AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
            AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
            ERR_READ_ON_DIRECTORY,
            null));
  }

  /**
   * Checks if the current path is a directory (for both implicit and explicit) in FNS account.
   * If the path is a directory, throws an exception indicating that read operations are not permitted.
   *
   * @throws IOException if the path is a directory or if there is an error accessing the path status
   */
  private void checkIfDirPathInFNS() throws IOException {
    AbfsHttpOperation gpsOp = client.getPathStatus(
        getRelativePath(new Path(path)),
        false,
        tracingContext,
        contextEncryptionAdapter).getResult();

    if (client.checkIsDir(gpsOp)) {
      throw directoryReadException();
    }
  }

  /**
   * Extracts the content length from the HTTP response headers.
   * Uses the Content-Range header to determine the total file size, which is necessary
   * for handling partial reads correctly.
   *
   * @param op the ABFS HTTP operation containing the response headers
   * @return the content length of the file
   */
  private long extractContentLength(AbfsHttpOperation op) {
    // We need to use content range header instead of content length to take care of partial reads
    String contentRange = op.getResponseHeader(HttpHeaderConfigurations.CONTENT_RANGE);
    contentLength = 0;
    if (!StringUtils.isEmpty(contentRange)) {
      contentLength = Long.parseLong(contentRange.split(AbfsHttpConstants.FORWARD_SLASH)[1]);
    }
    return contentLength;
  }

  /**
   * Reads data from the remote store into the provided buffer.
   *
   * @param position the position in the file to start reading from
   * @param b the buffer into which the data is read
   * @param offset the start offset in the buffer at which the data is written
   * @param length the maximum number of bytes to read
   * @param tracingContext the tracing context for this operation
   * @param endpoint the endpoint URL to use for the read operation, or null to use the default
   * @return the number of bytes read, or -1 if the end of the file is reached
   * @throws IOException if an I/O error occurs or if invalid arguments are provided
   */
  int readRemote(long position, byte[] b, int offset, int length,
      TracingContext tracingContext, String endpoint) throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException(
          "attempting to read from negative offset");
    }
    if (!(shouldRestrictGpsOnOpenFile() && isFirstRead()) && position >= contentLength) {
      return -1;  // Hadoop prefers -1 to EOFException
    }
    if (b == null) {
      throw new IllegalArgumentException(
          "null byte array passed in to read() method");
    }
    if (offset >= b.length) {
      throw new IllegalArgumentException("offset greater than length of array");
    }
    if (length < 0) {
      throw new IllegalArgumentException(
          "requested read length is less than zero");
    }
    if (length > (b.length - offset)) {
      throw new IllegalArgumentException(
          "requested read length is more than will fit after requested offset in buffer");
    }

    final AbfsRestOperation op = readTask(position, b, offset, length,
        tracingContext, endpoint);
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
   * Executes a remote read operation using the ABFS client.
   *
   * This method performs the actual HTTP request to read data from the remote
   * Azure Blob File System, handling both the default and endpoint-specific
   * read logic. It also updates performance tracking and stream statistics,
   * manages SAS token renewal, and logs relevant debug information.
   *
   * @param position the position in the file to start reading from
   * @param b the buffer into which the data is read
   * @param offset the start offset in the buffer at which the data is written
   * @param length the maximum number of bytes to read
   * @param tracingContext the tracing context for this operation
   * @param endpoint the endpoint URL to use for the read operation, or null to use the default
   * @return the AbfsRestOperation representing the remote read
   * @throws IOException if an I/O error occurs or if the ABFS client throws an exception
   */
  private AbfsRestOperation readTask(long position, byte[] b, int offset,
      int length, TracingContext tracingContext, String endpoint)
      throws IOException {
    final AbfsRestOperation op;
    AbfsPerfTracker tracker = client.getAbfsPerfTracker();
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker, "readRemote",
        "read")) {
      if (streamStatistics != null) {
        streamStatistics.remoteReadOperation();
      }
      LOG.trace(
          "Trigger client.read for path={} position={} offset={} length={}",
          path, position, offset, length);
      tracingContext.setPosition(String.valueOf(position));
      if (endpoint != null) {
        op = client.read(path, position, b, offset, length,
            tolerateOobAppends ? "*" : eTag, cachedSasToken.get(),
            contextEncryptionAdapter, tracingContext, endpoint);
      } else {
        op = client.read(path, position, b, offset, length,
            tolerateOobAppends ? "*" : eTag, cachedSasToken.get(),
            contextEncryptionAdapter, tracingContext);
      }

      // Update metadata on first read if restrictGpsOnOpenFile is enabled
      if (shouldRestrictGpsOnOpenFile() && isFirstRead()) {
        if (client.checkIsDir(op.getResult())) {
          throw directoryReadException();
        }
        contentLength = extractContentLength(op.getResult());
        eTag = extractEtagHeader(op.getResult());
      }
      cachedSasToken.update(op.getSasToken());
      LOG.debug("issuing HTTP GET request params position = {} b.length = {} "
          + "offset = {} length = {}", position, b.length, offset, length);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
      incrementReadOps();
    } catch (AzureBlobFileSystemException ex) {
      if (ex instanceof AbfsRestOperationException ere) {
        int status = ere.getStatusCode();
        if (ere.getErrorMessage().contains(ERR_READ_ON_DIRECTORY)) {
          throw ere;
        }
        boolean isHnsEnabled = client.getIsNamespaceEnabled();

        // Case-1: 404 NOT FOUND
        if (status == HttpURLConnection.HTTP_NOT_FOUND) {
          /*
           * If HNS account or restrictGpsOnOpenFile disabled,
           * we dont need any further checks
           */
          if (isHnsEnabled || !shouldRestrictGpsOnOpenFile()) {
            throw new FileNotFoundException(ere.getMessage());
          }

          try {
            /*
             * For FNS account with restrictGpsOnOpenFile enabled,
             * need to rule out if the path is an implicit directory
             */
            checkIfDirPathInFNS();
          } catch (AzureBlobFileSystemException gpsEx) {
            AbfsRestOperationException gpsEre = (AbfsRestOperationException) gpsEx;
            if (gpsEre.getErrorMessage().contains(ERR_READ_ON_DIRECTORY)) {
              throw gpsEre;
            }
            // The file does not exist
            else {
              throw new FileNotFoundException(gpsEre.getMessage());
            }
          }
        }

        // Case-2: 416 INVALID RANGE
        if (!isHnsEnabled && INVALID_RANGE.equals(ere.getErrorCode())) {
          // Need to rule out if the path is an explicit directory
          checkIfDirPathInFNS();
        }
      }
      // Default: propagate original error
      throw new IOException(ex);
    }
    return op;
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
    if (!(shouldRestrictGpsOnOpenFile() && isFirstRead())) {
      final long remaining = this.contentLength - this.getPos();
      return remaining <= Integer.MAX_VALUE
              ? (int) remaining : Integer.MAX_VALUE;
    }
    return Integer.MAX_VALUE;
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

  /**
   * Get the tracing context associated with this stream.
   * @return the tracing context
   */
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
    if (getReadBufferManager() != null) {
      getReadBufferManager().purgeBuffersForStream(this);
    }
    buffer = null; // de-reference the buffer so it can be GC'ed sooner
    if (contextEncryptionAdapter != null) {
      contextEncryptionAdapter.destroy();
    }
    if (layoutCache != null) {
      layoutCache.deregisterStream(eTag);
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
    return switch (toLowerCase(capability)) {
      case StreamCapabilities.UNBUFFER, StreamCapabilities.VECTOREDIO -> true;
      default -> false;
    };
  }

  /**
   * Getter for buffer.
   * @return the buffer
   */
  synchronized byte[] getBuffer() {
    return buffer;
  }

  /**
   * Setter for buffer.
   * @param buffer the buffer to set
   */
  protected synchronized void setBuffer(byte[] buffer) {
    this.buffer = buffer;
  }

  /**
   * Checks if any version of read ahead is enabled.
   * If both are disabled, then skip read ahead logic.
   * @return true if read ahead is enabled, false otherwise.
   */
  @VisibleForTesting
  public boolean isReadAheadEnabled() {
    return (readAheadEnabled || readAheadV2Enabled) && getReadBufferManager() != null;
  }

  /**
   * Getter for user configured read ahead range.
   * @return the read ahead range in int.
   */
  @VisibleForTesting
  public int getReadAheadRange() {
    return readAheadRange;
  }

  /**
   * Setter for cachedSasToken.
   * @param cachedSasToken the cachedSasToken to set
   */
  @VisibleForTesting
  protected void setCachedSasToken(final CachedSASToken cachedSasToken) {
    this.cachedSasToken = cachedSasToken;
  }

  /**
   * Getter for inputStreamId.
   * @return the inputStreamId
   */
  @VisibleForTesting
  public String getStreamID() {
    return inputStreamId;
  }

  /**
   * Getter for eTag.
   *
   * @return the eTag
   */
  public String getETag() {
    return eTag;
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

  /**
   * Register a listener for this stream.
   * @param listener1 the listener to register
   */
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

  /**
   * Getter for buffer size.
   * @return the buffer size
   */
  @VisibleForTesting
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * Getter for footer read buffer size.
   * @return the footer read buffer size
   */
  @VisibleForTesting
  protected int getFooterReadBufferSize() {
    return footerReadSize;
  }

  /**
   * Getter for read ahead queue depth.
   * @return the read ahead queue depth
   */
  @VisibleForTesting
  public int getReadAheadQueueDepth() {
    return readAheadQueueDepth;
  }

  /**
   * Getter for alwaysReadBufferSize.
   * @return the alwaysReadBufferSize
   */
  @VisibleForTesting
  public boolean shouldAlwaysReadBufferSize() {
    return alwaysReadBufferSize;
  }

  /**
   * Get the IOStatistics for the stream.
   * @return IOStatistics
   */
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
    sb.append("AbfsInputStream@(").append(this.hashCode()).append("){");
    sb.append("[" + CAPABILITY_SAFE_READAHEAD + "]");
    if (streamStatistics != null) {
      sb.append(", ").append(streamStatistics);
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Getter for bCursor.
   * @return the bCursor
   */
  @VisibleForTesting
  synchronized int getBCursor() {
    return this.bCursor;
  }

  /**
   * Setter for bCursor.
   * @param bCursor the bCursor to set
   */
  protected synchronized void setBCursor(int bCursor) {
    this.bCursor = bCursor;
  }

  /**
   * Getter for fCursor.
   * @return the fCursor
   */
  @VisibleForTesting
  synchronized long getFCursor() {
    return this.fCursor;
  }

  /**
   * Setter for fCursor.
   * @param fCursor the fCursor to set
   */
  protected synchronized void setFCursor(long fCursor) {
    this.fCursor = fCursor;
  }

  /**
   * Getter for fCursorAfterLastRead.
   * @return the fCursorAfterLastRead
   */
  @VisibleForTesting
  synchronized long getFCursorAfterLastRead() {
    return this.fCursorAfterLastRead;
  }

  /**
   * Setter for fCursorAfterLastRead.
   * @param fCursorAfterLastRead the fCursorAfterLastRead to set
   */
  protected synchronized void setFCursorAfterLastRead(long fCursorAfterLastRead) {
    this.fCursorAfterLastRead = fCursorAfterLastRead;
  }

  /**
   * Getter for limit.
   * @return the limit
   */
  @VisibleForTesting
  synchronized int getLimit() {
    return this.limit;
  }

  /**
   * Setter for limit.
   * @param limit the limit to set
   */
  protected synchronized void setLimit(int limit) {
    this.limit = limit;
  }

  /**
   * Getter for firstRead.
   * @return the firstRead
   */
  boolean isFirstRead() {
    return this.firstRead;
  }

  /**
   * Setter for firstRead.
   * @param firstRead the firstRead to set
   */
  protected void setFirstRead(boolean firstRead) {
    this.firstRead = firstRead;
  }

  /**
   * Getter for fsBackRef.
   * @return the fsBackRef
   */
  @VisibleForTesting
  BackReference getFsBackRef() {
    return fsBackRef;
  }

  /**
   * Getter for readBufferManager.
   * @return the readBufferManager
   */
  @VisibleForTesting
  ReadBufferManager getReadBufferManager() {
    return readBufferManager;
  }

  /**
   * Minimum seek distance for vector reads.
   * @return the minimum seek distance
   */
  @Override
  public int minSeekForVectorReads() {
    return client.getAbfsConfiguration().getMinSeekForVectoredReads();
  }

  /**
   * Maximum read size for vector reads.
   * @return the maximum read size
   */
  @Override
  public int maxReadSizeForVectorReads() {
    return client.getAbfsConfiguration().getMaxReadSizeForVectoredReads();
  }

  /**
   * Getter for contentLength.
   * @return the contentLength
   */
  protected long getContentLength() {
    return contentLength;
  }

  public boolean shouldRestrictGpsOnOpenFile() {
    return context.shouldRestrictGpsOnOpenFile();
  }
}
