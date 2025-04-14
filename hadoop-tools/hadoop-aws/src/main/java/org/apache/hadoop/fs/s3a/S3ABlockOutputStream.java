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

package org.apache.hadoop.fs.s3a;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.s3a.impl.ProgressListener;
import org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.PutTracker;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.util.Progressable;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.CONTENT_TYPE_OCTET_STREAM;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.*;
import static org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext.EMPTY_BLOCK_OUTPUT_STREAM_STATISTICS;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.util.functional.FutureIO.awaitAllFutures;
import static org.apache.hadoop.util.functional.FutureIO.cancelAllFuturesAndAwaitCompletion;

/**
 * Upload files/parts directly via different buffering mechanisms:
 * including memory and disk.
 * <p>
 * Key Features
 * <ol>
 *   <li>Support single/multipart uploads</li>
 *   <li>Multiple buffering options</li>
 *   <li>Magic files are uploaded but not completed</li>
 *   <li>Implements {@link Abortable} API</li>
 *   <li>Doesn't implement {@link Syncable}; whether to ignore or reject calls is configurable</li>a
 *   <li>When multipart uploads are triggered, will queue blocks for asynchronous uploads</li>
 *   <li>Provides progress information to any supplied {@link Progressable} callback,
 *       during async uploads and in the {@link #close()} operation.</li>
 *   <li>If a {@link Progressable} passed in to the create() call implements
 *       {@link ProgressListener}, it will get detailed callbacks on internal events.
 *       Important: these may come from different threads.
 *   </li>
 *
 * </ol>
 * This class is best described as "complicated".
 * <ol>
 *   <li>For "normal" files, data is buffered until either of:
 *   the limit of {@link #blockSize} is reached or the stream is closed.
 *   </li>
 *   <li>If if there are any problems call mukund</li>
 * </ol>
 * <p>
 * The upload will not be completed until {@link #close()}, and
 * then only if {@link PutTracker#outputImmediatelyVisible()} is true.
 * <p>
 * If less than a single block of data has been written before {@code close()}
 * then it will uploaded as a single PUT (non-magic files), otherwise
 * (larger files, magic files) a multipart upload is initiated and blocks
 * uploaded as the data accrued reaches the block size.
 * <p>
 * The {@code close()} call blocks until all uploads have been completed.
 * This may be a slow operation: progress callbacks are made during this
 * process to reduce the risk of timeouts.
 * <p>
 * Syncable is declared as supported so the calls can be
 * explicitly rejected if the filesystem is configured to do so.
 * <p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3ABlockOutputStream extends OutputStream implements
    StreamCapabilities, IOStatisticsSource, Syncable, Abortable {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3ABlockOutputStream.class);

  private static final String E_NOT_SYNCABLE =
      "S3A streams are not Syncable. See HADOOP-17597.";

  /**
   * How long to wait for uploads to complete after being cancelled before
   * the blocks themselves are closed: 15 seconds.
   */
  private static final Duration TIME_TO_AWAIT_CANCEL_COMPLETION = Duration.ofSeconds(15);

  /** Object being uploaded. */
  private final String key;

  /** Size of all blocks. */
  private final long blockSize;

  /** IO Statistics. */
  private final IOStatistics iostatistics;

  /**
   * The options this instance was created with.
   */
  private final BlockOutputStreamBuilder builder;

  /** Total bytes for uploads submitted so far. */
  private long bytesSubmitted;

  /** Callback for progress. */
  private final ProgressListener progressListener;
  private final ListeningExecutorService executorService;

  /**
   * Factory for blocks.
   */
  private final S3ADataBlocks.BlockFactory blockFactory;

  /** Preallocated byte buffer for writing single characters. */
  private final byte[] singleCharWrite = new byte[1];

  /** Multipart upload details; null means none started. */
  private MultiPartUpload multiPartUpload;

  /** Closed flag. */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Current data block. Null means none currently active */
  private S3ADataBlocks.DataBlock activeBlock;

  /** Count of blocks uploaded. */
  private long blockCount = 0;

  /** Statistics to build up. */
  private final BlockOutputStreamStatistics statistics;

  /**
   * Write operation helper; encapsulation of the filesystem operations.
   * This contains the audit span for the operation, and activates/deactivates
   * it within calls.
   */
  private final WriteOperations writeOperationHelper;

  /**
   * Track multipart put operation.
   */
  private final PutTracker putTracker;

  /** Should Syncable calls be downgraded? */
  private final boolean downgradeSyncableExceptions;

  /**
   * Downagraded syncable API calls are only logged at warn
   * once across the entire process.
   */
  private static final LogExactlyOnce WARN_ON_SYNCABLE =
      new LogExactlyOnce(LOG);

  /** is client side encryption enabled? */
  private final boolean isCSEEnabled;

  /** Thread level IOStatistics Aggregator. */
  private final IOStatisticsAggregator threadIOStatisticsAggregator;

  /** Is multipart upload enabled? */
  private final boolean isMultipartUploadEnabled;

  /**
   * Object write option flags.
   */
  private final EnumSet<WriteObjectFlags> writeObjectFlags;

  /**
   * An S3A output stream which uploads partitions in a separate pool of
   * threads; different {@link S3ADataBlocks.BlockFactory}
   * instances can control where data is buffered.
   * If the passed in put tracker returns true on
   * {@link PutTracker#initialize()} then a multipart upload is
   * initiated; this triggers a remote call to the store.
   * On a normal upload no such operation takes place; the only
   * failures which surface will be related to buffer creation.
   * @throws IOException on any problem initiating a multipart upload or creating
   *                     a disk storage buffer.
   * @throws OutOfMemoryError lack of space to create any memory buffer
   */
  @Retries.RetryTranslated
  S3ABlockOutputStream(BlockOutputStreamBuilder builder)
      throws IOException {
    builder.validate();
    this.builder = builder;
    this.key = builder.key;
    this.blockFactory = builder.blockFactory;
    this.statistics = builder.statistics;
    // test instantiations may not provide statistics;
    this.iostatistics = statistics.getIOStatistics();
    this.writeOperationHelper = builder.writeOperations;
    this.putTracker = builder.putTracker;
    this.writeObjectFlags = builder.putOptions.getWriteObjectFlags();
    this.executorService = MoreExecutors.listeningDecorator(
        builder.executorService);
    this.multiPartUpload = null;
    Progressable progress = builder.progress;
    this.progressListener = (progress instanceof ProgressListener) ?
        (ProgressListener) progress
        : new ProgressableListener(progress);
    downgradeSyncableExceptions = builder.downgradeSyncableExceptions;

    // look for multipart support.
    this.isMultipartUploadEnabled = builder.isMultipartUploadEnabled;
    // block size is infinite if multipart is disabled, so ignore
    // what was passed in from the builder.
    this.blockSize = isMultipartUploadEnabled
        ? builder.blockSize
        : -1;

    // if required to be multipart by the committer put tracker or
    // write flags (i.e createFile() options, initiate multipart uploads.
    // this will fail fast if the store doesn't support multipart uploads
    if (putTracker.initialize()) {
      LOG.debug("Put tracker requests multipart upload");
      initMultipartUpload();
    } else if (writeObjectFlags.contains(WriteObjectFlags.CreateMultipart)) {
      // this not merged simply to avoid confusion
      // to what to do it both are set, so as to guarantee
      // the put tracker initialization always takes priority
      // over any file flag.
      LOG.debug("Multipart initiated from createFile() options");
      initMultipartUpload();
    }
    this.isCSEEnabled = builder.isCSEEnabled;
    this.threadIOStatisticsAggregator = builder.ioStatisticsAggregator;
    // create that first block. This guarantees that an open + close sequence
    // writes a 0-byte entry.
    createBlockIfNeeded();
    LOG.debug("Initialized S3ABlockOutputStream for {}" +
        " output to {}", key, activeBlock);
  }

  /**
   * Demand create a destination block.
   * @return the active block; null if there isn't one.
   * @throws IOException any failure to create a block in the local FS.
   * @throws OutOfMemoryError lack of space to create any memory buffer
   */
  private synchronized S3ADataBlocks.DataBlock createBlockIfNeeded()
      throws IOException {
    if (activeBlock == null) {
      blockCount++;
      if (blockCount>= Constants.MAX_MULTIPART_COUNT) {
        LOG.error("Number of partitions in stream exceeds limit for S3: "
             + Constants.MAX_MULTIPART_COUNT +  " write may fail.");
      }
      activeBlock = blockFactory.create(blockCount, this.blockSize, statistics);
    }
    return activeBlock;
  }

  /**
   * Synchronized accessor to the active block.
   * @return the active block; null if there isn't one.
   */
  private synchronized S3ADataBlocks.DataBlock getActiveBlock() {
    return activeBlock;
  }

  /**
   * Predicate to query whether or not there is an active block.
   * @return true if there is an active block.
   */
  private synchronized boolean hasActiveBlock() {
    return activeBlock != null;
  }

  /**
   * Clear the active block.
   */
  private void clearActiveBlock() {
    if (activeBlock != null) {
      LOG.debug("Clearing active block");
    }
    synchronized (this) {
      activeBlock = null;
    }
  }

  /**
   * Check for the stream being open.
   * @throws ClosedIOException if the stream is closed.
   */
  @VisibleForTesting
  void checkOpen() throws ClosedIOException {
    if (closed.get()) {
      throw new ClosedIOException(key, "Stream is closed:  " + this);
    }
  }

  /**
   * The flush operation does not trigger an upload; that awaits
   * the next block being full. What it does do is call {@code flush() }
   * on the current block, leaving it to choose how to react.
   * <p>
   * If the stream is closed, a warning is logged but the exception
   * is swallowed.
   * @throws IOException Any IO problem flushing the active data block.
   */
  @Override
  public synchronized void flush() throws IOException {
    try {
      checkOpen();
    } catch (ClosedIOException e) {
      LOG.warn("Stream closed: {}", e.getMessage());
      return;
    }
    S3ADataBlocks.DataBlock dataBlock = getActiveBlock();
    if (dataBlock != null) {
      dataBlock.flush();
    }
  }

  /**
   * Writes a byte to the destination. If this causes the buffer to reach
   * its limit, the actual upload is submitted to the threadpool.
   * @param b the int of which the lowest byte is written
   * @throws IOException on any problem
   */
  @Override
  public synchronized void write(int b) throws IOException {
    singleCharWrite[0] = (byte)b;
    write(singleCharWrite, 0, 1);
  }

  /**
   * Writes a range of bytes from to the memory buffer. If this causes the
   * buffer to reach its limit, the actual upload is submitted to the
   * threadpool and the remainder of the array is written to memory
   * (recursively).
   * In such a case, if not already initiated, a multipart upload is
   * started.
   * @param source byte array containing
   * @param offset offset in array where to start
   * @param len number of bytes to be written
   * @throws IOException on any problem
   * @throws ClosedIOException if the stream is closed.
   */
  @Override
  @Retries.RetryTranslated
  public synchronized void write(@Nonnull byte[] source, int offset, int len)
      throws IOException {

    S3ADataBlocks.validateWriteArgs(source, offset, len);
    checkOpen();
    if (len == 0) {
      return;
    }
    statistics.writeBytes(len);
    S3ADataBlocks.DataBlock block = createBlockIfNeeded();
    int written = block.write(source, offset, len);
    if (!isMultipartUploadEnabled) {
      // no need to check for space as multipart uploads
      // are not available...everything is saved to a single
      // (disk) block.
      return;
    }
    // look to see if another block is needed to complete
    // the upload or exactly a block was written.
    int remainingCapacity = (int) block.remainingCapacity();
    if (written < len) {
      // not everything was written —the block has run out
      // of capacity
      // Trigger an upload then process the remainder.
      LOG.debug("writing more data than block has capacity -triggering upload");
      uploadCurrentBlock(false);
      // tail recursion is mildly expensive, but given buffer sizes must be MB.
      // it's unlikely to recurse very deeply.
      this.write(source, offset + written, len - written);
    } else {
      if (remainingCapacity == 0 && !isCSEEnabled) {
        // the whole buffer is done, trigger an upload
        uploadCurrentBlock(false);
      }
    }
  }

  /**
   * Start an asynchronous upload of the current block.
   *
   * @param isLast true, if part being uploaded is last and client side
   *               encryption is enabled.
   * @throws IOException Problems opening the destination for upload,
   *                     initializing the upload, or if a previous operation
   *                     has failed.
   */
  @Retries.RetryTranslated
  private synchronized void uploadCurrentBlock(boolean isLast)
      throws IOException {
    Preconditions.checkState(hasActiveBlock(), "No active block");
    LOG.debug("Writing block # {}", blockCount);
    initMultipartUpload();
    try {
      multiPartUpload.uploadBlockAsync(getActiveBlock(), isLast);
      bytesSubmitted += getActiveBlock().dataSize();
    } finally {
      // set the block to null, so the next write will create a new block.
      clearActiveBlock();
    }
  }

  /**
   * Init multipart upload. Assumption: this is called from
   * a synchronized block.
   * Note that this makes a blocking HTTPS request to the far end, so
   * can take time and potentially fail.
   * @throws IOException failure to initialize the upload
   */
  @Retries.RetryTranslated
  private void initMultipartUpload() throws IOException {
    Preconditions.checkState(isMultipartUploadEnabled,
        "multipart upload is disabled");
    if (multiPartUpload == null) {
      LOG.debug("Initiating Multipart upload");
      multiPartUpload = new MultiPartUpload(key);
    }
  }

  /**
   * Close the stream.
   * <p>
   * This will not return until the upload is complete
   * or the attempt to perform the upload has failed or been interrupted.
   * Exceptions raised in this method are indicative that the write has
   * failed and data is at risk of being lost.
   * @throws IOException on any failure.
   * @throws InterruptedIOException if the wait for uploads to complete was interrupted.
   */
  @Override
  @Retries.RetryTranslated
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      // already closed
      LOG.debug("Ignoring close() as stream is already closed");
      return;
    }
    progressListener.progressChanged(CLOSE_EVENT, 0);
    S3ADataBlocks.DataBlock block = getActiveBlock();
    boolean hasBlock = hasActiveBlock();
    LOG.debug("{}: Closing block #{}: current block= {}",
        this,
        blockCount,
        hasBlock ? block : "(none)");
    long bytes = 0;
    try {
      if (multiPartUpload == null) {
        if (hasBlock) {
          // no uploads of data have taken place, put the single block up.
          // This must happen even if there is no data, so that 0 byte files
          // are created.
          bytes = putObject();
          bytesSubmitted = bytes;
        }
      } else {
        // there's an MPU in progress
        // IF there is more data to upload, or no data has yet been uploaded,
        // PUT the final block
        if (hasBlock &&
            (block.hasData() || multiPartUpload.getPartsSubmitted() == 0)) {
          // send last part and set the value of isLastPart to true.
          // Necessary to set this "true" in case of client side encryption.
          uploadCurrentBlock(true);
        }
        // wait for the part uploads to finish
        // this may raise CancellationException as well as any IOE.
        final List<CompletedPart> partETags =
            multiPartUpload.waitForAllPartUploads();
        bytes = bytesSubmitted;
        final String uploadId = multiPartUpload.getUploadId();
        LOG.debug("Multipart upload to {} ID {} containing {} blocks",
            key, uploadId, partETags.size());

        // then complete the operation
        if (putTracker.aboutToComplete(uploadId,
            partETags,
            bytes,
            iostatistics)) {
          multiPartUpload.complete(partETags);
        } else {
          LOG.info("File {} will be visible when the job is committed", key);
        }
      }
      if (!putTracker.outputImmediatelyVisible()) {
        // track the number of bytes uploaded as commit operations.
        statistics.commitUploaded(bytes);
      }
      LOG.debug("Upload complete to {} by {}", key, writeOperationHelper);
    } catch (IOException ioe) {
      // the operation failed.
      // if this happened during a multipart upload, abort the
      // operation, so as to not leave (billable) data
      // pending on the bucket
      maybeAbortMultipart();
      writeOperationHelper.writeFailed(ioe);
      throw ioe;
    } catch (CancellationException e) {
      // waiting for the upload was cancelled.
      // abort uploads
      maybeAbortMultipart();
      writeOperationHelper.writeFailed(e);
      // and raise an InterruptedIOException
      throw (IOException)(new InterruptedIOException(e.getMessage())
          .initCause(e));
    } finally {
      cleanupOnClose();
    }
    // Note end of write. This does not change the state of the remote FS.
    writeOperationHelper.writeSuccessful(bytes);
  }

  /**
   * Final operations in close/abort of stream.
   * Shuts down block factory, closes any active block,
   * and pushes out statistics.
   */
  private synchronized void cleanupOnClose() {
    cleanupWithLogger(LOG, getActiveBlock(), blockFactory);
    mergeThreadIOStatistics(statistics.getIOStatistics());
    LOG.debug("Statistics: {}", statistics);
    cleanupWithLogger(LOG, statistics);
    clearActiveBlock();
  }

  /**
   * Merging the current thread's IOStatistics with the current IOStatistics
   * context.
   *
   * @param streamStatistics Stream statistics to be merged into thread
   *                         statistics aggregator.
   */
  private void mergeThreadIOStatistics(IOStatistics streamStatistics) {
    getThreadIOStatistics().aggregate(streamStatistics);
  }

  /**
   * Best effort abort of the multipart upload; sets
   * the field to null afterwards.
   * <p>
   * Cancels any active uploads on the first invocation.
   * @return any exception caught during the operation. If FileNotFoundException
   * it means the upload was not found.
   */
  @Retries.RetryTranslated
  private synchronized IOException maybeAbortMultipart() {
    if (multiPartUpload != null) {
      try {
        return multiPartUpload.abort();
      } finally {
        multiPartUpload = null;
      }
    } else {
      return null;
    }
  }

  /**
   * Abort any active uploads, enter closed state.
   * @return the outcome
   */
  @Override
  @Retries.RetryTranslated
  public AbortableResult abort() {
    if (closed.getAndSet(true)) {
      // already closed
      LOG.debug("Ignoring abort() as stream is already closed");
      return new AbortableResultImpl(true, null);
    }

    // abort the upload.
    // if not enough data has been written to trigger an upload: this is no-op.
    // if a multipart had started: abort it by cancelling all active uploads
    // and aborting the multipart upload on s3.
    try (DurationTracker d =
             statistics.trackDuration(INVOCATION_ABORT.getSymbol())) {
      // abort. If the upload is not found, report as already closed.
      final IOException anyCleanupException = maybeAbortMultipart();
      return new AbortableResultImpl(
          anyCleanupException instanceof FileNotFoundException,
          anyCleanupException);
    } finally {
      cleanupOnClose();
    }
  }

  /**
   * Abortable result.
   */
  private static final class AbortableResultImpl implements AbortableResult {

    /**
     * Had the stream already been closed/aborted?
     */
    private final boolean alreadyClosed;

    /**
     * Was any exception raised during non-essential
     * cleanup actions (i.e. MPU abort)?
     */
    private final IOException anyCleanupException;

    /**
     * Constructor.
     * @param alreadyClosed Had the stream already been closed/aborted?
     * @param anyCleanupException Was any exception raised during cleanup?
     */
    private AbortableResultImpl(final boolean alreadyClosed,
        final IOException anyCleanupException) {
      this.alreadyClosed = alreadyClosed;
      this.anyCleanupException = anyCleanupException;
    }

    @Override
    public boolean alreadyClosed() {
      return alreadyClosed;
    }

    @Override
    public IOException anyCleanupException() {
      return anyCleanupException;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ",
          AbortableResultImpl.class.getSimpleName() + "[", "]")
          .add("alreadyClosed=" + alreadyClosed)
          .add("anyCleanupException=" + anyCleanupException)
          .toString();
    }
  }

  /**
   * Upload the current block as a single PUT request; if the buffer is empty a
   * 0-byte PUT will be invoked, as it is needed to create an entry at the far
   * end.
   * @return number of bytes uploaded.
   * @throws IOException any problem.
   */
  @Retries.RetryTranslated
  private long putObject() throws IOException {
    LOG.debug("Executing regular upload for {}", writeOperationHelper);

    final S3ADataBlocks.DataBlock block = getActiveBlock();
    final long size = block.dataSize();
    final S3ADataBlocks.BlockUploadData uploadData = block.startUpload();
    final PutObjectRequest putObjectRequest =
        writeOperationHelper.createPutObjectRequest(
            key,
            uploadData.getSize(),
            builder.putOptions);
    clearActiveBlock();

    BlockUploadProgress progressCallback =
        new BlockUploadProgress(block, progressListener, now());
    statistics.blockUploadQueued(size);
    try {
      progressCallback.progressChanged(PUT_STARTED_EVENT);
      // the putObject call automatically closes the upload data
      writeOperationHelper.putObject(putObjectRequest,
          builder.putOptions,
          uploadData,
          statistics);
      progressCallback.progressChanged(REQUEST_BYTE_TRANSFER_EVENT);
      progressCallback.progressChanged(PUT_COMPLETED_EVENT);
    } catch (InterruptedIOException ioe){
      progressCallback.progressChanged(PUT_INTERRUPTED_EVENT);
      throw ioe;
    } catch (IOException ioe){
      progressCallback.progressChanged(PUT_FAILED_EVENT);
      throw ioe;
    } finally {
      cleanupWithLogger(LOG, uploadData, block);
    }
    return size;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3ABlockOutputStream{");
    sb.append(writeOperationHelper.toString());
    sb.append(", blockSize=").append(blockSize);
    sb.append(", isMultipartUploadEnabled=").append(isMultipartUploadEnabled);
    // unsynced access; risks consistency in exchange for no risk of deadlock.
    S3ADataBlocks.DataBlock block = activeBlock;
    if (block != null) {
      sb.append(", activeBlock=").append(block);
    }
    sb.append(" Statistics=")
        .append(IOStatisticsLogging.ioStatisticsSourceToString(this));
    sb.append('}');
    return sb.toString();
  }

  private void incrementWriteOperations() {
    writeOperationHelper.incrementWriteOperations();
  }

  /**
   * Current time in milliseconds.
   * @return time
   */
  private Instant now() {
    return Instant.now();
  }

  /**
   * Get the statistics for this stream.
   * @return stream statistics
   */
  BlockOutputStreamStatistics getStatistics() {
    return statistics;
  }

  /**
   * Return the stream capabilities.
   * This stream always returns false when queried about hflush and hsync.
   * If asked about {@link CommitConstants#STREAM_CAPABILITY_MAGIC_OUTPUT}
   * it will return true iff this is an active "magic" output stream.
   * @param capability string to query the stream support for.
   * @return true if the capability is supported by this instance.
   */
  @SuppressWarnings("deprecation")
  @Override
  public boolean hasCapability(String capability) {
    final String cap = capability.toLowerCase(Locale.ENGLISH);
    switch (cap) {

      // does the output stream have delayed visibility
    case CommitConstants.STREAM_CAPABILITY_MAGIC_OUTPUT:
    case CommitConstants.STREAM_CAPABILITY_MAGIC_OUTPUT_OLD:
      return !putTracker.outputImmediatelyVisible();

      // The flush/sync options are absolutely not supported
    case StreamCapabilities.HFLUSH:
    case StreamCapabilities.HSYNC:
      return false;

      // yes, we do statistics.
    case StreamCapabilities.IOSTATISTICS:
      return true;

      // S3A supports abort.
    case StreamCapabilities.ABORTABLE_STREAM:
      return true;

      // IOStatistics context support for thread-level IOStatistics.
    case StreamCapabilities.IOSTATISTICS_CONTEXT:
      return true;

    default:
      // scan flags for the capability
      for (WriteObjectFlags flag : writeObjectFlags) {
        if (flag.hasKey(cap)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public void hflush() throws IOException {
    statistics.hflushInvoked();
    handleSyncableInvocation();
  }

  @Override
  public void hsync() throws IOException {
    statistics.hsyncInvoked();
    handleSyncableInvocation();
  }

  /**
   * Shared processing of Syncable operation reporting/downgrade.
   * @throws UnsupportedOperationException if required.
   */
  private void handleSyncableInvocation() {
    final UnsupportedOperationException ex
        = new UnsupportedOperationException(E_NOT_SYNCABLE);
    if (!downgradeSyncableExceptions) {
      throw ex;
    }
    // downgrading.
    WARN_ON_SYNCABLE.warn("Application invoked the Syncable API against"
        + " stream writing to {}. This is Unsupported",
        key);
    // and log at debug
    LOG.debug("Downgrading Syncable call", ex);
  }

  @Override
  public IOStatistics getIOStatistics() {
    return iostatistics;
  }

  /**
   * Get the IOStatistics aggregator passed in the builder.
   * @return an aggregator
   */
  protected IOStatisticsAggregator getThreadIOStatistics() {
    return threadIOStatisticsAggregator;
  }

  /**
   * Multiple partition upload.
   */
  private class MultiPartUpload {

    /**
     * ID of this upload.
     */
    private final String uploadId;

    /**
     * List of completed uploads, in order of blocks written.
     */
    private final List<Future<CompletedPart>> partETagsFutures =
        Collections.synchronizedList(new ArrayList<>());

    /** blocks which need to be closed when aborting a stream. */
    private final Map<Integer, S3ADataBlocks.DataBlock> blocksToClose =
        new ConcurrentHashMap<>();

    /**
     * Count of parts submitted, including those queued.
     */
    private int partsSubmitted;

    /**
     * Count of parts which have actually been uploaded.
     */
    private int partsUploaded;

    /**
     * Count of bytes submitted.
     */
    private long bytesSubmitted;

    /**
     * Has this upload been aborted?
     * This value is checked when each future is executed.
     * and to stop re-entrant attempts to abort an upload.
     */
    private final AtomicBoolean uploadAborted = new AtomicBoolean(false);

    /**
     * Any IOException raised during block upload.
     * if non-null, then close() MUST NOT complete
     * the file upload.
     */
    private IOException blockUploadFailure;

    /**
     * Constructor.
     * Initiates the MPU request against S3.
     * @param key upload destination
     * @throws IOException failure
     */
    @Retries.RetryTranslated
    MultiPartUpload(String key) throws IOException {
      this.uploadId = trackDuration(statistics,
          OBJECT_MULTIPART_UPLOAD_INITIATED.getSymbol(),
          () -> writeOperationHelper.initiateMultiPartUpload(
              key,
              builder.putOptions));

      LOG.debug("Initiated multi-part upload for {} with " +
          "id '{}'", writeOperationHelper, uploadId);
      progressListener.progressChanged(TRANSFER_MULTIPART_INITIATED_EVENT, 0);
    }

    /**
     * Get a count of parts submitted.
     * @return the number of parts submitted; will always be >= the
     * value of {@link #getPartsUploaded()}
     */
    public int getPartsSubmitted() {
      return partsSubmitted;
    }

    /**
     * Count of parts actually uploaded.
     * @return the count of successfully completed part uploads.
     */
    public int getPartsUploaded() {
      return partsUploaded;
    }

    /**
     * Get the upload ID; will be null after construction completes.
     * @return the upload ID
     */
    public String getUploadId() {
      return uploadId;
    }

    /**
     * Get the count of bytes submitted.
     * @return the current upload size.
     */
    public long getBytesSubmitted() {
      return bytesSubmitted;
    }

    /**
     * A block upload has failed.
     * Recorded it if there has been no previous failure.
     * @param e error
     */
    public void noteUploadFailure(final IOException e) {
      if (blockUploadFailure == null) {
        blockUploadFailure = e;
      }
    }

    /**
     * If there is a block upload failure -throw it.
     * @throws IOException if one has already been caught.
     */
    public void maybeRethrowUploadFailure() throws IOException {
      if (blockUploadFailure != null) {
        throw blockUploadFailure;
      }
    }

    /**
     * Upload a block of data.
     * This will take the block and queue it for upload.
     * There is no communication with S3 in this operation;
     * it is all done in the asynchronous threads.
     * @param block block to upload
     * @param isLast this the last block?
     * @throws IOException failure to initiate upload or a previous exception
     *                     has been raised -which is then rethrown.
     * @throws PathIOException if too many blocks were written
     */
    private void uploadBlockAsync(final S3ADataBlocks.DataBlock block,
        Boolean isLast)
        throws IOException {
      LOG.debug("Queueing upload of {} for upload {}", block, uploadId);
      Preconditions.checkNotNull(uploadId, "Null uploadId");
      // if another upload has failed, throw it rather than try to submit
      // a new upload
      maybeRethrowUploadFailure();
      partsSubmitted++;
      final long size = block.dataSize();
      bytesSubmitted += size;
      final int currentPartNumber = partETagsFutures.size() + 1;

      // this is the request which will be asynchronously uploaded
      final UploadPartRequest request;
      final S3ADataBlocks.BlockUploadData uploadData;
      final RequestBody requestBody;
      try {
        uploadData = block.startUpload();
        // get the content provider from the upload data; this allows
        // different buffering mechanisms to provide their own
        // implementations of efficient and recoverable content streams.
        requestBody = RequestBody.fromContentProvider(
            uploadData.getContentProvider(),
            uploadData.getSize(),
            CONTENT_TYPE_OCTET_STREAM);

        UploadPartRequest.Builder requestBuilder = writeOperationHelper.newUploadPartRequestBuilder(
            key,
            uploadId,
            currentPartNumber,
            isLast,
            size);
        request = requestBuilder.build();
      } catch (SdkException aws) {
        // catch and translate
        IOException e = translateException("upload", key, aws);
        // failure to start the upload.
        noteUploadFailure(e);
        throw e;
      } catch (IOException e) {
        // failure to prepare the upload.
        noteUploadFailure(e);
        throw e;
      }

      BlockUploadProgress progressCallback =
          new BlockUploadProgress(block, progressListener, now());

      statistics.blockUploadQueued(block.dataSize());

      /* BEGIN: asynchronous upload */
      ListenableFuture<CompletedPart> partETagFuture =
          executorService.submit(() -> {
            // this is the queued upload operation
            // do the upload
            try {
              LOG.debug("Uploading part {} for id '{}'",
                  currentPartNumber, uploadId);

              // update statistics
              progressCallback.progressChanged(TRANSFER_PART_STARTED_EVENT);

              if (uploadAborted.get()) {
                // upload was cancelled; record as a failure
                LOG.debug("Upload of part {} was cancelled", currentPartNumber);
                progressCallback.progressChanged(TRANSFER_PART_ABORTED_EVENT);

                // return stub entry.
                return CompletedPart.builder()
                    .eTag("")
                    .partNumber(currentPartNumber)
                    .build();
              }

              // this is potentially slow.
              // if the stream is aborted, this will be interrupted.
              UploadPartResponse response = writeOperationHelper
                  .uploadPart(request, requestBody, statistics);
              LOG.debug("Completed upload of {} to with etag {}",
                  block, response.eTag());
              partsUploaded++;
              progressCallback.progressChanged(TRANSFER_PART_SUCCESS_EVENT);

              return CompletedPart.builder()
                  .eTag(response.eTag())
                  .partNumber(currentPartNumber)
                  .checksumCRC32(response.checksumCRC32())
                  .checksumCRC32C(response.checksumCRC32C())
                  .checksumSHA1(response.checksumSHA1())
                  .checksumSHA256(response.checksumSHA256())
                  .build();
            } catch (Exception e) {
              final IOException ex = e instanceof IOException
                  ? (IOException) e
                  : new IOException(e);
              LOG.debug("Failed to upload part {}", currentPartNumber, ex);
              // save immediately.
              noteUploadFailure(ex);
              progressCallback.progressChanged(TRANSFER_PART_FAILED_EVENT);
              throw ex;
            } finally {
              progressCallback.progressChanged(TRANSFER_PART_COMPLETED_EVENT);
              // close the stream and block
              LOG.debug("closing block");
              completeUpload(currentPartNumber, block, uploadData);
            }
          });
      /* END: asynchronous upload */

      addSubmission(currentPartNumber, block, partETagFuture);
    }

    /**
     * Add a submission to the list of active uploads and the map of
     * blocks to close when interrupted.
     * @param currentPartNumber part number
     * @param block block
     * @param partETagFuture queued upload
     */
    private void addSubmission(
        final int currentPartNumber,
        final S3ADataBlocks.DataBlock block,
        final ListenableFuture<CompletedPart> partETagFuture) {
      partETagsFutures.add(partETagFuture);
      blocksToClose.put(currentPartNumber, block);
    }

    /**
     * Complete an upload.
     * <p>
     * This closes the block and upload data.
     * It removes the block from {@link #blocksToClose}.
     * @param currentPartNumber part number
     * @param block block
     * @param uploadData upload data
     */
    private void completeUpload(
        final int currentPartNumber,
        final S3ADataBlocks.DataBlock block,
        final S3ADataBlocks.BlockUploadData uploadData) {
      // this may not actually be in the map if the upload executed
      // before the relevant submission was noted
      blocksToClose.remove(currentPartNumber);
      cleanupWithLogger(LOG, uploadData);
      cleanupWithLogger(LOG, block);
    }

    /**
     * Block awaiting all outstanding uploads to complete.
     * Any interruption of this thread or a failure in an upload will
     * trigger cancellation of pending uploads and an abort of the MPU.
     * @return list of results or null if interrupted.
     * @throws CancellationException waiting for the uploads to complete was cancelled
     * @throws IOException IO Problems
     */
    private List<CompletedPart> waitForAllPartUploads()
        throws CancellationException, IOException {
      LOG.debug("Waiting for {} uploads to complete", partETagsFutures.size());
      try {
        // wait for the uploads to finish in order.
        final List<CompletedPart> completedParts = awaitAllFutures(partETagsFutures);
        for (CompletedPart part : completedParts) {
          if (StringUtils.isEmpty(part.eTag())) {
            // this was somehow cancelled/aborted
            // explicitly fail.
            throw new CancellationException("Upload of part "
                + part.partNumber() + " was aborted");
          }
        }
        return completedParts;
      } catch (CancellationException e) {
        // One or more of the futures has been cancelled.
        LOG.warn("Cancelled while waiting for uploads to {} to complete", key, e);
        throw e;
      } catch (RuntimeException | IOException ie) {
        // IO failure or low level problem.
        LOG.debug("Failure while waiting for uploads to {} to complete;"
                + " uploadAborted={}",
            key, uploadAborted.get(), ie);
        abort();
        throw ie;
      }
    }

    /**
     * Cancel all active uploads and close all blocks.
     * This waits for {@link #TIME_TO_AWAIT_CANCEL_COMPLETION}
     * for the cancellations to be processed.
     * All exceptions thrown by the futures are ignored. as is any TimeoutException.
     */
    private void cancelAllActiveUploads() {

      // interrupt futures if not already attempted

      LOG.debug("Cancelling {} futures", partETagsFutures.size());
      cancelAllFuturesAndAwaitCompletion(partETagsFutures,
          true,
          TIME_TO_AWAIT_CANCEL_COMPLETION);

      // now close all the blocks.
      LOG.debug("Closing blocks");
      blocksToClose.forEach((key1, value) ->
          cleanupWithLogger(LOG, value));
    }

    /**
     * This completes a multipart upload.
     * Sometimes it fails; here retries are handled to avoid losing all data
     * on a transient failure.
     * @param partETags list of partial uploads
     * @throws IOException on any problem which did not recover after retries.
     */
    @Retries.RetryTranslated
    private void complete(List<CompletedPart> partETags)
        throws IOException {
      maybeRethrowUploadFailure();
      AtomicInteger errorCount = new AtomicInteger(0);
      try {
        trackDurationOfInvocation(statistics,
            MULTIPART_UPLOAD_COMPLETED.getSymbol(), () -> {
              writeOperationHelper.completeMPUwithRetries(key,
                  uploadId,
                  partETags,
                  bytesSubmitted,
                  errorCount,
                  builder.putOptions);
            });
      } finally {
        statistics.exceptionInMultipartComplete(errorCount.get());
      }
    }

    /**
     * Abort a multi-part upload, after first attempting to
     * cancel active uploads via {@link #cancelAllActiveUploads()} on
     * the first invocation.
     * <p>
     * IOExceptions are caught; this is expected to be run as a cleanup process.
     * @return any caught exception.
     */
    @Retries.RetryTranslated
    private IOException abort() {
      try {
        // set the cancel flag so any newly scheduled uploads exit fast.
        if (!uploadAborted.getAndSet(true)) {
          LOG.debug("Aborting upload");
          progressListener.progressChanged(TRANSFER_MULTIPART_ABORTED_EVENT, 0);
          // an abort is double counted; the outer one also includes time to cancel
          // all pending aborts so is important to measure.
          trackDurationOfInvocation(statistics,
              OBJECT_MULTIPART_UPLOAD_ABORTED.getSymbol(), () -> {
                cancelAllActiveUploads();
                writeOperationHelper.abortMultipartUpload(key, uploadId,
                    false, null);
              });
        }
        return null;
      } catch (FileNotFoundException e) {
        // The abort has already taken place
        return e;
      } catch (IOException e) {
        // this point is only reached if abortMultipartUpload failed
        LOG.warn("Unable to abort multipart upload,"
            + " you may need to purge uploaded parts", e);
        statistics.exceptionInMultipartAbort();
        return e;
      }
    }
  }


  /**
   * The upload progress listener registered for events returned
   * during the upload of a single block.
   * It updates statistics and handles the end of the upload.
   * Transfer failures are logged at WARN.
   */
  private final class BlockUploadProgress  {

    private final S3ADataBlocks.DataBlock block;
    private final ProgressListener nextListener;
    private final Instant transferQueueTime;
    private Instant transferStartTime;
    private long size;

    /**
     * Track the progress of a single block upload.
     * @param block block to monitor
     * @param transferQueueTime time the block was transferred
     * into the queue
     */
    private BlockUploadProgress(S3ADataBlocks.DataBlock block,
        ProgressListener nextListener,
        Instant transferQueueTime) {
      this.block = block;
      this.transferQueueTime = transferQueueTime;
      this.size = block.dataSize();
      this.nextListener = nextListener;
      this.transferStartTime = now();  // will be updated when progress is made
    }

    public void progressChanged(ProgressListenerEvent eventType) {

      switch (eventType) {

      case PUT_STARTED_EVENT:
      case TRANSFER_PART_STARTED_EVENT:
        transferStartTime = now();
        statistics.blockUploadStarted(
            Duration.between(transferQueueTime, transferStartTime),
            size);
        incrementWriteOperations();
        break;

      case TRANSFER_PART_COMPLETED_EVENT:
      case PUT_COMPLETED_EVENT:
        statistics.blockUploadCompleted(
            Duration.between(transferStartTime, now()),
            size);
        statistics.bytesTransferred(size);
        break;

      case TRANSFER_PART_FAILED_EVENT:
      case PUT_FAILED_EVENT:
      case PUT_INTERRUPTED_EVENT:
        statistics.blockUploadFailed(
            Duration.between(transferStartTime, now()),
            size);
        LOG.warn("Transfer failure of block {}", block);
        break;

      default:
        // nothing
      }

      if (nextListener != null) {
        nextListener.progressChanged(eventType, size);
      }
    }
  }

  /**
   * Bridge from {@link ProgressListener} to Hadoop {@link Progressable}.
   * All progress events invoke {@link Progressable#progress()}.
   */
  private static final class ProgressableListener implements ProgressListener {
    private final Progressable progress;

    ProgressableListener(Progressable progress) {
      this.progress = progress;
    }

    @Override
    public void progressChanged(ProgressListenerEvent eventType, long bytesTransferred) {
      if (progress != null) {
        progress.progress();
      }
    }

  }

  /**
   * Create a builder.
   * @return a new builder.
   */
  public static BlockOutputStreamBuilder builder() {
    return new BlockOutputStreamBuilder();
  }

  /**
   * Builder class for constructing an output stream.
   */
  public static final class BlockOutputStreamBuilder {

    /** S3 object to work on. */
    private String key;

    /** The executor service to use to schedule work. */
    private ExecutorService executorService;

    /**
     * Report progress in order to prevent timeouts.
     * this object implements {@code ProgressListener} then it will be
     * directly wired up to the AWS client, so receive detailed progress
     * information.
     */
    private Progressable progress;

    /** The size of a single block. */
    private long blockSize;

    /** The factory for creating stream destinations. */
    private S3ADataBlocks.BlockFactory blockFactory;

    /** The output statistics for the stream. */
    private BlockOutputStreamStatistics statistics =
        EMPTY_BLOCK_OUTPUT_STREAM_STATISTICS;

    /** Operations to write data. */
    private WriteOperations writeOperations;

    /** put tracking for commit support. */
    private PutTracker putTracker;

    /** Should Syncable calls be downgraded? */
    private boolean downgradeSyncableExceptions;

    /** is Client side Encryption enabled? */
    private boolean isCSEEnabled;

    /**
     * Put object options.
     */
    private PutObjectOptions putOptions;

    /**
     * thread-level IOStatistics Aggregator.
     */
    private IOStatisticsAggregator ioStatisticsAggregator;

    /**
     * Is Multipart Uploads enabled for the given upload.
     */
    private boolean isMultipartUploadEnabled;

    private BlockOutputStreamBuilder() {
    }

    /**
     * Validate the arguments.
     */
    public void validate() {
      requireNonNull(key, "null key");
      requireNonNull(executorService, "null executorService");
      requireNonNull(blockFactory, "null blockFactory");
      requireNonNull(statistics, "null statistics");
      requireNonNull(writeOperations, "null writeOperationHelper");
      requireNonNull(putTracker, "null putTracker");
      requireNonNull(putOptions, "null putOptions");
      Preconditions.checkArgument(blockSize >= Constants.MULTIPART_MIN_SIZE,
          "Block size is too small: %s", blockSize);
      requireNonNull(ioStatisticsAggregator, "null ioStatisticsAggregator");
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withKey(
        final String value) {
      key = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withExecutorService(
        final ExecutorService value) {
      executorService = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withProgress(
        final Progressable value) {
      progress = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withBlockSize(
        final long value) {
      blockSize = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withBlockFactory(
        final S3ADataBlocks.BlockFactory value) {
      blockFactory = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withStatistics(
        final BlockOutputStreamStatistics value) {
      statistics = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withWriteOperations(
        final WriteOperationHelper value) {
      writeOperations = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withPutTracker(
        final PutTracker value) {
      putTracker = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withDowngradeSyncableExceptions(
        final boolean value) {
      downgradeSyncableExceptions = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withCSEEnabled(boolean value) {
      isCSEEnabled = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withPutOptions(
        final PutObjectOptions value) {
      putOptions = value;
      return this;
    }

    /**
     * Set builder value.
     *
     * @param value new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withIOStatisticsAggregator(
        final IOStatisticsAggregator value) {
      ioStatisticsAggregator = value;
      return this;
    }

    /**
     * Is multipart upload enabled?
     * @param value the new value
     * @return the builder
     */
    public BlockOutputStreamBuilder withMultipartEnabled(
        final boolean value) {
      isMultipartUploadEnabled = value;
      return this;
    }
  }
}
