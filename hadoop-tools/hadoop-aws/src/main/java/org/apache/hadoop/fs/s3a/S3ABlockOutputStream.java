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

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
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
import org.apache.hadoop.fs.s3a.impl.LogExactlyOnce;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.Progressable;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext.EMPTY_BLOCK_OUTPUT_STREAM_STATISTICS;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Upload files/parts directly via different buffering mechanisms:
 * including memory and disk.
 *
 * If the stream is closed and no update has started, then the upload
 * is instead done as a single PUT operation.
 *
 * Unstable: statistics and error handling might evolve.
 *
 * Syncable is declared as supported so the calls can be
 * explicitly rejected.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3ABlockOutputStream extends OutputStream implements
    StreamCapabilities, IOStatisticsSource, Syncable, Abortable {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3ABlockOutputStream.class);

  private static final String E_NOT_SYNCABLE =
      "S3A streams are not Syncable. See HADOOP-17597.";

  /** Object being uploaded. */
  private final String key;

  /** Size of all blocks. */
  private final int blockSize;

  /** IO Statistics. */
  private final IOStatistics iostatistics;

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

  /**
   * An S3A output stream which uploads partitions in a separate pool of
   * threads; different {@link S3ADataBlocks.BlockFactory}
   * instances can control where data is buffered.
   * @throws IOException on any problem
   */
  S3ABlockOutputStream(BlockOutputStreamBuilder builder)
      throws IOException {
    builder.validate();
    this.key = builder.key;
    this.blockFactory = builder.blockFactory;
    this.blockSize = (int) builder.blockSize;
    this.statistics = builder.statistics;
    // test instantiations may not provide statistics;
    this.iostatistics = statistics.getIOStatistics();
    this.writeOperationHelper = builder.writeOperations;
    this.putTracker = builder.putTracker;
    this.executorService = MoreExecutors.listeningDecorator(
        builder.executorService);
    this.multiPartUpload = null;
    final Progressable progress = builder.progress;
    this.progressListener = (progress instanceof ProgressListener) ?
        (ProgressListener) progress
        : new ProgressableListener(progress);
    downgradeSyncableExceptions = builder.downgradeSyncableExceptions;
    // create that first block. This guarantees that an open + close sequence
    // writes a 0-byte entry.
    createBlockIfNeeded();
    LOG.debug("Initialized S3ABlockOutputStream for {}" +
        " output to {}", key, activeBlock);
    if (putTracker.initialize()) {
      LOG.debug("Put tracker requests multipart upload");
      initMultipartUpload();
    }
  }

  /**
   * Demand create a destination block.
   * @return the active block; null if there isn't one.
   * @throws IOException on any failure to create
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
   * Check for the filesystem being open.
   * @throws IOException if the filesystem is closed.
   */
  void checkOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Filesystem " + writeOperationHelper + " closed");
    }
  }

  /**
   * The flush operation does not trigger an upload; that awaits
   * the next block being full. What it does do is call {@code flush() }
   * on the current block, leaving it to choose how to react.
   * @throws IOException Any IO problem.
   */
  @Override
  public synchronized void flush() throws IOException {
    try {
      checkOpen();
    } catch (IOException e) {
      LOG.warn("Stream closed: " + e.getMessage());
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
   * @param source byte array containing
   * @param offset offset in array where to start
   * @param len number of bytes to be written
   * @throws IOException on any problem
   */
  @Override
  public synchronized void write(byte[] source, int offset, int len)
      throws IOException {

    S3ADataBlocks.validateWriteArgs(source, offset, len);
    checkOpen();
    if (len == 0) {
      return;
    }
    statistics.writeBytes(len);
    S3ADataBlocks.DataBlock block = createBlockIfNeeded();
    int written = block.write(source, offset, len);
    int remainingCapacity = block.remainingCapacity();
    if (written < len) {
      // not everything was written —the block has run out
      // of capacity
      // Trigger an upload then process the remainder.
      LOG.debug("writing more data than block has capacity -triggering upload");
      uploadCurrentBlock();
      // tail recursion is mildly expensive, but given buffer sizes must be MB.
      // it's unlikely to recurse very deeply.
      this.write(source, offset + written, len - written);
    } else {
      if (remainingCapacity == 0) {
        // the whole buffer is done, trigger an upload
        uploadCurrentBlock();
      }
    }
  }

  /**
   * Start an asynchronous upload of the current block.
   * @throws IOException Problems opening the destination for upload,
   * initializing the upload, or if a previous operation has failed.
   */
  private synchronized void uploadCurrentBlock() throws IOException {
    Preconditions.checkState(hasActiveBlock(), "No active block");
    LOG.debug("Writing block # {}", blockCount);
    initMultipartUpload();
    try {
      multiPartUpload.uploadBlockAsync(getActiveBlock());
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
  private void initMultipartUpload() throws IOException {
    if (multiPartUpload == null) {
      LOG.debug("Initiating Multipart upload");
      multiPartUpload = new MultiPartUpload(key);
    }
  }

  /**
   * Close the stream.
   *
   * This will not return until the upload is complete
   * or the attempt to perform the upload has failed.
   * Exceptions raised in this method are indicative that the write has
   * failed and data is at risk of being lost.
   * @throws IOException on any failure.
   */
  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      // already closed
      LOG.debug("Ignoring close() as stream is already closed");
      return;
    }
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
        // there's an MPU in progress';
        // IF there is more data to upload, or no data has yet been uploaded,
        // PUT the final block
        if (hasBlock &&
            (block.hasData() || multiPartUpload.getPartsSubmitted() == 0)) {
          //send last part
          uploadCurrentBlock();
        }
        // wait for the partial uploads to finish
        final List<PartETag> partETags =
            multiPartUpload.waitForAllPartUploads();
        bytes = bytesSubmitted;
        // then complete the operation
        if (putTracker.aboutToComplete(multiPartUpload.getUploadId(),
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
    LOG.debug("Statistics: {}", statistics);
    cleanupWithLogger(LOG, statistics);
    clearActiveBlock();
  }

  /**
   * Best effort abort of the multipart upload; sets
   * the field to null afterwards.
   * @return any exception caught during the operation.
   */
  private synchronized IOException maybeAbortMultipart() {
    if (multiPartUpload != null) {
      final IOException ioe = multiPartUpload.abort();
      multiPartUpload = null;
      return ioe;
    } else {
      return null;
    }
  }

  /**
   * Abort any active uploads, enter closed state.
   * @return the outcome
   */
  @Override
  public AbortableResult abort() {
    if (closed.getAndSet(true)) {
      // already closed
      LOG.debug("Ignoring abort() as stream is already closed");
      return new AbortableResultImpl(true, null);
    }
    try (DurationTracker d =
             statistics.trackDuration(INVOCATION_ABORT.getSymbol())) {
      return new AbortableResultImpl(false, maybeAbortMultipart());
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
   * Upload the current block as a single PUT request; if the buffer
   * is empty a 0-byte PUT will be invoked, as it is needed to create an
   * entry at the far end.
   * @throws IOException any problem.
   * @return number of bytes uploaded. If thread was interrupted while
   * waiting for upload to complete, returns zero with interrupted flag set
   * on this thread.
   */
  private int putObject() throws IOException {
    LOG.debug("Executing regular upload for {}", writeOperationHelper);

    final S3ADataBlocks.DataBlock block = getActiveBlock();
    int size = block.dataSize();
    final S3ADataBlocks.BlockUploadData uploadData = block.startUpload();
    final PutObjectRequest putObjectRequest = uploadData.hasFile() ?
        writeOperationHelper.createPutObjectRequest(key, uploadData.getFile())
        : writeOperationHelper.createPutObjectRequest(key,
            uploadData.getUploadStream(), size, null);
    BlockUploadProgress callback =
        new BlockUploadProgress(
            block, progressListener,  now());
    putObjectRequest.setGeneralProgressListener(callback);
    statistics.blockUploadQueued(size);
    ListenableFuture<PutObjectResult> putObjectResult =
        executorService.submit(() -> {
          try {
            // the putObject call automatically closes the input
            // stream afterwards.
            return writeOperationHelper.putObject(putObjectRequest);
          } finally {
            cleanupWithLogger(LOG, uploadData, block);
          }
        });
    clearActiveBlock();
    //wait for completion
    try {
      putObjectResult.get();
      return size;
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted object upload", ie);
      Thread.currentThread().interrupt();
      return 0;
    } catch (ExecutionException ee) {
      throw extractException("regular upload", key, ee);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3ABlockOutputStream{");
    sb.append(writeOperationHelper.toString());
    sb.append(", blockSize=").append(blockSize);
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
    switch (capability.toLowerCase(Locale.ENGLISH)) {

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

    default:
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
   */
  private void handleSyncableInvocation() {
    final UnsupportedOperationException ex
        = new UnsupportedOperationException(E_NOT_SYNCABLE);
    if (!downgradeSyncableExceptions) {
      throw ex;
    }
    // downgrading.
    WARN_ON_SYNCABLE.warn("Application invoked the Syncable API against"
        + " stream writing to {}. This is unsupported",
        key);
    // and log at debug
    LOG.debug("Downgrading Syncable call", ex);
  }

  @Override
  public IOStatistics getIOStatistics() {
    return iostatistics;
  }

  /**
   * Multiple partition upload.
   */
  private class MultiPartUpload {
    private final String uploadId;
    private final List<ListenableFuture<PartETag>> partETagsFutures;
    private int partsSubmitted;
    private int partsUploaded;
    private long bytesSubmitted;

    /**
     * Any IOException raised during block upload.
     * if non-null, then close() MUST NOT complete
     * the file upload.
     */
    private IOException blockUploadFailure;

    MultiPartUpload(String key) throws IOException {
      this.uploadId = writeOperationHelper.initiateMultiPartUpload(key);
      this.partETagsFutures = new ArrayList<>(2);
      LOG.debug("Initiated multi-part upload for {} with " +
          "id '{}'", writeOperationHelper, uploadId);
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
     * This will take the block
     * @param block block to upload
     * @throws IOException upload failure
     * @throws PathIOException if too many blocks were written
     */
    private void uploadBlockAsync(final S3ADataBlocks.DataBlock block)
        throws IOException {
      LOG.debug("Queueing upload of {} for upload {}", block, uploadId);
      Preconditions.checkNotNull(uploadId, "Null uploadId");
      maybeRethrowUploadFailure();
      partsSubmitted++;
      final int size = block.dataSize();
      bytesSubmitted += size;
      final int currentPartNumber = partETagsFutures.size() + 1;
      final UploadPartRequest request;
      final S3ADataBlocks.BlockUploadData uploadData;
      try {
        uploadData = block.startUpload();
        request = writeOperationHelper.newUploadPartRequest(
            key,
            uploadId,
            currentPartNumber,
            size,
            uploadData.getUploadStream(),
            uploadData.getFile(),
            0L);
      } catch (IOException e) {
        // failure to start the upload.
        noteUploadFailure(e);
        throw e;
      }
      BlockUploadProgress callback =
          new BlockUploadProgress(
              block, progressListener, now());
      request.setGeneralProgressListener(callback);
      statistics.blockUploadQueued(block.dataSize());
      ListenableFuture<PartETag> partETagFuture =
          executorService.submit(() -> {
            // this is the queued upload operation
            // do the upload
            try {
              LOG.debug("Uploading part {} for id '{}'",
                  currentPartNumber, uploadId);
              PartETag partETag = writeOperationHelper.uploadPart(request)
                  .getPartETag();
              LOG.debug("Completed upload of {} to part {}",
                  block, partETag.getETag());
              LOG.debug("Stream statistics of {}", statistics);
              partsUploaded++;
              return partETag;
            } catch (IOException e) {
              // save immediately.
              noteUploadFailure(e);
              throw e;
            } finally {
              // close the stream and block
              cleanupWithLogger(LOG, uploadData, block);
            }
          });
      partETagsFutures.add(partETagFuture);
    }

    /**
     * Block awaiting all outstanding uploads to complete.
     * @return list of results
     * @throws IOException IO Problems
     */
    private List<PartETag> waitForAllPartUploads() throws IOException {
      LOG.debug("Waiting for {} uploads to complete", partETagsFutures.size());
      try {
        return Futures.allAsList(partETagsFutures).get();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted partUpload", ie);
        Thread.currentThread().interrupt();
        return null;
      } catch (ExecutionException ee) {
        //there is no way of recovering so abort
        //cancel all partUploads
        LOG.debug("While waiting for upload completion", ee);
        //abort multipartupload
        this.abort();
        throw extractException("Multi-part upload with id '" + uploadId
            + "' to " + key, key, ee);
      }
    }

    /**
     * Cancel all active uploads.
     */
    private void cancelAllActiveFutures() {
      LOG.debug("Cancelling futures");
      for (ListenableFuture<PartETag> future : partETagsFutures) {
        future.cancel(true);
      }
    }

    /**
     * This completes a multipart upload.
     * Sometimes it fails; here retries are handled to avoid losing all data
     * on a transient failure.
     * @param partETags list of partial uploads
     * @throws IOException on any problem
     */
    private void complete(List<PartETag> partETags)
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
                  errorCount);
            });
      } finally {
        statistics.exceptionInMultipartComplete(errorCount.get());
      }
    }

    /**
     * Abort a multi-part upload. Retries are not attempted on failures.
     * IOExceptions are caught; this is expected to be run as a cleanup process.
     * @return any caught exception.
     */
    private IOException abort() {
      LOG.debug("Aborting upload");
      try {
        trackDurationOfInvocation(statistics,
            OBJECT_MULTIPART_UPLOAD_ABORTED.getSymbol(), () -> {
              cancelAllActiveFutures();
              writeOperationHelper.abortMultipartUpload(key, uploadId,
                  false, null);
            });
        return null;
      } catch (IOException e) {
        // this point is only reached if the operation failed more than
        // the allowed retry count
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
  private final class BlockUploadProgress implements ProgressListener {
    private final S3ADataBlocks.DataBlock block;
    private final ProgressListener nextListener;
    private final Instant transferQueueTime;
    private Instant transferStartTime;

    /**
     * Track the progress of a single block upload.
     * @param block block to monitor
     * @param nextListener optional next progress listener
     * @param transferQueueTime time the block was transferred
     * into the queue
     */
    private BlockUploadProgress(S3ADataBlocks.DataBlock block,
        ProgressListener nextListener,
        Instant transferQueueTime) {
      this.block = block;
      this.transferQueueTime = transferQueueTime;
      this.nextListener = nextListener;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      ProgressEventType eventType = progressEvent.getEventType();
      long bytesTransferred = progressEvent.getBytesTransferred();

      int size = block.dataSize();
      switch (eventType) {

      case REQUEST_BYTE_TRANSFER_EVENT:
        // bytes uploaded
        statistics.bytesTransferred(bytesTransferred);
        break;

      case TRANSFER_PART_STARTED_EVENT:
        transferStartTime = now();
        statistics.blockUploadStarted(
            Duration.between(transferQueueTime, transferStartTime),
            size);
        incrementWriteOperations();
        break;

      case TRANSFER_PART_COMPLETED_EVENT:
        statistics.blockUploadCompleted(
            Duration.between(transferStartTime, now()),
            size);
        break;

      case TRANSFER_PART_FAILED_EVENT:
        statistics.blockUploadFailed(
            Duration.between(transferStartTime, now()),
            size);
        LOG.warn("Transfer failure of block {}", block);
        break;

      default:
        // nothing
      }

      if (nextListener != null) {
        nextListener.progressChanged(progressEvent);
      }
    }
  }

  /**
   * Bridge from AWS {@code ProgressListener} to Hadoop {@link Progressable}.
   */
  private static class ProgressableListener implements ProgressListener {
    private final Progressable progress;

    ProgressableListener(Progressable progress) {
      this.progress = progress;
    }

    public void progressChanged(ProgressEvent progressEvent) {
      if (progress != null) {
        progress.progress();
      }
    }
  }

  /**
   * Create a builder.
   * @return
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
      Preconditions.checkArgument(blockSize >= Constants.MULTIPART_MIN_SIZE,
          "Block size is too small: %s", blockSize);
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
  }
}
