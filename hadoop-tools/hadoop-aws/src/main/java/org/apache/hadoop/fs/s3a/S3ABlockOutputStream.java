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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;

/**
 * Upload files/parts directly via different buffering mechanisms:
 * including memory and disk.
 *
 * If the stream is closed and no update has started, then the upload
 * is instead done as a single PUT operation.
 *
 * Unstable: statistics and error handling might evolve.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3ABlockOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ABlockOutputStream.class);
  /** Owner fs. */
  private final S3AFileSystem fs;

  /** Object being uploaded. */
  private final String key;

  /** Bucket; used in building requests. */
  private final String bucket;

  /** Size of all blocks. */
  private final int blockSize;

  /** optional hard-coded ACL. */
  private final CannedAccessControlList cannedACL;

  /** Callback for progress. */
  private final ProgressListener progressListener;
  private final ListeningExecutorService executorService;

  /** Retry policy for multipart commits; not all AWS SDK versions retry */
  private final RetryPolicy retryPolicy =
      RetryPolicies.retryUpToMaximumCountWithProportionalSleep(
          5,
          2000,
          TimeUnit.MILLISECONDS);
  /**
   * Factory for blocks.
   */
  private final S3ADataBlocks.AbstractBlockFactory blockFactory;
  /** Preallocated byte buffer for writing single characters. */
  private final byte[] singleCharWrite = new byte[1];

  /** Multipart upload details; null means none started */
  private MultiPartUpload multiPartUpload;

  /** Closed flag. */
  private volatile boolean closed;

  /** Current data block. Null means none currently active */
  private S3ADataBlocks.DataBlock currentBlock;

  /** Count of blocks uploaded. */
  private long blockCount = 0;

  /** Statistics to build up. */
  private final S3AInstrumentation.OutputStreamStatistics statistics;

  /**
   * An S3A output stream which uploads partitions in a separate pool of
   * threads; different {@link S3ADataBlocks.AbstractBlockFactory}
   * instances can control where data is buffered.
   *
   * @param fs S3AFilesystem
   * @param key S3 object to work on.
   * @param progress report progress in order to prevent timeouts. If
   * this class implements {@code ProgressListener} then it will be
   * directly wired up to the AWS client, so receive detailed progress information.
   * @param blockSize size of a single block.
   * @param blockFactory factory for creating stream destinations
   * @param statistics stats for this stream
   * @throws IOException on any problem
   */
  S3ABlockOutputStream(S3AFileSystem fs,
      String key,
      Progressable progress,
      long blockSize,
      S3ADataBlocks.AbstractBlockFactory blockFactory,
      S3AInstrumentation.OutputStreamStatistics statistics)
      throws IOException {
    this.fs = fs;
    this.bucket = fs.getBucket();
    this.key = key;
    this.cannedACL = fs.getCannedACL();
    this.blockFactory = blockFactory;
    this.blockSize = (int) blockSize;
    this.statistics = statistics;
    Preconditions.checkArgument(blockSize >= Constants.MULTIPART_MIN_SIZE,
        "Block size is too small: %d", blockSize);
    this.executorService = MoreExecutors.listeningDecorator(
        fs.getThreadPoolExecutor());
    this.multiPartUpload = null;
    this.progressListener = (progress instanceof ProgressListener) ?
        (ProgressListener) progress
        : new ProgressableListener(progress);
    LOG.debug("Initialized S3AFastOutputStream for bucket '{}' key '{}' " +
            "output to {}", bucket, key, currentBlock);
    maybeCreateDestStream();
  }

  /**
   * Get the S3 Client to talk to.
   * @return the S3Client instance of the owner FS.
   */
  private AmazonS3 getS3Client() {
    return fs.getAmazonS3Client();
  }

  /**
   * Demand create a destination stream.
   * @throws IOException on any failure to create
   */
  private synchronized void maybeCreateDestStream() throws IOException {
    if (currentBlock == null) {
      blockCount++;
      currentBlock = blockFactory.create(this.blockSize);
    }
  }

  /**
   * Check for the filesystem being open.
   * @throws IOException if the filesystem is closed.
   */
  void checkOpen() throws IOException {
    if (closed) {
      throw new IOException("Filesystem closed");
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
    checkOpen();
    if (currentBlock != null) {
      currentBlock.flush();
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
    maybeCreateDestStream();
    int written = currentBlock.write(source, offset, len);
    if (written < len) {
      // not everything was written.
      // Trigger an upload then process the remainder.
      LOG.debug("writing more data than block has capacity");
      uploadCurrentBlock();
      // tail recursion is mildly expensive, but given buffer sizes must be MB.
      // it's unlikely to recurse very deeply.
      this.write(source, offset + written, len - written);
    } else {
      if (currentBlock.remainingCapacity() == 0) {
        // the whole buffer is done, trigger an upload
        uploadCurrentBlock();
      }
    }
  }

  /**
   * Start an asynchronous upload of the current block.
   * @throws IOException Problems opening the destination for upload
   * or initializing the upload.
   */
  private synchronized void uploadCurrentBlock() throws IOException {
    Preconditions.checkNotNull(currentBlock, "current block");
    LOG.debug("Writing block # {}", blockCount);
    if (multiPartUpload == null) {
      multiPartUpload = initiateMultiPartUpload();
    }
    multiPartUpload.uploadBlockAsync(currentBlock);
    // close the block
    currentBlock.close();
    // set it to null, so the next write will create a new block.
    currentBlock = null;
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
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    LOG.debug("{}: Closing block #{}: current block= {}, data to upload = {}",
        this,
        blockCount,
        currentBlock == null ? "(none)" : currentBlock,
        currentBlock == null ? 0 : currentBlock.dataSize() );
    try {
      if (multiPartUpload == null) {
        if (currentBlock != null){
          // no uploads of data have taken place, put the single block up.
          putObject();
        }
      } else {
        // there has already been at least one block scheduled for upload;
        // put up the current then wait
        if (currentBlock != null && currentBlock.hasData()) {
          //send last part
          multiPartUpload.uploadBlockAsync(currentBlock);
        }
        // wait for the partial uploads to finish
        final List<PartETag> partETags =
            multiPartUpload.waitForAllPartUploads();
        // then complete the operation
        multiPartUpload.complete(partETags);
      }
      // This will delete unnecessary fake parent directories
      fs.finishedWrite(key);
      LOG.debug("Upload complete for bucket '{}' key '{}'", bucket, key);
    } finally {
      LOG.debug("Closing block and factory");
      IOUtils.closeStream(currentBlock);
      IOUtils.closeStream(blockFactory);
      LOG.debug("Statistics: {}", statistics);
      IOUtils.closeStream(statistics);
      currentBlock = null;
    }
  }

  /**
   * Create the default metadata for a multipart upload operation.
   * @return the metadata to use/extend.
   */
  private ObjectMetadata createDefaultMetadata() {
    return fs.newObjectMetadata();
  }

  private MultiPartUpload initiateMultiPartUpload() throws IOException {
    LOG.debug("Initiating Multipart upload for block {}", currentBlock);
    final InitiateMultipartUploadRequest initiateMPURequest =
        new InitiateMultipartUploadRequest(bucket,
            key,
            createDefaultMetadata());
    initiateMPURequest.setCannedACL(cannedACL);
    try {
      return new MultiPartUpload(
          getS3Client().initiateMultipartUpload(initiateMPURequest).getUploadId());
    } catch (AmazonClientException ace) {
      throw translateException("initiate MultiPartUpload", key, ace);
    }
  }

  /**
   * Upload the current block as a single PUT request; if the buffer
   * is empty a 0-byte PUT will be invoked, as it is needed to create an
   * entry at the far end.
   * @throws IOException any problem.
   */
  private void putObject() throws IOException {
    LOG.debug("Executing regular upload for bucket '{}' key '{}'",
        bucket, key);

    final ObjectMetadata om = createDefaultMetadata();
    final S3ADataBlocks.DataBlock block = this.currentBlock;
    final int size = block.dataSize();
    om.setContentLength(size);
    final PutObjectRequest putObjectRequest =
        fs.newPutObjectRequest(key,
            om,
            block.openForUpload());
    long transferQueueTime = now();
    BlockUploadProgress callback =
        new BlockUploadProgress(
            block, progressListener, transferQueueTime);
    putObjectRequest.setGeneralProgressListener(callback);
    statistics.blockUploadQueued(block.dataSize());
    ListenableFuture<PutObjectResult> putObjectResult =
        executorService.submit(new Callable<PutObjectResult>() {
          @Override
          public PutObjectResult call() throws Exception {
            PutObjectResult result = fs.putObjectDirect(putObjectRequest);
            block.close();
            return result;
          }
        });
    currentBlock = null;
    //wait for completion
    try {
      putObjectResult.get();
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted object upload: {}", ie, ie);
      Thread.currentThread().interrupt();
    } catch (ExecutionException ee) {
      throw extractException("regular upload", key, ee);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3ABlockOutputStream{");
    sb.append("key='").append(key).append('\'');
    sb.append(", bucket='").append(bucket).append('\'');
    sb.append(", blockSize=").append(blockSize);
    if (currentBlock != null) {
      sb.append(", currentBlock=").append(currentBlock);
    }
    sb.append('}');
    return sb.toString();
  }

  private void incrementWriteOperations() {
    fs.incrementWriteOperations();
  }

  /**
   * Current time in nanoseconds.
   * @return time
   */
  private long now() {
    return System.currentTimeMillis();
  }

  /**
   * Multiple partition upload.
   */
  private class MultiPartUpload {
    private final String uploadId;
    private final List<ListenableFuture<PartETag>> partETagsFutures;

    public MultiPartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.partETagsFutures = new ArrayList<>(2);
      LOG.debug("Initiated multi-part upload for bucket '{}' key '{}' with " +
          "id '{}'", bucket, key, uploadId);
    }

    /**
     * Upload a block of data.
     * @param block block to upload
     * @throws IOException upload failure
     */
    private void uploadBlockAsync(final S3ADataBlocks.DataBlock block)
        throws IOException {
      LOG.debug("Queueing upload of {}", block);
      final int size = block.dataSize();
      final InputStream uploadStream = block.openForUpload();
      final int currentPartNumber = partETagsFutures.size() + 1;
      final UploadPartRequest request =
          new UploadPartRequest()
              .withBucketName(bucket)
              .withKey(key)
              .withUploadId(uploadId)
              .withInputStream(uploadStream)
              .withPartNumber(currentPartNumber)
              .withPartSize(size);
      long transferQueueTime = now();
      BlockUploadProgress callback =
          new BlockUploadProgress(
              block, progressListener, transferQueueTime);
      request.setGeneralProgressListener(callback);
      statistics.blockUploadQueued(block.dataSize());
      ListenableFuture<PartETag> partETagFuture =
          executorService.submit(new Callable<PartETag>() {
            @Override
            public PartETag call() throws Exception {
              // this is the queued upload operation
              LOG.debug("Uploading part {} for id '{}'", currentPartNumber,
                  uploadId);
              // do the upload
              PartETag partETag = fs.uploadPart(request).getPartETag();
              LOG.debug("Completed upload of {}", block);
              LOG.debug("Stream statistics of {}", statistics);

              // close the block
              block.close();
              return partETag;
            }
          });
      partETagsFutures.add(partETagFuture);
    }

    /**
     * Block awating all outstanding uploads to complete.
     * @return list of results
     * @throws IOException IO Problems
     */
    private List<PartETag> waitForAllPartUploads() throws IOException {
      LOG.debug("Waiting for {} uploads to complete", partETagsFutures.size());
      try {
        return Futures.allAsList(partETagsFutures).get();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted partUpload: {}", ie, ie);
        Thread.currentThread().interrupt();
        return null;
      } catch (ExecutionException ee) {
        //there is no way of recovering so abort
        //cancel all partUploads
        LOG.debug("While waiting for upload completion", ee);
        LOG.debug("Cancelling futures");
        for (ListenableFuture<PartETag> future : partETagsFutures) {
          future.cancel(true);
        }
        //abort multipartupload
        this.abort();
        throw extractException("Multi-part upload with id '" + uploadId + "'",
            key, ee);
      }
    }

    /**
     * This completes a multipart upload.
     * Sometimes it fails; here retries are handled to avoid losing all data
     * on a transient failure.
     * @param partETags list of partial uploads
     * @throws IOException on any problem
     */
    private CompleteMultipartUploadResult complete(List<PartETag> partETags)
        throws IOException {
      int retryCount = 0;
      AmazonClientException lastException;
      do {
        try {
          LOG.debug("Completing multi-part upload for key '{}'," +
                  " id '{}' with {} partitions " ,
                  key, uploadId, partETags.size());
          return getS3Client().completeMultipartUpload(
              new CompleteMultipartUploadRequest(bucket,
                  key,
                  uploadId,
                  partETags));
        } catch (AmazonClientException e) {
          lastException = e;
          statistics.exceptionInMultipartComplete();
        }
      } while (shouldRetry(lastException, retryCount++));
      // this point is only reached if the operation failed more than
      // the allowed retry count
      throw translateException("Completing multi-part upload", key,
          lastException);
    }

    /**
     * Abort a multi-part upload. Retries are attempted on failures.
     */
    public void abort() {
      int retryCount = 0;
      AmazonClientException lastException;
      fs.incrementStatistic(OBJECT_MULTIPART_UPLOAD_ABORTED);
      do {
        try {
          LOG.debug("Aborting multi-part upload for key '{}', id '{}'",
              key, uploadId);
          getS3Client().abortMultipartUpload(new AbortMultipartUploadRequest(bucket,
              key, uploadId));
          return;
        } catch (AmazonClientException e) {
          lastException = e;
          statistics.exceptionInMultipartAbort();
        }
      } while (shouldRetry(lastException, retryCount++));
      // this point is only reached if the operation failed more than
      // the allowed retry count
      LOG.warn("Unable to abort multipart upload, you may need to purge  " +
          "uploaded parts: {}", lastException, lastException);
    }

    /**
     * Predicate to determine whether a failed operation should
     * be attempted again.
     * If a retry is advised, the exception is automatically logged and
     * the filesystem statistic {@link Statistic#IGNORED_ERRORS} incremented.
     * @param e exception raised.
     * @param retryCount  number of retries already attempted
     * @return true if another attempt should be made
     */
    private boolean shouldRetry(AmazonClientException e, int retryCount) {
      try {
        boolean retry = retryPolicy.shouldRetry(e, retryCount, 0, true)
                == RetryPolicy.RetryAction.RETRY;
        if (retry) {
          fs.incrementStatistic(IGNORED_ERRORS);
          LOG.info("Retrying operation after exception " + e, e);
        }
        return retry;
      } catch (Exception ignored) {
        return false;
      }
    }

  }

  /**
   * The upload progress listener registered for events.
   * It updates statistics and handles the end of the upload
   */
  private class BlockUploadProgress implements ProgressListener {
    private final S3ADataBlocks.DataBlock block;
    private final ProgressListener nextListener;
    private final long transferQueueTime;
    private long transferStartTime;

    /**
     * Track the progress of a single block upload.
     * @param block block to monitor
     * @param nextListener optional next progress listener
     * @param transferQueueTime time the block was transferred
     * into the queue
     */
    private BlockUploadProgress(S3ADataBlocks.DataBlock block,
        ProgressListener nextListener,
        long transferQueueTime) {
      this.block = block;
      this.transferQueueTime = transferQueueTime;
      this.nextListener = nextListener;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      ProgressEventType eventType = progressEvent.getEventType();
      long bytesTransferred = progressEvent.getBytesTransferred();

      int blockSize = block.dataSize();
      switch (eventType) {

      case REQUEST_BYTE_TRANSFER_EVENT:
        // bytes uploaded
        statistics.bytesTransferred(bytesTransferred);
        break;

      case TRANSFER_PART_STARTED_EVENT:
        transferStartTime = now();
        statistics.blockUploadStarted(transferStartTime - transferQueueTime,
            blockSize);
        incrementWriteOperations();
        break;

      case TRANSFER_PART_COMPLETED_EVENT:
        statistics.blockUploadCompleted(now() - transferStartTime, blockSize);
        break;

      case TRANSFER_PART_FAILED_EVENT:
        statistics.blockUploadFailed(now() - transferStartTime, blockSize);
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

    public ProgressableListener(Progressable progress) {
      this.progress = progress;
    }

    public void progressChanged(ProgressEvent progressEvent) {
      if (progress != null) {
        progress.progress();
      }
    }
  }

}
