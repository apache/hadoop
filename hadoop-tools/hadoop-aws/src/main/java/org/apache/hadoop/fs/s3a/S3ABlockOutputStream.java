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
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
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

import static org.apache.hadoop.fs.s3a.S3AUtils.extractException;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;
import static org.apache.hadoop.fs.s3a.Statistic.IGNORED_ERRORS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_ABORTED;

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
  private final S3AFileSystem fs;
  private final String key;
  private final String bucket;
  private final int blockSize;
  private final CannedAccessControlList cannedACL;
  private final ProgressListener progressListener;
  private final ListeningExecutorService executorService;
  private final RetryPolicy retryPolicy;
  private final S3ADataBlocks.AbstractBlockFactory blockFactory;
  private final byte[] singleCharWrite = new byte[1];
  private MultiPartUpload multiPartUpload;
  private volatile boolean closed;
  private S3ADataBlocks.DataBlock currentBlock;

  /**
   * S3A output stream which uploads blocks as soon as there is enough
   * data.
   *
   * @param fs S3AFilesystem
   * @param key S3 key name
   * @param progress report progress in order to prevent timeouts. If
   * this class implements {@code ProgressListener} then it will be
   * directly wired up to the AWS client, so receive detailed progress information.
   * @param blockSize size of a single block.
   * @param blockFactory factory for creating stream destinations
   * @throws IOException on any problem
   */
  public S3ABlockOutputStream(S3AFileSystem fs,
      String key,
      Progressable progress,
      long blockSize,
      S3ADataBlocks.AbstractBlockFactory blockFactory)
      throws IOException {
    this.fs = fs;
    this.bucket = fs.getBucket();
    this.key = key;
    this.cannedACL = fs.getCannedACL();
    this.blockFactory = blockFactory;
    this.blockSize = (int) blockSize;
    Preconditions.checkArgument(blockSize >= Constants.MULTIPART_MIN_SIZE,
        "Block size is too small: %d", blockSize);
    this.executorService = MoreExecutors.listeningDecorator(
        fs.getThreadPoolExecutor());
    this.multiPartUpload = null;
    this.progressListener = (progress instanceof ProgressListener) ?
        (ProgressListener) progress
        : new ProgressableListener(progress);
    this.retryPolicy = RetryPolicies.retryUpToMaximumCountWithProportionalSleep(
        5, 2000, TimeUnit.MILLISECONDS);
    LOG.debug("Initialized S3AFastOutputStream for bucket '{}' key '{}' " +
            "output to {}", bucket, key, currentBlock);
    maybeCreateDestStream();
  }

  public void maybeCreateDestStream() throws IOException {
    if (currentBlock == null) {
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
        LOG.debug("Current block is full");
        uploadCurrentBlock();
      }
    }
  }

  /**
   * Trigger the upload.
   * @throws IOException Problems opening the destination for upload
   * or initializing the upload.
   */
  private synchronized void uploadCurrentBlock() throws IOException {
    if (multiPartUpload == null) {
      multiPartUpload = initiateMultiPartUpload();
    }
    multiPartUpload.uploadBlockAsync(currentBlock);
    currentBlock.close();
    currentBlock = null;
  }

  /**
   * Close the stream. This will not return until the upload is complete
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
    LOG.debug("Closing {}, data to upload = {}", this,
      currentBlock == null ? 0 : currentBlock.dataSize() );
    closed = true;
    if (currentBlock == null) {
      return;
    }
    try {
      if (multiPartUpload == null) {
        // no uploads of data have taken place, put the single block up.
        putObject();
      } else {
        // there has already been at least one block scheduled for upload;
        // put up the current then wait
        if (currentBlock.hasData()) {
          //send last part
          multiPartUpload.uploadBlockAsync(currentBlock);
        }
        final List<PartETag> partETags = multiPartUpload
            .waitForAllPartUploads();
        multiPartUpload.complete(partETags);
      }
      // This will delete unnecessary fake parent directories
      fs.finishedWrite(key);
      LOG.debug("Upload complete for bucket '{}' key '{}'", bucket, key);
    } finally {
      LOG.debug("Closing block and factory");
      IOUtils.closeStream(currentBlock);
      IOUtils.closeStream(blockFactory);
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
          getClient().initiateMultipartUpload(initiateMPURequest).getUploadId());
    } catch (AmazonClientException ace) {
      throw translateException("initiate MultiPartUpload", key, ace);
    }
  }

  /**
   * Upload the current block as a single PUT request.
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
    putObjectRequest.setGeneralProgressListener(progressListener);
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

  private AmazonS3Client getClient() {
    return fs.getAmazonS3Client();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3ABlockOutputStream{");
    sb.append("key='").append(key).append('\'');
    sb.append(", bucket='").append(bucket).append('\'');
    sb.append(", blockSize=").append(blockSize);
    sb.append(", dest=").append(currentBlock);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Multiple partition upload.
   */
  private class MultiPartUpload {
    private final String uploadId;
    private final List<ListenableFuture<PartETag>> partETagsFutures;

    public MultiPartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.partETagsFutures = new ArrayList<>();
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
      final InputStream inputStream = block.openForUpload();
      final int currentPartNumber = partETagsFutures.size() + 1;
      final UploadPartRequest request =
          new UploadPartRequest().withBucketName(bucket).withKey(key)
              .withUploadId(uploadId).withInputStream(inputStream)
              .withPartNumber(currentPartNumber).withPartSize(size);
      request.setGeneralProgressListener(progressListener);
      ListenableFuture<PartETag> partETagFuture =
          executorService.submit(new Callable<PartETag>() {
            @Override
            public PartETag call() throws Exception {
              // this is the queued upload operation
              LOG.debug("Uploading part {} for id '{}'", currentPartNumber,
                  uploadId);
              // do the upload
              PartETag partETag = fs.uploadPart(request).getPartETag();
              // close the block, triggering its cleanup
              block.close();
              return partETag;
            }
          });
      partETagsFutures.add(partETagFuture);
    }

    private List<PartETag> waitForAllPartUploads() throws IOException {
      try {
        return Futures.allAsList(partETagsFutures).get();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted partUpload: {}", ie, ie);
        Thread.currentThread().interrupt();
        return null;
      } catch (ExecutionException ee) {
        //there is no way of recovering so abort
        //cancel all partUploads
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
          LOG.debug("Completing multi-part upload for key '{}', id '{}'" ,
                  key, uploadId);
          return getClient().completeMultipartUpload(
              new CompleteMultipartUploadRequest(bucket,
                  key,
                  uploadId,
                  partETags));
        } catch (AmazonClientException e) {
          lastException = e;
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
          getClient().abortMultipartUpload(new AbortMultipartUploadRequest(bucket,
              key, uploadId));
          return;
        } catch (AmazonClientException e) {
          lastException = e;
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
