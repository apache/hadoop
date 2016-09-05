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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.s3a.S3AUtils.extractException;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;
import static org.apache.hadoop.fs.s3a.Statistic.IGNORED_ERRORS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_ABORTED;

/**
 * Upload files/parts asap directly via different buffering mechanisms:
 * memory, disk.
 * <p>
 * Uploads are managed low-level rather than through the AWS TransferManager.
 * This allows for uploading each part of a multi-part upload as soon as
 * the bytes are in memory, rather than waiting until the file is closed.
 * <p>
 * Unstable: statistics and error handling might evolve
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class S3AIncrementalOutputStream extends OutputStream {

  private static final Logger LOG = S3AFileSystem.LOG;
  private final S3AFileSystem fs;
  private final String key;
  private final String bucket;
  private final int partitionSize;
  private final CannedAccessControlList cannedACL;
  private final ProgressListener progressListener;
  private final ListeningExecutorService executorService;
  private MultiPartUpload multiPartUpload;
  private boolean closed;
  private S3AOutput.StreamDestination dest;
  private int bufferLimit;
  private final RetryPolicy retryPolicy;
  private final S3AOutput.StreamDestinationFactory destFactory;
  private final byte[] singleCharWrite = new byte[1];


  /**
   * Creates a fast OutputStream that uploads to S3 from memory.
   * For MultiPartUploads, as soon as sufficient bytes have been written to
   * the stream a part is uploaded immediately (by using the low-level
   * multi-part upload API on the AmazonS3Client).
   *
   *
   * @param fs S3AFilesystem
   * @param bucket S3 bucket name
   * @param key S3 key name
   * @param progress report progress in order to prevent timeouts. If
   * this class implements {@code ProgressListener} then it will be
   * directly wired up to the AWS client, so receive detailed progress information.
   * @param cannedACL used CannedAccessControlList
   * @param partitionSize size of a single part in a multi-part upload (except
   * last part)
   * @param multiPartThreshold files at least this size use multi-part upload
   * @param threadPoolExecutor thread factory
   * @param destinationFactory factory for creating stream destinations
   * @throws IOException on any problem
   */
  public S3AIncrementalOutputStream(S3AFileSystem fs,
      String bucket,
      String key,
      Progressable progress,
      CannedAccessControlList cannedACL,
      int partitionSize,
      ExecutorService threadPoolExecutor,
      S3AOutput.StreamDestinationFactory destinationFactory)
      throws IOException {
    this.fs = fs;
    this.bucket = bucket;
    this.key = key;
    this.cannedACL = cannedACL;
    this.destFactory = destinationFactory;
    //Ensure limit as ByteArrayOutputStream size cannot exceed Integer.MAX_VALUE
    if (partitionSize > Integer.MAX_VALUE) {
      this.partitionSize = Integer.MAX_VALUE;
      LOG.warn("s3a: MULTIPART_SIZE capped to ~2.14GB (maximum allowed size " +
          "when using 'FAST_UPLOAD = true')");
    } else {
      this.partitionSize = partitionSize;
    }
    this.closed = false;
    int initialBufferSize = this.fs.getConf()
        .getInt(Constants.FAST_BUFFER_SIZE, Constants.DEFAULT_FAST_BUFFER_SIZE);
    if (initialBufferSize < 0) {
      LOG.warn("s3a: FAST_BUFFER_SIZE should be a positive number. Using " +
          "default value");
      initialBufferSize = Constants.DEFAULT_FAST_BUFFER_SIZE;
    } else if (initialBufferSize > this.bufferLimit) {
      LOG.warn("s3a: automatically adjusting FAST_BUFFER_SIZE to not " +
          "exceed MIN_MULTIPART_THRESHOLD");
      initialBufferSize = this.bufferLimit;
    }
    destFactory.init(fs);
    maybeCreateDestStream();
    this.executorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    this.multiPartUpload = null;
    this.progressListener = (progress instanceof ProgressListener) ?
        (ProgressListener) progress
        : new ProgressableListener(progress);
    this.retryPolicy = RetryPolicies.retryUpToMaximumCountWithProportionalSleep(
        5, 2000, TimeUnit.MILLISECONDS);
    LOG.debug("Initialized S3AFastOutputStream for bucket '{}' key '{}' " +
            "output to {}", bucket, key, dest);
  }

  public void maybeCreateDestStream() throws IOException {
    if (dest == null) {
      dest = destFactory.create(this.partitionSize);
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

    S3AOutput.validateWriteArgs(source, offset, len);
    checkOpen();
    if (len == 0) {
      return;
    }
    maybeCreateDestStream();
    int written = dest.write(source, offset, len);
    if (written < len) {
      // not everything was written. Trigger an upload then
      // process the remainder.
      uploadBuffer();
      this.write(source, offset + written, len - written);
    }
  }

  private synchronized void uploadBuffer() throws IOException {
    if (multiPartUpload == null) {
      multiPartUpload = initiateMultiPartUpload();
    }
    multiPartUpload.uploadPartAsync(dest.openForUpload(), partitionSize);
    dest.close();
    dest = null;

    {
       /* Upload the existing buffer if it exceeds partSize. This possibly
       requires multiple parts! */
      final byte[] allBytes = dest.toByteArray();
      dest = null; //earlier gc?
      LOG.debug("Total length of initial buffer: {}", allBytes.length);
      int processedPos = 0;
      while ((multiPartThreshold - processedPos) >= partitionSize) {
        LOG.debug("Initial buffer: processing from byte {} to byte {}",
            processedPos, (processedPos + partitionSize - 1));
        multiPartUpload.uploadPartAsync(new ByteArrayInputStream(allBytes,
            processedPos, partitionSize), partitionSize);
        processedPos += partitionSize;
      }
      //resize and reset stream
      bufferLimit = partitionSize;
      dest = new ByteArrayOutputStream(bufferLimit);
      dest.write(allBytes, processedPos, multiPartThreshold - processedPos);
    } else {
      //upload next part
      multiPartUpload.uploadPartAsync(new ByteArrayInputStream(dest
          .toByteArray()), partitionSize);
      dest.reset();
    }
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
    closed = true;
    try {
      if (multiPartUpload == null) {
        putObject();
      } else {
        int size = dest.size();
        if (size > 0) {
          //send last part
          multiPartUpload.uploadPartAsync(new ByteArrayInputStream(dest
              .toByteArray()), size);
        }
        final List<PartETag> partETags = multiPartUpload
            .waitForAllPartUploads();
        multiPartUpload.complete(partETags);
      }
      // This will delete unnecessary fake parent directories
      fs.finishedWrite(key);
      LOG.debug("Upload complete for bucket '{}' key '{}'", bucket, key);
    } finally {
      dest = null;
      super.close();
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

  private void putObject() throws IOException {
    LOG.debug("Executing regular upload for bucket '{}' key '{}'",
        bucket, key);
    final ObjectMetadata om = createDefaultMetadata();
    final int size = dest.size();
    om.setContentLength(size);
    final PutObjectRequest putObjectRequest =
        fs.newPutObjectRequest(key,
            om,
            new ByteArrayInputStream(dest.toByteArray()));
    putObjectRequest.setGeneralProgressListener(progressListener);
    ListenableFuture<PutObjectResult> putObjectResult =
        executorService.submit(new Callable<PutObjectResult>() {
          @Override
          public PutObjectResult call() throws Exception {
            return fs.putObjectDirect(putObjectRequest);
          }
        });
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

  private class MultiPartUpload {
    private final String uploadId;
    private final List<ListenableFuture<PartETag>> partETagsFutures;

    public MultiPartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.partETagsFutures = new ArrayList<ListenableFuture<PartETag>>();
      LOG.debug("Initiated multi-part upload for bucket '{}' key '{}' with " +
          "id '{}'", bucket, key, uploadId);
    }

    private void uploadPartAsync(InputStream inputStream,
        int partSize) {
      final int currentPartNumber = partETagsFutures.size() + 1;
      final UploadPartRequest request =
          new UploadPartRequest().withBucketName(bucket).withKey(key)
              .withUploadId(uploadId).withInputStream(inputStream)
              .withPartNumber(currentPartNumber).withPartSize(partSize);
      request.setGeneralProgressListener(progressListener);
      ListenableFuture<PartETag> partETagFuture =
          executorService.submit(new Callable<PartETag>() {
            @Override
            public PartETag call() throws Exception {
              LOG.debug("Uploading part {} for id '{}'", currentPartNumber,
                  uploadId);
              return fs.uploadPart(request).getPartETag();
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
     * This completes the upload.
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
          LOG.debug("Completing multi-part upload for key '{}', id '{}'",
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
      LOG.warn("Aborting multi-part upload with id '{}'", uploadId);
      fs.incrementStatistic(OBJECT_MULTIPART_UPLOAD_ABORTED);
      do {
        try {
          LOG.debug("Completing multi-part upload for key '{}', id '{}'",
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
     * the filesystem statistic {@link Statistic.IGNORED_ERRORS} incremented.
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
