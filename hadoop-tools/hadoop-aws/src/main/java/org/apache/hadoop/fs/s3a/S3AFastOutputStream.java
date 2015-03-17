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
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Upload files/parts asap directly from a memory buffer (instead of buffering
 * to a file).
 * <p>
 * Uploads are managed low-level rather than through the AWS TransferManager.
 * This allows for uploading each part of a multi-part upload as soon as
 * the bytes are in memory, rather than waiting until the file is closed.
 * <p>
 * Unstable: statistics and error handling might evolve
 */
@InterfaceStability.Unstable
public class S3AFastOutputStream extends OutputStream {

  private static final Logger LOG = S3AFileSystem.LOG;
  private final String key;
  private final String bucket;
  private final AmazonS3Client client;
  private final int partSize;
  private final int multiPartThreshold;
  private final S3AFileSystem fs;
  private final CannedAccessControlList cannedACL;
  private final FileSystem.Statistics statistics;
  private final String serverSideEncryptionAlgorithm;
  private final ProgressListener progressListener;
  private final ListeningExecutorService executorService;
  private MultiPartUpload multiPartUpload;
  private boolean closed;
  private ByteArrayOutputStream buffer;
  private int bufferLimit;


  /**
   * Creates a fast OutputStream that uploads to S3 from memory.
   * For MultiPartUploads, as soon as sufficient bytes have been written to
   * the stream a part is uploaded immediately (by using the low-level
   * multi-part upload API on the AmazonS3Client).
   *
   * @param client AmazonS3Client used for S3 calls
   * @param fs S3AFilesystem
   * @param bucket S3 bucket name
   * @param key S3 key name
   * @param progress report progress in order to prevent timeouts
   * @param statistics track FileSystem.Statistics on the performed operations
   * @param cannedACL used CannedAccessControlList
   * @param serverSideEncryptionAlgorithm algorithm for server side encryption
   * @param partSize size of a single part in a multi-part upload (except
   * last part)
   * @param multiPartThreshold files at least this size use multi-part upload
   * @throws IOException
   */
  public S3AFastOutputStream(AmazonS3Client client, S3AFileSystem fs,
      String bucket, String key, Progressable progress,
      FileSystem.Statistics statistics, CannedAccessControlList cannedACL,
      String serverSideEncryptionAlgorithm, long partSize,
      long multiPartThreshold, ThreadPoolExecutor threadPoolExecutor)
      throws IOException {
    this.bucket = bucket;
    this.key = key;
    this.client = client;
    this.fs = fs;
    this.cannedACL = cannedACL;
    this.statistics = statistics;
    this.serverSideEncryptionAlgorithm = serverSideEncryptionAlgorithm;
    //Ensure limit as ByteArrayOutputStream size cannot exceed Integer.MAX_VALUE
    if (partSize > Integer.MAX_VALUE) {
      this.partSize = Integer.MAX_VALUE;
      LOG.warn("s3a: MULTIPART_SIZE capped to ~2.14GB (maximum allowed size " +
          "when using 'FAST_UPLOAD = true')");
    } else {
      this.partSize = (int) partSize;
    }
    if (multiPartThreshold > Integer.MAX_VALUE) {
      this.multiPartThreshold = Integer.MAX_VALUE;
      LOG.warn("s3a: MIN_MULTIPART_THRESHOLD capped to ~2.14GB (maximum " +
          "allowed size when using 'FAST_UPLOAD = true')");
    } else {
      this.multiPartThreshold = (int) multiPartThreshold;
    }
    this.bufferLimit = this.multiPartThreshold;
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
    this.buffer = new ByteArrayOutputStream(initialBufferSize);
    this.executorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    this.multiPartUpload = null;
    this.progressListener = new ProgressableListener(progress);
    if (LOG.isDebugEnabled()){
      LOG.debug("Initialized S3AFastOutputStream for bucket '{}' key '{}'",
          bucket, key);
    }
  }

  /**
   * Writes a byte to the memory buffer. If this causes the buffer to reach
   * its limit, the actual upload is submitted to the threadpool.
   * @param b the int of which the lowest byte is written
   * @throws IOException
   */
  @Override
  public synchronized void write(int b) throws IOException {
    buffer.write(b);
    if (buffer.size() == bufferLimit) {
      uploadBuffer();
    }
  }

  /**
   * Writes a range of bytes from to the memory buffer. If this causes the
   * buffer to reach its limit, the actual upload is submitted to the
   * threadpool and the remainder of the array is written to memory
   * (recursively).
   * @param b byte array containing
   * @param off offset in array where to start
   * @param len number of bytes to be written
   * @throws IOException
   */
  @Override
  public synchronized void write(byte b[], int off, int len)
      throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }
    if (buffer.size() + len < bufferLimit) {
      buffer.write(b, off, len);
    } else {
      int firstPart = bufferLimit - buffer.size();
      buffer.write(b, off, firstPart);
      uploadBuffer();
      this.write(b, off + firstPart, len - firstPart);
    }
  }

  private synchronized void uploadBuffer() throws IOException {
    if (multiPartUpload == null) {
      multiPartUpload = initiateMultiPartUpload();
       /* Upload the existing buffer if it exceeds partSize. This possibly
       requires multiple parts! */
      final byte[] allBytes = buffer.toByteArray();
      buffer = null; //earlier gc?
      if (LOG.isDebugEnabled()) {
        LOG.debug("Total length of initial buffer: {}", allBytes.length);
      }
      int processedPos = 0;
      while ((multiPartThreshold - processedPos) >= partSize) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initial buffer: processing from byte {} to byte {}",
              processedPos, (processedPos + partSize - 1));
        }
        multiPartUpload.uploadPartAsync(new ByteArrayInputStream(allBytes,
            processedPos, partSize), partSize);
        processedPos += partSize;
      }
      //resize and reset stream
      bufferLimit = partSize;
      buffer = new ByteArrayOutputStream(bufferLimit);
      buffer.write(allBytes, processedPos, multiPartThreshold - processedPos);
    } else {
      //upload next part
      multiPartUpload.uploadPartAsync(new ByteArrayInputStream(buffer
          .toByteArray()), partSize);
      buffer.reset();
    }
  }


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
        if (buffer.size() > 0) {
          //send last part
          multiPartUpload.uploadPartAsync(new ByteArrayInputStream(buffer
              .toByteArray()), buffer.size());
        }
        final List<PartETag> partETags = multiPartUpload
            .waitForAllPartUploads();
        multiPartUpload.complete(partETags);
      }
      statistics.incrementWriteOps(1);
      // This will delete unnecessary fake parent directories
      fs.finishedWrite(key);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Upload complete for bucket '{}' key '{}'", bucket, key);
      }
    } finally {
      buffer = null;
      super.close();
    }
  }

  private ObjectMetadata createDefaultMetadata() {
    ObjectMetadata om = new ObjectMetadata();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    return om;
  }

  private MultiPartUpload initiateMultiPartUpload() throws IOException {
    final ObjectMetadata om = createDefaultMetadata();
    final InitiateMultipartUploadRequest initiateMPURequest =
        new InitiateMultipartUploadRequest(bucket, key, om);
    initiateMPURequest.setCannedACL(cannedACL);
    try {
      return new MultiPartUpload(
          client.initiateMultipartUpload(initiateMPURequest).getUploadId());
    } catch (AmazonServiceException ase) {
      throw new IOException("Unable to initiate MultiPartUpload (server side)" +
          ": " + ase, ase);
    } catch (AmazonClientException ace) {
      throw new IOException("Unable to initiate MultiPartUpload (client side)" +
          ": " + ace, ace);
    }
  }

  private void putObject() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing regular upload for bucket '{}' key '{}'", bucket,
          key);
    }
    final ObjectMetadata om = createDefaultMetadata();
    om.setContentLength(buffer.size());
    final PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        new ByteArrayInputStream(buffer.toByteArray()), om);
    putObjectRequest.setCannedAcl(cannedACL);
    putObjectRequest.setGeneralProgressListener(progressListener);
    ListenableFuture<PutObjectResult> putObjectResult =
        executorService.submit(new Callable<PutObjectResult>() {
          @Override
          public PutObjectResult call() throws Exception {
            return client.putObject(putObjectRequest);
          }
        });
    //wait for completion
    try {
      putObjectResult.get();
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted object upload:" + ie, ie);
      Thread.currentThread().interrupt();
    } catch (ExecutionException ee) {
      throw new IOException("Regular upload failed", ee.getCause());
    }
  }

  private class MultiPartUpload {
    private final String uploadId;
    private final List<ListenableFuture<PartETag>> partETagsFutures;

    public MultiPartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.partETagsFutures = new ArrayList<ListenableFuture<PartETag>>();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initiated multi-part upload for bucket '{}' key '{}' with " +
            "id '{}'", bucket, key, uploadId);
      }
    }

    public void uploadPartAsync(ByteArrayInputStream inputStream,
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
              if (LOG.isDebugEnabled()) {
                LOG.debug("Uploading part {} for id '{}'", currentPartNumber,
                    uploadId);
              }
              return client.uploadPart(request).getPartETag();
            }
          });
      partETagsFutures.add(partETagFuture);
    }

    public List<PartETag> waitForAllPartUploads() throws IOException {
      try {
        return Futures.allAsList(partETagsFutures).get();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted partUpload:" + ie, ie);
        Thread.currentThread().interrupt();
      } catch (ExecutionException ee) {
        //there is no way of recovering so abort
        //cancel all partUploads
        for (ListenableFuture<PartETag> future : partETagsFutures) {
          future.cancel(true);
        }
        //abort multipartupload
        this.abort();
        throw new IOException("Part upload failed in multi-part upload with " +
            "id '" +uploadId + "':" + ee, ee);
      }
      //should not happen?
      return null;
    }

    public void complete(List<PartETag> partETags) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completing multi-part upload for key '{}', id '{}'", key,
            uploadId);
      }
      final CompleteMultipartUploadRequest completeRequest =
          new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags);
      client.completeMultipartUpload(completeRequest);

    }

    public void abort() {
      LOG.warn("Aborting multi-part upload with id '{}'", uploadId);
      try {
        client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket,
            key, uploadId));
      } catch (Exception e2) {
        LOG.warn("Unable to abort multipart upload, you may need to purge  " +
            "uploaded parts: " + e2, e2);
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
