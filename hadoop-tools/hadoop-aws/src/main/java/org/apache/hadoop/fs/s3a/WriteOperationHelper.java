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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.select.SelectBinding;
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Invoker.*;

/**
 * Helper for low-level operations against an S3 Bucket for writing data,
 * creating and committing pending writes, and other S3-layer operations.
 * <p>
 * It hides direct access to the S3 API
 * and is a location where the object operations can be evolved/enhanced.
 * <p>
 * Features
 * <ul>
 *   <li>Methods to create and submit requests to S3, so avoiding
 *   all direct interaction with the AWS APIs.</li>
 *   <li>Some extra preflight checks of arguments, so failing fast on
 *   errors.</li>
 *   <li>Callbacks to let the FS know of events in the output stream
 *   upload process.</li>
 *   <li>Other low-level access to S3 functions, for private use.</li>
 *   <li>Failure handling, including converting exceptions to IOEs.</li>
 *   <li>Integration with instrumentation and S3Guard.</li>
 *   <li>Evolution to add more low-level operations, such as S3 select.</li>
 * </ul>
 *
 * This API is for internal use only.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class WriteOperationHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(WriteOperationHelper.class);

  /**
   * Owning filesystem.
   */
  private final S3AFileSystem owner;

  /**
   * Invoker for operations; uses the S3A retry policy and calls int
   * {@link #operationRetried(String, Exception, int, boolean)} on retries.
   */
  private final Invoker invoker;

  /** Configuration of the owner. This is a reference, not a copy. */
  private final Configuration conf;

  /** Bucket of the owner FS. */
  private final String bucket;

  /**
   * Constructor.
   * @param owner owner FS creating the helper
   * @param conf Configuration object
   *
   */
  protected WriteOperationHelper(S3AFileSystem owner, Configuration conf) {
    this.owner = owner;
    this.invoker = new Invoker(new S3ARetryPolicy(conf),
        this::operationRetried);
    this.conf = conf;
    bucket = owner.getBucket();
  }

  /**
   * Callback from {@link Invoker} when an operation is retried.
   * @param text text of the operation
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  void operationRetried(String text, Exception ex, int retries,
      boolean idempotent) {
    owner.operationRetried(text, ex, retries, idempotent);
  }

  /**
   * Execute a function with retry processing.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param operation operation to execute
   * @param <T> type of return value
   * @return the result of the call
   * @throws IOException any IOE raised, or translated exception
   */
  public <T> T retry(String action,
      String path,
      boolean idempotent,
      Invoker.Operation<T> operation)
      throws IOException {

    return invoker.retry(action, path, idempotent, operation);
  }

  /**
   * Create a {@link PutObjectRequest} request against the specific key.
   * @param destKey destination key
   * @param inputStream source data.
   * @param length size, if known. Use -1 for not known
   * @return the request
   */
  public PutObjectRequest createPutObjectRequest(String destKey,
      InputStream inputStream, long length) {
    return owner.newPutObjectRequest(destKey,
        newObjectMetadata(length),
        inputStream);
  }

  /**
   * Create a {@link PutObjectRequest} request to upload a file.
   * @param dest key to PUT to.
   * @param sourceFile source file
   * @return the request
   */
  public PutObjectRequest createPutObjectRequest(String dest,
      File sourceFile) {
    Preconditions.checkState(sourceFile.length() < Integer.MAX_VALUE,
        "File length is too big for a single PUT upload");
    return owner.newPutObjectRequest(dest,
        newObjectMetadata((int) sourceFile.length()),
        sourceFile);
  }

  /**
   * Callback on a successful write.
   * @param length length of the write
   */
  public void writeSuccessful(long length) {
  }

  /**
   * Callback on a write failure.
   * @param ex Any exception raised which triggered the failure.
   */
  public void writeFailed(Exception ex) {
    LOG.debug("Write to {} failed", this, ex);
  }

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   * @param length size, if known. Use -1 for not known
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata(long length) {
    return owner.newObjectMetadata(length);
  }

  /**
   * Start the multipart upload process.
   * Retry policy: retrying, translated.
   * @param destKey destination of upload
   * @return the upload result containing the ID
   * @throws IOException IO problem
   */
  @Retries.RetryTranslated
  public String initiateMultiPartUpload(String destKey) throws IOException {
    LOG.debug("Initiating Multipart upload to {}", destKey);
    final InitiateMultipartUploadRequest initiateMPURequest =
        new InitiateMultipartUploadRequest(bucket,
            destKey,
            newObjectMetadata(-1));
    initiateMPURequest.setCannedACL(owner.getCannedACL());
    owner.setOptionalMultipartUploadRequestParameters(initiateMPURequest);

    return retry("initiate MultiPartUpload", destKey, true,
        () -> owner.initiateMultipartUpload(initiateMPURequest).getUploadId());
  }

  /**
   * Finalize a multipart PUT operation.
   * This completes the upload, and, if that works, calls
   * {@link S3AFileSystem#finishedWrite(String, long)} to update the filesystem.
   * Retry policy: retrying, translated.
   * @param destKey destination of the commit
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @param length length of the upload
   * @param retrying retrying callback
   * @return the result of the operation.
   * @throws IOException on problems.
   */
  @Retries.RetryTranslated
  private CompleteMultipartUploadResult finalizeMultipartUpload(
      String destKey,
      String uploadId,
      List<PartETag> partETags,
      long length,
      Retried retrying) throws IOException {
    if (partETags.isEmpty()) {
      throw new IOException(
          "No upload parts in multipart upload to " + destKey);
    }
    return invoker.retry("Completing multipart commit", destKey,
        true,
        retrying,
        () -> {
          // a copy of the list is required, so that the AWS SDK doesn't
          // attempt to sort an unmodifiable list.
          CompleteMultipartUploadResult result =
              owner.getAmazonS3Client().completeMultipartUpload(
                  new CompleteMultipartUploadRequest(bucket,
                      destKey,
                      uploadId,
                      new ArrayList<>(partETags)));
          owner.finishedWrite(destKey, length);
          return result;
        }
    );
  }

  /**
   * This completes a multipart upload to the destination key via
   * {@code finalizeMultipartUpload()}.
   * Retry policy: retrying, translated.
   * Retries increment the {@code errorCount} counter.
   * @param destKey destination
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @param length length of the upload
   * @param errorCount a counter incremented by 1 on every error; for
   * use in statistics
   * @return the result of the operation.
   * @throws IOException if problems arose which could not be retried, or
   * the retry count was exceeded
   */
  @Retries.RetryTranslated
  public CompleteMultipartUploadResult completeMPUwithRetries(
      String destKey,
      String uploadId,
      List<PartETag> partETags,
      long length,
      AtomicInteger errorCount)
      throws IOException {
    checkNotNull(uploadId);
    checkNotNull(partETags);
    LOG.debug("Completing multipart upload {} with {} parts",
        uploadId, partETags.size());
    return finalizeMultipartUpload(destKey,
        uploadId,
        partETags,
        length,
        (text, e, r, i) -> errorCount.incrementAndGet());
  }

  /**
   * Abort a multipart upload operation.
   * @param destKey destination key of the upload
   * @param uploadId multipart operation Id
   * @param retrying callback invoked on every retry
   * @throws IOException failure to abort
   * @throws FileNotFoundException if the abort ID is unknown
   */
  @Retries.RetryTranslated
  public void abortMultipartUpload(String destKey, String uploadId,
      Retried retrying)
      throws IOException {
    invoker.retry("Aborting multipart upload", destKey, true,
        retrying,
        () -> owner.abortMultipartUpload(
            destKey,
            uploadId));
  }

  /**
   * Abort a multipart commit operation.
   * @param upload upload to abort.
   * @throws IOException on problems.
   */
  @Retries.RetryTranslated
  public void abortMultipartUpload(MultipartUpload upload)
      throws IOException {
    invoker.retry("Aborting multipart commit", upload.getKey(), true,
        () -> owner.abortMultipartUpload(upload));
  }


  /**
   * Abort multipart uploads under a path: limited to the first
   * few hundred.
   * @param prefix prefix for uploads to abort
   * @return a count of aborts
   * @throws IOException trouble; FileNotFoundExceptions are swallowed.
   */
  @Retries.RetryTranslated
  public int abortMultipartUploadsUnderPath(String prefix)
      throws IOException {
    LOG.debug("Aborting multipart uploads under {}", prefix);
    int count = 0;
    List<MultipartUpload> multipartUploads = owner.listMultipartUploads(prefix);
    LOG.debug("Number of outstanding uploads: {}", multipartUploads.size());
    for (MultipartUpload upload: multipartUploads) {
      try {
        abortMultipartUpload(upload);
        count++;
      } catch (FileNotFoundException e) {
        LOG.debug("Already aborted: {}", upload.getKey(), e);
      }
    }
    return count;
  }

  /**
   * Abort a multipart commit operation.
   * @param destKey destination key of ongoing operation
   * @param uploadId multipart operation Id
   * @throws IOException on problems.
   * @throws FileNotFoundException if the abort ID is unknown
   */
  @Retries.RetryTranslated
  public void abortMultipartCommit(String destKey, String uploadId)
      throws IOException {
    abortMultipartUpload(destKey, uploadId, invoker.getRetryCallback());
  }

  /**
   * Create and initialize a part request of a multipart upload.
   * Exactly one of: {@code uploadStream} or {@code sourceFile}
   * must be specified.
   * A subset of the file may be posted, by providing the starting point
   * in {@code offset} and a length of block in {@code size} equal to
   * or less than the remaining bytes.
   * @param destKey destination key of ongoing operation
   * @param uploadId ID of ongoing upload
   * @param partNumber current part number of the upload
   * @param size amount of data
   * @param uploadStream source of data to upload
   * @param sourceFile optional source file.
   * @param offset offset in file to start reading.
   * @return the request.
   */
  public UploadPartRequest newUploadPartRequest(
      String destKey,
      String uploadId,
      int partNumber,
      int size,
      InputStream uploadStream,
      File sourceFile,
      Long offset) {
    checkNotNull(uploadId);
    // exactly one source must be set; xor verifies this
    checkArgument((uploadStream != null) ^ (sourceFile != null),
        "Data source");
    checkArgument(size >= 0, "Invalid partition size %s", size);
    checkArgument(partNumber > 0 && partNumber <= 10000,
        "partNumber must be between 1 and 10000 inclusive, but is %s",
        partNumber);

    LOG.debug("Creating part upload request for {} #{} size {}",
        uploadId, partNumber, size);
    UploadPartRequest request = new UploadPartRequest()
        .withBucketName(bucket)
        .withKey(destKey)
        .withUploadId(uploadId)
        .withPartNumber(partNumber)
        .withPartSize(size);
    if (uploadStream != null) {
      // there's an upload stream. Bind to it.
      request.setInputStream(uploadStream);
    } else {
      checkArgument(sourceFile.exists(),
          "Source file does not exist: %s", sourceFile);
      checkArgument(offset >= 0, "Invalid offset %s", offset);
      long length = sourceFile.length();
      checkArgument(offset == 0 || offset < length,
          "Offset %s beyond length of file %s", offset, length);
      request.setFile(sourceFile);
      request.setFileOffset(offset);
    }
    return request;
  }

  /**
   * The toString method is intended to be used in logging/toString calls.
   * @return a string description.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "WriteOperationHelper {bucket=").append(bucket);
    sb.append('}');
    return sb.toString();
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  public PutObjectResult putObject(PutObjectRequest putObjectRequest)
      throws IOException {
    return retry("put",
        putObjectRequest.getKey(), true,
        () -> owner.putObjectDirect(putObjectRequest));
  }

  /**
   * PUT an object via the transfer manager.
   * @param putObjectRequest the request
   * @return the result of the operation
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  public UploadResult uploadObject(PutObjectRequest putObjectRequest)
      throws IOException {
    // no retry; rely on xfer manager logic
    return retry("put",
        putObjectRequest.getKey(), true,
        () -> owner.executePut(putObjectRequest, null));
  }

  /**
   * Revert a commit by deleting the file.
   * Relies on retry code in filesystem
   * @throws IOException on problems
   * @param destKey destination key
   */
  @Retries.OnceTranslated
  public void revertCommit(String destKey) throws IOException {
    once("revert commit", destKey,
        () -> {
          Path destPath = owner.keyToQualifiedPath(destKey);
          owner.deleteObjectAtPath(destPath,
              destKey, true);
          owner.maybeCreateFakeParentDirectory(destPath);
        }
    );
  }

  /**
   * Upload part of a multi-partition file.
   * @param request request
   * @return the result of the operation.
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  public UploadPartResult uploadPart(UploadPartRequest request)
      throws IOException {
    return retry("upload part",
        request.getKey(),
        true,
        () -> owner.uploadPart(request));
  }

  /**
   * Get the configuration of this instance; essentially the owning
   * filesystem configuration.
   * @return the configuration.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Create a S3 Select request for the destination path.
   * This does not build the query.
   * @param path pre-qualified path for query
   * @return the request
   */
  public SelectObjectContentRequest newSelectRequest(Path path) {
    SelectObjectContentRequest request = new SelectObjectContentRequest();
    request.setBucketName(bucket);
    request.setKey(owner.pathToKey(path));
    return request;
  }

  /**
   * Execute an S3 Select operation.
   * On a failure, the request is only logged at debug to avoid the
   * select exception being printed.
   * @param source source for selection
   * @param request Select request to issue.
   * @param action the action for use in exception creation
   * @return response
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  public SelectObjectContentResult select(
      final Path source,
      final SelectObjectContentRequest request,
      final String action)
      throws IOException {
    String bucketName = request.getBucketName();
    Preconditions.checkArgument(bucket.equals(bucketName),
        "wrong bucket: %s", bucketName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initiating select call {} {}",
          source, request.getExpression());
      LOG.debug(SelectBinding.toString(request));
    }
    return invoker.retry(
        action,
        source.toString(),
        true,
        () -> {
          try (DurationInfo ignored =
                   new DurationInfo(LOG, "S3 Select operation")) {
            try {
              return owner.getAmazonS3Client().selectObjectContent(request);
            } catch (AmazonS3Exception e) {
              LOG.error("Failure of S3 Select request against {}",
                  source);
              LOG.debug("S3 Select request against {}:\n{}",
                  source,
                  SelectBinding.toString(request),
                  e);
              throw e;
            }
          }
        });
  }
}
