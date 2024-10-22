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

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

import static org.apache.hadoop.util.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Invoker.*;
import static org.apache.hadoop.fs.store.audit.AuditingFunctions.withinAuditSpan;

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
 *   <li>Integration with instrumentation.</li>
 * </ul>
 *
 * This API is for internal use only.
 * Span scoping: This helper is instantiated with span; it will be used
 * before operations which query/update S3
 *
 * History
 * <pre>
 * - A nested class in S3AFileSystem
 * - Single shared instance created and reused.
 * - [HADOOP-13786] A separate class, single instance in S3AFS
 * - [HDFS-13934] Split into interface and implementation
 * - [HADOOP-15711] Adds audit tracking; one instance per use.
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class WriteOperationHelper implements WriteOperations {
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
   * statistics context.
   */
  private final S3AStatisticsContext statisticsContext;

  /**
   * Store Context; extracted from owner.
   */
  private final StoreContext storeContext;

  /**
   * Source of Audit spans.
   */
  private final AuditSpanSource auditSpanSource;

  /**
   * Audit Span.
   */
  private AuditSpan auditSpan;

  /**
   * Factory for AWS requests.
   */
  private final RequestFactory requestFactory;

  /**
   * WriteOperationHelper callbacks.
   */
  private final WriteOperationHelperCallbacks writeOperationHelperCallbacks;

  /**
   * Constructor.
   * @param owner owner FS creating the helper
   * @param conf Configuration object
   * @param statisticsContext statistics context
   * @param auditSpanSource source of spans
   * @param auditSpan span to activate
   * @param writeOperationHelperCallbacks callbacks used by writeOperationHelper
   */
  protected WriteOperationHelper(S3AFileSystem owner,
      Configuration conf,
      S3AStatisticsContext statisticsContext,
      final AuditSpanSource auditSpanSource,
      final AuditSpan auditSpan,
      final WriteOperationHelperCallbacks writeOperationHelperCallbacks) {
    this.owner = owner;
    this.invoker = new Invoker(new S3ARetryPolicy(conf),
        this::operationRetried);
    this.conf = conf;
    this.statisticsContext = statisticsContext;
    this.storeContext = owner.createStoreContext();
    this.bucket = owner.getBucket();
    this.auditSpanSource = auditSpanSource;
    this.auditSpan = checkNotNull(auditSpan);
    this.requestFactory = owner.getRequestFactory();
    this.writeOperationHelperCallbacks = writeOperationHelperCallbacks;
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
    LOG.info("{}: Retried {}: {}", text, retries, ex.toString());
    LOG.debug("Stack", ex);
    owner.operationRetried(text, ex, retries, idempotent);
  }

  /**
   * Execute a function with retry processing.
   * Also activates the current span.
   * @param <T> type of return value
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param operation operation to execute
   * @return the result of the call
   * @throws IOException any IOE raised, or translated exception
   */
  public <T> T retry(String action,
      String path,
      boolean idempotent,
      CallableRaisingIOE<T> operation)
      throws IOException {
    activateAuditSpan();
    return invoker.retry(action, path, idempotent, operation);
  }

  /**
   * Get the audit span this object was created with.
   * @return the audit span
   */
  public AuditSpan getAuditSpan() {
    return auditSpan;
  }

  /**
   * Activate the audit span.
   * @return the span
   */
  private AuditSpan activateAuditSpan() {
    return auditSpan.activate();
  }

  /**
   * Deactivate the audit span.
   */
  private void deactivateAuditSpan() {
    auditSpan.deactivate();
  }

  /**
   * Create a {@link PutObjectRequest} request against the specific key.
   * @param destKey destination key
   * @param length size, if known. Use -1 for not known
   * @param options options for the request
   * @return the request
   */
  @Retries.OnceRaw
  public PutObjectRequest createPutObjectRequest(String destKey,
      long length,
      final PutObjectOptions options) {

    activateAuditSpan();

    return getRequestFactory()
        .newPutObjectRequestBuilder(destKey, options, length, false)
        .build();
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
   * {@inheritDoc}
   */
  @Retries.RetryTranslated
  public String initiateMultiPartUpload(
      final String destKey,
      final PutObjectOptions options)
      throws IOException {
    LOG.debug("Initiating Multipart upload to {}", destKey);
    try (AuditSpan span = activateAuditSpan()) {
      return retry("initiate MultiPartUpload", destKey, true,
          () -> {
            final CreateMultipartUploadRequest.Builder initiateMPURequestBuilder =
                getRequestFactory().newMultipartUploadRequestBuilder(
                    destKey, options);
            return owner.initiateMultipartUpload(initiateMPURequestBuilder.build())
                .uploadId();
          });
    }
  }

  /**
   * Finalize a multipart PUT operation.
   * This completes the upload, and, if that works, calls
   * {@link WriteOperationHelperCallbacks#finishedWrite(String, long, PutObjectOptions)}
   * to update the filesystem.
   * Retry policy: retrying, translated.
   * @param destKey destination of the commit
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @param length length of the upload
   * @param putOptions put object options
   * @param retrying retrying callback
   * @return the result of the operation.
   * @throws IOException on problems.
   */
  @Retries.RetryTranslated
  private CompleteMultipartUploadResponse finalizeMultipartUpload(
      String destKey,
      String uploadId,
      List<CompletedPart> partETags,
      long length,
      PutObjectOptions putOptions,
      Retried retrying) throws IOException {
    if (partETags.isEmpty()) {
      throw new PathIOException(destKey,
          "No upload parts in multipart upload");
    }
    try (AuditSpan span = activateAuditSpan()) {
      CompleteMultipartUploadResponse uploadResult;
      uploadResult = invoker.retry("Completing multipart upload", destKey,
          true,
          retrying,
          () -> {
            final CompleteMultipartUploadRequest.Builder requestBuilder =
                getRequestFactory().newCompleteMultipartUploadRequestBuilder(
                    destKey, uploadId, partETags);
            return writeOperationHelperCallbacks.completeMultipartUpload(requestBuilder.build());
          });
      writeOperationHelperCallbacks.finishedWrite(destKey, length,
          putOptions);
      return uploadResult;
    }
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
   * @param putOptions put object options
   * @return the result of the operation.
   * @throws IOException if problems arose which could not be retried, or
   * the retry count was exceeded
   */
  @Retries.RetryTranslated
  public CompleteMultipartUploadResponse completeMPUwithRetries(
      String destKey,
      String uploadId,
      List<CompletedPart> partETags,
      long length,
      AtomicInteger errorCount,
      PutObjectOptions putOptions)
      throws IOException {
    checkNotNull(uploadId);
    checkNotNull(partETags);
    LOG.debug("Completing multipart upload {} with {} parts",
        uploadId, partETags.size());
    return finalizeMultipartUpload(destKey,
        uploadId,
        partETags,
        length,
        putOptions,
        (text, e, r, i) -> errorCount.incrementAndGet());
  }

  /**
   * Abort a multipart upload operation.
   * @param destKey destination key of the upload
   * @param uploadId multipart operation Id
   * @param shouldRetry should failures trigger a retry?
   * @param retrying callback invoked on every retry
   * @throws IOException failure to abort
   * @throws FileNotFoundException if the abort ID is unknown
   */
  @Retries.RetryTranslated
  public void abortMultipartUpload(String destKey, String uploadId,
      boolean shouldRetry, Retried retrying)
      throws IOException {
    if (shouldRetry) {
      // retrying option
      invoker.retry("Aborting multipart upload ID " + uploadId,
          destKey,
          true,
          retrying,
          withinAuditSpan(getAuditSpan(), () ->
              owner.abortMultipartUpload(
                  destKey, uploadId)));
    } else {
      // single pass attempt.
      once("Aborting multipart upload ID " + uploadId,
          destKey,
          withinAuditSpan(getAuditSpan(), () ->
              owner.abortMultipartUpload(
                  destKey,
                  uploadId)));
    }
  }

  /**
   * Abort a multipart commit operation.
   * @param upload upload to abort.
   * @throws FileNotFoundException if the upload is unknown
   * @throws IOException on problems.
   */
  @Retries.RetryTranslated
  public void abortMultipartUpload(MultipartUpload upload)
      throws FileNotFoundException, IOException {
    invoker.retry("Aborting multipart commit", upload.key(), true,
        withinAuditSpan(getAuditSpan(),
            () -> owner.abortMultipartUpload(upload)));
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
    List<MultipartUpload> multipartUploads = listMultipartUploads(prefix);
    LOG.debug("Number of outstanding uploads: {}", multipartUploads.size());
    for (MultipartUpload upload: multipartUploads) {
      try {
        abortMultipartUpload(upload);
        count++;
      } catch (FileNotFoundException e) {
        LOG.debug("Already aborted: {}", upload.key(), e);
      }
    }
    return count;
  }

  @Override
  @Retries.RetryTranslated
  public List<MultipartUpload> listMultipartUploads(final String prefix)
      throws IOException {
    activateAuditSpan();
    return owner.listMultipartUploads(prefix);
  }

  /**
   * Abort a multipart commit operation.
   * @param destKey destination key of ongoing operation
   * @param uploadId multipart operation Id
   * @throws IOException on problems.
   * @throws FileNotFoundException if the abort ID is unknown
   */
  @Override
  @Retries.RetryTranslated
  public void abortMultipartCommit(String destKey, String uploadId)
      throws IOException {
    abortMultipartUpload(destKey, uploadId, true, invoker.getRetryCallback());
  }

  /**
   * Create and initialize a part request builder of a multipart upload.
   * The part number must be less than 10000.
   * Retry policy is once-translated; to much effort
   * @param destKey destination key of ongoing operation
   * @param uploadId ID of ongoing upload
   * @param partNumber current part number of the upload
   * @param isLastPart is this the last part?
   * @param size amount of data
   * @return the request builder.
   * @throws IllegalArgumentException if the parameters are invalid.
   * @throws PathIOException if the part number is out of range.
   */
  @Override
  @Retries.OnceTranslated
  public UploadPartRequest.Builder newUploadPartRequestBuilder(
      String destKey,
      String uploadId,
      int partNumber,
      boolean isLastPart,
      long size) throws IOException {
    return once("upload part request", destKey,
        withinAuditSpan(getAuditSpan(), () ->
            getRequestFactory().newUploadPartRequestBuilder(
                destKey,
                uploadId,
                partNumber,
                isLastPart,
                size)));
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
   * @param putOptions put object options
   * @param uploadData data to be uploaded
   * @param durationTrackerFactory factory for duration tracking
   * @return the upload initiated
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  public PutObjectResponse putObject(PutObjectRequest putObjectRequest,
      PutObjectOptions putOptions,
      S3ADataBlocks.BlockUploadData uploadData,
      DurationTrackerFactory durationTrackerFactory)
      throws IOException {
    return retry("Writing Object", putObjectRequest.key(), true, withinAuditSpan(getAuditSpan(),
        () -> owner.putObjectDirect(putObjectRequest, putOptions, uploadData,
            durationTrackerFactory)));
  }

  /**
   * Revert a commit by deleting the file.
   * Relies on retry code in filesystem.
   * Does not attempt to recreate the parent directory
   * @throws IOException on problems
   * @param destKey destination key
   */
  @Retries.OnceTranslated
  public void revertCommit(String destKey) throws IOException {
    once("revert commit", destKey,
        withinAuditSpan(getAuditSpan(), () -> {
          Path destPath = owner.keyToQualifiedPath(destKey);
          owner.deleteObjectAtPath(destPath,
              destKey, true);
        }));
  }

  /**
   * This completes a multipart upload to the destination key via
   * {@code finalizeMultipartUpload()}.
   * Markers are never deleted on commit; this avoids having to
   * issue many duplicate deletions.
   * Retry policy: retrying, translated.
   * Retries increment the {@code errorCount} counter.
   * @param destKey destination
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @param length length of the upload
   * @return the result of the operation.
   * @throws IOException if problems arose which could not be retried, or
   * the retry count was exceeded
   */
  @Retries.RetryTranslated
  public CompleteMultipartUploadResponse commitUpload(
      String destKey,
      String uploadId,
      List<CompletedPart> partETags,
      long length)
      throws IOException {
    checkNotNull(uploadId);
    checkNotNull(partETags);
    LOG.debug("Completing multipart upload {} with {} parts",
        uploadId, partETags.size());
    return finalizeMultipartUpload(destKey,
        uploadId,
        partETags,
        length,
        PutObjectOptions.keepingDirs(),
        Invoker.NO_OP);
  }

  /**
   * Upload part of a multi-partition file.
   * @param durationTrackerFactory duration tracker factory for operation
   * @param request the upload part request.
   * @param body the request body.
   * @return the result of the operation.
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  public UploadPartResponse uploadPart(UploadPartRequest request, RequestBody body,
      final DurationTrackerFactory durationTrackerFactory)
      throws IOException {
    return retry("upload part #" + request.partNumber()
            + " upload ID " + request.uploadId(),
        request.key(),
        true,
        withinAuditSpan(getAuditSpan(),
            () -> writeOperationHelperCallbacks.uploadPart(request,
                body,
                durationTrackerFactory)));
  }

  /**
   * Get the configuration of this instance; essentially the owning
   * filesystem configuration.
   * @return the configuration.
   */
  public Configuration getConf() {
    return conf;
  }

  @Override
  public AuditSpan createSpan(final String operation,
      @Nullable final String path1,
      @Nullable final String path2) throws IOException {
    return auditSpanSource.createSpan(operation, path1, path2);
  }

  @Override
  public void incrementWriteOperations() {
    owner.incrementWriteOperations();
  }

  /**
   * Deactivate the audit span.
    */
  @Override
  public void close() throws IOException {
    deactivateAuditSpan();
  }

  /**
   * Get the request factory which uses this store's audit span.
   * @return the request factory.
   */
  public RequestFactory getRequestFactory() {
    return requestFactory;
  }

  /***
   * Callbacks for writeOperationHelper.
   */
  public interface WriteOperationHelperCallbacks {

    /**
     * Initiates a complete multi-part upload request.
     * @param request Complete multi-part upload request
     * @return completeMultipartUploadResult
     */
    @Retries.OnceRaw
    CompleteMultipartUploadResponse completeMultipartUpload(
        CompleteMultipartUploadRequest request);

    /**
     * Upload part of a multi-partition file.
     * Increments the write and put counters.
     * <i>Important: this call does not close any input stream in the body.</i>
     * <p>
     * Retry Policy: none.
     * @param durationTrackerFactory duration tracker factory for operation
     * @param request the upload part request.
     * @param body the request body.
     * @return the result of the operation.
     * @throws AwsServiceException on problems
     * @throws UncheckedIOException failure to instantiate the s3 client
     */
    @Retries.OnceRaw
    UploadPartResponse uploadPart(
        UploadPartRequest request,
        RequestBody body,
        DurationTrackerFactory durationTrackerFactory)
        throws AwsServiceException, UncheckedIOException;

    /**
     * Perform post-write actions.
     * <p>
     * This operation MUST be called after any PUT/multipart PUT completes
     * successfully.
     * @param key key written to
     * @param length total length of file written
     * @param putOptions put object options
     */
    @Retries.RetryExceptionsSwallowed
    void finishedWrite(
        String key,
        long length,
        PutObjectOptions putOptions);
  }

}
