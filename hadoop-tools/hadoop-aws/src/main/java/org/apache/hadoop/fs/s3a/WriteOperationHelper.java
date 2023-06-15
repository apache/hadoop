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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.hadoop.fs.s3a.select.SelectBinding;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.Preconditions;

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
 *   <li>Evolution to add more low-level operations, such as S3 select.</li>
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
   * @param inputStream source data.
   * @param length size, if known. Use -1 for not known
   * @param options options for the request
   * @return the request
   */
  @Retries.OnceRaw
  public PutObjectRequest createPutObjectRequest(String destKey,
      InputStream inputStream,
      long length,
      final PutObjectOptions options) {
    activateAuditSpan();
    ObjectMetadata objectMetadata = newObjectMetadata(length);
    return getRequestFactory().newPutObjectRequest(
        destKey,
        objectMetadata,
        options,
        inputStream);
  }

  /**
   * Create a {@link PutObjectRequest} request to upload a file.
   * @param dest key to PUT to.
   * @param sourceFile source file
   * @param options options for the request
   * @return the request
   */
  @Retries.OnceRaw
  public PutObjectRequest createPutObjectRequest(
      String dest,
      File sourceFile,
      final PutObjectOptions options) {
    activateAuditSpan();
    final ObjectMetadata objectMetadata =
        newObjectMetadata((int) sourceFile.length());

    PutObjectRequest putObjectRequest = getRequestFactory().
        newPutObjectRequest(dest,
            objectMetadata,
            options,
            sourceFile);
    return putObjectRequest;
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
    return getRequestFactory().newObjectMetadata(length);
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
            final InitiateMultipartUploadRequest initiateMPURequest =
                getRequestFactory().newMultipartUploadRequest(
                    destKey, options);
            return owner.initiateMultipartUpload(initiateMPURequest)
                .getUploadId();
          });
    }
  }

  /**
   * Finalize a multipart PUT operation.
   * This completes the upload, and, if that works, calls
   * {@link S3AFileSystem#finishedWrite(String, long, String, String, org.apache.hadoop.fs.s3a.impl.PutObjectOptions)}
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
  private CompleteMultipartUploadResult finalizeMultipartUpload(
      String destKey,
      String uploadId,
      List<PartETag> partETags,
      long length,
      PutObjectOptions putOptions,
      Retried retrying) throws IOException {
    if (partETags.isEmpty()) {
      throw new PathIOException(destKey,
          "No upload parts in multipart upload");
    }
    try (AuditSpan span = activateAuditSpan()) {
      CompleteMultipartUploadResult uploadResult;
      uploadResult = invoker.retry("Completing multipart upload", destKey,
          true,
          retrying,
          () -> {
            final CompleteMultipartUploadRequest request =
                getRequestFactory().newCompleteMultipartUploadRequest(
                    destKey, uploadId, partETags);
            return writeOperationHelperCallbacks.completeMultipartUpload(request);
          });
      owner.finishedWrite(destKey, length, uploadResult.getETag(),
          uploadResult.getVersionId(),
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
  public CompleteMultipartUploadResult completeMPUwithRetries(
      String destKey,
      String uploadId,
      List<PartETag> partETags,
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
   * @throws IOException on problems.
   */
  @Retries.RetryTranslated
  public void abortMultipartUpload(MultipartUpload upload)
      throws IOException {
    invoker.retry("Aborting multipart commit", upload.getKey(), true,
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
        LOG.debug("Already aborted: {}", upload.getKey(), e);
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
   * Create and initialize a part request of a multipart upload.
   * Exactly one of: {@code uploadStream} or {@code sourceFile}
   * must be specified.
   * A subset of the file may be posted, by providing the starting point
   * in {@code offset} and a length of block in {@code size} equal to
   * or less than the remaining bytes.
   * The part number must be less than 10000.
   * Retry policy is once-translated; to much effort
   * @param destKey destination key of ongoing operation
   * @param uploadId ID of ongoing upload
   * @param partNumber current part number of the upload
   * @param size amount of data
   * @param uploadStream source of data to upload
   * @param sourceFile optional source file.
   * @param offset offset in file to start reading.
   * @return the request.
   * @throws IllegalArgumentException if the parameters are invalid.
   * @throws PathIOException if the part number is out of range.
   */
  @Override
  @Retries.OnceTranslated
  public UploadPartRequest newUploadPartRequest(
      String destKey,
      String uploadId,
      int partNumber,
      long size,
      InputStream uploadStream,
      File sourceFile,
      Long offset) throws IOException {
    return once("upload part request", destKey,
        withinAuditSpan(getAuditSpan(), () ->
            getRequestFactory().newUploadPartRequest(
                destKey,
                uploadId,
                partNumber,
                size,
                uploadStream,
                sourceFile,
                offset)));
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
   * @param durationTrackerFactory factory for duration tracking
   * @return the upload initiated
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  public PutObjectResult putObject(PutObjectRequest putObjectRequest,
      PutObjectOptions putOptions,
      DurationTrackerFactory durationTrackerFactory)
      throws IOException {
    return retry("Writing Object",
        putObjectRequest.getKey(), true,
        withinAuditSpan(getAuditSpan(), () ->
            owner.putObjectDirect(putObjectRequest, putOptions, durationTrackerFactory)));
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
  public CompleteMultipartUploadResult commitUpload(
      String destKey,
      String uploadId,
      List<PartETag> partETags,
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
   * @param request request
   * @param durationTrackerFactory duration tracker factory for operation
   * @return the result of the operation.
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  public UploadPartResult uploadPart(UploadPartRequest request,
      final DurationTrackerFactory durationTrackerFactory)
      throws IOException {
    return retry("upload part #" + request.getPartNumber()
            + " upload ID " + request.getUploadId(),
        request.getKey(),
        true,
        withinAuditSpan(getAuditSpan(),
            () -> owner.uploadPart(request, durationTrackerFactory)));
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
    try (AuditSpan span = getAuditSpan()) {
      return getRequestFactory().newSelectRequest(
          storeContext.pathToKey(path));
    }
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
    // no setting of span here as the select binding is (statically) created
    // without any span.
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
        withinAuditSpan(getAuditSpan(), () -> {
          try (DurationInfo ignored =
                   new DurationInfo(LOG, "S3 Select operation")) {
            try {
              return writeOperationHelperCallbacks.selectObjectContent(request);
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
        }));
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
     * Initiates a select request.
     * @param request selectObjectContent request
     * @return selectObjectContentResult
     */
    SelectObjectContentResult selectObjectContent(SelectObjectContentRequest request);

    /**
     * Initiates a complete multi-part upload request.
     * @param request Complete multi-part upload request
     * @return completeMultipartUploadResult
     */
    CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request);

  }

}
