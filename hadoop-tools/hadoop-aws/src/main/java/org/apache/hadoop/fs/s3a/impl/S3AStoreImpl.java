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

package org.apache.hadoop.fs.s3a.impl;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.ProgressableProgressListener;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.UploadInfo;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.RateLimiting;
import org.apache.hadoop.util.functional.Tuples;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.extractException;
import static org.apache.hadoop.fs.s3a.S3AUtils.getPutRequestLength;
import static org.apache.hadoop.fs.s3a.S3AUtils.isThrottleException;
import static org.apache.hadoop.fs.s3a.Statistic.IGNORED_ERRORS;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_PART_PUT;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_OBJECTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_PUT_BYTES;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_PUT_BYTES_PENDING;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_PUT_REQUESTS_ACTIVE;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_PUT_REQUESTS_COMPLETED;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_RATE_LIMITED;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_RETRY;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLED;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLE_RATE;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.isObjectNotFound;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfSupplier;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Store Layer.
 * This is where lower level storage operations are intended
 * to move.
 */
public class S3AStoreImpl implements S3AStore {

  private static final Logger LOG = LoggerFactory.getLogger(S3AStoreImpl.class);

  /**
   * Progress logger; fairly noisy.
   */
  private static final Logger PROGRESS =
      LoggerFactory.getLogger(InternalConstants.UPLOAD_PROGRESS_LOG_NAME);

  /** Factory to create store contexts. */
  private final StoreContextFactory storeContextFactory;

  /** Source of the S3 clients. */
  private final ClientManager clientManager;

  /** The S3 bucket to communicate with. */
  private final String bucket;

  /** Request factory for creating requests. */
  private final RequestFactory requestFactory;

  /** Duration tracker factory. */
  private final DurationTrackerFactory durationTrackerFactory;

  /** The core instrumentation. */
  private final S3AInstrumentation instrumentation;

  /** Accessors to statistics for this FS. */
  private final S3AStatisticsContext statisticsContext;

  /** Storage Statistics Bonded to the instrumentation. */
  private final S3AStorageStatistics storageStatistics;

  /** Rate limiter for read operations. */
  private final RateLimiting readRateLimiter;

  /** Rate limiter for write operations. */
  private final RateLimiting writeRateLimiter;

  /** Store context. */
  private final StoreContext storeContext;

  /** Invoker for retry operations. */
  private final Invoker invoker;

  /** Audit span source. */
  private final AuditSpanSource<AuditSpanS3A> auditSpanSource;

  /**
   * The original file system statistics: fairly minimal but broadly
   * collected so it is important to pick up.
   * This may be null.
   */
  private final FileSystem.Statistics fsStatistics;

  /** Constructor to create S3A store. */
  S3AStoreImpl(StoreContextFactory storeContextFactory,
      ClientManager clientManager,
      DurationTrackerFactory durationTrackerFactory,
      S3AInstrumentation instrumentation,
      S3AStatisticsContext statisticsContext,
      S3AStorageStatistics storageStatistics,
      RateLimiting readRateLimiter,
      RateLimiting writeRateLimiter,
      AuditSpanSource<AuditSpanS3A> auditSpanSource,
      @Nullable FileSystem.Statistics fsStatistics) {
    this.storeContextFactory = requireNonNull(storeContextFactory);
    this.clientManager = requireNonNull(clientManager);
    this.durationTrackerFactory = requireNonNull(durationTrackerFactory);
    this.instrumentation = requireNonNull(instrumentation);
    this.statisticsContext = requireNonNull(statisticsContext);
    this.storageStatistics = requireNonNull(storageStatistics);
    this.readRateLimiter = requireNonNull(readRateLimiter);
    this.writeRateLimiter = requireNonNull(writeRateLimiter);
    this.auditSpanSource = requireNonNull(auditSpanSource);
    this.storeContext = requireNonNull(storeContextFactory.createStoreContext());
    this.fsStatistics = fsStatistics;
    this.invoker = storeContext.getInvoker();
    this.bucket = storeContext.getBucket();
    this.requestFactory = storeContext.getRequestFactory();
  }

  @Override
  public void close() {
    clientManager.close();
  }

  /** Acquire write capacity for rate limiting {@inheritDoc}. */
  @Override
  public Duration acquireWriteCapacity(final int capacity) {
    return writeRateLimiter.acquire(capacity);
  }

  /** Acquire read capacity for rate limiting {@inheritDoc}. */
  @Override
  public Duration acquireReadCapacity(final int capacity) {
    return readRateLimiter.acquire(capacity);

  }

  /**
   * Create a new store context.
   * @return a new store context.
   */
  private StoreContext createStoreContext() {
    return storeContextFactory.createStoreContext();
  }

  @Override
  public StoreContext getStoreContext() {
    return storeContext;
  }

  /**
   * Get the S3 client.
   * @return the S3 client.
   * @throws UncheckedIOException on any failure to create the client.
   */
  private S3Client getS3Client() throws UncheckedIOException {
    return clientManager.getOrCreateS3ClientUnchecked();
  }

  @Override
  public S3TransferManager getOrCreateTransferManager() throws IOException {
    return clientManager.getOrCreateTransferManager();
  }

  @Override
  public S3Client getOrCreateS3Client() throws IOException {
    return clientManager.getOrCreateS3Client();
  }

  @Override
  public S3AsyncClient getOrCreateAsyncClient() throws IOException {
    return clientManager.getOrCreateAsyncClient();
  }

  @Override
  public S3Client getOrCreateS3ClientUnchecked() throws UncheckedIOException {
    return clientManager.getOrCreateS3ClientUnchecked();
  }

  @Override
  public S3Client getOrCreateAsyncS3ClientUnchecked() throws UncheckedIOException {
    return clientManager.getOrCreateAsyncS3ClientUnchecked();
  }

  @Override
  public S3Client getOrCreateUnencryptedS3Client() throws IOException {
    return clientManager.getOrCreateUnencryptedS3Client();
  }

  @Override
  public DurationTrackerFactory getDurationTrackerFactory() {
    return durationTrackerFactory;
  }

  private S3AInstrumentation getInstrumentation() {
    return instrumentation;
  }

  @Override
  public S3AStatisticsContext getStatisticsContext() {
    return statisticsContext;
  }

  private S3AStorageStatistics getStorageStatistics() {
    return storageStatistics;
  }

  @Override
  public RequestFactory getRequestFactory() {
    return requestFactory;
  }

  /**
   * Get the client manager.
   * @return the client manager.
   */
  @Override
  public ClientManager clientManager() {
    return clientManager;
  }

  /**
   * Increment a statistic by 1.
   * This increments both the instrumentation and storage statistics.
   * @param statistic The operation to increment
   */
  protected void incrementStatistic(Statistic statistic) {
    incrementStatistic(statistic, 1);
  }

  /**
   * Increment a statistic by a specific value.
   * This increments both the instrumentation and storage statistics.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementStatistic(Statistic statistic, long count) {
    statisticsContext.incrementCounter(statistic, count);
  }

  /**
   * Decrement a gauge by a specific value.
   * @param statistic The operation to decrement
   * @param count the count to decrement
   */
  protected void decrementGauge(Statistic statistic, long count) {
    statisticsContext.decrementGauge(statistic, count);
  }

  /**
   * Increment a gauge by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementGauge(Statistic statistic, long count) {
    statisticsContext.incrementGauge(statistic, count);
  }

  /**
   * Callback when an operation was retried.
   * Increments the statistics of ignored errors or throttled requests,
   * depending up on the exception class.
   * @param ex exception.
   */
  public void operationRetried(Exception ex) {
    if (isThrottleException(ex)) {
      LOG.debug("Request throttled");
      incrementStatistic(STORE_IO_THROTTLED);
      statisticsContext.addValueToQuantiles(STORE_IO_THROTTLE_RATE, 1);
    } else {
      incrementStatistic(STORE_IO_RETRY);
      incrementStatistic(IGNORED_ERRORS);
    }
  }

  /**
   * Callback from {@link Invoker} when an operation is retried.
   * @param text text of the operation
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  public void operationRetried(String text, Exception ex, int retries, boolean idempotent) {
    operationRetried(ex);
  }

  /**
   * Get the instrumentation's IOStatistics.
   * @return statistics
   */
  @Override
  public IOStatistics getIOStatistics() {
    return instrumentation.getIOStatistics();
  }

  /**
   * Increment read operations.
   */
  @Override
  public void incrementReadOperations() {
    if (fsStatistics != null) {
      fsStatistics.incrementReadOps(1);
    }
  }

  /**
   * Increment the write operation counter.
   * This is somewhat inaccurate, as it appears to be invoked more
   * often than needed in progress callbacks.
   */
  @Override
  public void incrementWriteOperations() {
    if (fsStatistics != null) {
      fsStatistics.incrementWriteOps(1);
    }
  }


  /**
   * Increment the bytes written statistic.
   * @param bytes number of bytes written.
   */
  private void incrementBytesWritten(final long bytes) {
    if (fsStatistics != null) {
      fsStatistics.incrementBytesWritten(bytes);
    }
  }

  /**
   * At the start of a put/multipart upload operation, update the
   * relevant counters.
   *
   * @param bytes bytes in the request.
   */
  @Override
  public void incrementPutStartStatistics(long bytes) {
    LOG.debug("PUT start {} bytes", bytes);
    incrementWriteOperations();
    incrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
    if (bytes > 0) {
      incrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
  }

  /**
   * At the end of a put/multipart upload operation, update the
   * relevant counters and gauges.
   *
   * @param success did the operation succeed?
   * @param bytes bytes in the request.
   */
  @Override
  public void incrementPutCompletedStatistics(boolean success, long bytes) {
    LOG.debug("PUT completed success={}; {} bytes", success, bytes);
    if (bytes > 0) {
      incrementStatistic(OBJECT_PUT_BYTES, bytes);
      decrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
    incrementStatistic(OBJECT_PUT_REQUESTS_COMPLETED);
    decrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
  }

  /**
   * Callback for use in progress callbacks from put/multipart upload events.
   * Increments those statistics which are expected to be updated during
   * the ongoing upload operation.
   * @param key key to file that is being written (for logging)
   * @param bytes bytes successfully uploaded.
   */
  @Override
  public void incrementPutProgressStatistics(String key, long bytes) {
    PROGRESS.debug("PUT {}: {} bytes", key, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      incrementBytesWritten(bytes);
    }
  }

  /**
   * Given a possibly null duration tracker factory, return a non-null
   * one for use in tracking durations -either that or the FS tracker
   * itself.
   *
   * @param factory factory.
   * @return a non-null factory.
   */
  @Override
  public DurationTrackerFactory nonNullDurationTrackerFactory(
      DurationTrackerFactory factory) {
    return factory != null
        ? factory
        : getDurationTrackerFactory();
  }

  /**
   * Start an operation; this informs the audit service of the event
   * and then sets it as the active span.
   * @param operation operation name.
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a span for the audit
   * @throws IOException failure
   */
  public AuditSpanS3A createSpan(String operation, @Nullable String path1, @Nullable String path2)
      throws IOException {

    return auditSpanSource.createSpan(operation, path1, path2);
  }

  /**
   * Reject any request to delete an object where the key is root.
   * @param key key to validate
   * @throws IllegalArgumentException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  private void blockRootDelete(String key) throws IllegalArgumentException {
    checkArgument(!key.isEmpty() && !"/".equals(key), "Bucket %s cannot be deleted", bucket);
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  @Retries.RetryRaw
  public Map.Entry<Duration, DeleteObjectsResponse> deleteObjects(
      final DeleteObjectsRequest deleteRequest)
      throws SdkException {

    DeleteObjectsResponse response;
    BulkDeleteRetryHandler retryHandler = new BulkDeleteRetryHandler(createStoreContext());

    final List<ObjectIdentifier> keysToDelete = deleteRequest.delete().objects();
    int keyCount = keysToDelete.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initiating delete operation for {} objects", keysToDelete.size());
      keysToDelete.stream().forEach(objectIdentifier -> {
        LOG.debug(" \"{}\" {}", objectIdentifier.key(),
            objectIdentifier.versionId() != null ? objectIdentifier.versionId() : "");
      });
    }
    // block root calls
    keysToDelete.stream().map(ObjectIdentifier::key).forEach(this::blockRootDelete);

    try (DurationInfo d = new DurationInfo(LOG, false, "DELETE %d keys", keyCount)) {
      response =
          invoker.retryUntranslated("delete",
              DELETE_CONSIDERED_IDEMPOTENT, (text, e, r, i) -> {
                // handle the failure
                retryHandler.bulkDeleteRetried(deleteRequest, e);
              },
              // duration is tracked in the bulk delete counters
              trackDurationOfOperation(getDurationTrackerFactory(),
                  OBJECT_BULK_DELETE_REQUEST.getSymbol(), () -> {
                    // acquire the write capacity for the number of keys to delete
                    // and record the duration.
                    Duration durationToAcquireWriteCapacity = acquireWriteCapacity(keyCount);
                    instrumentation.recordDuration(STORE_IO_RATE_LIMITED,
                        true,
                        durationToAcquireWriteCapacity);
                    incrementStatistic(OBJECT_DELETE_OBJECTS, keyCount);
                    return getS3Client().deleteObjects(deleteRequest);
                  }));
      if (!response.errors().isEmpty()) {
        // one or more of the keys could not be deleted.
        // log and then throw
        List<S3Error> errors = response.errors();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partial failure of delete, {} errors", errors.size());
          for (S3Error error : errors) {
            LOG.debug("{}: \"{}\" - {}", error.key(), error.code(), error.message());
          }
        }
      }
      d.close();
      return Tuples.pair(d.asDuration(), response);

    } catch (IOException e) {
      // convert to unchecked.
      throw new UncheckedIOException(e);
    }
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  @Retries.RetryRaw
  public Map.Entry<Duration, Optional<DeleteObjectResponse>> deleteObject(
      final DeleteObjectRequest request)
      throws SdkException {

    String key = request.key();
    blockRootDelete(key);
    DurationInfo d = new DurationInfo(LOG, false, "deleting %s", key);
    try {
      DeleteObjectResponse response =
          invoker.retryUntranslated(String.format("Delete %s:/%s", bucket, key),
              DELETE_CONSIDERED_IDEMPOTENT,
              trackDurationOfOperation(getDurationTrackerFactory(),
                  OBJECT_DELETE_REQUEST.getSymbol(), () -> {
                    incrementStatistic(OBJECT_DELETE_OBJECTS);
                    // We try to acquire write capacity just before delete call.
                    Duration durationToAcquireWriteCapacity = acquireWriteCapacity(1);
                    instrumentation.recordDuration(STORE_IO_RATE_LIMITED,
                        true, durationToAcquireWriteCapacity);
                    return getS3Client().deleteObject(request);
                  }));
      d.close();
      return Tuples.pair(d.asDuration(), Optional.of(response));
    } catch (AwsServiceException ase) {
      // 404 errors get swallowed; this can be raised by
      // third party stores (GCS).
      if (!isObjectNotFound(ase)) {
        throw ase;
      }
      d.close();
      return Tuples.pair(d.asDuration(), Optional.empty());
    } catch (IOException e) {
      // convert to unchecked.
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters.
   * <i>Important: this call does not close any input stream in the body.</i>
   * <p>
   * Retry Policy: none.
   * @param trackerFactory duration tracker factory for operation
   * @param request the upload part request.
   * @param body the request body.
   * @return the result of the operation.
   * @throws AwsServiceException on problems
   * @throws UncheckedIOException failure to instantiate the s3 client
   */
  @Override
  @Retries.OnceRaw
  public UploadPartResponse uploadPart(
      final UploadPartRequest request,
      final RequestBody body,
      @Nullable final DurationTrackerFactory trackerFactory)
      throws AwsServiceException, UncheckedIOException {
    long len = request.contentLength();
    incrementPutStartStatistics(len);
    try {
      UploadPartResponse uploadPartResponse = trackDurationOfSupplier(
          nonNullDurationTrackerFactory(trackerFactory),
          MULTIPART_UPLOAD_PART_PUT.getSymbol(), () ->
              getS3Client().uploadPart(request, body));
      incrementPutCompletedStatistics(true, len);
      return uploadPartResponse;
    } catch (AwsServiceException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * Start a transfer-manager managed async PUT of an object,
   * incrementing the put requests and put bytes
   * counters.
   * <p>
   * It does not update the other counters,
   * as existing code does that as progress callbacks come in.
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * <p>
   * Because the operation is async, any stream supplied in the request
   * must reference data (files, buffers) which stay valid until the upload
   * completes.
   * Retry policy: N/A: the transfer manager is performing the upload.
   * Auditing: must be inside an audit span.
   * @param putObjectRequest the request
   * @param file the file to be uploaded
   * @param listener the progress listener for the request
   * @return the upload initiated
   * @throws IOException if transfer manager creation failed.
   */
  @Override
  @Retries.OnceRaw
  public UploadInfo putObject(
      PutObjectRequest putObjectRequest,
      File file,
      ProgressableProgressListener listener) throws IOException {
    long len = getPutRequestLength(putObjectRequest);
    LOG.debug("PUT {} bytes to {} via transfer manager ", len, putObjectRequest.key());
    incrementPutStartStatistics(len);

    FileUpload upload = getOrCreateTransferManager().uploadFile(
            UploadFileRequest.builder()
                .putObjectRequest(putObjectRequest)
                .source(file)
                .addTransferListener(listener)
                .build());

    return new UploadInfo(upload, len);
  }

  /**
   * Wait for an upload to complete.
   * If the upload (or its result collection) failed, this is where
   * the failure is raised as an AWS exception.
   * Calls {@link S3AStore#incrementPutCompletedStatistics(boolean, long)}
   * to update the statistics.
   * @param key destination key
   * @param uploadInfo upload to wait for
   * @return the upload result
   * @throws IOException IO failure
   * @throws CancellationException if the wait() was cancelled
   */
  @Override
  @Retries.OnceTranslated
  public CompletedFileUpload waitForUploadCompletion(String key, UploadInfo uploadInfo)
      throws IOException {
    FileUpload upload = uploadInfo.getFileUpload();
    try {
      CompletedFileUpload result = upload.completionFuture().join();
      incrementPutCompletedStatistics(true, uploadInfo.getLength());
      return result;
    } catch (CompletionException e) {
      LOG.info("Interrupted: aborting upload");
      incrementPutCompletedStatistics(false, uploadInfo.getLength());
      throw extractException("upload", key, e);
    }
  }

  /**
   * Complete a multipart upload.
   * @param request request
   * @return the response
   */
  @Override
  @Retries.OnceRaw
  public CompleteMultipartUploadResponse completeMultipartUpload(
      CompleteMultipartUploadRequest request) {
    return getS3Client().completeMultipartUpload(request);
  }

}
