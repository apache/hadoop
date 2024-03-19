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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;

import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.Statistic;
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
import static org.apache.hadoop.fs.s3a.S3AUtils.isThrottleException;
import static org.apache.hadoop.fs.s3a.Statistic.IGNORED_ERRORS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_OBJECTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_RETRY;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLED;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLE_RATE;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.isObjectNotFound;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Store Layer.
 * This is where lower level storage operations are intended
 * to move.
 */
public class S3AStoreImpl implements S3AStore {

  private static final Logger LOG = LoggerFactory.getLogger(S3AStoreImpl.class);

  private final StoreContextFactory storeContextFactory;

  private final S3Client s3Client;

  private final String bucket;

  private final RequestFactory requestFactory;

  /** Async client is used for transfer manager. */
  private S3AsyncClient s3AsyncClient;

  private final DurationTrackerFactory durationTrackerFactory;

  /** The core instrumentation. */
  private final S3AInstrumentation instrumentation;

  /** Accessors to statistics for this FS. */
  private final S3AStatisticsContext statisticsContext;

  /** Storage Statistics Bonded to the instrumentation. */
  private final S3AStorageStatistics storageStatistics;

  private final RateLimiting readRateLimiter;

  private final RateLimiting writeRateLimiter;

  private final StoreContext storeContext;

  private final Invoker invoker;

  private final AuditSpanSource<AuditSpanS3A> auditSpanSource;

  S3AStoreImpl(StoreContextFactory storeContextFactory,
      S3Client s3Client,
      DurationTrackerFactory durationTrackerFactory,
      S3AInstrumentation instrumentation,
      S3AStatisticsContext statisticsContext,
      S3AStorageStatistics storageStatistics,
      RateLimiting readRateLimiter,
      RateLimiting writeRateLimiter,
      AuditSpanSource<AuditSpanS3A> auditSpanSource) {
    this.storeContextFactory = requireNonNull(storeContextFactory);
    this.s3Client = requireNonNull(s3Client);
    this.durationTrackerFactory = requireNonNull(durationTrackerFactory);
    this.instrumentation = requireNonNull(instrumentation);
    this.statisticsContext = requireNonNull(statisticsContext);
    this.storageStatistics = requireNonNull(storageStatistics);
    this.readRateLimiter = requireNonNull(readRateLimiter);
    this.writeRateLimiter = requireNonNull(writeRateLimiter);
    this.auditSpanSource = requireNonNull(auditSpanSource);
    this.storeContext = requireNonNull(storeContextFactory.createStoreContext());
    this.invoker = storeContext.getInvoker();
    this.bucket = storeContext.getBucket();
    this.requestFactory = storeContext.getRequestFactory();
  }

  @Override
  public Duration acquireWriteCapacity(final int capacity) {
    return writeRateLimiter.acquire(capacity);
  }

  @Override
  public Duration acquireReadCapacity(final int capacity) {
    return readRateLimiter.acquire(capacity);

  }

  /**
   * Create the store context.
   * @return a new store context.
   */
  private StoreContext createStoreContext() {
    return storeContextFactory.createStoreContext();
  }

  @Override
  public StoreContext getStoreContext() {
    return storeContext;
  }

  private S3Client getS3Client() {
    return s3Client;
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
   * Perform a bulk object delete operation against S3.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics
   * <p>
   * {@code OBJECT_DELETE_OBJECTS} is updated with the actual number
   * of objects deleted in the request.
   * <p>
   * Retry policy: retry untranslated; delete considered idempotent.
   * If the request is throttled, this is logged in the throttle statistics,
   * with the counter set to the number of keys, rather than the number
   * of invocations of the delete operation.
   * This is because S3 considers each key as one mutating operation on
   * the store when updating its load counters on a specific partition
   * of an S3 bucket.
   * If only the request was measured, this operation would under-report.
   * @param deleteRequest keys to delete on the s3-backend
   * @return the AWS response
   * @throws IllegalArgumentException if the request was rejected due to
   * a mistaken attempt to delete the root directory
   * @throws SdkException amazon-layer failure.
   */
  @Override
  @Retries.RetryRaw
  public Map.Entry<Duration, DeleteObjectsResponse> deleteObjects(final DeleteObjectsRequest deleteRequest)
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
          invoker.retryUntranslated("delete", DELETE_CONSIDERED_IDEMPOTENT, (text, e, r, i) -> {
                // handle the failure
                retryHandler.bulkDeleteRetried(deleteRequest, e);
              },
              // duration is tracked in the bulk delete counters
              trackDurationOfOperation(getDurationTrackerFactory(),
                  OBJECT_BULK_DELETE_REQUEST.getSymbol(), () -> {
                    acquireWriteCapacity(keyCount);
                    incrementStatistic(OBJECT_DELETE_OBJECTS, keyCount);
                    return s3Client.deleteObjects(deleteRequest);
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
      // this is part of the retry signature, nothing else.
      // convert to unchecked.
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Delete an object.
   * Increments the {@code OBJECT_DELETE_REQUESTS} statistics.
   * <p>
   * Retry policy: retry untranslated; delete considered idempotent.
   * 404 errors other than bucket not found are swallowed;
   * this can be raised by third party stores (GCS).
   * If an exception is caught and swallowed, the response will be empty;
   * otherwise it will be the response from the delete operation.
   * @param request request to make
   * @return the total duration and response.
   * @throws SdkException problems working with S3
   * @throws IllegalArgumentException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  @Override
  @Retries.RetryRaw
  public Map.Entry<Duration, Optional<DeleteObjectResponse>> deleteObject(final DeleteObjectRequest request)
      throws SdkException {

    String key = request.key();
    blockRootDelete(key);
    DurationInfo d = new DurationInfo(LOG, false, "deleting %s", key);
    try {
      DeleteObjectResponse response =
          invoker.retryUntranslated(String.format("Delete %s:/%s", bucket, key),
              DELETE_CONSIDERED_IDEMPOTENT, trackDurationOfOperation(getDurationTrackerFactory(),
                  OBJECT_DELETE_REQUEST.getSymbol(), () -> {
                    incrementStatistic(OBJECT_DELETE_OBJECTS);
                    acquireWriteCapacity(1);
                    return s3Client.deleteObject(request);
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
      // this is part of the retry signature, nothing else.
      // convert to unchecked.
      throw new UncheckedIOException(e);
    }
  }

}
