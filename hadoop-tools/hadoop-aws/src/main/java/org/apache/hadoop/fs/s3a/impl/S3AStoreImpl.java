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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.s3a.impl.streams.FactoryBindingParameters;
import org.apache.hadoop.fs.s3a.impl.streams.InputStreamType;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStream;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamFactory;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectReadParameters;
import org.apache.hadoop.fs.s3a.impl.streams.StreamFactoryRequirements;
import org.apache.hadoop.fs.s3a.impl.write.StoreWriterService;
import org.apache.hadoop.fs.s3a.impl.write.WriteOperationHelperCallbacks;
import org.apache.hadoop.fs.s3a.impl.write.WriteOperationHelperCallbacksImpl;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.RateLimiting;
import org.apache.hadoop.util.functional.Tuples;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR;
import static org.apache.hadoop.fs.s3a.Constants.HADOOP_TMP_DIR;
import static org.apache.hadoop.fs.s3a.S3AUtils.isThrottleException;
import static org.apache.hadoop.fs.s3a.Statistic.ACTION_HTTP_HEAD_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.IGNORED_ERRORS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_OBJECTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_METADATA_REQUESTS;
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
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.OBJECT_DELETE_WRITE_CAPACITY;
import static org.apache.hadoop.fs.s3a.impl.write.StoreWriter.STORE_WRITER;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration.factoryFromConfig;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.Preconditions.checkState;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * Store Layer.
 * This is where lower level storage operations are intended
 * to move.
 */
public class S3AStoreImpl
    extends CompositeService
    implements S3AStore, ObjectInputStreamFactory {

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

  /**
   * Allocator of local FS storage.
   */
  private LocalDirAllocator directoryAllocator;

  /**
   * Factory for input streams.
   */
  private ObjectInputStreamFactory objectInputStreamFactory;

  /**
   * store writer Service.
   */
  private final StoreWriterService storeWriter;

  /**
   * Map of services, used for easy lookup and so allow service retrieval to
   * be a stable part of the of S3AStore API.
   */
  private final Map<String, Service> serviceMap = new HashMap<>();

  /**
   * Constructor to create S3A store.
   * Package private, as {@link S3AStoreBuilder} creates them.
   * */
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
    super("S3AStore");
    this.auditSpanSource = requireNonNull(auditSpanSource);
    this.clientManager = requireNonNull(clientManager);
    this.durationTrackerFactory = requireNonNull(durationTrackerFactory);
    this.fsStatistics = fsStatistics;
    this.instrumentation = requireNonNull(instrumentation);
    this.statisticsContext = requireNonNull(statisticsContext);
    this.storeContextFactory = requireNonNull(storeContextFactory);
    this.storageStatistics = requireNonNull(storageStatistics);
    this.readRateLimiter = requireNonNull(readRateLimiter);
    this.writeRateLimiter = requireNonNull(writeRateLimiter);
    this.storeContext = requireNonNull(storeContextFactory.createStoreContext());

    this.invoker = requireNonNull(storeContext.getInvoker());
    this.bucket = requireNonNull(storeContext.getBucket());
    this.requestFactory = requireNonNull(storeContext.getRequestFactory());
    registerChildService(CLIENT_MANAGER, clientManager);
    this.storeWriter = registerChildService(STORE_WRITER, new StoreWriterService());
  }

  /**
   * Add and register a service, using an ID which may be used to retrieve it later.
   * @param id ID of the service
   * @param service service instance.
   * @return the service
   * @param <T> type of the service.
   */
  public synchronized <T extends Service> T registerChildService(final String id, final T service) {
    serviceMap.put(id, service);
    super.addService(service);
    return service;
  }

  /**
   * Add a service using its name as the service ID.
   * This is not a robust way to do this as any extension point with multiple services
   * (e.g object stream factory) will not be resolvable by a well known name.
   * @param service the {@link Service} to be added
   */
  @Override
  protected void addService(final Service service) {
    registerChildService(service.getName(), service);
  }

  @Override
  @VisibleForTesting
  public synchronized boolean removeService(final Service service) {
    // cut from service map.
    for (Iterator<Map.Entry<String, Service>> it = serviceMap.entrySet().iterator();
        it.hasNext(); ) {
      Map.Entry<String, Service> entry = it.next();
      if (entry.getValue() == service) {
        it.remove();
      }
    }
    return super.removeService(service);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <S extends Service> S lookupService(String id, Class<S> serviceClass) {
    final Service service = serviceMap.get(id);
    checkState(service != null, "No service found for ID " + id);
    checkState(serviceClass.isAssignableFrom(service.getClass()),
        "Service ID %s is of type %s but the desired class is %s",
        id, service.getClass(), serviceClass);
    return (S) service;
  }

  /**
   * Create and initialize any subsidiary services, including the input stream factory.
   * @param conf configuration
   */
  @Override
  protected void serviceInit(final Configuration conf) throws Exception {

    // create and register the stream factory, which will
    // then follow the service lifecycle
    objectInputStreamFactory = factoryFromConfig(conf);
    registerChildService(OBJECT_INPUT_STREAM_FACTORY, objectInputStreamFactory);

    // init all child services, including the stream factory
    super.serviceInit(conf);
    // complete store writer binding
    storeWriter.bind(this, clientManager, this);

    // pass down extra information to the stream factory.
    finishStreamFactoryInit();
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    initLocalDirAllocator();
  }

  /**
   * Check the service is running.
   * @throws IllegalStateException if not in STARTED.
   */
  protected void checkRunning() throws IllegalStateException {
    checkState(isInState(STATE.STARTED),
        "Store is in state %s", getServiceState());
  }

  /**
   * Return the store path capabilities.
   * If the object stream factory is non-null, hands off the
   * query to that factory if not handled here.
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   * @return known capabilities
   */
  @Override
  public boolean hasPathCapability(final Path path, final String capability) {
    checkRunning();
    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS:
      return true;
    default:
      return inputStreamHasCapability(capability);
    }
  }

  /**
   * Return the capabilities of input streams created
   * through the store.
   * @param capability string to query the stream support for.
   * @return capabilities declared supported in streams.
   */
  @Override
  public boolean inputStreamHasCapability(final String capability) {
    checkRunning();
    if (objectInputStreamFactory != null) {
      return objectInputStreamFactory.hasCapability(capability);
    }
    return false;
  }

  /**
   * Initialize dir allocator if not already initialized.
   */
  private void initLocalDirAllocator() {
    String key = BUFFER_DIR;
    if (StringUtils.isEmpty(getConfig().getTrimmed(key))) {
      key = HADOOP_TMP_DIR;
    }
    directoryAllocator = new LocalDirAllocator(key);
  }

  /** Acquire write capacity for rate limiting {@inheritDoc}. */
  @Override
  public Duration acquireWriteCapacity(final int capacity) {
    checkRunning();
    return writeRateLimiter.acquire(capacity);
  }

  /** Acquire read capacity for rate limiting {@inheritDoc}. */
  @Override
  public Duration acquireReadCapacity(final int capacity) {
    checkRunning();
    return readRateLimiter.acquire(capacity);
  }

  @Override
  public StoreContext createStoreContext() {
    checkRunning();
    return storeContextFactory.createStoreContext();
  }

  @Override
  public StoreContext getStoreContext() {
    checkRunning();
    return storeContext;
  }

  /**
   * Get the S3 client manager.
   * @return client manager.
   */
  public ClientManager getClientManager() {
    return clientManager;
  }

  /**
   * Get the S3 client.
   * @return the S3 client.
   * @throws UncheckedIOException on any failure to create the client.
   */
  S3Client getS3Client() throws UncheckedIOException {
    checkRunning();
    return clientManager.getOrCreateS3ClientUnchecked();
  }

  @Override
  public S3TransferManager getOrCreateTransferManager() throws IOException {
    checkRunning();
    return clientManager.getOrCreateTransferManager();
  }

  @Override
  public S3Client getOrCreateS3Client() throws IOException {
    checkRunning();
    return clientManager.getOrCreateS3Client();
  }

  @Override
  public S3AsyncClient getOrCreateAsyncClient() throws IOException {
    checkRunning();
    return clientManager.getOrCreateAsyncClient();
  }

  @Override
  public S3Client getOrCreateS3ClientUnchecked() throws UncheckedIOException {
    checkRunning();
    return clientManager.getOrCreateS3ClientUnchecked();
  }

  @Override
  public S3Client getOrCreateAsyncS3ClientUnchecked() throws UncheckedIOException {
    checkRunning();
    return clientManager.getOrCreateAsyncS3ClientUnchecked();
  }

  @Override
  public S3Client getOrCreateUnencryptedS3Client() throws IOException {
    checkRunning();
    return clientManager.getOrCreateUnencryptedS3Client();
  }

  @Override
  public DurationTrackerFactory getDurationTrackerFactory() {
    checkRunning();
    return durationTrackerFactory;
  }

  @Override
  public S3AInstrumentation getInstrumentation() {
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

  @Override
  public void decrementGauge(Statistic statistic, long count) {
    statisticsContext.decrementGauge(statistic, count);
  }

  @Override
  public void incrementGauge(Statistic statistic, long count) {
    statisticsContext.incrementGauge(statistic, count);
  }

  @Override
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

  @Override
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
    checkRunning();
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

    checkRunning();
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
   * Performs a HEAD request on an S3 object to retrieve its metadata.
   *
   * @param key           The S3 object key to perform the HEAD operation on
   * @param changeTracker Tracks changes to the object's metadata across operations
   * @param changeInvoker The invoker responsible for executing the HEAD request with retries
   * @param fsHandler     Handler for filesystem-level operations and configurations
   * @param operation     Description of the operation being performed for tracking purposes
   * @return              HeadObjectResponse containing the object's metadata
   * @throws IOException  If the HEAD request fails, object doesn't exist, or other I/O errors occur
   */
  @Override
  @Retries.RetryRaw
  public HeadObjectResponse headObject(String key,
      ChangeTracker changeTracker,
      Invoker changeInvoker,
      S3AFileSystemOperations fsHandler,
      String operation) throws IOException {
    checkRunning();
    HeadObjectResponse response = getStoreContext().getInvoker()
        .retryUntranslated("HEAD " + key, true,
            () -> {
              HeadObjectRequest.Builder requestBuilder =
                  getRequestFactory().newHeadObjectRequestBuilder(key);
              incrementStatistic(OBJECT_METADATA_REQUESTS);
              DurationTracker duration =
                  getDurationTrackerFactory().trackDuration(ACTION_HTTP_HEAD_REQUEST.getSymbol());
              try {
                LOG.debug("HEAD {} with change tracker {}", key, changeTracker);
                if (changeTracker != null) {
                  changeTracker.maybeApplyConstraint(requestBuilder);
                }
                HeadObjectResponse headObjectResponse =
                    getS3Client().headObject(requestBuilder.build());
                if (fsHandler != null) {
                  long length =
                      fsHandler.getS3ObjectSize(key, headObjectResponse.contentLength(), this,
                          headObjectResponse);
                  // overwrite the content length
                  headObjectResponse = headObjectResponse.toBuilder().contentLength(length).build();
                }
                if (changeTracker != null) {
                  changeTracker.processMetadata(headObjectResponse, operation);
                }
                return headObjectResponse;
              } catch (AwsServiceException ase) {
                if (!isObjectNotFound(ase)) {
                  // file not found is not considered a failure of the call,
                  // so only switch the duration tracker to update failure
                  // metrics on other exception outcomes.
                  duration.failed();
                }
                throw ase;
              } finally {
                // update the tracker.
                duration.close();
              }
            });
    incrementReadOperations();
    return response;
  }

  /**
   * Retrieves a specific byte range of an S3 object as a stream.
   *
   * @param key    The S3 object key to retrieve
   * @param start  The starting byte position (inclusive) of the range to retrieve
   * @param end    The ending byte position (inclusive) of the range to retrieve
   * @return       A ResponseInputStream containing the requested byte range of the S3 object
   * @throws IOException  If the object cannot be retrieved other I/O errors occur
   * @see GetObjectResponse  For additional metadata about the retrieved object
   */
  @Override
  @Retries.RetryRaw
  public ResponseInputStream<GetObjectResponse> getRangedS3Object(String key,
      long start,
      long end) throws IOException {
    checkRunning();
    final GetObjectRequest request = getRequestFactory().newGetObjectRequestBuilder(key)
        .range(S3AUtils.formatRange(start, end))
        .build();
    DurationTracker duration = getDurationTrackerFactory()
        .trackDuration(ACTION_HTTP_GET_REQUEST);
    ResponseInputStream<GetObjectResponse> objectRange;
    try {
      objectRange = getStoreContext().getInvoker()
          .retryUntranslated("GET Ranged Object " + key, true,
              () -> getS3Client().getObject(request));

    } catch (IOException ex) {
      duration.failed();
      throw ex;
    } finally {
      duration.close();
    }
    return objectRange;
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  @Retries.RetryRaw
  public Map.Entry<Duration, Optional<DeleteObjectResponse>> deleteObject(
      final DeleteObjectRequest request)
      throws SdkException, UncheckedIOException {
    checkRunning();
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
      d.close();
      // 404 errors get swallowed; this can be raised by
      // third party stores (GCS).
      if (!isObjectNotFound(ase)) {
        throw ase;
      }
      return Tuples.pair(d.asDuration(), Optional.empty());
    } catch (IOException e) {
      d.close();
      // convert to unchecked.
      throw new UncheckedIOException(e);
    }
  }

  @Retries.RetryRaw
  @Override
  public void deleteObjectAtPath(
      String key,
      boolean isFile)
      throws SdkException, UncheckedIOException {
    checkRunning();
    if (isFile) {
      instrumentation.fileDeleted(1);
    } else {
      instrumentation.directoryDeleted();
    }
    acquireWriteCapacity(OBJECT_DELETE_WRITE_CAPACITY);
    deleteObject(getRequestFactory()
            .newDeleteObjectRequestBuilder(key)
            .build());
  }


  /**
   * Get the directory allocator.
   * @return the directory allocator
   */
  @Override
  public LocalDirAllocator getDirectoryAllocator() {
    return directoryAllocator;
  }

  /**
   * Demand create the directory allocator, then create a temporary file.
   * This does not mark the file for deletion when a process exits.
   * Pass in a file size of {@link LocalDirAllocator#SIZE_UNKNOWN} if the
   * size is unknown.
   * {@link LocalDirAllocator#createTmpFileForWrite(String, long, Configuration)}.
   * @param pathStr prefix for the temporary file
   * @param size the size of the file that is going to be written
   * @param conf the Configuration object
   * @return a unique temporary file
   * @throws IOException IO problems
   */
  @Override
  public File createTemporaryFileForWriting(String pathStr,
      long size,
      Configuration conf) throws IOException {
    checkRunning();
    requireNonNull(directoryAllocator, "directory allocator not initialized");
    Path path = directoryAllocator.getLocalPathForWrite(pathStr,
        size, conf);
    File dir = new File(path.getParent().toUri().getPath());
    String prefix = path.getName();
    // create a temp file on this directory
    return File.createTempFile(prefix, null, dir);
  }

  /*
   =============== BEGIN ObjectInputStreamFactory ===============
   */

  /**
   * All stream factory initialization required after {@code Service.init()},
   * after all other services have themselves been initialized.
   */
  private void finishStreamFactoryInit() throws IOException {
    // must be on be invoked during service initialization
    checkState(isInState(STATE.INITED),
        "Store is in wrong state: %s", getServiceState());
    checkState(clientManager.isInState(STATE.INITED),
        "Client Manager is in wrong state: %s", clientManager.getServiceState());

    // finish initialization and pass down callbacks to self
    objectInputStreamFactory.bind(new FactoryBindingParameters(new FactoryCallbacks()));
  }

  @Override /* ObjectInputStreamFactory */
  public ObjectInputStream readObject(ObjectReadParameters parameters)
      throws IOException {
    parameters.withDirectoryAllocator(getDirectoryAllocator());
    return objectInputStreamFactory.readObject(parameters.validate());
  }

  @Override /* ObjectInputStreamFactory */
  public StreamFactoryRequirements factoryRequirements() {
    return objectInputStreamFactory.factoryRequirements();
  }

  /**
   * This operation is not implemented, as
   * is this class which invokes it on the actual factory.
   * @param factoryBindingParameters ignored
   * @throws UnsupportedOperationException always
   */
  @Override /* ObjectInputStreamFactory */
  public void bind(final FactoryBindingParameters factoryBindingParameters) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override /* ObjectInputStreamFactory */
  public InputStreamType streamType() {
    return objectInputStreamFactory.streamType();
  }

  /**
   * Callbacks from {@link ObjectInputStreamFactory} instances.
   */
  private class FactoryCallbacks implements StreamFactoryCallbacks {

    @Override
    public S3Client getOrCreateSyncClient() throws IOException {
      LOG.debug("Stream factory requested sync client");
      return clientManager().getOrCreateS3Client();
    }

    @Override
    public void incrementFactoryStatistic(Statistic statistic) {
      incrementStatistic(statistic);
    }

  }

  /*
   =============== END ObjectInputStreamFactory ===============
   */


  /*
   =============== BEGIN WriteOperationHelperCallbacks ===============
   These either invoke internal store operations or those of multipart IO.
   */

  @Override
  public WriteOperationHelperCallbacks createWriteOperationHelperCallbacks() {
    return new WriteOperationHelperCallbacksImpl(this);
  }

  /*
   =============== END WriteOperationHelperCallbacks ===============
   */
}
