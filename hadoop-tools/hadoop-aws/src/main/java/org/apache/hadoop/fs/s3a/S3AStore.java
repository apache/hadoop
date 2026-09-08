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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.PathCapabilities;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.impl.ClientManager;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteException;
import org.apache.hadoop.fs.s3a.impl.write.StoreWriterService;
import org.apache.hadoop.fs.s3a.impl.write.StoreWriter;
import org.apache.hadoop.fs.s3a.impl.S3AFileSystemOperations;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamFactory;
import org.apache.hadoop.fs.s3a.impl.write.WriteOperationHelperCallbacks;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.s3a.api.IORateLimiting;
import org.apache.hadoop.service.Service;

/**
 * Interface for the S3A Store;
 * S3 client interactions should be via this; mocking
 * is possible for unit tests.
 * <p>
 * The {@link ClientManager} interface is used to create the AWS clients;
 * the base implementation forwards to the implementation of this interface
 * passed in at construction time.
 * <p>
 * The interface extends the Hadoop {@link Service} interface
 * and follows its lifecycle: it MUST NOT be used until
 * {@link Service#init(Configuration)} has been invoked.
 */
@InterfaceAudience.LimitedPrivate("Extensions")
@InterfaceStability.Unstable
public interface S3AStore extends
    ClientManager,
    IORateLimiting,
    IOStatisticsSource,
    ObjectInputStreamFactory,
    PathCapabilities,
    Service {

  /**
   * Acquire write capacity for operations.
   * This should be done within retry loops.
   * @param capacity capacity to acquire.
   * @return time spent waiting for output.
   */
  Duration acquireWriteCapacity(int capacity);

  /**
   * Acquire read capacity for operations.
   * This should be done within retry loops.
   * @param capacity capacity to acquire.
   * @return time spent waiting for output.
   */
  Duration acquireReadCapacity(int capacity);

  /**
   * Create a new store context.
   * @return a new store context.
   */
  StoreContext createStoreContext();

  StoreContext getStoreContext();

  DurationTrackerFactory getDurationTrackerFactory();

  S3AInstrumentation getInstrumentation();

  S3AStatisticsContext getStatisticsContext();

  RequestFactory getRequestFactory();

  ClientManager clientManager();

  /**
   * Get the store writer operations.
   * @return an instance of StoreWriterService.
   */
  default StoreWriterService getStoreWriter() {
    return lookupService(StoreWriter.STORE_WRITER, StoreWriterService.class);
  }

  /**
   * Look up a service by name, validate its class type and then return the cast value.
   * This allows for the lookup of any registered service within the store, if the name
   * and type is known.
   * @param name service name
   * @param serviceClass service class.
   * @return the class
   * @param <S> type of service
   * @throws IllegalStateException if the service is not found or the class type is wrong.
   */
  <S extends Service> S lookupService(String name, Class<S> serviceClass);

  /**
   * Decrement a gauge by a specific value.
   * @param statistic The operation to decrement
   * @param count the count to decrement
   */
  void decrementGauge(Statistic statistic, long count);

  /**
   * Increment a gauge by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  void incrementGauge(Statistic statistic, long count);

  /**
   * Callback when an operation was retried.
   * Increments the statistics of ignored errors or throttled requests,
   * depending up on the exception class.
   * @param ex exception.
   */
  void operationRetried(Exception ex);

  /**
   * Callback from {@link Invoker} when an operation is retried.
   * @param text text of the operation
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  void operationRetried(String text, Exception ex, int retries, boolean idempotent);

  /**
   * Increment read operations.
   */
  void incrementReadOperations();

  /**
   * Increment the write operation counter.
   * This is somewhat inaccurate, as it appears to be invoked more
   * often than needed in progress callbacks.
   */
  void incrementWriteOperations();

  /**
   * At the start of a put/multipart upload operation, update the
   * relevant counters.
   *
   * @param bytes bytes in the request.
   */
  void incrementPutStartStatistics(long bytes);

  /**
   * At the end of a put/multipart upload operation, update the
   * relevant counters and gauges.
   *
   * @param success did the operation succeed?
   * @param bytes bytes in the request.
   */
  void incrementPutCompletedStatistics(boolean success, long bytes);

  /**
   * Callback for use in progress callbacks from put/multipart upload events.
   * Increments those statistics which are expected to be updated during
   * the ongoing upload operation.
   * @param key key to file that is being written (for logging)
   * @param bytes bytes successfully uploaded.
   */
  void incrementPutProgressStatistics(String key, long bytes);

  /**
   * Given a possibly null duration tracker factory, return a non-null
   * one for use in tracking durations -either that or the store tracker
   * itself.
   *
   * @param factory factory.
   * @return a non-null factory.
   */
  DurationTrackerFactory nonNullDurationTrackerFactory(
      DurationTrackerFactory factory);

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
   * A write capacity will be requested proportional to the number of keys
   * preset in the request and will be re-requested during retries such that
   * retries throttle better. If the request is throttled, the time spent is
   * recorded in a duration IOStat named {@code STORE_IO_RATE_LIMITED_DURATION}.
   * @param deleteRequest keys to delete on the s3-backend
   * @return the AWS response
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted.
   * @throws SdkException amazon-layer failure.
   * @throws IOException IO problems.
   */
  @Retries.RetryRaw
  Map.Entry<Duration, DeleteObjectsResponse> deleteObjects(DeleteObjectsRequest deleteRequest)
      throws MultiObjectDeleteException, SdkException, IOException;

  /**
   * Delete an object.
   * Increments the {@code OBJECT_DELETE_REQUESTS} statistics.
   * <p>
   * Retry policy: retry untranslated; delete considered idempotent.
   * 404 errors other than bucket not found are swallowed;
   * this can be raised by third party stores (GCS).
   * <p>
   * A write capacity of 1 ( as it is signle object delete) will be requested before
   * the delete call and will be re-requested during retries such that
   * retries throttle better. If the request is throttled, the time spent is
   * recorded in a duration IOStat named {@code STORE_IO_RATE_LIMITED_DURATION}.
   * If an exception is caught and swallowed, the response will be empty;
   * otherwise it will be the response from the delete operation.
   * @param request request to make
   * @return the total duration and response.
   * @throws SdkException problems working with S3
   * @throws IllegalArgumentException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws UncheckedIOException from invocation operations
   */
  @Retries.RetryRaw
  Map.Entry<Duration, Optional<DeleteObjectResponse>> deleteObject(
      DeleteObjectRequest request) throws SdkException, UncheckedIOException;

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
  @Retries.RetryRaw
  HeadObjectResponse headObject(String key,
      ChangeTracker changeTracker,
      Invoker changeInvoker,
      S3AFileSystemOperations fsHandler,
      String operation) throws IOException;

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
  @Retries.RetryRaw
  ResponseInputStream<GetObjectResponse> getRangedS3Object(String key,
      long start,
      long end) throws IOException;

  /**
   * Delete an object after acquiring write capacity.
   * This call does <i>not</i> create any mock parent entries.
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param key key of entry
   * @param isFile is the path a file (used for instrumentation only)
   * @throws SdkException problems working with S3
   * @throws UncheckedIOException from invoker signature only -should not be raised.
   */
  @Retries.RetryRaw
  void deleteObjectAtPath(
      String key,
      boolean isFile)
      throws SdkException, UncheckedIOException;


  /**
   * Get the directory allocator.
   * @return the directory allocator
   */
  LocalDirAllocator getDirectoryAllocator();

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
  File createTemporaryFileForWriting(String pathStr,
      long size,
      Configuration conf) throws IOException;


  /*
   =============== BEGIN ObjectInputStreamFactory ===============
   */

  /**
   * Return the capabilities of input streams created
   * through the store.
   * @param capability string to query the stream support for.
   * @return capabilities declared supported in streams.
   */
  boolean inputStreamHasCapability(String capability);

  /**
   * The StreamCapabilities is part of ObjectInputStreamFactory.
   * To avoid confusion with any other streams which may
   * be added here: always return false.
   * @param capability string to query the stream support for.
   * @return false, always.
   */
  default boolean hasCapability(String capability) {
    return false;
  }

  /*
   =============== END ObjectInputStreamFactory ===============
   */

  /*
   =============== BEGIN WriteOperationHelperCallbacks ===============
   */

  /**
   * Create a new instance of the write operation callbacks.
   * @return a callback instance
   */
  WriteOperationHelperCallbacks createWriteOperationHelperCallbacks();


  /*
   =============== END WriteOperationHelperCallbacks ===============
   */
}
