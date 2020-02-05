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

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.Statistic;

import static org.apache.hadoop.fs.s3a.S3AUtils.isThrottleException;
import static org.apache.hadoop.fs.s3a.Statistic.IGNORED_ERRORS;
import static org.apache.hadoop.fs.s3a.Statistic.S3GUARD_METADATASTORE_RETRY;
import static org.apache.hadoop.fs.s3a.Statistic.S3GUARD_METADATASTORE_THROTTLED;
import static org.apache.hadoop.fs.s3a.Statistic.S3GUARD_METADATASTORE_THROTTLE_RATE;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLED;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLE_RATE;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.THROTTLE_LOG_NAME;

/**
 * Handler for retry callbacks.
 * This is intended to be pulled out for all operations, but for now it is only
 * used in delete objects.
 */
public class StandardInvokeRetryHandler extends AbstractStoreOperation {


  private static final Logger LOG = LoggerFactory.getLogger(
      StandardInvokeRetryHandler.class);

  private static final Logger THROTTLE_LOG = LoggerFactory.getLogger(
      THROTTLE_LOG_NAME);

  private final S3AInstrumentation instrumentation;

  private final S3AStorageStatistics storageStatistics;

  public StandardInvokeRetryHandler(final StoreContext storeContext) {
    super(storeContext);
    instrumentation = storeContext.getInstrumentation();
    storageStatistics = storeContext.getStorageStatistics();
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
    instrumentation.incrementCounter(statistic, count);
    storageStatistics.incrementCounter(statistic, count);
  }

  /**
   * Decrement a gauge by a specific value.
   * @param statistic The operation to decrement
   * @param count the count to decrement
   */
  protected void decrementGauge(Statistic statistic, long count) {
    instrumentation.decrementGauge(statistic, count);
  }

  /**
   * Increment a gauge by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementGauge(Statistic statistic, long count) {
    instrumentation.incrementGauge(statistic, count);
  }

  /**
   * Callback when an operation was retried.
   * Increments the statistics of ignored errors or throttled requests,
   * depending up on the exception class.
   * @param ex exception.
   */
  public void operationRetried(Exception ex) {
    if (isThrottleException(ex)) {
      operationThrottled(false);
    } else {
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
  public void operationRetried(
      String text,
      Exception ex,
      int retries,
      boolean idempotent) {
    operationRetried(ex);
  }

  /**
   * Callback from {@link Invoker} when an operation against a metastore
   * is retried.
   * Always increments the {@link Statistic#S3GUARD_METADATASTORE_RETRY}
   * statistic/counter;
   * if it is a throttling exception will update the associated
   * throttled metrics/statistics.
   *
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  public void metastoreOperationRetried(Exception ex,
      int retries,
      boolean idempotent) {
    incrementStatistic(S3GUARD_METADATASTORE_RETRY);
    if (isThrottleException(ex)) {
      operationThrottled(true);
    } else {
      incrementStatistic(IGNORED_ERRORS);
    }
  }


  /**
   * Handler for failure of bulk delete requests.
   * @param deleteRequest request which was retried.
   * @param ex exception
   */
  public void bulkDeleteRetried(
      DeleteObjectsRequest deleteRequest,
      Exception ex) {
    if (isThrottleException(ex)) {
      final int size = deleteRequest.getKeys().size();
      incrementStatistic(STORE_IO_THROTTLED, size);
      instrumentation.addValueToQuantiles(STORE_IO_THROTTLE_RATE, size);
      THROTTLE_LOG.warn("Bulk delete {} keys throttled -first key = {}",
          size, deleteRequest.getKeys().get(0));
    } else {
      incrementStatistic(IGNORED_ERRORS);
    }
  }


  /**
   * Note that an operation was throttled -this will update
   * specific counters/metrics.
   * @param metastore was the throttling observed in the S3Guard metastore?
   */
  private void operationThrottled(boolean metastore) {
    THROTTLE_LOG.debug("Request throttled on {}", metastore ? "S3" : "DynamoDB");
    if (metastore) {
      incrementStatistic(S3GUARD_METADATASTORE_THROTTLED);
      instrumentation.addValueToQuantiles(S3GUARD_METADATASTORE_THROTTLE_RATE,
          1);
    } else {
      incrementStatistic(STORE_IO_THROTTLED);
      instrumentation.addValueToQuantiles(STORE_IO_THROTTLE_RATE, 1);
    }
  }

}
