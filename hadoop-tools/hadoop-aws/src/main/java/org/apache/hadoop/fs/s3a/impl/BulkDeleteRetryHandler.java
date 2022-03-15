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

import java.util.List;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;

import static org.apache.hadoop.fs.s3a.S3AUtils.isThrottleException;
import static org.apache.hadoop.fs.s3a.Statistic.IGNORED_ERRORS;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLED;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLE_RATE;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.THROTTLE_LOG_NAME;

/**
 * Handler for bulk delete retry events.
 */
public class BulkDeleteRetryHandler extends AbstractStoreOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      BulkDeleteRetryHandler.class);

  private static final Logger THROTTLE_LOG = LoggerFactory.getLogger(
      THROTTLE_LOG_NAME);

  /**
   * This is an error string we see in exceptions when the XML parser
   * failed: {@value}.
   */
  public static final String XML_PARSE_BROKEN = "Failed to parse XML document";

  private final S3AStatisticsContext instrumentation;

  private final S3AStorageStatistics storageStatistics;

  /**
   * Constructor.
   * @param storeContext context
   */
  public BulkDeleteRetryHandler(final StoreContext storeContext) {
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
  }

  /**
   * Handler for failure of bulk delete requests.
   * @param deleteRequest request which was retried.
   * @param ex exception
   */
  public void bulkDeleteRetried(
      DeleteObjectsRequest deleteRequest,
      Exception ex) {
    LOG.debug("Retrying on error during bulk delete", ex);
    if (isThrottleException(ex)) {
      onDeleteThrottled(deleteRequest);
    } else if (isSymptomOfBrokenConnection(ex)) {
      // this is one which surfaces when an HTTPS connection is broken while
      // the service is reading the result.
      // it is treated as a throttle event for statistics
      LOG.warn("Bulk delete operation interrupted: {}", ex.getMessage());
      onDeleteThrottled(deleteRequest);
    } else {
      incrementStatistic(IGNORED_ERRORS);
    }
  }

  /**
   * Handle a delete throttling event.
   * @param deleteRequest request which failed.
   */
  private void onDeleteThrottled(final DeleteObjectsRequest deleteRequest) {
    final List<DeleteObjectsRequest.KeyVersion> keys = deleteRequest.getKeys();
    final int size = keys.size();
    incrementStatistic(STORE_IO_THROTTLED, size);
    instrumentation.addValueToQuantiles(STORE_IO_THROTTLE_RATE, size);
    THROTTLE_LOG.info(
        "Bulk delete {} keys throttled -first key = {}; last = {}",
        size,
        keys.get(0).getKey(),
        keys.get(size - 1).getKey());
  }

  /**
   * Does this error indicate that the connection was ultimately broken while
   * the XML Response was parsed? As this seems a symptom of the far end
   * blocking the response (i.e. server-side throttling) while
   * the client eventually times out.
   * @param ex exception received.
   * @return true if this exception is considered a sign of a broken connection.
   */
  private boolean isSymptomOfBrokenConnection(final Exception ex) {
    return ex instanceof AWSClientIOException
        && ex.getCause() instanceof SdkClientException
        && ex.getMessage().contains(XML_PARSE_BROKEN);
  }

}
