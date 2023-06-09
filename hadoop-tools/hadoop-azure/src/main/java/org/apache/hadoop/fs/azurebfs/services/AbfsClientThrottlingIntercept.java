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

package org.apache.hadoop.fs.azurebfs.services;

import java.net.HttpURLConnection;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsStatistic;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

/**
 * Throttles Azure Blob File System read and write operations to achieve maximum
 * throughput by minimizing errors.  The errors occur when the account ingress
 * or egress limits are exceeded and the server-side throttles requests.
 * Server-side throttling causes the retry policy to be used, but the retry
 * policy sleeps for long periods of time causing the total ingress or egress
 * throughput to be as much as 35% lower than optimal.  The retry policy is also
 * after the fact, in that it applies after a request fails.  On the other hand,
 * the client-side throttling implemented here happens before requests are made
 * and sleeps just enough to minimize errors, allowing optimal ingress and/or
 * egress throughput.
 */
public final class AbfsClientThrottlingIntercept implements AbfsThrottlingIntercept {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsClientThrottlingIntercept.class);
  private static final String RANGE_PREFIX = "bytes=";
  private static AbfsClientThrottlingIntercept singleton; // singleton, initialized in static initialization block
  private static final ReentrantLock LOCK = new ReentrantLock();
  private final AbfsClientThrottlingAnalyzer readThrottler;
  private final AbfsClientThrottlingAnalyzer writeThrottler;
  private final String accountName;

  // Hide default constructor
  public AbfsClientThrottlingIntercept(String accountName, AbfsConfiguration abfsConfiguration) {
    this.accountName = accountName;
    this.readThrottler = setAnalyzer("read " + accountName, abfsConfiguration);
    this.writeThrottler = setAnalyzer("write " + accountName, abfsConfiguration);
    LOG.debug("Client-side throttling is enabled for the ABFS file system for the account : {}", accountName);
  }

  // Hide default constructor
  private AbfsClientThrottlingIntercept(AbfsConfiguration abfsConfiguration) {
    // Account name is kept as empty as same instance is shared across all accounts.
    this.accountName = "";
    this.readThrottler = setAnalyzer("read", abfsConfiguration);
    this.writeThrottler = setAnalyzer("write", abfsConfiguration);
    LOG.debug("Client-side throttling is enabled for the ABFS file system using singleton intercept");
  }

  /**
   * Sets the analyzer for the intercept.
   * @param name Name of the analyzer.
   * @param abfsConfiguration The configuration.
   * @return AbfsClientThrottlingAnalyzer instance.
   */
  private AbfsClientThrottlingAnalyzer setAnalyzer(String name, AbfsConfiguration abfsConfiguration) {
    return new AbfsClientThrottlingAnalyzer(name, abfsConfiguration);
  }

  /**
   * Returns the analyzer for read operations.
   * @return AbfsClientThrottlingAnalyzer for read.
   */
  AbfsClientThrottlingAnalyzer getReadThrottler() {
    return readThrottler;
  }

  /**
   * Returns the analyzer for write operations.
   * @return AbfsClientThrottlingAnalyzer for write.
   */
  AbfsClientThrottlingAnalyzer getWriteThrottler() {
    return writeThrottler;
  }

  /**
   * Creates a singleton object of the AbfsClientThrottlingIntercept.
   * which is shared across all filesystem instances.
   * @param abfsConfiguration configuration set.
   * @return singleton object of intercept.
   */
  static AbfsClientThrottlingIntercept initializeSingleton(AbfsConfiguration abfsConfiguration) {
    if (singleton == null) {
      LOCK.lock();
      try {
        if (singleton == null) {
          singleton = new AbfsClientThrottlingIntercept(abfsConfiguration);
          LOG.debug("Client-side throttling is enabled for the ABFS file system.");
        }
      } finally {
        LOCK.unlock();
      }
    }
    return singleton;
  }

  /**
   * Updates the metrics for the case when response code signifies throttling
   * but there are some expected bytes to be sent.
   * @param isThrottledOperation returns true if status code is HTTP_UNAVAILABLE
   * @param abfsHttpOperation Used for status code and data transferred.
   * @return true if the operation is throttled and has some bytes to transfer.
   */
  private boolean updateBytesTransferred(boolean isThrottledOperation,
      AbfsHttpOperation abfsHttpOperation) {
    return isThrottledOperation && abfsHttpOperation.getExpectedBytesToBeSent() > 0;
  }

  /**
   * Updates the metrics for successful and failed read and write operations.
   * @param operationType Only applicable for read and write operations.
   * @param abfsHttpOperation Used for status code and data transferred.
   */
  @Override
  public void updateMetrics(AbfsRestOperationType operationType,
      AbfsHttpOperation abfsHttpOperation) {
    if (abfsHttpOperation == null) {
      return;
    }

    int status = abfsHttpOperation.getStatusCode();
    long contentLength = 0;
    // If the socket is terminated prior to receiving a response, the HTTP
    // status may be 0 or -1.  A status less than 200 or greater than or equal
    // to 500 is considered an error.
    boolean isFailedOperation = (status < HttpURLConnection.HTTP_OK
        || status >= HttpURLConnection.HTTP_INTERNAL_ERROR);

    // If status code is 503, it is considered as a throttled operation.
    boolean isThrottledOperation = (status == HTTP_UNAVAILABLE);

    switch (operationType) {
      case Append:
        contentLength = abfsHttpOperation.getBytesSent();
        if (contentLength == 0) {
          /*
            Signifies the case where we could not update the bytesSent due to
            throttling but there were some expectedBytesToBeSent.
           */
          if (updateBytesTransferred(isThrottledOperation, abfsHttpOperation)) {
            LOG.debug("Updating metrics due to throttling for path {}", abfsHttpOperation.getConnUrl().getPath());
            contentLength = abfsHttpOperation.getExpectedBytesToBeSent();
          }
        }
        if (contentLength > 0) {
          writeThrottler.addBytesTransferred(contentLength,
              isFailedOperation);
        }
        break;
      case ReadFile:
        String range = abfsHttpOperation.getConnection().getRequestProperty(HttpHeaderConfigurations.RANGE);
        contentLength = getContentLengthIfKnown(range);
        if (contentLength > 0) {
          readThrottler.addBytesTransferred(contentLength,
              isFailedOperation);
        }
        break;
      default:
        break;
    }
  }

  /**
   * Called before the request is sent.  Client-side throttling
   * uses this to suspend the request, if necessary, to minimize errors and
   * maximize throughput.
   */
  @Override
  public void sendingRequest(AbfsRestOperationType operationType,
      AbfsCounters abfsCounters) {
    switch (operationType) {
      case ReadFile:
        if (readThrottler.suspendIfNecessary()
            && abfsCounters != null) {
          abfsCounters.incrementCounter(AbfsStatistic.READ_THROTTLES, 1);
        }
        break;
      case Append:
        if (writeThrottler.suspendIfNecessary()
            && abfsCounters != null) {
          abfsCounters.incrementCounter(AbfsStatistic.WRITE_THROTTLES, 1);
        }
        break;
      default:
        break;
    }
  }

  private static long getContentLengthIfKnown(String range) {
    long contentLength = 0;
    // Format is "bytes=%d-%d"
    if (range != null && range.startsWith(RANGE_PREFIX)) {
      String[] offsets = range.substring(RANGE_PREFIX.length()).split("-");
      if (offsets.length == 2) {
        contentLength = Long.parseLong(offsets[1]) - Long.parseLong(offsets[0])
                + 1;
      }
    }
    return contentLength;
  }
}
