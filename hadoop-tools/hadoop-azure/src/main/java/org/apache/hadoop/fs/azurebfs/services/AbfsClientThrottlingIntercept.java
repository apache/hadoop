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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class AbfsClientThrottlingIntercept {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsClientThrottlingIntercept.class);
  private static AbfsClientThrottlingIntercept singleton = null;
  private AbfsClientThrottlingAnalyzer readThrottler = null;
  private AbfsClientThrottlingAnalyzer writeThrottler = null;
  private static boolean isAutoThrottlingEnabled = false;

  // Hide default constructor
  private AbfsClientThrottlingIntercept() {
    readThrottler = new AbfsClientThrottlingAnalyzer("read");
    writeThrottler = new AbfsClientThrottlingAnalyzer("write");
  }

  public static synchronized void initializeSingleton(boolean enableAutoThrottling) {
    if (!enableAutoThrottling) {
      return;
    }
    if (singleton == null) {
      singleton = new AbfsClientThrottlingIntercept();
      isAutoThrottlingEnabled = true;
      LOG.debug("Client-side throttling is enabled for the ABFS file system.");
    }
  }

  static void updateMetrics(AbfsRestOperationType operationType,
                            AbfsHttpOperation abfsHttpOperation) {
    if (!isAutoThrottlingEnabled || abfsHttpOperation == null) {
      return;
    }

    int status = abfsHttpOperation.getStatusCode();
    long contentLength = 0;
    // If the socket is terminated prior to receiving a response, the HTTP
    // status may be 0 or -1.  A status less than 200 or greater than or equal
    // to 500 is considered an error.
    boolean isFailedOperation = (status < HttpURLConnection.HTTP_OK
        || status >= HttpURLConnection.HTTP_INTERNAL_ERROR);

    switch (operationType) {
      case Append:
        contentLength = abfsHttpOperation.getBytesSent();
        if (contentLength > 0) {
          singleton.writeThrottler.addBytesTransferred(contentLength,
              isFailedOperation);
        }
        break;
      case ReadFile:
        contentLength = abfsHttpOperation.getBytesReceived();
        if (contentLength > 0) {
          singleton.readThrottler.addBytesTransferred(contentLength,
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
  static void sendingRequest(AbfsRestOperationType operationType) {
    if (!isAutoThrottlingEnabled) {
      return;
    }

    switch (operationType) {
      case ReadFile:
        singleton.readThrottler.suspendIfNecessary();
        break;
      case Append:
        singleton.writeThrottler.suspendIfNecessary();
        break;
      default:
        break;
    }
  }
}