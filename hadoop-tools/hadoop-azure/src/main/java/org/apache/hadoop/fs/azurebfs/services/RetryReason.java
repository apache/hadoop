/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.ClientErrorRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.ConnectionResetRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.ConnectionTimeoutRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.ReadTimeoutRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.RetryReasonCategory;
import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.ServerErrorRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.UnknownHostRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.UnknownIOExceptionRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonCategories.UnknownSocketExceptionRetryReason;


/**
 * This utility class exposes methods to convert a server response-error to a
 * category of error.
 */
final class RetryReason {

  /**
   * Linked-list of the implementations of RetryReasonCategory. The objects in the
   * list are arranged by the rank of their significance.
   * <ul>
   *   <li>ServerError (statusCode==5XX), ClientError (statusCode==4XX) are
   *   independent of other retryReason categories.</li>
   *   <li>Since {@link java.net.SocketException} is subclass of
   *   {@link java.io.IOException},
   *   hence, {@link UnknownIOExceptionRetryReason} is placed before
   *   {@link UnknownSocketExceptionRetryReason}</li>
   *   <li>Since, connectionTimeout, readTimeout, and connectionReset are
   *   {@link java.net.SocketTimeoutException} exceptions with different messages,
   *   hence, {@link ConnectionTimeoutRetryReason}, {@link ReadTimeoutRetryReason},
   *   {@link ConnectionResetRetryReason} are above {@link UnknownIOExceptionRetryReason}.
   *   There is no order between the three reasons as they are differentiated
   *   by exception-message.</li>
   *   <li>Since, {@link java.net.UnknownHostException} is subclass of
   *   {@link java.io.IOException}, {@link UnknownHostRetryReason} is placed
   *   over {@link UnknownIOExceptionRetryReason}</li>
   * </ul>
   */
  private static List<RetryReasonCategory> rankedReasonCategories
      = new LinkedList<RetryReasonCategory>() {{
    add(new ServerErrorRetryReason());
    add(new ClientErrorRetryReason());
    add(new UnknownIOExceptionRetryReason());
    add(new UnknownSocketExceptionRetryReason());
    add(new ConnectionTimeoutRetryReason());
    add(new ReadTimeoutRetryReason());
    add(new UnknownHostRetryReason());
    add(new ConnectionResetRetryReason());
  }};

  private RetryReason() {

  }

  /**
   * Method to get correct abbreviation for a given set of exception, statusCode,
   * storageStatusCode.
   *
   * @param ex exception caught during server communication.
   * @param statusCode statusCode in the server response.
   * @param storageErrorMessage storageErrorMessage in the server response.
   *
   * @return abbreviation for the the given set of exception, statusCode, storageStatusCode.
   */
  static String getAbbreviation(Exception ex,
      Integer statusCode,
      String storageErrorMessage) {
    String result = null;
    for (RetryReasonCategory retryReasonCategory : rankedReasonCategories) {
      final String abbreviation
          = retryReasonCategory.captureAndGetAbbreviation(ex,
          statusCode, storageErrorMessage);
      if (abbreviation != null) {
        result = abbreviation;
      }
    }
    return result;
  }
}
