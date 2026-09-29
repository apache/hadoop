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

package org.apache.hadoop.fs.azurebfs.services.retryReasonCategories;

import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_TAIL_LATENCY_REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.TAIL_LATENCY_REQUEST_TIMEOUT_ABBREVIATION;

/**
 * Retry reason category for tail latency request timeout scenarios.
 */
public class TailLatencyRequestTimeoutRetryReason extends RetryReasonCategory {

  /**
   * Get abbreviation for the tail latency request timeout retry reason.
   * @param statusCode statusCode on the server response
   * @param serverErrorMessage serverErrorMessage on the server response.
   *
   * @return abbreviation string.
   */
  @Override
  String getAbbreviation(final Integer statusCode,
      final String serverErrorMessage) {
    return TAIL_LATENCY_REQUEST_TIMEOUT_ABBREVIATION;
  }

  /**
   * Determine if the exception can be captured as a tail latency request timeout.
   * @param ex exception captured in the server response.
   * @param statusCode statusCode on the server response
   * @param serverErrorMessage serverErrorMessage on the server response.
   *
   * @return true if it can be captured, false otherwise.
   */
  @Override
  Boolean canCapture(final Exception ex,
      final Integer statusCode,
      final String serverErrorMessage) {
    return checkExceptionMessage(ex, ERR_TAIL_LATENCY_REQUEST_TIMEOUT);
  }
}
