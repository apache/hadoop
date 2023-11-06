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

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_CONTINUE;

/**
 * Abstract Class for Retry policy to be used by {@link AbfsClient}
 * Implementation to be used is based on retry cause.
 */
public abstract class AbfsRetryPolicy {

  /**
   * The maximum number of retry attempts.
   */
  private final int maxRetryCount;

  /**
   * Retry Policy Abbreviation for logging purpose.
   */
  private final String retryPolicyAbbreviation;

  protected AbfsRetryPolicy(final int maxRetryCount, final String retryPolicyAbbreviation) {
    this.maxRetryCount = maxRetryCount;
    this.retryPolicyAbbreviation = retryPolicyAbbreviation;
  }

  /**
   * Returns if a request should be retried based on the retry count, current response,
   * and the current strategy. The valid http status code lies in the range of 1xx-5xx.
   * But an invalid status code might be set due to network or timeout kind of issues.
   * Such invalid status code also qualify for retry.
   *
   * @param retryCount The current retry attempt count.
   * @param statusCode The status code of the response, or -1 for socket error.
   * @return true if the request should be retried; false otherwise.
   */
  public boolean shouldRetry(final int retryCount, final int statusCode) {
    return retryCount < maxRetryCount
        && (statusCode < HTTP_CONTINUE
        || statusCode == HttpURLConnection.HTTP_CLIENT_TIMEOUT
        || (statusCode >= HttpURLConnection.HTTP_INTERNAL_ERROR
        && statusCode != HttpURLConnection.HTTP_NOT_IMPLEMENTED
        && statusCode != HttpURLConnection.HTTP_VERSION));
  }

  /**
   * Returns backoff interval to be used for a particular retry count
   * Child class should define how they want to calculate retry interval
   *
   * @param retryCount The current retry attempt count.
   * @return backoff Interval time
   */
  public abstract long getRetryInterval(int retryCount);

  /**
   * Returns a String value of the abbreviation
   * denoting which type of retry policy is used
   * @return retry policy abbreviation
   */
  public String getAbbreviation() {
    return retryPolicyAbbreviation;
  }

  /**
   * Returns maximum number of retries allowed in this retry policy
   * @return max retry count
   */
  protected int getMaxRetryCount() {
    return maxRetryCount;
  }

  @Override
  public String toString() {
    return "AbfsRetryPolicy of subtype: "
        + retryPolicyAbbreviation
        + " and max retry count: "
        + maxRetryCount;
  }
}
