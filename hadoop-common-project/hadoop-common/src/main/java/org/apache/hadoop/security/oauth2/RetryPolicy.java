package org.apache.hadoop.security.oauth2;

public interface RetryPolicy {

  /**
   * Check whether should retry the request.
   * @param httpResponseCode HTTP response code
   * @param lastException last exception encountered during the request
   * @return true if the request should be retried
   */
  boolean shouldRetry(int httpResponseCode, Exception lastException);
}
