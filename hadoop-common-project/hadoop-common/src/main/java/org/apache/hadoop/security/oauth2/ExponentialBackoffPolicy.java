package org.apache.hadoop.security.oauth2;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ExponentialBackoffPolicy implements RetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      ExponentialBackoffPolicy.class);

  private int retryCount = 0;
  private int maxRetries;
  private int exponentialRetryInterval;
  private int exponentialFactor;
  private long lastAttemptStartTime;

  public ExponentialBackoffPolicy(
      int maxRetries, int exponentialRetryInterval, int exponentialFactor) {
    this.maxRetries = maxRetries;
    this.exponentialRetryInterval = exponentialRetryInterval;
    this.exponentialFactor = exponentialFactor;
    setLastAttemptStartTime();
  }

  @Override
  public boolean shouldRetry(int httpResponseCode, Exception lastException) {
    if ((httpResponseCode < HttpStatus.MULTIPLE_CHOICES_300 ||
        httpResponseCode >= HttpStatus.INTERNAL_SERVER_ERROR_500 ||
        httpResponseCode == HttpStatus.REQUEST_TIMEOUT_408 ||
        httpResponseCode == HttpStatus.TOO_MANY_REQUESTS_429 ||
        httpResponseCode == HttpStatus.UNAUTHORIZED_401) &&
        httpResponseCode != HttpStatus.NOT_IMPLEMENTED_501 &&
        httpResponseCode != HttpStatus.HTTP_VERSION_NOT_SUPPORTED_505) {
      if (lastException == null &&
          httpResponseCode < HttpStatus.INTERNAL_SERVER_ERROR_500 &&
          httpResponseCode != HttpStatus.REQUEST_TIMEOUT_408 &&
          httpResponseCode != HttpStatus.TOO_MANY_REQUESTS_429 &&
          httpResponseCode != HttpStatus.UNAUTHORIZED_401) {
        return httpResponseCode < HttpStatus.CONTINUE_100;
      } else if (this.retryCount < this.maxRetries) {
        int timeSpent = (int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
            - this.lastAttemptStartTime);
        this.wait(this.exponentialRetryInterval - timeSpent);
        this.exponentialRetryInterval *= this.exponentialFactor;
        this.retryCount++;
        setLastAttemptStartTime();
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  private void setLastAttemptStartTime() {
    long now = System.nanoTime();
    if (now < 0) {
      LOG.error("System.nanoTime() returned " + now + ", resetting to 0.");
      now = 0;
    }
    this.lastAttemptStartTime = now;
  }

  private void wait(int milliseconds) {
    if (milliseconds > 0) {
      try {
        Thread.sleep((long) milliseconds);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }
}