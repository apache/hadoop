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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.ConnectTimeoutException;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Invoker.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.verifyExceptionClass;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test the {@link Invoker} code and the associated {@link S3ARetryPolicy}.
 *
 * Some of the tests look at how Connection Timeout Exceptions are processed.
 * Because of how the AWS libraries shade the classes, there have been some
 * regressions here during development. These tests are intended to verify that
 * the current match process based on classname works.
 */
@SuppressWarnings("ThrowableNotThrown")
public class TestInvoker extends Assert {

  /** Configuration to use for short retry intervals. */
  private static final Configuration FAST_RETRY_CONF;
  private static final ConnectTimeoutException
      HADOOP_CONNECTION_TIMEOUT_EX = new ConnectTimeoutException("hadoop");
  private static final Local.ConnectTimeoutException
      LOCAL_CONNECTION_TIMEOUT_EX
      = new Local.ConnectTimeoutException("local");
  private static final org.apache.http.conn.ConnectTimeoutException
      HTTP_CONNECTION_TIMEOUT_EX
      = new org.apache.http.conn.ConnectTimeoutException("apache");
  private static final SocketTimeoutException SOCKET_TIMEOUT_EX
      = new SocketTimeoutException("socket");

  /**
   * What retry limit to use.
   */
  private static final int ACTIVE_RETRY_LIMIT = RETRY_LIMIT_DEFAULT;

  /**
   * A retry count guaranteed to be out of range.
   */
  private static final int RETRIES_TOO_MANY = ACTIVE_RETRY_LIMIT + 10;

  /**
   * A count of retry attempts guaranteed to be within the permitted range.
   */
  public static final int SAFE_RETRY_COUNT = 5;

  static {
    FAST_RETRY_CONF = new Configuration();
    String interval = "10ms";
    FAST_RETRY_CONF.set(RETRY_INTERVAL, interval);
    FAST_RETRY_CONF.set(RETRY_THROTTLE_INTERVAL, interval);
    FAST_RETRY_CONF.setInt(RETRY_LIMIT, ACTIVE_RETRY_LIMIT);
    FAST_RETRY_CONF.setInt(RETRY_THROTTLE_LIMIT, ACTIVE_RETRY_LIMIT);
  }

  private static final S3ARetryPolicy RETRY_POLICY =
      new S3ARetryPolicy(FAST_RETRY_CONF);

  private int retryCount;
  private Invoker invoker = new Invoker(RETRY_POLICY,
      (text, e, retries, idempotent) -> retryCount++);
  private static final AmazonClientException CLIENT_TIMEOUT_EXCEPTION =
      new AmazonClientException(new Local.ConnectTimeoutException("timeout"));
  private static final AmazonServiceException BAD_REQUEST = serviceException(
      AWSBadRequestException.STATUS_CODE,
      "bad request");

  @Before
  public void setup() {
    resetCounters();
  }

  private static AmazonServiceException serviceException(int code,
      String text) {
    AmazonServiceException ex = new AmazonServiceException(text);
    ex.setStatusCode(code);
    return ex;
  }

  private static AmazonS3Exception createS3Exception(int code) {
    return createS3Exception(code, "", null);
  }

  private static AmazonS3Exception createS3Exception(int code,
      String message,
      Throwable inner) {
    AmazonS3Exception ex = new AmazonS3Exception(message);
    ex.setStatusCode(code);
    ex.initCause(inner);
    return ex;
  }

  protected <E extends Throwable> void verifyTranslated(
      int status,
      Class<E> expected) throws Exception {
    verifyTranslated(expected, createS3Exception(status));
  }

  private static <E extends Throwable> E verifyTranslated(Class<E> clazz,
      SdkBaseException exception) throws Exception {
    return verifyExceptionClass(clazz,
        translateException("test", "/", exception));
  }

  private void resetCounters() {
    retryCount = 0;
  }

  @Test
  public void test503isThrottled() throws Exception {
    verifyTranslated(503, AWSServiceThrottledException.class);
  }

  @Test
  public void testS3500isStatus500Exception() throws Exception {
    verifyTranslated(500, AWSStatus500Exception.class);
  }

  @Test
  public void test500isStatus500Exception() throws Exception {
    AmazonServiceException ex = new AmazonServiceException("");
    ex.setStatusCode(500);
    verifyTranslated(AWSStatus500Exception.class,
        ex);
  }

  @Test(expected = org.apache.hadoop.net.ConnectTimeoutException.class)
  public void testExtractConnectTimeoutException() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            new AmazonClientException(LOCAL_CONNECTION_TIMEOUT_EX)));
  }

  @Test(expected = SocketTimeoutException.class)
  public void testExtractSocketTimeoutException() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            new AmazonClientException(SOCKET_TIMEOUT_EX)));
  }

  /**
   * Make an assertion about a retry policy.
   * @param text text for error message
   * @param policy policy to check against
   * @param expected expected action
   * @param ex exception to analyze
   * @param retries number of times there has already been a retry
   * @param idempotent whether or not the operation is idempotent
   * @throws Exception if the retry policy raises it
   * @throws AssertionError if the returned action was not that expected.
   */
  private void assertRetryAction(String text,
      S3ARetryPolicy policy,
      RetryPolicy.RetryAction expected,
      Exception ex,
      int retries,
      boolean idempotent) throws Exception {
    RetryPolicy.RetryAction outcome = policy.shouldRetry(ex, retries, 0,
        idempotent);
    if (!expected.action.equals(outcome.action)) {
      throw new AssertionError(
          String.format(
              "%s Expected action %s from shouldRetry(%s, %s, %s), but got"
                  + " %s",
              text,
              expected, ex.toString(), retries, idempotent,
              outcome.action),
          ex);
    }
  }

  @Test
  public void testRetryThrottled() throws Throwable {
    S3ARetryPolicy policy = RETRY_POLICY;
    IOException ex = translateException("GET", "/", newThrottledException());

    assertRetryAction("Expected retry on first throttle",
        policy, RetryPolicy.RetryAction.RETRY,
        ex, 0, true);
    int retries = SAFE_RETRY_COUNT;
    assertRetryAction("Expected retry on repeated throttle",
        policy, RetryPolicy.RetryAction.RETRY,
        ex, retries, true);
    assertRetryAction("Expected retry on non-idempotent throttle",
        policy, RetryPolicy.RetryAction.RETRY,
        ex, retries, false);
  }

  @Test
  public void testRetryThrottledDDB() throws Throwable {
    assertRetryAction("Expected retry on connection timeout",
        RETRY_POLICY, RetryPolicy.RetryAction.RETRY,
        new ProvisionedThroughputExceededException("IOPs"), 1, false);
  }


  protected AmazonServiceException newThrottledException() {
    return serviceException(
        AWSServiceThrottledException.STATUS_CODE, "throttled");
  }

  /**
   * Repeatedly retry until a throttle eventually stops being raised.
   */
  @Test
  public void testRetryOnThrottle() throws Throwable {

    final AtomicInteger counter = new AtomicInteger(0);
    invoker.retry("test", null, false,
        () -> {
          if (counter.incrementAndGet() < 5) {
            throw newThrottledException();
          }
        });
  }

  /**
   * Non-idempotent operations fail on anything which isn't a throttle
   * or connectivity problem.
   */
  @Test(expected = AWSBadRequestException.class)
  public void testNoRetryOfBadRequestNonIdempotent() throws Throwable {
    invoker.retry("test", null, false,
        () -> {
          throw serviceException(400, "bad request");
        });
  }

  /**
   * AWS nested socket problems.
   */
  @Test
  public void testRetryAWSConnectivity() throws Throwable {
    final AtomicInteger counter = new AtomicInteger(0);
    invoker.retry("test", null, false,
        () -> {
          if (counter.incrementAndGet() < ACTIVE_RETRY_LIMIT) {
            throw CLIENT_TIMEOUT_EXCEPTION;
          }
        });
    assertEquals(ACTIVE_RETRY_LIMIT, counter.get());
  }

  /**
   * Repeatedly retry until eventually a bad request succeeds.
   */
  @Test
  public void testRetryBadRequestIdempotent() throws Throwable {
    final AtomicInteger counter = new AtomicInteger(0);
    final int attemptsBeforeSuccess = ACTIVE_RETRY_LIMIT;
    invoker.retry("test", null, true,
        () -> {
          if (counter.incrementAndGet() < attemptsBeforeSuccess) {
            throw BAD_REQUEST;
          }
        });
    assertEquals(attemptsBeforeSuccess, counter.get());
    assertEquals("retry count ", attemptsBeforeSuccess - 1, retryCount);
  }

  @Test
  public void testConnectionRetryPolicyIdempotent() throws Throwable {
    assertRetryAction("Expected retry on connection timeout",
        RETRY_POLICY, RetryPolicy.RetryAction.RETRY,
        HADOOP_CONNECTION_TIMEOUT_EX, 1, true);
    assertRetryAction("Expected connection timeout failure",
        RETRY_POLICY, RetryPolicy.RetryAction.FAIL,
        HADOOP_CONNECTION_TIMEOUT_EX, RETRIES_TOO_MANY, true);
  }

  /**
   * Even on a non-idempotent call, connection failures are considered
   * retryable.
   */
  @Test
  public void testConnectionRetryPolicyNonIdempotent() throws Throwable {
    assertRetryAction("Expected retry on connection timeout",
        RETRY_POLICY, RetryPolicy.RetryAction.RETRY,
        HADOOP_CONNECTION_TIMEOUT_EX, 1, false);
  }

  /**
   * Interrupted IOEs are not retryable.
   */
  @Test
  public void testInterruptedIOExceptionRetry() throws Throwable {
    assertRetryAction("Expected retry on connection timeout",
        RETRY_POLICY, RetryPolicy.RetryAction.FAIL,
        new InterruptedIOException("interrupted"), 1, false);
  }

  @Test
  public void testUnshadedConnectionTimeoutExceptionMatching()
      throws Throwable {
    // connection timeout exceptions are special, but as AWS shades
    // theirs, we need to string match them
    verifyTranslated(ConnectTimeoutException.class,
        new AmazonClientException(HTTP_CONNECTION_TIMEOUT_EX));
  }

  @Test
  public void testShadedConnectionTimeoutExceptionMatching() throws Throwable {
    // connection timeout exceptions are special, but as AWS shades
    // theirs, we need to string match them
    verifyTranslated(ConnectTimeoutException.class,
        new AmazonClientException(LOCAL_CONNECTION_TIMEOUT_EX));
  }

  @Test
  public void testShadedConnectionTimeoutExceptionNotMatching()
      throws Throwable {
    InterruptedIOException ex = verifyTranslated(InterruptedIOException.class,
        new AmazonClientException(new Local.NotAConnectTimeoutException()));
    if (ex instanceof ConnectTimeoutException) {
      throw ex;
    }
  }

  /**
   * Test that NPEs aren't retried. Also verify that the
   * catch counts are incremented, while the retry count isn't.
   */
  @Test
  public void testNPEsNotRetried() throws Throwable {
    assertRetryAction("Expected NPE trigger failure",
        RETRY_POLICY, RetryPolicy.RetryAction.FAIL,
        new NullPointerException("oops"), 1, true);
    // catch notification didn't see it
    assertEquals("retry count ", 0, retryCount);
  }

  /**
   * Container for the local exceptions, to help keep visible which
   * specific class of exception.
   */
  private static class Local {

    /**
     * A local exception with a name to match the expected one.
     */
    private static class ConnectTimeoutException
        extends InterruptedIOException {
      ConnectTimeoutException(String s) {
        super(s);
      }
    }

    /**
     * A local exception whose name should not match.
     */
    private static class NotAConnectTimeoutException
        extends InterruptedIOException {

    }

  }

  @Test
  public void testQuietlyVoid() {
    quietlyEval("", "",
        () -> {
        throw HADOOP_CONNECTION_TIMEOUT_EX;
      });
  }

  @Test
  public void testQuietlyEvalReturnValueSuccess() {
    assertOptionalEquals("quietly", 3,
        quietlyEval("", "", () -> 3));
  }

  @Test
  public void testQuietlyEvalReturnValueFail() {
    // use a variable so IDEs don't warn of numeric overflows
    int d = 0;
    assertOptionalUnset("quietly",
        quietlyEval("", "", () -> 3 / d));
  }

  @Test
  public void testDynamoDBThrottleConversion() throws Throwable {
    ProvisionedThroughputExceededException exceededException =
        new ProvisionedThroughputExceededException("iops");
    AWSServiceThrottledException ddb = verifyTranslated(
        AWSServiceThrottledException.class, exceededException);
    assertTrue(isThrottleException(exceededException));
    assertTrue(isThrottleException(ddb));
    assertRetryAction("Expected throttling retry",
        RETRY_POLICY,
        RetryPolicy.RetryAction.RETRY,
        ddb, SAFE_RETRY_COUNT, false);
    // and briefly attempt an operation
    CatchCallback catcher = new CatchCallback();
    AtomicBoolean invoked = new AtomicBoolean(false);
    invoker.retry("test", null, false, catcher,
        () -> {
          if (!invoked.getAndSet(true)) {
            throw exceededException;
          }
        });
    // to verify that the ex was translated by the time it
    // got to the callback
    verifyExceptionClass(AWSServiceThrottledException.class,
        catcher.lastException);
  }

  /**
   * Catch the exception and preserve it for later queries.
   */
  private static final class CatchCallback implements Retried {
    private IOException lastException;
    @Override
    public void onFailure(String text,
        IOException exception,
        int retries,
        boolean idempotent) {
      lastException = exception;
    }
  }

}
