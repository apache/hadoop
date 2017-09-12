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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSBadRequestException;
import org.apache.hadoop.fs.s3a.AWSS3IOException;
import org.apache.hadoop.fs.s3a.AWSServiceIOException;
import org.apache.hadoop.fs.s3a.AWSServiceThrottledException;
import org.apache.hadoop.fs.s3a.S3ALambda;
import org.apache.hadoop.fs.s3a.S3ARetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.ConnectTimeoutException;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.verifyExceptionClass;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;


/**
 * Test the {@link S3ALambda} code
 */
@SuppressWarnings("ThrowableNotThrown")
public class TestS3ALambda extends Assert {

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
  private static final int RETRIES_TOO_MANY = 10;

  static {
    FAST_RETRY_CONF = new Configuration();
    String interval = "10ms";
    FAST_RETRY_CONF.set(RETRY_INTERVAL, interval);
    FAST_RETRY_CONF.set(RETRY_THROTTLE_INTERVAL, interval);
  }

  private static final S3ARetryPolicy RETRY_POLICY =
      new S3ARetryPolicy(FAST_RETRY_CONF);

  private int retryCount;
  private S3ALambda lambda = new S3ALambda(RETRY_POLICY,
      (Exception e, int retries, boolean idempotent) -> retryCount ++,
      S3ALambda.CATCH_LOG);
  private static final AmazonClientException CLIENT_TIMEOUT_EXCEPTION = new AmazonClientException(
      new Local.ConnectTimeoutException("timeout"));
  private static final AmazonServiceException BAD_REQUEST = serviceException(
      AWSBadRequestException.STATUS_CODE,
      "bad request");

  private static AmazonServiceException serviceException(int code, String text) {
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
      AmazonClientException exception) throws Exception {
    return verifyExceptionClass(clazz,
        translateException("test", "/", exception));
  }

  @Test
  public void test503isThrottled() throws Exception {
    verifyTranslated(503, AWSServiceThrottledException.class);
  }

  @Test
  public void testS3500isServiceException() throws Exception {
    verifyTranslated(500, AWSS3IOException.class);
  }

  @Test
  public void test500isServiceException() throws Exception {
    AmazonServiceException ex = new AmazonServiceException("");
    ex.setStatusCode(500);
    verifyTranslated(AWSServiceIOException.class,
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
   * Make an assertion about a retry policy
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
    assertRetryAction("Expected retry on repeated throttle",
        policy, RetryPolicy.RetryAction.RETRY,
        ex, 5, true);
    assertRetryAction("Expected retry on non-idempotent throttle",
        policy, RetryPolicy.RetryAction.RETRY,
        ex, 5, false);
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
  public void testS3LambdaRetryOnThrottle() throws Throwable {

    final AtomicInteger counter = new AtomicInteger(0);
    lambda.retry("test", null, false,
        () -> {
          if (counter.incrementAndGet() < 5) {
            throw newThrottledException();
          }
        });
  }

  /**
   * Non-idempotent operations fail on anything which isn't a throttle
   * or connectivity problem
   */
  @Test(expected = AWSBadRequestException.class)
  public void testS3LambdaRetryNonIdempotent() throws Throwable {
    lambda.retry("test", null, false,
        () -> {
          throw serviceException(400, "bad request");
        });
  }

  /**
   * AWS nested socket problems
   */
  @Test
  public void testS3LambdaRetryAWSConnectivity() throws Throwable {
    final AtomicInteger counter = new AtomicInteger(0);
    lambda.retry("test", null, false,
        () -> {
          if (counter.incrementAndGet() < RETRY_LIMIT_DEFAULT) {
            throw CLIENT_TIMEOUT_EXCEPTION;
          }
        });
    assertEquals(RETRY_LIMIT_DEFAULT, counter.get());
  }

  /**
   * Repeatedly retry until eventually a bad request succeeds.
   */
  @Test
  public void testS3LambdaRetryBadRequestIdempotent() throws Throwable {
    final AtomicInteger counter = new AtomicInteger(0);
    lambda.retry("test", null, true,
        () -> {
          if (counter.incrementAndGet() < RETRY_LIMIT_DEFAULT) {
            throw BAD_REQUEST;
          }
        });
    assertEquals(RETRY_LIMIT_DEFAULT, counter.get());
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
   * Interrupted IOEs are not retryable
   */
  @Test
  public void testInterruptedIOExceptionRetry() throws Throwable {
    assertRetryAction("Expected retry on connection timeout",
        RETRY_POLICY, RetryPolicy.RetryAction.FAIL,
        new InterruptedIOException(), 1, false);
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

  @Test
  public void testNPEsNotRetried() throws Throwable {
    assertRetryAction("Expected NPE trigger failure",
        RETRY_POLICY, RetryPolicy.RetryAction.FAIL,
        new NullPointerException(), 1, true);
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
      public ConnectTimeoutException(String s) {
        super(s);
      }
    }

    /**
     * A local exception whose name should not match
     */
    private static class NotAConnectTimeoutException
        extends InterruptedIOException {

    }

  }

  @Test
  public void testQuietlyVoid() throws Throwable {
    Optional<Object> quietly = lambda.quietlyEval("", "", () -> {
      throw HADOOP_CONNECTION_TIMEOUT_EX;
    });
  }

  @Test
  public void testQuietlyEvalReturnValueSuccess() throws Throwable {
    assertOptionalEquals("quietly", 3,
        lambda.quietlyEval("", "", () -> 3));
  }

  @Test
  public void testQuietlyEvalReturnValueFail() throws Throwable {
    assertOptionalUnset("quietly",
        lambda.quietlyEval("", "", () -> 3 / 0));
  }


}
