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

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Invoker.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.verifyExceptionClass;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_400_BAD_REQUEST;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_500_INTERNAL_SERVER_ERROR;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_501_NOT_IMPLEMENTED;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_503_SERVICE_UNAVAILABLE;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_504_GATEWAY_TIMEOUT;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test the {@link Invoker} code and the associated {@link S3ARetryPolicy}.
 * <p>
 * Some of the tests look at how Connection Timeout Exceptions are processed.
 * Because of how the AWS libraries shade the classes, there have been some
 * regressions here during development. These tests are intended to verify that
 * the current match process based on classname works.
 * <p>
 * 500 errors may or may not be considered retriable; these tests validate
 * both configurations with different retry policies for each.
 */
public class TestInvoker extends HadoopTestBase {

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

  public static final String INTERNAL_ERROR_PLEASE_TRY_AGAIN =
      "We encountered an internal error. Please try again";

  /**
   * Retry configuration derived from {@link #FAST_RETRY_CONF} with 500 errors
   * never retried.
   */
  public static final Configuration RETRY_EXCEPT_500_ERRORS;

  static {
    FAST_RETRY_CONF = new Configuration();
    String interval = "10ms";
    FAST_RETRY_CONF.set(RETRY_INTERVAL, interval);
    FAST_RETRY_CONF.set(RETRY_THROTTLE_INTERVAL, interval);
    FAST_RETRY_CONF.setInt(RETRY_LIMIT, ACTIVE_RETRY_LIMIT);
    FAST_RETRY_CONF.setInt(RETRY_THROTTLE_LIMIT, ACTIVE_RETRY_LIMIT);
    FAST_RETRY_CONF.setBoolean(RETRY_HTTP_5XX_ERRORS, DEFAULT_RETRY_HTTP_5XX_ERRORS);
    RETRY_EXCEPT_500_ERRORS = new Configuration(FAST_RETRY_CONF);
    RETRY_EXCEPT_500_ERRORS.setBoolean(RETRY_HTTP_5XX_ERRORS, false);
  }

  /**
   * Retry policy with 500 error retry the default.
   */
  private static final S3ARetryPolicy RETRY_POLICY =
      new S3ARetryPolicy(FAST_RETRY_CONF);

  /**
   * Retry policyd with 500 errors never retried.
   */
  private static final S3ARetryPolicy RETRY_POLICY_NO_500_ERRORS =
      new S3ARetryPolicy(RETRY_EXCEPT_500_ERRORS);


  /**
   * Count of retries performed when invoking an operation which
   * failed.
   */
  private int retryCount;

  /**
   * Retry handler which increments {@link #retryCount}.
   */
  private final Retried retryHandler = (text, e, retries, idempotent) -> retryCount++;

  private final Invoker invoker = new Invoker(RETRY_POLICY, retryHandler);

  /**
   * AWS SDK exception wrapping a ConnectTimeoutException.
   */
  private static final SdkException CLIENT_TIMEOUT_EXCEPTION =
      SdkException.builder()
          .cause(new Local.ConnectTimeoutException("timeout"))
          .build();

  /**
   * AWS SDK 400 Bad Request exception.
   */
  private static final AwsServiceException BAD_REQUEST = serviceException(
      SC_400_BAD_REQUEST,
      "bad request");

  @Before
  public void setup() {
    resetCounters();
  }

  private static AwsServiceException serviceException(int code,
      String text) {
    return AwsServiceException.builder()
        .message(text)
        .statusCode(code)
        .build();
  }

  private static S3Exception createS3Exception(int code) {
    return createS3Exception(code, "", null);
  }

  private static S3Exception createS3Exception(int code,
      String message,
      Throwable inner) {
    return (S3Exception) S3Exception.builder()
        .message(message)
        .statusCode(code)
        .cause(inner)
        .build();
  }

  protected <E extends Throwable> void verifyTranslated(
      int status,
      Class<E> expected) throws Exception {
    verifyTranslated(expected, createS3Exception(status));
  }

  private static <E extends Throwable> E verifyTranslated(Class<E> clazz,
      SdkException exception) throws Exception {
    return verifyExceptionClass(clazz,
        translateException("test", "/", exception));
  }

  /**
   * jReset the retry count.
   */
  private void resetCounters() {
    retryCount = 0;
  }

  @Test
  public void test503isThrottled() throws Exception {
    verifyTranslated(SC_503_SERVICE_UNAVAILABLE, AWSServiceThrottledException.class);
  }

  @Test
  public void testS3500isStatus500Exception() throws Exception {
    verifyTranslated(SC_500_INTERNAL_SERVER_ERROR, AWSStatus500Exception.class);
  }

  /**
   * 500 error handling with the default options: the responses
   * trigger retry.
   */
  @Test
  public void test500ResponseHandling() throws Exception {

    // create a 500 SDK Exception;
    AwsServiceException ex = awsException(SC_500_INTERNAL_SERVER_ERROR,
        INTERNAL_ERROR_PLEASE_TRY_AGAIN);

    // translate this to a Hadoop IOE.
    AWSStatus500Exception ex500 =
        verifyTranslated(AWSStatus500Exception.class, ex);

    // the status code is preserved
    Assertions.assertThat(ex500.statusCode())
        .describedAs("status code of %s", ex)
        .isEqualTo(SC_500_INTERNAL_SERVER_ERROR);

    // the default retry policies reject this and fail
    assertRetryAction("Expected retry on 500 error",
        RETRY_POLICY, RetryPolicy.RetryAction.RETRY,
        ex, 0, true);

    Assertions.assertThat(invoker.getRetryPolicy()
        .shouldRetry(ex500, 1, 0, false).action)
        .describedAs("should retry %s", ex500)
        .isEqualTo(RetryPolicy.RetryAction.RETRY.action);
  }

  /**
   * Validate behavior on 500 errors when retry is disabled.
   */
  @Test
  public void test500ResponseHandlingRetryDisabled() throws Exception {
    // create a 500 SDK Exception;
    AwsServiceException ex = awsException(SC_500_INTERNAL_SERVER_ERROR,
        INTERNAL_ERROR_PLEASE_TRY_AGAIN);

    // translate this to a Hadoop IOE.
    AWSStatus500Exception ex500 =
        verifyTranslated(AWSStatus500Exception.class, ex);

    // the no 500 retry policies reject this and fail
    final Invoker failingInvoker = new Invoker(RETRY_POLICY_NO_500_ERRORS, retryHandler);
    assertRetryAction("Expected failure first throttle",
        RETRY_POLICY_NO_500_ERRORS, RetryPolicy.RetryAction.FAIL,
        ex, 0, true);
    Assertions.assertThat(failingInvoker.getRetryPolicy()
        .shouldRetry(ex500, 1, 0, false).action)
        .describedAs("should retry %s", ex500)
        .isEqualTo(RetryPolicy.RetryAction.FAIL.action);
  }
  /**
   * A 501 error is never retried.
   */
  @Test
  public void test501UnsupportedFeatureNoRetry() throws Throwable {

    AwsServiceException ex = awsException(501,
        "501 We encountered an internal error. Please try again");
    final AWSUnsupportedFeatureException ex501 =
        intercept(AWSUnsupportedFeatureException.class, "501", () ->
            invoker.retry("ex", null, true, () -> {
              throw ex;
            }));
    Assertions.assertThat(ex501.statusCode())
        .describedAs("status code of %s", ex)
        .isEqualTo(501);
    Assertions.assertThat(retryCount)
        .describedAs("retry count")
        .isEqualTo(0);
  }

  /**
   * Construct an S3Exception.
   * @param statusCode status code
   * @param message message
   * @return the exception
   */
  private static AwsServiceException awsException(final int statusCode, final String message) {
    return S3Exception.builder()
        .statusCode(statusCode)
        .message(message)
        .requestId("reqID")
        .extendedRequestId("extreqID")
        .build();
  }

  /**
   * Assert expected retry actions on 5xx responses when 5xx errors are disabled.
   */
  @Test
  public void test5xxRetriesDisabled() throws Throwable {
    final S3ARetryPolicy policy = RETRY_POLICY_NO_500_ERRORS;
    assertRetryAction("500", policy, RetryPolicy.RetryAction.FAIL,
        awsException(SC_500_INTERNAL_SERVER_ERROR, INTERNAL_ERROR_PLEASE_TRY_AGAIN), 1, true);
    assertRetryAction("501", policy, RetryPolicy.RetryAction.FAIL,
        awsException(SC_501_NOT_IMPLEMENTED, INTERNAL_ERROR_PLEASE_TRY_AGAIN), 1, true);
    assertRetryAction("510", policy, RetryPolicy.RetryAction.FAIL,
        awsException(510, INTERNAL_ERROR_PLEASE_TRY_AGAIN), 1, true);
    assertRetryAction("gateway", policy, RetryPolicy.RetryAction.RETRY,
        awsException(SC_504_GATEWAY_TIMEOUT, "gateway"), 1, true);
  }

  /**
   * Various 5xx exceptions when 5xx errors are enabled.
   */
  @Test
  public void test5xxRetriesEnabled() throws Throwable {
    final Configuration conf = new Configuration(FAST_RETRY_CONF);
    conf.setBoolean(RETRY_HTTP_5XX_ERRORS, true);
    final S3ARetryPolicy policy = new S3ARetryPolicy(conf);
    assertRetryAction("500", policy, RetryPolicy.RetryAction.RETRY,
        awsException(SC_500_INTERNAL_SERVER_ERROR, INTERNAL_ERROR_PLEASE_TRY_AGAIN), 1, true);
    assertRetryAction("501", policy, RetryPolicy.RetryAction.FAIL,
        awsException(SC_501_NOT_IMPLEMENTED, INTERNAL_ERROR_PLEASE_TRY_AGAIN), 1, true);
    assertRetryAction("510", policy, RetryPolicy.RetryAction.RETRY,
        awsException(510, INTERNAL_ERROR_PLEASE_TRY_AGAIN), 1, true);
    assertRetryAction("gateway", policy, RetryPolicy.RetryAction.RETRY,
        awsException(SC_504_GATEWAY_TIMEOUT, "gateway"), 1, true);
  }

  @Test
  public void testExceptionsWithTranslatableMessage() throws Exception {
    SdkException xmlParsing = SdkException.builder()
        .message(EOF_MESSAGE_IN_XML_PARSER)
        .build();
    SdkException differentLength = SdkException.builder()
        .message(EOF_READ_DIFFERENT_LENGTH)
        .build();

    verifyTranslated(EOFException.class, xmlParsing);
    verifyTranslated(EOFException.class, differentLength);
  }


  @Test
  public void testSdkDifferentLengthExceptionIsTranslatable() throws Throwable {
    final AtomicInteger counter = new AtomicInteger(0);
    invoker.retry("test", null, false, () -> {
      if (counter.incrementAndGet() < ACTIVE_RETRY_LIMIT) {
        throw SdkClientException.builder()
            .message(EOF_READ_DIFFERENT_LENGTH)
            .build();
      }
    });

    assertEquals(ACTIVE_RETRY_LIMIT, counter.get());
  }

  @Test
  public void testSdkXmlParsingExceptionIsTranslatable() throws Throwable {
    final AtomicInteger counter = new AtomicInteger(0);
    invoker.retry("test", null, false, () -> {
      if (counter.incrementAndGet() < ACTIVE_RETRY_LIMIT) {
        throw SdkClientException.builder()
            .message(EOF_MESSAGE_IN_XML_PARSER)
            .build();
      }
    });

    assertEquals(ACTIVE_RETRY_LIMIT, counter.get());
  }

  @Test(expected = org.apache.hadoop.net.ConnectTimeoutException.class)
  public void testExtractConnectTimeoutException() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            SdkException.builder()
                .cause(LOCAL_CONNECTION_TIMEOUT_EX)
                .build()));
  }

  @Test(expected = SocketTimeoutException.class)
  public void testExtractSocketTimeoutException() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            SdkException.builder()
                .cause(SOCKET_TIMEOUT_EX)
                .build()));
  }

  @Test(expected = org.apache.hadoop.net.ConnectTimeoutException.class)
  public void testExtractConnectTimeoutExceptionFromCompletionException() throws Throwable {
    throw extractException("", "",
        new CompletionException(
            SdkException.builder()
                .cause(LOCAL_CONNECTION_TIMEOUT_EX)
                .build()));
  }

  @Test(expected = SocketTimeoutException.class)
  public void testExtractSocketTimeoutExceptionFromCompletionException() throws Throwable {
    throw extractException("", "",
        new CompletionException(
            SdkException.builder()
                .cause(SOCKET_TIMEOUT_EX)
                .build()));
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
      RetryPolicy policy,
      RetryPolicy.RetryAction expected,
      Exception ex,
      int retries,
      boolean idempotent) throws Exception {
    RetryPolicy.RetryAction outcome = policy.shouldRetry(ex, retries, 0,
        idempotent);
    Assertions.assertThat(outcome.action)
        .describedAs("%s Expected action %s from shouldRetry(%s, %s, %s)",
                      text, expected, ex.toString(), retries, idempotent)
        .isEqualTo(expected.action);
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

  protected AwsServiceException newThrottledException() {
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
  @Test(expected = AWSBadRequestException.class)
  public void testRetryBadRequestNotIdempotent() throws Throwable {
    invoker.retry("test", null, false,
        () -> {
          throw BAD_REQUEST;
        });
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
        SdkException.builder()
            .cause(HTTP_CONNECTION_TIMEOUT_EX)
            .build());
  }

  @Test
  public void testShadedConnectionTimeoutExceptionMatching() throws Throwable {
    // connection timeout exceptions are special, but as AWS shades
    // theirs, we need to string match them
    verifyTranslated(ConnectTimeoutException.class,
        SdkException.builder()
            .cause(LOCAL_CONNECTION_TIMEOUT_EX)
            .build());
  }

  @Test
  public void testShadedConnectionTimeoutExceptionNotMatching()
      throws Throwable {
    InterruptedIOException ex = verifyTranslated(InterruptedIOException.class,
        SdkException.builder()
            .cause(new Local.NotAConnectTimeoutException())
            .build());
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
