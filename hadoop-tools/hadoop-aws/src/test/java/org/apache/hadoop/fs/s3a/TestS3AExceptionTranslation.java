/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.file.AccessDeniedException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.http.conn.ConnectTimeoutException;

import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT_DEFAULT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.verifyExceptionClass;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.junit.Assert.*;


/**
 * Unit test suite covering translation of AWS SDK exceptions to S3A exceptions,
 * an retry/recovery policies.
 */
public class TestS3AExceptionTranslation {

  @Test
  public void test301ContainsEndpoint() throws Exception {
    String bucket = "bucket.s3-us-west-2.amazonaws.com";
    AmazonS3Exception s3Exception = createS3Exception("wrong endpoint", 301,
        Collections.singletonMap(S3AUtils.ENDPOINT_KEY,
            bucket));
    AWSRedirectException ex = (AWSRedirectException)verifyTranslated(
        AWSRedirectException.class, s3Exception);
    assertStatusCode(301, ex);
    assertNotNull(ex.getMessage());

    assertContained(ex.getMessage(), bucket);
    assertContained(ex.getMessage(), ENDPOINT);
    assertContained(ex.toString(), bucket);
  }

  protected void assertContained(String text, String contained) {
    assertTrue("string \""+ contained + "\" not found in \"" + text + "\"",
        text != null && text.contains(contained));
  }

  protected void verifyTranslated(int status, Class expected) throws Exception {
    verifyTranslated(expected, createS3Exception(status));
  }


  @Test
  public void test400isBad() throws Exception {
    verifyTranslated(400, AWSBadRequestException.class);
  }

  @Test
  public void test401isNotPermittedFound() throws Exception {
    verifyTranslated(401, AccessDeniedException.class);
  }

  @Test
  public void test403isNotPermittedFound() throws Exception {
    verifyTranslated(403, AccessDeniedException.class);
  }

  @Test
  public void test404isNotFound() throws Exception {
    verifyTranslated(404, FileNotFoundException.class);
  }

  @Test
  public void test410isNotFound() throws Exception {
    verifyTranslated(410, FileNotFoundException.class);
  }

  @Test
  public void test416isEOF() throws Exception {
    verifyTranslated(416, EOFException.class);
  }
  @Test
  public void test503isThrottled() throws Exception {
    verifyTranslated(503, AWSServiceThrottledException.class);
  }

  @Test
  public void testGenericS3Exception() throws Exception {
    // S3 exception of no known type
    AWSS3IOException ex = (AWSS3IOException)verifyTranslated(
        AWSS3IOException.class,
        createS3Exception(451));
    assertStatusCode(451, ex);
  }

  @Test
  public void testGenericServiceS3Exception() throws Exception {
    // service exception of no known type
    AmazonServiceException ase = new AmazonServiceException("unwind");
    ase.setStatusCode(500);
    AWSServiceIOException ex = (AWSServiceIOException)verifyTranslated(
        AWSServiceIOException.class,
        ase);
    assertStatusCode(500, ex);
  }

  protected void assertStatusCode(int expected, AWSServiceIOException ex) {
    assertNotNull("Null exception", ex);
    if (expected != ex.getStatusCode()) {
      throw new AssertionError("Expected status code " + expected
          + "but got " + ex.getStatusCode(),
          ex);
    }
  }

  @Test
  public void testGenericClientException() throws Exception {
    // Generic Amazon exception
    verifyTranslated(AWSClientIOException.class,
        new AmazonClientException(""));
  }

  private static AmazonS3Exception createS3Exception(int code) {
    return createS3Exception("", code, null);
  }

  private static AmazonS3Exception createS3Exception(String message, int code,
      Map<String, String> additionalDetails) {
    AmazonS3Exception source = new AmazonS3Exception(message);
    source.setStatusCode(code);
    source.setAdditionalDetails(additionalDetails);
    return source;
  }

  private static Exception verifyTranslated(Class clazz,
      AmazonClientException exception) throws Exception {
    return verifyExceptionClass(clazz,
        translateException("test", "/", exception));
  }

  private void assertContainsInterrupted(boolean expected, Throwable thrown)
      throws Throwable {
    boolean wasInterrupted = containsInterruptedException(thrown) != null;
    if (wasInterrupted != expected) {
      throw thrown;
    }
  }

  @Test
  public void testInterruptExceptionDetecting() throws Throwable {
    InterruptedException interrupted = new InterruptedException("irq");
    assertContainsInterrupted(true, interrupted);
    IOException ioe = new IOException("ioe");
    assertContainsInterrupted(false, ioe);
    assertContainsInterrupted(true, ioe.initCause(interrupted));
    assertContainsInterrupted(true,
        new InterruptedIOException("ioirq"));
  }

  @Test(expected = InterruptedIOException.class)
  public void testExtractInterrupted() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            new AmazonClientException(
                new InterruptedException(""))));
  }

  @Test(expected = InterruptedIOException.class)
  public void testExtractInterruptedIO() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            new AmazonClientException(
              new InterruptedIOException(""))));
  }

  @Test(expected = ConnectTimeoutException.class)
  public void testExtractConnectTimeoutException() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            new AmazonClientException(
              new ConnectTimeoutException(""))));
  }

  @Test(expected = SocketTimeoutException.class)
  public void testExtractSocketTimeoutException() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            new AmazonClientException(
              new SocketTimeoutException(""))));
  }

  private AmazonServiceException serviceException(int code, String text) {
    AmazonServiceException ex = new AmazonServiceException(text);
    ex.setStatusCode(code);
    return ex;
  }


  private void assertRetryAction(String text,
      RetryPolicy policy,
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
              outcome.action, ex));
    }
  }

  @Test
  public void testRetryThrottled() throws Throwable {
    S3ARetryPolicy policy = new S3ARetryPolicy(new Configuration());
    AmazonServiceException se = newThrottledException();
    IOException ex = translateException("GET", "/", se);

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

  protected AmazonServiceException newThrottledException() {
    return serviceException(
        AWSServiceThrottledException.STATUS_CODE, "throttled");
  }

  /**
   * Repeatedly retry until a throttle fails.
   * @throws Throwable
   */
  @Test
  public void testS3LambdaRetryOnThrottle() throws Throwable {
    S3ALambda lambda = new S3ALambda(
        new S3ARetryPolicy(new Configuration()));
    final AtomicInteger counter = new AtomicInteger(0);
    lambda.retry("test", null, false,
      () -> {
        if (counter.incrementAndGet() < 5) {
          throw newThrottledException();
        }
      });
  }

  /**
   * Non-idempotent operations fail on anything which isn't a throttle.
   * @throws Throwable
   */
  @Test(expected = AWSBadRequestException.class)
  public void testS3LambdaRetryNonIdempotent() throws Throwable {
    S3ALambda lambda = new S3ALambda(
        new S3ARetryPolicy(new Configuration()));
    lambda.retry("test", null, false,
      () -> {
          throw serviceException(400, "bad request");
      });
  }

  /**
   * Repeatedly retry until a throttle fails.
   * @throws Throwable
   */
  @Test
  public void testS3LambdaRetryIdempotent() throws Throwable {
    S3ALambda lambda = new S3ALambda(
        new S3ARetryPolicy(new Configuration()));
    final AtomicInteger counter = new AtomicInteger(0);
    lambda.retry("test", null, true,
      () -> {
        if (counter.incrementAndGet() < RETRY_LIMIT_DEFAULT) {
          throw serviceException(AWSBadRequestException.STATUS_CODE,
              "bad request");
        }
      });
  }

}
