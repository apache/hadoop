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

import static org.apache.hadoop.fs.s3a.AWSCredentialProviderList.maybeTranslateCredentialException;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.sdkClientException;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.verifyExceptionClass;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.audit.AuditIntegration.maybeTranslateAuditException;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.maybeExtractChannelException;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.verifyCause;
import static org.junit.Assert.*;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.AccessDeniedException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.api.UnsupportedRequestException;
import org.apache.hadoop.fs.s3a.audit.AuditFailureException;
import org.apache.hadoop.fs.s3a.audit.AuditOperationRejectedException;
import org.apache.hadoop.fs.s3a.impl.ErrorTranslation;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.http.NoHttpResponseException;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;

/**
 * Unit test suite covering translation of AWS/network exceptions to S3A exceptions,
 * and retry/recovery policies.
 */
@SuppressWarnings("ThrowableNotThrown")
public class TestS3AExceptionTranslation extends AbstractHadoopTestBase {

  public static final String WFOPENSSL_0035_STREAM_IS_CLOSED =
      "Unable to execute HTTP request: "
          + ErrorTranslation.OPENSSL_STREAM_CLOSED
          + " Stream is closed";

  /**
   * Retry policy to use in tests.
   */
  private S3ARetryPolicy retryPolicy;

  @Before
  public void setup() {
    retryPolicy = new S3ARetryPolicy(new Configuration(false));
  }

  @Test
  public void test301ContainsRegion() throws Exception {
    String region = "us-west-1";

    AwsErrorDetails redirectError = AwsErrorDetails.builder()
        .sdkHttpResponse(
            SdkHttpResponse.builder().putHeader(BUCKET_REGION_HEADER, region).build())
        .build();

    S3Exception s3Exception = createS3Exception("wrong region",
        SC_301_MOVED_PERMANENTLY,
        redirectError);
    AWSRedirectException ex = verifyTranslated(
        AWSRedirectException.class, s3Exception);
    assertStatusCode(SC_301_MOVED_PERMANENTLY, ex);
    assertNotNull(ex.getMessage());

    assertContained(ex.getMessage(), region);
    assertContained(ex.getMessage(), AWS_REGION);
    assertExceptionContains(AWS_REGION, ex, "region");
    assertExceptionContains(region, ex, "region name");
  }

  protected void assertContained(String text, String contained) {
    assertTrue("string \""+ contained + "\" not found in \"" + text + "\"",
        text != null && text.contains(contained));
  }

  protected <E extends Throwable> E verifyTranslated(
      int status,
      Class<E> expected) throws Exception {
    return verifyTranslated(expected, createS3Exception(status));
  }

  @Test
  public void test400isBad() throws Exception {
    verifyTranslated(SC_400_BAD_REQUEST, AWSBadRequestException.class);
  }

  @Test
  public void test401isNotPermittedFound() throws Exception {
    verifyTranslated(SC_401_UNAUTHORIZED, AccessDeniedException.class);
  }

  @Test
  public void test403isNotPermittedFound() throws Exception {
    verifyTranslated(SC_403_FORBIDDEN, AccessDeniedException.class);
  }

  /**
   * 404 defaults to FileNotFound.
   */
  @Test
  public void test404isNotFound() throws Exception {
    verifyTranslated(SC_404_NOT_FOUND, FileNotFoundException.class);
  }

  /**
   * 404 + NoSuchBucket == Unknown bucket.
   */
  @Test
  public void testUnknownBucketException() throws Exception {
    S3Exception ex404 = createS3Exception(b -> b
        .statusCode(SC_404_NOT_FOUND)
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode(ErrorTranslation.AwsErrorCodes.E_NO_SUCH_BUCKET)
            .build()));
    verifyTranslated(
        UnknownStoreException.class,
        ex404);
  }

  @Test
  public void test410isNotFound() throws Exception {
    verifyTranslated(SC_410_GONE, FileNotFoundException.class);
  }

  @Test
  public void test416isEOF() throws Exception {

    // 416 maps the the subclass of EOFException
    final IOException ex = verifyTranslated(SC_416_RANGE_NOT_SATISFIABLE,
            RangeNotSatisfiableEOFException.class);
    Assertions.assertThat(ex)
        .isInstanceOf(EOFException.class);
  }

  @Test
  public void testGenericS3Exception() throws Exception {
    // S3 exception of no known type
    AWSS3IOException ex = verifyTranslated(
        AWSS3IOException.class,
        createS3Exception(451));
    assertStatusCode(451, ex);
  }

  @Test
  public void testGenericServiceS3Exception() throws Exception {
    // service exception of no known type
    AwsServiceException ase = AwsServiceException.builder()
        .message("unwind")
        .statusCode(SC_500_INTERNAL_SERVER_ERROR)
        .build();
    AWSServiceIOException ex = verifyTranslated(
        AWSStatus500Exception.class,
        ase);
    assertStatusCode(SC_500_INTERNAL_SERVER_ERROR, ex);
  }

  protected void assertStatusCode(int expected, AWSServiceIOException ex) {
    assertNotNull("Null exception", ex);
    if (expected != ex.statusCode()) {
      throw new AssertionError("Expected status code " + expected
          + "but got " + ex.statusCode(),
          ex);
    }
  }

  @Test
  public void testGenericClientException() throws Exception {
    // Generic Amazon exception
    verifyTranslated(AWSClientIOException.class,
        SdkException.builder().message("").build());
  }

  private static S3Exception createS3Exception(
      Consumer<S3Exception.Builder> consumer) {
    S3Exception.Builder builder = S3Exception.builder()
        .awsErrorDetails(AwsErrorDetails.builder()
            .build());
    consumer.accept(builder);
    return (S3Exception) builder.build();
  }

  private static S3Exception createS3Exception(int code) {
    return createS3Exception(b -> b.message("").statusCode(code));
  }

  private static S3Exception createS3Exception(String message, int code,
      AwsErrorDetails additionalDetails) {

    S3Exception source = (S3Exception) S3Exception.builder()
        .message(message)
        .statusCode(code)
        .awsErrorDetails(additionalDetails)
        .build();
    return source;
  }

  private static <E extends Throwable> E verifyTranslated(Class<E> clazz,
      SdkException exception) throws Exception {
    // Verifying that the translated exception have the correct error message.
    IOException ioe = translateException("test", "/", exception);
    assertExceptionContains(exception.getMessage(), ioe,
        "Translated Exception should contain the error message of the "
            + "actual exception");
    return verifyExceptionClass(clazz, ioe);
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
            SdkException.builder()
                .cause(new InterruptedException(""))
                .build()));
  }

  @Test(expected = InterruptedIOException.class)
  public void testExtractInterruptedIO() throws Throwable {
    throw extractException("", "",
        new ExecutionException(
            SdkException.builder()
                .cause(new InterruptedIOException(""))
                .build()));
  }

  @Test
  public void testTranslateCredentialException() throws Throwable {
    verifyExceptionClass(AccessDeniedException.class,
        maybeTranslateCredentialException("/",
            new CredentialInitializationException("Credential initialization failed")));
  }

  @Test
  public void testTranslateNestedCredentialException() throws Throwable {
    final AccessDeniedException ex =
        verifyExceptionClass(AccessDeniedException.class,
            maybeTranslateCredentialException("/",
                sdkClientException("",
                    new CredentialInitializationException("Credential initialization failed"))));
    // unwrap and verify that the initial client exception has been stripped
    final Throwable cause = ex.getCause();
    Assertions.assertThat(cause)
        .isInstanceOf(CredentialInitializationException.class);
    CredentialInitializationException cie = (CredentialInitializationException) cause;
    Assertions.assertThat(cie.retryable())
        .describedAs("Retryable flag")
        .isFalse();
  }


  @Test
  public void testTranslateNonCredentialException() throws Throwable {
    Assertions.assertThat(
            maybeTranslateCredentialException("/",
                sdkClientException("not a credential exception", null)))
        .isNull();
    Assertions.assertThat(
            maybeTranslateCredentialException("/",
                sdkClientException("", sdkClientException("not a credential exception", null))))
        .isNull();
  }

  @Test
  public void testTranslateAuditException() throws Throwable {
    verifyExceptionClass(AccessDeniedException.class,
        maybeTranslateAuditException("/",
            new AuditFailureException("failed")));
  }

  @Test
  public void testTranslateNestedAuditException() throws Throwable {
    verifyExceptionClass(AccessDeniedException.class,
        maybeTranslateAuditException("/",
            sdkClientException("", new AuditFailureException("failed"))));
  }


  @Test
  public void testTranslateNestedAuditRejectedException() throws Throwable {
    final UnsupportedRequestException ex =
        verifyExceptionClass(UnsupportedRequestException.class,
            maybeTranslateAuditException("/",
                sdkClientException("", new AuditOperationRejectedException("rejected"))));
    Assertions.assertThat(ex.getCause())
        .isInstanceOf(AuditOperationRejectedException.class);
  }

  @Test
  public void testTranslateNonAuditException() throws Throwable {
    Assertions.assertThat(
            maybeTranslateAuditException("/",
                sdkClientException("not an audit exception", null)))
        .isNull();
    Assertions.assertThat(
            maybeTranslateAuditException("/",
                sdkClientException("", sdkClientException("not an audit exception", null))))
        .isNull();
  }

  /**
   * 504 gateway timeout is translated to a {@link AWSApiCallTimeoutException}.
   */
  @Test
  public void test504ToTimeout() throws Throwable {
    AWSApiCallTimeoutException ex =
        verifyExceptionClass(AWSApiCallTimeoutException.class,
        translateException("test", "/", createS3Exception(504)));
    verifyCause(S3Exception.class, ex);
  }

  /**
   * SDK ApiCallTimeoutException is translated to a
   * {@link AWSApiCallTimeoutException}.
   */
  @Test
  public void testApiCallTimeoutExceptionToTimeout() throws Throwable {
    AWSApiCallTimeoutException ex =
        verifyExceptionClass(AWSApiCallTimeoutException.class,
        translateException("test", "/",
            ApiCallTimeoutException.builder()
                .message("timeout")
                .build()));
    verifyCause(ApiCallTimeoutException.class, ex);
  }

  /**
   * SDK ApiCallAttemptTimeoutException is translated to a
   * {@link AWSApiCallTimeoutException}.
   */
  @Test
  public void testApiCallAttemptTimeoutExceptionToTimeout() throws Throwable {
    AWSApiCallTimeoutException ex =
        verifyExceptionClass(AWSApiCallTimeoutException.class,
        translateException("test", "/",
            ApiCallAttemptTimeoutException.builder()
                .message("timeout")
                .build()));
    verifyCause(ApiCallAttemptTimeoutException.class, ex);

    // and confirm these timeouts are retried.
    assertRetried(ex);
  }

  @Test
  public void testChannelExtraction() throws Throwable {
    verifyExceptionClass(HttpChannelEOFException.class,
        maybeExtractChannelException("", "/",
            new NoHttpResponseException("no response")));
  }

  @Test
  public void testShadedChannelExtraction() throws Throwable {
    verifyExceptionClass(HttpChannelEOFException.class,
        maybeExtractChannelException("", "/",
            shadedNoHttpResponse()));
  }

  @Test
  public void testOpenSSLErrorChannelExtraction() throws Throwable {
    verifyExceptionClass(HttpChannelEOFException.class,
        maybeExtractChannelException("", "/",
            sdkClientException(WFOPENSSL_0035_STREAM_IS_CLOSED, null)));
  }

  /**
   * Test handling of the unshaded HTTP client exception.
   */
  @Test
  public void testRawNoHttpResponseExceptionRetry() throws Throwable {
    assertRetried(
        verifyExceptionClass(HttpChannelEOFException.class,
            translateException("test", "/",
                sdkClientException(new NoHttpResponseException("no response")))));
  }

  /**
   * Test handling of the shaded HTTP client exception.
   */
  @Test
  public void testShadedNoHttpResponseExceptionRetry() throws Throwable {
    assertRetried(
        verifyExceptionClass(HttpChannelEOFException.class,
            translateException("test", "/",
                sdkClientException(shadedNoHttpResponse()))));
  }

  @Test
  public void testOpenSSLErrorRetry() throws Throwable {
    assertRetried(
        verifyExceptionClass(HttpChannelEOFException.class,
            translateException("test", "/",
                sdkClientException(WFOPENSSL_0035_STREAM_IS_CLOSED, null))));
  }

  /**
   * Create a shaded NoHttpResponseException.
   * @return an exception.
   */
  private static Exception shadedNoHttpResponse() {
    return new software.amazon.awssdk.thirdparty.org.apache.http.NoHttpResponseException("shaded");
  }

  /**
   * Assert that an exception is retried.
   * @param ex exception
   * @throws Exception failure during retry policy evaluation.
   */
  private void assertRetried(final Exception ex) throws Exception {
    assertRetryOutcome(ex, RetryPolicy.RetryAction.RetryDecision.RETRY);
  }

  /**
   * Assert that the retry policy is as expected for a given exception.
   * @param ex exception
   * @param decision expected decision
   * @throws Exception failure during retry policy evaluation.
   */
  private void assertRetryOutcome(
      final Exception ex,
      final RetryPolicy.RetryAction.RetryDecision decision) throws Exception {
    Assertions.assertThat(retryPolicy.shouldRetry(ex, 0, 0, true).action)
        .describedAs("retry policy for exception %s", ex)
        .isEqualTo(decision);
  }

}
