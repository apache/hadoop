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

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_404;
import static org.junit.Assert.*;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.file.AccessDeniedException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import org.junit.Test;

import org.apache.hadoop.fs.s3a.impl.ErrorTranslation;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;

/**
 * Unit test suite covering translation of AWS SDK exceptions to S3A exceptions,
 * and retry/recovery policies.
 */
@SuppressWarnings("ThrowableNotThrown")
public class TestS3AExceptionTranslation {

  private static final org.apache.http.conn.ConnectTimeoutException
      HTTP_CONNECTION_TIMEOUT_EX
      = new org.apache.http.conn.ConnectTimeoutException("apache");
  private static final SocketTimeoutException SOCKET_TIMEOUT_EX
      = new SocketTimeoutException("socket");

  @Test
  public void test301ContainsEndpoint() throws Exception {
    String bucket = "bucket.s3-us-west-2.amazonaws.com";
    int sc301 = 301;
    AmazonS3Exception s3Exception = createS3Exception("wrong endpoint", sc301,
        Collections.singletonMap(S3AUtils.ENDPOINT_KEY,
            bucket));
    AWSRedirectException ex = verifyTranslated(
        AWSRedirectException.class, s3Exception);
    assertStatusCode(sc301, ex);
    assertNotNull(ex.getMessage());

    assertContained(ex.getMessage(), bucket);
    assertContained(ex.getMessage(), ENDPOINT);
    assertExceptionContains(ENDPOINT, ex, "endpoint");
    assertExceptionContains(bucket, ex, "bucket name");
  }

  protected void assertContained(String text, String contained) {
    assertTrue("string \""+ contained + "\" not found in \"" + text + "\"",
        text != null && text.contains(contained));
  }

  protected <E extends Throwable> void verifyTranslated(
      int status,
      Class<E> expected) throws Exception {
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

  /**
   * 404 defaults to FileNotFound.
   */
  @Test
  public void test404isNotFound() throws Exception {
    verifyTranslated(SC_404, FileNotFoundException.class);
  }

  /**
   * 404 + NoSuchBucket == Unknown bucket.
   */
  @Test
  public void testUnknownBucketException() throws Exception {
    AmazonS3Exception ex404 = createS3Exception(SC_404);
    ex404.setErrorCode(ErrorTranslation.AwsErrorCodes.E_NO_SUCH_BUCKET);
    verifyTranslated(
        UnknownStoreException.class,
        ex404);
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
    AmazonServiceException ase = new AmazonServiceException("unwind");
    ase.setStatusCode(500);
    AWSServiceIOException ex = verifyTranslated(
        AWSStatus500Exception.class,
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

  private static <E extends Throwable> E verifyTranslated(Class<E> clazz,
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

}
