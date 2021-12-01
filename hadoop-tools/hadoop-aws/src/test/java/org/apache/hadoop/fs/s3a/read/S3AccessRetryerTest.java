/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.read;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.common.ExceptionAsserts;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.Test;

import java.io.IOException;

public class S3AccessRetryerTest {


  @Test
  public void testArgChecks() {
    // Should not throw.
    new S3AccessRetryer();
    new S3AccessRetryer(10, 100);

    // Verify it throws correctly.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'baseDelay' must be a positive integer",
        () -> new S3AccessRetryer(-1, 2));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'baseDelay' must be a positive integer",
        () -> new S3AccessRetryer(0, 2));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'maxDelay' (1) must be greater than 'baseDelay' (2)",
        () -> new S3AccessRetryer(2, 1));
  }

  @Test
  public void testRetryableExceptions() {
    assertRetryableException(new IOException("foo"));

    AmazonS3Exception s3e = new AmazonS3Exception("foo");
    s3e.setErrorType(AmazonServiceException.ErrorType.Service);
    s3e.setStatusCode(503);
    assertRetryableException(s3e);
  }

  @Test
  public void testNonRetryableExceptions() {
    assertNonRetryableException(new Exception("foo"));

    AmazonS3Exception s3e = new AmazonS3Exception("foo");
    s3e.setErrorType(AmazonServiceException.ErrorType.Client);
    assertNonRetryableException(s3e);
  }

  private void assertRetryableException(Exception e) {
    S3AccessRetryer retryer = new S3AccessRetryer(1, 2);
    assertTrue(retryer.retry(e));
  }

  private void assertNonRetryableException(Exception e) {
    S3AccessRetryer retryer = new S3AccessRetryer(1, 2);
    assertFalse(retryer.retry(e));
  }
}
