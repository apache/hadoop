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
import static org.junit.Assert.*;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.nio.file.AccessDeniedException;
import java.util.Collections;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import org.junit.Test;

/**
 * Unit test suite covering translation of AWS SDK exceptions to S3A exceptions.
 */
public class TestS3AExceptionTranslation {

  @Test
  public void test301ContainsEndpoint() throws Exception {
    AmazonS3Exception s3Exception = createS3Exception("wrong endpoint", 301,
        Collections.singletonMap(S3AUtils.ENDPOINT_KEY,
            "bucket.s3-us-west-2.amazonaws.com"));
    AWSS3IOException ex = (AWSS3IOException)verifyTranslated(
        AWSS3IOException.class, s3Exception);
    assertEquals(301, ex.getStatusCode());
    assertNotNull(ex.getMessage());
    assertTrue(ex.getMessage().contains("bucket.s3-us-west-2.amazonaws.com"));
    assertTrue(ex.getMessage().contains(ENDPOINT));
  }

  @Test
  public void test401isNotPermittedFound() throws Exception {
    verifyTranslated(AccessDeniedException.class,
        createS3Exception(401));
  }

  @Test
  public void test403isNotPermittedFound() throws Exception {
    verifyTranslated(AccessDeniedException.class,
        createS3Exception(403));
  }

  @Test
  public void test404isNotFound() throws Exception {
    verifyTranslated(FileNotFoundException.class, createS3Exception(404));
  }

  @Test
  public void test410isNotFound() throws Exception {
    verifyTranslated(FileNotFoundException.class, createS3Exception(410));
  }

  @Test
  public void test416isEOF() throws Exception {
    verifyTranslated(EOFException.class, createS3Exception(416));
  }

  @Test
  public void testGenericS3Exception() throws Exception {
    // S3 exception of no known type
    AWSS3IOException ex = (AWSS3IOException)verifyTranslated(
        AWSS3IOException.class,
        createS3Exception(451));
    assertEquals(451, ex.getStatusCode());
  }

  @Test
  public void testGenericServiceS3Exception() throws Exception {
    // service exception of no known type
    AmazonServiceException ase = new AmazonServiceException("unwind");
    ase.setStatusCode(500);
    AWSServiceIOException ex = (AWSServiceIOException)verifyTranslated(
        AWSServiceIOException.class,
        ase);
    assertEquals(500, ex.getStatusCode());
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
}
