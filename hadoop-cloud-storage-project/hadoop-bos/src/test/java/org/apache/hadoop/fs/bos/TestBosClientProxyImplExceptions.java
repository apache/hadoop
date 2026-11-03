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

package org.apache.hadoop.fs.bos;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.GetObjectMetadataRequest;
import org.apache.hadoop.fs.bos.exceptions.BandwidthLimitException;
import org.apache.hadoop.fs.bos.exceptions.BosException;
import org.apache.hadoop.fs.bos.exceptions.BosHotObjectException;
import org.apache.hadoop.fs.bos.exceptions.BosServerException;
import org.apache.hadoop.fs.bos.exceptions.SessionTokenExpireException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Tests how {@link BosClientProxyImpl} translates BOS service errors
 * into Hadoop-side exceptions. The BOS SDK client is mocked, so no
 * endpoint or credentials are needed.
 */
public class TestBosClientProxyImplExceptions {

  private BosClient sdkClient;
  private BosClientProxyImpl proxy;

  @BeforeEach
  public void setUp() throws Exception {
    sdkClient = mock(BosClient.class);
    proxy = new BosClientProxyImpl();
    Field field = BosClientProxyImpl.class.getDeclaredField("bosClient");
    field.setAccessible(true);
    field.set(proxy, sdkClient);
  }

  private static BceServiceException serviceError(int status,
      String errorCode) {
    BceServiceException e = new BceServiceException("test error");
    e.setStatusCode(status);
    e.setErrorCode(errorCode);
    return e;
  }

  private void failAbortWith(BceServiceException e) {
    doThrow(e).when(sdkClient).abortMultipartUpload(any(
        com.baidubce.services.bos.model.AbortMultipartUploadRequest.class));
  }

  @Test
  public void testInvalidArgumentIsNotRetriedAsExpiredToken() {
    failAbortWith(serviceError(400, "InvalidArgument"));
    assertThrows(BosException.class,
        () -> proxy.abortMultipartUpload("bucket", "key", "id"));
  }

  @Test
  public void testInvalidArgumentWithPaddedCodeIsBosException() {
    failAbortWith(serviceError(400, " InvalidArgument "));
    assertThrows(BosException.class,
        () -> proxy.abortMultipartUpload("bucket", "key", "id"));
  }

  @Test
  public void testNullErrorCodeMeansSessionExpired() {
    failAbortWith(serviceError(400, null));
    assertThrows(SessionTokenExpireException.class,
        () -> proxy.abortMultipartUpload("bucket", "key", "id"));
  }

  @Test
  public void testInvalidSessionTokenMeansSessionExpired() {
    failAbortWith(serviceError(400, "InvalidSessionToken"));
    assertThrows(SessionTokenExpireException.class,
        () -> proxy.abortMultipartUpload("bucket", "key", "id"));
  }

  @Test
  public void testOtherBadRequestIsNotRetriedAsExpiredToken() {
    failAbortWith(serviceError(400, "SomethingElse"));
    assertThrows(BosException.class,
        () -> proxy.abortMultipartUpload("bucket", "key", "id"));
  }

  @Test
  public void testRateLimitWithoutCode() {
    failAbortWith(serviceError(429, null));
    assertThrows(BandwidthLimitException.class,
        () -> proxy.abortMultipartUpload("bucket", "key", "id"));
  }

  @Test
  public void testRateLimitOnHotObject() {
    failAbortWith(serviceError(429, "RequestRateLimitExceeded"));
    assertThrows(BosHotObjectException.class,
        () -> proxy.abortMultipartUpload("bucket", "key", "id"));
  }

  @Test
  public void testServerErrorIsBosServerException() {
    failAbortWith(serviceError(503, "ServiceUnavailable"));
    assertThrows(BosServerException.class,
        () -> proxy.abortMultipartUpload("bucket", "key", "id"));
  }

  @Test
  public void testMissingKeyIsFileNotFound() {
    doThrow(serviceError(404, "NoSuchKey")).when(sdkClient)
        .getObjectMetadata(any(GetObjectMetadataRequest.class));
    assertThrows(FileNotFoundException.class,
        () -> proxy.getObjectMetadata("bucket", "key"));
  }
}
