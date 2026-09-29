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

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TosException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.comm.HttpStatus;
import com.volcengine.tos.model.RequestInfo;
import com.volcengine.tos.model.object.PutObjectOutput;
import com.volcengine.tos.model.object.UploadPartV2Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.InputStreamProvider;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.object.tos.auth.SimpleCredentialsProvider;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLException;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTOSRetryPolicy {

  private final String retryKey = "retryKey.txt";
  private TOSV2 tosClient;
  private DelegationClient client;

  @BeforeAll
  public static void before() {
    assumeTrue(TestEnv.checkTestEnabled());
  }

  @BeforeEach
  public void setUp() {
    client = createRetryableDelegationClient();
    tosClient = mock(TOSV2.class);
    client.setClient(tosClient);
  }

  @AfterEach
  public void tearDown() throws IOException {
    tosClient.close();
    client.close();
  }

  private DelegationClient createRetryableDelegationClient() {
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME),
        "https://tos-cn-beijing.ivolces.com");
    conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, SimpleCredentialsProvider.NAME);
    conf.setBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE, true);
    conf.set(TosKeys.FS_TOS_ACCESS_KEY_ID, "ACCESS_KEY");
    conf.set(TosKeys.FS_TOS_SECRET_ACCESS_KEY, "SECRET_KEY");
    return new DelegationClientBuilder().bucket("test").conf(conf).build();
  }

  @Test
  public void testShouldThrowExceptionAfterRunOut5RetryTimesIfNoRetryConfigSet()
      throws IOException {
    TOS storage =
        (TOS) ObjectStorageFactory.create(TOS_SCHEME, TestUtility.bucket(), new Configuration());
    storage.setClient(client);
    client.setMaxRetryTimes(5);

    PutObjectOutput response = mock(PutObjectOutput.class);
    InputStreamProvider streamProvider = mock(InputStreamProvider.class);

    when(tosClient.putObject(any())).thenThrow(
        new TosServerException(HttpStatus.INTERNAL_SERVER_ERROR),
        new TosServerException(HttpStatus.TOO_MANY_REQUESTS),
        new TosException(new SocketException("fake msg")),
        new TosException(new UnknownHostException("fake msg")),
        new TosException(new SSLException("fake msg")),
        new TosException(new InterruptedException("fake msg")),
        new TosException(new InterruptedException("fake msg"))).thenReturn(response);

    // after run out retry times, should throw exception
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> storage.put(retryKey, streamProvider, 0));
    assertTrue(exception instanceof TosException);
    assertTrue(exception.getCause() instanceof SSLException);

    // the newStream method of stream provider should be called 5 times
    verify(streamProvider, times(5)).newStream();

    storage.close();
  }

  @Test
  public void testShouldReturnResultAfterRetry8TimesIfConfigured10TimesRetry()
      throws IOException {
    TOS storage =
        (TOS) ObjectStorageFactory.create(TOS_SCHEME, TestUtility.bucket(), new Configuration());
    DelegationClient delegationClient = createRetryableDelegationClient();
    delegationClient.setClient(tosClient);
    delegationClient.setMaxRetryTimes(10);
    storage.setClient(delegationClient);

    UploadPartV2Output response = new UploadPartV2Output().setPartNumber(1).setEtag("etag");

    InputStream in = mock(InputStream.class);
    InputStreamProvider streamProvider = mock(InputStreamProvider.class);
    when(streamProvider.newStream()).thenReturn(in);

    when(tosClient.uploadPart(any())).thenThrow(
        new TosServerException(HttpStatus.INTERNAL_SERVER_ERROR),
        new TosServerException(HttpStatus.TOO_MANY_REQUESTS),
        new TosException(new SocketException("fake msg")),
        new TosException(new UnknownHostException("fake msg")),
        new TosException(new SSLException("fake msg")),
        new TosException(new InterruptedException("fake msg")),
        new TosException(new InterruptedException("fake msg"))).thenReturn(response);

    // after run out retry times, should throw exception
    Part part = storage.uploadPart(retryKey, "uploadId", 1, streamProvider, 0);
    assertEquals(1, part.num());
    assertEquals("etag", part.eTag());

    // the newStream method of stream provider should be called 8 times
    verify(streamProvider, times(8)).newStream();

    storage.close();
  }

  @Test
  public void testShouldReturnResultIfRetry3TimesSucceed() throws IOException {
    TOS storage =
        (TOS) ObjectStorageFactory.create(TOS_SCHEME, TestUtility.bucket(), new Configuration());
    storage.setClient(client);

    PutObjectOutput response = mock(PutObjectOutput.class);
    InputStreamProvider streamProvider = mock(InputStreamProvider.class);

    RequestInfo requestInfo = mock(RequestInfo.class);
    Map<String, String> header = new HashMap<>();
    when(response.getRequestInfo()).thenReturn(requestInfo);
    when(requestInfo.getHeader()).thenReturn(header);

    when(tosClient.putObject(any())).thenThrow(
        new TosServerException(HttpStatus.INTERNAL_SERVER_ERROR),
        new TosServerException(HttpStatus.TOO_MANY_REQUESTS)).thenReturn(response);

    storage.put(retryKey, streamProvider, 0);
    // the newStream method of stream provider should be called 3 times
    verify(streamProvider, times(3)).newStream();

    storage.close();
  }

  @Test
  public void testShouldNotRetryIfThrowUnRetryException() throws IOException {
    TOS storage =
        (TOS) ObjectStorageFactory.create(TOS_SCHEME, TestUtility.bucket(), new Configuration());
    storage.setClient(client);

    InputStreamProvider streamProvider = mock(InputStreamProvider.class);

    when(tosClient.putObject(any())).thenThrow(
        new TosException(new NullPointerException("fake msg.")));

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> storage.put(retryKey, streamProvider, 0));
    assertTrue(exception instanceof TosException);
    assertTrue(exception.getCause() instanceof NullPointerException);

    // the newStream method of stream provider should be only called once.
    verify(streamProvider, times(1)).newStream();

    storage.close();
  }
}
