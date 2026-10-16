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
import com.volcengine.tos.TOSV2ClientBuilder;
import com.volcengine.tos.TosClientException;
import com.volcengine.tos.TosException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.auth.Credential;
import com.volcengine.tos.auth.StaticCredentials;
import com.volcengine.tos.comm.HttpStatus;
import com.volcengine.tos.model.object.DeleteObjectInput;
import com.volcengine.tos.model.object.HeadObjectV2Input;
import com.volcengine.tos.model.object.HeadObjectV2Output;
import com.volcengine.tos.model.object.ListObjectsV2Input;
import com.volcengine.tos.model.object.ListObjectsV2Output;
import com.volcengine.tos.model.object.PutObjectInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.common.Tasks;
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.tos.auth.EnvironmentCredentialsProvider;
import org.apache.hadoop.fs.tosfs.object.tos.auth.SimpleCredentialsProvider;
import org.apache.hadoop.fs.tosfs.util.ParseUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestReporter;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.net.ssl.SSLException;

import static org.apache.hadoop.fs.tosfs.object.tos.DelegationClient.isRetryableException;
import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestDelegationClientBuilder {

  private static final String TEST_KEY = UUIDUtils.random();
  private static final String TEST_DATA = "1234567890";
  private static String envAccessKey;
  private static String envSecretKey;
  private static String envEndpoint;

  // Maximum retry times of the tos http client.
  public static final String MAX_RETRY_COUNT_KEY = "fs.tos.http.maxRetryCount";

  @BeforeAll
  public static void before() {
    assumeTrue(TestEnv.checkTestEnabled());

    envAccessKey =
        ParseUtils.envAsString(TOS.ENV_TOS_ACCESS_KEY_ID, false);
    envSecretKey =
        ParseUtils.envAsString(TOS.ENV_TOS_SECRET_ACCESS_KEY, false);
    envEndpoint = ParseUtils.envAsString(TOS.ENV_TOS_ENDPOINT, false);
  }

  @BeforeEach
  public void setUp() {
    TOSV2 tosSdkClientV2 =
        new TOSV2ClientBuilder().build(TestUtility.region(), TestUtility.endpoint(),
            new StaticCredentials(envAccessKey, envSecretKey));
    try (ByteArrayInputStream stream = new ByteArrayInputStream(TEST_DATA.getBytes())) {
      PutObjectInput putObjectInput =
          new PutObjectInput().setBucket(TestUtility.bucket()).setKey(TEST_KEY).setContent(stream);
      tosSdkClientV2.putObject(putObjectInput);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testHeadApiRetry() throws IOException {
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME),
        "https://test.tos-cn-beijing.ivolces.com");
    conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, SimpleCredentialsProvider.NAME);
    conf.setBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE, false);
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key("test"), "ACCESS_KEY");
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key("test"), "SECRET_KEY");

    DelegationClient tosV2 = new DelegationClientBuilder().bucket("test").conf(conf).build();
    TOSV2 mockClient = mock(TOSV2.class);
    tosV2.setClient(mockClient);
    tosV2.setMaxRetryTimes(5);

    HeadObjectV2Input input = HeadObjectV2Input.builder().bucket("test").build();
    when(tosV2.headObject(input)).thenThrow(
            new TosServerException(HttpStatus.INTERNAL_SERVER_ERROR),
            new TosServerException(HttpStatus.TOO_MANY_REQUESTS),
            new TosClientException("fake toe", new IOException("fake ioe")),
            new TosException(new SocketException("fake msg")),
            new TosException(new UnknownHostException("fake msg")),
            new TosException(new SSLException("fake msg")),
            new TosException(new InterruptedException("fake msg")),
            new TosException(new InterruptedException("fake msg")))
        .thenReturn(new HeadObjectV2Output());

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> tosV2.headObject(input));
    assertTrue(exception instanceof TosException);
    assertTrue(exception.getCause() instanceof UnknownHostException);
    verify(tosV2.client(), times(5)).headObject(input);

    HeadObjectV2Input inputOneTime = HeadObjectV2Input.builder().bucket("inputOneTime").build();
    HeadObjectV2Output output = new HeadObjectV2Output();
    when(tosV2.headObject(inputOneTime)).thenReturn(output);
    HeadObjectV2Output headObject = tosV2.headObject(inputOneTime);
    assertEquals(headObject, output);
    verify(tosV2.client(), times(1)).headObject(inputOneTime);
    tosV2.close();

    DelegationClient newClient = new DelegationClientBuilder().bucket("test").conf(conf).build();
    mockClient = mock(TOSV2.class);
    newClient.setClient(mockClient);
    newClient.setMaxRetryTimes(5);
    when(newClient.headObject(input)).thenThrow(
        new TosClientException("fake toe", new EOFException("fake eof")),
        new TosServerException(HttpStatus.INTERNAL_SERVER_ERROR),
        new TosServerException(HttpStatus.TOO_MANY_REQUESTS)).thenReturn(new HeadObjectV2Output());

    exception = assertThrows(RuntimeException.class, () -> newClient.headObject(input));
    assertTrue(exception instanceof TosClientException);
    assertTrue(exception.getCause() instanceof EOFException);
    verify(newClient.client(), times(1)).headObject(input);
    newClient.close();
  }

  @Test
  public void testEnableCrcCheck(TestInfo testInfo, TestReporter testReporter) throws IOException {
    String bucket = testInfo.getTestMethod().map(method -> method.getName()).orElse("Unknown");
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME),
        "https://test.tos-cn-beijing.ivolces.com");
    conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, SimpleCredentialsProvider.NAME);
    conf.setBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE, true);
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key(bucket), "ACCESS_KEY");
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key(bucket), "SECRET_KEY");

    DelegationClient tosV2 = new DelegationClientBuilder().bucket(bucket).conf(conf).build();
    assertTrue(tosV2.config().isEnableCrc());

    conf.setBoolean(TosKeys.FS_TOS_CRC_CHECK_ENABLED, false);
    tosV2 = new DelegationClientBuilder().bucket(bucket).conf(conf).build();
    assertFalse(tosV2.config().isEnableCrc());

    tosV2.close();
  }

  @Test
  public void testClientCache(TestInfo testInfo, TestReporter testReporter) throws IOException {
    String bucket = testInfo.getTestMethod().map(method -> method.getName()).orElse("Unknown");
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME),
        "https://test.tos-cn-beijing.ivolces.com");
    conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, SimpleCredentialsProvider.NAME);
    conf.setBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE, false);
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key(bucket), "ACCESS_KEY_A");
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key(bucket), "SECRET_KEY_A");

    DelegationClient tosV2 = new DelegationClientBuilder().bucket(bucket).conf(conf).build();
    DelegationClient tosV2Cached = new DelegationClientBuilder().bucket(bucket).conf(conf).build();
    assertEquals(tosV2Cached, tosV2, "client must be load in cache");
    assertEquals("ACCESS_KEY_A", tosV2.usedCredential().getAccessKeyId());
    tosV2Cached.close();

    String newBucket = "new-test-bucket";
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key(newBucket), "ACCESS_KEY_B");
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key(newBucket), "SECRET_KEY_B");
    DelegationClient changeBucketClient =
        new DelegationClientBuilder().bucket(newBucket).conf(conf).build();
    assertNotEquals(changeBucketClient, tosV2, "client should be created entirely new");
    assertEquals("ACCESS_KEY_B", changeBucketClient.usedCredential().getAccessKeyId());
    changeBucketClient.close();

    conf.setBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE, true); // disable cache: true
    DelegationClient tosV2NotCached =
        new DelegationClientBuilder().bucket(bucket).conf(conf).build();
    assertNotEquals(tosV2NotCached, tosV2, "client should be created entirely new");
    assertEquals("ACCESS_KEY_A", tosV2NotCached.usedCredential().getAccessKeyId());
    tosV2NotCached.close();

    tosV2.close();
  }

  @Test
  public void testOverwriteHttpConfig() throws IOException {
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME),
        "https://tos-cn-beijing.ivolces.com");
    conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, SimpleCredentialsProvider.NAME);
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key("test"), "ACCESS_KEY");
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key("test"), "SECRET_KEY");
    conf.setInt(TosKeys.FS_TOS_HTTP_MAX_CONNECTIONS, 24);
    conf.setInt(MAX_RETRY_COUNT_KEY, 24);
    conf.setInt(TosKeys.FS_TOS_REQUEST_MAX_RETRY_TIMES, 24);
    conf.setBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE, true);

    DelegationClient tosV2 = new DelegationClientBuilder().bucket("test").conf(conf).build();
    assertEquals("ACCESS_KEY", tosV2.usedCredential().getAccessKeyId());
    assertEquals(24, tosV2.config().getTransportConfig().getMaxConnections(),
        "http max connection overwrite to 24 from 1024, must be 24");
    assertEquals(DelegationClientBuilder.DISABLE_TOS_RETRY_VALUE,
        tosV2.config().getTransportConfig().getMaxRetryCount(),
        "tos maxRetryCount disabled, must be -1");
    assertEquals(24, tosV2.maxRetryTimes(), "maxRetryTimes must be 24");
    assertEquals("https://tos-cn-beijing.ivolces.com", tosV2.config().getEndpoint(),
        "endpoint must be equals to https://tos-cn-beijing.ivolces.com");

    tosV2.close();
  }

  @Test
  public void testDynamicRefreshAkSk() throws IOException {
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME), envEndpoint);
    conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, SimpleCredentialsProvider.NAME);
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key(TestUtility.bucket()), envAccessKey);
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key(TestUtility.bucket()), envSecretKey);
    conf.setInt(TosKeys.FS_TOS_HTTP_MAX_CONNECTIONS, 24);
    conf.setInt(MAX_RETRY_COUNT_KEY, 24);

    TOSV2 tosSdkClientV2 =
        new TOSV2ClientBuilder().build(TestUtility.region(), TestUtility.endpoint(),
            new StaticCredentials("a", "b"));
    DelegationClient delegationClientV2 =
        new DelegationClientBuilder().bucket(TestUtility.bucket()).conf(conf).build();

    ListObjectsV2Input inputV2 =
        ListObjectsV2Input.builder().bucket(TestUtility.bucket()).prefix(TEST_KEY).marker("")
            .maxKeys(10).build();

    assertThrows(TosServerException.class, () -> tosSdkClientV2.listObjects(inputV2));

    tosSdkClientV2.changeCredentials(new StaticCredentials(envAccessKey, envSecretKey));

    ListObjectsV2Output tosSdkOutput = tosSdkClientV2.listObjects(inputV2);
    ListObjectsV2Output delegateOutput = delegationClientV2.listObjects(inputV2);
    int nativeContentSize =
        tosSdkOutput.getContents() == null ? -1 : tosSdkOutput.getContents().size();
    int delegateContentSize =
        delegateOutput.getContents() == null ? -1 : delegateOutput.getContents().size();

    assertEquals(nativeContentSize, delegateContentSize,
        "delegation client must same as native client");
    assertEquals(envAccessKey, delegationClientV2.usedCredential().getAccessKeyId());

    delegationClientV2.close();
  }

  @Test
  public void testCreateClientWithEnvironmentCredentials() throws IOException {
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME), envEndpoint);
    conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, EnvironmentCredentialsProvider.NAME);

    DelegationClient tosV2 =
        new DelegationClientBuilder().bucket(TestUtility.bucket()).conf(conf).build();
    Credential cred = tosV2.usedCredential();

    String assertMsg =
        String.format("expect %s, but got %s", envAccessKey, cred.getAccessKeyId());
    assertEquals(cred.getAccessKeyId(), envAccessKey, assertMsg);
    assertMsg = String.format("expect %s, but got %s", envSecretKey, cred.getAccessKeySecret());
    assertEquals(cred.getAccessKeySecret(), envSecretKey, assertMsg);

    tosV2.close();
  }

  @Test
  public void testCreateClientWithSimpleCredentials() throws IOException {
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME), envEndpoint);
    conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, SimpleCredentialsProvider.NAME);
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key(TestUtility.bucket()), envAccessKey);
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key(TestUtility.bucket()), envSecretKey);
    conf.setInt(TosKeys.FS_TOS_HTTP_MAX_CONNECTIONS, 24);
    conf.setInt(MAX_RETRY_COUNT_KEY, 24);

    ListObjectsV2Input input =
        ListObjectsV2Input.builder().bucket(TestUtility.bucket()).prefix(TEST_KEY).marker("")
            .maxKeys(10).build();

    TOSV2 v2 = new TOSV2ClientBuilder().build(TestUtility.region(), TestUtility.endpoint(),
        new StaticCredentials(envAccessKey, envSecretKey));
    ListObjectsV2Output outputV2 = v2.listObjects(input);

    DelegationClient tosV2 =
        new DelegationClientBuilder().bucket(TestUtility.bucket()).conf(conf).build();

    ListObjectsV2Output output = tosV2.listObjects(input);
    assertEquals(outputV2.getContents().size(), output.getContents().size(),
        "delegation client must be same as native client");

    tosV2.close();
  }

  @Test
  public void testCachedConcurrently(TestInfo testInfo, TestReporter testReporter) {
    String bucketName = testInfo.getTestMethod().map(method -> method.getName()).orElse("Unknown");

    Function<String, Configuration> commonConf = bucket -> {
      Configuration conf = new Configuration();
      conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME), envEndpoint);
      conf.set(TosKeys.FS_TOS_CREDENTIALS_PROVIDER, SimpleCredentialsProvider.NAME);
      conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key(bucket), envAccessKey);
      conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key(bucket), envSecretKey);
      return conf;
    };

    // enable cache
    Function<String, Configuration> enableCachedConf = bucket -> {
      Configuration conf = commonConf.apply(bucket);
      conf.setBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE, false);
      return conf;
    };

    ExecutorService es = ThreadPools.newWorkerPool("testCachedConcurrently", 32);
    int bucketCount = 5;
    int taskCount = 10000;

    AtomicInteger success = new AtomicInteger(0);
    AtomicInteger failure = new AtomicInteger(0);
    Tasks.foreach(IntStream.range(0, taskCount).boxed().map(i -> bucketName + (i % bucketCount)))
        .executeWith(es).run(bucket -> {
          try {
            Configuration conf = enableCachedConf.apply(bucket);
            DelegationClient client =
                new DelegationClientBuilder().bucket(bucket).conf(conf).build();
            client.close();
            success.incrementAndGet();
          } catch (Exception e) {
            failure.incrementAndGet();
          }
        });

    assertEquals(bucketCount, DelegationClientBuilder.CACHE.size());
    assertEquals(taskCount, success.get());
    assertEquals(0, failure.get());

    // clear cache
    DelegationClientBuilder.CACHE.clear();

    // disable cache
    Function<String, Configuration> disableCachedConf = bucket -> {
      Configuration conf = commonConf.apply(bucket);
      conf.setBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE, true);
      return conf;
    };

    success.set(0);
    failure.set(0);
    Tasks.foreach(IntStream.range(0, taskCount).boxed().map(i -> bucketName + (i % bucketCount)))
        .executeWith(es).run(bucket -> {
          try {
            Configuration conf = disableCachedConf.apply(bucket);
            DelegationClient client =
                new DelegationClientBuilder().bucket(bucket).conf(conf).build();
            client.close();
            success.incrementAndGet();
          } catch (Exception e) {
            failure.incrementAndGet();
          }
        });

    assertTrue(DelegationClientBuilder.CACHE.isEmpty());
    assertEquals(taskCount, success.get());
    assertEquals(0, failure.get());

    es.shutdown();
  }

  @AfterEach
  public void deleteAllTestData() throws IOException {
    TOSV2 tosSdkClientV2 =
        new TOSV2ClientBuilder().build(TestUtility.region(), TestUtility.endpoint(),
            new StaticCredentials(envAccessKey, envSecretKey));
    tosSdkClientV2.deleteObject(
        DeleteObjectInput.builder().bucket(TestUtility.bucket()).key(TEST_KEY).build());

    tosSdkClientV2.close();
    DelegationClientBuilder.CACHE.clear();
  }

  @Test
  public void testRetryableException() {
    assertTrue(retryableException(new TosServerException(500)));
    assertTrue(retryableException(new TosServerException(501)));
    assertTrue(retryableException(new TosServerException(429)));
    assertFalse(retryableException(new TosServerException(404)));

    assertTrue(retryableException(new TosException(new SocketException())));
    assertTrue(retryableException(new TosException(new UnknownHostException())));
    assertTrue(retryableException(new TosException(new SSLException("fake ssl"))));
    assertTrue(retryableException(new TosException(new SocketTimeoutException())));
    assertTrue(retryableException(new TosException(new InterruptedException())));

    assertTrue(retryableException(new TosClientException("fake ioe", new IOException())));
    assertFalse(retryableException(new TosClientException("fake eof", new EOFException())));

    assertTrue(retryableException(new TosServerException(409)));
    assertTrue(
        retryableException(new TosServerException(409).setEc(TOSErrorCodes.PATH_LOCK_CONFLICT)));
    assertFalse(
        retryableException(new TosServerException(409).setEc(TOSErrorCodes.DELETE_NON_EMPTY_DIR)));
    assertFalse(
        retryableException(new TosServerException(409).setEc(TOSErrorCodes.LOCATED_UNDER_A_FILE)));
    assertFalse(retryableException(
        new TosServerException(409).setEc(TOSErrorCodes.COPY_BETWEEN_DIR_AND_FILE)));
    assertFalse(retryableException(
        new TosServerException(409).setEc(TOSErrorCodes.RENAME_TO_AN_EXISTED_DIR)));
    assertFalse(
        retryableException(new TosServerException(409).setEc(TOSErrorCodes.RENAME_TO_SUB_DIR)));
    assertFalse(retryableException(
        new TosServerException(409).setEc(TOSErrorCodes.RENAME_BETWEEN_DIR_AND_FILE)));
  }

  private boolean retryableException(TosException e) {
    return isRetryableException(e,
        Arrays.asList(TOSErrorCodes.FAST_FAILURE_CONFLICT_ERROR_CODES.split(",")));
  }
}
