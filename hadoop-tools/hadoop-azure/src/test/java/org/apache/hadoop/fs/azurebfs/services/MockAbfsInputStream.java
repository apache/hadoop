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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.apache.hadoop.fs.FileSystem.Statistics;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TestMockHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockAbfsInputStream extends AbfsInputStream {
  //Diff between Filetime epoch and Unix epoch (in ms)
  private static final long FILETIME_EPOCH_DIFF = 11644473600000L;
  // 1ms in units of nanoseconds
  private static final long FILETIME_ONE_MILLISECOND = 10 * 1000;
  int errStatus = 0;
  boolean mockRequestException = false;
  boolean mockConnectionException = false;
  boolean disableForceFastpathMock = false;

  public MockAbfsInputStream(final AbfsClient mockClient,
      final Statistics statistics,
      final String path,
      final long contentLength,
      final AbfsInputStreamContext abfsInputStreamContext,
      final String eTag,
      TracingContext tracingContext) throws Exception {
    super(mockClient, statistics, path, contentLength, abfsInputStreamContext,
        eTag,
        new TracingContext("MockFastpathTest",
            UUID.randomUUID().toString(), FSOperationType.OPEN, TracingHeaderFormat.ALL_ID_FORMAT,
            null));
    createMockAbfsFastpathSession();
  }

  public MockAbfsInputStream(final AbfsClient client, final AbfsInputStream in)
      throws IOException {
    super(new MockAbfsClient(client), in.getFSStatistics(), in.getPath(),
        in.getContentLength(), in.getContext().withFastpathEnabledState(false),
        in.getETag(),
        in.getTracingContext());
    try {
      createMockAbfsFastpathSession();
    } catch (Exception e) {
      Assert.fail("createMockAbfsFastpathSession failed " + e);
    }
  }

  protected void createAbfsFastpathSession(boolean isFastpathFeatureConfigOn) {
    if (isFastpathFeatureConfigOn) {
      try {
        fastpathSession = new MockAbfsFastpathSession(client, path, eTag, tracingContext);
      } catch (IOException e) {
        Assert.fail("Failure in creating MockAbfsFastpathSession instance " + e);
      }
    }
  }

  public void createMockAbfsFastpathSession()
      throws Exception {
    AbfsFastpathSession fastpathSsn = MockAbfsInputStream.getStubAbfsFastpathSession(
        client, path, eTag,
        tracingContext);
    setFastpathSession(new MockAbfsFastpathSession(fastpathSsn));
  }

  protected AbfsRestOperation executeRead(String path, byte[] b, String sasToken, ReadRequestParameters reqParam, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();
    // Force fastpath connection so that test fails and not pass on REST fallback
    return ((MockAbfsClient)client).read(path, b, sasToken, reqParam, tracingContext);
  }

  private void signalErrorConditionToMockClient() {
    if (errStatus != 0) {
      ((MockAbfsClient) client).induceError(errStatus);
    }

    if (mockRequestException) {
      ((MockAbfsClient) client).induceRequestException();
    }

    if (mockConnectionException) {
      ((MockAbfsClient) client).induceConnectionException();
    }

    if (disableForceFastpathMock) {
      ((MockAbfsClient) client).forceFastpathReadAlways = false;
    }
  }

  public AbfsClient getClient() {
    return this.client;
  }

  public Statistics getFSStatistics() {
    return super.getFSStatistics();
  }

  public void induceError(int httpStatus) {
    errStatus = httpStatus;
  }

  public void induceRequestException() {
    mockRequestException = true;
  }

  public void induceConnectionException() {
    mockConnectionException = true;
  }

  public void disableAlwaysOnFastpathTestMock() {
    disableForceFastpathMock = true;
    ((MockAbfsClient)client).forceFastpathReadAlways = false;
  }

  public void resetAllMockErrStates() {
    errStatus = 0;
    mockRequestException = false;
    mockConnectionException = false;
  }

  public static AbfsFastpathSession getStubAbfsFastpathSession(final AbfsClient client,
      final String path,
      final String eTag,
      TracingContext tracingContext,
  AbfsFastpathSessionInfo ssnInfo) throws Exception {
    AbfsFastpathSession mockSession = getStubAbfsFastpathSession(client, path, eTag, tracingContext);
    // set the sessionInfo so that fileHandle and connectionMode are set
    // (session token and expiry will also get set but they will be rewritten)
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "fastpathSessionInfo", ssnInfo);
    // Overwrite session token and expiry so that refresh of the token is
    // triggered as well.
    mockSession.updateAbfsFastpathSessionToken(ssnInfo.getSessionToken(), ssnInfo.getSessionTokenExpiry());
    return mockSession;
  }

  public static AbfsFastpathSession getStubAbfsFastpathSession(final AbfsClient client,
      final String path,
      final String eTag,
      TracingContext tracingContext) throws Exception {

    AbfsFastpathSession mockSession = mock(AbfsFastpathSession.class);
    Logger log = LoggerFactory.getLogger(AbfsInputStream.class);
    double session_refresh_internal_factor = 0.75;
    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // override fields
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "LOG", log);
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "SESSION_REFRESH_INTERVAL_FACTOR", session_refresh_internal_factor);
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "client", client);
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "path", path);
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "eTag", eTag);
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "tracingContext", tracingContext);
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "scheduledExecutorService", scheduledExecutorService);
    mockSession = TestMockHelpers.setClassField(AbfsFastpathSession.class,
        mockSession, "rwLock", rwLock);

    doCallRealMethod().when(mockSession)
        .updateAbfsFastpathSessionToken(any(), any());
    doCallRealMethod().when(mockSession)
        .updateConnectionMode(any(AbfsConnectionMode.class));
    doCallRealMethod().when(mockSession)
        .updateConnectionModeForFailures(any(AbfsConnectionMode.class));
    doCallRealMethod().when(mockSession).close();
    doCallRealMethod().when(mockSession)
        .setConnectionMode(any(AbfsConnectionMode.class));
    doCallRealMethod().when(mockSession)
        .getExpiry(any(byte[].class));

    when(mockSession.executeFastpathClose()).thenCallRealMethod();
    when(mockSession.executeFastpathOpen()).thenCallRealMethod();
    when(mockSession.getCurrentAbfsFastpathSessionInfoCopy()).thenCallRealMethod();
    when(mockSession.executeFetchFastpathSessionToken()).thenCallRealMethod();
    when(mockSession.getSessionRefreshIntervalInSec()).thenCallRealMethod();
    when(mockSession.fetchFastpathSessionToken()).thenCallRealMethod();

    return mockSession;
  }

  public static AbfsRestOperation getMockSuccessRestOp(AbfsClient client, byte[] token, Duration tokenDuration)
      throws IOException {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    long w32FileTime =
        (Instant.now().plus(tokenDuration).toEpochMilli() + FILETIME_EPOCH_DIFF)
            * FILETIME_ONE_MILLISECOND;
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(Long.reverseBytes(w32FileTime));
    byte[] timeArray = buffer.array();
    byte[] sessionToken = new byte[token.length + timeArray.length + 8];
    System.arraycopy(timeArray,0,sessionToken,8,timeArray.length);
    System.arraycopy(token,0,sessionToken,16,token.length);
    when(httpOp.getResponseContentBuffer()).thenReturn(sessionToken);
    when(op.getResult()).thenReturn(httpOp);
    return op;
  }
}
