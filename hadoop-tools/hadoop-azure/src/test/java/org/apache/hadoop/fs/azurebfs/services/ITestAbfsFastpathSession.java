/**
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
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.base.Stopwatch;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;
import org.apache.hadoop.fs.azurebfs.utils.TestCachedSASToken;
import org.apache.hadoop.fs.azurebfs.utils.TestMockHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsFastpathSession extends AbstractAbfsIntegrationTest {
  protected static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);
  public static final Duration TWO_MIN = Duration.ofMinutes(2);
  public static final Duration FIVE_MIN = Duration.ofMinutes(5);

  public ITestAbfsFastpathSession() throws Exception {
    super();
  }

  @Test
  public void testFastpathSessionTokenFetch() throws Exception {
    describe("Tests success scenario on an account which has feature enabled on backend");

    // Run this test only if feature is set to on
    Assume.assumeTrue(getDefaultFastpathFeatureStatus());
    Path testPath = new Path("testFastpathSessionTokenFetch");
    byte[] fileContent = createTestFileAndRegisterToMock(testPath, 8 * ONE_MB);

    try (FSDataInputStream inputStream = openMockAbfsInputStream(this.getFileSystem(), testPath)) {
      AbfsInputStream currStream = (AbfsInputStream) inputStream.getWrappedStream();

      // Fastpath session should be valid now. Check.
      validateFastpathSession(currStream.getFastpathSession());

      // Perform read checks
      byte[] buffer = new byte[3 * ONE_MB];
      seekForwardAndRead(currStream, fileContent, buffer);
      seekBackwardAndRead(currStream, fileContent, buffer);

      MockFastpathConnection.unregisterAppend(testPath.getName());
    }
  }

  @Test
  public void testFastpathSessionRefresh() throws Exception {
    describe("Tests successful session refresh  on an account which has feature enabled on backend");

    // Run mock test only if feature is set to off
    Assume.assumeTrue(getDefaultFastpathFeatureStatus());

    Path testPath = new Path("testFastpathSessionRefresh");
    byte[] fileContent = createTestFileAndRegisterToMock(testPath, 8 * ONE_MB);

    try (FSDataInputStream inputStream = openMockAbfsInputStream(this.getFileSystem(), testPath)) {
      AbfsInputStream currStream = (AbfsInputStream) inputStream.getWrappedStream();

      // Take snap of current fastpath session
      AbfsFastpathSession fastpathSsn = currStream.getFastpathSession();
      AbfsFastpathSessionInfo fastpathSsnInfo = fastpathSsn.getCurrentAbfsFastpathSessionInfoCopy();
      String fastpathFileHandle = fastpathSsnInfo.getFastpathFileHandle();
      String sessionToken = fastpathSsnInfo.getSessionToken();

      // overwrite session expiry with a quicker expiry time
      OffsetDateTime utcNow = OffsetDateTime.now(java.time.ZoneOffset.UTC);
      Stopwatch stopwatch = Stopwatch.createStarted();
      fastpathSsn.updateAbfsFastpathSessionToken(sessionToken, utcNow.plusMinutes(1));

       // assert that
      // session token is still the same,
      // session expiry is 3/4th of expiry, in this case 45 sec
      // and file handle remains the same
      validateFastpathSession(fastpathSsn, sessionToken, 45, fastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);

      // test read before refresh
      byte[] buffer = new byte[3 * ONE_MB];
      seekForwardAndRead(currStream, fileContent, buffer);

      stopwatch.stop();
      LOG.debug("Put thread on sleep until just before session refresh time");
      Thread.sleep(
          (fastpathSsn.getSessionRefreshIntervalInSec()
              - stopwatch.elapsed(TimeUnit.SECONDS)) * 1000);

      // When refresh is in progress, current token should be valid
      validateFastpathSession(fastpathSsn, sessionToken, 45, fastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);

      LOG.debug("Put thread on sleep until a time session refresh is complete");
      Thread.sleep(20 * 1000);

      // Ensure session token is new, expiry is different (server default)
      // and file handle is still the same
      validateFastpathSessionOnRefresh(fastpathSsn, sessionToken, 2700, fastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);

      // execute read using new token
      seekBackwardAndRead(currStream, fileContent, buffer);

      MockFastpathConnection.unregisterAppend(testPath.getName());
    }
  }

  @Test
  public void testMockFastpathSessionRefreshFail() throws Exception {
    describe("Tests failed session refresh on an account which has feature enabled on backend");

    // Run mock test only if feature is set to off
    Assume.assumeTrue(getDefaultFastpathFeatureStatus());

    Path testPath = new Path("testMockFastpathSessionRefreshFail");
    byte[] fileContent = createTestFileAndRegisterToMock(testPath, 8 * ONE_MB);

    try (FSDataInputStream inputStream = openMockAbfsInputStream(this.getFileSystem(), testPath)) {
      MockAbfsInputStream currStream = (MockAbfsInputStream) inputStream.getWrappedStream();
      AbfsFastpathSession currFastpathSession = currStream.getFastpathSession();
      AbfsFastpathSessionInfo currFastpathSessionInfo = currFastpathSession.getCurrentAbfsFastpathSessionInfoCopy();
      String currFastpathFileHandle = currFastpathSessionInfo.getFastpathFileHandle();
      OffsetDateTime currSessionTokenExpiry = currFastpathSessionInfo.getSessionTokenExpiry();
      String currSessionToken = currFastpathSessionInfo.getSessionToken();

      // Fetch a mocked session using current values
      AbfsFastpathSession mockSsn = MockAbfsInputStream.getStubAbfsFastpathSession(
          currStream.client, currStream.path, currStream.eTag,
          currStream.tracingContext);
      AbfsFastpathSessionInfo mockSsnInfo = getMockAbfsFastpathSessionInfo(
          currSessionToken,
          currSessionTokenExpiry,
          currFastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);

      when(mockSsn.executeFetchFastpathSessionToken()).thenThrow(
          new AbfsRestOperationException(400, "", "",
              new Exception("session token fetch failed")));

      mockSsn.setAbfsFastpathSessionInfo(mockSsnInfo);
      currStream.setFastpathSession(mockSsn);
      currFastpathSession.close();

      OffsetDateTime utcNow = OffsetDateTime.now(java.time.ZoneOffset.UTC);
      Stopwatch stopwatch = Stopwatch.createStarted();
      // overwrite session expiry with a quicker expiry time
      mockSsn.updateAbfsFastpathSessionToken(currSessionToken, utcNow.plusMinutes(1));

      // assert that
      // session token is still the same,
      // session expiry is 3/4th of expiry, in this case 45 sec
      // and file handle remains the same
      validateFastpathSession(currStream.getFastpathSession(), currSessionToken,
          45, currFastpathFileHandle, AbfsConnectionMode.FASTPATH_CONN);

      // test read before refresh
      byte[] buffer = new byte[3 * ONE_MB];
      seekForwardAndRead(currStream, fileContent, buffer);

      stopwatch.stop();
      LOG.debug("Put thread on sleep until just before session refresh time");
      // is mocked , no network call
      Thread.sleep(
          (mockSsn.getSessionRefreshIntervalInSec() - stopwatch.elapsed(
              TimeUnit.SECONDS) - 1) * 1000);

      // When refresh is in progress, current token should be valid
      validateFastpathSession(currStream.getFastpathSession(), currSessionToken,
          45, currFastpathFileHandle, AbfsConnectionMode.FASTPATH_CONN);

      LOG.debug("Put thread on sleep until a time session refresh is complete");
      Thread.sleep(20 * 1000);

      // Refresh would have failed now
      // Ensure that connection mode has switched to REST on failure
      validateFailedFastpathRefresh(currStream);

      // next read will be over REST
      // unregister file from fastpath mock
      // read will fail if it attempts read on fastpath after this
      MockFastpathConnection.unregisterAppend(testPath.getName());
      ((MockAbfsInputStream) currStream).disableAlwaysOnFastpathTestMock();

      seekBackwardAndRead(currStream, fileContent, buffer);
    }
  }

  @Test
  public void testSuccessfulFastpathSessionRefreshOverMock() throws Exception {
    describe("Tests successful session refresh using mocks");
    testMockFastpathSessionRefresh(true, "testSuccessfulFastpathSessionRefreshOverMock");
  }

  @Test
  public void testFailedFastpathSessionRefreshOverMock() throws Exception {
    describe("Tests failed session refresh using mocks");
    testMockFastpathSessionRefresh(false, "testFailedFastpathSessionRefreshOverMock");
  }

  public void testMockFastpathSessionRefresh(boolean testSuccessfulRefresh, String fileName) throws Exception {
    AbfsClient client = TestAbfsClient.getMockAbfsClient(
        getAbfsClient(getFileSystem()),
        this.getConfiguration());
    Path testPath = new Path(fileName);
    createTestFileAndRegisterToMock(testPath, ONE_KB);
    try(AbfsInputStream inputStream = getInputStreamWithMockFastpathSession(client, testPath, FIVE_MIN)) {
      AbfsFastpathSession fastpathSsn = inputStream.getFastpathSession();
      AbfsFastpathSessionInfo fastpathSsnInfo
          = fastpathSsn.getCurrentAbfsFastpathSessionInfoCopy();
      String fastpathFileHandle = fastpathSsnInfo.getFastpathFileHandle();
      String sessionToken = fastpathSsnInfo.getSessionToken();

      // overwrite session expiry with a quicker expiry time
      // next refresh will get mock token with 2 min expiry
      OffsetDateTime utcNow = OffsetDateTime.now(java.time.ZoneOffset.UTC);
      Stopwatch stopwatch = Stopwatch.createStarted();
      fastpathSsn.updateAbfsFastpathSessionToken(sessionToken,
          utcNow.plusMinutes(1));

      // assert that
      // session token is still the same,
      // session expiry is 3/4th of expiry, in this case 45 sec
      // and file handle remains the same
      validateFastpathSession(fastpathSsn, sessionToken, 45, fastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);

      stopwatch.stop();
      if (testSuccessfulRefresh) {
        String mockSecondToken = "secondToken";
        AbfsRestOperation ssnTokenRspOp2
            = MockAbfsInputStream.getMockSuccessRestOp(client,
            mockSecondToken.getBytes(), TWO_MIN);
        when(fastpathSsn.executeFetchFastpathSessionToken()).thenReturn(
            ssnTokenRspOp2);
      } else {
        doThrow(new AbfsRestOperationException(400, "", "",
            new Exception("session token fetch failed")))
            .when(fastpathSsn)
            .executeFetchFastpathSessionToken();
      }

      LOG.debug("Put thread on sleep until just before session refresh time");
      Thread.sleep(
          (fastpathSsn.getSessionRefreshIntervalInSec()
              - stopwatch.elapsed(TimeUnit.SECONDS) - 1) * 1000);

      // When refresh is in progress, current token should be valid
      validateFastpathSession(fastpathSsn, sessionToken, 45, fastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);

      LOG.debug("Put thread on sleep until a time session refresh is complete");
      Thread.sleep(20 * 1000);

      if (testSuccessfulRefresh) {
        // Ensure session token is new, expiry is different (server default)
        // and file handle is still the same
        validateFastpathSessionOnRefresh(fastpathSsn, sessionToken, 90,
            fastpathFileHandle,
            AbfsConnectionMode.FASTPATH_CONN);
      } else {
        // Refresh would have failed now
        // Ensure that connection mode has switched to REST on failure
        validateFailedFastpathRefresh(inputStream);
      }
    }
  }

  @Test
  public void testFastpathReadAheadFailureOverMock() throws Exception {
    AbfsClient client = TestAbfsClient.getMockAbfsClient(
        getAbfsClient(getFileSystem()),
        this.getConfiguration());
    AbfsRestOperation successOp_Fastpath_Conn = getMockReadRestOp();

    org.mockito.stubbing.Answer<AbfsRestOperation> answer = invocation -> {
      ReadRequestParameters params = (ReadRequestParameters) invocation.getArguments()[3];
        params.getAbfsFastpathSessionInfo()
            .setConnectionMode(AbfsConnectionMode.REST_ON_FASTPATH_CONN_FAILURE);
      return successOp_Fastpath_Conn;
    };

    // Fail readAheads with Fastpath connection
    when(client.read(any(), any(byte[].class), any(), any(ReadRequestParameters.class), any(TracingContext.class)))
        .thenAnswer(answer);

    String fileName = "testFailedReadAheadOnFastpath,txt";
    Path testPath = new Path(fileName);
    createTestFileAndRegisterToMock(testPath, 3 * ONE_KB);
    try(AbfsInputStream inputStream = getInputStreamWithMockFastpathSession(client, testPath, FIVE_MIN)) {
      // Initially sessionInfo is valid and is in FASTPATH_CONN mode
      Assertions.assertThat(inputStream.getFastpathSession()
          .getCurrentAbfsFastpathSessionInfoCopy()
          .getConnectionMode()).describedAs(
          "Valid Fastpath session should be in FASTPATH_CONN mode")
          .isEqualTo(AbfsConnectionMode.FASTPATH_CONN);
      Assertions.assertThat(inputStream.tracingContext.getConnectionMode())
          .describedAs(
              "InputStream tracing context should be in FASTPATH_CONN mode")
          .isEqualTo(AbfsConnectionMode.FASTPATH_CONN);

      // trigger read
      // one of the readAhead threads fail on fastpath
      inputStream.read(new byte[3 * ONE_KB]);

      // Fastpath request failure should have flipped the inputStream
      // to REST and it should have no fastpath session info
      Assertions.assertThat(
          inputStream.getFastpathSession().getCurrentAbfsFastpathSessionInfoCopy())
          .describedAs(
              "As a readAhead thread failed, fastpath session should have been invalidated")
          .isEqualTo(null);
      Assertions.assertThat(inputStream.tracingContext.getConnectionMode())
          .describedAs(
              "InputStream tracing context should be in FASTPATH_CONN mode")
          .isEqualTo(AbfsConnectionMode.REST_ON_FASTPATH_CONN_FAILURE);
    }
  }

  protected byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  private void validateFailedFastpathRefresh(AbfsInputStream inputStream) {
    Assertions.assertThat(
        inputStream.getFastpathSession().getCurrentAbfsFastpathSessionInfoCopy())
        .describedAs(
            "No fastpath session info should be returned if refresh failed")
        .isEqualTo(null);
    Assertions.assertThat(inputStream.tracingContext.getConnectionMode())
        .describedAs(
            "InputStream trackingContext should have REST_ON_FASTPATH_SESSION_UPD_FAILURE")
        .isEqualTo(AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
  }

  private void validateFastpathSessionToken(AbfsFastpathSessionInfo sessionInfo, String sessionToken) {
    Assertions.assertThat(sessionInfo.getSessionToken()).describedAs(
        "Fastpath session token not as expected")
        .isEqualTo(sessionToken);
  }

  private void validateRefreshedFastpathSessionToken(AbfsFastpathSessionInfo sessionInfo, String oldSessionToken) {
    Assertions.assertThat(sessionInfo.getSessionToken()).describedAs(
        "Fastpath session token not as expected")
        .isNotEqualTo(oldSessionToken);
  }

  private void validateFastpathSessionOnRefresh(AbfsFastpathSession fastpathSession, String oldSessionToken,
      int sessionRefreshInternal, String fastpathFileHandle,
      AbfsConnectionMode connectionMode) {
    validateFastpathSession(true, fastpathSession, oldSessionToken,
        sessionRefreshInternal, fastpathFileHandle, connectionMode);
  }

  private void validateFastpathSession(AbfsFastpathSession fastpathSession, String sessionToken,
      int sessionRefreshInternal, String fastpathFileHandle,
      AbfsConnectionMode connectionMode) {
    validateFastpathSession(false, fastpathSession, sessionToken,
        sessionRefreshInternal, fastpathFileHandle, connectionMode);
  }

  private void validateFastpathSession(boolean isRefreshValidation,
      AbfsFastpathSession fastpathSession,
      String sessionToken,
      int sessionRefreshInternal,
      String fastpathFileHandle,
      AbfsConnectionMode connectionMode) {
    AbfsFastpathSessionInfo sessionInfo = fastpathSession.getCurrentAbfsFastpathSessionInfoCopy();
    if (isRefreshValidation) {
      validateRefreshedFastpathSessionToken(sessionInfo, sessionToken);
    } else {
      validateFastpathSessionToken(sessionInfo, sessionToken);
    }

    Assertions.assertThat(fastpathSession.getSessionRefreshIntervalInSec()).describedAs(
        "Fastpath session interval should be less than or equal to {} secs", sessionRefreshInternal)
        .isLessThanOrEqualTo(sessionRefreshInternal);
    Assertions.assertThat(sessionInfo.getFastpathFileHandle()).describedAs(
        "Fastpath session refresh should not affect fileHandle")
        .isEqualTo(fastpathFileHandle);
    Assertions.assertThat(sessionInfo.getConnectionMode()).describedAs(
        "Fastpath connection mode must be {}", connectionMode)
        .isEqualTo(connectionMode);
  }

  private void validateFastpathSession(AbfsFastpathSession fastpathSession) {
    AbfsFastpathSessionInfo sessionInfo = fastpathSession.getCurrentAbfsFastpathSessionInfoCopy();
    Assertions.assertThat(sessionInfo.getSessionToken()).describedAs(
        "Fastpath session token should have a non null value")
        .isNotNull();
    Assertions.assertThat(sessionInfo.getSessionToken()).describedAs(
        "Fastpath session token should have a non empty value")
        .isNotEmpty();
    Assertions.assertThat(fastpathSession.getSessionRefreshIntervalInSec()).describedAs(
        "Fastpath session token expiry interval should be > 0")
        .isGreaterThan(0);
    Assertions.assertThat(sessionInfo.getFastpathFileHandle()).describedAs(
        "Fastpath fileHandle should have a non null value")
        .isNotNull();
    Assertions.assertThat(sessionInfo.getFastpathFileHandle()).describedAs(
        "Fastpath fileHandle should have a non empty value")
        .isNotEmpty();
    Assertions.assertThat(sessionInfo.getConnectionMode()).describedAs(
        "Fastpath connection mode must be Fastpath")
        .isEqualTo(AbfsConnectionMode.FASTPATH_CONN);
  }

  private byte[] createTestFileAndRegisterToMock(Path testPath, int size)
      throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    byte[] fileContent = getRandomBytesArray(size);
    ContractTestUtils.createFile(fs, testPath, true, fileContent);
    MockFastpathConnection.registerAppend(size,
        testPath.getName(), fileContent, 0, fileContent.length);
    return fileContent;
  }

  private void seekForwardAndRead(AbfsInputStream inputStream, byte[] fileContent, byte[] buffer)
      throws IOException {
    // forward seek and read a kilobyte into first kilobyte of bufferV2
    inputStream.seek(5 * ONE_MB);

    int numBytesRead = inputStream.read(buffer, 0, ONE_KB);
    assertEquals("Wrong number of bytes read", ONE_KB, numBytesRead);
    byte[] expectedReadBytes = Arrays.copyOfRange(fileContent, 5 * ONE_MB,
        5 * ONE_MB + ONE_KB);
    byte[] actualReadBytes = Arrays.copyOfRange(buffer, 0, ONE_KB);
    assertTrue("(Position 5MB) : Data mismatch read ",
        Arrays.equals(actualReadBytes, expectedReadBytes));
  }

  private void seekBackwardAndRead(AbfsInputStream inputStream, byte[] fileContent, byte[] buffer)
      throws IOException {
    int len = ONE_MB;
    int offset = buffer.length - len;

    // reverse seek and read a megabyte into last megabyte of bufferV1
    inputStream.seek(3 * ONE_MB);
    int numBytesRead = inputStream.read(buffer, offset, len);
    assertEquals("Wrong number of bytes read after seek", len, numBytesRead);
    byte[] expectedReadBytes = Arrays.copyOfRange(fileContent, 3 * ONE_MB, 3 * ONE_MB + len);
    byte[] actualReadBytes = Arrays.copyOfRange(buffer, offset, offset + len);
    assertTrue("(Position 3MB) : Data mismatch read",
        Arrays.equals(actualReadBytes, expectedReadBytes));

  }

  private AbfsRestOperation getMockReadRestOp() {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    when(httpOp.getBytesReceived()).thenReturn(1024L);
    when(op.getResult()).thenReturn(httpOp);
    when(op.getSasToken()).thenReturn(TestCachedSASToken.getTestCachedSASTokenInstance().get());
    return op;
  }

  private AbfsInputStream getInputStreamWithMockFastpathSession(AbfsClient mockClient, Path testPath, Duration initialSessionValidityDuration)
      throws Exception {
    TestAbfsInputStream inStreamTest = new TestAbfsInputStream();

    AbfsInputStream inputStream = inStreamTest.getAbfsInputStream(mockClient, testPath.getName());

    AbfsFastpathSession fastpathSsn = MockAbfsInputStream.getStubAbfsFastpathSession(
        inputStream.client, inputStream.path, inputStream.eTag,
        inputStream.tracingContext);

    String mockFirstToken = "firstToken";
    AbfsRestOperation ssnTokenRspOp1 = MockAbfsInputStream.getMockSuccessRestOp(mockClient, mockFirstToken.getBytes(), initialSessionValidityDuration);
    when(fastpathSsn.executeFetchFastpathSessionToken()).thenReturn(ssnTokenRspOp1);

    // Create mock session for initialSessionValidityDuration
    fastpathSsn.fetchFastpathSessionToken();
    AbfsFastpathSessionInfo fastpathSsnInfo = fastpathSsn.getCurrentAbfsFastpathSessionInfoCopy();
    fastpathSsnInfo.setFastpathFileHandle(UUID.randomUUID().toString());
    inputStream.setFastpathSession(fastpathSsn);
    return inputStream;
  }

  private static AbfsFastpathSessionInfo getMockAbfsFastpathSessionInfo(final String sessionToken,
      final OffsetDateTime sessionTokenExpiry,
      final String fastpathFileHandle,
      AbfsConnectionMode connectionMode) throws Exception {

    Logger log = LoggerFactory.getLogger(AbfsInputStream.class);
    AbfsFastpathSessionInfo mockSsnInfo = mock(AbfsFastpathSessionInfo.class);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionInfo.class,
        mockSsnInfo, "LOG", log);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionInfo.class,
        mockSsnInfo, "sessionToken", sessionToken);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionInfo.class,
        mockSsnInfo, "sessionTokenExpiry", sessionTokenExpiry);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionInfo.class,
        mockSsnInfo, "fastpathFileHandle", fastpathFileHandle);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionInfo.class,
        mockSsnInfo, "connectionMode", connectionMode);

    doCallRealMethod().when(mockSsnInfo)
        .updateSessionToken(any(), any());
    doCallRealMethod().when(mockSsnInfo)
        .setFastpathFileHandle(any(), any(AbfsConnectionMode.class));
    doCallRealMethod().when(mockSsnInfo)
        .setConnectionMode(any(AbfsConnectionMode.class));

    when(mockSsnInfo.getSessionTokenExpiry()).thenCallRealMethod();
    when(mockSsnInfo.getFastpathFileHandle()).thenCallRealMethod();
    when(mockSsnInfo.getConnectionMode()).thenCallRealMethod();
    when(mockSsnInfo.getSessionToken()).thenCallRealMethod();
    when(mockSsnInfo.isValidSession()).thenCallRealMethod();

    return mockSsnInfo;
  }
}