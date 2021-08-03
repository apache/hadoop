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
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Assert;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.mockito.Mockito.doReturn;

public class MockAbfsFastpathSession extends AbfsFastpathSession {
  public static final Duration FIVE_MIN = Duration.ofMinutes(5);

  int errStatus = 0;
  boolean mockRequestException = false;
  boolean mockConnectionException = false;
  boolean disableForceFastpathMock = false;

  public MockAbfsFastpathSession(final AbfsClient client,
      final String path,
      final String eTag,
      TracingContext tracingContext) throws IOException {
    super((new MockAbfsClient(client)), path, eTag, tracingContext);
  }

  public MockAbfsFastpathSession(final AbfsFastpathSession srcSession) {
    super(srcSession.client, srcSession.path, srcSession.eTag,
        srcSession.tracingContext);
  }

  protected void fetchSessionTokenAndFileHandle() {
    try {
      AbfsFastpathSession fastpathSsn = MockAbfsInputStream.getStubAbfsFastpathSession(
          client, path, eTag,
          tracingContext);
      String mockFirstToken = "firstToken";
      AbfsRestOperation ssnTokenRspOp1 = MockAbfsInputStream.getMockSuccessRestOp(
          client, mockFirstToken.getBytes(), FIVE_MIN);

      doReturn(ssnTokenRspOp1)
          .when(fastpathSsn)
          .executeFetchFastpathSessionToken();

      fastpathSsn.fetchFastpathSessionToken();

      AbfsFastpathSessionInfo stubbedInfo = fastpathSsn.getCurrentAbfsFastpathSessionInfoCopy();
      setAbfsFastpathSessionInfo(stubbedInfo);
      fetchFastpathFileHandle();
    } catch (Exception ex) {
      Assert.fail(
          "Failure in creating mock AbfsFastpathSessionInfo instance with 5 min validity");
    }
  }

  void setAbfsFastpathSessionInfo(AbfsFastpathSessionInfo sessionInfo) {
    rwLock.writeLock().lock();
    try {
      fastpathSessionInfo = sessionInfo;
      OffsetDateTime utcNow = OffsetDateTime.now(ZoneOffset.UTC);
      sessionRefreshIntervalInSec = (int) Math.floor(
          utcNow.until(fastpathSessionInfo.getSessionTokenExpiry(), ChronoUnit.SECONDS)
              * SESSION_REFRESH_INTERVAL_FACTOR);

      // 0 or negative sessionRefreshIntervalInSec indicates a session token
      // whose expiry is near as soon as its received. This will end up
      // generating a lot of REST calls refreshing the session. Better to
      // switch off Fastpath in that case.
      if (sessionRefreshIntervalInSec <= 0) {
        LOG.debug(
            "Expiry time at present or past. Drop Fastpath session (could be clock skew). Received expiry {} ",
            fastpathSessionInfo.getSessionTokenExpiry());
        tracingContext.setConnectionMode(
            AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
        fastpathSessionInfo.setConnectionMode(
            AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
        return;
      }

      // schedule for refresh right away
      ScheduledFuture scheduledFuture =
          scheduledExecutorService.schedule(new Callable() {
            public Boolean call() throws Exception {
              return fetchFastpathSessionToken();
            }
          }, sessionRefreshIntervalInSec, TimeUnit.SECONDS);
      LOG.debug(
          "Fastpath session token fetch successful, valid till {}. Refresh scheduled after {} secs",
          fastpathSessionInfo.getSessionTokenExpiry(), sessionRefreshIntervalInSec);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  protected AbfsRestOperation executeFastpathClose()
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();
    return client.fastPathClose(path, eTag,
        fastpathSessionInfo, tracingContext);
  }

  protected AbfsRestOperation executeFastpathOpen()
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();
    return client.fastPathOpen(path, eTag,
        fastpathSessionInfo, tracingContext);
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
  }

  public void resetAllMockErrStates() {
    errStatus = 0;
    mockRequestException = false;
    mockConnectionException = false;
  }
}
