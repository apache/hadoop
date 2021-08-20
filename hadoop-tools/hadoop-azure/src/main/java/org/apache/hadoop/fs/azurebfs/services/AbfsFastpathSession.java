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

import java.nio.ByteBuffer;
import java.time.format.DateTimeFormatter;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Date;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_EXPIRY;

public class AbfsFastpathSession {
  protected static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);
  //Diff between Filetime epoch and Unix epoch (in ms)
  private static final long FILETIME_EPOCH_DIFF = 11644473600000L;
  // 1ms in units of nanoseconds
  private static final long FILETIME_ONE_MILLISECOND = 10 * 1000;
  private static final double SESSION_REFRESH_INTERVAL_FACTOR = 0.75;
  protected final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  private AbfsFastpathSessionInfo fastpathSessionInfo;
  private AbfsClient client;
  private String path;
  private String eTag;
  private TracingContext tracingContext;
  private final ScheduledExecutorService scheduledExecutorService
      = Executors.newScheduledThreadPool(1);
  private int sessionRefreshIntervalInSec = -1;

  public AbfsFastpathSession(final AbfsClient client,
      final String path,
      final String eTag,
      TracingContext tracingContext) {
    this.client = client;
    this.path = path;
    this.eTag = eTag;
    this.tracingContext = tracingContext;
    fetchSessionTokenAndFileHandle();
  }

  /**
   * This returns a snap of the current sessionInfo
   * SessionInfo updates can happen for various reasons:
   * 1. Request processing threads are changing connection mode to retry
   * store request (incase of reads, readAhead threads)
   * 2. session update (success or failure)
   * @return abfsFastpathSessionInfo instance
   */
  public AbfsFastpathSessionInfo getCurrentAbfsFastpathSessionInfoCopy() {
    rwLock.readLock().lock();
    try {
      if (fastpathSessionInfo.isValidSession()) {
        return new AbfsFastpathSessionInfo(fastpathSessionInfo);
      }

      LOG.debug("There is no valid Fastpath session currently");
      return null;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public void updateConnectionModeForFailures(AbfsConnectionMode connectionMode) {
    // Fastpath connection and session refresh failures are not recoverable,
    // update connection mode if that happens
    if ((connectionMode == AbfsConnectionMode.REST_ON_FASTPATH_CONN_FAILURE)
        || (connectionMode == AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE)) {
      LOG.debug(
          "{}: Switching to error connection mode : {}",
          Thread.currentThread().getName(), connectionMode);
      updateConnectionMode(connectionMode);
    }
  }

  public void close() {
    if ((fastpathSessionInfo != null)
        && (fastpathSessionInfo.getFastpathFileHandle() != null)) {
      try {
        executeFastpathClose();
        updateConnectionMode(AbfsConnectionMode.REST_CONN);
      } catch (AzureBlobFileSystemException e) {
        LOG.debug("Fastpath handle close failed - {} - {}",
            fastpathSessionInfo.getFastpathFileHandle(), e);
      }
    }
  }

  protected void fetchSessionTokenAndFileHandle() {
    fetchFastpathSessionToken();
    if ((fastpathSessionInfo != null)
        && (fastpathSessionInfo.isValidSession())) {
      fetchFastpathFileHandle();
    }
  }

  @VisibleForTesting
  protected void updateAbfsFastpathSessionToken(String token, OffsetDateTime expiry) {
    rwLock.writeLock().lock();
    try {
      if (fastpathSessionInfo == null) {
        fastpathSessionInfo = new AbfsFastpathSessionInfo(token, expiry);
      } else {
        fastpathSessionInfo.updateSessionToken(token, expiry);
      }

      OffsetDateTime utcNow = OffsetDateTime.now(ZoneOffset.UTC);
      sessionRefreshIntervalInSec = (int) Math.floor(
          utcNow.until(expiry, ChronoUnit.SECONDS)
              * SESSION_REFRESH_INTERVAL_FACTOR);

      // 0 or negative sessionRefreshIntervalInSec indicates a session token
      // whose expiry is near as soon as its received. This will end up
      // generating a lot of REST calls refreshing the session. Better to
      // switch off Fastpath in that case.
      if (sessionRefreshIntervalInSec <= 0) {
        LOG.debug(
            "Expiry time at present or past. Drop Fastpath session (could be clock skew). Received expiry {} ",
            expiry);
        tracingContext.setConnectionMode(
            AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
        fastpathSessionInfo.setConnectionMode(
            AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
        return;
      }

      // schedule for refresh right away
      scheduledExecutorService.schedule(() -> {
        fetchFastpathSessionToken();
      }, sessionRefreshIntervalInSec, TimeUnit.SECONDS);
      LOG.debug(
          "Fastpath session token fetch successful, valid till {}. Refresh scheduled after {} secs",
          expiry, sessionRefreshIntervalInSec);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  protected void updateConnectionMode(AbfsConnectionMode connectionMode) {
    rwLock.writeLock().lock();
    try {
      tracingContext.setConnectionMode(connectionMode);
      if (fastpathSessionInfo != null) {
        fastpathSessionInfo.setConnectionMode(connectionMode);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private void updateFastpathFileHandle(String fileHandle) {
    rwLock.writeLock().lock();
    try {
      fastpathSessionInfo.setFastpathFileHandle(fileHandle);
      LOG.debug("Fastpath handled opened {}", fastpathSessionInfo.getFastpathFileHandle());
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  boolean fetchFastpathSessionToken() {
    if ((fastpathSessionInfo != null)
        && (fastpathSessionInfo.getConnectionMode()
        != AbfsConnectionMode.FASTPATH_CONN)) {
      // no need to refresh or schedule another
      return false;
    }

    try {
      AbfsRestOperation op = executeFetchFastpathSessionToken();
      byte[] buffer = new byte[Integer.parseInt(
          op.getResult().getResponseHeader(CONTENT_LENGTH))];
      op.getResult().getResponseContentBuffer(buffer);
      updateAbfsFastpathSessionToken(Base64.getEncoder().encodeToString(buffer),
          getExpiry(buffer,
              op.getResult().getResponseHeader(X_MS_FASTPATH_SESSION_EXPIRY)));
      return true;
    } catch (AzureBlobFileSystemException e) {
      LOG.debug("Fastpath session token fetch unsuccessful {}", e);
      updateConnectionMode(
          AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
    }

    return false;
  }

  protected OffsetDateTime getExpiry(byte[] tokenBuffer, String expiryHeader) {
    if (expiryHeader != null && !expiryHeader.isEmpty()) {
      return OffsetDateTime.parse(expiryHeader, DateTimeFormatter.RFC_1123_DATE_TIME);
    }

    // if header is absent
    ByteBuffer bb = ByteBuffer.allocate(tokenBuffer.length).order(
        java.nio.ByteOrder.LITTLE_ENDIAN);
    bb.put(tokenBuffer);
    bb.rewind();
    long w32FileTime = bb.getLong(8);
    Date date = new Date((w32FileTime/ FILETIME_ONE_MILLISECOND) - + FILETIME_EPOCH_DIFF);
    return date.toInstant().atOffset(ZoneOffset.UTC);
  }

  @VisibleForTesting
  boolean fetchFastpathFileHandle() {
    try {
      AbfsRestOperation op = executeFastpathOpen();
      String fileHandle
          = ((AbfsFastpathConnection) op.getResult()).getFastpathFileHandle();
      updateFastpathFileHandle(fileHandle);
      return true;
    } catch (AzureBlobFileSystemException e) {
      LOG.debug("Fastpath open failed with {}", e);
      updateConnectionMode(AbfsConnectionMode.REST_ON_FASTPATH_CONN_FAILURE);
    }

    return false;
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFetchFastpathSessionToken()
      throws AzureBlobFileSystemException {
    return client.getReadFastpathSessionToken(path, eTag, tracingContext);
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFastpathOpen()
      throws AzureBlobFileSystemException {
    return client.fastPathOpen(path, eTag, fastpathSessionInfo, tracingContext);
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFastpathClose()
      throws AzureBlobFileSystemException {
    return client.fastPathClose(path, eTag, fastpathSessionInfo,
        tracingContext);
  }

  @VisibleForTesting
  int getSessionRefreshIntervalInSec() {
    rwLock.readLock().lock();
    try {
      return sessionRefreshIntervalInSec;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @VisibleForTesting
  void setConnectionMode(AbfsConnectionMode connMode) {
    updateConnectionMode(connMode);
  }

  @VisibleForTesting
  protected String getPath() {
    return path;
  }

  @VisibleForTesting
  protected String geteTag() {
    return eTag;
  }

  @VisibleForTesting
  protected TracingContext getTracingContext() {
    return tracingContext;
  }

  @VisibleForTesting
  protected AbfsClient getClient() {
    return client;
  }

  @VisibleForTesting
  protected AbfsFastpathSessionInfo getFastpathSessionInfo() {
    return fastpathSessionInfo;
  }

  @VisibleForTesting
  public static double getSessionRefreshIntervalFactor() {
    return SESSION_REFRESH_INTERVAL_FACTOR;
  }
}
