package org.apache.hadoop.fs.azurebfs.services;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_BG_FASTPATH_SSSN_UPD_INTERVAL_SEC;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FASTPATH_SSSN_UPD_INTERVAL_SEC;

public class AbfsFastpathSessionInfo {
  protected static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);
  String sessionToken;
  OffsetDateTime sasTokenExpiry;
  String fileHandle = null;

  AtomicBoolean refreshInitiated = new AtomicBoolean(false);

  public void updateSession(String sessionToken, OffsetDateTime sasTokenExpiry) {
    this.sessionToken = sessionToken;
    this.sasTokenExpiry = sasTokenExpiry;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public OffsetDateTime getExpiry() {
    return sasTokenExpiry;
  }

  public String getFileHandle() {
    return fileHandle;
  }

  public void setFileHandle(final String fileHandle) {
    this.fileHandle = fileHandle;
  }

  public void setRefreshInitiated(final boolean refreshInitiated) {
    this.refreshInitiated.set(refreshInitiated);
  }

  public boolean isBeingRefreshed() {
    return refreshInitiated.get();
  }

  public boolean needsActiveThreadRefresh() {
    return isNearExpiry(sasTokenExpiry,
        DEFAULT_FASTPATH_SSSN_UPD_INTERVAL_SEC);
  }

  public boolean needsBackgroundThreadRefresh() {
    return isNearExpiry(sasTokenExpiry,
        DEFAULT_BG_FASTPATH_SSSN_UPD_INTERVAL_SEC);
  }

  private static boolean isNearExpiry(OffsetDateTime expiry, long minExpiryInSeconds) {
    if (expiry == OffsetDateTime.MIN) {
      return true;
    }

    OffsetDateTime utcNow = OffsetDateTime.now(ZoneOffset.UTC);
    return utcNow.until(expiry, SECONDS) <= minExpiryInSeconds;
  }

}
