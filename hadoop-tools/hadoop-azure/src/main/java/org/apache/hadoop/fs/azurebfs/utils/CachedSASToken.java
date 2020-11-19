/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * CachedSASToken provides simple utility for managing renewal
 * of SAS tokens used by Input/OutputStream.  This enables SAS re-use
 * and reduces calls to the SASTokenProvider.
 */
public final class CachedSASToken {
  public static final Logger LOG = LoggerFactory.getLogger(CachedSASToken.class);
  private final long minExpirationInSeconds;
  private String sasToken;
  private OffsetDateTime sasExpiry;

  /**
   * Create instance with default minimum expiration.  SAS tokens are
   * automatically renewed when their expiration is within this period.
   */
  public CachedSASToken() {
    this(DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS);
  }

  /**
   * Create instance with specified minimum expiration.  SAS tokens are
   * automatically renewed when their expiration is within this period.
   * @param minExpirationInSeconds
   */
  public CachedSASToken(long minExpirationInSeconds) {
    this.minExpirationInSeconds = minExpirationInSeconds;
  }

  /**
   * Checks if the SAS token is expired or near expiration.
   * @param expiry
   * @param minExpiryInSeconds
   * @return true if the SAS is near sasExpiry; otherwise false
   */
  private static boolean isNearExpiry(OffsetDateTime expiry, long minExpiryInSeconds) {
    if (expiry == OffsetDateTime.MIN) {
      return true;
    }
    OffsetDateTime utcNow = OffsetDateTime.now(ZoneOffset.UTC);
    return utcNow.until(expiry, SECONDS) <= minExpiryInSeconds;
  }

  /**
   * Parse the sasExpiry from the SAS token.  The sasExpiry is the minimum
   * of the ske and se parameters.  The se parameter is required and the
   * ske parameter is optional.
   * @param token an Azure Storage SAS token
   * @return the sasExpiry or OffsetDateTime.MIN if invalid.
   */
  private static OffsetDateTime getExpiry(String token) {
    // return MIN for all invalid input, including a null token
    if (token == null) {
      return OffsetDateTime.MIN;
    }

    String signedExpiry = "se=";
    int signedExpiryLen = 3;

    int start = token.indexOf(signedExpiry);

    // return MIN if the required se parameter is absent
    if (start == -1) {
      return OffsetDateTime.MIN;
    }

    start += signedExpiryLen;

    // extract the value of se parameter
    int end = token.indexOf("&", start);
    String seValue = (end == -1) ? token.substring(start) : token.substring(start, end);

    try {
      seValue = URLDecoder.decode(seValue, "utf-8");
    } catch (UnsupportedEncodingException ex) {
      LOG.error("Error decoding se query parameter ({}) from SAS.", seValue, ex);
      return OffsetDateTime.MIN;
    }

    // parse the ISO 8601 date value; return MIN if invalid
    OffsetDateTime seDate = OffsetDateTime.MIN;
    try {
      seDate = OffsetDateTime.parse(seValue, DateTimeFormatter.ISO_DATE_TIME);
    } catch (DateTimeParseException ex) {
      LOG.error("Error parsing se query parameter ({}) from SAS.", seValue, ex);
    }

    String signedKeyExpiry = "ske=";
    int signedKeyExpiryLen = 4;

    // if ske is present, the sasExpiry is the minimum of ske and se
    start = token.indexOf(signedKeyExpiry);

    // return seDate if ske is absent
    if (start == -1) {
      return seDate;
    }

    start += signedKeyExpiryLen;

    // extract the value of ske parameter
    end = token.indexOf("&", start);
    String skeValue = (end == -1) ? token.substring(start) : token.substring(start, end);

    try {
      skeValue = URLDecoder.decode(skeValue, "utf-8");
    } catch (UnsupportedEncodingException ex) {
      LOG.error("Error decoding ske query parameter ({}) from SAS.", skeValue, ex);
      return OffsetDateTime.MIN;
    }

    // parse the ISO 8601 date value; return MIN if invalid
    OffsetDateTime skeDate = OffsetDateTime.MIN;
    try {
      skeDate = OffsetDateTime.parse(skeValue, DateTimeFormatter.ISO_DATE_TIME);
    } catch (DateTimeParseException ex) {
      LOG.error("Error parsing ske query parameter ({}) from SAS.", skeValue, ex);
      return OffsetDateTime.MIN;
    }

    return skeDate.isBefore(seDate) ? skeDate : seDate;
  }

  /**
   * Updates the cached SAS token and expiry.  If the token is invalid, the cached value
   * is cleared by setting it to null and the expiry to MIN.
   * @param token an Azure Storage SAS token
   */
  public void update(String token) {
    // quickly return if token and cached sasToken are the same reference
    // Note: use of operator == is intentional
    if (token == sasToken) {
      return;
    }
    OffsetDateTime newExpiry = getExpiry(token);
    boolean isInvalid = isNearExpiry(newExpiry, minExpirationInSeconds);
    synchronized (this) {
      if (isInvalid) {
        sasToken = null;
        sasExpiry = OffsetDateTime.MIN;
      } else {
        sasToken = token;
        sasExpiry = newExpiry;
      }
    }
  }

  /**
   * Gets the token if still valid.
   * @return the token or null if it is expired or near sasExpiry.
   */
  public String get() {
    // quickly return null if not set
    if (sasToken == null) {
      return null;
    }
    String token;
    OffsetDateTime exp;
    synchronized (this) {
      token = sasToken;
      exp = sasExpiry;
    }
    boolean isInvalid = isNearExpiry(exp, minExpirationInSeconds);
    return isInvalid ? null : token;
  }

  @VisibleForTesting
  void setForTesting(String token, OffsetDateTime expiry) {
    synchronized (this) {
      sasToken = token;
      sasExpiry = expiry;
    }
  }
}