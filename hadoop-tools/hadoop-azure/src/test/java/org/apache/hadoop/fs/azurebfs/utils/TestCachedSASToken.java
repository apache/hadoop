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

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.DAYS;

/**
 * Test CachedSASToken.
 */
public final class TestCachedSASToken {

  @Test
  public void testUpdateAndGet() throws IOException {
    CachedSASToken cachedSasToken = new CachedSASToken();

    String se1 = OffsetDateTime.now(ZoneOffset.UTC).plus(
        DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS * 2,
        SECONDS).format(DateTimeFormatter.ISO_DATE_TIME);
    String token1 = "se=" + se1;

    // set first time and ensure reference equality
    cachedSasToken.update(token1);
    String cachedToken = cachedSasToken.get();
    Assert.assertTrue(token1 == cachedToken);

    // update with same token and ensure reference equality
    cachedSasToken.update(token1);
    cachedToken = cachedSasToken.get();
    Assert.assertTrue(token1 == cachedToken);

    // renew and ensure reference equality
    String se2 = OffsetDateTime.now(ZoneOffset.UTC).plus(
        DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS * 2,
        SECONDS).format(DateTimeFormatter.ISO_DATE_TIME);
    String token2 = "se=" + se2;
    cachedSasToken.update(token2);
    cachedToken = cachedSasToken.get();
    Assert.assertTrue(token2 == cachedToken);

    // renew and ensure reference equality with ske
    String se3 = OffsetDateTime.now(ZoneOffset.UTC).plus(
        DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS * 4,
        SECONDS).format(DateTimeFormatter.ISO_DATE_TIME);

    String ske3 = OffsetDateTime.now(ZoneOffset.UTC).plus(
        DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS * 2,
        SECONDS).format(DateTimeFormatter.ISO_DATE_TIME);
    String token3 = "se=" + se3 + "&ske=" + ske3;
    cachedSasToken.update(token3);
    cachedToken = cachedSasToken.get();
    Assert.assertTrue(token3 == cachedToken);
  }

  @Test
  public void testGetExpiration() throws IOException {
    CachedSASToken cachedSasToken = new CachedSASToken();

    String se = OffsetDateTime.now(ZoneOffset.UTC).plus(
        DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS - 1,
        SECONDS).format(DateTimeFormatter.ISO_DATE_TIME);
    OffsetDateTime seDate = OffsetDateTime.parse(se, DateTimeFormatter.ISO_DATE_TIME);
    String token = "se=" + se;

    // By-pass the normal validation provided by update method
    // by callng set with expired SAS, then ensure the get
    // method returns null (auto expiration as next REST operation will use
    // SASTokenProvider to get a new SAS).
    cachedSasToken.setForTesting(token, seDate);
    String cachedToken = cachedSasToken.get();
    Assert.assertNull(cachedToken);
  }

  @Test
  public void testUpdateAndGetWithExpiredToken() throws IOException {
    CachedSASToken cachedSasToken = new CachedSASToken();

    String se1 = OffsetDateTime.now(ZoneOffset.UTC).plus(
        DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS - 1,
        SECONDS).format(DateTimeFormatter.ISO_DATE_TIME);
    String token1 = "se=" + se1;

    // set expired token and ensure not cached
    cachedSasToken.update(token1);
    String cachedToken = cachedSasToken.get();
    Assert.assertNull(cachedToken);

    String se2 = OffsetDateTime.now(ZoneOffset.UTC).plus(
        DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS * 2,
        SECONDS).format(DateTimeFormatter.ISO_DATE_TIME);

    String ske2 = OffsetDateTime.now(ZoneOffset.UTC).plus(
        DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS - 1,
        SECONDS).format(DateTimeFormatter.ISO_DATE_TIME);
    String token2 = "se=" + se2 + "&ske=" + ske2;

    // set with expired ske and ensure not cached
    cachedSasToken.update(token2);
    cachedToken = cachedSasToken.get();
    Assert.assertNull(cachedToken);

  }

  @Test
  public void testUpdateAndGetWithInvalidToken() throws IOException {
    CachedSASToken cachedSasToken = new CachedSASToken();

    // set and ensure reference that it is not cached
    String token1 = "se=";
    cachedSasToken.update(token1);
    String cachedToken = cachedSasToken.get();
    Assert.assertNull(cachedToken);

    // set and ensure reference that it is not cached
    String token2 = "se=xyz";
    cachedSasToken.update(token2);
    cachedToken = cachedSasToken.get();
    Assert.assertNull(cachedToken);

    // set and ensure reference that it is not cached
    String token3 = "se=2100-01-01T00:00:00Z&ske=";
    cachedSasToken.update(token3);
    cachedToken = cachedSasToken.get();
    Assert.assertNull(cachedToken);

    // set and ensure reference that it is not cached
    String token4 = "se=2100-01-01T00:00:00Z&ske=xyz&";
    cachedSasToken.update(token4);
    cachedToken = cachedSasToken.get();
    Assert.assertNull(cachedToken);

    // set and ensure reference that it is not cached
    String token5 = "se=abc&ske=xyz&";
    cachedSasToken.update(token5);
    cachedToken = cachedSasToken.get();
    Assert.assertNull(cachedToken);
  }

  public static CachedSASToken getTestCachedSASTokenInstance() {
    String expiryPostADay = OffsetDateTime.now(ZoneOffset.UTC)
        .plus(1, DAYS)
        .format(DateTimeFormatter.ISO_DATE_TIME);
    String version = "2020-20-20";

    StringBuilder sb = new StringBuilder();
    sb.append("skoid=");
    sb.append(UUID.randomUUID().toString());
    sb.append("&sktid=");
    sb.append(UUID.randomUUID().toString());
    sb.append("&skt=");
    sb.append(OffsetDateTime.now(ZoneOffset.UTC)
        .minus(1, DAYS)
        .format(DateTimeFormatter.ISO_DATE_TIME));
    sb.append("&ske=");
    sb.append(expiryPostADay);
    sb.append("&sks=b");
    sb.append("&skv=");
    sb.append(version);
    sb.append("&sp=rw");
    sb.append("&sr=b");
    sb.append("&se=");
    sb.append(expiryPostADay);
    sb.append("&sv=2");
    sb.append(version);

    CachedSASToken cachedSASToken = new CachedSASToken();
    cachedSASToken.update(sb.toString());
    return cachedSASToken;
  }
}