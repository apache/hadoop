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

package org.apache.hadoop.fs.azurebfs.utils;

import java.io.UnsupportedEncodingException;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Locale;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.services.AbfsUriQueryBuilder;

/**
 * Test container SAS generator.
 */
public class SASGenerator {

  private static final String HMAC_SHA256 = "HmacSHA256";
  private static final int TOKEN_START_PERIOD_IN_SECONDS = 5 * 60;
  private static final int TOKEN_EXPIRY_PERIOD_IN_SECONDS = 24 * 60 * 60;
  public static final DateTimeFormatter ISO_8601_UTC_DATE_FORMATTER =
      DateTimeFormatter
          .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT)
          .withZone(ZoneId.of("UTC"));
  private Mac hmacSha256;
  private byte[] key;

  public SASGenerator(byte[] key) {
    this.key = key;
    initializeMac();
  }

  public String getContainerSASWithFullControl(String accountName, String containerName) {
    String sp = "rcwdl";
    String sv = "2018-11-09";
    String sr = "c";
    String st = ISO_8601_UTC_DATE_FORMATTER.format(Instant.now().minusSeconds(TOKEN_START_PERIOD_IN_SECONDS));
    String se =
        ISO_8601_UTC_DATE_FORMATTER.format(Instant.now().plusSeconds(TOKEN_EXPIRY_PERIOD_IN_SECONDS));

    String signature = computeSignatureForSAS(sp, st, se, sv, "c",
        accountName, containerName);

    AbfsUriQueryBuilder qb = new AbfsUriQueryBuilder();
    qb.addQuery("sp", sp);
    qb.addQuery("st", st);
    qb.addQuery("se", se);
    qb.addQuery("sv", sv);
    qb.addQuery("sr", sr);
    qb.addQuery("sig", signature);
    return qb.toString().substring(1);
  }

  private String computeSignatureForSAS(String sp, String st,
      String se, String sv, String sr, String accountName, String containerName) {

    StringBuilder sb = new StringBuilder();
    sb.append(sp);
    sb.append("\n");
    sb.append(st);
    sb.append("\n");
    sb.append(se);
    sb.append("\n");
    // canonicalized resource
    sb.append("/blob/");
    sb.append(accountName);
    sb.append("/");
    sb.append(containerName);
    sb.append("\n");
    sb.append("\n"); // si
    sb.append("\n"); // sip
    sb.append("\n"); // spr
    sb.append(sv);
    sb.append("\n");
    sb.append(sr);
    sb.append("\n");
    sb.append("\n"); // - For optional : rscc - ResponseCacheControl
    sb.append("\n"); // - For optional : rscd - ResponseContentDisposition
    sb.append("\n"); // - For optional : rsce - ResponseContentEncoding
    sb.append("\n"); // - For optional : rscl - ResponseContentLanguage
    sb.append("\n"); // - For optional : rsct - ResponseContentType

    String stringToSign = sb.toString();
    return computeHmac256(stringToSign);
  }

  private void initializeMac() {
    // Initializes the HMAC-SHA256 Mac and SecretKey.
    try {
      hmacSha256 = Mac.getInstance(HMAC_SHA256);
      hmacSha256.init(new SecretKeySpec(key, HMAC_SHA256));
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private String computeHmac256(final String stringToSign) {
    byte[] utf8Bytes;
    try {
      utf8Bytes = stringToSign.getBytes(AbfsHttpConstants.UTF_8);
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }
    byte[] hmac;
    synchronized (this) {
      hmac = hmacSha256.doFinal(utf8Bytes);
    }
    return Base64.encode(hmac);
  }
}
