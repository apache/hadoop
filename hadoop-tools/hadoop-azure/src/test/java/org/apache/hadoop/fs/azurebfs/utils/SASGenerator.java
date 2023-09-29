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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.util.Locale;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Test SAS generator.
 */
public abstract class SASGenerator {

  public enum AuthenticationVersion {
    Nov18("2018-11-09"),
    Dec19("2019-12-12"),
    Feb20("2020-02-10");

    private final String ver;

    AuthenticationVersion(String version) {
      this.ver = version;
    }

    @Override
    public String toString() {
      return ver;
    }
  }

  protected static final Logger LOG = LoggerFactory.getLogger(SASGenerator.class);
  public static final Duration FIVE_MINUTES = Duration.ofMinutes(5);
  public static final Duration ONE_DAY = Duration.ofDays(1);
  public static final DateTimeFormatter ISO_8601_FORMATTER =
      DateTimeFormatter
          .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT)
          .withZone(ZoneId.of("UTC"));

  private Mac hmacSha256;
  private byte[] key;

  // hide default constructor
  private SASGenerator() {
  }

  /**
   * Called by subclasses to initialize the cryptographic SHA-256 HMAC provider.
   * @param key - a 256-bit secret key
   */
  protected SASGenerator(byte[] key) {
    this.key = key;
    initializeMac();
  }

  private void initializeMac() {
    // Initializes the HMAC-SHA256 Mac and SecretKey.
    try {
      hmacSha256 = Mac.getInstance("HmacSHA256");
      hmacSha256.init(new SecretKeySpec(key, "HmacSHA256"));
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  protected String computeHmac256(final String stringToSign) {
    byte[] utf8Bytes;
    try {
      utf8Bytes = stringToSign.getBytes(StandardCharsets.UTF_8.toString());
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