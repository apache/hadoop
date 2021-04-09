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
import java.net.URLEncoder;
import java.util.regex.Pattern;

/**
 * Utility class to help with Abfs url transformation to blob urls.
 */
public final class UriUtils {
  private static final String ABFS_URI_REGEX = "[^.]+\\.dfs\\.(preprod\\.){0,1}core\\.windows\\.net";
  private static final Pattern ABFS_URI_PATTERN = Pattern.compile(ABFS_URI_REGEX);
  public static final String SIGNATURE_QUERY_PARAM_KEY = "sig=";
  private static final String[] MASK_PARAM_KEYS = {"skoid=", "saoid=", "suoid=",
      SIGNATURE_QUERY_PARAM_KEY};

  /**
   * Checks whether a string includes abfs url.
   * @param string the string to check.
   * @return true if string has abfs url.
   */
  public static boolean containsAbfsUrl(final String string) {
    if (string == null || string.isEmpty()) {
      return false;
    }

    return ABFS_URI_PATTERN.matcher(string).matches();
  }

  /**
   * Extracts the account name from the host name.
   * @param hostName the fully-qualified domain name of the storage service
   *                 endpoint (e.g. {account}.dfs.core.windows.net.
   * @return the storage service account name.
   */
  public static String extractAccountNameFromHostName(final String hostName) {
    if (hostName == null || hostName.isEmpty()) {
      return null;
    }

    if (!containsAbfsUrl(hostName)) {
      return null;
    }

    String[] splitByDot = hostName.split("\\.");
    if (splitByDot.length == 0) {
      return null;
    }

    return splitByDot[0];
  }

  /**
   * Generate unique test path for multiple user tests.
   *
   * @return root test path
   */
  public static String generateUniqueTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null ? "/test" : "/" + testUniqueForkId + "/test";
  }

  public static String encodedUrlStr(String url) {
    try {
      return URLEncoder.encode(url, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return "https%3A%2F%2Ffailed%2Fto%2Fencode%2Furl";
    }
  }

  public static String getMaskedUrl(String url) {
    return maskSigAndOIDs(url);
  }

  public static String getMaskedEncodedUrl(String url) {
    return encodedUrlStr(getMaskedUrl(url));
  }

  public static String maskSigAndOIDs(String url) {
    for (String qpKey : MASK_PARAM_KEYS) {
      int qpStrIdx = url.indexOf('&' + qpKey);
      if (qpStrIdx == -1) {
        qpStrIdx = url.indexOf('?' + qpKey);
        if (qpStrIdx == -1) {
          continue;
        }
      }
      int startIdx = qpStrIdx + qpKey.length() + 1;
      int ampIdx = url.indexOf("&", startIdx);
      int endIndex = (ampIdx != -1) ? ampIdx : url.length();
      if (qpKey.equals(SIGNATURE_QUERY_PARAM_KEY)) {
        String signature = url.substring(startIdx, endIndex);
        url = url.replace(signature, "XXXX");
      } else {
        url = url.substring(0, startIdx + 4) + "XXXX" + url.substring(endIndex);
      }
    }
    return url;
  }

  private UriUtils() {
  }
}
