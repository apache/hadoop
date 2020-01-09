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

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.http.client.utils.URIBuilder;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.regex.Pattern;

/**
 * Utility class to help with Abfs url transformation to blob urls.
 */
public final class UriUtils {
  private static final String ABFS_URI_REGEX = "[^.]+\\.dfs\\.(preprod\\.){0,1}core\\.windows\\.net";
  private static final Pattern ABFS_URI_PATTERN = Pattern.compile(ABFS_URI_REGEX);

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

  private UriUtils() {
  }

  public static URL addSASToRequestUrl(URL requestUrl, String sasToken)
      throws URISyntaxException, MalformedURLException {
    URIBuilder uriBuilder = new URIBuilder(requestUrl.toURI());
    String[] sasQueryParamKVPairs = sasToken.split("&");
    for(String sasQueryParam : sasQueryParamKVPairs)
    {
      String[] queryParam = sasQueryParam.split("=");
      String key = queryParam[0];
      String value = queryParam[1];

      uriBuilder.addParameter(key, value);
    }

    return uriBuilder.build().toURL();
  }

  public static String[] authorityParts(URI uri) throws
      InvalidUriAuthorityException, InvalidUriException {
    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || authorityParts[0] != null
        && authorityParts[0].isEmpty()) {
      final String errMsg = String
          .format("'%s' has a malformed authority, expected container name. "
                  + "Authority takes the form "
                  + FileSystemUriSchemes.ABFS_SCHEME + "://[<container name>@]<account name>",
              uri.toString());
      throw new InvalidUriException(errMsg);
    }
    return authorityParts;
  }
}
