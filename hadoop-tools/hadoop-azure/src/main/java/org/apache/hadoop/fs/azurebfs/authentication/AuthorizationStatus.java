/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.authentication;

import org.apache.hadoop.fs.azurebfs.extensions.AuthorizationResourceResult;
import org.apache.hadoop.fs.azurebfs.extensions.AuthorizationResult;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_SAS_REFRESH_INTERVAL_BEFORE_EXPIRY;

/**
 * AuthorizationStatus maintains the status of Authorization and also SAS
 * token if authorizer is providing it.
 */
public class AuthorizationStatus {
  private boolean isAuthorized;
  private HashMap<URI, SasTokenData> sasTokenMap;

  public AuthorizationStatus() {
    sasTokenMap = new HashMap<>();
  }

  /**
   * Fetch the SAS token
   *
   * @return
   */
  public String getSasTokenQuery(URI storePathUri) {
    if (sasTokenMap.containsKey(storePathUri)) {
      SasTokenData sasTokenData = sasTokenMap.get(storePathUri);
      if (isValidSas(sasTokenData)) {
        return sasTokenData.sasToken;
      }
    }

    return null;
  }

  /**
   * Update authTokenMap
   * Also update the refresh interval for each SAS token.
   *
   * @param authResult - Authorizer AuthorizationResult
   */
  public void setSasToken(AuthorizationResult authResult) {

    AuthorizationResourceResult[] resourceResult = authResult
        .getAuthResourceResult();

    for (AuthorizationResourceResult singleResourceAuth : resourceResult) {
      SasTokenData authToken = new SasTokenData();

      // By default SASToken will be set for refresh
      // DEFAULT_SAS_REFRESH_INTERVAL_BEFORE_EXPIRY (5 mins) seconds before
      // expiry
      // If the sas token is short lived and below 5 mins, set the sas token
      // refresh to be half time before expiry
      authToken.sasExpiryTime = getSasExpiryDateTime(
          singleResourceAuth.authToken);

      authToken.sasToken = singleResourceAuth.authToken;
      long durationToExpiryInSec = (
          Duration.between(authToken.sasExpiryTime, Instant.now()).toMillis()
              / 1000);

      if (durationToExpiryInSec < DEFAULT_SAS_REFRESH_INTERVAL_BEFORE_EXPIRY) {
        authToken.sasRefreshIntervalBeforeExpiryInSec =
            (int) durationToExpiryInSec / 2;
      }

      sasTokenMap.put(singleResourceAuth.storePathUri, authToken);
    }
  }

  /**
   * Fetch SAS token expiry
   *
   * @return
   */
  private Instant getSasExpiryDateTime(String sasToken) {
    int startIndex = sasToken.indexOf("ske");
    int endIndex = sasToken.indexOf("&", startIndex);
    if (endIndex == -1) {
      endIndex = sasToken.length();
    }
    String ske = sasToken.substring(sasToken.indexOf("ske") + 4, // remove ske=
        endIndex);

    return Instant.parse(ske);
  }

  /**
   * Check if the SAS is valid and if it needs update
   *
   * @return true if SAS token is valid, false otherwise
   */
  public boolean isValidSas(SasTokenData sasTokenData) {
    String sasTokenQuery = sasTokenData.sasToken;

    if ((sasTokenQuery == null) || sasTokenQuery.isEmpty()) {
      // If there is no SAS
      return false;
    }

    Instant currentDateTime = Instant.now();

    // if expiry is within configured refresh interval,
    // SAS token needs update. Return status as invalid.
    return sasTokenData.sasExpiryTime.isBefore(currentDateTime
        .minusSeconds(sasTokenData.sasRefreshIntervalBeforeExpiryInSec));
  }

  /**
   * Fetches and checks SAS token for provided Store Path URI
   *
   * @param storepathUri
   * @return
   */
  public boolean isValidSas(URI storepathUri) {
    if (sasTokenMap.containsKey(storepathUri)) {
      return isValidSas(sasTokenMap.get(storepathUri));
    }

    return false;
  }

  public boolean isAuthorized() {
    return isAuthorized;
  }

  public void setAuthorized(boolean authorized) {
    isAuthorized = authorized;
  }
}