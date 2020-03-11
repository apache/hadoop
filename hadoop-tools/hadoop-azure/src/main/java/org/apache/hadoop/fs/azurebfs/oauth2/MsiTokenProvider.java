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

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides tokens based on Azure VM's Managed Service Identity.
 */
public class MsiTokenProvider extends AccessTokenProvider {

  private final String authEndpoint;

  private final String authority;

  private final String tenantGuid;

  private final String clientId;

  private long tokenFetchTime = -1;

  private static final long ONE_HOUR = 3600 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenProvider.class);

  public MsiTokenProvider(final String authEndpoint, final String tenantGuid,
      final String clientId, final String authority) {
    this.authEndpoint = authEndpoint;
    this.tenantGuid = tenantGuid;
    this.clientId = clientId;
    this.authority = authority;
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing token from MSI");
    AzureADToken token = AzureADAuthenticator
        .getTokenFromMsi(authEndpoint, tenantGuid, clientId, authority, false);
    tokenFetchTime = System.currentTimeMillis();
    return token;
  }

  /**
   * Checks if the token is about to expire as per base expiry logic.
   * Otherwise try to expire every 1 hour
   *
   * @return true if the token is expiring in next 1 hour or if a token has
   * never been fetched
   */
  @Override
  protected boolean isTokenAboutToExpire() {
    if (tokenFetchTime == -1 || super.isTokenAboutToExpire()) {
      return true;
    }

    boolean expiring = false;
    long elapsedTimeSinceLastTokenRefreshInMillis =
        System.currentTimeMillis() - tokenFetchTime;
    expiring = elapsedTimeSinceLastTokenRefreshInMillis >= ONE_HOUR
        || elapsedTimeSinceLastTokenRefreshInMillis < 0;
    // In case of, Token is not refreshed for 1 hr or any clock skew issues,
    // refresh token.
    if (expiring) {
      LOG.debug("MSIToken: token renewing. Time elapsed since last token fetch:"
          + " {} milli seconds", elapsedTimeSinceLastTokenRefreshInMillis);
    }

    return expiring;
  }

}
