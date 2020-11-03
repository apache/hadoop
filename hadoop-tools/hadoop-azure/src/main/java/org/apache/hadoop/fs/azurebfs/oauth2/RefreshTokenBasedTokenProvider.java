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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides tokens based on refresh token.
 */
public class RefreshTokenBasedTokenProvider extends AccessTokenProvider {
  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenProvider.class);

  private final String authEndpoint;

  private final String clientId;

  private final String refreshToken;

  /**
   * Constructs a token provider based on the refresh token provided.
   *
   * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
   * @param refreshToken the refresh token
   */
  public RefreshTokenBasedTokenProvider(final String authEndpoint,
      String clientId, String refreshToken) {
    Preconditions.checkNotNull(authEndpoint, "authEndpoint");
    Preconditions.checkNotNull(clientId, "clientId");
    Preconditions.checkNotNull(refreshToken, "refreshToken");
    this.authEndpoint = authEndpoint;
    this.clientId = clientId;
    this.refreshToken = refreshToken;
  }


  @Override
  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing refresh-token based token");
    return AzureADAuthenticator
        .getTokenUsingRefreshToken(authEndpoint, clientId, refreshToken);
  }
}
