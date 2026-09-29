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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

/**
 * Provides tokens based on Azure AD Workload Identity.
 */
public class WorkloadIdentityTokenProvider extends AccessTokenProvider {

  private static final String OAUTH2_TOKEN_PATH = "/oauth2/v2.0/token";
  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenProvider.class);
  private static final String EMPTY_TOKEN_FILE_ERROR = "Empty token file found at specified path: ";
  private static final String TOKEN_FILE_READ_ERROR = "Error reading token file at specified path: ";

  /**
   * Internal implementation of ClientAssertionProvider for file-based token reading.
   * This provides backward compatibility for the file-based constructor.
   */
  private static class FileBasedClientAssertionProvider implements ClientAssertionProvider {
    private final String tokenFile;

    FileBasedClientAssertionProvider(String tokenFile) {
      this.tokenFile = tokenFile;
    }

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {
      // No initialization needed for file-based provider
    }

    @Override
    public String getClientAssertion() throws IOException {
      String clientAssertion = EMPTY_STRING;
      try {
        File file = new File(tokenFile);
        clientAssertion = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new IOException(TOKEN_FILE_READ_ERROR + tokenFile, e);
      }
      clientAssertion = clientAssertion.trim();
      if (Strings.isNullOrEmpty(clientAssertion)) {
        throw new IOException(EMPTY_TOKEN_FILE_ERROR + tokenFile);
      }
      return clientAssertion;
    }
  }

  private final String authEndpoint;
  private final String clientId;
  private final ClientAssertionProvider clientAssertionProvider;
  private long tokenFetchTime = -1;

  /**
   * Constructor with custom ClientAssertionProvider.
   * Use this for custom token retrieval mechanisms like Kubernetes Token Request API.
   *
   * @param authority OAuth authority URL
   * @param tenantId Azure AD tenant ID
   * @param clientId Azure AD client ID
   * @param clientAssertionProvider Custom provider for client assertions
   */
  public WorkloadIdentityTokenProvider(final String authority, final String tenantId,
                                       final String clientId, ClientAssertionProvider clientAssertionProvider) {
    Preconditions.checkNotNull(authority, "authority");
    Preconditions.checkNotNull(tenantId, "tenantId");
    Preconditions.checkNotNull(clientId, "clientId");
    Preconditions.checkNotNull(clientAssertionProvider, "clientAssertionProvider");

    this.authEndpoint = authority + tenantId + OAUTH2_TOKEN_PATH;
    this.clientId = clientId;
    this.clientAssertionProvider = clientAssertionProvider;
  }

  /**
   * Constructor with file-based token reading (backward compatibility).
   *
   * @param authority OAuth authority URL
   * @param tenantId Azure AD tenant ID
   * @param clientId Azure AD client ID
   * @param tokenFile Path to file containing the JWT token
   */
  public WorkloadIdentityTokenProvider(final String authority, final String tenantId,
      final String clientId, final String tokenFile) {
    Preconditions.checkNotNull(authority, "authority");
    Preconditions.checkNotNull(tenantId, "tenantId");
    Preconditions.checkNotNull(clientId, "clientId");
    Preconditions.checkNotNull(tokenFile, "tokenFile");

    this.authEndpoint = authority + tenantId + OAUTH2_TOKEN_PATH;
    this.clientId = clientId;
    this.clientAssertionProvider = new FileBasedClientAssertionProvider(tokenFile);
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing token from JWT Assertion");
    String clientAssertion = clientAssertionProvider.getClientAssertion();
    AzureADToken token = getTokenUsingJWTAssertion(clientAssertion);
    tokenFetchTime = System.currentTimeMillis();
    return token;
  }

  /**
   * Checks if the token is about to expire as per base expiry logic.
   * Otherwise, expire if there is a clock skew issue in the system.
   *
   * @return true if the token is expiring in next 1 hour or if a token has
   * never been fetched
   */
  @Override
  protected boolean isTokenAboutToExpire() {
    if (tokenFetchTime == -1 || super.isTokenAboutToExpire()) {
      return true;
    }

    // In case of, any clock skew issues, refresh token.
    long elapsedTimeSinceLastTokenRefreshInMillis =
        System.currentTimeMillis() - tokenFetchTime;
    boolean expiring = elapsedTimeSinceLastTokenRefreshInMillis < 0;
    if (expiring) {
      // Clock Skew issue. Refresh token.
      LOG.debug("JWTToken: token renewing. Time elapsed since last token fetch:"
          + " {} milliseconds", elapsedTimeSinceLastTokenRefreshInMillis);
    }

    return expiring;
  }

  /**
   * Gets the Azure AD token from a client assertion in JWT format.
   * This method exists to make unit testing possible.
   *
   * @param clientAssertion the client assertion.
   * @return the Azure AD token.
   * @throws IOException if there is a failure in connecting to Azure AD.
   */
  @VisibleForTesting
  AzureADToken getTokenUsingJWTAssertion(String clientAssertion) throws IOException {
    return AzureADAuthenticator
        .getTokenUsingJWTAssertion(authEndpoint, clientId, clientAssertion);
  }

  /**
   * Returns the last time the token was fetched from the token file.
   * This method exists to make unit testing possible.
   *
   * @return the time the token was last fetched.
   */
  @VisibleForTesting
  long getTokenFetchTime() {
    return tokenFetchTime;
  }
}
