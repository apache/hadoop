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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.util.Preconditions;

/**
 * Provides tokens based on Azure AD Workload Identity.
 */
public class WorkloadIdentityTokenProvider extends AccessTokenProvider {

  private static final String OAUTH2_TOKEN_PATH = "/oauth2/v2.0/token";
  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenProvider.class);
  private static final String EMPTY_TOKEN_FILE_ERROR = "Empty token file found at specified path: ";
  private static final String TOKEN_FILE_READ_ERROR = "Error reading token file at specified path: ";

  private final String authEndpoint;
  private final String clientId;
  private final String tokenFile;
  private long tokenFetchTime = -1;

  public WorkloadIdentityTokenProvider(final String authority, final String tenantId,
      final String clientId, final String tokenFile) {
    Preconditions.checkNotNull(authority, "authority");
    Preconditions.checkNotNull(tenantId, "tenantId");
    Preconditions.checkNotNull(clientId, "clientId");
    Preconditions.checkNotNull(tokenFile, "tokenFile");

    this.authEndpoint = authority + tenantId + OAUTH2_TOKEN_PATH;
    this.clientId = clientId;
    this.tokenFile = tokenFile;
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing token from JWT Assertion");
    String clientAssertion = getClientAssertion();
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
   * Gets the client assertion from the token file.
   * The token file should contain the client assertion in JWT format.
   * It should be a String containing Base64Url encoded JSON Web Token (JWT).
   * See <a href="https://azure.github.io/azure-workload-identity/docs/faq.html#does-workload-identity-work-in-disconnected-environments">
   * Azure Workload Identity FAQ</a>.
   *
   * @return the client assertion.
   * @throws IOException if the token file is empty.
   */
  private String getClientAssertion()
      throws IOException {
    String clientAssertion = "";
    try {
      File file = new File(tokenFile);
      clientAssertion = FileUtils.readFileToString(file, "UTF-8");
    } catch (Exception e) {
      throw new IOException(TOKEN_FILE_READ_ERROR + tokenFile, e);
    }
    if (Strings.isNullOrEmpty(clientAssertion)) {
      throw new IOException(EMPTY_TOKEN_FILE_ERROR + tokenFile);
    }
    return clientAssertion;
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
