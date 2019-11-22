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

package org.apache.hadoop.security.msgraph.oauth2;

import com.microsoft.graph.authentication.IAuthenticationProvider;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class for retrieving AzureADTokens to access Azure resources.
 *
 * Implements token refresh when tokens are about to expire.
 * The token retrieval/refresh mechanism itself is implementation specific.
 */
public abstract class AccessTokenProvider implements IAuthenticationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(
      AccessTokenProvider.class);

  /** The cached token. */
  protected AzureADToken token;

  public AccessTokenProvider() {
    // Empty constructor
  }

  /**
   * Get a token.
   */
  public synchronized AzureADToken getToken() throws IOException {
    if (this.isTokenAboutToExpire()) {
      LOG.debug("AAD Token is missing or expired: Calling refresh-token " +
          "from abstract base class");
      this.token = this.refreshToken();
    }

    return this.token;
  }

  /**
   * Refresh the token.
   */
  protected abstract AzureADToken refreshToken() throws IOException;

  /**
   * Check if the token is about to expire.
   */
  private boolean isTokenAboutToExpire() {
    if (this.token == null) {
      LOG.debug("AADToken: no token. Returning expiring=true.");
      return true;
    }

    Date expiry = this.token.getExpiry();
    if (expiry == null) {
      LOG.debug("AADToken: no token expiry set. Returning expiring=true.");
      return true;
    }

    Calendar c = Calendar.getInstance();
    c.add(Calendar.MILLISECOND, (int) TimeUnit.MINUTES.toMillis(5));

    boolean expiring = c.getTime().after(expiry);
    if (expiring) {
      LOG.debug("AADToken: token expiring: {} : Five-minute window: {}.",
          expiry, c.getTime());
    }

    return expiring;
  }
}