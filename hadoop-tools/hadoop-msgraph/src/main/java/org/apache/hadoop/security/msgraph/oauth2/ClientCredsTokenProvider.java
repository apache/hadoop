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

import com.microsoft.graph.http.IHttpRequest;

import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of AccessTokenProvider that uses client (application)
 * credentials to request tokens from Azure AD.
 */
public class ClientCredsTokenProvider extends AccessTokenProvider {

  private static final Logger LOG = LoggerFactory.getLogger(
      ClientCredsTokenProvider.class);

  private final String authEndpoint;
  private final String clientId;
  private final String clientSecret;
  private final String grantType;
  private final String resource;

  public ClientCredsTokenProvider(
      String authEndpoint, String clientId, String clientSecret,
      String grantType, String resource) {
    this.authEndpoint = authEndpoint;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.grantType = grantType;
    this.resource = resource;
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing client-credential based token");
    return AzureADAuthenticator.getTokenUsingClientCreds(authEndpoint,
        clientId, clientSecret, grantType, resource);
  }

  @Override // IAuthenticationProvider
  public void authenticateRequest(IHttpRequest iHttpRequest) {
    AzureADToken token;
    try {
      token = getToken();
    } catch (IOException e) {
      LOG.error("Couldn't get token, skipping authenticating: {}",
          iHttpRequest.getRequestUrl(), e);
      return;
    }
    iHttpRequest.addHeader(HttpHeader.AUTHORIZATION.asString(),
        "Bearer " + token.getAccessToken());
  }
}