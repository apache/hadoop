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
package org.apache.hadoop.security.token.delegation.web;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link Authenticator} wrapper that enhances an {@link Authenticator} with
 * Delegation Token support.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class DelegationTokenAuthenticator implements Authenticator {
  private static Logger LOG = 
      LoggerFactory.getLogger(DelegationTokenAuthenticator.class);
  
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String APPLICATION_JSON_MIME = "application/json";

  private static final String HTTP_GET = "GET";
  private static final String HTTP_PUT = "PUT";

  public static final String OP_PARAM = "op";

  public static final String DELEGATION_PARAM = "delegation";
  public static final String TOKEN_PARAM = "token";
  public static final String RENEWER_PARAM = "renewer";
  public static final String DELEGATION_TOKEN_JSON = "Token";
  public static final String DELEGATION_TOKEN_URL_STRING_JSON = "urlString";
  public static final String RENEW_DELEGATION_TOKEN_JSON = "long";

  /**
   * DelegationToken operations.
   */
  @InterfaceAudience.Private
  public static enum DelegationTokenOperation {
    GETDELEGATIONTOKEN(HTTP_GET, true),
    RENEWDELEGATIONTOKEN(HTTP_PUT, true),
    CANCELDELEGATIONTOKEN(HTTP_PUT, false);

    private String httpMethod;
    private boolean requiresKerberosCredentials;

    private DelegationTokenOperation(String httpMethod,
        boolean requiresKerberosCredentials) {
      this.httpMethod = httpMethod;
      this.requiresKerberosCredentials = requiresKerberosCredentials;
    }

    public String getHttpMethod() {
      return httpMethod;
    }

    public boolean requiresKerberosCredentials() {
      return requiresKerberosCredentials;
    }
  }

  private Authenticator authenticator;

  public DelegationTokenAuthenticator(Authenticator authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public void setConnectionConfigurator(ConnectionConfigurator configurator) {
    authenticator.setConnectionConfigurator(configurator);
  }

  private boolean hasDelegationToken(URL url) {
    String queryStr = url.getQuery();
    return (queryStr != null) && queryStr.contains(DELEGATION_PARAM + "=");
  }

  @Override
  public void authenticate(URL url, AuthenticatedURL.Token token)
      throws IOException, AuthenticationException {
    if (!hasDelegationToken(url)) {
      authenticator.authenticate(url, token);
    }
  }

  /**
   * Requests a delegation token using the configured <code>Authenticator</code>
   * for authentication.
   *
   * @param url the URL to get the delegation token from. Only HTTP/S URLs are
   * supported.
   * @param token the authentication token being used for the user where the
   * Delegation token will be stored.
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public Token<AbstractDelegationTokenIdentifier> getDelegationToken(URL url,
      AuthenticatedURL.Token token, String renewer)
      throws IOException, AuthenticationException {
    Map json = doDelegationTokenOperation(url, token,
        DelegationTokenOperation.GETDELEGATIONTOKEN, renewer, null, true);
    json = (Map) json.get(DELEGATION_TOKEN_JSON);
    String tokenStr = (String) json.get(DELEGATION_TOKEN_URL_STRING_JSON);
    Token<AbstractDelegationTokenIdentifier> dToken =
        new Token<AbstractDelegationTokenIdentifier>();
    dToken.decodeFromUrlString(tokenStr);
    InetSocketAddress service = new InetSocketAddress(url.getHost(),
        url.getPort());
    SecurityUtil.setTokenService(dToken, service);
    return dToken;
  }

  /**
   * Renews a delegation token from the server end-point using the
   * configured <code>Authenticator</code> for authentication.
   *
   * @param url the URL to renew the delegation token from. Only HTTP/S URLs are
   * supported.
   * @param token the authentication token with the Delegation Token to renew.
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public long renewDelegationToken(URL url,
      AuthenticatedURL.Token token,
      Token<AbstractDelegationTokenIdentifier> dToken)
      throws IOException, AuthenticationException {
    Map json = doDelegationTokenOperation(url, token,
        DelegationTokenOperation.RENEWDELEGATIONTOKEN, null, dToken, true);
    return (Long) json.get(RENEW_DELEGATION_TOKEN_JSON);
  }

  /**
   * Cancels a delegation token from the server end-point. It does not require
   * being authenticated by the configured <code>Authenticator</code>.
   *
   * @param url the URL to cancel the delegation token from. Only HTTP/S URLs
   * are supported.
   * @param token the authentication token with the Delegation Token to cancel.
   * @throws IOException if an IO error occurred.
   */
  public void cancelDelegationToken(URL url,
      AuthenticatedURL.Token token,
      Token<AbstractDelegationTokenIdentifier> dToken)
      throws IOException {
    try {
      doDelegationTokenOperation(url, token,
          DelegationTokenOperation.CANCELDELEGATIONTOKEN, null, dToken, false);
    } catch (AuthenticationException ex) {
      throw new IOException("This should not happen: " + ex.getMessage(), ex);
    }
  }

  private Map doDelegationTokenOperation(URL url,
      AuthenticatedURL.Token token, DelegationTokenOperation operation,
      String renewer, Token<?> dToken, boolean hasResponse)
      throws IOException, AuthenticationException {
    Map ret = null;
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, operation.toString());
    if (renewer != null) {
      params.put(RENEWER_PARAM, renewer);
    }
    if (dToken != null) {
      params.put(TOKEN_PARAM, dToken.encodeToUrlString());
    }
    String urlStr = url.toExternalForm();
    StringBuilder sb = new StringBuilder(urlStr);
    String separator = (urlStr.contains("?")) ? "&" : "?";
    for (Map.Entry<String, String> entry : params.entrySet()) {
      sb.append(separator).append(entry.getKey()).append("=").
          append(URLEncoder.encode(entry.getValue(), "UTF8"));
      separator = "&";
    }
    url = new URL(sb.toString());
    AuthenticatedURL aUrl = new AuthenticatedURL(this);
    HttpURLConnection conn = aUrl.openConnection(url, token);
    conn.setRequestMethod(operation.getHttpMethod());
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    if (hasResponse) {
      String contentType = conn.getHeaderField(CONTENT_TYPE);
      contentType = (contentType != null) ? contentType.toLowerCase()
                                          : null;
      if (contentType != null &&
          contentType.contains(APPLICATION_JSON_MIME)) {
        try {
          ObjectMapper mapper = new ObjectMapper();
          ret = mapper.readValue(conn.getInputStream(), Map.class);
        } catch (Exception ex) {
          throw new AuthenticationException(String.format(
              "'%s' did not handle the '%s' delegation token operation: %s",
              url.getAuthority(), operation, ex.getMessage()), ex);
        }
      } else {
        throw new AuthenticationException(String.format("'%s' did not " +
                "respond with JSON to the '%s' delegation token operation",
            url.getAuthority(), operation));
      }
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  private static void validateResponse(HttpURLConnection conn, int expected)
      throws IOException {
    int status = conn.getResponseCode();
    if (status != expected) {
      try {
        conn.getInputStream().close();
      } catch (IOException ex) {
        //NOP
      }
      String msg = String.format("HTTP status, expected [%d], got [%d]: %s", 
          expected, status, conn.getResponseMessage());
      LOG.debug(msg);
      throw new IOException(msg);
    }
  }

}
