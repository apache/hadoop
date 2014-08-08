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
package org.apache.hadoop.fs.http.client;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>KerberosAuthenticator</code> subclass that fallback to
 * {@link HttpFSPseudoAuthenticator}.
 */
@InterfaceAudience.Private
public class HttpFSKerberosAuthenticator extends KerberosAuthenticator {

  /**
   * Returns the fallback authenticator if the server does not use
   * Kerberos SPNEGO HTTP authentication.
   *
   * @return a {@link HttpFSPseudoAuthenticator} instance.
   */
  @Override
  protected Authenticator getFallBackAuthenticator() {
    return new HttpFSPseudoAuthenticator();
  }

  private static final String HTTP_GET = "GET";
  private static final String HTTP_PUT = "PUT";

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

  public static void injectDelegationToken(Map<String, String> params,
                                          Token<?> dtToken)
    throws IOException {
    if (dtToken != null) {
      params.put(DELEGATION_PARAM, dtToken.encodeToUrlString());
    }
  }

  private boolean hasDelegationToken(URL url) {
    return url.getQuery().contains(DELEGATION_PARAM + "=");
  }

  @Override
  public void authenticate(URL url, AuthenticatedURL.Token token)
    throws IOException, AuthenticationException {
    if (!hasDelegationToken(url)) {
      super.authenticate(url, token);
    }
  }

  public static final String OP_PARAM = "op";

  public static Token<?> getDelegationToken(URI fsURI,
    InetSocketAddress httpFSAddr, AuthenticatedURL.Token token,
    String renewer) throws IOException {
    DelegationTokenOperation op = 
      DelegationTokenOperation.GETDELEGATIONTOKEN;
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, op.toString());
    params.put(RENEWER_PARAM,renewer);
    URL url = HttpFSUtils.createURL(new Path(fsURI), params);
    AuthenticatedURL aUrl =
      new AuthenticatedURL(new HttpFSKerberosAuthenticator());
    try {
      HttpURLConnection conn = aUrl.openConnection(url, token);
      conn.setRequestMethod(op.getHttpMethod());
      HttpFSUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
      JSONObject json = (JSONObject) ((JSONObject)
        HttpFSUtils.jsonParse(conn)).get(DELEGATION_TOKEN_JSON);
      String tokenStr = (String)
        json.get(DELEGATION_TOKEN_URL_STRING_JSON);
      Token<AbstractDelegationTokenIdentifier> dToken =
        new Token<AbstractDelegationTokenIdentifier>();
      dToken.decodeFromUrlString(tokenStr);
      SecurityUtil.setTokenService(dToken, httpFSAddr);
      return dToken;
    } catch (AuthenticationException ex) {
      throw new IOException(ex.toString(), ex);
    }
  }

  public static long renewDelegationToken(URI fsURI,
    AuthenticatedURL.Token token, Token<?> dToken) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM,
               DelegationTokenOperation.RENEWDELEGATIONTOKEN.toString());
    params.put(TOKEN_PARAM, dToken.encodeToUrlString());
    URL url = HttpFSUtils.createURL(new Path(fsURI), params);
    AuthenticatedURL aUrl =
      new AuthenticatedURL(new HttpFSKerberosAuthenticator());
    try {
      HttpURLConnection conn = aUrl.openConnection(url, token);
      conn.setRequestMethod(
        DelegationTokenOperation.RENEWDELEGATIONTOKEN.getHttpMethod());
      HttpFSUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
      JSONObject json = (JSONObject) ((JSONObject)
        HttpFSUtils.jsonParse(conn)).get(DELEGATION_TOKEN_JSON);
      return (Long)(json.get(RENEW_DELEGATION_TOKEN_JSON));
    } catch (AuthenticationException ex) {
      throw new IOException(ex.toString(), ex);
    }
  }

  public static void cancelDelegationToken(URI fsURI,
    AuthenticatedURL.Token token, Token<?> dToken) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM,
               DelegationTokenOperation.CANCELDELEGATIONTOKEN.toString());
    params.put(TOKEN_PARAM, dToken.encodeToUrlString());
    URL url = HttpFSUtils.createURL(new Path(fsURI), params);
    AuthenticatedURL aUrl =
      new AuthenticatedURL(new HttpFSKerberosAuthenticator());
    try {
      HttpURLConnection conn = aUrl.openConnection(url, token);
      conn.setRequestMethod(
        DelegationTokenOperation.CANCELDELEGATIONTOKEN.getHttpMethod());
      HttpFSUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    } catch (AuthenticationException ex) {
      throw new IOException(ex.toString(), ex);
    }
  }

}
