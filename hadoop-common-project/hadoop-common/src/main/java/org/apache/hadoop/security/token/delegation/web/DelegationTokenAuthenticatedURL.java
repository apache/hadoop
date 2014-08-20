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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * The <code>DelegationTokenAuthenticatedURL</code> is a
 * {@link AuthenticatedURL} sub-class with built-in Hadoop Delegation Token
 * functionality.
 * <p/>
 * The authentication mechanisms supported by default are Hadoop Simple
 * authentication (also known as pseudo authentication) and Kerberos SPNEGO
 * authentication.
 * <p/>
 * Additional authentication mechanisms can be supported via {@link
 * DelegationTokenAuthenticator} implementations.
 * <p/>
 * The default {@link DelegationTokenAuthenticator} is the {@link
 * KerberosDelegationTokenAuthenticator} class which supports
 * automatic fallback from Kerberos SPNEGO to Hadoop Simple authentication via
 * the {@link PseudoDelegationTokenAuthenticator} class.
 * <p/>
 * <code>AuthenticatedURL</code> instances are not thread-safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DelegationTokenAuthenticatedURL extends AuthenticatedURL {

  /**
   * Constant used in URL's query string to perform a proxy user request, the
   * value of the <code>DO_AS</code> parameter is the user the request will be
   * done on behalf of.
   */
  static final String DO_AS = "doAs";

  /**
   * Client side authentication token that handles Delegation Tokens.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static class Token extends AuthenticatedURL.Token {
    private
    org.apache.hadoop.security.token.Token<AbstractDelegationTokenIdentifier>
        delegationToken;

    org.apache.hadoop.security.token.Token<AbstractDelegationTokenIdentifier>
    getDelegationToken() {
      return delegationToken;
    }

  }

  private static Class<? extends DelegationTokenAuthenticator>
      DEFAULT_AUTHENTICATOR = KerberosDelegationTokenAuthenticator.class;

  /**
   * Sets the default {@link DelegationTokenAuthenticator} class to use when an
   * {@link DelegationTokenAuthenticatedURL} instance is created without
   * specifying one.
   *
   * The default class is {@link KerberosDelegationTokenAuthenticator}
   *
   * @param authenticator the authenticator class to use as default.
   */
  public static void setDefaultDelegationTokenAuthenticator(
      Class<? extends DelegationTokenAuthenticator> authenticator) {
    DEFAULT_AUTHENTICATOR = authenticator;
  }

  /**
   * Returns the default {@link DelegationTokenAuthenticator} class to use when
   * an {@link DelegationTokenAuthenticatedURL} instance is created without
   * specifying one.
   * <p/>
   * The default class is {@link KerberosDelegationTokenAuthenticator}
   *
   * @return the delegation token authenticator class to use as default.
   */
  public static Class<? extends DelegationTokenAuthenticator>
      getDefaultDelegationTokenAuthenticator() {
    return DEFAULT_AUTHENTICATOR;
  }

  private static DelegationTokenAuthenticator
      obtainDelegationTokenAuthenticator(DelegationTokenAuthenticator dta) {
    try {
      return (dta != null) ? dta : DEFAULT_AUTHENTICATOR.newInstance();
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  /**
   * Creates an <code>DelegationTokenAuthenticatedURL</code>.
   * <p/>
   * An instance of the default {@link DelegationTokenAuthenticator} will be
   * used.
   */
  public DelegationTokenAuthenticatedURL() {
    this(null, null);
  }

  /**
   * Creates an <code>DelegationTokenAuthenticatedURL</code>.
   *
   * @param authenticator the {@link DelegationTokenAuthenticator} instance to
   * use, if <code>null</code> the default one will be used.
   */
  public DelegationTokenAuthenticatedURL(
      DelegationTokenAuthenticator authenticator) {
    this(authenticator, null);
  }

  /**
   * Creates an <code>DelegationTokenAuthenticatedURL</code> using the default
   * {@link DelegationTokenAuthenticator} class.
   *
   * @param connConfigurator a connection configurator.
   */
  public DelegationTokenAuthenticatedURL(
      ConnectionConfigurator connConfigurator) {
    this(null, connConfigurator);
  }

  /**
   * Creates an <code>DelegationTokenAuthenticatedURL</code>.
   *
   * @param authenticator the {@link DelegationTokenAuthenticator} instance to
   * use, if <code>null</code> the default one will be used.
   * @param connConfigurator a connection configurator.
   */
  public DelegationTokenAuthenticatedURL(
      DelegationTokenAuthenticator authenticator,
      ConnectionConfigurator connConfigurator) {
    super(obtainDelegationTokenAuthenticator(authenticator), connConfigurator);
  }

  /**
   * Returns an authenticated {@link HttpURLConnection}, it uses a Delegation
   * Token only if the given auth token is an instance of {@link Token} and
   * it contains a Delegation Token, otherwise use the configured
   * {@link DelegationTokenAuthenticator} to authenticate the connection.
   *
   * @param url the URL to connect to. Only HTTP/S URLs are supported.
   * @param token the authentication token being used for the user.
   * @return an authenticated {@link HttpURLConnection}.
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  @Override
  public HttpURLConnection openConnection(URL url, AuthenticatedURL.Token token)
      throws IOException, AuthenticationException {
    return (token instanceof Token) ? openConnection(url, (Token) token)
                                    : super.openConnection(url ,token);
  }

  /**
   * Returns an authenticated {@link HttpURLConnection}. If the Delegation
   * Token is present, it will be used taking precedence over the configured
   * <code>Authenticator</code>.
   *
   * @param url the URL to connect to. Only HTTP/S URLs are supported.
   * @param token the authentication token being used for the user.
   * @return an authenticated {@link HttpURLConnection}.
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public HttpURLConnection openConnection(URL url, Token token)
      throws IOException, AuthenticationException {
    return openConnection(url, token, null);
  }

  private URL augmentURL(URL url, Map<String, String> params)
      throws IOException {
    if (params != null && params.size() > 0) {
      String urlStr = url.toExternalForm();
      StringBuilder sb = new StringBuilder(urlStr);
      String separator = (urlStr.contains("?")) ? "&" : "?";
      for (Map.Entry<String, String> param : params.entrySet()) {
        sb.append(separator).append(param.getKey()).append("=").append(
            param.getValue());
        separator = "&";
      }
      url = new URL(sb.toString());
    }
    return url;
  }

  /**
   * Returns an authenticated {@link HttpURLConnection}. If the Delegation
   * Token is present, it will be used taking precedence over the configured
   * <code>Authenticator</code>. If the <code>doAs</code> parameter is not NULL,
   * the request will be done on behalf of the specified <code>doAs</code> user.
   *
   * @param url the URL to connect to. Only HTTP/S URLs are supported.
   * @param token the authentication token being used for the user.
   * @param doAs user to do the the request on behalf of, if NULL the request is
   * as self.
   * @return an authenticated {@link HttpURLConnection}.
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public HttpURLConnection openConnection(URL url, Token token, String doAs)
      throws IOException, AuthenticationException {
    Preconditions.checkNotNull(url, "url");
    Preconditions.checkNotNull(token, "token");
    Map<String, String> extraParams = new HashMap<String, String>();

    // delegation token
    Credentials creds = UserGroupInformation.getCurrentUser().getCredentials();
    if (!creds.getAllTokens().isEmpty()) {
      InetSocketAddress serviceAddr = new InetSocketAddress(url.getHost(),
          url.getPort());
      Text service = SecurityUtil.buildTokenService(serviceAddr);
      org.apache.hadoop.security.token.Token<? extends TokenIdentifier> dt =
          creds.getToken(service);
      if (dt != null) {
        extraParams.put(KerberosDelegationTokenAuthenticator.DELEGATION_PARAM,
            dt.encodeToUrlString());
      }
    }

    // proxyuser
    if (doAs != null) {
      extraParams.put(DO_AS, URLEncoder.encode(doAs, "UTF-8"));
    }

    url = augmentURL(url, extraParams);
    return super.openConnection(url, token);
  }

  /**
   * Requests a delegation token using the configured <code>Authenticator</code>
   * for authentication.
   *
   * @param url the URL to get the delegation token from. Only HTTP/S URLs are
   * supported.
   * @param token the authentication token being used for the user where the
   * Delegation token will be stored.
   * @return a delegation token.
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public org.apache.hadoop.security.token.Token<AbstractDelegationTokenIdentifier>
      getDelegationToken(URL url, Token token, String renewer)
          throws IOException, AuthenticationException {
    Preconditions.checkNotNull(url, "url");
    Preconditions.checkNotNull(token, "token");
    try {
      token.delegationToken =
          ((KerberosDelegationTokenAuthenticator) getAuthenticator()).
              getDelegationToken(url, token, renewer);
      return token.delegationToken;
    } catch (IOException ex) {
      token.delegationToken = null;
      throw ex;
    }
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
  public long renewDelegationToken(URL url, Token token)
      throws IOException, AuthenticationException {
    Preconditions.checkNotNull(url, "url");
    Preconditions.checkNotNull(token, "token");
    Preconditions.checkNotNull(token.delegationToken,
        "No delegation token available");
    try {
      return ((KerberosDelegationTokenAuthenticator) getAuthenticator()).
          renewDelegationToken(url, token, token.delegationToken);
    } catch (IOException ex) {
      token.delegationToken = null;
      throw ex;
    }
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
  public void cancelDelegationToken(URL url, Token token)
      throws IOException {
    Preconditions.checkNotNull(url, "url");
    Preconditions.checkNotNull(token, "token");
    Preconditions.checkNotNull(token.delegationToken,
        "No delegation token available");
    try {
      ((KerberosDelegationTokenAuthenticator) getAuthenticator()).
          cancelDelegationToken(url, token, token.delegationToken);
    } finally {
      token.delegationToken = null;
    }
  }

}
