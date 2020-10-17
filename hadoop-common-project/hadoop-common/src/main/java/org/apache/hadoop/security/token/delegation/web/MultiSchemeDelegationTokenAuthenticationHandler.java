/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationHandlerUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.CompositeAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.HttpConstants;
import org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;

/**
 * A {@link CompositeAuthenticationHandler} that supports multiple HTTP
 * authentication schemes along with Delegation Token functionality. e.g.
 * server can support multiple authentication mechanisms such as Kerberos
 * (SPENGO) and LDAP. During the authentication phase, server will specify
 * all possible authentication schemes and let client choose the appropriate
 * scheme. Please refer to RFC-2616 and HADOOP-12082 for more details.
 *
 * Internally it uses {@link MultiSchemeAuthenticationHandler} implementation.
 * This handler also provides an option to enable delegation token management
 * functionality for only a specified subset of authentication schemes. This is
 * required to ensure that only schemes with strongest level of security should
 * be used for delegation token management.
 *
 * <p>
 * In addition to the wrapped {@link AuthenticationHandler} configuration
 * properties, this handler supports the following properties prefixed with the
 * type of the wrapped <code>AuthenticationHandler</code>:
 * <ul>
 * <li>delegation-token.token-kind: the token kind for generated tokens (no
 * default, required property).</li>
 * <li>delegation-token.update-interval.sec: secret manager master key update
 * interval in seconds (default 1 day).</li>
 * <li>delegation-token.max-lifetime.sec: maximum life of a delegation token in
 * seconds (default 7 days).</li>
 * <li>delegation-token.renewal-interval.sec: renewal interval for delegation
 * tokens in seconds (default 1 day).</li>
 * <li>delegation-token.removal-scan-interval.sec: delegation tokens removal
 * scan interval in seconds (default 1 hour).</li>
 * <li>delegation.http.schemes: A comma separated list of HTTP authentication
 * mechanisms (e.g. Negotiate, Basic) etc. to be allowed for delegation token
 * management operations.</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MultiSchemeDelegationTokenAuthenticationHandler extends
    DelegationTokenAuthenticationHandler implements
    CompositeAuthenticationHandler {

  public static final String DELEGATION_TOKEN_SCHEMES_PROPERTY =
      "multi-scheme-auth-handler.delegation.schemes";
  private static final Splitter STR_SPLITTER = Splitter.on(',').trimResults()
      .omitEmptyStrings();

  private Set<String> delegationAuthSchemes = null;

  public MultiSchemeDelegationTokenAuthenticationHandler() {
    super(new MultiSchemeAuthenticationHandler(
        MultiSchemeAuthenticationHandler.TYPE + TYPE_POSTFIX));
  }

  @Override
  public Collection<String> getTokenTypes() {
    return ((CompositeAuthenticationHandler) getAuthHandler()).getTokenTypes();
  }

  @Override
  public void init(Properties config) throws ServletException {
    super.init(config);

    // Figure out the HTTP authentication schemes configured.
    String schemesProperty =
        Preconditions.checkNotNull(config
            .getProperty(MultiSchemeAuthenticationHandler.SCHEMES_PROPERTY));

    // Figure out the HTTP authentication schemes configured for delegation
    // tokens.
    String delegationAuthSchemesProp =
        Preconditions.checkNotNull(config
            .getProperty(DELEGATION_TOKEN_SCHEMES_PROPERTY));

    Set<String> authSchemes = new HashSet<>();
    for (String scheme : STR_SPLITTER.split(schemesProperty)) {
      authSchemes.add(AuthenticationHandlerUtil.checkAuthScheme(scheme));
    }

    delegationAuthSchemes = new HashSet<>();
    for (String scheme : STR_SPLITTER.split(delegationAuthSchemesProp)) {
      delegationAuthSchemes.add(AuthenticationHandlerUtil
          .checkAuthScheme(scheme));
    }

    Preconditions.checkArgument(authSchemes.containsAll(delegationAuthSchemes));
  }

  /**
   * This method is overridden to restrict HTTP authentication schemes
   * available for delegation token management functionality. The
   * authentication schemes to be used for delegation token management are
   * configured using {@link DELEGATION_TOKEN_SCHEMES_PROPERTY}
   *
   * The basic logic here is to check if the current request is for delegation
   * token management. If yes then check if the request contains an
   * "Authorization" header. If it is missing, then return the HTTP 401
   * response with WWW-Authenticate header for each scheme configured for
   * delegation token management.
   *
   * It is also possible for a client to preemptively send Authorization header
   * for a scheme not configured for delegation token management. We detect
   * this case and return the HTTP 401 response with WWW-Authenticate header
   * for each scheme configured for delegation token management.
   *
   * If a client has sent a request with "Authorization" header for a scheme
   * configured for delegation token management, then it is forwarded to
   * underlying {@link MultiSchemeAuthenticationHandler} for actual
   * authentication.
   *
   * Finally all other requests (excluding delegation token management) are
   * forwarded to underlying {@link MultiSchemeAuthenticationHandler} for
   * actual authentication.
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
      HttpServletResponse response)
          throws IOException, AuthenticationException {
    String authorization =
        request.getHeader(HttpConstants.AUTHORIZATION_HEADER);

    if (isManagementOperation(request)) {
      boolean schemeConfigured = false;
      if (authorization != null) {
        for (String scheme : delegationAuthSchemes) {
          if (AuthenticationHandlerUtil.
              matchAuthScheme(scheme, authorization)) {
            schemeConfigured = true;
            break;
          }
        }
      }
      if (!schemeConfigured) {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        for (String scheme : delegationAuthSchemes) {
          response.addHeader(WWW_AUTHENTICATE, scheme);
        }
        return null;
      }
    }

    return super.authenticate(request, response);
  }
}