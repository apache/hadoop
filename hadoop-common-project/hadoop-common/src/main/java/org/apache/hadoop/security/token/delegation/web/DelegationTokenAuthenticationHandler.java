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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * An {@link AuthenticationHandler} that implements Kerberos SPNEGO mechanism
 * for HTTP and supports Delegation Token functionality.
 * <p/>
 * In addition to the wrapped {@link AuthenticationHandler} configuration
 * properties, this handler supports the following properties prefixed
 * with the type of the wrapped <code>AuthenticationHandler</code>:
 * <ul>
 * <li>delegation-token.token-kind: the token kind for generated tokens
 * (no default, required property).</li>
 * <li>delegation-token.update-interval.sec: secret manager master key
 * update interval in seconds (default 1 day).</li>
 * <li>delegation-token.max-lifetime.sec: maximum life of a delegation
 * token in seconds (default 7 days).</li>
 * <li>delegation-token.renewal-interval.sec: renewal interval for
 * delegation tokens in seconds (default 1 day).</li>
 * <li>delegation-token.removal-scan-interval.sec: delegation tokens
 * removal scan interval in seconds (default 1 hour).</li>
 * </ul>
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class DelegationTokenAuthenticationHandler
    implements AuthenticationHandler {

  protected static final String TYPE_POSTFIX = "-dt";

  public static final String PREFIX = "delegation-token.";

  public static final String TOKEN_KIND = PREFIX + "token-kind";

  public static final String UPDATE_INTERVAL = PREFIX + "update-interval.sec";
  public static final long UPDATE_INTERVAL_DEFAULT = 24 * 60 * 60;

  public static final String MAX_LIFETIME = PREFIX + "max-lifetime.sec";
  public static final long MAX_LIFETIME_DEFAULT = 7 * 24 * 60 * 60;

  public static final String RENEW_INTERVAL = PREFIX + "renew-interval.sec";
  public static final long RENEW_INTERVAL_DEFAULT = 24 * 60 * 60;

  public static final String REMOVAL_SCAN_INTERVAL = PREFIX +
      "removal-scan-interval.sec";
  public static final long REMOVAL_SCAN_INTERVAL_DEFAULT = 60 * 60;

  private static final Set<String> DELEGATION_TOKEN_OPS = new HashSet<String>();

  static final String DELEGATION_TOKEN_UGI_ATTRIBUTE =
      "hadoop.security.delegation-token.ugi";

  static {
    DELEGATION_TOKEN_OPS.add(KerberosDelegationTokenAuthenticator.
        DelegationTokenOperation.GETDELEGATIONTOKEN.toString());
    DELEGATION_TOKEN_OPS.add(KerberosDelegationTokenAuthenticator.
        DelegationTokenOperation.RENEWDELEGATIONTOKEN.toString());
    DELEGATION_TOKEN_OPS.add(KerberosDelegationTokenAuthenticator.
        DelegationTokenOperation.CANCELDELEGATIONTOKEN.toString());
  }

  private AuthenticationHandler authHandler;
  private DelegationTokenManager tokenManager;
  private String authType;

  public DelegationTokenAuthenticationHandler(AuthenticationHandler handler) {
    authHandler = handler;
    authType = handler.getType();
  }

  @VisibleForTesting
  DelegationTokenManager getTokenManager() {
    return tokenManager;
  }

  @Override
  public void init(Properties config) throws ServletException {
    authHandler.init(config);
    initTokenManager(config);
  }

  /**
   * Sets an external <code>DelegationTokenSecretManager</code> instance to
   * manage creation and verification of Delegation Tokens.
   * <p/>
   * This is useful for use cases where secrets must be shared across multiple
   * services.
   *
   * @param secretManager a <code>DelegationTokenSecretManager</code> instance
   */
  public void setExternalDelegationTokenSecretManager(
      AbstractDelegationTokenSecretManager secretManager) {
    tokenManager.setExternalDelegationTokenSecretManager(secretManager);
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  public void initTokenManager(Properties config) {
    String configPrefix = authHandler.getType() + ".";
    Configuration conf = new Configuration(false);
    for (Map.Entry entry : config.entrySet()) {
      conf.set((String) entry.getKey(), (String) entry.getValue());
    }
    String tokenKind = conf.get(TOKEN_KIND);
    if (tokenKind == null) {
      throw new IllegalArgumentException(
          "The configuration does not define the token kind");
    }
    tokenKind = tokenKind.trim();
    long updateInterval = conf.getLong(configPrefix + UPDATE_INTERVAL,
        UPDATE_INTERVAL_DEFAULT);
    long maxLifeTime = conf.getLong(configPrefix + MAX_LIFETIME,
        MAX_LIFETIME_DEFAULT);
    long renewInterval = conf.getLong(configPrefix + RENEW_INTERVAL,
        RENEW_INTERVAL_DEFAULT);
    long removalScanInterval = conf.getLong(
        configPrefix + REMOVAL_SCAN_INTERVAL, REMOVAL_SCAN_INTERVAL_DEFAULT);
    tokenManager = new DelegationTokenManager(new Text(tokenKind),
        updateInterval * 1000, maxLifeTime * 1000, renewInterval * 1000,
        removalScanInterval * 1000);
    tokenManager.init();
  }

  @Override
  public void destroy() {
    tokenManager.destroy();
    authHandler.destroy();
  }

  @Override
  public String getType() {
    return authType;
  }

  private static final String ENTER = System.getProperty("line.separator");

  @Override
  @SuppressWarnings("unchecked")
  public boolean managementOperation(AuthenticationToken token,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    boolean requestContinues = true;
    String op = ServletUtils.getParameter(request,
        KerberosDelegationTokenAuthenticator.OP_PARAM);
    op = (op != null) ? op.toUpperCase() : null;
    if (DELEGATION_TOKEN_OPS.contains(op) &&
        !request.getMethod().equals("OPTIONS")) {
      KerberosDelegationTokenAuthenticator.DelegationTokenOperation dtOp =
          KerberosDelegationTokenAuthenticator.
              DelegationTokenOperation.valueOf(op);
      if (dtOp.getHttpMethod().equals(request.getMethod())) {
        boolean doManagement;
        if (dtOp.requiresKerberosCredentials() && token == null) {
          token = authenticate(request, response);
          if (token == null) {
            requestContinues = false;
            doManagement = false;
          } else {
            doManagement = true;
          }
        } else {
          doManagement = true;
        }
        if (doManagement) {
          UserGroupInformation requestUgi = (token != null)
              ? UserGroupInformation.createRemoteUser(token.getUserName())
              : null;
          Map map = null;
          switch (dtOp) {
            case GETDELEGATIONTOKEN:
              if (requestUgi == null) {
                throw new IllegalStateException("request UGI cannot be NULL");
              }
              String renewer = ServletUtils.getParameter(request,
                  KerberosDelegationTokenAuthenticator.RENEWER_PARAM);
              try {
                Token<?> dToken = tokenManager.createToken(requestUgi, renewer);
                map = delegationTokenToJSON(dToken);
              } catch (IOException ex) {
                throw new AuthenticationException(ex.toString(), ex);
              }
              break;
            case RENEWDELEGATIONTOKEN:
              if (requestUgi == null) {
                throw new IllegalStateException("request UGI cannot be NULL");
              }
              String tokenToRenew = ServletUtils.getParameter(request,
                  KerberosDelegationTokenAuthenticator.TOKEN_PARAM);
              if (tokenToRenew == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    MessageFormat.format(
                        "Operation [{0}] requires the parameter [{1}]", dtOp,
                        KerberosDelegationTokenAuthenticator.TOKEN_PARAM)
                );
                requestContinues = false;
              } else {
                Token<DelegationTokenIdentifier> dt =
                    new Token<DelegationTokenIdentifier>();
                try {
                  dt.decodeFromUrlString(tokenToRenew);
                  long expirationTime = tokenManager.renewToken(dt,
                      requestUgi.getShortUserName());
                  map = new HashMap();
                  map.put("long", expirationTime);
                } catch (IOException ex) {
                  throw new AuthenticationException(ex.toString(), ex);
                }
              }
              break;
            case CANCELDELEGATIONTOKEN:
              String tokenToCancel = ServletUtils.getParameter(request,
                  KerberosDelegationTokenAuthenticator.TOKEN_PARAM);
              if (tokenToCancel == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    MessageFormat.format(
                        "Operation [{0}] requires the parameter [{1}]", dtOp,
                        KerberosDelegationTokenAuthenticator.TOKEN_PARAM)
                );
                requestContinues = false;
              } else {
                Token<DelegationTokenIdentifier> dt =
                    new Token<DelegationTokenIdentifier>();
                try {
                  dt.decodeFromUrlString(tokenToCancel);
                  tokenManager.cancelToken(dt, (requestUgi != null)
                      ? requestUgi.getShortUserName() : null);
                } catch (IOException ex) {
                  response.sendError(HttpServletResponse.SC_NOT_FOUND,
                      "Invalid delegation token, cannot cancel");
                  requestContinues = false;
                }
              }
              break;
          }
          if (requestContinues) {
            response.setStatus(HttpServletResponse.SC_OK);
            if (map != null) {
              response.setContentType(MediaType.APPLICATION_JSON);
              Writer writer = response.getWriter();
              ObjectMapper jsonMapper = new ObjectMapper();
              jsonMapper.writeValue(writer, map);
              writer.write(ENTER);
              writer.flush();
            }
            requestContinues = false;
          }
        }
      } else {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
            MessageFormat.format(
                "Wrong HTTP method [{0}] for operation [{1}], it should be " +
                    "[{2}]", request.getMethod(), dtOp, dtOp.getHttpMethod()));
        requestContinues = false;
      }
    }
    return requestContinues;
  }

  @SuppressWarnings("unchecked")
  private static Map delegationTokenToJSON(Token token) throws IOException {
    Map json = new LinkedHashMap();
    json.put(
        KerberosDelegationTokenAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON,
        token.encodeToUrlString());
    Map response = new LinkedHashMap();
    response.put(KerberosDelegationTokenAuthenticator.DELEGATION_TOKEN_JSON,
        json);
    return response;
  }

  /**
   * Authenticates a request looking for the <code>delegation</code>
   * query-string parameter and verifying it is a valid token. If there is not
   * <code>delegation</code> query-string parameter, it delegates the
   * authentication to the {@link KerberosAuthenticationHandler} unless it is
   * disabled.
   *
   * @param request the HTTP client request.
   * @param response the HTTP client response.
   * @return the authentication token for the authenticated request.
   * @throws IOException thrown if an IO error occurred.
   * @throws AuthenticationException thrown if the authentication failed.
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
      HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken token;
    String delegationParam = ServletUtils.getParameter(request,
        KerberosDelegationTokenAuthenticator.DELEGATION_PARAM);
    if (delegationParam != null) {
      try {
        Token<DelegationTokenIdentifier> dt =
            new Token<DelegationTokenIdentifier>();
        dt.decodeFromUrlString(delegationParam);
        UserGroupInformation ugi = tokenManager.verifyToken(dt);
        final String shortName = ugi.getShortUserName();

        // creating a ephemeral token
        token = new AuthenticationToken(shortName, ugi.getUserName(),
            getType());
        token.setExpires(0);
        request.setAttribute(DELEGATION_TOKEN_UGI_ATTRIBUTE, ugi);
      } catch (Throwable ex) {
        throw new AuthenticationException("Could not verify DelegationToken, " +
            ex.toString(), ex);
      }
    } else {
      token = authHandler.authenticate(request, response);
    }
    return token;
  }

}
