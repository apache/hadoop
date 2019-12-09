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

import java.io.IOException;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

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
  private static final Logger LOG =
      LoggerFactory.getLogger(DelegationTokenAuthenticationHandler.class);

  protected static final String TYPE_POSTFIX = "-dt";

  public static final String PREFIX = "delegation-token.";

  public static final String TOKEN_KIND = PREFIX + "token-kind";

  private static final Set<String> DELEGATION_TOKEN_OPS = new HashSet<String>();

  public static final String DELEGATION_TOKEN_UGI_ATTRIBUTE =
      "hadoop.security.delegation-token.ugi";

  public static final String JSON_MAPPER_PREFIX = PREFIX + "json-mapper.";

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
  private JsonFactory jsonFactory;

  public DelegationTokenAuthenticationHandler(AuthenticationHandler handler) {
    authHandler = handler;
    authType = handler.getType();
  }

  @VisibleForTesting
  DelegationTokenManager getTokenManager() {
    return tokenManager;
  }

  AuthenticationHandler getAuthHandler() {
    return authHandler;
  }

  @Override
  public void init(Properties config) throws ServletException {
    authHandler.init(config);
    initTokenManager(config);
    initJsonFactory(config);
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
    tokenManager = new DelegationTokenManager(conf, new Text(tokenKind));
    tokenManager.init();
  }

  @VisibleForTesting
  public void initJsonFactory(Properties config) {
    boolean hasFeature = false;
    JsonFactory tmpJsonFactory = new JsonFactory();

    for (Map.Entry entry : config.entrySet()) {
      String key = (String)entry.getKey();
      if (key.startsWith(JSON_MAPPER_PREFIX)) {
        JsonGenerator.Feature feature =
            JsonGenerator.Feature.valueOf(key.substring(JSON_MAPPER_PREFIX
                .length()));
        if (feature != null) {
          hasFeature = true;
          boolean enabled = Boolean.parseBoolean((String)entry.getValue());
          tmpJsonFactory.configure(feature, enabled);
        }
      }
    }

    if (hasFeature) {
      jsonFactory = tmpJsonFactory;
    }
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

  /**
   * This method checks if the given HTTP request corresponds to a management
   * operation.
   *
   * @param request The HTTP request
   * @return true if the given HTTP request corresponds to a management
   *         operation false otherwise
   * @throws IOException In case of I/O error.
   */
  protected final boolean isManagementOperation(HttpServletRequest request)
      throws IOException {
    String op = ServletUtils.getParameter(request,
        KerberosDelegationTokenAuthenticator.OP_PARAM);
    op = (op != null) ? StringUtils.toUpperCase(op) : null;
    return DELEGATION_TOKEN_OPS.contains(op) &&
        !request.getMethod().equals("OPTIONS");
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean managementOperation(AuthenticationToken token,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    boolean requestContinues = true;
    LOG.trace("Processing operation for req=({}), token: {}", request, token);
    String op = ServletUtils.getParameter(request,
        KerberosDelegationTokenAuthenticator.OP_PARAM);
    op = (op != null) ? StringUtils.toUpperCase(op) : null;
    if (isManagementOperation(request)) {
      KerberosDelegationTokenAuthenticator.DelegationTokenOperation dtOp =
          KerberosDelegationTokenAuthenticator.
              DelegationTokenOperation.valueOf(op);
      if (dtOp.getHttpMethod().equals(request.getMethod())) {
        boolean doManagement;
        if (dtOp.requiresKerberosCredentials() && token == null) {
          // Don't authenticate via DT for DT ops.
          token = authHandler.authenticate(request, response);
          LOG.trace("Got token: {}.", token);
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
          // Create the proxy user if doAsUser exists
          String doAsUser = DelegationTokenAuthenticationFilter.getDoAs(request);
          if (requestUgi != null && doAsUser != null) {
            requestUgi = UserGroupInformation.createProxyUser(
                doAsUser, requestUgi);
            try {
              ProxyUsers.authorize(requestUgi, request.getRemoteAddr());
            } catch (AuthorizationException ex) {
              HttpExceptionUtils.createServletExceptionResponse(response,
                  HttpServletResponse.SC_FORBIDDEN, ex);
              return false;
            }
          }
          Map map = null;
          switch (dtOp) {
            case GETDELEGATIONTOKEN:
              if (requestUgi == null) {
                throw new IllegalStateException("request UGI cannot be NULL");
              }
              String renewer = ServletUtils.getParameter(request,
                  KerberosDelegationTokenAuthenticator.RENEWER_PARAM);
              String service = ServletUtils.getParameter(request,
                  KerberosDelegationTokenAuthenticator.SERVICE_PARAM);
              try {
                Token<?> dToken = tokenManager.createToken(requestUgi, renewer,
                    service);
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
                Token<AbstractDelegationTokenIdentifier> dt = new Token();
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
                Token<AbstractDelegationTokenIdentifier> dt = new Token();
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
              ObjectMapper jsonMapper = new ObjectMapper(jsonFactory);
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
  @SuppressWarnings("unchecked")
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
      HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken token;
    String delegationParam = getDelegationToken(request);
    if (delegationParam != null) {
      LOG.debug("Authenticating with dt param: {}", delegationParam);
      try {
        Token<AbstractDelegationTokenIdentifier> dt = new Token();
        dt.decodeFromUrlString(delegationParam);
        UserGroupInformation ugi = tokenManager.verifyToken(dt);
        final String shortName = ugi.getShortUserName();

        // creating a ephemeral token
        token = new AuthenticationToken(shortName, ugi.getUserName(),
            getType());
        token.setExpires(0);
        request.setAttribute(DELEGATION_TOKEN_UGI_ATTRIBUTE, ugi);
      } catch (Throwable ex) {
        token = null;
        HttpExceptionUtils.createServletExceptionResponse(response,
            HttpServletResponse.SC_FORBIDDEN, new AuthenticationException(ex));
      }
    } else {
      LOG.debug("Falling back to {} (req={})", authHandler.getClass(), request);
      token = authHandler.authenticate(request, response);
    }
    return token;
  }

  private String getDelegationToken(HttpServletRequest request)
      throws IOException {
    String dToken = request.getHeader(
        DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER);
    if (dToken == null) {
      dToken = ServletUtils.getParameter(request,
          KerberosDelegationTokenAuthenticator.DELEGATION_PARAM);
    }
    return dToken;
  }

}
