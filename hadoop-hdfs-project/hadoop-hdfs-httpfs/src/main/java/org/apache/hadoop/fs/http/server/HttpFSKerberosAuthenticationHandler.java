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
package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.client.HttpFSKerberosAuthenticator;
import org.apache.hadoop.fs.http.client.HttpFSKerberosAuthenticator.DelegationTokenOperation;
import org.apache.hadoop.lib.service.DelegationTokenIdentifier;
import org.apache.hadoop.lib.service.DelegationTokenManager;
import org.apache.hadoop.lib.service.DelegationTokenManagerException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.token.Token;
import org.json.simple.JSONObject;

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
import java.util.Set;

/**
 * Server side <code>AuthenticationHandler</code> that authenticates requests
 * using the incoming delegation token as a 'delegation' query string parameter.
 * <p/>
 * If not delegation token is present in the request it delegates to the
 * {@link KerberosAuthenticationHandler}
 */
@InterfaceAudience.Private
public class HttpFSKerberosAuthenticationHandler
  extends KerberosAuthenticationHandler {

  static final Set<String> DELEGATION_TOKEN_OPS =
    new HashSet<String>();

  static {
    DELEGATION_TOKEN_OPS.add(
      DelegationTokenOperation.GETDELEGATIONTOKEN.toString());
    DELEGATION_TOKEN_OPS.add(
      DelegationTokenOperation.RENEWDELEGATIONTOKEN.toString());
    DELEGATION_TOKEN_OPS.add(
      DelegationTokenOperation.CANCELDELEGATIONTOKEN.toString());
  }

  public static final String TYPE = "kerberos-dt";

  /**
   * Returns authentication type of the handler.
   *
   * @return <code>delegationtoken-kerberos</code>
   */
  @Override
  public String getType() {
    return TYPE;
  }

  private static final String ENTER = System.getProperty("line.separator");

  @Override
  @SuppressWarnings("unchecked")
  public boolean managementOperation(AuthenticationToken token,
      HttpServletRequest request, HttpServletResponse response)
    throws IOException, AuthenticationException {
    boolean requestContinues = true;
    String op = request.getParameter(HttpFSFileSystem.OP_PARAM);
    op = (op != null) ? op.toUpperCase() : null;
    if (DELEGATION_TOKEN_OPS.contains(op) &&
        !request.getMethod().equals("OPTIONS")) {
      DelegationTokenOperation dtOp =
        DelegationTokenOperation.valueOf(op);
      if (dtOp.getHttpMethod().equals(request.getMethod())) {
        if (dtOp.requiresKerberosCredentials() && token == null) {
          response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
            MessageFormat.format(
              "Operation [{0}] requires SPNEGO authentication established",
              dtOp));
          requestContinues = false;
        } else {
          DelegationTokenManager tokenManager =
            HttpFSServerWebApp.get().get(DelegationTokenManager.class);
          try {
            Map map = null;
            switch (dtOp) {
              case GETDELEGATIONTOKEN:
                String renewerParam =
                  request.getParameter(HttpFSKerberosAuthenticator.RENEWER_PARAM);
                if (renewerParam == null) {
                  renewerParam = token.getUserName();
                }
                Token<?> dToken = tokenManager.createToken(
                  UserGroupInformation.getCurrentUser(), renewerParam);
                map = delegationTokenToJSON(dToken);
                break;
              case RENEWDELEGATIONTOKEN:
              case CANCELDELEGATIONTOKEN:
                String tokenParam =
                  request.getParameter(HttpFSKerberosAuthenticator.TOKEN_PARAM);
                if (tokenParam == null) {
                  response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    MessageFormat.format(
                      "Operation [{0}] requires the parameter [{1}]",
                      dtOp, HttpFSKerberosAuthenticator.TOKEN_PARAM));
                  requestContinues = false;
                } else {
                  if (dtOp == DelegationTokenOperation.CANCELDELEGATIONTOKEN) {
                    Token<DelegationTokenIdentifier> dt =
                      new Token<DelegationTokenIdentifier>();
                    dt.decodeFromUrlString(tokenParam);
                    tokenManager.cancelToken(dt,
                      UserGroupInformation.getCurrentUser().getUserName());
                  } else {
                    Token<DelegationTokenIdentifier> dt =
                      new Token<DelegationTokenIdentifier>();
                    dt.decodeFromUrlString(tokenParam);
                    long expirationTime =
                      tokenManager.renewToken(dt, token.getUserName());
                    map = new HashMap();
                    map.put("long", expirationTime);
                  }
                }
                break;
            }
            if (requestContinues) {
              response.setStatus(HttpServletResponse.SC_OK);
              if (map != null) {
                response.setContentType(MediaType.APPLICATION_JSON);
                Writer writer = response.getWriter();
                JSONObject.writeJSONString(map, writer);
                writer.write(ENTER);
                writer.flush();

              }
              requestContinues = false;
            }
          } catch (DelegationTokenManagerException ex) {
            throw new AuthenticationException(ex.toString(), ex);
          }
        }
      } else {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
          MessageFormat.format(
            "Wrong HTTP method [{0}] for operation [{1}], it should be [{2}]",
            request.getMethod(), dtOp, dtOp.getHttpMethod()));
        requestContinues = false;
      }
    }
    return requestContinues;
  }

  @SuppressWarnings("unchecked")
  private static Map delegationTokenToJSON(Token token) throws IOException {
    Map json = new LinkedHashMap();
    json.put(HttpFSKerberosAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON,
             token.encodeToUrlString());
    Map response = new LinkedHashMap();
    response.put(HttpFSKerberosAuthenticator.DELEGATION_TOKEN_JSON, json);
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
   *
   * @return the authentication token for the authenticated request.
   * @throws IOException thrown if an IO error occurred.
   * @throws AuthenticationException thrown if the authentication failed.
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
                                          HttpServletResponse response)
    throws IOException, AuthenticationException {
    AuthenticationToken token;
    String delegationParam =
      request.getParameter(HttpFSKerberosAuthenticator.DELEGATION_PARAM);
    if (delegationParam != null) {
      try {
        Token<DelegationTokenIdentifier> dt =
          new Token<DelegationTokenIdentifier>();
        dt.decodeFromUrlString(delegationParam);
        DelegationTokenManager tokenManager =
          HttpFSServerWebApp.get().get(DelegationTokenManager.class);
        UserGroupInformation ugi = tokenManager.verifyToken(dt);
        final String shortName = ugi.getShortUserName();

        // creating a ephemeral token
        token = new AuthenticationToken(shortName, ugi.getUserName(),
                                        getType());
        token.setExpires(0);
      } catch (Throwable ex) {
        throw new AuthenticationException("Could not verify DelegationToken, " +
                                          ex.toString(), ex);
      }
    } else {
      token = super.authenticate(request, response);
    }
    return token;
  }


}
