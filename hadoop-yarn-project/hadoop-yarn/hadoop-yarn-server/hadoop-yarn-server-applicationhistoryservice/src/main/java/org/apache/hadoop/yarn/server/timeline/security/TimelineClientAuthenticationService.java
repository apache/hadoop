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

package org.apache.hadoop.yarn.server.timeline.security;

import java.io.IOException;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDelegationTokenResponse;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenOperation;
import org.apache.hadoop.yarn.security.client.TimelineAuthenticationConsts;
import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AHSWebApp;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Server side <code>AuthenticationHandler</code> that authenticates requests
 * using the incoming delegation token as a 'delegation' query string parameter.
 * <p/>
 * If not delegation token is present in the request it delegates to the
 * {@link KerberosAuthenticationHandler}
 */
@Private
@Unstable
public class TimelineClientAuthenticationService
    extends KerberosAuthenticationHandler {

  public static final String TYPE = "kerberos-dt";
  private static final Set<String> DELEGATION_TOKEN_OPS = new HashSet<String>();
  private static final String OP_PARAM = "op";
  private static final String ENTER = System.getProperty("line.separator");

  private ObjectMapper mapper;

  static {
    DELEGATION_TOKEN_OPS.add(
        TimelineDelegationTokenOperation.GETDELEGATIONTOKEN.toString());
    DELEGATION_TOKEN_OPS.add(
        TimelineDelegationTokenOperation.RENEWDELEGATIONTOKEN.toString());
    DELEGATION_TOKEN_OPS.add(
        TimelineDelegationTokenOperation.CANCELDELEGATIONTOKEN.toString());
  }

  public TimelineClientAuthenticationService() {
    super();
    mapper = new ObjectMapper();
    YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
  }

  /**
   * Returns authentication type of the handler.
   * 
   * @return <code>delegationtoken-kerberos</code>
   */
  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean managementOperation(AuthenticationToken token,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    boolean requestContinues = true;
    String op = request.getParameter(OP_PARAM);
    op = (op != null) ? op.toUpperCase() : null;
    if (DELEGATION_TOKEN_OPS.contains(op) &&
        !request.getMethod().equals("OPTIONS")) {
      TimelineDelegationTokenOperation dtOp =
          TimelineDelegationTokenOperation.valueOf(op);
      if (dtOp.getHttpMethod().equals(request.getMethod())) {
        if (dtOp.requiresKerberosCredentials() && token == null) {
          response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
              MessageFormat.format(
                  "Operation [{0}] requires SPNEGO authentication established",
                  dtOp));
          requestContinues = false;
        } else {
          TimelineDelegationTokenSecretManagerService secretManager =
              AHSWebApp.getInstance()
                  .getTimelineDelegationTokenSecretManagerService();
          try {
            TimelineDelegationTokenResponse res = null;
            switch (dtOp) {
              case GETDELEGATIONTOKEN:
                UserGroupInformation ownerUGI =
                    UserGroupInformation.createRemoteUser(token.getUserName());
                String renewerParam =
                    request
                        .getParameter(TimelineAuthenticationConsts.RENEWER_PARAM);
                if (renewerParam == null) {
                  renewerParam = token.getUserName();
                }
                Token<?> dToken =
                    secretManager.createToken(ownerUGI, renewerParam);
                res = new TimelineDelegationTokenResponse();
                res.setType(TimelineAuthenticationConsts.DELEGATION_TOKEN_URL);
                res.setContent(dToken.encodeToUrlString());
                break;
              case RENEWDELEGATIONTOKEN:
              case CANCELDELEGATIONTOKEN:
                String tokenParam =
                    request
                        .getParameter(TimelineAuthenticationConsts.TOKEN_PARAM);
                if (tokenParam == null) {
                  response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                      MessageFormat
                          .format(
                              "Operation [{0}] requires the parameter [{1}]",
                              dtOp,
                              TimelineAuthenticationConsts.TOKEN_PARAM));
                  requestContinues = false;
                } else {
                  if (dtOp == TimelineDelegationTokenOperation.CANCELDELEGATIONTOKEN) {
                    Token<TimelineDelegationTokenIdentifier> dt =
                        new Token<TimelineDelegationTokenIdentifier>();
                    dt.decodeFromUrlString(tokenParam);
                    secretManager.cancelToken(dt, token.getUserName());
                  } else {
                    Token<TimelineDelegationTokenIdentifier> dt =
                        new Token<TimelineDelegationTokenIdentifier>();
                    dt.decodeFromUrlString(tokenParam);
                    long expirationTime =
                        secretManager.renewToken(dt, token.getUserName());
                    res = new TimelineDelegationTokenResponse();
                    res.setType(TimelineAuthenticationConsts.DELEGATION_TOKEN_EXPIRATION_TIME);
                    res.setContent(expirationTime);
                  }
                }
                break;
            }
            if (requestContinues) {
              response.setStatus(HttpServletResponse.SC_OK);
              if (res != null) {
                response.setContentType(MediaType.APPLICATION_JSON);
                Writer writer = response.getWriter();
                mapper.writeValue(writer, res);
                writer.write(ENTER);
                writer.flush();

              }
              requestContinues = false;
            }
          } catch (IOException e) {
            throw new AuthenticationException(e.toString(), e);
          }
        }
      } else {
        response
            .sendError(
                HttpServletResponse.SC_BAD_REQUEST,
                MessageFormat
                    .format(
                        "Wrong HTTP method [{0}] for operation [{1}], it should be [{2}]",
                        request.getMethod(), dtOp, dtOp.getHttpMethod()));
        requestContinues = false;
      }
    }
    return requestContinues;
  }

  /**
   * Authenticates a request looking for the <code>delegation</code>
   * query-string parameter and verifying it is a valid token. If there is not
   * <code>delegation</code> query-string parameter, it delegates the
   * authentication to the {@link KerberosAuthenticationHandler} unless it is
   * disabled.
   * 
   * @param request
   *          the HTTP client request.
   * @param response
   *          the HTTP client response.
   * 
   * @return the authentication token for the authenticated request.
   * @throws IOException
   *           thrown if an IO error occurred.
   * @throws AuthenticationException
   *           thrown if the authentication failed.
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
      HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken token;
    String delegationParam =
        request
            .getParameter(TimelineAuthenticationConsts.DELEGATION_PARAM);
    if (delegationParam != null) {
      Token<TimelineDelegationTokenIdentifier> dt =
          new Token<TimelineDelegationTokenIdentifier>();
      dt.decodeFromUrlString(delegationParam);
      TimelineDelegationTokenSecretManagerService secretManager =
          AHSWebApp.getInstance()
              .getTimelineDelegationTokenSecretManagerService();
      UserGroupInformation ugi = secretManager.verifyToken(dt);
      final String shortName = ugi.getShortUserName();
      // creating a ephemeral token
      token = new AuthenticationToken(shortName, ugi.getUserName(), getType());
      token.setExpires(0);
    } else {
      token = super.authenticate(request, response);
    }
    return token;
  }

}
