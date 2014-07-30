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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

public class RMAuthenticationHandler extends KerberosAuthenticationHandler {

  public static final String TYPE = "kerberos-dt";
  public static final String HEADER = "Hadoop-YARN-Auth-Delegation-Token";

  static RMDelegationTokenSecretManager secretManager;
  static boolean secretManagerInitialized = false;

  public RMAuthenticationHandler() {
    super();
  }

  /**
   * Returns authentication type of the handler.
   * 
   * @return <code>kerberos-dt</code>
   */
  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean managementOperation(AuthenticationToken token,
      HttpServletRequest request, HttpServletResponse response) {
    return true;
  }

  /**
   * Authenticates a request looking for the <code>delegation</code> header and
   * verifying it is a valid token. If the header is missing, it delegates the
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
      HttpServletResponse response) throws IOException, AuthenticationException {

    AuthenticationToken token;
    String delegationParam = this.getEncodedDelegationTokenFromRequest(request);
    if (delegationParam != null) {
      Token<RMDelegationTokenIdentifier> dt =
          new Token<RMDelegationTokenIdentifier>();
      ;
      dt.decodeFromUrlString(delegationParam);
      UserGroupInformation ugi = this.verifyToken(dt);
      if (ugi == null) {
        throw new AuthenticationException("Invalid token");
      }
      final String shortName = ugi.getShortUserName();
      token = new AuthenticationToken(shortName, ugi.getUserName(), getType());
    } else {
      token = super.authenticate(request, response);
      if (token != null) {
        // create a token with auth type set correctly
        token =
            new AuthenticationToken(token.getUserName(), token.getName(),
              super.getType());
      }
    }
    return token;
  }

  /**
   * Verifies a delegation token.
   * 
   * @param token
   *          delegation token to verify.
   * @return the UGI for the token; null if the verification fails
   * @throws IOException
   *           thrown if the token could not be verified.
   */
  protected UserGroupInformation verifyToken(
      Token<RMDelegationTokenIdentifier> token) throws IOException {
    if (secretManagerInitialized == false) {
      throw new IllegalStateException("Secret manager not initialized");
    }
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream dis = new DataInputStream(buf);
    RMDelegationTokenIdentifier id = secretManager.createIdentifier();
    try {
      id.readFields(dis);
      secretManager.verifyToken(id, token.getPassword());
    } catch (Throwable t) {
      return null;
    } finally {
      dis.close();
    }
    return id.getUser();
  }

  /**
   * Extract encoded delegation token from request
   * 
   * @param req
   *          HTTPServletRequest object
   * 
   * @return String containing the encoded token; null if encoded token not
   *         found
   * 
   */
  protected String getEncodedDelegationTokenFromRequest(HttpServletRequest req) {
    String header = req.getHeader(HEADER);
    return header;
  }

  public static void setSecretManager(RMDelegationTokenSecretManager manager) {
    secretManager = manager;
    secretManagerInitialized = true;
  }

}
