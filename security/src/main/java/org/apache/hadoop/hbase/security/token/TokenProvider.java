/*
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

package org.apache.hadoop.hbase.security.token;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.SecureServer;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

/**
 * Provides a service for obtaining authentication tokens via the
 * {@link AuthenticationProtocol} coprocessor protocol.
 */
public class TokenProvider extends BaseEndpointCoprocessor
    implements AuthenticationProtocol {

  public static final long VERSION = 0L;
  private static Log LOG = LogFactory.getLog(TokenProvider.class);

  private AuthenticationTokenSecretManager secretManager;


  @Override
  public void start(CoprocessorEnvironment env) {
    super.start(env);

    // if running at region
    if (env instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment regionEnv =
          (RegionCoprocessorEnvironment)env;
      RpcServer server = regionEnv.getRegionServerServices().getRpcServer();
      if (server instanceof SecureServer) {
        SecretManager mgr = ((SecureServer)server).getSecretManager();
        if (mgr instanceof AuthenticationTokenSecretManager) {
          secretManager = (AuthenticationTokenSecretManager)mgr;
        }
      }
    }
  }

  @Override
  public Token<AuthenticationTokenIdentifier> getAuthenticationToken()
      throws IOException {
    if (secretManager == null) {
      throw new IOException(
          "No secret manager configured for token authentication");
    }

    User currentUser = RequestContext.getRequestUser();
    UserGroupInformation ugi = null;
    if (currentUser != null) {
      ugi = currentUser.getUGI();
    }
    if (currentUser == null) {
      throw new AccessDeniedException("No authenticated user for request!");
    } else if (ugi.getAuthenticationMethod() !=
        UserGroupInformation.AuthenticationMethod.KERBEROS) {
      LOG.warn("Token generation denied for user="+currentUser.getName()
          +", authMethod="+ugi.getAuthenticationMethod());
      throw new AccessDeniedException(
          "Token generation only allowed for Kerberos authenticated clients");
    }

    return secretManager.generateToken(currentUser.getName());
  }

  @Override
  public String whoami() {
    return RequestContext.getRequestUserName();
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (AuthenticationProtocol.class.getName().equals(protocol)) {
      return TokenProvider.VERSION;
    }
    LOG.warn("Unknown protocol requested: "+protocol);
    return -1;
  }
}
