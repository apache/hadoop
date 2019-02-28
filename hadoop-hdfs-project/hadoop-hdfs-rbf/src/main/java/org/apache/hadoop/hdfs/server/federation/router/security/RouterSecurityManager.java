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

package org.apache.hadoop.hdfs.server.federation.router.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * Manager to hold underlying delegation token secret manager implementations.
 */
public class RouterSecurityManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterSecurityManager.class);

  private AbstractDelegationTokenSecretManager<DelegationTokenIdentifier>
      dtSecretManager = null;

  public RouterSecurityManager(Configuration conf) {
    AuthenticationMethod authMethodConfigured =
        SecurityUtil.getAuthenticationMethod(conf);
    AuthenticationMethod authMethodToInit =
        AuthenticationMethod.KERBEROS;
    if (authMethodConfigured.equals(authMethodToInit)) {
      this.dtSecretManager = newSecretManager(conf);
    }
  }

  @VisibleForTesting
  public RouterSecurityManager(AbstractDelegationTokenSecretManager
      <DelegationTokenIdentifier> dtSecretManager) {
    this.dtSecretManager = dtSecretManager;
  }

  /**
   * Creates an instance of a SecretManager from the configuration.
   *
   * @param conf Configuration that defines the secret manager class.
   * @return New secret manager.
   */
  public static AbstractDelegationTokenSecretManager<DelegationTokenIdentifier>
      newSecretManager(Configuration conf) {
    Class<? extends AbstractDelegationTokenSecretManager> clazz =
        conf.getClass(
        RBFConfigKeys.DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS,
        RBFConfigKeys.DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS_DEFAULT,
        AbstractDelegationTokenSecretManager.class);
    AbstractDelegationTokenSecretManager secretManager;
    try {
      Constructor constructor = clazz.getConstructor(Configuration.class);
      secretManager = (AbstractDelegationTokenSecretManager)
          constructor.newInstance(conf);
      LOG.info("Delegation token secret manager object instantiated");
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: {}", clazz.getSimpleName(),
          e.getCause());
      return null;
    } catch (RuntimeException e) {
      LOG.error("RuntimeException to instantiate: {}",
          clazz.getSimpleName(), e);
      return null;
    }
    return secretManager;
  }

  public AbstractDelegationTokenSecretManager<DelegationTokenIdentifier>
      getSecretManager() {
    return this.dtSecretManager;
  }

  public void stop() {
    LOG.info("Stopping security manager");
    if(this.dtSecretManager != null) {
      this.dtSecretManager.stopThreads();
    }
  }

  private static UserGroupInformation getRemoteUser() throws IOException {
    return RouterRpcServer.getRemoteUser();
  }
  /**
   * Returns authentication method used to establish the connection.
   * @return AuthenticationMethod used to establish connection.
   * @throws IOException
   */
  private UserGroupInformation.AuthenticationMethod
      getConnectionAuthenticationMethod() throws IOException {
    UserGroupInformation ugi = getRemoteUser();
    UserGroupInformation.AuthenticationMethod authMethod
        = ugi.getAuthenticationMethod();
    if (authMethod == UserGroupInformation.AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  /**
   *
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL)
        && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }

  /**
   * @param renewer Renewer information
   * @return delegation token
   * @throws IOException on error
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    LOG.debug("Generate delegation token with renewer " + renewer);
    final String operationName = "getDelegationToken";
    boolean success = false;
    String tokenId = "";
    Token<DelegationTokenIdentifier> token;
    try {
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
            "Delegation Token can be issued only " +
                "with kerberos or web authentication");
      }
      if (dtSecretManager == null || !dtSecretManager.isRunning()) {
        LOG.warn("trying to get DT with no secret manager running");
        return null;
      }
      UserGroupInformation ugi = getRemoteUser();
      String user = ugi.getUserName();
      Text owner = new Text(user);
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }
      DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner,
          renewer, realUser);
      token = new Token<DelegationTokenIdentifier>(
          dtId, dtSecretManager);
      tokenId = dtId.toStringStable();
      success = true;
    } finally {
      logAuditEvent(success, operationName, tokenId);
    }
    return token;
  }

  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
          throws SecretManager.InvalidToken, IOException {
    LOG.debug("Renew delegation token");
    final String operationName = "renewDelegationToken";
    boolean success = false;
    String tokenId = "";
    long expiryTime;
    try {
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
            "Delegation Token can be renewed only " +
                "with kerberos or web authentication");
      }
      String renewer = getRemoteUser().getShortUserName();
      expiryTime = dtSecretManager.renewToken(token, renewer);
      final DelegationTokenIdentifier id = DFSUtil.decodeDelegationToken(token);
      tokenId = id.toStringStable();
      success = true;
    } catch (AccessControlException ace) {
      final DelegationTokenIdentifier id = DFSUtil.decodeDelegationToken(token);
      tokenId = id.toStringStable();
      throw ace;
    } finally {
      logAuditEvent(success, operationName, tokenId);
    }
    return expiryTime;
  }

  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
          throws IOException {
    LOG.debug("Cancel delegation token");
    final String operationName = "cancelDelegationToken";
    boolean success = false;
    String tokenId = "";
    try {
      String canceller = getRemoteUser().getUserName();
      LOG.info("Cancel request by " + canceller);
      DelegationTokenIdentifier id =
          dtSecretManager.cancelToken(token, canceller);
      tokenId = id.toStringStable();
      success = true;
    } catch (AccessControlException ace) {
      final DelegationTokenIdentifier id = DFSUtil.decodeDelegationToken(token);
      tokenId = id.toStringStable();
      throw ace;
    } finally {
      logAuditEvent(success, operationName, tokenId);
    }
  }

  /**
   * Log status of delegation token related operation.
   * Extend in future to use audit logger instead of local logging.
   */
  void logAuditEvent(boolean succeeded, String cmd, String tokenId)
      throws IOException {
    LOG.debug(
        "Operation:" + cmd +
        " Status:" + succeeded +
        " TokenId:" + tokenId);
  }
}
