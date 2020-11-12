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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Delegation Token Manager used by the
 * {@link KerberosDelegationTokenAuthenticationHandler}.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DelegationTokenManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(DelegationTokenManager.class);

  public static final String ENABLE_ZK_KEY = "zk-dt-secret-manager.enable";

  public static final String PREFIX = "delegation-token.";

  public static final String UPDATE_INTERVAL = PREFIX + "update-interval.sec";
  public static final long UPDATE_INTERVAL_DEFAULT = 24 * 60 * 60;

  public static final String MAX_LIFETIME = PREFIX + "max-lifetime.sec";
  public static final long MAX_LIFETIME_DEFAULT = 7 * 24 * 60 * 60;

  public static final String RENEW_INTERVAL = PREFIX + "renew-interval.sec";
  public static final long RENEW_INTERVAL_DEFAULT = 24 * 60 * 60;

  public static final String REMOVAL_SCAN_INTERVAL = PREFIX +
      "removal-scan-interval.sec";
  public static final long REMOVAL_SCAN_INTERVAL_DEFAULT = 60 * 60;

  private static class DelegationTokenSecretManager
      extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

    private Text tokenKind;

    public DelegationTokenSecretManager(Configuration conf, Text tokenKind) {
      super(conf.getLong(UPDATE_INTERVAL, UPDATE_INTERVAL_DEFAULT) * 1000,
          conf.getLong(MAX_LIFETIME, MAX_LIFETIME_DEFAULT) * 1000,
          conf.getLong(RENEW_INTERVAL, RENEW_INTERVAL_DEFAULT) * 1000,
          conf.getLong(REMOVAL_SCAN_INTERVAL, REMOVAL_SCAN_INTERVAL_DEFAULT)
              * 1000);
      this.tokenKind = tokenKind;
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return new DelegationTokenIdentifier(tokenKind);
    }

    @Override
    public DelegationTokenIdentifier decodeTokenIdentifier(
        Token<DelegationTokenIdentifier> token) throws IOException {
      return DelegationTokenManager.decodeToken(token, tokenKind);
    }

  }

  private static class ZKSecretManager
      extends ZKDelegationTokenSecretManager<DelegationTokenIdentifier> {

    private Text tokenKind;

    public ZKSecretManager(Configuration conf, Text tokenKind) {
      super(conf);
      this.tokenKind = tokenKind;
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return new DelegationTokenIdentifier(tokenKind);
    }

    @Override
    public DelegationTokenIdentifier decodeTokenIdentifier(
        Token<DelegationTokenIdentifier> token) throws IOException {
      return DelegationTokenManager.decodeToken(token, tokenKind);
    }
  }

  private AbstractDelegationTokenSecretManager secretManager = null;
  private boolean managedSecretManager;

  public DelegationTokenManager(Configuration conf, Text tokenKind) {
    if (conf.getBoolean(ENABLE_ZK_KEY, false)) {
      this.secretManager = new ZKSecretManager(conf, tokenKind);
    } else {
      this.secretManager = new DelegationTokenSecretManager(conf, tokenKind);
    }
    managedSecretManager = true;
  }

  /**
   * Sets an external <code>DelegationTokenSecretManager</code> instance to
   * manage creation and verification of Delegation Tokens.
   * <p>
   * This is useful for use cases where secrets must be shared across multiple
   * services.
   *
   * @param secretManager a <code>DelegationTokenSecretManager</code> instance
   */
  public void setExternalDelegationTokenSecretManager(
      AbstractDelegationTokenSecretManager secretManager) {
    this.secretManager.stopThreads();
    this.secretManager = secretManager;
    managedSecretManager = false;
  }

  public void init() {
    if (managedSecretManager) {
      try {
        secretManager.startThreads();
      } catch (IOException ex) {
        throw new RuntimeException("Could not start " +
            secretManager.getClass() + ": " + ex.toString(), ex);
      }
    }
  }

  public void destroy() {
    if (managedSecretManager) {
      secretManager.stopThreads();
    }
  }

  @SuppressWarnings("unchecked")
  public Token<? extends AbstractDelegationTokenIdentifier> createToken(
      UserGroupInformation ugi, String renewer) {
    return createToken(ugi, renewer, null);
  }

  @SuppressWarnings("unchecked")
  public Token<? extends AbstractDelegationTokenIdentifier> createToken(
      UserGroupInformation ugi, String renewer, String service) {
    LOG.debug("Creating token with ugi:{}, renewer:{}, service:{}.",
        ugi, renewer, service !=null ? service : "");
    renewer = (renewer == null) ? ugi.getShortUserName() : renewer;
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    AbstractDelegationTokenIdentifier tokenIdentifier =
        (AbstractDelegationTokenIdentifier) secretManager.createIdentifier();
    tokenIdentifier.setOwner(owner);
    tokenIdentifier.setRenewer(new Text(renewer));
    tokenIdentifier.setRealUser(realUser);
    Token token = new Token(tokenIdentifier, secretManager);
    if (service != null) {
      token.setService(new Text(service));
    }
    return token;
  }

  @SuppressWarnings("unchecked")
  public long renewToken(
      Token<? extends AbstractDelegationTokenIdentifier> token, String renewer)
          throws IOException {
    LOG.debug("Renewing token:{} with renewer:{}.", token, renewer);
    return secretManager.renewToken(token, renewer);
  }

  @SuppressWarnings("unchecked")
  public void cancelToken(
      Token<? extends AbstractDelegationTokenIdentifier> token,
      String canceler) throws IOException {
    LOG.debug("Cancelling token:{} with canceler:{}.", token, canceler);
    canceler = (canceler != null) ? canceler :
               verifyToken(token).getShortUserName();
    secretManager.cancelToken(token, canceler);
  }

  @SuppressWarnings("unchecked")
  public UserGroupInformation verifyToken(
      Token<? extends AbstractDelegationTokenIdentifier> token)
          throws IOException {
    AbstractDelegationTokenIdentifier id = secretManager.decodeTokenIdentifier(token);
    secretManager.verifyToken(id, token.getPassword());
    return id.getUser();
  }

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public AbstractDelegationTokenSecretManager getDelegationTokenSecretManager() {
    return secretManager;
  }

  private static DelegationTokenIdentifier decodeToken(
      Token<DelegationTokenIdentifier> token, Text tokenKind)
          throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream dis = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier(tokenKind);
    id.readFields(dis);
    dis.close();
    return id;
  }
}
