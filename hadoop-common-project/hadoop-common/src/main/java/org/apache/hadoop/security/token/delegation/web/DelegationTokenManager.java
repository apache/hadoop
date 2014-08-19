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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Delegation Token Manager used by the
 * {@link KerberosDelegationTokenAuthenticationHandler}.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class DelegationTokenManager {

  private static class DelegationTokenSecretManager
      extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

    private Text tokenKind;

    public DelegationTokenSecretManager(Text tokenKind,
        long delegationKeyUpdateInterval,
        long delegationTokenMaxLifetime,
        long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
      this.tokenKind = tokenKind;
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return new DelegationTokenIdentifier(tokenKind);
    }

  }

  private AbstractDelegationTokenSecretManager secretManager = null;
  private boolean managedSecretManager;
  private Text tokenKind;

  public DelegationTokenManager(Text tokenKind,
      long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime,
      long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval) {
    this.secretManager = new DelegationTokenSecretManager(tokenKind,
        delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.tokenKind = tokenKind;
    managedSecretManager = true;
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
    this.secretManager.stopThreads();
    this.secretManager = secretManager;
    this.tokenKind = secretManager.createIdentifier().getKind();
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
  public Token<DelegationTokenIdentifier> createToken(UserGroupInformation ugi,
      String renewer) {
    renewer = (renewer == null) ? ugi.getShortUserName() : renewer;
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    DelegationTokenIdentifier tokenIdentifier = new DelegationTokenIdentifier(
        tokenKind, owner, new Text(renewer), realUser);
    return new Token<DelegationTokenIdentifier>(tokenIdentifier, secretManager);
  }

  @SuppressWarnings("unchecked")
  public long renewToken(Token<DelegationTokenIdentifier> token, String renewer)
      throws IOException {
    return secretManager.renewToken(token, renewer);
  }

  @SuppressWarnings("unchecked")
  public void cancelToken(Token<DelegationTokenIdentifier> token,
      String canceler) throws IOException {
    canceler = (canceler != null) ? canceler :
               verifyToken(token).getShortUserName();
    secretManager.cancelToken(token, canceler);
  }

  @SuppressWarnings("unchecked")
  public UserGroupInformation verifyToken(Token<DelegationTokenIdentifier>
      token) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream dis = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier(tokenKind);
    id.readFields(dis);
    dis.close();
    secretManager.verifyToken(id, token.getPassword());
    return id.getUser();
  }

}
