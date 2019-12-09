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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

import com.google.common.annotations.VisibleForTesting;

/**
 * A ResourceManager specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMDelegationTokenSecretManager extends
    AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> implements
    Recoverable {
  private static final Log LOG = LogFactory
      .getLog(RMDelegationTokenSecretManager.class);

  private final ResourceManager rm;

  /**
   * Create a secret manager
   * @param delegationKeyUpdateInterval the number of milliseconds for rolling
   *        new secret keys.
   * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
   *        tokens in milliseconds
   * @param delegationTokenRenewInterval how often the tokens must be renewed
   *        in milliseconds
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   *        for expired tokens in milliseconds
   * @param rmContext current context of the ResourceManager
   */
  public RMDelegationTokenSecretManager(long delegationKeyUpdateInterval,
                                      long delegationTokenMaxLifetime,
                                      long delegationTokenRenewInterval,
                                      long delegationTokenRemoverScanInterval,
                                      RMContext rmContext) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.rm = rmContext.getResourceManager();
  }

  @Override
  public RMDelegationTokenIdentifier createIdentifier() {
    return new RMDelegationTokenIdentifier();
  }

  private boolean shouldIgnoreException(Exception e) {
    return !running && e.getCause() instanceof InterruptedException;
  }

  @Override
  protected void storeNewMasterKey(DelegationKey newKey) {
    try {
      LOG.info("storing master key with keyID " + newKey.getKeyId());
      rm.getRMContext().getStateStore().storeRMDTMasterKey(newKey);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error(
            "Error in storing master key with KeyID: " + newKey.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void removeStoredMasterKey(DelegationKey key) {
    try {
      LOG.info("removing master key with keyID " + key.getKeyId());
      rm.getRMContext().getStateStore().removeRMDTMasterKey(key);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing master key with KeyID: " + key.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void storeNewToken(RMDelegationTokenIdentifier identifier,
      long renewDate) {
    try {
      LOG.info("storing RMDelegation token with sequence number: "
          + identifier.getSequenceNumber());
      rm.getRMContext().getStateStore().storeRMDelegationToken(identifier,
          renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing RMDelegationToken with sequence number: "
            + identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void updateStoredToken(RMDelegationTokenIdentifier id,
      long renewDate) {
    try {
      LOG.info("updating RMDelegation token with sequence number: "
          + id.getSequenceNumber());
      rm.getRMContext().getStateStore().updateRMDelegationToken(id, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in updating persisted RMDelegationToken"
            + " with sequence number: " + id.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void removeStoredToken(RMDelegationTokenIdentifier ident)
      throws IOException {
    try {
      LOG.info("removing RMDelegation token with sequence number: "
          + ident.getSequenceNumber());
      rm.getRMContext().getStateStore().removeRMDelegationToken(ident);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error(
            "Error in removing RMDelegationToken with sequence number: "
                + ident.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Private
  @VisibleForTesting
  public synchronized Set<DelegationKey> getAllMasterKeys() {
    HashSet<DelegationKey> keySet = new HashSet<DelegationKey>();
    keySet.addAll(allKeys.values());
    return keySet;
  }

  @Private
  @VisibleForTesting
  public synchronized Map<RMDelegationTokenIdentifier, Long> getAllTokens() {
    Map<RMDelegationTokenIdentifier, Long> allTokens =
        new HashMap<RMDelegationTokenIdentifier, Long>();

    for (Map.Entry<RMDelegationTokenIdentifier,
        DelegationTokenInformation> entry : currentTokens.entrySet()) {
      allTokens.put(entry.getKey(), entry.getValue().getRenewDate());
    }
    return allTokens;
  }

  @Private
  @VisibleForTesting
  public int getLatestDTSequenceNumber() {
    return delegationTokenSequenceNumber;
  }

  @Override
  public void recover(RMState rmState) throws Exception {

    LOG.info("recovering RMDelegationTokenSecretManager.");
    // recover RMDTMasterKeys
    for (DelegationKey dtKey : rmState.getRMDTSecretManagerState()
      .getMasterKeyState()) {
      addKey(dtKey);
    }

    // recover RMDelegationTokens
    Map<RMDelegationTokenIdentifier, Long> rmDelegationTokens =
        rmState.getRMDTSecretManagerState().getTokenState();
    this.delegationTokenSequenceNumber =
        rmState.getRMDTSecretManagerState().getDTSequenceNumber();
    for (Map.Entry<RMDelegationTokenIdentifier, Long> entry : rmDelegationTokens
      .entrySet()) {
      addPersistedDelegationToken(entry.getKey(), entry.getValue());
    }
  }

  public long getRenewDate(RMDelegationTokenIdentifier ident)
      throws InvalidToken {
    DelegationTokenInformation info = currentTokens.get(ident);
    if (info == null) {
      throw new InvalidToken("token (" + ident.toString()
          + ") can't be found in cache");
    }
    return info.getRenewDate();
  }
}
