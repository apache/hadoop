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
package org.apache.hadoop.yarn.server.router.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.*;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A Router specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 */
public class RouterDelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(RouterDelegationTokenSecretManager.class);

  private FederationStateStoreFacade federationFacade;

  /**
   * Create a secret manager.
   *
   * @param delegationKeyUpdateInterval        the number of milliseconds for rolling
   *                                           new secret keys.
   * @param delegationTokenMaxLifetime         the maximum lifetime of the delegation
   *                                           tokens in milliseconds
   * @param delegationTokenRenewInterval       how often the tokens must be renewed
   *                                           in milliseconds
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   */
  public RouterDelegationTokenSecretManager(long delegationKeyUpdateInterval,
                                            long delegationTokenMaxLifetime,
                                            long delegationTokenRenewInterval,
                                            long delegationTokenRemoverScanInterval) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    federationFacade = FederationStateStoreFacade.getInstance();
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
      LOG.info("Storing master key with keyID {}.", newKey.getKeyId());
      ByteBuffer keyBytes = ByteBuffer.wrap(newKey.getEncodedKey());
      RouterMasterKey masterKey = RouterMasterKey.newInstance(newKey.getKeyId(),
          keyBytes, newKey.getExpiryDate());
      StoreNewMasterKeyRequest keyRequest = StoreNewMasterKeyRequest.newInstance(masterKey);
      federationFacade.getStateStore().storeNewMasterKey(keyRequest);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing master key with KeyID: {}.", newKey.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void removeStoredMasterKey(DelegationKey key) {
    try {
      LOG.info("removing master key with keyID {}.", key.getKeyId());
      ByteBuffer keyBytes = ByteBuffer.wrap(key.getEncodedKey());
      RouterMasterKey masterKey = RouterMasterKey.newInstance(key.getKeyId(),
          keyBytes, key.getExpiryDate());
      RemoveStoredMasterKeyRequest keyRequest = RemoveStoredMasterKeyRequest.newInstance(masterKey);
      federationFacade.getStateStore().removeStoredMasterKey(keyRequest);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing master key with KeyID: {}.", key.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void storeNewToken(RMDelegationTokenIdentifier identifier,
      long renewDate) throws IOException {
    try {
      LOG.info("storing RMDelegation token with sequence number: {}.",
          identifier.getSequenceNumber());
      RouterStoreToken storeToken = RouterStoreToken.newInstance(identifier, renewDate);
      RouterStoreNewTokenRequest request = RouterStoreNewTokenRequest.newInstance(storeToken);
      federationFacade.getStateStore().storeNewToken(request);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing RMDelegationToken with sequence number: {}.",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void updateStoredToken(RMDelegationTokenIdentifier id, long renewDate) {
    try {
      LOG.info("updating RMDelegation token with sequence number: {}.",
          id.getSequenceNumber());
        // rm.getRMContext().getStateStore().updateRMDelegationToken(id, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in updating persisted RMDelegationToken with sequence number: {}.",
            id.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void removeStoredToken(RMDelegationTokenIdentifier
      identifier) throws IOException {
    try {
      LOG.info("removing RMDelegation token with sequence number: {}",
          identifier.getSequenceNumber());
       // rm.getRMContext().getStateStore().removeRMDelegationToken(ident);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing RMDelegationToken with sequence number: {}",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public int getLatestDTSequenceNumber() {
    return delegationTokenSequenceNumber;
  }
}
