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
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
   * Create a Router Secret manager.
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
  }

  @Override
  public RMDelegationTokenIdentifier createIdentifier() {
    return new RMDelegationTokenIdentifier();
  }

  private boolean shouldIgnoreException(Exception e) {
    return !running && e.getCause() instanceof InterruptedException;
  }

  /**
   * The Router Supports Store the new master key.
   *
   * @param newKey DelegationKey
   */
  @Override
  protected void storeNewMasterKey(DelegationKey newKey) {
    try {
      federationFacade.storeNewMasterKey(newKey);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing master key with KeyID: {}.", newKey.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Remove the master key.
   *
   * @param delegationKey DelegationKey
   */
  @Override
  protected void removeStoredMasterKey(DelegationKey delegationKey) {
    try {
      federationFacade.removeStoredMasterKey(delegationKey);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing master key with KeyID: {}.", delegationKey.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Store new Token.
   *
   * @param identifier Delegation Token
   * @param renewDate renewDate
   * @throws IOException IO exception occurred.
   */
  @Override
  protected void storeNewToken(RMDelegationTokenIdentifier identifier,
      long renewDate) throws IOException {
    try {
      federationFacade.storeNewToken(identifier, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing RMDelegationToken with sequence number: {}.",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Update Token.
   *
   * @param id Delegation Token
   * @param renewDate renewDate
   * @throws IOException IO exception occurred.
   */
  @Override
  protected void updateStoredToken(RMDelegationTokenIdentifier id, long renewDate)
      throws IOException {
    try {
      federationFacade.updateStoredToken(id, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in updating persisted RMDelegationToken with sequence number: {}.",
            id.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Remove Token.
   *
   * @param identifier Delegation Token
   * @throws IOException IO exception occurred.
   */
  @Override
  protected void removeStoredToken(RMDelegationTokenIdentifier
      identifier) throws IOException {
    try {
      federationFacade.removeStoredToken(identifier);
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
