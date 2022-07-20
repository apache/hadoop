/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.security;

import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RouterDelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterDelegationTokenSecretManager.class);

  public RouterDelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
  }

  @Override
  public RMDelegationTokenIdentifier createIdentifier() {
    return new RMDelegationTokenIdentifier();
  }

  @Override
  public synchronized void addKey(DelegationKey key) throws IOException {
    this.allKeys.put(key.getKeyId(), key);
  }

  @Override
  public synchronized void addPersistedDelegationToken(
      RMDelegationTokenIdentifier identifier, long renewDate) throws IOException {
    int keyId = identifier.getMasterKeyId();
    DelegationKey dKey = this.allKeys.get(keyId);
    if (dKey == null) {
      LOG.warn("No KEY found for persisted identifier ({}).", identifier);
    } else {
      byte[] password = createPassword(identifier.getBytes(), dKey.getKey());
      if (identifier.getSequenceNumber() > this.getDelegationTokenSeqNum()) {
        this.setDelegationTokenSeqNum(identifier.getSequenceNumber());
      }
      this.currentTokens.put(identifier,
          new DelegationTokenInformation(renewDate, password,
          this.getTrackingIdIfEnabled(identifier)));
    }
  }
}
