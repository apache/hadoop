/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMDTSecretManagerState;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for MemoryFederationStateStore.
 */
public class TestMemoryFederationStateStore extends FederationStateStoreBaseTest {

  @Override
  protected FederationStateStore createStateStore() {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS, 10);
    super.setConf(conf);
    return new MemoryFederationStateStore();
  }

  @Override
  protected void checkRouterMasterKey(DelegationKey delegationKey,
      RouterMasterKey routerMasterKey) throws YarnException, IOException {
    MemoryFederationStateStore memoryStateStore =
        MemoryFederationStateStore.class.cast(this.getStateStore());
    RouterRMDTSecretManagerState secretManagerState =
        memoryStateStore.getRouterRMSecretManagerState();
    assertNotNull(secretManagerState);

    Set<DelegationKey> delegationKeys = secretManagerState.getMasterKeyState();
    assertNotNull(delegationKeys);

    assertTrue(delegationKeys.contains(delegationKey));

    RouterMasterKey resultRouterMasterKey = RouterMasterKey.newInstance(delegationKey.getKeyId(),
        ByteBuffer.wrap(delegationKey.getEncodedKey()), delegationKey.getExpiryDate());
    assertEquals(resultRouterMasterKey, routerMasterKey);
  }

  @Override
  protected void checkRouterStoreToken(RMDelegationTokenIdentifier identifier,
      RouterStoreToken token) throws YarnException, IOException {
    MemoryFederationStateStore memoryStateStore =
        MemoryFederationStateStore.class.cast(this.getStateStore());
    RouterRMDTSecretManagerState secretManagerState =
        memoryStateStore.getRouterRMSecretManagerState();
    assertNotNull(secretManagerState);

    Map<RMDelegationTokenIdentifier, RouterStoreToken> tokenStateMap =
        secretManagerState.getTokenState();
    assertNotNull(tokenStateMap);

    assertTrue(tokenStateMap.containsKey(identifier));

    YARNDelegationTokenIdentifier tokenIdentifier = token.getTokenIdentifier();
    assertTrue(tokenIdentifier instanceof RMDelegationTokenIdentifier);
    assertEquals(identifier, tokenIdentifier);
  }
}