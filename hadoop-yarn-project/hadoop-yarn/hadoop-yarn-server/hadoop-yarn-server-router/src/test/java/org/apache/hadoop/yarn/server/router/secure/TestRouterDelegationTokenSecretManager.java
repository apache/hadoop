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
package org.apache.hadoop.yarn.server.router.secure;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TestRouterDelegationTokenSecretManager extends AbstractSecureRouterTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterDelegationTokenSecretManager.class);

  @Test
  public void testRouterStoreNewMasterKey() throws Exception {
    LOG.info("Test RouterDelegationTokenSecretManager: StoreNewMasterKey.");

    // Start the Router in Secure Mode
    startSecureRouter();

    // Store NewMasterKey
    RouterClientRMService routerClientRMService = this.getRouter().getClientRMProxyService();
    RouterDelegationTokenSecretManager secretManager =
        routerClientRMService.getRouterDTSecretManager();
    DelegationKey storeKey = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    secretManager.storeNewMasterKey(storeKey);

    // Get DelegationKey
    DelegationKey paramKey = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    DelegationKey responseKey = secretManager.getMasterKeyByDelegationKey(paramKey);

    assertNotNull(paramKey);
    assertEquals(storeKey.getExpiryDate(), responseKey.getExpiryDate());
    assertEquals(storeKey.getKeyId(), responseKey.getKeyId());
    assertArrayEquals(storeKey.getEncodedKey(), responseKey.getEncodedKey());
    assertEquals(storeKey, responseKey);

    stopSecureRouter();
  }

  @Test
  public void testRouterRemoveStoredMasterKey() throws Exception {
    LOG.info("Test RouterDelegationTokenSecretManager: RemoveStoredMasterKey.");

    // Start the Router in Secure Mode
    startSecureRouter();

    // Store NewMasterKey
    RouterClientRMService routerClientRMService = this.getRouter().getClientRMProxyService();
    RouterDelegationTokenSecretManager secretManager =
        routerClientRMService.getRouterDTSecretManager();
    DelegationKey storeKey = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    secretManager.storeNewMasterKey(storeKey);

    // Remove DelegationKey
    secretManager.removeStoredMasterKey(storeKey);

    // Get DelegationKey
    DelegationKey paramKey = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    LambdaTestUtils.intercept(IOException.class,
        "GetMasterKey with keyID: " + storeKey.getKeyId() + " does not exist.",
        () -> secretManager.getMasterKeyByDelegationKey(paramKey));

    stopSecureRouter();
  }

  @Test
  public void testRouterStoreNewToken() throws Exception {
    LOG.info("Test RouterDelegationTokenSecretManager: StoreNewToken.");

    // Start the Router in Secure Mode
    startSecureRouter();

    // Store new rm-token
    RouterClientRMService routerClientRMService = this.getRouter().getClientRMProxyService();
    RouterDelegationTokenSecretManager secretManager =
        routerClientRMService.getRouterDTSecretManager();
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner1"), new Text("renewer1"), new Text("realuser1"));
    int sequenceNumber = 1;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    secretManager.storeNewToken(dtId1, renewDate1);

    // query rm-token
    RMDelegationTokenIdentifier dtId2 = new RMDelegationTokenIdentifier(
        new Text("owner1"), new Text("renewer1"), new Text("realuser1"));
    dtId2.setSequenceNumber(sequenceNumber);
    RMDelegationTokenIdentifier dtId3 = secretManager.getTokenByRouterStoreToken(dtId2);
    Assert.assertEquals(dtId1, dtId3);

    // query rm-token2 not exists
    sequenceNumber++;
    dtId2.setSequenceNumber(2);
    LambdaTestUtils.intercept(YarnException.class,
        "RMDelegationToken: " + dtId2 + " does not exist.",
        () -> secretManager.getTokenByRouterStoreToken(dtId2));

    stopSecureRouter();
  }

  @Test
  public void testRouterUpdateNewToken() throws Exception {
    LOG.info("Test RouterDelegationTokenSecretManager: UpdateNewToken.");

    // Start the Router in Secure Mode
    startSecureRouter();

    // Store new rm-token
    RouterClientRMService routerClientRMService = this.getRouter().getClientRMProxyService();
    RouterDelegationTokenSecretManager secretManager =
        routerClientRMService.getRouterDTSecretManager();
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner1"), new Text("renewer1"), new Text("realuser1"));
    int sequenceNumber = 1;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    secretManager.storeNewToken(dtId1, renewDate1);

    sequenceNumber++;
    dtId1.setSequenceNumber(sequenceNumber);
    secretManager.updateStoredToken(dtId1, renewDate1);

    // query rm-token
    RMDelegationTokenIdentifier dtId2 = new RMDelegationTokenIdentifier(
        new Text("owner1"), new Text("renewer1"), new Text("realuser1"));
    dtId2.setSequenceNumber(sequenceNumber);
    RMDelegationTokenIdentifier dtId3 = secretManager.getTokenByRouterStoreToken(dtId2);
    assertNotNull(dtId3);
    assertEquals(dtId1.getKind(), dtId3.getKind());
    assertEquals(dtId1.getOwner(), dtId3.getOwner());
    assertEquals(dtId1.getRealUser(), dtId3.getRealUser());
    assertEquals(dtId1.getRenewer(), dtId3.getRenewer());
    assertEquals(dtId1.getIssueDate(), dtId3.getIssueDate());
    assertEquals(dtId1.getMasterKeyId(), dtId3.getMasterKeyId());
    assertEquals(dtId1.getSequenceNumber(), dtId3.getSequenceNumber());
    assertEquals(sequenceNumber, dtId3.getSequenceNumber());
    assertEquals(dtId1, dtId3);

    stopSecureRouter();
  }

  @Test
  public void testRouterRemoveToken() throws Exception {
    LOG.info("Test RouterDelegationTokenSecretManager: RouterRemoveToken.");

    // Start the Router in Secure Mode
    startSecureRouter();

    // Store new rm-token
    RouterClientRMService routerClientRMService = this.getRouter().getClientRMProxyService();
    RouterDelegationTokenSecretManager secretManager =
        routerClientRMService.getRouterDTSecretManager();
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner1"), new Text("renewer1"), new Text("realuser1"));
    int sequenceNumber = 1;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    secretManager.storeNewToken(dtId1, renewDate1);

    // Remove rm-token
    secretManager.removeStoredToken(dtId1);

    // query rm-token
    LambdaTestUtils.intercept(YarnException.class,
        "RMDelegationToken: " + dtId1 + " does not exist.",
        () -> secretManager.getTokenByRouterStoreToken(dtId1));

    stopSecureRouter();
  }
}
