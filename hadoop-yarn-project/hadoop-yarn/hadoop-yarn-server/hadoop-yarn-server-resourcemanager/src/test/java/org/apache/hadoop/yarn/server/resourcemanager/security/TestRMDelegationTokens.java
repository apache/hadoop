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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockMemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMSecretManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart.TestSecurityMockRM;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRMDelegationTokens {

  private YarnConfiguration testConf;

  @Before
  public void setup() {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    ExitUtil.disableSystemExit();
    testConf = new YarnConfiguration();
    testConf
        .set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    UserGroupInformation.setLoginUser(null);
    UserGroupInformation.setConfiguration(testConf);
  }

  // Test the DT mast key in the state-store when the mast key is being rolled.
  @Test(timeout = 15000)
  public void testRMDTMasterKeyStateOnRollingMasterKey() throws Exception {
    Configuration conf = new Configuration(testConf);
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.getLoginUser()
        .setAuthenticationMethod(AuthenticationMethod.KERBEROS);

    MemoryRMStateStore memStore = new MockMemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();

    Map<RMDelegationTokenIdentifier, Long> rmDTState =
        rmState.getRMDTSecretManagerState().getTokenState();
    Set<DelegationKey> rmDTMasterKeyState =
        rmState.getRMDTSecretManagerState().getMasterKeyState();

    MockRM rm1 = new MyMockRM(conf, memStore);
    rm1.start();
    // on rm start, two master keys are created.
    // One is created at RMDTSecretMgr.startThreads.updateCurrentKey();
    // the other is created on the first run of
    // tokenRemoverThread.rollMasterKey()

    RMDelegationTokenSecretManager dtSecretManager =
        rm1.getRMContext().getRMDelegationTokenSecretManager();
    // assert all master keys are saved
    dtSecretManager.getAllMasterKeys().forEach(managerKey -> {
      int keyId = managerKey.getKeyId();
      boolean found = false;
      for (DelegationKey stateKey: rmDTMasterKeyState) {
        if (stateKey.getKeyId() == keyId) {
          found = true;
          break;
        }
      }
      Assert.assertTrue("Master key not found: " + keyId, found);
    });

    // request to generate a RMDelegationToken
    GetDelegationTokenRequest request = mock(GetDelegationTokenRequest.class);
    when(request.getRenewer()).thenReturn("renewer1");
    GetDelegationTokenResponse response =
        rm1.getClientRMService().getDelegationToken(request);
    org.apache.hadoop.yarn.api.records.Token delegationToken =
        response.getRMDelegationToken();
    Token<RMDelegationTokenIdentifier> token1 =
        ConverterUtils.convertFromYarn(delegationToken, (Text) null);
    RMDelegationTokenIdentifier dtId1 = token1.decodeIdentifier();

    // For all keys that still remain in memory, we should have them stored
    // in state-store also.
    while (((TestRMDelegationTokenSecretManager) dtSecretManager).numUpdatedKeys
      .get() < 3) {
      ((TestRMDelegationTokenSecretManager) dtSecretManager)
        .checkCurrentKeyInStateStore(rmDTMasterKeyState);
      Thread.sleep(100);
    }

    // wait for token to expire and remove from state-store
    // rollMasterKey is called every 1 second.
    int count = 0;
    while (rmDTState.containsKey(dtId1) && count < 100) {
      Thread.sleep(100);
      count++;
    }
    rm1.stop();
  }

  // Test all expired keys are removed from state-store.
  @Test(timeout = 15000)
  public void testRemoveExpiredMasterKeyInRMStateStore() throws Exception {
    MemoryRMStateStore memStore = new MockMemoryRMStateStore();
    memStore.init(testConf);
    RMState rmState = memStore.getState();

    Set<DelegationKey> rmDTMasterKeyState =
        rmState.getRMDTSecretManagerState().getMasterKeyState();

    MockRM rm1 = new MyMockRM(testConf, memStore);
    rm1.start();
    RMDelegationTokenSecretManager dtSecretManager =
        rm1.getRMContext().getRMDelegationTokenSecretManager();

    // assert all master keys are saved
    Assert.assertEquals(dtSecretManager.getAllMasterKeys(), rmDTMasterKeyState);
    Set<DelegationKey> expiringKeys = new HashSet<DelegationKey>();
    expiringKeys.addAll(dtSecretManager.getAllMasterKeys());

    // wait for expiringKeys to expire
    while (true) {
      boolean allExpired = true;
      for (DelegationKey key : expiringKeys) {
        if (rmDTMasterKeyState.contains(key)) {
          allExpired = false;
        }
      }
      if (allExpired)
        break;
      Thread.sleep(500);
    }
    rm1.stop();
  }

  // Test removing token without key from state-store.
  @Test(timeout = 15000)
  public void testUnknownKeyTokensOnRecover() throws Exception {
    final int masterID = 1234;
    final int sequenceNumber = 1000;

    MemoryRMStateStore memStore = new MockMemoryRMStateStore();
    memStore.init(testConf);
    // Need RM to get the secret manager and call recover
    MockRM rm1 = new MyMockRM(testConf, memStore);
    rm1.start();
    RMDelegationTokenSecretManager dtSecretManager =
        rm1.getRMContext().getRMDelegationTokenSecretManager();
    RMState rmState = memStore.getState();
    // short cut to generate a basic token with unknown key
    RMDelegationTokenIdentifier rmDT = new RMDelegationTokenIdentifier(
        new Text("owner"), new Text("renewer"), new Text("realuser"));
    // set a master key which is not used
    rmDT.setMasterKeyId(masterID);
    rmDT.setSequenceNumber(sequenceNumber);
    final long tokenTime = Time.now() + 60000;
    rmDT.setMaxDate(tokenTime);
    dtSecretManager.storeNewToken(rmDT, tokenTime);
    // give it time to process
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return rmState.getRMDTSecretManagerState().getTokenState().
            containsKey(rmDT);
      }
    }, 10, 2000);
    // Cannot recover while running: stop and clear
    dtSecretManager.stopThreads();
    dtSecretManager.reset();
    Assert.assertEquals("Secret manager should have no tokens",
        dtSecretManager.getAllTokens().size(), 0);
    Assert.assertEquals("Secret manager should have no keys",
        dtSecretManager.getAllMasterKeys().size(), 0);
    dtSecretManager.recover(rmState);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return
            rmState.getRMDTSecretManagerState().getTokenState().
                containsKey(rmDT);
      }
    }, 10, 2000);
    Assert.assertEquals("Token should have been expired but is not", 0L,
        dtSecretManager.getRenewDate(rmDT));
    // The remover thread should immediately do its work,
    // still give it some time to process
    dtSecretManager.startThreads();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return
            !rmState.getRMDTSecretManagerState().getTokenState().
                containsKey(rmDT);
      }
    }, 10, 2000);
    rm1.stop();
  }

  class MyMockRM extends TestSecurityMockRM {

    public MyMockRM(Configuration conf, RMStateStore store) {
      super(conf, store);
    }

    @Override
    protected RMSecretManagerService createRMSecretManagerService() {
      return new RMSecretManagerService(testConf, rmContext) {

        @Override
        protected RMDelegationTokenSecretManager
        createRMDelegationTokenSecretManager(Configuration conf,
                                             RMContext rmContext) {
          // KeyUpdateInterval-> 1 seconds
          // TokenMaxLifetime-> 2 seconds.
          return new TestRMDelegationTokenSecretManager(1000, 1000, 2000, 1000,
              rmContext);
        }
      };
    }

  }

  public class TestRMDelegationTokenSecretManager extends
      RMDelegationTokenSecretManager {
    public AtomicInteger numUpdatedKeys = new AtomicInteger(0);

    public TestRMDelegationTokenSecretManager(long delegationKeyUpdateInterval,
        long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval, RMContext rmContext) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval,
        rmContext);
    }

    @Override
    protected void storeNewMasterKey(DelegationKey newKey) {
      super.storeNewMasterKey(newKey);
      numUpdatedKeys.incrementAndGet();
    }

    public synchronized DelegationKey checkCurrentKeyInStateStore(
        Set<DelegationKey> rmDTMasterKeyState) {
      for (int keyId : allKeys.keySet()) {
        if (keyId == currentId) {
          DelegationKey currentKey = allKeys.get(keyId);
          // There's a small window where the key expiry has changed in memory
          // but not the state store yet, and DelegationKey hashcode/equals
          // uses the expiry so the contains method will fail to find it.
          boolean found = false;
          for (DelegationKey k : rmDTMasterKeyState) {
            if (k.getKeyId() == keyId) {
              found = true;
              break;
            }
          }
          Assert.assertTrue(found);
          return currentKey;
        }
      }
      return null;
    }
  }

}
