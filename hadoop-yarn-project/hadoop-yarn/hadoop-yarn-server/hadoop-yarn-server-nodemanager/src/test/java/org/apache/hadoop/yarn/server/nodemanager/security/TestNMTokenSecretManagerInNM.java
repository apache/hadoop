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

package org.apache.hadoop.yarn.server.nodemanager.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.security.BaseNMTokenSecretManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;

public class TestNMTokenSecretManagerInNM {

  @Test
  public void testRecovery() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    final NodeId nodeId = NodeId.newInstance("somehost", 1234);
    final ApplicationAttemptId attempt1 =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    final ApplicationAttemptId attempt2 =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(2, 2), 2);
    NMTokenKeyGeneratorForTest keygen = new NMTokenKeyGeneratorForTest();
    NMMemoryStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    NMTokenSecretManagerInNM secretMgr =
        new NMTokenSecretManagerInNM(stateStore);
    secretMgr.setNodeId(nodeId);
    MasterKey currentKey = keygen.generateKey();
    secretMgr.setMasterKey(currentKey);
    NMTokenIdentifier attemptToken1 =
        getNMTokenId(secretMgr.createNMToken(attempt1, nodeId, "user1"));
    NMTokenIdentifier attemptToken2 =
        getNMTokenId(secretMgr.createNMToken(attempt2, nodeId, "user2"));
    secretMgr.appAttemptStartContainer(attemptToken1);
    secretMgr.appAttemptStartContainer(attemptToken2);
    assertTrue(secretMgr.isAppAttemptNMTokenKeyPresent(attempt1));
    assertTrue(secretMgr.isAppAttemptNMTokenKeyPresent(attempt2));
    assertNotNull(secretMgr.retrievePassword(attemptToken1));
    assertNotNull(secretMgr.retrievePassword(attemptToken2));

    // restart and verify key is still there and token still valid
    secretMgr = new NMTokenSecretManagerInNM(stateStore);
    secretMgr.recover();
    secretMgr.setNodeId(nodeId);
    assertEquals(currentKey, secretMgr.getCurrentKey());
    assertTrue(secretMgr.isAppAttemptNMTokenKeyPresent(attempt1));
    assertTrue(secretMgr.isAppAttemptNMTokenKeyPresent(attempt2));
    assertNotNull(secretMgr.retrievePassword(attemptToken1));
    assertNotNull(secretMgr.retrievePassword(attemptToken2));

    // roll master key and remove an app
    currentKey = keygen.generateKey();
    secretMgr.setMasterKey(currentKey);
    secretMgr.appFinished(attempt1.getApplicationId());

    // restart and verify attempt1 key is still valid due to prev key persist
    secretMgr = new NMTokenSecretManagerInNM(stateStore);
    secretMgr.recover();
    secretMgr.setNodeId(nodeId);
    assertEquals(currentKey, secretMgr.getCurrentKey());
    assertFalse(secretMgr.isAppAttemptNMTokenKeyPresent(attempt1));
    assertTrue(secretMgr.isAppAttemptNMTokenKeyPresent(attempt2));
    assertNotNull(secretMgr.retrievePassword(attemptToken1));
    assertNotNull(secretMgr.retrievePassword(attemptToken2));

    // roll master key again, restart, and verify attempt1 key is bad but
    // attempt2 is still good due to app key persist
    currentKey = keygen.generateKey();
    secretMgr.setMasterKey(currentKey);
    secretMgr = new NMTokenSecretManagerInNM(stateStore);
    secretMgr.recover();
    secretMgr.setNodeId(nodeId);
    assertEquals(currentKey, secretMgr.getCurrentKey());
    assertFalse(secretMgr.isAppAttemptNMTokenKeyPresent(attempt1));
    assertTrue(secretMgr.isAppAttemptNMTokenKeyPresent(attempt2));
    try {
      secretMgr.retrievePassword(attemptToken1);
      fail("attempt token should not still be valid");
    } catch (InvalidToken e) {
      // expected
    }
    assertNotNull(secretMgr.retrievePassword(attemptToken2));

    // remove last attempt, restart, verify both tokens are now bad
    secretMgr.appFinished(attempt2.getApplicationId());
    secretMgr = new NMTokenSecretManagerInNM(stateStore);
    secretMgr.recover();
    secretMgr.setNodeId(nodeId);
    assertEquals(currentKey, secretMgr.getCurrentKey());
    assertFalse(secretMgr.isAppAttemptNMTokenKeyPresent(attempt1));
    assertFalse(secretMgr.isAppAttemptNMTokenKeyPresent(attempt2));
    try {
      secretMgr.retrievePassword(attemptToken1);
      fail("attempt token should not still be valid");
    } catch (InvalidToken e) {
      // expected
    }
    try {
      secretMgr.retrievePassword(attemptToken2);
      fail("attempt token should not still be valid");
    } catch (InvalidToken e) {
      // expected
    }

    stateStore.close();
  }

  private NMTokenIdentifier getNMTokenId(
      org.apache.hadoop.yarn.api.records.Token token) throws IOException {
    Token<NMTokenIdentifier> convertedToken =
        ConverterUtils.convertFromYarn(token, (Text) null);
    return convertedToken.decodeIdentifier();
  }

  private static class NMTokenKeyGeneratorForTest extends
      BaseNMTokenSecretManager {
    public MasterKey generateKey() {
      return createNewMasterKey().getMasterKey();
    }
  }
}
