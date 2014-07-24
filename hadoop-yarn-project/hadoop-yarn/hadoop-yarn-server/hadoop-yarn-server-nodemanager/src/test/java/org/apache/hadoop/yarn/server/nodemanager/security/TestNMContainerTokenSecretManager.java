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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Test;

public class TestNMContainerTokenSecretManager {

  @Test
  public void testRecovery() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    final NodeId nodeId = NodeId.newInstance("somehost", 1234);
    final ContainerId cid1 = BuilderUtils.newContainerId(1, 1, 1, 1);
    final ContainerId cid2 = BuilderUtils.newContainerId(2, 2, 2, 2);
    ContainerTokenKeyGeneratorForTest keygen =
        new ContainerTokenKeyGeneratorForTest(conf);
    NMMemoryStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    NMContainerTokenSecretManager secretMgr =
        new NMContainerTokenSecretManager(conf, stateStore);
    secretMgr.setNodeId(nodeId);
    MasterKey currentKey = keygen.generateKey();
    secretMgr.setMasterKey(currentKey);
    ContainerTokenIdentifier tokenId1 =
        createContainerTokenId(cid1, nodeId, "user1", secretMgr);
    ContainerTokenIdentifier tokenId2 =
        createContainerTokenId(cid2, nodeId, "user2", secretMgr);
    assertNotNull(secretMgr.retrievePassword(tokenId1));
    assertNotNull(secretMgr.retrievePassword(tokenId2));

    // restart and verify tokens still valid
    secretMgr = new NMContainerTokenSecretManager(conf, stateStore);
    secretMgr.setNodeId(nodeId);
    secretMgr.recover();
    assertEquals(currentKey, secretMgr.getCurrentKey());
    assertTrue(secretMgr.isValidStartContainerRequest(tokenId1));
    assertTrue(secretMgr.isValidStartContainerRequest(tokenId2));
    assertNotNull(secretMgr.retrievePassword(tokenId1));
    assertNotNull(secretMgr.retrievePassword(tokenId2));

    // roll master key and start a container
    secretMgr.startContainerSuccessful(tokenId2);
    currentKey = keygen.generateKey();
    secretMgr.setMasterKey(currentKey);

    // restart and verify tokens still valid due to prev key persist
    secretMgr = new NMContainerTokenSecretManager(conf, stateStore);
    secretMgr.setNodeId(nodeId);
    secretMgr.recover();
    assertEquals(currentKey, secretMgr.getCurrentKey());
    assertTrue(secretMgr.isValidStartContainerRequest(tokenId1));
    assertFalse(secretMgr.isValidStartContainerRequest(tokenId2));
    assertNotNull(secretMgr.retrievePassword(tokenId1));
    assertNotNull(secretMgr.retrievePassword(tokenId2));

    // roll master key again, restart, and verify keys no longer valid
    currentKey = keygen.generateKey();
    secretMgr.setMasterKey(currentKey);
    secretMgr = new NMContainerTokenSecretManager(conf, stateStore);
    secretMgr.setNodeId(nodeId);
    secretMgr.recover();
    assertEquals(currentKey, secretMgr.getCurrentKey());
    assertTrue(secretMgr.isValidStartContainerRequest(tokenId1));
    assertFalse(secretMgr.isValidStartContainerRequest(tokenId2));
    try {
      secretMgr.retrievePassword(tokenId1);
      fail("token should not be valid");
    } catch (InvalidToken e) {
      // expected
    }
    try {
      secretMgr.retrievePassword(tokenId2);
      fail("token should not be valid");
    } catch (InvalidToken e) {
      // expected
    }

    stateStore.close();
  }

  private static ContainerTokenIdentifier createContainerTokenId(
      ContainerId cid, NodeId nodeId, String user,
      NMContainerTokenSecretManager secretMgr) throws IOException {
    long rmid = cid.getApplicationAttemptId().getApplicationId()
        .getClusterTimestamp();
    ContainerTokenIdentifier ctid = new ContainerTokenIdentifier(cid,
        nodeId.toString(), user, BuilderUtils.newResource(1024, 1),
        System.currentTimeMillis() + 100000L,
        secretMgr.getCurrentKey().getKeyId(), rmid,
        Priority.newInstance(0), 0);
    Token token = BuilderUtils.newContainerToken(nodeId,
        secretMgr.createPassword(ctid), ctid);
    return BuilderUtils.newContainerTokenIdentifier(token);
  }

  private static class ContainerTokenKeyGeneratorForTest extends
      BaseContainerTokenSecretManager {
    public ContainerTokenKeyGeneratorForTest(Configuration conf) {
      super(conf);
    }

    public MasterKey generateKey() {
      return createNewMasterKey().getMasterKey();
    }
  }
}
