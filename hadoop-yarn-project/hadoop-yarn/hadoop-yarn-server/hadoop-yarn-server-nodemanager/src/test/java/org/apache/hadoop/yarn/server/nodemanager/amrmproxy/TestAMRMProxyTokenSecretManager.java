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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for AMRMProxyTokenSecretManager.
 */
public class TestAMRMProxyTokenSecretManager {

  private YarnConfiguration conf;
  private AMRMProxyTokenSecretManager secretManager;
  private NMMemoryStateStoreService stateStore;

  @Before
  public void setup() {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);

    stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();

    secretManager = new AMRMProxyTokenSecretManager(stateStore);
    secretManager.init(conf);
    secretManager.start();
  }

  @After
  public void breakdown() {
    if (secretManager != null) {
      secretManager.stop();
    }
    if (stateStore != null) {
      stateStore.stop();
    }
  }

  @Test
  public void testNormalCase() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);

    Token<AMRMTokenIdentifier> localToken =
        secretManager.createAndGetAMRMToken(attemptId);

    AMRMTokenIdentifier identifier = secretManager.createIdentifier();
    identifier.readFields(new DataInputStream(
        new ByteArrayInputStream(localToken.getIdentifier())));

    secretManager.retrievePassword(identifier);

    secretManager.applicationMasterFinished(attemptId);

    try {
      secretManager.retrievePassword(identifier);
      Assert.fail("Expect InvalidToken exception");
    } catch (InvalidToken e) {
    }
  }

  @Test
  public void testRecovery() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);

    Token<AMRMTokenIdentifier> localToken =
        secretManager.createAndGetAMRMToken(attemptId);

    AMRMTokenIdentifier identifier = secretManager.createIdentifier();
    identifier.readFields(new DataInputStream(
        new ByteArrayInputStream(localToken.getIdentifier())));

    secretManager.retrievePassword(identifier);

    // Generate next master key
    secretManager.rollMasterKey();

    // Restart and recover
    secretManager.stop();
    secretManager = new AMRMProxyTokenSecretManager(stateStore);
    secretManager.init(conf);
    secretManager.recover(stateStore.loadAMRMProxyState());
    secretManager.start();
    // Recover the app
    secretManager.createAndGetAMRMToken(attemptId);

    // Current master key should be recovered, and thus pass here
    secretManager.retrievePassword(identifier);

    // Roll key, current master key will be replaced
    secretManager.activateNextMasterKey();

    // Restart and recover
    secretManager.stop();
    secretManager = new AMRMProxyTokenSecretManager(stateStore);
    secretManager.init(conf);
    secretManager.recover(stateStore.loadAMRMProxyState());
    secretManager.start();
    // Recover the app
    secretManager.createAndGetAMRMToken(attemptId);

    try {
      secretManager.retrievePassword(identifier);
      Assert.fail("Expect InvalidToken exception because the "
          + "old master key should have expired");
    } catch (InvalidToken e) {
    }
  }
}
