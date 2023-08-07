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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.IOException;

public class TestRouterDelegationTokenSecretManager extends AbstractSecureRouterTest {

  private volatile RouterDelegationTokenSecretManager secretManager_1;
  private volatile RouterDelegationTokenSecretManager secretManager_2;
  private final Text owner = new Text("owner");
  private final Text renewer = new Text("renewer");
  private final Text realUser = new Text("realUser");
  private final int keyUpdateInterval = 1000;
  private final int tokenRenewInterval = 2000;
  private final int tokenMaxLifeTime = 10000;

  @Before
  public void setup() {

    // Setup multiple secret managers to validate stateless secret managers.
    // They are using same instance of FederationStateStoreFacade thus the in memory state store is shared
    secretManager_1 = Mockito.spy(new RouterDelegationTokenSecretManager(
        keyUpdateInterval, tokenMaxLifeTime, tokenRenewInterval, 100)
    );
    secretManager_2 = Mockito.spy(new RouterDelegationTokenSecretManager(
        keyUpdateInterval, tokenMaxLifeTime, tokenRenewInterval, 100)
    );
  }

  @After
  public void cleanup() throws Exception {
    secretManager_1.stopThreads();
    secretManager_2.stopThreads();
    secretManager_1 = null;
    secretManager_2 = null;
    FederationStateStoreFacade.getInstance().getStateStore().close();
  }

  @Test
  public void testNewTokenIsVerifiedAcrossManagers() throws IOException {

    secretManager_1.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    Token<RMDelegationTokenIdentifier> token = new Token<>(tokenIdentifier, secretManager_1);

    Token<RMDelegationTokenIdentifier> token2 = new Token<>();
    token2.decodeFromUrlString(token.encodeToUrlString());

    RMDelegationTokenIdentifier tokenIdentifier_2 = secretManager_1.decodeTokenIdentifier(token2);
    Assertions.assertDoesNotThrow(() -> secretManager_1.verifyToken(tokenIdentifier_2, token2.getPassword()));

    secretManager_2.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier_3 = secretManager_2.decodeTokenIdentifier(token2);
    Assertions.assertDoesNotThrow(() -> secretManager_2.verifyToken(tokenIdentifier_3, token2.getPassword()));
  }

  @Test
  public void testMasterKeyIsRolled() throws IOException, InterruptedException {

    secretManager_1.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier1 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    new Token<>(tokenIdentifier1, secretManager_1);

    RMDelegationTokenIdentifier tokenIdentifier2 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    new Token<>(tokenIdentifier2, secretManager_1);

    // Check multiple tokens have same master key
    Assert.assertEquals(tokenIdentifier1.getMasterKeyId(), tokenIdentifier2.getMasterKeyId());
    // Sleep until master key is updated
    Thread.sleep(keyUpdateInterval + 100);

    RMDelegationTokenIdentifier tokenIdentifier3 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    new Token<>(tokenIdentifier3, secretManager_1);
    // Check master key is updated
    Assert.assertNotEquals(tokenIdentifier1.getMasterKeyId(), tokenIdentifier3.getMasterKeyId());

  }

  @Test
  public void testNewTokenIsCancelledAcrossManagers() throws IOException {

    secretManager_1.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    Token<RMDelegationTokenIdentifier> token = new Token<>(tokenIdentifier, secretManager_1);

    Token<RMDelegationTokenIdentifier> token2 = new Token<>();
    token2.decodeFromUrlString(token.encodeToUrlString());

    secretManager_2.startThreads();
    secretManager_2.cancelToken(token2, owner.toString());

    RMDelegationTokenIdentifier tokenIdentifier_2 = secretManager_1.decodeTokenIdentifier(token2);
    Assertions.assertThrows(SecretManager.InvalidToken.class,
        () -> secretManager_1.verifyToken(tokenIdentifier_2, token2.getPassword())
    );

  }

  @Test
  public void testNewTokenIsRenewedAcrossManagers() throws IOException, InterruptedException {

    secretManager_1.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    Token<RMDelegationTokenIdentifier> token = new Token<>(tokenIdentifier, secretManager_1);

    Token<RMDelegationTokenIdentifier> token2 = new Token<>();
    token2.decodeFromUrlString(token.encodeToUrlString());

    Thread.sleep(tokenRenewInterval / 2 + 100);
    secretManager_2.startThreads();
    secretManager_2.renewToken(token2, renewer.toString());

    Thread.sleep(tokenRenewInterval / 2 + 100);
    RMDelegationTokenIdentifier tokenIdentifier_2 = secretManager_1.decodeTokenIdentifier(token2);
    Assertions.assertDoesNotThrow(() -> secretManager_1.verifyToken(tokenIdentifier_2, token2.getPassword()));

  }

  @Test
  public void testTokenOperationsOnMasterKeyRollover() throws IOException, InterruptedException {

    secretManager_1.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier1 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    Token<RMDelegationTokenIdentifier> token1 = new Token<>(tokenIdentifier1, secretManager_1);

    // Sleep until master key is updated
    Thread.sleep(keyUpdateInterval + 100);

    RMDelegationTokenIdentifier tokenIdentifier2 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    new Token<>(tokenIdentifier2, secretManager_1);
    // Check master key is updated
    Assert.assertNotEquals(tokenIdentifier1.getMasterKeyId(), tokenIdentifier2.getMasterKeyId());

    // Verify token with old master key is still considered valid
    Assertions.assertDoesNotThrow(() -> secretManager_1.verifyToken(tokenIdentifier1, token1.getPassword()));
    // Verify token with old master key can be renewed
    Assertions.assertDoesNotThrow(() -> secretManager_1.renewToken(token1, renewer.toString()));
    // Verify token with old master key can be cancelled
    Assertions.assertDoesNotThrow(() -> secretManager_1.cancelToken(token1, owner.toString()));
    // Verify token with old master key is now cancelled
    Assert.assertThrows(SecretManager.InvalidToken.class,
        () -> secretManager_1.verifyToken(tokenIdentifier1, token1.getPassword()));

  }

  @Test
  public void testMasterKeyIsNotRolledOver() throws IOException, InterruptedException {

    secretManager_1.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier1 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    new Token<>(tokenIdentifier1, secretManager_1);

    Mockito.doThrow(new IOException("failure")).when(secretManager_1).storeNewMasterKey(Mockito.any());

    // Sleep until master key is updated
    Thread.sleep(keyUpdateInterval + 100);

    RMDelegationTokenIdentifier tokenIdentifier2 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    new Token<>(tokenIdentifier2, secretManager_1);
    // Verify master key is not updated
    Assert.assertEquals(tokenIdentifier1.getMasterKeyId(), tokenIdentifier2.getMasterKeyId());
  }

  @Test
  public void testNewTokenFailsOnDBFailure() throws IOException {
    secretManager_1.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier1 = new RMDelegationTokenIdentifier(owner, renewer, realUser);

    Mockito.doThrow(new IOException("failure")).when(secretManager_1).storeToken(Mockito.any(), Mockito.any());
    Assert.assertThrows(RuntimeException.class, () -> new Token<>(tokenIdentifier1, secretManager_1));
  }

  @Test
  public void testTokenIsNotRenewedOnDBFailure() throws IOException, InterruptedException {
    secretManager_1.startThreads();
    RMDelegationTokenIdentifier tokenIdentifier1 = new RMDelegationTokenIdentifier(owner, renewer, realUser);

    Token<RMDelegationTokenIdentifier> token = new Token<>(tokenIdentifier1, secretManager_1);
    Mockito.doThrow(new IOException("failure")).when(secretManager_1).updateToken(Mockito.any(), Mockito.any());

    Thread.sleep(tokenRenewInterval / 2 + 100);
    Assert.assertThrows(IOException.class, () -> secretManager_1.renewToken(token, renewer.toString()));
    // validate that token is currently valid
    Assertions.assertDoesNotThrow(() -> secretManager_1.verifyToken(tokenIdentifier1, token.getPassword()));

    Thread.sleep(tokenRenewInterval / 2 + 100);
    // token is no longer valid because token renewal had failed
    Assertions.assertThrows(SecretManager.InvalidToken.class,
        () -> secretManager_1.verifyToken(tokenIdentifier1, token.getPassword())
    );
  }

  @Ignore
  public void testNewTokenFailureIfMasterKeyNotRolledOverAtAll() throws IOException, InterruptedException {
    secretManager_1.startThreads();

    // Token generation succeeds initially because master key generated on initialisation was saved
    RMDelegationTokenIdentifier tokenIdentifier1 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    new Token<>(tokenIdentifier1, secretManager_1);

    Mockito.doThrow(new IOException("failure")).when(secretManager_1).storeNewMasterKey(Mockito.any());

    // Sleep until current master key expires. New master key isn't generated because rollovers are failing
    Thread.sleep(tokenMaxLifeTime + keyUpdateInterval + 100);

    RMDelegationTokenIdentifier tokenIdentifier2 = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    // New token generation fails because current master key is expired
    Assert.assertThrows(RuntimeException.class, () -> new Token<>(tokenIdentifier2, secretManager_1));
  }

  @Test
  public void testMasterKeyCreationFailureOnStartup() throws IOException {
    Mockito.doThrow(new IOException("failure")).when(secretManager_1).storeNewMasterKey(Mockito.any());

    Assert.assertThrows(IOException.class, () -> secretManager_1.startThreads());

    RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(owner, renewer, realUser);
    // New token generation fails because master key is not yet set
    Assert.assertThrows(NullPointerException.class, () -> new Token<>(tokenIdentifier, secretManager_1));
  }

}
