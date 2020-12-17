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
package org.apache.hadoop.mapreduce.v2.hs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestJHSDelegationTokenSecretManager {

  @Test
  public void testRecovery() throws Exception {
    Configuration conf = new Configuration();
    HistoryServerStateStoreService store =
        new HistoryServerMemStateStoreService();
    store.init(conf);
    store.start();
    Map<MRDelegationTokenIdentifier, Long> tokenState =
        ((HistoryServerMemStateStoreService) store).state.getTokenState();
    JHSDelegationTokenSecretManagerForTest mgr =
        new JHSDelegationTokenSecretManagerForTest(store);
    mgr.startThreads();

    MRDelegationTokenIdentifier tokenId1 = new MRDelegationTokenIdentifier(
        new Text("tokenOwner"), new Text("tokenRenewer"),
        new Text("tokenUser"));
    Token<MRDelegationTokenIdentifier> token1 =
        new Token<MRDelegationTokenIdentifier>(tokenId1, mgr);

    MRDelegationTokenIdentifier tokenId2 = new MRDelegationTokenIdentifier(
        new Text("tokenOwner"), new Text("tokenRenewer"),
        new Text("tokenUser"));
    Token<MRDelegationTokenIdentifier> token2 =
        new Token<MRDelegationTokenIdentifier>(tokenId2, mgr);
    DelegationKey[] keys = mgr.getAllKeys();
    long tokenRenewDate1 = mgr.getAllTokens().get(tokenId1).getRenewDate();
    long tokenRenewDate2 = mgr.getAllTokens().get(tokenId2).getRenewDate();
    // Make sure we stored the tokens
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return tokenState.size() == 2;
      }
    }, 10, 2000);

    stopAndCleanSecretManager(mgr);

    mgr.recover(store.loadState());
    List<DelegationKey> recoveredKeys = Arrays.asList(mgr.getAllKeys());
    for (DelegationKey key : keys) {
      assertTrue("key missing after recovery", recoveredKeys.contains(key));
    }
    assertTrue("token1 missing", mgr.getAllTokens().containsKey(tokenId1));
    assertEquals("token1 renew date", tokenRenewDate1,
        mgr.getAllTokens().get(tokenId1).getRenewDate());
    assertTrue("token2 missing", mgr.getAllTokens().containsKey(tokenId2));
    assertEquals("token2 renew date", tokenRenewDate2,
        mgr.getAllTokens().get(tokenId2).getRenewDate());

    mgr.startThreads();
    mgr.verifyToken(tokenId1, token1.getPassword());
    mgr.verifyToken(tokenId2, token2.getPassword());
    MRDelegationTokenIdentifier tokenId3 = new MRDelegationTokenIdentifier(
        new Text("tokenOwner"), new Text("tokenRenewer"),
        new Text("tokenUser"));
    Token<MRDelegationTokenIdentifier> token3 =
        new Token<MRDelegationTokenIdentifier>(tokenId3, mgr);
    assertEquals("sequence number restore", tokenId2.getSequenceNumber() + 1,
        tokenId3.getSequenceNumber());
    mgr.cancelToken(token1, "tokenOwner");

    // Testing with full principal name
    MRDelegationTokenIdentifier tokenIdFull = new MRDelegationTokenIdentifier(
        new Text("tokenOwner/localhost@LOCALHOST"), new Text("tokenRenewer"),
        new Text("tokenUser"));
    KerberosName.setRules("RULE:[1:$1]\nRULE:[2:$1]");
    Token<MRDelegationTokenIdentifier> tokenFull = new Token<MRDelegationTokenIdentifier>(
        tokenIdFull, mgr);
    // Negative test
    try {
      mgr.cancelToken(tokenFull, "tokenOwner");
    } catch (AccessControlException ace) {
      assertTrue(ace.getMessage().contains(
          "is not authorized to cancel the token"));
    }
    // Succeed to cancel with full principal
    mgr.cancelToken(tokenFull, tokenIdFull.getOwner().toString());
    // Make sure we removed the stored token
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return tokenState.size() == 2;
      }
    }, 10, 2000);

    long tokenRenewDate3 = mgr.getAllTokens().get(tokenId3).getRenewDate();
    stopAndCleanSecretManager(mgr);

    mgr.recover(store.loadState());
    assertFalse("token1 should be missing",
        mgr.getAllTokens().containsKey(tokenId1));
    assertTrue("token2 missing", mgr.getAllTokens().containsKey(tokenId2));
    assertEquals("token2 renew date incorrect", tokenRenewDate2,
        mgr.getAllTokens().get(tokenId2).getRenewDate());
    assertTrue("token3 missing from manager",
        mgr.getAllTokens().containsKey(tokenId3));
    assertEquals("token3 renew date", tokenRenewDate3,
        mgr.getAllTokens().get(tokenId3).getRenewDate());

    mgr.startThreads();
    mgr.verifyToken(tokenId2, token2.getPassword());
    mgr.verifyToken(tokenId3, token3.getPassword());
    // Set an unknown key ID: token should not be restored
    tokenId3.setMasterKeyId(1000);
    // Update renewal date to check the store write
    mgr.updateStoredToken(tokenId3, tokenRenewDate3 + 5000);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return tokenState.get(tokenId3).equals(tokenRenewDate3 + 5000);
      }
    }, 10, 2000);
    stopAndCleanSecretManager(mgr);

    // Store should contain token but manager should not
    Assert.assertTrue("Store does not contain token3",
        tokenState.containsKey(tokenId3));
    Assert.assertFalse("Store does not contain token3",
        mgr.getAllTokens().containsKey(tokenId3));
    // Recover to load the token into the manager; renew date is set to 0
    mgr.recover(store.loadState());
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return mgr.getAllTokens().get(tokenId3).getRenewDate() == 0L;
      }
    }, 10, 2000);
    // Start the removal threads: cleanup manager and store
    mgr.startThreads();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return !mgr.getAllTokens().containsKey(tokenId3);
      }
    }, 10, 2000);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return !tokenState.containsKey(tokenId3);
      }
    }, 10, 2000);
 }

  private void stopAndCleanSecretManager(
      JHSDelegationTokenSecretManagerForTest mgr) {
    mgr.stopThreads();
    mgr.reset();
    assertThat(mgr.getAllKeys().length)
        .withFailMessage("Secret manager should not contain keys").isZero();
    assertThat(mgr.getAllTokens().size())
        .withFailMessage("Secret manager should not contain tokens").isZero();
  }

  private static class JHSDelegationTokenSecretManagerForTest
      extends JHSDelegationTokenSecretManager {

    public JHSDelegationTokenSecretManagerForTest(
        HistoryServerStateStoreService store) {
      super(10000, 10000, 10000, 10000, store);
    }

    public Map<MRDelegationTokenIdentifier, DelegationTokenInformation> getAllTokens() {
      return new HashMap<MRDelegationTokenIdentifier, DelegationTokenInformation>(currentTokens);
    }
  }
}
