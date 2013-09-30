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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.hs.HistoryServerStateStoreService.HistoryServerState;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHistoryServerFileSystemStateStoreService {

  private static final File testDir = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      "TestHistoryServerFileSystemStateStoreService");

  private Configuration conf;

  @Before
  public void setup() {
    FileUtil.fullyDelete(testDir);
    testDir.mkdirs();
    conf = new Configuration();
    conf.setBoolean(JHAdminConfig.MR_HS_RECOVERY_ENABLE, true);
    conf.setClass(JHAdminConfig.MR_HS_STATE_STORE,
        HistoryServerFileSystemStateStoreService.class,
        HistoryServerStateStoreService.class);
    conf.set(JHAdminConfig.MR_HS_FS_STATE_STORE_URI,
        testDir.getAbsoluteFile().toURI().toString());
  }

  @After
  public void cleanup() {
    FileUtil.fullyDelete(testDir);
  }

  private HistoryServerStateStoreService createAndStartStore()
      throws IOException {
    HistoryServerStateStoreService store =
        HistoryServerStateStoreServiceFactory.getStore(conf);
    assertTrue("Factory did not create a filesystem store",
        store instanceof HistoryServerFileSystemStateStoreService);
    store.init(conf);
    store.start();
    return store;
  }

  @Test
  public void testTokenStore() throws IOException {
    HistoryServerStateStoreService store = createAndStartStore();

    HistoryServerState state = store.loadState();
    assertTrue("token state not empty", state.tokenState.isEmpty());
    assertTrue("key state not empty", state.tokenMasterKeyState.isEmpty());

    final DelegationKey key1 = new DelegationKey(1, 2, "keyData1".getBytes());
    final MRDelegationTokenIdentifier token1 =
        new MRDelegationTokenIdentifier(new Text("tokenOwner1"),
            new Text("tokenRenewer1"), new Text("tokenUser1"));
    token1.setSequenceNumber(1);
    final Long tokenDate1 = 1L;
    final MRDelegationTokenIdentifier token2 =
        new MRDelegationTokenIdentifier(new Text("tokenOwner2"),
            new Text("tokenRenewer2"), new Text("tokenUser2"));
    token2.setSequenceNumber(12345678);
    final Long tokenDate2 = 87654321L;

    store.storeTokenMasterKey(key1);
    try {
      store.storeTokenMasterKey(key1);
      fail("redundant store of key undetected");
    } catch (IOException e) {
      // expected
    }
    store.storeToken(token1, tokenDate1);
    store.storeToken(token2, tokenDate2);
    try {
      store.storeToken(token1, tokenDate1);
      fail("redundant store of token undetected");
    } catch (IOException e) {
      // expected
    }
    store.close();

    store = createAndStartStore();
    state = store.loadState();
    assertEquals("incorrect loaded token count", 2, state.tokenState.size());
    assertTrue("missing token 1", state.tokenState.containsKey(token1));
    assertEquals("incorrect token 1 date", tokenDate1,
        state.tokenState.get(token1));
    assertTrue("missing token 2", state.tokenState.containsKey(token2));
    assertEquals("incorrect token 2 date", tokenDate2,
        state.tokenState.get(token2));
    assertEquals("incorrect master key count", 1,
        state.tokenMasterKeyState.size());
    assertTrue("missing master key 1",
        state.tokenMasterKeyState.contains(key1));

    final DelegationKey key2 = new DelegationKey(3, 4, "keyData2".getBytes());
    final DelegationKey key3 = new DelegationKey(5, 6, "keyData3".getBytes());
    final MRDelegationTokenIdentifier token3 =
        new MRDelegationTokenIdentifier(new Text("tokenOwner3"),
            new Text("tokenRenewer3"), new Text("tokenUser3"));
    token3.setSequenceNumber(12345679);
    final Long tokenDate3 = 87654321L;

    store.removeToken(token1);
    store.storeTokenMasterKey(key2);
    final Long newTokenDate2 = 975318642L;
    store.updateToken(token2, newTokenDate2);
    store.removeTokenMasterKey(key1);
    store.storeTokenMasterKey(key3);
    store.storeToken(token3, tokenDate3);
    store.close();

    store = createAndStartStore();
    state = store.loadState();
    assertEquals("incorrect loaded token count", 2, state.tokenState.size());
    assertFalse("token 1 not removed", state.tokenState.containsKey(token1));
    assertTrue("missing token 2", state.tokenState.containsKey(token2));
    assertEquals("incorrect token 2 date", newTokenDate2,
        state.tokenState.get(token2));
    assertTrue("missing token 3", state.tokenState.containsKey(token3));
    assertEquals("incorrect token 3 date", tokenDate3,
        state.tokenState.get(token3));
    assertEquals("incorrect master key count", 2,
        state.tokenMasterKeyState.size());
    assertFalse("master key 1 not removed",
        state.tokenMasterKeyState.contains(key1));
    assertTrue("missing master key 2",
        state.tokenMasterKeyState.contains(key2));
    assertTrue("missing master key 3",
        state.tokenMasterKeyState.contains(key3));
  }
}
