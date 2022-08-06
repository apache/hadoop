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

package org.apache.hadoop.yarn.server.timeline.recovery;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore.TimelineServiceState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestLeveldbTimelineStateStore {

  private FileContext fsContext;
  private File fsPath;
  private Configuration conf;
  private TimelineStateStore store;

  @BeforeEach
  public void setup() throws Exception {
    fsPath = new File("target", getClass().getSimpleName() +
        "-tmpDir").getAbsoluteFile();
    fsContext = FileContext.getLocalFSFileContext();
    fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_RECOVERY_ENABLED, true);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STATE_STORE_CLASS,
        LeveldbTimelineStateStore.class,
        TimelineStateStore.class);
    conf.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_STATE_STORE_PATH,
        fsPath.getAbsolutePath());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (store != null) {
      store.stop();
    }
    if (fsContext != null) {
      fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
    }
  }

  private LeveldbTimelineStateStore initAndStartTimelineServiceStateStoreService() {
    store = new LeveldbTimelineStateStore();
    store.init(conf);
    store.start();
    return (LeveldbTimelineStateStore) store;
  }

  @Test
  void testTokenStore() throws Exception {
    initAndStartTimelineServiceStateStoreService();
    TimelineServiceState state = store.loadState();
    assertTrue(state.tokenState.isEmpty(), "token state not empty");
    assertTrue(state.tokenMasterKeyState.isEmpty(), "key state not empty");

    final DelegationKey key1 = new DelegationKey(1, 2, "keyData1".getBytes());
    final TimelineDelegationTokenIdentifier token1 =
        new TimelineDelegationTokenIdentifier(new Text("tokenOwner1"),
            new Text("tokenRenewer1"), new Text("tokenUser1"));
    token1.setSequenceNumber(1);
    token1.getBytes();
    final Long tokenDate1 = 1L;
    final TimelineDelegationTokenIdentifier token2 =
        new TimelineDelegationTokenIdentifier(new Text("tokenOwner2"),
            new Text("tokenRenewer2"), new Text("tokenUser2"));
    token2.setSequenceNumber(12345678);
    token2.getBytes();
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

    initAndStartTimelineServiceStateStoreService();
    state = store.loadState();
    assertEquals(2, state.tokenState.size(), "incorrect loaded token count");
    assertTrue(state.tokenState.containsKey(token1), "missing token 1");
    assertEquals(tokenDate1,
        state.tokenState.get(token1),
        "incorrect token 1 date");
    assertTrue(state.tokenState.containsKey(token2), "missing token 2");
    assertEquals(tokenDate2,
        state.tokenState.get(token2),
        "incorrect token 2 date");
    assertEquals(1,
        state.tokenMasterKeyState.size(),
        "incorrect master key count");
    assertTrue(state.tokenMasterKeyState.contains(key1),
        "missing master key 1");
    assertEquals(12345678,
        state.getLatestSequenceNumber(),
        "incorrect latest sequence number");

    final DelegationKey key2 = new DelegationKey(3, 4, "keyData2".getBytes());
    final DelegationKey key3 = new DelegationKey(5, 6, "keyData3".getBytes());
    final TimelineDelegationTokenIdentifier token3 =
        new TimelineDelegationTokenIdentifier(new Text("tokenOwner3"),
            new Text("tokenRenewer3"), new Text("tokenUser3"));
    token3.setSequenceNumber(12345679);
    token3.getBytes();
    final Long tokenDate3 = 87654321L;

    store.removeToken(token1);
    store.storeTokenMasterKey(key2);
    final Long newTokenDate2 = 975318642L;
    store.updateToken(token2, newTokenDate2);
    store.removeTokenMasterKey(key1);
    store.storeTokenMasterKey(key3);
    store.storeToken(token3, tokenDate3);
    store.close();

    initAndStartTimelineServiceStateStoreService();
    state = store.loadState();
    assertEquals(2, state.tokenState.size(), "incorrect loaded token count");
    assertFalse(state.tokenState.containsKey(token1), "token 1 not removed");
    assertTrue(state.tokenState.containsKey(token2), "missing token 2");
    assertEquals(newTokenDate2,
        state.tokenState.get(token2),
        "incorrect token 2 date");
    assertTrue(state.tokenState.containsKey(token3), "missing token 3");
    assertEquals(tokenDate3,
        state.tokenState.get(token3),
        "incorrect token 3 date");
    assertEquals(2,
        state.tokenMasterKeyState.size(),
        "incorrect master key count");
    assertFalse(state.tokenMasterKeyState.contains(key1),
        "master key 1 not removed");
    assertTrue(state.tokenMasterKeyState.contains(key2),
        "missing master key 2");
    assertTrue(state.tokenMasterKeyState.contains(key3),
        "missing master key 3");
    assertEquals(12345679,
        state.getLatestSequenceNumber(),
        "incorrect latest sequence number");
    store.close();
  }

  @Test
  void testCheckVersion() throws IOException {
    LeveldbTimelineStateStore store =
        initAndStartTimelineServiceStateStoreService();
    // default version
    Version defaultVersion = store.getCurrentVersion();
    assertEquals(defaultVersion, store.loadVersion());

    // compatible version
    Version compatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion(),
            defaultVersion.getMinorVersion() + 2);
    store.storeVersion(compatibleVersion);
    assertEquals(compatibleVersion, store.loadVersion());
    store.stop();

    // overwrite the compatible version
    store = initAndStartTimelineServiceStateStoreService();
    assertEquals(defaultVersion, store.loadVersion());

    // incompatible version
    Version incompatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion() + 1,
            defaultVersion.getMinorVersion());
    store.storeVersion(incompatibleVersion);
    store.stop();

    try {
      initAndStartTimelineServiceStateStoreService();
      fail("Incompatible version, should expect fail here.");
    } catch (ServiceStateException e) {
      assertTrue(e.getMessage().contains("Incompatible version for timeline state store"),
          "Exception message mismatch");
    }
  }
}
