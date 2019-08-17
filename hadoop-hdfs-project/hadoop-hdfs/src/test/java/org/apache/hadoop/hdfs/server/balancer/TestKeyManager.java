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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test KeyManager class.
 */
public class TestKeyManager {
  @Rule
  public Timeout globalTimeout = new Timeout(120000);

  @Test
  public void testNewDataEncryptionKey() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // Enable data transport encryption and access token
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);

    final long keyUpdateInterval = 2 * 1000;
    final long tokenLifeTime = keyUpdateInterval;
    final String blockPoolId = "bp-foo";
    FakeTimer fakeTimer = new FakeTimer();
    BlockTokenSecretManager btsm = new BlockTokenSecretManager(
        keyUpdateInterval, tokenLifeTime, 0, 1, blockPoolId, null, false);
    Whitebox.setInternalState(btsm, "timer", fakeTimer);

    // When KeyManager asks for block keys, return them from btsm directly
    NamenodeProtocol namenode = mock(NamenodeProtocol.class);
    when(namenode.getBlockKeys()).thenReturn(btsm.exportKeys());

    // Instantiate a KeyManager instance and get data encryption key.
    KeyManager keyManager = new KeyManager(blockPoolId, namenode,
        true, conf);
    Whitebox.setInternalState(keyManager, "timer", fakeTimer);
    Whitebox.setInternalState(
        Whitebox.getInternalState(keyManager, "blockTokenSecretManager"),
        "timer", fakeTimer);
    final DataEncryptionKey dek = keyManager.newDataEncryptionKey();
    final long remainingTime = dek.expiryDate - fakeTimer.now();
    assertEquals("KeyManager dataEncryptionKey should expire in 2 seconds",
        keyUpdateInterval, remainingTime);
    // advance the timer to expire the block key and data encryption key
    fakeTimer.advance(keyUpdateInterval + 1);

    // After the initial data encryption key expires, KeyManager should
    // regenerate a valid data encryption key using the current block key.
    final DataEncryptionKey dekAfterExpiration =
        keyManager.newDataEncryptionKey();
    assertNotEquals("KeyManager should generate a new data encryption key",
        dek, dekAfterExpiration);
    assertTrue("KeyManager has an expired DataEncryptionKey!",
        dekAfterExpiration.expiryDate > fakeTimer.now());
  }
}