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

package org.apache.hadoop.hdfs.security.token.block;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests the behaviour of {@link BlockTokenSecretManager} when it is configured
 * with a token lifetime of zero, which is what
 * {@code dfs.block.access.token.lifetime = 0} produces (HDFS-17947).
 *
 * A zero lifetime makes every token expire at the instant it is minted, so it
 * is rejected by the very next verification. These tests pin that behaviour
 * down at the secret-manager level, without needing a cluster.
 */
public class TestBlockTokenZeroLifetime {

  private static final String BLOCK_POOL_ID = "fake-pool";
  private static final String USER = "someuser";
  private static final ExtendedBlock BLOCK =
      new ExtendedBlock(BLOCK_POOL_ID, 1L);
  private static final StorageType[] STORAGE_TYPES =
      new StorageType[]{StorageType.DEFAULT};

  private static final long KEY_UPDATE_INTERVAL = 10 * 60 * 1000L; // 10 mins
  private static final long ZERO_LIFETIME = 0L;
  /** The hdfs-default.xml value of 600 minutes, used as the control. */
  private static final long DEFAULT_LIFETIME = 600 * 60 * 1000L;

  private static BlockTokenSecretManager newMaster(long tokenLifetime,
      boolean enableProtobuf) {
    return new BlockTokenSecretManager(KEY_UPDATE_INTERVAL, tokenLifetime, 0, 1,
        BLOCK_POOL_ID, null, enableProtobuf);
  }

  private static BlockTokenSecretManager newSlave(long tokenLifetime,
      boolean enableProtobuf) {
    return new BlockTokenSecretManager(KEY_UPDATE_INTERVAL, tokenLifetime,
        BLOCK_POOL_ID, null, enableProtobuf);
  }

  private static BlockTokenIdentifier decode(Token<BlockTokenIdentifier> token)
      throws IOException {
    BlockTokenIdentifier id = new BlockTokenIdentifier();
    id.readFields(
        new DataInputStream(new ByteArrayInputStream(token.getIdentifier())));
    return id;
  }

  /**
   * Block token expiry is evaluated against the wall clock with a strict
   * comparison ({@code Time.now() > expiryDate}), so a token whose expiry
   * equals its creation instant becomes invalid as soon as the clock ticks
   * once. Waiting for that tick keeps the assertions below deterministic
   * rather than racing the clock.
   */
  private static void awaitClockStrictlyAfter(long timestamp)
      throws InterruptedException {
    while (Time.now() <= timestamp) {
      Thread.sleep(1);
    }
  }

  /**
   * With a lifetime of zero the minted token carries no validity window at
   * all: its expiry date is its creation instant.
   */
  private void testZeroLifetimeLeavesNoValidityWindow(boolean enableProtobuf)
      throws Exception {
    BlockTokenSecretManager master = newMaster(ZERO_LIFETIME, enableProtobuf);

    long before = Time.now();
    Token<BlockTokenIdentifier> token = master.generateToken(USER, BLOCK,
        EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE), STORAGE_TYPES, null);
    long after = Time.now();

    long expiryDate = decode(token).getExpiryDate();
    assertTrue(expiryDate >= before && expiryDate <= after,
        "with lifetime=0 a token must expire at the instant it is created, but"
            + " expiryDate=" + expiryDate + " is outside the creation window ["
            + before + ", " + after + "]");
  }

  @Test
  public void testZeroLifetimeLeavesNoValidityWindowLegacy() throws Exception {
    testZeroLifetimeLeavesNoValidityWindow(false);
  }

  @Test
  public void testZeroLifetimeLeavesNoValidityWindowProtobuf()
      throws Exception {
    testZeroLifetimeLeavesNoValidityWindow(true);
  }

  /**
   * A token minted with a zero lifetime is rejected by the DataNode-side
   * (worker) secret manager. This is the check that fails on every write
   * pipeline setup in HDFS-17947.
   */
  private void testZeroLifetimeTokenIsRejected(boolean enableProtobuf)
      throws Exception {
    BlockTokenSecretManager master = newMaster(ZERO_LIFETIME, enableProtobuf);
    BlockTokenSecretManager slave = newSlave(ZERO_LIFETIME, enableProtobuf);
    slave.addKeys(master.exportKeys());

    Token<BlockTokenIdentifier> token = master.generateToken(USER, BLOCK,
        EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE), STORAGE_TYPES, null);
    awaitClockStrictlyAfter(decode(token).getExpiryDate());

    InvalidToken e = assertThrows(InvalidToken.class,
        () -> slave.checkAccess(token, USER, BLOCK,
            BlockTokenIdentifier.AccessMode.WRITE),
        "a token minted with lifetime=0 must not pass verification");
    assertTrue(e.getMessage().contains("is expired"),
        "expected an expiry failure, got: " + e.getMessage());
  }

  @Test
  public void testZeroLifetimeTokenIsRejectedLegacy() throws Exception {
    testZeroLifetimeTokenIsRejected(false);
  }

  @Test
  public void testZeroLifetimeTokenIsRejectedProtobuf() throws Exception {
    testZeroLifetimeTokenIsRejected(true);
  }

  /**
   * Control: the same sequence with the documented default lifetime of 600
   * minutes verifies successfully.
   */
  private void testDefaultLifetimeTokenIsAccepted(boolean enableProtobuf)
      throws Exception {
    BlockTokenSecretManager master =
        newMaster(DEFAULT_LIFETIME, enableProtobuf);
    BlockTokenSecretManager slave =
        newSlave(DEFAULT_LIFETIME, enableProtobuf);
    slave.addKeys(master.exportKeys());

    Token<BlockTokenIdentifier> token = master.generateToken(USER, BLOCK,
        EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE), STORAGE_TYPES, null);

    slave.checkAccess(token, USER, BLOCK,
        BlockTokenIdentifier.AccessMode.WRITE);
  }

  @Test
  public void testDefaultLifetimeTokenIsAcceptedLegacy() throws Exception {
    testDefaultLifetimeTokenIsAccepted(false);
  }

  @Test
  public void testDefaultLifetimeTokenIsAcceptedProtobuf() throws Exception {
    testDefaultLifetimeTokenIsAccepted(true);
  }
}
