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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test how a namenode answers for a delegation token it does not recognise,
 * depending on the HA state it is serving in.
 *
 * <p>An observer serves reads from a namespace that trails the active, so a
 * token the active has just issued may not have been tailed here yet.
 * Rejecting it outright is fatal to the caller, so the client is asked to
 * retry instead - the same treatment HDFS-5322 gave a namenode that is still
 * transitioning to active.
 *
 * <p>Both token entry points are covered: the RPC path, through
 * {@link DelegationTokenSecretManager#retriableRetrievePassword}, and the
 * WebHDFS path, through {@link FSNamesystem#verifyToken}.
 *
 * <p>See also
 * {@link TestDelegationTokensWithHA#testDelegationTokenDuringNNFailover},
 * which covers the transition-to-active half of the same predicate.
 */
@Timeout(value = 300)
public class TestDelegationTokensWithObserver {
  private static final int ACTIVE_INDEX = 0;
  private static final int STANDBY_INDEX = 1;
  private static final int OBSERVER_INDEX = 2;

  private static final String TOKEN_NOT_IN_CACHE = "can't be found in cache";

  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;

  @BeforeAll
  public static void startUpCluster() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    // fast tailing would race the explicit rollEditLogAndTail that the
    // token tests below depend on, as TestConsistentReadsObserver documents
    qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 1, 0, false);
    dfsCluster = qjmhaCluster.getDfsCluster();

    assertServiceState(ACTIVE_INDEX, HAServiceState.ACTIVE);
    assertServiceState(STANDBY_INDEX, HAServiceState.STANDBY);
    assertServiceState(OBSERVER_INDEX, HAServiceState.OBSERVER);
  }

  @AfterAll
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
      qjmhaCluster = null;
      dfsCluster = null;
    }
  }

  /**
   * An observer cannot tell a token the active has just issued from one that
   * never existed, so it must ask the caller to retry rather than fail it.
   */
  @Test
  public void testUnknownTokenOnObserverIsRetriable() {
    DelegationTokenIdentifier unknown = unknownTokenIdentifier();

    RetriableException thrown = assertThrows(RetriableException.class,
        () -> retrievePasswordOn(OBSERVER_INDEX, unknown));

    assertInstanceOf(InvalidToken.class, thrown.getCause());
    GenericTestUtils.assertExceptionContains(
        TOKEN_NOT_IN_CACHE, thrown.getCause());
  }

  /**
   * The active is authoritative on which tokens exist, so an unknown token
   * there is genuinely invalid and must stay fatal.
   */
  @Test
  public void testUnknownTokenOnActiveIsInvalid() {
    DelegationTokenIdentifier unknown = unknownTokenIdentifier();

    InvalidToken thrown = assertThrows(InvalidToken.class,
        () -> retrievePasswordOn(ACTIVE_INDEX, unknown));

    GenericTestUtils.assertExceptionContains(TOKEN_NOT_IN_CACHE, thrown);
  }

  /**
   * A standby refuses the read outright, before the token cache is consulted
   * at all, which is retriable by way of failover to the active.
   */
  @Test
  public void testUnknownTokenOnStandbyIsRejectedBeforeLookup() {
    DelegationTokenIdentifier unknown = unknownTokenIdentifier();

    StandbyException thrown = assertThrows(StandbyException.class,
        () -> retrievePasswordOn(STANDBY_INDEX, unknown));

    GenericTestUtils.assertExceptionContains("READ", thrown);
  }

  /**
   * Retrying unknown tokens must not come at the cost of the tokens an
   * observer legitimately knows: once the creating edit has been tailed, the
   * observer serves the same password as the active.
   */
  @Test
  public void testKnownTokenOnObserverIsAccepted() throws Exception {
    Token<DelegationTokenIdentifier> token =
        dfsCluster.getFileSystem(ACTIVE_INDEX).getDelegationToken("renewer");
    dfsCluster.rollEditLogAndTail(ACTIVE_INDEX);

    byte[] password =
        retrievePasswordOn(OBSERVER_INDEX, token.decodeIdentifier());

    assertArrayEquals(token.getPassword(), password);
  }

  /**
   * The bug itself, with a real token: the active issues one, and until the
   * observer tails the creating edit it must ask the caller to retry rather
   * than reject a token that is perfectly valid. Deterministic because fast
   * tailing is off, so nothing reaches the observer until the log is rolled.
   */
  @Test
  public void testUntailedTokenOnObserverIsRetriable() throws Exception {
    Token<DelegationTokenIdentifier> token =
        dfsCluster.getFileSystem(ACTIVE_INDEX).getDelegationToken("renewer");

    RetriableException thrown = assertThrows(RetriableException.class,
        () -> retrievePasswordOn(OBSERVER_INDEX, token.decodeIdentifier()));

    assertInstanceOf(InvalidToken.class, thrown.getCause());
    GenericTestUtils.assertExceptionContains(
        TOKEN_NOT_IN_CACHE, thrown.getCause());
  }

  /**
   * A renewal the observer has not tailed leaves it holding a token whose
   * renew date has already passed. That is staleness too, not a dead token,
   * so it is retriable in the same way an entirely untailed token is.
   */
  @Test
  public void testStaleExpiryOnObserverIsRetriable() throws Exception {
    Token<DelegationTokenIdentifier> token =
        dfsCluster.getFileSystem(ACTIVE_INDEX).getDelegationToken("renewer");
    dfsCluster.rollEditLogAndTail(ACTIVE_INDEX);
    DelegationTokenIdentifier identifier = token.decodeIdentifier();
    NameNodeAdapter.getDtSecretManager(namesystemOn(OBSERVER_INDEX))
        .updatePersistedTokenRenewal(identifier, Time.now() - 1);

    RetriableException thrown = assertThrows(RetriableException.class,
        () -> retrievePasswordOn(OBSERVER_INDEX, identifier));

    GenericTestUtils.assertExceptionContains("has expired", thrown.getCause());
  }

  /**
   * The WebHDFS path reaches the token cache through a different entry point,
   * and must answer for an observer the same way the RPC path does.
   */
  @Test
  public void testUnknownTokenOnObserverIsRetriableOverWebHdfsPath() {
    DelegationTokenIdentifier unknown = unknownTokenIdentifier();

    RetriableException thrown = assertThrows(RetriableException.class,
        () -> verifyTokenOn(OBSERVER_INDEX, unknown, noPassword()));

    assertInstanceOf(InvalidToken.class, thrown.getCause());
    GenericTestUtils.assertExceptionContains(
        TOKEN_NOT_IN_CACHE, thrown.getCause());
  }

  /**
   * The widened guard must not have made the active lenient in passing.
   */
  @Test
  public void testUnknownTokenOnActiveIsInvalidOverWebHdfsPath() {
    DelegationTokenIdentifier unknown = unknownTokenIdentifier();

    InvalidToken thrown = assertThrows(InvalidToken.class,
        () -> verifyTokenOn(ACTIVE_INDEX, unknown, noPassword()));

    GenericTestUtils.assertExceptionContains(TOKEN_NOT_IN_CACHE, thrown);
  }

  /**
   * The WebHDFS path must show the same restraint as the RPC path: retrying
   * unknown tokens must not cost the observer the tokens it does know.
   */
  @Test
  public void testKnownTokenOnObserverIsAcceptedOverWebHdfsPath()
      throws Exception {
    Token<DelegationTokenIdentifier> token =
        dfsCluster.getFileSystem(ACTIVE_INDEX).getDelegationToken("renewer");
    dfsCluster.rollEditLogAndTail(ACTIVE_INDEX);

    assertDoesNotThrow(() -> verifyTokenOn(
        OBSERVER_INDEX, token.decodeIdentifier(), token.getPassword()));
  }

  /**
   * A wrong password is never a symptom of the observer lagging the active,
   * so it stays fatal there rather than being dressed up as retriable.
   */
  @Test
  public void testWrongPasswordOnObserverStaysInvalidOverWebHdfsPath()
      throws Exception {
    Token<DelegationTokenIdentifier> token =
        dfsCluster.getFileSystem(ACTIVE_INDEX).getDelegationToken("renewer");
    dfsCluster.rollEditLogAndTail(ACTIVE_INDEX);
    byte[] wrongPassword = "not the real password".getBytes(UTF_8);

    InvalidToken thrown = assertThrows(InvalidToken.class, () -> verifyTokenOn(
        OBSERVER_INDEX, token.decodeIdentifier(), wrongPassword));

    GenericTestUtils.assertExceptionContains("password doesn't match", thrown);
  }

  /**
   * The WebHDFS path has no operation check of its own, so a standby reaches
   * the token cache and reports the refusal as an InvalidToken wrapping the
   * StandbyException, rather than throwing StandbyException outright.
   */
  @Test
  public void testUnknownTokenOnStandbyIsInvalidOverWebHdfsPath() {
    DelegationTokenIdentifier unknown = unknownTokenIdentifier();

    InvalidToken thrown = assertThrows(InvalidToken.class,
        () -> verifyTokenOn(STANDBY_INDEX, unknown, noPassword()));

    assertInstanceOf(StandbyException.class, thrown.getCause());
  }

  private static byte[] retrievePasswordOn(
      int nnIndex, DelegationTokenIdentifier identifier) throws IOException {
    return NameNodeAdapter.getDtSecretManager(namesystemOn(nnIndex))
        .retriableRetrievePassword(identifier);
  }

  private static void verifyTokenOn(int nnIndex,
      DelegationTokenIdentifier identifier, byte[] password)
      throws IOException {
    namesystemOn(nnIndex).verifyToken(identifier, password);
  }

  private static FSNamesystem namesystemOn(int nnIndex) {
    return dfsCluster.getNameNode(nnIndex).getNamesystem();
  }

  private static void assertServiceState(int nnIndex, HAServiceState expected) {
    assertEquals(expected,
        NameNodeAdapter.getServiceState(dfsCluster.getNameNode(nnIndex)),
        "unexpected HA state for NN[" + nnIndex + "]");
  }

  private static byte[] noPassword() {
    return new byte[0];
  }

  private static DelegationTokenIdentifier unknownTokenIdentifier() {
    return new DelegationTokenIdentifier(
        new Text("unknownOwner"), new Text("renewer"),
        new Text("unknownRealUser"));
  }
}
