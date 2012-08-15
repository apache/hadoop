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
package org.apache.hadoop.hdfs.qjournal.client;

import static org.junit.Assert.*;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.JID;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.FAKE_NSINFO;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.writeSegment;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.writeTxns;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.verifyEdits;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * Functional tests for QuorumJournalManager.
 * For true unit tests, see {@link TestQuorumJournalManagerUnit}.
 */
public class TestQuorumJournalManager {
  private static final Log LOG = LogFactory.getLog(
      TestQuorumJournalManager.class);
  
  private MiniJournalCluster cluster;
  private Configuration conf;
  private QuorumJournalManager qjm;
  private List<AsyncLogger> spies;
  
  static {
    ((Log4JLogger)ProtobufRpcEngine.LOG).getLogger().setLevel(Level.ALL);
  }

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    // Don't retry connections - it just slows down the tests.
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    
    cluster = new MiniJournalCluster.Builder(conf)
      .build();
    
    qjm = createSpyingQJM();
    spies = qjm.getLoggerSetForTests().getLoggersForTests();

    qjm.format(QJMTestUtil.FAKE_NSINFO);
    qjm.recoverUnfinalizedSegments();
    assertEquals(1, qjm.getLoggerSetForTests().getEpoch());
  }
  
  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testSingleWriter() throws Exception {
    writeSegment(cluster, qjm, 1, 3, true);
    
    // Should be finalized
    checkRecovery(cluster, 1, 3);
    
    // Start a new segment
    writeSegment(cluster, qjm, 4, 1, true);

    // Should be finalized
    checkRecovery(cluster, 4, 4);
  }
  
  @Test
  public void testFormat() throws Exception {
    QuorumJournalManager qjm = new QuorumJournalManager(
        conf, cluster.getQuorumJournalURI("testFormat-jid"), FAKE_NSINFO);
    assertFalse(qjm.hasSomeData());
    qjm.format(FAKE_NSINFO);
    assertTrue(qjm.hasSomeData());
  }
  
  @Test
  public void testReaderWhileAnotherWrites() throws Exception {
    
    QuorumJournalManager readerQjm = createSpyingQJM();
    List<EditLogInputStream> streams = Lists.newArrayList();
    readerQjm.selectInputStreams(streams, 0, false);
    assertEquals(0, streams.size());
    writeSegment(cluster, qjm, 1, 3, true);

    readerQjm.selectInputStreams(streams, 0, false);
    try {
      assertEquals(1, streams.size());
      // Validate the actual stream contents.
      EditLogInputStream stream = streams.get(0);
      assertEquals(1, stream.getFirstTxId());
      assertEquals(3, stream.getLastTxId());
      
      verifyEdits(streams, 1, 3);
      assertNull(stream.readOp());
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      streams.clear();
    }
    
    // Ensure correct results when there is a stream in-progress, but we don't
    // ask for in-progress.
    writeSegment(cluster, qjm, 4, 3, false);
    readerQjm.selectInputStreams(streams, 0, false);
    try {
      assertEquals(1, streams.size());
      EditLogInputStream stream = streams.get(0);
      assertEquals(1, stream.getFirstTxId());
      assertEquals(3, stream.getLastTxId());
      verifyEdits(streams, 1, 3);
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      streams.clear();
    }
    
    // TODO: check results for selectInputStreams with inProgressOK = true.
    // This doesn't currently work, due to a bug where RedundantEditInputStream
    // throws an exception if there are any unvalidated in-progress edits in the list!
    // But, it shouldn't be necessary for current use cases.
    
    qjm.finalizeLogSegment(4, 6);
    readerQjm.selectInputStreams(streams, 0, false);
    try {
      assertEquals(2, streams.size());
      assertEquals(4, streams.get(1).getFirstTxId());
      assertEquals(6, streams.get(1).getLastTxId());

      verifyEdits(streams, 1, 6);
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      streams.clear();
    }
  }
  
  /**
   * Regression test for HDFS-3725. One of the journal nodes is down
   * during the writing of one segment, then comes back up later to
   * take part in a later segment. Thus, its local edits are
   * not a contiguous sequence. This should be handled correctly.
   */
  @Test
  public void testOneJNMissingSegments() throws Exception {
    writeSegment(cluster, qjm, 1, 3, true);
    waitForAllPendingCalls(qjm.getLoggerSetForTests());
    cluster.getJournalNode(0).stopAndJoin(0);
    writeSegment(cluster, qjm, 4, 3, true);
    waitForAllPendingCalls(qjm.getLoggerSetForTests());
    cluster.restartJournalNode(0);
    writeSegment(cluster, qjm, 7, 3, true);
    waitForAllPendingCalls(qjm.getLoggerSetForTests());
    cluster.getJournalNode(1).stopAndJoin(0);
    
    QuorumJournalManager readerQjm = createSpyingQJM();
    List<EditLogInputStream> streams = Lists.newArrayList();
    try {
      readerQjm.selectInputStreams(streams, 1, false);
      verifyEdits(streams, 1, 9);
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      readerQjm.close();
    }
  }
  
  
  /**
   * Test case where a new writer picks up from an old one with no failures
   * and the previous unfinalized segment entirely consistent -- i.e. all
   * the JournalNodes end at the same transaction ID.
   */
  @Test
  public void testChangeWritersLogsInSync() throws Exception {
    writeSegment(cluster, qjm, 1, 3, false);
    QJMTestUtil.assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(1));

    // Make a new QJM
    qjm = new QuorumJournalManager(
        conf, cluster.getQuorumJournalURI(JID), FAKE_NSINFO);
    qjm.recoverUnfinalizedSegments();
    checkRecovery(cluster, 1, 3);
  }
  
  /**
   * Test case where a new writer picks up from an old one which crashed
   * with the three loggers at different txnids
   */
  @Test
  public void testChangeWritersLogsOutOfSync1() throws Exception {
    // Journal states:  [3, 4, 5]
    // During recovery: [x, 4, 5]
    // Should recovery to txn 5
    doOutOfSyncTest(0, 5L);
  }

  @Test
  public void testChangeWritersLogsOutOfSync2() throws Exception {
    // Journal states:  [3, 4, 5]
    // During recovery: [3, x, 5]
    // Should recovery to txn 5
    doOutOfSyncTest(1, 5L);
  }

  @Test
  public void testChangeWritersLogsOutOfSync3() throws Exception {
    // Journal states:  [3, 4, 5]
    // During recovery: [3, 4, x]
    // Should recovery to txn 4
    doOutOfSyncTest(2, 4L);
  }

  
  private void doOutOfSyncTest(int missingOnRecoveryIdx,
      long expectedRecoveryTxnId) throws Exception {
    EditLogOutputStream stm = qjm.startLogSegment(1);
    
    failLoggerAtTxn(spies.get(0), 4);
    failLoggerAtTxn(spies.get(1), 5);
    
    writeTxns(stm, 1, 3);
    
    // This should succeed to 2/3 loggers
    writeTxns(stm, 4, 1);
    
    // This should only succeed to 1 logger (index 2). Hence it should
    // fail
    try {
      writeTxns(stm, 5, 1);
      fail("Did not fail to write when only a minority succeeded");
    } catch (QuorumException qe) {
      GenericTestUtils.assertExceptionContains(
          "too many exceptions to achieve quorum size 2/3",
          qe);
    }
    
    QJMTestUtil.assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(1));

    // Shut down the specified JN, so it's not present during recovery.
    cluster.getJournalNode(missingOnRecoveryIdx).stopAndJoin(0);

    // Make a new QJM
    qjm = createSpyingQJM();
    
    qjm.recoverUnfinalizedSegments();
    checkRecovery(cluster, 1, expectedRecoveryTxnId);
  }
  
  
  private void failLoggerAtTxn(AsyncLogger spy, long txid) {
    TestQuorumJournalManagerUnit.futureThrows(new IOException("mock failure"))
      .when(spy).sendEdits(
        Mockito.eq(txid), Mockito.eq(1), Mockito.<byte[]>any());
  }
  
  /**
   * edit lengths [3,4,5]
   * first recovery:
   * - sees [3,4,x]
   * - picks length 4 for recoveryEndTxId
   * - calls acceptRecovery()
   * - crashes before finalizing
   * second recovery:
   * - sees [x, 4, 5]
   * - should pick recovery length 4, even though it saw
   *   a larger txid, because a previous recovery accepted it
   */
  @Test
  public void testRecoverAfterIncompleteRecovery() throws Exception {
    EditLogOutputStream stm = qjm.startLogSegment(1);
    
    failLoggerAtTxn(spies.get(0), 4);
    failLoggerAtTxn(spies.get(1), 5);
    
    writeTxns(stm, 1, 3);
    
    // This should succeed to 2/3 loggers
    writeTxns(stm, 4, 1);
    
    // This should only succeed to 1 logger (index 2). Hence it should
    // fail
    try {
      writeTxns(stm, 5, 1);
      fail("Did not fail to write when only a minority succeeded");
    } catch (QuorumException qe) {
      GenericTestUtils.assertExceptionContains(
          "too many exceptions to achieve quorum size 2/3",
          qe);
    }

    // Shut down the logger that has length = 5
    cluster.getJournalNode(2).stopAndJoin(0);

    qjm = createSpyingQJM();
    spies = qjm.getLoggerSetForTests().getLoggersForTests();

    // Allow no logger to finalize
    for (AsyncLogger spy : spies) {
      TestQuorumJournalManagerUnit.futureThrows(new IOException("injected"))
        .when(spy).finalizeLogSegment(Mockito.eq(1L),
            Mockito.eq(4L));
    }
    try {
      qjm.recoverUnfinalizedSegments();
      fail("Should have failed recovery since no finalization occurred");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("injected", ioe);
    }
    
    // Now bring back the logger that had 5, and run recovery again.
    // We should recover to 4, even though there's a longer log.
    cluster.getJournalNode(0).stopAndJoin(0);
    cluster.restartJournalNode(2);
    
    qjm = createSpyingQJM();
    spies = qjm.getLoggerSetForTests().getLoggersForTests();
    qjm.recoverUnfinalizedSegments();
    checkRecovery(cluster, 1, 4);
  }
  
  @Test
  public void testPurgeLogs() throws Exception {
    for (int txid = 1; txid <= 5; txid++) {
      writeSegment(cluster, qjm, txid, 1, true);
    }
    File curDir = cluster.getCurrentDir(0, JID);
    GenericTestUtils.assertGlobEquals(curDir, "edits_.*",
        NNStorage.getFinalizedEditsFileName(1, 1),
        NNStorage.getFinalizedEditsFileName(2, 2),
        NNStorage.getFinalizedEditsFileName(3, 3),
        NNStorage.getFinalizedEditsFileName(4, 4),
        NNStorage.getFinalizedEditsFileName(5, 5));
    File paxosDir = new File(curDir, "paxos");
    GenericTestUtils.assertExists(paxosDir);

    // Create new files in the paxos directory, which should get purged too.
    assertTrue(new File(paxosDir, "1").createNewFile());
    assertTrue(new File(paxosDir, "3").createNewFile());
    
    GenericTestUtils.assertGlobEquals(paxosDir, "\\d+",
        "1", "3");
    
    qjm.purgeLogsOlderThan(3);
    
    // Log purging is asynchronous, so we have to wait for the calls
    // to be sent and respond before verifying.
    waitForAllPendingCalls(qjm.getLoggerSetForTests());
    
    // Older edits should be purged
    GenericTestUtils.assertGlobEquals(curDir, "edits_.*",
        NNStorage.getFinalizedEditsFileName(3, 3),
        NNStorage.getFinalizedEditsFileName(4, 4),
        NNStorage.getFinalizedEditsFileName(5, 5));
   
    // Older paxos files should be purged
    GenericTestUtils.assertGlobEquals(paxosDir, "\\d+",
        "3");
  }
  
  
  private QuorumJournalManager createSpyingQJM()
      throws IOException, URISyntaxException {
    AsyncLogger.Factory spyFactory = new AsyncLogger.Factory() {
      @Override
      public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
          String journalId, InetSocketAddress addr) {
        return Mockito.spy(IPCLoggerChannel.FACTORY.createLogger(
            conf, nsInfo, journalId, addr));
      }
    };
    return new QuorumJournalManager(
        conf, cluster.getQuorumJournalURI(JID), FAKE_NSINFO, spyFactory);
  }

  private static void waitForAllPendingCalls(AsyncLoggerSet als)
      throws InterruptedException {
    for (AsyncLogger l : als.getLoggersForTests()) {
      IPCLoggerChannel ch = (IPCLoggerChannel)l;
      ch.waitForAllPendingCalls();
    }
  }

  private void checkRecovery(MiniJournalCluster cluster,
      long segmentTxId, long expectedEndTxId)
      throws IOException {
    int numFinalized = 0;
    for (int i = 0; i < cluster.getNumNodes(); i++) {
      File logDir = cluster.getCurrentDir(i, JID);
      EditLogFile elf = FileJournalManager.getLogFile(logDir, segmentTxId);
      if (elf == null) {
        continue;
      }
      if (!elf.isInProgress()) {
        numFinalized++;
        if (elf.getLastTxId() != expectedEndTxId) {
          fail("File " + elf + " finalized to wrong txid, expected " +
              expectedEndTxId);
        }
      }      
    }
    
    if (numFinalized < cluster.getQuorumSize()) {
      fail("Did not find a quorum of finalized logs starting at " +
          segmentTxId);
    }
  }
}
