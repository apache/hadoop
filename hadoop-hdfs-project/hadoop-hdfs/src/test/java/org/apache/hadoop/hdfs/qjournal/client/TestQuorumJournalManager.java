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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.client.AsyncLogger;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.client.QuorumException;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Functional tests for QuorumJournalManager.
 * For true unit tests, see {@link TestQuorumJournalManagerUnit}.
 */
public class TestQuorumJournalManager {
  private static final Log LOG = LogFactory.getLog(
      TestQuorumJournalManager.class);
  
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L, 0);
  private static final String JID = "testQuorumJournalManager";
  private MiniJournalCluster cluster;
  private Configuration conf;
  private QuorumJournalManager qjm;
  private List<AsyncLogger> spies;

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    cluster = new MiniJournalCluster.Builder(conf)
      .build();
    
    qjm = createSpyingQJM();
    spies = qjm.getLoggerSetForTests().getLoggersForTests();

    qjm.recoverUnfinalizedSegments();
    assertEquals(1, qjm.getLoggerSetForTests().getEpoch());
  }
  
  @After
  public void shutdown() throws IOException {
    cluster.shutdown();
  }
  
  @Test
  public void testSingleWriter() throws Exception {
    writeSegment(qjm, 1, 3, true);
    
    // Should be finalized
    checkRecovery(cluster, 1, 3);
    
    // Start a new segment
    writeSegment(qjm, 4, 1, true);

    // Should be finalized
    checkRecovery(cluster, 4, 4);
  }
  
  @Test
  public void testReaderWhileAnotherWrites() throws Exception {
    
    QuorumJournalManager readerQjm = createSpyingQJM();
    List<EditLogInputStream> streams = Lists.newArrayList();
    readerQjm.selectInputStreams(streams, 0, false);
    assertEquals(0, streams.size());
    writeSegment(qjm, 1, 3, true);

    readerQjm.selectInputStreams(streams, 0, false);
    try {
      assertEquals(1, streams.size());
      // Validate the actual stream contents.
      EditLogInputStream stream = streams.get(0);
      assertEquals(1, stream.getFirstTxId());
      assertEquals(3, stream.getLastTxId());
      
      for (int i = 1; i <= 3; i++) {
        FSEditLogOp op = stream.readOp();
        assertEquals(FSEditLogOpCodes.OP_MKDIR, op.opCode);
        assertEquals(i, op.getTransactionId());
      }
      assertNull(stream.readOp());
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      streams.clear();
    }
    
    // Ensure correct results when there is a stream in-progress, but we don't
    // ask for in-progress.
    writeSegment(qjm, 4, 3, false);
    readerQjm.selectInputStreams(streams, 0, false);
    try {
      assertEquals(1, streams.size());
      EditLogInputStream stream = streams.get(0);
      assertEquals(1, stream.getFirstTxId());
      assertEquals(3, stream.getLastTxId());
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
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      streams.clear();
    }
  }
  
  /**
   * TODO: this test needs to be fleshed out to be an exhaustive failure test
   * @throws Exception
   */
  @Test
  public void testOrchestratedFailures() throws Exception {
    writeSegment(qjm, 1, 3, true);
    writeSegment(qjm, 4, 3, true);
    
    SortedSet<Long> serials = Sets.newTreeSet();
    for (AsyncLogger l : qjm.getLoggerSetForTests().getLoggersForTests()) {
      IPCLoggerChannel ch = (IPCLoggerChannel)l;
      ch.waitForAllPendingCalls();
      serials.add(ch.getNextIpcSerial());
    }

    // All of the loggers should have sent the same number of RPCs, since there
    // were no failures.
    assertEquals(1, serials.size());
    
    long maxSerial = serials.first();
    LOG.info("Max IPC serial = " + maxSerial);
    
    cluster.shutdown();
    
    cluster = new MiniJournalCluster.Builder(conf)
      .build();
    qjm = createSpyingQJM();
    spies = qjm.getLoggerSetForTests().getLoggersForTests();

  }
  
  /**
   * Test case where a new writer picks up from an old one with no failures
   * and the previous unfinalized segment entirely consistent -- i.e. all
   * the JournalNodes end at the same transaction ID.
   */
  @Test
  public void testChangeWritersLogsInSync() throws Exception {
    writeSegment(qjm, 1, 3, false);
    assertExistsInQuorum(cluster,
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
    
    assertExistsInQuorum(cluster,
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
      writeSegment(qjm, txid, 1, true);
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
    return new QuorumJournalManager(
        conf, cluster.getQuorumJournalURI(JID), FAKE_NSINFO) {
          @Override
          protected List<AsyncLogger> createLoggers() throws IOException {
            List<AsyncLogger> realLoggers = super.createLoggers();
            List<AsyncLogger> spies = Lists.newArrayList();
            for (AsyncLogger logger : realLoggers) {
              spies.add(Mockito.spy(logger));
            }
            return spies;
          }
    };
  }

  private void writeSegment(QuorumJournalManager qjm,
      int startTxId, int numTxns, boolean finalize) throws IOException {
    EditLogOutputStream stm = qjm.startLogSegment(startTxId);
    // Should create in-progress
    assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(startTxId));
    
    writeTxns(stm, startTxId, numTxns);
    if (finalize) {
      stm.close();
      qjm.finalizeLogSegment(startTxId, startTxId + numTxns - 1);
    }
  }

  private void writeTxns(EditLogOutputStream stm, int startTxId, int numTxns)
      throws IOException {
    for (long txid = startTxId; txid < startTxId + numTxns; txid++) {
      TestQuorumJournalManagerUnit.writeOp(stm, txid);
    }
    stm.setReadyToFlush();
    stm.flush();
  }
  
  private static void waitForAllPendingCalls(AsyncLoggerSet als)
      throws InterruptedException {
    for (AsyncLogger l : als.getLoggersForTests()) {
      IPCLoggerChannel ch = (IPCLoggerChannel)l;
      ch.waitForAllPendingCalls();
    }
  }

  private void assertExistsInQuorum(MiniJournalCluster cluster,
      String fname) {
    int count = 0;
    for (int i = 0; i < 3; i++) {
      File dir = cluster.getCurrentDir(i, JID);
      if (new File(dir, fname).exists()) {
        count++;
      }
    }
    assertTrue("File " + fname + " should exist in a quorum of dirs",
        count >= cluster.getQuorumSize());
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
