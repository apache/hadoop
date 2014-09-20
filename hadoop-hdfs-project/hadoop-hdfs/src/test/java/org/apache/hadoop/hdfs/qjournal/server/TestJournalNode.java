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
package org.apache.hadoop.hdfs.qjournal.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;


public class TestJournalNode {
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L);

  private static final File TEST_BUILD_DATA = PathUtils.getTestDir(TestJournalNode.class);

  private JournalNode jn;
  private Journal journal; 
  private final Configuration conf = new Configuration();
  private IPCLoggerChannel ch;
  private String journalId;

  static {
    // Avoid an error when we double-initialize JvmMetrics
    DefaultMetricsSystem.setMiniClusterMode(true);
  }
  
  @Before
  public void setup() throws Exception {
    File editsDir = new File(MiniDFSCluster.getBaseDirectory() +
        File.separator + "TestJournalNode");
    FileUtil.fullyDelete(editsDir);
    
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
        editsDir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY,
        "0.0.0.0:0");
    jn = new JournalNode();
    jn.setConf(conf);
    jn.start();
    journalId = "test-journalid-" + GenericTestUtils.uniqueSequenceId();
    journal = jn.getOrCreateJournal(journalId);
    journal.format(FAKE_NSINFO);
    
    ch = new IPCLoggerChannel(conf, FAKE_NSINFO, journalId, jn.getBoundIpcAddress());
  }
  
  @After
  public void teardown() throws Exception {
    jn.stop(0);
  }
  
  @Test(timeout=100000)
  public void testJournal() throws Exception {
    MetricsRecordBuilder metrics = MetricsAsserts.getMetrics(
        journal.getMetricsForTests().getName());
    MetricsAsserts.assertCounter("BatchesWritten", 0L, metrics);
    MetricsAsserts.assertCounter("BatchesWrittenWhileLagging", 0L, metrics);
    MetricsAsserts.assertGauge("CurrentLagTxns", 0L, metrics);

    IPCLoggerChannel ch = new IPCLoggerChannel(
        conf, FAKE_NSINFO, journalId, jn.getBoundIpcAddress());
    ch.newEpoch(1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION).get();
    ch.sendEdits(1L, 1, 1, "hello".getBytes(Charsets.UTF_8)).get();
    
    metrics = MetricsAsserts.getMetrics(
        journal.getMetricsForTests().getName());
    MetricsAsserts.assertCounter("BatchesWritten", 1L, metrics);
    MetricsAsserts.assertCounter("BatchesWrittenWhileLagging", 0L, metrics);
    MetricsAsserts.assertGauge("CurrentLagTxns", 0L, metrics);

    ch.setCommittedTxId(100L);
    ch.sendEdits(1L, 2, 1, "goodbye".getBytes(Charsets.UTF_8)).get();

    metrics = MetricsAsserts.getMetrics(
        journal.getMetricsForTests().getName());
    MetricsAsserts.assertCounter("BatchesWritten", 2L, metrics);
    MetricsAsserts.assertCounter("BatchesWrittenWhileLagging", 1L, metrics);
    MetricsAsserts.assertGauge("CurrentLagTxns", 98L, metrics);

  }
  
  
  @Test(timeout=100000)
  public void testReturnsSegmentInfoAtEpochTransition() throws Exception {
    ch.newEpoch(1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION).get();
    ch.sendEdits(1L, 1, 2, QJMTestUtil.createTxnData(1, 2)).get();
    
    // Switch to a new epoch without closing earlier segment
    NewEpochResponseProto response = ch.newEpoch(2).get();
    ch.setEpoch(2);
    assertEquals(1, response.getLastSegmentTxId());
    
    ch.finalizeLogSegment(1, 2).get();
    
    // Switch to a new epoch after just closing the earlier segment.
    response = ch.newEpoch(3).get();
    ch.setEpoch(3);
    assertEquals(1, response.getLastSegmentTxId());
    
    // Start a segment but don't write anything, check newEpoch segment info
    ch.startLogSegment(3, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION).get();
    response = ch.newEpoch(4).get();
    ch.setEpoch(4);
    // Because the new segment is empty, it is equivalent to not having
    // started writing it. Hence, we should return the prior segment txid.
    assertEquals(1, response.getLastSegmentTxId());
  }
  
  @Test(timeout=100000)
  public void testHttpServer() throws Exception {
    String urlRoot = jn.getHttpServerURI();
    
    // Check default servlets.
    String pageContents = DFSTestUtil.urlGet(new URL(urlRoot + "/jmx"));
    assertTrue("Bad contents: " + pageContents,
        pageContents.contains(
            "Hadoop:service=JournalNode,name=JvmMetrics"));
    
    // Check JSP page.
    pageContents = DFSTestUtil.urlGet(
        new URL(urlRoot + "/journalstatus.jsp"));
    assertTrue(pageContents.contains("JournalNode"));

    // Create some edits on server side
    byte[] EDITS_DATA = QJMTestUtil.createTxnData(1, 3);
    IPCLoggerChannel ch = new IPCLoggerChannel(
        conf, FAKE_NSINFO, journalId, jn.getBoundIpcAddress());
    ch.newEpoch(1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION).get();
    ch.sendEdits(1L, 1, 3, EDITS_DATA).get();
    ch.finalizeLogSegment(1, 3).get();

    // Attempt to retrieve via HTTP, ensure we get the data back
    // including the header we expected
    byte[] retrievedViaHttp = DFSTestUtil.urlGetBytes(new URL(urlRoot +
        "/getJournal?segmentTxId=1&jid=" + journalId));
    byte[] expected = Bytes.concat(
            Ints.toByteArray(HdfsConstants.NAMENODE_LAYOUT_VERSION),
            (new byte[] { 0, 0, 0, 0 }), // layout flags section
            EDITS_DATA);

    assertArrayEquals(expected, retrievedViaHttp);
    
    // Attempt to fetch a non-existent file, check that we get an
    // error status code
    URL badUrl = new URL(urlRoot + "/getJournal?segmentTxId=12345&jid=" + journalId);
    HttpURLConnection connection = (HttpURLConnection)badUrl.openConnection();
    try {
      assertEquals(404, connection.getResponseCode());
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Test that the JournalNode performs correctly as a Paxos
   * <em>Acceptor</em> process.
   */
  @Test(timeout=100000)
  public void testAcceptRecoveryBehavior() throws Exception {
    // We need to run newEpoch() first, or else we have no way to distinguish
    // different proposals for the same decision.
    try {
      ch.prepareRecovery(1L).get();
      fail("Did not throw IllegalState when trying to run paxos without an epoch");
    } catch (ExecutionException ise) {
      GenericTestUtils.assertExceptionContains("bad epoch", ise);
    }
    
    ch.newEpoch(1).get();
    ch.setEpoch(1);
    
    // prepare() with no previously accepted value and no logs present
    PrepareRecoveryResponseProto prep = ch.prepareRecovery(1L).get();
    System.err.println("Prep: " + prep);
    assertFalse(prep.hasAcceptedInEpoch());
    assertFalse(prep.hasSegmentState());
    
    // Make a log segment, and prepare again -- this time should see the
    // segment existing.
    ch.startLogSegment(1L, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION).get();
    ch.sendEdits(1L, 1L, 1, QJMTestUtil.createTxnData(1, 1)).get();

    prep = ch.prepareRecovery(1L).get();
    System.err.println("Prep: " + prep);
    assertFalse(prep.hasAcceptedInEpoch());
    assertTrue(prep.hasSegmentState());
    
    // accept() should save the accepted value in persistent storage
    ch.acceptRecovery(prep.getSegmentState(), new URL("file:///dev/null")).get();

    // So another prepare() call from a new epoch would return this value
    ch.newEpoch(2);
    ch.setEpoch(2);
    prep = ch.prepareRecovery(1L).get();
    assertEquals(1L, prep.getAcceptedInEpoch());
    assertEquals(1L, prep.getSegmentState().getEndTxId());
    
    // A prepare() or accept() call from an earlier epoch should now be rejected
    ch.setEpoch(1);
    try {
      ch.prepareRecovery(1L).get();
      fail("prepare from earlier epoch not rejected");
    } catch (ExecutionException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 2",
          ioe);
    }
    try {
      ch.acceptRecovery(prep.getSegmentState(), new URL("file:///dev/null")).get();
      fail("accept from earlier epoch not rejected");
    } catch (ExecutionException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 2",
          ioe);
    }
  }
  
  @Test(timeout=100000)
  public void testFailToStartWithBadConfig() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY, "non-absolute-path");
    assertJNFailsToStart(conf, "should be an absolute path");
    
    // Existing file which is not a directory 
    File existingFile = new File(TEST_BUILD_DATA, "testjournalnodefile");
    assertTrue(existingFile.createNewFile());
    try {
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
          existingFile.getAbsolutePath());
      assertJNFailsToStart(conf, "Not a directory");
    } finally {
      existingFile.delete();
    }
    
    // Directory which cannot be created
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
        Shell.WINDOWS ? "\\\\cannotBeCreated" : "/proc/does-not-exist");
    assertJNFailsToStart(conf, "Cannot create directory");
  }

  private static void assertJNFailsToStart(Configuration conf,
      String errString) {
    try {
      JournalNode jn = new JournalNode();
      jn.setConf(conf);
      jn.start();
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(errString, e);
    }
  }
  
  /**
   * Simple test of how fast the code path is to write edits.
   * This isn't a true unit test, but can be run manually to
   * check performance.
   * 
   * At the time of development, this test ran in ~4sec on an
   * SSD-enabled laptop (1.8ms/batch).
   */
  @Test(timeout=100000)
  public void testPerformance() throws Exception {
    doPerfTest(8192, 1024); // 8MB
  }
  
  private void doPerfTest(int editsSize, int numEdits) throws Exception {
    byte[] data = new byte[editsSize];
    ch.newEpoch(1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION).get();
    
    Stopwatch sw = new Stopwatch().start();
    for (int i = 1; i < numEdits; i++) {
      ch.sendEdits(1L, i, 1, data).get();
    }
    long time = sw.elapsedMillis();
    
    System.err.println("Wrote " + numEdits + " batches of " + editsSize +
        " bytes in " + time + "ms");
    float avgRtt = (float)time/(float)numEdits;
    long throughput = ((long)numEdits * editsSize * 1000L)/time;
    System.err.println("Time per batch: " + avgRtt + "ms");
    System.err.println("Throughput: " + throughput + " bytes/sec");
  }
}
