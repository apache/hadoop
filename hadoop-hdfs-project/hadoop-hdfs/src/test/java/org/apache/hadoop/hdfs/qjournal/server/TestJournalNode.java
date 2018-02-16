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

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StopWatch;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class TestJournalNode {
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L);
  @Rule
  public TestName testName = new TestName();

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
    journalId = "test-journalid-" + GenericTestUtils.uniqueSequenceId();

    if (testName.getMethodName().equals("testJournalDirPerNameSpace")) {
      setFederationConf();
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY+ ".ns1",
          editsDir + File.separator + "ns1");
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY+ ".ns2",
          editsDir + File.separator + "ns2");
    } else if (testName.getMethodName().equals(
        "testJournalCommonDirAcrossNameSpace")){
      setFederationConf();
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
          editsDir.getAbsolutePath());
    } else if (testName.getMethodName().equals(
        "testJournalDefaultDirForOneNameSpace")) {
      FileUtil.fullyDelete(new File(DFSConfigKeys
          .DFS_JOURNALNODE_EDITS_DIR_DEFAULT));
      setFederationConf();
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY+ ".ns1",
          editsDir + File.separator + "ns1");
    } else {
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
          editsDir.getAbsolutePath());
    }
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY,
        "0.0.0.0:0");
    if (testName.getMethodName().equals(
        "testJournalNodeSyncerNotStartWhenSyncDisabled")) {
      conf.setBoolean(DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_KEY,
          false);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
          "qjournal://jn0:9900;jn1:9901/" + journalId);
    } else if (testName.getMethodName().equals(
        "testJournalNodeSyncerNotStartWhenSyncEnabledIncorrectURI")) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
          "qjournal://journal0\\:9900;journal1:9901/" + journalId);
    } else if (testName.getMethodName().equals(
        "testJournalNodeSyncerNotStartWhenSyncEnabled")) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
          "qjournal://jn0:9900;jn1:9901/" + journalId);
    } else if (testName.getMethodName().equals(
        "testJournalNodeSyncwithFederationTypeConfigWithNameServiceId")) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns1",
          "qjournal://journalnode0:9900;journalnode0:9901/" + journalId);
    } else if (testName.getMethodName().equals(
        "testJournalNodeSyncwithFederationTypeConfigWithNamenodeId")) {
      conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns1" +".nn1",
          "qjournal://journalnode0:9900;journalnode1:9901/" +journalId);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns1" +".nn2",
          "qjournal://journalnode0:9900;journalnode1:9901/" +journalId);
    } else if (testName.getMethodName().equals(
        "testJournalNodeSyncwithFederationTypeIncorrectConfigWithNamenodeId")) {
      conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns1" +".nn1",
          "qjournal://journalnode0:9900;journalnode1:9901/" + journalId);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns1" +".nn2",
          "qjournal://journalnode0:9902;journalnode1:9903/" + journalId);
    }
    jn = new JournalNode();
    jn.setConf(conf);
    jn.start();


    if (testName.getMethodName().equals("testJournalDirPerNameSpace") ||
        testName.getMethodName().equals(
            "testJournalCommonDirAcrossNameSpace") ||
        testName.getMethodName().equals(
            "testJournalDefaultDirForOneNameSpace")) {
      Collection<String> nameServiceIds = DFSUtilClient.getNameServiceIds(conf);
      for(String nsId: nameServiceIds) {
        journalId = "test-journalid-" + nsId;
        journal = jn.getOrCreateJournal(journalId, nsId,
            HdfsServerConstants.StartupOption.REGULAR);
        NamespaceInfo fakeNameSpaceInfo = new NamespaceInfo(
            12345, "mycluster", "my-bp"+nsId, 0L);
        journal.format(fakeNameSpaceInfo);
      }
    } else {
      journal = jn.getOrCreateJournal(journalId);
      journal.format(FAKE_NSINFO);
    }

    
    ch = new IPCLoggerChannel(conf, FAKE_NSINFO, journalId, jn.getBoundIpcAddress());
  }

  private void setFederationConf() {
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1, ns2");

    //ns1
    conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns1" +".nn1",
        "qjournal://journalnode0:9900;journalnode1:9901/test-journalid-ns1");
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns1" +".nn2",
        "qjournal://journalnode0:9900;journalnode1:9901/test-journalid-ns1");

    //ns2
    conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns2", "nn3,nn4");
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns2" +".nn3",
        "qjournal://journalnode0:9900;journalnode1:9901/test-journalid-ns2");
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY +".ns2" +".nn4",
        "qjournal://journalnode0:9900;journalnode1:9901/test-journalid-ns2");
  }
  
  @After
  public void teardown() throws Exception {
    jn.stop(0);
  }

  @Test(timeout=100000)
  public void testJournalDirPerNameSpace() {
    Collection<String> nameServiceIds = DFSUtilClient.getNameServiceIds(conf);
    setupStaticHostResolution(2, "journalnode");
    for (String nsId : nameServiceIds) {
      String jid = "test-journalid-" + nsId;
      Journal nsJournal = jn.getJournal(jid);
      JNStorage journalStorage = nsJournal.getStorage();
      File editsDir = new File(MiniDFSCluster.getBaseDirectory() +
          File.separator + "TestJournalNode" + File.separator
          + nsId + File.separator + jid);
      assertEquals(editsDir.toString(), journalStorage.getRoot().toString());
    }
  }

  @Test(timeout=100000)
  public void testJournalCommonDirAcrossNameSpace() {
    Collection<String> nameServiceIds = DFSUtilClient.getNameServiceIds(conf);
    setupStaticHostResolution(2, "journalnode");
    for (String nsId : nameServiceIds) {
      String jid = "test-journalid-" + nsId;
      Journal nsJournal = jn.getJournal(jid);
      JNStorage journalStorage = nsJournal.getStorage();
      File editsDir = new File(MiniDFSCluster.getBaseDirectory() +
          File.separator + "TestJournalNode" + File.separator + jid);
      assertEquals(editsDir.toString(), journalStorage.getRoot().toString());
    }
  }

  @Test(timeout=100000)
  public void testJournalDefaultDirForOneNameSpace() {
    Collection<String> nameServiceIds = DFSUtilClient.getNameServiceIds(conf);
    setupStaticHostResolution(2, "journalnode");
    String jid = "test-journalid-ns1";
    Journal nsJournal = jn.getJournal(jid);
    JNStorage journalStorage = nsJournal.getStorage();
    File editsDir = new File(MiniDFSCluster.getBaseDirectory() +
        File.separator + "TestJournalNode" + File.separator + "ns1" + File
        .separator + jid);
    assertEquals(editsDir.toString(), journalStorage.getRoot().toString());
    jid = "test-journalid-ns2";
    nsJournal = jn.getJournal(jid);
    journalStorage = nsJournal.getStorage();
    editsDir = new File(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_DEFAULT +
        File.separator + jid);
    assertEquals(editsDir.toString(), journalStorage.getRoot().toString());
  }
  @Test(timeout=100000)
  public void testJournal() throws Exception {
    MetricsRecordBuilder metrics = MetricsAsserts.getMetrics(
        journal.getMetrics().getName());
    MetricsAsserts.assertCounter("BatchesWritten", 0L, metrics);
    MetricsAsserts.assertCounter("BatchesWrittenWhileLagging", 0L, metrics);
    MetricsAsserts.assertGauge("CurrentLagTxns", 0L, metrics);
    MetricsAsserts.assertGauge("LastJournalTimestamp", 0L, metrics);

    long beginTimestamp = System.currentTimeMillis();
    IPCLoggerChannel ch = new IPCLoggerChannel(
        conf, FAKE_NSINFO, journalId, jn.getBoundIpcAddress());
    ch.newEpoch(1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION).get();
    ch.sendEdits(1L, 1, 1, "hello".getBytes(Charsets.UTF_8)).get();
    
    metrics = MetricsAsserts.getMetrics(
        journal.getMetrics().getName());
    MetricsAsserts.assertCounter("BatchesWritten", 1L, metrics);
    MetricsAsserts.assertCounter("BatchesWrittenWhileLagging", 0L, metrics);
    MetricsAsserts.assertGauge("CurrentLagTxns", 0L, metrics);
    long lastJournalTimestamp = MetricsAsserts.getLongGauge(
        "LastJournalTimestamp", metrics);
    assertTrue(lastJournalTimestamp > beginTimestamp);
    beginTimestamp = lastJournalTimestamp;

    ch.setCommittedTxId(100L);
    ch.sendEdits(1L, 2, 1, "goodbye".getBytes(Charsets.UTF_8)).get();

    metrics = MetricsAsserts.getMetrics(
        journal.getMetrics().getName());
    MetricsAsserts.assertCounter("BatchesWritten", 2L, metrics);
    MetricsAsserts.assertCounter("BatchesWrittenWhileLagging", 1L, metrics);
    MetricsAsserts.assertGauge("CurrentLagTxns", 98L, metrics);
    lastJournalTimestamp = MetricsAsserts.getLongGauge(
        "LastJournalTimestamp", metrics);
    assertTrue(lastJournalTimestamp > beginTimestamp);

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
            Ints.toByteArray(HdfsServerConstants.NAMENODE_LAYOUT_VERSION),
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
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
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
    
    StopWatch sw = new StopWatch().start();
    for (int i = 1; i < numEdits; i++) {
      ch.sendEdits(1L, i, 1, data).get();
    }
    long time = sw.now(TimeUnit.MILLISECONDS);
    
    System.err.println("Wrote " + numEdits + " batches of " + editsSize +
        " bytes in " + time + "ms");
    float avgRtt = (float)time/(float)numEdits;
    long throughput = ((long)numEdits * editsSize * 1000L)/time;
    System.err.println("Time per batch: " + avgRtt + "ms");
    System.err.println("Throughput: " + throughput + " bytes/sec");
  }

  /**
   * Test case to check if JournalNode exits cleanly when httpserver or rpc
   * server fails to start. Call to JournalNode start should fail with bind
   * exception as the port is in use by the JN started in @Before routine
   */
  @Test
  public void testJournalNodeStartupFailsCleanly() {
    JournalNode jNode = Mockito.spy(new JournalNode());
    try {
      jNode.setConf(conf);
      jNode.start();
      fail("Should throw bind exception");
    } catch (Exception e) {
      GenericTestUtils
          .assertExceptionContains("java.net.BindException: Port in use", e);
    }
    Mockito.verify(jNode).stop(1);
  }

  @Test
  public void testJournalNodeSyncerNotStartWhenSyncDisabled()
      throws IOException {
    //JournalSyncer will not be started, as journalsync is not enabled
    conf.setBoolean(DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_KEY, false);
    jn.getOrCreateJournal(journalId);
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(false,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

    //Trying by passing nameserviceId still journalnodesyncer should not start
    // IstriedJournalSyncerStartWithnsId should also be false
    jn.getOrCreateJournal(journalId, "mycluster");
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(false,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

  }

  @Test
  public void testJournalNodeSyncerNotStartWhenSyncEnabledIncorrectURI()
      throws IOException {
    //JournalSyncer will not be started,
    // as shared edits hostnames are not resolved
    jn.getOrCreateJournal(journalId);
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(false,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

    //Trying by passing nameserviceId, now
    // IstriedJournalSyncerStartWithnsId should be set
    // but journalnode syncer will not be started,
    // as hostnames are not resolved
    jn.getOrCreateJournal(journalId, "mycluster");
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(true,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

  }

  @Test
  public void testJournalNodeSyncerNotStartWhenSyncEnabled()
      throws IOException {
    //JournalSyncer will not be started,
    // as shared edits hostnames are not resolved
    jn.getOrCreateJournal(journalId);
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(false,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

    //Trying by passing nameserviceId and resolve hostnames
    // now IstriedJournalSyncerStartWithnsId should be set
    // and also journalnode syncer will also be started
    setupStaticHostResolution(2, "jn");
    jn.getOrCreateJournal(journalId, "mycluster");
    Assert.assertEquals(true,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(true,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

  }


  @Test
  public void testJournalNodeSyncwithFederationTypeConfigWithNameServiceId()
      throws IOException {
    //JournalSyncer will not be started, as nameserviceId passed is null,
    // but configured shared edits dir is appended with nameserviceId
    setupStaticHostResolution(2, "journalnode");
    jn.getOrCreateJournal(journalId);
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(false,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

    //Trying by passing nameserviceId and resolve hostnames
    // now IstriedJournalSyncerStartWithnsId should be set
    // and also journalnode syncer will also be started

    jn.getOrCreateJournal(journalId, "ns1");
    Assert.assertEquals(true,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(true,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());
  }

  @Test
  public void testJournalNodeSyncwithFederationTypeConfigWithNamenodeId()
      throws IOException {
    //JournalSyncer will not be started, as nameserviceId passed is null,
    // but configured shared edits dir is appended with nameserviceId +
    // namenodeId
    setupStaticHostResolution(2, "journalnode");
    jn.getOrCreateJournal(journalId);
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(false,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

    //Trying by passing nameserviceId and resolve hostnames
    // now IstriedJournalSyncerStartWithnsId should be set
    // and also journalnode syncer will also be started

    jn.getOrCreateJournal(journalId, "ns1");
    Assert.assertEquals(true,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(true,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());
  }

  @Test
  public void
      testJournalNodeSyncwithFederationTypeIncorrectConfigWithNamenodeId()
      throws IOException {
    //JournalSyncer will not be started, as nameserviceId passed is null,
    // but configured shared edits dir is appended with nameserviceId +
    // namenodeId
    setupStaticHostResolution(2, "journalnode");
    jn.getOrCreateJournal(journalId);
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(false,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());

    //Trying by passing nameserviceId and resolve hostnames
    // now IstriedJournalSyncerStartWithnsId should  be set
    // and  journalnode syncer will not  be started
    // as for each nnId, different shared Edits dir value is configured

    jn.getOrCreateJournal(journalId, "ns1");
    Assert.assertEquals(false,
        jn.getJournalSyncerStatus(journalId));
    Assert.assertEquals(true,
        jn.getJournal(journalId).getTriedJournalSyncerStartedwithnsId());
  }


  private void setupStaticHostResolution(int journalNodeCount,
                                         String hostname) {
    for (int i = 0; i < journalNodeCount; i++) {
      NetUtils.addStaticResolution(hostname + i,
          "localhost");
    }
  }

}
