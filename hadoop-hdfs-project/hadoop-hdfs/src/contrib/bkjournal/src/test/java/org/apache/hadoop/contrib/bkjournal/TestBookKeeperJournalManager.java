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
package org.apache.hadoop.contrib.bkjournal;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestBookKeeperJournalManager {
  static final Log LOG = LogFactory.getLog(TestBookKeeperJournalManager.class);
  
  private static final long DEFAULT_SEGMENT_SIZE = 1000;

  protected static Configuration conf = new Configuration();
  private ZooKeeper zkc;
  private static BKJMUtil bkutil;
  static int numBookies = 3;
  private BookieServer newBookie;

  @BeforeClass
  public static void setupBookkeeper() throws Exception {
    bkutil = new BKJMUtil(numBookies);
    bkutil.start();
  }

  @AfterClass
  public static void teardownBookkeeper() throws Exception {
    bkutil.teardown();
  }

  @Before
  public void setup() throws Exception {
    zkc = BKJMUtil.connectZooKeeper();
  }

  @After
  public void teardown() throws Exception {
    zkc.close();
    if (newBookie != null) {
      newBookie.shutdown();
    }
  }

  private NamespaceInfo newNSInfo() {
    Random r = new Random();
    return new NamespaceInfo(r.nextInt(), "testCluster", "TestBPID", -1);
  }

  @Test
  public void testSimpleWrite() throws Exception {
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-simplewrite"), nsi);
    bkjm.format(nsi);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    for (long i = 1 ; i <= 100; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, 100);
 
    String zkpath = bkjm.finalizedLedgerZNode(1, 100);
    
    assertNotNull(zkc.exists(zkpath, false));
    assertNull(zkc.exists(bkjm.inprogressZNode(1), false));
  }

  @Test
  public void testNumberOfTransactions() throws Exception {
    NamespaceInfo nsi = newNSInfo();

    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-txncount"), nsi);
    bkjm.format(nsi);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    for (long i = 1 ; i <= 100; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, 100);

    long numTrans = bkjm.getNumberOfTransactions(1, true);
    assertEquals(100, numTrans);
  }

  @Test 
  public void testNumberOfTransactionsWithGaps() throws Exception {
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-gaps"), nsi);
    bkjm.format(nsi);

    long txid = 1;
    for (long i = 0; i < 3; i++) {
      long start = txid;
      EditLogOutputStream out = bkjm.startLogSegment(start,
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      out.close();
      bkjm.finalizeLogSegment(start, txid-1);
      assertNotNull(
          zkc.exists(bkjm.finalizedLedgerZNode(start, txid-1), false));
    }
    zkc.delete(bkjm.finalizedLedgerZNode(DEFAULT_SEGMENT_SIZE+1,
                                         DEFAULT_SEGMENT_SIZE*2), -1);
    
    long numTrans = bkjm.getNumberOfTransactions(1, true);
    assertEquals(DEFAULT_SEGMENT_SIZE, numTrans);
    
    try {
      numTrans = bkjm.getNumberOfTransactions(DEFAULT_SEGMENT_SIZE+1, true);
      fail("Should have thrown corruption exception by this point");
    } catch (JournalManager.CorruptionException ce) {
      // if we get here, everything is going good
    }

    numTrans = bkjm.getNumberOfTransactions((DEFAULT_SEGMENT_SIZE*2)+1, true);
    assertEquals(DEFAULT_SEGMENT_SIZE, numTrans);
  }

  @Test
  public void testNumberOfTransactionsWithInprogressAtEnd() throws Exception {
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-inprogressAtEnd"), nsi);
    bkjm.format(nsi);

    long txid = 1;
    for (long i = 0; i < 3; i++) {
      long start = txid;
      EditLogOutputStream out = bkjm.startLogSegment(start,
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      
      out.close();
      bkjm.finalizeLogSegment(start, (txid-1));
      assertNotNull(
          zkc.exists(bkjm.finalizedLedgerZNode(start, (txid-1)), false));
    }
    long start = txid;
    EditLogOutputStream out = bkjm.startLogSegment(start,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE/2; j++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(txid++);
      out.write(op);
    }
    out.setReadyToFlush();
    out.flush();
    out.abort();
    out.close();
    
    long numTrans = bkjm.getNumberOfTransactions(1, true);
    assertEquals((txid-1), numTrans);
  }

  /**
   * Create a bkjm namespace, write a journal from txid 1, close stream.
   * Try to create a new journal from txid 1. Should throw an exception.
   */
  @Test
  public void testWriteRestartFrom1() throws Exception {
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-restartFrom1"), nsi);
    bkjm.format(nsi);

    long txid = 1;
    long start = txid;
    EditLogOutputStream out = bkjm.startLogSegment(txid,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(txid++);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(start, (txid-1));
    
    txid = 1;
    try {
      out = bkjm.startLogSegment(txid,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      fail("Shouldn't be able to start another journal from " + txid
          + " when one already exists");
    } catch (Exception ioe) {
      LOG.info("Caught exception as expected", ioe);
    }

    // test border case
    txid = DEFAULT_SEGMENT_SIZE;
    try {
      out = bkjm.startLogSegment(txid,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      fail("Shouldn't be able to start another journal from " + txid
          + " when one already exists");
    } catch (IOException ioe) {
      LOG.info("Caught exception as expected", ioe);
    }

    // open journal continuing from before
    txid = DEFAULT_SEGMENT_SIZE + 1;
    start = txid;
    out = bkjm.startLogSegment(start,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    assertNotNull(out);

    for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(txid++);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(start, (txid-1));

    // open journal arbitarily far in the future
    txid = DEFAULT_SEGMENT_SIZE * 4;
    out = bkjm.startLogSegment(txid,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    assertNotNull(out);
  }

  @Test
  public void testTwoWriters() throws Exception {
    long start = 1;
    NamespaceInfo nsi = newNSInfo();

    BookKeeperJournalManager bkjm1 = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-dualWriter"), nsi);
    bkjm1.format(nsi);

    BookKeeperJournalManager bkjm2 = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-dualWriter"), nsi);


    EditLogOutputStream out1 = bkjm1.startLogSegment(start,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    try {
      bkjm2.startLogSegment(start,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      fail("Shouldn't have been able to open the second writer");
    } catch (IOException ioe) {
      LOG.info("Caught exception as expected", ioe);
    }finally{
      out1.close();
    }
  }

  @Test
  public void testSimpleRead() throws Exception {
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-simpleread"),
        nsi);
    bkjm.format(nsi);

    final long numTransactions = 10000;
    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);;
    for (long i = 1 ; i <= numTransactions; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, numTransactions);

    List<EditLogInputStream> in = new ArrayList<EditLogInputStream>();
    bkjm.selectInputStreams(in, 1, true);
    try {
      assertEquals(numTransactions, 
                   FSEditLogTestUtil.countTransactionsInStream(in.get(0)));
    } finally {
      in.get(0).close();
    }
  }

  @Test
  public void testSimpleRecovery() throws Exception {
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-simplerecovery"),
        nsi);
    bkjm.format(nsi);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);;
    for (long i = 1 ; i <= 100; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.setReadyToFlush();
    out.flush();

    out.abort();
    out.close();


    assertNull(zkc.exists(bkjm.finalizedLedgerZNode(1, 100), false));
    assertNotNull(zkc.exists(bkjm.inprogressZNode(1), false));

    bkjm.recoverUnfinalizedSegments();

    assertNotNull(zkc.exists(bkjm.finalizedLedgerZNode(1, 100), false));
    assertNull(zkc.exists(bkjm.inprogressZNode(1), false));
  }

  /**
   * Test that if enough bookies fail to prevent an ensemble,
   * writes the bookkeeper will fail. Test that when once again
   * an ensemble is available, it can continue to write.
   */
  @Test
  public void testAllBookieFailure() throws Exception {
    // bookie to fail
    newBookie = bkutil.newBookie();
    BookieServer replacementBookie = null;

    try {
      int ensembleSize = numBookies + 1;
      assertEquals("New bookie didn't start",
                   ensembleSize, bkutil.checkBookiesUp(ensembleSize, 10));

      // ensure that the journal manager has to use all bookies,
      // so that a failure will fail the journal manager
      Configuration conf = new Configuration();
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
                  ensembleSize);
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_QUORUM_SIZE,
                  ensembleSize);
      long txid = 1;
      NamespaceInfo nsi = newNSInfo();
      BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
          BKJMUtil.createJournalURI("/hdfsjournal-allbookiefailure"),
          nsi);
      bkjm.format(nsi);
      EditLogOutputStream out = bkjm.startLogSegment(txid,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);

      for (long i = 1 ; i <= 3; i++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      out.setReadyToFlush();
      out.flush();
      newBookie.shutdown();
      assertEquals("New bookie didn't die",
                   numBookies, bkutil.checkBookiesUp(numBookies, 10));

      try {
        for (long i = 1 ; i <= 3; i++) {
          FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
          op.setTransactionId(txid++);
          out.write(op);
        }
        out.setReadyToFlush();
        out.flush();
        fail("should not get to this stage");
      } catch (IOException ioe) {
        LOG.debug("Error writing to bookkeeper", ioe);
        assertTrue("Invalid exception message",
                   ioe.getMessage().contains("Failed to write to bookkeeper"));
      }
      replacementBookie = bkutil.newBookie();

      assertEquals("New bookie didn't start",
                   numBookies+1, bkutil.checkBookiesUp(numBookies+1, 10));
      bkjm.recoverUnfinalizedSegments();
      out = bkjm.startLogSegment(txid,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      for (long i = 1 ; i <= 3; i++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }

      out.setReadyToFlush();
      out.flush();

    } catch (Exception e) {
      LOG.error("Exception in test", e);
      throw e;
    } finally {
      if (replacementBookie != null) {
        replacementBookie.shutdown();
      }
      newBookie.shutdown();

      if (bkutil.checkBookiesUp(numBookies, 30) != numBookies) {
        LOG.warn("Not all bookies from this test shut down, expect errors");
      }
    }
  }

  /**
   * Test that a BookKeeper JM can continue to work across the
   * failure of a bookie. This should be handled transparently
   * by bookkeeper.
   */
  @Test
  public void testOneBookieFailure() throws Exception {
    newBookie = bkutil.newBookie();
    BookieServer replacementBookie = null;

    try {
      int ensembleSize = numBookies + 1;
      assertEquals("New bookie didn't start",
                   ensembleSize, bkutil.checkBookiesUp(ensembleSize, 10));

      // ensure that the journal manager has to use all bookies,
      // so that a failure will fail the journal manager
      Configuration conf = new Configuration();
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
                  ensembleSize);
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_QUORUM_SIZE,
                  ensembleSize);
      long txid = 1;

      NamespaceInfo nsi = newNSInfo();
      BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
          BKJMUtil.createJournalURI("/hdfsjournal-onebookiefailure"),
          nsi);
      bkjm.format(nsi);

      EditLogOutputStream out = bkjm.startLogSegment(txid,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      for (long i = 1 ; i <= 3; i++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      out.setReadyToFlush();
      out.flush();

      replacementBookie = bkutil.newBookie();
      assertEquals("replacement bookie didn't start",
                   ensembleSize+1, bkutil.checkBookiesUp(ensembleSize+1, 10));
      newBookie.shutdown();
      assertEquals("New bookie didn't die",
                   ensembleSize, bkutil.checkBookiesUp(ensembleSize, 10));

      for (long i = 1 ; i <= 3; i++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      out.setReadyToFlush();
      out.flush();
    } catch (Exception e) {
      LOG.error("Exception in test", e);
      throw e;
    } finally {
      if (replacementBookie != null) {
        replacementBookie.shutdown();
      }
      newBookie.shutdown();

      if (bkutil.checkBookiesUp(numBookies, 30) != numBookies) {
        LOG.warn("Not all bookies from this test shut down, expect errors");
      }
    }
  }
  
  /**
   * If a journal manager has an empty inprogress node, ensure that we throw an
   * error, as this should not be possible, and some third party has corrupted
   * the zookeeper state
   */
  @Test
  public void testEmptyInprogressNode() throws Exception {
    URI uri = BKJMUtil.createJournalURI("/hdfsjournal-emptyInprogress");
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri,
                                                                 nsi);
    bkjm.format(nsi);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);;
    for (long i = 1; i <= 100; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, 100);

    out = bkjm.startLogSegment(101,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    out.close();
    bkjm.close();
    String inprogressZNode = bkjm.inprogressZNode(101);
    zkc.setData(inprogressZNode, new byte[0], -1);

    bkjm = new BookKeeperJournalManager(conf, uri, nsi);
    try {
      bkjm.recoverUnfinalizedSegments();
      fail("Should have failed. There should be no way of creating"
          + " an empty inprogess znode");
    } catch (IOException e) {
      // correct behaviour
      assertTrue("Exception different than expected", e.getMessage().contains(
          "Invalid/Incomplete data in znode"));
    } finally {
      bkjm.close();
    }
  }

  /**
   * If a journal manager has an corrupt inprogress node, ensure that we throw
   * an error, as this should not be possible, and some third party has
   * corrupted the zookeeper state
   */
  @Test
  public void testCorruptInprogressNode() throws Exception {
    URI uri = BKJMUtil.createJournalURI("/hdfsjournal-corruptInprogress");
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri,
                                                                 nsi);
    bkjm.format(nsi);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);;
    for (long i = 1; i <= 100; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, 100);

    out = bkjm.startLogSegment(101,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    out.close();
    bkjm.close();

    String inprogressZNode = bkjm.inprogressZNode(101);
    zkc.setData(inprogressZNode, "WholeLottaJunk".getBytes(), -1);

    bkjm = new BookKeeperJournalManager(conf, uri, nsi);
    try {
      bkjm.recoverUnfinalizedSegments();
      fail("Should have failed. There should be no way of creating"
          + " an empty inprogess znode");
    } catch (IOException e) {
      // correct behaviour
      assertTrue("Exception different than expected", e.getMessage().contains(
          "has no field named"));
    } finally {
      bkjm.close();
    }
  }

  /**
   * Cases can occur where we create a segment but crash before we even have the
   * chance to write the START_SEGMENT op. If this occurs we should warn, but
   * load as normal
   */
  @Test
  public void testEmptyInprogressLedger() throws Exception {
    URI uri = BKJMUtil.createJournalURI("/hdfsjournal-emptyInprogressLedger");
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri,
                                                                 nsi);
    bkjm.format(nsi);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);;
    for (long i = 1; i <= 100; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, 100);

    out = bkjm.startLogSegment(101,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    out.close();
    bkjm.close();

    bkjm = new BookKeeperJournalManager(conf, uri, nsi);
    bkjm.recoverUnfinalizedSegments();
    out = bkjm.startLogSegment(101,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    for (long i = 1; i <= 100; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(101, 200);

    bkjm.close();
  }

  /**
   * Test that if we fail between finalizing an inprogress and deleting the
   * corresponding inprogress znode.
   */
  @Test
  public void testRefinalizeAlreadyFinalizedInprogress() throws Exception {
    URI uri = BKJMUtil
        .createJournalURI("/hdfsjournal-refinalizeInprogressLedger");
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri,
                                                                 nsi);
    bkjm.format(nsi);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);;
    for (long i = 1; i <= 100; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.close();

    String inprogressZNode = bkjm.inprogressZNode(1);
    String finalizedZNode = bkjm.finalizedLedgerZNode(1, 100);
    assertNotNull("inprogress znode doesn't exist", zkc.exists(inprogressZNode,
        null));
    assertNull("finalized znode exists", zkc.exists(finalizedZNode, null));

    byte[] inprogressData = zkc.getData(inprogressZNode, false, null);

    // finalize
    bkjm = new BookKeeperJournalManager(conf, uri, nsi);
    bkjm.recoverUnfinalizedSegments();
    bkjm.close();

    assertNull("inprogress znode exists", zkc.exists(inprogressZNode, null));
    assertNotNull("finalized znode doesn't exist", zkc.exists(finalizedZNode,
        null));

    zkc.create(inprogressZNode, inprogressData, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    // should work fine
    bkjm = new BookKeeperJournalManager(conf, uri, nsi);
    bkjm.recoverUnfinalizedSegments();
    bkjm.close();
  }

  /**
   * Tests that the edit log file meta data reading from ZooKeeper should be
   * able to handle the NoNodeException. bkjm.getInputStream(fromTxId,
   * inProgressOk) should suppress the NoNodeException and continue. HDFS-3441.
   */
  @Test
  public void testEditLogFileNotExistsWhenReadingMetadata() throws Exception {
    URI uri = BKJMUtil.createJournalURI("/hdfsjournal-editlogfile");
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri,
                                                                 nsi);
    bkjm.format(nsi);

    try {
      // start new inprogress log segment with txid=1
      // and write transactions till txid=50
      String zkpath1 = startAndFinalizeLogSegment(bkjm, 1, 50);

      // start new inprogress log segment with txid=51
      // and write transactions till txid=100
      String zkpath2 = startAndFinalizeLogSegment(bkjm, 51, 100);

      // read the metadata from ZK. Here simulating the situation
      // when reading,the edit log metadata can be removed by purger thread.
      ZooKeeper zkspy = spy(BKJMUtil.connectZooKeeper());
      bkjm.setZooKeeper(zkspy);
      Mockito.doThrow(
          new KeeperException.NoNodeException(zkpath2 + " doesn't exists"))
          .when(zkspy).getData(zkpath2, false, null);

      List<EditLogLedgerMetadata> ledgerList = bkjm.getLedgerList(false);
      assertEquals("List contains the metadata of non exists path.", 1,
          ledgerList.size());
      assertEquals("LogLedgerMetadata contains wrong zk paths.", zkpath1,
          ledgerList.get(0).getZkPath());
    } finally {
      bkjm.close();
    }
  }

  private enum ThreadStatus {
    COMPLETED, GOODEXCEPTION, BADEXCEPTION;
  };

  /**
   * Tests that concurrent calls to format will still allow one to succeed.
   */
  @Test
  public void testConcurrentFormat() throws Exception {
    final URI uri = BKJMUtil.createJournalURI("/hdfsjournal-concurrentformat");
    final NamespaceInfo nsi = newNSInfo();

    // populate with data first
    BookKeeperJournalManager bkjm
      = new BookKeeperJournalManager(conf, uri, nsi);
    bkjm.format(nsi);
    for (int i = 1; i < 100*2; i += 2) {
      bkjm.startLogSegment(i, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      bkjm.finalizeLogSegment(i, i+1);
    }
    bkjm.close();

    final int numThreads = 40;
    List<Callable<ThreadStatus>> threads
      = new ArrayList<Callable<ThreadStatus>>();
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);

    for (int i = 0; i < numThreads; i++) {
      threads.add(new Callable<ThreadStatus>() {
          public ThreadStatus call() {
            BookKeeperJournalManager bkjm = null;
            try {
              bkjm = new BookKeeperJournalManager(conf, uri, nsi);
              barrier.await();
              bkjm.format(nsi);
              return ThreadStatus.COMPLETED;
            } catch (IOException ioe) {
              LOG.info("Exception formatting ", ioe);
              return ThreadStatus.GOODEXCEPTION;
            } catch (InterruptedException ie) {
              LOG.error("Interrupted. Something is broken", ie);
              Thread.currentThread().interrupt();
              return ThreadStatus.BADEXCEPTION;
            } catch (Exception e) {
              LOG.error("Some other bad exception", e);
              return ThreadStatus.BADEXCEPTION;
            } finally {
              if (bkjm != null) {
                try {
                  bkjm.close();
                } catch (IOException ioe) {
                  LOG.error("Error closing journal manager", ioe);
                }
              }
            }
          }
        });
    }
    ExecutorService service = Executors.newFixedThreadPool(numThreads);
    List<Future<ThreadStatus>> statuses = service.invokeAll(threads, 60,
                                                      TimeUnit.SECONDS);
    int numCompleted = 0;
    for (Future<ThreadStatus> s : statuses) {
      assertTrue(s.isDone());
      assertTrue("Thread threw invalid exception",
          s.get() == ThreadStatus.COMPLETED
          || s.get() == ThreadStatus.GOODEXCEPTION);
      if (s.get() == ThreadStatus.COMPLETED) {
        numCompleted++;
      }
    }
    LOG.info("Completed " + numCompleted + " formats");
    assertTrue("No thread managed to complete formatting", numCompleted > 0);
  }

  @Test(timeout = 120000)
  public void testDefaultAckQuorum() throws Exception {
    newBookie = bkutil.newBookie();
    int ensembleSize = numBookies + 1;
    int quorumSize = numBookies + 1;
    // ensure that the journal manager has to use all bookies,
    // so that a failure will fail the journal manager
    Configuration conf = new Configuration();
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
        ensembleSize);
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_QUORUM_SIZE,
        quorumSize);
    // sets 2 secs
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ADD_ENTRY_TIMEOUT_SEC,
        2);
    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-onebookiefailure"), nsi);
    bkjm.format(nsi);
    CountDownLatch sleepLatch = new CountDownLatch(1);
    sleepBookie(sleepLatch, newBookie);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    int numTransactions = 100;
    for (long i = 1; i <= numTransactions; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    try {
      out.close();
      bkjm.finalizeLogSegment(1, numTransactions);

      List<EditLogInputStream> in = new ArrayList<EditLogInputStream>();
      bkjm.selectInputStreams(in, 1, true);
      try {
        assertEquals(numTransactions,
            FSEditLogTestUtil.countTransactionsInStream(in.get(0)));
      } finally {
        in.get(0).close();
      }
      fail("Should throw exception as not enough non-faulty bookies available!");
    } catch (IOException ioe) {
      // expected
    }
  }

  /**
   * Test ack quorum feature supported by bookkeeper. Keep ack quorum bookie
   * alive and sleep all the other bookies. Now the client would wait for the
   * acknowledgement from the ack size bookies and after receiving the success
   * response will continue writing. Non ack client will hang long time to add
   * entries.
   */
  @Test(timeout = 120000)
  public void testAckQuorum() throws Exception {
    // slow bookie
    newBookie = bkutil.newBookie();
    // make quorum size and ensemble size same to avoid the interleave writing
    // of the ledger entries
    int ensembleSize = numBookies + 1;
    int quorumSize = numBookies + 1;
    int ackSize = numBookies;
    // ensure that the journal manager has to use all bookies,
    // so that a failure will fail the journal manager
    Configuration conf = new Configuration();
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
        ensembleSize);
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_QUORUM_SIZE,
        quorumSize);
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ACK_QUORUM_SIZE,
        ackSize);
    // sets 60 minutes
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ADD_ENTRY_TIMEOUT_SEC,
        3600);

    NamespaceInfo nsi = newNSInfo();
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-onebookiefailure"), nsi);
    bkjm.format(nsi);
    CountDownLatch sleepLatch = new CountDownLatch(1);
    sleepBookie(sleepLatch, newBookie);

    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    int numTransactions = 100;
    for (long i = 1; i <= numTransactions; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, numTransactions);

    List<EditLogInputStream> in = new ArrayList<EditLogInputStream>();
    bkjm.selectInputStreams(in, 1, true);
    try {
      assertEquals(numTransactions,
          FSEditLogTestUtil.countTransactionsInStream(in.get(0)));
    } finally {
      sleepLatch.countDown();
      in.get(0).close();
      bkjm.close();
    }
  }

  /**
   * Sleep a bookie until I count down the latch
   *
   * @param latch
   *          Latch to wait on
   * @param bookie
   *          bookie server
   * @throws Exception
   */
  private void sleepBookie(final CountDownLatch l, final BookieServer bookie)
      throws Exception {

    Thread sleeper = new Thread() {
      public void run() {
        try {
          bookie.suspendProcessing();
          l.await(60, TimeUnit.SECONDS);
          bookie.resumeProcessing();
        } catch (Exception e) {
          LOG.error("Error suspending bookie", e);
        }
      }
    };
    sleeper.setName("BookieServerSleeper-" + bookie.getBookie().getId());
    sleeper.start();
  }


  private String startAndFinalizeLogSegment(BookKeeperJournalManager bkjm,
      int startTxid, int endTxid) throws IOException, KeeperException,
      InterruptedException {
    EditLogOutputStream out = bkjm.startLogSegment(startTxid,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    for (long i = startTxid; i <= endTxid; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    // finalize the inprogress_1 log segment.
    bkjm.finalizeLogSegment(startTxid, endTxid);
    String zkpath1 = bkjm.finalizedLedgerZNode(startTxid, endTxid);
    assertNotNull(zkc.exists(zkpath1, false));
    assertNull(zkc.exists(bkjm.inprogressZNode(startTxid), false));
    return zkpath1;
  }
}
