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

import java.net.URI;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FilenameFilter;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.setupEdits;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableList;

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestBookKeeperJournalManager {
  static final Log LOG = LogFactory.getLog(TestBookKeeperJournalManager.class);
  
  private static final long DEFAULT_SEGMENT_SIZE = 1000;
  private static final String zkEnsemble = "localhost:2181";
  final static private int numBookies = 5;

  private static Thread bkthread;
  protected static Configuration conf = new Configuration();
  private ZooKeeper zkc;

  
  static int nextPort = 6000; // next port for additionally created bookies

  private static ZooKeeper connectZooKeeper(String ensemble) 
      throws IOException, KeeperException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
        
    ZooKeeper zkc = new ZooKeeper(zkEnsemble, 3600, new Watcher() {
        public void process(WatchedEvent event) {
          if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            latch.countDown();
          }
        }
      });
    if (!latch.await(3, TimeUnit.SECONDS)) {
      throw new IOException("Zookeeper took too long to connect");
    }
    return zkc;
  }

  private static BookieServer newBookie() throws Exception {
    int port = nextPort++;
    ServerConfiguration bookieConf = new ServerConfiguration();
    bookieConf.setBookiePort(port);
    File tmpdir = File.createTempFile("bookie" + Integer.toString(port) + "_",
                                      "test");
    tmpdir.delete();
    tmpdir.mkdir();

    bookieConf.setZkServers(zkEnsemble);
    bookieConf.setJournalDirName(tmpdir.getPath());
    bookieConf.setLedgerDirNames(new String[] { tmpdir.getPath() });

    BookieServer b = new BookieServer(bookieConf);
    b.start();
    for (int i = 0; i < 10 && !b.isRunning(); i++) {
      Thread.sleep(10000);
    }
    if (!b.isRunning()) {
      throw new IOException("Bookie would not start");
    }
    return b;
  }

  /**
   * Check that a number of bookies are available
   * @param count number of bookies required
   * @param timeout number of seconds to wait for bookies to start
   * @throws IOException if bookies are not started by the time the timeout hits
   */
  private static int checkBookiesUp(int count, int timeout) throws Exception {
    ZooKeeper zkc = connectZooKeeper(zkEnsemble);
    try {
      boolean up = false;
      int mostRecentSize = 0;
      for (int i = 0; i < timeout; i++) {
        try {
          List<String> children = zkc.getChildren("/ledgers/available",
                                                  false);
          mostRecentSize = children.size();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found " + mostRecentSize + " bookies up, "
                      + "waiting for " + count);
            if (LOG.isTraceEnabled()) {
              for (String child : children) {
                LOG.trace(" server: " + child);
              }
            }
          }
          if (mostRecentSize == count) {
            up = true;
            break;
          }
        } catch (KeeperException e) {
          // ignore
        }
        Thread.sleep(1000);
      }
      return mostRecentSize;
    } finally {
      zkc.close();
    }
  }

  @BeforeClass
  public static void setupBookkeeper() throws Exception {
    bkthread = new Thread() {
        public void run() {
          try {
            String[] args = new String[1];
            args[0] = String.valueOf(numBookies);
            LOG.info("Starting bk");
            LocalBookKeeper.main(args);
          } catch (InterruptedException e) {
            // go away quietly
          } catch (Exception e) {
            LOG.error("Error starting local bk", e);
          }
        }
      };
    bkthread.start();
    
    if (!LocalBookKeeper.waitForServerUp(zkEnsemble, 10000)) {
      throw new Exception("Error starting zookeeper/bookkeeper");
    }
    assertEquals("Not all bookies started", 
                 numBookies, checkBookiesUp(numBookies, 10));
  }
  
  @Before
  public void setup() throws Exception {
    zkc = connectZooKeeper(zkEnsemble);
  }

  @After
  public void teardown() throws Exception {
    zkc.close();
  }

  @AfterClass
  public static void teardownBookkeeper() throws Exception {
    if (bkthread != null) {
      bkthread.interrupt();
      bkthread.join();
    }
  }

  @Test
  public void testSimpleWrite() throws Exception {
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-simplewrite"));
    long txid = 1;
    EditLogOutputStream out = bkjm.startLogSegment(1);
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
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-txncount"));
    long txid = 1;
    EditLogOutputStream out = bkjm.startLogSegment(1);
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
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-gaps"));
    long txid = 1;
    for (long i = 0; i < 3; i++) {
      long start = txid;
      EditLogOutputStream out = bkjm.startLogSegment(start);
      for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      out.close();
      bkjm.finalizeLogSegment(start, txid-1);
      assertNotNull(zkc.exists(bkjm.finalizedLedgerZNode(start, txid-1), false));
    }
    zkc.delete(bkjm.finalizedLedgerZNode(DEFAULT_SEGMENT_SIZE+1, DEFAULT_SEGMENT_SIZE*2), -1);
    
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
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-inprogressAtEnd"));
    long txid = 1;
    for (long i = 0; i < 3; i++) {
      long start = txid;
      EditLogOutputStream out = bkjm.startLogSegment(start);
      for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      
      out.close();
      bkjm.finalizeLogSegment(start, (txid-1));
      assertNotNull(zkc.exists(bkjm.finalizedLedgerZNode(start, (txid-1)), false));
    }
    long start = txid;
    EditLogOutputStream out = bkjm.startLogSegment(start);
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
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-restartFrom1"));
    long txid = 1;
    long start = txid;
    EditLogOutputStream out = bkjm.startLogSegment(txid);
    for (long j = 1 ; j <= DEFAULT_SEGMENT_SIZE; j++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(txid++);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(start, (txid-1));
    
    txid = 1;
    try {
      out = bkjm.startLogSegment(txid);
      fail("Shouldn't be able to start another journal from " + txid
          + " when one already exists");
    } catch (Exception ioe) {
      LOG.info("Caught exception as expected", ioe);
    }

    // test border case
    txid = DEFAULT_SEGMENT_SIZE;
    try {
      out = bkjm.startLogSegment(txid);
      fail("Shouldn't be able to start another journal from " + txid
          + " when one already exists");
    } catch (IOException ioe) {
      LOG.info("Caught exception as expected", ioe);
    }

    // open journal continuing from before
    txid = DEFAULT_SEGMENT_SIZE + 1;
    start = txid;
    out = bkjm.startLogSegment(start);
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
    out = bkjm.startLogSegment(txid);
    assertNotNull(out);
  }

  @Test
  public void testTwoWriters() throws Exception {
    long start = 1;
    BookKeeperJournalManager bkjm1 = new BookKeeperJournalManager(conf, 
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-dualWriter"));
    BookKeeperJournalManager bkjm2 = new BookKeeperJournalManager(conf, 
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-dualWriter"));
    
    EditLogOutputStream out1 = bkjm1.startLogSegment(start);
    try {
      EditLogOutputStream out2 = bkjm2.startLogSegment(start);
      fail("Shouldn't have been able to open the second writer");
    } catch (IOException ioe) {
      LOG.info("Caught exception as expected", ioe);
    }
  }

  @Test
  public void testSimpleRead() throws Exception {
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-simpleread"));
    long txid = 1;
    final long numTransactions = 10000;
    EditLogOutputStream out = bkjm.startLogSegment(1);
    for (long i = 1 ; i <= numTransactions; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, numTransactions);

     
    EditLogInputStream in = bkjm.getInputStream(1, true);
    try {
      assertEquals(numTransactions, 
                   FSEditLogTestUtil.countTransactionsInStream(in));
    } finally {
      in.close();
    }
  }

  @Test
  public void testSimpleRecovery() throws Exception {
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, 
        URI.create("bookkeeper://" + zkEnsemble + "/hdfsjournal-simplerecovery"));
    EditLogOutputStream out = bkjm.startLogSegment(1);
    long txid = 1;
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
    BookieServer bookieToFail = newBookie();
    BookieServer replacementBookie = null;

    try {
      int ensembleSize = numBookies + 1;
      assertEquals("New bookie didn't start",
                   ensembleSize, checkBookiesUp(ensembleSize, 10));

      // ensure that the journal manager has to use all bookies,
      // so that a failure will fail the journal manager
      Configuration conf = new Configuration();
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
                  ensembleSize);
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_QUORUM_SIZE,
                  ensembleSize);
      long txid = 1;
      BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
          URI.create("bookkeeper://" + zkEnsemble
                     + "/hdfsjournal-allbookiefailure"));
      EditLogOutputStream out = bkjm.startLogSegment(txid);

      for (long i = 1 ; i <= 3; i++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      out.setReadyToFlush();
      out.flush();
      bookieToFail.shutdown();
      assertEquals("New bookie didn't die",
                   numBookies, checkBookiesUp(numBookies, 10));

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
      replacementBookie = newBookie();

      assertEquals("New bookie didn't start",
                   numBookies+1, checkBookiesUp(numBookies+1, 10));
      out = bkjm.startLogSegment(txid);
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
      bookieToFail.shutdown();

      if (checkBookiesUp(numBookies, 30) != numBookies) {
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
    BookieServer bookieToFail = newBookie();
    BookieServer replacementBookie = null;

    try {
      int ensembleSize = numBookies + 1;
      assertEquals("New bookie didn't start",
                   ensembleSize, checkBookiesUp(ensembleSize, 10));

      // ensure that the journal manager has to use all bookies,
      // so that a failure will fail the journal manager
      Configuration conf = new Configuration();
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
                  ensembleSize);
      conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_QUORUM_SIZE,
                  ensembleSize);
      long txid = 1;
      BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
          URI.create("bookkeeper://" + zkEnsemble
                     + "/hdfsjournal-onebookiefailure"));
      EditLogOutputStream out = bkjm.startLogSegment(txid);
      for (long i = 1 ; i <= 3; i++) {
        FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
        op.setTransactionId(txid++);
        out.write(op);
      }
      out.setReadyToFlush();
      out.flush();

      replacementBookie = newBookie();
      assertEquals("replacement bookie didn't start",
                   ensembleSize+1, checkBookiesUp(ensembleSize+1, 10));
      bookieToFail.shutdown();
      assertEquals("New bookie didn't die",
                   ensembleSize, checkBookiesUp(ensembleSize, 10));

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
      bookieToFail.shutdown();

      if (checkBookiesUp(numBookies, 30) != numBookies) {
        LOG.warn("Not all bookies from this test shut down, expect errors");
      }
    }
  }

}