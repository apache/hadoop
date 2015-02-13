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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBookKeeperSpeculativeRead {
  private static final Log LOG = LogFactory
      .getLog(TestBookKeeperSpeculativeRead.class);

  private ZooKeeper zkc;
  private static BKJMUtil bkutil;
  private static int numLocalBookies = 1;
  private static List<BookieServer> bks = new ArrayList<BookieServer>();

  @BeforeClass
  public static void setupBookkeeper() throws Exception {
    bkutil = new BKJMUtil(1);
    bkutil.start();
  }

  @AfterClass
  public static void teardownBookkeeper() throws Exception {
    bkutil.teardown();
    for (BookieServer bk : bks) {
      bk.shutdown();
    }
  }

  @Before
  public void setup() throws Exception {
    zkc = BKJMUtil.connectZooKeeper();
  }

  @After
  public void teardown() throws Exception {
    zkc.close();
  }

  private NamespaceInfo newNSInfo() {
    Random r = new Random();
    return new NamespaceInfo(r.nextInt(), "testCluster", "TestBPID", -1);
  }

  /**
   * Test speculative read feature supported by bookkeeper. Keep one bookie
   * alive and sleep all the other bookies. Non spec client will hang for long
   * time to read the entries from the bookkeeper.
   */
  @Test(timeout = 120000)
  public void testSpeculativeRead() throws Exception {
    // starting 9 more servers
    for (int i = 1; i < 10; i++) {
      bks.add(bkutil.newBookie());
    }
    NamespaceInfo nsi = newNSInfo();
    Configuration conf = new Configuration();
    int ensembleSize = numLocalBookies + 9;
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
        ensembleSize);
    conf.setInt(BookKeeperJournalManager.BKJM_BOOKKEEPER_QUORUM_SIZE,
        ensembleSize);
    conf.setInt(
        BookKeeperJournalManager.BKJM_BOOKKEEPER_SPECULATIVE_READ_TIMEOUT_MS,
        100);
    // sets 60 minute
    conf.setInt(
        BookKeeperJournalManager.BKJM_BOOKKEEPER_READ_ENTRY_TIMEOUT_SEC, 3600);
    BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf,
        BKJMUtil.createJournalURI("/hdfsjournal-specread"), nsi);
    bkjm.format(nsi);

    final long numTransactions = 1000;
    EditLogOutputStream out = bkjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    for (long i = 1; i <= numTransactions; i++) {
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      op.setTransactionId(i);
      out.write(op);
    }
    out.close();
    bkjm.finalizeLogSegment(1, numTransactions);

    List<EditLogInputStream> in = new ArrayList<EditLogInputStream>();
    bkjm.selectInputStreams(in, 1, true);

    // sleep 9 bk servers. Now only one server is running and responding to the
    // clients
    CountDownLatch sleepLatch = new CountDownLatch(1);
    for (final BookieServer bookie : bks) {
      sleepBookie(sleepLatch, bookie);
    }
    try {
      assertEquals(numTransactions,
          FSEditLogTestUtil.countTransactionsInStream(in.get(0)));
    } finally {
      in.get(0).close();
      sleepLatch.countDown();
      bkjm.close();
    }
  }

  /**
   * Sleep a bookie until I count down the latch
   *
   * @param latch
   *          latch to wait on
   * @param bookie
   *          bookie server
   * @throws Exception
   */
  private void sleepBookie(final CountDownLatch latch, final BookieServer bookie)
      throws Exception {

    Thread sleeper = new Thread() {
      public void run() {
        try {
          bookie.suspendProcessing();
          latch.await(2, TimeUnit.MINUTES);
          bookie.resumeProcessing();
        } catch (Exception e) {
          LOG.error("Error suspending bookie", e);
        }
      }
    };
    sleeper.setName("BookieServerSleeper-" + bookie.getBookie().getId());
    sleeper.start();
  }
}
