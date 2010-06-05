/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test transitions of state across the master.  Sets up the cluster once and
 * then runs a couple of tests.
 */
public class TestMasterTransitions {
  private static final Log LOG = LogFactory.getLog(TestMasterTransitions.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "master_transitions";
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a"),
    Bytes.toBytes("b"), Bytes.toBytes("c")};

  /**
   * Start up a mini cluster and put a small table of many empty regions into it.
   * @throws Exception
   */
  @BeforeClass public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // Parcel out the regions, don't give them out in big lumps.  We've only
    // a few in this test.  Let a couple of cycles pass is more realistic and
    // gives stuff a chance to work.
    TEST_UTIL.getConfiguration().setInt("hbase.regions.percheckin", 2);
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
    // Create a table of three families.  This will assign a region.
    TEST_UTIL.createTable(Bytes.toBytes(TABLENAME), FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    int countOfRegions = TEST_UTIL.createMultiRegions(t, getTestFamily());
    waitUntilAllRegionsAssigned(countOfRegions);
    addToEachStartKey(countOfRegions);
  }

  @AfterClass public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    if (TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size() < 2) {
      // Need at least two servers.
      LOG.info("Started new server=" +
        TEST_UTIL.getHBaseCluster().startRegionServer());
      
    }
  }

  /**
   * Listener for regionserver events testing hbase-2428 (Infinite loop of
   * region closes if META region is offline).  In particular, listen
   * for the close of the 'metaServer' and when it comes in, requeue it with a
   * delay as though there were an issue processing the shutdown.  As part of
   * the requeuing,  send over a close of a region on 'otherServer' so it comes
   * into a master that has its meta region marked as offline.
   */
  static class HBase2428Listener implements RegionServerOperationListener {
    // Map of what we've delayed so we don't do do repeated delays.
    private final Set<RegionServerOperation> postponed =
      new CopyOnWriteArraySet<RegionServerOperation>();
    private boolean done = false;;
    private boolean metaShutdownReceived = false;
    private final HServerAddress metaAddress;
    private final MiniHBaseCluster cluster;
    private final int otherServerIndex;
    private final HRegionInfo hri;
    private int closeCount = 0;
    static final int SERVER_DURATION = 3 * 1000;
    static final int CLOSE_DURATION = 1 * 1000;
 
    HBase2428Listener(final MiniHBaseCluster c, final HServerAddress metaAddress,
        final HRegionInfo closingHRI, final int otherServerIndex) {
      this.cluster = c;
      this.metaAddress = metaAddress;
      this.hri = closingHRI;
      this.otherServerIndex = otherServerIndex;
    }

    @Override
    public boolean process(final RegionServerOperation op) throws IOException {
      // If a regionserver shutdown and its of the meta server, then we want to
      // delay the processing of the shutdown and send off a close of a region on
      // the 'otherServer.
      boolean result = true;
      if (op instanceof ProcessServerShutdown) {
        ProcessServerShutdown pss = (ProcessServerShutdown)op;
        if (pss.getDeadServerAddress().equals(this.metaAddress)) {
          // Don't postpone more than once.
          if (!this.postponed.contains(pss)) {
            // Close some region.
            this.cluster.addMessageToSendRegionServer(this.otherServerIndex,
              new HMsg(HMsg.Type.MSG_REGION_CLOSE, hri,
              Bytes.toBytes("Forcing close in test")));
            this.postponed.add(pss);
            // Put off the processing of the regionserver shutdown processing.
            pss.setDelay(SERVER_DURATION);
            this.metaShutdownReceived = true;
            // Return false.  This will add this op to the delayed queue.
            result = false;
          }
        }
      } else {
        // Have the close run frequently.
        if (isWantedCloseOperation(op) != null) {
          op.setDelay(CLOSE_DURATION);
          // Count how many times it comes through here.
          this.closeCount++;
        }
      }
      return result;
    }

    public void processed(final RegionServerOperation op) {
      if (isWantedCloseOperation(op) != null) return;
      this.done = true;
    }

    /*
     * @param op
     * @return Null if not the wanted ProcessRegionClose, else <code>op</code>
     * cast as a ProcessRegionClose.
     */
    private ProcessRegionClose isWantedCloseOperation(final RegionServerOperation op) {
      // Count every time we get a close operation.
      if (op instanceof ProcessRegionClose) {
        ProcessRegionClose c = (ProcessRegionClose)op;
        if (c.regionInfo.equals(hri)) {
          return c;
        }
      }
      return null;
    }

    boolean isDone() {
      return this.done;
    }

    boolean isMetaShutdownReceived() {
      return metaShutdownReceived;
    }

    int getCloseCount() {
      return this.closeCount;
    }

    @Override
    public boolean process(HServerInfo serverInfo, HMsg incomingMsg) {
      return true;
    }
  }

  /**
   * In 2428, the meta region has just been set offline and then a close comes
   * in.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2428">HBASE-2428</a> 
   */
  @Test (timeout=300000) public void testRegionCloseWhenNoMetaHBase2428()
  throws Exception {
    LOG.info("Running testRegionCloseWhenNoMetaHBase2428");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster master = cluster.getMaster();
    int metaIndex = cluster.getServerWithMeta();
    // Figure the index of the server that is not server the .META.
    int otherServerIndex = -1;
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      if (i == metaIndex) continue;
      otherServerIndex = i;
      break;
    }
    final HRegionServer otherServer = cluster.getRegionServer(otherServerIndex);
    final HRegionServer metaHRS = cluster.getRegionServer(metaIndex);

    // Get a region out on the otherServer.
    final HRegionInfo hri =
      otherServer.getOnlineRegions().iterator().next().getRegionInfo();
 
    // Add our RegionServerOperationsListener
    HBase2428Listener listener = new HBase2428Listener(cluster,
      metaHRS.getHServerInfo().getServerAddress(), hri, otherServerIndex);
    master.getRegionServerOperationQueue().
      registerRegionServerOperationListener(listener);
    try {
      // Now close the server carrying meta.
      cluster.abortRegionServer(metaIndex);

      // First wait on receipt of meta server shutdown message.
      while(!listener.metaShutdownReceived) Threads.sleep(100);
      while(!listener.isDone()) Threads.sleep(10);
      // We should not have retried the close more times than it took for the
      // server shutdown message to exit the delay queue and get processed
      // (Multiple by two to add in some slop in case of GC or something).
      assertTrue(listener.getCloseCount() > 1);
      assertTrue(listener.getCloseCount() <
        ((HBase2428Listener.SERVER_DURATION/HBase2428Listener.CLOSE_DURATION) * 2));

      // Assert the closed region came back online
      assertRegionIsBackOnline(hri);
    } finally {
      master.getRegionServerOperationQueue().
        unregisterRegionServerOperationListener(listener);
    }
  }

  /**
   * Test adding in a new server before old one on same host+port is dead.
   * Make the test more onerous by having the server under test carry the meta.
   * If confusion between old and new, purportedly meta never comes back.  Test
   * that meta gets redeployed.
   */
  @Test (timeout=300000) public void testAddingServerBeforeOldIsDead2413()
  throws IOException {
    LOG.info("Running testAddingServerBeforeOldIsDead2413");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    int count = count();
    int metaIndex = cluster.getServerWithMeta();
    MiniHBaseClusterRegionServer metaHRS =
      (MiniHBaseClusterRegionServer)cluster.getRegionServer(metaIndex);
    int port = metaHRS.getServerInfo().getServerAddress().getPort();
    Configuration c = TEST_UTIL.getConfiguration();
    String oldPort = c.get(HConstants.REGIONSERVER_PORT, "0");
    try {
      LOG.info("KILLED=" + metaHRS);
      metaHRS.kill();
      c.set(HConstants.REGIONSERVER_PORT, Integer.toString(port));
      // Try and start new regionserver.  It might clash with the old
      // regionserver port so keep trying to get past the BindException.
      HRegionServer hrs = null;
      while (true) {
        try {
          hrs = cluster.startRegionServer().getRegionServer();
          break;
        } catch (IOException e) {
          if (e.getCause() != null && e.getCause() instanceof InvocationTargetException) {
            InvocationTargetException ee = (InvocationTargetException)e.getCause();
            if (ee.getCause() != null && ee.getCause() instanceof BindException) {
              LOG.info("BindException; retrying: " + e.toString());
            }
          }
        }
      }
      LOG.info("STARTED=" + hrs);
      // Wait until he's been given at least 3 regions before we go on to try
      // and count rows in table.
      while (hrs.getOnlineRegions().size() < 3) Threads.sleep(100);
      LOG.info(hrs.toString() + " has " + hrs.getOnlineRegions().size() +
        " regions");
      assertEquals(count, count());
    } finally {
      c.set(HConstants.REGIONSERVER_PORT, oldPort);
    }
  }


  /**
   * HBase2482 is about outstanding region openings.  If any are outstanding
   * when a regionserver goes down, then they'll never deploy.  They'll be
   * stuck in the regions-in-transition list for ever.  This listener looks
   * for a region opening HMsg and if its from the server passed on construction,
   * then we kill it.  It also looks out for a close message on the victim
   * server because that signifies start of the fireworks.
   */
  static class HBase2482Listener implements RegionServerOperationListener {
    private final HRegionServer victim;
    private boolean abortSent = false;
    // We closed regions on new server.
    private volatile boolean closed = false;
    // Copy of regions on new server
    private final Collection<HRegion> copyOfOnlineRegions;
    // This is the region that was in transition on the server we aborted. Test
    // passes if this region comes back online successfully.
    private HRegionInfo regionToFind;

    HBase2482Listener(final HRegionServer victim) {
      this.victim = victim;
      // Copy regions currently open on this server so I can notice when
      // there is a close.
      this.copyOfOnlineRegions =
        this.victim.getCopyOfOnlineRegionsSortedBySize().values();
    }
 
    @Override
    public boolean process(HServerInfo serverInfo, HMsg incomingMsg) {
      if (!victim.getServerInfo().equals(serverInfo) ||
          this.abortSent || !this.closed) {
        return true;
      }
      if (!incomingMsg.isType(HMsg.Type.MSG_REPORT_PROCESS_OPEN)) return true;
      // Save the region that is in transition so can test later it came back.
      this.regionToFind = incomingMsg.getRegionInfo();
      LOG.info("ABORTING " + this.victim + " because got a " +
        HMsg.Type.MSG_REPORT_PROCESS_OPEN + " on this server for " +
        incomingMsg.getRegionInfo().getRegionNameAsString());
      this.victim.abort();
      this.abortSent = true;
      return true;
    }

    @Override
    public boolean process(RegionServerOperation op) throws IOException {
      return true;
    }

    @Override
    public void processed(RegionServerOperation op) {
      if (this.closed || !(op instanceof ProcessRegionClose)) return;
      ProcessRegionClose close = (ProcessRegionClose)op;
      for (HRegion r: this.copyOfOnlineRegions) {
        if (r.getRegionInfo().equals(close.regionInfo)) {
          // We've closed one of the regions that was on the victim server.
          // Now can start testing for when all regions are back online again
          LOG.info("Found close of " +
            r.getRegionInfo().getRegionNameAsString() +
            "; setting close happened flag");
          this.closed = true;
          break;
        }
      }
    }
  }

  /**
   * In 2482, a RS with an opening region on it dies.  The said region is then
   * stuck in the master's regions-in-transition and never leaves it.  This
   * test works by bringing up a new regionserver, waiting for the load
   * balancer to give it some regions.  Then, we close all on the new server.
   * After sending all the close messages, we send the new regionserver the
   * special blocking message so it can not process any more messages.
   * Meantime reopening of the just-closed regions is backed up on the new
   * server.  Soon as master gets an opening region from the new regionserver,
   * we kill it.  We then wait on all regions to come back on line.  If bug
   * is fixed, this should happen soon as the processing of the killed server is
   * done.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2482">HBASE-2482</a> 
   */
  @Test (timeout=300000) public void testKillRSWithOpeningRegion2482()
  throws Exception {
    LOG.info("Running testKillRSWithOpeningRegion2482");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    if (cluster.getLiveRegionServerThreads().size() < 2) {
      // Need at least two servers.
      cluster.startRegionServer();
    }
    // Count how many regions are online.  They need to be all back online for
    // this test to succeed.
    int countOfMetaRegions = countOfMetaRegions();
    // Add a listener on the server.
    HMaster m = cluster.getMaster();
    // Start new regionserver.
    MiniHBaseClusterRegionServer hrs =
      (MiniHBaseClusterRegionServer)cluster.startRegionServer().getRegionServer();
    LOG.info("Started new regionserver: " + hrs.toString());
    // Wait until has some regions before proceeding.  Balancer will give it some.
    int minimumRegions =
      countOfMetaRegions/(cluster.getRegionServerThreads().size() * 2);
    while (hrs.getOnlineRegions().size() < minimumRegions) Threads.sleep(100);
    // Set the listener only after some regions have been opened on new server.
    HBase2482Listener listener = new HBase2482Listener(hrs);
    m.getRegionServerOperationQueue().
      registerRegionServerOperationListener(listener);
    try {
      // Go close all non-catalog regions on this new server
      closeAllNonCatalogRegions(cluster, hrs);
      // After all closes, add blocking message before the region opens start to
      // come in.
      cluster.addMessageToSendRegionServer(hrs,
        new HMsg(HMsg.Type.TESTING_MSG_BLOCK_RS));
      // Wait till one of the above close messages has an effect before we start
      // wait on all regions back online.
      while (!listener.closed) Threads.sleep(100);
      LOG.info("Past close");
      // Make sure the abort server message was sent.
      while(!listener.abortSent) Threads.sleep(100);
      LOG.info("Past abort send; waiting on all regions to redeploy");
      // Now wait for regions to come back online.
      assertRegionIsBackOnline(listener.regionToFind);
    } finally {
      m.getRegionServerOperationQueue().
        unregisterRegionServerOperationListener(listener);
    }
  }

  /*
   * @return Count of all non-catalog regions on the designated server
   */
  private int closeAllNonCatalogRegions(final MiniHBaseCluster cluster,
    final MiniHBaseCluster.MiniHBaseClusterRegionServer hrs)
  throws IOException {
    int countOfRegions = 0;
    for (HRegion r: hrs.getOnlineRegions()) {
      if (r.getRegionInfo().isMetaRegion()) continue;
      cluster.addMessageToSendRegionServer(hrs,
        new HMsg(HMsg.Type.MSG_REGION_CLOSE, r.getRegionInfo()));
      LOG.info("Sent close of " + r.getRegionInfo().getRegionNameAsString() +
        " on " + hrs.toString());
      countOfRegions++;
    }
    return countOfRegions;
  }

  private void assertRegionIsBackOnline(final HRegionInfo hri)
  throws IOException {
    // Region should have an entry in its startkey because of addRowToEachRegion.
    byte [] row = getStartKey(hri);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    Get g =  new Get(row);
    assertTrue((t.get(g)).size() > 0);
  }

  /*
   * Wait until all rows in .META. have a non-empty info:server.  This means
   * all regions have been deployed, master has been informed and updated
   * .META. with the regions deployed server.
   * @param countOfRegions How many regions in .META.
   * @throws IOException
   */
  private static void waitUntilAllRegionsAssigned(final int countOfRegions)
  throws IOException {
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
      HConstants.META_TABLE_NAME);
    while (true) {
      int rows = 0;
      Scan scan = new Scan();
      scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      ResultScanner s = meta.getScanner(scan);
      for (Result r = null; (r = s.next()) != null;) {
        byte [] b =
          r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
        if (b == null || b.length <= 0) break;
        rows++;
      }
      s.close();
      // If I get to here and all rows have a Server, then all have been assigned.
      if (rows == countOfRegions) break;
      LOG.info("Found=" + rows);
      Threads.sleep(1000); 
    }
  }

  /*
   * @return Count of regions in meta table.
   * @throws IOException
   */
  private static int countOfMetaRegions()
  throws IOException {
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
      HConstants.META_TABLE_NAME);
    int rows = 0;
    Scan scan = new Scan();
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    ResultScanner s = meta.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      byte [] b =
        r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      if (b == null || b.length <= 0) break;
      rows++;
    }
    s.close();
    return rows;
  }

  /*
   * Add to each of the regions in .META. a value.  Key is the startrow of the
   * region (except its 'aaa' for first region).  Actual value is the row name.
   * @param expected
   * @return
   * @throws IOException
   */
  private static int addToEachStartKey(final int expected) throws IOException {
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
        HConstants.META_TABLE_NAME);
    int rows = 0;
    Scan scan = new Scan();
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    ResultScanner s = meta.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      byte [] b =
        r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      if (b == null || b.length <= 0) break;
      HRegionInfo hri = Writables.getHRegionInfo(b);
      // If start key, add 'aaa'.
      byte [] row = getStartKey(hri);
      Put p = new Put(row);
      p.add(getTestFamily(), getTestQualifier(), row);
      t.put(p);
      rows++;
    }
    s.close();
    Assert.assertEquals(expected, rows);
    return rows;
  }

  /*
   * @return Count of rows in TABLENAME
   * @throws IOException
   */
  private static int count() throws IOException {
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    int rows = 0;
    Scan scan = new Scan();
    ResultScanner s = t.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      rows++;
    }
    s.close();
    LOG.info("Counted=" + rows);
    return rows;
  }

  /*
   * @param hri
   * @return Start key for hri (If start key is '', then return 'aaa'.
   */
  private static byte [] getStartKey(final HRegionInfo hri) {
    return Bytes.equals(HConstants.EMPTY_START_ROW, hri.getStartKey())?
        Bytes.toBytes("aaa"): hri.getStartKey();
  }

  private static byte [] getTestFamily() {
    return FAMILIES[0];
  }

  private static byte [] getTestQualifier() {
    return getTestFamily();
  }
}
