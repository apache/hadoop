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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test transitions of state across the master.
 */
public class TestMasterTransistions {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "master_transitions";
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a"),
    Bytes.toBytes("b"), Bytes.toBytes("c")};

  /**
   * Start up a mini cluster and put a small table of many empty regions into it.
   * @throws Exception
   */
  @BeforeClass public static void beforeAllTests() throws Exception {
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
    // Create a table of three families.  This will assign a region.
    TEST_UTIL.createTable(Bytes.toBytes(TABLENAME), FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    int countOfRegions = TEST_UTIL.createMultiRegions(t, FAMILIES[0]);
    waitUntilAllRegionsAssigned(countOfRegions);
  }

  @AfterClass public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
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
  }

  /**
   * In 2428, the meta region has just been set offline and then a close comes
   * in.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2428">HBASE-2428</a> 
   */
  @Test public void testRegionCloseWhenNoMetaHBase2428() throws Exception {
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
 
    // Add our ReionServerOperationsListener
    HBase2428Listener listener = new HBase2428Listener(cluster,
      metaHRS.getHServerInfo().getServerAddress(), hri, otherServerIndex);
    master.getRegionServerOperationQueue().
      registerRegionServerOperationListener(listener);
    try {
      // Now close the server carrying index.
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

      assertClosedRegionIsBackOnline(hri);
    } finally {
      master.getRegionServerOperationQueue().
        unregisterRegionServerOperationListener(listener);
    }
  }

  private void assertClosedRegionIsBackOnline(final HRegionInfo hri)
  throws IOException {
    // When we get here, region should be successfully deployed. Assert so.
    // 'aaa' is safe as first row if startkey is EMPTY_BYTE_ARRAY because we
    // loaded with HBaseTestingUtility#createMultiRegions.
    byte [] row = Bytes.equals(HConstants.EMPTY_BYTE_ARRAY, hri.getStartKey())?
      new byte [] {'a', 'a', 'a'}: hri.getStartKey();
    Put p = new Put(row);
    p.add(FAMILIES[0], FAMILIES[0], FAMILIES[0]);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    t.put(p);
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
      // If I got to hear and all rows have a Server, then all have been assigned.
      if (rows == countOfRegions) break;
    }
  }
}
