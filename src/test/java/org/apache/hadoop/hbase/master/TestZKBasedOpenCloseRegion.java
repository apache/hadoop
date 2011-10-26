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


import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventHandler.EventHandlerListener;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.master.handler.TotesHRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test open and close of regions using zk.
 */
public class TestZKBasedOpenCloseRegion {
  private static final Log LOG = LogFactory.getLog(TestZKBasedOpenCloseRegion.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "TestZKBasedOpenCloseRegion";
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a"),
    Bytes.toBytes("b"), Bytes.toBytes("c")};
  private static int countOfRegions;

  @BeforeClass public static void beforeAllTests() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setBoolean("dfs.support.append", true);
    c.setInt("hbase.regionserver.info.port", 0);
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.createTable(Bytes.toBytes(TABLENAME), FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    countOfRegions = TEST_UTIL.createMultiRegions(t, getTestFamily());
    waitUntilAllRegionsAssigned();
    addToEachStartKey(countOfRegions);
  }

  @AfterClass public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    if (TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size() < 2) {
      // Need at least two servers.
      LOG.info("Started new server=" +
        TEST_UTIL.getHBaseCluster().startRegionServer());

    }
    waitUntilAllRegionsAssigned();
  }

  /**
   * Test we reopen a region once closed.
   * @throws Exception
   */
  @Test (timeout=300000) public void testReOpenRegion()
  throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Number of region servers = " +
      cluster.getLiveRegionServerThreads().size());

    int rsIdx = 0;
    HRegionServer regionServer =
      TEST_UTIL.getHBaseCluster().getRegionServer(rsIdx);
    HRegionInfo hri = getNonMetaRegion(regionServer.getOnlineRegions());
    LOG.debug("Asking RS to close region " + hri.getRegionNameAsString());

    AtomicBoolean closeEventProcessed = new AtomicBoolean(false);
    AtomicBoolean reopenEventProcessed = new AtomicBoolean(false);

    EventHandlerListener closeListener =
      new ReopenEventListener(hri.getRegionNameAsString(),
          closeEventProcessed, EventType.RS_ZK_REGION_CLOSED);
    cluster.getMaster().executorService.
      registerListener(EventType.RS_ZK_REGION_CLOSED, closeListener);

    EventHandlerListener openListener =
      new ReopenEventListener(hri.getRegionNameAsString(),
          reopenEventProcessed, EventType.RS_ZK_REGION_OPENED);
    cluster.getMaster().executorService.
      registerListener(EventType.RS_ZK_REGION_OPENED, openListener);

    LOG.info("Unassign " + hri.getRegionNameAsString());
    cluster.getMaster().assignmentManager.unassign(hri);

    while (!closeEventProcessed.get()) {
      Threads.sleep(100);
    }

    while (!reopenEventProcessed.get()) {
      Threads.sleep(100);
    }

    LOG.info("Done with testReOpenRegion");
  }

  private HRegionInfo getNonMetaRegion(final Collection<HRegionInfo> regions) {
    HRegionInfo hri = null;
    for (HRegionInfo i: regions) {
      LOG.info(i.getRegionNameAsString());
      if (!i.isMetaRegion()) {
        hri = i;
        break;
      }
    }
    return hri;
  }

  public static class ReopenEventListener implements EventHandlerListener {
    private static final Log LOG = LogFactory.getLog(ReopenEventListener.class);
    String regionName;
    AtomicBoolean eventProcessed;
    EventType eventType;

    public ReopenEventListener(String regionName,
        AtomicBoolean eventProcessed, EventType eventType) {
      this.regionName = regionName;
      this.eventProcessed = eventProcessed;
      this.eventType = eventType;
    }

    @Override
    public void beforeProcess(EventHandler event) {
      if(event.getEventType() == eventType) {
        LOG.info("Received " + eventType + " and beginning to process it");
      }
    }

    @Override
    public void afterProcess(EventHandler event) {
      LOG.info("afterProcess(" + event + ")");
      if(event.getEventType() == eventType) {
        LOG.info("Finished processing " + eventType);
        String regionName = "";
        if(eventType == EventType.RS_ZK_REGION_OPENED) {
          TotesHRegionInfo hriCarrier = (TotesHRegionInfo)event;
          regionName = hriCarrier.getHRegionInfo().getRegionNameAsString();
        } else if(eventType == EventType.RS_ZK_REGION_CLOSED) {
          TotesHRegionInfo hriCarrier = (TotesHRegionInfo)event;
          regionName = hriCarrier.getHRegionInfo().getRegionNameAsString();
        }
        if(this.regionName.equals(regionName)) {
          eventProcessed.set(true);
        }
        synchronized(eventProcessed) {
          eventProcessed.notifyAll();
        }
      }
    }
  }

  public static class CloseRegionEventListener implements EventHandlerListener {
    private static final Log LOG = LogFactory.getLog(CloseRegionEventListener.class);
    String regionToClose;
    AtomicBoolean closeEventProcessed;

    public CloseRegionEventListener(String regionToClose,
        AtomicBoolean closeEventProcessed) {
      this.regionToClose = regionToClose;
      this.closeEventProcessed = closeEventProcessed;
    }

    @Override
    public void afterProcess(EventHandler event) {
      LOG.info("afterProcess(" + event + ")");
      if(event.getEventType() == EventType.RS_ZK_REGION_CLOSED) {
        LOG.info("Finished processing CLOSE REGION");
        TotesHRegionInfo hriCarrier = (TotesHRegionInfo)event;
        if (regionToClose.equals(hriCarrier.getHRegionInfo().getRegionNameAsString())) {
          LOG.info("Setting closeEventProcessed flag");
          closeEventProcessed.set(true);
        } else {
          LOG.info("Region to close didn't match");
        }
      }
    }

    @Override
    public void beforeProcess(EventHandler event) {
      if(event.getEventType() == EventType.M_RS_CLOSE_REGION) {
        LOG.info("Received CLOSE RPC and beginning to process it");
      }
    }
  }

  /**
   * This test shows how a region won't be able to be assigned to a RS
   * if it's already "processing" it.
   * @throws Exception
   */
  @Test
  public void testRSAlreadyProcessingRegion() throws Exception {
    LOG.info("starting testRSAlreadyProcessingRegion");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    HRegionServer hr0 =
        cluster.getLiveRegionServerThreads().get(0).getRegionServer();
    HRegionServer hr1 =
        cluster.getLiveRegionServerThreads().get(1).getRegionServer();
    HRegionInfo hri = getNonMetaRegion(hr0.getOnlineRegions());

    // fake that hr1 is processing the region
    hr1.getRegionsInTransitionInRS().putIfAbsent(hri.getEncodedNameAsBytes(), true);

    AtomicBoolean reopenEventProcessed = new AtomicBoolean(false);
    EventHandlerListener openListener =
      new ReopenEventListener(hri.getRegionNameAsString(),
          reopenEventProcessed, EventType.RS_ZK_REGION_OPENED);
    cluster.getMaster().executorService.
      registerListener(EventType.RS_ZK_REGION_OPENED, openListener);

    // now ask the master to move the region to hr1, will fail
    TEST_UTIL.getHBaseAdmin().move(hri.getEncodedNameAsBytes(),
        Bytes.toBytes(hr1.getServerName().toString()));

    // make sure the region came back
    assertEquals(hr1.getOnlineRegion(hri.getEncodedNameAsBytes()), null);

    // remove the block and reset the boolean
    hr1.getRegionsInTransitionInRS().remove(hri.getEncodedNameAsBytes());
    reopenEventProcessed.set(false);
    
    // now try moving a region when there is no region in transition.
    hri = getNonMetaRegion(hr1.getOnlineRegions());

    openListener =
      new ReopenEventListener(hri.getRegionNameAsString(),
          reopenEventProcessed, EventType.RS_ZK_REGION_OPENED);

    cluster.getMaster().executorService.
      registerListener(EventType.RS_ZK_REGION_OPENED, openListener);
    
    TEST_UTIL.getHBaseAdmin().move(hri.getEncodedNameAsBytes(),
        Bytes.toBytes(hr0.getServerName().toString()));

    while (!reopenEventProcessed.get()) {
      Threads.sleep(100);
    }

    // make sure the region has moved from the original RS
    assertTrue(hr1.getOnlineRegion(hri.getEncodedNameAsBytes()) == null);

  }

  @Test (timeout=300000) public void testCloseRegion()
  throws Exception {
    LOG.info("Running testCloseRegion");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Number of region servers = " + cluster.getLiveRegionServerThreads().size());

    int rsIdx = 0;
    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(rsIdx);
    HRegionInfo hri = getNonMetaRegion(regionServer.getOnlineRegions());
    LOG.debug("Asking RS to close region " + hri.getRegionNameAsString());

    AtomicBoolean closeEventProcessed = new AtomicBoolean(false);
    EventHandlerListener listener =
      new CloseRegionEventListener(hri.getRegionNameAsString(),
          closeEventProcessed);
    cluster.getMaster().executorService.registerListener(EventType.RS_ZK_REGION_CLOSED, listener);

    cluster.getMaster().assignmentManager.unassign(hri);

    while (!closeEventProcessed.get()) {
      Threads.sleep(100);
    }
    LOG.info("Done with testCloseRegion");
  }

  private static void waitUntilAllRegionsAssigned()
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
        if (b == null || b.length <= 0) {
          break;
        }
        rows++;
      }
      s.close();
      // If I get to here and all rows have a Server, then all have been assigned.
      if (rows >= countOfRegions) {
        break;
      }
      LOG.info("Found=" + rows);
      Threads.sleep(1000);
    }
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
      if (b == null || b.length <= 0) {
        break;
      }
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

  public static void main(String args[]) throws Exception {
    TestZKBasedOpenCloseRegion.beforeAllTests();

    TestZKBasedOpenCloseRegion test = new TestZKBasedOpenCloseRegion();
    test.setup();
    test.testCloseRegion();

    TestZKBasedOpenCloseRegion.afterAllTests();
  }
}
