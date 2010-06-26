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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test issues assigning ROOT.
 */
public class TestROOTAssignment {
  private static final Log LOG = LogFactory.getLog(TestROOTAssignment.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte [] TABLENAME = Bytes.toBytes("root_assignments");
  private static final byte [][] FAMILIES =
    new byte [][] {Bytes.toBytes("family")};

  /**
   * Start up a mini cluster and put a small table of many empty regions into it.
   * @throws Exception
   */
  @BeforeClass public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regions.percheckin", 2);
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
    // Create a table of three families.  This will assign a region.
    TEST_UTIL.createTable(TABLENAME, FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    int countOfRegions = TEST_UTIL.createMultiRegions(t, FAMILIES[0]);
    TEST_UTIL.waitUntilAllRegionsAssigned(countOfRegions);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    TEST_UTIL.loadTable(table, FAMILIES[0]);
    table.close();
  }

  @AfterClass public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  /**
   * Interrupt processing of server shutdown so it gets put on delay queue.
   */
  static class PostponeShutdownProcessing implements RegionServerOperationListener {
    // Map of what we've delayed so we don't do do repeated delays.
    private final Set<RegionServerOperation> postponed =
      new CopyOnWriteArraySet<RegionServerOperation>();
    private boolean done = false;
    private final HServerAddress rootServerAddress;
    private final HMaster master;
 
    PostponeShutdownProcessing(final HMaster master,
        final HServerAddress rootServerAddress) {
      this.master = master;
      this.rootServerAddress = rootServerAddress;
    }

    @Override
    public boolean process(final RegionServerOperation op) throws IOException {
      // If a regionserver shutdown and its of the root server, then we want to
      // delay the processing of the shutdown
      boolean result = true;
      if (op instanceof ProcessServerShutdown) {
        ProcessServerShutdown pss = (ProcessServerShutdown)op;
        if (pss.getDeadServerAddress().equals(this.rootServerAddress)) {
          // Don't postpone more than once.
          if (!this.postponed.contains(pss)) {
            this.postponed.add(pss);
            Assert.assertNull(this.master.getRegionManager().getRootRegionLocation());
            pss.setDelay(1 * 1000);
            // Return false.  This will add this op to the delayed queue.
            result = false;
          }
        }
      }
      return result;
    }

    @Override
    public boolean process(HServerInfo serverInfo, HMsg incomingMsg) {
      return true;
    }

    @Override
    public void processed(RegionServerOperation op) {
      if (op instanceof ProcessServerShutdown) {
        ProcessServerShutdown pss = (ProcessServerShutdown)op;
        if (pss.getDeadServerAddress().equals(this.rootServerAddress)) {
          this.done = true;
        }
      }
    }

    public boolean isDone() {
      return this.done;
    }
  }

  /**
   * If the split of the log for the regionserver hosting ROOT doesn't go off
   * smoothly, if the process server shutdown gets added to the delayed queue
   * of events to process, then ROOT was not being allocated, ever.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2707">HBASE-2707</a> 
   */
  @Test (timeout=300000) public void testROOTDeployedThoughProblemSplittingLog()
  throws Exception {
    LOG.info("Running testROOTDeployedThoughProblemSplittingLog");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster master = cluster.getMaster();
    byte [] rootRegion = Bytes.toBytes("-ROOT-,,0");
    int rootIndex = cluster.getServerWith(rootRegion);
    final HRegionServer rootHRS = cluster.getRegionServer(rootIndex);
 
    // Add our RegionServerOperationsListener
    PostponeShutdownProcessing listener = new PostponeShutdownProcessing(master,
      rootHRS.getHServerInfo().getServerAddress());
    master.getRegionServerOperationQueue().
      registerRegionServerOperationListener(listener);
    try {
      // Now close the server carrying meta.
      cluster.abortRegionServer(rootIndex);

      // Wait for processing of the shutdown server.
      while(!listener.isDone()) Threads.sleep(100);
      master.getRegionManager().waitForRootRegionLocation();
    } finally {
      master.getRegionServerOperationQueue().
        unregisterRegionServerOperationListener(listener);
    }
  }
}
