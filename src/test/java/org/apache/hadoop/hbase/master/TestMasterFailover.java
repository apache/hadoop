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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.Test;

public class TestMasterFailover {
  private static final Log LOG = LogFactory.getLog(TestMasterFailover.class);

  /**
   * Simple test of master failover.
   * <p>
   * Starts with three masters.  Kills a backup master.  Then kills the active
   * master.  Ensures the final master becomes active and we can still contact
   * the cluster.
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testSimpleMasterFailover() throws Exception {

    final int NUM_MASTERS = 3;
    final int NUM_RS = 3;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();

    // wait for each to come online
    for (MasterThread mt : masterThreads) {
      assertTrue(mt.isAlive());
    }

    // verify only one is the active master and we have right number
    int numActive = 0;
    int activeIndex = -1;
    String activeName = null;
    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i).getMaster().isActiveMaster()) {
        numActive++;
        activeIndex = i;
        activeName = masterThreads.get(i).getMaster().getServerName();
      }
    }
    assertEquals(1, numActive);
    assertEquals(NUM_MASTERS, masterThreads.size());

    // attempt to stop one of the inactive masters
    LOG.debug("\n\nStopping a backup master\n");
    int backupIndex = (activeIndex == 0 ? 1 : activeIndex - 1);
    cluster.stopMaster(backupIndex, false);
    cluster.waitOnMaster(backupIndex);

    // verify still one active master and it's the same
    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i).getMaster().isActiveMaster()) {
        assertTrue(activeName.equals(
            masterThreads.get(i).getMaster().getServerName()));
        activeIndex = i;
      }
    }
    assertEquals(1, numActive);
    assertEquals(2, masterThreads.size());

    // kill the active master
    LOG.debug("\n\nStopping the active master\n");
    cluster.stopMaster(activeIndex, false);
    cluster.waitOnMaster(activeIndex);

    // wait for an active master to show up and be ready
    assertTrue(cluster.waitForActiveAndReadyMaster());

    LOG.debug("\n\nVerifying backup master is now active\n");
    // should only have one master now
    assertEquals(1, masterThreads.size());
    // and he should be active
    assertTrue(masterThreads.get(0).getMaster().isActiveMaster());

    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
}
