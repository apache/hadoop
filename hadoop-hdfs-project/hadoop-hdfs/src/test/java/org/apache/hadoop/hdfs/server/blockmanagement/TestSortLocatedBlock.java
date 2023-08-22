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

package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the sorting of located blocks based on
 * multiple states.
 */
public class TestSortLocatedBlock {

  private static final long STALE_INTERVAL = 30 * 1000 * 60;

  /**
   * Test to verify sorting with multiple state
   * datanodes exists in storage lists.
   *
   * We have the following list of datanodes, and create LocatedBlock.
   * d0 - decommissioned
   * d1 - entering_maintenance
   * d2 - decommissioned
   * d3 - stale
   * d4 - live(in-service)
   *
   * After sorting the expected datanodes list will be:
   * live -> stale -> entering_maintenance -> decommissioned,
   * (d4 -> d3 -> d1 -> d0 -> d2)
   * or
   * (d4 -> d3 -> d1 -> d2 -> d0).
   */
  @Test(timeout = 30000)
  public void testWithStaleDatanodes() throws IOException {
    long blockID = Long.MAX_VALUE;
    int totalDns = 5;
    DatanodeInfo[] locs = new DatanodeInfo[totalDns];

    // create datanodes
    for (int i = 0; i < totalDns; i++) {
      String ip = i + "." + i + "." + i + "." + i;
      locs[i] = DFSTestUtil.getDatanodeInfo(ip);
      locs[i].setLastUpdateMonotonic(Time.monotonicNow());
    }

    // set decommissioned state
    locs[0].setDecommissioned();
    locs[2].setDecommissioned();
    ArrayList<DatanodeInfo> decommissionedNodes = new ArrayList<>();
    decommissionedNodes.add(locs[0]);
    decommissionedNodes.add(locs[2]);

    // set entering_maintenance state
    locs[1].startMaintenance();

    // set stale state
    locs[3].setLastUpdateMonotonic(Time.monotonicNow() -
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT * 1000 - 1);

    ArrayList<LocatedBlock> locatedBlocks = new ArrayList<>();
    locatedBlocks.add(new LocatedBlock(
        new ExtendedBlock("pool", blockID,
        1024L, new Date().getTime()), locs));

    // sort located blocks
    DatanodeManager dm = mockDatanodeManager(true, false);
    dm.sortLocatedBlocks(null, locatedBlocks);

    // get locations after sorting
    LocatedBlock locatedBlock = locatedBlocks.get(0);
    DatanodeInfoWithStorage[] locations = locatedBlock.getLocations();

    // assert location order:
    // live -> stale -> entering_maintenance -> decommissioned
    // (d4 -> d3 -> d1 -> d0 -> d2)
    // or
    // (d4 -> d3 -> d1 -> d2 -> d0).
    // live
    assertEquals(locs[4].getIpAddr(), locations[0].getIpAddr());
    // stale
    assertEquals(locs[3].getIpAddr(), locations[1].getIpAddr());
    // entering_maintenance
    assertEquals(locs[1].getIpAddr(), locations[2].getIpAddr());
    // decommissioned
    assertEquals(true,
        decommissionedNodes.contains(locations[3])
        && decommissionedNodes.contains(locations[4]));
  }

  /**
   * Test to verify sorting with multiple state
   * datanodes exists in storage lists.
   *
   * After sorting the expected datanodes list will be:
   * live -> slow -> stale -> staleAndSlow ->
   * entering_maintenance -> decommissioning -> decommissioned.
   *
   * avoidStaleDataNodesForRead=true && avoidSlowDataNodesForRead=true
   * d6 -> d5 -> d4 -> d3 -> d2 -> d1 -> d0
   */
  @Test(timeout = 30000)
  public void testAviodStaleAndSlowDatanodes() throws IOException {
    DatanodeManager dm = mockDatanodeManager(true, true);
    DatanodeInfo[] locs = mockDatanodes(dm);

    ArrayList<LocatedBlock> locatedBlocks = new ArrayList<>();
    locatedBlocks.add(new LocatedBlock(
        new ExtendedBlock("pool", Long.MAX_VALUE,
            1024L, new Date().getTime()), locs));

    // sort located blocks
    dm.sortLocatedBlocks(null, locatedBlocks);

    // get locations after sorting
    LocatedBlock locatedBlock = locatedBlocks.get(0);
    DatanodeInfoWithStorage[] locations = locatedBlock.getLocations();

    // assert location order:
    // live -> stale -> entering_maintenance -> decommissioning -> decommissioned
    // live
    assertEquals(locs[6].getIpAddr(), locations[0].getIpAddr());
    // slow
    assertEquals(locs[5].getIpAddr(), locations[1].getIpAddr());
    // stale
    assertEquals(locs[4].getIpAddr(), locations[2].getIpAddr());
    // stale and slow
    assertEquals(locs[3].getIpAddr(), locations[3].getIpAddr());
    // entering_maintenance
    assertEquals(locs[2].getIpAddr(), locations[4].getIpAddr());
    // decommissioning
    assertEquals(locs[1].getIpAddr(), locations[5].getIpAddr());
    // decommissioned
    assertEquals(locs[0].getIpAddr(), locations[6].getIpAddr());
  }

  /**
   * Test to verify sorting with multiple state
   * datanodes exists in storage lists.
   *
   * After sorting the expected datanodes list will be:
   * (live <-> slow) -> (stale <-> staleAndSlow) ->
   * entering_maintenance -> decommissioning -> decommissioned.
   *
   * avoidStaleDataNodesForRead=true && avoidSlowDataNodesForRead=false
   * (d6 <-> d5) -> (d4 <-> d3) -> d2 -> d1 -> d0
   */
  @Test(timeout = 30000)
  public void testAviodStaleDatanodes() throws IOException {
    DatanodeManager dm = mockDatanodeManager(true, false);
    DatanodeInfo[] locs = mockDatanodes(dm);

    ArrayList<LocatedBlock> locatedBlocks = new ArrayList<>();
    locatedBlocks.add(new LocatedBlock(
        new ExtendedBlock("pool", Long.MAX_VALUE,
            1024L, new Date().getTime()), locs));

    // sort located blocks
    dm.sortLocatedBlocks(null, locatedBlocks);

    // get locations after sorting
    LocatedBlock locatedBlock = locatedBlocks.get(0);
    DatanodeInfoWithStorage[] locations = locatedBlock.getLocations();

    // assert location order:
    // live -> stale -> entering_maintenance -> decommissioning -> decommissioned.
    // live
    assertTrue((locs[5].getIpAddr() == locations[0].getIpAddr() &&
        locs[6].getIpAddr() == locations[1].getIpAddr()) ||
        (locs[5].getIpAddr() == locations[1].getIpAddr() &&
            locs[6].getIpAddr() == locations[0].getIpAddr()));
    // stale
    assertTrue((locs[4].getIpAddr() == locations[3].getIpAddr() &&
        locs[3].getIpAddr() == locations[2].getIpAddr()) ||
        (locs[4].getIpAddr() == locations[2].getIpAddr() &&
            locs[3].getIpAddr() == locations[3].getIpAddr()));
    // entering_maintenance
    assertEquals(locs[2].getIpAddr(), locations[4].getIpAddr());
    // decommissioning
    assertEquals(locs[1].getIpAddr(), locations[5].getIpAddr());
    // decommissioned
    assertEquals(locs[0].getIpAddr(), locations[6].getIpAddr());
  }

  /**
   * Test to verify sorting with multiple state
   * datanodes exists in storage lists.
   *
   * After sorting the expected datanodes list will be:
   * (live <-> stale) -> (slow <-> staleAndSlow) ->
   * entering_maintenance -> decommissioning -> decommissioned.
   *
   * avoidStaleDataNodesForRead=false && avoidSlowDataNodesForRead=true
   * (d6 -> d4) -> (d5 <-> d3) -> d2 -> d1 -> d0
   */
  @Test(timeout = 30000)
  public void testAviodSlowDatanodes() throws IOException {
    DatanodeManager dm = mockDatanodeManager(false, true);
    DatanodeInfo[] locs = mockDatanodes(dm);

    ArrayList<LocatedBlock> locatedBlocks = new ArrayList<>();
    locatedBlocks.add(new LocatedBlock(
        new ExtendedBlock("pool", Long.MAX_VALUE,
            1024L, new Date().getTime()), locs));

    // sort located blocks
    dm.sortLocatedBlocks(null, locatedBlocks);

    // get locations after sorting
    LocatedBlock locatedBlock = locatedBlocks.get(0);
    DatanodeInfoWithStorage[] locations = locatedBlock.getLocations();

    // assert location order:
    // live -> slow -> entering_maintenance -> decommissioning -> decommissioned.
    // live
    assertTrue((locs[6].getIpAddr() == locations[0].getIpAddr() &&
        locs[4].getIpAddr() == locations[1].getIpAddr()) ||
        (locs[6].getIpAddr() == locations[1].getIpAddr() &&
            locs[4].getIpAddr() == locations[0].getIpAddr()));
    // slow
    assertTrue((locs[5].getIpAddr() == locations[2].getIpAddr() &&
        locs[3].getIpAddr() == locations[3].getIpAddr()) ||
        (locs[5].getIpAddr() == locations[3].getIpAddr() &&
            locs[3].getIpAddr() == locations[2].getIpAddr()));
    // entering_maintenance
    assertEquals(locs[2].getIpAddr(), locations[4].getIpAddr());
    // decommissioning
    assertEquals(locs[1].getIpAddr(), locations[5].getIpAddr());
    // decommissioned
    assertEquals(locs[0].getIpAddr(), locations[6].getIpAddr());
  }

  /**
   * Test to verify sorting with multiple state
   * datanodes exists in storage lists.
   *
   * After sorting the expected datanodes list will be:
   * (live <-> stale <-> slow <-> staleAndSlow) ->
   * entering_maintenance -> decommissioning -> decommissioned.
   *
   * avoidStaleDataNodesForRead=false && avoidSlowDataNodesForRead=false
   * (d6 <-> d5 <-> d4 <-> d3) -> d2 -> d1 -> d0
   */
  @Test(timeout = 30000)
  public void testWithServiceComparator() throws IOException {
    DatanodeManager dm = mockDatanodeManager(false, false);
    DatanodeInfo[] locs = mockDatanodes(dm);

    // mark live/slow/stale datanodes
    ArrayList<DatanodeInfo> list = new ArrayList<>();
    for (DatanodeInfo loc : locs) {
      list.add(loc);
    }

    // generate blocks
    ArrayList<LocatedBlock> locatedBlocks = new ArrayList<>();
    locatedBlocks.add(new LocatedBlock(
        new ExtendedBlock("pool", Long.MAX_VALUE,
            1024L, new Date().getTime()), locs));

    // sort located blocks
    dm.sortLocatedBlocks(null, locatedBlocks);

    // get locations after sorting
    LocatedBlock locatedBlock = locatedBlocks.get(0);
    DatanodeInfoWithStorage[] locations = locatedBlock.getLocations();

    // assert location order:
    // live/slow/stale -> entering_maintenance -> decommissioning -> decommissioned.
    // live/slow/stale
    assertTrue(list.contains(locations[0]) &&
        list.contains(locations[1]) &&
        list.contains(locations[2]) &&
        list.contains(locations[3]));
    // entering_maintenance
    assertEquals(locs[2].getIpAddr(), locations[4].getIpAddr());
    // decommissioning
    assertEquals(locs[1].getIpAddr(), locations[5].getIpAddr());
    // decommissioned
    assertEquals(locs[0].getIpAddr(), locations[6].getIpAddr());
  }

  /**
   * We mock the following list of datanodes, and create LocatedBlock.
   * d0 - decommissioned
   * d1 - decommissioning
   * d2 - entering_maintenance
   * d3 - stale and slow
   * d4 - stale
   * d5 - slow
   * d6 - live(in-service)
   */
  private static DatanodeInfo[] mockDatanodes(DatanodeManager dm) {
    int totalDns = 7;
    DatanodeInfo[] locs = new DatanodeInfo[totalDns];

    // create datanodes
    for (int i = 0; i < totalDns; i++) {
      String ip = i + "." + i + "." + i + "." + i;
      locs[i] = DFSTestUtil.getDatanodeInfo(ip);
      locs[i].setLastUpdateMonotonic(Time.monotonicNow());
    }
    // set decommissioned state
    locs[0].setDecommissioned();
    // set decommissioning state
    locs[1].startDecommission();
    // set entering_maintenance state
    locs[2].startMaintenance();
    // set stale and slow state
    locs[3].setLastUpdateMonotonic(Time.monotonicNow() -
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT * 1000 - 1);
    dm.addSlowPeers(locs[3].getDatanodeUuid());
    // set stale state
    locs[4].setLastUpdateMonotonic(Time.monotonicNow() -
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT * 1000 - 1);
    // set slow state
    dm.addSlowPeers(locs[5].getDatanodeUuid());

    return locs;
  }

  private static DatanodeManager mockDatanodeManager(
      boolean avoidStaleDNForRead, boolean avoidSlowDNForRead)
      throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY,
        avoidStaleDNForRead);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY,
        avoidSlowDNForRead);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        STALE_INTERVAL);
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    BlockManager bm = Mockito.mock(BlockManager.class);
    BlockReportLeaseManager blm = new BlockReportLeaseManager(conf);
    Mockito.when(bm.getBlockReportLeaseManager()).thenReturn(blm);
    return new DatanodeManager(bm, fsn, conf);
  }
}