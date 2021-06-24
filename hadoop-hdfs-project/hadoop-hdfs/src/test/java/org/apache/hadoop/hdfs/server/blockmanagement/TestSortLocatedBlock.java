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
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * This class tests the sorting of located blocks based on
 * multiple states.
 */
public class TestSortLocatedBlock {
  static final Logger LOG = LoggerFactory
      .getLogger(TestSortLocatedBlock.class);

  private static DatanodeManager dm;
  private static final long STALE_INTERVAL = 30 * 1000 * 60;

  @BeforeClass
  public static void setup() throws IOException {
    dm = mockDatanodeManager();
  }

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
  public void testWithMultipleStateDatanodes() {
    LOG.info("Starting test testWithMultipleStateDatanodes");
    long blockID = Long.MIN_VALUE;
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
    dm.sortLocatedBlocks(null, locatedBlocks);

    // get locations after sorting
    LocatedBlock locatedBlock = locatedBlocks.get(0);
    DatanodeInfoWithStorage[] locations = locatedBlock.getLocations();

    // assert location order:
    // live -> stale -> entering_maintenance -> decommissioned
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

  private static DatanodeManager mockDatanodeManager() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY,
        true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        STALE_INTERVAL);
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    BlockManager bm = Mockito.mock(BlockManager.class);
    BlockReportLeaseManager blm = new BlockReportLeaseManager(conf);
    Mockito.when(bm.getBlockReportLeaseManager()).thenReturn(blm);
    return new DatanodeManager(bm, fsn, conf);
  }
}