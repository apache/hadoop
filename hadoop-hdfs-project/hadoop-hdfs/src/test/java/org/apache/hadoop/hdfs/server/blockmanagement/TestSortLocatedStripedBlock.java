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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the sorting of located striped blocks based on
 * decommissioned states.
 */
public class TestSortLocatedStripedBlock {
  static final Logger LOG = LoggerFactory
      .getLogger(TestSortLocatedStripedBlock.class);

  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int cellSize = ecPolicy.getCellSize();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final int groupSize = dataBlocks + parityBlocks;

  static DatanodeManager dm;
  static final long STALE_INTERVAL = 30 * 1000 * 60;

  @BeforeClass
  public static void setup() throws IOException {
    dm = mockDatanodeManager();
  }

  /**
   * Test to verify sorting with multiple decommissioned datanodes exists in
   * storage lists.
   *
   * We have storage list, marked decommissioned internal blocks with a '
   * d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12
   * mapping to indices
   * 0', 1', 2, 3, 4, 5, 6, 7', 8', 0, 1, 7, 8
   *
   * Decommissioned node indices: 0, 1, 7, 8
   *
   * So in the original list nodes d0, d1, d7, d8 are decommissioned state.
   *
   * After sorting the expected block indices list should be,
   * 0, 1, 2, 3, 4, 5, 6, 7, 8, 0', 1', 7', 8'
   *
   * After sorting the expected storage list will be,
   * d9, d10, d2, d3, d4, d5, d6, d11, d12, d0, d1, d7, d8.
   *
   * Note: after sorting block indices will not be in ascending order.
   */
  @Test(timeout = 10000)
  public void testWithMultipleDecommnDatanodes() {
    LOG.info("Starting test testSortWithMultipleDecommnDatanodes");
    int lbsCount = 2; // two located block groups
    List<Integer> decommnNodeIndices = new ArrayList<>();
    decommnNodeIndices.add(0);
    decommnNodeIndices.add(1);
    decommnNodeIndices.add(7);
    decommnNodeIndices.add(8);
    List<Integer> targetNodeIndices = new ArrayList<>();
    targetNodeIndices.addAll(decommnNodeIndices);
    // map contains decommissioned node details in each located strip block
    // which will be used for assertions
    HashMap<Integer, List<String>> decommissionedNodes = new HashMap<>(
        lbsCount * decommnNodeIndices.size());
    List<LocatedBlock> lbs = createLocatedStripedBlocks(lbsCount,
        dataBlocks, parityBlocks, decommnNodeIndices,
        targetNodeIndices, decommissionedNodes);

    // prepare expected block index and token list.
    List<HashMap<DatanodeInfo, Byte>> locToIndexList = new ArrayList<>();
    List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList =
        new ArrayList<>();
    prepareBlockIndexAndTokenList(lbs, locToIndexList, locToTokenList);

    dm.sortLocatedBlocks(null, lbs);

    assertDecommnNodePosition(groupSize, decommissionedNodes, lbs);
    assertBlockIndexAndTokenPosition(lbs, locToIndexList, locToTokenList);
  }

  /**
   * Test to verify sorting with two decommissioned datanodes exists in
   * storage lists for the same block index.
   *
   * We have storage list, marked decommissioned internal blocks with a '
   * d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13
   * mapping to indices
   * 0', 1', 2, 3, 4', 5', 6, 7, 8, 0, 1', 4, 5, 1
   *
   * Decommissioned node indices: 0', 1', 4', 5', 1'
   *
   * Here decommissioned has done twice to the datanode block index 1.
   * So in the original list nodes d0, d1, d4, d5, d10 are decommissioned state.
   *
   * After sorting the expected block indices list will be,
   * 0, 1, 2, 3, 4, 5, 6, 7, 8, 0', 1', 1', 4', 5'
   *
   * After sorting the expected storage list will be,
   * d9, d13, d2, d3, d11, d12, d6, d7, d8, d0, d1, d10, d4, d5.
   *
   * Note: after sorting block indices will not be in ascending order.
   */
  @Test(timeout = 10000)
  public void testTwoDatanodesWithSameBlockIndexAreDecommn() {
    LOG.info("Starting test testTwoDatanodesWithSameBlockIndexAreDecommn");
    int lbsCount = 2; // two located block groups
    List<Integer> decommnNodeIndices = new ArrayList<>();
    decommnNodeIndices.add(0);
    decommnNodeIndices.add(1);
    decommnNodeIndices.add(4);
    decommnNodeIndices.add(5);
    // representing blockIndex 1, later this also decommissioned
    decommnNodeIndices.add(1);

    List<Integer> targetNodeIndices = new ArrayList<>();
    targetNodeIndices.addAll(decommnNodeIndices);
    // map contains decommissioned node details in each located strip block
    // which will be used for assertions
    HashMap<Integer, List<String>> decommissionedNodes = new HashMap<>(
        lbsCount * decommnNodeIndices.size());
    List<LocatedBlock> lbs = createLocatedStripedBlocks(lbsCount,
        dataBlocks, parityBlocks, decommnNodeIndices,
        targetNodeIndices, decommissionedNodes);

    // prepare expected block index and token list.
    List<HashMap<DatanodeInfo, Byte>> locToIndexList = new ArrayList<>();
    List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList =
        new ArrayList<>();
    prepareBlockIndexAndTokenList(lbs, locToIndexList, locToTokenList);

    dm.sortLocatedBlocks(null, lbs);
    assertDecommnNodePosition(groupSize, decommissionedNodes, lbs);
    assertBlockIndexAndTokenPosition(lbs, locToIndexList, locToTokenList);
  }

  /**
   * Test to verify sorting with decommissioned datanodes exists in storage
   * list which is smaller than stripe size.
   *
   * We have storage list, marked decommissioned internal blocks with a '
   * d0, d1, d2, d3, d6, d7, d8, d9, d10, d11
   * mapping to indices
   * 0', 1, 2', 3, 6, 7, 8, 0, 2', 2
   *
   * Decommissioned node indices: 0', 2', 2'
   *
   * Here decommissioned has done twice to the datanode block index 2.
   * So in the original list nodes d0, d2, d10 are decommissioned state.
   *
   * After sorting the expected block indices list should be,
   * 0, 1, 2, 3, 6, 7, 8, 0', 2', 2'
   *
   * After sorting the expected storage list will be,
   * d9, d1, d11, d3, d6, d7, d8, d0, d2, d10.
   *
   * Note: after sorting block indices will not be in ascending order.
   */
  @Test(timeout = 10000)
  public void testSmallerThanOneStripeWithMultpleDecommnNodes()
      throws Exception {
    LOG.info("Starting test testSmallerThanOneStripeWithDecommn");
    int lbsCount = 2; // two located block groups
    List<Integer> decommnNodeIndices = new ArrayList<>();
    decommnNodeIndices.add(0);
    decommnNodeIndices.add(2);
    // representing blockIndex 1, later this also decommissioned
    decommnNodeIndices.add(2);

    List<Integer> targetNodeIndices = new ArrayList<>();
    targetNodeIndices.addAll(decommnNodeIndices);
    // map contains decommissioned node details in each located strip block
    // which will be used for assertions
    HashMap<Integer, List<String>> decommissionedNodes = new HashMap<>(
        lbsCount * decommnNodeIndices.size());
    int dataBlksNum = dataBlocks - 2;
    List<LocatedBlock> lbs = createLocatedStripedBlocks(lbsCount, dataBlksNum,
        parityBlocks, decommnNodeIndices, targetNodeIndices,
        decommissionedNodes);

    // prepare expected block index and token list.
    List<HashMap<DatanodeInfo, Byte>> locToIndexList = new ArrayList<>();
    List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList =
        new ArrayList<>();
    prepareBlockIndexAndTokenList(lbs, locToIndexList, locToTokenList);

    dm.sortLocatedBlocks(null, lbs);

    // After this index all are decommissioned nodes.
    int blkGrpWidth = dataBlksNum + parityBlocks;
    assertDecommnNodePosition(blkGrpWidth, decommissionedNodes, lbs);
    assertBlockIndexAndTokenPosition(lbs, locToIndexList, locToTokenList);
  }

  /**
   * Test to verify sorting with decommissioned datanodes exists in storage
   * list but the corresponding new target datanode doesn't exists.
   *
   * We have storage list, marked decommissioned internal blocks with a '
   * d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11
   * mapping to indices
   * 0', 1', 2', 3, 4', 5', 6, 7, 8, 0, 2, 4
   *
   * Decommissioned node indices: 0', 1', 2', 4', 5'
   *
   * 1 and 5 nodes doesn't exists in the target list. This can happen, the
   * target node block corrupted or lost after the successful decommissioning.
   * So in the original list nodes corresponding to the decommissioned block
   * index 1 and 5 doesn't have any target entries.
   *
   * After sorting the expected block indices list should be,
   * 0, 2, 3, 4, 6, 7, 8, 0', 1', 2', 4', 5'
   *
   * After sorting the expected storage list will be,
   * d9, d10, d3, d11, d6, d7, d8, d0, d1, d2, d4, d5.
   *
   * Note: after sorting block indices will not be in ascending order.
   */
  @Test(timeout = 10000)
  public void testTargetDecommnDatanodeDoesntExists() {
    LOG.info("Starting test testTargetDecommnDatanodeDoesntExists");
    int lbsCount = 2; // two located block groups
    List<Integer> decommnNodeIndices = new ArrayList<>();
    decommnNodeIndices.add(0);
    decommnNodeIndices.add(1);
    decommnNodeIndices.add(2);
    decommnNodeIndices.add(4);
    decommnNodeIndices.add(5);

    List<Integer> targetNodeIndices = new ArrayList<>();
    targetNodeIndices.add(0);
    targetNodeIndices.add(2);
    targetNodeIndices.add(4);
    // 1 and 5 nodes doesn't exists in the target list. One such case is, the
    // target node block corrupted or lost after the successful decommissioning

    // map contains decommissioned node details in each located strip block
    // which will be used for assertions
    HashMap<Integer, List<String>> decommissionedNodes = new HashMap<>(
        lbsCount * decommnNodeIndices.size());
    List<LocatedBlock> lbs = createLocatedStripedBlocks(lbsCount,
        dataBlocks, parityBlocks, decommnNodeIndices,
        targetNodeIndices, decommissionedNodes);

    // prepare expected block index and token list.
    List<HashMap<DatanodeInfo, Byte>> locToIndexList = new ArrayList<>();
    List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList =
        new ArrayList<>();
    prepareBlockIndexAndTokenList(lbs, locToIndexList, locToTokenList);

    dm.sortLocatedBlocks(null, lbs);

    // After this index all are decommissioned nodes. Needs to reconstruct two
    // more block indices.
    int blkGrpWidth = dataBlocks + parityBlocks - 2;
    assertDecommnNodePosition(blkGrpWidth, decommissionedNodes, lbs);
    assertBlockIndexAndTokenPosition(lbs, locToIndexList, locToTokenList);
  }

  /**
   * Test to verify sorting with multiple in-service and decommissioned
   * datanodes exists in storage lists.
   *
   * We have storage list, marked decommissioned internal blocks with a '
   * d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13
   * mapping to indices
   * 0', 1', 2, 3, 4, 5, 6, 7', 8', 0, 1, 7, 8, 1
   *
   * Decommissioned node indices: 0', 1', 7', 8'
   *
   * Additional In-Service node d13 at the end, block index: 1
   *
   * So in the original list nodes d0, d1, d7, d8 are decommissioned state.
   *
   * After sorting the expected block indices list will be,
   * 0, 1, 2, 3, 4, 5, 6, 7, 8, 1, 0', 1', 7', 8'
   *
   * After sorting the expected storage list will be,
   * d9, d10, d2, d3, d4, d5, d6, d11, d12, d13, d0, d1, d7, d8.
   *
   * Note: after sorting block indices will not be in ascending order.
   */
  @Test(timeout = 10000)
  public void testWithMultipleInServiceAndDecommnDatanodes() {
    LOG.info("Starting test testWithMultipleInServiceAndDecommnDatanodes");
    int lbsCount = 2; // two located block groups
    List<Integer> decommnNodeIndices = new ArrayList<>();
    decommnNodeIndices.add(0);
    decommnNodeIndices.add(1);
    decommnNodeIndices.add(7);
    decommnNodeIndices.add(8);
    List<Integer> targetNodeIndices = new ArrayList<>();
    targetNodeIndices.addAll(decommnNodeIndices);

    // at the end add an additional In-Service node to blockIndex=1
    targetNodeIndices.add(1);

    // map contains decommissioned node details in each located strip block
    // which will be used for assertions
    HashMap<Integer, List<String>> decommissionedNodes = new HashMap<>(
        lbsCount * decommnNodeIndices.size());
    List<LocatedBlock> lbs = createLocatedStripedBlocks(lbsCount,
        dataBlocks, parityBlocks, decommnNodeIndices,
        targetNodeIndices, decommissionedNodes);
    List <DatanodeInfo> staleDns = new ArrayList<>();
    for (LocatedBlock lb : lbs) {
      DatanodeInfo[] locations = lb.getLocations();
      DatanodeInfo staleDn = locations[locations.length - 1];
      staleDn
          .setLastUpdateMonotonic(Time.monotonicNow() - (STALE_INTERVAL * 2));
      staleDns.add(staleDn);
    }

    // prepare expected block index and token list.
    List<HashMap<DatanodeInfo, Byte>> locToIndexList = new ArrayList<>();
    List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList =
        new ArrayList<>();
    prepareBlockIndexAndTokenList(lbs, locToIndexList, locToTokenList);

    dm.sortLocatedBlocks(null, lbs);

    assertDecommnNodePosition(groupSize + 1, decommissionedNodes, lbs);
    assertBlockIndexAndTokenPosition(lbs, locToIndexList, locToTokenList);

    for (LocatedBlock lb : lbs) {
      byte[] blockIndices = ((LocatedStripedBlock) lb).getBlockIndices();
      // after sorting stale block index will be placed after normal nodes.
      Assert.assertEquals("Failed to move stale node to bottom!", 1,
          blockIndices[9]);
      DatanodeInfo[] locations = lb.getLocations();
      // After sorting stale node d13 will be placed after normal nodes
      Assert.assertEquals("Failed to move stale dn after normal one!",
          staleDns.remove(0), locations[9]);
    }
  }

  /**
   * Verify that decommissioned/stale nodes must be positioned after normal
   * nodes.
   */
  private void assertDecommnNodePosition(int blkGrpWidth,
      HashMap<Integer, List<String>> decommissionedNodes,
      List<LocatedBlock> lbs) {
    for (int i = 0; i < lbs.size(); i++) { // for each block
      LocatedBlock blk = lbs.get(i);
      DatanodeInfo[] nodes = blk.getLocations();
      List<String> decommissionedNodeList = decommissionedNodes.get(i);

      for (int j = 0; j < nodes.length; j++) { // for each replica
        DatanodeInfo dnInfo = nodes[j];
        LOG.info("Block Locations size={}, locs={}, j=", nodes.length,
            dnInfo.toString(), j);
        if (j < blkGrpWidth) {
          Assert.assertEquals("Node shouldn't be decommissioned",
              AdminStates.NORMAL, dnInfo.getAdminState());
        } else {
          // check against decommissioned list
          Assert.assertTrue(
              "For block " + blk.getBlock() + " decommissioned node " + dnInfo
                  + " is not last node in list: " + j + "th index of "
                  + nodes.length,
              decommissionedNodeList.contains(dnInfo.getXferAddr()));
          Assert.assertEquals("Node should be decommissioned",
              AdminStates.DECOMMISSIONED, dnInfo.getAdminState());
        }
      }
    }
  }

  private List<LocatedBlock> createLocatedStripedBlocks(int blkGrpCount,
      int dataNumBlk, int numParityBlk, List<Integer> decommnNodeIndices,
      List<Integer> targetNodeIndices,
      HashMap<Integer, List<String>> decommissionedNodes) {

    final List<LocatedBlock> lbs = new ArrayList<>(blkGrpCount);
    for (int i = 0; i < blkGrpCount; i++) {
      ArrayList<String> decommNodeInfo = new ArrayList<String>();
      decommissionedNodes.put(new Integer(i), decommNodeInfo);
      List<Integer> dummyDecommnNodeIndices = new ArrayList<>();
      dummyDecommnNodeIndices.addAll(decommnNodeIndices);

      LocatedStripedBlock lsb = createEachLocatedBlock(dataNumBlk, numParityBlk,
          dummyDecommnNodeIndices, targetNodeIndices, decommNodeInfo);
      lbs.add(lsb);
    }
    return lbs;
  }

  private LocatedStripedBlock createEachLocatedBlock(int numDataBlk,
      int numParityBlk, List<Integer> decommnNodeIndices,
      List<Integer> targetNodeIndices, ArrayList<String> decommNodeInfo) {
    final long blockGroupID = Long.MIN_VALUE;
    int totalDns = numDataBlk + numParityBlk + targetNodeIndices.size();
    DatanodeInfo[] locs = new DatanodeInfo[totalDns];
    String[] storageIDs = new String[totalDns];
    StorageType[] storageTypes = new StorageType[totalDns];
    byte[] blkIndices = new byte[totalDns];

    // Adding data blocks
    int index = 0;
    for (; index < numDataBlk; index++) {
      blkIndices[index] = (byte) index;
      // Location port always equal to logical index of a block,
      // for easier verification
      locs[index] = DFSTestUtil.getLocalDatanodeInfo(blkIndices[index]);
      locs[index].setLastUpdateMonotonic(Time.monotonicNow());
      storageIDs[index] = locs[index].getDatanodeUuid();
      storageTypes[index] = StorageType.DISK;
      // set decommissioned state
      if (decommnNodeIndices.contains(index)) {
        locs[index].setDecommissioned();
        decommNodeInfo.add(locs[index].toString());
        // Removing it from the list to ensure that all the given nodes are
        // successfully marked as decommissioned.
        decommnNodeIndices.remove(new Integer(index));
      }
    }
    // Adding parity blocks after data blocks
    index = dataBlocks;
    for (int j = numDataBlk; j < numDataBlk + numParityBlk; j++, index++) {
      blkIndices[j] = (byte) index;
      // Location port always equal to logical index of a block,
      // for easier verification
      locs[j] = DFSTestUtil.getLocalDatanodeInfo(blkIndices[j]);
      locs[j].setLastUpdateMonotonic(Time.monotonicNow());
      storageIDs[j] = locs[j].getDatanodeUuid();
      storageTypes[j] = StorageType.DISK;
      // set decommissioned state
      if (decommnNodeIndices.contains(index)) {
        locs[j].setDecommissioned();
        decommNodeInfo.add(locs[j].toString());
        // Removing it from the list to ensure that all the given nodes are
        // successfully marked as decommissioned.
        decommnNodeIndices.remove(new Integer(index));
      }
    }
    // Add extra target nodes to storage list after the parity blocks
    int basePortValue = dataBlocks + parityBlocks;
    index = numDataBlk + numParityBlk;
    for (int i = 0; i < targetNodeIndices.size(); i++, index++) {
      int blkIndexPos = targetNodeIndices.get(i);
      blkIndices[index] = (byte) blkIndexPos;
      // Location port always equal to logical index of a block,
      // for easier verification
      locs[index] = DFSTestUtil.getLocalDatanodeInfo(basePortValue++);
      locs[index].setLastUpdateMonotonic(Time.monotonicNow());
      storageIDs[index] = locs[index].getDatanodeUuid();
      storageTypes[index] = StorageType.DISK;
      // set decommissioned state. This can happen, the target node is again
      // decommissioned by administrator
      if (decommnNodeIndices.contains(blkIndexPos)) {
        locs[index].setDecommissioned();
        decommNodeInfo.add(locs[index].toString());
        // Removing it from the list to ensure that all the given nodes are
        // successfully marked as decommissioned.
        decommnNodeIndices.remove(new Integer(blkIndexPos));
      }
    }
    return new LocatedStripedBlock(
        new ExtendedBlock("pool", blockGroupID,
            cellSize, 1001),
        locs, storageIDs, storageTypes, blkIndices, 0, false, null);
  }

  private static DatanodeManager mockDatanodeManager() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        STALE_INTERVAL);
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    BlockManager bm = Mockito.mock(BlockManager.class);
    BlockReportLeaseManager blm = new BlockReportLeaseManager(conf);
    Mockito.when(bm.getBlockReportLeaseManager()).thenReturn(blm);
    DatanodeManager dm = new DatanodeManager(bm, fsn, conf);
    return dm;
  }

  private void prepareBlockIndexAndTokenList(List<LocatedBlock> lbs,
      List<HashMap<DatanodeInfo, Byte>> locToIndexList,
      List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList) {
    for (LocatedBlock lb : lbs) {
      HashMap<DatanodeInfo, Byte> locToIndex = new HashMap<DatanodeInfo, Byte>();
      locToIndexList.add(locToIndex);

      HashMap<DatanodeInfo, Token<BlockTokenIdentifier>> locToToken =
          new HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>();
      locToTokenList.add(locToToken);

      DatanodeInfo[] di = lb.getLocations();
      LocatedStripedBlock stripedBlk = (LocatedStripedBlock) lb;
      for (int i = 0; i < di.length; i++) {
        locToIndex.put(di[i], stripedBlk.getBlockIndices()[i]);
        locToToken.put(di[i], stripedBlk.getBlockTokens()[i]);
      }
    }
  }

  /**
   * Verify block index and token values. Must update block indices and block
   * tokens after sorting.
   */
  private void assertBlockIndexAndTokenPosition(List<LocatedBlock> lbs,
      List<HashMap<DatanodeInfo, Byte>> locToIndexList,
      List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList) {
    for (int i = 0; i < lbs.size(); i++) {
      LocatedBlock lb = lbs.get(i);
      LocatedStripedBlock stripedBlk = (LocatedStripedBlock) lb;
      HashMap<DatanodeInfo, Byte> locToIndex = locToIndexList.get(i);
      HashMap<DatanodeInfo, Token<BlockTokenIdentifier>> locToToken =
          locToTokenList.get(i);
      DatanodeInfo[] di = lb.getLocations();
      for (int j = 0; j < di.length; j++) {
        Assert.assertEquals("Block index value mismatches after sorting",
            (byte) locToIndex.get(di[j]), stripedBlk.getBlockIndices()[j]);
        Assert.assertEquals("Block token value mismatches after sorting",
            locToToken.get(di[j]), stripedBlk.getBlockTokens()[j]);
      }
    }
  }
}