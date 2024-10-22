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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Test if FSNamesystem handles heartbeat right
 */
public class TestHeartbeatHandling {


  /**
   * Set a timeout for every test case.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300_000);

  /**
   * Test if
   * {@link FSNamesystem#handleHeartbeat}
   * can pick up replication and/or invalidate requests and observes the max
   * limit
   */
  @Test
  public void testHeartbeat() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final HeartbeatManager hm = namesystem.getBlockManager(
          ).getDatanodeManager().getHeartbeatManager();
      final String poolId = namesystem.getBlockPoolId();
      final DatanodeRegistration nodeReg =
        InternalDataNodeTestUtils.
        getDNRegistrationForBP(cluster.getDataNodes().get(0), poolId);
      final DatanodeDescriptor dd = NameNodeAdapter.getDatanode(namesystem, nodeReg);
      final String storageID = DatanodeStorage.generateUuid();
      dd.updateStorage(new DatanodeStorage(storageID));

      final int REMAINING_BLOCKS = 1;
      final int MAX_REPLICATE_LIMIT =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 2);
      final int MAX_INVALIDATE_LIMIT = DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT;
      final int MAX_INVALIDATE_BLOCKS = 2*MAX_INVALIDATE_LIMIT+REMAINING_BLOCKS;
      final int MAX_REPLICATE_BLOCKS = 2*MAX_REPLICATE_LIMIT+REMAINING_BLOCKS;
      final DatanodeStorageInfo[] ONE_TARGET = {dd.getStorageInfo(storageID)};

      try {
        namesystem.writeLock();
        synchronized(hm) {
          for (int i=0; i<MAX_REPLICATE_BLOCKS; i++) {
            dd.addBlockToBeReplicated(
                new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP),
                ONE_TARGET);
          }
          DatanodeCommand[] cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd,
              namesystem).getCommands();
          assertEquals(1, cmds.length);
          assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
          assertEquals(MAX_REPLICATE_LIMIT, ((BlockCommand)cmds[0]).getBlocks().length);

          ArrayList<Block> blockList = new ArrayList<Block>(MAX_INVALIDATE_BLOCKS);
          for (int i=0; i<MAX_INVALIDATE_BLOCKS; i++) {
            blockList.add(new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP));
          }
          dd.addBlocksToBeInvalidated(blockList);
          cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
              .getCommands();
          assertEquals(2, cmds.length);
          assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
          assertEquals(MAX_REPLICATE_LIMIT, ((BlockCommand)cmds[0]).getBlocks().length);
          assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[1].getAction());
          assertEquals(MAX_INVALIDATE_LIMIT, ((BlockCommand)cmds[1]).getBlocks().length);
          
          cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
              .getCommands();
          assertEquals(2, cmds.length);
          assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
          assertEquals(REMAINING_BLOCKS, ((BlockCommand)cmds[0]).getBlocks().length);
          assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[1].getAction());
          assertEquals(MAX_INVALIDATE_LIMIT, ((BlockCommand)cmds[1]).getBlocks().length);
          
          cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
              .getCommands();
          assertEquals(1, cmds.length);
          assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[0].getAction());
          assertEquals(REMAINING_BLOCKS, ((BlockCommand)cmds[0]).getBlocks().length);

          cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
              .getCommands();
          assertEquals(0, cmds.length);
        }
      } finally {
        namesystem.writeUnlock();
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test if
   * {@link FSNamesystem#handleHeartbeat}
   * correctly selects data node targets for block recovery.
   */
  @Test
  public void testHeartbeatBlockRecovery() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final HeartbeatManager hm = namesystem.getBlockManager(
          ).getDatanodeManager().getHeartbeatManager();
      final String poolId = namesystem.getBlockPoolId();
      final DatanodeRegistration nodeReg1 =
        InternalDataNodeTestUtils.
        getDNRegistrationForBP(cluster.getDataNodes().get(0), poolId);
      final DatanodeDescriptor dd1 = NameNodeAdapter.getDatanode(namesystem, nodeReg1);
      dd1.updateStorage(new DatanodeStorage(DatanodeStorage.generateUuid()));
      final DatanodeRegistration nodeReg2 =
        InternalDataNodeTestUtils.
        getDNRegistrationForBP(cluster.getDataNodes().get(1), poolId);
      final DatanodeDescriptor dd2 = NameNodeAdapter.getDatanode(namesystem, nodeReg2);
      dd2.updateStorage(new DatanodeStorage(DatanodeStorage.generateUuid()));
      final DatanodeRegistration nodeReg3 = 
        InternalDataNodeTestUtils.
        getDNRegistrationForBP(cluster.getDataNodes().get(2), poolId);
      final DatanodeDescriptor dd3 = NameNodeAdapter.getDatanode(namesystem, nodeReg3);
      dd3.updateStorage(new DatanodeStorage(DatanodeStorage.generateUuid()));

      try {
        namesystem.writeLock();
        synchronized(hm) {
          NameNodeAdapter.sendHeartBeat(nodeReg1, dd1, namesystem);
          NameNodeAdapter.sendHeartBeat(nodeReg2, dd2, namesystem);
          NameNodeAdapter.sendHeartBeat(nodeReg3, dd3, namesystem);

          // Test with all alive nodes.
          DFSTestUtil.resetLastUpdatesWithOffset(dd1, 0);
          DFSTestUtil.resetLastUpdatesWithOffset(dd2, 0);
          DFSTestUtil.resetLastUpdatesWithOffset(dd3, 0);
          final DatanodeStorageInfo[] storages = {
              dd1.getStorageInfos()[0],
              dd2.getStorageInfos()[0],
              dd3.getStorageInfos()[0]};
          BlockInfo blockInfo = new BlockInfoContiguous(
              new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP), (short) 3);
          blockInfo.convertToBlockUnderConstruction(BlockUCState.UNDER_RECOVERY,
              storages);
          dd1.addBlockToBeRecovered(blockInfo);
          DatanodeCommand[] cmds =
              NameNodeAdapter.sendHeartBeat(nodeReg1, dd1, namesystem).getCommands();
          assertEquals(1, cmds.length);
          assertEquals(DatanodeProtocol.DNA_RECOVERBLOCK, cmds[0].getAction());
          BlockRecoveryCommand recoveryCommand = (BlockRecoveryCommand)cmds[0];
          assertEquals(1, recoveryCommand.getRecoveringBlocks().size());
          DatanodeInfo[] recoveringNodes = recoveryCommand.getRecoveringBlocks()
              .toArray(new BlockRecoveryCommand.RecoveringBlock[0])[0].getLocations();
          assertEquals(3, recoveringNodes.length);
          assertEquals(recoveringNodes[0], dd1);
          assertEquals(recoveringNodes[1], dd2);
          assertEquals(recoveringNodes[2], dd3);

          // Test with one stale node.
          DFSTestUtil.resetLastUpdatesWithOffset(dd1, 0);
          // More than the default stale interval of 30 seconds.
          DFSTestUtil.resetLastUpdatesWithOffset(dd2, -40 * 1000);
          DFSTestUtil.resetLastUpdatesWithOffset(dd3, 0);
          blockInfo = new BlockInfoContiguous(
              new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP), (short) 3);
          blockInfo.convertToBlockUnderConstruction(BlockUCState.UNDER_RECOVERY,
              storages);
          dd1.addBlockToBeRecovered(blockInfo);
          cmds = NameNodeAdapter.sendHeartBeat(nodeReg1, dd1, namesystem).getCommands();
          assertEquals(1, cmds.length);
          assertEquals(DatanodeProtocol.DNA_RECOVERBLOCK, cmds[0].getAction());
          recoveryCommand = (BlockRecoveryCommand)cmds[0];
          assertEquals(1, recoveryCommand.getRecoveringBlocks().size());
          recoveringNodes = recoveryCommand.getRecoveringBlocks()
              .toArray(new BlockRecoveryCommand.RecoveringBlock[0])[0].getLocations();
          assertEquals(2, recoveringNodes.length);
          // dd2 is skipped.
          assertEquals(recoveringNodes[0], dd1);
          assertEquals(recoveringNodes[1], dd3);

          // Test with all stale node.
          DFSTestUtil.resetLastUpdatesWithOffset(dd1, - 60 * 1000);
          // More than the default stale interval of 30 seconds.
          DFSTestUtil.resetLastUpdatesWithOffset(dd2, - 40 * 1000);
          DFSTestUtil.resetLastUpdatesWithOffset(dd3, - 80 * 1000);
          blockInfo = new BlockInfoContiguous(
              new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP), (short) 3);
          blockInfo.convertToBlockUnderConstruction(BlockUCState.UNDER_RECOVERY,
              storages);
          dd1.addBlockToBeRecovered(blockInfo);
          cmds = NameNodeAdapter.sendHeartBeat(nodeReg1, dd1, namesystem).getCommands();
          assertEquals(1, cmds.length);
          assertEquals(DatanodeProtocol.DNA_RECOVERBLOCK, cmds[0].getAction());
          recoveryCommand = (BlockRecoveryCommand)cmds[0];
          assertEquals(1, recoveryCommand.getRecoveringBlocks().size());
          recoveringNodes = recoveryCommand.getRecoveringBlocks()
              .toArray(new BlockRecoveryCommand.RecoveringBlock[0])[0].getLocations();
          // Only dd1 is included since it heart beated and hence its not stale
          // when the list of recovery blocks is constructed.
          assertEquals(3, recoveringNodes.length);
          assertEquals(recoveringNodes[0], dd1);
          assertEquals(recoveringNodes[1], dd2);
          assertEquals(recoveringNodes[2], dd3);
        }
      } finally {
        namesystem.writeUnlock();
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testHeartbeatStopWatch() throws Exception {
    Namesystem ns = Mockito.mock(Namesystem.class);
    Configuration conf = new Configuration();
    long recheck = 2000;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, recheck);
    BlockManager bm = new BlockManager(ns, false, conf);
    HeartbeatManager monitor = new HeartbeatManager(ns, bm, conf);
    monitor.restartHeartbeatStopWatch();
    assertFalse(monitor.shouldAbortHeartbeatCheck(0));
    // sleep shorter than recheck and verify shouldn't abort
    Thread.sleep(100);
    assertFalse(monitor.shouldAbortHeartbeatCheck(0));
    // sleep longer than recheck and verify should abort unless ignore delay
    Thread.sleep(recheck);
    assertTrue(monitor.shouldAbortHeartbeatCheck(0));
    assertFalse(monitor.shouldAbortHeartbeatCheck(-recheck * 3));
    // ensure it resets properly
    monitor.restartHeartbeatStopWatch();
    assertFalse(monitor.shouldAbortHeartbeatCheck(0));
  }
}
