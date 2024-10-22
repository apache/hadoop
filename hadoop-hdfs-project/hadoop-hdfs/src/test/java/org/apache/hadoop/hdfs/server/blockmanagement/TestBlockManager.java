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

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.LinkedListMultimap;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.TestProvidedImpl;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.TestINodeFile;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;
import org.slf4j.event.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestBlockManager {
  private DatanodeStorageInfo[] storages;
  private List<DatanodeDescriptor> nodes;
  private List<DatanodeDescriptor> rackA;
  private List<DatanodeDescriptor> rackB;

  /**
   * Some of these tests exercise code which has some randomness involved -
   * ie even if there's a bug, they may pass because the random node selection
   * chooses the correct result.
   * 
   * Since they're true unit tests and run quickly, we loop them a number
   * of times trying to trigger the incorrect behavior.
   */
  private static final int NUM_TEST_ITERS = 30;
  private static final int BLOCK_SIZE = 64*1024;
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TestBlockManager.class);

  private FSNamesystem fsn;
  private BlockManager bm;
  private long mockINodeId;


  @Before
  public void setupMockCluster() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
             "need to set a dummy value here so it assumes a multi-rack cluster");
    fsn = Mockito.mock(FSNamesystem.class);
    Mockito.doReturn(true).when(fsn).hasWriteLock();
    Mockito.doReturn(true).when(fsn).hasReadLock();
    Mockito.doReturn(true).when(fsn).isRunning();
    //Make shouldPopulaeReplQueues return true
    HAContext haContext = Mockito.mock(HAContext.class);
    HAState haState = Mockito.mock(HAState.class);
    Mockito.when(haContext.getState()).thenReturn(haState);
    Mockito.when(haState.shouldPopulateReplQueues()).thenReturn(true);
    Mockito.when(fsn.getHAContext()).thenReturn(haContext);
    bm = new BlockManager(fsn, false, conf);
    CacheManager cm = Mockito.mock(CacheManager.class);
    Mockito.doReturn(cm).when(fsn).getCacheManager();
    GSet<CachedBlock, CachedBlock> cb =
        new LightWeightGSet<CachedBlock, CachedBlock>(1);
    Mockito.when(cm.getCachedBlocks()).thenReturn(cb);

    final String[] racks = {
        "/rackA",
        "/rackA",
        "/rackA",
        "/rackB",
        "/rackB",
        "/rackB"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    nodes = Arrays.asList(DFSTestUtil.toDatanodeDescriptor(storages));
    rackA = nodes.subList(0, 3);
    rackB = nodes.subList(3, 6);
    mockINodeId = INodeId.ROOT_INODE_ID + 1;
  }

  private void addNodes(Iterable<DatanodeDescriptor> nodesToAdd) {
    NetworkTopology cluster = bm.getDatanodeManager().getNetworkTopology();
    // construct network topology
    for (DatanodeDescriptor dn : nodesToAdd) {
      cluster.add(dn);
      dn.getStorageInfos()[0].setUtilizationForTesting(
          2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L);
      dn.updateHeartbeat(
          BlockManagerTestUtil.getStorageReportsForDatanode(dn), 0L, 0L, 0, 0,
          null);
      bm.getDatanodeManager().checkIfClusterIsNowMultiRack(dn);
    }
  }

  private void removeNode(DatanodeDescriptor deadNode) {
    NetworkTopology cluster = bm.getDatanodeManager().getNetworkTopology();
    cluster.remove(deadNode);
    bm.removeBlocksAssociatedTo(deadNode);
  }


  /**
   * Test that replication of under-replicated blocks is detected
   * and basically works
   */
  @Test
  public void testBasicReplication() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doBasicTest(i);
    }
  }
  
  private void doBasicTest(int testIndex) {
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);

    DatanodeStorageInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertEquals(2, pipeline.length);
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        origStorages.contains(pipeline[0]));
    assertTrue("Destination of replication should be on the other rack. " +
        "Was: " + pipeline[1],
        rackB.contains(pipeline[1].getDatanodeDescriptor()));
  }
  

  /**
   * Regression test for HDFS-1480
   * - Cluster has 2 racks, A and B, each with three nodes.
   * - Block initially written on A1, A2, B1
   * - Admin decommissions two of these nodes (let's say A1 and A2 but it doesn't matter)
   * - Re-replication should respect rack policy
   */
  @Test
  public void testTwoOfThreeNodesDecommissioned() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestTwoOfThreeNodesDecommissioned(i);
    }
  }
  
  private void doTestTwoOfThreeNodesDecommissioned(int testIndex) throws Exception {
    // Block originally on A1, A2, B1
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1, 3);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    // Decommission two of the nodes (A1, A2)
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1);
    
    DatanodeStorageInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        origStorages.contains(pipeline[0]));
    assertEquals("Should have three targets", 3, pipeline.length);
    
    boolean foundOneOnRackA = false;
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeDescriptor target = pipeline[i].getDatanodeDescriptor();
      if (rackA.contains(target)) {
        foundOneOnRackA = true;
      }
      assertFalse(decomNodes.contains(target));
      assertFalse(origNodes.contains(target));
    }
    
    assertTrue("Should have at least one target on rack A. Pipeline: " +
        Joiner.on(",").join(pipeline),
        foundOneOnRackA);
  }
  

  /**
   * Test what happens when a block is on three nodes, and all three of those
   * nodes are decommissioned. It should properly re-replicate to three new
   * nodes. 
   */
  @Test
  public void testAllNodesHoldingReplicasDecommissioned() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestAllNodesHoldingReplicasDecommissioned(i);
    }
  }

  private void doTestAllNodesHoldingReplicasDecommissioned(int testIndex) throws Exception {
    // Block originally on A1, A2, B1
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1, 3);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    // Decommission all of the nodes
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1, 3);
    
    DatanodeStorageInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        origStorages.contains(pipeline[0]));
    assertEquals("Should have three targets", 4, pipeline.length);
    
    boolean foundOneOnRackA = false;
    boolean foundOneOnRackB = false;
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeDescriptor target = pipeline[i].getDatanodeDescriptor();
      if (rackA.contains(target)) {
        foundOneOnRackA = true;
      } else if (rackB.contains(target)) {
        foundOneOnRackB = true;
      }
      assertFalse(decomNodes.contains(target));
      assertFalse(origNodes.contains(target));
    }
    
    assertTrue("Should have at least one target on rack A. Pipeline: " +
        Joiner.on(",").join(pipeline),
        foundOneOnRackA);
    assertTrue("Should have at least one target on rack B. Pipeline: " +
        Joiner.on(",").join(pipeline),
        foundOneOnRackB);
  }

  /**
   * Test what happens when there are two racks, and an entire rack is
   * decommissioned.
   * 
   * Since the cluster is multi-rack, it will consider the block
   * under-replicated rather than create a third replica on the
   * same rack. Adding a new node on a third rack should cause re-replication
   * to that node.
   */
  @Test
  public void testOneOfTwoRacksDecommissioned() throws Exception {
    addNodes(nodes);
    NameNode.initMetrics(new Configuration(),
        HdfsServerConstants.NamenodeRole.NAMENODE);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestOneOfTwoRacksDecommissioned(i);
    }
  }
  
  private void doTestOneOfTwoRacksDecommissioned(int testIndex) throws Exception {
    // Block originally on A1, A2, B1
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1, 3);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    // Decommission all of the nodes in rack A
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1, 2);
    
    DatanodeStorageInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        origStorages.contains(pipeline[0]));
    // Only up to two nodes can be picked per rack when there are two racks.
    assertEquals("Should have two targets", 2, pipeline.length);
    
    boolean foundOneOnRackB = false;
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeDescriptor target = pipeline[i].getDatanodeDescriptor();
      if (rackB.contains(target)) {
        foundOneOnRackB = true;
      }
      assertFalse(decomNodes.contains(target));
      assertFalse(origNodes.contains(target));
    }
    
    assertTrue("Should have at least one target on rack B. Pipeline: " +
        Joiner.on(",").join(pipeline),
        foundOneOnRackB);
    
    // Mark the block as received on the target nodes in the pipeline
    fulfillPipeline(blockInfo, pipeline);

    // the block is still under-replicated. Add a new node. This should allow
    // the third off-rack replica.
    DatanodeDescriptor rackCNode =
      DFSTestUtil.getDatanodeDescriptor("7.7.7.7", "/rackC");
    rackCNode.updateStorage(new DatanodeStorage(DatanodeStorage.generateUuid()));
    addNodes(ImmutableList.of(rackCNode));
    try {
      DatanodeStorageInfo[] pipeline2 = scheduleSingleReplication(blockInfo);
      assertEquals(2, pipeline2.length);
      assertEquals(rackCNode, pipeline2[1].getDatanodeDescriptor());
    } finally {
      removeNode(rackCNode);
    }
  }

  /**
   * Unit test version of testSufficientlyReplBlocksUsesNewRack from
   * {@link TestBlocksWithNotEnoughRacks}.
   **/
  @Test
  public void testSufficientlyReplBlocksUsesNewRack() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestSufficientlyReplBlocksUsesNewRack(i);
    }
  }

  private void doTestSufficientlyReplBlocksUsesNewRack(int testIndex) {
    // Originally on only nodes in rack A.
    List<DatanodeDescriptor> origNodes = rackA;
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    DatanodeStorageInfo pipeline[] = scheduleSingleReplication(blockInfo);
    
    assertEquals(2, pipeline.length); // single new copy
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        origNodes.contains(pipeline[0].getDatanodeDescriptor()));
    assertTrue("Destination of replication should be on the other rack. " +
        "Was: " + pipeline[1],
        rackB.contains(pipeline[1].getDatanodeDescriptor()));
  }
  
  @Test
  public void testBlocksAreNotUnderreplicatedInSingleRack() throws Exception {
    List<DatanodeDescriptor> nodes = ImmutableList.of(
        BlockManagerTestUtil.getDatanodeDescriptor("1.1.1.1", "/rackA", true),
        BlockManagerTestUtil.getDatanodeDescriptor("2.2.2.2", "/rackA", true),
        BlockManagerTestUtil.getDatanodeDescriptor("3.3.3.3", "/rackA", true),
        BlockManagerTestUtil.getDatanodeDescriptor("4.4.4.4", "/rackA", true),
        BlockManagerTestUtil.getDatanodeDescriptor("5.5.5.5", "/rackA", true),
        BlockManagerTestUtil.getDatanodeDescriptor("6.6.6.6", "/rackA", true)
      );
    addNodes(nodes);
    List<DatanodeDescriptor> origNodes = nodes.subList(0, 3);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestSingleRackClusterHasSufficientRedundancy(i, origNodes);
    }
  }
  
  private void doTestSingleRackClusterHasSufficientRedundancy(int testIndex,
      List<DatanodeDescriptor> origNodes)
      throws Exception {
    assertEquals(0, bm.numOfUnderReplicatedBlocks());
    BlockInfo block = addBlockOnNodes(testIndex, origNodes);
    assertFalse(bm.isNeededReconstruction(block,
        bm.countNodes(block, fsn.isInStartupSafeMode())));
  }

  @Test(timeout = 60000)
  public void testNeededReconstructionWhileAppending() throws IOException {
    Configuration conf = new HdfsConfiguration();
    String src = "/test-file";
    Path file = new Path(src);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      BlockManager bm = cluster.getNamesystem().getBlockManager();
      FileSystem fs = cluster.getFileSystem();
      NamenodeProtocols namenode = cluster.getNameNodeRpc();
      DFSOutputStream out = null;
      try {
        out = (DFSOutputStream) (fs.create(file).
            getWrappedStream());
        out.write(1);
        out.hflush();
        out.close();
        FSDataInputStream in = null;
        ExtendedBlock oldBlock = null;
        try {
          in = fs.open(file);
          oldBlock = DFSTestUtil.getAllBlocks(in).get(0).getBlock();
        } finally {
          IOUtils.closeStream(in);
        }

        String clientName =
            ((DistributedFileSystem) fs).getClient().getClientName();
        namenode.append(src, clientName, new EnumSetWritable<>(
            EnumSet.of(CreateFlag.APPEND)));
        LocatedBlock newLocatedBlock =
            namenode.updateBlockForPipeline(oldBlock, clientName);
        ExtendedBlock newBlock =
            new ExtendedBlock(oldBlock.getBlockPoolId(), oldBlock.getBlockId(),
                oldBlock.getNumBytes(),
                newLocatedBlock.getBlock().getGenerationStamp());
        namenode.updatePipeline(clientName, oldBlock, newBlock,
            newLocatedBlock.getLocations(), newLocatedBlock.getStorageIDs());
        BlockInfo bi = bm.getStoredBlock(newBlock.getLocalBlock());
        assertFalse(bm.isNeededReconstruction(bi, bm.countNodes(bi,
            cluster.getNamesystem().isInStartupSafeMode())));
      } finally {
        IOUtils.closeStream(out);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 60000)
  public void testDeleteCorruptReplicaWithStatleStorages() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.
        MIN_REPLICATION, 2);
    Path file = new Path("/test-file");
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      BlockManager blockManager = cluster.getNamesystem().getBlockManager();
      blockManager.getDatanodeManager().markAllDatanodesStaleAndSetKeyUpdateIfNeed();
      FileSystem fs = cluster.getFileSystem();
      FSDataOutputStream out = fs.create(file);
      for (int i = 0; i < 1024 * 1024 * 1; i++) {
        out.write(i);
      }
      out.hflush();
      MiniDFSCluster.DataNodeProperties datanode = cluster.stopDataNode(0);
      for (int i = 0; i < 1024 * 1024 * 1; i++) {
        out.write(i);
      }
      out.close();
      cluster.restartDataNode(datanode);
      cluster.triggerBlockReports();
      DataNodeTestUtils.triggerBlockReport(datanode.getDatanode());
      assertEquals(0, blockManager.getCorruptBlocks());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Tell the block manager that replication is completed for the given
   * pipeline.
   */
  private void fulfillPipeline(BlockInfo blockInfo,
      DatanodeStorageInfo[] pipeline) throws IOException {
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeStorageInfo storage = pipeline[i];
      bm.addBlock(storage, blockInfo, null);
      blockInfo.addStorage(storage, blockInfo);
    }
  }

  private BlockInfo blockOnNodes(long blkId, List<DatanodeDescriptor> nodes) {
    Block block = new Block(blkId);
    BlockInfo blockInfo = new BlockInfoContiguous(block, (short) 3);

    for (DatanodeDescriptor dn : nodes) {
      for (DatanodeStorageInfo storage : dn.getStorageInfos()) {
        blockInfo.addStorage(storage, blockInfo);
      }
    }
    return blockInfo;
  }

  private List<DatanodeDescriptor> getNodes(int ... indexes) {
    List<DatanodeDescriptor> ret = Lists.newArrayList();
    for (int idx : indexes) {
      ret.add(nodes.get(idx));
    }
    return ret;
  }

  private List<DatanodeDescriptor> getNodes(List<DatanodeStorageInfo> storages) {
    List<DatanodeDescriptor> ret = Lists.newArrayList();
    for (DatanodeStorageInfo s : storages) {
      ret.add(s.getDatanodeDescriptor());
    }
    return ret;
  }

  private List<DatanodeStorageInfo> getStorages(int ... indexes) {
    List<DatanodeStorageInfo> ret = Lists.newArrayList();
    for (int idx : indexes) {
      ret.add(storages[idx]);
    }
    return ret;
  }

  private List<DatanodeDescriptor> startDecommission(int ... indexes) {
    List<DatanodeDescriptor> nodes = getNodes(indexes);
    for (DatanodeDescriptor node : nodes) {
      node.startDecommission();
    }
    return nodes;
  }
  
  private BlockInfo addBlockOnNodes(long blockId, List<DatanodeDescriptor> nodes) {
    long inodeId = ++mockINodeId;
    final INodeFile bc = TestINodeFile.createINodeFile(inodeId);

    BlockInfo blockInfo = blockOnNodes(blockId, nodes);
    blockInfo.setReplication((short) 3);
    blockInfo.setBlockCollectionId(inodeId);
    Mockito.doReturn(bc).when(fsn).getBlockCollection(inodeId);
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    return blockInfo;
  }

  private BlockInfo addCorruptBlockOnNodes(long blockId,
      List<DatanodeDescriptor> nodes) throws IOException {
    long inodeId = ++mockINodeId;
    final INodeFile bc = TestINodeFile.createINodeFile(inodeId);

    BlockInfo blockInfo = blockOnNodes(blockId, nodes);
    blockInfo.setReplication((short) 3);
    blockInfo.setBlockCollectionId(inodeId);
    Mockito.doReturn(bc).when(fsn).getBlockCollection(inodeId);
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    bm.markBlockReplicasAsCorrupt(blockInfo, blockInfo,
        blockInfo.getGenerationStamp() + 1, blockInfo.getNumBytes(),
        new DatanodeStorageInfo[]{nodes.get(0).getStorageInfos()[0]});
    return blockInfo;
  }

  private DatanodeStorageInfo[] scheduleSingleReplication(BlockInfo block) {
    // list for priority 1
    List<BlockInfo> list_p1 = new ArrayList<>();
    list_p1.add(block);

    // list of lists for each priority
    List<List<BlockInfo>> list_all = new ArrayList<>();
    list_all.add(new ArrayList<BlockInfo>()); // for priority 0
    list_all.add(list_p1); // for priority 1

    assertEquals("Block not initially pending reconstruction", 0,
        bm.pendingReconstruction.getNumReplicas(block));
    assertEquals(
        "computeBlockReconstructionWork should indicate reconstruction is needed",
        1, bm.computeReconstructionWorkForBlocks(list_all));
    assertTrue("reconstruction is pending after work is computed",
        bm.pendingReconstruction.getNumReplicas(block) > 0);

    LinkedListMultimap<DatanodeStorageInfo, BlockTargetPair> repls =
        getAllPendingReconstruction();
    assertEquals(1, repls.size());
    Entry<DatanodeStorageInfo, BlockTargetPair> repl =
      repls.entries().iterator().next();
        
    DatanodeStorageInfo[] targets = repl.getValue().targets;

    DatanodeStorageInfo[] pipeline = new DatanodeStorageInfo[1 + targets.length];
    pipeline[0] = repl.getKey();
    System.arraycopy(targets, 0, pipeline, 1, targets.length);

    return pipeline;
  }

  private LinkedListMultimap<DatanodeStorageInfo, BlockTargetPair> getAllPendingReconstruction() {
    LinkedListMultimap<DatanodeStorageInfo, BlockTargetPair> repls =
      LinkedListMultimap.create();
    for (DatanodeDescriptor dn : nodes) {
      List<BlockTargetPair> thisRepls = dn.getReplicationCommand(10);
      if (thisRepls != null) {
        for(DatanodeStorageInfo storage : dn.getStorageInfos()) {
          repls.putAll(storage, thisRepls);
        }
      }
    }
    return repls;
  }

  /**
   * Test that a source node for a highest-priority reconstruction is chosen
   * even if all available source nodes have reached their replication limits.
   */
  @Test
  public void testHighestPriReplSrcChosenDespiteMaxReplLimit() throws Exception {
    bm.setMaxReplicationStreams(0, false);
    bm.setReplicationStreamsHardLimit(1);

    long blockId = 42;         // arbitrary
    Block aBlock = new Block(blockId, 0, 0);

    List<DatanodeDescriptor> origNodes = getNodes(0, 1);
    // Add the block to the first node.
    addBlockOnNodes(blockId,origNodes.subList(0,1));

    List<DatanodeDescriptor> cntNodes = new LinkedList<DatanodeDescriptor>();
    List<DatanodeStorageInfo> liveNodes = new LinkedList<DatanodeStorageInfo>();

    assertNotNull("Chooses source node for a highest-priority replication"
        + " even if all available source nodes have reached their replication"
        + " limits below the hard limit.",
        bm.chooseSourceDatanodes(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
            LowRedundancyBlocks.QUEUE_HIGHEST_PRIORITY)[0]);

    assertEquals("Does not choose a source node for a less-than-highest-priority"
            + " replication since all available source nodes have reached"
            + " their replication limits.", 0,
        bm.chooseSourceDatanodes(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
            LowRedundancyBlocks.QUEUE_VERY_LOW_REDUNDANCY).length);

    // Increase the replication count to test replication count > hard limit
    DatanodeStorageInfo targets[] = { origNodes.get(1).getStorageInfos()[0] };
    origNodes.get(0).addBlockToBeReplicated(aBlock, targets);

    assertEquals("Does not choose a source node for a highest-priority"
            + " replication when all available nodes exceed the hard limit.", 0,
        bm.chooseSourceDatanodes(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
            LowRedundancyBlocks.QUEUE_HIGHEST_PRIORITY).length);
  }

  @Test
  public void testScheduleReconstructionWithDupEC() throws Exception {
    NameNode.initMetrics(new HdfsConfiguration(), HdfsServerConstants.NamenodeRole.NAMENODE);
    bm.setMaxReplicationStreams(4, false);

    // ec policy
    ECSchema rsSchema = new ECSchema("rs", 3, 2);
    String policyName = "RS-3-2-128k";
    int cellSize = 128 * 1024;
    ErasureCodingPolicy ecPolicy =
        new ErasureCodingPolicy(policyName, rsSchema, cellSize, (byte) -1);

    long blockId = -9223372036854775776L; // real ec block id
    Block aBlock = new Block(blockId, ecPolicy.getCellSize() * ecPolicy.getNumDataUnits(), 0);
    // striped blockInfo
    BlockInfoStriped aBlockInfoStriped = new BlockInfoStriped(aBlock, ecPolicy);
    aBlockInfoStriped.setBlockCollectionId(1);
    INodeFile inodeFile = mock(INodeFile.class);
    doReturn(inodeFile).when(fsn).getBlockCollection(1);
    // ec storageInfo
    DatanodeStorageInfo ds1 = DFSTestUtil.createDatanodeStorageInfo(
        "storage1", "1.1.1.1", "rack1", "host1");
    DatanodeStorageInfo ds2 = DFSTestUtil.createDatanodeStorageInfo(
        "storage2", "2.2.2.2", "rack2", "host2");
    DatanodeStorageInfo ds3 = DFSTestUtil.createDatanodeStorageInfo(
        "storage3", "3.3.3.3", "rack3", "host3");
    DatanodeStorageInfo ds4 = DFSTestUtil.createDatanodeStorageInfo(
        "storage4", "4.4.4.4", "rack4", "host4");
    DatanodeStorageInfo ds5 = DFSTestUtil.createDatanodeStorageInfo(
        "storage5", "5.5.5.5", "rack5", "host5");
    // link block with storage
    aBlockInfoStriped.addStorage(ds1, aBlock);
    aBlockInfoStriped.addStorage(ds2, new Block(blockId + 1, 0, 0));
    aBlockInfoStriped.addStorage(ds3, new Block(blockId + 2, 0, 0));
    // dup internal block
    aBlockInfoStriped.addStorage(ds4, new Block(blockId + 3, 0, 0));
    aBlockInfoStriped.addStorage(ds5, new Block(blockId + 3, 0, 0));
    // simulate the node 2 arrive maxReplicationStreams
    for(int i = 0; i < 4; i++){
      ds4.getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
    }

    addEcBlockToBM(blockId, ecPolicy);

    BlockReconstructionWork work = bm.scheduleReconstruction(aBlockInfoStriped, 0);
    assertTrue(work instanceof ErasureCodingWork);
    assertEquals(1, work.getAdditionalReplRequired());
    assertEquals(4, ((ErasureCodingWork) work).getLiveBlockIndices().length);
    assertTrue(containAllIndices(Sets.newHashSet((byte) 0, (byte) 1, (byte) 2, (byte) 3),
        ((ErasureCodingWork) work).getLiveBlockIndices()));
    work.getLiveReplicaStorages().contains(ds5);
  }

  @Test
  public void testChooseSrcDNWithDupECInDecommissioningNode() throws Exception {
    NameNode.initMetrics(new HdfsConfiguration(), HdfsServerConstants.NamenodeRole.NAMENODE);
    long blockId = -9223372036854775776L; // real ec block id
    // RS-3-2 EC policy
    ErasureCodingPolicy ecPolicy =
        SystemErasureCodingPolicies.getPolicies().get(1);
    Block aBlock = new Block(blockId, ecPolicy.getCellSize() * ecPolicy.getNumDataUnits(), 0);
    // striped blockInfo
    BlockInfoStriped aBlockInfoStriped = new BlockInfoStriped(aBlock, ecPolicy);
    aBlockInfoStriped.setBlockCollectionId(1);
    INodeFile inodeFile = mock(INodeFile.class);
    doReturn(inodeFile).when(fsn).getBlockCollection(1);
    // ec storageInfo
    DatanodeStorageInfo ds1 = DFSTestUtil.createDatanodeStorageInfo(
        "storage1", "1.1.1.1", "rack1", "host1");
    DatanodeStorageInfo ds2 = DFSTestUtil.createDatanodeStorageInfo(
        "storage2", "2.2.2.2", "rack2", "host2");
    DatanodeStorageInfo ds3 = DFSTestUtil.createDatanodeStorageInfo(
        "storage3", "3.3.3.3", "rack3", "host3");
    DatanodeStorageInfo ds4 = DFSTestUtil.createDatanodeStorageInfo(
        "storage4", "4.4.4.4", "rack4", "host4");
    DatanodeStorageInfo ds5 = DFSTestUtil.createDatanodeStorageInfo(
        "storage5", "5.5.5.5", "rack5", "host5");
    DatanodeStorageInfo ds6 = DFSTestUtil.createDatanodeStorageInfo(
        "storage6", "6.6.6.6", "rack6", "host6");

    // link block with storage
    aBlockInfoStriped.addStorage(ds1, aBlock);
    aBlockInfoStriped.addStorage(ds2, new Block(blockId + 1, 0, 0));
    aBlockInfoStriped.addStorage(ds3, new Block(blockId + 2, 0, 0));
    aBlockInfoStriped.addStorage(ds4, new Block(blockId + 3, 0, 0));
    aBlockInfoStriped.addStorage(ds5, new Block(blockId + 4, 0, 0));
    // NOTE: duplicate block 0，this DN will replace the decommission ds1 DN
    aBlockInfoStriped.addStorage(ds6, aBlock);

    addEcBlockToBM(blockId, ecPolicy);
    // decommission datanode where store block 0
    ds1.getDatanodeDescriptor().startDecommission();

    // should not reschedule work
    assertNull(bm.scheduleReconstruction(aBlockInfoStriped, 0));
  }

  @Test
  public void testSkipReconstructionWithManyBusyNodes() {
    NameNode.initMetrics(new Configuration(), HdfsServerConstants.NamenodeRole.NAMENODE);
    long blockId = -9223372036854775776L; // real ec block id
    // RS-3-2 EC policy
    ErasureCodingPolicy ecPolicy =
        SystemErasureCodingPolicies.getPolicies().get(1);

    // create an EC block group: 3 data blocks + 2 parity blocks
    Block aBlockGroup = new Block(blockId, ecPolicy.getCellSize() * ecPolicy.getNumDataUnits(), 0);
    BlockInfoStriped aBlockInfoStriped = new BlockInfoStriped(aBlockGroup, ecPolicy);

    // create 4 storageInfo, which means 1 block is missing
    DatanodeStorageInfo ds1 = DFSTestUtil.createDatanodeStorageInfo(
        "storage1", "1.1.1.1", "rack1", "host1");
    DatanodeStorageInfo ds2 = DFSTestUtil.createDatanodeStorageInfo(
        "storage2", "2.2.2.2", "rack2", "host2");
    DatanodeStorageInfo ds3 = DFSTestUtil.createDatanodeStorageInfo(
        "storage3", "3.3.3.3", "rack3", "host3");
    DatanodeStorageInfo ds4 = DFSTestUtil.createDatanodeStorageInfo(
        "storage4", "4.4.4.4", "rack4", "host4");

    // link block with storage
    aBlockInfoStriped.addStorage(ds1, aBlockGroup);
    aBlockInfoStriped.addStorage(ds2, new Block(blockId + 1, 0, 0));
    aBlockInfoStriped.addStorage(ds3, new Block(blockId + 2, 0, 0));
    aBlockInfoStriped.addStorage(ds4, new Block(blockId + 3, 0, 0));

    addEcBlockToBM(blockId, ecPolicy);
    aBlockInfoStriped.setBlockCollectionId(mockINodeId);

    // reconstruction should be scheduled
    BlockReconstructionWork work = bm.scheduleReconstruction(aBlockInfoStriped, 3);
    assertNotNull(work);

    // simulate the 2 nodes reach maxReplicationStreams
    for(int i = 0; i < bm.getMaxReplicationStreams(); i++){
      ds3.getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
      ds4.getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
    }

    // reconstruction should be skipped since the number of non-busy nodes are not enough
    work = bm.scheduleReconstruction(aBlockInfoStriped, 3);
    assertNull(work);
  }

  @Test
  public void testSkipReconstructionWithManyBusyNodes2() {
    NameNode.initMetrics(new Configuration(), HdfsServerConstants.NamenodeRole.NAMENODE);
    long blockId = -9223372036854775776L; // real ec block id
    // RS-3-2 EC policy
    ErasureCodingPolicy ecPolicy =
        SystemErasureCodingPolicies.getPolicies().get(1);

    // create an EC block group: 2 data blocks + 2 parity blocks
    Block aBlockGroup = new Block(blockId,
        ecPolicy.getCellSize() * (ecPolicy.getNumDataUnits() - 1), 0);
    BlockInfoStriped aBlockInfoStriped = new BlockInfoStriped(aBlockGroup, ecPolicy);

    // create 3 storageInfo, which means 1 block is missing
    DatanodeStorageInfo ds1 = DFSTestUtil.createDatanodeStorageInfo(
        "storage1", "1.1.1.1", "rack1", "host1");
    DatanodeStorageInfo ds2 = DFSTestUtil.createDatanodeStorageInfo(
        "storage2", "2.2.2.2", "rack2", "host2");
    DatanodeStorageInfo ds3 = DFSTestUtil.createDatanodeStorageInfo(
        "storage3", "3.3.3.3", "rack3", "host3");

    // link block with storage
    aBlockInfoStriped.addStorage(ds1, aBlockGroup);
    aBlockInfoStriped.addStorage(ds2, new Block(blockId + 1, 0, 0));
    aBlockInfoStriped.addStorage(ds3, new Block(blockId + 2, 0, 0));

    addEcBlockToBM(blockId, ecPolicy);
    aBlockInfoStriped.setBlockCollectionId(mockINodeId);

    // reconstruction should be scheduled
    BlockReconstructionWork work = bm.scheduleReconstruction(aBlockInfoStriped, 3);
    assertNotNull(work);

    // simulate the 1 node reaches maxReplicationStreams
    for(int i = 0; i < bm.getMaxReplicationStreams(); i++){
      ds2.getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
    }

    // reconstruction should still be scheduled since there are 2 source nodes to create 2 blocks
    work = bm.scheduleReconstruction(aBlockInfoStriped, 3);
    assertNotNull(work);

    // simulate the 1 more node reaches maxReplicationStreams
    for(int i = 0; i < bm.getMaxReplicationStreams(); i++){
      ds3.getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
    }

    // reconstruction should be skipped since the number of non-busy nodes are not enough
    work = bm.scheduleReconstruction(aBlockInfoStriped, 3);
    assertNull(work);
  }

  @Test
  public void testSkipReconstructionWithManyBusyNodes3() {
    NameNode.initMetrics(new Configuration(), HdfsServerConstants.NamenodeRole.NAMENODE);
    long blockId = -9223372036854775776L; // Real ec block id
    // RS-3-2 EC policy
    ErasureCodingPolicy ecPolicy =
            SystemErasureCodingPolicies.getPolicies().get(1);

    // Create an EC block group: 3 data blocks + 2 parity blocks.
    Block aBlockGroup = new Block(blockId, ecPolicy.getCellSize() * ecPolicy.getNumDataUnits(), 0);
    BlockInfoStriped aBlockInfoStriped = new BlockInfoStriped(aBlockGroup, ecPolicy);

    // Create 4 storageInfo, which means 1 block is missing.
    DatanodeStorageInfo ds1 = DFSTestUtil.createDatanodeStorageInfo(
            "storage1", "1.1.1.1", "rack1", "host1");
    DatanodeStorageInfo ds2 = DFSTestUtil.createDatanodeStorageInfo(
            "storage2", "2.2.2.2", "rack2", "host2");
    DatanodeStorageInfo ds3 = DFSTestUtil.createDatanodeStorageInfo(
            "storage3", "3.3.3.3", "rack3", "host3");
    DatanodeStorageInfo ds4 = DFSTestUtil.createDatanodeStorageInfo(
            "storage4", "4.4.4.4", "rack4", "host4");

    // Link block with storage.
    aBlockInfoStriped.addStorage(ds1, aBlockGroup);
    aBlockInfoStriped.addStorage(ds2, new Block(blockId + 1, 0, 0));
    aBlockInfoStriped.addStorage(ds3, new Block(blockId + 2, 0, 0));
    aBlockInfoStriped.addStorage(ds4, new Block(blockId + 3, 0, 0));

    addEcBlockToBM(blockId, ecPolicy);
    aBlockInfoStriped.setBlockCollectionId(mockINodeId);

    // Reconstruction should be scheduled.
    BlockReconstructionWork work = bm.scheduleReconstruction(aBlockInfoStriped, 3);
    assertNotNull(work);

    ExtendedBlock dummyBlock = new ExtendedBlock("bpid", 1, 1, 1);
    DatanodeDescriptor dummyDD = ds1.getDatanodeDescriptor();
    DatanodeDescriptor[] dummyDDArray = new DatanodeDescriptor[]{dummyDD};
    DatanodeStorageInfo[] dummyDSArray = new DatanodeStorageInfo[]{ds1};
    // Simulate the 2 nodes reach maxReplicationStreams.
    for(int i = 0; i < bm.getMaxReplicationStreams(); i++){ //Add some dummy EC reconstruction task.
      ds3.getDatanodeDescriptor().addBlockToBeErasureCoded(dummyBlock, dummyDDArray,
              dummyDSArray, new byte[0], new byte[0], ecPolicy);
      ds4.getDatanodeDescriptor().addBlockToBeErasureCoded(dummyBlock, dummyDDArray,
              dummyDSArray, new byte[0], new byte[0], ecPolicy);
    }

    // Reconstruction should be skipped since the number of non-busy nodes are not enough.
    work = bm.scheduleReconstruction(aBlockInfoStriped, 3);
    assertNull(work);
  }

  @Test
  public void testFavorDecomUntilHardLimit() throws Exception {
    bm.setMaxReplicationStreams(0, false);
    bm.setReplicationStreamsHardLimit(1);

    long blockId = 42;         // arbitrary
    Block aBlock = new Block(blockId, 0, 0);
    List<DatanodeDescriptor> origNodes = getNodes(0, 1);
    // Add the block to the first node.
    addBlockOnNodes(blockId,origNodes.subList(0,1));
    origNodes.get(0).startDecommission();

    List<DatanodeDescriptor> cntNodes = new LinkedList<DatanodeDescriptor>();
    List<DatanodeStorageInfo> liveNodes = new LinkedList<DatanodeStorageInfo>();

    assertNotNull("Chooses decommissioning source node for a normal replication"
        + " if all available source nodes have reached their replication"
        + " limits below the hard limit.",
        bm.chooseSourceDatanodes(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
            LowRedundancyBlocks.QUEUE_LOW_REDUNDANCY)[0]);


    // Increase the replication count to test replication count > hard limit
    DatanodeStorageInfo targets[] = { origNodes.get(1).getStorageInfos()[0] };
    origNodes.get(0).addBlockToBeReplicated(aBlock, targets);

    assertEquals("Does not choose a source decommissioning node for a normal"
        + " replication when all available nodes exceed the hard limit.", 0,
        bm.chooseSourceDatanodes(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
            LowRedundancyBlocks.QUEUE_LOW_REDUNDANCY).length);
  }

  @Test
  public void testSafeModeIBR() throws Exception {
    DatanodeDescriptor node = spy(nodes.get(0));
    DatanodeStorageInfo ds = node.getStorageInfos()[0];
    node.setAlive(true);

    DatanodeRegistration nodeReg =
        new DatanodeRegistration(node, null, null, "");

    // pretend to be in safemode
    doReturn(true).when(fsn).isInStartupSafeMode();
    
    // register new node
    bm.getDatanodeManager().registerDatanode(nodeReg);
    bm.getDatanodeManager().addDatanode(node); // swap in spy    
    assertEquals(node, bm.getDatanodeManager().getDatanode(node));
    assertEquals(0, ds.getBlockReportCount());
    // send block report, should be processed
    reset(node);
    
    bm.processReport(node, new DatanodeStorage(ds.getStorageID()),
        BlockListAsLongs.EMPTY, null);
    assertEquals(1, ds.getBlockReportCount());
    // send block report again, should NOT be processed
    reset(node);
    bm.processReport(node, new DatanodeStorage(ds.getStorageID()),
        BlockListAsLongs.EMPTY, null);
    assertEquals(1, ds.getBlockReportCount());

    // re-register as if node restarted, should update existing node
    bm.getDatanodeManager().removeDatanode(node);
    reset(node);
    bm.getDatanodeManager().registerDatanode(nodeReg);
    verify(node).updateRegInfo(nodeReg);
    // send block report, should be processed after restart
    reset(node);
    bm.processReport(node, new DatanodeStorage(ds.getStorageID()),
                     BlockListAsLongs.EMPTY, null);
    // Reinitialize as registration with empty storage list pruned
    // node.storageMap.
    ds = node.getStorageInfos()[0];
    assertEquals(1, ds.getBlockReportCount());
  }
  
  @Test
  public void testSafeModeIBRAfterIncremental() throws Exception {
    DatanodeDescriptor node = spy(nodes.get(0));
    DatanodeStorageInfo ds = node.getStorageInfos()[0];

    node.setAlive(true);

    DatanodeRegistration nodeReg =
        new DatanodeRegistration(node, null, null, "");

    // pretend to be in safemode
    doReturn(true).when(fsn).isInStartupSafeMode();

    // register new node
    bm.getDatanodeManager().registerDatanode(nodeReg);
    bm.getDatanodeManager().addDatanode(node); // swap in spy    
    assertEquals(node, bm.getDatanodeManager().getDatanode(node));
    assertEquals(0, ds.getBlockReportCount());
    // send block report while pretending to already have blocks
    reset(node);
    doReturn(1).when(node).numBlocks();
    bm.processReport(node, new DatanodeStorage(ds.getStorageID()),
        BlockListAsLongs.EMPTY, null);
    assertEquals(1, ds.getBlockReportCount());
  }

  /**
   * test when NN starts and in same mode, it receives an incremental blockReport
   * firstly. Then receives first full block report.
   */
  @Test
  public void testSafeModeIBRBeforeFirstFullBR() throws Exception {
    // pretend to be in safemode
    doReturn(true).when(fsn).isInStartupSafeMode();

    DatanodeDescriptor node = nodes.get(0);
    DatanodeStorageInfo ds = node.getStorageInfos()[0];
    node.setAlive(true);
    DatanodeRegistration nodeReg =  new DatanodeRegistration(node, null, null, "");

    // register new node
    bm.getDatanodeManager().registerDatanode(nodeReg);
    bm.getDatanodeManager().addDatanode(node);
    assertEquals(node, bm.getDatanodeManager().getDatanode(node));
    assertEquals(0, ds.getBlockReportCount());
    // Build a incremental report
    List<ReceivedDeletedBlockInfo> rdbiList = new ArrayList<>();
    // Build a full report
    BlockListAsLongs.Builder builder = BlockListAsLongs.builder();

    // blk_42 is finalized.
    long receivedBlockId = 42;  // arbitrary
    BlockInfo receivedBlock = addBlockToBM(receivedBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null));
    builder.add(new FinalizedReplica(receivedBlock, null, null));

    // blk_43 is under construction.
    long receivingBlockId = 43;
    BlockInfo receivingBlock = addUcBlockToBM(receivingBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVING_BLOCK, null));
    builder.add(new ReplicaBeingWritten(receivingBlock, null, null, null));

    // blk_44 has 2 records in IBR. It's finalized. So full BR has 1 record.
    long receivingReceivedBlockId = 44;
    BlockInfo receivingReceivedBlock = addBlockToBM(receivingReceivedBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingReceivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVING_BLOCK, null));
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingReceivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null));
    builder.add(new FinalizedReplica(receivingReceivedBlock, null, null));

    // blk_45 is not in full BR, because it's deleted.
    long ReceivedDeletedBlockId = 45;
    rdbiList.add(new ReceivedDeletedBlockInfo(
        new Block(ReceivedDeletedBlockId),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null));
    rdbiList.add(new ReceivedDeletedBlockInfo(
        new Block(ReceivedDeletedBlockId),
        ReceivedDeletedBlockInfo.BlockStatus.DELETED_BLOCK, null));

    // blk_46 exists in DN for a long time, so it's in full BR, but not in IBR.
    long existedBlockId = 46;
    BlockInfo existedBlock = addBlockToBM(existedBlockId);
    builder.add(new FinalizedReplica(existedBlock, null, null));

    // process IBR and full BR
    StorageReceivedDeletedBlocks srdb =
        new StorageReceivedDeletedBlocks(new DatanodeStorage(ds.getStorageID()),
            rdbiList.toArray(new ReceivedDeletedBlockInfo[rdbiList.size()]));
    bm.processIncrementalBlockReport(node, srdb);
    // Make sure it's the first full report
    assertEquals(0, ds.getBlockReportCount());
    bm.processReport(node, new DatanodeStorage(ds.getStorageID()),
        builder.build(), null);
    assertEquals(1, ds.getBlockReportCount());

    // verify the storage info is correct
    assertTrue(bm.getStoredBlock(new Block(receivedBlockId)).findStorageInfo
        (ds) >= 0);
    assertTrue(bm.getStoredBlock(new Block(receivingBlockId))
        .getUnderConstructionFeature().getNumExpectedLocations() > 0);
    assertTrue(bm.getStoredBlock(new Block(receivingReceivedBlockId))
        .findStorageInfo(ds) >= 0);
    assertNull(bm.getStoredBlock(new Block(ReceivedDeletedBlockId)));
    assertTrue(bm.getStoredBlock(new Block(existedBlock)).findStorageInfo
        (ds) >= 0);
  }

  @Test
  public void testSafeModeWithProvidedStorageBR() throws Exception {
    DatanodeDescriptor node0 = spy(nodes.get(0));
    DatanodeStorageInfo ds0 = node0.getStorageInfos()[0];
    node0.setAlive(true);
    DatanodeDescriptor node1 = spy(nodes.get(1));
    DatanodeStorageInfo ds1 = node1.getStorageInfos()[0];
    node1.setAlive(true);

    String providedStorageID = DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT;
    DatanodeStorage providedStorage = new DatanodeStorage(
        providedStorageID, DatanodeStorage.State.NORMAL, StorageType.PROVIDED);

    // create block manager with provided storage enabled
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);
    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TestProvidedImpl.TestFileRegionBlockAliasMap.class,
        BlockAliasMap.class);
    BlockManager bmPs = new BlockManager(fsn, false, conf);
    bmPs.setBlockPoolId("BP-12344-10.1.1.2-12344");

    // pretend to be in safemode
    doReturn(true).when(fsn).isInStartupSafeMode();

    // register new node
    DatanodeRegistration nodeReg0 =
        new DatanodeRegistration(node0, null, null, "");
    bmPs.getDatanodeManager().registerDatanode(nodeReg0);
    bmPs.getDatanodeManager().addDatanode(node0);
    DatanodeRegistration nodeReg1 =
        new DatanodeRegistration(node1, null, null, "");
    bmPs.getDatanodeManager().registerDatanode(nodeReg1);
    bmPs.getDatanodeManager().addDatanode(node1);

    // process reports of provided storage and disk storage
    bmPs.processReport(node0, providedStorage, BlockListAsLongs.EMPTY, null);
    bmPs.processReport(node0, new DatanodeStorage(ds0.getStorageID()),
        BlockListAsLongs.EMPTY, null);
    bmPs.processReport(node1, providedStorage, BlockListAsLongs.EMPTY, null);
    bmPs.processReport(node1, new DatanodeStorage(ds1.getStorageID()),
        BlockListAsLongs.EMPTY, null);

    // The provided stoage report should not affect disk storage report
    DatanodeStorageInfo dsPs =
        bmPs.getProvidedStorageMap().getProvidedStorageInfo();
    assertEquals(2, dsPs.getBlockReportCount());
    assertEquals(1, ds0.getBlockReportCount());
    assertEquals(1, ds1.getBlockReportCount());
  }

  @Test
  public void testUCBlockNotConsideredMissing() throws Exception {
    DatanodeDescriptor node = nodes.get(0);
    DatanodeStorageInfo ds = node.getStorageInfos()[0];
    node.setAlive(true);
    DatanodeRegistration nodeReg =
        new DatanodeRegistration(node, null, null, "");

    // register new node
    bm.getDatanodeManager().registerDatanode(nodeReg);
    bm.getDatanodeManager().addDatanode(node);

    // Build an incremental report
    List<ReceivedDeletedBlockInfo> rdbiList = new ArrayList<>();

    // blk_42 is under construction, finalizes on one node and is
    // immediately deleted on same node
    long blockId = 42;  // arbitrary
    BlockInfo receivedBlock = addUcBlockToBM(blockId);

    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null));
    rdbiList.add(new ReceivedDeletedBlockInfo(
        new Block(blockId),
        ReceivedDeletedBlockInfo.BlockStatus.DELETED_BLOCK, null));

    // process IBR
    StorageReceivedDeletedBlocks srdb =
        new StorageReceivedDeletedBlocks(new DatanodeStorage(ds.getStorageID()),
            rdbiList.toArray(new ReceivedDeletedBlockInfo[rdbiList.size()]));
    bm.setInitializedReplQueues(true);
    bm.processIncrementalBlockReport(node, srdb);
    // Needed replications should still be 0.
    assertEquals("UC block was incorrectly added to needed Replications",
        0, bm.neededReconstruction.size());
    bm.setInitializedReplQueues(false);
  }

  private BlockInfo addEcBlockToBM(long blkId, ErasureCodingPolicy ecPolicy) {
    Block block = new Block(blkId);
    BlockInfo blockInfo = new BlockInfoStriped(block, ecPolicy);
    long inodeId = ++mockINodeId;
    final INodeFile bc = TestINodeFile.createINodeFile(inodeId);
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    blockInfo.setBlockCollectionId(inodeId);
    doReturn(bc).when(fsn).getBlockCollection(inodeId);
    return blockInfo;
  }

  private BlockInfo addBlockToBM(long blkId) {
    Block block = new Block(blkId);
    BlockInfo blockInfo = new BlockInfoContiguous(block, (short) 3);
    long inodeId = ++mockINodeId;
    final INodeFile bc = TestINodeFile.createINodeFile(inodeId);
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    blockInfo.setBlockCollectionId(inodeId);
    doReturn(bc).when(fsn).getBlockCollection(inodeId);
    return blockInfo;
  }

  private BlockInfo addUcBlockToBM(long blkId) {
    Block block = new Block(blkId);
    BlockInfo blockInfo = new BlockInfoContiguous(block, (short) 3);
    blockInfo.convertToBlockUnderConstruction(UNDER_CONSTRUCTION, null);
    long inodeId = ++mockINodeId;
    final INodeFile bc = TestINodeFile.createINodeFile(inodeId);
    blockInfo.setBlockCollectionId(inodeId);
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    doReturn(bc).when(fsn).getBlockCollection(inodeId);
    return blockInfo;
  }
  
  /**
   * Tests that a namenode doesn't choose a datanode with full disks to 
   * store blocks.
   * @throws Exception
   */
  @Test
  public void testStorageWithRemainingCapacity() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = FileSystem.get(conf);
    Path file1 = null;
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final String poolId = namesystem.getBlockPoolId();
      final DatanodeRegistration nodeReg =
        InternalDataNodeTestUtils.getDNRegistrationForBP(cluster.getDataNodes().
        		get(0), poolId);
      final DatanodeDescriptor dd = NameNodeAdapter.getDatanode(namesystem,
    		  nodeReg);
      // By default, MiniDFSCluster will create 1 datanode with 2 storages.
      // Assigning 64k for remaining storage capacity and will 
      //create a file with 100k.
      for(DatanodeStorageInfo storage:  dd.getStorageInfos()) { 
    	  storage.setUtilizationForTesting(65536, 0, 65536, 0);
      }
      //sum of the remaining capacity of both the storages
      dd.setRemaining(131072);
      file1 = new Path("testRemainingStorage.dat");
      try {
        DFSTestUtil.createFile(fs, file1, 102400, 102400, 102400, (short)1,
        		0x1BAD5EED);
      }
      catch (RemoteException re) {
        GenericTestUtils.assertExceptionContains("of the 1 minReplication", re);
      }
    }
    finally {
      // Clean up
      assertTrue(fs.exists(file1));
      fs.delete(file1, true);
      assertTrue(!fs.exists(file1));
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testUseDelHint() {
    DatanodeStorageInfo delHint = new DatanodeStorageInfo(
        DFSTestUtil.getLocalDatanodeDescriptor(), new DatanodeStorage("id"));
    List<DatanodeStorageInfo> moreThan1Racks = Arrays.asList(delHint);
    List<StorageType> excessTypes = new ArrayList<>();
    BlockPlacementPolicyDefault policyDefault =
        (BlockPlacementPolicyDefault) bm.getBlockPlacementPolicy();
    excessTypes.add(StorageType.DEFAULT);
    Assert.assertTrue(policyDefault.useDelHint(delHint, null, moreThan1Racks,
        null, excessTypes));
    excessTypes.remove(0);
    excessTypes.add(StorageType.SSD);
    Assert.assertFalse(policyDefault.useDelHint(delHint, null, moreThan1Racks,
        null, excessTypes));
  }

  @Test
  public void testBlockReportQueueing() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      final FSNamesystem fsn = cluster.getNamesystem();
      final BlockManager bm = fsn.getBlockManager();
      final ExecutorService executor = Executors.newCachedThreadPool();

      final CyclicBarrier startBarrier = new CyclicBarrier(2);
      final CountDownLatch endLatch = new CountDownLatch(3);
      final CountDownLatch doneLatch = new CountDownLatch(1);

      // create a task intended to block while processing, thus causing
      // the queue to backup.  simulates how a full BR is processed.
      FutureTask<?> blockingOp = new FutureTask<Void>(
          new Callable<Void>(){
            @Override
            public Void call() throws IOException {
              bm.runBlockOp(new Callable<Void>() {
                @Override
                public Void call()
                    throws InterruptedException, BrokenBarrierException {
                  // use a barrier to control the blocking.
                  startBarrier.await();
                  endLatch.countDown();
                  return null;
                }
              });
              // signal that runBlockOp returned
              doneLatch.countDown();
              return null;
            }
          });

      // create an async task.  simulates how an IBR is processed.
      Callable<?> asyncOp = new Callable<Void>(){
        @Override
        public Void call() throws IOException {
          bm.enqueueBlockOp(new Runnable() {
            @Override
            public void run() {
              // use the latch to signal if the op has run.
              endLatch.countDown();
            }
          });
          return null;
        }
      };

      // calling get forces its execution so we can test if it's blocked.
      Future<?> blockedFuture = executor.submit(blockingOp);
      boolean isBlocked = false;
      try {
        // wait 1s for the future to block.  it should run instantaneously.
        blockedFuture.get(1, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        isBlocked = true;
      }
      assertTrue(isBlocked);

      // should effectively return immediately since calls are queued.
      // however they should be backed up in the queue behind the blocking
      // operation.
      executor.submit(asyncOp).get(1, TimeUnit.SECONDS);
      executor.submit(asyncOp).get(1, TimeUnit.SECONDS);

      // check the async calls are queued, and first is still blocked.
      assertEquals(2, bm.getBlockOpQueueLength());
      assertFalse(blockedFuture.isDone());

      // unblock the queue, wait for last op to complete, check the blocked
      // call has returned
      startBarrier.await(1, TimeUnit.SECONDS);
      assertTrue(endLatch.await(1, TimeUnit.SECONDS));
      assertEquals(0, bm.getBlockOpQueueLength());
      assertTrue(doneLatch.await(1, TimeUnit.SECONDS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  // spam the block manager with IBRs to verify queuing is occurring.
  @Test
  public void testAsyncIBR() throws Exception {
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME), Level.WARN);

    // will create files with many small blocks.
    final int blkSize = 4*1024;
    final int fileSize = blkSize * 100;
    final byte[] buf = new byte[2*blkSize];
    final int numWriters = 4;
    final int repl = 3;

    final CyclicBarrier barrier = new CyclicBarrier(numWriters);
    final CountDownLatch writeLatch = new CountDownLatch(numWriters);
    final AtomicBoolean failure = new AtomicBoolean();

    final Configuration conf = new HdfsConfiguration();
    conf.getLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, blkSize);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(8).build();

    try {
      cluster.waitActive();
      // create multiple writer threads to create a file with many blocks.
      // will test that concurrent writing causes IBR batching in the NN
      Thread[] writers = new Thread[numWriters];
      for (int i=0; i < writers.length; i++) {
        final Path p = new Path("/writer"+i);
        writers[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              FileSystem fs = cluster.getFileSystem();
              FSDataOutputStream os =
                  fs.create(p, true, buf.length, (short)repl, blkSize);
              // align writers for maximum chance of IBR batching.
              barrier.await();
              int remaining = fileSize;
              while (remaining > 0) {
                os.write(buf);
                remaining -= buf.length;
              }
              os.close();
            } catch (Exception e) {
              e.printStackTrace();
              failure.set(true);
            }
            // let main thread know we are done.
            writeLatch.countDown();
          }
        });
        writers[i].start();
      }

      // when and how many IBRs are queued is indeterminate, so just watch
      // the metrics and verify something was queued at during execution.
      boolean sawQueued = false;
      while (!writeLatch.await(10, TimeUnit.MILLISECONDS)) {
        assertFalse(failure.get());
        MetricsRecordBuilder rb = getMetrics("NameNodeActivity");
        long queued = MetricsAsserts.getIntGauge("BlockOpsQueued", rb);
        sawQueued |= (queued > 0);
      }
      assertFalse(failure.get());
      assertTrue(sawQueued);

      // verify that batching of the IBRs occurred.
      MetricsRecordBuilder rb = getMetrics("NameNodeActivity");
      long batched = MetricsAsserts.getLongCounter("BlockOpsBatched", rb);
      assertTrue(batched > 0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 60000)
  public void testBlockManagerMachinesArray() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
    try {
      cluster.waitActive();
      BlockManager blockManager = cluster.getNamesystem().getBlockManager();
      FileSystem fs = cluster.getFileSystem();
      final Path filePath = new Path("/tmp.txt");
      final long fileLen = 1L;
      DFSTestUtil.createFile(fs, filePath, fileLen, (short) 3, 1L);
      DFSTestUtil.waitForReplication((DistributedFileSystem)fs,
          filePath, (short) 3, 60000);
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 4);
      FSNamesystem ns = cluster.getNamesystem();
      // get the block
      final String bpid = cluster.getNamesystem().getBlockPoolId();
      File storageDir = cluster.getInstanceStorageDir(0, 0);
      File dataDir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
      assertTrue("Data directory does not exist", dataDir.exists());
      BlockInfo blockInfo =
          blockManager.blocksMap.getBlocks().iterator().next();
      ExtendedBlock blk = new ExtendedBlock(bpid, blockInfo.getBlockId(),
          blockInfo.getNumBytes(), blockInfo.getGenerationStamp());
      DatanodeDescriptor failedStorageDataNode =
          blockManager.getStoredBlock(blockInfo).getDatanode(0);
      DatanodeDescriptor corruptStorageDataNode =
          blockManager.getStoredBlock(blockInfo).getDatanode(1);

      ArrayList<StorageReport> reports = new ArrayList<StorageReport>();
      for(int i=0; i<failedStorageDataNode.getStorageInfos().length; i++) {
        DatanodeStorageInfo storageInfo = failedStorageDataNode
            .getStorageInfos()[i];
        DatanodeStorage dns = new DatanodeStorage(
            failedStorageDataNode.getStorageInfos()[i].getStorageID(),
            DatanodeStorage.State.FAILED,
            failedStorageDataNode.getStorageInfos()[i].getStorageType());
        while(storageInfo.getBlockIterator().hasNext()) {
          BlockInfo blockInfo1 = storageInfo.getBlockIterator().next();
          if(blockInfo1.equals(blockInfo)) {
            StorageReport report = new StorageReport(
                dns, true, storageInfo.getCapacity(),
                storageInfo.getDfsUsed(), storageInfo.getRemaining(),
                storageInfo.getBlockPoolUsed(), 0L);
            reports.add(report);
            break;
          }
        }
      }
      failedStorageDataNode.updateHeartbeat(reports.toArray(StorageReport
          .EMPTY_ARRAY), 0L, 0L, 0, 0, null);
      ns.writeLock();
      DatanodeStorageInfo corruptStorageInfo= null;
      for(int i=0; i<corruptStorageDataNode.getStorageInfos().length; i++) {
        corruptStorageInfo = corruptStorageDataNode.getStorageInfos()[i];
        while(corruptStorageInfo.getBlockIterator().hasNext()) {
          BlockInfo blockInfo1 = corruptStorageInfo.getBlockIterator().next();
          if (blockInfo1.equals(blockInfo)) {
            break;
          }
        }
      }
      blockManager.findAndMarkBlockAsCorrupt(blk, corruptStorageDataNode,
          corruptStorageInfo.getStorageID(),
          CorruptReplicasMap.Reason.ANY.toString());
      ns.writeUnlock();
      BlockInfo[] blockInfos = new BlockInfo[] {blockInfo};
      ns.readLock();
      LocatedBlocks locatedBlocks =
          blockManager.createLocatedBlocks(blockInfos, 3L, false, 0L, 3L,
              false, false, null, null);
      assertTrue("Located Blocks should exclude corrupt" +
              "replicas and failed storages",
          locatedBlocks.getLocatedBlocks().size() == 1);
      ns.readUnlock();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testMetaSaveCorruptBlocks() throws Exception {
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    addCorruptBlockOnNodes(0, origNodes);
    File file = new File("test.log");
    PrintWriter out = new PrintWriter(file);
    bm.metaSave(out);
    out.flush();
    FileInputStream fstream = new FileInputStream(file);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String corruptBlocksLine;
    Boolean foundIt = false;
    try {
      while ((corruptBlocksLine = reader.readLine()) != null) {
        if (corruptBlocksLine.compareTo("Corrupt Blocks:") == 0) {
          foundIt = true;
          break;
        }
      }
      assertTrue("Unexpected text in metasave," +
              "was expecting corrupt blocks section!", foundIt);
      corruptBlocksLine = reader.readLine();
      String regex = "Block=blk_[0-9]+_[0-9]+\\tSize=.*\\tNode=.*" +
          "\\tStorageID=.*\\tStorageState.*" +
          "\\tTotalReplicas=.*\\tReason=GENSTAMP_MISMATCH";
      assertTrue("Unexpected corrupt block section in metasave!",
          corruptBlocksLine.matches(regex));
      corruptBlocksLine = reader.readLine();
      regex = "Metasave: Number of datanodes.*";
      assertTrue("Unexpected corrupt block section in metasave!",
          corruptBlocksLine.matches(regex));
    } finally {
      if (reader != null)
        reader.close();
      file.delete();
    }
  }

  @Test
  public void testIsReplicaCorruptCall() throws Exception {
    BlockManager spyBM = spy(bm);
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1, 3);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(0, origNodes);
    spyBM.createLocatedBlocks(new BlockInfo[]{blockInfo}, 3L, false, 0L, 3L,
        false, false, null, null);
    verify(spyBM, Mockito.atLeast(0)).
        isReplicaCorrupt(any(BlockInfo.class),
            any(DatanodeDescriptor.class));
    addCorruptBlockOnNodes(0, origNodes);
    spyBM.createLocatedBlocks(new BlockInfo[]{blockInfo}, 3L, false, 0L, 3L,
        false, false, null, null);
    verify(spyBM, Mockito.atLeast(1)).
        isReplicaCorrupt(any(BlockInfo.class),
            any(DatanodeDescriptor.class));
  }

  @Test (timeout = 300000)
  public void testPlacementPolicySatisfied() throws Exception {
    LOG.info("Starting testPlacementPolicySatisfied.");
    final String[] initialRacks = new String[]{
        "/rack0", "/rack1", "/rack2", "/rack3", "/rack4", "/rack5"};
    final String[] initialHosts = new String[]{
        "host0", "host1", "host2", "host3", "host4", "host5"};
    final int numDataBlocks = StripedFileTestUtil.getDefaultECPolicy()
        .getNumDataUnits();
    final int numParityBlocks = StripedFileTestUtil.getDefaultECPolicy()
        .getNumParityUnits();
    final long blockSize = 6 * 1024 * 1024;
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .racks(initialRacks)
          .hosts(initialHosts)
          .numDataNodes(initialRacks.length)
          .build();
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final Path ecDir = new Path("/ec");
      final Path testFileUnsatisfied = new Path(ecDir, "test1");
      final Path testFileSatisfied = new Path(ecDir, "test2");
      dfs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      cluster.getFileSystem().getClient().mkdirs(ecDir.toString(), null, true);
      cluster.getFileSystem().getClient()
          .setErasureCodingPolicy(ecDir.toString(),
              StripedFileTestUtil.getDefaultECPolicy().getName());
      long fileLen = blockSize * numDataBlocks;

      // Create a file to be stored in 6 racks.
      DFSTestUtil.createFile(dfs, testFileUnsatisfied, fileLen, (short) 1, 1);
      // Block placement policy should be satisfied as rack count
      // is less than numDataBlocks + numParityBlocks.
      verifyPlacementPolicy(cluster, testFileUnsatisfied, true);

      LOG.info("Adding 3 new hosts in the existing racks.");
      cluster.startDataNodes(conf, 3, true, null,
          new String[]{"/rack3", "/rack4", "/rack5"},
          new String[]{"host3-2", "host4-2", "host5-2"}, null);
      cluster.triggerHeartbeats();

      LOG.info("Waiting for EC reconstruction to complete.");
      DFSTestUtil.waitForReplication(dfs, testFileUnsatisfied,
          (short)(numDataBlocks + numParityBlocks), 30 * 1000);
      // Block placement policy should still be satisfied
      // as there are only 6 racks.
      verifyPlacementPolicy(cluster, testFileUnsatisfied, true);

      LOG.info("Adding 3 new hosts in 3 new racks.");
      cluster.startDataNodes(conf, 3, true, null,
          new String[]{"/rack6", "/rack7", "/rack8"},
          new String[]{"host6", "host7", "host8"},
          null);
      cluster.triggerHeartbeats();
      // Addition of new racks can make the existing EC files block
      // placements unsatisfied and there is NO automatic block
      // reconstruction for this yet.
      // TODO:
      //  Verify for block placement satisfied once the automatic
      //  block reconstruction is implemented.
      verifyPlacementPolicy(cluster, testFileUnsatisfied, false);

      // Create a new file
      DFSTestUtil.createFile(dfs, testFileSatisfied, fileLen, (short) 1, 1);
      // The new file should be rightly placed on all 9 racks
      // and the block placement policy should be satisfied.
      verifyPlacementPolicy(cluster, testFileUnsatisfied, false);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void verifyPlacementPolicy(final MiniDFSCluster cluster,
      final Path file, boolean isBlockPlacementSatisfied) throws IOException {
    DistributedFileSystem dfs = cluster.getFileSystem();
    BlockManager blockManager = cluster.getNamesystem().getBlockManager();
    LocatedBlock lb = DFSTestUtil.getAllBlocks(dfs, file).get(0);
    BlockInfo blockInfo =
        blockManager.getStoredBlock(lb.getBlock().getLocalBlock());
    LOG.info("Block " + blockInfo + " storages: ");
    Iterator<DatanodeStorageInfo> itr = blockInfo.getStorageInfos();
    while (itr.hasNext()) {
      DatanodeStorageInfo dn = itr.next();
      LOG.info(" Rack: " + dn.getDatanodeDescriptor().getNetworkLocation()
          + ", DataNode: " + dn.getDatanodeDescriptor().getXferAddr());
    }
    if (isBlockPlacementSatisfied) {
      assertTrue("Block group of " + file + "should be placement" +
              " policy satisfied, currently!",
          blockManager.isPlacementPolicySatisfied(blockInfo));
    } else {
      assertFalse("Block group of " + file + " should be placement" +
              " policy unsatisfied, currently!",
          blockManager.isPlacementPolicySatisfied(blockInfo));
    }
  }

  /**
   * Unit test to check the race condition for adding a Block to
   * postponedMisreplicatedBlocks set which may not present in BlockManager
   * thus avoiding NullPointerException.
   **/
  @Test
  public void testMetaSavePostponedMisreplicatedBlocks() throws IOException {
    bm.postponeBlock(new Block());

    File file = new File("test.log");
    PrintWriter out = new PrintWriter(file);

    bm.metaSave(out);
    out.flush();

    FileInputStream fstream = new FileInputStream(file);
    DataInputStream in = new DataInputStream(fstream);

    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    StringBuilder buffer = new StringBuilder();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        buffer.append(line);
      }
      String output = buffer.toString();
      assertTrue("Metasave output should not have null block ",
          output.contains("Block blk_0_0 is Null"));

    } finally {
      reader.close();
      file.delete();
    }
  }

  @Test
  public void testMetaSaveMissingReplicas() throws Exception {
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo block = makeBlockReplicasMissing(0, origNodes);
    File file = new File("test.log");
    PrintWriter out = new PrintWriter(file);
    bm.metaSave(out);
    out.flush();
    FileInputStream fstream = new FileInputStream(file);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    StringBuilder buffer = new StringBuilder();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        buffer.append(line);
      }
      String output = buffer.toString();
      assertTrue("Metasave output should have reported missing blocks.",
          output.contains("Metasave: Blocks currently missing: 1"));
      assertTrue("There should be 0 blocks waiting for reconstruction",
          output.contains("Metasave: Blocks waiting for reconstruction: 0"));
      String blockNameGS = block.getBlockName() + "_" +
          block.getGenerationStamp();
      assertTrue("Block " + blockNameGS + " should be MISSING.",
          output.contains(blockNameGS + " MISSING"));
    } finally {
      reader.close();
      file.delete();
    }
  }

  private BlockInfo makeBlockReplicasMissing(long blockId,
      List<DatanodeDescriptor> nodesList) throws IOException {
    long inodeId = ++mockINodeId;
    final INodeFile bc = TestINodeFile.createINodeFile(inodeId);

    BlockInfo blockInfo = blockOnNodes(blockId, nodesList);
    blockInfo.setReplication((short) 3);
    blockInfo.setBlockCollectionId(inodeId);

    Mockito.doReturn(bc).when(fsn).getBlockCollection(inodeId);
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    bm.markBlockReplicasAsCorrupt(blockInfo, blockInfo,
        blockInfo.getGenerationStamp() + 1,
        blockInfo.getNumBytes(),
        new DatanodeStorageInfo[]{});
    BlockCollection mockedBc = mock(BlockCollection.class);
    when(mockedBc.getBlocks()).thenReturn(new BlockInfo[]{blockInfo});
    bm.checkRedundancy(mockedBc);
    return blockInfo;
  }

  private BlockInfo makeBlockReplicasMaintenance(long blockId,
      List<DatanodeDescriptor> nodesList) throws IOException {
    long inodeId = ++mockINodeId;
    final INodeFile bc = TestINodeFile.createINodeFile(inodeId);

    BlockInfo blockInfo = blockOnNodes(blockId, nodesList);
    blockInfo.setReplication((short) 3);
    blockInfo.setBlockCollectionId(inodeId);

    Mockito.doReturn(bc).when(fsn).getBlockCollection(inodeId);
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    nodesList.get(0).setInMaintenance();
    BlockCollection mockedBc = mock(BlockCollection.class);
    when(mockedBc.getBlocks()).thenReturn(new BlockInfo[]{blockInfo});
    bm.checkRedundancy(mockedBc);
    return blockInfo;
  }

  @Test
  public void testMetaSaveInMaintenanceReplicas() throws Exception {
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo block = makeBlockReplicasMaintenance(0, origNodes);
    File file = new File("test.log");
    PrintWriter out = new PrintWriter(file);
    bm.metaSave(out);
    out.flush();
    FileInputStream fstream = new FileInputStream(file);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    StringBuilder buffer = new StringBuilder();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        buffer.append(line);
        System.out.println(line);
      }
      String output = buffer.toString();
      assertTrue("Metasave output should not have reported " +
              "missing blocks.",
          output.contains("Metasave: Blocks currently missing: 0"));
      assertTrue("There should be 1 block waiting for reconstruction",
          output.contains("Metasave: Blocks waiting for reconstruction: 1"));
      String blockNameGS = block.getBlockName() + "_" +
          block.getGenerationStamp();
      assertTrue("Block " + blockNameGS +
              " should be list as maintenance.",
          output.contains(blockNameGS + " (replicas: live: 1 decommissioning " +
              "and decommissioned: 0 corrupt: 0 in excess: " +
              "0 maintenance mode: 1)"));
    } finally {
      reader.close();
      file.delete();
    }
  }

  private BlockInfo makeBlockReplicasDecommission(long blockId,
      List<DatanodeDescriptor> nodesList) throws IOException {
    long inodeId = ++mockINodeId;
    final INodeFile bc = TestINodeFile.createINodeFile(inodeId);

    BlockInfo blockInfo = blockOnNodes(blockId, nodesList);
    blockInfo.setReplication((short) 3);
    blockInfo.setBlockCollectionId(inodeId);

    Mockito.doReturn(bc).when(fsn).getBlockCollection(inodeId);
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    nodesList.get(0).startDecommission();
    BlockCollection mockedBc = mock(BlockCollection.class);
    when(mockedBc.getBlocks()).thenReturn(new BlockInfo[]{blockInfo});
    bm.checkRedundancy(mockedBc);
    return blockInfo;
  }

  @Test
  public void testMetaSaveDecommissioningReplicas() throws Exception {
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo block = makeBlockReplicasDecommission(0, origNodes);
    File file = new File("test.log");
    PrintWriter out = new PrintWriter(file);
    bm.metaSave(out);
    out.flush();
    FileInputStream fstream = new FileInputStream(file);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    StringBuilder buffer = new StringBuilder();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        buffer.append(line);
      }
      String output = buffer.toString();
      assertTrue("Metasave output should not have reported " +
              "missing blocks.",
          output.contains("Metasave: Blocks currently missing: 0"));
      assertTrue("There should be 1 block waiting for reconstruction",
          output.contains("Metasave: Blocks waiting for reconstruction: 1"));
      String blockNameGS = block.getBlockName() + "_" +
          block.getGenerationStamp();
      assertTrue("Block " + blockNameGS +
              " should be list as maintenance.",
          output.contains(blockNameGS + " (replicas: live: 1 decommissioning " +
              "and decommissioned: 1 corrupt: 0 in excess: " +
              "0 maintenance mode: 0)"));
    } finally {
      reader.close();
      file.delete();
    }
  }

  @Test
  public void testLegacyBlockInInvalidateBlocks() {
    final long legancyGenerationStampLimit = 10000;
    BlockIdManager bim = Mockito.mock(BlockIdManager.class);

    when(bim.getLegacyGenerationStampLimit())
        .thenReturn(legancyGenerationStampLimit);
    when(bim.isStripedBlock(any(Block.class))).thenCallRealMethod();
    when(bim.isLegacyBlock(any(Block.class))).thenCallRealMethod();

    InvalidateBlocks ibs = new InvalidateBlocks(100, 30000, bim);

    Block legacy = new Block(-1, 10, legancyGenerationStampLimit / 10);
    Block striped = new Block(
        bm.nextBlockId(BlockType.STRIPED), 10,
        legancyGenerationStampLimit + 10);

    DatanodeInfo legacyDnInfo = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo stripedDnInfo = DFSTestUtil.getLocalDatanodeInfo();

    ibs.add(legacy, legacyDnInfo, false);
    assertEquals(1, ibs.getBlocks());
    assertEquals(0, ibs.getECBlocks());

    ibs.add(striped, stripedDnInfo, false);
    assertEquals(1, ibs.getBlocks());
    assertEquals(1, ibs.getECBlocks());

    ibs.remove(legacyDnInfo);
    assertEquals(0, ibs.getBlocks());
    assertEquals(1, ibs.getECBlocks());

    ibs.remove(stripedDnInfo);
    assertEquals(0, ibs.getBlocks());
    assertEquals(0, ibs.getECBlocks());
  }

  @Test
  public void testValidateReconstructionWorkAndRacksNotEnough() {
    addNodes(nodes);
    // Originally on only nodes in rack A.
    List<DatanodeDescriptor> origNodes = rackA;
    BlockInfo blockInfo = addBlockOnNodes(0, origNodes);
    BlockPlacementStatus status = bm.getBlockPlacementStatus(blockInfo);
    // Block has enough copies, but not enough racks.
    assertFalse(status.isPlacementPolicySatisfied());
    DatanodeStorageInfo newNode = DFSTestUtil.createDatanodeStorageInfo(
            "storage8", "8.8.8.8", "/rackA", "host8");
    BlockReconstructionWork work = bm.scheduleReconstruction(blockInfo, 3);
    assertNotNull(work);
    assertEquals(1, work.getAdditionalReplRequired());
    // the new targets in rack A.
    work.setTargets(new DatanodeStorageInfo[]{newNode});
    // the new targets do not meet the placement policy return false.
    assertFalse(bm.validateReconstructionWork(work));
    // validateReconstructionWork return false, need to perform resetTargets().
    assertNull(work.getTargets());
  }

  /**
   * Test whether the first block report after DataNode restart is completely
   * processed.
   */
  @Test
  public void testBlockReportAfterDataNodeRestart() throws Exception {
    Configuration conf = new HdfsConfiguration();
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
           .numDataNodes(3).storagesPerDatanode(1).build()) {
      cluster.waitActive();
      BlockManager blockManager = cluster.getNamesystem().getBlockManager();
      DistributedFileSystem fs = cluster.getFileSystem();
      final Path filePath = new Path("/tmp.txt");
      final long fileLen = 1L;
      DFSTestUtil.createFile(fs, filePath, fileLen, (short) 3, 1L);
      DFSTestUtil.waitForReplication(fs, filePath, (short) 3, 60000);
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 3);

      // Stop RedundancyMonitor.
      blockManager.setInitializedReplQueues(false);

      // Delete the replica on the first datanode.
      DataNode dn = datanodes.get(0);
      int dnIpcPort = dn.getIpcPort();
      File dnDir = dn.getFSDataset().getVolumeList().get(0).getCurrentDir();
      String[] children = FileUtil.list(dnDir);
      for (String s : children) {
        if (!s.equals("VERSION")) {
          FileUtil.fullyDeleteContents(new File(dnDir, s));
        }
      }

      // The number of replicas is still 3 because the datanode has not sent
      // a new block report.
      FileStatus stat = fs.getFileStatus(filePath);
      BlockLocation[] locs = fs.getFileBlockLocations(stat, 0, stat.getLen());
      assertEquals(3, locs[0].getHosts().length);

      // Restart the first datanode.
      cluster.restartDataNode(0, true);

      // Wait for the block report to be processed.
      cluster.waitDatanodeFullyStarted(cluster.getDataNode(dnIpcPort), 10000);
      cluster.waitFirstBRCompleted(0, 10000);

      // The replica num should be 2.
      locs = fs.getFileBlockLocations(stat, 0, stat.getLen());
      assertEquals(2, locs[0].getHosts().length);
    }
  }

  /**
   * Test processing toInvalidate in block reported, if the block not exists need
   * to set the numBytes of the block to NO_ACK,
   * the DataNode processing will not report incremental blocks.
   */
  @Test(timeout = 360000)
  public void testBlockReportSetNoAckBlockToInvalidate() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 10);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    try (MiniDFSCluster cluster =
             new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
      cluster.waitActive();
      BlockManager blockManager = cluster.getNamesystem().getBlockManager();
      DistributedFileSystem fs = cluster.getFileSystem();
      // Write file.
      Path file = new Path("/test");
      DFSTestUtil.createFile(fs, file, 10240L, (short)1, 0L);
      DFSTestUtil.waitReplication(fs, file, (short) 1);
      LocatedBlock lb = DFSTestUtil.getAllBlocks(fs, file).get(0);
      DatanodeInfo[] loc = lb.getLocations();
      assertEquals(1, loc.length);
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(1, datanodes.size());
      DataNode datanode = datanodes.get(0);
      assertEquals(datanode.getDatanodeUuid(), loc[0].getDatanodeUuid());

      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      // Check the IncrementalBlockReportsNumOps of DataNode, it will be 0.
      assertEquals(1, getLongCounter("IncrementalBlockReportsNumOps", rb));

      // Delete file and remove block.
      fs.delete(file, false);

      // Wait for the processing of the marked deleted block to complete.
      BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(blockManager);
      assertNull(blockManager.getStoredBlock(lb.getBlock().getLocalBlock()));

      // Expire heartbeat on the NameNode,and datanode to be marked dead.
      datanode.setHeartbeatsDisabledForTests(true);
      cluster.setDataNodeDead(datanode.getDatanodeId());
      assertFalse(blockManager.containsInvalidateBlock(loc[0], lb.getBlock().getLocalBlock()));

      // Wait for re-registration and heartbeat.
      datanode.setHeartbeatsDisabledForTests(false);
      final DatanodeDescriptor dn1Desc = cluster.getNamesystem(0)
          .getBlockManager().getDatanodeManager()
          .getDatanode(datanode.getDatanodeId());
      GenericTestUtils.waitFor(
          () -> dn1Desc.isAlive() && dn1Desc.isHeartbeatedSinceRegistration(),
          100, 5000);

      // Trigger BlockReports and block is not exists,
      // it will add invalidateBlocks and set block numBytes be NO_ACK.
      cluster.triggerBlockReports();
      GenericTestUtils.waitFor(
          () -> blockManager.containsInvalidateBlock(loc[0], lb.getBlock().getLocalBlock()),
          100, 1000);

      // Trigger schedule blocks for deletion at datanode.
      int workCount = blockManager.computeInvalidateWork(1);
      assertEquals(1, workCount);
      assertFalse(blockManager.containsInvalidateBlock(loc[0], lb.getBlock().getLocalBlock()));

      // Wait for the blocksRemoved value in DataNode to be 1.
      GenericTestUtils.waitFor(
          () -> datanode.getMetrics().getBlocksRemoved()  == 1,
          100, 5000);

      // Trigger immediate deletion report at datanode.
      cluster.triggerDeletionReports();

      // Delete block numBytes be NO_ACK and will not deletion block report,
      // so check the IncrementalBlockReportsNumOps of DataNode still 1.
      assertEquals(1, getLongCounter("IncrementalBlockReportsNumOps", rb));
    }
  }

  /**
   * Test NameNode should process time out excess redundancy blocks.
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test(timeout = 360000)
  public void testProcessTimedOutExcessBlocks() throws IOException,
      InterruptedException, TimeoutException {
    Configuration config = new HdfsConfiguration();
    // Bump up replication interval.
    config.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 10000);
    // Set the excess redundancy block timeout.
    long timeOut = 60L;
    config.setLong(DFSConfigKeys.DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_SEC_KEY, timeOut);

    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();

    final Semaphore semaphore = new Semaphore(0);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(config).numDataNodes(3).build()) {
      DistributedFileSystem fs = cluster.getFileSystem();
      BlockManager blockManager = cluster.getNameNode().getNamesystem().getBlockManager();
      cluster.waitActive();

      final DataNodeFaultInjector injector = new DataNodeFaultInjector() {
        @Override
        public void delayDeleteReplica() {
          // Lets wait for the remove replica process.
          try {
            semaphore.acquire(1);
          } catch (InterruptedException e) {
            // ignore.
          }
        }
      };
      DataNodeFaultInjector.set(injector);

      // Create file.
      Path path = new Path("/testfile");
      DFSTestUtil.createFile(fs, path, 1024, (short) 3, 0);
      DFSTestUtil.waitReplication(fs, path, (short) 3);
      LocatedBlock lb = DFSTestUtil.getAllBlocks(fs, path).get(0);
      ExtendedBlock extendedBlock = lb.getBlock();
      DatanodeInfo[] loc = lb.getLocations();
      assertEquals(3, loc.length);

      // Set replication as 2, to choose excess.
      fs.setReplication(path, (short) 2);

      // Check excessRedundancyMap and invalidateBlocks size as 1.
      assertEquals(1, blockManager.getExcessBlocksCount());
      assertEquals(1, blockManager.getPendingDeletionBlocksCount());
      DataNode excessDn = Arrays.stream(loc).
          filter(datanodeInfo -> blockManager.getExcessSize4Testing(
              datanodeInfo.getDatanodeUuid()) > 0)
          .map(datanodeInfo -> cluster.getDataNode(datanodeInfo.getIpcPort()))
          .findFirst()
          .orElse(null);

      // Schedule blocks for deletion at excessDn.
      assertEquals(1, blockManager.computeInvalidateWork(1));
      // Check excessRedundancyMap size as 1.
      assertEquals(1, blockManager.getExcessBlocksCount());
      // Check invalidateBlocks size as 0.
      assertEquals(0, blockManager.getPendingDeletionBlocksCount());
      assertNotNull(excessDn);

      // NameNode will ask datanode to delete replicas in heartbeat response.
      cluster.triggerHeartbeats();

      // Wait for the datanode to process any block deletions
      // that have already been asynchronously queued.
      DataNode finalExcessDn = excessDn;
      GenericTestUtils.waitFor(
          () -> cluster.getFsDatasetTestUtils(finalExcessDn).getPendingAsyncDeletions() == 1,
          100, 1000);

      // Restart the datanode.
      int ipcPort = excessDn.getDatanodeId().getIpcPort();
      MiniDFSCluster.DataNodeProperties dataNodeProperties = cluster.stopDataNode(
          excessDn.getDatanodeId().getXferAddr());
      assertTrue(cluster.restartDataNode(dataNodeProperties, true));
      semaphore.release(1);
      cluster.waitActive();

      // Check replica is exists in excessDn.
      excessDn = cluster.getDataNode(ipcPort);
      assertNotNull(cluster.getFsDatasetTestUtils(excessDn).fetchReplica(extendedBlock));
      assertEquals(0, cluster.getFsDatasetTestUtils(excessDn).getPendingAsyncDeletions());

      // Verify excess redundancy blocks have not timed out.
      blockManager.processTimedOutExcessBlocks();
      assertEquals(0, blockManager.getPendingDeletionBlocksCount());

      // Verify excess redundancy block time out.
      Thread.sleep(timeOut * 1000);
      blockManager.processTimedOutExcessBlocks();

      // Check excessRedundancyMap and invalidateBlocks size as 1.
      assertEquals(1, blockManager.getExcessSize4Testing(excessDn.getDatanodeUuid()));
      assertEquals(1, blockManager.getExcessBlocksCount());
      assertEquals(1, blockManager.getPendingDeletionBlocksCount());

      // Schedule blocks for deletion.
      assertEquals(1, blockManager.computeInvalidateWork(1));

      cluster.triggerHeartbeats();

      // Make it resume the removeReplicaFromMem method.
      semaphore.release(1);

      // Wait for the datanode in the cluster to process any block
      // deletions that have already been asynchronously queued
      cluster.waitForDNDeletions();

      // Trigger immediate deletion report.
      cluster.triggerDeletionReports();

      // The replica num should be 2.
      assertEquals(2, DFSTestUtil.getAllBlocks(fs, path).get(0).getLocations().length);
      assertEquals(0, blockManager.getExcessBlocksCount());
    } finally {
      DataNodeFaultInjector.set(oldInjector);
    }
  }

  @Test
  public void testScheduleReconstructionStriped() throws Exception {
    Configuration conf = new HdfsConfiguration();
    NameNode.initMetrics(conf, HdfsServerConstants.NamenodeRole.NAMENODE);
    // RS-3-2 EC policy
    ErasureCodingPolicy ecPolicy =
        SystemErasureCodingPolicies.getPolicies().get(1);
    long blockId = -9223372036854775776L; // real ec block id
    int bcId = 1;
    Block aBlock = new Block(blockId, ecPolicy.getCellSize() * ecPolicy.getNumDataUnits(), 0);
    BlockInfoStriped stripedBlock = new BlockInfoStriped(aBlock, ecPolicy);
    INodeFile inodeFile = mock(INodeFile.class);
    doReturn(inodeFile).when(fsn).getBlockCollection(bcId);
    stripedBlock.setBlockCollectionId(bcId);
    // DataNode
    List<DatanodeStorageInfo> storageList = new ArrayList<>();
    for (int i = 0; i < storages.length; i++) {
      storageList.add(storages[i]);
      // Add node to NetworkTopology
      DatanodeDescriptor node = nodes.get(i);
      node.setAlive(true);
      bm.getDatanodeManager().addDatanode(node);
    }

    // Block Indices
    List<Byte> blockIndices = new ArrayList<>();
    for (int i = 0; i < ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(); i++) {
      blockIndices.add((byte) i);
    }
    ErasureCodingWork ecWork;
    ErasureCodingReplicationWork replWork;

    // Case 1: No blocks are missing.
    // Case 1.1: If no block are lost, we will get no work
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 5),
        blockIndices.subList(0, 5));
    assertNull(bm.scheduleReconstruction(stripedBlock, 0));

    // Case 1.2: If no block is missing, but need rack, will get one
    //   ErasureCodingReplicationWork with one additional replication required
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, rackA.size()),
        blockIndices.subList(0, rackA.size()));
    List<DatanodeStorageInfo> storageInfos = new ArrayList<>();
    for (int i = rackA.size(); i < ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(); i++) {
      int index = storages.length + i;
      DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo(
          String.format("storage%d", index),
          String.format("%d.%d.%d.%d", index, index, index, index),
          storageList.get(0).getDatanodeDescriptor().getNetworkLocation(),
          String.format("host%d", index));
      storageInfos.add(storage);
    }
    addStorageForStripedBlock(stripedBlock, storageInfos, blockIndices.subList(3, 5));
    replWork = (ErasureCodingReplicationWork) bm.scheduleReconstruction(stripedBlock, 0);
    assertEquals(1, replWork.getAdditionalReplRequired());
    assertEquals(1, replWork.getSrcNodes().length);
    assertEquals(5, replWork.getContainingNodes().size());
    assertEquals(5, replWork.getLiveReplicaStorages().size());

    // Case 1.3: If no block is missing, but need rack, but all src is busy , will get no work
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, rackA.size()),
        blockIndices.subList(0, rackA.size()));
    storageInfos.clear();
    for (int i = rackA.size(); i < ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(); i++) {
      int index = storages.length + i;
      DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo(
          String.format("storage%d", index),
          String.format("%d.%d.%d.%d", index, index, index, index),
          storageList.get(0).getDatanodeDescriptor().getNetworkLocation(),
          String.format("host%d", index));
      storageInfos.add(storage);
    }
    addStorageForStripedBlock(stripedBlock, storageInfos, blockIndices.subList(3, 5));
    // make all sources are busy
    Iterator<DatanodeStorageInfo> iterator = stripedBlock.getStorageInfos();
    while(iterator.hasNext()) {
      DatanodeDescriptor dn = iterator.next().getDatanodeDescriptor();
      for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
        dn.incrementPendingReplicationWithoutTargets();
      }
    }
    assertNull(bm.scheduleReconstruction(stripedBlock, 0));
    // release
    iterator = stripedBlock.getStorageInfos();
    while(iterator.hasNext()) {
      DatanodeDescriptor dn = iterator.next().getDatanodeDescriptor();
      for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
        dn.decrementPendingReplicationWithoutTargets();
      }
    }

    // Case 1.4:
    // If no block is missing, but one node is decommissions, will get one
    //   ReplicationWork, and the src node is just the decommissioning node.
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 5),
        blockIndices.subList(0, 5));
    storageList.get(0).getDatanodeDescriptor().startDecommission();
    replWork = (ErasureCodingReplicationWork) bm.scheduleReconstruction(stripedBlock, 0);
    assertEquals(1, replWork.getAdditionalReplRequired());
    assertEquals(1, replWork.getSrcNodes().length);
    assertEquals(storageList.get(0).getDatanodeDescriptor(), replWork.getSrcNodes()[0]);
    assertEquals(5, replWork.getContainingNodes().size());
    assertTrue(replWork.getContainingNodes().containsAll(
        storageList.subList(0, 5).stream().map(s -> s.getDatanodeDescriptor())
            .collect(Collectors.toSet())));
    assertEquals(4, replWork.getLiveReplicaStorages().size());
    assertTrue(replWork.getLiveReplicaStorages().containsAll(storageList.subList(1, 5)));
    updateDataNodeAdminState(storageList.get(0).getDatanodeDescriptor(), AdminStates.NORMAL);

    // Case 1.5: If no block is missing, but five nodes are decommissions, will get five
    // ReplicationWork.
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 5),
        blockIndices.subList(0, 5));
    for (int i = 0; i < 5; i++) {
      storageList.get(i).getDatanodeDescriptor().startDecommission();
    }
    replWork = (ErasureCodingReplicationWork) bm.scheduleReconstruction(stripedBlock, 0);
    assertEquals(5, replWork.getAdditionalReplRequired());
    assertEquals(5, replWork.getSrcNodes().length);
    assertEquals(5, replWork.getSrcIndices().length);
    assertEquals(5, replWork.getContainingNodes().size());
    assertTrue(replWork.getContainingNodes().containsAll(
        storageList.subList(0, 5).stream().map(s -> s.getDatanodeDescriptor())
            .collect(Collectors.toSet())));
    assertEquals(0, replWork.getLiveReplicaStorages().size());
    for (int i = 0; i < 5; i++) {
      assertEquals(storageList.get(i).getDatanodeDescriptor(), replWork.getSrcNodes()[i]);
      assertEquals(blockIndices.get(i), replWork.getSrcIndices()[i]);
    }
    for (int i = 0; i < 5; i++) {
      updateDataNodeAdminState(storageList.get(i).getDatanodeDescriptor(), AdminStates.NORMAL);
    }

    // Case 1.6: If no block is missing, but one nodes is decommissioning and busy, will
    //   get no work
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 5),
        blockIndices.subList(0, 5));
    storageList.get(0).getDatanodeDescriptor().startDecommission();
    for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
      storageList.get(0).getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
    }
    assertNull(bm.scheduleReconstruction(stripedBlock, 0));
    updateDataNodeAdminState(storageList.get(0).getDatanodeDescriptor(), AdminStates.NORMAL);
    for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
      storageList.get(0).getDatanodeDescriptor().decrementPendingReplicationWithoutTargets();
    }

    // Case 1.7: If no block is missing, but three block are busy, and the other block
    //   is decommissioning. we should get replication work.
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 5),
        blockIndices.subList(0, 5));
    storageList.get(0).getDatanodeDescriptor().startDecommission();
    for (int m = 1; m < 4; m++) {
      for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
        storageList.get(m).getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
      }
    }
    replWork = (ErasureCodingReplicationWork) bm.scheduleReconstruction(stripedBlock, 0);
    assertEquals(1, replWork.getAdditionalReplRequired());
    assertEquals(1, replWork.getSrcNodes().length);
    assertEquals(storageList.get(0).getDatanodeDescriptor(), replWork.getSrcNodes()[0]);
    assertEquals(1, replWork.getSrcIndices().length);
    assertEquals(0, (byte) replWork.getSrcIndices()[0]);
    updateDataNodeAdminState(storageList.get(0).getDatanodeDescriptor(), AdminStates.NORMAL);
    for (int m = 1; m < 4; m++) {
      for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
        storageList.get(m).getDatanodeDescriptor().decrementPendingReplicationWithoutTargets();
      }
    }

    // Case 2: One block is missing
    // Case 2.1: If one block is missing, we will get one ErasureCodingWork,
    //   and require one replicas
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 4),
        blockIndices.subList(0, 4));
    ecWork = (ErasureCodingWork) bm.scheduleReconstruction(stripedBlock, 0);
    assertEquals(1, ecWork.getAdditionalReplRequired());
    assertEquals(4, ecWork.getContainingNodes().size());
    assertTrue(ecWork.getContainingNodes().containsAll(
        storageList.subList(0, 4).stream().map(s -> s.getDatanodeDescriptor())
            .collect(Collectors.toSet())));
    assertEquals(4, ecWork.getLiveReplicaStorages().size());
    assertTrue(ecWork.getLiveReplicaStorages().containsAll(storageList.subList(0, 4)));
    assertEquals(4, ecWork.getLiveBlockIndices().length);
    assertTrue(containAllIndices(blockIndices.subList(0, 4), ecWork.getLiveBlockIndices()));
    assertEquals(0, ecWork.getExcludeReconstructedIndices().length);

    // Case 2.2: If one block is missing, the other one is decommissioning, still get one
    //   ErasureCodingWork but not ReplicationWork
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 4),
        blockIndices.subList(0, 4));
    storageList.get(0).getDatanodeDescriptor().startDecommission();
    ecWork = (ErasureCodingWork) bm.scheduleReconstruction(stripedBlock, 0);
    assertEquals(1, ecWork.getAdditionalReplRequired());
    assertEquals(4, ecWork.getContainingNodes().size());
    assertTrue(ecWork.getContainingNodes().containsAll(
        storageList.subList(0, 4).stream().map(s -> s.getDatanodeDescriptor())
            .collect(Collectors.toSet())));
    assertEquals(3, ecWork.getLiveReplicaStorages().size());
    assertTrue(ecWork.getLiveReplicaStorages().containsAll(storageList.subList(1, 4)));
    assertEquals(4, ecWork.getLiveBlockIndices().length);
    assertTrue(containAllIndices(blockIndices.subList(0, 4), ecWork.getLiveBlockIndices()));
    assertEquals(0, ecWork.getExcludeReconstructedIndices().length);
    updateDataNodeAdminState(storageList.get(0).getDatanodeDescriptor(), AdminStates.NORMAL);

    // Case 2.3: If one block is missing, the other one is decommissioning, the other one is
    //   busy, still get one ErasureCodingWork but not ReplicationWork
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 4),
        blockIndices.subList(0, 4));
    storageList.get(0).getDatanodeDescriptor().startDecommission();
    for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
      storageList.get(1).getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
    }
    ecWork = (ErasureCodingWork) bm.scheduleReconstruction(stripedBlock, 0);
    assertEquals(1, ecWork.getAdditionalReplRequired());
    assertEquals(4, ecWork.getContainingNodes().size());
    assertTrue(ecWork.getContainingNodes().containsAll(
        storageList.subList(0, 4).stream().map(s -> s.getDatanodeDescriptor())
            .collect(Collectors.toSet())));
    assertEquals(3, ecWork.getLiveReplicaStorages().size());
    assertTrue(ecWork.getLiveReplicaStorages().containsAll(storageList.subList(1, 4)));
    assertEquals(3, ecWork.getLiveBlockIndices().length);
    List<Byte> subBlockIndices = blockIndices.subList(2, 4);
    subBlockIndices.add(blockIndices.get(0));
    assertTrue(containAllIndices(subBlockIndices, ecWork.getLiveBlockIndices()));
    assertEquals(1, ecWork.getExcludeReconstructedIndices().length);
    assertEquals((byte) blockIndices.get(1), ecWork.getExcludeReconstructedIndices()[0]);
    updateDataNodeAdminState(storageList.get(0).getDatanodeDescriptor(), AdminStates.NORMAL);
    for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
      storageList.get(1).getDatanodeDescriptor().decrementPendingReplicationWithoutTargets();
    }

    // Case 2.4: If one block is missing, the other one is decommissioning, the other two is
    //   busy, still get no work
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 4),
        blockIndices.subList(0, 4));
    storageList.get(2).getDatanodeDescriptor().startDecommission();
    for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
      storageList.get(0).getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
      storageList.get(1).getDatanodeDescriptor().incrementPendingReplicationWithoutTargets();
    }
    assertNull(bm.scheduleReconstruction(stripedBlock, 0));
    updateDataNodeAdminState(storageList.get(2).getDatanodeDescriptor(), AdminStates.NORMAL);
    for (int i = 0; i < bm.getReplicationStreamsHardLimit(); i++) {
      storageList.get(0).getDatanodeDescriptor().decrementPendingReplicationWithoutTargets();
      storageList.get(1).getDatanodeDescriptor().decrementPendingReplicationWithoutTargets();
    }

    // Case 3: If two block are missing, we will get one work, and require two replicas
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 3),
        blockIndices.subList(0, 3));
    ecWork = (ErasureCodingWork) bm.scheduleReconstruction(stripedBlock, 0);
    assertEquals(2, ecWork.getAdditionalReplRequired());
    assertEquals(3, ecWork.getContainingNodes().size());
    assertTrue(ecWork.getContainingNodes().containsAll(
        storageList.subList(0, 3).stream().map(s -> s.getDatanodeDescriptor())
            .collect(Collectors.toSet())));
    assertEquals(3, ecWork.getLiveReplicaStorages().size());
    assertTrue(ecWork.getLiveReplicaStorages().containsAll(storageList.subList(0, 3)));
    assertEquals(3, ecWork.getLiveBlockIndices().length);
    assertTrue(containAllIndices(blockIndices.subList(0, 3), ecWork.getLiveBlockIndices()));
    assertEquals(0, ecWork.getExcludeReconstructedIndices().length);

    // Case 4: If we lost three block, we will get no work.
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 2),
        blockIndices.subList(0, 2));
    assertNull(bm.scheduleReconstruction(stripedBlock, 0));

    // Case 5: If we lost foure block, we will get no work.
    clearStorageForStripedBlock(stripedBlock);
    addStorageForStripedBlock(stripedBlock, storageList.subList(0, 1),
        blockIndices.subList(0, 1));
    assertNull(bm.scheduleReconstruction(stripedBlock, 0));

    // Case 6: All block are missing, we will get no work.
    clearStorageForStripedBlock(stripedBlock);
    assertNull(bm.scheduleReconstruction(stripedBlock, 0));
  }

  private static void clearStorageForStripedBlock(BlockInfoStriped striped) {
    Iterator<DatanodeStorageInfo> iterator = striped.getStorageInfos();
    while (iterator.hasNext()) {
      striped.removeStorage(iterator.next());
    }
  }

  private static void addStorageForStripedBlock(BlockInfoStriped striped,
      List<DatanodeStorageInfo> storages, List<Byte> blockIndices) {
    for (int i = 0; i < storages.size(); i++) {
      striped.addStorage(storages.get(i),
          new Block(blockIndices.get(i) + striped.getBlockId(), 0, 0));
    }
  }

  private static boolean containAllIndices(Collection<Byte> collection, byte[] indices) {
    for (byte index : indices) {
      if (!collection.contains(index)) {
        return false;
      }
    }
    return true;
  }

  private static void updateDataNodeAdminState(DatanodeDescriptor dn, AdminStates state) {
    Whitebox.setInternalState(dn, "adminState", state);
  }
}