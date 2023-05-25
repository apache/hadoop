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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.TestBlockManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests if getblocks request works correctly.
 */
public class TestGetBlocks {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockManager.class);

  private static final int BLOCK_SIZE = 8192;
  private static final String[] RACKS = new String[]{"/d1/r1", "/d1/r1",
      "/d1/r2", "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3"};
  private static final int NUM_DATA_NODES = RACKS.length;

  /**
   * Stop the heartbeat of a datanode in the MiniDFSCluster
   * 
   * @param cluster
   *          The MiniDFSCluster
   * @param hostName
   *          The hostName of the datanode to be stopped
   * @return The DataNode whose heartbeat has been stopped
   */
  private DataNode stopDataNodeHeartbeat(MiniDFSCluster cluster, String hostName) {
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeId().getHostName().equals(hostName)) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
        return dn;
      }
    }
    return null;
  }

  /**
   * Test if the datanodes returned by
   * {@link ClientProtocol#getBlockLocations(String, long, long)} is correct
   * when stale nodes checking is enabled. Also test during the scenario when 1)
   * stale nodes checking is enabled, 2) a writing is going on, 3) a datanode
   * becomes stale happen simultaneously
   * 
   * @throws Exception
   */
  @Test
  public void testReadSelectNonStaleDatanode() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    long staleInterval = 30 * 1000 * 60;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        staleInterval);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATA_NODES).racks(RACKS).build();

    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost",
        cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    List<DatanodeDescriptor> nodeInfoList = cluster.getNameNode()
        .getNamesystem().getBlockManager().getDatanodeManager()
        .getDatanodeListForReport(DatanodeReportType.LIVE);
    assertEquals("Unexpected number of datanodes", NUM_DATA_NODES,
        nodeInfoList.size());
    FileSystem fileSys = cluster.getFileSystem();
    FSDataOutputStream stm = null;
    try {
      // do the writing but do not close the FSDataOutputStream
      // in order to mimic the ongoing writing
      final Path fileName = new Path("/file1");
      stm = fileSys.create(fileName, true,
          fileSys.getConf().getInt(
              CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
          (short) 3, BLOCK_SIZE);
      stm.write(new byte[(BLOCK_SIZE * 3) / 2]);
      // We do not close the stream so that
      // the writing seems to be still ongoing
      stm.hflush();

      LocatedBlocks blocks = client.getNamenode().getBlockLocations(
          fileName.toString(), 0, BLOCK_SIZE);
      DatanodeInfo[] nodes = blocks.get(0).getLocations();
      assertEquals(nodes.length, 3);
      DataNode staleNode = null;
      DatanodeDescriptor staleNodeInfo = null;
      // stop the heartbeat of the first node
      staleNode = this.stopDataNodeHeartbeat(cluster, nodes[0].getHostName());
      assertNotNull(staleNode);
      // set the first node as stale
      staleNodeInfo = cluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager()
          .getDatanode(staleNode.getDatanodeId());
      DFSTestUtil.resetLastUpdatesWithOffset(staleNodeInfo,
          -(staleInterval + 1));

      LocatedBlocks blocksAfterStale = client.getNamenode().getBlockLocations(
          fileName.toString(), 0, BLOCK_SIZE);
      DatanodeInfo[] nodesAfterStale = blocksAfterStale.get(0).getLocations();
      assertEquals(nodesAfterStale.length, 3);
      assertEquals(nodesAfterStale[2].getHostName(), nodes[0].getHostName());

      // restart the staleNode's heartbeat
      DataNodeTestUtils.setHeartbeatsDisabledForTests(staleNode, false);
      // reset the first node as non-stale, so as to avoid two stale nodes
      DFSTestUtil.resetLastUpdatesWithOffset(staleNodeInfo, 0);
      LocatedBlock lastBlock = client.getLocatedBlocks(fileName.toString(), 0,
          Long.MAX_VALUE).getLastLocatedBlock();
      nodes = lastBlock.getLocations();
      assertEquals(nodes.length, 3);
      // stop the heartbeat of the first node for the last block
      staleNode = this.stopDataNodeHeartbeat(cluster, nodes[0].getHostName());
      assertNotNull(staleNode);
      // set the node as stale
      DatanodeDescriptor dnDesc = cluster.getNameNode().getNamesystem()
          .getBlockManager().getDatanodeManager()
          .getDatanode(staleNode.getDatanodeId());
      DFSTestUtil.resetLastUpdatesWithOffset(dnDesc, -(staleInterval + 1));

      LocatedBlock lastBlockAfterStale = client.getLocatedBlocks(
          fileName.toString(), 0, Long.MAX_VALUE).getLastLocatedBlock();
      nodesAfterStale = lastBlockAfterStale.getLocations();
      assertEquals(nodesAfterStale.length, 3);
      assertEquals(nodesAfterStale[2].getHostName(), nodes[0].getHostName());
    } finally {
      if (stm != null) {
        stm.close();
      }
      client.close();
      cluster.shutdown();
    }
  }

  /**
   * Test getBlocks.
   */
  @Test
  public void testGetBlocks() throws Exception {
    DistributedFileSystem fs = null;
    Path testFile = null;
    BlockWithLocations[] locs;
    final int blkSize = 1024;
    final String filePath = "/tmp.txt";
    final int blkLocsSize = 13;
    long fileLen = 12 * blkSize + 1;
    final short replicationFactor = (short) 2;
    final Configuration config = new HdfsConfiguration();

    // set configurations
    config.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blkSize);
    config.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY,
        blkSize);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(replicationFactor)
        .storagesPerDatanode(4)
        .build();

    try {
      cluster.waitActive();
      // the third block will not be visible to getBlocks
      testFile = new Path(filePath);
      DFSTestUtil.createFile(cluster.getFileSystem(), testFile,
          fileLen, replicationFactor, 0L);

      // get blocks & data nodes
      fs = cluster.getFileSystem();
      DFSTestUtil.waitForReplication(fs, testFile, replicationFactor, 60000);
      RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(testFile);
      LocatedFileStatus stat = it.next();
      BlockLocation[] blockLocations = stat.getBlockLocations();
      assertEquals(blkLocsSize, blockLocations.length);
      HdfsDataInputStream dis = (HdfsDataInputStream) fs.open(testFile);
      Collection<LocatedBlock> dinfo = dis.getAllBlocks();
      dis.close();
      DatanodeInfo[] dataNodes = dinfo.iterator().next().getLocations();
      // get RPC client to namenode
      InetSocketAddress addr = new InetSocketAddress("localhost",
          cluster.getNameNodePort());
      NamenodeProtocol namenode = NameNodeProxies.createProxy(config,
          DFSUtilClient.getNNUri(addr), NamenodeProtocol.class).getProxy();

      // Should return all 13 blocks, as minBlockSize is not passed
      locs = namenode.getBlocks(dataNodes[0], fileLen, 0, 0).getBlocks();
      assertEquals(blkLocsSize, locs.length);

      assertEquals(locs[0].getStorageIDs().length, replicationFactor);
      assertEquals(locs[1].getStorageIDs().length, replicationFactor);

      // Should return 12 blocks, as minBlockSize is blkSize
      locs = namenode.getBlocks(dataNodes[0], fileLen, blkSize, 0).getBlocks();
      assertEquals(blkLocsSize - 1, locs.length);
      assertEquals(locs[0].getStorageIDs().length, replicationFactor);
      assertEquals(locs[1].getStorageIDs().length, replicationFactor);

      // get blocks of size BlockSize from dataNodes[0]
      locs = namenode.getBlocks(dataNodes[0], blkSize,
          blkSize, 0).getBlocks();
      assertEquals(locs.length, 1);
      assertEquals(locs[0].getStorageIDs().length, replicationFactor);

      // get blocks of size 1 from dataNodes[0]
      locs = namenode.getBlocks(dataNodes[0], 1, 1, 0).getBlocks();
      assertEquals(locs.length, 1);
      assertEquals(locs[0].getStorageIDs().length, replicationFactor);

      // get blocks of size 0 from dataNodes[0]
      getBlocksWithException(namenode, dataNodes[0], 0, 0,
          RemoteException.class, "IllegalArgumentException");

      // get blocks of size -1 from dataNodes[0]
      getBlocksWithException(namenode, dataNodes[0], -1, 0,
          RemoteException.class, "IllegalArgumentException");

      // minBlockSize is -1
      getBlocksWithException(namenode, dataNodes[0], blkSize, -1,
          RemoteException.class, "IllegalArgumentException");

      // get blocks of size BlockSize from a non-existent datanode
      DatanodeInfo info = DFSTestUtil.getDatanodeInfo("1.2.3.4");
      getBlocksWithException(namenode, info, replicationFactor, 0,
          RemoteException.class, "HadoopIllegalArgumentException");

      testBlockIterator(cluster);

      // Namenode should refuse to provide block locations to the balancer
      // while in safemode.
      locs = namenode.getBlocks(dataNodes[0], fileLen, 0, 0).getBlocks();
      assertEquals(blkLocsSize, locs.length);
      assertFalse(fs.isInSafeMode());
      LOG.info("Entering safe mode");
      fs.setSafeMode(SafeModeAction.ENTER);
      LOG.info("Entered safe mode");
      assertTrue(fs.isInSafeMode());
      getBlocksWithException(namenode, info, replicationFactor, 0,
          RemoteException.class,
          "Cannot execute getBlocks. Name node is in safe mode.");
      fs.setSafeMode(SafeModeAction.LEAVE);
      assertFalse(fs.isInSafeMode());
    }  finally {
      if (fs != null) {
        fs.delete(testFile, true);
        fs.close();
      }
      cluster.shutdown();
    }
  }

  private void getBlocksWithException(NamenodeProtocol namenode,
      DatanodeInfo datanode, long size, long minBlkSize, Class exClass,
      String msg) throws Exception {

    // Namenode should refuse should fail
    LambdaTestUtils.intercept(exClass,
        msg, () -> namenode.getBlocks(datanode, size, minBlkSize, 0));
  }

  /**
   * BlockIterator iterates over all blocks belonging to DatanodeDescriptor
   * through multiple storages.
   * The test verifies that BlockIterator can be set to start iterating from
   * a particular starting block index.
   */
  void testBlockIterator(MiniDFSCluster cluster) {
    FSNamesystem ns = cluster.getNamesystem();
    String dId = cluster.getDataNodes().get(0).getDatanodeUuid();
    DatanodeDescriptor dnd = BlockManagerTestUtil.getDatanode(ns, dId);
    DatanodeStorageInfo[] storages = dnd.getStorageInfos();
    assertEquals("DataNode should have 4 storages", 4, storages.length);

    Iterator<BlockInfo> dnBlockIt = null;
    // check illegal start block number
    try {
      dnBlockIt = BlockManagerTestUtil.getBlockIterator(
          cluster.getNamesystem(), dId, -1);
      assertTrue("Should throw IllegalArgumentException", false);
    } catch(IllegalArgumentException ei) {
      // as expected
    }
    assertNull("Iterator should be null", dnBlockIt);

    // form an array of all DataNode blocks
    int numBlocks = dnd.numBlocks();
    BlockInfo[] allBlocks = new BlockInfo[numBlocks];
    int idx = 0;
    for(DatanodeStorageInfo s : storages) {
      Iterator<BlockInfo> storageBlockIt =
          BlockManagerTestUtil.getBlockIterator(s);
      while(storageBlockIt.hasNext()) {
        allBlocks[idx++] = storageBlockIt.next();
        try {
          storageBlockIt.remove();
          assertTrue(
              "BlockInfo iterator should have been unmodifiable", false);
        } catch (UnsupportedOperationException e) {
          //expected exception
        }
      }
    }

    // check iterator for every block as a starting point
    for(int i = 0; i < allBlocks.length; i++) {
      // create iterator starting from i
      dnBlockIt = BlockManagerTestUtil.getBlockIterator(ns, dId, i);
      assertTrue("Block iterator should have next block", dnBlockIt.hasNext());
      // check iterator lists blocks in the desired order
      for(int j = i; j < allBlocks.length; j++) {
        assertEquals("Wrong block order", allBlocks[j], dnBlockIt.next());
      }
    }

    // check start block number larger than numBlocks in the DataNode
    dnBlockIt = BlockManagerTestUtil.getBlockIterator(
        ns, dId, allBlocks.length + 1);
    assertFalse("Iterator should not have next block", dnBlockIt.hasNext());
  }

  @Test
  public void testBlockKey() {
    Map<Block, Long> map = new HashMap<Block, Long>();
    final Random RAN = new Random();
    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    long[] blkids = new long[10];
    for (int i = 0; i < blkids.length; i++) {
      blkids[i] = 1000L + RAN.nextInt(100000);
      map.put(new Block(blkids[i], 0, blkids[i]), blkids[i]);
    }
    System.out.println("map=" + map.toString().replace(",", "\n  "));

    for (int i = 0; i < blkids.length; i++) {
      Block b = new Block(blkids[i], 0,
          HdfsConstants.GRANDFATHER_GENERATION_STAMP);
      Long v = map.get(b);
      System.out.println(b + " => " + v);
      assertEquals(blkids[i], v.longValue());
    }
  }

  private boolean belongToFile(BlockWithLocations blockWithLocations,
                               List<LocatedBlock> blocks) {
    for(LocatedBlock block : blocks) {
      if (block.getBlock().getLocalBlock().equals(
          blockWithLocations.getBlock())) {
        return true;
      }
    }
    return false;
  }

  /**
   * test GetBlocks with dfs.namenode.hot.block.interval.
   * Balancer prefer to get blocks which are belong to the cold files
   * created before this time period.
   */
  @Test
  public void testGetBlocksWithHotBlockTimeInterval() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final short repFactor = (short) 1;
    final int blockNum = 2;
    final int fileLen = BLOCK_SIZE * blockNum;
    final long hotInterval = 2000;

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(repFactor).build();
    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      final DFSClient dfsclient = ((DistributedFileSystem) fs).getClient();

      String fileOld = "/f.old";
      DFSTestUtil.createFile(fs, new Path(fileOld), fileLen, repFactor, 0);

      List<LocatedBlock> locatedBlocksOld = dfsclient.getNamenode().
          getBlockLocations(fileOld, 0, fileLen).getLocatedBlocks();
      DatanodeInfo[] dataNodes = locatedBlocksOld.get(0).getLocations();

      InetSocketAddress addr = new InetSocketAddress("localhost",
          cluster.getNameNodePort());
      NamenodeProtocol namenode = NameNodeProxies.createProxy(conf,
          DFSUtilClient.getNNUri(addr), NamenodeProtocol.class).getProxy();

      // make the file as old.
      dfsclient.getNamenode().setTimes(fileOld, 0, 0);

      String fileNew = "/f.new";
      DFSTestUtil.createFile(fs, new Path(fileNew), fileLen, repFactor, 0);
      List<LocatedBlock> locatedBlocksNew = dfsclient.getNamenode()
          .getBlockLocations(fileNew, 0, fileLen).getLocatedBlocks();

      BlockWithLocations[] locsAll = namenode.getBlocks(
          dataNodes[0], fileLen*2, 0, hotInterval).getBlocks();
      assertEquals(locsAll.length, 4);

      for(int i = 0; i < blockNum; i++) {
        assertTrue(belongToFile(locsAll[i], locatedBlocksOld));
      }
      for(int i = blockNum; i < blockNum*2; i++) {
        assertTrue(belongToFile(locsAll[i], locatedBlocksNew));
      }

      BlockWithLocations[]  locs2 = namenode.getBlocks(
          dataNodes[0], fileLen*2, 0, hotInterval).getBlocks();
      for(int i = 0; i < 2; i++) {
        assertTrue(belongToFile(locs2[i], locatedBlocksOld));
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testReadSkipStaleStorage() throws Exception {
    final short repFactor = (short) 1;
    final int blockNum = 64;
    final int storageNum = 2;
    final int fileLen = BLOCK_SIZE * blockNum;
    final Path path = new Path("testReadSkipStaleStorage");
    final Configuration conf = new HdfsConfiguration();

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storagesPerDatanode(storageNum)
        .build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, path, false, 1024, fileLen,
        BLOCK_SIZE, repFactor, 0, true);

    // get datanode info
    ClientProtocol client = NameNodeProxies.createProxy(conf,
        cluster.getFileSystem(0).getUri(),
        ClientProtocol.class).getProxy();
    DatanodeInfo[] dataNodes = client.getDatanodeReport(DatanodeReportType.ALL);

    // get storage info
    BlockManager bm0 = cluster.getNamesystem(0).getBlockManager();
    DatanodeStorageInfo[] storageInfos = bm0.getDatanodeManager()
        .getDatanode(dataNodes[0].getDatanodeUuid()).getStorageInfos();

    InetSocketAddress addr = new InetSocketAddress("localhost",
        cluster.getNameNodePort());
    NamenodeProtocol namenode = NameNodeProxies.createProxy(conf,
        DFSUtilClient.getNNUri(addr), NamenodeProtocol.class).getProxy();

    // check blocks count equals to blockNum
    BlockWithLocations[] blocks = namenode.getBlocks(
        dataNodes[0], fileLen*2, 0, 0).getBlocks();
    assertEquals(blockNum, blocks.length);

    // calculate the block count on storage[0]
    int count = 0;
    for (BlockWithLocations b : blocks) {
      for (String s : b.getStorageIDs()) {
        if (s.equals(storageInfos[0].getStorageID())) {
          count++;
        }
      }
    }

    // set storage[0] stale
    storageInfos[0].setBlockContentsStale(true);
    blocks = namenode.getBlocks(
        dataNodes[0], fileLen*2, 0, 0).getBlocks();
    assertEquals(blockNum - count, blocks.length);

    // set all storage stale
    bm0.getDatanodeManager().markAllDatanodesStale();
    blocks = namenode.getBlocks(
        dataNodes[0], fileLen*2, 0, 0).getBlocks();
    assertEquals(0, blocks.length);
  }
}