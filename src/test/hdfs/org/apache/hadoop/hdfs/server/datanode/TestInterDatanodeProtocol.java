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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaUnderRecovery.Info;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.junit.Assert;
import org.junit.Test;

/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestInterDatanodeProtocol extends junit.framework.TestCase {
  public static void checkMetaInfo(Block b, InterDatanodeProtocol idp,
      DataBlockScanner scanner) throws IOException {
    BlockMetaDataInfo metainfo = idp.getBlockMetaDataInfo(b);
    assertEquals(b.getBlockId(), metainfo.getBlockId());
    assertEquals(b.getNumBytes(), metainfo.getNumBytes());
    if (scanner != null) {
      assertEquals(scanner.getLastScanTime(b),
          metainfo.getLastScanTime());
    }
  }

  public static LocatedBlock getLastLocatedBlock(
      ClientProtocol namenode, String src
  ) throws IOException {
    //get block info for the last block
    LocatedBlocks locations = namenode.getBlockLocations(src, 0, Long.MAX_VALUE);
    List<LocatedBlock> blocks = locations.getLocatedBlocks();
    DataNode.LOG.info("blocks.size()=" + blocks.size());
    assertTrue(blocks.size() > 0);

    return blocks.get(blocks.size() - 1);
  }

  /**
   * The following test first creates a file.
   * It verifies the block information from a datanode.
   * Then, it updates the block with new information and verifies again. 
   */
  public void testBlockMetaDataInfo() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 3, true, null);
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, 1024L, (short)3, 0L);
      assertTrue(dfs.getClient().exists(filestr));

      //get block info
      LocatedBlock locatedblock = getLastLocatedBlock(dfs.getClient().getNamenode(), filestr);
      DatanodeInfo[] datanodeinfo = locatedblock.getLocations();
      assertTrue(datanodeinfo.length > 0);

      //connect to a data node
      InterDatanodeProtocol idp = DataNode.createInterDataNodeProtocolProxy(
          datanodeinfo[0], conf);
      DataNode datanode = cluster.getDataNode(datanodeinfo[0].getIpcPort());
      assertTrue(datanode != null);
      
      //stop block scanner, so we could compare lastScanTime
      datanode.blockScannerThread.interrupt();

      //verify BlockMetaDataInfo
      Block b = locatedblock.getBlock();
      InterDatanodeProtocol.LOG.info("b=" + b + ", " + b.getClass());
      checkMetaInfo(b, idp, datanode.blockScanner);

      //verify updateBlock
      Block newblock = new Block(
          b.getBlockId(), b.getNumBytes()/2, b.getGenerationStamp()+1);
      idp.updateBlock(b, newblock, false);
      checkMetaInfo(newblock, idp, datanode.blockScanner);
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  private static ReplicaInfo createReplicaInfo(Block b) {
    return new ReplicaBeingWritten(b.getBlockId(), b.getGenerationStamp(),
        null, null);
  }

  private static void assertEquals(ReplicaInfo originalInfo, Info recoveryInfo) {
    Assert.assertEquals(originalInfo.getBlockId(), recoveryInfo.getBlockId());
    Assert.assertEquals(originalInfo.getGenerationStamp(), recoveryInfo.getGenerationStamp());
    Assert.assertEquals(originalInfo.getBytesOnDisk(), recoveryInfo.getNumBytes());
    Assert.assertEquals(originalInfo.getState(), recoveryInfo.getOriginalReplicaState());
  }

  /** Test {@link FSDataset#initReplicaRecovery(ReplicasMap, Block, long)} */
  @Test
  public void testInitReplicaRecovery() throws IOException {
    final long firstblockid = 10000L;
    final long gs = 7777L;
    final long length = 22L;
    final ReplicasMap map = new ReplicasMap();
    final Block[] blocks = new Block[5];
    for(int i = 0; i < blocks.length; i++) {
      blocks[i] = new Block(firstblockid + i, length, gs);
      map.add(createReplicaInfo(blocks[i]));
    }
    
    { 
      //normal case
      final Block b = blocks[0];
      final ReplicaInfo originalInfo = map.get(b);

      final long recoveryid = gs + 1;
      final Info recoveryInfo = FSDataset.initReplicaRecovery(map, blocks[0], recoveryid);
      assertEquals(originalInfo, recoveryInfo);

      final ReplicaUnderRecovery updatedInfo = (ReplicaUnderRecovery)map.get(b);
      Assert.assertEquals(originalInfo.getBlockId(), updatedInfo.getBlockId());
      Assert.assertEquals(recoveryid, updatedInfo.getRecoveryID());

      //recover one more time 
      final long recoveryid2 = gs + 2;
      final Info recoveryInfo2 = FSDataset.initReplicaRecovery(map, blocks[0], recoveryid2);
      assertEquals(originalInfo, recoveryInfo2);

      final ReplicaUnderRecovery updatedInfo2 = (ReplicaUnderRecovery)map.get(b);
      Assert.assertEquals(originalInfo.getBlockId(), updatedInfo2.getBlockId());
      Assert.assertEquals(recoveryid2, updatedInfo2.getRecoveryID());
      
      //case RecoveryInProgressException
      try {
        FSDataset.initReplicaRecovery(map, b, recoveryid);
        Assert.fail();
      }
      catch(RecoveryInProgressException ripe) {
        System.out.println("GOOD: getting " + ripe);
      }
    }

    { //replica not found
      final long recoveryid = gs + 1;
      final Block b = new Block(firstblockid - 1, length, gs);
      try {
        FSDataset.initReplicaRecovery(map, b, recoveryid);
        Assert.fail();
      }
      catch(ReplicaNotFoundException rnfe) {
        System.out.println("GOOD: getting " + rnfe);
      }
    }
    
    { //case "THIS IS NOT SUPPOSED TO HAPPEN"
      final long recoveryid = gs - 1;
      final Block b = new Block(firstblockid + 1, length, gs);
      try {
        FSDataset.initReplicaRecovery(map, b, recoveryid);
        Assert.fail();
      }
      catch(IOException ioe) {
        System.out.println("GOOD: getting " + ioe);
      }
    }

  }

  /** Test {@link FSDataset#updateReplicaUnderRecovery(ReplicaUnderRecovery, long, long)} */
  @Test
  public void testUpdateReplicaUnderRecovery() throws IOException {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 3, true, null);
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, 1024L, (short)3, 0L);

      //get block info
      final LocatedBlock locatedblock = getLastLocatedBlock(
          dfs.getClient().getNamenode(), filestr);
      final DatanodeInfo[] datanodeinfo = locatedblock.getLocations();
      Assert.assertTrue(datanodeinfo.length > 0);

      //get DataNode and FSDataset objects
      final DataNode datanode = cluster.getDataNode(datanodeinfo[0].getIpcPort());
      Assert.assertTrue(datanode != null);
      Assert.assertTrue(datanode.data instanceof FSDataset);
      final FSDataset fsdataset = (FSDataset)datanode.data;

      //initReplicaRecovery
      final Block b = locatedblock.getBlock();
      final long recoveryid = b.getGenerationStamp() + 1;
      final long newlength = b.getNumBytes() - 1;
      FSDataset.initReplicaRecovery(fsdataset.volumeMap, b, recoveryid);

      //check replica
      final ReplicaInfo replica = fsdataset.getReplica(b.getBlockId());
      Assert.assertTrue(replica instanceof ReplicaUnderRecovery);
      final ReplicaUnderRecovery rur = (ReplicaUnderRecovery)replica;

      //check meta data before update
      FSDataset.checkReplicaFiles(rur);

      //update
      final FinalizedReplica finalized = fsdataset.updateReplicaUnderRecovery(
          rur, recoveryid, newlength);

      //check meta data after update
      FSDataset.checkReplicaFiles(finalized);
      Assert.assertEquals(b.getBlockId(), finalized.getBlockId());
      Assert.assertEquals(recoveryid, finalized.getGenerationStamp());
      Assert.assertEquals(newlength, finalized.getNumBytes());

    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }
}
