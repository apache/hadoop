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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.junit.Assert;
import org.junit.Test;

/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestInterDatanodeProtocol {
  public static void checkMetaInfo(Block b, DataNode dn) throws IOException {
    Block metainfo = dn.data.getStoredBlock(b.getBlockId());
    Assert.assertEquals(b.getBlockId(), metainfo.getBlockId());
    Assert.assertEquals(b.getNumBytes(), metainfo.getNumBytes());
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
  @Test
  public void testBlockMetaDataInfo() throws Exception {
    Configuration conf = new HdfsConfiguration();
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
      checkMetaInfo(b, datanode);
      long recoveryId = b.getGenerationStamp() + 1;
      idp.initReplicaRecovery(
          new RecoveringBlock(b, locatedblock.getLocations(), recoveryId));

      //verify updateBlock
      Block newblock = new Block(
          b.getBlockId(), b.getNumBytes()/2, b.getGenerationStamp()+1);
      idp.updateReplicaUnderRecovery(b, recoveryId, newblock.getNumBytes());
      checkMetaInfo(newblock, datanode);
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  private static ReplicaInfo createReplicaInfo(Block b) {
    return new FinalizedReplica(b, null, null);
  }

  private static void assertEquals(ReplicaInfo originalInfo, ReplicaRecoveryInfo recoveryInfo) {
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
      final ReplicaRecoveryInfo recoveryInfo = FSDataset.initReplicaRecovery(map, blocks[0], recoveryid);
      assertEquals(originalInfo, recoveryInfo);

      final ReplicaUnderRecovery updatedInfo = (ReplicaUnderRecovery)map.get(b);
      Assert.assertEquals(originalInfo.getBlockId(), updatedInfo.getBlockId());
      Assert.assertEquals(recoveryid, updatedInfo.getRecoveryID());

      //recover one more time 
      final long recoveryid2 = gs + 2;
      final ReplicaRecoveryInfo recoveryInfo2 = FSDataset.initReplicaRecovery(map, blocks[0], recoveryid2);
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

    { // BlockRecoveryFI_01: replica not found
      final long recoveryid = gs + 1;
      final Block b = new Block(firstblockid - 1, length, gs);
      ReplicaRecoveryInfo r = FSDataset.initReplicaRecovery(map, b, recoveryid);
      Assert.assertNull("Data-node should not have this replica.", r);
    }
    
    { // BlockRecoveryFI_02: "THIS IS NOT SUPPOSED TO HAPPEN" with recovery id < gs  
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

    // BlockRecoveryFI_03: Replica's gs is less than the block's gs
    {
      final long recoveryid = gs + 1;
      final Block b = new Block(firstblockid, length, gs+1);
      try {
        FSDataset.initReplicaRecovery(map, b, recoveryid);
        fail("InitReplicaRecovery should fail because replica's " +
        		"gs is less than the block's gs");
      } catch (IOException e) {
        e.getMessage().startsWith(
           "replica.getGenerationStamp() < block.getGenerationStamp(), block=");
      }
    }
  }

  /** Test {@link FSDataset#updateReplicaUnderRecovery(ReplicaUnderRecovery, long, long)} */
  @Test
  public void testUpdateReplicaUnderRecovery() throws IOException {
    final Configuration conf = new HdfsConfiguration();
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
      final ReplicaRecoveryInfo rri = fsdataset.initReplicaRecovery(
          new RecoveringBlock(b, null, recoveryid));

      //check replica
      final ReplicaInfo replica = fsdataset.fetchReplicaInfo(b.getBlockId());
      Assert.assertEquals(ReplicaState.RUR, replica.getState());

      //check meta data before update
      FSDataset.checkReplicaFiles(replica);

      //case "THIS IS NOT SUPPOSED TO HAPPEN"
      //with (block length) != (stored replica's on disk length). 
      {
        //create a block with same id and gs but different length.
        final Block tmp = new Block(rri.getBlockId(), rri.getNumBytes() - 1,
            rri.getGenerationStamp());
        try {
          //update should fail
          fsdataset.updateReplicaUnderRecovery(tmp, recoveryid, newlength);
          Assert.fail();
        } catch(IOException ioe) {
          System.out.println("GOOD: getting " + ioe);
        }
      }

      //update
      final ReplicaInfo finalized = fsdataset.updateReplicaUnderRecovery(
          rri, recoveryid, newlength);

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
