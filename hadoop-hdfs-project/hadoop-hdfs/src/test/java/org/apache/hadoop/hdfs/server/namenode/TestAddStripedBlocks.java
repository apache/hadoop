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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStripedUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestAddStripedBlocks {
  private final short GROUP_SIZE = HdfsConstants.NUM_DATA_BLOCKS +
      HdfsConstants.NUM_PARITY_BLOCKS;

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  @Before
  public void setup() throws IOException {
    cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
        .numDataNodes(GROUP_SIZE).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfs.setStoragePolicy(new Path("/"), HdfsConstants.EC_STORAGE_POLICY_NAME);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAddStripedBlock() throws Exception {
    final Path file = new Path("/file1");
    // create an empty file
    FSDataOutputStream out = null;
    try {
      out = dfs.create(file, (short) 1);

      FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
      INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();
      LocatedBlock newBlock = cluster.getNamesystem().getAdditionalBlock(
          file.toString(), fileNode.getId(), dfs.getClient().getClientName(),
          null, null, null);
      assertEquals(GROUP_SIZE, newBlock.getLocations().length);
      assertEquals(GROUP_SIZE, newBlock.getStorageIDs().length);

      BlockInfo[] blocks = fileNode.getBlocks();
      assertEquals(1, blocks.length);
      Assert.assertTrue(blocks[0].isStriped());

      checkStripedBlockUC((BlockInfoStriped) fileNode.getLastBlock(), true);
    } finally {
      IOUtils.cleanup(null, out);
    }

    // restart NameNode to check editlog
    cluster.restartNameNode(true);
    FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
    INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();
    BlockInfo[] blocks = fileNode.getBlocks();
    assertEquals(1, blocks.length);
    Assert.assertTrue(blocks[0].isStriped());
    checkStripedBlockUC((BlockInfoStriped) fileNode.getLastBlock(), false);

    // save namespace, restart namenode, and check
    dfs = cluster.getFileSystem();
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    dfs.saveNamespace();
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(true);
    fsdir = cluster.getNamesystem().getFSDirectory();
    fileNode = fsdir.getINode4Write(file.toString()).asFile();
    blocks = fileNode.getBlocks();
    assertEquals(1, blocks.length);
    Assert.assertTrue(blocks[0].isStriped());
    checkStripedBlockUC((BlockInfoStriped) fileNode.getLastBlock(), false);
  }

  private void checkStripedBlockUC(BlockInfoStriped block,
      boolean checkReplica) {
    assertEquals(0, block.numNodes());
    Assert.assertFalse(block.isComplete());
    Assert.assertEquals(HdfsConstants.NUM_DATA_BLOCKS, block.getDataBlockNum());
    Assert.assertEquals(HdfsConstants.NUM_PARITY_BLOCKS,
        block.getParityBlockNum());
    Assert.assertEquals(0,
        block.getBlockId() & HdfsConstants.BLOCK_GROUP_INDEX_MASK);

    final BlockInfoStripedUnderConstruction blockUC =
        (BlockInfoStripedUnderConstruction) block;
    Assert.assertEquals(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
        blockUC.getBlockUCState());
    if (checkReplica) {
      Assert.assertEquals(GROUP_SIZE, blockUC.getNumExpectedLocations());
      DatanodeStorageInfo[] storages = blockUC.getExpectedStorageLocations();
      for (DataNode dn : cluster.getDataNodes()) {
        Assert.assertTrue(includeDataNode(dn.getDatanodeId(), storages));
      }
    }
  }

  private boolean includeDataNode(DatanodeID dn, DatanodeStorageInfo[] storages) {
    for (DatanodeStorageInfo storage : storages) {
      if (storage.getDatanodeDescriptor().equals(dn)) {
        return true;
      }
    }
    return false;
  }
}
