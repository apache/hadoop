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

import java.util.UUID;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.VersionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * Test if FSNamesystem handles heartbeat right
 */
public class TestComputeInvalidateWork {

  private Configuration conf;
  private final int NUM_OF_DATANODES = 3;
  private MiniDFSCluster cluster;
  private FSNamesystem namesystem;
  private BlockManager bm;
  private DatanodeDescriptor[] nodes;

  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES)
        .build();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    bm = namesystem.getBlockManager();
    nodes = bm.getDatanodeManager().getHeartbeatManager().getDatanodes();
    assertEquals(nodes.length, NUM_OF_DATANODES);
  }

  @After
  public void teardown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test if {@link BlockManager#computeInvalidateWork(int)}
   * can schedule invalidate work correctly 
   */
  @Test(timeout=120000)
  public void testCompInvalidate() throws Exception {
    final int blockInvalidateLimit = bm.getDatanodeManager()
        .blockInvalidateLimit;
    namesystem.writeLock();
    try {
      for (int i=0; i<nodes.length; i++) {
        for(int j=0; j<3*blockInvalidateLimit+1; j++) {
          Block block = new Block(i*(blockInvalidateLimit+1)+j, 0,
              GenerationStamp.LAST_RESERVED_STAMP);
          bm.addToInvalidates(block, nodes[i]);
        }
      }
      
      assertEquals(blockInvalidateLimit*NUM_OF_DATANODES,
          bm.computeInvalidateWork(NUM_OF_DATANODES+1));
      assertEquals(blockInvalidateLimit*NUM_OF_DATANODES,
          bm.computeInvalidateWork(NUM_OF_DATANODES));
      assertEquals(blockInvalidateLimit*(NUM_OF_DATANODES-1),
          bm.computeInvalidateWork(NUM_OF_DATANODES-1));
      int workCount = bm.computeInvalidateWork(1);
      if (workCount == 1) {
        assertEquals(blockInvalidateLimit+1, bm.computeInvalidateWork(2));
      } else {
        assertEquals(workCount, blockInvalidateLimit);
        assertEquals(2, bm.computeInvalidateWork(2));
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Reformatted DataNodes will replace the original UUID in the
   * {@link DatanodeManager#datanodeMap}. This tests if block
   * invalidation work on the original DataNode can be skipped.
   */
  @Test(timeout=120000)
  public void testDatanodeReformat() throws Exception {
    namesystem.writeLock();
    try {
      // Change the datanode UUID to emulate a reformat
      String poolId = cluster.getNamesystem().getBlockPoolId();
      DatanodeRegistration dnr = cluster.getDataNode(nodes[0].getIpcPort())
                                        .getDNRegistrationForBP(poolId);
      dnr = new DatanodeRegistration(UUID.randomUUID().toString(), dnr);
      cluster.stopDataNode(nodes[0].getXferAddr());

      Block block = new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP);
      bm.addToInvalidates(block, nodes[0]);
      bm.getDatanodeManager().registerDatanode(dnr);

      // Since UUID has changed, the invalidation work should be skipped
      assertEquals(0, bm.computeInvalidateWork(1));
      assertEquals(0, bm.getPendingDeletionBlocksCount());
    } finally {
      namesystem.writeUnlock();
    }
  }

  @Test(timeout=12000)
  public void testDatanodeReRegistration() throws Exception {
    // Create a test file
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final Path path = new Path("/testRR");
    // Create a file and shutdown the DNs, which populates InvalidateBlocks
    DFSTestUtil.createFile(dfs, path, dfs.getDefaultBlockSize(),
        (short) NUM_OF_DATANODES, 0xED0ED0);
    for (DataNode dn : cluster.getDataNodes()) {
      dn.shutdown();
    }
    dfs.delete(path, false);
    namesystem.writeLock();
    InvalidateBlocks invalidateBlocks;
    int expected = NUM_OF_DATANODES;
    try {
      invalidateBlocks = (InvalidateBlocks) Whitebox
          .getInternalState(cluster.getNamesystem().getBlockManager(),
              "invalidateBlocks");
      assertEquals("Expected invalidate blocks to be the number of DNs",
          (long) expected, invalidateBlocks.numBlocks());
    } finally {
      namesystem.writeUnlock();
    }
    // Re-register each DN and see that it wipes the invalidation work
    for (DataNode dn : cluster.getDataNodes()) {
      DatanodeID did = dn.getDatanodeId();
      DatanodeRegistration reg = new DatanodeRegistration(
          new DatanodeID(UUID.randomUUID().toString(), did),
          new StorageInfo(HdfsServerConstants.NodeType.DATA_NODE),
          new ExportedBlockKeys(),
          VersionInfo.getVersion());
      namesystem.writeLock();
      try {
        bm.getDatanodeManager().registerDatanode(reg);
        expected--;
        assertEquals("Expected number of invalidate blocks to decrease",
            (long) expected, invalidateBlocks.numBlocks());
      } finally {
          namesystem.writeUnlock();
      }
    }
  }
}
