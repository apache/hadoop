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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.Test;

/**
 * This class tests the internals of PendingReplicationBlocks.java,
 * as well as how PendingReplicationBlocks acts in BlockManager
 */
public class TestPendingReplication {
  final static int TIMEOUT = 3;     // 3 seconds
  private static final int DFS_REPLICATION_INTERVAL = 1;
  // Number of datanodes in the cluster
  private static final int DATANODE_COUNT = 5;

  @Test
  public void testPendingReplication() {
    PendingReplicationBlocks pendingReplications;
    pendingReplications = new PendingReplicationBlocks(TIMEOUT * 1000);
    pendingReplications.start();

    //
    // Add 10 blocks to pendingReplications.
    //
    for (int i = 0; i < 10; i++) {
      Block block = new Block(i, i, 0);
      pendingReplications.increment(block, i);
    }
    assertEquals("Size of pendingReplications ",
                 10, pendingReplications.size());


    //
    // remove one item and reinsert it
    //
    Block blk = new Block(8, 8, 0);
    pendingReplications.decrement(blk);             // removes one replica
    assertEquals("pendingReplications.getNumReplicas ",
                 7, pendingReplications.getNumReplicas(blk));

    for (int i = 0; i < 7; i++) {
      pendingReplications.decrement(blk);           // removes all replicas
    }
    assertTrue(pendingReplications.size() == 9);
    pendingReplications.increment(blk, 8);
    assertTrue(pendingReplications.size() == 10);

    //
    // verify that the number of replicas returned
    // are sane.
    //
    for (int i = 0; i < 10; i++) {
      Block block = new Block(i, i, 0);
      int numReplicas = pendingReplications.getNumReplicas(block);
      assertTrue(numReplicas == i);
    }

    //
    // verify that nothing has timed out so far
    //
    assertTrue(pendingReplications.getTimedOutBlocks() == null);

    //
    // Wait for one second and then insert some more items.
    //
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }

    for (int i = 10; i < 15; i++) {
      Block block = new Block(i, i, 0);
      pendingReplications.increment(block, i);
    }
    assertTrue(pendingReplications.size() == 15);

    //
    // Wait for everything to timeout.
    //
    int loop = 0;
    while (pendingReplications.size() > 0) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      loop++;
    }
    System.out.println("Had to wait for " + loop +
                       " seconds for the lot to timeout");

    //
    // Verify that everything has timed out.
    //
    assertEquals("Size of pendingReplications ",
                 0, pendingReplications.size());
    Block[] timedOut = pendingReplications.getTimedOutBlocks();
    assertTrue(timedOut != null && timedOut.length == 15);
    for (int i = 0; i < timedOut.length; i++) {
      assertTrue(timedOut[i].getBlockId() < 15);
    }
    pendingReplications.stop();
  }
  
  /**
   * Test if BlockManager can correctly remove corresponding pending records
   * when a file is deleted
   * 
   * @throws Exception
   */
  @Test
  public void testPendingAndInvalidate() throws Exception {
    final Configuration CONF = new HdfsConfiguration();
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_REPLICATION_INTERVAL);
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 
        DFS_REPLICATION_INTERVAL);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(
        DATANODE_COUNT).build();
    cluster.waitActive();
    
    FSNamesystem namesystem = cluster.getNamesystem();
    BlockManager bm = namesystem.getBlockManager();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      // 1. create a file
      Path filePath = new Path("/tmp.txt");
      DFSTestUtil.createFile(fs, filePath, 1024, (short) 3, 0L);
      
      // 2. disable the heartbeats
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
      }
      
      // 3. mark a couple of blocks as corrupt
      LocatedBlock block = NameNodeAdapter.getBlockLocations(
          cluster.getNameNode(), filePath.toString(), 0, 1).get(0);
      cluster.getNamesystem().writeLock();
      try {
        bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[0],
            "TEST");
        bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[1],
            "TEST");
      } finally {
        cluster.getNamesystem().writeUnlock();
      }
      BlockManagerTestUtil.computeAllPendingWork(bm);
      BlockManagerTestUtil.updateState(bm);
      assertEquals(bm.getPendingReplicationBlocksCount(), 1L);
      assertEquals(bm.pendingReplications.getNumReplicas(block.getBlock()
          .getLocalBlock()), 2);
      
      // 4. delete the file
      fs.delete(filePath, true);
      // retry at most 10 times, each time sleep for 1s. Note that 10s is much
      // less than the default pending record timeout (5~10min)
      int retries = 10; 
      long pendingNum = bm.getPendingReplicationBlocksCount();
      while (pendingNum != 0 && retries-- > 0) {
        Thread.sleep(1000);  // let NN do the deletion
        BlockManagerTestUtil.updateState(bm);
        pendingNum = bm.getPendingReplicationBlocksCount();
      }
      assertEquals(pendingNum, 0L);
    } finally {
      cluster.shutdown();
    }
  }
}
