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
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.junit.Test;

import java.util.Iterator;


public class TestUnderReplicatedBlocks {
  @Test(timeout=60000) // 1 min timeout
  public void testSetrepIncWithUnderReplicatedBlocks() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final short REPLICATION_FACTOR = 2;
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION_FACTOR + 1).build();
    try {
      // create a file with one block with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      
      // remove one replica from the blocksMap so block becomes under-replicated
      // but the block does not get put into the under-replicated blocks queue
      final BlockManager bm = cluster.getNamesystem().getBlockManager();
      ExtendedBlock b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);
      DatanodeDescriptor dn = bm.blocksMap.getStorages(b.getLocalBlock())
          .iterator().next().getDatanodeDescriptor();
      bm.addToInvalidates(b.getLocalBlock(), dn);
      Thread.sleep(5000);
      bm.blocksMap.removeNode(b.getLocalBlock(), dn);
      
      // increment this file's replication factor
      FsShell shell = new FsShell(conf);
      assertEquals(0, shell.run(new String[]{
          "-setrep", "-w", Integer.toString(1+REPLICATION_FACTOR), FILE_NAME}));
    } finally {
      cluster.shutdown();
    }
    
  }

  /**
   * The test verifies the number of outstanding replication requests for a
   * given DN shouldn't exceed the limit set by configuration property
   * dfs.namenode.replication.max-streams-hard-limit.
   * The test does the followings:
   * 1. Create a mini cluster with 2 DNs. Set large heartbeat interval so that
   *    replication requests won't be picked by any DN right away.
   * 2. Create a file with 10 blocks and replication factor 2. Thus each
   *    of the 2 DNs have one replica of each block.
   * 3. Add a DN to the cluster for later replication.
   * 4. Remove a DN that has data.
   * 5. Ask BlockManager to compute the replication work. This will assign
   *    replication requests to the only DN that has data.
   * 6. Make sure the number of pending replication requests of that DN don't
   *    exceed the limit.
   * @throws Exception
   */
  @Test(timeout=60000) // 1 min timeout
  public void testNumberOfBlocksToBeReplicated() throws Exception {
    Configuration conf = new HdfsConfiguration();

    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);

    // Large value to make sure the pending replication request can stay in
    // DatanodeDescriptor.replicateBlocks before test timeout.
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 100);

    // Make sure BlockManager can pull all blocks from UnderReplicatedBlocks via
    // chooseUnderReplicatedBlocks at once.
     conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, 5);

    int NUM_OF_BLOCKS = 10;
    final short REP_FACTOR = 2;
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
            REP_FACTOR).build();
    try {
      // create a file with 10 blocks with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, NUM_OF_BLOCKS, REP_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REP_FACTOR);

      cluster.startDataNodes(conf, 1, true, null, null, null, null);

      final BlockManager bm = cluster.getNamesystem().getBlockManager();
      ExtendedBlock b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);
      Iterator<DatanodeStorageInfo> storageInfos =
          bm.blocksMap.getStorages(b.getLocalBlock())
          .iterator();
      DatanodeDescriptor firstDn = storageInfos.next().getDatanodeDescriptor();
      DatanodeDescriptor secondDn = storageInfos.next().getDatanodeDescriptor();

      bm.getDatanodeManager().removeDatanode(firstDn);

      assertEquals(NUM_OF_BLOCKS, bm.getUnderReplicatedNotMissingBlocks());
      bm.computeDatanodeWork();


      assertTrue("The number of blocks to be replicated should be less than "
          + "or equal to " + bm.replicationStreamsHardLimit,
          secondDn.getNumberOfBlocksToBeReplicated()
          <= bm.replicationStreamsHardLimit);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * The test verifies the following scenarios:
   *
   * 1. Missing one replication blocks count is increased
   * when a block with replication factor one becomes corrupt
   * and is decreased if the file is removed.
   *
   * 2. When a file with replication factor two becomes corrupt,
   * then missing one replication blocks count does not change.
   *
   * 3. When a file with replication factor 1 only has read only replicas,
   * then missing one replication blocks count does not change
   */
  @Test(timeout = 60000)
  public void testOneReplicaMissingBlocksCountIsReported() {
    UnderReplicatedBlocks underReplicatedBlocks = new UnderReplicatedBlocks();
    BlockInfo blockInfo = new BlockInfoContiguous(new Block(1), (short) 1);
    boolean addResult
        = underReplicatedBlocks.add(blockInfo, 0, 0, 0, 1);
    assertTrue(addResult);
    assertInLevel(underReplicatedBlocks, blockInfo,
        UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
    assertEquals(1, underReplicatedBlocks.getCorruptBlockSize());
    assertEquals(1, underReplicatedBlocks.getCorruptReplOneBlockSize());

    underReplicatedBlocks.remove(blockInfo, UnderReplicatedBlocks.LEVEL);
    assertEquals(0, underReplicatedBlocks.getCorruptBlockSize());
    assertEquals(0, underReplicatedBlocks.getCorruptReplOneBlockSize());

    blockInfo.setReplication((short) 2);
    addResult = underReplicatedBlocks.add(blockInfo, 0, 0, 0, 2);
    assertTrue(addResult);
    assertInLevel(underReplicatedBlocks, blockInfo,
        UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
    assertEquals(1, underReplicatedBlocks.getCorruptBlockSize());
    assertEquals(0, underReplicatedBlocks.getCorruptReplOneBlockSize());

    underReplicatedBlocks.remove(blockInfo, UnderReplicatedBlocks.LEVEL);
    assertEquals(0, underReplicatedBlocks.getCorruptBlockSize());
    assertEquals(0, underReplicatedBlocks.getCorruptReplOneBlockSize());

    blockInfo.setReplication((short) 1);
    addResult = underReplicatedBlocks.add(blockInfo, 0, 1, 0, 1);
    assertTrue(addResult);
    assertInLevel(underReplicatedBlocks, blockInfo,
        UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY);
    assertEquals(0, underReplicatedBlocks.getCorruptBlockSize());
    assertEquals(0, underReplicatedBlocks.getCorruptReplOneBlockSize());

    underReplicatedBlocks.remove(blockInfo, UnderReplicatedBlocks.LEVEL);
    assertEquals(0, underReplicatedBlocks.getCorruptBlockSize());
    assertEquals(0, underReplicatedBlocks.getCorruptReplOneBlockSize());
  }

  /**
   * Determine whether or not a block is in a level without changing the API.
   * Instead get the per-level iterator and run though it looking for a match.
   * If the block is not found, an assertion is thrown.
   *
   * This is inefficient, but this is only a test case.
   * @param queues queues to scan
   * @param block block to look for
   * @param level level to select
   */
  private void assertInLevel(UnderReplicatedBlocks queues,
                             Block block,
                             int level) {
    final Iterator<BlockInfo> bi = queues.iterator(level);
    while (bi.hasNext()) {
      Block next = bi.next();
      if (block.equals(next)) {
        return;
      }
    }
    fail("Block " + block + " not found in level " + level);
  }
}
