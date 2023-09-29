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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test RedudantBlocks.
 */
public class TestRedudantBlocks {

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private final Path dirPath = new Path("/striped");
  private Path filePath = new Path(dirPath, "file");
  private final ErasureCodingPolicy ecPolicy =
      SystemErasureCodingPolicies.getPolicies().get(1);
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final short groupSize = (short) (dataBlocks + parityBlocks);
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripesPerBlock = 4;
  private final int blockSize = stripesPerBlock * cellSize;
  private final int numDNs = groupSize;

  @Before
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    SimulatedFSDataset.setFactory(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    fs.mkdirs(dirPath);
    fs.getClient().setErasureCodingPolicy(dirPath.toString(),
        ecPolicy.getName());
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testProcessOverReplicatedAndRedudantBlock() throws Exception {
    long fileLen = dataBlocks * blockSize;
    DFSTestUtil.createStripedFile(cluster, filePath, null, 1, stripesPerBlock,
        false);
    LocatedBlocks lbs = cluster.getNameNodeRpc()
        .getBlockLocations(filePath.toString(), 0, fileLen);
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
    long gs = bg.getBlock().getGenerationStamp();
    String bpid = bg.getBlock().getBlockPoolId();
    long groupId = bg.getBlock().getBlockId();
    Block blk = new Block(groupId, blockSize, gs);
    int i = 0;
    // one missing block
    for (; i < groupSize - 1; i++) {
      blk.setBlockId(groupId + i);
      cluster.injectBlocks(i, Arrays.asList(blk), bpid);
    }
    cluster.triggerBlockReports();
    // one redundant block
    blk.setBlockId(groupId + 2);
    cluster.injectBlocks(i, Arrays.asList(blk), bpid);

    BlockInfoStriped blockInfo =
        (BlockInfoStriped)cluster.getNamesystem().getBlockManager()
            .getStoredBlock(new Block(groupId));
    // update blocksMap
    cluster.triggerBlockReports();
    // delete redundant block
    cluster.triggerHeartbeats();
    //wait for IBR
    GenericTestUtils.waitFor(
        () -> cluster.getNamesystem().getBlockManager()
            .countNodes(blockInfo).liveReplicas() >= groupSize -1,
        500, 10000);

    // trigger reconstruction
    cluster.triggerHeartbeats();
    //wait for IBR
    GenericTestUtils.waitFor(
        () -> cluster.getNamesystem().getBlockManager()
            .countNodes(blockInfo).liveReplicas() >= groupSize,
        500, 10000);

    HashSet<Long> blockIdsSet = new HashSet<Long>();

    lbs = cluster.getNameNodeRpc().getBlockLocations(filePath.toString(), 0,
        fileLen);
    bg = (LocatedStripedBlock) (lbs.get(0));

    final LocatedBlock[] blocks = StripedBlockUtil.parseStripedBlockGroup(bg,
        cellSize, dataBlocks, parityBlocks);

    for (LocatedBlock dn : blocks) {
      if (dn != null) {
        blockIdsSet.add(dn.getBlock().getBlockId());
      }
    }
    assertEquals(groupSize, blockIdsSet.size());

  }
}
