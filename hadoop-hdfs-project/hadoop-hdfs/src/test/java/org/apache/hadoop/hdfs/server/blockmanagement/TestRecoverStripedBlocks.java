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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.EC_STORAGE_POLICY_NAME;
import static org.junit.Assert.assertTrue;

public class TestRecoverStripedBlocks {
  private final short GROUP_SIZE =
      HdfsConstants.NUM_DATA_BLOCKS + HdfsConstants.NUM_PARITY_BLOCKS;
  private final short NUM_OF_DATANODES = GROUP_SIZE + 1;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private HdfsAdmin dfsAdmin;
  private FSNamesystem namesystem;
  private Path ECFilePath;

  @Before
  public void setupCluster() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    // Large value to make sure the pending replication request can stay in
    // DatanodeDescriptor.replicateBlocks before test timeout.
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 100);
    // Make sure BlockManager can pull all blocks from UnderReplicatedBlocks via
    // chooseUnderReplicatedBlocks at once.
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, 5);

    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(NUM_OF_DATANODES).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);
    namesystem = cluster.getNamesystem();
    ECFilePath = new Path("/ecfile");
    DFSTestUtil.createFile(fs, ECFilePath, 4 * BLOCK_SIZE, GROUP_SIZE, 0);
    dfsAdmin.setStoragePolicy(ECFilePath, EC_STORAGE_POLICY_NAME);
  }

  @Test
  public void testMissingStripedBlock() throws Exception {
    final BlockManager bm = cluster.getNamesystem().getBlockManager();
    ExtendedBlock b = DFSTestUtil.getFirstBlock(fs, ECFilePath);
    Iterator<DatanodeStorageInfo> storageInfos =
        bm.blocksMap.getStorages(b.getLocalBlock())
            .iterator();

    DatanodeDescriptor firstDn = storageInfos.next().getDatanodeDescriptor();
    Iterator<BlockInfo> it = firstDn.getBlockIterator();
    int missingBlkCnt = 0;
    while (it.hasNext()) {
      BlockInfo blk = it.next();
      BlockManager.LOG.debug("Block " + blk + " will be lost");
      missingBlkCnt++;
    }
    BlockManager.LOG.debug("Missing in total " + missingBlkCnt + " blocks");

    bm.getDatanodeManager().removeDatanode(firstDn);

    bm.computeDatanodeWork();

    short cnt = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      DatanodeDescriptor dnDescriptor =
          bm.getDatanodeManager().getDatanode(dn.getDatanodeUuid());
      cnt += dnDescriptor.getNumberOfBlocksToBeErasureCoded();
    }

    assertTrue("Counting the number of outstanding EC tasks", cnt == missingBlkCnt);
  }
}
