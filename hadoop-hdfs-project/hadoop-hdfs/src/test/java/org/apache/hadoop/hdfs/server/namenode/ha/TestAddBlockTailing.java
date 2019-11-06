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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Tests the race condition that IBR and add block may result
 * in inconsistent block genstamp.
 */
public class TestAddBlockTailing {
  private static final int BLOCK_SIZE = 8192;
  private static final String TEST_DIR = "/TestAddBlockTailing";

  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;
  private static FSNamesystem fsn0;
  private static FSNamesystem fsn1;
  private static DataNode dn0;

  @BeforeClass
  public static void startUpCluster() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    MiniQJMHACluster.Builder qjmBuilder = new MiniQJMHACluster.Builder(conf)
        .setNumNameNodes(2);
    qjmBuilder.getDfsBuilder().numDataNodes(1);
    qjmhaCluster = qjmBuilder.build();
    dfsCluster = qjmhaCluster.getDfsCluster();
    dfsCluster.waitActive();
    dfsCluster.transitionToActive(0);
    dfs = dfsCluster.getFileSystem(0);
    fsn0 = dfsCluster.getNameNode(0).getNamesystem();
    fsn1 = dfsCluster.getNameNode(1).getNamesystem();
    dfs.mkdirs(new Path(TEST_DIR), new FsPermission("755"));
    dn0 = dfsCluster.getDataNodes().get(0);
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test
  public void testStandbyAddBlockIBRRace() throws Exception {
    String testFile = TEST_DIR +"/testStandbyAddBlockIBRRace";

    // initial global generation stamp check
    assertEquals("Global Generation stamps on NNs should be the same",
        NameNodeAdapter.getGenerationStamp(fsn0),
        NameNodeAdapter.getGenerationStamp(fsn1));

    // create a file, add a block on NN0
    // do not journal addBlock yet
    dfs.create(new Path(testFile), true, dfs.getConf()
            .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) 1, BLOCK_SIZE);
    DatanodeManager dnManager = fsn0.getBlockManager().getDatanodeManager();
    DatanodeStorageInfo[] targets =
        dnManager.getDatanode(dn0.getDatanodeId()).getStorageInfos();
    targets = new DatanodeStorageInfo[] {targets[0]};
    BlockInfo newBlock = NameNodeAdapter.addBlockNoJournal(
        fsn0, testFile, targets);

    // NN1 tails increment generation stamp transaction
    fsn0.getEditLog().logSync();
    fsn1.getEditLogTailer().doTailEdits();

    assertEquals("Global Generation stamps on NN0 and "
            + "impending on NN1 should be equal",
        NameNodeAdapter.getGenerationStamp(fsn0),
        NameNodeAdapter.getImpendingGenerationStamp(fsn1));

    // NN1 processes IBR with the replica
    StorageReceivedDeletedBlocks[] report = DFSTestUtil
        .makeReportForReceivedBlock(newBlock,
            ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK,
            dn0.getFSDataset().getStorage(targets[0].getStorageID()));
    fsn1.processIncrementalBlockReport(dn0.getDatanodeId(), report[0]);

    // NN0 persists the block, i.e adds update block transaction
    INodeFile file = (INodeFile)fsn0.getFSDirectory().getINode(testFile);
    NameNodeAdapter.persistBlocks(fsn0, testFile, file);

    // NN1 tails update block transaction
    fsn0.getEditLog().logSync();
    fsn1.getEditLogTailer().doTailEdits();

    assertEquals("Global Generation stamps on NN0 and "
            + "impending on NN1 should be equal",
        NameNodeAdapter.getGenerationStamp(fsn0),
        NameNodeAdapter.getImpendingGenerationStamp(fsn1));

    // The new block on NN1 should have the replica
    BlockInfo newBlock1 = NameNodeAdapter.getStoredBlock(fsn1, newBlock);
    assertTrue("New block on NN1 should contain the replica",
        newBlock1.numNodes() > 0);
    assertEquals("Generation stamps of the block on NNs should be the same",
        newBlock.getGenerationStamp(), newBlock1.getGenerationStamp());
    assertEquals("Global Generation stamps on NNs should be the same",
        NameNodeAdapter.getGenerationStamp(fsn0),
        NameNodeAdapter.getGenerationStamp(fsn1));

    // Check that the generation stamp restores on Standby after failover
    ClientProtocol rpc0 = dfsCluster.getNameNode(0).getRpcServer();
    ClientProtocol rpc1 = dfsCluster.getNameNode(1).getRpcServer();
    LocatedBlock lb = rpc0.getBlockLocations(testFile, 0, 0).get(0);
    rpc0.updateBlockForPipeline(lb.getBlock(), dfs.getClient().getClientName());
    long gs0 = NameNodeAdapter.getGenerationStamp(fsn0);
    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    assertEquals("Global Generation stamps on new active should be "
            + "the same as on the old one", gs0,
        NameNodeAdapter.getGenerationStamp(fsn1));

    rpc1.delete(testFile, false);
  }
}
