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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test verifies that incremental block reports from a single DataNode are
 * correctly handled by NN. Tests the following variations:
 *  #1 - Incremental BRs from all storages combined in a single call.
 *  #2 - Incremental BRs from separate storages sent in separate calls.
 *  #3 - Incremental BR from an unknown storage should be rejected.
 *
 *  We also verify that the DataNode is not splitting the reports (it may do so
 *  in the future).
 */
public class TestIncrementalBrVariations {
  public static final Log LOG = LogFactory.getLog(TestIncrementalBrVariations.class);

  private static final short NUM_DATANODES = 1;
  static final int BLOCK_SIZE = 1024;
  static final int NUM_BLOCKS = 10;
  private static final long seed = 0xFACEFEEDL;
  private static final String NN_METRICS = "NameNodeActivity";


  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient client;
  private static Configuration conf;
  private String poolId;
  private DataNode dn0;           // DataNode at index0 in the MiniDFSCluster
  private DatanodeRegistration dn0Reg;  // DataNodeRegistration for dn0

  static {
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, Level.ALL);
    GenericTestUtils.setLogLevel(BlockManager.blockLog, Level.ALL);
    GenericTestUtils.setLogLevel(NameNode.blockStateChangeLog, Level.ALL);
    GenericTestUtils
        .setLogLevel(LogFactory.getLog(FSNamesystem.class), Level.ALL);
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(TestIncrementalBrVariations.LOG, Level.ALL);
  }

  @Before
  public void startUpCluster() throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
    fs = cluster.getFileSystem();
    client = new DFSClient(new InetSocketAddress("localhost", cluster.getNameNodePort()),
                           cluster.getConfiguration(0));
    dn0 = cluster.getDataNodes().get(0);
    poolId = cluster.getNamesystem().getBlockPoolId();
    dn0Reg = dn0.getDNRegistrationForBP(poolId);
  }

  @After
  public void shutDownCluster() throws IOException {
    client.close();
    fs.close();
    cluster.shutdownDataNodes();
    cluster.shutdown();
  }

  /**
   * Incremental BRs from all storages combined in a single message.
   */
  @Test
  public void testCombinedIncrementalBlockReport() throws IOException {
    verifyIncrementalBlockReports(false);
  }

  /**
   * One incremental BR per storage.
   */
  @Test
  public void testSplitIncrementalBlockReport() throws IOException {
    verifyIncrementalBlockReports(true);
  }

  private LocatedBlocks createFileGetBlocks(String filenamePrefix) throws IOException {
    Path filePath = new Path("/" + filenamePrefix + ".dat");

    // Write out a file with a few blocks, get block locations.
    DFSTestUtil.createFile(fs, filePath, BLOCK_SIZE, BLOCK_SIZE * NUM_BLOCKS,
                           BLOCK_SIZE, NUM_DATANODES, seed);

    // Get the block list for the file with the block locations.
    LocatedBlocks blocks = client.getLocatedBlocks(
        filePath.toString(), 0, BLOCK_SIZE * NUM_BLOCKS);
    assertThat(cluster.getNamesystem().getUnderReplicatedBlocks(), is(0L));
    return blocks;
  }

  public void verifyIncrementalBlockReports(boolean splitReports) throws IOException {
    // Get the block list for the file with the block locations.
    LocatedBlocks blocks = createFileGetBlocks(GenericTestUtils.getMethodName());

    // We will send 'fake' incremental block reports to the NN that look
    // like they originated from DN 0.
    StorageReceivedDeletedBlocks reports[] =
        new StorageReceivedDeletedBlocks[dn0.getFSDataset().getVolumes().size()];

    // Lie to the NN that one block on each storage has been deleted.
    for (int i = 0; i < reports.length; ++i) {
      FsVolumeSpi volume = dn0.getFSDataset().getVolumes().get(i);

      boolean foundBlockOnStorage = false;
      ReceivedDeletedBlockInfo rdbi[] = new ReceivedDeletedBlockInfo[1];

      // Find the first block on this storage and mark it as deleted for the
      // report.
      for (LocatedBlock block : blocks.getLocatedBlocks()) {
        if (block.getStorageIDs()[0].equals(volume.getStorageID())) {
          rdbi[0] = new ReceivedDeletedBlockInfo(block.getBlock().getLocalBlock(),
              ReceivedDeletedBlockInfo.BlockStatus.DELETED_BLOCK, null);
          foundBlockOnStorage = true;
          break;
        }
      }

      assertTrue(foundBlockOnStorage);
      reports[i] = new StorageReceivedDeletedBlocks(volume.getStorageID(), rdbi);

      if (splitReports) {
        // If we are splitting reports then send the report for this storage now.
        StorageReceivedDeletedBlocks singletonReport[] = { reports[i] };
        cluster.getNameNodeRpc().blockReceivedAndDeleted(
            dn0Reg, poolId, singletonReport);
      }
    }

    if (!splitReports) {
      // Send a combined report.
      cluster.getNameNodeRpc().blockReceivedAndDeleted(dn0Reg, poolId, reports);
    }

    // Make sure that the deleted block from each storage was picked up
    // by the NameNode. IBRs are async, make sure the NN processes
    // all of them.
    cluster.getNamesystem().getBlockManager().flushBlockOps();
    assertThat(cluster.getNamesystem().getMissingBlocksCount(),
        is((long) reports.length));
  }

  /**
   * Verify that the DataNode sends a single incremental block report for all
   * storages.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=60000)
  public void testDataNodeDoesNotSplitReports()
      throws IOException, InterruptedException {
    LocatedBlocks blocks = createFileGetBlocks(GenericTestUtils.getMethodName());
    assertThat(cluster.getDataNodes().size(), is(1));

    // Remove all blocks from the DataNode.
    for (LocatedBlock block : blocks.getLocatedBlocks()) {
      dn0.notifyNamenodeDeletedBlock(
          block.getBlock(), block.getStorageIDs()[0]);
    }

    LOG.info("Triggering report after deleting blocks");
    long ops = getLongCounter("BlockReceivedAndDeletedOps", getMetrics(NN_METRICS));

    // Trigger a report to the NameNode and give it a few seconds.
    DataNodeTestUtils.triggerBlockReport(dn0);
    Thread.sleep(5000);

    // Ensure that NameNodeRpcServer.blockReceivedAndDeletes is invoked
    // exactly once after we triggered the report.
    assertCounter("BlockReceivedAndDeletedOps", ops+1, getMetrics(NN_METRICS));
  }

  private static Block getDummyBlock() {
    return new Block(10000000L, 100L, 1048576L);
  }

  private static StorageReceivedDeletedBlocks[] makeReportForReceivedBlock(
      Block block, DatanodeStorage storage) {
    ReceivedDeletedBlockInfo[] receivedBlocks = new ReceivedDeletedBlockInfo[1];
    receivedBlocks[0] = new ReceivedDeletedBlockInfo(block, BlockStatus.RECEIVED_BLOCK, null);
    StorageReceivedDeletedBlocks[] reports = new StorageReceivedDeletedBlocks[1];
    reports[0] = new StorageReceivedDeletedBlocks(storage, receivedBlocks);
    return reports;
  }

  /**
   * Verify that the NameNode can learn about new storages from incremental
   * block reports.
   * This tests the fix for the error condition seen in HDFS-6904.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=60000)
  public void testNnLearnsNewStorages()
      throws IOException, InterruptedException {

    // Generate a report for a fake block on a fake storage.
    final String newStorageUuid = UUID.randomUUID().toString();
    final DatanodeStorage newStorage = new DatanodeStorage(newStorageUuid);
    StorageReceivedDeletedBlocks[] reports = makeReportForReceivedBlock(
        getDummyBlock(), newStorage);

    // Send the report to the NN.
    cluster.getNameNodeRpc().blockReceivedAndDeleted(dn0Reg, poolId, reports);
    // IBRs are async, make sure the NN processes all of them.
    cluster.getNamesystem().getBlockManager().flushBlockOps();
    // Make sure that the NN has learned of the new storage.
    DatanodeStorageInfo storageInfo = cluster.getNameNode()
                                             .getNamesystem()
                                             .getBlockManager()
                                             .getDatanodeManager()
                                             .getDatanode(dn0.getDatanodeId())
                                             .getStorageInfo(newStorageUuid);
    assertNotNull(storageInfo);
  }
}
