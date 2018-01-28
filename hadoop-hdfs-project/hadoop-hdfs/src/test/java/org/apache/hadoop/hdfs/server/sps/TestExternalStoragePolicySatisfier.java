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
package org.apache.hadoop.hdfs.server.sps;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.sps.BlockMovementListener;
import org.apache.hadoop.hdfs.server.namenode.sps.Context;
import org.apache.hadoop.hdfs.server.namenode.sps.FileIdCollector;
import org.apache.hadoop.hdfs.server.namenode.sps.SPSService;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.hdfs.server.namenode.sps.TestStoragePolicySatisfier;
import org.junit.Assert;
import org.junit.Ignore;

/**
 * Tests the external sps service plugins.
 */
public class TestExternalStoragePolicySatisfier
    extends TestStoragePolicySatisfier {
  private StorageType[][] allDiskTypes =
      new StorageType[][]{{StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK}};
  private NameNodeConnector nnc;

  @Override
  public void setUp() {
    super.setUp();

    getConf().set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());
  }

  @Override
  public void createCluster() throws IOException {
    getConf().setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    setCluster(startCluster(getConf(), allDiskTypes, NUM_OF_DATANODES,
        STORAGES_PER_DATANODE, CAPACITY));
    getFS();
    writeContent(FILE);
  }

  @Override
  public MiniDFSCluster startCluster(final Configuration conf,
      StorageType[][] storageTypes, int numberOfDatanodes, int storagesPerDn,
      long nodeCapacity) throws IOException {
    long[][] capacities = new long[numberOfDatanodes][storagesPerDn];
    for (int i = 0; i < numberOfDatanodes; i++) {
      for (int j = 0; j < storagesPerDn; j++) {
        capacities[i][j] = nodeCapacity;
      }
    }
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numberOfDatanodes).storagesPerDatanode(storagesPerDn)
        .storageTypes(storageTypes).storageCapacities(capacities).build();
    cluster.waitActive();

    nnc = getNameNodeConnector(getConf());

    BlockManager blkMgr = cluster.getNameNode().getNamesystem()
        .getBlockManager();
    SPSService spsService = blkMgr.getSPSService();
    spsService.stopGracefully();

    ExternalSPSContext context = new ExternalSPSContext(spsService,
        getNameNodeConnector(conf));

    ExternalBlockMovementListener blkMoveListener =
        new ExternalBlockMovementListener();
    ExternalSPSBlockMoveTaskHandler externalHandler =
        new ExternalSPSBlockMoveTaskHandler(conf, nnc,
            blkMgr.getSPSService());
    externalHandler.init();
    spsService.init(context,
        new ExternalSPSFileIDCollector(context, blkMgr.getSPSService()),
        externalHandler,
        blkMoveListener);
    spsService.start(true, StoragePolicySatisfierMode.EXTERNAL);
    return cluster;
  }

  public void restartNamenode() throws IOException{
    BlockManager blkMgr = getCluster().getNameNode().getNamesystem()
        .getBlockManager();
    SPSService spsService = blkMgr.getSPSService();
    spsService.stopGracefully();

    getCluster().restartNameNodes();
    getCluster().waitActive();
    blkMgr = getCluster().getNameNode().getNamesystem()
        .getBlockManager();
    spsService = blkMgr.getSPSService();
    spsService.stopGracefully();

    ExternalSPSContext context = new ExternalSPSContext(spsService,
        getNameNodeConnector(getConf()));
    ExternalBlockMovementListener blkMoveListener =
        new ExternalBlockMovementListener();
    ExternalSPSBlockMoveTaskHandler externalHandler =
        new ExternalSPSBlockMoveTaskHandler(getConf(), nnc,
            blkMgr.getSPSService());
    externalHandler.init();
    spsService.init(context,
        new ExternalSPSFileIDCollector(context, blkMgr.getSPSService()),
        externalHandler,
        blkMoveListener);
    spsService.start(true, StoragePolicySatisfierMode.EXTERNAL);
  }

  @Override
  public FileIdCollector createFileIdCollector(StoragePolicySatisfier sps,
      Context ctxt) {
    return new ExternalSPSFileIDCollector(ctxt, sps);
  }

  private class ExternalBlockMovementListener implements BlockMovementListener {

    private List<Block> actualBlockMovements = new ArrayList<>();

    @Override
    public void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks) {
      for (Block block : moveAttemptFinishedBlks) {
        actualBlockMovements.add(block);
      }
      LOG.info("Movement attempted blocks", actualBlockMovements);
    }
  }

  private NameNodeConnector getNameNodeConnector(Configuration conf)
      throws IOException {
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    Assert.assertEquals(1, namenodes.size());
    final Path externalSPSPathId = new Path("/system/tmp.id");
    NameNodeConnector.checkOtherInstanceRunning(false);
    while (true) {
      try {
        final List<NameNodeConnector> nncs = NameNodeConnector
            .newNameNodeConnectors(namenodes,
                StoragePolicySatisfier.class.getSimpleName(),
                externalSPSPathId, conf,
                NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
        return nncs.get(0);
      } catch (IOException e) {
        LOG.warn("Failed to connect with namenode", e);
        // Ignore
      }

    }
  }

  /**
   * This test need not run as external scan is not a batch based scanning right
   * now.
   */
  @Ignore("ExternalFileIdCollector is not batch based right now."
      + " So, ignoring it.")
  public void testBatchProcessingForSPSDirectory() throws Exception {
  }

  /**
   * Status won't be supported for external SPS, now. So, ignoring it.
   */
  @Ignore("Status is not supported for external SPS. So, ignoring it.")
  public void testStoragePolicySatisfyPathStatus() throws Exception {
  }

  /**
   * This test case is more specific to internal.
   */
  @Ignore("This test is specific to internal, so skipping here.")
  public void testWhenMoverIsAlreadyRunningBeforeStoragePolicySatisfier()
      throws Exception {
  }

  /**
   * Status won't be supported for external SPS, now. So, ignoring it.
   */
  @Ignore("Status is not supported for external SPS. So, ignoring it.")
  public void testMaxRetryForFailedBlock() throws Exception {
  }
}
