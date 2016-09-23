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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

/**
 * This class tests the behavior of moving block replica to the given storage
 * type to fulfill the storage policy requirement.
 */
public class TestStoragePolicySatisfyWorker {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestStoragePolicySatisfyWorker.class);

  private static final int DEFAULT_BLOCK_SIZE = 100;

  private static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  /**
   * Tests to verify that the block replica is moving to ARCHIVE storage type to
   * fulfill the storage policy requirement.
   */
  @Test(timeout = 120000)
  public void testMoveSingleBlockToAnotherDatanode() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(4)
            .storageTypes(
                new StorageType[][]{{StorageType.DISK, StorageType.ARCHIVE},
                    {StorageType.DISK, StorageType.ARCHIVE},
                    {StorageType.ARCHIVE, StorageType.ARCHIVE},
                    {StorageType.ARCHIVE, StorageType.ARCHIVE}})
            .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoveSingleBlockToAnotherDatanode";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testMoveSingleBlockToAnotherDatanode");
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");

      lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      DataNode src = cluster.getDataNodes().get(3);
      DatanodeInfo targetDnInfo = DFSTestUtil
          .getLocalDatanodeInfo(src.getXferPort());

      // TODO: Need to revisit this when NN is implemented to be able to send
      // block moving commands.
      StoragePolicySatisfyWorker worker = new StoragePolicySatisfyWorker(conf,
          src);
      List<BlockMovingInfo> blockMovingInfos = new ArrayList<>();
      BlockMovingInfo blockMovingInfo = prepareBlockMovingInfo(
          lb.getBlock().getLocalBlock(), lb.getLocations()[0], targetDnInfo,
          lb.getStorageTypes()[0], StorageType.ARCHIVE);
      blockMovingInfos.add(blockMovingInfo);
      INode inode = cluster.getNamesystem().getFSDirectory().getINode(file);
      worker.processBlockMovingTasks(inode.getId(),
          cluster.getNamesystem().getBlockPoolId(), blockMovingInfos);
      cluster.triggerHeartbeats();

      // Wait till NameNode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs, file, 1, 30000);
    } finally {
      cluster.shutdown();
    }
  }

  private void waitForLocatedBlockWithArchiveStorageType(
      final DistributedFileSystem dfs, final String file,
      int expectedArchiveCount, int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlock lb = null;
        try {
          lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }

        int archiveCount = 0;
        for (StorageType storageType : lb.getStorageTypes()) {
          if (StorageType.ARCHIVE == storageType) {
            archiveCount++;
          }
        }
        LOG.info("Archive replica count, expected={} and actual={}",
            expectedArchiveCount, archiveCount);
        return expectedArchiveCount == archiveCount;
      }
    }, 100, timeout);
  }

  BlockMovingInfo prepareBlockMovingInfo(Block block,
      DatanodeInfo src, DatanodeInfo destin, StorageType storageType,
      StorageType targetStorageType) {
    return new BlockMovingInfo(block, new DatanodeInfo[] {src},
        new DatanodeInfo[] {destin}, new StorageType[] {storageType},
        new StorageType[] {targetStorageType});
  }
}
