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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

/**
 * Tests that StoragePolicySatisfier daemon is able to check the blocks to be
 * moved and finding its suggested target locations to move.
 */
public class TestStoragePolicySatisfier {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestStoragePolicySatisfier.class);
  private final Configuration config = new HdfsConfiguration();
  private StorageType[][] allDiskTypes =
      new StorageType[][]{{StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK}};
  private MiniDFSCluster hdfsCluster = null;
  final private int numOfDatanodes = 3;
  final private int storagesPerDatanode = 2;
  final private long capacity = 2 * 256 * 1024 * 1024;
  final private String file = "/testMoveWhenStoragePolicyNotSatisfying";
  private DistributedFileSystem distributedFS = null;

  @Before
  public void setUp() throws IOException {
    config.setLong("dfs.block.size", 1024);
    hdfsCluster = startCluster(config, allDiskTypes, numOfDatanodes,
        storagesPerDatanode, capacity);
    distributedFS = hdfsCluster.getFileSystem();
    writeContent(distributedFS, file);
  }

  @Test(timeout = 300000)
  public void testWhenStoragePolicySetToCOLD()
      throws Exception {

    try {
      // Change policy to ALL_SSD
      distributedFS.setStoragePolicy(new Path(file), "COLD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE}};
      startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);

      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());

      hdfsCluster.triggerHeartbeats();
      // Wait till namenode notified about the block location details
      waitExpectedStorageType(file, StorageType.ARCHIVE, distributedFS, 3,
          30000);
    } finally {
      hdfsCluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testWhenStoragePolicySetToALLSSD()
      throws Exception {
    try {
      // Change policy to ALL_SSD
      distributedFS.setStoragePolicy(new Path(file), "ALL_SSD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK},
              {StorageType.SSD, StorageType.DISK},
              {StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 3, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier Identified that block to move to SSD
      // areas
      waitExpectedStorageType(file, StorageType.SSD, distributedFS, 3, 30000);
    } finally {
      hdfsCluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testWhenStoragePolicySetToONESSD()
      throws Exception {
    try {
      // Change policy to ONE_SSD
      distributedFS.setStoragePolicy(new Path(file), "ONE_SSD");
      FSNamesystem namesystem = hdfsCluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);

      StorageType[][] newtypes =
          new StorageType[][]{{StorageType.SSD, StorageType.DISK}};

      // Making sure SDD based nodes added to cluster. Adding SSD based
      // datanodes.
      startAdditionalDNs(config, 1, numOfDatanodes, newtypes,
          storagesPerDatanode, capacity, hdfsCluster);
      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());
      hdfsCluster.triggerHeartbeats();
      // Wait till StorgePolicySatisfier Identified that block to move to SSD
      // areas
      waitExpectedStorageType(file, StorageType.SSD, distributedFS, 1, 30000);
      waitExpectedStorageType(file, StorageType.DISK, distributedFS, 2, 30000);
    } finally {
      hdfsCluster.shutdown();
    }
  }

  private void writeContent(final DistributedFileSystem dfs,
      final String fileName) throws IOException {
    // write to DISK
    final FSDataOutputStream out = dfs.create(new Path(fileName));
    for (int i = 0; i < 1000; i++) {
      out.writeChars("t");
    }
    out.close();
  }

  private void startAdditionalDNs(final Configuration conf,
      int newNodesRequired, int existingNodesNum, StorageType[][] newTypes,
      int storagesPerDatanode, long capacity, final MiniDFSCluster cluster)
          throws IOException {
    long[][] capacities;
    existingNodesNum += newNodesRequired;
    capacities = new long[newNodesRequired][storagesPerDatanode];
    for (int i = 0; i < newNodesRequired; i++) {
      for (int j = 0; j < storagesPerDatanode; j++) {
        capacities[i][j] = capacity;
      }
    }

    cluster.startDataNodes(conf, newNodesRequired, newTypes, true, null, null,
        null, capacities, null, false, false, false, null);
    cluster.triggerHeartbeats();
  }

  private MiniDFSCluster startCluster(final Configuration conf,
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
    return cluster;
  }

  // Check whether the Block movement has been successfully completed to satisfy
  // the storage policy for the given file.
  private void waitExpectedStorageType(final String fileName,
      final StorageType expectedStorageType, final DistributedFileSystem dfs,
      int expectedStorageCount, int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlock lb = null;
        try {
          lb = dfs.getClient().getLocatedBlocks(fileName, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }
        int actualStorageCount = 0;
        for (StorageType storageType : lb.getStorageTypes()) {
          if (expectedStorageType == storageType) {
            actualStorageCount++;
          }
        }
        LOG.info(
            expectedStorageType + " replica count, expected={} and actual={}",
            expectedStorageType, actualStorageCount);
        return expectedStorageCount == actualStorageCount;
      }
    }, 100, timeout);
  }
}
