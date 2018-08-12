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
package org.apache.hadoop.hdfs.server.namenode.sps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.sps.ExternalSPSContext;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

/**
 * Tests that StoragePolicySatisfier daemon is able to check the striped blocks
 * to be moved and finding its expected target locations in order to satisfy the
 * storage policy.
 */
public class TestStoragePolicySatisfierWithStripedFile {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestStoragePolicySatisfierWithStripedFile.class);

  private final int stripesPerBlock = 2;

  private ErasureCodingPolicy ecPolicy;
  private int dataBlocks;
  private int parityBlocks;
  private int cellSize;
  private int defaultStripeBlockSize;
  private Configuration conf;
  private StoragePolicySatisfier sps;
  private ExternalSPSContext ctxt;
  private NameNodeConnector nnc;

  private ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  /**
   * Initialize erasure coding policy.
   */
  @Before
  public void init(){
    ecPolicy = getEcPolicy();
    dataBlocks = ecPolicy.getNumDataUnits();
    parityBlocks = ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    defaultStripeBlockSize = cellSize * stripesPerBlock;
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());
    // Reduced refresh cycle to update latest datanodes.
    conf.setLong(DFSConfigKeys.DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS,
        1000);
    conf.setInt(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_KEY, 30);
    initConfWithStripe(conf, defaultStripeBlockSize);
  }

  /**
   * Tests to verify that all the striped blocks(data + parity blocks) are
   * moving to satisfy the storage policy.
   */
  @Test(timeout = 300000)
  public void testMoverWithFullStripe() throws Exception {
    // start 11 datanodes
    int numOfDatanodes = 11;
    int storagesPerDatanode = 2;
    long capacity = 20 * defaultStripeBlockSize;
    long[][] capacities = new long[numOfDatanodes][storagesPerDatanode];
    for (int i = 0; i < numOfDatanodes; i++) {
      for (int j = 0; j < storagesPerDatanode; j++) {
        capacities[i][j] = capacity;
      }
    }

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .storagesPerDatanode(storagesPerDatanode)
        .storageTypes(new StorageType[][]{
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE}})
        .storageCapacities(capacities)
        .build();

    HdfsAdmin hdfsAdmin = new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    try {
      cluster.waitActive();
      startSPS();
      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // set "/bar" directory with HOT storage policy.
      ClientProtocol client = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();
      String barDir = "/bar";
      client.mkdirs(barDir, new FsPermission((short) 777), true);
      client.setStoragePolicy(barDir, HdfsConstants.HOT_STORAGE_POLICY_NAME);
      // set an EC policy on "/bar" directory
      client.setErasureCodingPolicy(barDir,
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // write file to barDir
      final String fooFile = "/bar/foo";
      long fileLen = cellSize * dataBlocks;
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fooFile),
          fileLen, (short) 3, 0);

      // verify storage types and locations
      LocatedBlocks locatedBlocks = client.getBlockLocations(fooFile, 0,
          fileLen);
      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        for (StorageType type : lb.getStorageTypes()) {
          Assert.assertEquals(StorageType.DISK, type);
        }
      }
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks,
          dataBlocks + parityBlocks);

      // start 5 more datanodes
      int numOfNewDatanodes = 5;
      capacities = new long[numOfNewDatanodes][storagesPerDatanode];
      for (int i = 0; i < numOfNewDatanodes; i++) {
        for (int j = 0; j < storagesPerDatanode; j++) {
          capacities[i][j] = capacity;
        }
      }
      cluster.startDataNodes(conf, 5,
          new StorageType[][]{
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE}},
          true, null, null, null, capacities, null, false, false, false, null);
      cluster.triggerHeartbeats();

      // move file to ARCHIVE
      client.setStoragePolicy(barDir, "COLD");
      hdfsAdmin.satisfyStoragePolicy(new Path(fooFile));
      LOG.info("Sets storage policy to COLD and invoked satisfyStoragePolicy");
      cluster.triggerHeartbeats();

      // verify storage types and locations
      waitExpectedStorageType(cluster, fooFile, fileLen, StorageType.ARCHIVE, 9,
          9, 60000);
    } finally {
      cluster.shutdown();
      sps.stopGracefully();
    }
  }

  /**
   * Tests to verify that only few datanodes are available and few striped
   * blocks are able to move. Others are still trying to find available nodes.
   *
   * For example, we have 3 nodes A(disk, disk), B(disk, disk), C(disk, archive)
   *
   * Assume a block with storage locations A(disk), B(disk), C(disk). Now, set
   * policy as COLD and invoked {@link HdfsAdmin#satisfyStoragePolicy(Path)},
   * while choosing the target node for A, it shouldn't choose C. For C, it
   * should do local block movement as it has ARCHIVE storage type.
   */
  @Test(timeout = 300000)
  public void testWhenOnlyFewTargetNodesAreAvailableToSatisfyStoragePolicy()
      throws Exception {
    // start 10 datanodes
    int numOfDatanodes = 11;
    int storagesPerDatanode = 2;
    long capacity = 20 * defaultStripeBlockSize;
    long[][] capacities = new long[numOfDatanodes][storagesPerDatanode];
    for (int i = 0; i < numOfDatanodes; i++) {
      for (int j = 0; j < storagesPerDatanode; j++) {
        capacities[i][j] = capacity;
      }
    }

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .storagesPerDatanode(storagesPerDatanode)
        .storageTypes(new StorageType[][]{
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE}})
        .storageCapacities(capacities)
        .build();

    HdfsAdmin hdfsAdmin = new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    try {
      cluster.waitActive();
      startSPS();
      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      // set "/bar" directory with HOT storage policy.
      ClientProtocol client = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();
      String barDir = "/bar";
      client.mkdirs(barDir, new FsPermission((short) 777), true);
      client.setStoragePolicy(barDir, HdfsConstants.HOT_STORAGE_POLICY_NAME);
      // set an EC policy on "/bar" directory
      client.setErasureCodingPolicy(barDir,
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // write file to barDir
      final String fooFile = "/bar/foo";
      long fileLen = cellSize * dataBlocks;
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fooFile),
          fileLen, (short) 3, 0);

      // verify storage types and locations
      LocatedBlocks locatedBlocks = client.getBlockLocations(fooFile, 0,
          fileLen);
      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        for (StorageType type : lb.getStorageTypes()) {
          Assert.assertEquals(StorageType.DISK, type);
        }
      }
      Thread.sleep(5000);
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks,
          dataBlocks + parityBlocks);

      // start 2 more datanodes
      int numOfNewDatanodes = 2;
      capacities = new long[numOfNewDatanodes][storagesPerDatanode];
      for (int i = 0; i < numOfNewDatanodes; i++) {
        for (int j = 0; j < storagesPerDatanode; j++) {
          capacities[i][j] = capacity;
        }
      }
      cluster.startDataNodes(conf, 2,
          new StorageType[][]{
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE}},
          true, null, null, null, capacities, null, false, false, false, null);
      cluster.triggerHeartbeats();

      // Move file to ARCHIVE. Only 5 datanodes are available with ARCHIVE
      // storage type.
      client.setStoragePolicy(barDir, "COLD");
      hdfsAdmin.satisfyStoragePolicy(new Path(fooFile));
      LOG.info("Sets storage policy to COLD and invoked satisfyStoragePolicy");
      cluster.triggerHeartbeats();

      waitForAttemptedItems(1, 30000);
      // verify storage types and locations.
      waitExpectedStorageType(cluster, fooFile, fileLen, StorageType.ARCHIVE, 5,
          9, 60000);
    } finally {
      cluster.shutdown();
      sps.stopGracefully();
    }
  }

  /**
   * Test SPS for low redundant file blocks.
   * 1. Create cluster with 10 datanode.
   * 1. Create one striped file with default EC Policy.
   * 2. Set policy and call satisfyStoragePolicy for file.
   * 3. Stop NameNode and Datanodes.
   * 4. Start NameNode with 5 datanode and wait for block movement.
   * 5. Start remaining 5 datanode.
   * 6. All replica  should be moved in proper storage based on policy.
   */
  @Test(timeout = 300000)
  public void testSPSWhenFileHasLowRedundancyBlocks() throws Exception {
    // start 9 datanodes
    int numOfDatanodes = 9;
    int storagesPerDatanode = 2;
    long capacity = 20 * defaultStripeBlockSize;
    long[][] capacities = new long[numOfDatanodes][storagesPerDatanode];
    for (int i = 0; i < numOfDatanodes; i++) {
      for (int j = 0; j < storagesPerDatanode; j++) {
        capacities[i][j] = capacity;
      }
    }

    conf.set(DFSConfigKeys
        .DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
        "3000");
    conf.set(DFSConfigKeys
        .DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
        "5000");
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .storagesPerDatanode(storagesPerDatanode)
        .storageTypes(new StorageType[][]{
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE}})
        .storageCapacities(capacities)
        .build();
    try {
      cluster.waitActive();
      startSPS();
      DistributedFileSystem fs = cluster.getFileSystem();
      fs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      Path barDir = new Path("/bar");
      fs.mkdirs(barDir);
      // set an EC policy on "/bar" directory
      fs.setErasureCodingPolicy(barDir,
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // write file to barDir
      final Path fooFile = new Path("/bar/foo");
      long fileLen = cellSize * dataBlocks;
      DFSTestUtil.createFile(cluster.getFileSystem(), fooFile,
          fileLen, (short) 3, 0);

      // Move file to ARCHIVE.
      fs.setStoragePolicy(barDir, "COLD");
      //Stop DataNodes and restart namenode
      List<DataNodeProperties> list = new ArrayList<>(numOfDatanodes);
      for (int i = 0; i < numOfDatanodes; i++) {
        list.add(cluster.stopDataNode(0));
      }
      cluster.restartNameNodes();
      // Restart half datanodes
      for (int i = 0; i < 5; i++) {
        cluster.restartDataNode(list.get(i), false);
      }
      cluster.waitActive();
      fs.satisfyStoragePolicy(fooFile);
      DFSTestUtil.waitExpectedStorageType(fooFile.toString(),
          StorageType.ARCHIVE, 5, 30000, cluster.getFileSystem());
      //Start remaining datanodes
      for (int i = numOfDatanodes - 1; i >= 5; i--) {
        cluster.restartDataNode(list.get(i), false);
      }
      cluster.waitActive();
      // verify storage types and locations.
      waitExpectedStorageType(cluster, fooFile.toString(), fileLen,
          StorageType.ARCHIVE, 9, 9, 60000);
    } finally {
      cluster.shutdown();
      sps.stopGracefully();
    }
  }


  /**
   * Tests to verify that for the given path, no blocks under the given path
   * will be scheduled for block movement as there are no available datanode
   * with required storage type.
   *
   * For example, there are two block for a file:
   *
   * File1 => blk_1[locations=A(DISK),B(DISK),C(DISK)],
   * blk_2[locations=A(DISK),B(DISK),C(DISK)]. Now, set storage policy to COLD.
   * No datanode is available with storage type ARCHIVE.
   *
   * SPS won't schedule any block movement for this path.
   */
  @Test(timeout = 300000)
  public void testWhenNoTargetDatanodeToSatisfyStoragePolicy()
      throws Exception {
    // start 10 datanodes
    int numOfDatanodes = 10;
    int storagesPerDatanode = 2;
    long capacity = 20 * defaultStripeBlockSize;
    long[][] capacities = new long[numOfDatanodes][storagesPerDatanode];
    for (int i = 0; i < numOfDatanodes; i++) {
      for (int j = 0; j < storagesPerDatanode; j++) {
        capacities[i][j] = capacity;
      }
    }

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .storagesPerDatanode(storagesPerDatanode)
        .storageTypes(new StorageType[][]{
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK}})
        .storageCapacities(capacities)
        .build();

    HdfsAdmin hdfsAdmin = new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    try {
      cluster.waitActive();
      startSPS();
      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      // set "/bar" directory with HOT storage policy.
      ClientProtocol client = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();
      String barDir = "/bar";
      client.mkdirs(barDir, new FsPermission((short) 777), true);
      client.setStoragePolicy(barDir, HdfsConstants.HOT_STORAGE_POLICY_NAME);
      // set an EC policy on "/bar" directory
      client.setErasureCodingPolicy(barDir,
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // write file to barDir
      final String fooFile = "/bar/foo";
      long fileLen = cellSize * dataBlocks;
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fooFile),
          fileLen, (short) 3, 0);

      // verify storage types and locations
      LocatedBlocks locatedBlocks = client.getBlockLocations(fooFile, 0,
          fileLen);
      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        for (StorageType type : lb.getStorageTypes()) {
          Assert.assertEquals(StorageType.DISK, type);
        }
      }
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks,
          dataBlocks + parityBlocks);

      // Move file to ARCHIVE. Only 5 datanodes are available with ARCHIVE
      // storage type.
      client.setStoragePolicy(barDir, "COLD");
      hdfsAdmin.satisfyStoragePolicy(new Path(fooFile));
      LOG.info("Sets storage policy to COLD and invoked satisfyStoragePolicy");
      cluster.triggerHeartbeats();

      waitForAttemptedItems(1, 30000);
      // verify storage types and locations.
      waitExpectedStorageType(cluster, fooFile, fileLen, StorageType.DISK, 9, 9,
          60000);
      waitForAttemptedItems(1, 30000);
    } finally {
      cluster.shutdown();
      sps.stopGracefully();
    }
  }

  private void startSPS() throws IOException {
    nnc = DFSTestUtil.getNameNodeConnector(conf,
        HdfsServerConstants.MOVER_ID_PATH, 1, false);

    sps = new StoragePolicySatisfier(conf);
    ctxt = new ExternalSPSContext(sps, nnc);
    sps.init(ctxt);
    sps.start(StoragePolicySatisfierMode.EXTERNAL);
  }

  private static void initConfWithStripe(Configuration conf,
      int stripeBlockSize) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, stripeBlockSize);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
  }

  // Check whether the Block movement has been successfully completed to satisfy
  // the storage policy for the given file.
  private void waitExpectedStorageType(MiniDFSCluster cluster,
      final String fileName, long fileLen,
      final StorageType expectedStorageType, int expectedStorageCount,
      int expectedBlkLocationCount, int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        int actualStorageCount = 0;
        try {
          LocatedBlocks locatedBlocks = cluster.getFileSystem().getClient()
              .getLocatedBlocks(fileName, 0, fileLen);
          for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
            LOG.info("LocatedBlocks => Size {}, locs {}",
                lb.getLocations().length, lb);
            if (lb.getLocations().length > expectedBlkLocationCount) {
              return false;
            }
            for (StorageType storageType : lb.getStorageTypes()) {
              if (expectedStorageType == storageType) {
                actualStorageCount++;
              } else {
                LOG.info("Expected storage type {} and actual {}",
                    expectedStorageType, storageType);
              }
            }
          }
          LOG.info(
              expectedStorageType + " replica count, expected={} and actual={}",
              expectedStorageCount, actualStorageCount);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }
        return expectedStorageCount == actualStorageCount;
      }
    }, 100, timeout);
  }

  private void waitForAttemptedItems(long expectedBlkMovAttemptedCount,
      int timeout) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LOG.info("expectedAttemptedItemsCount={} actualAttemptedItemsCount={}",
            expectedBlkMovAttemptedCount,
            ((BlockStorageMovementAttemptedItems) (sps
                .getAttemptedItemsMonitor())).getAttemptedItemsCount());
        return ((BlockStorageMovementAttemptedItems) (sps
            .getAttemptedItemsMonitor()))
                .getAttemptedItemsCount() == expectedBlkMovAttemptedCount;
      }
    }, 100, timeout);
  }
}
