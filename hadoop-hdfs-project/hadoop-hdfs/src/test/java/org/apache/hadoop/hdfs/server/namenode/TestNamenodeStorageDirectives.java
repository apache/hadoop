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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.net.Node;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test to ensure that the StorageType and StorageID sent from Namenode
 * to DFSClient are respected.
 */
public class TestNamenodeStorageDirectives {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestNamenodeStorageDirectives.class);

  private static final int BLOCK_SIZE = 512;

  private MiniDFSCluster cluster;

  @After
  public void tearDown() {
    shutdown();
  }

  private void startDFSCluster(int numNameNodes, int numDataNodes,
      int storagePerDataNode, StorageType[][] storageTypes)
      throws IOException {
    startDFSCluster(numNameNodes, numDataNodes, storagePerDataNode,
        storageTypes, RoundRobinVolumeChoosingPolicy.class,
        BlockPlacementPolicyDefault.class);
  }

  private void startDFSCluster(int numNameNodes, int numDataNodes,
      int storagePerDataNode, StorageType[][] storageTypes,
      Class<? extends VolumeChoosingPolicy> volumeChoosingPolicy,
      Class<? extends BlockPlacementPolicy> blockPlacementPolicy) throws
      IOException {
    shutdown();
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    /*
     * Lower the DN heartbeat, DF rate, and recheck interval to one second
     * so state about failures and datanode death propagates faster.
     */
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_DF_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        1000);
    /* Allow 1 volume failure */
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    conf.setTimeDuration(DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY,
        0, TimeUnit.MILLISECONDS);
    conf.setClass(
        DFSConfigKeys.DFS_DATANODE_FSDATASET_VOLUME_CHOOSING_POLICY_KEY,
        volumeChoosingPolicy, VolumeChoosingPolicy.class);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        blockPlacementPolicy, BlockPlacementPolicy.class);

    MiniDFSNNTopology nnTopology =
        MiniDFSNNTopology.simpleFederatedTopology(numNameNodes);

    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(nnTopology)
        .numDataNodes(numDataNodes)
        .storagesPerDatanode(storagePerDataNode)
        .storageTypes(storageTypes)
        .build();
    cluster.waitActive();
  }

  private void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void createFile(Path path, int numBlocks, short replicateFactor)
      throws IOException, InterruptedException, TimeoutException {
    createFile(0, path, numBlocks, replicateFactor);
  }

  private void createFile(int fsIdx, Path path, int numBlocks,
      short replicateFactor)
      throws IOException, TimeoutException, InterruptedException {
    final int seed = 0;
    final DistributedFileSystem fs = cluster.getFileSystem(fsIdx);
    DFSTestUtil.createFile(fs, path, BLOCK_SIZE * numBlocks,
        replicateFactor, seed);
    DFSTestUtil.waitReplication(fs, path, replicateFactor);
  }

  private boolean verifyFileReplicasOnStorageType(Path path, int numBlocks,
      StorageType storageType) throws IOException {
    MiniDFSCluster.NameNodeInfo info = cluster.getNameNodeInfos()[0];
    InetSocketAddress addr = info.nameNode.getServiceRpcAddress();
    assert addr.getPort() != 0;
    DFSClient client = new DFSClient(addr, cluster.getConfiguration(0));

    FileSystem fs = cluster.getFileSystem();

    if (!fs.exists(path)) {
      LOG.info("verifyFileReplicasOnStorageType: file {} does not exist", path);
      return false;
    }
    long fileLength = client.getFileInfo(path.toString()).getLen();
    int foundBlocks = 0;
    LocatedBlocks locatedBlocks =
        client.getLocatedBlocks(path.toString(), 0, fileLength);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      for (StorageType st : locatedBlock.getStorageTypes()) {
        if (st == storageType) {
          foundBlocks++;
        }
      }
    }

    LOG.info("Found {}/{} blocks on StorageType {}",
        foundBlocks, numBlocks, storageType);
    final boolean isValid = foundBlocks >= numBlocks;
    return isValid;
  }

  private void testStorageTypes(StorageType[][] storageTypes,
      String storagePolicy, StorageType[] expectedStorageTypes,
      StorageType[] unexpectedStorageTypes) throws ReconfigurationException,
      InterruptedException, TimeoutException, IOException {
    final int numDataNodes = storageTypes.length;
    final int storagePerDataNode = storageTypes[0].length;
    startDFSCluster(1, numDataNodes, storagePerDataNode, storageTypes);
    cluster.getFileSystem(0).setStoragePolicy(new Path("/"), storagePolicy);
    Path testFile = new Path("/test");
    final short replFactor = 2;
    final int numBlocks = 10;
    createFile(testFile, numBlocks, replFactor);

    for (StorageType storageType: expectedStorageTypes) {
      assertTrue(verifyFileReplicasOnStorageType(testFile, numBlocks,
          storageType));
    }

    for (StorageType storageType: unexpectedStorageTypes) {
      assertFalse(verifyFileReplicasOnStorageType(testFile, numBlocks,
          storageType));
    }
  }

  /**
   * Verify that writing to SSD and DISK will write to the correct Storage
   * Types.
   * @throws IOException
   */
  @Test(timeout=60000)
  public void testTargetStorageTypes() throws ReconfigurationException,
      InterruptedException, TimeoutException, IOException {
    // DISK and not anything else.
    testStorageTypes(new StorageType[][]{
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK}},
        "ONE_SSD",
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.RAM_DISK, StorageType.ARCHIVE});
    // only on SSD.
    testStorageTypes(new StorageType[][]{
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK}},
        "ALL_SSD",
        new StorageType[]{StorageType.SSD},
        new StorageType[]{StorageType.RAM_DISK, StorageType.DISK,
            StorageType.ARCHIVE});
    // only on SSD.
    testStorageTypes(new StorageType[][]{
            {StorageType.SSD, StorageType.DISK, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK, StorageType.DISK}},
        "ALL_SSD",
        new StorageType[]{StorageType.SSD},
        new StorageType[]{StorageType.RAM_DISK, StorageType.DISK,
            StorageType.ARCHIVE});

    // DISK and not anything else.
    testStorageTypes(new StorageType[][] {
            {StorageType.RAM_DISK, StorageType.SSD},
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK}},
        "HOT",
        new StorageType[]{StorageType.DISK},
        new StorageType[] {StorageType.RAM_DISK, StorageType.SSD,
            StorageType.ARCHIVE});

    testStorageTypes(new StorageType[][] {
            {StorageType.RAM_DISK, StorageType.SSD},
            {StorageType.SSD, StorageType.DISK},
            {StorageType.ARCHIVE, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.ARCHIVE}},
        "WARM",
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.RAM_DISK, StorageType.SSD});

    testStorageTypes(new StorageType[][] {
            {StorageType.RAM_DISK, StorageType.SSD},
            {StorageType.SSD, StorageType.DISK},
            {StorageType.ARCHIVE, StorageType.ARCHIVE},
            {StorageType.ARCHIVE, StorageType.ARCHIVE}},
        "COLD",
        new StorageType[]{StorageType.ARCHIVE},
        new StorageType[]{StorageType.RAM_DISK, StorageType.SSD,
            StorageType.DISK});

    // We wait for Lasy Persist to write to disk.
    testStorageTypes(new StorageType[][] {
            {StorageType.RAM_DISK, StorageType.SSD},
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK}},
        "LAZY_PERSIST",
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.RAM_DISK, StorageType.SSD,
            StorageType.ARCHIVE});
  }

  /**
   * A VolumeChoosingPolicy test stub used to verify that the storageId passed
   * in is indeed in the list of volumes.
   * @param <V>
   */
  private static class TestVolumeChoosingPolicy<V extends FsVolumeSpi>
      extends RoundRobinVolumeChoosingPolicy<V> {
    static String expectedStorageId;

    @Override
    public V chooseVolume(List<V> volumes, long replicaSize, String storageId)
        throws IOException {
      assertEquals(expectedStorageId, storageId);
      return super.chooseVolume(volumes, replicaSize, storageId);
    }
  }

  private static class TestBlockPlacementPolicy
      extends BlockPlacementPolicyDefault {
    static DatanodeStorageInfo[] dnStorageInfosToReturn;

    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
        Node writer, List<DatanodeStorageInfo> chosenNodes,
        boolean returnChosenNodes, Set<Node> excludedNodes, long blocksize,
        final BlockStoragePolicy storagePolicy, EnumSet<AddBlockFlag> flags) {
      return dnStorageInfosToReturn;
    }
  }

  private DatanodeStorageInfo getDatanodeStorageInfo(int dnIndex)
      throws UnregisteredNodeException {
    if (cluster == null) {
      return null;
    }
    DatanodeID dnId = cluster.getDataNodes().get(dnIndex).getDatanodeId();
    DatanodeManager dnManager = cluster.getNamesystem()
            .getBlockManager().getDatanodeManager();
    return dnManager.getDatanode(dnId).getStorageInfos()[0];
  }

  @Test(timeout=60000)
  public void testStorageIDBlockPlacementSpecific()
      throws ReconfigurationException, InterruptedException, TimeoutException,
      IOException {
    final StorageType[][] storageTypes = {
        {StorageType.DISK, StorageType.DISK},
        {StorageType.DISK, StorageType.DISK},
        {StorageType.DISK, StorageType.DISK},
        {StorageType.DISK, StorageType.DISK},
        {StorageType.DISK, StorageType.DISK},
    };
    final int numDataNodes = storageTypes.length;
    final int storagePerDataNode = storageTypes[0].length;
    startDFSCluster(1, numDataNodes, storagePerDataNode, storageTypes,
        TestVolumeChoosingPolicy.class, TestBlockPlacementPolicy.class);
    Path testFile = new Path("/test");
    final short replFactor = 1;
    final int numBlocks = 10;
    DatanodeStorageInfo dnInfoToUse = getDatanodeStorageInfo(0);
    TestBlockPlacementPolicy.dnStorageInfosToReturn =
        new DatanodeStorageInfo[] {dnInfoToUse};
    TestVolumeChoosingPolicy.expectedStorageId = dnInfoToUse.getStorageID();
    //file creation invokes both BlockPlacementPolicy and VolumeChoosingPolicy,
    //and will test that the storage ids match
    createFile(testFile, numBlocks, replFactor);
  }
}
