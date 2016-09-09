/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test Disk Balancer.
 */
public class TestDiskBalancer {

  private static final String PLAN_FILE = "/system/current.plan.json";
  static final Logger LOG = LoggerFactory.getLogger(TestDiskBalancer.class);

  @Test
  public void testDiskBalancerNameNodeConnectivity() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    final int numDatanodes = 2;
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes).build();
    try {
      cluster.waitActive();
      ClusterConnector nameNodeConnector =
          ConnectorFactory.getCluster(cluster.getFileSystem(0).getUri(), conf);

      DiskBalancerCluster diskBalancerCluster =
          new DiskBalancerCluster(nameNodeConnector);
      diskBalancerCluster.readClusterInfo();
      assertEquals(diskBalancerCluster.getNodes().size(), numDatanodes);
      DataNode dnNode = cluster.getDataNodes().get(0);
      DiskBalancerDataNode dbDnNode =
          diskBalancerCluster.getNodeByUUID(dnNode.getDatanodeUuid());
      assertEquals(dnNode.getDatanodeUuid(), dbDnNode.getDataNodeUUID());
      assertEquals(dnNode.getDatanodeId().getIpAddr(),
          dbDnNode.getDataNodeIP());
      assertEquals(dnNode.getDatanodeId().getHostName(),
          dbDnNode.getDataNodeName());
      try (FsDatasetSpi.FsVolumeReferences ref = dnNode.getFSDataset()
          .getFsVolumeReferences()) {
        assertEquals(ref.size(), dbDnNode.getVolumeCount());
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * This test simulates a real Data node working with DiskBalancer.
   * <p>
   * Here is the overview of this test.
   * <p>
   * 1. Write a bunch of blocks and move them to one disk to create imbalance.
   * 2. Rewrite  the capacity of the disks in DiskBalancer Model so that planner
   * will produce a move plan. 3. Execute the move plan and wait unitl the plan
   * is done. 4. Verify the source disk has blocks now.
   *
   * @throws Exception
   */
  @Test
  public void testDiskBalancerEndToEnd() throws Exception {

    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    final int blockCount = 100;
    final int blockSize = 1024;
    final int diskCount = 2;
    final int dataNodeCount = 1;
    final int dataNodeIndex = 0;
    final int sourceDiskIndex = 0;

    MiniDFSCluster cluster = new ClusterBuilder()
        .setBlockCount(blockCount)
        .setBlockSize(blockSize)
        .setDiskCount(diskCount)
        .setNumDatanodes(dataNodeCount)
        .setConf(conf)
        .build();
    try {
      DataMover dataMover = new DataMover(cluster, dataNodeIndex,
          sourceDiskIndex, conf, blockSize, blockCount);
      dataMover.moveDataToSourceDisk();
      NodePlan plan = dataMover.generatePlan();
      dataMover.executePlan(plan);
      dataMover.verifyPlanExectionDone();
      dataMover.verifyAllVolumesHaveData();
      dataMover.verifyTolerance(plan, 0, sourceDiskIndex, 10);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testBalanceDataBetweenMultiplePairsOfVolumes()
      throws Exception {

    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    final int blockCount = 1000;
    final int blockSize = 1024;

    // create 3 disks, that means we will have 2 plans
    // Move Data from disk0->disk1 and disk0->disk2.
    final int diskCount = 3;
    final int dataNodeCount = 1;
    final int dataNodeIndex = 0;
    final int sourceDiskIndex = 0;


    MiniDFSCluster cluster = new ClusterBuilder()
        .setBlockCount(blockCount)
        .setBlockSize(blockSize)
        .setDiskCount(diskCount)
        .setNumDatanodes(dataNodeCount)
        .setConf(conf)
        .build();


    try {
      DataMover dataMover = new DataMover(cluster, dataNodeIndex,
          sourceDiskIndex, conf, blockSize, blockCount);
      dataMover.moveDataToSourceDisk();
      NodePlan plan = dataMover.generatePlan();

      // 3 disks , The plan should move data both disks,
      // so we must have 2 plan steps.
      assertEquals(plan.getVolumeSetPlans().size(), 2);

      dataMover.executePlan(plan);
      dataMover.verifyPlanExectionDone();
      dataMover.verifyAllVolumesHaveData();
      dataMover.verifyTolerance(plan, 0, sourceDiskIndex, 10);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Sets alll Disks capacity to size specified.
   *
   * @param cluster - DiskBalancerCluster
   * @param size    - new size of the disk
   */
  private void setVolumeCapacity(DiskBalancerCluster cluster, long size,
                                 String diskType) {
    Preconditions.checkNotNull(cluster);
    for (DiskBalancerDataNode node : cluster.getNodes()) {
      for (DiskBalancerVolume vol :
          node.getVolumeSets().get(diskType).getVolumes()) {
        vol.setCapacity(size);
      }
      node.getVolumeSets().get(diskType).computeVolumeDataDensity();
    }
  }

  /**
   * Helper class that allows us to create different kinds of MiniDFSClusters
   * and populate data.
   */
  static class ClusterBuilder {
    private Configuration conf;
    private int blockSize;
    private int numDatanodes;
    private int fileLen;
    private int blockCount;
    private int diskCount;

    public ClusterBuilder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public ClusterBuilder setBlockSize(int blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public ClusterBuilder setNumDatanodes(int datanodeCount) {
      this.numDatanodes = datanodeCount;
      return this;
    }

    public ClusterBuilder setBlockCount(int blockCount) {
      this.blockCount = blockCount;
      return this;
    }

    public ClusterBuilder setDiskCount(int diskCount) {
      this.diskCount = diskCount;
      return this;
    }

    private long[] getCapacities(int diskCount, int bSize, int fSize) {
      Preconditions.checkState(diskCount > 0);
      long[] capacities = new long[diskCount];
      for (int x = 0; x < diskCount; x++) {
        capacities[x] = diskCount * bSize * fSize * 2L;
      }
      return capacities;
    }

    private StorageType[] getStorageTypes(int diskCount) {
      Preconditions.checkState(diskCount > 0);
      StorageType[] array = new StorageType[diskCount];
      for (int x = 0; x < diskCount; x++) {
        array[x] = StorageType.DISK;
      }
      return array;
    }

    public MiniDFSCluster build() throws IOException, TimeoutException,
        InterruptedException {
      Preconditions.checkNotNull(this.conf);
      Preconditions.checkState(blockSize > 0);
      Preconditions.checkState(numDatanodes > 0);
      fileLen = blockCount * blockSize;
      Preconditions.checkState(fileLen > 0);
      conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
      conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, blockSize);
      conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);

      final String fileName = "/tmp.txt";
      Path filePath = new Path(fileName);
      fileLen = blockCount * blockSize;


      // Write a file and restart the cluster
      MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(numDatanodes)
          .storageCapacities(getCapacities(diskCount, blockSize, fileLen))
          .storageTypes(getStorageTypes(diskCount))
          .storagesPerDatanode(diskCount)
          .build();
      generateData(filePath, cluster);
      cluster.restartDataNodes();
      cluster.waitActive();
      return cluster;
    }

    private void generateData(Path filePath, MiniDFSCluster cluster)
        throws IOException, InterruptedException, TimeoutException {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem(0);
      TestBalancer.createFile(cluster, filePath, fileLen, (short) 1,
          numDatanodes - 1);
      DFSTestUtil.waitReplication(fs, filePath, (short) 1);
      cluster.restartDataNodes();
      cluster.waitActive();
    }
  }

  class DataMover {
    private final MiniDFSCluster cluster;
    private final int sourceDiskIndex;
    private final int dataNodeIndex;
    private final Configuration conf;
    private final int blockCount;
    private final int blockSize;
    private DataNode node;

    /**
     * Constructs a DataMover class.
     *
     * @param cluster         - MiniDFSCluster.
     * @param dataNodeIndex   - Datanode to operate against.
     * @param sourceDiskIndex - source Disk Index.
     */
    public DataMover(MiniDFSCluster cluster, int dataNodeIndex, int
        sourceDiskIndex, Configuration conf, int blockSize, int
                         blockCount) {
      this.cluster = cluster;
      this.dataNodeIndex = dataNodeIndex;
      this.node = cluster.getDataNodes().get(dataNodeIndex);
      this.sourceDiskIndex = sourceDiskIndex;
      this.conf = conf;
      this.blockCount = blockCount;
      this.blockSize = blockSize;
    }

    /**
     * Moves all data to a source disk to create disk imbalance so we can run a
     * planner.
     *
     * @throws IOException
     */
    public void moveDataToSourceDisk() throws IOException {
      moveAllDataToDestDisk(this.node, sourceDiskIndex);
      cluster.restartDataNodes();
      cluster.waitActive();

    }

    /**
     * Moves all data in the data node to one disk.
     *
     * @param dataNode      - Datanode
     * @param destDiskindex - Index of the destination disk.
     */
    private void moveAllDataToDestDisk(DataNode dataNode, int destDiskindex)
        throws IOException {
      Preconditions.checkNotNull(dataNode);
      Preconditions.checkState(destDiskindex >= 0);
      try (FsDatasetSpi.FsVolumeReferences refs =
               dataNode.getFSDataset().getFsVolumeReferences()) {
        if (refs.size() <= destDiskindex) {
          throw new IllegalArgumentException("Invalid Disk index.");
        }
        FsVolumeImpl dest = (FsVolumeImpl) refs.get(destDiskindex);
        for (int x = 0; x < refs.size(); x++) {
          if (x == destDiskindex) {
            continue;
          }
          FsVolumeImpl source = (FsVolumeImpl) refs.get(x);
          DiskBalancerTestUtil.moveAllDataToDestVolume(dataNode.getFSDataset(),
              source, dest);

        }
      }
    }

    /**
     * Generates a NodePlan for the datanode specified.
     *
     * @return NodePlan.
     */
    public NodePlan generatePlan() throws Exception {

      // Start up a disk balancer and read the cluster info.
      node = cluster.getDataNodes().get(dataNodeIndex);
      ClusterConnector nameNodeConnector =
          ConnectorFactory.getCluster(cluster.getFileSystem(dataNodeIndex)
              .getUri(), conf);

      DiskBalancerCluster diskBalancerCluster =
          new DiskBalancerCluster(nameNodeConnector);
      diskBalancerCluster.readClusterInfo();
      List<DiskBalancerDataNode> nodesToProcess = new LinkedList<>();

      // Rewrite the capacity in the model to show that disks need
      // re-balancing.
      setVolumeCapacity(diskBalancerCluster, blockSize * 2L * blockCount,
          "DISK");
      // Pick a node to process.
      nodesToProcess.add(diskBalancerCluster.getNodeByUUID(
          node.getDatanodeUuid()));
      diskBalancerCluster.setNodesToProcess(nodesToProcess);

      // Compute a plan.
      List<NodePlan> clusterplan = diskBalancerCluster.computePlan(0.0f);

      // Now we must have a plan,since the node is imbalanced and we
      // asked the disk balancer to create a plan.
      assertTrue(clusterplan.size() == 1);

      NodePlan plan = clusterplan.get(0);
      plan.setNodeUUID(node.getDatanodeUuid());
      plan.setTimeStamp(Time.now());

      assertNotNull(plan.getVolumeSetPlans());
      assertTrue(plan.getVolumeSetPlans().size() > 0);
      plan.getVolumeSetPlans().get(0).setTolerancePercent(10);
      return plan;
    }

    /**
     * Waits for a plan executing to finish.
     */
    public void executePlan(NodePlan plan) throws
        IOException, TimeoutException, InterruptedException {

      node = cluster.getDataNodes().get(dataNodeIndex);
      String planJson = plan.toJson();
      String planID = DigestUtils.shaHex(planJson);

      // Submit the plan and wait till the execution is done.
      node.submitDiskBalancerPlan(planID, 1, PLAN_FILE, planJson,
          false);
      String jmxString = node.getDiskBalancerStatus();
      assertNotNull(jmxString);
      DiskBalancerWorkStatus status =
          DiskBalancerWorkStatus.parseJson(jmxString);
      DiskBalancerWorkStatus realStatus = node.queryDiskBalancerPlan();
      assertEquals(realStatus.getPlanID(), status.getPlanID());

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            return node.queryDiskBalancerPlan().getResult() ==
                DiskBalancerWorkStatus.Result.PLAN_DONE;
          } catch (IOException ex) {
            return false;
          }
        }
      }, 1000, 100000);
    }

    /**
     * Verifies the Plan Execution has been done.
     */
    public void verifyPlanExectionDone() throws IOException {
      node = cluster.getDataNodes().get(dataNodeIndex);
      assertEquals(node.queryDiskBalancerPlan().getResult(),
          DiskBalancerWorkStatus.Result.PLAN_DONE);
    }

    /**
     * Once diskBalancer is run, all volumes mush has some data.
     */
    public void verifyAllVolumesHaveData() throws IOException {
      node = cluster.getDataNodes().get(dataNodeIndex);
      try (FsDatasetSpi.FsVolumeReferences refs =
               node.getFSDataset().getFsVolumeReferences()) {
        for (FsVolumeSpi volume : refs) {
          assertTrue(DiskBalancerTestUtil.getBlockCount(volume) > 0);
          LOG.info(refs.toString() + " : Block Count : {}",
              DiskBalancerTestUtil.getBlockCount(volume));
        }
      }
    }

    /**
     * Verifies that tolerance values are honored correctly.
     */
    public void verifyTolerance(NodePlan plan, int planIndex, int
        sourceDiskIndex, int tolerance) throws IOException {
      // Tolerance
      long delta = (plan.getVolumeSetPlans().get(planIndex).getBytesToMove()
          * tolerance) / 100;
      FsVolumeImpl volume = null;
      try (FsDatasetSpi.FsVolumeReferences refs =
               node.getFSDataset().getFsVolumeReferences()) {
        volume = (FsVolumeImpl) refs.get(sourceDiskIndex);
        assertTrue(DiskBalancerTestUtil.getBlockCount(volume) > 0);

        assertTrue(
            (DiskBalancerTestUtil.getBlockCount(volume) *
                (blockSize + delta)) >=
                plan.getVolumeSetPlans().get(0).getBytesToMove());
      }
    }
  }
}
