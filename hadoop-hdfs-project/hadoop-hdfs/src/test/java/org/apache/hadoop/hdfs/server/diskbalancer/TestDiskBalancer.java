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
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test Disk Balancer.
 */
public class TestDiskBalancer {

  private static final String PLAN_FILE = "/system/current.plan.json";

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
    final int defaultBlockSize = 100;
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, defaultBlockSize);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, defaultBlockSize);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    final int numDatanodes = 1;
    final String fileName = "/tmp.txt";
    final Path filePath = new Path(fileName);
    final int blocks = 100;
    final int blocksSize = 1024;
    final int fileLen = blocks * blocksSize;


    // Write a file and restart the cluster
    long[] capacities = new long[]{defaultBlockSize * 2 * fileLen,
        defaultBlockSize * 2 * fileLen};
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes)
        .storageCapacities(capacities)
        .storageTypes(new StorageType[]{StorageType.DISK, StorageType.DISK})
        .storagesPerDatanode(2)
        .build();
    FsVolumeImpl source = null;
    FsVolumeImpl dest = null;
    try {
      cluster.waitActive();
      Random r = new Random();
      FileSystem fs = cluster.getFileSystem(0);
      TestBalancer.createFile(cluster, filePath, fileLen, (short) 1,
          numDatanodes - 1);

      DFSTestUtil.waitReplication(fs, filePath, (short) 1);
      cluster.restartDataNodes();
      cluster.waitActive();

      // Get the data node and move all data to one disk.
      DataNode dnNode = cluster.getDataNodes().get(numDatanodes - 1);
      try (FsDatasetSpi.FsVolumeReferences refs =
               dnNode.getFSDataset().getFsVolumeReferences()) {
        source = (FsVolumeImpl) refs.get(0);
        dest = (FsVolumeImpl) refs.get(1);
        assertTrue(DiskBalancerTestUtil.getBlockCount(source) > 0);
        DiskBalancerTestUtil.moveAllDataToDestVolume(dnNode.getFSDataset(),
            source, dest);
        assertTrue(DiskBalancerTestUtil.getBlockCount(source) == 0);
      }

      cluster.restartDataNodes();
      cluster.waitActive();

      // Start up a disk balancer and read the cluster info.
      final DataNode newDN = cluster.getDataNodes().get(numDatanodes - 1);
      ClusterConnector nameNodeConnector =
          ConnectorFactory.getCluster(cluster.getFileSystem(0).getUri(), conf);

      DiskBalancerCluster diskBalancerCluster =
          new DiskBalancerCluster(nameNodeConnector);
      diskBalancerCluster.readClusterInfo();
      List<DiskBalancerDataNode> nodesToProcess = new LinkedList<>();

      // Rewrite the capacity in the model to show that disks need
      // re-balancing.
      setVolumeCapacity(diskBalancerCluster, defaultBlockSize * 2 * fileLen,
          "DISK");
      // Pick a node to process.
      nodesToProcess.add(diskBalancerCluster.getNodeByUUID(dnNode
          .getDatanodeUuid()));
      diskBalancerCluster.setNodesToProcess(nodesToProcess);

      // Compute a plan.
      List<NodePlan> clusterplan = diskBalancerCluster.computePlan(0.0f);

      // Now we must have a plan,since the node is imbalanced and we
      // asked the disk balancer to create a plan.
      assertTrue(clusterplan.size() == 1);

      NodePlan plan = clusterplan.get(0);
      plan.setNodeUUID(dnNode.getDatanodeUuid());
      plan.setTimeStamp(Time.now());
      String planJson = plan.toJson();
      String planID = DigestUtils.shaHex(planJson);
      assertNotNull(plan.getVolumeSetPlans());
      assertTrue(plan.getVolumeSetPlans().size() > 0);
      plan.getVolumeSetPlans().get(0).setTolerancePercent(10);

      // Submit the plan and wait till the execution is done.
      newDN.submitDiskBalancerPlan(planID, 1, PLAN_FILE, planJson, false);
      String jmxString = newDN.getDiskBalancerStatus();
      assertNotNull(jmxString);
      DiskBalancerWorkStatus status =
          DiskBalancerWorkStatus.parseJson(jmxString);
      DiskBalancerWorkStatus realStatus = newDN.queryDiskBalancerPlan();
      assertEquals(realStatus.getPlanID(), status.getPlanID());

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            return newDN.queryDiskBalancerPlan().getResult() ==
                DiskBalancerWorkStatus.Result.PLAN_DONE;
          } catch (IOException ex) {
            return false;
          }
        }
      }, 1000, 100000);


      //verify that it worked.
      dnNode = cluster.getDataNodes().get(numDatanodes - 1);
      assertEquals(dnNode.queryDiskBalancerPlan().getResult(),
          DiskBalancerWorkStatus.Result.PLAN_DONE);
      try (FsDatasetSpi.FsVolumeReferences refs =
               dnNode.getFSDataset().getFsVolumeReferences()) {
        source = (FsVolumeImpl) refs.get(0);
        assertTrue(DiskBalancerTestUtil.getBlockCount(source) > 0);
      }


      // Tolerance
      long delta = (plan.getVolumeSetPlans().get(0).getBytesToMove()
          * 10) / 100;
      assertTrue(
          (DiskBalancerTestUtil.getBlockCount(source) *
              defaultBlockSize + delta) >=
              plan.getVolumeSetPlans().get(0).getBytesToMove());

    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testBalanceDataBetweenMultiplePairsOfVolumes()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    final int DEFAULT_BLOCK_SIZE = 2048;
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    final int NUM_DATANODES = 1;
    final long CAP = 512 * 1024;
    final Path testFile = new Path("/testfile");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .storageCapacities(new long[]{CAP, CAP, CAP, CAP})
        .storagesPerDatanode(4)
        .build();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      TestBalancer.createFile(cluster, testFile, CAP, (short) 1, 0);

      DFSTestUtil.waitReplication(fs, testFile, (short) 1);
      DataNode dnNode = cluster.getDataNodes().get(0);
      // Move data out of two volumes to make them empty.
      try (FsDatasetSpi.FsVolumeReferences refs =
               dnNode.getFSDataset().getFsVolumeReferences()) {
        assertEquals(4, refs.size());
        for (int i = 0; i < refs.size(); i += 2) {
          FsVolumeImpl source = (FsVolumeImpl) refs.get(i);
          FsVolumeImpl dest = (FsVolumeImpl) refs.get(i + 1);
          assertTrue(DiskBalancerTestUtil.getBlockCount(source) > 0);
          DiskBalancerTestUtil.moveAllDataToDestVolume(dnNode.getFSDataset(),
              source, dest);
          assertTrue(DiskBalancerTestUtil.getBlockCount(source) == 0);
        }
      }

      cluster.restartDataNodes();
      cluster.waitActive();

      // Start up a disk balancer and read the cluster info.
      final DataNode dataNode = cluster.getDataNodes().get(0);
      ClusterConnector nameNodeConnector =
          ConnectorFactory.getCluster(cluster.getFileSystem(0).getUri(), conf);

      DiskBalancerCluster diskBalancerCluster =
          new DiskBalancerCluster(nameNodeConnector);
      diskBalancerCluster.readClusterInfo();
      List<DiskBalancerDataNode> nodesToProcess = new LinkedList<>();
      // Rewrite the capacity in the model to show that disks need
      // re-balancing.
      setVolumeCapacity(diskBalancerCluster, CAP, "DISK");
      nodesToProcess.add(diskBalancerCluster.getNodeByUUID(
          dataNode.getDatanodeUuid()));
      diskBalancerCluster.setNodesToProcess(nodesToProcess);

      // Compute a plan.
      List<NodePlan> clusterPlan = diskBalancerCluster.computePlan(10.0f);

      NodePlan plan = clusterPlan.get(0);
      assertEquals(2, plan.getVolumeSetPlans().size());
      plan.setNodeUUID(dnNode.getDatanodeUuid());
      plan.setTimeStamp(Time.now());
      String planJson = plan.toJson();
      String planID = DigestUtils.shaHex(planJson);

      dataNode.submitDiskBalancerPlan(planID, 1, PLAN_FILE, planJson, false);

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            return dataNode.queryDiskBalancerPlan().getResult() ==
                DiskBalancerWorkStatus.Result.PLAN_DONE;
          } catch (IOException ex) {
            return false;
          }
        }
      }, 1000, 100000);
      assertEquals(dataNode.queryDiskBalancerPlan().getResult(),
          DiskBalancerWorkStatus.Result.PLAN_DONE);

      try (FsDatasetSpi.FsVolumeReferences refs =
               dataNode.getFSDataset().getFsVolumeReferences()) {
        for (FsVolumeSpi vol : refs) {
          assertTrue(DiskBalancerTestUtil.getBlockCount(vol) > 0);
        }
      }
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
}
