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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException.Result;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.GreedyPlanner;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.Result.NO_PLAN;
import static org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.Result.PLAN_DONE;
import static org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.Result.PLAN_UNDER_PROGRESS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test DiskBalancer RPC.
 */
public class TestDiskBalancerRPC {

  private static final String PLAN_FILE = "/system/current.plan.json";
  private MiniDFSCluster cluster;
  private Configuration conf;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSubmitPlan() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHash = rpcTestHelper.getPlanHash();
    int planVersion = rpcTestHelper.getPlanVersion();
    NodePlan plan = rpcTestHelper.getPlan();
    dataNode.submitDiskBalancerPlan(planHash, planVersion, PLAN_FILE,
        plan.toJson(), false);
  }

  @Test
  public void testSubmitPlanWithInvalidHash() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHashValid = rpcTestHelper.getPlanHash();
    char[] hashArray = planHashValid.toCharArray();
    hashArray[0]++;
    final String planHash = String.valueOf(hashArray);
    int planVersion = rpcTestHelper.getPlanVersion();
    NodePlan plan = rpcTestHelper.getPlan();
    final DiskBalancerException thrown =
        Assertions.assertThrows(DiskBalancerException.class, () -> {
          dataNode.submitDiskBalancerPlan(planHash, planVersion, PLAN_FILE,
              plan.toJson(), false);
        });
    Assertions.assertEquals(thrown.getResult(), Result.INVALID_PLAN_HASH);
  }

  @Test
  public void testSubmitPlanWithInvalidVersion() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHash = rpcTestHelper.getPlanHash();
    final int planVersion = rpcTestHelper.getPlanVersion() + 1;
    NodePlan plan = rpcTestHelper.getPlan();
    final DiskBalancerException thrown =
        Assertions.assertThrows(DiskBalancerException.class, () -> {
          dataNode.submitDiskBalancerPlan(planHash, planVersion, PLAN_FILE,
              plan.toJson(), false);
        });
    Assertions.assertEquals(thrown.getResult(), Result.INVALID_PLAN_VERSION);
  }

  @Test
  public void testSubmitPlanWithInvalidPlan() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHash = rpcTestHelper.getPlanHash();
    int planVersion = rpcTestHelper.getPlanVersion();
    NodePlan plan = rpcTestHelper.getPlan();
    final DiskBalancerException thrown =
        Assertions.assertThrows(DiskBalancerException.class, () -> {
          dataNode.submitDiskBalancerPlan(planHash, planVersion, "", "",
              false);
        });
    Assertions.assertEquals(thrown.getResult(), Result.INVALID_PLAN);
  }

  @Test
  public void testCancelPlan() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHash = rpcTestHelper.getPlanHash();
    int planVersion = rpcTestHelper.getPlanVersion();
    NodePlan plan = rpcTestHelper.getPlan();
    dataNode.submitDiskBalancerPlan(planHash, planVersion, PLAN_FILE,
        plan.toJson(), false);
    dataNode.cancelDiskBalancePlan(planHash);
  }

  @Test
  public void testCancelNonExistentPlan() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHashValid = rpcTestHelper.getPlanHash();
    char[] hashArray= planHashValid.toCharArray();
    hashArray[0]++;
    final String planHash = String.valueOf(hashArray);
    NodePlan plan = rpcTestHelper.getPlan();
    final DiskBalancerException thrown =
        Assertions.assertThrows(DiskBalancerException.class, () -> {
          dataNode.cancelDiskBalancePlan(planHash);
        });
    Assertions.assertEquals(thrown.getResult(), Result.NO_SUCH_PLAN);
  }

  @Test
  public void testCancelEmptyPlan() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHash = "";
    NodePlan plan = rpcTestHelper.getPlan();
    final DiskBalancerException thrown =
        Assertions.assertThrows(DiskBalancerException.class, () -> {
          dataNode.cancelDiskBalancePlan(planHash);
        });
    Assertions.assertEquals(thrown.getResult(), Result.NO_SUCH_PLAN);
  }

  @Test
  public void testGetDiskBalancerVolumeMapping() throws Exception {
    final int dnIndex = 0;
    DataNode dataNode = cluster.getDataNodes().get(dnIndex);
    String volumeNameJson = dataNode.getDiskBalancerSetting(
        DiskBalancerConstants.DISKBALANCER_VOLUME_NAME);
    Assertions.assertNotNull(volumeNameJson);
    ObjectMapper mapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    Map<String, String> volumemap =
        mapper.readValue(volumeNameJson, HashMap.class);

    Assertions.assertEquals(2, volumemap.size());
  }

  @Test
  public void testGetDiskBalancerInvalidSetting() throws Exception {
    final int dnIndex = 0;
    final String invalidSetting = "invalidSetting";
    DataNode dataNode = cluster.getDataNodes().get(dnIndex);
    final DiskBalancerException thrown =
        Assertions.assertThrows(DiskBalancerException.class, () -> {
          dataNode.getDiskBalancerSetting(invalidSetting);
        });
    Assertions.assertEquals(thrown.getResult(), Result.UNKNOWN_KEY);
  }

  @Test
  public void testgetDiskBalancerBandwidth() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHash = rpcTestHelper.getPlanHash();
    int planVersion = rpcTestHelper.getPlanVersion();
    NodePlan plan = rpcTestHelper.getPlan();

    dataNode.submitDiskBalancerPlan(planHash, planVersion, PLAN_FILE,
        plan.toJson(), false);
    String bandwidthString = dataNode.getDiskBalancerSetting(
        DiskBalancerConstants.DISKBALANCER_BANDWIDTH);
    long value = Long.decode(bandwidthString);
    Assertions.assertEquals(10L, value);
  }

  @Test
  public void testQueryPlan() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();
    String planHash = rpcTestHelper.getPlanHash();
    int planVersion = rpcTestHelper.getPlanVersion();
    NodePlan plan = rpcTestHelper.getPlan();

    dataNode.submitDiskBalancerPlan(planHash, planVersion, PLAN_FILE,
        plan.toJson(), false);
    DiskBalancerWorkStatus status = dataNode.queryDiskBalancerPlan();
    Assertions.assertTrue(status.getResult() == PLAN_UNDER_PROGRESS ||
        status.getResult() == PLAN_DONE);
  }

  @Test
  public void testQueryPlanWithoutSubmit() throws Exception {
    RpcTestHelper rpcTestHelper = new RpcTestHelper().invoke();
    DataNode dataNode = rpcTestHelper.getDataNode();

    DiskBalancerWorkStatus status = dataNode.queryDiskBalancerPlan();
    Assertions.assertTrue(status.getResult() == NO_PLAN);
  }

  @Test
  public void testMoveBlockAcrossVolume() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final int defaultBlockSize = 100;
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, defaultBlockSize);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, defaultBlockSize);
    String fileName = "/tmp.txt";
    Path filePath = new Path(fileName);
    final int numDatanodes = 1;
    final int dnIndex = 0;
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes).build();
    FsVolumeImpl source = null;
    FsVolumeImpl dest = null;
    try {
      cluster.waitActive();
      Random r = new Random();
      FileSystem fs = cluster.getFileSystem(dnIndex);
      DFSTestUtil.createFile(fs, filePath, 10 * 1024,
          (short) 1, r.nextLong());
      DataNode dnNode = cluster.getDataNodes().get(dnIndex);
      FsDatasetSpi.FsVolumeReferences refs =
          dnNode.getFSDataset().getFsVolumeReferences();
      try {
        source = (FsVolumeImpl) refs.get(0);
        dest = (FsVolumeImpl) refs.get(1);
        DiskBalancerTestUtil.moveAllDataToDestVolume(dnNode.getFSDataset(),
            source, dest);
        assertEquals(0, DiskBalancerTestUtil.getBlockCount(source, false));
      } finally {
        refs.close();
      }
    } finally {
      cluster.shutdown();
    }
  }


  private class RpcTestHelper {
    private NodePlan plan;
    private int planVersion;
    private DataNode dataNode;
    private String planHash;

    public NodePlan getPlan() {
      return plan;
    }

    public int getPlanVersion() {
      return planVersion;
    }

    public DataNode getDataNode() {
      return dataNode;
    }

    public String getPlanHash() {
      return planHash;
    }

    public RpcTestHelper invoke() throws Exception {
      final int dnIndex = 0;
      cluster.restartDataNode(dnIndex);
      cluster.waitActive();
      ClusterConnector nameNodeConnector =
          ConnectorFactory.getCluster(cluster.getFileSystem(0).getUri(), conf);

      DiskBalancerCluster diskBalancerCluster =
          new DiskBalancerCluster(nameNodeConnector);
      diskBalancerCluster.readClusterInfo();
      Assertions.assertEquals(cluster.getDataNodes().size(),
          diskBalancerCluster.getNodes().size());
      diskBalancerCluster.setNodesToProcess(diskBalancerCluster.getNodes());
      dataNode = cluster.getDataNodes().get(dnIndex);
      DiskBalancerDataNode node = diskBalancerCluster.getNodeByUUID(
          dataNode.getDatanodeUuid());
      GreedyPlanner planner = new GreedyPlanner(10.0f, node);
      plan = new NodePlan(node.getDataNodeName(), node.getDataNodePort());
      planner.balanceVolumeSet(node, node.getVolumeSets().get("DISK"), plan);
      planVersion = 1;
      planHash = DigestUtils.sha1Hex(plan.toJson());
      return this;
    }
  }
}
