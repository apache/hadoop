/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.NullConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.GreedyPlanner;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test Planner.
 */
public class TestPlanner {
  static final Logger LOG =
      LoggerFactory.getLogger(TestPlanner.class);

  @Test
  public void testGreedyPlannerBalanceVolumeSet() throws Exception {
    URI clusterJson = getClass()
        .getResource("/diskBalancer/data-cluster-3node-3disk.json").toURI();
    ClusterConnector jsonConnector = ConnectorFactory.getCluster(clusterJson,
        null);
    DiskBalancerCluster cluster = new DiskBalancerCluster(jsonConnector);
    cluster.readClusterInfo();
    Assert.assertEquals(3, cluster.getNodes().size());
    cluster.setNodesToProcess(cluster.getNodes());
    DiskBalancerDataNode node = cluster.getNodes().get(0);
    GreedyPlanner planner = new GreedyPlanner(10.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);
  }

  @Test
  public void testGreedyPlannerComputePlan() throws Exception {
    URI clusterJson = getClass()
        .getResource("/diskBalancer/data-cluster-3node-3disk.json").toURI();
    ClusterConnector jsonConnector = ConnectorFactory.getCluster(clusterJson,
        null);
    DiskBalancerCluster cluster = new DiskBalancerCluster(jsonConnector);
    cluster.readClusterInfo();
    Assert.assertEquals(3, cluster.getNodes().size());
    cluster.setNodesToProcess(cluster.getNodes());
    List<NodePlan> plan = cluster.computePlan(10.0f);
    Assert.assertNotNull(plan);
  }

  private DiskBalancerVolume createVolume(String path, int capacityInGB,
                                          int usedInGB) {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerVolume volume = util.createRandomVolume(StorageType.SSD);
    volume.setPath(path);
    volume.setCapacity(capacityInGB * DiskBalancerTestUtil.GB);
    volume.setReserved(0);
    volume.setUsed(usedInGB * DiskBalancerTestUtil.GB);
    return volume;
  }

  @Test
  public void testGreedyPlannerNoNodeCluster() throws Exception {
    GreedyPlanner planner = new GreedyPlanner(10.0f, null);
    assertNotNull(planner);
  }

  @Test
  public void testGreedyPlannerNoVolumeTest() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);
    List<NodePlan> planList = cluster.computePlan(10.0f);
    assertNotNull(planList);
  }

  @Test
  public void testGreedyPlannerOneVolumeNoPlanTest() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    DiskBalancerVolume volume30 = createVolume("volume30", 100, 30);
    node.addVolume(volume30);
    nullConnector.addNode(node);
    cluster.readClusterInfo();
    Assert.assertEquals(1, cluster.getNodes().size());

    GreedyPlanner planner = new GreedyPlanner(10.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);

    // With a single volume we should not have any plans for moves.
    assertEquals(0, plan.getVolumeSetPlans().size());
  }

  @Test
  public void testGreedyPlannerTwoVolume() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    DiskBalancerVolume volume30 = createVolume("volume30", 100, 30);
    DiskBalancerVolume volume10 = createVolume("volume10", 100, 10);

    node.addVolume(volume10);
    node.addVolume(volume30);

    nullConnector.addNode(node);
    cluster.readClusterInfo();
    Assert.assertEquals(1, cluster.getNodes().size());

    GreedyPlanner planner = new GreedyPlanner(5.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeUUID(),
        node.getDataNodePort());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);

    // We should have only one planned move from
    // volume30 to volume10 of 10 GB Size.

    assertEquals(1, plan.getVolumeSetPlans().size());
    Step step = plan.getVolumeSetPlans().get(0);
    assertEquals("volume30", step.getSourceVolume().getPath());
    assertEquals("volume10", step.getDestinationVolume().getPath());
    assertEquals("10 G", step.getSizeString(step.getBytesToMove()));
  }

  /**
   * In this test we pass 3 volumes with 30, 20 and 10 GB of data used. We
   * expect the planner to print out 20 GB on each volume.
   * <p/>
   * That is the plan should say move 10 GB from volume30 to volume10.
   */
  @Test
  public void testGreedyPlannerEqualizeData() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    DiskBalancerVolume volume30 = createVolume("volume30", 100, 30);
    DiskBalancerVolume volume20 = createVolume("volume20", 100, 20);
    DiskBalancerVolume volume10 = createVolume("volume10", 100, 10);

    node.addVolume(volume10);
    node.addVolume(volume20);
    node.addVolume(volume30);

    nullConnector.addNode(node);
    cluster.readClusterInfo();
    Assert.assertEquals(1, cluster.getNodes().size());

    GreedyPlanner planner = new GreedyPlanner(5.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeUUID(),
        node.getDataNodePort());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);

    // We should have only one planned move from
    // volume30 to volume10 of 10 GB Size.

    assertEquals(1, plan.getVolumeSetPlans().size());
    Step step = plan.getVolumeSetPlans().get(0);
    assertEquals("volume30", step.getSourceVolume().getPath());
    assertEquals("volume10", step.getDestinationVolume().getPath());
    assertEquals("10 G", step.getSizeString(step.getBytesToMove()));
  }

  @Test
  public void testGreedyPlannerEqualDisksNoMoves() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    // All disks have same capacity of data
    DiskBalancerVolume volume1 = createVolume("volume1", 100, 30);
    DiskBalancerVolume volume2 = createVolume("volume2", 100, 30);
    DiskBalancerVolume volume3 = createVolume("volume3", 100, 30);

    node.addVolume(volume1);
    node.addVolume(volume2);
    node.addVolume(volume3);

    nullConnector.addNode(node);
    cluster.readClusterInfo();
    Assert.assertEquals(1, cluster.getNodes().size());

    GreedyPlanner planner = new GreedyPlanner(10.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);

    // since we have same size of data in all disks , we should have
    // no moves planned.
    assertEquals(0, plan.getVolumeSetPlans().size());
  }

  @Test
  public void testGreedyPlannerMoveFromSingleDisk() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    // All disks have same capacity of data
    DiskBalancerVolume volume1 = createVolume("volume100", 200, 100);
    DiskBalancerVolume volume2 = createVolume("volume0-1", 200, 0);
    DiskBalancerVolume volume3 = createVolume("volume0-2", 200, 0);

    node.addVolume(volume1);
    node.addVolume(volume2);
    node.addVolume(volume3);

    nullConnector.addNode(node);
    cluster.readClusterInfo();
    Assert.assertEquals(1, cluster.getNodes().size());

    GreedyPlanner planner = new GreedyPlanner(10.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);

    // We should see 2 move plans. One from volume100 to volume0-1
    // and another from volume100 to volume0-2

    assertEquals(2, plan.getVolumeSetPlans().size());
    Step step = plan.getVolumeSetPlans().get(0);
    assertEquals("volume100", step.getSourceVolume().getPath());
    assertTrue(step.getSizeString(
        step.getBytesToMove()).matches("33.[2|3|4] G"));
    step = plan.getVolumeSetPlans().get(1);
    assertEquals("volume100", step.getSourceVolume().getPath());
    assertTrue(step.getSizeString(
        step.getBytesToMove()).matches("33.[2|3|4] G"));
  }

  @Test
  public void testGreedyPlannerThresholdTest() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    DiskBalancerVolume volume1 = createVolume("volume100", 1000, 100);
    DiskBalancerVolume volume2 = createVolume("volume0-1", 300, 0);
    DiskBalancerVolume volume3 = createVolume("volume0-2", 300, 0);

    node.addVolume(volume1);
    node.addVolume(volume2);
    node.addVolume(volume3);

    nullConnector.addNode(node);
    cluster.readClusterInfo();
    Assert.assertEquals(1, cluster.getNodes().size());

    GreedyPlanner planner = new GreedyPlanner(10.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);

    //We should see NO moves since the total data on the volume100
    // is less than or equal to threashold value that we pass, which is 10%
    assertEquals(0, plan.getVolumeSetPlans().size());

    // for this new planner we are passing 1% as as threshold value
    // hence planner must move data if possible.
    GreedyPlanner newPlanner = new GreedyPlanner(01.0f, node);
    NodePlan newPlan = new NodePlan(node.getDataNodeName(), node
        .getDataNodePort());
    newPlanner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), newPlan);

    assertEquals(2, newPlan.getVolumeSetPlans().size());

    // Move size should say move 19 GB
    // Here is how the math works out.
    // TotalCapacity = 1000 + 300 + 300 = 1600 GB
    // TotolUsed = 100
    // Expected data% on each disk = 0.0625
    // On Disk (volume0-1) = 300 * 0.0625 - 18.75 -- We round it up
    // in the display string -- hence 18.8 GB, it will be same on volume 2 too.
    // since they are equal sized disks with same used capacity

    Step step = newPlan.getVolumeSetPlans().get(0);
    assertEquals("volume100", step.getSourceVolume().getPath());
    assertTrue(step.getSizeString(
        step.getBytesToMove()).matches("18.[6|7|8] G"));

    step = newPlan.getVolumeSetPlans().get(1);
    assertEquals("volume100", step.getSourceVolume().getPath());
    assertTrue(
        step.getSizeString(step.getBytesToMove()).matches("18.[6|7|8] G"));

  }

  @Test
  public void testGreedyPlannerPlanWithDifferentDiskSizes() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    DiskBalancerVolume volume1 = createVolume("volume100", 1000, 100);
    DiskBalancerVolume volume2 = createVolume("volume0-1", 500, 0);
    DiskBalancerVolume volume3 = createVolume("volume0-2", 250, 0);

    node.addVolume(volume1);
    node.addVolume(volume2);
    node.addVolume(volume3);

    nullConnector.addNode(node);
    cluster.readClusterInfo();
    Assert.assertEquals(1, cluster.getNodes().size());

    GreedyPlanner newPlanner = new GreedyPlanner(01.0f, node);
    NodePlan newPlan = new NodePlan(node.getDataNodeName(), node
        .getDataNodePort());
    newPlanner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), newPlan);

    assertEquals(2, newPlan.getVolumeSetPlans().size());

    // Move size should say move 26.6 GB and 13.3 GB
    // Here is how the math works out.
    // TotalCapacity = 1000 + 500 + 250 = 1750 GB
    // TotolUsed = 100
    // Expected data% on each disk = 0.05714
    // On Disk (volume0-1) = 500 * 0.05714 = 28.57
    // on Voulume0-2 = 300 * 0.05714 = 14.28

    for (Step step : newPlan.getVolumeSetPlans()) {

      if (step.getDestinationVolume().getPath().equals("volume0-1")) {
        assertEquals("volume100", step.getSourceVolume().getPath());
        assertEquals("28.5 G",
            step.getSizeString(step.getBytesToMove()));
      }

      if (step.getDestinationVolume().getPath().equals("volume0-2")) {
        assertEquals("volume100", step.getSourceVolume().getPath());
        assertEquals("14.3 G",
            step.getSizeString(step.getBytesToMove()));
      }
    }

    Step step = newPlan.getVolumeSetPlans().get(0);
    assertEquals(0.05714f, step.getIdealStorage(), 0.001f);
  }

  @Test
  public void testLoadsCorrectClusterConnector() throws Exception {
    ClusterConnector connector = ConnectorFactory.getCluster(getClass()
            .getResource("/diskBalancer/data-cluster-3node-3disk.json").toURI()
        , null);
    assertEquals(connector.getClass().toString(),
        "class org.apache.hadoop.hdfs.server.diskbalancer.connectors." +
            "JsonNodeConnector");

  }

  @Test
  public void testPlannerScale() throws Exception {
    final int diskCount = 256; // it is rare to see more than 48 disks
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerVolumeSet vSet =
        util.createRandomVolumeSet(StorageType.SSD, diskCount);
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());
    int diskNum = 0;
    for (DiskBalancerVolume vol : vSet.getVolumes()) {
      vol.setPath("volume" + diskNum++);
      node.addVolume(vol);
    }

    nullConnector.addNode(node);
    cluster.readClusterInfo();

    GreedyPlanner newPlanner = new GreedyPlanner(01.0f, node);
    NodePlan newPlan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    newPlanner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"),
        newPlan);

    // Assuming that our random disks at least generated one step
    assertTrue("No Steps Generated from random disks, very unlikely",
        newPlan.getVolumeSetPlans().size() > 0);

    assertTrue("Steps Generated less than disk count - false",
        newPlan.getVolumeSetPlans().size() < diskCount);
    LOG.info("Number of steps are : %d%n", newPlan.getVolumeSetPlans().size());

  }

  @Test
  public void testNodePlanSerialize() throws Exception {
    final int diskCount = 12;
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerVolumeSet vSet =
        util.createRandomVolumeSet(StorageType.SSD, diskCount);
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());
    int diskNum = 0;
    for (DiskBalancerVolume vol : vSet.getVolumes()) {
      vol.setPath("volume" + diskNum++);
      node.addVolume(vol);
    }

    nullConnector.addNode(node);
    cluster.readClusterInfo();

    GreedyPlanner newPlanner = new GreedyPlanner(01.0f, node);
    NodePlan newPlan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    newPlanner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"),
        newPlan);
    String planString = newPlan.toJson();
    assertNotNull(planString);
    NodePlan copy = NodePlan.parseJson(planString);
    assertNotNull(copy);
    assertEquals(newPlan.getVolumeSetPlans().size(),
        copy.getVolumeSetPlans().size());
  }

  @Test
  public void testGreedyPlannerLargeDisksWithData() throws Exception {
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    // All disks have same capacity of data
    DiskBalancerVolume volume1 = createVolume("volume1", 1968, 88);
    DiskBalancerVolume volume2 = createVolume("volume2", 1968, 88);
    DiskBalancerVolume volume3 = createVolume("volume3", 1968, 111);
    DiskBalancerVolume volume4 = createVolume("volume4", 1968, 111);
    DiskBalancerVolume volume5 = createVolume("volume5", 1968, 30);
    DiskBalancerVolume volume6 = createVolume("volume6", 1563, 30);
    DiskBalancerVolume volume7 = createVolume("volume7", 1563, 30);
    DiskBalancerVolume volume8 = createVolume("volume8", 1563, 30);
    DiskBalancerVolume volume9 = createVolume("volume9", 1563, 210);




    node.addVolume(volume1);
    node.addVolume(volume2);
    node.addVolume(volume3);

    node.addVolume(volume4);
    node.addVolume(volume5);
    node.addVolume(volume6);

    node.addVolume(volume7);
    node.addVolume(volume8);
    node.addVolume(volume9);


    nullConnector.addNode(node);
    cluster.readClusterInfo();
    Assert.assertEquals(1, cluster.getNodes().size());

    GreedyPlanner planner = new GreedyPlanner(1.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);

    assertTrue(plan.getVolumeSetPlans().size() > 2);
  }
}