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

package org.apache.hadoop.yarn.server.router.webapp;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceRequestInfo;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to validate RouterWebServiceUtil methods.
 */
public class TestRouterWebServiceUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterWebServiceUtil.class);

  private static final ApplicationId APPID1 = ApplicationId.newInstance(1, 1);
  private static final ApplicationId APPID2 = ApplicationId.newInstance(2, 1);
  private static final ApplicationId APPID3 = ApplicationId.newInstance(3, 1);
  private static final ApplicationId APPID4 = ApplicationId.newInstance(4, 1);

  private static final String NODE1 = "Node1";
  private static final String NODE2 = "Node2";
  private static final String NODE3 = "Node3";
  private static final String NODE4 = "Node4";

  /**
   * This test validates the correctness of RouterWebServiceUtil#mergeAppsInfo
   * in case we want to merge 4 AMs. The expected result would be the same 4
   * AMs.
   */
  @Test
  public void testMerge4DifferentApps() {

    AppsInfo apps = new AppsInfo();
    int value = 1000;

    AppInfo app1 = new AppInfo();
    app1.setAppId(APPID1.toString());
    app1.setAMHostHttpAddress("http://i_am_the_AM1:1234");
    app1.setState(YarnApplicationState.FINISHED);
    app1.setNumAMContainerPreempted(value);
    apps.add(app1);

    AppInfo app2 = new AppInfo();
    app2.setAppId(APPID2.toString());
    app2.setAMHostHttpAddress("http://i_am_the_AM2:1234");
    app2.setState(YarnApplicationState.ACCEPTED);
    app2.setAllocatedVCores(2 * value);

    apps.add(app2);

    AppInfo app3 = new AppInfo();
    app3.setAppId(APPID3.toString());
    app3.setAMHostHttpAddress("http://i_am_the_AM3:1234");
    app3.setState(YarnApplicationState.RUNNING);
    app3.setReservedMB(3 * value);
    apps.add(app3);

    AppInfo app4 = new AppInfo();
    app4.setAppId(APPID4.toString());
    app4.setAMHostHttpAddress("http://i_am_the_AM4:1234");
    app4.setState(YarnApplicationState.NEW);
    app4.setAllocatedMB(4 * value);
    apps.add(app4);

    AppsInfo result = RouterWebServiceUtil.mergeAppsInfo(apps.getApps(), false);
    Assert.assertNotNull(result);
    Assert.assertEquals(4, result.getApps().size());

    List<String> appIds = new ArrayList<String>();
    AppInfo appInfo1 = null, appInfo2 = null, appInfo3 = null, appInfo4 = null;
    for (AppInfo app : result.getApps()) {
      appIds.add(app.getAppId());
      if (app.getAppId().equals(APPID1.toString())) {
        appInfo1 = app;
      }
      if (app.getAppId().equals(APPID2.toString())) {
        appInfo2 = app;
      }
      if (app.getAppId().equals(APPID3.toString())) {
        appInfo3 = app;
      }
      if (app.getAppId().equals(APPID4.toString())) {
        appInfo4 = app;
      }
    }

    Assert.assertTrue(appIds.contains(APPID1.toString()));
    Assert.assertTrue(appIds.contains(APPID2.toString()));
    Assert.assertTrue(appIds.contains(APPID3.toString()));
    Assert.assertTrue(appIds.contains(APPID4.toString()));

    // Check preservations APP1
    Assert.assertEquals(app1.getState(), appInfo1.getState());
    Assert.assertEquals(app1.getNumAMContainerPreempted(),
        appInfo1.getNumAMContainerPreempted());

    // Check preservations APP2
    Assert.assertEquals(app2.getState(), appInfo2.getState());
    Assert.assertEquals(app3.getAllocatedVCores(),
        appInfo3.getAllocatedVCores());

    // Check preservations APP3
    Assert.assertEquals(app3.getState(), appInfo3.getState());
    Assert.assertEquals(app3.getReservedMB(), appInfo3.getReservedMB());

    // Check preservations APP3
    Assert.assertEquals(app4.getState(), appInfo4.getState());
    Assert.assertEquals(app3.getAllocatedMB(), appInfo3.getAllocatedMB());
  }

  /**
   * This test validates the correctness of RouterWebServiceUtil#mergeAppsInfo
   * in case we want to merge 2 UAMs and their own AM. The status of the AM is
   * FINISHED, so we check the correctness of the merging of the historical
   * values. The expected result would be 1 report with the merged information.
   */
  @Test
  public void testMergeAppsFinished() {

    AppsInfo apps = new AppsInfo();

    String amHost = "http://i_am_the_AM1:1234";
    AppInfo am = new AppInfo();
    am.setAppId(APPID1.toString());
    am.setAMHostHttpAddress(amHost);
    am.setState(YarnApplicationState.FINISHED);

    int value = 1000;
    setAppInfoFinished(am, value);

    apps.add(am);

    AppInfo uam1 = new AppInfo();
    uam1.setAppId(APPID1.toString());
    apps.add(uam1);

    setAppInfoFinished(uam1, value);

    AppInfo uam2 = new AppInfo();
    uam2.setAppId(APPID1.toString());
    apps.add(uam2);

    setAppInfoFinished(uam2, value);

    // in this case the result does not change if we enable partial result
    AppsInfo result = RouterWebServiceUtil.mergeAppsInfo(apps.getApps(), false);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.getApps().size());

    AppInfo app = result.getApps().get(0);

    Assert.assertEquals(APPID1.toString(), app.getAppId());
    Assert.assertEquals(amHost, app.getAMHostHttpAddress());
    Assert.assertEquals(value * 3, app.getPreemptedResourceMB());
    Assert.assertEquals(value * 3, app.getPreemptedResourceVCores());
    Assert.assertEquals(value * 3, app.getNumNonAMContainerPreempted());
    Assert.assertEquals(value * 3, app.getNumAMContainerPreempted());
    Assert.assertEquals(value * 3, app.getPreemptedMemorySeconds());
    Assert.assertEquals(value * 3, app.getPreemptedVcoreSeconds());
  }

  private void setAppInfoFinished(AppInfo am, int value) {
    am.setPreemptedResourceMB(value);
    am.setPreemptedResourceVCores(value);
    am.setNumNonAMContainerPreempted(value);
    am.setNumAMContainerPreempted(value);
    am.setPreemptedMemorySeconds(value);
    am.setPreemptedVcoreSeconds(value);
  }

  /**
   * This test validates the correctness of RouterWebServiceUtil#mergeAppsInfo
   * in case we want to merge 2 UAMs and their own AM. The status of the AM is
   * RUNNING, so we check the correctness of the merging of the runtime values.
   * The expected result would be 1 report with the merged information.
   */
  @Test
  public void testMergeAppsRunning() {

    AppsInfo apps = new AppsInfo();

    String amHost = "http://i_am_the_AM2:1234";
    AppInfo am = new AppInfo();
    am.setAppId(APPID2.toString());
    am.setAMHostHttpAddress(amHost);
    am.setState(YarnApplicationState.RUNNING);

    int value = 1000;
    setAppInfoRunning(am, value);

    apps.add(am);

    AppInfo uam1 = new AppInfo();
    uam1.setAppId(APPID2.toString());
    uam1.setState(YarnApplicationState.RUNNING);
    apps.add(uam1);

    setAppInfoRunning(uam1, value);

    AppInfo uam2 = new AppInfo();
    uam2.setAppId(APPID2.toString());
    uam2.setState(YarnApplicationState.RUNNING);
    apps.add(uam2);

    setAppInfoRunning(uam2, value);

    // in this case the result does not change if we enable partial result
    AppsInfo result = RouterWebServiceUtil.mergeAppsInfo(apps.getApps(), false);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.getApps().size());

    AppInfo app = result.getApps().get(0);

    Assert.assertEquals(APPID2.toString(), app.getAppId());
    Assert.assertEquals(amHost, app.getAMHostHttpAddress());
    Assert.assertEquals(value * 3, app.getAllocatedMB());
    Assert.assertEquals(value * 3, app.getAllocatedVCores());
    Assert.assertEquals(value * 3, app.getReservedMB());
    Assert.assertEquals(value * 3, app.getReservedVCores());
    Assert.assertEquals(value * 3, app.getRunningContainers());
    Assert.assertEquals(value * 3, app.getMemorySeconds());
    Assert.assertEquals(value * 3, app.getVcoreSeconds());
    Assert.assertEquals(3, app.getResourceRequests().size());
  }

  private void setAppInfoRunning(AppInfo am, int value) {
    am.getResourceRequests().add(new ResourceRequestInfo());
    am.setAllocatedMB(value);
    am.setAllocatedVCores(value);
    am.setReservedMB(value);
    am.setReservedVCores(value);
    am.setRunningContainers(value);
    am.setMemorySeconds(value);
    am.setVcoreSeconds(value);
  }

  /**
   * This test validates the correctness of RouterWebServiceUtil#mergeAppsInfo
   * in case we want to merge 2 UAMs without their own AM. The expected result
   * would be an empty report or a partial report of the 2 UAMs depending on the
   * selected policy.
   */
  @Test
  public void testMerge2UAM() {

    AppsInfo apps = new AppsInfo();

    AppInfo app1 = new AppInfo();
    app1.setAppId(APPID1.toString());
    app1.setName(UnmanagedApplicationManager.APP_NAME);
    app1.setState(YarnApplicationState.RUNNING);
    apps.add(app1);

    AppInfo app2 = new AppInfo();
    app2.setAppId(APPID1.toString());
    app2.setName(UnmanagedApplicationManager.APP_NAME);
    app2.setState(YarnApplicationState.RUNNING);
    apps.add(app2);

    AppsInfo result = RouterWebServiceUtil.mergeAppsInfo(apps.getApps(), false);
    Assert.assertNotNull(result);
    Assert.assertEquals(0, result.getApps().size());

    // By enabling partial result, the expected result would be a partial report
    // of the 2 UAMs
    AppsInfo result2 = RouterWebServiceUtil.mergeAppsInfo(apps.getApps(), true);
    Assert.assertNotNull(result2);
    Assert.assertEquals(1, result2.getApps().size());
    Assert.assertEquals(YarnApplicationState.RUNNING,
        result2.getApps().get(0).getState());
  }

  /**
   * This test validates the correctness of RouterWebServiceUtil#mergeAppsInfo
   * in case we want to merge 1 UAM that does not depend on Federation. The
   * excepted result would be the same app report.
   */
  @Test
  public void testMergeUAM() {

    AppsInfo apps = new AppsInfo();

    AppInfo app1 = new AppInfo();
    app1.setAppId(APPID1.toString());
    app1.setName("Test");
    apps.add(app1);

    // in this case the result does not change if we enable partial result
    AppsInfo result = RouterWebServiceUtil.mergeAppsInfo(apps.getApps(), false);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.getApps().size());
  }

  /**
   * This test validates the correctness of
   * RouterWebServiceUtil#deleteDuplicateNodesInfo in case we want to merge 4
   * Nodes. The expected result would be the same 4 Nodes.
   */
  @Test
  public void testDeleteDuplicate4DifferentNodes() {

    NodesInfo nodes = new NodesInfo();

    NodeInfo nodeInfo1 = new NodeInfo();
    nodeInfo1.setId(NODE1);
    nodes.add(nodeInfo1);

    NodeInfo nodeInfo2 = new NodeInfo();
    nodeInfo2.setId(NODE2);
    nodes.add(nodeInfo2);

    NodeInfo nodeInfo3 = new NodeInfo();
    nodeInfo3.setId(NODE3);
    nodes.add(nodeInfo3);

    NodeInfo nodeInfo4 = new NodeInfo();
    nodeInfo4.setId(NODE4);
    nodes.add(nodeInfo4);

    NodesInfo result =
        RouterWebServiceUtil.deleteDuplicateNodesInfo(nodes.getNodes());
    Assert.assertNotNull(result);
    Assert.assertEquals(4, result.getNodes().size());

    List<String> nodesIds = new ArrayList<String>();

    for (NodeInfo node : result.getNodes()) {
      nodesIds.add(node.getNodeId());
    }

    Assert.assertTrue(nodesIds.contains(NODE1));
    Assert.assertTrue(nodesIds.contains(NODE2));
    Assert.assertTrue(nodesIds.contains(NODE3));
    Assert.assertTrue(nodesIds.contains(NODE4));
  }

  /**
   * This test validates the correctness of
   * {@link RouterWebServiceUtil#deleteDuplicateNodesInfo(ArrayList)} in case we
   * want to merge 3 nodes with the same id. The expected result would be 1 node
   * report with the newest healthy report.
   */
  @Test
  public void testDeleteDuplicateNodes() {

    NodesInfo nodes = new NodesInfo();

    NodeInfo node1 = new NodeInfo();
    node1.setId(NODE1);
    node1.setLastHealthUpdate(0);
    nodes.add(node1);

    NodeInfo node2 = new NodeInfo();
    node2.setId(NODE1);
    node2.setLastHealthUpdate(1);
    nodes.add(node2);

    NodeInfo node3 = new NodeInfo();
    node3.setId(NODE1);
    node3.setLastHealthUpdate(2);
    nodes.add(node3);

    NodesInfo result =
        RouterWebServiceUtil.deleteDuplicateNodesInfo(nodes.getNodes());
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.getNodes().size());

    NodeInfo node = result.getNodes().get(0);

    Assert.assertEquals(NODE1, node.getNodeId());
    Assert.assertEquals(2, node.getLastHealthUpdate());
  }

  /**
   * This test validates the correctness of
   * {@link RouterWebServiceUtil#mergeMetrics}.
   */
  @Test
  public void testMergeMetrics() {
    ClusterMetricsInfo metrics = new ClusterMetricsInfo();
    ClusterMetricsInfo metricsResponse = new ClusterMetricsInfo();

    long seed = System.currentTimeMillis();
    setUpClusterMetrics(metrics, seed);
    // ensure that we don't reuse the same seed when setting up metricsResponse
    // or it might mask bugs
    seed += 1000000000;
    setUpClusterMetrics(metricsResponse, seed);
    ClusterMetricsInfo metricsClone = createClusterMetricsClone(metrics);
    RouterWebServiceUtil.mergeMetrics(metrics, metricsResponse);

    Assert.assertEquals(
        metricsResponse.getAppsSubmitted() + metricsClone.getAppsSubmitted(),
        metrics.getAppsSubmitted());
    Assert.assertEquals(
        metricsResponse.getAppsCompleted() + metricsClone.getAppsCompleted(),
        metrics.getAppsCompleted());
    Assert.assertEquals(
        metricsResponse.getAppsPending() + metricsClone.getAppsPending(),
        metrics.getAppsPending());
    Assert.assertEquals(
        metricsResponse.getAppsRunning() + metricsClone.getAppsRunning(),
        metrics.getAppsRunning());
    Assert.assertEquals(
        metricsResponse.getAppsFailed() + metricsClone.getAppsFailed(),
        metrics.getAppsFailed());
    Assert.assertEquals(
        metricsResponse.getAppsKilled() + metricsClone.getAppsKilled(),
        metrics.getAppsKilled());

    Assert.assertEquals(
        metricsResponse.getReservedMB() + metricsClone.getReservedMB(),
        metrics.getReservedMB());
    Assert.assertEquals(
        metricsResponse.getAvailableMB() + metricsClone.getAvailableMB(),
        metrics.getAvailableMB());
    Assert.assertEquals(
        metricsResponse.getAllocatedMB() + metricsClone.getAllocatedMB(),
        metrics.getAllocatedMB());

    Assert.assertEquals(
        metricsResponse.getReservedVirtualCores()
            + metricsClone.getReservedVirtualCores(),
        metrics.getReservedVirtualCores());
    Assert.assertEquals(
        metricsResponse.getAvailableVirtualCores()
            + metricsClone.getAvailableVirtualCores(),
        metrics.getAvailableVirtualCores());
    Assert.assertEquals(
        metricsResponse.getAllocatedVirtualCores()
            + metricsClone.getAllocatedVirtualCores(),
        metrics.getAllocatedVirtualCores());

    Assert.assertEquals(
        metricsResponse.getContainersAllocated()
            + metricsClone.getContainersAllocated(),
        metrics.getContainersAllocated());
    Assert.assertEquals(
        metricsResponse.getReservedContainers()
            + metricsClone.getReservedContainers(),
        metrics.getReservedContainers());
    Assert.assertEquals(
        metricsResponse.getPendingContainers()
            + metricsClone.getPendingContainers(),
        metrics.getPendingContainers());

    Assert.assertEquals(
        metricsResponse.getTotalMB() + metricsClone.getTotalMB(),
        metrics.getTotalMB());
    Assert.assertEquals(
        metricsResponse.getTotalVirtualCores()
            + metricsClone.getTotalVirtualCores(),
        metrics.getTotalVirtualCores());
    Assert.assertEquals(
        metricsResponse.getTotalNodes() + metricsClone.getTotalNodes(),
        metrics.getTotalNodes());
    Assert.assertEquals(
        metricsResponse.getLostNodes() + metricsClone.getLostNodes(),
        metrics.getLostNodes());
    Assert.assertEquals(
        metricsResponse.getUnhealthyNodes() + metricsClone.getUnhealthyNodes(),
        metrics.getUnhealthyNodes());
    Assert.assertEquals(
        metricsResponse.getDecommissioningNodes()
            + metricsClone.getDecommissioningNodes(),
        metrics.getDecommissioningNodes());
    Assert.assertEquals(
        metricsResponse.getDecommissionedNodes()
            + metricsClone.getDecommissionedNodes(),
        metrics.getDecommissionedNodes());
    Assert.assertEquals(
        metricsResponse.getRebootedNodes() + metricsClone.getRebootedNodes(),
        metrics.getRebootedNodes());
    Assert.assertEquals(
        metricsResponse.getActiveNodes() + metricsClone.getActiveNodes(),
        metrics.getActiveNodes());
    Assert.assertEquals(
        metricsResponse.getShutdownNodes() + metricsClone.getShutdownNodes(),
        metrics.getShutdownNodes());
  }

  private ClusterMetricsInfo createClusterMetricsClone(
      ClusterMetricsInfo metrics) {
    ClusterMetricsInfo metricsClone = new ClusterMetricsInfo();
    metricsClone.setAppsSubmitted(metrics.getAppsSubmitted());
    metricsClone.setAppsCompleted(metrics.getAppsCompleted());
    metricsClone.setAppsPending(metrics.getAppsPending());
    metricsClone.setAppsRunning(metrics.getAppsRunning());
    metricsClone.setAppsFailed(metrics.getAppsFailed());
    metricsClone.setAppsKilled(metrics.getAppsKilled());

    metricsClone.setReservedMB(metrics.getReservedMB());
    metricsClone.setAvailableMB(metrics.getAvailableMB());
    metricsClone.setAllocatedMB(metrics.getAllocatedMB());

    metricsClone.setReservedVirtualCores(metrics.getReservedVirtualCores());
    metricsClone.setAvailableVirtualCores(metrics.getAvailableVirtualCores());
    metricsClone.setAllocatedVirtualCores(metrics.getAllocatedVirtualCores());

    metricsClone.setContainersAllocated(metrics.getContainersAllocated());
    metricsClone.setContainersReserved(metrics.getReservedContainers());
    metricsClone.setContainersPending(metrics.getPendingContainers());

    metricsClone.setTotalMB(metrics.getTotalMB());
    metricsClone.setTotalVirtualCores(metrics.getTotalVirtualCores());
    metricsClone.setTotalNodes(metrics.getTotalNodes());
    metricsClone.setLostNodes(metrics.getLostNodes());
    metricsClone.setUnhealthyNodes(metrics.getUnhealthyNodes());
    metricsClone.setDecommissioningNodes(metrics.getDecommissioningNodes());
    metricsClone.setDecommissionedNodes(metrics.getDecommissionedNodes());
    metricsClone.setRebootedNodes(metrics.getRebootedNodes());
    metricsClone.setActiveNodes(metrics.getActiveNodes());
    metricsClone.setShutdownNodes(metrics.getShutdownNodes());
    return metricsClone;

  }

  private void setUpClusterMetrics(ClusterMetricsInfo metrics, long seed) {
    LOG.info("Using seed: " + seed);
    Random rand = new Random(seed);
    metrics.setAppsSubmitted(rand.nextInt(1000));
    metrics.setAppsCompleted(rand.nextInt(1000));
    metrics.setAppsPending(rand.nextInt(1000));
    metrics.setAppsRunning(rand.nextInt(1000));
    metrics.setAppsFailed(rand.nextInt(1000));
    metrics.setAppsKilled(rand.nextInt(1000));

    metrics.setReservedMB(rand.nextInt(1000));
    metrics.setAvailableMB(rand.nextInt(1000));
    metrics.setAllocatedMB(rand.nextInt(1000));

    metrics.setReservedVirtualCores(rand.nextInt(1000));
    metrics.setAvailableVirtualCores(rand.nextInt(1000));
    metrics.setAllocatedVirtualCores(rand.nextInt(1000));

    metrics.setContainersAllocated(rand.nextInt(1000));
    metrics.setContainersReserved(rand.nextInt(1000));
    metrics.setContainersPending(rand.nextInt(1000));

    metrics.setTotalMB(rand.nextInt(1000));
    metrics.setTotalVirtualCores(rand.nextInt(1000));
    metrics.setTotalNodes(rand.nextInt(1000));
    metrics.setLostNodes(rand.nextInt(1000));
    metrics.setUnhealthyNodes(rand.nextInt(1000));
    metrics.setDecommissioningNodes(rand.nextInt(1000));
    metrics.setDecommissionedNodes(rand.nextInt(1000));
    metrics.setRebootedNodes(rand.nextInt(1000));
    metrics.setActiveNodes(rand.nextInt(1000));
    metrics.setShutdownNodes(rand.nextInt(1000));
  }
}