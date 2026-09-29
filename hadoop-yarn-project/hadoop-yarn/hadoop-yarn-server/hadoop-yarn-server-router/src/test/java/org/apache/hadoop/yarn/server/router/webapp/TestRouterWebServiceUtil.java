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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import org.glassfish.jersey.client.ClientProperties;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    assertNotNull(result);
    assertEquals(4, result.getApps().size());

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

    assertTrue(appIds.contains(APPID1.toString()));
    assertTrue(appIds.contains(APPID2.toString()));
    assertTrue(appIds.contains(APPID3.toString()));
    assertTrue(appIds.contains(APPID4.toString()));

    // Check preservations APP1
    assertEquals(app1.getState(), appInfo1.getState());
    assertEquals(app1.getNumAMContainerPreempted(),
        appInfo1.getNumAMContainerPreempted());

    // Check preservations APP2
    assertEquals(app2.getState(), appInfo2.getState());
    assertEquals(app3.getAllocatedVCores(),
        appInfo3.getAllocatedVCores());

    // Check preservations APP3
    assertEquals(app3.getState(), appInfo3.getState());
    assertEquals(app3.getReservedMB(), appInfo3.getReservedMB());

    // Check preservations APP3
    assertEquals(app4.getState(), appInfo4.getState());
    assertEquals(app3.getAllocatedMB(), appInfo3.getAllocatedMB());
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
    assertNotNull(result);
    assertEquals(1, result.getApps().size());

    AppInfo app = result.getApps().get(0);

    assertEquals(APPID1.toString(), app.getAppId());
    assertEquals(amHost, app.getAMHostHttpAddress());
    assertEquals(value * 3, app.getPreemptedResourceMB());
    assertEquals(value * 3, app.getPreemptedResourceVCores());
    assertEquals(value * 3, app.getNumNonAMContainerPreempted());
    assertEquals(value * 3, app.getNumAMContainerPreempted());
    assertEquals(value * 3, app.getPreemptedMemorySeconds());
    assertEquals(value * 3, app.getPreemptedVcoreSeconds());
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
    assertNotNull(result);
    assertEquals(1, result.getApps().size());

    AppInfo app = result.getApps().get(0);

    assertEquals(APPID2.toString(), app.getAppId());
    assertEquals(amHost, app.getAMHostHttpAddress());
    assertEquals(value * 3, app.getAllocatedMB());
    assertEquals(value * 3, app.getAllocatedVCores());
    assertEquals(value * 3, app.getReservedMB());
    assertEquals(value * 3, app.getReservedVCores());
    assertEquals(value * 3, app.getRunningContainers());
    assertEquals(value * 3, app.getMemorySeconds());
    assertEquals(value * 3, app.getVcoreSeconds());
    assertEquals(3, app.getResourceRequests().size());
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
    assertNotNull(result);
    assertEquals(0, result.getApps().size());

    // By enabling partial result, the expected result would be a partial report
    // of the 2 UAMs
    AppsInfo result2 = RouterWebServiceUtil.mergeAppsInfo(apps.getApps(), true);
    assertNotNull(result2);
    assertEquals(1, result2.getApps().size());
    assertEquals(YarnApplicationState.RUNNING,
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
    assertNotNull(result);
    assertEquals(1, result.getApps().size());
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
    assertNotNull(result);
    assertEquals(4, result.getNodes().size());

    List<String> nodesIds = new ArrayList<String>();

    for (NodeInfo node : result.getNodes()) {
      nodesIds.add(node.getNodeId());
    }

    assertTrue(nodesIds.contains(NODE1));
    assertTrue(nodesIds.contains(NODE2));
    assertTrue(nodesIds.contains(NODE3));
    assertTrue(nodesIds.contains(NODE4));
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
    assertNotNull(result);
    assertEquals(1, result.getNodes().size());

    NodeInfo node = result.getNodes().get(0);

    assertEquals(NODE1, node.getNodeId());
    assertEquals(2, node.getLastHealthUpdate());
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

    assertEquals(
        metricsResponse.getAppsSubmitted() + metricsClone.getAppsSubmitted(),
        metrics.getAppsSubmitted());
    assertEquals(
        metricsResponse.getAppsCompleted() + metricsClone.getAppsCompleted(),
        metrics.getAppsCompleted());
    assertEquals(
        metricsResponse.getAppsPending() + metricsClone.getAppsPending(),
        metrics.getAppsPending());
    assertEquals(
        metricsResponse.getAppsRunning() + metricsClone.getAppsRunning(),
        metrics.getAppsRunning());
    assertEquals(
        metricsResponse.getAppsFailed() + metricsClone.getAppsFailed(),
        metrics.getAppsFailed());
    assertEquals(
        metricsResponse.getAppsKilled() + metricsClone.getAppsKilled(),
        metrics.getAppsKilled());

    assertEquals(
        metricsResponse.getReservedMB() + metricsClone.getReservedMB(),
        metrics.getReservedMB());
    assertEquals(
        metricsResponse.getAvailableMB() + metricsClone.getAvailableMB(),
        metrics.getAvailableMB());
    assertEquals(
        metricsResponse.getAllocatedMB() + metricsClone.getAllocatedMB(),
        metrics.getAllocatedMB());

    assertEquals(
        metricsResponse.getReservedVirtualCores()
            + metricsClone.getReservedVirtualCores(),
        metrics.getReservedVirtualCores());
    assertEquals(
        metricsResponse.getAvailableVirtualCores()
            + metricsClone.getAvailableVirtualCores(),
        metrics.getAvailableVirtualCores());
    assertEquals(
        metricsResponse.getAllocatedVirtualCores()
            + metricsClone.getAllocatedVirtualCores(),
        metrics.getAllocatedVirtualCores());

    assertEquals(
        metricsResponse.getContainersAllocated()
            + metricsClone.getContainersAllocated(),
        metrics.getContainersAllocated());
    assertEquals(
        metricsResponse.getReservedContainers()
            + metricsClone.getReservedContainers(),
        metrics.getReservedContainers());
    assertEquals(
        metricsResponse.getPendingContainers()
            + metricsClone.getPendingContainers(),
        metrics.getPendingContainers());

    assertEquals(
        metricsResponse.getTotalMB() + metricsClone.getTotalMB(),
        metrics.getTotalMB());
    assertEquals(
        metricsResponse.getUtilizedMB() + metricsClone.getUtilizedMB(),
        metrics.getUtilizedMB());
    assertEquals(
        metricsResponse.getTotalVirtualCores()
            + metricsClone.getTotalVirtualCores(),
        metrics.getTotalVirtualCores());
    assertEquals(
        metricsResponse.getUtilizedVirtualCores() + metricsClone.getUtilizedVirtualCores(),
        metrics.getUtilizedVirtualCores());
    assertEquals(
        metricsResponse.getTotalNodes() + metricsClone.getTotalNodes(),
        metrics.getTotalNodes());
    assertEquals(
        metricsResponse.getLostNodes() + metricsClone.getLostNodes(),
        metrics.getLostNodes());
    assertEquals(
        metricsResponse.getUnhealthyNodes() + metricsClone.getUnhealthyNodes(),
        metrics.getUnhealthyNodes());
    assertEquals(
        metricsResponse.getDecommissioningNodes()
            + metricsClone.getDecommissioningNodes(),
        metrics.getDecommissioningNodes());
    assertEquals(
        metricsResponse.getDecommissionedNodes()
            + metricsClone.getDecommissionedNodes(),
        metrics.getDecommissionedNodes());
    assertEquals(
        metricsResponse.getRebootedNodes() + metricsClone.getRebootedNodes(),
        metrics.getRebootedNodes());
    assertEquals(
        metricsResponse.getActiveNodes() + metricsClone.getActiveNodes(),
        metrics.getActiveNodes());
    assertEquals(
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
    metricsClone.setUtilizedMB(metrics.getUtilizedMB());
    metricsClone.setTotalVirtualCores(metrics.getTotalVirtualCores());
    metricsClone.setUtilizedVirtualCores(metrics.getUtilizedVirtualCores());
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
    metrics.setUtilizedMB(metrics.getTotalMB() - rand.nextInt(100));
    metrics.setTotalVirtualCores(rand.nextInt(1000));
    metrics.setUtilizedVirtualCores(metrics.getUtilizedVirtualCores() - rand.nextInt(100));
    metrics.setTotalNodes(rand.nextInt(1000));
    metrics.setLostNodes(rand.nextInt(1000));
    metrics.setUnhealthyNodes(rand.nextInt(1000));
    metrics.setDecommissioningNodes(rand.nextInt(1000));
    metrics.setDecommissionedNodes(rand.nextInt(1000));
    metrics.setRebootedNodes(rand.nextInt(1000));
    metrics.setActiveNodes(rand.nextInt(1000));
    metrics.setShutdownNodes(rand.nextInt(1000));
  }

  public static AppAttemptInfo generateAppAttemptInfo(int attemptId) {
    AppAttemptInfo appAttemptInfo = mock(AppAttemptInfo.class);
    when(appAttemptInfo.getAppAttemptId()).thenReturn("AppAttemptId_" + attemptId);
    when(appAttemptInfo.getAttemptId()).thenReturn(0);
    when(appAttemptInfo.getFinishedTime()).thenReturn(1659621705L);
    when(appAttemptInfo.getLogsLink()).thenReturn("LogLink_" + attemptId);
    return appAttemptInfo;
  }

  @Test
  public void testMergeApplicationStatisticsInfo() {
    ApplicationStatisticsInfo infoA = new ApplicationStatisticsInfo();
    ApplicationStatisticsInfo infoB = new ApplicationStatisticsInfo();

    StatisticsItemInfo item1 = new StatisticsItemInfo(YarnApplicationState.ACCEPTED, "*", 10);
    StatisticsItemInfo item2 = new StatisticsItemInfo(YarnApplicationState.ACCEPTED, "*", 20);

    infoA.add(item1);
    infoB.add(item2);

    List<ApplicationStatisticsInfo> lists = new ArrayList<>();
    lists.add(infoA);
    lists.add(infoB);

    ApplicationStatisticsInfo mergeInfo =
        RouterWebServiceUtil.mergeApplicationStatisticsInfo(lists);
    ArrayList<StatisticsItemInfo> statItem = mergeInfo.getStatItems();

    assertNotNull(statItem);
    assertEquals(1, statItem.size());

    StatisticsItemInfo first = statItem.get(0);

    assertEquals(item1.getCount() + item2.getCount(), first.getCount());
    assertEquals(item1.getType(), first.getType());
    assertEquals(item1.getState(), first.getState());
  }

  @Test
  public void testMergeDiffApplicationStatisticsInfo() {
    ApplicationStatisticsInfo infoA = new ApplicationStatisticsInfo();
    StatisticsItemInfo item1 = new StatisticsItemInfo(YarnApplicationState.ACCEPTED, "*", 10);
    StatisticsItemInfo item2 =
        new StatisticsItemInfo(YarnApplicationState.NEW_SAVING, "test1", 20);
    infoA.add(item1);
    infoA.add(item2);

    ApplicationStatisticsInfo infoB = new ApplicationStatisticsInfo();
    StatisticsItemInfo item3 =
        new StatisticsItemInfo(YarnApplicationState.NEW_SAVING, "test1", 30);
    StatisticsItemInfo item4 = new StatisticsItemInfo(YarnApplicationState.FINISHED, "test3", 40);
    infoB.add(item3);
    infoB.add(item4);

    List<ApplicationStatisticsInfo> lists = new ArrayList<>();
    lists.add(infoA);
    lists.add(infoB);

    ApplicationStatisticsInfo mergeInfo =
        RouterWebServiceUtil.mergeApplicationStatisticsInfo(lists);

    assertEquals(3, mergeInfo.getStatItems().size());
    List<StatisticsItemInfo> mergeInfoStatItems = mergeInfo.getStatItems();

    StatisticsItemInfo item1Result = null;
    StatisticsItemInfo item2Result = null;
    StatisticsItemInfo item3Result = null;

    for (StatisticsItemInfo item : mergeInfoStatItems) {
      // ACCEPTED
      if (item.getState() == YarnApplicationState.ACCEPTED) {
        item1Result = item;
      }

      // NEW_SAVING
      if (item.getState() == YarnApplicationState.NEW_SAVING) {
        item2Result = item;
      }

      // FINISHED
      if (item.getState() == YarnApplicationState.FINISHED) {
        item3Result = item;
      }
    }

    assertEquals(YarnApplicationState.ACCEPTED, item1Result.getState());
    assertEquals(item1.getCount(), item1Result.getCount());
    assertEquals(YarnApplicationState.NEW_SAVING, item2Result.getState());
    assertEquals((item2.getCount() + item3.getCount()), item2Result.getCount());
    assertEquals(YarnApplicationState.FINISHED, item3Result.getState());
    assertEquals(item4.getCount(), item3Result.getCount());
  }

  @Test
  public void testCreateJerseyClient() {
    // Case1,  default timeout, The default timeout is 30s.
    YarnConfiguration configuration = new YarnConfiguration();
    Client client01 = RouterWebServiceUtil.createJerseyClient(configuration);
    Map<String, Object> properties = client01.getConfiguration().getProperties();
    int readTimeOut = (int) properties.get(ClientProperties.READ_TIMEOUT);
    int connectTimeOut = (int) properties.get(ClientProperties.CONNECT_TIMEOUT);
    assertEquals(30000, readTimeOut);
    assertEquals(30000, connectTimeOut);
    client01.close();

    // Case2, set a negative timeout, We'll get the default timeout(30s)
    YarnConfiguration configuration2 = new YarnConfiguration();
    configuration2.setLong(YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, -1L);
    configuration2.setLong(YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT, -1L);
    Client client02 = RouterWebServiceUtil.createJerseyClient(configuration2);
    Map<String, Object> properties02 = client02.getConfiguration().getProperties();
    int readTimeOut02 = (int) properties02.get(ClientProperties.READ_TIMEOUT);
    int connectTimeOut02 =  (int) properties02.get(ClientProperties.CONNECT_TIMEOUT);
    assertEquals(30000, readTimeOut02);
    assertEquals(30000, connectTimeOut02);
    client02.close();

    // Case3, Set the maximum value that exceeds the integer
    // We'll get the default timeout(30s)
    YarnConfiguration configuration3 = new YarnConfiguration();
    long connectTimeOutLong = (long) Integer.MAX_VALUE + 1;
    long readTimeOutLong = (long) Integer.MAX_VALUE + 1;

    configuration3.setLong(YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, connectTimeOutLong);
    configuration3.setLong(YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT, readTimeOutLong);
    Client client03 = RouterWebServiceUtil.createJerseyClient(configuration3);
    Map<String, Object> properties03 = client03.getConfiguration().getProperties();
    int readTimeOut03 = (int) properties03.get(ClientProperties.READ_TIMEOUT);
    int connectTimeOut03 = (int) properties03.get(ClientProperties.CONNECT_TIMEOUT);
    assertEquals(30000, readTimeOut03);
    assertEquals(30000, connectTimeOut03);
    client03.close();
  }

  @Test
  public void testJerseyClient() {
    // Case1, Set to negative 1.
    YarnConfiguration conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, -1L);
    conf.setLong(YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT, -1L);

    int connectTimeOut = (int) getTimeDuration(conf,
        YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT);
    int readTimeout = (int) getTimeDuration(conf,
        YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_READ_TIMEOUT);
    assertEquals(-1, connectTimeOut);
    assertEquals(-1, readTimeout);

    // Case2, Set the maximum value that exceeds the integer.
    // Converted to int, there will be a value out of bounds.
    YarnConfiguration conf1 = new YarnConfiguration();
    long connectTimeOutLong = (long) Integer.MAX_VALUE + 1;
    long readTimeOutLong = (long) Integer.MAX_VALUE + 1;
    conf1.setLong(YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, connectTimeOutLong);
    conf1.setLong(YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT, readTimeOutLong);

    int connectTimeOut1 = (int) getTimeDuration(conf1,
        YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT);
    int readTimeout1 = (int) getTimeDuration(conf1,
        YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_READ_TIMEOUT);
    assertEquals(-2147483648, connectTimeOut1);
    assertEquals(-2147483648, readTimeout1);
  }

  private long getTimeDuration(YarnConfiguration conf, String varName, long defaultValue) {
    return conf.getTimeDuration(varName, defaultValue, TimeUnit.MILLISECONDS);
  }
}