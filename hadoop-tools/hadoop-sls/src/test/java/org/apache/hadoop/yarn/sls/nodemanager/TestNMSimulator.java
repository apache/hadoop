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
package org.apache.hadoop.yarn.sls.nodemanager;

import java.util.function.Supplier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.scheduler.SLSCapacityScheduler;
import org.apache.hadoop.yarn.sls.scheduler.SLSFairScheduler;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;

public class TestNMSimulator {
  private final int GB = 1024;
  private ResourceManager rm;
  private YarnConfiguration conf;

  private Class slsScheduler;
  private Class scheduler;

  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][] {
        {SLSFairScheduler.class, FairScheduler.class},
        {SLSCapacityScheduler.class, CapacityScheduler.class}
    });
  }

  public void initTestNMSimulator(Class pSlsScheduler, Class pScheduler) {
    this.slsScheduler = pSlsScheduler;
    this.scheduler = pScheduler;
    setup();
  }

  public void setup() {
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_SCHEDULER, slsScheduler.getName());
    conf.set(SLSConfiguration.RM_SCHEDULER, scheduler.getName());
    conf.setBoolean(SLSConfiguration.METRICS_SWITCH, false);
    rm = new ResourceManager();
    rm.init(conf);
    rm.start();
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testNMSimulator(Class<?> pSlsScheduler, Class<?> pScheduler) throws Exception {
    initTestNMSimulator(pSlsScheduler, pScheduler);
    // Register one node
    NMSimulator node1 = new NMSimulator();
    node1.init("/rack1/node1", Resources.createResource(GB * 10, 10), 0, 1000,
        rm, -1f);
    node1.middleStep();

    int numClusterNodes = rm.getResourceScheduler().getNumClusterNodes();
    int cumulativeSleepTime = 0;
    int sleepInterval = 100;

    while(numClusterNodes != 1 && cumulativeSleepTime < 5000) {
      Thread.sleep(sleepInterval);
      cumulativeSleepTime = cumulativeSleepTime + sleepInterval;
      numClusterNodes = rm.getResourceScheduler().getNumClusterNodes();
    }

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override public Boolean get() {
        return rm.getResourceScheduler().getRootQueueMetrics()
            .getAvailableMB() > 0;
      }
    }, 500, 10000);

    Assertions.assertEquals(1, rm.getResourceScheduler().getNumClusterNodes());
    Assertions.assertEquals(GB * 10,
        rm.getResourceScheduler().getRootQueueMetrics().getAvailableMB());
    Assertions.assertEquals(10,
        rm.getResourceScheduler().getRootQueueMetrics()
            .getAvailableVirtualCores());

    // Allocate one container on node1
    ContainerId cId1 = newContainerId(1, 1, 1);
    Container container1 = Container.newInstance(cId1, null, null,
        Resources.createResource(GB, 1), null, null);
    node1.addNewContainer(container1, 100000l, null);
    Assertions.assertTrue(
       node1.getRunningContainers().containsKey(cId1), "Node1 should have one running container.");

    // Allocate one AM container on node1
    ContainerId cId2 = newContainerId(2, 1, 1);
    Container container2 = Container.newInstance(cId2, null, null,
        Resources.createResource(GB, 1), null, null);
    node1.addNewContainer(container2, -1l, null);
    Assertions.assertTrue(
       node1.getAMContainers().contains(cId2), "Node1 should have one running AM container");

    // Remove containers
    node1.cleanupContainer(cId1);
    Assertions.assertTrue(
       node1.getCompletedContainers().contains(cId1), "Container1 should be removed from Node1.");
    node1.cleanupContainer(cId2);
    Assertions.assertFalse(
       node1.getAMContainers().contains(cId2), "Container2 should be removed from Node1.");
  }

  private ContainerId newContainerId(int appId, int appAttemptId, int cId) {
    return BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(System.currentTimeMillis(), appId),
            appAttemptId), cId);
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testNMSimAppAddedAndRemoved(Class<?> pSlsScheduler, Class<?> pScheduler)
    throws Exception {
    initTestNMSimulator(pSlsScheduler, pScheduler);
    // Register one node
    NMSimulator node = new NMSimulator();
    node.init("/rack1/node1", Resources.createResource(GB * 10, 10), 0, 1000,
        rm, -1f);
    node.middleStep();

    int numClusterNodes = rm.getResourceScheduler().getNumClusterNodes();
    int cumulativeSleepTime = 0;
    int sleepInterval = 100;

    while (numClusterNodes != 1 && cumulativeSleepTime < 5000) {
      Thread.sleep(sleepInterval);
      cumulativeSleepTime = cumulativeSleepTime + sleepInterval;
      numClusterNodes = rm.getResourceScheduler().getNumClusterNodes();
    }

    GenericTestUtils.waitFor(() ->
            rm.getResourceScheduler().getRootQueueMetrics()
                .getAvailableMB() > 0,
        500, 10000);

    Assertions.assertEquals(
       node.getNode().getRunningApps().size(), 0, "Node should have no runningApps.");

    // Allocate one app container on node
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId cId = BuilderUtils.newContainerId(appAttemptId, 1);
    Container container = Container.newInstance(cId, null, null,
        Resources.createResource(GB, 1), null, null);
    node.addNewContainer(container, 100000l, appId);
    Assertions.assertTrue(
       node.getNode().getRunningApps().contains(appId), "Node should have app: "
            + appId + " in runningApps list.");

    // Finish the app on the node.
    node.finishApplication(appId);
    Assertions.assertFalse(
       node.getNode().getRunningApps().contains(appId), "Node should not have app: "
            + appId + " in runningApps list.");
    Assertions.assertEquals(
       node.getNode().getRunningApps().size(), 0, "Node should have no runningApps.");
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testNMSimNullAppAddedAndRemoved(Class<?> pSlsScheduler, Class<?> pScheduler)
    throws Exception {
    initTestNMSimulator(pSlsScheduler, pScheduler);
    // Register one node
    NMSimulator node = new NMSimulator();
    node.init("/rack1/node1", Resources.createResource(GB * 10, 10), 0, 1000,
        rm, -1f);
    node.middleStep();

    int numClusterNodes = rm.getResourceScheduler().getNumClusterNodes();
    int cumulativeSleepTime = 0;
    int sleepInterval = 100;

    while (numClusterNodes != 1 && cumulativeSleepTime < 5000) {
      Thread.sleep(sleepInterval);
      cumulativeSleepTime = cumulativeSleepTime + sleepInterval;
      numClusterNodes = rm.getResourceScheduler().getNumClusterNodes();
    }

    GenericTestUtils.waitFor(() ->
            rm.getResourceScheduler().getRootQueueMetrics()
                .getAvailableMB() > 0,
        500, 10000);

    Assertions.assertEquals(
       node.getNode().getRunningApps().size(), 0, "Node should have no runningApps.");

    // Allocate null app container on node
    ContainerId cId = newContainerId(1, 1, 1);
    Container container = Container.newInstance(cId, null, null,
        Resources.createResource(GB, 1), null, null);
    node.addNewContainer(container, 100000l, null);
    Assertions.assertEquals(
       node.getNode().getRunningApps().size(), 0, "Node should have no runningApps if appId is null.");

    // Finish non-existent app on the node.
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    node.finishApplication(appId);
    Assertions.assertEquals(
       node.getNode().getRunningApps().size(), 0, "Node should have no runningApps.");
  }

  @AfterEach
  public void tearDown() throws Exception {
    rm.stop();
  }
}
