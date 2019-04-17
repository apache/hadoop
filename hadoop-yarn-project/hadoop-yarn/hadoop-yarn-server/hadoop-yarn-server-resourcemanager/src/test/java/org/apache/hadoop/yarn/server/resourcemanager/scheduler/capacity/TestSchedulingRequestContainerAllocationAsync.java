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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test SchedulingRequest With Asynchronous Scheduling.
 */
@RunWith(Parameterized.class)
public class TestSchedulingRequestContainerAllocationAsync {
  private final int GB = 1024;

  private YarnConfiguration conf;
  private String placementConstraintHandler;

  RMNodeLabelsManager mgr;

  @Parameters
  public static Collection<Object[]> placementConstarintHandlers() {
    Object[][] params = new Object[][] {
        {YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER},
        {YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER} };
    return Arrays.asList(params);
  }

  public TestSchedulingRequestContainerAllocationAsync(
      String placementConstraintHandler) {
    this.placementConstraintHandler = placementConstraintHandler;
  }

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        this.placementConstraintHandler);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }


  private void testIntraAppAntiAffinityAsync(int numThreads) throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        conf);
    csConf.setInt(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD,
        numThreads);
    csConf.setInt(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
        + ".scheduling-interval-ms", 0);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 200 NMs.
    int nNMs = 200;
    MockNM[] nms = new MockNM[nNMs];
    RMNode[] rmNodes = new RMNode[nNMs];
    for (int i = 0; i < nNMs; i++) {
      nms[i] = rm1.registerNode("127.0.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 1000 anti-affinity containers for the same app. It should
    // only get 200 containers allocated because we only have 200 nodes.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(1000, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("mapper"), "mapper");

    List<Container> allocated = TestSchedulingRequestContainerAllocation.
            waitForAllocation(nNMs, 6000, am1, nms);
    Assert.assertEquals(nNMs, allocated.size());
    Assert.assertEquals(nNMs, TestSchedulingRequestContainerAllocation.
            getContainerNodesNum(allocated));

    rm1.close();
  }

  @Test(timeout = 300000)
  public void testSingleThreadAsyncContainerAllocation() throws Exception {
    testIntraAppAntiAffinityAsync(1);
  }

  @Test(timeout = 300000)
  public void testTwoThreadsAsyncContainerAllocation() throws Exception {
    testIntraAppAntiAffinityAsync(2);
  }

  @Test(timeout = 300000)
  public void testThreeThreadsAsyncContainerAllocation() throws Exception {
    testIntraAppAntiAffinityAsync(3);
  }

  @Test(timeout = 300000)
  public void testFourThreadsAsyncContainerAllocation() throws Exception {
    testIntraAppAntiAffinityAsync(4);
  }

  @Test(timeout = 300000)
  public void testFiveThreadsAsyncContainerAllocation() throws Exception {
    testIntraAppAntiAffinityAsync(5);
  }
}
