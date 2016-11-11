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

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestCapacitySchedulerAsyncScheduling {
  private final int GB = 1024;

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, true);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @Test(timeout = 300000)
  public void testSingleThreadAsyncContainerAllocation() throws Exception {
    testAsyncContainerAllocation(1);
  }

  @Test(timeout = 300000)
  public void testTwoThreadsAsyncContainerAllocation() throws Exception {
    testAsyncContainerAllocation(2);
  }

  @Test(timeout = 300000)
  public void testThreeThreadsAsyncContainerAllocation() throws Exception {
    testAsyncContainerAllocation(3);
  }

  public void testAsyncContainerAllocation(int numThreads) throws Exception {
    conf.setInt(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD,
        numThreads);
    conf.setInt(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
        + ".scheduling-interval-ms", 100);

    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);

    // inject node label manager
    MockRM rm = new MockRM(TestUtils.getConfigurationWithMultipleQueues(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();

    List<MockNM> nms = new ArrayList<>();
    // Add 10 nodes to the cluster, in the cluster we have 200 GB resource
    for (int i = 0; i < 10; i++) {
      nms.add(rm.registerNode("h-" + i + ":1234", 20 * GB));
    }

    List<MockAM> ams = new ArrayList<>();
    // Add 3 applications to the cluster, one app in one queue
    // the i-th app ask (20 * i) containers. So in total we will have
    // 123G container allocated
    int totalAsked = 3 * GB; // 3 AMs

    for (int i = 0; i < 3; i++) {
      RMApp rmApp = rm.submitApp(1024, "app", "user", null, false,
          Character.toString((char) (i % 34 + 97)), 1, null, null, false);
      MockAM am = MockRM.launchAMWhenAsyncSchedulingEnabled(rmApp, rm);
      am.registerAppAttempt();
      ams.add(am);
    }

    for (int i = 0; i < 3; i++) {
      ams.get(i).allocate("*", 1024, 20 * (i + 1), new ArrayList<>());
      totalAsked += 20 * (i + 1) * GB;
    }

    // Wait for at most 15000 ms
    int waitTime = 15000; // ms
    while (waitTime > 0) {
      if (rm.getResourceScheduler().getRootQueueMetrics().getAllocatedMB()
          == totalAsked) {
        break;
      }
      Thread.sleep(50);
      waitTime -= 50;
    }

    Assert.assertEquals(
        rm.getResourceScheduler().getRootQueueMetrics().getAllocatedMB(),
        totalAsked);

    // Wait for another 2 sec to make sure we will not allocate more than
    // required
    waitTime = 2000; // ms
    while (waitTime > 0) {
      Assert.assertEquals(
          rm.getResourceScheduler().getRootQueueMetrics().getAllocatedMB(),
          totalAsked);
      waitTime -= 50;
      Thread.sleep(50);
    }

    rm.close();
  }
}
