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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

/**
 * Test class for CS metrics.
 */
public class TestCapacitySchedulerMetrics {

  private MockRM rm;

  @Test
  public void testCSMetrics() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, true);

    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    rm = new MockRM(conf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 2048);
    MockNM nm2 = rm.registerNode("host2:1234", 2048);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    CapacitySchedulerMetrics csMetrics = CapacitySchedulerMetrics.getMetrics();
    Assert.assertNotNull(csMetrics);
    try {
      GenericTestUtils.waitFor(()
          -> csMetrics.getNumOfNodeUpdate() == 2, 100, 3000);
    } catch(TimeoutException e) {
      Assert.fail("CS metrics not updated on node-update events.");
    }

    Assert.assertEquals(0, csMetrics.getNumOfAllocates());
    Assert.assertEquals(0, csMetrics.getNumOfCommitSuccess());

    RMApp rmApp = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withUnmanagedAM(false)
            .withQueue("default")
            .withMaxAppAttempts(1)
            .withCredentials(null)
            .withAppType(null)
            .withWaitForAppAcceptedState(false)
            .build());
    MockAM am = MockRM.launchAMWhenAsyncSchedulingEnabled(rmApp, rm);
    am.registerAppAttempt();
    am.allocate("*", 1024, 1, new ArrayList<>());

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    try {
      // Verify HB metrics updated
      GenericTestUtils.waitFor(()
          -> csMetrics.getNumOfNodeUpdate() == 4, 100, 3000);
      // For async mode, the number of alloc might be bigger than 1
      Assert.assertTrue(csMetrics.getNumOfAllocates() > 0);
      // But there will be only 2 successful commit (1 AM + 1 task)
      GenericTestUtils.waitFor(()
          -> csMetrics.getNumOfCommitSuccess() == 2, 100, 3000);
    } catch(TimeoutException e) {
      Assert.fail("CS metrics not updated on node-update events.");
    }
  }

  @After
  public void tearDown() {
    if (rm != null) {
      rm.stop();
    }
  }
}
