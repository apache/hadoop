/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTagWithNamespace;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.delayedOr;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.timedOpportunitiesConstraint;

public class TestCapacitySchedulerPlacementConstraintsHandler {

  private final int GB = 1024;
  private CapacitySchedulerConfiguration conf;

  @Before
  public void setUp() {
    CapacitySchedulerConfiguration config =
        new CapacitySchedulerConfiguration();
    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    conf = new CapacitySchedulerConfiguration(config);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
  }

  @Test(timeout = 30000L)
  public void testDelayedOrPlacementConstraint() throws Exception {
    MockRM rm1 = new MockRM(conf);
    rm1.start();

    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB);

    // submit 2 apps
    MockRMAppSubmissionData submissionData1 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app1")
            .withUser("root")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, submissionData1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    MockRMAppSubmissionData submissionData2 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app2")
            .withUser("root")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, submissionData2);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    // Init scheduling-request with delayed_or placement-constraint
    // which will try to allocate on node with specified allocation-tag
    // in the first 3 scheduling attempts.
    PlacementConstraint constraint = delayedOr(timedOpportunitiesConstraint(
        targetIn("node", allocationTagWithNamespace("self", "test")),
        3)).build();
    SchedulingRequest sc = SchedulingRequest.newInstance(1,
        Priority.newInstance(1),
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
        ImmutableSet.of("AM"),
        ResourceSizing.newInstance(1, Resource.newInstance(GB, 1)),
        constraint);

    // test for app1
    am1.addSchedulingRequest(ImmutableList.of(sc));
    am1.doHeartbeat();
    for (int i = 0; i < 3; i++) {
      nm1.nodeHeartbeat(true);
      rm1.drainEvents();
      Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    }
    nm1.nodeHeartbeat(true);
    rm1.drainEvents();
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());

    // test for app2
    am2.addSchedulingRequest(ImmutableList.of(sc));
    am2.doHeartbeat();
    for (int i = 0; i < 3; i++) {
      nm1.nodeHeartbeat(true);
      rm1.drainEvents();
      Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    }
    nm1.nodeHeartbeat(true);
    rm1.drainEvents();
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());

    rm1.stop();
  }
}
