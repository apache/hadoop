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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.checkPendingResource;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.toSet;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test class for verifying Scheduling requests in CS.
 */
public class TestCapacitySchedulerSchedulingRequestUpdate {
  @Test
  public void testBasicPendingResourceUpdate() throws Exception {
    Configuration conf = TestUtils.getConfigurationWithQueueLabels(
        new Configuration(false));
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);

    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.start();
    MockNM nm1 = // label = x
        new MockNM("h1:1234", 200 * GB, rm.getResourceTrackerService());
    nm1.registerNode();

    MockNM nm2 = // label = ""
        new MockNM("h2:1234", 200 * GB, rm.getResourceTrackerService());
    nm2.registerNode();

    // Launch app1 in queue=a1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    // Launch app2 in queue=b1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(8 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b1")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);
    // am1 asks for 8 * 1GB container for no label
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(8, Resource.newInstance(1 * GB, 1)),
        Priority.newInstance(1), 0, ImmutableSet.of("mapper", "reducer"),
        "mapper", "reducer");

    checkPendingResource(rm, "a1", 8 * GB, null);
    checkPendingResource(rm, "a", 8 * GB, null);
    checkPendingResource(rm, "root", 8 * GB, null);

    // am2 asks for 8 * 1GB container for no label
    am2.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(1), "*",
            Resources.createResource(1 * GB), 8)), null);

    checkPendingResource(rm, "a1", 8 * GB, null);
    checkPendingResource(rm, "a", 8 * GB, null);
    checkPendingResource(rm, "b1", 8 * GB, null);
    checkPendingResource(rm, "b", 8 * GB, null);
    // root = a + b
    checkPendingResource(rm, "root", 16 * GB, null);

    // am2 asks for 8 * 1GB container in another priority for no label
    am2.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(2), "*",
            Resources.createResource(1 * GB), 8)), null);

    checkPendingResource(rm, "a1", 8 * GB, null);
    checkPendingResource(rm, "a", 8 * GB, null);
    checkPendingResource(rm, "b1", 16 * GB, null);
    checkPendingResource(rm, "b", 16 * GB, null);
    // root = a + b
    checkPendingResource(rm, "root", 24 * GB, null);

    // am1 asks 4 GB resource instead of 8 * GB for priority=1
    // am1 asks for 8 * 1GB container for no label
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(4, Resource.newInstance(1 * GB, 1)),
        Priority.newInstance(1), 0, ImmutableSet.of("mapper", "reducer"),
        "mapper", "reducer");

    checkPendingResource(rm, "a1", 4 * GB, null);
    checkPendingResource(rm, "a", 4 * GB, null);
    checkPendingResource(rm, "b1", 16 * GB, null);
    checkPendingResource(rm, "b", 16 * GB, null);
    // root = a + b
    checkPendingResource(rm, "root", 20 * GB, null);

    // am1 asks 8 * GB resource which label=x
    am1.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(2), "*",
            Resources.createResource(8 * GB), 1, true, "x")), null);

    checkPendingResource(rm, "a1", 4 * GB, null);
    checkPendingResource(rm, "a", 4 * GB, null);
    checkPendingResource(rm, "a1", 8 * GB, "x");
    checkPendingResource(rm, "a", 8 * GB, "x");
    checkPendingResource(rm, "b1", 16 * GB, null);
    checkPendingResource(rm, "b", 16 * GB, null);
    // root = a + b
    checkPendingResource(rm, "root", 20 * GB, null);
    checkPendingResource(rm, "root", 8 * GB, "x");

    // complete am1/am2, pending resource should be 0 now
    AppAttemptRemovedSchedulerEvent appRemovedEvent =
        new AppAttemptRemovedSchedulerEvent(am2.getApplicationAttemptId(),
            RMAppAttemptState.FINISHED, false);
    rm.getResourceScheduler().handle(appRemovedEvent);
    appRemovedEvent = new AppAttemptRemovedSchedulerEvent(
        am1.getApplicationAttemptId(), RMAppAttemptState.FINISHED, false);
    rm.getResourceScheduler().handle(appRemovedEvent);

    checkPendingResource(rm, "a1", 0 * GB, null);
    checkPendingResource(rm, "a", 0 * GB, null);
    checkPendingResource(rm, "a1", 0 * GB, "x");
    checkPendingResource(rm, "a", 0 * GB, "x");
    checkPendingResource(rm, "b1", 0 * GB, null);
    checkPendingResource(rm, "b", 0 * GB, null);
    checkPendingResource(rm, "root", 0 * GB, null);
    checkPendingResource(rm, "root", 0 * GB, "x");
    rm.stop();
  }

  @Test
  public void testNodePartitionPendingResourceUpdate() throws Exception {
    Configuration conf = TestUtils.getConfigurationWithQueueLabels(
        new Configuration(false));
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);

    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.start();
    MockNM nm1 = // label = x
        new MockNM("h1:1234", 200 * GB, rm.getResourceTrackerService());
    nm1.registerNode();

    MockNM nm2 = // label = ""
        new MockNM("h2:1234", 200 * GB, rm.getResourceTrackerService());
    nm2.registerNode();

    // Launch app1 in queue=a1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    // Launch app2 in queue=b1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(8 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b1")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);
    // am1 asks for 8 * 1GB container for "x"
    am1.allocateIntraAppAntiAffinity("x",
        ResourceSizing.newInstance(8, Resource.newInstance(1 * GB, 1)),
        Priority.newInstance(1), 0, "mapper", "reducer");

    checkPendingResource(rm, "a1", 8 * GB, "x");
    checkPendingResource(rm, "a", 8 * GB, "x");
    checkPendingResource(rm, "root", 8 * GB, "x");

    // am2 asks for 8 * 1GB container for "x"
    am2.allocateIntraAppAntiAffinity("x",
        ResourceSizing.newInstance(8, Resource.newInstance(1 * GB, 1)),
        Priority.newInstance(1), 0, "mapper", "reducer");

    checkPendingResource(rm, "a1", 8 * GB, "x");
    checkPendingResource(rm, "a", 8 * GB, "x");
    checkPendingResource(rm, "b1", 8 * GB, "x");
    checkPendingResource(rm, "b", 8 * GB, "x");
    // root = a + b
    checkPendingResource(rm, "root", 16 * GB, "x");

    // am1 asks for 6 * 1GB container for "x" in another priority
    am1.allocateIntraAppAntiAffinity("x",
        ResourceSizing.newInstance(6, Resource.newInstance(1 * GB, 1)),
        Priority.newInstance(2), 0, "mapper", "reducer");

    checkPendingResource(rm, "a1", 14 * GB, "x");
    checkPendingResource(rm, "a", 14 * GB, "x");
    checkPendingResource(rm, "b1", 8 * GB, "x");
    checkPendingResource(rm, "b", 8 * GB, "x");
    // root = a + b
    checkPendingResource(rm, "root", 22 * GB, "x");

    // am1 asks for 4 * 1GB container for "x" in priority=1, which should
    // override 8 * 1GB
    am1.allocateIntraAppAntiAffinity("x",
        ResourceSizing.newInstance(4, Resource.newInstance(1 * GB, 1)),
        Priority.newInstance(1), 0, "mapper", "reducer");

    checkPendingResource(rm, "a1", 10 * GB, "x");
    checkPendingResource(rm, "a", 10 * GB, "x");
    checkPendingResource(rm, "b1", 8 * GB, "x");
    checkPendingResource(rm, "b", 8 * GB, "x");
    // root = a + b
    checkPendingResource(rm, "root", 18 * GB, "x");

    // complete am1/am2, pending resource should be 0 now
    AppAttemptRemovedSchedulerEvent appRemovedEvent =
        new AppAttemptRemovedSchedulerEvent(am2.getApplicationAttemptId(),
            RMAppAttemptState.FINISHED, false);
    rm.getResourceScheduler().handle(appRemovedEvent);
    appRemovedEvent = new AppAttemptRemovedSchedulerEvent(
        am1.getApplicationAttemptId(), RMAppAttemptState.FINISHED, false);
    rm.getResourceScheduler().handle(appRemovedEvent);

    checkPendingResource(rm, "a1", 0 * GB, null);
    checkPendingResource(rm, "a", 0 * GB, null);
    checkPendingResource(rm, "a1", 0 * GB, "x");
    checkPendingResource(rm, "a", 0 * GB, "x");
    checkPendingResource(rm, "b1", 0 * GB, null);
    checkPendingResource(rm, "b", 0 * GB, null);
    checkPendingResource(rm, "root", 0 * GB, null);
    checkPendingResource(rm, "root", 0 * GB, "x");
    rm.stop();
  }
}
