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

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.toSet;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils.getConfigurationWithQueueLabels;

public class TestCapacitySchedulerRequestsHandler {

  /**
   * Simple e2e verification for requests-handler.
   * - requests-handler can be enabled dynamically via reinitializing scheduler
   * - partition will be updated for the matched app and request
   */
  @Test
  public void testRequestsHandlerSimpleCase() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    conf = getConfigurationWithQueueLabels(conf);

    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(
        NodeId.newInstance("h1", 0), toSet("x")));

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
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app1").withUser("root").withAcls(null)
            .withQueue("a1").withUnmanagedAM(false).build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    // am1 asks for a container with no label which will be allocated on nm2
    am1.allocate("*", GB, 1, 10, new ArrayList<>(), "");
    ContainerId containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm.waitForState(nm2, containerId, RMContainerState.ALLOCATED);

    // refresh conf with requests-handler enabled
    // partition will be updated to 'x' for apps in a1 queue
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED, "true");
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[{\"appMatchExpr\":\"queue=='a1'\"," +
            "\"requestMatchExpr\":\"priority>5\"," +
            " \"partition\":\"x\"}]}");
    rm.getResourceScheduler().reinitialize(conf, rm.getRMContext());

    // am1 asks for another container with no label
    // request matched, partition will be updated to 'x' for this request
    // so that it will be allocated on nm1
    am1.allocate("*", GB, 1, 10, new ArrayList<>(), "");
    containerId = ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    rm.waitForState(nm1, containerId, RMContainerState.ALLOCATED);

    // am1 asks for another container with no label
    // request not matched, partition won't be updated to 'x' for this request
    // so that it will be allocated on nm2
    am1.allocate("*", GB, 1, new ArrayList<>(), "");
    containerId = ContainerId.newContainerId(am1.getApplicationAttemptId(), 4);
    rm.waitForState(nm2, containerId, RMContainerState.ALLOCATED);

    rm.stop();
  }
}
