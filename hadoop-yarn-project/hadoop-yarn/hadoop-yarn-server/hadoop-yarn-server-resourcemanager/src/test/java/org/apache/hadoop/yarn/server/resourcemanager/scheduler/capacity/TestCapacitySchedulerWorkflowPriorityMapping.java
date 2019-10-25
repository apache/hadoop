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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.WorkflowPriorityMappingsManager.WorkflowPriorityMapping;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestCapacitySchedulerWorkflowPriorityMapping
    extends CapacitySchedulerTestBase {
  private MockRM mockRM = null;

  private static void setWorkFlowPriorityMappings(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(
        CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setCapacity(A2, A2_CAPACITY);

    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setCapacity(B3, B3_CAPACITY);

    List<WorkflowPriorityMapping> mappings = Arrays.asList(
        new WorkflowPriorityMapping("workflow1", B, Priority.newInstance(2)),
        new WorkflowPriorityMapping("workflow2", A1, Priority.newInstance(3)),
        new WorkflowPriorityMapping("workflow3", A, Priority.newInstance(4)));
    conf.setWorkflowPriorityMappings(mappings);
  }

  @Test
  public void testWorkflowPriorityMappings() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(CapacitySchedulerConfiguration
        .ENABLE_WORKFLOW_PRIORITY_MAPPINGS_OVERRIDE, true);
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);

    // Initialize workflow priority mappings.
    setWorkFlowPriorityMappings(conf);

    mockRM = new MockRM(conf);
    CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
    mockRM.start();
    cs.start();

    Map<String, Map<String, Object>> expected = ImmutableMap.of(
        A, ImmutableMap.of("workflow3",
        new WorkflowPriorityMapping(
            "workflow3", A, Priority.newInstance(4))),
        B, ImmutableMap.of("workflow1",
        new WorkflowPriorityMapping(
            "workflow1", B, Priority.newInstance(2))),
        A1, ImmutableMap.of("workflow2",
        new WorkflowPriorityMapping(
            "workflow2", A1, Priority.newInstance(3))));
    assertEquals(expected, cs.getWorkflowPriorityMappingsManager()
        .getWorkflowPriorityMappings());

    // Maps to rule corresponding to parent queue "a" for workflow3.
    mockRM.submitApp(1, "a2", true, ApplicationId.newInstance(0,1),
        Priority.newInstance(0), ImmutableSet.of(
            YarnConfiguration.DEFAULT_YARN_WORKFLOW_ID_TAG_PREFIX
            + "workflow3"));
    RMApp app =
        mockRM.getRMContext().getRMApps().get(ApplicationId.newInstance(0,1));
    assertEquals(4, app.getApplicationSubmissionContext().getPriority()
        .getPriority());

    // Does not match any rule as rule for queue + workflow does not exist.
    // Priority passed in the app is taken up.
    mockRM.submitApp(1, "a1", true, ApplicationId.newInstance(0,2),
        Priority.newInstance(6), ImmutableSet.of(
            YarnConfiguration.DEFAULT_YARN_WORKFLOW_ID_TAG_PREFIX
            + "workflow1"));
    app =
        mockRM.getRMContext().getRMApps().get(ApplicationId.newInstance(0,2));
    assertEquals(6, app.getApplicationSubmissionContext().getPriority()
        .getPriority());

    // Maps to rule corresponding to parent queue "a1" for workflow2.
    mockRM.submitApp(1, "a1", true, ApplicationId.newInstance(0,3),
        Priority.newInstance(0), ImmutableSet.of(
            YarnConfiguration.DEFAULT_YARN_WORKFLOW_ID_TAG_PREFIX
            + "workflow2"));
    app =
        mockRM.getRMContext().getRMApps().get(ApplicationId.newInstance(0,3));
    assertEquals(3, app.getApplicationSubmissionContext().getPriority()
        .getPriority());

    // Maps to rule corresponding to parent queue "b" for workflow1.
    mockRM.submitApp(1, "b3", true, ApplicationId.newInstance(0,4),
        Priority.newInstance(0), ImmutableSet.of(
            YarnConfiguration.DEFAULT_YARN_WORKFLOW_ID_TAG_PREFIX
            + "workflow1"));
    app = mockRM.getRMContext().getRMApps().get(ApplicationId.newInstance(0,4));
    assertEquals(2, app.getApplicationSubmissionContext().getPriority()
        .getPriority());

    // Disable workflow priority mappings override and reinitialize scheduler.
    conf.setBoolean(CapacitySchedulerConfiguration
        .ENABLE_WORKFLOW_PRIORITY_MAPPINGS_OVERRIDE, false);
    cs.reinitialize(conf, mockRM.getRMContext());
    mockRM.submitApp(1, "a2", true, ApplicationId.newInstance(0,5),
        Priority.newInstance(0), ImmutableSet.of(
            YarnConfiguration.DEFAULT_YARN_WORKFLOW_ID_TAG_PREFIX
            + "workflow3"));
    app = mockRM.getRMContext().getRMApps().get(ApplicationId.newInstance(0,5));
    assertEquals(0, app.getApplicationSubmissionContext().getPriority()
        .getPriority());
  }
}
