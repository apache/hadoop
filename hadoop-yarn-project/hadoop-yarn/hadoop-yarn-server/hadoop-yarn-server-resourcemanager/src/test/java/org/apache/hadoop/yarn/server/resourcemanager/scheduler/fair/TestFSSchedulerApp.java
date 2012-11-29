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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerApp;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSSchedulerApp {
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
    ApplicationAttemptId attId = recordFactory.newRecordInstance(ApplicationAttemptId.class);
    ApplicationId appIdImpl = recordFactory.newRecordInstance(ApplicationId.class);
    appIdImpl.setId(appId);
    attId.setAttemptId(attemptId);
    attId.setApplicationId(appIdImpl);
    return attId;
  }

  @Test
  public void testDelayScheduling() {
    Queue queue = Mockito.mock(Queue.class);
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);
    double nodeLocalityThreshold = .5;
    double rackLocalityThreshold = .6;

    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    FSSchedulerApp schedulerApp =
        new FSSchedulerApp(applicationAttemptId, "user1", queue , null, null);

    // Default level should be node-local
    assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // First five scheduling opportunities should remain node local
    for (int i = 0; i < 5; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    // After five it should switch to rack local
    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // Manually set back to node local
    schedulerApp.resetAllowedLocalityLevel(prio, NodeType.NODE_LOCAL);
    schedulerApp.resetSchedulingOpportunities(prio);
    assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // Now escalate again to rack-local, then to off-switch
    for (int i = 0; i < 5; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    for (int i = 0; i < 6; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.OFF_SWITCH, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
  }

  @Test
  /**
   * Ensure that when negative paramaters are given (signaling delay scheduling
   * no tin use), the least restrictive locality level is returned.
   */
  public void testLocalityLevelWithoutDelays() {
    Queue queue = Mockito.mock(Queue.class);
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);

    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    FSSchedulerApp schedulerApp =
        new FSSchedulerApp(applicationAttemptId, "user1", queue , null, null);
    assertEquals(NodeType.OFF_SWITCH, schedulerApp.getAllowedLocalityLevel(
        prio, 10, -1.0, -1.0));
  }
}
