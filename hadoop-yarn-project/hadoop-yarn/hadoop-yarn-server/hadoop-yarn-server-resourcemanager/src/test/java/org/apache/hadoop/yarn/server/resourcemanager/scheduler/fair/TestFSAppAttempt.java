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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSAppAttempt extends FairSchedulerTestBase {

  private class MockClock implements Clock {
    private long time = 0;
    @Override
    public long getTime() {
      return time;
    }

    public void tick(int seconds) {
      time = time + seconds * 1000;
    }

  }

  @Before
  public void setup() {
    Configuration conf = createConfiguration();
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
  }

  @Test
  public void testDelayScheduling() {
    FSLeafQueue queue = Mockito.mock(FSLeafQueue.class);
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);
    double nodeLocalityThreshold = .5;
    double rackLocalityThreshold = .6;

    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    RMContext rmContext = resourceManager.getRMContext();
    FSAppAttempt schedulerApp =
        new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue ,
            null, rmContext);

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
  public void testDelaySchedulingForContinuousScheduling()
          throws InterruptedException {
    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue("queue", true);
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);

    MockClock clock = new MockClock();
    scheduler.setClock(clock);

    long nodeLocalityDelayMs = 5 * 1000L;    // 5 seconds
    long rackLocalityDelayMs = 6 * 1000L;    // 6 seconds

    RMContext rmContext = resourceManager.getRMContext();
    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    FSAppAttempt schedulerApp =
            new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue,
                    null, rmContext);

    // Default level should be node-local
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // after 4 seconds should remain node local
    clock.tick(4);
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // after 6 seconds should switch to rack local
    clock.tick(2);
    assertEquals(NodeType.RACK_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // manually set back to node local
    schedulerApp.resetAllowedLocalityLevel(prio, NodeType.NODE_LOCAL);
    schedulerApp.resetSchedulingOpportunities(prio, clock.getTime());
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // Now escalate again to rack-local, then to off-switch
    clock.tick(6);
    assertEquals(NodeType.RACK_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    clock.tick(7);
    assertEquals(NodeType.OFF_SWITCH,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));
  }

  @Test
  /**
   * Ensure that when negative paramaters are given (signaling delay scheduling
   * no tin use), the least restrictive locality level is returned.
   */
  public void testLocalityLevelWithoutDelays() {
    FSLeafQueue queue = Mockito.mock(FSLeafQueue.class);
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);

    RMContext rmContext = resourceManager.getRMContext();
    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    FSAppAttempt schedulerApp =
        new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue ,
            null, rmContext);
    assertEquals(NodeType.OFF_SWITCH, schedulerApp.getAllowedLocalityLevel(
        prio, 10, -1.0, -1.0));
  }
}
