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
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.util.SampleStat;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestContinuousScheduling extends FairSchedulerTestBase {
  private MockClock mockClock;

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean(
        FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED, true);
    conf.setInt(FairSchedulerConfiguration.LOCALITY_DELAY_NODE_MS, 100);
    conf.setInt(FairSchedulerConfiguration.LOCALITY_DELAY_RACK_MS, 100);
    return conf;
  }

  @Before
  public void setup() {
    mockClock = new MockClock();
    conf = createConfiguration();
    resourceManager = new MockRM(conf);
    resourceManager.start();

    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
    scheduler.setClock(mockClock);

    assertTrue(scheduler.isContinuousSchedulingEnabled());
    assertEquals(
        FairSchedulerConfiguration.DEFAULT_CONTINUOUS_SCHEDULING_SLEEP_MS,
        scheduler.getContinuousSchedulingSleepMs());
    assertEquals(mockClock, scheduler.getClock());
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
  }

  @Test (timeout = 60000)
  public void testSchedulingDelay() throws InterruptedException {
    // Add one node
    String host = "127.0.0.1";
    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(4096, 4), 1, host);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    NodeUpdateSchedulerEvent nodeUpdateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeUpdateEvent);

    // Create one application and submit one each of node-local, rack-local
    // and ANY requests
    ApplicationAttemptId appAttemptId =
        createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    createMockRMApp(appAttemptId);

    scheduler.addApplication(appAttemptId.getApplicationId(), "queue11", "user11", false);
    scheduler.addApplicationAttempt(appAttemptId, false, false);
    List<ResourceRequest> ask = new ArrayList<>();
    ask.add(createResourceRequest(1024, 1, ResourceRequest.ANY, 1, 1, true));
    scheduler.allocate(
        appAttemptId, ask, new ArrayList<ContainerId>(), null, null);
    FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);

    // Advance time and let continuous scheduling kick in
    mockClock.tick(1);
    while (1024 != app.getCurrentConsumption().getMemory()) {
      Thread.sleep(100);
    }
    assertEquals(1024, app.getCurrentConsumption().getMemory());
  }
}
