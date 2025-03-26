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
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSchedulingUpdate extends FairSchedulerTestBase {

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();

    // Make the update loop to never finish to ensure zero update calls
    conf.setInt(
        FairSchedulerConfiguration.UPDATE_INTERVAL_MS,
        Integer.MAX_VALUE);
    return conf;
  }

  @BeforeEach
  public void setup() {
    conf = createConfiguration();
    resourceManager = new MockRM(conf);
    resourceManager.start();

    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
  }

  @AfterEach
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
  }

  @Test
  @Timeout(value = 3)
  public void testSchedulingUpdateOnNodeJoinLeave() throws InterruptedException {

    verifyNoCalls();

    // Add one node
    String host = "127.0.0.1";
    final int memory = 4096;
    final int cores = 4;
    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(memory, cores), 1, host);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    long expectedCalls = 1;
    verifyExpectedCalls(expectedCalls, memory, cores);

    // Remove the node
    NodeRemovedSchedulerEvent nodeEvent2 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    expectedCalls = 2;
    verifyExpectedCalls(expectedCalls, 0, 0);
  }

  private void verifyExpectedCalls(long expectedCalls, int memory, int vcores)
    throws InterruptedException {
    boolean verified = false;
    int count = 0;
    while (count < 100) {
      if (scheduler.fsOpDurations.hasUpdateThreadRunChanged()) {
        break;
      }
      count++;
      Thread.sleep(10);
    }
    assertTrue(scheduler.fsOpDurations.hasUpdateThreadRunChanged(),
        "Update Thread has not run based on its metrics");
    assertEquals(memory, scheduler.getRootQueueMetrics().getAvailableMB(),
        "Root queue metrics memory does not have expected value");
    assertEquals(vcores, scheduler.getRootQueueMetrics().getAvailableVirtualCores(),
        "Root queue metrics cpu does not have expected value");

    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    scheduler.fsOpDurations.getMetrics(collector, true);
    MetricsRecord record = collector.getRecords().get(0);
    for (AbstractMetric abstractMetric : record.metrics()) {
      if (abstractMetric.name().contains("UpdateThreadRunNumOps")) {
        assertEquals(expectedCalls, abstractMetric.value(),
            "Update Thread did not run expected number of times " +
            "based on metric record count");
        verified = true;
      }
    }
    assertTrue(verified, "Did not find metric for UpdateThreadRunNumOps");
  }

  private void verifyNoCalls() {
    assertFalse(scheduler.fsOpDurations.hasUpdateThreadRunChanged(),
        "Update thread should not have executed");
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB(),
        "Scheduler queue memory should not have been updated");
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores(),
        "Scheduler queue cpu should not have been updated");
  }
}
