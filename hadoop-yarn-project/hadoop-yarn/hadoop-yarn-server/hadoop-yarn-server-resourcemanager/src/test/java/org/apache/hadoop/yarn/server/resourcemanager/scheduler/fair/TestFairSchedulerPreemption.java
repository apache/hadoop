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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFairSchedulerPreemption extends FairSchedulerTestBase {
  private final static String ALLOC_FILE = new File(TEST_DIR,
      TestFairSchedulerPreemption.class.getName() + ".xml").getAbsolutePath();

  private MockClock clock;

  private static class StubbedFairScheduler extends FairScheduler {
    public int lastPreemptMemory = -1;

    @Override
    protected void preemptResources(Resource toPreempt) {
      lastPreemptMemory = toPreempt.getMemory();
    }

    public void resetLastPreemptResources() {
      lastPreemptMemory = -1;
    }
  }

  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, StubbedFairScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    return conf;
  }

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    clock = new MockClock();
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    conf = null;
  }

  private void startResourceManager(float utilizationThreshold) {
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD,
        utilizationThreshold);
    resourceManager = new MockRM(conf);
    resourceManager.start();

    assertTrue(
        resourceManager.getResourceScheduler() instanceof StubbedFairScheduler);
    scheduler = (FairScheduler)resourceManager.getResourceScheduler();

    scheduler.setClock(clock);
    scheduler.updateInterval = 60 * 1000;
  }

  private void registerNodeAndSubmitApp(
      int memory, int vcores, int appContainers, int appMemory) {
    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(memory, vcores), 1, "node1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    assertEquals("Incorrect amount of resources in the cluster",
        memory, scheduler.rootMetrics.getAvailableMB());
    assertEquals("Incorrect amount of resources in the cluster",
        vcores, scheduler.rootMetrics.getAvailableVirtualCores());

    createSchedulingRequest(appMemory, "queueA", "user1", appContainers);
    scheduler.update();
    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);
    }
    assertEquals("app1's request is not met",
        memory - appContainers * appMemory,
        scheduler.rootMetrics.getAvailableMB());
  }

  @Test
  public void testPreemptionWithFreeResources() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>10</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    startResourceManager(0f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 2, 1024);

    // Verify submitting another request triggers preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should have been called", 1024,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);

    resourceManager.stop();

    startResourceManager(0.8f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 3, 1024);

    // Verify submitting another request doesn't trigger preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should not have been called", -1,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);

    resourceManager.stop();

    startResourceManager(0.7f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 3, 1024);

    // Verify submitting another request triggers preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should have been called", 1024,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);
  }
}
