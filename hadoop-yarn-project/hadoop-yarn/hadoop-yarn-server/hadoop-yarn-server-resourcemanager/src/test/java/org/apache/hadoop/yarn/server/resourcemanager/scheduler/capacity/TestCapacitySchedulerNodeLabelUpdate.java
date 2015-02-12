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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestCapacitySchedulerNodeLabelUpdate {
  private final int GB = 1024;

  private YarnConfiguration conf;
  
  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }
  
  private Configuration getConfigurationWithQueueLabels(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "z", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 100);
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("x", "y", "z"));
    conf.setCapacityByLabel(A, "x", 100);
    conf.setCapacityByLabel(A, "y", 100);
    conf.setCapacityByLabel(A, "z", 100);
    
    return conf;
  }
  
  private Set<String> toSet(String... elements) {
    Set<String> set = Sets.newHashSet(elements);
    return set;
  }
  
  private void checkUsedResource(MockRM rm, String queueName, int memory) {
    checkUsedResource(rm, queueName, memory, RMNodeLabelsManager.NO_LABEL);
  }
  
  private void checkUsedResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = scheduler.getQueue(queueName);
    Assert.assertEquals(memory, queue.getQueueResourceUsage().getUsed(label)
        .getMemory());
  }

  @Test (timeout = 30000)
  public void testNodeUpdate() throws Exception {
    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y", "z"));
    
    // set mapping:
    // h1 -> x
    // h2 -> y
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 8000);
    MockNM nm2 = rm.registerNode("h2:1234", 8000);
    MockNM nm3 = rm.registerNode("h3:1234", 8000);
    
    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm.submitApp(GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm3);

    // request a container.
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "x");
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    
    // check used resource:
    // queue-a used x=1G, ""=1G
    checkUsedResource(rm, "a", 1024, "x");
    checkUsedResource(rm, "a", 1024);
    
    // change h1's label to z, container should be killed
    mgr.replaceLabelsOnNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        toSet("z")));
    Assert.assertTrue(rm.waitForState(nm1, containerId,
        RMContainerState.KILLED, 10 * 1000));
    
    // check used resource:
    // queue-a used x=0G, ""=1G ("" not changed)
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 1024);
    
    // request a container with label = y
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "y");
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    Assert.assertTrue(rm.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    
    // check used resource:
    // queue-a used y=1G, ""=1G
    checkUsedResource(rm, "a", 1024, "y");
    checkUsedResource(rm, "a", 1024);
    
    // change h2's label to no label, container should be killed
    mgr.replaceLabelsOnNode(ImmutableMap.of(NodeId.newInstance("h2", 0),
        CommonNodeLabelsManager.EMPTY_STRING_SET));
    Assert.assertTrue(rm.waitForState(nm1, containerId,
        RMContainerState.KILLED, 10 * 1000));
    
    // check used resource:
    // queue-a used x=0G, y=0G, ""=1G ("" not changed)
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 0, "y");
    checkUsedResource(rm, "a", 1024);
    
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    
    // change h3's label to z, AM container should be killed
    mgr.replaceLabelsOnNode(ImmutableMap.of(NodeId.newInstance("h3", 0),
        toSet("z")));
    Assert.assertTrue(rm.waitForState(nm1, containerId,
        RMContainerState.KILLED, 10 * 1000));
    
    // check used resource:
    // queue-a used x=0G, y=0G, ""=1G ("" not changed)
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 0, "y");
    checkUsedResource(rm, "a", 0);

    rm.close();
  }
}
