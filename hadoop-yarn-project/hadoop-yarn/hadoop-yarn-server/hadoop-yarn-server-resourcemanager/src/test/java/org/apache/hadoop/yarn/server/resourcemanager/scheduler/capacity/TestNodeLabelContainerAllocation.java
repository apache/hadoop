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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestNodeLabelContainerAllocation {
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
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "z", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setAccessibleNodeLabels(A, toSet("x"));
    conf.setCapacityByLabel(A, "x", 100);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setCapacityByLabel(B, "y", 100);
    conf.setCapacityByLabel(B, "z", 100);

    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);
    conf.setAccessibleNodeLabels(C, RMNodeLabelsManager.EMPTY_STRING_SET);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    conf.setQueues(A, new String[] {"a1"});
    conf.setCapacity(A1, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setCapacityByLabel(A1, "x", 100);
    
    final String B1 = B + ".b1";
    conf.setQueues(B, new String[] {"b1"});
    conf.setCapacity(B1, 100);
    conf.setMaximumCapacity(B1, 100);
    conf.setCapacityByLabel(B1, "y", 100);
    conf.setCapacityByLabel(B1, "z", 100);

    final String C1 = C + ".c1";
    conf.setQueues(C, new String[] {"c1"});
    conf.setCapacity(C1, 100);
    conf.setMaximumCapacity(C1, 100);
    
    return conf;
  }
  
  @SuppressWarnings("unchecked")
  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
  
  /**
   * JIRA YARN-4140, In Resource request set node label will be set only on ANY
   * reqest. RACK/NODE local and default requests label expression need to be
   * updated. This testcase is to verify the label expression is getting changed
   * based on ANY requests.
   *
   * @throws Exception
   */
  @Test
  public void testResourceRequestUpdateNodePartitions() throws Exception {
    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y", "z"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));
    // inject node label manager
    MockRM rm1 = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm2 = rm1.registerNode("h2:1234", 40 * GB); // label = y
    // launch an app to queue b1 (label = y), AM container should be launched in
    // nm2
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    // Creating request set when request before ANY is not having label and any
    // is having label
    List<ResourceRequest> resourceRequest = new ArrayList<ResourceRequest>();
    resourceRequest.add(am1.createResourceReq("/default-rack", 1024, 3, 1,
        RMNodeLabelsManager.NO_LABEL));
    resourceRequest.add(am1.createResourceReq("*", 1024, 3, 5, "y"));
    resourceRequest.add(am1.createResourceReq("h1:1234", 1024, 3, 2,
        RMNodeLabelsManager.NO_LABEL));
    resourceRequest.add(am1.createResourceReq("*", 1024, 2, 3, "y"));
    resourceRequest.add(am1.createResourceReq("h2:1234", 1024, 2, 4, null));
    resourceRequest.add(am1.createResourceReq("*", 1024, 4, 3, null));
    resourceRequest.add(am1.createResourceReq("h2:1234", 1024, 4, 4, null));
    am1.allocate(resourceRequest, new ArrayList<ContainerId>());
    CapacityScheduler cs =
        (CapacityScheduler) rm1.getRMContext().getScheduler();
    FiCaSchedulerApp app =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    List<ResourceRequest> allResourceRequests =
        app.getAppSchedulingInfo().getAllResourceRequests();
    for (ResourceRequest changeReq : allResourceRequests) {
      if (changeReq.getPriority().getPriority() == 2
          || changeReq.getPriority().getPriority() == 3) {
        Assert.assertEquals("Expected label y", "y",
            changeReq.getNodeLabelExpression());
      } else if (changeReq.getPriority().getPriority() == 4) {
        Assert.assertEquals("Expected label EMPTY",
            RMNodeLabelsManager.NO_LABEL, changeReq.getNodeLabelExpression());
      }
    }

    // Previous any request was Y trying to update with z and the
    // request before ANY label is null
    List<ResourceRequest> newReq = new ArrayList<ResourceRequest>();
    newReq.add(am1.createResourceReq("h2:1234", 1024, 3, 4, null));
    newReq.add(am1.createResourceReq("*", 1024, 3, 5, "z"));
    newReq.add(am1.createResourceReq("h1:1234", 1024, 3, 4, null));
    newReq.add(am1.createResourceReq("*", 1024, 4, 5, "z"));
    am1.allocate(newReq, new ArrayList<ContainerId>());
    allResourceRequests = app.getAppSchedulingInfo().getAllResourceRequests();
    for (ResourceRequest changeReq : allResourceRequests) {
      if (changeReq.getPriority().getPriority() == 3
          || changeReq.getPriority().getPriority() == 4) {
        Assert.assertEquals("Expected label z", "z",
            changeReq.getNodeLabelExpression());
      } else if (changeReq.getPriority().getPriority() == 2) {
        Assert.assertEquals("Expected label y", "y",
            changeReq.getNodeLabelExpression());
      }
    }
    // Request before ANY and ANY request is set as NULL. Request should be set
    // with Empty Label
    List<ResourceRequest> resourceRequest1 = new ArrayList<ResourceRequest>();
    resourceRequest1.add(am1.createResourceReq("/default-rack", 1024, 3, 1,
        null));
    resourceRequest1.add(am1.createResourceReq("*", 1024, 3, 5, null));
    resourceRequest1.add(am1.createResourceReq("h1:1234", 1024, 3, 2,
        RMNodeLabelsManager.NO_LABEL));
    resourceRequest1.add(am1.createResourceReq("/default-rack", 1024, 2, 1,
        null));
    resourceRequest1.add(am1.createResourceReq("*", 1024, 2, 3,
        RMNodeLabelsManager.NO_LABEL));
    resourceRequest1.add(am1.createResourceReq("h2:1234", 1024, 2, 4, null));
    am1.allocate(resourceRequest1, new ArrayList<ContainerId>());
    allResourceRequests = app.getAppSchedulingInfo().getAllResourceRequests();
    for (ResourceRequest changeReq : allResourceRequests) {
      if (changeReq.getPriority().getPriority() == 3) {
        Assert.assertEquals("Expected label Empty",
            RMNodeLabelsManager.NO_LABEL, changeReq.getNodeLabelExpression());
      } else if (changeReq.getPriority().getPriority() == 2) {
        Assert.assertEquals("Expected label y", RMNodeLabelsManager.NO_LABEL,
            changeReq.getNodeLabelExpression());
      }
    }
  }
}
