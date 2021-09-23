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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestQueueResourceCalculation
    extends TestCapacitySchedulerAutoCreatedQueueBase {

  private CapacitySchedulerConfiguration csConf;
  private static final Resource QUEUE_A_RES = Resource.newInstance(80 * GB,
      10);
  private static final Resource QUEUE_B_RES = Resource.newInstance( 120 * GB,
      15);
  private static final Resource QUEUE_A1_RES = Resource.newInstance(50 * GB,
      15);
  private static final Resource UPDATE_RES = Resource.newInstance(250 * GB, 40);

  public void setUp() throws Exception {
    csConf = new CapacitySchedulerConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    // By default, set 3 queues, a/b, and a.a1
    csConf.setQueues("root", new String[]{"a", "b"});
    csConf.setNonLabeledQueueWeight("root", 1f);
    csConf.setNonLabeledQueueWeight("root.a", 6f);
    csConf.setNonLabeledQueueWeight("root.b", 4f);
    csConf.setQueues("root.a", new String[]{"a1", "a2"});
    csConf.setNonLabeledQueueWeight("root.a.a1", 1f);

    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(csConf);
    mockRM = new MockRM(csConf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    // Policy for new auto created queue's auto deletion when expired
    mockRM.start();
    cs.start();
    mockRM.registerNode("h1:1234", 10 * GB); // label = x
  }

  @Test
  public void testPercentageResourceCalculation() throws IOException {
    csConf.setCapacity("root.a", 30);
    csConf.setCapacity("root.b", 70);
    csConf.setCapacity("root.a.a1", 17);
    csConf.setCapacity("root.a.a2", 83);
    cs.reinitialize(csConf, mockRM.getRMContext());

    CapacitySchedulerQueueCapacityHandler queueController =
        new CapacitySchedulerQueueCapacityHandler();
    CSQueue a = cs.getQueue("root.a");
    CSQueue b = cs.getQueue("root.b");
    CSQueue a1 = cs.getQueue("root.a.a1");
    CSQueue a2 = cs.getQueue("root.a.a2");

    queueController.setup(a, csConf);
    queueController.setup(b, csConf);
    queueController.setup(a1, csConf);
    queueController.setup(a2, csConf);
    queueController.update(Resource.newInstance(10 * GB, 20), cs.getQueue("root"));

    Assert.assertEquals(0.3 * 10 * GB, a.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(), 1e-6);
    Assert.assertEquals(0.3, a.getQueueCapacities().getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(0.3, a.getQueueCapacities().getCapacity(), 1e-6);

    Assert.assertEquals(0.7 * 10 * GB, b.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(), 1e-6);
    Assert.assertEquals(0.7, b.getQueueCapacities().getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(0.7, b.getQueueCapacities().getCapacity(), 1e-6);

    Assert.assertEquals(Math.round(0.3 * 0.17 * 10 * GB), a1.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(0.3 * 0.17, a1.getQueueCapacities().getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(0.17, a1.getQueueCapacities().getCapacity(), 1e-6);

    Assert.assertEquals(Math.round(0.3 * 0.83 * 10 * GB), a2.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(0.3 * 0.83, a2.getQueueCapacities().getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(0.83, a2.getQueueCapacities().getCapacity(), 1e-6);
  }
}