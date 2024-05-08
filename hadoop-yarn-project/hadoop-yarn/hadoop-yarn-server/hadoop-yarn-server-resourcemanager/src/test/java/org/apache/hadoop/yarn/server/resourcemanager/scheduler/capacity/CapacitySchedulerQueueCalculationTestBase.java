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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.GB;

public class CapacitySchedulerQueueCalculationTestBase {
  protected static final QueuePath ROOT = new QueuePath("root");
  protected static final QueuePath A = new QueuePath("root.a");
  protected static final QueuePath A1 = new QueuePath("root.a.a1");
  protected static final QueuePath A11 = new QueuePath("root.a.a1.a11");
  protected static final QueuePath A12 = new QueuePath("root.a.a1.a12");
  protected static final QueuePath A2 = new QueuePath("root.a.a2");
  protected static final QueuePath B = new QueuePath("root.b");
  protected static final QueuePath B1 = new QueuePath("root.b.b1");
  protected static final QueuePath C = new QueuePath("root.c");

  private static final String CAPACITY_VECTOR_TEMPLATE = "[memory=%s, vcores=%s]";

  protected ResourceCalculator resourceCalculator;

  protected MockRM mockRM;
  protected CapacityScheduler cs;
  protected CapacitySchedulerConfiguration csConf;
  protected NullRMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    csConf = new CapacitySchedulerConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    csConf.setQueues(ROOT, new String[]{"a", "b"});
    csConf.setCapacity(A, 50f);
    csConf.setCapacity(B, 50f);
    csConf.setQueues(A, new String[]{"a1", "a2"});
    csConf.setCapacity(A1, 100f);
    csConf.setQueues(A1, new String[]{"a11", "a12"});
    csConf.setCapacity(A11, 50f);
    csConf.setCapacity(A12, 50f);

    mgr = new NullRMNodeLabelsManager();
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
    resourceCalculator = cs.getResourceCalculator();
  }
  protected QueueCapacityUpdateContext update(
      QueueAssertionBuilder assertions, Resource clusterResource)
      throws IOException {
    return update(assertions, clusterResource, clusterResource);
  }

  protected QueueCapacityUpdateContext update(
      QueueAssertionBuilder assertions, Resource clusterResource, Resource emptyLabelResource)
      throws IOException {
    cs.reinitialize(csConf, mockRM.getRMContext());

    CapacitySchedulerQueueCapacityHandler queueController =
        new CapacitySchedulerQueueCapacityHandler(mgr, csConf);
    mgr.setResourceForLabel(CommonNodeLabelsManager.NO_LABEL, emptyLabelResource);

    queueController.updateRoot(cs.getQueue("root"), clusterResource);
    QueueCapacityUpdateContext updateContext =
        queueController.updateChildren(clusterResource, cs.getQueue("root"));

    assertions.finishAssertion();

    return updateContext;
  }

  protected QueueAssertionBuilder createAssertionBuilder() {
    return new QueueAssertionBuilder(cs);
  }

  protected static String createCapacityVector(Object memory, Object vcores) {
    return String.format(CAPACITY_VECTOR_TEMPLATE, memory, vcores);
  }

  protected static String absolute(double value) {
    return String.valueOf((long) value);
  }

  protected static String weight(float value) {
    return value + "w";
  }

  protected static String percentage(float value) {
    return value + "%";
  }

  protected static Resource createResource(double memory, double vcores) {
    return Resource.newInstance((int) memory, (int) vcores);
  }
}
