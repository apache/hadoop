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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for dynamic auto created leaf queues.
 * @see ReservationQueue
 */
public class TestReservationQueue {

  private CapacitySchedulerConfiguration csConf;
  private CapacitySchedulerContext csContext;
  private CapacitySchedulerQueueContext queueContext;
  final static int DEF_MAX_APPS = 10000;
  final static int GB = 1024;
  private final ResourceCalculator resourceCalculator =
      new DefaultResourceCalculator();
  private ReservationQueue autoCreatedLeafQueue;
  private PlanQueue planQueue;
  private final Resource clusterResource = Resources.createResource(100 * 16 * GB, 100 * 32);

  @Before
  public void setup() throws IOException, SchedulerDynamicEditException {
    // setup a context / conf
    csConf = new CapacitySchedulerConfiguration();

    YarnConfiguration conf = new YarnConfiguration();
    csContext = mock(CapacitySchedulerContext.class);
    CapacitySchedulerQueueManager csQm = mock(
        CapacitySchedulerQueueManager.class);
    ConfiguredNodeLabels labels = new ConfiguredNodeLabels(csConf);
    when(csQm.getConfiguredNodeLabelsForAllQueues()).thenReturn(labels);
    NullRMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(csConf);
    mgr.setResourceForLabel(CommonNodeLabelsManager.NO_LABEL, clusterResource);
    when(csQm.getQueueCapacityHandler()).thenReturn(
        new CapacitySchedulerQueueCapacityHandler(mgr, csConf));
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getCapacitySchedulerQueueManager()).thenReturn(csQm);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(16 * GB, 32));
    when(csContext.getClusterResource()).thenReturn(
        clusterResource);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getPreemptionManager()).thenReturn(new PreemptionManager());
    RMContext mockRMContext = TestUtils.getMockRMContext();
    when(csContext.getRMContext()).thenReturn(mockRMContext);

    queueContext = new CapacitySchedulerQueueContext(csContext);

    // create a queue
    planQueue = new PlanQueue(queueContext, "root", null, null);
    autoCreatedLeafQueue = new ReservationQueue(queueContext, "a", planQueue);
    planQueue.addChildQueue(autoCreatedLeafQueue);
  }

  private void validateAutoCreatedLeafQueue(double capacity) {
    assertTrue(" actual capacity: " + autoCreatedLeafQueue.getCapacity(),
        autoCreatedLeafQueue.getCapacity() - capacity < CSQueueUtils.EPSILON);
    assertEquals(autoCreatedLeafQueue.maxApplications, DEF_MAX_APPS);
    assertEquals(autoCreatedLeafQueue.maxApplicationsPerUser, DEF_MAX_APPS);
  }

  @Test
  public void testAddSubtractCapacity() throws Exception {
    // verify that setting, adding, subtracting capacity works
    autoCreatedLeafQueue.setCapacity(1.0F);
    autoCreatedLeafQueue.setMaxCapacity(1.0F);


    planQueue.updateClusterResource(
        clusterResource, new ResourceLimits(clusterResource));

    validateAutoCreatedLeafQueue(1);
    autoCreatedLeafQueue.setEntitlement(new QueueEntitlement(0.9f, 1f));
    validateAutoCreatedLeafQueue(0.9);
    autoCreatedLeafQueue.setEntitlement(new QueueEntitlement(1f, 1f));
    validateAutoCreatedLeafQueue(1);
    autoCreatedLeafQueue.setEntitlement(new QueueEntitlement(0f, 1f));
    validateAutoCreatedLeafQueue(0);

    try {
      autoCreatedLeafQueue.setEntitlement(new QueueEntitlement(1.1f, 1f));
      fail();
    } catch (SchedulerDynamicEditException iae) {
      // expected
      validateAutoCreatedLeafQueue(1);
    }

    try {
      autoCreatedLeafQueue.setEntitlement(new QueueEntitlement(-0.1f, 1f));
      fail();
    } catch (SchedulerDynamicEditException iae) {
      // expected
      validateAutoCreatedLeafQueue(1);
    }

  }
}
