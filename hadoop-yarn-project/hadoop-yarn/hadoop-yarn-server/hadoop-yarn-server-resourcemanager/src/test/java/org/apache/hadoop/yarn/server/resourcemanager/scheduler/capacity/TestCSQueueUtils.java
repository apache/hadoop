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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

public class TestCSQueueUtils {

  private static final Log LOG = LogFactory.getLog(TestCSQueueUtils.class);

  final static int GB = 1024;

  @Test
  public void testAbsoluteMaxAvailCapacityInvalidDivisor() throws Exception {
    runInvalidDivisorTest(false);
    runInvalidDivisorTest(true);
  }
    
  public void runInvalidDivisorTest(boolean useDominant) throws Exception {
  
    ResourceCalculator resourceCalculator;
    Resource clusterResource;
    if (useDominant) {
      resourceCalculator = new DominantResourceCalculator();
      clusterResource = Resources.createResource(10, 0);
    } else {
      resourceCalculator = new DefaultResourceCalculator();
      clusterResource = Resources.createResource(0, 99);
    }
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
  
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getClusterResource()).thenReturn(clusterResource);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(0, 0));
    RMContext rmContext = TestUtils.getMockRMContext();
    when(csContext.getRMContext()).thenReturn(rmContext);
  
    final String L1Q1 = "L1Q1";
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {L1Q1});
    
    final String L1Q1P = CapacitySchedulerConfiguration.ROOT + "." + L1Q1;
    csConf.setCapacity(L1Q1P, 90);
    csConf.setMaximumCapacity(L1Q1P, 90);
    
    ParentQueue root = new ParentQueue(csContext, 
        CapacitySchedulerConfiguration.ROOT, null, null);
    LeafQueue l1q1 = new LeafQueue(csContext, L1Q1, root, null);
    
    LOG.info("t1 root " + CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, root));
    
    LOG.info("t1 l1q1 " + CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l1q1));
    
    assertEquals(0.0f, CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l1q1), 0.000001f);
    
  }
  
  @Test
  public void testAbsoluteMaxAvailCapacityNoUse() throws Exception {
    
    ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
    Resource clusterResource = Resources.createResource(100 * 16 * GB, 100 * 32);
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getClusterResource()).thenReturn(clusterResource);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(16*GB, 32));
    RMContext rmContext = TestUtils.getMockRMContext();
    when(csContext.getRMContext()).thenReturn(rmContext);
    
    final String L1Q1 = "L1Q1";
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {L1Q1});
    
    final String L1Q1P = CapacitySchedulerConfiguration.ROOT + "." + L1Q1;
    csConf.setCapacity(L1Q1P, 90);
    csConf.setMaximumCapacity(L1Q1P, 90);
    
    ParentQueue root = new ParentQueue(csContext, 
        CapacitySchedulerConfiguration.ROOT, null, null);
    LeafQueue l1q1 = new LeafQueue(csContext, L1Q1, root, null);
    
    LOG.info("t1 root " + CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, root));
    
    LOG.info("t1 l1q1 " + CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l1q1));
    
    assertEquals(1.0f, CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, root), 0.000001f);
    
    assertEquals(0.9f, CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l1q1), 0.000001f);
    
  }
  
  @Test
  public void testAbsoluteMaxAvailCapacityWithUse() throws Exception {
    
    ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
    Resource clusterResource = Resources.createResource(100 * 16 * GB, 100 * 32);
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getClusterResource()).thenReturn(clusterResource);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(16*GB, 32));
    
    RMContext rmContext = TestUtils.getMockRMContext();
    when(csContext.getRMContext()).thenReturn(rmContext);
    
    final String L1Q1 = "L1Q1";
    final String L1Q2 = "L1Q2";
    final String L2Q1 = "L2Q1";
    final String L2Q2 = "L2Q2";
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {L1Q1, L1Q2,
                     L2Q1, L2Q2});
    
    final String L1Q1P = CapacitySchedulerConfiguration.ROOT + "." + L1Q1;
    csConf.setCapacity(L1Q1P, 80);
    csConf.setMaximumCapacity(L1Q1P, 80);
    
    final String L1Q2P = CapacitySchedulerConfiguration.ROOT + "." + L1Q2;
    csConf.setCapacity(L1Q2P, 20);
    csConf.setMaximumCapacity(L1Q2P, 100);
    
    final String L2Q1P = L1Q1P + "." + L2Q1;
    csConf.setCapacity(L2Q1P, 50);
    csConf.setMaximumCapacity(L2Q1P, 50);
    
    final String L2Q2P = L1Q1P + "." + L2Q2;
    csConf.setCapacity(L2Q2P, 50);
    csConf.setMaximumCapacity(L2Q2P, 50);
    
    float result;
    
    ParentQueue root = new ParentQueue(csContext, 
        CapacitySchedulerConfiguration.ROOT, null, null);
    
    LeafQueue l1q1 = new LeafQueue(csContext, L1Q1, root, null);
    LeafQueue l1q2 = new LeafQueue(csContext, L1Q2, root, null);
    LeafQueue l2q2 = new LeafQueue(csContext, L2Q2, l1q1, null);
    LeafQueue l2q1 = new LeafQueue(csContext, L2Q1, l1q1, null);
    
    //no usage, all based on maxCapacity (prior behavior)
    result = CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l2q2);
    assertEquals( 0.4f, result, 0.000001f);
    LOG.info("t2 l2q2 " + result);
    
    //some usage, but below the base capacity
    Resources.addTo(root.getUsedResources(), Resources.multiply(clusterResource, 0.1f));
    Resources.addTo(l1q2.getUsedResources(), Resources.multiply(clusterResource, 0.1f));
    result = CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l2q2);
    assertEquals( 0.4f, result, 0.000001f);
    LOG.info("t2 l2q2 " + result);
    
    //usage gt base on parent sibling
    Resources.addTo(root.getUsedResources(), Resources.multiply(clusterResource, 0.3f));
    Resources.addTo(l1q2.getUsedResources(), Resources.multiply(clusterResource, 0.3f));
    result = CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l2q2);
    assertEquals( 0.3f, result, 0.000001f);
    LOG.info("t2 l2q2 " + result);
    
    //same as last, but with usage also on direct parent
    Resources.addTo(root.getUsedResources(), Resources.multiply(clusterResource, 0.1f));
    Resources.addTo(l1q1.getUsedResources(), Resources.multiply(clusterResource, 0.1f));
    result = CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l2q2);
    assertEquals( 0.3f, result, 0.000001f);
    LOG.info("t2 l2q2 " + result);
    
    //add to direct sibling, below the threshold of effect at present
    Resources.addTo(root.getUsedResources(), Resources.multiply(clusterResource, 0.2f));
    Resources.addTo(l1q1.getUsedResources(), Resources.multiply(clusterResource, 0.2f));
    Resources.addTo(l2q1.getUsedResources(), Resources.multiply(clusterResource, 0.2f));
    result = CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l2q2);
    assertEquals( 0.3f, result, 0.000001f);
    LOG.info("t2 l2q2 " + result);
    
    //add to direct sibling, now above the threshold of effect
    //(it's cumulative with prior tests)
    Resources.addTo(root.getUsedResources(), Resources.multiply(clusterResource, 0.2f));
    Resources.addTo(l1q1.getUsedResources(), Resources.multiply(clusterResource, 0.2f));
    Resources.addTo(l2q1.getUsedResources(), Resources.multiply(clusterResource, 0.2f));
    result = CSQueueUtils.getAbsoluteMaxAvailCapacity(
      resourceCalculator, clusterResource, l2q2);
    assertEquals( 0.1f, result, 0.000001f);
    LOG.info("t2 l2q2 " + result);
    
    
  }

}
