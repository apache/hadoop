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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.PriorityUtilizationQueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TestQueueParsing {

  private static final Log LOG = LogFactory.getLog(TestQueueParsing.class);
  
  private static final double DELTA = 0.000001;
  
  private RMNodeLabelsManager nodeLabelManager;
  
  @Before
  public void setup() {
    nodeLabelManager = new NullRMNodeLabelsManager();
    nodeLabelManager.init(new YarnConfiguration());
    nodeLabelManager.start();
  }
  
  @Test
  public void testQueueParsing() throws Exception {
    CapacitySchedulerConfiguration csConf = 
      new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(conf);
    capacityScheduler.setRMContext(TestUtils.getMockRMContext());
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, TestUtils.getMockRMContext());
    
    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue b1 = capacityScheduler.getQueue("b1");
    Assert.assertEquals(0.2 * 0.5, b1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Parent B has no MAX_CAP", 
        0.85, b1.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue c12 = capacityScheduler.getQueue("c12");
    Assert.assertEquals(0.7 * 0.5 * 0.45, c12.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.7 * 0.55 * 0.7, 
        c12.getAbsoluteMaximumCapacity(), DELTA);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  private void setupQueueConfigurationWithSpacesShouldBeTrimmed(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.set(
        CapacitySchedulerConfiguration
            .getQueuePrefix(CapacitySchedulerConfiguration.ROOT)
            + CapacitySchedulerConfiguration.QUEUES, " a ,b, c");

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);
  }
  
  private void setupNestedQueueConfigurationWithSpacesShouldBeTrimmed(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.set(
        CapacitySchedulerConfiguration
            .getQueuePrefix(CapacitySchedulerConfiguration.ROOT)
            + CapacitySchedulerConfiguration.QUEUES, " a ,b, c");

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);

    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);

    // sub queues for A
    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.QUEUES, "a1, a2 ");

    final String A1 = CapacitySchedulerConfiguration.ROOT + ".a.a1";
    conf.setCapacity(A1, 60);

    final String A2 = CapacitySchedulerConfiguration.ROOT + ".a.a2";
    conf.setCapacity(A2, 40);
  }
  
  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);

    LOG.info("Setup top-level queues");
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    
    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);

    final String C1 = C + ".c1";
    final String C2 = C + ".c2";
    final String C3 = C + ".c3";
    final String C4 = C + ".c4";
    conf.setQueues(C, new String[] {"c1", "c2", "c3", "c4"});
    conf.setCapacity(C1, 50);
    conf.setMaximumCapacity(C1, 55);
    conf.setCapacity(C2, 10);
    conf.setMaximumCapacity(C2, 25);
    conf.setCapacity(C3, 35);
    conf.setMaximumCapacity(C3, 38);
    conf.setCapacity(C4, 5);
    conf.setMaximumCapacity(C4, 5);
    
    LOG.info("Setup 2nd-level queues");
    
    // Define 3rd-level queues
    final String C11 = C1 + ".c11";
    final String C12 = C1 + ".c12";
    final String C13 = C1 + ".c13";
    conf.setQueues(C1, new String[] {"c11", "c12", "c13"});
    conf.setCapacity(C11, 15);
    conf.setMaximumCapacity(C11, 30);
    conf.setCapacity(C12, 45);
    conf.setMaximumCapacity(C12, 70);
    conf.setCapacity(C13, 40);
    conf.setMaximumCapacity(C13, 40);
    
    LOG.info("Setup 3rd-level queues");
  }

  @Test (expected=java.lang.IllegalArgumentException.class)
  public void testRootQueueParsing() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    // non-100 percent value will throw IllegalArgumentException
    conf.setCapacity(CapacitySchedulerConfiguration.ROOT, 90);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(new YarnConfiguration());
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, null);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  public void testMaxCapacity() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 50);
    conf.setMaximumCapacity(A, 60);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 50);
    conf.setMaximumCapacity(B, 45);  // Should throw an exception


    boolean fail = false;
    CapacityScheduler capacityScheduler;
    try {
      capacityScheduler = new CapacityScheduler();
      capacityScheduler.setConf(new YarnConfiguration());
      capacityScheduler.init(conf);
      capacityScheduler.start();
      capacityScheduler.reinitialize(conf, null);
    } catch (IllegalArgumentException iae) {
      fail = true;
    }
    Assert.assertTrue("Didn't throw IllegalArgumentException for wrong maxCap", 
        fail);

    conf.setMaximumCapacity(B, 60);
    
    // Now this should work
    capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(new YarnConfiguration());
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, null);
    
    fail = false;
    try {
    LeafQueue a = (LeafQueue)capacityScheduler.getQueue(A);
    a.setMaxCapacity(45);
    } catch  (IllegalArgumentException iae) {
      fail = true;
    }
    Assert.assertTrue("Didn't throw IllegalArgumentException for wrong " +
    		"setMaxCap", fail);
    capacityScheduler.stop();
  }
  
  private void setupQueueConfigurationWithoutLabels(CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);

    LOG.info("Setup top-level queues");
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    
    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);
  }
  
  private void setupQueueConfigurationWithLabels(CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "red", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "blue", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);

    LOG.info("Setup top-level queues");
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 50);
    conf.setMaximumCapacityByLabel(A, "red", 50);
    conf.setCapacityByLabel(A, "blue", 50);
    
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacityByLabel(A1, "red", 50);
    conf.setCapacityByLabel(A1, "blue", 100);
    
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    conf.setAccessibleNodeLabels(A2, ImmutableSet.of("red"));
    conf.setCapacityByLabel(A2, "red", 50);
    conf.setMaximumCapacityByLabel(A2, "red", 60);
    
    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setAccessibleNodeLabels(B, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(B, "red", 50);
    conf.setCapacityByLabel(B, "blue", 50);
    
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacityByLabel(B1, "red", 50);
    conf.setCapacityByLabel(B1, "blue", 50);
    
    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacityByLabel(B2, "red", 25);
    conf.setCapacityByLabel(B2, "blue", 25);
    
    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);
    conf.setCapacityByLabel(B3, "red", 25);
    conf.setCapacityByLabel(B3, "blue", 25);
  }

  private void setupQueueConfigurationWithLabelsAndReleaseCheck
      (CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "red", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "blue", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    // The cap <= max-cap check is not needed
    conf.setCapacity(A, 50);
    conf.setMaximumCapacity(A, 100);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 50);
    conf.setMaximumCapacity(B, 100);

    LOG.info("Setup top-level queues");

    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 50);
    conf.setMaximumCapacityByLabel(A, "red", 100);
    conf.setCapacityByLabel(A, "blue", 30);
    conf.setMaximumCapacityByLabel(A, "blue", 50);

    conf.setCapacity(A1, 60);
    conf.setMaximumCapacity(A1, 60);
    conf.setCapacityByLabel(A1, "red", 60);
    conf.setMaximumCapacityByLabel(A1, "red", 30);
    conf.setCapacityByLabel(A1, "blue", 100);
    conf.setMaximumCapacityByLabel(A1, "blue", 100);

    conf.setCapacity(A2, 40);
    conf.setMaximumCapacity(A2, 85);
    conf.setAccessibleNodeLabels(A2, ImmutableSet.of("red"));
    conf.setCapacityByLabel(A2, "red", 40);
    conf.setMaximumCapacityByLabel(A2, "red", 60);

    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setAccessibleNodeLabels(B, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(B, "red", 50);
    conf.setMaximumCapacityByLabel(B, "red", 100);
    conf.setCapacityByLabel(B, "blue", 70);
    conf.setMaximumCapacityByLabel(B, "blue", 100);

    conf.setCapacity(B1, 10);
    conf.setMaximumCapacity(B1, 10);
    conf.setCapacityByLabel(B1, "red", 60);
    conf.setMaximumCapacityByLabel(B1, "red", 30);
    conf.setCapacityByLabel(B1, "blue", 50);
    conf.setMaximumCapacityByLabel(B1, "blue", 100);

    conf.setCapacity(B2, 80);
    conf.setMaximumCapacity(B2, 40);
    conf.setCapacityByLabel(B2, "red", 30);
    conf.setCapacityByLabel(B2, "blue", 25);

    conf.setCapacity(B3, 10);
    conf.setMaximumCapacity(B3, 25);
    conf.setCapacityByLabel(B3, "red", 10);
    conf.setCapacityByLabel(B3, "blue", 25);
  }

  private void setupQueueConfigurationWithLabelsInherit(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "red", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "blue", 100);

    // Set A configuration
    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 100);
    conf.setCapacityByLabel(A, "blue", 100);
    
    // Set B configuraiton
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);
    conf.setAccessibleNodeLabels(B, CommonNodeLabelsManager.EMPTY_STRING_SET);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacityByLabel(A1, "red", 50);
    conf.setCapacityByLabel(A1, "blue", 100);
    
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    conf.setAccessibleNodeLabels(A2, ImmutableSet.of("red"));
    conf.setCapacityByLabel(A2, "red", 50);
  }
  
  private void setupQueueConfigurationWithSingleLevel(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});

    // Set A configuration
    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 90);
    conf.setCapacityByLabel(A, "blue", 90);
    
    // Set B configuraiton
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);
    conf.setAccessibleNodeLabels(B, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(B, "red", 10);
    conf.setCapacityByLabel(B, "blue", 10);
  }
  
  @Test
  public void testQueueParsingReinitializeWithLabels() throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("red", "blue"));
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutLabels(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(conf),
            new NMTokenSecretManagerInRM(conf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(conf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(conf);
    capacityScheduler.start();
    csConf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabels(csConf);
    conf = new YarnConfiguration(csConf);
    capacityScheduler.reinitialize(conf, rmContext);
    checkQueueLabels(capacityScheduler);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  private void checkQueueLabels(CapacityScheduler capacityScheduler) {
    // queue-A is red, blue
    Assert.assertTrue(capacityScheduler.getQueue("a").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-A1 inherits A's configuration
    Assert.assertTrue(capacityScheduler.getQueue("a1")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // queue-A2 is "red"
    Assert.assertEquals(1, capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().size());
    Assert.assertTrue(capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().contains("red"));

    // queue-B is "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-B2 inherits "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b2")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // check capacity of A2
    CSQueue qA2 = capacityScheduler.getQueue("a2");
    Assert.assertEquals(0.7, qA2.getCapacity(), DELTA);
    Assert.assertEquals(0.5, qA2.getQueueCapacities().getCapacity("red"), DELTA);
    Assert.assertEquals(0.07, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.25, qA2.getQueueCapacities().getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.1275, qA2.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(0.3, qA2.getQueueCapacities().getAbsoluteMaximumCapacity("red"), DELTA);

    // check capacity of B3
    CSQueue qB3 = capacityScheduler.getQueue("b3");
    Assert.assertEquals(0.18, qB3.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.125, qB3.getQueueCapacities().getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.35, qB3.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(1, qB3.getQueueCapacities().getAbsoluteMaximumCapacity("red"), DELTA);
  }

  private void checkQueueLabelsWithLeafQueueDisableElasticity
      (CapacityScheduler capacityScheduler) {
    // queue-A is red, blue
    Assert.assertTrue(capacityScheduler.getQueue("a").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-A1 inherits A's configuration
    Assert.assertTrue(capacityScheduler.getQueue("a1")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // queue-A2 is "red"
    Assert.assertEquals(1, capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().size());
    Assert.assertTrue(capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().contains("red"));

    // queue-B is "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-B2 inherits "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b2")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // check capacity of A2
    CSQueue qA2 = capacityScheduler.getQueue("a2");
    Assert.assertEquals(0.4, qA2.getCapacity(), DELTA);
    Assert.assertEquals(0.4, qA2.getQueueCapacities()
        .getCapacity("red"), DELTA);
    Assert.assertEquals(0.2, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.2, qA2.getQueueCapacities()
        .getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.85, qA2.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(0.6, qA2.getQueueCapacities()
        .getAbsoluteMaximumCapacity("red"), DELTA);

    // check disable elasticity at leaf queue level without label
    CSQueue qB2 = capacityScheduler.getQueue("b2");
    Assert.assertEquals(0.4, qB2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.4, qB2.getAbsoluteMaximumCapacity(), DELTA);

    // check disable elasticity at leaf queue level with label
    CSQueue qA1 = capacityScheduler.getQueue("a1");
    Assert.assertEquals(0.3, qA1.getQueueCapacities().
        getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.3, qA1.getQueueCapacities().
        getAbsoluteMaximumCapacity("red"), DELTA);

    CSQueue qB1 = capacityScheduler.getQueue("b1");
    Assert.assertEquals(0.3, qB1.getQueueCapacities()
        .getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.3, qB1.getQueueCapacities()
        .getAbsoluteMaximumCapacity("red"), DELTA);

    // check capacity of B3
    CSQueue qB3 = capacityScheduler.getQueue("b3");
    Assert.assertEquals(0.05, qB3.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.175, qB3.getQueueCapacities()
        .getAbsoluteCapacity("blue"), DELTA);
    Assert.assertEquals(0.25, qB3.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(1, qB3.getQueueCapacities()
        .getAbsoluteMaximumCapacity("blue"), DELTA);
  }

  private void
      checkQueueLabelsInheritConfig(CapacityScheduler capacityScheduler) {
    // queue-A is red, blue
    Assert.assertTrue(capacityScheduler.getQueue("a").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-A1 inherits A's configuration
    Assert.assertTrue(capacityScheduler.getQueue("a1")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // queue-A2 is "red"
    Assert.assertEquals(1, capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().size());
    Assert.assertTrue(capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().contains("red"));

    // queue-B is "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b").getAccessibleNodeLabels()
        .isEmpty());
  }
  
  @Test
  public void testQueueParsingWithLabels() throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("red", "blue"));

    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    checkQueueLabels(capacityScheduler);
    ServiceOperations.stopQuietly(capacityScheduler);
  }

  @Test
  public void testQueueParsingWithLeafQueueDisableElasticity()
      throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity
        (ImmutableSet.of("red", "blue"));

    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabelsAndReleaseCheck(csConf);
    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    checkQueueLabelsWithLeafQueueDisableElasticity(capacityScheduler);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  @Test
  public void testQueueParsingWithLabelsInherit() throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("red", "blue"));

    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabelsInherit(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    checkQueueLabelsInheritConfig(capacityScheduler);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  @Test
  public void testQueueParsingWhenLabelsNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    
    RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
    nodeLabelsManager.init(conf);
    nodeLabelsManager.start();
    
    rmContext.setNodeLabelManager(nodeLabelsManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test
  public void testQueueParsingWhenLabelsInheritedNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabelsInherit(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    
    RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
    nodeLabelsManager.init(conf);
    nodeLabelsManager.start();
    
    rmContext.setNodeLabelManager(nodeLabelsManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test
  public void testSingleLevelQueueParsingWhenLabelsNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithSingleLevel(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    
    RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
    nodeLabelsManager.init(conf);
    nodeLabelsManager.start();
    
    rmContext.setNodeLabelManager(nodeLabelsManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test
  public void testQueueParsingWhenLabelsNotExist() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    
    RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
    nodeLabelsManager.init(conf);
    nodeLabelsManager.start();
    
    rmContext.setNodeLabelManager(nodeLabelsManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test
  public void testQueueParsingWithUnusedLabels() throws IOException {
    final ImmutableSet<String> labels = ImmutableSet.of("red", "blue");
    
    // Initialize a cluster with labels, but doesn't use them, reinitialize
    // shouldn't fail
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(labels);

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    csConf.setAccessibleNodeLabels(CapacitySchedulerConfiguration.ROOT, labels);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(conf);
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, rmContext);
    
    // check root queue's capacity by label -- they should be all zero
    CSQueue root = capacityScheduler.getQueue(CapacitySchedulerConfiguration.ROOT);
    Assert.assertEquals(0, root.getQueueCapacities().getCapacity("red"), DELTA);
    Assert.assertEquals(0, root.getQueueCapacities().getCapacity("blue"), DELTA);

    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);

    CSQueue b1 = capacityScheduler.getQueue("b1");
    Assert.assertEquals(0.2 * 0.5, b1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Parent B has no MAX_CAP", 0.85,
        b1.getAbsoluteMaximumCapacity(), DELTA);

    CSQueue c12 = capacityScheduler.getQueue("c12");
    Assert.assertEquals(0.7 * 0.5 * 0.45, c12.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.7 * 0.55 * 0.7, c12.getAbsoluteMaximumCapacity(),
        DELTA);
    capacityScheduler.stop();
  }
  
  @Test
  public void testQueueParsingShouldTrimSpaces() throws Exception {
    CapacitySchedulerConfiguration csConf = 
      new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithSpacesShouldBeTrimmed(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(conf);
    capacityScheduler.setRMContext(TestUtils.getMockRMContext());
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, TestUtils.getMockRMContext());
    
    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertNotNull(a);
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue c = capacityScheduler.getQueue("c");
    Assert.assertNotNull(c);
    Assert.assertEquals(0.70, c.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.70, c.getAbsoluteMaximumCapacity(), DELTA);
  }
  
  @Test
  public void testNestedQueueParsingShouldTrimSpaces() throws Exception {
    CapacitySchedulerConfiguration csConf = 
      new CapacitySchedulerConfiguration();
    setupNestedQueueConfigurationWithSpacesShouldBeTrimmed(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(conf);
    capacityScheduler.setRMContext(TestUtils.getMockRMContext());
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, TestUtils.getMockRMContext());
    
    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertNotNull(a);
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue c = capacityScheduler.getQueue("c");
    Assert.assertNotNull(c);
    Assert.assertEquals(0.70, c.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.70, c.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue a1 = capacityScheduler.getQueue("a1");
    Assert.assertNotNull(a1);
    Assert.assertEquals(0.10 * 0.6, a1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a1.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue a2 = capacityScheduler.getQueue("a2");
    Assert.assertNotNull(a2);
    Assert.assertEquals(0.10 * 0.4, a2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a2.getAbsoluteMaximumCapacity(), DELTA);
  }
  
  /**
   * Test init a queue configuration, children's capacity for a given label
   * doesn't equals to 100%. This expect IllegalArgumentException thrown.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testQueueParsingFailWhenSumOfChildrenNonLabeledCapacityNot100Percent()
      throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet
        .of("red", "blue"));
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfiguration(csConf);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".c.c2", 5);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  /**
   * Test init a queue configuration, children's capacity for a given label
   * doesn't equals to 100%. This expect IllegalArgumentException thrown.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testQueueParsingFailWhenSumOfChildrenLabeledCapacityNot100Percent()
      throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet
        .of("red", "blue"));
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT + ".b.b3",
        "red", 24);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
  }

  /**
   * Test init a queue configuration, children's capacity for a given label
   * doesn't equals to 100%. This expect IllegalArgumentException thrown.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testQueueParsingWithSumOfChildLabelCapacityNot100PercentWithWildCard()
      throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet
        .of("red", "blue"));
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT + ".b.b3",
        "red", 24);
    csConf.setAccessibleNodeLabels(CapacitySchedulerConfiguration.ROOT,
        ImmutableSet.of(RMNodeLabelsManager.ANY));
    csConf.setAccessibleNodeLabels(CapacitySchedulerConfiguration.ROOT + ".b",
        ImmutableSet.of(RMNodeLabelsManager.ANY));

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  @Test(expected = IOException.class)
  public void testQueueParsingWithMoveQueue()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    csConf.setQueues("root", new String[] { "a" });
    csConf.setQueues("root.a", new String[] { "x", "y" });
    csConf.setCapacity("root.a", 100);
    csConf.setCapacity("root.a.x", 50);
    csConf.setCapacity("root.a.y", 50);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    
    csConf.setQueues("root", new String[] { "a", "x" });
    csConf.setQueues("root.a", new String[] { "y" });
    csConf.setCapacity("root.x", 50);
    csConf.setCapacity("root.a", 50);
    csConf.setCapacity("root.a.y", 100);
    
    capacityScheduler.reinitialize(csConf, rmContext);
  }

  @Test(timeout = 60000, expected = java.lang.IllegalArgumentException.class)
  public void testRMStartWrongNodeCapacity() throws Exception {
    YarnConfiguration config = new YarnConfiguration();
    nodeLabelManager = new NullRMNodeLabelsManager();
    nodeLabelManager.init(config);
    config.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a" });
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "z", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 100);
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("x", "y", "z"));
    conf.setCapacityByLabel(A, "x", 100);
    conf.setCapacityByLabel(A, "y", 100);
    conf.setCapacityByLabel(A, "z", 70);
    MockRM rm = null;
    try {
      rm = new MockRM(conf) {
        @Override
        public RMNodeLabelsManager createNodeLabelManager() {
          return nodeLabelManager;
        }
      };
    } finally {
      IOUtils.closeStream(rm);
    }
  }


  @Test
  public void testQueueOrderingPolicyUpdatedAfterReinitialize()
      throws IOException {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutLabels(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(conf),
            new NMTokenSecretManagerInRM(conf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(conf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(conf);
    capacityScheduler.start();

    // Add a new b4 queue
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT + ".b",
        new String[] { "b1", "b2", "b3", "b4" });
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".b.b4", 0f);
    ParentQueue bQ = (ParentQueue) capacityScheduler.getQueue("b");
    checkEqualsToQueueSet(bQ.getChildQueues(),
        new String[] { "b1", "b2", "b3" });
    capacityScheduler.reinitialize(new YarnConfiguration(csConf), rmContext);

    // Check child queue of b
    checkEqualsToQueueSet(bQ.getChildQueues(),
        new String[] { "b1", "b2", "b3", "b4" });

    PriorityUtilizationQueueOrderingPolicy queueOrderingPolicy =
        (PriorityUtilizationQueueOrderingPolicy) bQ.getQueueOrderingPolicy();
    checkEqualsToQueueSet(queueOrderingPolicy.getQueues(),
        new String[] { "b1", "b2", "b3", "b4" });

    ServiceOperations.stopQuietly(capacityScheduler);
  }

  private void checkEqualsToQueueSet(List<CSQueue> queues, String[] queueNames) {
    Set<String> existedQueues = new HashSet<>();
    for (CSQueue q : queues) {
      existedQueues.add(q.getQueueName());
    }
    for (String q : queueNames) {
      Assert.assertTrue(existedQueues.remove(q));
    }
    Assert.assertTrue(existedQueues.isEmpty());
  }
}
