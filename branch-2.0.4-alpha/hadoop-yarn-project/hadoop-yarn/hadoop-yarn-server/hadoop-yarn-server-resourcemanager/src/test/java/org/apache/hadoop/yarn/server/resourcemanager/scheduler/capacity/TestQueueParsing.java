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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.junit.Test;

public class TestQueueParsing {

  private static final Log LOG = LogFactory.getLog(TestQueueParsing.class);
  
  private static final double DELTA = 0.000001;
  
  @Test
  public void testQueueParsing() throws Exception {
    CapacitySchedulerConfiguration csConf = 
      new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(conf);
    capacityScheduler.reinitialize(conf, new RMContextImpl(null, null,
      null, null, null, null, new RMContainerTokenSecretManager(conf),
      new ClientToAMTokenSecretManagerInRM()));
    
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
    capacityScheduler.reinitialize(conf, null);
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
  }
  
}
