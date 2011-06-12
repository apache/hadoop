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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;

public class TestCapacityScheduler extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestCapacityScheduler.class);
  
  private ResourceManager resourceManager = null;
  
  @Before
  public void setUp() throws Exception {
    Store store = StoreFactory.getStore(new Configuration());
    resourceManager = new ResourceManager(store);
    CapacitySchedulerConfiguration csConf = 
      new CapacitySchedulerConfiguration();
    csConf.setClass(RMConfig.RESOURCE_SCHEDULER, 
        CapacityScheduler.class, ResourceScheduler.class);
    setupQueueConfiguration(csConf);
    resourceManager.init(csConf);
    resourceManager.getRMContext().getDispatcher().start();
  }

  @After
  public void tearDown() throws Exception {
  }
  
  private org.apache.hadoop.yarn.server.resourcemanager.NodeManager
      registerNode(String hostName, int containerManagerPort, int httpPort,
          String rackName, int memory)
          throws IOException {
    return new org.apache.hadoop.yarn.server.resourcemanager.NodeManager(
        hostName, containerManagerPort, httpPort, rackName, memory,
        resourceManager.getResourceTracker());
  }  

  @Test
  public void testCapacityScheduler() throws Exception {

    LOG.info("--- START: testCapacityScheduler ---");
        
    final int GB = 1024;
    
    // Register node1
    String host_0 = "host_0";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm_0 = 
      registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK, 4 * GB);
    nm_0.heartbeat();
    
    // Register node2
    String host_1 = "host_1";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm_1 = 
      registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK, 2 * GB);
    nm_1.heartbeat();

    // ResourceRequest priorities
    Priority priority_0 = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(0); 
    Priority priority_1 = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(1);
    
    // Submit an application
    Application application_0 = new Application("user_0", "a1", resourceManager);
    application_0.submit();
    
    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(1 * GB);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);
    
    Resource capability_0_1 = Resources.createResource(2 * GB);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 = new Task(application_0, priority_1, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_0);
       
    // Submit another application
    Application application_1 = new Application("user_1", "b2", resourceManager);
    application_1.submit();
    
    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);
    
    Resource capability_1_0 = Resources.createResource(3 * GB);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);
    
    Resource capability_1_1 = Resources.createResource(2 * GB);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 = new Task(application_1, priority_1, 
        new String[] {host_0, host_1});
    application_1.addTask(task_1_0);
        
    // Send resource requests to the scheduler
    application_0.schedule();
    application_1.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Kick!");
    nm_0.heartbeat();             // task_0_0 and task_1_0 allocated, used=4G
    nm_1.heartbeat();             // nothing allocated

    // Get allocations from the scheduler
    application_0.schedule();     // task_0_0 
    checkApplicationResourceUsage(1 * GB, application_0);

    application_1.schedule();     // task_1_0
    checkApplicationResourceUsage(3 * GB, application_1);
    
    nm_0.heartbeat();
    nm_1.heartbeat();
    
    checkNodeResourceUsage(4*GB, nm_0);  // task_0_0 (1G) and task_1_0 (3G)
    checkNodeResourceUsage(0*GB, nm_1);  // no tasks, 2G available

    LOG.info("Adding new tasks...");
    
    Task task_1_1 = new Task(application_1, priority_0, 
        new String[] {NodeManager.ANY});
    application_1.addTask(task_1_1);

    application_1.schedule();

    Task task_0_1 = new Task(application_0, priority_0, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_1);

    application_0.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Sending hb from " + nm_0.getHostName());
    nm_0.heartbeat();                   // nothing new, used=4G
    
    LOG.info("Sending hb from " + nm_1.getHostName());
    nm_1.heartbeat();                   // task_0_3, used=2G
    
    // Get allocations from the scheduler
    LOG.info("Trying to allocate...");
    application_0.schedule();
    checkApplicationResourceUsage(1 * GB, application_0);

    application_1.schedule();
    checkApplicationResourceUsage(5 * GB, application_1);
    
    nm_0.heartbeat();
    nm_1.heartbeat();
    checkNodeResourceUsage(4*GB, nm_0);
    checkNodeResourceUsage(2*GB, nm_1);

    LOG.info("--- END: testCapacityScheduler ---");
  }
  
  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacityScheduler.ROOT, new String[] {"a", "b"});
    conf.setCapacity(CapacityScheduler.ROOT, 100);
    
    final String A = CapacityScheduler.ROOT + ".a";
    conf.setCapacity(A, 10);
    
    final String B = CapacityScheduler.ROOT + ".b";
    conf.setCapacity(B, 90);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, 70);
    conf.setUserLimitFactor(A2, 100.0f);
    
    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, 50);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, 30);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, 20);
    conf.setUserLimitFactor(B3, 100.0f);

    LOG.info("Setup top-level queues a and b");
  }
  
  private void checkApplicationResourceUsage(int expected, 
      Application application) {
    Assert.assertEquals(expected, application.getUsedResources().getMemory());
  }
  
  private void checkNodeResourceUsage(int expected,
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager node) {
    Assert.assertEquals(expected, node.getUsed().getMemory());
    node.checkResourceUsage();
  }

}
