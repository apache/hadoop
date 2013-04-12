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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestResourceManager {
  private static final Log LOG = LogFactory.getLog(TestResourceManager.class);
  
  private ResourceManager resourceManager = null;
  
  @Before
  public void setUp() throws Exception {
    Configuration conf = new YarnConfiguration();
    resourceManager = new ResourceManager();
    resourceManager.init(conf);
  }

  @After
  public void tearDown() throws Exception {
  }

  private org.apache.hadoop.yarn.server.resourcemanager.NodeManager
      registerNode(String hostName, int containerManagerPort, int httpPort,
          String rackName, Resource capability) throws IOException {
    return new org.apache.hadoop.yarn.server.resourcemanager.NodeManager(
        hostName, containerManagerPort, httpPort, rackName, capability,
        resourceManager.getResourceTrackerService(), resourceManager
            .getRMContext());
  }

//  @Test
  public void testResourceAllocation() throws IOException {
    LOG.info("--- START: testResourceAllocation ---");
        
    final int memory = 4 * 1024;
    
    // Register node1
    String host1 = "host1";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm1 = 
      registerNode(host1, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(memory, 1));
    nm1.heartbeat();
    
    // Register node2
    String host2 = "host2";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm2 = 
      registerNode(host2, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(memory/2, 1));
    nm2.heartbeat();

    // Submit an application
    Application application = new Application("user1", resourceManager);
    application.submit();
    
    application.addNodeManager(host1, 1234, nm1);
    application.addNodeManager(host2, 1234, nm2);
    
    // Application resource requirements
    final int memory1 = 1024;
    Resource capability1 = Resources.createResource(memory1, 1);
    Priority priority1 = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(1);
    application.addResourceRequestSpec(priority1, capability1);
    
    Task t1 = new Task(application, priority1, new String[] {host1, host2});
    application.addTask(t1);
        
    final int memory2 = 2048;
    Resource capability2 = Resources.createResource(memory2, 1);
    Priority priority0 = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(0); // higher
    application.addResourceRequestSpec(priority0, capability2);
    
    // Send resource requests to the scheduler
    application.schedule();
    
    // Send a heartbeat to kick the tires on the Scheduler
    nm1.heartbeat();
    
    // Get allocations from the scheduler
    application.schedule();
    
    nm1.heartbeat();
    checkResourceUsage(nm1, nm2);
    
    LOG.info("Adding new tasks...");
    
    Task t2 = new Task(application, priority1, new String[] {host1, host2});
    application.addTask(t2);

    Task t3 = new Task(application, priority0, new String[] {RMNode.ANY});
    application.addTask(t3);

    // Send resource requests to the scheduler
    application.schedule();
    checkResourceUsage(nm1, nm2);
    
    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Sending hb from host2");
    nm2.heartbeat();
    
    LOG.info("Sending hb from host1");
    nm1.heartbeat();
    
    // Get allocations from the scheduler
    LOG.info("Trying to allocate...");
    application.schedule();

    nm1.heartbeat();
    nm2.heartbeat();
    checkResourceUsage(nm1, nm2);
    
    // Complete tasks
    LOG.info("Finishing up tasks...");
    application.finishTask(t1);
    application.finishTask(t2);
    application.finishTask(t3);
    
    // Send heartbeat
    nm1.heartbeat();
    nm2.heartbeat();
    checkResourceUsage(nm1, nm2);
    
    LOG.info("--- END: testResourceAllocation ---");
  }
  
  @Test
  public void testNodeHealthReportIsNotNull() throws Exception{
    String host1 = "host1";
    final int memory = 4 * 1024;
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm1 = 
      registerNode(host1, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(memory, 1));
    nm1.heartbeat();
    nm1.heartbeat();
    Collection<RMNode> values = resourceManager.getRMContext().getRMNodes().values();
    for (RMNode ni : values)
    {
      NodeHealthStatus nodeHealthStatus = ni.getNodeHealthStatus();
      String healthReport = nodeHealthStatus.getHealthReport();
      assertNotNull(healthReport);
    }
  }

  private void checkResourceUsage(
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager... nodes ) {
    for (org.apache.hadoop.yarn.server.resourcemanager.NodeManager nodeManager : nodes) {
      nodeManager.checkResourceUsage();
    }
  }

}
