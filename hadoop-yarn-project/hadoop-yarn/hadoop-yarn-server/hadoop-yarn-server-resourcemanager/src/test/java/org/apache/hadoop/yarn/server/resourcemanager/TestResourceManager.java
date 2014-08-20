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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
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
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
  }

  @After
  public void tearDown() throws Exception {
  }

  private org.apache.hadoop.yarn.server.resourcemanager.NodeManager
      registerNode(String hostName, int containerManagerPort, int httpPort,
          String rackName, Resource capability) throws IOException,
          YarnException {
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm = 
        new org.apache.hadoop.yarn.server.resourcemanager.NodeManager(
            hostName, containerManagerPort, httpPort, rackName, capability,
            resourceManager);
    NodeAddedSchedulerEvent nodeAddEvent1 = 
        new NodeAddedSchedulerEvent(resourceManager.getRMContext()
            .getRMNodes().get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  @Test
  public void testResourceAllocation() throws IOException,
      YarnException, InterruptedException {
    LOG.info("--- START: testResourceAllocation ---");
        
    final int memory = 4 * 1024;
    final int vcores = 4;
    
    // Register node1
    String host1 = "host1";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm1 = 
      registerNode(host1, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(memory, vcores));
    
    // Register node2
    String host2 = "host2";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm2 = 
      registerNode(host2, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(memory/2, vcores/2));

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
    nodeUpdate(nm1);
    
    // Get allocations from the scheduler
    application.schedule();
    
    checkResourceUsage(nm1, nm2);
    
    LOG.info("Adding new tasks...");
    
    Task t2 = new Task(application, priority1, new String[] {host1, host2});
    application.addTask(t2);

    Task t3 = new Task(application, priority0, new String[] {ResourceRequest.ANY});
    application.addTask(t3);

    // Send resource requests to the scheduler
    application.schedule();
    checkResourceUsage(nm1, nm2);
    
    // Send heartbeats to kick the tires on the Scheduler
    nodeUpdate(nm2);
    nodeUpdate(nm2);
    nodeUpdate(nm1);
    nodeUpdate(nm1);
    
    // Get allocations from the scheduler
    LOG.info("Trying to allocate...");
    application.schedule();

    checkResourceUsage(nm1, nm2);
    
    // Complete tasks
    LOG.info("Finishing up tasks...");
    application.finishTask(t1);
    application.finishTask(t2);
    application.finishTask(t3);
    
    // Notify scheduler application is finished.
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(
          application.getApplicationAttemptId(), RMAppAttemptState.FINISHED, false);
    resourceManager.getResourceScheduler().handle(appRemovedEvent1);
    
    checkResourceUsage(nm1, nm2);
    
    LOG.info("--- END: testResourceAllocation ---");
  }

  private void nodeUpdate(
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm1) {
    RMNode node = resourceManager.getRMContext().getRMNodes().get(nm1.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);
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
    for (RMNode ni : values) {
      assertNotNull(ni.getHealthReport());
    }
  }

  private void checkResourceUsage(
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager... nodes ) {
    for (org.apache.hadoop.yarn.server.resourcemanager.NodeManager nodeManager : nodes) {
      nodeManager.checkResourceUsage();
    }
  }

  @Test (timeout = 30000)
  public void testResourceManagerInitConfigValidation() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, -1);
    resourceManager = new ResourceManager();
    try {
      resourceManager.init(conf);
      fail("Exception is expected because the global max attempts" +
          " is negative.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      if (!e.getMessage().startsWith(
              "Invalid global max attempts configuration")) throw e;
    }
  }

  @Test
  public void testNMExpiryAndHeartbeatIntervalsValidation() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 1000);
    conf.setLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 1001);
    resourceManager = new ResourceManager();;
    try {
      resourceManager.init(conf);
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      if (!e.getMessage().startsWith("Nodemanager expiry interval should be no"
          + " less than heartbeat interval")) {
        throw e;
      }
    }
  }

  @Test(timeout = 50000)
  public void testFilterOverrides() throws Exception {
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    String[] filterInitializers =
        {
            AuthenticationFilterInitializer.class.getName(),
            RMAuthenticationFilterInitializer.class.getName(),
            AuthenticationFilterInitializer.class.getName() + ","
                + RMAuthenticationFilterInitializer.class.getName(),
            AuthenticationFilterInitializer.class.getName() + ", "
                + RMAuthenticationFilterInitializer.class.getName(),
            AuthenticationFilterInitializer.class.getName() + ", "
                + this.getClass().getName() };
    for (String filterInitializer : filterInitializers) {
      resourceManager = new ResourceManager();
      Configuration conf = new YarnConfiguration();
      conf.set(filterInitializerConfKey, filterInitializer);
      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("hadoop.http.authentication.type", "kerberos");
      try {
        try {
          UserGroupInformation.setConfiguration(conf);
        } catch (Exception e) {
          // ignore we just care about getting true for
          // isSecurityEnabled()
          LOG.info("Got expected exception");
        }
        resourceManager.init(conf);
        resourceManager.startWepApp();
      } catch (RuntimeException e) {
        // Exceptions are expected because we didn't setup everything
        // just want to test filter settings
        String tmp = resourceManager.getConfig().get(filterInitializerConfKey);
        if (filterInitializer.contains(this.getClass().getName())) {
          Assert.assertEquals(RMAuthenticationFilterInitializer.class.getName()
              + "," + this.getClass().getName(), tmp);
        } else {
          Assert.assertEquals(
            RMAuthenticationFilterInitializer.class.getName(), tmp);
        }
        resourceManager.stop();
      }
    }

    // simple mode overrides
    String[] simpleFilterInitializers =
        { "", StaticUserWebFilter.class.getName() };
    for (String filterInitializer : simpleFilterInitializers) {
      resourceManager = new ResourceManager();
      Configuration conf = new YarnConfiguration();
      conf.set(filterInitializerConfKey, filterInitializer);
      try {
        UserGroupInformation.setConfiguration(conf);
        resourceManager.init(conf);
        resourceManager.startWepApp();
      } catch (RuntimeException e) {
        // Exceptions are expected because we didn't setup everything
        // just want to test filter settings
        String tmp = resourceManager.getConfig().get(filterInitializerConfKey);
        if (filterInitializer.equals(StaticUserWebFilter.class.getName())) {
          Assert.assertEquals(RMAuthenticationFilterInitializer.class.getName()
              + "," + StaticUserWebFilter.class.getName(), tmp);
        } else {
          Assert.assertEquals(
            RMAuthenticationFilterInitializer.class.getName(), tmp);
        }
        resourceManager.stop();
      }
    }
  }

}
