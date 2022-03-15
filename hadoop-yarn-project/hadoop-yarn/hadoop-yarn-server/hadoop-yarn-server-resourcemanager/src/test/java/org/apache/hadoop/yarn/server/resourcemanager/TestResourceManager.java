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

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigConverterTestCommons;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestResourceManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestResourceManager.class);

  private ResourceManager resourceManager = null;

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private FSConfigConverterTestCommons converterTestCommons;

  @Before
  public void setUp() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    UserGroupInformation.setConfiguration(conf);
    DefaultMetricsSystem.setMiniClusterMode(true);
    resourceManager = new ResourceManager();
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();

    converterTestCommons = new FSConfigConverterTestCommons();
    converterTestCommons.setUp();
  }

  @After
  public void tearDown() throws Exception {
    resourceManager.stop();
    converterTestCommons.tearDown();
  }

  private org.apache.hadoop.yarn.server.resourcemanager.NodeManager
      registerNode(String hostName, int containerManagerPort, int httpPort,
          String rackName, Resource capability, NodeStatus nodeStatus)
          throws IOException, YarnException {
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm = 
        new org.apache.hadoop.yarn.server.resourcemanager.NodeManager(
            hostName, containerManagerPort, httpPort, rackName, capability,
            resourceManager, nodeStatus);
    NodeAddedSchedulerEvent nodeAddEvent1 = 
        new NodeAddedSchedulerEvent(resourceManager.getRMContext()
            .getRMNodes().get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  @Test
  public void testResourceAllocation()
      throws IOException, YarnException, InterruptedException,
      TimeoutException {
    LOG.info("--- START: testResourceAllocation ---");
        
    final int memory = 4 * 1024;
    final int vcores = 4;

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host1 = "host1";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm1 = 
      registerNode(host1, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(memory, vcores), mockNodeStatus);
    
    // Register node2
    String host2 = "host2";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm2 = 
      registerNode(host2, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(memory/2, vcores/2), mockNodeStatus);

    // nodes should be in RUNNING state
    RMNodeImpl node1 = (RMNodeImpl) resourceManager.getRMContext().getRMNodes().get(
        nm1.getNodeId());
    RMNodeImpl node2 = (RMNodeImpl) resourceManager.getRMContext().getRMNodes().get(
        nm2.getNodeId());
    node1.handle(new RMNodeStartedEvent(nm1.getNodeId(), null, null,
        mockNodeStatus));
    node2.handle(new RMNodeStartedEvent(nm2.getNodeId(), null, null,
        mockNodeStatus));

    // Submit an application
    Application application = new Application("user1", resourceManager);
    application.submit();
    
    application.addNodeManager(host1, 1234, nm1);
    application.addNodeManager(host2, 1234, nm2);
    
    // Application resource requirements
    final int memory1 = 1024;
    Resource capability1 = Resources.createResource(memory1, 1);
    Priority priority1 = Priority.newInstance(1);
    application.addResourceRequestSpec(priority1, capability1);
    
    Task t1 = new Task(application, priority1, new String[] {host1, host2});
    application.addTask(t1);
    
    final int memory2 = 2048;
    Resource capability2 = Resources.createResource(memory2, 1);
    Priority priority0 = Priority.newInstance(0); // higher
    application.addResourceRequestSpec(priority0, capability2);
    
    // Send resource requests to the scheduler
    application.schedule();

   // Send a heartbeat to kick the tires on the Scheduler
    nodeUpdate(nm1);
    ((AbstractYarnScheduler)resourceManager.getResourceScheduler()).update();
    
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

    NodeStatus mockNodeStatus = createMockNodeStatus();

    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm1 = 
        registerNode(host1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
        Resources.createResource(memory, 1), mockNodeStatus);
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
    conf.setInt(YarnConfiguration.GLOBAL_RM_AM_MAX_ATTEMPTS, -1);
    try {
      resourceManager = new MockRM(conf);
      fail("Exception is expected because the global max attempts" +
          " is negative.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      if (!e.getMessage().startsWith(
              "Invalid global max attempts configuration")) throw e;
    }
    Configuration yarnConf = new YarnConfiguration();
    yarnConf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, -1);
    try {
      resourceManager = new MockRM(yarnConf);
      fail("Exception is expected because AM max attempts" +
          " is negative.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      if (!e.getMessage().startsWith(
              "Invalid rm am max attempts configuration")) throw e;
    }
  }

  @Test
  public void testNMExpiryAndHeartbeatIntervalsValidation() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 1000);
    conf.setLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 1001);
    try {
      resourceManager = new MockRM(conf);
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
      resourceManager = new ResourceManager() {
        @Override
        protected void doSecureLogin() throws IOException {
          // Skip the login.
        }
      };
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

  /**
   * Test whether ResourceManager passes user-provided conf to
   * UserGroupInformation class. If it reads this (incorrect)
   * AuthenticationMethod enum an exception is thrown.
   */
  @Test
  public void testUserProvidedUGIConf() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid attribute value for "
        + CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION
        + " of DUMMYAUTH");
    Configuration dummyConf = new YarnConfiguration();
    dummyConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "DUMMYAUTH");
    ResourceManager dummyResourceManager = new ResourceManager();
    try {
      dummyResourceManager.init(dummyConf);
    } finally {
      dummyResourceManager.stop();
    }
  }
}
