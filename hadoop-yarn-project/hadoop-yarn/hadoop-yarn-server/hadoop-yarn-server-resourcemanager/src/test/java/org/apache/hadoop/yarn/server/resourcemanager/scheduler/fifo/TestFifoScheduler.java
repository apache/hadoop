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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFifoScheduler {
  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);
  
  private ResourceManager resourceManager = null;
  
  @Before
  public void setUp() throws Exception {
    resourceManager = new ResourceManager();
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, 
        FifoScheduler.class, ResourceScheduler.class);
    resourceManager.init(conf);
  }

  @After
  public void tearDown() throws Exception {
    resourceManager.stop();
  }
  
  private org.apache.hadoop.yarn.server.resourcemanager.NodeManager
      registerNode(String hostName, int containerManagerPort, int nmHttpPort,
          String rackName, Resource capability) throws IOException {
    return new org.apache.hadoop.yarn.server.resourcemanager.NodeManager(
        hostName, containerManagerPort, nmHttpPort, rackName, capability,
        resourceManager.getResourceTrackerService(), resourceManager
            .getRMContext());
  }
  
  @Test
  public void testFifoSchedulerCapacityWhenNoNMs() {
    FifoScheduler scheduler = new FifoScheduler();
    QueueInfo queueInfo = scheduler.getQueueInfo(null, false, false);
    Assert.assertEquals(0.0f, queueInfo.getCurrentCapacity());
  }
  
  @Test
  public void testAppAttemptMetrics() throws Exception {
    AsyncDispatcher dispatcher = new InlineDispatcher();
    RMContext rmContext = new RMContextImpl(dispatcher, null,
        null, null, null, null, null, null);

    FifoScheduler schedular = new FifoScheduler();
    schedular.reinitialize(new Configuration(), rmContext);

    ApplicationId appId = BuilderUtils.newApplicationId(200, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);

    SchedulerEvent event = new AppAddedSchedulerEvent(appAttemptId, "queue",
        "user");
    schedular.handle(event);

    appAttemptId = BuilderUtils.newApplicationAttemptId(appId, 2);

    event = new AppAddedSchedulerEvent(appAttemptId, "queue", "user");
    schedular.handle(event);

    QueueMetrics metrics = schedular.getRootQueueMetrics();
    Assert.assertEquals(1, metrics.getAppsSubmitted());
  }

//  @Test
  public void testFifoScheduler() throws Exception {

    LOG.info("--- START: testFifoScheduler ---");
        
    final int GB = 1024;
    
    // Register node1
    String host_0 = "host_0";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm_0 = 
      registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(4 * GB, 1));
    nm_0.heartbeat();
    
    // Register node2
    String host_1 = "host_1";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm_1 = 
      registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(2 * GB, 1));
    nm_1.heartbeat();

    // ResourceRequest priorities
    Priority priority_0 = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(0); 
    Priority priority_1 = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(1);
    
    // Submit an application
    Application application_0 = new Application("user_0", resourceManager);
    application_0.submit();
    
    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(GB);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);
    
    Resource capability_0_1 = Resources.createResource(2 * GB);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 = new Task(application_0, priority_1, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_0);
       
    // Submit another application
    Application application_1 = new Application("user_1", resourceManager);
    application_1.submit();
    
    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);
    
    Resource capability_1_0 = Resources.createResource(3 * GB);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);
    
    Resource capability_1_1 = Resources.createResource(4 * GB);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 = new Task(application_1, priority_1, 
        new String[] {host_0, host_1});
    application_1.addTask(task_1_0);
        
    // Send resource requests to the scheduler
    LOG.info("Send resource requests to the scheduler");
    application_0.schedule();
    application_1.schedule();
    
    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Send a heartbeat to kick the tires on the Scheduler... " +
    		"nm0 -> task_0_0 and task_1_0 allocated, used=4G " +
    		"nm1 -> nothing allocated");
    nm_0.heartbeat();             // task_0_0 and task_1_0 allocated, used=4G
    nm_1.heartbeat();             // nothing allocated
    
    // Get allocations from the scheduler
    application_0.schedule();     // task_0_0 
    checkApplicationResourceUsage(GB, application_0);

    application_1.schedule();     // task_1_0
    checkApplicationResourceUsage(3 * GB, application_1);
    
    nm_0.heartbeat();
    nm_1.heartbeat();
    
    checkNodeResourceUsage(4*GB, nm_0);  // task_0_0 (1G) and task_1_0 (3G)
    checkNodeResourceUsage(0*GB, nm_1);  // no tasks, 2G available
    
    LOG.info("Adding new tasks...");
    
    Task task_1_1 = new Task(application_1, priority_1, 
        new String[] {RMNode.ANY});
    application_1.addTask(task_1_1);

    Task task_1_2 = new Task(application_1, priority_1, 
        new String[] {RMNode.ANY});
    application_1.addTask(task_1_2);

    Task task_1_3 = new Task(application_1, priority_0, 
        new String[] {RMNode.ANY});
    application_1.addTask(task_1_3);
    
    application_1.schedule();
    
    Task task_0_1 = new Task(application_0, priority_1, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_1);

    Task task_0_2 = new Task(application_0, priority_1, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_2);
    
    Task task_0_3 = new Task(application_0, priority_0, 
        new String[] {RMNode.ANY});
    application_0.addTask(task_0_3);

    application_0.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Sending hb from " + nm_0.getHostName());
    nm_0.heartbeat();                   // nothing new, used=4G
    
    LOG.info("Sending hb from " + nm_1.getHostName());
    nm_1.heartbeat();                   // task_0_3, used=2G
    
    // Get allocations from the scheduler
    LOG.info("Trying to allocate...");
    application_0.schedule();
    checkApplicationResourceUsage(3 * GB, application_0);
    application_1.schedule();
    checkApplicationResourceUsage(3 * GB, application_1);
    nm_0.heartbeat();
    nm_1.heartbeat();
    checkNodeResourceUsage(4*GB, nm_0);
    checkNodeResourceUsage(2*GB, nm_1);
    
    // Complete tasks
    LOG.info("Finishing up task_0_0");
    application_0.finishTask(task_0_0); // Now task_0_1
    application_0.schedule();
    application_1.schedule();
    nm_0.heartbeat();
    nm_1.heartbeat();
    checkApplicationResourceUsage(3 * GB, application_0);
    checkApplicationResourceUsage(3 * GB, application_1);
    checkNodeResourceUsage(4*GB, nm_0);
    checkNodeResourceUsage(2*GB, nm_1);

    LOG.info("Finishing up task_1_0");
    application_1.finishTask(task_1_0);  // Now task_0_2
    application_0.schedule(); // final overcommit for app0 caused here
    application_1.schedule();
    nm_0.heartbeat(); // final overcommit for app0 occurs here
    nm_1.heartbeat();
    checkApplicationResourceUsage(4 * GB, application_0);
    checkApplicationResourceUsage(0 * GB, application_1);
    //checkNodeResourceUsage(1*GB, nm_0);  // final over-commit -> rm.node->1G, test.node=2G
    checkNodeResourceUsage(2*GB, nm_1);

    LOG.info("Finishing up task_0_3");
    application_0.finishTask(task_0_3); // No more
    application_0.schedule();
    application_1.schedule();
    nm_0.heartbeat();
    nm_1.heartbeat();
    checkApplicationResourceUsage(2 * GB, application_0);
    checkApplicationResourceUsage(0 * GB, application_1);
    //checkNodeResourceUsage(2*GB, nm_0);  // final over-commit, rm.node->1G, test.node->2G
    checkNodeResourceUsage(0*GB, nm_1);
    
    LOG.info("Finishing up task_0_1");
    application_0.finishTask(task_0_1);
    application_0.schedule();
    application_1.schedule();
    nm_0.heartbeat();
    nm_1.heartbeat();
    checkApplicationResourceUsage(1 * GB, application_0);
    checkApplicationResourceUsage(0 * GB, application_1);
    
    LOG.info("Finishing up task_0_2");
    application_0.finishTask(task_0_2); // now task_1_3 can go!
    application_0.schedule();
    application_1.schedule();
    nm_0.heartbeat();
    nm_1.heartbeat();
    checkApplicationResourceUsage(0 * GB, application_0);
    checkApplicationResourceUsage(4 * GB, application_1);
    
    LOG.info("Finishing up task_1_3");
    application_1.finishTask(task_1_3); // now task_1_1
    application_0.schedule();
    application_1.schedule();
    nm_0.heartbeat();
    nm_1.heartbeat();
    checkApplicationResourceUsage(0 * GB, application_0);
    checkApplicationResourceUsage(3 * GB, application_1);
    
    LOG.info("Finishing up task_1_1");
    application_1.finishTask(task_1_1);
    application_0.schedule();
    application_1.schedule();
    nm_0.heartbeat();
    nm_1.heartbeat();
    checkApplicationResourceUsage(0 * GB, application_0);
    checkApplicationResourceUsage(3 * GB, application_1);
    
    LOG.info("--- END: testFifoScheduler ---");
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

  public static void main(String[] arg) throws Exception {
    TestFifoScheduler t = new TestFifoScheduler();
    t.setUp();
    t.testFifoScheduler();
    t.tearDown();
  }
}
