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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFifoScheduler {
  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);
  private final int GB = 1024;

  private ResourceManager resourceManager = null;
  private static Configuration conf;
  private static final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  private final static ContainerUpdates NULL_UPDATE_REQUESTS =
      new ContainerUpdates();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        FifoScheduler.class, ResourceScheduler.class);
    resourceManager = new MockRM(conf);
  }

  @After
  public void tearDown() throws Exception {
    resourceManager.stop();
  }
  
  private org.apache.hadoop.yarn.server.resourcemanager.NodeManager
      registerNode(String hostName, int containerManagerPort, int nmHttpPort,
          String rackName, Resource capability) throws IOException,
          YarnException {
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm =
        new org.apache.hadoop.yarn.server.resourcemanager.NodeManager(hostName,
            containerManagerPort, nmHttpPort, rackName, capability,
            resourceManager);
    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(resourceManager.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }
  
  private ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
    ApplicationId appIdImpl = ApplicationId.newInstance(0, appId);
    ApplicationAttemptId attId =
        ApplicationAttemptId.newInstance(appIdImpl, attemptId);
    return attId;
  }

  private ResourceRequest createResourceRequest(int memory, String host,
      int priority, int numContainers) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setCapability(Resources.createResource(memory));
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    return request;
  }

  @Test(timeout=5000)
  public void testFifoSchedulerCapacityWhenNoNMs() {
    FifoScheduler scheduler = new FifoScheduler();
    QueueInfo queueInfo = scheduler.getQueueInfo(null, false, false);
    Assert.assertEquals(0.0f, queueInfo.getCurrentCapacity(), 0.0f);
  }
  
  @Test(timeout=5000)
  public void testAppAttemptMetrics() throws Exception {
    AsyncDispatcher dispatcher = new InlineDispatcher();
    
    FifoScheduler scheduler = new FifoScheduler();
    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    RMContext rmContext = new RMContextImpl(dispatcher, null,
        null, null, null, null, null, null, null, scheduler);
    ((RMContextImpl) rmContext).setSystemMetricsPublisher(
        mock(SystemMetricsPublisher.class));

    Configuration conf = new Configuration();
    ((RMContextImpl) rmContext).setScheduler(scheduler);
    scheduler.setRMContext(rmContext);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, rmContext);
    QueueMetrics metrics = scheduler.getRootQueueMetrics();
    int beforeAppsSubmitted = metrics.getAppsSubmitted();

    ApplicationId appId = BuilderUtils.newApplicationId(200, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);

    SchedulerEvent appEvent = new AppAddedSchedulerEvent(appId, "queue", "user");
    scheduler.handle(appEvent);
    SchedulerEvent attemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    scheduler.handle(attemptEvent);

    appAttemptId = BuilderUtils.newApplicationAttemptId(appId, 2);
    SchedulerEvent attemptEvent2 =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    scheduler.handle(attemptEvent2);

    int afterAppsSubmitted = metrics.getAppsSubmitted();
    Assert.assertEquals(1, afterAppsSubmitted - beforeAppsSubmitted);
    scheduler.stop();
  }

  @Test(timeout=2000)
  public void testNodeLocalAssignment() throws Exception {
    AsyncDispatcher dispatcher = new InlineDispatcher();
    Configuration conf = new Configuration();
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.rollMasterKey();
    NMTokenSecretManagerInRM nmTokenSecretManager =
        new NMTokenSecretManagerInRM(conf);
    nmTokenSecretManager.rollMasterKey();
    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    
    FifoScheduler scheduler = new FifoScheduler();
    RMContext rmContext = new RMContextImpl(dispatcher, null, null, null, null,
        null, containerTokenSecretManager, nmTokenSecretManager, null, scheduler);
    AllocationTagsManager ptm = mock(AllocationTagsManager.class);
    rmContext.setAllocationTagsManager(ptm);
    rmContext.setSystemMetricsPublisher(mock(SystemMetricsPublisher.class));
    rmContext.setRMApplicationHistoryWriter(
        mock(RMApplicationHistoryWriter.class));
    ((RMContextImpl) rmContext).setYarnConfiguration(new YarnConfiguration());

    scheduler.setRMContext(rmContext);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(new Configuration(), rmContext);

    RMNode node0 = MockNodes.newNodeInfo(1,
        Resources.createResource(1024 * 64), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node0);
    scheduler.handle(nodeEvent1);

    int _appId = 1;
    int _appAttemptId = 1;
    ApplicationAttemptId appAttemptId = createAppAttemptId(_appId,
        _appAttemptId);

    createMockRMApp(appAttemptId, rmContext);

    AppAddedSchedulerEvent appEvent =
        new AppAddedSchedulerEvent(appAttemptId.getApplicationId(), "queue1",
            "user1");
    scheduler.handle(appEvent);
    AppAttemptAddedSchedulerEvent attemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    scheduler.handle(attemptEvent);

    int memory = 64;
    int nConts = 3;
    int priority = 20;

    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest nodeLocal = createResourceRequest(memory,
        node0.getHostName(), priority, nConts);
    ResourceRequest rackLocal = createResourceRequest(memory,
        node0.getRackName(), priority, nConts);
    ResourceRequest any = createResourceRequest(memory, ResourceRequest.ANY, priority,
        nConts);
    ask.add(nodeLocal);
    ask.add(rackLocal);
    ask.add(any);
    scheduler.allocate(appAttemptId, ask, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);

    NodeUpdateSchedulerEvent node0Update = new NodeUpdateSchedulerEvent(node0);

    // Before the node update event, there are 3 local requests outstanding
    Assert.assertEquals(3, nodeLocal.getNumContainers());

    scheduler.handle(node0Update);

    // After the node update event, check that there are no more local requests
    // outstanding
    Assert.assertEquals(0, nodeLocal.getNumContainers());
    //Also check that the containers were scheduled
    SchedulerAppReport info = scheduler.getSchedulerAppInfo(appAttemptId);
    Assert.assertEquals(3, info.getLiveContainers().size());
    scheduler.stop();
  }
  
  @Test(timeout=2000)
  public void testUpdateResourceOnNode() throws Exception {
    AsyncDispatcher dispatcher = new InlineDispatcher();
    Configuration conf = new Configuration();
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.rollMasterKey();
    NMTokenSecretManagerInRM nmTokenSecretManager =
        new NMTokenSecretManagerInRM(conf);
    nmTokenSecretManager.rollMasterKey();
    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    
    FifoScheduler scheduler = new FifoScheduler();
    RMContext rmContext = new RMContextImpl(dispatcher, null, null, null, null,
        null, containerTokenSecretManager, nmTokenSecretManager, null, scheduler);
    AllocationTagsManager ptm = mock(AllocationTagsManager.class);
    rmContext.setSystemMetricsPublisher(mock(SystemMetricsPublisher.class));
    rmContext.setRMApplicationHistoryWriter(mock(RMApplicationHistoryWriter.class));
    ((RMContextImpl) rmContext).setYarnConfiguration(new YarnConfiguration());
    NullRMNodeLabelsManager nlm = new NullRMNodeLabelsManager();
    nlm.init(new Configuration());
    rmContext.setNodeLabelManager(nlm);
    rmContext.setAllocationTagsManager(ptm);

    scheduler.setRMContext(rmContext);
    ((RMContextImpl) rmContext).setScheduler(scheduler);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(new Configuration(), rmContext);
    RMNode node0 = MockNodes.newNodeInfo(1,
        Resources.createResource(2048, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node0);
    scheduler.handle(nodeEvent1);
    
    assertEquals(scheduler.getNumClusterNodes(), 1);
    
    Resource newResource = Resources.createResource(1024, 4);
    
    NodeResourceUpdateSchedulerEvent node0ResourceUpdate = new 
        NodeResourceUpdateSchedulerEvent(node0, ResourceOption.newInstance(
            newResource, ResourceOption.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT));
    scheduler.handle(node0ResourceUpdate);
    
    // SchedulerNode's total resource and available resource are changed.
    assertEquals(1024, scheduler.getNodeTracker().getNode(node0.getNodeID())
        .getTotalResource().getMemorySize());
    assertEquals(1024, scheduler.getNodeTracker().getNode(node0.getNodeID()).
        getUnallocatedResource().getMemorySize(), 1024);
    QueueInfo queueInfo = scheduler.getQueueInfo(null, false, false);
    Assert.assertEquals(0.0f, queueInfo.getCurrentCapacity(), 0.0f);
    
    int _appId = 1;
    int _appAttemptId = 1;
    ApplicationAttemptId appAttemptId = createAppAttemptId(_appId,
        _appAttemptId);
    createMockRMApp(appAttemptId, rmContext);

    AppAddedSchedulerEvent appEvent =
        new AppAddedSchedulerEvent(appAttemptId.getApplicationId(), "queue1",
          "user1");
    scheduler.handle(appEvent);
    AppAttemptAddedSchedulerEvent attemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    scheduler.handle(attemptEvent);

    int memory = 1024;
    int priority = 1;

    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest nodeLocal = createResourceRequest(memory,
        node0.getHostName(), priority, 1);
    ResourceRequest rackLocal = createResourceRequest(memory,
        node0.getRackName(), priority, 1);
    ResourceRequest any = createResourceRequest(memory, ResourceRequest.ANY, priority,
        1);
    ask.add(nodeLocal);
    ask.add(rackLocal);
    ask.add(any);
    scheduler.allocate(appAttemptId, ask, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);

    // Before the node update event, there are one local request
    Assert.assertEquals(1, nodeLocal.getNumContainers());

    NodeUpdateSchedulerEvent node0Update = new NodeUpdateSchedulerEvent(node0);
    // Now schedule.
    scheduler.handle(node0Update);

    // After the node update event, check no local request
    Assert.assertEquals(0, nodeLocal.getNumContainers());
    // Also check that one container was scheduled
    SchedulerAppReport info = scheduler.getSchedulerAppInfo(appAttemptId);
    Assert.assertEquals(1, info.getLiveContainers().size());
    // And check the default Queue now is full.
    queueInfo = scheduler.getQueueInfo(null, false, false);
    Assert.assertEquals(1.0f, queueInfo.getCurrentCapacity(), 0.0f);
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
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

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
        new String[] {ResourceRequest.ANY});
    application_1.addTask(task_1_1);

    Task task_1_2 = new Task(application_1, priority_1, 
        new String[] {ResourceRequest.ANY});
    application_1.addTask(task_1_2);

    Task task_1_3 = new Task(application_1, priority_0, 
        new String[] {ResourceRequest.ANY});
    application_1.addTask(task_1_3);
    
    application_1.schedule();
    
    Task task_0_1 = new Task(application_0, priority_1, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_1);

    Task task_0_2 = new Task(application_0, priority_1, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_2);
    
    Task task_0_3 = new Task(application_0, priority_0, 
        new String[] {ResourceRequest.ANY});
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

  @Test
  public void testGetAppsInQueue() throws Exception {
    Application application_0 = new Application("user_0", resourceManager);
    application_0.submit();
    
    Application application_1 = new Application("user_0", resourceManager);
    application_1.submit();
    
    ResourceScheduler scheduler = resourceManager.getResourceScheduler();
    
    List<ApplicationAttemptId> appsInDefault = scheduler.getAppsInQueue("default");
    assertTrue(appsInDefault.contains(application_0.getApplicationAttemptId()));
    assertTrue(appsInDefault.contains(application_1.getApplicationAttemptId()));
    assertEquals(2, appsInDefault.size());
    
    Assert.assertNull(scheduler.getAppsInQueue("someotherqueue"));
  }

  @Test
  public void testAddAndRemoveAppFromFiFoScheduler() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
      ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    @SuppressWarnings("unchecked")
    AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> fs =
        (AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) rm
          .getResourceScheduler();
    TestSchedulerUtils.verifyAppAddedAndRemovedFromScheduler(
      fs.getSchedulerApplications(), fs, "queue");
  }

  @Test(timeout = 30000)
  public void testConfValidation() throws Exception {
    FifoScheduler scheduler = new FifoScheduler();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2048);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    try {
      scheduler.serviceInit(conf);
      fail("Exception is expected because the min memory allocation is"
          + " larger than the max memory allocation.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      assertTrue("The thrown exception is not the expected one.", e
          .getMessage().startsWith("Invalid resource scheduler memory"));
    }
  }

  @Test(timeout = 60000)
  public void testAllocateContainerOnNodeWithoutOffSwitchSpecified()
      throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);

    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    RMApp app1 = rm.submitApp(2048);
    // kick the scheduling, 2 GB given to AM1, remaining 4GB on nm1
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // add request for containers
    List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
    requests.add(am1.createResourceReq("127.0.0.1", 1 * GB, 1, 1));
    requests.add(am1.createResourceReq("/default-rack", 1 * GB, 1, 1));
    am1.allocate(requests, null); // send the request

    try {
      // kick the schedule
      nm1.nodeHeartbeat(true);
    } catch (NullPointerException e) {
      Assert.fail("NPE when allocating container on node but "
          + "forget to set off-switch request should be handled");
    }
    rm.stop();
  }

  @Test(timeout = 60000)
  public void testFifoScheduling() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);
    MockNM nm2 = rm.registerNode("127.0.0.2:5678", 4 * GB);

    RMApp app1 = rm.submitApp(2048);
    // kick the scheduling, 2 GB given to AM1, remaining 4GB on nm1
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemorySize());

    RMApp app2 = rm.submitApp(2048);
    // kick the scheduling, 2GB given to AM, remaining 2 GB on nm2
    nm2.nodeHeartbeat(true);
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();
    SchedulerNodeReport report_nm2 =
        rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(2 * GB, report_nm2.getUsedResource().getMemorySize());

    // add request for containers
    am1.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request
    // add request for containers
    am2.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, 3 * GB, 0, 1);
    AllocateResponse alloc2Response = am2.schedule(); // send the request

    // kick the scheduler, 1 GB and 3 GB given to AM1 and AM2, remaining 0
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(1000);
      alloc1Response = am1.schedule();
    }
    while (alloc2Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 2...");
      Thread.sleep(1000);
      alloc2Response = am2.schedule();
    }
    // kick the scheduler, nothing given remaining 2 GB.
    nm2.nodeHeartbeat(true);

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemorySize());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());

    List<Container> allocated2 = alloc2Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated2.size());
    Assert.assertEquals(3 * GB, allocated2.get(0).getResource().getMemorySize());
    Assert.assertEquals(nm1.getNodeId(), allocated2.get(0).getNodeId());

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    report_nm2 = rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(0, report_nm1.getAvailableResource().getMemorySize());
    Assert.assertEquals(2 * GB, report_nm2.getAvailableResource().getMemorySize());

    Assert.assertEquals(6 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(2 * GB, report_nm2.getUsedResource().getMemorySize());

    Container c1 = allocated1.get(0);
    Assert.assertEquals(GB, c1.getResource().getMemorySize());
    ContainerStatus containerStatus =
        BuilderUtils.newContainerStatus(c1.getId(), ContainerState.COMPLETE,
            "", 0, c1.getResource());
    nm1.containerStatus(containerStatus);
    int waitCount = 0;
    while (attempt1.getJustFinishedContainers().size() < 1 && waitCount++ != 20) {
      LOG.info("Waiting for containers to be finished for app 1... Tried "
          + waitCount + " times already..");
      Thread.sleep(1000);
    }
    Assert.assertEquals(1, attempt1.getJustFinishedContainers().size());
    Assert.assertEquals(1, am1.schedule().getCompletedContainersStatuses()
        .size());
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(5 * GB, report_nm1.getUsedResource().getMemorySize());

    rm.stop();
  }

  @Test(timeout = 60000)
  public void testNodeUpdateBeforeAppAttemptInit() throws Exception {
    FifoScheduler scheduler = new FifoScheduler();
    MockRM rm = new MockRM(conf);
    scheduler.setRMContext(rm.getRMContext());
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, rm.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(1024, 4), 1,
            "127.0.0.1");
    scheduler.handle(new NodeAddedSchedulerEvent(node));

    ApplicationId appId = ApplicationId.newInstance(0, 1);
    scheduler.addApplication(appId, "queue1", "user1", false);

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    try {
      scheduler.handle(updateEvent);
    } catch (NullPointerException e) {
      Assert.fail();
    }

    ApplicationAttemptId attId = ApplicationAttemptId.newInstance(appId, 1);
    scheduler.addApplicationAttempt(attId, false, false);

    rm.stop();
  }

  private void testMinimumAllocation(YarnConfiguration conf, int testAlloc)
      throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    // Submit an application
    RMApp app1 = rm.submitApp(testAlloc);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());

    int checkAlloc =
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    Assert.assertEquals(checkAlloc, report_nm1.getUsedResource().getMemorySize());

    rm.stop();
  }

  @Test(timeout = 60000)
  public void testDefaultMinimumAllocation() throws Exception {
    // Test with something lesser than default
    testMinimumAllocation(new YarnConfiguration(TestFifoScheduler.conf),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB / 2);
  }

  @Test(timeout = 60000)
  public void testNonDefaultMinimumAllocation() throws Exception {
    // Set custom min-alloc to test tweaking it
    int allocMB = 1536;
    YarnConfiguration conf = new YarnConfiguration(TestFifoScheduler.conf);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, allocMB);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        allocMB * 10);
    // Test for something lesser than this.
    testMinimumAllocation(conf, allocMB / 2);
  }

  @Test(timeout = 50000)
  public void testReconnectedNode() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setQueues("default", new String[] { "default" });
    conf.setCapacity("default", 100);
    FifoScheduler fs = new FifoScheduler();
    fs.init(conf);
    fs.start();
    // mock rmContext to avoid NPE.
    RMContext context = mock(RMContext.class);
    fs.reinitialize(conf, null);
    fs.setRMContext(context);

    RMNode n1 =
        MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, "127.0.0.2");
    RMNode n2 =
        MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 2, "127.0.0.3");

    fs.handle(new NodeAddedSchedulerEvent(n1));
    fs.handle(new NodeAddedSchedulerEvent(n2));
    fs.handle(new NodeUpdateSchedulerEvent(n1));
    Assert.assertEquals(6 * GB, fs.getRootQueueMetrics().getAvailableMB());

    // reconnect n1 with downgraded memory
    n1 =
        MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 1, "127.0.0.2");
    fs.handle(new NodeRemovedSchedulerEvent(n1));
    fs.handle(new NodeAddedSchedulerEvent(n1));
    fs.handle(new NodeUpdateSchedulerEvent(n1));

    Assert.assertEquals(4 * GB, fs.getRootQueueMetrics().getAvailableMB());
    fs.stop();
  }

  @Test(timeout = 50000)
  public void testBlackListNodes() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    FifoScheduler fs = (FifoScheduler) rm.getResourceScheduler();

    int rack_num_0 = 0;
    int rack_num_1 = 1;
    // Add 4 nodes in 2 racks

    // host_0_0 in rack0
    String host_0_0 = "127.0.0.1";
    RMNode n1 =
        MockNodes.newNodeInfo(rack_num_0, MockNodes.newResource(4 * GB), 1,
            host_0_0);
    fs.handle(new NodeAddedSchedulerEvent(n1));

    // host_0_1 in rack0
    String host_0_1 = "127.0.0.2";
    RMNode n2 =
        MockNodes.newNodeInfo(rack_num_0, MockNodes.newResource(4 * GB), 1,
            host_0_1);
    fs.handle(new NodeAddedSchedulerEvent(n2));

    // host_1_0 in rack1
    String host_1_0 = "127.0.0.3";
    RMNode n3 =
        MockNodes.newNodeInfo(rack_num_1, MockNodes.newResource(4 * GB), 1,
            host_1_0);
    fs.handle(new NodeAddedSchedulerEvent(n3));

    // host_1_1 in rack1
    String host_1_1 = "127.0.0.4";
    RMNode n4 =
        MockNodes.newNodeInfo(rack_num_1, MockNodes.newResource(4 * GB), 1,
            host_1_1);
    fs.handle(new NodeAddedSchedulerEvent(n4));

    // Add one application
    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 =
        BuilderUtils.newApplicationAttemptId(appId1, 1);
    createMockRMApp(appAttemptId1, rm.getRMContext());

    SchedulerEvent appEvent =
        new AppAddedSchedulerEvent(appId1, "queue", "user");
    fs.handle(appEvent);
    SchedulerEvent attemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    fs.handle(attemptEvent);

    List<ContainerId> emptyId = new ArrayList<ContainerId>();
    List<ResourceRequest> emptyAsk = new ArrayList<ResourceRequest>();

    // Allow rack-locality for rack_1, but blacklist host_1_0

    // Set up resource requests
    // Ask for a 1 GB container for app 1
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ask1.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        "rack1", BuilderUtils.newResource(GB, 1), 1,
        RMNodeLabelsManager.NO_LABEL));
    ask1.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(GB, 1), 1,
        RMNodeLabelsManager.NO_LABEL));
    fs.allocate(appAttemptId1, ask1, null, emptyId,
        Collections.singletonList(host_1_0), null, NULL_UPDATE_REQUESTS);

    // Trigger container assignment
    fs.handle(new NodeUpdateSchedulerEvent(n3));

    // Get the allocation for the application and verify no allocation on
    // blacklist node
    Allocation allocation1 =
        fs.allocate(appAttemptId1, emptyAsk, null, emptyId,
            null, null, NULL_UPDATE_REQUESTS);

    Assert.assertEquals("allocation1", 0, allocation1.getContainers().size());

    // verify host_1_1 can get allocated as not in blacklist
    fs.handle(new NodeUpdateSchedulerEvent(n4));
    Allocation allocation2 =
        fs.allocate(appAttemptId1, emptyAsk, null, emptyId,
            null, null, NULL_UPDATE_REQUESTS);
    Assert.assertEquals("allocation2", 1, allocation2.getContainers().size());
    List<Container> containerList = allocation2.getContainers();
    for (Container container : containerList) {
      Assert.assertEquals("Container is allocated on n4",
          container.getNodeId(), n4.getNodeID());
    }

    // Ask for a 1 GB container again for app 1
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    // this time, rack0 is also in blacklist, so only host_1_1 is available to
    // be assigned
    ask2.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(GB, 1), 1));
    fs.allocate(appAttemptId1, ask2, null, emptyId,
        Collections.singletonList("rack0"), null, NULL_UPDATE_REQUESTS);

    // verify n1 is not qualified to be allocated
    fs.handle(new NodeUpdateSchedulerEvent(n1));
    Allocation allocation3 =
        fs.allocate(appAttemptId1, emptyAsk, null, emptyId,
            null, null, NULL_UPDATE_REQUESTS);
    Assert.assertEquals("allocation3", 0, allocation3.getContainers().size());

    // verify n2 is not qualified to be allocated
    fs.handle(new NodeUpdateSchedulerEvent(n2));
    Allocation allocation4 =
        fs.allocate(appAttemptId1, emptyAsk, null, emptyId,
            null, null, NULL_UPDATE_REQUESTS);
    Assert.assertEquals("allocation4", 0, allocation4.getContainers().size());

    // verify n3 is not qualified to be allocated
    fs.handle(new NodeUpdateSchedulerEvent(n3));
    Allocation allocation5 =
        fs.allocate(appAttemptId1, emptyAsk, null, emptyId,
            null, null, NULL_UPDATE_REQUESTS);
    Assert.assertEquals("allocation5", 0, allocation5.getContainers().size());

    fs.handle(new NodeUpdateSchedulerEvent(n4));
    Allocation allocation6 =
        fs.allocate(appAttemptId1, emptyAsk, null, emptyId,
            null, null, NULL_UPDATE_REQUESTS);
    Assert.assertEquals("allocation6", 1, allocation6.getContainers().size());

    containerList = allocation6.getContainers();
    for (Container container : containerList) {
      Assert.assertEquals("Container is allocated on n4",
          container.getNodeId(), n4.getNodeID());
    }

    rm.stop();
  }

  @Test(timeout = 50000)
  public void testHeadroom() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    FifoScheduler fs = (FifoScheduler) rm.getResourceScheduler();

    // Add a node
    RMNode n1 =
        MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, "127.0.0.2");
    fs.handle(new NodeAddedSchedulerEvent(n1));

    // Add two applications
    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 =
        BuilderUtils.newApplicationAttemptId(appId1, 1);
    createMockRMApp(appAttemptId1, rm.getRMContext());
    SchedulerEvent appEvent =
        new AppAddedSchedulerEvent(appId1, "queue", "user");
    fs.handle(appEvent);
    SchedulerEvent attemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    fs.handle(attemptEvent);

    ApplicationId appId2 = BuilderUtils.newApplicationId(200, 2);
    ApplicationAttemptId appAttemptId2 =
        BuilderUtils.newApplicationAttemptId(appId2, 1);
    createMockRMApp(appAttemptId2, rm.getRMContext());
    SchedulerEvent appEvent2 =
        new AppAddedSchedulerEvent(appId2, "queue", "user");
    fs.handle(appEvent2);
    SchedulerEvent attemptEvent2 =
        new AppAttemptAddedSchedulerEvent(appAttemptId2, false);
    fs.handle(attemptEvent2);

    List<ContainerId> emptyId = new ArrayList<ContainerId>();
    List<ResourceRequest> emptyAsk = new ArrayList<ResourceRequest>();

    // Set up resource requests

    // Ask for a 1 GB container for app 1
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ask1.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(GB, 1), 1));
    fs.allocate(appAttemptId1, ask1, null, emptyId,
        null, null, NULL_UPDATE_REQUESTS);

    // Ask for a 2 GB container for app 2
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    ask2.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(2 * GB, 1), 1));
    fs.allocate(appAttemptId2, ask2, null, emptyId,
        null, null, NULL_UPDATE_REQUESTS);

    // Trigger container assignment
    fs.handle(new NodeUpdateSchedulerEvent(n1));

    // Get the allocation for the applications and verify headroom
    Allocation allocation1 =
        fs.allocate(appAttemptId1, emptyAsk, null, emptyId,
            null, null, NULL_UPDATE_REQUESTS);
    Assert.assertEquals("Allocation headroom", 1 * GB, allocation1
        .getResourceLimit().getMemorySize());

    Allocation allocation2 =
        fs.allocate(appAttemptId2, emptyAsk, null, emptyId,
            null, null, NULL_UPDATE_REQUESTS);
    Assert.assertEquals("Allocation headroom", 1 * GB, allocation2
        .getResourceLimit().getMemorySize());

    rm.stop();
  }

  @Test(timeout = 60000)
  public void testResourceOverCommit() throws Exception {
    int waitCount;
    MockRM rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * GB);

    RMApp app1 = rm.submitApp(2048);
    // kick the scheduling, 2 GB given to AM1, remaining 2GB on nm1
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    // check node report, 2 GB used and 2 GB available
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(2 * GB, report_nm1.getAvailableResource().getMemorySize());

    // add request for containers
    am1.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, 2 * GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler, 2 GB given to AM1, resource remaining 0
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(1000);
      alloc1Response = am1.schedule();
    }

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated1.size());
    Assert.assertEquals(2 * GB, allocated1.get(0).getResource().getMemorySize());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    // check node report, 4 GB used and 0 GB available
    Assert.assertEquals(0, report_nm1.getAvailableResource().getMemorySize());
    Assert.assertEquals(4 * GB, report_nm1.getUsedResource().getMemorySize());

    // check container is assigned with 2 GB.
    Container c1 = allocated1.get(0);
    Assert.assertEquals(2 * GB, c1.getResource().getMemorySize());

    // update node resource to 2 GB, so resource is over-consumed.
    Map<NodeId, ResourceOption> nodeResourceMap =
        new HashMap<NodeId, ResourceOption>();
    nodeResourceMap.put(nm1.getNodeId(),
        ResourceOption.newInstance(Resource.newInstance(2 * GB, 1), -1));
    UpdateNodeResourceRequest request =
        UpdateNodeResourceRequest.newInstance(nodeResourceMap);
    rm.getAdminService().updateNodeResource(request);

    waitCount = 0;
    while (waitCount++ != 20) {
      report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
      if (null != report_nm1 &&
          report_nm1.getAvailableResource().getMemorySize() != 0) {
        break;
      }
      LOG.info("Waiting for RMNodeResourceUpdateEvent to be handled... Tried "
          + waitCount + " times already..");
      Thread.sleep(1000);
    }
    // Now, the used resource is still 4 GB, and available resource is minus
    // value.
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(4 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(-2 * GB, report_nm1.getAvailableResource().getMemorySize());

    // Check container can complete successfully in case of resource
    // over-commitment.
    ContainerStatus containerStatus =
        BuilderUtils.newContainerStatus(c1.getId(), ContainerState.COMPLETE,
            "", 0, c1.getResource());
    nm1.containerStatus(containerStatus);
    waitCount = 0;
    while (attempt1.getJustFinishedContainers().size() < 1 && waitCount++ != 20) {
      LOG.info("Waiting for containers to be finished for app 1... Tried "
          + waitCount + " times already..");
      Thread.sleep(100);
    }
    Assert.assertEquals(1, attempt1.getJustFinishedContainers().size());
    Assert.assertEquals(1, am1.schedule().getCompletedContainersStatuses()
        .size());
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemorySize());
    // As container return 2 GB back, the available resource becomes 0 again.
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemorySize());
    rm.stop();
  }

  @Test
  public void testResourceUpdateDecommissioningNode() throws Exception {
    // Mock the RMNodeResourceUpdate event handler to update SchedulerNode
    // to have 0 available resource
    RMContext spyContext = Mockito.spy(resourceManager.getRMContext());
    Dispatcher mockDispatcher = mock(AsyncDispatcher.class);
    when(mockDispatcher.getEventHandler()).thenReturn(new EventHandler<Event>() {
      @Override
      public void handle(Event event) {
        if (event instanceof RMNodeResourceUpdateEvent) {
          RMNodeResourceUpdateEvent resourceEvent =
              (RMNodeResourceUpdateEvent) event;
          resourceManager
              .getResourceScheduler()
              .getSchedulerNode(resourceEvent.getNodeId())
              .updateTotalResource(resourceEvent.getResourceOption().getResource());
        }
      }
    });
    Mockito.doReturn(mockDispatcher).when(spyContext).getDispatcher();
    ((FifoScheduler) resourceManager.getResourceScheduler())
        .setRMContext(spyContext);
    ((AsyncDispatcher) mockDispatcher).start();
    // Register node
    String host_0 = "host_0";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(8 * GB, 4));
    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);

    // Submit an application
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit();

    application_0.addNodeManager(host_0, 1234, nm_0);

    Resource capability_0_0 = Resources.createResource(1 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_0);

    Task task_0_0 =
        new Task(application_0, priority_0, new String[] { host_0 });
    application_0.addTask(task_0_0);

    // Send resource requests to the scheduler
    application_0.schedule();

    RMNode node =
        resourceManager.getRMContext().getRMNodes().get(nm_0.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);

    // Kick off another heartbeat with the node state mocked to decommissioning
    // This should update the schedulernodes to have 0 available resource
    RMNode spyNode =
        Mockito.spy(resourceManager.getRMContext().getRMNodes()
            .get(nm_0.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));

    // Get allocations from the scheduler
    application_0.schedule();

    // Check the used resource is 1 GB 1 core
    // Assert.assertEquals(1 * GB, nm_0.getUsed().getMemory());
    Resource usedResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getAllocatedResource();
    Assert.assertEquals(usedResource.getMemorySize(), 1 * GB);
    Assert.assertEquals(usedResource.getVirtualCores(), 1);
    // Check total resource of scheduler node is also changed to 1 GB 1 core
    Resource totalResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getTotalResource();
    Assert.assertEquals(totalResource.getMemorySize(), 1 * GB);
    Assert.assertEquals(totalResource.getVirtualCores(), 1);
    // Check the available resource is 0/0
    Resource availableResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getUnallocatedResource();
    Assert.assertEquals(availableResource.getMemorySize(), 0);
    Assert.assertEquals(availableResource.getVirtualCores(), 0);
  }

  private void checkApplicationResourceUsage(int expected, 
      Application application) {
    Assert.assertEquals(expected, application.getUsedResources().getMemorySize());
  }
  
  private void checkNodeResourceUsage(int expected,
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager node) {
    Assert.assertEquals(expected, node.getUsed().getMemorySize());
    node.checkResourceUsage();
  }

  public static void main(String[] arg) throws Exception {
    TestFifoScheduler t = new TestFifoScheduler();
    t.setUp();
    t.testFifoScheduler();
    t.tearDown();
  }

  private RMAppImpl createMockRMApp(ApplicationAttemptId attemptId,
      RMContext context) {
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(attemptId.getApplicationId());
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    when(attempt.getAppAttemptId()).thenReturn(attemptId);
    RMAppAttemptMetrics attemptMetric = mock(RMAppAttemptMetrics.class);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);
    ApplicationSubmissionContext submissionContext = mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUnmanagedAM()).thenReturn(false);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    context.getRMApps().putIfAbsent(attemptId.getApplicationId(), app);
    return app;
  }

}
