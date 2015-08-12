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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFifoScheduler {
  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);
  private final int GB = 1024;

  private ResourceManager resourceManager = null;
  
  private static final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);
  
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
          String rackName, Resource capability) throws IOException,
          YarnException {
    return new org.apache.hadoop.yarn.server.resourcemanager.NodeManager(
        hostName, containerManagerPort, nmHttpPort, rackName, capability,
        resourceManager);
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
    scheduler.allocate(appAttemptId, ask, new ArrayList<ContainerId>(), null, null);

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
    
    FifoScheduler scheduler = new FifoScheduler(){
      @SuppressWarnings("unused")
      public Map<NodeId, FiCaSchedulerNode> getNodes(){
        return nodes;
      }
    };
    RMContext rmContext = new RMContextImpl(dispatcher, null, null, null, null,
        null, containerTokenSecretManager, nmTokenSecretManager, null, scheduler);
    rmContext.setSystemMetricsPublisher(mock(SystemMetricsPublisher.class));
    rmContext.setRMApplicationHistoryWriter(mock(RMApplicationHistoryWriter.class));
    ((RMContextImpl) rmContext).setYarnConfiguration(new YarnConfiguration());

    scheduler.setRMContext(rmContext);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(new Configuration(), rmContext);
    RMNode node0 = MockNodes.newNodeInfo(1,
        Resources.createResource(2048, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node0);
    scheduler.handle(nodeEvent1);
    
    Method method = scheduler.getClass().getDeclaredMethod("getNodes");
    @SuppressWarnings("unchecked")
    Map<NodeId, FiCaSchedulerNode> schedulerNodes = 
        (Map<NodeId, FiCaSchedulerNode>) method.invoke(scheduler);
    assertEquals(schedulerNodes.values().size(), 1);
    
    Resource newResource = Resources.createResource(1024, 4);
    
    NodeResourceUpdateSchedulerEvent node0ResourceUpdate = new 
        NodeResourceUpdateSchedulerEvent(node0, ResourceOption.newInstance(
            newResource, RMNode.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT));
    scheduler.handle(node0ResourceUpdate);
    
    // SchedulerNode's total resource and available resource are changed.
    assertEquals(schedulerNodes.get(node0.getNodeID()).getTotalResource()
        .getMemory(), 1024);
    assertEquals(schedulerNodes.get(node0.getNodeID()).
        getAvailableResource().getMemory(), 1024);
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
    scheduler.allocate(appAttemptId, ask, new ArrayList<ContainerId>(), null, null);

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
  public void testBlackListNodes() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    FifoScheduler fs = (FifoScheduler) rm.getResourceScheduler();

    String host = "127.0.0.1";
    RMNode node =
        MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, host);
    fs.handle(new NodeAddedSchedulerEvent(node));

    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);

    createMockRMApp(appAttemptId, rm.getRMContext());

    SchedulerEvent appEvent =
        new AppAddedSchedulerEvent(appId, "default",
          "user");
    fs.handle(appEvent);
    SchedulerEvent attemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    fs.handle(attemptEvent);

    // Verify the blacklist can be updated independent of requesting containers
    fs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null);
    Assert.assertTrue(fs.getApplicationAttempt(appAttemptId).isBlacklisted(host));
    fs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host));
    Assert.assertFalse(fs.getApplicationAttempt(appAttemptId).isBlacklisted(host));
    rm.stop();
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

  private RMAppImpl createMockRMApp(ApplicationAttemptId attemptId,
      RMContext context) {
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(attemptId.getApplicationId());
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    when(attempt.getAppAttemptId()).thenReturn(attemptId);
    RMAppAttemptMetrics attemptMetric = mock(RMAppAttemptMetrics.class);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);
    context.getRMApps().putIfAbsent(attemptId.getApplicationId(), app);
    return app;
  }

}
