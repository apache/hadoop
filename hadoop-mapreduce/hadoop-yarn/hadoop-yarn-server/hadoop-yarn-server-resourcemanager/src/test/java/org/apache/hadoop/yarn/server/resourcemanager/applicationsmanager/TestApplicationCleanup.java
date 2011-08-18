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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Testing application cleanup (notifications to nodemanagers).
 *
 */
@Ignore
public class TestApplicationCleanup {
//  private static final Log LOG = LogFactory.getLog(TestApplicationCleanup.class);
//  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
//  private AtomicInteger waitForState = new AtomicInteger(0);
//  private ResourceScheduler scheduler;
//  private final int memoryCapability = 1024;
//  private ExtASM asm;
//  private static final int memoryNeeded = 100;
//
//  private final RMContext context = new RMContextImpl(new MemStore());
//  private ClientRMService clientService;
//
//  @Before
//  public void setUp() {
//    new DummyApplicationTracker();
//    scheduler = new FifoScheduler();
//    context.getDispatcher().register(ApplicationTrackerEventType.class, scheduler);
//    Configuration conf = new Configuration();
//    context.getDispatcher().init(conf);
//    context.getDispatcher().start();
//    asm = new ExtASM(new ApplicationTokenSecretManager(), scheduler);
//    asm.init(conf);
//    clientService = new ClientRMService(context,
//        asm.getAmLivelinessMonitor(), asm.getClientToAMSecretManager(),
//        scheduler);
//  }
//
//  @After
//  public void tearDown() {
//
//  }
//
//
//  private class DummyApplicationTracker implements EventHandler<ASMEvent
//  <ApplicationTrackerEventType>> {
//
//    public DummyApplicationTracker() { 
//      context.getDispatcher().register(ApplicationTrackerEventType.class, this);
//    }
//    @Override
//    public void handle(ASMEvent<ApplicationTrackerEventType> event) {  
//    }
//
//  }
//  private class ExtASM extends ApplicationsManagerImpl {
//    boolean schedulerCleanupCalled = false;
//    boolean launcherLaunchCalled = false;
//    boolean launcherCleanupCalled = false;
//    boolean schedulerScheduleCalled = false;
//
//    private class DummyApplicationMasterLauncher implements EventHandler<ASMEvent<AMLauncherEventType>> {
//      private AtomicInteger notify = new AtomicInteger(0);
//      private AppAttempt application;
//
//      public DummyApplicationMasterLauncher(RMContext context) {
//        context.getDispatcher().register(AMLauncherEventType.class, this);
//      }
//
//      @Override
//      public void handle(ASMEvent<AMLauncherEventType> appEvent) {
//        AMLauncherEventType event = appEvent.getType();
//        switch (event) {
//        case CLEANUP:
//          launcherCleanupCalled = true;
//          break;
//        case LAUNCH:
//          LOG.info("Launcher Launch called");
//          launcherLaunchCalled = true;
//          application = appEvent.getApplication();
//          context.getDispatcher().getEventHandler().handle(
//              new ApplicationEvent(ApplicationEventType.LAUNCHED,
//                  application.getApplicationID()));
//          break;
//        default:
//          break;
//        }
//      }
//    }
//
//    private class DummySchedulerNegotiator implements EventHandler<ASMEvent<SNEventType>> {
//      private AtomicInteger snnotify = new AtomicInteger(0);
//      AppAttempt application;
//      public  DummySchedulerNegotiator(RMContext context) {
//        context.getDispatcher().register(SNEventType.class, this);
//      }
//
//      @Override
//      public void handle(ASMEvent<SNEventType> appEvent) {
//        SNEventType event = appEvent.getType();
//        switch (event) {
//        case RELEASE:
//          schedulerCleanupCalled = true;
//          break;
//        case SCHEDULE:
//          schedulerScheduleCalled = true;
//          application = appEvent.getAppAttempt();
//          context.getDispatcher().getEventHandler().handle(
//              new AMAllocatedEvent(application.getApplicationID(),
//                  application.getMasterContainer()));
//        default:
//          break;
//        }
//      }
//
//    }
//    public ExtASM(ApplicationTokenSecretManager applicationTokenSecretManager,
//        YarnScheduler scheduler) {
//      super(applicationTokenSecretManager, scheduler, context);
//    }
//
//    @Override
//    protected EventHandler<ASMEvent<SNEventType>> createNewSchedulerNegotiator(
//        YarnScheduler scheduler) {
//      return new DummySchedulerNegotiator(context);
//    }
//
//    @Override
//    protected EventHandler<ASMEvent<AMLauncherEventType>> createNewApplicationMasterLauncher(
//        ApplicationTokenSecretManager tokenSecretManager) {
//      return new DummyApplicationMasterLauncher(context);
//    }
//
//  }
//
//  private void waitForState(ApplicationState 
//      finalState, AppAttempt application) throws Exception {
//    int count = 0;
//    while(application.getState() != finalState && count < 10) {
//      Thread.sleep(500);
//      count++;
//    }
//    Assert.assertEquals(finalState, application.getState());
//  }
//
//
//  private ResourceRequest createNewResourceRequest(int capability, int i) {
//    ResourceRequest request = recordFactory.newRecordInstance(ResourceRequest.class);
//    request.setCapability(recordFactory.newRecordInstance(Resource.class));
//    request.getCapability().setMemory(capability);
//    request.setNumContainers(1);
//    request.setPriority(recordFactory.newRecordInstance(Priority.class));
//    request.getPriority().setPriority(i);
//    request.setHostName("*");
//    return request;
//  }
//
//  protected RMNode addNodes(String commonName, int i, int memoryCapability) throws IOException {
//    NodeId nodeId = recordFactory.newRecordInstance(NodeId.class);
//    nodeId.setId(i);
//    String hostName = commonName + "_" + i;
//    Node node = new NodeBase(hostName, NetworkTopology.DEFAULT_RACK);
//    Resource capability = recordFactory.newRecordInstance(Resource.class);
//    capability.setMemory(memoryCapability);
//    return new RMNodeImpl(nodeId, hostName, i, -i, node, capability);
//  }
//
//  @Test
//  public void testApplicationCleanUp() throws Exception {
//    ApplicationId appID = clientService.getNewApplicationId();
//    ApplicationSubmissionContext submissionContext = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
//    submissionContext.setApplicationId(appID);
//    submissionContext.setQueue("queuename");
//    submissionContext.setUser("dummyuser");
//    SubmitApplicationRequest request = recordFactory
//        .newRecordInstance(SubmitApplicationRequest.class);
//    request.setApplicationSubmissionContext(submissionContext);
//    clientService.submitApplication(request);
//    waitForState(ApplicationState.LAUNCHED, context.getApplications().get(
//        appID));
//    List<ResourceRequest> reqs = new ArrayList<ResourceRequest>();
//    ResourceRequest req = createNewResourceRequest(100, 1);
//    reqs.add(req);
//    reqs.add(createNewResourceRequest(memoryNeeded, 2));
//    List<Container> release = new ArrayList<Container>();
//    scheduler.allocate(appID, reqs, release);
//    ArrayList<RMNode> nodesAdded = new ArrayList<RMNode>();
//    for (int i = 0; i < 10; i++) {
//      nodesAdded.add(addNodes("localhost", i, memoryCapability));
//    }
//    /* let one node heartbeat */
//    Map<String, List<Container>> containers = new HashMap<String, List<Container>>();
//    RMNode firstNode = nodesAdded.get(0);
//    int firstNodeMemory = firstNode.getAvailableResource().getMemory();
//    RMNode secondNode = nodesAdded.get(1);
//  
//    context.getNodesCollection().updateListener(firstNode, containers);
//    context.getNodesCollection().updateListener(secondNode, containers);
//    LOG.info("Available resource on first node" + firstNode.getAvailableResource());
//    LOG.info("Available resource on second node" + secondNode.getAvailableResource());
//    /* only allocate the containers to the first node */
//    Assert.assertEquals((firstNodeMemory - (2 * memoryNeeded)), firstNode
//        .getAvailableResource().getMemory());
//    context.getDispatcher().getEventHandler().handle(
//        new ApplicationEvent(ApplicationEventType.KILL, appID));
//    while (asm.launcherCleanupCalled != true) {
//      Thread.sleep(500);
//    }
//    Assert.assertTrue(asm.launcherCleanupCalled);
//    Assert.assertTrue(asm.launcherLaunchCalled);
//    Assert.assertTrue(asm.schedulerCleanupCalled);
//    Assert.assertTrue(asm.schedulerScheduleCalled);
//    /* check for update of completed application */
//    context.getNodesCollection().updateListener(firstNode, containers);
//    NodeResponse response = firstNode.statusUpdate(containers);
//    Assert.assertTrue(response.getFinishedApplications().contains(appID));
//    LOG.info("The containers to clean up " + response.getContainersToCleanUp().size());
//    Assert.assertEquals(2, response.getContainersToCleanUp().size());
//  }
}
