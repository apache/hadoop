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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class TestAbstractYarnScheduler extends ParameterizedSchedulerTestBase {

  public TestAbstractYarnScheduler(SchedulerType type) {
    super(type);
  }

  @Test(timeout = 60000)
  public void testResourceRequestRestoreWhenRMContainerIsAtAllocated()
      throws Exception {
    configureScheduler();
    YarnConfiguration conf = getConf();
    MockRM rm1 = new MockRM(conf);
    try {
      rm1.start();
      RMApp app1 =
          rm1.submitApp(200, "name", "user",
              new HashMap<ApplicationAccessType, String>(), false, "default",
              -1, null, "Test", false, true);
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 10240, rm1.getResourceTrackerService());
      nm1.registerNode();

      MockNM nm2 =
          new MockNM("127.0.0.1:2351", 10240, rm1.getResourceTrackerService());
      nm2.registerNode();

      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

      int NUM_CONTAINERS = 1;
      // allocate NUM_CONTAINERS containers
      am1.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
          new ArrayList<ContainerId>());
      nm1.nodeHeartbeat(true);

      // wait for containers to be allocated.
      List<Container> containers =
          am1.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers();
      while (containers.size() != NUM_CONTAINERS) {
        nm1.nodeHeartbeat(true);
        containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers());
        Thread.sleep(200);
      }

      // launch the 2nd container, for testing running container transferred.
      nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
      ContainerId containerId2 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
      rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

      // 3rd container is in Allocated state.
      am1.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
          new ArrayList<ContainerId>());
      nm2.nodeHeartbeat(true);
      ContainerId containerId3 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
      rm1.waitForContainerAllocated(nm2, containerId3);
      rm1.waitForState(nm2, containerId3, RMContainerState.ALLOCATED);

      // NodeManager restart
      nm2.registerNode();

      // NM restart kills all allocated and running containers.
      rm1.waitForState(nm2, containerId3, RMContainerState.KILLED);

      // The killed RMContainer request should be restored. In successive
      // nodeHeartBeats AM should be able to get container allocated.
      containers =
          am1.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers();
      while (containers.size() != NUM_CONTAINERS) {
        nm2.nodeHeartbeat(true);
        containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers());
        Thread.sleep(200);
      }

      nm2.nodeHeartbeat(am1.getApplicationAttemptId(), 4,
          ContainerState.RUNNING);
      ContainerId containerId4 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 4);
      rm1.waitForState(nm2, containerId4, RMContainerState.RUNNING);
    } finally {
      rm1.stop();
    }
  }

  private class SleepHandler implements EventHandler<SchedulerEvent> {
    boolean sleepFlag = false;
    int sleepTime = 20;
    @Override
    public void handle(SchedulerEvent event) {
      try {
        if (sleepFlag) {
          Thread.sleep(sleepTime);
        }
      } catch(InterruptedException ie) {
      }
    }
  }

  private ResourceTrackerService getPrivateResourceTrackerService(
      Dispatcher privateDispatcher, ResourceManager rm,
      SleepHandler sleepHandler) {
    Configuration conf = getConf();

    RMContext privateContext =
        new RMContextImpl(privateDispatcher, null, null, null, null, null, null,
            null, null, null);
    privateContext.setNodeLabelManager(Mockito.mock(RMNodeLabelsManager.class));

    privateDispatcher.register(SchedulerEventType.class, sleepHandler);
    privateDispatcher.register(SchedulerEventType.class,
        rm.getResourceScheduler());
    privateDispatcher.register(RMNodeEventType.class,
        new ResourceManager.NodeEventDispatcher(privateContext));
    ((Service) privateDispatcher).init(conf);
    ((Service) privateDispatcher).start();
    NMLivelinessMonitor nmLivelinessMonitor =
        new NMLivelinessMonitor(privateDispatcher);
    nmLivelinessMonitor.init(conf);
    nmLivelinessMonitor.start();
    NodesListManager nodesListManager = new NodesListManager(privateContext);
    nodesListManager.init(conf);
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.start();
    NMTokenSecretManagerInRM nmTokenSecretManager =
        new NMTokenSecretManagerInRM(conf);
    nmTokenSecretManager.start();
    ResourceTrackerService privateResourceTrackerService =
        new ResourceTrackerService(privateContext, nodesListManager,
            nmLivelinessMonitor, containerTokenSecretManager,
            nmTokenSecretManager);
    privateResourceTrackerService.init(conf);
    privateResourceTrackerService.start();
    rm.getResourceScheduler().setRMContext(privateContext);
    return privateResourceTrackerService;
  }

  /**
   * Test the behavior of the scheduler when a node reconnects
   * with changed capabilities. This test is to catch any race conditions
   * that might occur due to the use of the RMNode object.
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testNodemanagerReconnect() throws Exception {
    configureScheduler();
    Configuration conf = getConf();
    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, false);
      DrainDispatcher privateDispatcher = new DrainDispatcher();
      SleepHandler sleepHandler = new SleepHandler();
      ResourceTrackerService privateResourceTrackerService =
          getPrivateResourceTrackerService(privateDispatcher, rm, sleepHandler);

      // Register node1
      String hostname1 = "localhost1";
      Resource capability = BuilderUtils.newResource(4096, 4);
      RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(null);

      RegisterNodeManagerRequest request1 =
          recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
      NodeId nodeId1 = NodeId.newInstance(hostname1, 0);
      request1.setNodeId(nodeId1);
      request1.setHttpPort(0);
      request1.setResource(capability);
      privateResourceTrackerService.registerNodeManager(request1);
      privateDispatcher.await();
      Resource clusterResource =
          rm.getResourceScheduler().getClusterResource();
      Assert.assertEquals("Initial cluster resources don't match", capability,
          clusterResource);

      Resource newCapability = BuilderUtils.newResource(1024, 1);
      RegisterNodeManagerRequest request2 =
          recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
      request2.setNodeId(nodeId1);
      request2.setHttpPort(0);
      request2.setResource(newCapability);
      // hold up the disaptcher and register the same node with lower capability
      sleepHandler.sleepFlag = true;
      privateResourceTrackerService.registerNodeManager(request2);
      privateDispatcher.await();
      Assert.assertEquals("Cluster resources don't match", newCapability,
          rm.getResourceScheduler().getClusterResource());
      privateResourceTrackerService.stop();
    } finally {
      rm.stop();
    }
  }
}
