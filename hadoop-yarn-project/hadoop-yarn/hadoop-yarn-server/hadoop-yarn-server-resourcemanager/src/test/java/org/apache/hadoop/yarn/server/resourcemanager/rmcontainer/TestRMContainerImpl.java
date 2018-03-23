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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestRMContainerImpl {

  @Test
  public void testReleaseWhileRunning() {

    DrainDispatcher drainDispatcher = new DrainDispatcher();
    EventHandler<RMAppAttemptEvent> appAttemptEventHandler = mock(EventHandler.class);
    EventHandler generic = mock(EventHandler.class);
    drainDispatcher.register(RMAppAttemptEventType.class,
        appAttemptEventHandler);
    drainDispatcher.register(RMNodeEventType.class, generic);
    drainDispatcher.init(new YarnConfiguration());
    drainDispatcher.start();
    NodeId nodeId = BuilderUtils.newNodeId("host", 3425);
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    ContainerAllocationExpirer expirer = mock(ContainerAllocationExpirer.class);

    Resource resource = BuilderUtils.newResource(512, 1);
    Priority priority = BuilderUtils.newPriority(5);

    Container container = BuilderUtils.newContainer(containerId, nodeId,
        "host:3465", resource, priority, null);
    ConcurrentMap<ApplicationId, RMApp> rmApps =
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    when(rmApp.getRMAppAttempt((ApplicationAttemptId)Matchers.any())).thenReturn(null);
    Mockito.doReturn(rmApp).when(rmApps).get((ApplicationId)Matchers.any());

    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getRMApps()).thenReturn(rmApps);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    AllocationTagsManager ptm = mock(AllocationTagsManager.class);
    when(rmContext.getAllocationTagsManager()).thenReturn(ptm);
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(
        YarnConfiguration.APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO,
        true);
    when(rmContext.getYarnConfiguration()).thenReturn(conf);
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId,
        nodeId, "user", rmContext);

    assertEquals(RMContainerState.NEW, rmContainer.getState());
    assertEquals(resource, rmContainer.getAllocatedResource());
    assertEquals(nodeId, rmContainer.getAllocatedNode());
    assertEquals(priority,
        rmContainer.getAllocatedSchedulerKey().getPriority());
    verify(writer).containerStarted(any(RMContainer.class));
    verify(publisher).containerCreated(any(RMContainer.class), anyLong());

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.START));
    drainDispatcher.await();
    assertEquals(RMContainerState.ALLOCATED, rmContainer.getState());
    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.ACQUIRED));
    drainDispatcher.await();
    assertEquals(RMContainerState.ACQUIRED, rmContainer.getState());

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.LAUNCHED));
    drainDispatcher.await();
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());
    assertEquals("http://host:3465/node/containerlogs/container_1_0001_01_000001/user",
        rmContainer.getLogURL());

    // In RUNNING state. Verify RELEASED and associated actions.
    reset(appAttemptEventHandler);
    ContainerStatus containerStatus = SchedulerUtils
        .createAbnormalContainerStatus(containerId,
            SchedulerUtils.RELEASED_CONTAINER);
    rmContainer.handle(new RMContainerFinishedEvent(containerId,
        containerStatus, RMContainerEventType.RELEASED));
    drainDispatcher.await();
    assertEquals(RMContainerState.RELEASED, rmContainer.getState());
    assertEquals(SchedulerUtils.RELEASED_CONTAINER,
        rmContainer.getDiagnosticsInfo());
    assertEquals(ContainerExitStatus.ABORTED,
        rmContainer.getContainerExitStatus());
    assertEquals(ContainerState.COMPLETE, rmContainer.getContainerState());
    verify(writer).containerFinished(any(RMContainer.class));
    verify(publisher).containerFinished(any(RMContainer.class), anyLong());

    ArgumentCaptor<RMAppAttemptContainerFinishedEvent> captor = ArgumentCaptor
        .forClass(RMAppAttemptContainerFinishedEvent.class);
    verify(appAttemptEventHandler).handle(captor.capture());
    RMAppAttemptContainerFinishedEvent cfEvent = captor.getValue();
    assertEquals(appAttemptId, cfEvent.getApplicationAttemptId());
    assertEquals(containerStatus, cfEvent.getContainerStatus());
    assertEquals(RMAppAttemptEventType.CONTAINER_FINISHED, cfEvent.getType());
    
    // In RELEASED state. A FINISHED event may come in.
    rmContainer.handle(new RMContainerFinishedEvent(containerId, SchedulerUtils
        .createAbnormalContainerStatus(containerId, "FinishedContainer"),
        RMContainerEventType.FINISHED));
    assertEquals(RMContainerState.RELEASED, rmContainer.getState());
  }

  @Test
  public void testExpireWhileRunning() {

    DrainDispatcher drainDispatcher = new DrainDispatcher();
    EventHandler<RMAppAttemptEvent> appAttemptEventHandler = mock(EventHandler.class);
    EventHandler generic = mock(EventHandler.class);
    drainDispatcher.register(RMAppAttemptEventType.class,
        appAttemptEventHandler);
    drainDispatcher.register(RMNodeEventType.class, generic);
    drainDispatcher.init(new YarnConfiguration());
    drainDispatcher.start();
    NodeId nodeId = BuilderUtils.newNodeId("host", 3425);
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    ContainerAllocationExpirer expirer = mock(ContainerAllocationExpirer.class);

    Resource resource = BuilderUtils.newResource(512, 1);
    Priority priority = BuilderUtils.newPriority(5);

    Container container = BuilderUtils.newContainer(containerId, nodeId,
        "host:3465", resource, priority, null);
    
    ConcurrentMap<ApplicationId, RMApp> appMap = new ConcurrentHashMap<>();
    RMApp rmApp = mock(RMApp.class);
    appMap.putIfAbsent(appId, rmApp);

    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    AllocationTagsManager ptm = mock(AllocationTagsManager.class);
    when(rmContext.getAllocationTagsManager()).thenReturn(ptm);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(
        YarnConfiguration.APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO,
        true);
    when(rmContext.getYarnConfiguration()).thenReturn(conf);
    when(rmContext.getRMApps()).thenReturn(appMap);
    
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId,
        nodeId, "user", rmContext);

    assertEquals(RMContainerState.NEW, rmContainer.getState());
    assertEquals(resource, rmContainer.getAllocatedResource());
    assertEquals(nodeId, rmContainer.getAllocatedNode());
    assertEquals(priority,
        rmContainer.getAllocatedSchedulerKey().getPriority());
    verify(writer).containerStarted(any(RMContainer.class));
    verify(publisher).containerCreated(any(RMContainer.class), anyLong());

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.START));
    drainDispatcher.await();
    assertEquals(RMContainerState.ALLOCATED, rmContainer.getState());

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.ACQUIRED));
    drainDispatcher.await();
    assertEquals(RMContainerState.ACQUIRED, rmContainer.getState());

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.LAUNCHED));
    drainDispatcher.await();
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());
    assertEquals("http://host:3465/node/containerlogs/container_1_0001_01_000001/user",
        rmContainer.getLogURL());

    // In RUNNING state. Verify EXPIRE and associated actions.
    reset(appAttemptEventHandler);
    ContainerStatus containerStatus = SchedulerUtils
        .createAbnormalContainerStatus(containerId,
            SchedulerUtils.EXPIRED_CONTAINER);
    rmContainer.handle(new RMContainerFinishedEvent(containerId,
        containerStatus, RMContainerEventType.EXPIRE));
    drainDispatcher.await();
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());
    verify(writer, never()).containerFinished(any(RMContainer.class));
    verify(publisher, never()).containerFinished(any(RMContainer.class),
        anyLong());
  }

  @Test
  public void testExistenceOfResourceRequestInRMContainer() throws Exception {
    Configuration conf = new Configuration();
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1 = rm1.submitApp(1024);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    ResourceScheduler scheduler = rm1.getResourceScheduler();

    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 = ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);

    // Verify whether list of ResourceRequest is present in RMContainer
    // while moving to ALLOCATED state
    Assert.assertNotNull(
        scheduler.getRMContainer(containerId2).getContainerRequest());

    // Allocate container
    am1.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>())
        .getAllocatedContainers();
    rm1.waitForState(nm1, containerId2, RMContainerState.ACQUIRED);

    // After RMContainer moving to ACQUIRED state, list of ResourceRequest will
    // be empty
    Assert.assertNull(
        scheduler.getRMContainer(containerId2).getContainerRequest());
  }

  @Test (timeout = 180000)
  public void testStoreAllContainerMetrics() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    conf.setBoolean(
        YarnConfiguration.APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO,
        true);
    MockRM rm1 = new MockRM(conf);

    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    rm1.getRMContext().setSystemMetricsPublisher(publisher);

    rm1.start();
    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1 = rm1.submitApp(1024);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);

    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 = ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);
    am1.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>())
        .getAllocatedContainers();
    rm1.waitForState(nm1, containerId2, RMContainerState.ACQUIRED);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.COMPLETE);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(nm1, containerId2, RMContainerState.COMPLETED);
    rm1.stop();

    // RMContainer should be publishing system metrics for all containers.
    // Since there is 1 AM container and 1 non-AM container, there should be 2
    // container created events and 2 container finished events.
    verify(publisher, times(2)).containerCreated(any(RMContainer.class), anyLong());
    verify(publisher, times(2)).containerFinished(any(RMContainer.class), anyLong());
  }

  @Test (timeout = 180000)
  public void testStoreOnlyAMContainerMetrics() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    conf.setBoolean(
        YarnConfiguration.APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO,
        false);
    MockRM rm1 = new MockRM(conf);

    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    rm1.getRMContext().setSystemMetricsPublisher(publisher);

    rm1.start();
    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1 = rm1.submitApp(1024);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);

    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 = ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);
    am1.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>())
        .getAllocatedContainers();
    rm1.waitForState(nm1, containerId2, RMContainerState.ACQUIRED);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.COMPLETE);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(nm1, containerId2, RMContainerState.COMPLETED);
    rm1.stop();

    // RMContainer should be publishing system metrics only for AM container.
    verify(publisher, times(1)).containerCreated(any(RMContainer.class), anyLong());
    verify(publisher, times(1)).containerFinished(any(RMContainer.class), anyLong());
  }

  @Test
  public void testContainerTransitionNotifyAllocationTagsManager()
      throws Exception {
    DrainDispatcher drainDispatcher = new DrainDispatcher();
    EventHandler<RMAppAttemptEvent> appAttemptEventHandler = mock(
        EventHandler.class);
    EventHandler generic = mock(EventHandler.class);
    drainDispatcher.register(RMAppAttemptEventType.class,
        appAttemptEventHandler);
    drainDispatcher.register(RMNodeEventType.class, generic);
    drainDispatcher.init(new YarnConfiguration());
    drainDispatcher.start();
    NodeId nodeId = BuilderUtils.newNodeId("host", 3425);
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    ContainerAllocationExpirer expirer = mock(ContainerAllocationExpirer.class);

    Resource resource = BuilderUtils.newResource(512, 1);
    Priority priority = BuilderUtils.newPriority(5);

    Container container = BuilderUtils.newContainer(containerId, nodeId,
        "host:3465", resource, priority, null);
    ConcurrentMap<ApplicationId, RMApp> rmApps =
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    when(rmApp.getRMAppAttempt(Matchers.any())).thenReturn(null);
    Mockito.doReturn(rmApp).when(rmApps).get(Matchers.any());

    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    RMContext rmContext = mock(RMContext.class);
    AllocationTagsManager tagsManager = new AllocationTagsManager(rmContext);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getRMApps()).thenReturn(rmApps);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getAllocationTagsManager()).thenReturn(tagsManager);
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(
        YarnConfiguration.APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO,
        true);
    when(rmContext.getYarnConfiguration()).thenReturn(conf);

    /* First container: ALLOCATED -> KILLED */
    RMContainerImpl rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId,
        nodeId, "user", rmContext);
    rmContainer.setAllocationTags(ImmutableSet.of("mapper"));

    Assert.assertEquals(0,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.START));

    Assert.assertEquals(1,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    rmContainer.handle(new RMContainerFinishedEvent(containerId, ContainerStatus
        .newInstance(containerId, ContainerState.COMPLETE, "", 0),
        RMContainerEventType.KILL));

    Assert.assertEquals(0,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    /* Second container: ACQUIRED -> FINISHED */
    rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId,
        nodeId, "user", rmContext);

    Assert.assertEquals(0,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    rmContainer.setAllocationTags(ImmutableSet.of("mapper"));
    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.START));

    Assert.assertEquals(1,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    rmContainer.handle(
        new RMContainerEvent(containerId, RMContainerEventType.ACQUIRED));

    rmContainer.handle(new RMContainerFinishedEvent(containerId, ContainerStatus
        .newInstance(containerId, ContainerState.COMPLETE, "", 0),
        RMContainerEventType.FINISHED));

    Assert.assertEquals(0,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    /* Third container: RUNNING -> FINISHED */
    rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId,
        nodeId, "user", rmContext);
    rmContainer.setAllocationTags(ImmutableSet.of("mapper"));

    Assert.assertEquals(0,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    rmContainer.handle(new RMContainerEvent(containerId,
        RMContainerEventType.START));

    Assert.assertEquals(1,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    rmContainer.handle(
        new RMContainerEvent(containerId, RMContainerEventType.ACQUIRED));

    rmContainer.handle(
        new RMContainerEvent(containerId, RMContainerEventType.LAUNCHED));

    rmContainer.handle(new RMContainerFinishedEvent(containerId, ContainerStatus
        .newInstance(containerId, ContainerState.COMPLETE, "", 0),
        RMContainerEventType.FINISHED));

    Assert.assertEquals(0,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    /* Fourth container: NEW -> RECOVERED */
    rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId, nodeId,
        "user", rmContext);
    rmContainer.setAllocationTags(ImmutableSet.of("mapper"));

    Assert.assertEquals(0,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));

    NMContainerStatus containerStatus = NMContainerStatus
        .newInstance(containerId, 0, ContainerState.NEW,
            Resource.newInstance(1024, 1), "recover container", 0,
            Priority.newInstance(0), 0);
    containerStatus.setAllocationTags(ImmutableSet.of("mapper"));
    rmContainer
        .handle(new RMContainerRecoverEvent(containerId, containerStatus));

    Assert.assertEquals(1,
        tagsManager.getNodeCardinalityByOp(nodeId, appId, null, Long::max));
  }
}
