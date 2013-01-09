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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestRMContainerImpl {

  @Test
  public void testReleaseWhileRunning() {

    DrainDispatcher drainDispatcher = new DrainDispatcher();
    EventHandler eventHandler = drainDispatcher.getEventHandler();
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

    RMContainer rmContainer = new RMContainerImpl(container, appAttemptId,
        nodeId, eventHandler, expirer);

    assertEquals(RMContainerState.NEW, rmContainer.getState());

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

    // In RUNNING state. Verify RELEASED and associated actions.
    reset(appAttemptEventHandler);
    ContainerStatus containerStatus = SchedulerUtils
        .createAbnormalContainerStatus(containerId,
            SchedulerUtils.RELEASED_CONTAINER);
    rmContainer.handle(new RMContainerFinishedEvent(containerId,
        containerStatus, RMContainerEventType.RELEASED));
    drainDispatcher.await();
    assertEquals(RMContainerState.RELEASED, rmContainer.getState());

    ArgumentCaptor<RMAppAttemptContainerFinishedEvent> captor = ArgumentCaptor
        .forClass(RMAppAttemptContainerFinishedEvent.class);
    verify(appAttemptEventHandler).handle(captor.capture());
    RMAppAttemptContainerFinishedEvent cfEvent = captor.getValue();
    assertEquals(appAttemptId, cfEvent.getApplicationAttemptId());
    assertEquals(containerStatus, cfEvent.getContainerStatus());
    assertEquals(RMAppAttemptEventType.CONTAINER_FINISHED, cfEvent.getType());
    
    // In RELEASED state. A FINIHSED event may come in.
    rmContainer.handle(new RMContainerFinishedEvent(containerId, SchedulerUtils
        .createAbnormalContainerStatus(containerId, "FinishedContainer"),
        RMContainerEventType.FINISHED));
    assertEquals(RMContainerState.RELEASED, rmContainer.getState());
  }

  @Test
  public void testExpireWhileRunning() {

    DrainDispatcher drainDispatcher = new DrainDispatcher();
    EventHandler eventHandler = drainDispatcher.getEventHandler();
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

    RMContainer rmContainer = new RMContainerImpl(container, appAttemptId,
        nodeId, eventHandler, expirer);

    assertEquals(RMContainerState.NEW, rmContainer.getState());

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

    // In RUNNING state. Verify EXPIRE and associated actions.
    reset(appAttemptEventHandler);
    ContainerStatus containerStatus = SchedulerUtils
        .createAbnormalContainerStatus(containerId,
            SchedulerUtils.EXPIRED_CONTAINER);
    rmContainer.handle(new RMContainerFinishedEvent(containerId,
        containerStatus, RMContainerEventType.EXPIRE));
    drainDispatcher.await();
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());
  }
}
