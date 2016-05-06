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

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumSet;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.recovery.MemoryTimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSystemMetricsPublisher {

  private static ApplicationHistoryServer timelineServer;
  private static SystemMetricsPublisher metricsPublisher;
  private static TimelineStore store;

  @BeforeClass
  public static void setup() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_ENABLED, true);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STORE,
        MemoryTimelineStore.class, TimelineStore.class);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STATE_STORE_CLASS,
        MemoryTimelineStateStore.class, TimelineStateStore.class);
    conf.setInt(
        YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE,
        2);

    timelineServer = new ApplicationHistoryServer();
    timelineServer.init(conf);
    timelineServer.start();
    store = timelineServer.getTimelineStore();

    metricsPublisher = new SystemMetricsPublisher();
    metricsPublisher.init(conf);
    metricsPublisher.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (metricsPublisher != null) {
      metricsPublisher.stop();
    }
    if (timelineServer != null) {
      timelineServer.stop();
    }
  }

  @Test(timeout = 10000)
  public void testPublishApplicationMetrics() throws Exception {
    for (int i = 1; i <= 2; ++i) {
      ApplicationId appId = ApplicationId.newInstance(0, i);
      RMApp app = createRMApp(appId);
      metricsPublisher.appCreated(app, app.getStartTime());
      metricsPublisher.appFinished(app, RMAppState.FINISHED, app.getFinishTime());
      if (i == 1) {
        metricsPublisher.appACLsUpdated(app, "uers1,user2", 4L);
      } else {
        // in case user doesn't specify the ACLs
        metricsPublisher.appACLsUpdated(app, null, 4L);
      }
      TimelineEntity entity = null;
      do {
        entity =
            store.getEntity(appId.toString(),
                ApplicationMetricsConstants.ENTITY_TYPE,
                EnumSet.allOf(Field.class));
        // ensure three events are both published before leaving the loop
      } while (entity == null || entity.getEvents().size() < 3);
      // verify all the fields
      Assert.assertEquals(ApplicationMetricsConstants.ENTITY_TYPE,
          entity.getEntityType());
      Assert
          .assertEquals(app.getApplicationId().toString(), entity.getEntityId());
      Assert
          .assertEquals(
              app.getName(),
              entity.getOtherInfo().get(
                  ApplicationMetricsConstants.NAME_ENTITY_INFO));
      Assert.assertEquals(app.getQueue(),
          entity.getOtherInfo()
              .get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO));
      Assert
          .assertEquals(
              app.getUser(),
              entity.getOtherInfo().get(
                  ApplicationMetricsConstants.USER_ENTITY_INFO));
      Assert
          .assertEquals(
              app.getApplicationType(),
              entity.getOtherInfo().get(
                  ApplicationMetricsConstants.TYPE_ENTITY_INFO));
      Assert.assertEquals(app.getSubmitTime(),
          entity.getOtherInfo().get(
              ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO));
      if (i == 1) {
        Assert.assertEquals("uers1,user2",
            entity.getOtherInfo().get(
                ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO));
      } else {
        Assert.assertEquals(
            "",
            entity.getOtherInfo().get(
                ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO));
        Assert.assertEquals(
            app.getRMAppMetrics().getMemorySeconds(),
            Long.parseLong(entity.getOtherInfo()
                .get(ApplicationMetricsConstants.APP_MEM_METRICS).toString()));
        Assert.assertEquals(
            app.getRMAppMetrics().getVcoreSeconds(),
            Long.parseLong(entity.getOtherInfo()
                .get(ApplicationMetricsConstants.APP_CPU_METRICS).toString()));
      }
      boolean hasCreatedEvent = false;
      boolean hasFinishedEvent = false;
      boolean hasACLsUpdatedEvent = false;
      for (TimelineEvent event : entity.getEvents()) {
        if (event.getEventType().equals(
            ApplicationMetricsConstants.CREATED_EVENT_TYPE)) {
          hasCreatedEvent = true;
          Assert.assertEquals(app.getStartTime(), event.getTimestamp());
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
          hasFinishedEvent = true;
          Assert.assertEquals(app.getFinishTime(), event.getTimestamp());
          Assert.assertEquals(
              app.getDiagnostics().toString(),
              event.getEventInfo().get(
                  ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO));
          Assert.assertEquals(
              app.getFinalApplicationStatus().toString(),
              event.getEventInfo().get(
                  ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO));
          Assert.assertEquals(YarnApplicationState.FINISHED.toString(), event
              .getEventInfo().get(ApplicationMetricsConstants.STATE_EVENT_INFO));
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.ACLS_UPDATED_EVENT_TYPE)) {
          hasACLsUpdatedEvent = true;
          Assert.assertEquals(4L, event.getTimestamp());
        }
      }
      Assert.assertTrue(hasCreatedEvent && hasFinishedEvent && hasACLsUpdatedEvent);
    }
  }

  @Test(timeout = 10000)
  public void testPublishAppAttemptMetricsForUnmanagedAM() throws Exception {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);
    RMAppAttempt appAttempt = createRMAppAttempt(appAttemptId,true);
    metricsPublisher.appAttemptRegistered(appAttempt, Integer.MAX_VALUE + 1L);
    RMApp app = mock(RMApp.class);
    when(app.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.UNDEFINED);
    metricsPublisher.appAttemptFinished(appAttempt, RMAppAttemptState.FINISHED, app,
        Integer.MAX_VALUE + 2L);
    TimelineEntity entity = null;
    do {
      entity =
          store.getEntity(appAttemptId.toString(),
              AppAttemptMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
      // ensure two events are both published before leaving the loop
    } while (entity == null || entity.getEvents().size() < 2);
  }

  @Test(timeout = 10000)
  public void testPublishAppAttemptMetrics() throws Exception {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);
    RMAppAttempt appAttempt = createRMAppAttempt(appAttemptId, false);
    metricsPublisher.appAttemptRegistered(appAttempt, Integer.MAX_VALUE + 1L);
    RMApp app = mock(RMApp.class);
    when(app.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.UNDEFINED);
    metricsPublisher.appAttemptFinished(appAttempt, RMAppAttemptState.FINISHED, app,
        Integer.MAX_VALUE + 2L);
    TimelineEntity entity = null;
    do {
      entity =
          store.getEntity(appAttemptId.toString(),
              AppAttemptMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
      // ensure two events are both published before leaving the loop
    } while (entity == null || entity.getEvents().size() < 2);
    // verify all the fields
    Assert.assertEquals(AppAttemptMetricsConstants.ENTITY_TYPE,
        entity.getEntityType());
    Assert.assertEquals(appAttemptId.toString(), entity.getEntityId());
    Assert.assertEquals(
        appAttemptId.getApplicationId().toString(),
        entity.getPrimaryFilters()
            .get(AppAttemptMetricsConstants.PARENT_PRIMARY_FILTER).iterator()
            .next());
    boolean hasRegisteredEvent = false;
    boolean hasFinishedEvent = false;
    for (TimelineEvent event : entity.getEvents()) {
      if (event.getEventType().equals(
          AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE)) {
        hasRegisteredEvent = true;
        Assert.assertEquals(appAttempt.getHost(),
            event.getEventInfo()
                .get(AppAttemptMetricsConstants.HOST_EVENT_INFO));
        Assert
            .assertEquals(appAttempt.getRpcPort(),
                event.getEventInfo().get(
                    AppAttemptMetricsConstants.RPC_PORT_EVENT_INFO));
        Assert.assertEquals(
            appAttempt.getMasterContainer().getId().toString(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.MASTER_CONTAINER_EVENT_INFO));
      } else if (event.getEventType().equals(
          AppAttemptMetricsConstants.FINISHED_EVENT_TYPE)) {
        hasFinishedEvent = true;
        Assert.assertEquals(appAttempt.getDiagnostics(), event.getEventInfo()
            .get(AppAttemptMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO));
        Assert.assertEquals(appAttempt.getTrackingUrl(), event.getEventInfo()
            .get(AppAttemptMetricsConstants.TRACKING_URL_EVENT_INFO));
        Assert.assertEquals(
            appAttempt.getOriginalTrackingUrl(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_EVENT_INFO));
        Assert.assertEquals(
            FinalApplicationStatus.UNDEFINED.toString(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.FINAL_STATUS_EVENT_INFO));
        Assert.assertEquals(
            YarnApplicationAttemptState.FINISHED.toString(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.STATE_EVENT_INFO));
      }
    }
    Assert.assertTrue(hasRegisteredEvent && hasFinishedEvent);
  }

  @Test(timeout = 10000)
  public void testPublishHostPortInfoOnContainerFinished() throws Exception {
    ContainerId containerId =
        ContainerId.newContainerId(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1);
    RMContainer container = createRMContainer(containerId);
    metricsPublisher.containerFinished(container, container.getFinishTime());
    TimelineEntity entity = null;
    do {
      entity =
          store.getEntity(containerId.toString(),
              ContainerMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
    } while (entity == null || entity.getEvents().size() < 1);
    Assert.assertNotNull(entity.getOtherInfo());
    Assert.assertEquals(2, entity.getOtherInfo().size());
    Assert.assertNotNull(entity.getOtherInfo().get(
        ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO));
    Assert.assertNotNull(entity.getOtherInfo().get(
        ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO));
    Assert.assertEquals(
        container.getAllocatedNode().getHost(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO));
    Assert.assertEquals(
        container.getAllocatedNode().getPort(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO));
  }

  @Test(timeout = 10000)
  public void testPublishContainerMetrics() throws Exception {
    ContainerId containerId =
        ContainerId.newContainerId(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1);
    RMContainer container = createRMContainer(containerId);
    metricsPublisher.containerCreated(container, container.getCreationTime());
    metricsPublisher.containerFinished(container, container.getFinishTime());
    TimelineEntity entity = null;
    do {
      entity =
          store.getEntity(containerId.toString(),
              ContainerMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
      // ensure two events are both published before leaving the loop
    } while (entity == null || entity.getEvents().size() < 2);
    // verify all the fields
    Assert.assertEquals(ContainerMetricsConstants.ENTITY_TYPE,
        entity.getEntityType());
    Assert.assertEquals(containerId.toString(), entity.getEntityId());
    Assert.assertEquals(
        containerId.getApplicationAttemptId().toString(),
        entity.getPrimaryFilters()
            .get(ContainerMetricsConstants.PARENT_PRIMARIY_FILTER).iterator()
            .next());
    Assert.assertEquals(
        container.getAllocatedNode().getHost(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO));
    Assert.assertEquals(
        container.getAllocatedNode().getPort(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO));
    Assert.assertEquals(
        container.getAllocatedResource().getMemory(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_MEMORY_ENTITY_INFO));
    Assert.assertEquals(
        container.getAllocatedResource().getVirtualCores(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_VCORE_ENTITY_INFO));
    Assert.assertEquals(
        container.getAllocatedPriority().getPriority(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_PRIORITY_ENTITY_INFO));
    boolean hasCreatedEvent = false;
    boolean hasFinishedEvent = false;
    for (TimelineEvent event : entity.getEvents()) {
      if (event.getEventType().equals(
          ContainerMetricsConstants.CREATED_EVENT_TYPE)) {
        hasCreatedEvent = true;
        Assert.assertEquals(container.getCreationTime(), event.getTimestamp());
      } else if (event.getEventType().equals(
          ContainerMetricsConstants.FINISHED_EVENT_TYPE)) {
        hasFinishedEvent = true;
        Assert.assertEquals(container.getFinishTime(), event.getTimestamp());
        Assert.assertEquals(
            container.getDiagnosticsInfo(),
            event.getEventInfo().get(
                ContainerMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO));
        Assert.assertEquals(
            container.getContainerExitStatus(),
            event.getEventInfo().get(
                ContainerMetricsConstants.EXIT_STATUS_EVENT_INFO));
        Assert.assertEquals(container.getContainerState().toString(), event
            .getEventInfo().get(ContainerMetricsConstants.STATE_EVENT_INFO));
      }
    }
    Assert.assertTrue(hasCreatedEvent && hasFinishedEvent);
  }

  private static RMApp createRMApp(ApplicationId appId) {
    RMApp app = mock(RMApp.class);
    when(app.getApplicationId()).thenReturn(appId);
    when(app.getName()).thenReturn("test app");
    when(app.getApplicationType()).thenReturn("test app type");
    when(app.getUser()).thenReturn("test user");
    when(app.getQueue()).thenReturn("test queue");
    when(app.getSubmitTime()).thenReturn(Integer.MAX_VALUE + 1L);
    when(app.getStartTime()).thenReturn(Integer.MAX_VALUE + 2L);
    when(app.getFinishTime()).thenReturn(Integer.MAX_VALUE + 3L);
    when(app.getDiagnostics()).thenReturn(
        new StringBuilder("test diagnostics info"));
    RMAppAttempt appAttempt = mock(RMAppAttempt.class);
    when(appAttempt.getAppAttemptId()).thenReturn(
        ApplicationAttemptId.newInstance(appId, 1));
    when(app.getCurrentAppAttempt()).thenReturn(appAttempt);
    when(app.getFinalApplicationStatus()).thenReturn(
        FinalApplicationStatus.UNDEFINED);
    when(app.getRMAppMetrics()).thenReturn(
        new RMAppMetrics(null, 0, 0, Integer.MAX_VALUE, Long.MAX_VALUE));
    return app;
  }

  private static RMAppAttempt createRMAppAttempt(
      ApplicationAttemptId appAttemptId, boolean unmanagedAMAttempt) {
    RMAppAttempt appAttempt = mock(RMAppAttempt.class);
    when(appAttempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(appAttempt.getHost()).thenReturn("test host");
    when(appAttempt.getRpcPort()).thenReturn(-100);
    if (!unmanagedAMAttempt) {
      Container container = mock(Container.class);
      when(container.getId())
          .thenReturn(ContainerId.newContainerId(appAttemptId, 1));
      when(appAttempt.getMasterContainer()).thenReturn(container);
    }
    when(appAttempt.getDiagnostics()).thenReturn("test diagnostics info");
    when(appAttempt.getTrackingUrl()).thenReturn("test tracking url");
    when(appAttempt.getOriginalTrackingUrl()).thenReturn(
        "test original tracking url");
    return appAttempt;
  }

  private static RMContainer createRMContainer(ContainerId containerId) {
    RMContainer container = mock(RMContainer.class);
    when(container.getContainerId()).thenReturn(containerId);
    when(container.getAllocatedNode()).thenReturn(
        NodeId.newInstance("test host", -100));
    when(container.getAllocatedResource()).thenReturn(
        Resource.newInstance(-1, -1));
    when(container.getAllocatedPriority()).thenReturn(Priority.UNDEFINED);
    when(container.getCreationTime()).thenReturn(Integer.MAX_VALUE + 1L);
    when(container.getFinishTime()).thenReturn(Integer.MAX_VALUE + 2L);
    when(container.getDiagnosticsInfo()).thenReturn("test diagnostics info");
    when(container.getContainerExitStatus()).thenReturn(-1);
    when(container.getContainerState()).thenReturn(ContainerState.COMPLETE);
    Container mockContainer = mock(Container.class);
    when(container.getContainer()).thenReturn(mockContainer);
    when(mockContainer.getNodeHttpAddress())
      .thenReturn("http://localhost:1234");
    return container;
  }

}
