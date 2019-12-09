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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * This class is responsible for posting application, appattempt &amp; Container
 * lifecycle related events to timeline service v1.
 */
public class TimelineServiceV1Publisher extends AbstractSystemMetricsPublisher {

  private static final Log LOG =
      LogFactory.getLog(TimelineServiceV1Publisher.class);

  public TimelineServiceV1Publisher() {
    super("TimelineserviceV1Publisher");
  }

  private TimelineClient client;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    client = TimelineClient.createTimelineClient();
    addIfService(client);
    super.serviceInit(conf);
    getDispatcher().register(SystemMetricsEventType.class,
        new TimelineV1EventHandler());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appCreated(RMApp app, long createdTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.NAME_ENTITY_INFO, app.getName());
    entityInfo.put(ApplicationMetricsConstants.TYPE_ENTITY_INFO,
        app.getApplicationType());
    entityInfo.put(ApplicationMetricsConstants.USER_ENTITY_INFO, app.getUser());
    entityInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
        app.getQueue());
    entityInfo.put(ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO,
        app.getSubmitTime());
    entityInfo.put(ApplicationMetricsConstants.APP_TAGS_INFO,
        app.getApplicationTags());
    entityInfo.put(
        ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO,
        app.getApplicationSubmissionContext().getUnmanagedAM());
    entityInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO,
        app.getApplicationPriority().getPriority());
    entityInfo.put(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION,
        app.getAmNodeLabelExpression());
    entityInfo.put(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION,
        app.getAppNodeLabelExpression());
    if (app.getCallerContext() != null) {
      if (app.getCallerContext().getContext() != null) {
        entityInfo.put(ApplicationMetricsConstants.YARN_APP_CALLER_CONTEXT,
            app.getCallerContext().getContext());
      }
      if (app.getCallerContext().getSignature() != null) {
        entityInfo.put(ApplicationMetricsConstants.YARN_APP_CALLER_SIGNATURE,
            app.getCallerContext().getSignature());
      }
    }

    ContainerLaunchContext amContainerSpec =
        app.getApplicationSubmissionContext().getAMContainerSpec();
    entityInfo.put(ApplicationMetricsConstants.AM_CONTAINER_LAUNCH_COMMAND,
        amContainerSpec.getCommands());
    entityInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        RMServerUtils.createApplicationState(app.getState()).toString());

    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(createdTime);

    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @Override
  public void appFinished(RMApp app, RMAppState state, long finishedTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(finishedTime);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        app.getDiagnostics().toString());
    eventInfo.put(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO,
        app.getFinalApplicationStatus().toString());
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        RMServerUtils.createApplicationState(state).toString());
    String latestApplicationAttemptId = app.getCurrentAppAttempt() == null
        ? null : app.getCurrentAppAttempt().getAppAttemptId().toString();
    if (latestApplicationAttemptId != null) {
      eventInfo.put(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO,
          latestApplicationAttemptId);
    }
    RMAppMetrics appMetrics = app.getRMAppMetrics();
    entity.addOtherInfo(ApplicationMetricsConstants.APP_CPU_METRICS,
        appMetrics.getVcoreSeconds());
    entity.addOtherInfo(ApplicationMetricsConstants.APP_MEM_METRICS,
        appMetrics.getMemorySeconds());
    entity.addOtherInfo(ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS,
            appMetrics.getPreemptedMemorySeconds());
    entity.addOtherInfo(ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS,
            appMetrics.getPreemptedVcoreSeconds());
    tEvent.setEventInfo(eventInfo);

    entity.addEvent(tEvent);

    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appUpdated(RMApp app, long updatedTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
        app.getQueue());
    eventInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO,
        app.getApplicationPriority().getPriority());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(updatedTime);
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appStateUpdated(RMApp app, YarnApplicationState appState,
      long updatedTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        appState);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(updatedTime);
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appACLsUpdated(RMApp app, String appViewACLs, long updatedTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());
    TimelineEvent tEvent = new TimelineEvent();
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
        (appViewACLs == null) ? "" : appViewACLs);
    entity.setOtherInfo(entityInfo);
    tEvent.setEventType(ApplicationMetricsConstants.ACLS_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(updatedTime);

    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appAttemptRegistered(RMAppAttempt appAttempt,
      long registeredTime) {
    TimelineEntity entity =
        createAppAttemptEntity(appAttempt.getAppAttemptId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
    tEvent.setTimestamp(registeredTime);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(AppAttemptMetricsConstants.TRACKING_URL_INFO,
        appAttempt.getTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO,
        appAttempt.getOriginalTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.HOST_INFO,
        appAttempt.getHost());
    eventInfo.put(AppAttemptMetricsConstants.RPC_PORT_INFO,
        appAttempt.getRpcPort());
    if (appAttempt.getMasterContainer() != null) {
      eventInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO,
          appAttempt.getMasterContainer().getId().toString());
    }
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(
        new TimelineV1PublishEvent(SystemMetricsEventType.PUBLISH_ENTITY,
            entity, appAttempt.getAppAttemptId().getApplicationId()));

  }

  @SuppressWarnings("unchecked")
  @Override
  public void appAttemptFinished(RMAppAttempt appAttempt,
      RMAppAttemptState appAttemtpState, RMApp app, long finishedTime) {
    TimelineEntity entity =
        createAppAttemptEntity(appAttempt.getAppAttemptId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(AppAttemptMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(finishedTime);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(AppAttemptMetricsConstants.TRACKING_URL_INFO,
        appAttempt.getTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO,
        appAttempt.getOriginalTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.DIAGNOSTICS_INFO,
        appAttempt.getDiagnostics());
    eventInfo.put(AppAttemptMetricsConstants.FINAL_STATUS_INFO,
        app.getFinalApplicationStatus().toString());
    eventInfo.put(AppAttemptMetricsConstants.STATE_INFO, RMServerUtils
        .createApplicationAttemptState(appAttemtpState).toString());
    if (appAttempt.getMasterContainer() != null) {
      eventInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO,
          appAttempt.getMasterContainer().getId().toString());
    }
    tEvent.setEventInfo(eventInfo);

    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(
        new TimelineV1PublishEvent(SystemMetricsEventType.PUBLISH_ENTITY,
            entity, appAttempt.getAppAttemptId().getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void containerCreated(RMContainer container, long createdTime) {
    TimelineEntity entity = createContainerEntity(container.getContainerId());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO,
        container.getAllocatedResource().getMemorySize());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_INFO,
        container.getAllocatedResource().getVirtualCores());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_INFO,
        container.getAllocatedNode().getHost());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_INFO,
        container.getAllocatedNode().getPort());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO,
        container.getAllocatedPriority().getPriority());
    entityInfo.put(
        ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO,
        container.getNodeHttpAddress());
    entity.setOtherInfo(entityInfo);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ContainerMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(createdTime);

    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, container
            .getContainerId().getApplicationAttemptId().getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void containerFinished(RMContainer container, long finishedTime) {
    TimelineEntity entity = createContainerEntity(container.getContainerId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ContainerMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(finishedTime);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
        container.getDiagnosticsInfo());
    eventInfo.put(ContainerMetricsConstants.EXIT_STATUS_INFO,
        container.getContainerExitStatus());
    eventInfo.put(ContainerMetricsConstants.STATE_INFO,
        container.getContainerState().toString());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_INFO,
        container.getAllocatedNode().getHost());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_INFO,
        container.getAllocatedNode().getPort());
    entity.setOtherInfo(entityInfo);
    tEvent.setEventInfo(eventInfo);

    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, container
            .getContainerId().getApplicationAttemptId().getApplicationId()));
  }

  private static TimelineEntity createApplicationEntity(
      ApplicationId applicationId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(ApplicationMetricsConstants.ENTITY_TYPE);
    entity.setEntityId(applicationId.toString());
    return entity;
  }

  private static TimelineEntity createAppAttemptEntity(
      ApplicationAttemptId appAttemptId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(AppAttemptMetricsConstants.ENTITY_TYPE);
    entity.setEntityId(appAttemptId.toString());
    entity.addPrimaryFilter(AppAttemptMetricsConstants.PARENT_PRIMARY_FILTER,
        appAttemptId.getApplicationId().toString());
    return entity;
  }

  private static TimelineEntity createContainerEntity(ContainerId containerId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(ContainerMetricsConstants.ENTITY_TYPE);
    entity.setEntityId(containerId.toString());
    entity.addPrimaryFilter(ContainerMetricsConstants.PARENT_PRIMARIY_FILTER,
        containerId.getApplicationAttemptId().toString());
    return entity;
  }

  private void putEntity(TimelineEntity entity) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Publishing the entity " + entity.getEntityId()
            + ", JSON-style content: "
            + TimelineUtils.dumpTimelineRecordtoJSON(entity));
      }
      client.putEntities(entity);
    } catch (Exception e) {
      LOG.error("Error when publishing entity [" + entity.getEntityType() + ","
          + entity.getEntityId() + "]", e);
    }
  }

  private class TimelineV1PublishEvent extends TimelinePublishEvent {
    private TimelineEntity entity;

    public TimelineV1PublishEvent(SystemMetricsEventType type,
        TimelineEntity entity, ApplicationId appId) {
      super(type, appId);
      this.entity = entity;
    }

    public TimelineEntity getEntity() {
      return entity;
    }
  }

  private class TimelineV1EventHandler
      implements EventHandler<TimelineV1PublishEvent> {
    @Override
    public void handle(TimelineV1PublishEvent event) {
      putEntity(event.getEntity());
    }
  }
}
