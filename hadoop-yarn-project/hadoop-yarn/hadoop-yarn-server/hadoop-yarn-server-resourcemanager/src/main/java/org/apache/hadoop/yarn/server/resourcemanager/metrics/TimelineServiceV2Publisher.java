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
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationAttemptEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.Identifier;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollector;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * This class is responsible for posting application, appattempt & Container
 * lifecycle related events to timeline service V2
 */
@Private
@Unstable
public class TimelineServiceV2Publisher extends
    AbstractTimelineServicePublisher {
  private static final Log LOG = LogFactory
      .getLog(TimelineServiceV2Publisher.class);
  protected RMTimelineCollectorManager rmTimelineCollectorManager;

  public TimelineServiceV2Publisher(RMContext rmContext) {
    super("TimelineserviceV2Publisher");
    rmTimelineCollectorManager = rmContext.getRMTimelineCollectorManager();
  }

  private boolean publishContainerMetrics;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    publishContainerMetrics =
        conf.getBoolean(YarnConfiguration.RM_PUBLISH_CONTAINER_METRICS_ENABLED,
            YarnConfiguration.DEFAULT_RM_PUBLISH_CONTAINER_METRICS_ENABLED);
    super.serviceInit(conf);
  }

  @Override
  void publishApplicationCreatedEvent(ApplicationCreatedEvent event) {
    TimelineEntity entity =
        createApplicationEntity(event.getApplicationId());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.NAME_ENTITY_INFO,
        event.getApplicationName());
    entityInfo.put(ApplicationMetricsConstants.TYPE_ENTITY_INFO,
        event.getApplicationType());
    entityInfo.put(ApplicationMetricsConstants.USER_ENTITY_INFO,
        event.getUser());
    entityInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
        event.getQueue());
    entityInfo.put(ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO,
        event.getSubmittedTime());
    entityInfo.put(ApplicationMetricsConstants.APP_TAGS_INFO,
        event.getAppTags());
    entityInfo.put(
        ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO,
        event.isUnmanagedApp());
    entityInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO,
        event.getApplicationPriority().getPriority());
    entityInfo.put(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION,
        event.getAppNodeLabelsExpression());
    entityInfo.put(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION,
        event.getAmNodeLabelsExpression());
    if (event.getCallerContext() != null) {
      if (event.getCallerContext().getContext() != null) {
        entityInfo.put(ApplicationMetricsConstants.YARN_APP_CALLER_CONTEXT,
            event.getCallerContext().getContext());
      }
      if (event.getCallerContext().getSignature() != null) {
        entityInfo.put(ApplicationMetricsConstants.YARN_APP_CALLER_SIGNATURE,
            event.getCallerContext().getSignature());
      }
    }
    entity.setInfo(entityInfo);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    entity.addEvent(tEvent);

    putEntity(entity, event.getApplicationId());
  }

  @Override
  void publishApplicationFinishedEvent(ApplicationFinishedEvent event) {
    ApplicationEntity entity =
        createApplicationEntity(event.getApplicationId());
    RMAppMetrics appMetrics = event.getAppMetrics();
    entity.addInfo(ApplicationMetricsConstants.APP_CPU_METRICS,
        appMetrics.getVcoreSeconds());
    entity.addInfo(ApplicationMetricsConstants.APP_MEM_METRICS,
        appMetrics.getMemorySeconds());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        event.getDiagnosticsInfo());
    eventInfo.put(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO, event
        .getFinalApplicationStatus().toString());
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO, event
        .getYarnApplicationState().toString());
    if (event.getLatestApplicationAttemptId() != null) {
      eventInfo.put(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO,
          event.getLatestApplicationAttemptId().toString());
    }
    tEvent.setInfo(eventInfo);

    entity.addEvent(tEvent);
    putEntity(entity, event.getApplicationId());

    //cleaning up the collector cached
    event.getApp().stopTimelineCollector();
  }

  @Override
  void publishApplicationUpdatedEvent(ApplicationUpdatedEvent event) {
    ApplicationEntity entity =
        createApplicationEntity(event.getApplicationId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
        event.getQueue());
    eventInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO, event
        .getApplicationPriority().getPriority());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    tEvent.setInfo(eventInfo);
    entity.addEvent(tEvent);
    putEntity(entity, event.getApplicationId());
  }

  @Override
  void publishApplicationStateUpdatedEvent(
      ApplicaitonStateUpdatedEvent event) {
    ApplicationEntity entity =
        createApplicationEntity(event.getApplicationId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        event.getAppState());
    TimelineEvent tEvent = new TimelineEvent();
   tEvent.setId(ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    tEvent.setInfo(eventInfo);
    entity.addEvent(tEvent);
    putEntity(entity, event.getApplicationId());
  }

  @Override
  void publishApplicationACLsUpdatedEvent(ApplicationACLsUpdatedEvent event) {
    ApplicationEntity entity =
        createApplicationEntity(event.getApplicationId());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
        event.getViewAppACLs());
    entity.setInfo(entityInfo);

    putEntity(entity, event.getApplicationId());
  }

  private static ApplicationEntity createApplicationEntity(
      ApplicationId applicationId) {
    ApplicationEntity entity = new ApplicationEntity();
    entity.setId(applicationId.toString());
    return entity;
  }

  @Override
  void publishAppAttemptRegisteredEvent(AppAttemptRegisteredEvent event) {
    TimelineEntity entity =
        createAppAttemptEntity(event.getApplicationAttemptId());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(
        AppAttemptMetricsConstants.TRACKING_URL_EVENT_INFO,
        event.getTrackingUrl());
    eventInfo.put(
        AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_EVENT_INFO,
        event.getOriginalTrackingURL());
    eventInfo.put(AppAttemptMetricsConstants.HOST_EVENT_INFO,
        event.getHost());
    eventInfo.put(AppAttemptMetricsConstants.RPC_PORT_EVENT_INFO,
        event.getRpcPort());
    if (event.getMasterContainerId() != null) {
      eventInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_EVENT_INFO,
          event.getMasterContainerId().toString());
    }
    tEvent.setInfo(eventInfo);
    entity.addEvent(tEvent);
    putEntity(entity, event.getApplicationAttemptId().getApplicationId());
  }

  @Override
  void publishAppAttemptFinishedEvent(AppAttemptFinishedEvent event) {
    ApplicationAttemptEntity entity =
        createAppAttemptEntity(event.getApplicationAttemptId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(AppAttemptMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(AppAttemptMetricsConstants.TRACKING_URL_EVENT_INFO,
        event.getTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_EVENT_INFO,
        event.getOriginalTrackingURL());
    eventInfo.put(AppAttemptMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        event.getDiagnosticsInfo());
    eventInfo.put(AppAttemptMetricsConstants.FINAL_STATUS_EVENT_INFO, event
        .getFinalApplicationStatus().toString());
    eventInfo.put(AppAttemptMetricsConstants.STATE_EVENT_INFO, event
        .getYarnApplicationAttemptState().toString());
    if (event.getMasterContainerId() != null) {
      eventInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_EVENT_INFO,
          event.getMasterContainerId().toString());
    }
    tEvent.setInfo(eventInfo);

    entity.addEvent(tEvent);
    putEntity(entity, event.getApplicationAttemptId().getApplicationId());
  }

  @Override
  void publishContainerCreatedEvent(ContainerCreatedEvent event) {
    TimelineEntity entity = createContainerEntity(event.getContainerId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ContainerMetricsConstants.CREATED_IN_RM_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    // updated as event info instead of entity info, as entity info is updated
    // by NM
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_ENTITY_INFO, event
        .getAllocatedResource().getMemorySize());
    eventInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_ENTITY_INFO, event
        .getAllocatedResource().getVirtualCores());
    eventInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO, event
        .getAllocatedNode().getHost());
    eventInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO, event
        .getAllocatedNode().getPort());
    eventInfo.put(ContainerMetricsConstants.ALLOCATED_PRIORITY_ENTITY_INFO,
        event.getAllocatedPriority().getPriority());
    eventInfo.put(
        ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_ENTITY_INFO,
        event.getNodeHttpAddress());
    tEvent.setInfo(eventInfo);

    entity.addEvent(tEvent);
    putEntity(entity, event.getContainerId().getApplicationAttemptId()
        .getApplicationId());
  }

  @Override
  void publishContainerFinishedEvent(ContainerFinishedEvent event) {
    TimelineEntity entity = createContainerEntity(event.getContainerId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ContainerMetricsConstants.FINISHED_IN_RM_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        event.getDiagnosticsInfo());
    eventInfo.put(ContainerMetricsConstants.EXIT_STATUS_EVENT_INFO,
        event.getContainerExitStatus());
    eventInfo.put(ContainerMetricsConstants.STATE_EVENT_INFO, event
        .getContainerState().toString());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO,
        event.getAllocatedNode().getHost());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO,
        event.getAllocatedNode().getPort());
    entity.setInfo(entityInfo);
    tEvent.setInfo(eventInfo);

    entity.addEvent(tEvent);
    putEntity(entity, event.getContainerId().getApplicationAttemptId()
        .getApplicationId());
  }

  private static ContainerEntity createContainerEntity(ContainerId containerId) {
    ContainerEntity entity = new ContainerEntity();
    entity.setId(containerId.toString());
    entity.setParent(new Identifier(TimelineEntityType.YARN_APPLICATION_ATTEMPT
        .name(), containerId.getApplicationAttemptId().toString()));
    return entity;
  }

  private void putEntity(TimelineEntity entity, ApplicationId appId) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Publishing the entity " + entity + ", JSON-style content: "
            + TimelineUtils.dumpTimelineRecordtoJSON(entity));
      }
      TimelineCollector timelineCollector =
          rmTimelineCollectorManager.get(appId);
      TimelineEntities entities = new TimelineEntities();
      entities.addEntity(entity);
      timelineCollector.putEntities(entities,
          UserGroupInformation.getCurrentUser());
    } catch (Exception e) {
      LOG.error("Error when publishing entity " + entity, e);
    }
  }

  private static ApplicationAttemptEntity createAppAttemptEntity(
      ApplicationAttemptId appAttemptId) {
    ApplicationAttemptEntity entity = new ApplicationAttemptEntity();
    entity.setId(appAttemptId.toString());
    entity.setParent(new Identifier(TimelineEntityType.YARN_APPLICATION.name(),
        appAttemptId.getApplicationId().toString()));
    return entity;
  }

  @Override
  public boolean publishRMContainerMetrics() {
    return publishContainerMetrics;
  }
}
