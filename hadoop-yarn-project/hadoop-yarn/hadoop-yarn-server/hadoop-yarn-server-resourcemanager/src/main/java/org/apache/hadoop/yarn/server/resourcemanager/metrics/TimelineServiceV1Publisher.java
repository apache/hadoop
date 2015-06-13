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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

public class TimelineServiceV1Publisher extends
    AbstractTimelineServicePublisher {

  private static final Log LOG = LogFactory
      .getLog(TimelineServiceV1Publisher.class);

  public TimelineServiceV1Publisher() {
    super("TimelineserviceV1Publisher");
  }

  private TimelineClient client;

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    client = TimelineClient.createTimelineClient();
    addIfService(client);
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
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(
        ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    entity.addEvent(tEvent);
    putEntity(entity);
  }

  @Override
  void publishApplicationFinishedEvent(ApplicationFinishedEvent event) {
    TimelineEntity entity = createApplicationEntity(event.getApplicationId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
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
    RMAppMetrics appMetrics = event.getAppMetrics();
    entity.addOtherInfo(ApplicationMetricsConstants.APP_CPU_METRICS,
        appMetrics.getVcoreSeconds());
    entity.addOtherInfo(ApplicationMetricsConstants.APP_MEM_METRICS,
        appMetrics.getMemorySeconds());
    tEvent.setEventInfo(eventInfo);

    entity.addEvent(tEvent);
    putEntity(entity);
  }

  @Override
  void publishApplicationUpdatedEvent(ApplicationUpdatedEvent event) {
    TimelineEntity entity = createApplicationEntity(event.getApplicationId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
        event.getQueue());
    eventInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO, event
        .getApplicationPriority().getPriority());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    putEntity(entity);
  }

  @Override
  void publishApplicationStateUpdatedEvent(
      ApplicaitonStateUpdatedEvent event) {
    TimelineEntity entity = createApplicationEntity(event.getApplicationId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        event.getAppState());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE);
   tEvent.setTimestamp(event.getTimestamp());
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    putEntity(entity);
  }

  @Override
  void publishApplicationACLsUpdatedEvent(ApplicationACLsUpdatedEvent event) {
    TimelineEntity entity = createApplicationEntity(event.getApplicationId());

    TimelineEvent tEvent = new TimelineEvent();
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
        event.getViewAppACLs());
    entity.setOtherInfo(entityInfo);
    tEvent.setEventType(ApplicationMetricsConstants.ACLS_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());

    entity.addEvent(tEvent);
    putEntity(entity);
  }

  private static TimelineEntity createApplicationEntity(
      ApplicationId applicationId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(ApplicationMetricsConstants.ENTITY_TYPE);
    entity.setEntityId(applicationId.toString());
    return entity;
  }

  @Override
  void publishAppAttemptRegisteredEvent(AppAttemptRegisteredEvent event) {
    TimelineEntity entity =
        createAppAttemptEntity(event.getApplicationAttemptId());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(
        AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
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
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    putEntity(entity);
  }

  @Override
  void publishAppAttemptFinishedEvent(AppAttemptFinishedEvent event) {
    TimelineEntity entity =
        createAppAttemptEntity(event.getApplicationAttemptId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(AppAttemptMetricsConstants.FINISHED_EVENT_TYPE);
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
    tEvent.setEventInfo(eventInfo);

    entity.addEvent(tEvent);
    putEntity(entity);
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

  @Override
  void publishContainerCreatedEvent(ContainerCreatedEvent event) {
    TimelineEntity entity = createContainerEntity(event.getContainerId());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_ENTITY_INFO,
        event.getAllocatedResource().getMemorySize());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_ENTITY_INFO, event
        .getAllocatedResource().getVirtualCores());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO, event
        .getAllocatedNode().getHost());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO, event
        .getAllocatedNode().getPort());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PRIORITY_ENTITY_INFO,
        event.getAllocatedPriority().getPriority());
    entityInfo.put(
        ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_ENTITY_INFO,
        event.getNodeHttpAddress());
    entity.setOtherInfo(entityInfo);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ContainerMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());

    entity.addEvent(tEvent);
    putEntity(entity);
  }

  @Override
  void publishContainerFinishedEvent(ContainerFinishedEvent event) {
    TimelineEntity entity = createContainerEntity(event.getContainerId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ContainerMetricsConstants.FINISHED_EVENT_TYPE);
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
    entity.setOtherInfo(entityInfo);
    tEvent.setEventInfo(eventInfo);

    entity.addEvent(tEvent);
    putEntity(entity);
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
}
