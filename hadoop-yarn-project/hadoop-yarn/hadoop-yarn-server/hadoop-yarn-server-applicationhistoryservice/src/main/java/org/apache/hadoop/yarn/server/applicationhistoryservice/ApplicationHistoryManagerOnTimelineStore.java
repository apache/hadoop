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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

public class ApplicationHistoryManagerOnTimelineStore extends AbstractService
    implements
    ApplicationHistoryManager {

  private TimelineDataManager timelineDataManager;
  private String serverHttpAddress;

  public ApplicationHistoryManagerOnTimelineStore(
      TimelineDataManager timelineDataManager) {
    super(ApplicationHistoryManagerOnTimelineStore.class.getName());
    this.timelineDataManager = timelineDataManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    serverHttpAddress = WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    super.serviceInit(conf);
  }

  @Override
  public ApplicationReport getApplication(ApplicationId appId)
      throws YarnException, IOException {
    return getApplication(appId, ApplicationReportField.ALL);
  }

  @Override
  public Map<ApplicationId, ApplicationReport> getAllApplications()
      throws YarnException, IOException {
    TimelineEntities entities = timelineDataManager.getEntities(
        ApplicationMetricsConstants.ENTITY_TYPE, null, null, null, null,
        null, null, Long.MAX_VALUE, EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    Map<ApplicationId, ApplicationReport> apps =
        new HashMap<ApplicationId, ApplicationReport>();
    if (entities != null && entities.getEntities() != null) {
      for (TimelineEntity entity : entities.getEntities()) {
        ApplicationReport app =
            generateApplicationReport(entity, ApplicationReportField.ALL);
        apps.put(app.getApplicationId(), app);
      }
    }
    return apps;
  }

  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptReport>
      getApplicationAttempts(ApplicationId appId)
          throws YarnException, IOException {
    TimelineEntities entities = timelineDataManager.getEntities(
        AppAttemptMetricsConstants.ENTITY_TYPE,
        new NameValuePair(
            AppAttemptMetricsConstants.PARENT_PRIMARY_FILTER, appId
                .toString()), null, null, null, null, null,
        Long.MAX_VALUE, EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    Map<ApplicationAttemptId, ApplicationAttemptReport> appAttempts =
        new HashMap<ApplicationAttemptId, ApplicationAttemptReport>();
    if (entities != null && entities.getEntities() != null) {
      for (TimelineEntity entity : entities.getEntities()) {
        ApplicationAttemptReport appAttempt =
            convertToApplicationAttemptReport(entity);
        appAttempts.put(appAttempt.getApplicationAttemptId(), appAttempt);
      }
    } else {
      // It is likely that the attemtps are not found due to non-existing
      // application. In this case, we need to throw ApplicationNotFoundException.
      getApplication(appId, ApplicationReportField.NONE);
    }
    return appAttempts;
  }

  @Override
  public ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    TimelineEntity entity = timelineDataManager.getEntity(
        AppAttemptMetricsConstants.ENTITY_TYPE,
        appAttemptId.toString(), EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    if (entity == null) {
      // Will throw ApplicationNotFoundException first
      getApplication(appAttemptId.getApplicationId(), ApplicationReportField.NONE);
      throw new ApplicationAttemptNotFoundException(
          "The entity for application attempt " + appAttemptId +
          " doesn't exist in the timeline store");
    } else {
      return convertToApplicationAttemptReport(entity);
    }
  }

  @Override
  public ContainerReport getContainer(ContainerId containerId)
      throws YarnException, IOException {
    ApplicationReport app = getApplication(
        containerId.getApplicationAttemptId().getApplicationId(),
        ApplicationReportField.USER);
    TimelineEntity entity = timelineDataManager.getEntity(
        ContainerMetricsConstants.ENTITY_TYPE,
        containerId.toString(), EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    if (entity == null) {
      throw new ContainerNotFoundException(
          "The entity for container " + containerId +
          " doesn't exist in the timeline store");
    } else {
      return convertToContainerReport(entity, serverHttpAddress, app.getUser());
    }
  }

  @Override
  public ContainerReport getAMContainer(ApplicationAttemptId appAttemptId)
      throws YarnException, IOException {
    ApplicationAttemptReport appAttempt = getApplicationAttempt(appAttemptId);
    return getContainer(appAttempt.getAMContainerId());
  }

  @Override
  public Map<ContainerId, ContainerReport> getContainers(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    ApplicationReport app = getApplication(
        appAttemptId.getApplicationId(), ApplicationReportField.USER);
    TimelineEntities entities = timelineDataManager.getEntities(
        ContainerMetricsConstants.ENTITY_TYPE,
        new NameValuePair(
            ContainerMetricsConstants.PARENT_PRIMARIY_FILTER,
            appAttemptId.toString()), null, null, null,
        null, null, Long.MAX_VALUE, EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    Map<ContainerId, ContainerReport> containers =
        new HashMap<ContainerId, ContainerReport>();
    if (entities != null && entities.getEntities() != null) {
      for (TimelineEntity entity : entities.getEntities()) {
        ContainerReport container =
            convertToContainerReport(entity, serverHttpAddress, app.getUser());
        containers.put(container.getContainerId(), container);
      }
    }
    return containers;
  }

  private static ApplicationReport convertToApplicationReport(
      TimelineEntity entity, ApplicationReportField field) {
    String user = null;
    String queue = null;
    String name = null;
    String type = null;
    long createdTime = 0;
    long finishedTime = 0;
    ApplicationAttemptId latestApplicationAttemptId = null;
    String diagnosticsInfo = null;
    FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
    YarnApplicationState state = null;
    if (field == ApplicationReportField.NONE) {
      return ApplicationReport.newInstance(
          ConverterUtils.toApplicationId(entity.getEntityId()),
          latestApplicationAttemptId, user, queue, name, null, -1, null, state,
          diagnosticsInfo, null, createdTime, finishedTime, finalStatus, null,
          null, 1.0F, type, null);
    }
    Map<String, Object> entityInfo = entity.getOtherInfo();
    if (entityInfo != null) {
      if (entityInfo.containsKey(ApplicationMetricsConstants.USER_ENTITY_INFO)) {
        user =
            entityInfo.get(ApplicationMetricsConstants.USER_ENTITY_INFO)
                .toString();
      }
      if (field == ApplicationReportField.USER) {
        return ApplicationReport.newInstance(
            ConverterUtils.toApplicationId(entity.getEntityId()),
            latestApplicationAttemptId, user, queue, name, null, -1, null, state,
            diagnosticsInfo, null, createdTime, finishedTime, finalStatus, null,
            null, 1.0F, type, null);
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)) {
        queue =
            entityInfo.get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.NAME_ENTITY_INFO)) {
        name =
            entityInfo.get(ApplicationMetricsConstants.NAME_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.TYPE_ENTITY_INFO)) {
        type =
            entityInfo.get(ApplicationMetricsConstants.TYPE_ENTITY_INFO)
                .toString();
      }
    }
    List<TimelineEvent> events = entity.getEvents();
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getEventType().equals(
            ApplicationMetricsConstants.CREATED_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
          finishedTime = event.getTimestamp();
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)) {
            latestApplicationAttemptId =
                ConverterUtils
                    .toApplicationAttemptId(
                    eventInfo
                        .get(
                            ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)
                        .toString());
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)) {
            diagnosticsInfo =
                eventInfo.get(
                    ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO)) {
            finalStatus =
                FinalApplicationStatus.valueOf(eventInfo.get(
                    ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO)
                    .toString());
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.STATE_EVENT_INFO)) {
            state =
                YarnApplicationState.valueOf(eventInfo.get(
                    ApplicationMetricsConstants.STATE_EVENT_INFO).toString());
          }
        }
      }
    }
    return ApplicationReport.newInstance(
        ConverterUtils.toApplicationId(entity.getEntityId()),
        latestApplicationAttemptId, user, queue, name, null, -1, null, state,
        diagnosticsInfo, null, createdTime, finishedTime, finalStatus, null,
        null, 1.0F, type, null);
  }

  private static ApplicationAttemptReport convertToApplicationAttemptReport(
      TimelineEntity entity) {
    String host = null;
    int rpcPort = -1;
    ContainerId amContainerId = null;
    String trackingUrl = null;
    String originalTrackingUrl = null;
    String diagnosticsInfo = null;
    YarnApplicationAttemptState state = null;
    List<TimelineEvent> events = entity.getEvents();
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getEventType().equals(
            AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo.containsKey(AppAttemptMetricsConstants.HOST_EVENT_INFO)) {
            host =
                eventInfo.get(AppAttemptMetricsConstants.HOST_EVENT_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.RPC_PORT_EVENT_INFO)) {
            rpcPort = (Integer) eventInfo.get(
                    AppAttemptMetricsConstants.RPC_PORT_EVENT_INFO);
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_EVENT_INFO)) {
            amContainerId =
                ConverterUtils.toContainerId(eventInfo.get(
                    AppAttemptMetricsConstants.MASTER_CONTAINER_EVENT_INFO)
                    .toString());
          }
        } else if (event.getEventType().equals(
            AppAttemptMetricsConstants.FINISHED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.TRACKING_URL_EVENT_INFO)) {
            trackingUrl =
                eventInfo.get(
                    AppAttemptMetricsConstants.TRACKING_URL_EVENT_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_EVENT_INFO)) {
            originalTrackingUrl =
                eventInfo
                    .get(
                        AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_EVENT_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)) {
            diagnosticsInfo =
                eventInfo.get(
                    AppAttemptMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.STATE_EVENT_INFO)) {
            state =
                YarnApplicationAttemptState.valueOf(eventInfo.get(
                    AppAttemptMetricsConstants.STATE_EVENT_INFO)
                    .toString());
          }
        }
      }
    }
    return ApplicationAttemptReport.newInstance(
        ConverterUtils.toApplicationAttemptId(entity.getEntityId()),
        host, rpcPort, trackingUrl, originalTrackingUrl, diagnosticsInfo,
        state, amContainerId);
  }

  private static ContainerReport convertToContainerReport(
      TimelineEntity entity, String serverHttpAddress, String user) {
    int allocatedMem = 0;
    int allocatedVcore = 0;
    String allocatedHost = null;
    int allocatedPort = -1;
    int allocatedPriority = 0;
    long createdTime = 0;
    long finishedTime = 0;
    String diagnosticsInfo = null;
    int exitStatus = ContainerExitStatus.INVALID;
    ContainerState state = null;
    Map<String, Object> entityInfo = entity.getOtherInfo();
    if (entityInfo != null) {
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_MEMORY_ENTITY_INFO)) {
        allocatedMem = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_MEMORY_ENTITY_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_VCORE_ENTITY_INFO)) {
        allocatedVcore = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_VCORE_ENTITY_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO)) {
        allocatedHost =
            entityInfo
                .get(ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO)) {
        allocatedPort = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_PRIORITY_ENTITY_INFO)) {
        allocatedPriority = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_PRIORITY_ENTITY_INFO);
      }
    }
    List<TimelineEvent> events = entity.getEvents();
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getEventType().equals(
            ContainerMetricsConstants.CREATED_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getEventType().equals(
            ContainerMetricsConstants.FINISHED_EVENT_TYPE)) {
          finishedTime = event.getTimestamp();
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(ContainerMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)) {
            diagnosticsInfo =
                eventInfo.get(
                    ContainerMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(ContainerMetricsConstants.EXIT_STATUS_EVENT_INFO)) {
            exitStatus = (Integer) eventInfo.get(
                    ContainerMetricsConstants.EXIT_STATUS_EVENT_INFO);
          }
          if (eventInfo
              .containsKey(ContainerMetricsConstants.STATE_EVENT_INFO)) {
            state =
                ContainerState.valueOf(eventInfo.get(
                    ContainerMetricsConstants.STATE_EVENT_INFO).toString());
          }
        }
      }
    }
    NodeId allocatedNode = NodeId.newInstance(allocatedHost, allocatedPort);
    ContainerId containerId =
        ConverterUtils.toContainerId(entity.getEntityId());
    String logUrl = WebAppUtils.getAggregatedLogURL(
        serverHttpAddress,
        allocatedNode.toString(),
        containerId.toString(),
        containerId.toString(),
        user);
    return ContainerReport.newInstance(
        ConverterUtils.toContainerId(entity.getEntityId()),
        Resource.newInstance(allocatedMem, allocatedVcore),
        NodeId.newInstance(allocatedHost, allocatedPort),
        Priority.newInstance(allocatedPriority),
        createdTime, finishedTime, diagnosticsInfo, logUrl, exitStatus, state);
  }

  private ApplicationReport generateApplicationReport(TimelineEntity entity,
      ApplicationReportField field) throws YarnException, IOException {
    ApplicationReport app = convertToApplicationReport(entity, field);
    if (field == ApplicationReportField.ALL &&
        app != null && app.getCurrentApplicationAttemptId() != null) {
      ApplicationAttemptReport appAttempt =
          getApplicationAttempt(app.getCurrentApplicationAttemptId());
      if (appAttempt != null) {
        app.setHost(appAttempt.getHost());
        app.setRpcPort(appAttempt.getRpcPort());
        app.setTrackingUrl(appAttempt.getTrackingUrl());
        app.setOriginalTrackingUrl(appAttempt.getOriginalTrackingUrl());
      }
    }
    return app;
  }

  private ApplicationReport getApplication(ApplicationId appId,
      ApplicationReportField field) throws YarnException, IOException {
    TimelineEntity entity = timelineDataManager.getEntity(
        ApplicationMetricsConstants.ENTITY_TYPE,
        appId.toString(), EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    if (entity == null) {
      throw new ApplicationNotFoundException("The entity for application " +
          appId + " doesn't exist in the timeline store");
    } else {
      return generateApplicationReport(entity, field);
    }
  }

  private static enum ApplicationReportField {
    ALL, // retrieve all the fields
    NONE, // retrieve no fields
    USER // retrieve user info only
  }

}
