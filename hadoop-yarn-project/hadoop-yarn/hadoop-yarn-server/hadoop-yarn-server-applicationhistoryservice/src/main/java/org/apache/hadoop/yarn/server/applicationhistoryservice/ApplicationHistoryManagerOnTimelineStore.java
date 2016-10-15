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
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

public class ApplicationHistoryManagerOnTimelineStore extends AbstractService
    implements
    ApplicationHistoryManager {
  private static final Log LOG = LogFactory
      .getLog(ApplicationHistoryManagerOnTimelineStore.class);

  @VisibleForTesting
  static final String UNAVAILABLE = "N/A";

  private TimelineDataManager timelineDataManager;
  private ApplicationACLsManager aclsManager;
  private String serverHttpAddress;
  private long maxLoadedApplications;

  public ApplicationHistoryManagerOnTimelineStore(
      TimelineDataManager timelineDataManager,
      ApplicationACLsManager aclsManager) {
    super(ApplicationHistoryManagerOnTimelineStore.class.getName());
    this.timelineDataManager = timelineDataManager;
    this.aclsManager = aclsManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    serverHttpAddress = WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    maxLoadedApplications =
        conf.getLong(YarnConfiguration.APPLICATION_HISTORY_MAX_APPS,
          YarnConfiguration.DEFAULT_APPLICATION_HISTORY_MAX_APPS);
    super.serviceInit(conf);
  }

  @Override
  public ApplicationReport getApplication(ApplicationId appId)
      throws YarnException, IOException {
    return getApplication(appId, ApplicationReportField.ALL).appReport;
  }

  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd) throws YarnException,
      IOException {
    TimelineEntities entities =
        timelineDataManager.getEntities(
          ApplicationMetricsConstants.ENTITY_TYPE, null, null,
          appStartedTimeBegin, appStartedTimeEnd, null, null,
          appsNum == Long.MAX_VALUE ? this.maxLoadedApplications : appsNum,
          EnumSet.allOf(Field.class), UserGroupInformation.getLoginUser());
    Map<ApplicationId, ApplicationReport> apps =
        new LinkedHashMap<ApplicationId, ApplicationReport>();
    if (entities != null && entities.getEntities() != null) {
      for (TimelineEntity entity : entities.getEntities()) {
        try {
          ApplicationReportExt app =
              generateApplicationReport(entity, ApplicationReportField.ALL);
          apps.put(app.appReport.getApplicationId(), app.appReport);
        } catch (Exception e) {
          LOG.error("Error on generating application report for " +
              entity.getEntityId(), e);
        }
      }
    }
    return apps;
  }

  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptReport>
      getApplicationAttempts(ApplicationId appId)
          throws YarnException, IOException {
    ApplicationReportExt app = getApplication(
        appId, ApplicationReportField.USER_AND_ACLS);
    checkAccess(app);
    TimelineEntities entities = timelineDataManager.getEntities(
        AppAttemptMetricsConstants.ENTITY_TYPE,
        new NameValuePair(
            AppAttemptMetricsConstants.PARENT_PRIMARY_FILTER, appId
                .toString()), null, null, null, null, null,
        Long.MAX_VALUE, EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    Map<ApplicationAttemptId, ApplicationAttemptReport> appAttempts =
        new LinkedHashMap<ApplicationAttemptId, ApplicationAttemptReport>();
    for (TimelineEntity entity : entities.getEntities()) {
      ApplicationAttemptReport appAttempt =
          convertToApplicationAttemptReport(entity);
      appAttempts.put(appAttempt.getApplicationAttemptId(), appAttempt);
    }
    return appAttempts;
  }

  @Override
  public ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    return getApplicationAttempt(appAttemptId, true);
  }

  private ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId, boolean checkACLs)
      throws YarnException, IOException {
    if (checkACLs) {
      ApplicationReportExt app = getApplication(
          appAttemptId.getApplicationId(),
          ApplicationReportField.USER_AND_ACLS);
      checkAccess(app);
    }
    TimelineEntity entity = timelineDataManager.getEntity(
        AppAttemptMetricsConstants.ENTITY_TYPE,
        appAttemptId.toString(), EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    if (entity == null) {
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
    ApplicationReportExt app = getApplication(
        containerId.getApplicationAttemptId().getApplicationId(),
        ApplicationReportField.USER_AND_ACLS);
    checkAccess(app);
    TimelineEntity entity = timelineDataManager.getEntity(
        ContainerMetricsConstants.ENTITY_TYPE,
        containerId.toString(), EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    if (entity == null) {
      throw new ContainerNotFoundException(
          "The entity for container " + containerId +
          " doesn't exist in the timeline store");
    } else {
      return convertToContainerReport(
          entity, serverHttpAddress, app.appReport.getUser());
    }
  }

  @Override
  public ContainerReport getAMContainer(ApplicationAttemptId appAttemptId)
      throws YarnException, IOException {
    ApplicationAttemptReport appAttempt =
        getApplicationAttempt(appAttemptId, false);
    return getContainer(appAttempt.getAMContainerId());
  }

  @Override
  public Map<ContainerId, ContainerReport> getContainers(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    ApplicationReportExt app = getApplication(
        appAttemptId.getApplicationId(), ApplicationReportField.USER_AND_ACLS);
    checkAccess(app);
    TimelineEntities entities = timelineDataManager.getEntities(
        ContainerMetricsConstants.ENTITY_TYPE,
        new NameValuePair(
            ContainerMetricsConstants.PARENT_PRIMARIY_FILTER,
            appAttemptId.toString()), null, null, null,
        null, null, Long.MAX_VALUE, EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    Map<ContainerId, ContainerReport> containers =
        new LinkedHashMap<ContainerId, ContainerReport>();
    if (entities != null && entities.getEntities() != null) {
      for (TimelineEntity entity : entities.getEntities()) {
        ContainerReport container = convertToContainerReport(
            entity, serverHttpAddress, app.appReport.getUser());
        containers.put(container.getContainerId(), container);
      }
    }
    return containers;
  }

  private static ApplicationReportExt convertToApplicationReport(
      TimelineEntity entity, ApplicationReportField field) {
    String user = null;
    String queue = null;
    String name = null;
    String type = null;
    boolean unmanagedApplication = false;
    long createdTime = 0;
    long finishedTime = 0;
    float progress = 0.0f;
    int applicationPriority = 0;
    ApplicationAttemptId latestApplicationAttemptId = null;
    String diagnosticsInfo = null;
    FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
    YarnApplicationState state = YarnApplicationState.ACCEPTED;
    ApplicationResourceUsageReport appResources = null;
    Set<String> appTags = null;
    Map<ApplicationAccessType, String> appViewACLs =
        new HashMap<ApplicationAccessType, String>();
    String appNodeLabelExpression = null;
    String amNodeLabelExpression = null;
    Map<String, Object> entityInfo = entity.getOtherInfo();
    if (entityInfo != null) {
      if (entityInfo.containsKey(ApplicationMetricsConstants.USER_ENTITY_INFO)) {
        user =
            entityInfo.get(ApplicationMetricsConstants.USER_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO)) {
        String appViewACLsStr = entityInfo.get(
            ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO).toString();
        if (appViewACLsStr.length() > 0) {
          appViewACLs.put(ApplicationAccessType.VIEW_APP, appViewACLsStr);
        }
      }
      if (field == ApplicationReportField.USER_AND_ACLS) {
        return new ApplicationReportExt(ApplicationReport.newInstance(
            ApplicationId.fromString(entity.getEntityId()),
            latestApplicationAttemptId, user, queue, name, null, -1, null,
            state, diagnosticsInfo, null, createdTime, finishedTime,
            finalStatus, null, null, progress, type, null, appTags,
            unmanagedApplication, Priority.newInstance(applicationPriority),
            appNodeLabelExpression, amNodeLabelExpression), appViewACLs);
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
      if (entityInfo.containsKey(ApplicationMetricsConstants.TYPE_ENTITY_INFO)) {
        type =
            entityInfo.get(ApplicationMetricsConstants.TYPE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)) {
        unmanagedApplication =
            Boolean.parseBoolean(entityInfo.get(
                ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)
                .toString());
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO)) {
        applicationPriority = Integer.parseInt(entityInfo.get(
            ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO).toString());
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION)) {
        appNodeLabelExpression = entityInfo
            .get(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION).toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION)) {
        amNodeLabelExpression =
            entityInfo.get(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION)
                .toString();
      }

      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_CPU_METRICS)) {
        long vcoreSeconds=Long.parseLong(entityInfo.get(
                ApplicationMetricsConstants.APP_CPU_METRICS).toString());
        long memorySeconds=Long.parseLong(entityInfo.get(
                ApplicationMetricsConstants.APP_MEM_METRICS).toString());
        appResources = ApplicationResourceUsageReport
            .newInstance(0, 0, null, null, null, memorySeconds, vcoreSeconds, 0,
                0);
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_TAGS_INFO)) {
        appTags = new HashSet<String>();
        Object obj = entityInfo.get(ApplicationMetricsConstants.APP_TAGS_INFO);
        if (obj != null && obj instanceof Collection<?>) {
          for(Object o : (Collection<?>)obj) {
            if (o != null) {
              appTags.add(o.toString());
            }
          }
        }
      }
    }
    List<TimelineEvent> events = entity.getEvents();
    long updatedTimeStamp = 0L;
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getEventType().equals(
            ApplicationMetricsConstants.CREATED_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.UPDATED_EVENT_TYPE)) {
          // This type of events are parsed in time-stamp descending order
          // which means the previous event could override the information
          // from the later same type of event. Hence compare timestamp
          // before over writing.
          if (event.getTimestamp() > updatedTimeStamp) {
            updatedTimeStamp = event.getTimestamp();
          } else {
            continue;
          }

          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          applicationPriority = Integer
              .parseInt(eventInfo.get(
                  ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO)
                  .toString());
          queue = eventInfo.get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)
              .toString();
        } else if (event.getEventType().equals(
              ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo.containsKey(
              ApplicationMetricsConstants.STATE_EVENT_INFO)) {
            if (!isFinalState(state)) {
              state = YarnApplicationState.valueOf(eventInfo.get(
                  ApplicationMetricsConstants.STATE_EVENT_INFO).toString());
            }
          }
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
          progress=1.0F;
          finishedTime = event.getTimestamp();
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)) {
            latestApplicationAttemptId = ApplicationAttemptId.fromString(
                eventInfo.get(
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
    return new ApplicationReportExt(ApplicationReport.newInstance(
        ApplicationId.fromString(entity.getEntityId()),
        latestApplicationAttemptId, user, queue, name, null, -1, null, state,
        diagnosticsInfo, null, createdTime, finishedTime, finalStatus,
        appResources, null, progress, type, null, appTags, unmanagedApplication,
        Priority.newInstance(applicationPriority), appNodeLabelExpression,
        amNodeLabelExpression), appViewACLs);
  }

  private static boolean isFinalState(YarnApplicationState state) {
    return state == YarnApplicationState.FINISHED
        || state == YarnApplicationState.FAILED
        || state == YarnApplicationState.KILLED;
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
          if (eventInfo.containsKey(AppAttemptMetricsConstants.HOST_INFO)) {
            host =
                eventInfo.get(AppAttemptMetricsConstants.HOST_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.RPC_PORT_INFO)) {
            rpcPort = (Integer) eventInfo.get(
                    AppAttemptMetricsConstants.RPC_PORT_INFO);
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)) {
            amContainerId =
                ContainerId.fromString(eventInfo.get(
                    AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)
                    .toString());
          }
        } else if (event.getEventType().equals(
            AppAttemptMetricsConstants.FINISHED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.TRACKING_URL_INFO)) {
            trackingUrl =
                eventInfo.get(
                    AppAttemptMetricsConstants.TRACKING_URL_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(
                  AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO)) {
            originalTrackingUrl =
                eventInfo
                    .get(
                        AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.DIAGNOSTICS_INFO)) {
            diagnosticsInfo =
                eventInfo.get(
                    AppAttemptMetricsConstants.DIAGNOSTICS_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.STATE_INFO)) {
            state =
                YarnApplicationAttemptState.valueOf(eventInfo.get(
                    AppAttemptMetricsConstants.STATE_INFO)
                    .toString());
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)) {
            amContainerId =
                ContainerId.fromString(eventInfo.get(
                    AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)
                    .toString());
          }
        }
      }
    }
    return ApplicationAttemptReport.newInstance(
        ApplicationAttemptId.fromString(entity.getEntityId()),
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
    String nodeHttpAddress = null;
    Map<String, Object> entityInfo = entity.getOtherInfo();
    if (entityInfo != null) {
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO)) {
        allocatedMem = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_MEMORY_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_VCORE_INFO)) {
        allocatedVcore = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_VCORE_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_HOST_INFO)) {
        allocatedHost =
            entityInfo
                .get(ContainerMetricsConstants.ALLOCATED_HOST_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_PORT_INFO)) {
        allocatedPort = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_PORT_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO)) {
        allocatedPriority = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO);
      }
      if (entityInfo.containsKey(
          ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO)) {
        nodeHttpAddress =
            (String) entityInfo
              .get(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO);
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
              .containsKey(ContainerMetricsConstants.DIAGNOSTICS_INFO)) {
            diagnosticsInfo =
                eventInfo.get(
                    ContainerMetricsConstants.DIAGNOSTICS_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(ContainerMetricsConstants.EXIT_STATUS_INFO)) {
            exitStatus = (Integer) eventInfo.get(
                    ContainerMetricsConstants.EXIT_STATUS_INFO);
          }
          if (eventInfo
              .containsKey(ContainerMetricsConstants.STATE_INFO)) {
            state =
                ContainerState.valueOf(eventInfo.get(
                    ContainerMetricsConstants.STATE_INFO).toString());
          }
        }
      }
    }
    ContainerId containerId =
        ContainerId.fromString(entity.getEntityId());
    String logUrl = null;
    NodeId allocatedNode = null;
    if (allocatedHost != null) {
      allocatedNode = NodeId.newInstance(allocatedHost, allocatedPort);
      logUrl = WebAppUtils.getAggregatedLogURL(
          serverHttpAddress,
          allocatedNode.toString(),
          containerId.toString(),
          containerId.toString(),
          user);
    }
    return ContainerReport.newInstance(
        ContainerId.fromString(entity.getEntityId()),
        Resource.newInstance(allocatedMem, allocatedVcore), allocatedNode,
        Priority.newInstance(allocatedPriority),
        createdTime, finishedTime, diagnosticsInfo, logUrl, exitStatus, state,
        nodeHttpAddress);
  }

  private ApplicationReportExt generateApplicationReport(TimelineEntity entity,
      ApplicationReportField field) throws YarnException, IOException {
    ApplicationReportExt app = convertToApplicationReport(entity, field);
    // If only user and acls are pulled to check attempt(s)/container(s) access
    // control, we can return immediately
    if (field == ApplicationReportField.USER_AND_ACLS) {
      return app;
    }
    try {
      checkAccess(app);
      if (app.appReport.getCurrentApplicationAttemptId() != null) {
        ApplicationAttemptReport appAttempt = getApplicationAttempt(
            app.appReport.getCurrentApplicationAttemptId(), false);
        app.appReport.setHost(appAttempt.getHost());
        app.appReport.setRpcPort(appAttempt.getRpcPort());
        app.appReport.setTrackingUrl(appAttempt.getTrackingUrl());
        app.appReport.setOriginalTrackingUrl(appAttempt.getOriginalTrackingUrl());
      }
    } catch (AuthorizationException | ApplicationAttemptNotFoundException e) {
      // AuthorizationException is thrown because the user doesn't have access
      if (e instanceof AuthorizationException) {
        LOG.warn("Failed to authorize when generating application report for "
            + app.appReport.getApplicationId()
            + ". Use a placeholder for its latest attempt id. ", e);
      } else { // Attempt not found
        LOG.info("No application attempt found for "
            + app.appReport.getApplicationId()
            + ". Use a placeholder for its latest attempt id. ", e);
      }
      // It's possible that the app is finished before the first attempt is created.
      app.appReport.setDiagnostics(null);
      app.appReport.setCurrentApplicationAttemptId(null);
    }
    if (app.appReport.getCurrentApplicationAttemptId() == null) {
      app.appReport.setCurrentApplicationAttemptId(
          ApplicationAttemptId.newInstance(app.appReport.getApplicationId(), -1));
    }
    if (app.appReport.getHost() == null) {
      app.appReport.setHost(UNAVAILABLE);
    }
    if (app.appReport.getRpcPort() < 0) {
      app.appReport.setRpcPort(-1);
    }
    if (app.appReport.getTrackingUrl() == null) {
      app.appReport.setTrackingUrl(UNAVAILABLE);
    }
    if (app.appReport.getOriginalTrackingUrl() == null) {
      app.appReport.setOriginalTrackingUrl(UNAVAILABLE);
    }
    if (app.appReport.getDiagnostics() == null) {
      app.appReport.setDiagnostics("");
    }
    return app;
  }

  private ApplicationReportExt getApplication(ApplicationId appId,
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

   private void checkAccess(ApplicationReportExt app)
           throws YarnException, IOException {
     if (app.appViewACLs != null) {
       aclsManager.addApplication(
           app.appReport.getApplicationId(), app.appViewACLs);
       try {
         if (!aclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
             ApplicationAccessType.VIEW_APP, app.appReport.getUser(),
             app.appReport.getApplicationId())) {
           throw new AuthorizationException("User "
               + UserGroupInformation.getCurrentUser().getShortUserName()
               + " does not have privilage to see this application "
               + app.appReport.getApplicationId());
         }
       } finally {
         aclsManager.removeApplication(app.appReport.getApplicationId());
       }
     }
   }

  private static enum ApplicationReportField {
    ALL, // retrieve all the fields
    USER_AND_ACLS // retrieve user and ACLs info only
  }

  private static class ApplicationReportExt {
     private ApplicationReport appReport;
     private Map<ApplicationAccessType, String> appViewACLs;

     public ApplicationReportExt(
         ApplicationReport appReport,
         Map<ApplicationAccessType, String> appViewACLs) {
       this.appReport = appReport;
       this.appViewACLs = appViewACLs;
     }
   }
}
