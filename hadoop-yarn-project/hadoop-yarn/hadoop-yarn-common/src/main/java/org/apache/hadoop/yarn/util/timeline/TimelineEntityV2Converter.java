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

package org.apache.hadoop.yarn.util.timeline;

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
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import static org.apache.hadoop.yarn.util.StringHelper.PATH_JOINER;

/**
 * Utility class to generate reports from timeline entities.
 */
public final class TimelineEntityV2Converter {
  private TimelineEntityV2Converter() {
  }

  public static ContainerReport convertToContainerReport(
      TimelineEntity entity, String serverAddress, String user) {
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
    Map<String, List<Map<String, String>>> exposedPorts = null;

    Map<String, Object> entityInfo = entity.getInfo();
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
        allocatedPriority = Integer.parseInt(entityInfo.get(
            ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO).toString());
      }
      if (entityInfo.containsKey(
          ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO)) {
        nodeHttpAddress =
            (String) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO);
      }
      if (entityInfo.containsKey(
          ContainerMetricsConstants.ALLOCATED_EXPOSED_PORTS)) {
        exposedPorts =
            (Map<String, List<Map<String, String>>>) entityInfo
                .get(ContainerMetricsConstants.ALLOCATED_EXPOSED_PORTS);
      }
      if (entityInfo.containsKey(ContainerMetricsConstants.DIAGNOSTICS_INFO)) {
        diagnosticsInfo =
            entityInfo.get(
                ContainerMetricsConstants.DIAGNOSTICS_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ContainerMetricsConstants.EXIT_STATUS_INFO)) {
        exitStatus = (Integer) entityInfo.get(
            ContainerMetricsConstants.EXIT_STATUS_INFO);
      }
      if (entityInfo.containsKey(ContainerMetricsConstants.STATE_INFO)) {
        state =
            ContainerState.valueOf(entityInfo.get(
                ContainerMetricsConstants.STATE_INFO).toString());
      }
    }
    NavigableSet<TimelineEvent> events = entity.getEvents();
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getId().equals(
            ContainerMetricsConstants.CREATED_IN_RM_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getId().equals(
            ContainerMetricsConstants.FINISHED_IN_RM_EVENT_TYPE)) {
          finishedTime = event.getTimestamp();
        }
      }
    }
    String logUrl = null;
    NodeId allocatedNode = null;
    String containerId = entity.getId();
    if (allocatedHost != null) {
      allocatedNode = NodeId.newInstance(allocatedHost, allocatedPort);
      if (serverAddress != null && user != null) {
        logUrl = PATH_JOINER.join(serverAddress,
            "logs", allocatedNode, containerId, containerId, user);
      }
    }
    ContainerReport container = ContainerReport.newInstance(
        ContainerId.fromString(entity.getId()),
        Resource.newInstance(allocatedMem, allocatedVcore), allocatedNode,
        Priority.newInstance(allocatedPriority),
        createdTime, finishedTime, diagnosticsInfo, logUrl, exitStatus, state,
        nodeHttpAddress);
    container.setExposedPorts(exposedPorts);

    return container;
  }

  public static ApplicationAttemptReport convertToApplicationAttemptReport(
      TimelineEntity entity) {
    String host = null;
    int rpcPort = -1;
    ContainerId amContainerId = null;
    String trackingUrl = null;
    String originalTrackingUrl = null;
    String diagnosticsInfo = null;
    YarnApplicationAttemptState state = null;
    Map<String, Object> entityInfo = entity.getInfo();
    long startTime = 0;
    long finishTime = 0;

    if (entityInfo != null) {
      if (entityInfo.containsKey(AppAttemptMetricsConstants.HOST_INFO)) {
        host =
            entityInfo.get(AppAttemptMetricsConstants.HOST_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.RPC_PORT_INFO)) {
        rpcPort = (Integer) entityInfo.get(
            AppAttemptMetricsConstants.RPC_PORT_INFO);
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)) {
        amContainerId =
            ContainerId.fromString(entityInfo.get(
                AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)
                .toString());
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.TRACKING_URL_INFO)) {
        trackingUrl =
            entityInfo.get(
                AppAttemptMetricsConstants.TRACKING_URL_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(
              AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO)) {
        originalTrackingUrl =
            entityInfo
                .get(
                    AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.DIAGNOSTICS_INFO)) {
        diagnosticsInfo =
            entityInfo.get(
                AppAttemptMetricsConstants.DIAGNOSTICS_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.STATE_INFO)) {
        state =
            YarnApplicationAttemptState.valueOf(entityInfo.get(
                AppAttemptMetricsConstants.STATE_INFO)
                .toString());
      }
      if (entityInfo
          .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)) {
        amContainerId =
            ContainerId.fromString(entityInfo.get(
                AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)
                .toString());
      }
    }
    NavigableSet<TimelineEvent> events = entity.getEvents();
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getId().equals(
            AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE)) {
          startTime = event.getTimestamp();
        } else if (event.getId().equals(
            AppAttemptMetricsConstants.FINISHED_EVENT_TYPE)) {
          finishTime = event.getTimestamp();
        }
      }
    }
    return ApplicationAttemptReport.newInstance(
        ApplicationAttemptId.fromString(entity.getId()),
        host, rpcPort, trackingUrl, originalTrackingUrl, diagnosticsInfo,
        state, amContainerId, startTime, finishTime);
  }

  public static ApplicationReport convertToApplicationReport(
      TimelineEntity entity) {
    String user = null;
    String queue = null;
    String name = null;
    String type = null;
    boolean unmanagedApplication = false;
    long createdTime = 0;
    long launchTime = 0;
    long finishedTime = 0;
    float progress = 0.0f;
    int applicationPriority = 0;
    ApplicationAttemptId latestApplicationAttemptId = null;
    String diagnosticsInfo = null;
    FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
    YarnApplicationState state = YarnApplicationState.ACCEPTED;
    ApplicationResourceUsageReport appResources = null;
    Set<String> appTags = null;
    String appNodeLabelExpression = null;
    String amNodeLabelExpression = null;
    Map<String, Object> entityInfo = entity.getInfo();
    if (entityInfo != null) {
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.USER_ENTITY_INFO)) {
        user =
            entityInfo.get(ApplicationMetricsConstants.USER_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.QUEUE_ENTITY_INFO)) {
        queue =
            entityInfo.get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.NAME_ENTITY_INFO)) {
        name =
            entityInfo.get(ApplicationMetricsConstants.NAME_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.TYPE_ENTITY_INFO)) {
        type =
            entityInfo.get(ApplicationMetricsConstants.TYPE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.TYPE_ENTITY_INFO)) {
        type =
            entityInfo.get(ApplicationMetricsConstants.TYPE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(
              ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)) {
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
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_TAGS_INFO)) {
        appTags = new HashSet<>();
        Object obj = entityInfo.get(ApplicationMetricsConstants.APP_TAGS_INFO);
        if (obj != null && obj instanceof Collection<?>) {
          for(Object o : (Collection<?>)obj) {
            if (o != null) {
              appTags.add(o.toString());
            }
          }
        }
      }
      if (entityInfo
          .containsKey(
              ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)) {
        latestApplicationAttemptId = ApplicationAttemptId.fromString(
            entityInfo.get(
                ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)
                .toString());
      }
      if (entityInfo.containsKey(
          ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)) {
        diagnosticsInfo =
            entityInfo.get(
                ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO)) {
        finalStatus =
            FinalApplicationStatus.valueOf(entityInfo.get(
                ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO)
                .toString());
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.STATE_EVENT_INFO)) {
        state =
            YarnApplicationState.valueOf(entityInfo.get(
                ApplicationMetricsConstants.STATE_EVENT_INFO).toString());
      }
    }

    Map<String, String> configs = entity.getConfigs();
    if (configs
        .containsKey(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION)) {
      appNodeLabelExpression = configs
          .get(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION);
    }
    if (configs
        .containsKey(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION)) {
      amNodeLabelExpression =
          configs.get(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION);
    }

    Set<TimelineMetric> metrics = entity.getMetrics();
    if (metrics != null) {
      long vcoreSeconds = 0;
      long memorySeconds = 0;
      long preemptedVcoreSeconds = 0;
      long preemptedMemorySeconds = 0;

      for (TimelineMetric metric : metrics) {
        switch (metric.getId()) {
        case ApplicationMetricsConstants.APP_CPU_METRICS:
          vcoreSeconds = getAverageValue(metric.getValues().values());
          break;
        case ApplicationMetricsConstants.APP_MEM_METRICS:
          memorySeconds = getAverageValue(metric.getValues().values());
          break;
        case ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS:
          preemptedVcoreSeconds = getAverageValue(metric.getValues().values());
          break;
        case ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS:
          preemptedVcoreSeconds = getAverageValue(metric.getValues().values());
          break;
        default:
          // Should not happen..
          break;
        }
      }
      Map<String, Long> resourceSecondsMap = new HashMap<>();
      Map<String, Long> preemptedResoureSecondsMap = new HashMap<>();
      resourceSecondsMap
          .put(ResourceInformation.MEMORY_MB.getName(), memorySeconds);
      resourceSecondsMap
          .put(ResourceInformation.VCORES.getName(), vcoreSeconds);
      preemptedResoureSecondsMap.put(ResourceInformation.MEMORY_MB.getName(),
          preemptedMemorySeconds);
      preemptedResoureSecondsMap
          .put(ResourceInformation.VCORES.getName(), preemptedVcoreSeconds);

      appResources = ApplicationResourceUsageReport
          .newInstance(0, 0, null, null, null, resourceSecondsMap, 0, 0,
              preemptedResoureSecondsMap);
    }

    NavigableSet<TimelineEvent> events = entity.getEvents();
    long updatedTimeStamp = 0L;
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getId().equals(
            ApplicationMetricsConstants.CREATED_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getId().equals(
            ApplicationMetricsConstants.LAUNCHED_EVENT_TYPE)) {
          launchTime = event.getTimestamp();
        } else if (event.getId().equals(
            ApplicationMetricsConstants.UPDATED_EVENT_TYPE)) {
          // This type of events are parsed in time-stamp descending order
          // which means the previous event could override the information
          // from the later same type of event. Hence compare timestamp
          // before over writing.
          if (event.getTimestamp() > updatedTimeStamp) {
            updatedTimeStamp = event.getTimestamp();
          }
        } else if (event.getId().equals(
            ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo.containsKey(
              ApplicationMetricsConstants.STATE_EVENT_INFO)) {
            if (state == YarnApplicationState.ACCEPTED) {
              state = YarnApplicationState.valueOf(eventInfo.get(
                  ApplicationMetricsConstants.STATE_EVENT_INFO).toString());
            }
          }
        } else if (event.getId().equals(
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
          progress=1.0F;
          state = YarnApplicationState.FINISHED;
          finishedTime = event.getTimestamp();
        }
      }
    }
    return ApplicationReport.newInstance(
        ApplicationId.fromString(entity.getId()),
        latestApplicationAttemptId, user, queue, name, null, -1, null, state,
        diagnosticsInfo, null, createdTime, launchTime,
        finishedTime, finalStatus, appResources, null,
        progress, type, null, appTags, unmanagedApplication,
        Priority.newInstance(applicationPriority), appNodeLabelExpression,
        amNodeLabelExpression);
  }

  private static long getAverageValue(Collection<Number> values) {
    if (values == null || values.isEmpty()) {
      return 0;
    }
    long sum = 0;
    for (Number value : values) {
      sum += value.longValue();
    }
    return sum/values.size();
  }
}
