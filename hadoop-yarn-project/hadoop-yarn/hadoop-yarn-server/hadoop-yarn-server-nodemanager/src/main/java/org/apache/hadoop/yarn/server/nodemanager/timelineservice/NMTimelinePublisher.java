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

package org.apache.hadoop.yarn.server.nodemanager.timelineservice;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerPauseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResumeEvent;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.Identifier;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl.ContainerMetric;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Metrics publisher service that publishes data to the timeline service v.2. It
 * is used only if the timeline service v.2 is enabled and the system publishing
 * of events and metrics is enabled.
 */
public class NMTimelinePublisher extends CompositeService {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMTimelinePublisher.class);

  private Dispatcher dispatcher;

  private Context context;

  private NodeId nodeId;

  private String httpAddress;
  private String httpPort;

  private UserGroupInformation nmLoginUGI;

  private final Map<ApplicationId, TimelineV2Client> appToClientMap;

  private boolean publishNMContainerEvents = true;

  public NMTimelinePublisher(Context context) {
    super(NMTimelinePublisher.class.getName());
    this.context = context;
    appToClientMap = new ConcurrentHashMap<>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    dispatcher = createDispatcher();
    dispatcher.register(NMTimelineEventType.class,
        new ForwardingEventHandler());
    addIfService(dispatcher);
    this.nmLoginUGI =  UserGroupInformation.isSecurityEnabled() ?
        UserGroupInformation.getLoginUser() :
        UserGroupInformation.getCurrentUser();
    LOG.info("Initialized NMTimelinePublisher UGI to " + nmLoginUGI);

    String webAppURLWithoutScheme =
        WebAppUtils.getNMWebAppURLWithoutScheme(conf);
    if (webAppURLWithoutScheme.contains(":")) {
      httpPort = webAppURLWithoutScheme.split(":")[1];
    }

    publishNMContainerEvents = conf.getBoolean(
        YarnConfiguration.NM_PUBLISH_CONTAINER_EVENTS_ENABLED,
        YarnConfiguration.DEFAULT_NM_PUBLISH_CONTAINER_EVENTS_ENABLED);
    super.serviceInit(conf);
  }

  protected AsyncDispatcher createDispatcher() {
    return new AsyncDispatcher("NM Timeline dispatcher");
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // context will be updated after containerManagerImpl is started
    // hence NMMetricsPublisher is added subservice of containerManagerImpl
    this.nodeId = context.getNodeId();
    this.httpAddress = nodeId.getHost() + ":" + httpPort;
  }

  @Override
  protected void serviceStop() throws Exception {
    for(ApplicationId app : appToClientMap.keySet()) {
      stopTimelineClient(app);
    }
    super.serviceStop();
  }

  @VisibleForTesting
  Map<ApplicationId, TimelineV2Client> getAppToClientMap() {
    return appToClientMap;
  }

  protected void handleNMTimelineEvent(NMTimelineEvent event) {
    switch (event.getType()) {
    case TIMELINE_ENTITY_PUBLISH:
      putEntity(((TimelinePublishEvent) event).getTimelineEntityToPublish(),
          ((TimelinePublishEvent) event).getApplicationId());
      break;
    case STOP_TIMELINE_CLIENT:
      removeAndStopTimelineClient(event.getApplicationId());
      break;
    default:
      LOG.error("Unknown NMTimelineEvent type: " + event.getType());
    }
  }

  public void reportContainerResourceUsage(Container container, Long pmemUsage,
      Float cpuUsagePercentPerCore) {
    if (publishNMContainerEvents) {
      if (pmemUsage != ResourceCalculatorProcessTree.UNAVAILABLE
          || cpuUsagePercentPerCore !=
          ResourceCalculatorProcessTree.UNAVAILABLE) {
        ContainerEntity entity =
            createContainerEntity(container.getContainerId());
        long currentTimeMillis = System.currentTimeMillis();
        if (pmemUsage != ResourceCalculatorProcessTree.UNAVAILABLE) {
          TimelineMetric memoryMetric = new TimelineMetric();
          memoryMetric.setId(ContainerMetric.MEMORY.toString());
          memoryMetric.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
          memoryMetric.addValue(currentTimeMillis, pmemUsage);
          entity.addMetric(memoryMetric);
        }
        if (cpuUsagePercentPerCore !=
            ResourceCalculatorProcessTree.UNAVAILABLE) {
          TimelineMetric cpuMetric = new TimelineMetric();
          cpuMetric.setId(ContainerMetric.CPU.toString());
          // TODO: support average
          cpuMetric.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
          cpuMetric.addValue(currentTimeMillis,
              Math.round(cpuUsagePercentPerCore));
          entity.addMetric(cpuMetric);
        }
        ApplicationId appId = container.getContainerId().
            getApplicationAttemptId().getApplicationId();
        try {
          // no need to put it as part of publisher as timeline client
          // already has Queuing concept
          TimelineV2Client timelineClient = getTimelineClient(appId);
          if (timelineClient != null) {
            timelineClient.putEntitiesAsync(entity);
          } else {
            LOG.error("Seems like client has been removed before the container"
                + " metric could be published for " +
                container.getContainerId());
          }
        } catch (IOException e) {
          LOG.error(
              "Failed to publish Container metrics for container " +
                  container.getContainerId());
          LOG.debug("Failed to publish Container metrics for container {}",
              container.getContainerId(), e);
        } catch (YarnException e) {
          LOG.error(
              "Failed to publish Container metrics for container " +
                  container.getContainerId(), e.getMessage());
          LOG.debug("Failed to publish Container metrics for container {}",
              container.getContainerId(), e);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerCreatedEvent(ContainerEvent event) {
    if (publishNMContainerEvents) {
      ContainerId containerId = event.getContainerID();
      ContainerEntity entity = createContainerEntity(containerId);
      Container container = context.getContainers().get(containerId);
      Resource resource = container.getResource();

      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO,
          resource.getMemorySize());
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_INFO,
          resource.getVirtualCores());
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_INFO,
          nodeId.getHost());
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_INFO,
          nodeId.getPort());
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO,
          container.getPriority().toString());
      entityInfo.put(
          ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO,
          httpAddress);
      entity.setInfo(entityInfo);

      TimelineEvent tEvent = new TimelineEvent();
      tEvent.setId(ContainerMetricsConstants.CREATED_EVENT_TYPE);
      tEvent.setTimestamp(event.getTimestamp());

      long containerStartTime = container.getContainerStartTime();
      entity.addEvent(tEvent);
      entity.setCreatedTime(containerStartTime);
      dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
          containerId.getApplicationAttemptId().getApplicationId()));
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerResumedEvent(
      ContainerEvent event) {
    if (publishNMContainerEvents) {
      ContainerResumeEvent resumeEvent = (ContainerResumeEvent) event;
      ContainerId containerId = resumeEvent.getContainerID();
      ContainerEntity entity = createContainerEntity(containerId);

      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
          resumeEvent.getDiagnostic());
      entity.setInfo(entityInfo);

      Container container = context.getContainers().get(containerId);
      if (container != null) {
        TimelineEvent tEvent = new TimelineEvent();
        tEvent.setId(ContainerMetricsConstants.RESUMED_EVENT_TYPE);
        tEvent.setTimestamp(event.getTimestamp());
        entity.addEvent(tEvent);
        dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
            containerId.getApplicationAttemptId().getApplicationId()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerPausedEvent(
      ContainerEvent event) {
    if (publishNMContainerEvents) {
      ContainerPauseEvent pauseEvent = (ContainerPauseEvent) event;
      ContainerId containerId = pauseEvent.getContainerID();
      ContainerEntity entity = createContainerEntity(containerId);

      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
          pauseEvent.getDiagnostic());
      entity.setInfo(entityInfo);

      Container container = context.getContainers().get(containerId);
      if (container != null) {
        TimelineEvent tEvent = new TimelineEvent();
        tEvent.setId(ContainerMetricsConstants.PAUSED_EVENT_TYPE);
        tEvent.setTimestamp(event.getTimestamp());
        entity.addEvent(tEvent);
        dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
            containerId.getApplicationAttemptId().getApplicationId()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerKilledEvent(
      ContainerEvent event) {
    if (publishNMContainerEvents) {
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      ContainerId containerId = killEvent.getContainerID();
      ContainerEntity entity = createContainerEntity(containerId);

      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
          killEvent.getDiagnostic());
      entityInfo.put(ContainerMetricsConstants.EXIT_STATUS_INFO,
          killEvent.getContainerExitStatus());
      entity.setInfo(entityInfo);

      Container container = context.getContainers().get(containerId);
      if (container != null) {
        TimelineEvent tEvent = new TimelineEvent();
        tEvent.setId(ContainerMetricsConstants.KILLED_EVENT_TYPE);
        tEvent.setTimestamp(event.getTimestamp());
        entity.addEvent(tEvent);
        dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
            containerId.getApplicationAttemptId().getApplicationId()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerFinishedEvent(ContainerStatus containerStatus,
      long containerFinishTime, long containerStartTime) {
    if (publishNMContainerEvents) {
      ContainerId containerId = containerStatus.getContainerId();
      TimelineEntity entity = createContainerEntity(containerId);

      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
          containerStatus.getDiagnostics());
      entityInfo.put(ContainerMetricsConstants.EXIT_STATUS_INFO,
          containerStatus.getExitStatus());
      entityInfo.put(ContainerMetricsConstants.STATE_INFO,
          ContainerState.COMPLETE.toString());
      entityInfo.put(ContainerMetricsConstants.CONTAINER_FINISHED_TIME,
          containerFinishTime);
      entity.setInfo(entityInfo);

      TimelineEvent tEvent = new TimelineEvent();
      tEvent.setId(ContainerMetricsConstants.FINISHED_EVENT_TYPE);
      tEvent.setTimestamp(containerFinishTime);
      entity.addEvent(tEvent);

      dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
          containerId.getApplicationAttemptId().getApplicationId()));
    }
  }

  private void publishContainerLocalizationEvent(
      ContainerLocalizationEvent event, String eventType) {
    if (publishNMContainerEvents) {
      Container container = event.getContainer();
      ContainerId containerId = container.getContainerId();
      TimelineEntity entity = createContainerEntity(containerId);

      TimelineEvent tEvent = new TimelineEvent();
      tEvent.setId(eventType);
      tEvent.setTimestamp(event.getTimestamp());
      entity.addEvent(tEvent);

      ApplicationId appId = container.getContainerId().
          getApplicationAttemptId().getApplicationId();
      try {
        // no need to put it as part of publisher as timeline client already has
        // Queuing concept
        TimelineV2Client timelineClient = getTimelineClient(appId);
        if (timelineClient != null) {
          timelineClient.putEntitiesAsync(entity);
        } else {
          LOG.error("Seems like client has been removed before the event"
              + " could be published for " + container.getContainerId());
        }
      } catch (IOException e) {
        LOG.error("Failed to publish Container metrics for container "
            + container.getContainerId());
        LOG.debug("Failed to publish Container metrics for container {}",
            container.getContainerId(), e);
      } catch (YarnException e) {
        LOG.error("Failed to publish Container metrics for container "
            + container.getContainerId(), e.getMessage());
        LOG.debug("Failed to publish Container metrics for container {}",
            container.getContainerId(), e);
      }
    }
  }

  private static ContainerEntity createContainerEntity(
      ContainerId containerId) {
    ContainerEntity entity = new ContainerEntity();
    entity.setId(containerId.toString());
    entity.setIdPrefix(TimelineServiceHelper.invertLong(
        containerId.getContainerId()));
    Identifier parentIdentifier = new Identifier();
    parentIdentifier
        .setType(TimelineEntityType.YARN_APPLICATION_ATTEMPT.name());
    parentIdentifier.setId(containerId.getApplicationAttemptId().toString());
    entity.setParent(parentIdentifier);
    return entity;
  }

  private void putEntity(TimelineEntity entity, ApplicationId appId) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Publishing the entity {} JSON-style content: {}",
            entity, TimelineUtils.dumpTimelineRecordtoJSON(entity));
      }
      TimelineV2Client timelineClient = getTimelineClient(appId);
      if (timelineClient != null) {
        timelineClient.putEntities(entity);
      } else {
        LOG.error("Seems like client has been removed before the entity "
            + "could be published for " + entity);
      }
    } catch (IOException e) {
      LOG.error("Error when publishing entity " + entity);
      LOG.debug("Error when publishing entity {}", entity, e);
    } catch (YarnException e) {
      LOG.error("Error when publishing entity " + entity, e.getMessage());
      LOG.debug("Error when publishing entity {}", entity, e);
    }
  }

  public void publishApplicationEvent(ApplicationEvent event) {
    // publish only when the desired event is received
    switch (event.getType()) {
    case INIT_APPLICATION:
    case FINISH_APPLICATION:
    case APPLICATION_LOG_HANDLING_FAILED:
      // TODO need to be handled in future,
      // not sure to publish under which entity
      break;
    case APPLICATION_CONTAINER_FINISHED:
      // this is actually used to publish the container Event
      ApplicationContainerFinishedEvent evnt =
          (ApplicationContainerFinishedEvent) event;
      publishContainerFinishedEvent(evnt.getContainerStatus(),
          event.getTimestamp(), evnt.getContainerStartTime());
      break;

    default:
      LOG.debug("{} is not a desired ApplicationEvent which"
          + " needs to be published by NMTimelinePublisher", event.getType());
      break;
    }
  }

  public void publishContainerEvent(ContainerEvent event) {
    // publish only when the desired event is received
    switch (event.getType()) {
    case INIT_CONTAINER:
      publishContainerCreatedEvent(event);
      break;
    case KILL_CONTAINER:
      publishContainerKilledEvent(event);
      break;
    case PAUSE_CONTAINER:
      publishContainerPausedEvent(event);
      break;
    case RESUME_CONTAINER:
      publishContainerResumedEvent(event);
      break;
    default:
      LOG.debug("{} is not a desired ContainerEvent which needs to be "
            + " published by NMTimelinePublisher", event.getType());
      break;
    }
  }

  public void publishLocalizationEvent(LocalizationEvent event) {
    // publish only when the desired event is received
    switch (event.getType()) {
    case CONTAINER_RESOURCES_LOCALIZED:
      publishContainerLocalizationEvent((ContainerLocalizationEvent) event,
          ContainerMetricsConstants.LOCALIZATION_FINISHED_EVENT_TYPE);
      break;
    case LOCALIZE_CONTAINER_RESOURCES:
      publishContainerLocalizationEvent((ContainerLocalizationEvent) event,
          ContainerMetricsConstants.LOCALIZATION_START_EVENT_TYPE);
      break;
    default:
      LOG.debug("{} is not a desired LocalizationEvent which needs to be"
            + " published by NMTimelinePublisher", event.getType());
      break;
    }
  }

  /**
   * EventHandler implementation which forward events to NMMetricsPublisher.
   * Making use of it, NMMetricsPublisher can avoid to have a public handle
   * method.
   */
  private final class ForwardingEventHandler implements
      EventHandler<NMTimelineEvent> {

    @Override
    public void handle(NMTimelineEvent event) {
      handleNMTimelineEvent(event);
    }
  }

  private static class TimelinePublishEvent extends NMTimelineEvent {
    private TimelineEntity entityToPublish;

    public TimelinePublishEvent(TimelineEntity entity, ApplicationId appId) {
      super(NMTimelineEventType.TIMELINE_ENTITY_PUBLISH, appId);
      this.entityToPublish = entity;
    }

    public TimelineEntity getTimelineEntityToPublish() {
      return entityToPublish;
    }
  }

  public void createTimelineClient(ApplicationId appId) {
    if (!appToClientMap.containsKey(appId)) {
      try {
        TimelineV2Client timelineClient =
            nmLoginUGI.doAs(new PrivilegedExceptionAction<TimelineV2Client>() {
              @Override
              public TimelineV2Client run() throws Exception {
                TimelineV2Client timelineClient =
                    TimelineV2Client.createTimelineClient(appId);
                timelineClient.init(getConfig());
                timelineClient.start();
                return timelineClient;
              }
            });
        appToClientMap.put(appId, timelineClient);
      } catch (IOException | InterruptedException | RuntimeException |
          Error e) {
        LOG.warn("Unable to create timeline client for app " + appId, e);
      }
    }
  }

  public void stopTimelineClient(ApplicationId appId) {
    dispatcher.getEventHandler().handle(
        new NMTimelineEvent(NMTimelineEventType.STOP_TIMELINE_CLIENT, appId));
  }

  private void removeAndStopTimelineClient(ApplicationId appId) {
    TimelineV2Client client = appToClientMap.remove(appId);
    if (client != null) {
      client.stop();
    }
  }

  public void setTimelineServiceAddress(ApplicationId appId,
      String collectorAddr) {
    TimelineV2Client client = appToClientMap.get(appId);
    if (client != null) {
      client.setTimelineCollectorInfo(CollectorInfo.newInstance(collectorAddr));
    }
  }

  private TimelineV2Client getTimelineClient(ApplicationId appId) {
    return appToClientMap.get(appId);
  }
}
