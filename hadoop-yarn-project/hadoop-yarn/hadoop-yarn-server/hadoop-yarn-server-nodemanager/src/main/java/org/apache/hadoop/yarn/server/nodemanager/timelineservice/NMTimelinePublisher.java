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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import org.apache.hadoop.yarn.client.api.TimelineClient;
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
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Metrics publisher service that publishes data to the timeline service v.2. It
 * is used only if the timeline service v.2 is enabled and the system publishing
 * of events and metrics is enabled.
 */
public class NMTimelinePublisher extends CompositeService {

  private static final Log LOG = LogFactory.getLog(NMTimelinePublisher.class);

  private Dispatcher dispatcher;

  private Context context;

  private NodeId nodeId;

  private String httpAddress;

  private final Map<ApplicationId, TimelineClient> appToClientMap;

  public NMTimelinePublisher(Context context) {
    super(NMTimelinePublisher.class.getName());
    this.context = context;
    appToClientMap = new ConcurrentHashMap<>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    dispatcher = new AsyncDispatcher();
    dispatcher.register(NMTimelineEventType.class,
        new ForwardingEventHandler());
    addIfService(dispatcher);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // context will be updated after containerManagerImpl is started
    // hence NMMetricsPublisher is added subservice of containerManagerImpl
    this.nodeId = context.getNodeId();
  }

  @VisibleForTesting
  Map<ApplicationId, TimelineClient> getAppToClientMap() {
    return appToClientMap;
  }

  protected void handleNMTimelineEvent(NMTimelineEvent event) {
    switch (event.getType()) {
    case TIMELINE_ENTITY_PUBLISH:
      putEntity(((TimelinePublishEvent) event).getTimelineEntityToPublish(),
          ((TimelinePublishEvent) event).getApplicationId());
      break;
    default:
      LOG.error("Unknown NMTimelineEvent type: " + event.getType());
    }
  }

  public void reportContainerResourceUsage(Container container, Long pmemUsage,
      Float cpuUsagePercentPerCore) {
    if (pmemUsage != ResourceCalculatorProcessTree.UNAVAILABLE ||
        cpuUsagePercentPerCore != ResourceCalculatorProcessTree.UNAVAILABLE) {
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
      if (cpuUsagePercentPerCore != ResourceCalculatorProcessTree.UNAVAILABLE) {
        TimelineMetric cpuMetric = new TimelineMetric();
        cpuMetric.setId(ContainerMetric.CPU.toString());
        // TODO: support average
        cpuMetric.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
        cpuMetric.addValue(currentTimeMillis,
            Math.round(cpuUsagePercentPerCore));
        entity.addMetric(cpuMetric);
      }
      ApplicationId appId = container.getContainerId().getApplicationAttemptId()
          .getApplicationId();
      try {
        // no need to put it as part of publisher as timeline client already has
        // Queuing concept
        TimelineClient timelineClient = getTimelineClient(appId);
        if (timelineClient != null) {
          timelineClient.putEntitiesAsync(entity);
        } else {
          LOG.error("Seems like client has been removed before the container"
              + " metric could be published for " + container.getContainerId());
        }
      } catch (IOException | YarnException e) {
        LOG.error("Failed to publish Container metrics for container "
            + container.getContainerId(), e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerCreatedEvent(ContainerEvent event) {
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

    entity.addEvent(tEvent);
    entity.setCreatedTime(event.getTimestamp());
    dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
        containerId.getApplicationAttemptId().getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  private void publishContainerFinishedEvent(ContainerStatus containerStatus,
      long timeStamp) {
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
        timeStamp);
    entity.setInfo(entityInfo);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ContainerMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(timeStamp);
    entity.addEvent(tEvent);

    dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
        containerId.getApplicationAttemptId().getApplicationId()));
  }

  private void publishContainerLocalizationEvent(
      ContainerLocalizationEvent event, String eventType) {
    Container container = event.getContainer();
    ContainerId containerId = container.getContainerId();
    TimelineEntity entity = createContainerEntity(containerId);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(eventType);
    tEvent.setTimestamp(event.getTimestamp());
    entity.addEvent(tEvent);

    ApplicationId appId =
        container.getContainerId().getApplicationAttemptId().getApplicationId();
    try {
      // no need to put it as part of publisher as timeline client already has
      // Queuing concept
      TimelineClient timelineClient = getTimelineClient(appId);
      if (timelineClient != null) {
        timelineClient.putEntitiesAsync(entity);
      } else {
        LOG.error("Seems like client has been removed before the event could be"
            + " published for " + container.getContainerId());
      }
    } catch (IOException | YarnException e) {
      LOG.error("Failed to publish Container metrics for container "
          + container.getContainerId(), e);
    }
  }

  private static ContainerEntity createContainerEntity(
      ContainerId containerId) {
    ContainerEntity entity = new ContainerEntity();
    entity.setId(containerId.toString());
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
        LOG.debug("Publishing the entity " + entity + ", JSON-style content: "
            + TimelineUtils.dumpTimelineRecordtoJSON(entity));
      }
      TimelineClient timelineClient = getTimelineClient(appId);
      if (timelineClient != null) {
        timelineClient.putEntities(entity);
      } else {
        LOG.error("Seems like client has been removed before the entity "
            + "could be published for " + entity);
      }
    } catch (Exception e) {
      LOG.error("Error when publishing entity " + entity, e);
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
          event.getTimestamp());
      break;

    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug(event.getType() + " is not a desired ApplicationEvent which"
            + " needs to be published by NMTimelinePublisher");
      }
      break;
    }
  }

  public void publishContainerEvent(ContainerEvent event) {
    // publish only when the desired event is received
    if (this.httpAddress == null) {
      // update httpAddress for first time. When this service started,
      // web server will not be started.
      this.httpAddress = nodeId.getHost() + ":" + context.getHttpPort();
    }
    switch (event.getType()) {
    case INIT_CONTAINER:
      publishContainerCreatedEvent(event);
      break;

    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug(event.getType()
            + " is not a desired ContainerEvent which needs to be published by"
            + " NMTimelinePublisher");
      }
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
      if (LOG.isDebugEnabled()) {
        LOG.debug(event.getType()
            + " is not a desired LocalizationEvent which needs to be published"
            + " by NMTimelinePublisher");
      }
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
    private ApplicationId appId;
    private TimelineEntity entityToPublish;

    public TimelinePublishEvent(TimelineEntity entity, ApplicationId appId) {
      super(NMTimelineEventType.TIMELINE_ENTITY_PUBLISH, System
          .currentTimeMillis());
      this.appId = appId;
      this.entityToPublish = entity;
    }

    public ApplicationId getApplicationId() {
      return appId;
    }

    public TimelineEntity getTimelineEntityToPublish() {
      return entityToPublish;
    }
  }

  public void createTimelineClient(ApplicationId appId) {
    if (!appToClientMap.containsKey(appId)) {
      TimelineClient timelineClient =
          TimelineClient.createTimelineClient(appId);
      timelineClient.init(getConfig());
      timelineClient.start();
      appToClientMap.put(appId, timelineClient);
    }
  }

  public void stopTimelineClient(ApplicationId appId) {
    TimelineClient client = appToClientMap.remove(appId);
    if (client != null) {
      client.stop();
    }
  }

  public void setTimelineServiceAddress(ApplicationId appId,
      String collectorAddr) {
    TimelineClient client = appToClientMap.get(appId);
    if (client != null) {
      client.setTimelineServiceAddress(collectorAddr);
    }
  }

  private TimelineClient getTimelineClient(ApplicationId appId) {
    return appToClientMap.get(appId);
  }
}