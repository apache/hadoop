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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.Identifier;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl.ContainerMetric;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

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

  public NMTimelinePublisher(Context context) {
    super(NMTimelinePublisher.class.getName());
    this.context = context;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    dispatcher = new AsyncDispatcher();
    dispatcher.register(NMTimelineEventType.class,
        new ForwardingEventHandler());
    dispatcher
        .register(ContainerEventType.class, new ContainerEventHandler());
    dispatcher.register(ApplicationEventType.class,
        new ApplicationEventHandler());
    dispatcher.register(LocalizationEventType.class,
        new LocalizationEventDispatcher());
    addIfService(dispatcher);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // context will be updated after containerManagerImpl is started
    // hence NMMetricsPublisher is added subservice of containerManagerImpl
    this.nodeId = context.getNodeId();
    this.httpAddress = nodeId.getHost() + ":" + context.getHttpPort();
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

  @SuppressWarnings("unchecked")
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
        memoryMetric.addValue(currentTimeMillis, pmemUsage);
        entity.addMetric(memoryMetric);
      }
      if (cpuUsagePercentPerCore != ResourceCalculatorProcessTree.UNAVAILABLE) {
        TimelineMetric cpuMetric = new TimelineMetric();
        cpuMetric.setId(ContainerMetric.CPU.toString());
        cpuMetric.addValue(currentTimeMillis,
            Math.round(cpuUsagePercentPerCore));
        entity.addMetric(cpuMetric);
      }
      dispatcher.getEventHandler()
          .handle(new TimelinePublishEvent(entity, container.getContainerId()
              .getApplicationAttemptId().getApplicationId()));
    }
  }

  private void publishContainerCreatedEvent(ContainerEntity entity,
      ContainerId containerId, Resource resource, Priority priority,
      long timestamp) {
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_ENTITY_INFO,
        resource.getMemory());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_ENTITY_INFO,
        resource.getVirtualCores());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO,
        nodeId.getHost());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO,
        nodeId.getPort());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PRIORITY_ENTITY_INFO,
        priority.toString());
    entityInfo.put(
        ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_ENTITY_INFO,
        httpAddress);
    entity.setInfo(entityInfo);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ContainerMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(timestamp);

    entity.addEvent(tEvent);
    entity.setCreatedTime(timestamp);
    putEntity(entity, containerId.getApplicationAttemptId().getApplicationId());
  }

  private void publishContainerFinishedEvent(ContainerStatus containerStatus,
      long timeStamp) {
    ContainerId containerId = containerStatus.getContainerId();
    TimelineEntity entity = createContainerEntity(containerId);

    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        containerStatus.getDiagnostics());
    eventInfo.put(ContainerMetricsConstants.EXIT_STATUS_EVENT_INFO,
        containerStatus.getExitStatus());
    eventInfo.put(ContainerMetricsConstants.STATE_EVENT_INFO, containerStatus
        .getState().toString());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ContainerMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(timeStamp);
    tEvent.setInfo(eventInfo);

    entity.addEvent(tEvent);
    putEntity(entity, containerId.getApplicationAttemptId().getApplicationId());
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
      TimelineClient timelineClient =
          context.getApplications().get(appId).getTimelineClient();
      timelineClient.putEntities(entity);
    } catch (Exception e) {
      LOG.error("Error when publishing entity " + entity, e);
    }
  }

  @SuppressWarnings("unchecked")
  public void publishApplicationEvent(ApplicationEvent event) {
    // publish only when the desired event is received
    switch (event.getType()) {
    case INIT_APPLICATION:
    case FINISH_APPLICATION:
    case APPLICATION_CONTAINER_FINISHED:
    case APPLICATION_LOG_HANDLING_FAILED:
      dispatcher.getEventHandler().handle(event);
      break;

    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug(event.getType() + " is not a desired ApplicationEvent which"
            + " needs to be published by NMTimelinePublisher");
      }
      break;
    }
  }

  @SuppressWarnings("unchecked")
  public void publishContainerEvent(ContainerEvent event) {
    // publish only when the desired event is received
    switch (event.getType()) {
    case INIT_CONTAINER:
      dispatcher.getEventHandler().handle(event);
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

  @SuppressWarnings("unchecked")
  public void publishLocalizationEvent(LocalizationEvent event) {
    // publish only when the desired event is received
    switch (event.getType()) {
    case CONTAINER_RESOURCES_LOCALIZED:
    case INIT_CONTAINER_RESOURCES:
      dispatcher.getEventHandler().handle(event);
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

  private class ApplicationEventHandler implements
      EventHandler<ApplicationEvent> {
    @Override
    public void handle(ApplicationEvent event) {
      switch (event.getType()) {
      case APPLICATION_CONTAINER_FINISHED:
        // this is actually used to publish the container Event
        ApplicationContainerFinishedEvent evnt =
            (ApplicationContainerFinishedEvent) event;
        publishContainerFinishedEvent(evnt.getContainerStatus(),
            event.getTimestamp());
        break;
      default:
        LOG.error("Seems like event type is captured only in "
            + "publishApplicationEvent method and not handled here");
        break;
      }
    }
  }

  private class ContainerEventHandler implements EventHandler<ContainerEvent> {
    @Override
    public void handle(ContainerEvent event) {
      ContainerId containerId = event.getContainerID();
      Container container = context.getContainers().get(containerId);
      long timestamp = event.getTimestamp();
      ContainerEntity entity = createContainerEntity(containerId);

      switch (event.getType()) {
      case INIT_CONTAINER:
        publishContainerCreatedEvent(entity, containerId,
            container.getResource(), container.getPriority(), timestamp);
        break;
      default:
        LOG.error("Seems like event type is captured only in "
            + "publishContainerEvent method and not handled here");
        break;
      }
    }
  }

  private static final class LocalizationEventDispatcher implements
      EventHandler<LocalizationEvent> {
    @Override
    public void handle(LocalizationEvent event) {
      switch (event.getType()) {
      case INIT_CONTAINER_RESOURCES:
      case CONTAINER_RESOURCES_LOCALIZED:
        // TODO after priority based flush jira is finished
        break;
      default:
        LOG.error("Seems like event type is captured only in "
            + "publishLocalizationEvent method and not handled here");
        break;
      }
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
}