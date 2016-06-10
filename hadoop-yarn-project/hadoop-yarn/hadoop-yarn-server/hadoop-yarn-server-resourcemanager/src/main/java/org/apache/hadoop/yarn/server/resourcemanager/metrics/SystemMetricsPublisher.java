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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
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

import com.google.common.annotations.VisibleForTesting;

/**
 * The class that helps RM publish metrics to the timeline server. RM will
 * always invoke the methods of this class regardless the service is enabled or
 * not. If it is disabled, publishing requests will be ignored silently.
 */
@Private
@Unstable
public class SystemMetricsPublisher extends CompositeService {

  private static final Log LOG = LogFactory
      .getLog(SystemMetricsPublisher.class);

  private Dispatcher dispatcher;
  private TimelineClient client;
  private boolean publishSystemMetrics;

  public SystemMetricsPublisher() {
    super(SystemMetricsPublisher.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    publishSystemMetrics =
        conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED) &&
        conf.getBoolean(YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_ENABLED,
            YarnConfiguration.DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_ENABLED);

    if (publishSystemMetrics) {
      client = TimelineClient.createTimelineClient();
      addIfService(client);

      dispatcher = createDispatcher(conf);
      dispatcher.register(SystemMetricsEventType.class,
          new ForwardingEventHandler());
      addIfService(dispatcher);
      LOG.info("YARN system metrics publishing service is enabled");
    } else {
      LOG.info("YARN system metrics publishing service is not enabled");
    }
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  public void appCreated(RMApp app, long createdTime) {
    if (publishSystemMetrics) {
      ApplicationSubmissionContext appSubmissionContext =
          app.getApplicationSubmissionContext();
      dispatcher.getEventHandler().handle(
          new ApplicationCreatedEvent(
              app.getApplicationId(),
              app.getName(),
              app.getApplicationType(),
              app.getUser(),
              app.getQueue(),
              app.getSubmitTime(),
              createdTime, app.getApplicationTags(),
              appSubmissionContext.getUnmanagedAM(),
              appSubmissionContext.getPriority(),
              app.getAppNodeLabelExpression(),
              app.getAmNodeLabelExpression(),
              app.getCallerContext()));
    }
  }

  @SuppressWarnings("unchecked")
  public void appUpdated(RMApp app, long updatedTime) {
    if (publishSystemMetrics) {
      dispatcher.getEventHandler()
          .handle(new ApplicationUpdatedEvent(app.getApplicationId(),
              app.getQueue(), updatedTime,
              app.getApplicationSubmissionContext().getPriority()));
    }
  }

  @SuppressWarnings("unchecked")
  public void appFinished(RMApp app, RMAppState state, long finishedTime) {
    if (publishSystemMetrics) {
      dispatcher.getEventHandler().handle(
          new ApplicationFinishedEvent(
              app.getApplicationId(),
              app.getDiagnostics().toString(),
              app.getFinalApplicationStatus(),
              RMServerUtils.createApplicationState(state),
              app.getCurrentAppAttempt() == null ?
                  null : app.getCurrentAppAttempt().getAppAttemptId(),
              finishedTime,
              app.getRMAppMetrics()));
    }
  }

  @SuppressWarnings("unchecked")
  public void appACLsUpdated(RMApp app, String appViewACLs,
      long updatedTime) {
    if (publishSystemMetrics) {
      dispatcher.getEventHandler().handle(
          new ApplicationACLsUpdatedEvent(
              app.getApplicationId(),
              appViewACLs == null ? "" : appViewACLs,
              updatedTime));
    }
  }

  @SuppressWarnings("unchecked")
  public void appStateUpdated(RMApp app, YarnApplicationState appState,
      long updatedTime) {
    if (publishSystemMetrics) {
      dispatcher.getEventHandler().handle(
          new ApplicaitonStateUpdatedEvent(
              app.getApplicationId(),
              appState,
              updatedTime));
    }
  }

  @SuppressWarnings("unchecked")
  public void appAttemptRegistered(RMAppAttempt appAttempt,
      long registeredTime) {
    if (publishSystemMetrics) {
      ContainerId container = (appAttempt.getMasterContainer() == null) ? null
          : appAttempt.getMasterContainer().getId();
      dispatcher.getEventHandler().handle(
          new AppAttemptRegisteredEvent(
              appAttempt.getAppAttemptId(),
              appAttempt.getHost(),
              appAttempt.getRpcPort(),
              appAttempt.getTrackingUrl(),
              appAttempt.getOriginalTrackingUrl(),
              container,
              registeredTime));
    }
  }

  @SuppressWarnings("unchecked")
  public void appAttemptFinished(RMAppAttempt appAttempt,
      RMAppAttemptState appAttemtpState, RMApp app, long finishedTime) {
    if (publishSystemMetrics) {
      ContainerId container = (appAttempt.getMasterContainer() == null) ? null
          : appAttempt.getMasterContainer().getId();
      dispatcher.getEventHandler().handle(
          new AppAttemptFinishedEvent(
              appAttempt.getAppAttemptId(),
              appAttempt.getTrackingUrl(),
              appAttempt.getOriginalTrackingUrl(),
              appAttempt.getDiagnostics(),
              // app will get the final status from app attempt, or create one
              // based on app state if it doesn't exist
              app.getFinalApplicationStatus(),
              RMServerUtils.createApplicationAttemptState(appAttemtpState),
              finishedTime,
              container));
    }
  }

  @SuppressWarnings("unchecked")
  public void containerCreated(RMContainer container, long createdTime) {
    if (publishSystemMetrics) {
      dispatcher.getEventHandler().handle(
          new ContainerCreatedEvent(
              container.getContainerId(),
              container.getAllocatedResource(),
              container.getAllocatedNode(),
              container.getAllocatedPriority(),
              createdTime, container.getNodeHttpAddress()));
    }
  }

  @SuppressWarnings("unchecked")
  public void containerFinished(RMContainer container, long finishedTime) {
    if (publishSystemMetrics) {
      dispatcher.getEventHandler().handle(
          new ContainerFinishedEvent(
              container.getContainerId(),
              container.getDiagnosticsInfo(),
              container.getContainerExitStatus(),
              container.getContainerState(),
              finishedTime, container.getAllocatedNode()));
    }
  }

  protected Dispatcher createDispatcher(Configuration conf) {
    MultiThreadedDispatcher dispatcher =
        new MultiThreadedDispatcher(
            conf.getInt(
                YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE,
                YarnConfiguration.DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE));
    dispatcher.setDrainEventsOnStop();
    return dispatcher;
  }

  protected void handleSystemMetricsEvent(
      SystemMetricsEvent event) {
    switch (event.getType()) {
    case APP_CREATED:
      publishApplicationCreatedEvent((ApplicationCreatedEvent) event);
      break;
    case APP_FINISHED:
      publishApplicationFinishedEvent((ApplicationFinishedEvent) event);
      break;
    case APP_ACLS_UPDATED:
      publishApplicationACLsUpdatedEvent((ApplicationACLsUpdatedEvent) event);
      break;
    case APP_UPDATED:
      publishApplicationUpdatedEvent((ApplicationUpdatedEvent) event);
      break;
    case APP_STATE_UPDATED:
      publishApplicationStateUpdatedEvent(
          (ApplicaitonStateUpdatedEvent)event);
      break;
    case APP_ATTEMPT_REGISTERED:
      publishAppAttemptRegisteredEvent((AppAttemptRegisteredEvent) event);
      break;
    case APP_ATTEMPT_FINISHED:
      publishAppAttemptFinishedEvent((AppAttemptFinishedEvent) event);
      break;
    case CONTAINER_CREATED:
      publishContainerCreatedEvent((ContainerCreatedEvent) event);
      break;
    case CONTAINER_FINISHED:
      publishContainerFinishedEvent((ContainerFinishedEvent) event);
      break;
    default:
      LOG.error("Unknown SystemMetricsEvent type: " + event.getType());
    }
  }

  private void publishApplicationCreatedEvent(ApplicationCreatedEvent event) {
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

  private void publishApplicationFinishedEvent(ApplicationFinishedEvent event) {
    TimelineEntity entity =
        createApplicationEntity(event.getApplicationId());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(
        ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        event.getDiagnosticsInfo());
    eventInfo.put(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO,
        event.getFinalApplicationStatus().toString());
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        event.getYarnApplicationState().toString());
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

  private void publishApplicationUpdatedEvent(ApplicationUpdatedEvent event) {
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

  private void publishApplicationStateUpdatedEvent(
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

  private void publishApplicationACLsUpdatedEvent(
      ApplicationACLsUpdatedEvent event) {
    TimelineEntity entity =
        createApplicationEntity(event.getApplicationId());
    TimelineEvent tEvent = new TimelineEvent();
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
        event.getViewAppACLs());
    entity.setOtherInfo(entityInfo);
    tEvent.setEventType(
        ApplicationMetricsConstants.ACLS_UPDATED_EVENT_TYPE);
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

  private void
      publishAppAttemptRegisteredEvent(AppAttemptRegisteredEvent event) {
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

  private void publishAppAttemptFinishedEvent(AppAttemptFinishedEvent event) {
    TimelineEntity entity =
        createAppAttemptEntity(event.getApplicationAttemptId());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(AppAttemptMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(
        AppAttemptMetricsConstants.TRACKING_URL_EVENT_INFO,
        event.getTrackingUrl());
    eventInfo.put(
        AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_EVENT_INFO,
        event.getOriginalTrackingURL());
    eventInfo.put(AppAttemptMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        event.getDiagnosticsInfo());
    eventInfo.put(AppAttemptMetricsConstants.FINAL_STATUS_EVENT_INFO,
        event.getFinalApplicationStatus().toString());
    eventInfo.put(AppAttemptMetricsConstants.STATE_EVENT_INFO,
        event.getYarnApplicationAttemptState().toString());
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
    entity.setEntityType(
        AppAttemptMetricsConstants.ENTITY_TYPE);
    entity.setEntityId(appAttemptId.toString());
    entity.addPrimaryFilter(AppAttemptMetricsConstants.PARENT_PRIMARY_FILTER,
        appAttemptId.getApplicationId().toString());
    return entity;
  }

  private void publishContainerCreatedEvent(ContainerCreatedEvent event) {
    TimelineEntity entity = createContainerEntity(event.getContainerId());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_ENTITY_INFO,
        event.getAllocatedResource().getMemorySize());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_ENTITY_INFO,
        event.getAllocatedResource().getVirtualCores());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_ENTITY_INFO,
        event.getAllocatedNode().getHost());
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_ENTITY_INFO,
        event.getAllocatedNode().getPort());
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

  private void publishContainerFinishedEvent(ContainerFinishedEvent event) {
    TimelineEntity entity = createContainerEntity(event.getContainerId());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ContainerMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(event.getTimestamp());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        event.getDiagnosticsInfo());
    eventInfo.put(ContainerMetricsConstants.EXIT_STATUS_EVENT_INFO,
        event.getContainerExitStatus());
    eventInfo.put(ContainerMetricsConstants.STATE_EVENT_INFO,
        event.getContainerState().toString());
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

  private static TimelineEntity createContainerEntity(
      ContainerId containerId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(
        ContainerMetricsConstants.ENTITY_TYPE);
    entity.setEntityId(containerId.toString());
    entity.addPrimaryFilter(ContainerMetricsConstants.PARENT_PRIMARIY_FILTER,
        containerId.getApplicationAttemptId().toString());
    return entity;
  }

  @Private
  @VisibleForTesting
  public void putEntity(TimelineEntity entity) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Publishing the entity " + entity.getEntityId() +
            ", JSON-style content: " + TimelineUtils.dumpTimelineRecordtoJSON(entity));
      }
      TimelinePutResponse response = client.putEntities(entity);
      List<TimelinePutResponse.TimelinePutError> errors = response.getErrors();
      if (errors.size() == 0) {
        LOG.debug("Timeline entities are successfully put");
      } else {
        for (TimelinePutResponse.TimelinePutError error : errors) {
          LOG.error(
              "Error when publishing entity [" + error.getEntityType() + ","
                  + error.getEntityId() + "], server side error code: "
                  + error.getErrorCode());
        }
      }
    } catch (Exception e) {
      LOG.error("Error when publishing entity [" + entity.getEntityType() + ","
          + entity.getEntityId() + "]", e);
    }
  }

  /**
   * EventHandler implementation which forward events to SystemMetricsPublisher.
   * Making use of it, SystemMetricsPublisher can avoid to have a public handle
   * method.
   */
  private final class ForwardingEventHandler implements
      EventHandler<SystemMetricsEvent> {

    @Override
    public void handle(SystemMetricsEvent event) {
      handleSystemMetricsEvent(event);
    }

  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected static class MultiThreadedDispatcher extends CompositeService
      implements Dispatcher {

    private List<AsyncDispatcher> dispatchers =
        new ArrayList<AsyncDispatcher>();

    public MultiThreadedDispatcher(int num) {
      super(MultiThreadedDispatcher.class.getName());
      for (int i = 0; i < num; ++i) {
        AsyncDispatcher dispatcher = createDispatcher();
        dispatchers.add(dispatcher);
        addIfService(dispatcher);
      }
    }

    @Override
    public EventHandler getEventHandler() {
      return new CompositEventHandler();
    }

    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
      for (AsyncDispatcher dispatcher : dispatchers) {
        dispatcher.register(eventType, handler);
      }
    }

    public void setDrainEventsOnStop() {
      for (AsyncDispatcher dispatcher : dispatchers) {
        dispatcher.setDrainEventsOnStop();
      }
    }

    private class CompositEventHandler implements EventHandler<Event> {

      @Override
      public void handle(Event event) {
        // Use hashCode (of ApplicationId) to dispatch the event to the child
        // dispatcher, such that all the writing events of one application will
        // be handled by one thread, the scheduled order of the these events
        // will be preserved
        int index = (event.hashCode() & Integer.MAX_VALUE) % dispatchers.size();
        dispatchers.get(index).getEventHandler().handle(event);
      }

    }

    protected AsyncDispatcher createDispatcher() {
      return new AsyncDispatcher();
    }

  }

}
