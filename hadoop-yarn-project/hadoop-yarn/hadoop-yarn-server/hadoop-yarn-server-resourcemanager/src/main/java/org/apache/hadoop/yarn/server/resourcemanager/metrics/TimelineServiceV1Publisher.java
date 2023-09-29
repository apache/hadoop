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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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

  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineServiceV1Publisher.class);

  public TimelineServiceV1Publisher() {
    super("TimelineserviceV1Publisher");
  }

  private TimelineClient client;
  private LinkedBlockingQueue<TimelineEntity> entityQueue;
  private ExecutorService sendEventThreadPool;
  private int dispatcherPoolSize;
  private int dispatcherBatchSize;
  private int putEventInterval;
  private boolean isTimeLineServerBatchEnabled;
  private volatile boolean stopped = false;
  private PutEventThread putEventThread;
  private Object sendEntityLock;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    isTimeLineServerBatchEnabled =
        conf.getBoolean(
            YarnConfiguration.RM_TIMELINE_SERVER_V1_PUBLISHER_BATCH_ENABLED,
            YarnConfiguration.DEFAULT_RM_TIMELINE_SERVER_V1_PUBLISHER_BATCH_ENABLED);
    if (isTimeLineServerBatchEnabled) {
      putEventInterval =
          conf.getInt(YarnConfiguration.RM_TIMELINE_SERVER_V1_PUBLISHER_INTERVAL,
              YarnConfiguration.DEFAULT_RM_TIMELINE_SERVER_V1_PUBLISHER_INTERVAL)
              * 1000;
      if (putEventInterval <= 0) {
        throw new IllegalArgumentException(
            "RM_TIMELINE_SERVER_V1_PUBLISHER_INTERVAL should be greater than 0");
      }
      dispatcherPoolSize = conf.getInt(
          YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE,
          YarnConfiguration.
              DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE);
      if (dispatcherPoolSize <= 0) {
        throw new IllegalArgumentException(
            "RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE should be greater than 0");
      }
      dispatcherBatchSize = conf.getInt(
          YarnConfiguration.RM_TIMELINE_SERVER_V1_PUBLISHER_DISPATCHER_BATCH_SIZE,
          YarnConfiguration.
              DEFAULT_RM_TIMELINE_SERVER_V1_PUBLISHER_DISPATCHER_BATCH_SIZE);
      if (dispatcherBatchSize <= 1) {
        throw new IllegalArgumentException(
            "RM_TIMELINE_SERVER_V1_PUBLISHER_DISPATCHER_BATCH_SIZE should be greater than 1");
      }
      putEventThread = new PutEventThread();
      sendEventThreadPool = Executors.newFixedThreadPool(dispatcherPoolSize);
      entityQueue = new LinkedBlockingQueue<>(dispatcherBatchSize + 1);
      sendEntityLock = new Object();
      LOG.info("Timeline service v1 batch publishing enabled");
    } else {
      LOG.info("Timeline service v1 batch publishing disabled");
    }
    client = TimelineClient.createTimelineClient();
    addIfService(client);
    super.serviceInit(conf);
    getDispatcher().register(SystemMetricsEventType.class,
        new TimelineV1EventHandler());
  }

  protected void serviceStart() throws Exception {
    if (isTimeLineServerBatchEnabled) {
      stopped = false;
      putEventThread.start();
    }
    super.serviceStart();
  }

  protected void serviceStop() throws Exception {
    super.serviceStop();
    if (isTimeLineServerBatchEnabled) {
      stopped = true;
      putEventThread.interrupt();
      try {
        putEventThread.join();
        SendEntity task = new SendEntity();
        if (!task.buffer.isEmpty()) {
          LOG.info("Initiating final putEntities, remaining entities left in entityQueue: {}",
              task.buffer.size());
          sendEventThreadPool.submit(task);
        }
      } finally {
        sendEventThreadPool.shutdown();
        if (!sendEventThreadPool.awaitTermination(3, TimeUnit.SECONDS)) {
          sendEventThreadPool.shutdownNow();
        }
      }
    }
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
      if (app.getCallerContext().isContextValid()) {
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
  public void appLaunched(RMApp app, long launchTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.LAUNCHED_EVENT_TYPE);
    tEvent.setTimestamp(launchTime);
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
      RMAppAttemptState appAttemptState, RMApp app, long finishedTime) {
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
        .createApplicationAttemptState(appAttemptState).toString());
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
    if (isTimeLineServerBatchEnabled) {
      try {
        entityQueue.put(entity);
        if (entityQueue.size() > dispatcherBatchSize) {
          SendEntity task = null;
          synchronized (sendEntityLock) {
            if (entityQueue.size() > dispatcherBatchSize) {
              task = new SendEntity();
            }
          }
          if (task != null) {
            sendEventThreadPool.submit(task);
          }
        }
      } catch (Exception e) {
        LOG.error("Error when publishing entity batch  [ " + entity.getEntityType() + ","
            + entity.getEntityId() + " ] ", e);
      }
    } else {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Publishing the entity " + entity.getEntityId()
              + ", JSON-style content: "
              + TimelineUtils.dumpTimelineRecordtoJSON(entity));
        }
        client.putEntities(entity);
      } catch (Exception e) {
        LOG.error("Error when publishing entity [ " + entity.getEntityType() + ","
            + entity.getEntityId() + " ] ", e);
      }
    }
  }

  private class SendEntity implements Runnable {

    private ArrayList<TimelineEntity> buffer;

    SendEntity() {
      buffer = new ArrayList();
      entityQueue.drainTo(buffer);
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Number of timeline entities being sent in batch: {}", buffer.size());
      }
      if (buffer.isEmpty()) {
        return;
      }
      try {
        client.putEntities(buffer.toArray(new TimelineEntity[0]));
      } catch (Exception e) {
        LOG.error("Error when publishing entity: ", e);
      }
    }
  }

  private class TimelineV1PublishEvent extends TimelinePublishEvent {
    private TimelineEntity entity;

    TimelineV1PublishEvent(SystemMetricsEventType type,
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

  private class PutEventThread extends Thread {
    PutEventThread() {
      super("PutEventThread");
    }

    @Override
    public void run() {
      LOG.info("System metrics publisher will put events every " +
          String.valueOf(putEventInterval) + " milliseconds");
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        if (System.currentTimeMillis() % putEventInterval >= 1000) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            LOG.warn(SystemMetricsPublisher.class.getName()
                + " is interrupted. Exiting.");
            break;
          }
          continue;
        }
        SendEntity task = null;
        synchronized (sendEntityLock) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Creating SendEntity task in PutEventThread");
          }
          task = new SendEntity();
        }
        if (task != null) {
          sendEventThreadPool.submit(task);
        }
        try {
          // sleep added to avoid multiple SendEntity task within a single interval.
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.warn(SystemMetricsPublisher.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }
  }
}