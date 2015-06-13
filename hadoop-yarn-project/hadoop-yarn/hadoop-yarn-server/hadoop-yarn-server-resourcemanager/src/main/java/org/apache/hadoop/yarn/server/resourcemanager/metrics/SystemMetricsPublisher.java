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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import com.google.common.annotations.VisibleForTesting;

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
  private boolean publishSystemMetrics;
  private boolean publishContainerMetrics;
  protected RMContext rmContext;

  public SystemMetricsPublisher(RMContext rmContext) {
    super(SystemMetricsPublisher.class.getName());
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    publishSystemMetrics =
        conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED);
    if (publishSystemMetrics) {
      TimelineServicePublisher timelineServicePublisher =
          getTimelineServicePublisher(conf);
      if (timelineServicePublisher != null) {
        addService(timelineServicePublisher);
        // init required to be called so that other methods of
        // TimelineServicePublisher can be utilized
        timelineServicePublisher.init(conf);
        dispatcher = createDispatcher(timelineServicePublisher);
        publishContainerMetrics =
            timelineServicePublisher.publishRMContainerMetrics();
        dispatcher.register(SystemMetricsEventType.class,
            timelineServicePublisher.getEventHandler());
        addIfService(dispatcher);
      } else {
        LOG.info("TimelineServicePublisher is not configured");
        publishSystemMetrics = false;
      }
      LOG.info("YARN system metrics publishing service is enabled");
    } else {
      LOG.info("YARN system metrics publishing service is not enabled");
    }
    super.serviceInit(conf);
  }

  @VisibleForTesting
  Dispatcher createDispatcher(TimelineServicePublisher timelineServicePublisher) {
    return timelineServicePublisher.getDispatcher();
  }

  TimelineServicePublisher getTimelineServicePublisher(Configuration conf) {
    if (conf.getBoolean(YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_ENABLED,
        YarnConfiguration.DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_ENABLED)) {
      return new TimelineServiceV1Publisher();
    } else if (conf.getBoolean(
        YarnConfiguration.SYSTEM_METRICS_PUBLISHER_ENABLED,
        YarnConfiguration.DEFAULT_SYSTEM_METRICS_PUBLISHER_ENABLED)) {
      return new TimelineServiceV2Publisher(rmContext);
    }
    return null;
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
              app.getRMAppMetrics(),
              (RMAppImpl)app));
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
    if (publishContainerMetrics) {
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
    if (publishContainerMetrics) {
      dispatcher.getEventHandler().handle(
          new ContainerFinishedEvent(
              container.getContainerId(),
              container.getDiagnosticsInfo(),
              container.getContainerExitStatus(),
              container.getContainerState(),
              finishedTime, container.getAllocatedNode()));
    }
  }

  @VisibleForTesting
  boolean isPublishContainerMetrics() {
    return publishContainerMetrics;
  }

  @VisibleForTesting
  Dispatcher getDispatcher() {
    return dispatcher;
  }

  interface TimelineServicePublisher extends Service {
    /**
     * @return the Dispatcher which needs to be used to dispatch events
     */
    Dispatcher getDispatcher();

    /**
     * @return true if RMContainerMetricsNeeds to be sent
     */
    boolean publishRMContainerMetrics();

    /**
     * @return EventHandler which needs to be registered to the dispatcher to
     *         handle the SystemMetricsEvent
     */
    EventHandler<SystemMetricsEvent> getEventHandler();
  }
}
