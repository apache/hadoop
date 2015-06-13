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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher.TimelineServicePublisher;

public abstract class AbstractTimelineServicePublisher extends CompositeService
    implements TimelineServicePublisher, EventHandler<SystemMetricsEvent> {

  private static final Log LOG = LogFactory
      .getLog(TimelineServiceV2Publisher.class);

  private Configuration conf;

  public AbstractTimelineServicePublisher(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  @Override
  public void handle(SystemMetricsEvent event) {
    switch (event.getType()) {
    case APP_CREATED:
      publishApplicationCreatedEvent((ApplicationCreatedEvent) event);
      break;
    case APP_FINISHED:
      publishApplicationFinishedEvent((ApplicationFinishedEvent) event);
      break;
    case APP_UPDATED:
      publishApplicationUpdatedEvent((ApplicationUpdatedEvent) event);
      break;
    case APP_STATE_UPDATED:
      publishApplicationStateUpdatedEvent(
          (ApplicaitonStateUpdatedEvent)event);
      break;
    case APP_ACLS_UPDATED:
      publishApplicationACLsUpdatedEvent((ApplicationACLsUpdatedEvent) event);
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

  abstract void publishAppAttemptFinishedEvent(AppAttemptFinishedEvent event);

  abstract void publishAppAttemptRegisteredEvent(AppAttemptRegisteredEvent event);

  abstract void publishApplicationUpdatedEvent(ApplicationUpdatedEvent event);

  abstract void publishApplicationStateUpdatedEvent(
      ApplicaitonStateUpdatedEvent event);

  abstract void publishApplicationACLsUpdatedEvent(
      ApplicationACLsUpdatedEvent event);

  abstract void publishApplicationFinishedEvent(ApplicationFinishedEvent event);

  abstract void publishApplicationCreatedEvent(ApplicationCreatedEvent event);

  abstract void publishContainerCreatedEvent(ContainerCreatedEvent event);

  abstract void publishContainerFinishedEvent(ContainerFinishedEvent event);

  @Override
  public Dispatcher getDispatcher() {
    MultiThreadedDispatcher dispatcher =
        new MultiThreadedDispatcher(
            conf.getInt(
                YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE,
                YarnConfiguration.DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE));
    dispatcher.setDrainEventsOnStop();
    return dispatcher;
  }

  @Override
  public boolean publishRMContainerMetrics() {
    return true;
  }

  @Override
  public EventHandler<SystemMetricsEvent> getEventHandler() {
    return this;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static class MultiThreadedDispatcher extends CompositeService
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
