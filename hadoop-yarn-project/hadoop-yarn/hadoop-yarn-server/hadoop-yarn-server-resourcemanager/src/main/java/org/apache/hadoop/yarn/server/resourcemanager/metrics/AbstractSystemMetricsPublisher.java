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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;

/**
 * Abstract implementation of SystemMetricsPublisher which is then extended by
 * metrics publisher implementations depending on timeline service version.
 */
public abstract class AbstractSystemMetricsPublisher extends CompositeService
    implements SystemMetricsPublisher {
  private MultiThreadedDispatcher dispatcher;

  protected Dispatcher getDispatcher() {
    return dispatcher;
  }

  public AbstractSystemMetricsPublisher(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    dispatcher =
    new MultiThreadedDispatcher(getConfig().getInt(
        YarnConfiguration.
        RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE,
        YarnConfiguration.
        DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE));
    dispatcher.setDrainEventsOnStop();
    addIfService(dispatcher);
    super.serviceInit(conf);
  }

  /**
   * Dispatches ATS related events using multiple threads.
   */
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
    public EventHandler<Event> getEventHandler() {
      return new CompositEventHandler();
    }

    @Override
    public void register(Class<? extends Enum> eventType,
        EventHandler handler) {
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
      return new AsyncDispatcher("RM Timeline dispatcher");
    }
  }

  /**
   * EventType which is used while publishing the events.
   */
  protected enum SystemMetricsEventType {
    PUBLISH_ENTITY, PUBLISH_APPLICATION_FINISHED_ENTITY
  }

  @Override
  public void appLaunched(RMApp app, long launchTime) {
  }

  /**
   * TimelinePublishEvent's hash code should be based on application's id this
   * will ensure all the events related to a particular app goes to particular
   * thread of MultiThreaded dispatcher.
   */
  protected static abstract class TimelinePublishEvent
      extends AbstractEvent<SystemMetricsEventType> {

    private ApplicationId appId;

    public TimelinePublishEvent(SystemMetricsEventType type,
        ApplicationId appId) {
      super(type);
      this.appId = appId;
    }

    public ApplicationId getApplicationId() {
      return appId;
    }

    @Override
    public int hashCode() {
      return appId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof TimelinePublishEvent)) {
        return false;
      }
      TimelinePublishEvent other = (TimelinePublishEvent) obj;
      if (appId == null) {
        if (other.appId != null) {
          return false;
        }
      } else if (getType() == null) {
        if (other.getType() != null) {
          return false;
        }
      } else {
        if (!appId.equals(other.appId) || !getType().equals(other.getType())) {
          return false;
        }
      }
      return true;
    }
  }
}
