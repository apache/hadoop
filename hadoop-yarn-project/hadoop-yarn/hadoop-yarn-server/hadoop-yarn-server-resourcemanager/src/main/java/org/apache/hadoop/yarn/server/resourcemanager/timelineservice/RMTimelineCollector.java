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

package org.apache.hadoop.yarn.server.resourcemanager.timelineservice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsEvent;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsEventType;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollector;

/**
 * This class is responsible for posting application and appattempt lifecycle
 * related events to timeline service V2
 */
@Private
@Unstable
public class RMTimelineCollector extends TimelineCollector {
  private static final Log LOG = LogFactory.getLog(RMTimelineCollector.class);

  public RMTimelineCollector() {
    super("Resource Manager TimelineCollector");
  }

  private Dispatcher dispatcher;

  private boolean publishSystemMetricsForV2;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    publishSystemMetricsForV2 =
        conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)
            && conf.getBoolean(
                YarnConfiguration.SYSTEM_METRICS_PUBLISHER_ENABLED,
                YarnConfiguration.DEFAULT_SYSTEM_METRICS_PUBLISHER_ENABLED);

    if (publishSystemMetricsForV2) {
      // having separate dispatcher to avoid load on RMDispatcher
      LOG.info("RMTimelineCollector has been configured to publish"
          + " System Metrics in ATS V2");
      dispatcher = new AsyncDispatcher();
      dispatcher.register(SystemMetricsEventType.class,
          new ForwardingEventHandler());
    } else {
      LOG.warn("RMTimelineCollector has not been configured to publish"
          + " System Metrics in ATS V2");
    }
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

  protected void handleSystemMetricsEvent(SystemMetricsEvent event) {
    switch (event.getType()) {
    default:
      LOG.error("Unknown SystemMetricsEvent type: " + event.getType());
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
}
