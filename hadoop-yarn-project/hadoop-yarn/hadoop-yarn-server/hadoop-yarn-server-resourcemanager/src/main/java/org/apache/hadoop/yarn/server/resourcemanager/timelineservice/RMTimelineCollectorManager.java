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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollector;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorManager;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * This class extends TimelineCollectorManager to provide RM specific
 * implementations.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMTimelineCollectorManager extends TimelineCollectorManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMTimelineCollectorManager.class);

  private ResourceManager rm;

  public RMTimelineCollectorManager(ResourceManager resourceManager) {
    super(RMTimelineCollectorManager.class.getName());
    this.rm = resourceManager;
  }

  @Override
  protected void doPostPut(ApplicationId appId, TimelineCollector collector) {
    RMApp app = rm.getRMContext().getRMApps().get(appId);
    if (app == null) {
      throw new YarnRuntimeException(
          "Unable to get the timeline collector context info for a " +
          "non-existing app " + appId);
    }
    String userId = app.getUser();
    TimelineCollectorContext context = collector.getTimelineEntityContext();
    if (userId != null && !userId.isEmpty()) {
      context.setUserId(userId);
    }

    // initialize the flow in the environment with default values for those
    // that do not specify the flow tags
    // flow name: app name (or app id if app name is missing),
    // flow version: "1", flow run id: start time
    context.setFlowName(TimelineUtils.generateDefaultFlowName(
        app.getName(), appId));
    context.setFlowVersion(TimelineUtils.DEFAULT_FLOW_VERSION);
    context.setFlowRunId(app.getStartTime());

    // the flow context is received via the application tags
    for (String tag : app.getApplicationTags()) {
      String[] parts = tag.split(":", 2);
      if (parts.length != 2 || parts[1].isEmpty()) {
        continue;
      }
      switch (parts[0].toUpperCase()) {
      case TimelineUtils.FLOW_NAME_TAG_PREFIX:
        LOG.debug("Setting the flow name: {}", parts[1]);
        context.setFlowName(parts[1]);
        break;
      case TimelineUtils.FLOW_VERSION_TAG_PREFIX:
        LOG.debug("Setting the flow version: {}", parts[1]);
        context.setFlowVersion(parts[1]);
        break;
      case TimelineUtils.FLOW_RUN_ID_TAG_PREFIX:
        LOG.debug("Setting the flow run id: {}", parts[1]);
        context.setFlowRunId(Long.parseLong(parts[1]));
        break;
      default:
        break;
      }
    }
  }
}
