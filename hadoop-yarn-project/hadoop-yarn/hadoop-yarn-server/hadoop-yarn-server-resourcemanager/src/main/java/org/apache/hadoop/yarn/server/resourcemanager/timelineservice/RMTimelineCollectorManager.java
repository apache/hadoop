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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollector;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorManager;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMTimelineCollectorManager extends TimelineCollectorManager {
  private RMContext rmContext;

  public RMTimelineCollectorManager(RMContext rmContext) {
    super(RMTimelineCollectorManager.class.getName());
    this.rmContext = rmContext;
  }

  @Override
  public void postPut(ApplicationId appId, TimelineCollector collector) {
    RMApp app = rmContext.getRMApps().get(appId);
    if (app == null) {
      throw new YarnRuntimeException(
          "Unable to get the timeline collector context info for a non-existing app " +
              appId);
    }
    String userId = app.getUser();
    if (userId != null && !userId.isEmpty()) {
      collector.getTimelineEntityContext().setUserId(userId);
    }
    for (String tag : app.getApplicationTags()) {
      String[] parts = tag.split(":", 2);
      if (parts.length != 2 || parts[1].isEmpty()) {
        continue;
      }
      switch (parts[0]) {
        case TimelineUtils.FLOW_NAME_TAG_PREFIX:
          collector.getTimelineEntityContext().setFlowName(parts[1]);
          break;
        case TimelineUtils.FLOW_VERSION_TAG_PREFIX:
          collector.getTimelineEntityContext().setFlowVersion(parts[1]);
          break;
        case TimelineUtils.FLOW_RUN_ID_TAG_PREFIX:
          collector.getTimelineEntityContext().setFlowRunId(
              Long.parseLong(parts[1]));
          break;
        default:
          break;
      }
    }
  }
}
