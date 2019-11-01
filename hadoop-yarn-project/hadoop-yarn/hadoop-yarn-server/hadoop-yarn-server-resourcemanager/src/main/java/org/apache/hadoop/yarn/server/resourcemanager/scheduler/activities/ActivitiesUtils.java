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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivityNodeInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for activities.
 */
public final class ActivitiesUtils {

  private ActivitiesUtils(){}

  public static List<ActivityNodeInfo> getRequestActivityNodeInfos(
      List<ActivityNode> activityNodes,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    if (activityNodes == null) {
      return null;
    }
    if (groupBy == RMWSConsts.ActivitiesGroupBy.DIAGNOSTIC) {
      Map<ActivityState, Map<String, List<String>>> groupingResults =
          activityNodes.stream()
              .filter(e -> e.getNodeId() != null)
              .collect(Collectors.groupingBy(ActivityNode::getState, Collectors
                  .groupingBy(ActivityNode::getShortDiagnostic,
                      Collectors.mapping(e -> e.getNodeId() == null ? "" :
                          e.getNodeId().toString(), Collectors.toList()))));
      return groupingResults.entrySet().stream().flatMap(
          stateMap -> stateMap.getValue().entrySet().stream().map(
              diagMap -> new ActivityNodeInfo(stateMap.getKey(),
                  diagMap.getKey().isEmpty() ? null : diagMap.getKey(),
                  diagMap.getValue())))
          .collect(Collectors.toList());
    } else {
      return activityNodes.stream().filter(e -> e.getNodeId() != null)
          .map(e -> new ActivityNodeInfo(e.getName(), e.getState(),
              e.getDiagnostic(), e.getNodeId())).collect(Collectors.toList());
    }
  }
}
