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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class TimelineReaderUtils {
  /**
   *
   * @param entityRelations the relations of an entity
   * @param relationFilters the relations for filtering
   * @return a boolean flag to indicate if both match
   */
  public static boolean matchRelations(
      Map<String, Set<String>> entityRelations,
      Map<String, Set<String>> relationFilters) {
    for (Map.Entry<String, Set<String>> relation : relationFilters.entrySet()) {
      Set<String> ids = entityRelations.get(relation.getKey());
      if (ids == null) {
        return false;
      }
      for (String id : relation.getValue()) {
        if (!ids.contains(id)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   *
   * @param map the map of key/value pairs in an entity
   * @param filters the map of key/value pairs for filtering
   * @return a boolean flag to indicate if both match
   */
  public static boolean matchFilters(Map<String, ? extends Object> map,
      Map<String, ? extends Object> filters) {
    for (Map.Entry<String, ? extends Object> filter : filters.entrySet()) {
      Object value = map.get(filter.getKey());
      if (value == null) {
        return false;
      }
      if (!value.equals(filter.getValue())) {
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @param entityEvents the set of event objects in an entity
   * @param eventFilters the set of event Ids for filtering
   * @return a boolean flag to indicate if both match
   */
  public static boolean matchEventFilters(Set<TimelineEvent> entityEvents,
      Set<String> eventFilters) {
    Set<String> eventIds = new HashSet<String>();
    for (TimelineEvent event : entityEvents) {
      eventIds.add(event.getId());
    }
    for (String eventFilter : eventFilters) {
      if (!eventIds.contains(eventFilter)) {
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @param metrics the set of metric objects in an entity
   * @param metricFilters the set of metric Ids for filtering
   * @return a boolean flag to indicate if both match
   */
  public static boolean matchMetricFilters(Set<TimelineMetric> metrics,
      Set<String> metricFilters) {
    Set<String> metricIds = new HashSet<String>();
    for (TimelineMetric metric : metrics) {
      metricIds.add(metric.getId());
    }

    for (String metricFilter : metricFilters) {
      if (!metricIds.contains(metricFilter)) {
        return false;
      }
    }
    return true;
  }
}
