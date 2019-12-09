/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter.TimelineFilterType;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;

/**
 * A bunch of utility functions used across TimelineReader and TimelineWriter.
 */
@Public
@Unstable
public final class TimelineStorageUtils {
  private TimelineStorageUtils() {
  }

  /**
   * Matches key-values filter. Used for relatesTo/isRelatedTo filters.
   *
   * @param entity entity which holds relatesTo/isRelatedTo relations which we
   *     will match against.
   * @param keyValuesFilter key-values filter.
   * @param entityFiltersType type of filters we are trying to match.
   * @return true, if filter matches, false otherwise.
   */
  private static boolean matchKeyValuesFilter(TimelineEntity entity,
      TimelineKeyValuesFilter keyValuesFilter,
      TimelineEntityFiltersType entityFiltersType) {
    Map<String, Set<String>> relations = null;
    if (entityFiltersType == TimelineEntityFiltersType.IS_RELATED_TO) {
      relations = entity.getIsRelatedToEntities();
    } else if (entityFiltersType == TimelineEntityFiltersType.RELATES_TO) {
      relations = entity.getRelatesToEntities();
    }
    if (relations == null) {
      return false;
    }
    Set<String> ids = relations.get(keyValuesFilter.getKey());
    if (ids == null) {
      return false;
    }
    boolean matched = false;
    for (Object id : keyValuesFilter.getValues()) {
      // Matches if id is found amongst the relationships for an entity and
      // filter's compare op is EQUAL.
      // If compare op is NOT_EQUAL, for a match to occur, id should not be
      // found amongst relationships for an entity.
      matched = !(ids.contains(id) ^
          keyValuesFilter.getCompareOp() == TimelineCompareOp.EQUAL);
      if (!matched) {
        return false;
      }
    }
    return true;
  }

  /**
   * Matches relatesto.
   *
   * @param entity entity which holds relatesto relations.
   * @param relatesTo the relations for filtering.
   * @return true, if filter matches, false otherwise.
   * @throws IOException if an unsupported filter for matching relations is
   *     being matched.
   */
  public static boolean matchRelatesTo(TimelineEntity entity,
      TimelineFilterList relatesTo) throws IOException {
    return matchFilters(
        entity, relatesTo, TimelineEntityFiltersType.RELATES_TO);
  }

  /**
   * Matches isrelatedto.
   *
   * @param entity entity which holds isRelatedTo relations.
   * @param isRelatedTo the relations for filtering.
   * @return true, if filter matches, false otherwise.
   * @throws IOException if an unsupported filter for matching relations is
   *     being matched.
   */
  public static boolean matchIsRelatedTo(TimelineEntity entity,
      TimelineFilterList isRelatedTo) throws IOException {
    return matchFilters(
        entity, isRelatedTo, TimelineEntityFiltersType.IS_RELATED_TO);
  }

  /**
   * Matches key-value filter. Used for config and info filters.
   *
   * @param entity entity which holds the config/info which we will match
   *     against.
   * @param kvFilter a key-value filter.
   * @param entityFiltersType type of filters we are trying to match.
   * @return true, if filter matches, false otherwise.
   */
  private static boolean matchKeyValueFilter(TimelineEntity entity,
      TimelineKeyValueFilter kvFilter,
      TimelineEntityFiltersType entityFiltersType) {
    Map<String, ? extends Object> map = null;
    // Supported only for config and info filters.
    if (entityFiltersType == TimelineEntityFiltersType.CONFIG) {
      map = entity.getConfigs();
    } else if (entityFiltersType == TimelineEntityFiltersType.INFO) {
      map = entity.getInfo();
    }
    if (map == null) {
      return false;
    }
    Object value = map.get(kvFilter.getKey());
    if (value == null) {
      return false;
    }
    // Matches if filter's value is equal to the value of the key and filter's
    // compare op is EQUAL.
    // If compare op is NOT_EQUAL, for a match to occur, value should not be
    // equal to the value of the key.
    return !(value.equals(kvFilter.getValue()) ^
        kvFilter.getCompareOp() == TimelineCompareOp.EQUAL);
  }

  /**
   * Matches config filters.
   *
   * @param entity entity which holds a map of config key-value pairs.
   * @param configFilters list of info filters.
   * @return a boolean flag to indicate if both match.
   * @throws IOException if an unsupported filter for matching config filters is
   *     being matched.
   */
  public static boolean matchConfigFilters(TimelineEntity entity,
      TimelineFilterList configFilters) throws IOException {
    return
        matchFilters(entity, configFilters, TimelineEntityFiltersType.CONFIG);
  }

  /**
   * Matches info filters.
   *
   * @param entity entity which holds a map of info key-value pairs.
   * @param infoFilters list of info filters.
   * @return a boolean flag to indicate if both match.
   * @throws IOException if an unsupported filter for matching info filters is
   *     being matched.
   */
  public static boolean matchInfoFilters(TimelineEntity entity,
      TimelineFilterList infoFilters) throws IOException {
    return matchFilters(entity, infoFilters, TimelineEntityFiltersType.INFO);
  }

  /**
   * Matches exists filter. Used for event filters.
   *
   * @param entity entity which holds the events which we will match against.
   * @param existsFilter exists filter.
   * @param entityFiltersType type of filters we are trying to match.
   * @return true, if filter matches, false otherwise.
   */
  private static boolean matchExistsFilter(TimelineEntity entity,
      TimelineExistsFilter existsFilter,
      TimelineEntityFiltersType entityFiltersType) {
    // Currently exists filter is only supported for event filters.
    if (entityFiltersType != TimelineEntityFiltersType.EVENT) {
      return false;
    }
    Set<String> eventIds = new HashSet<String>();
    for (TimelineEvent event : entity.getEvents()) {
      eventIds.add(event.getId());
    }
    // Matches if filter's value is contained in the list of events filter's
    // compare op is EQUAL.
    // If compare op is NOT_EQUAL, for a match to occur, value should not be
    // contained in the list of events.
    return !(eventIds.contains(existsFilter.getValue()) ^
        existsFilter.getCompareOp() == TimelineCompareOp.EQUAL);
  }

  /**
   * Matches event filters.
   *
   * @param entity entity which holds a set of event objects.
   * @param eventFilters the set of event Ids for filtering.
   * @return a boolean flag to indicate if both match.
   * @throws IOException if an unsupported filter for matching event filters is
   *     being matched.
   */
  public static boolean matchEventFilters(TimelineEntity entity,
      TimelineFilterList eventFilters) throws IOException {
    return matchFilters(entity, eventFilters, TimelineEntityFiltersType.EVENT);
  }

  /**
   * Compare two values based on comparison operator.
   *
   * @param compareOp comparison operator.
   * @param val1 value 1.
   * @param val2 value 2.
   * @return true, if relation matches, false otherwise
   */
  private static boolean compareValues(TimelineCompareOp compareOp,
      long val1, long val2) {
    switch (compareOp) {
    case LESS_THAN:
      return val1 < val2;
    case LESS_OR_EQUAL:
      return val1 <= val2;
    case EQUAL:
      return val1 == val2;
    case NOT_EQUAL:
      return val1 != val2;
    case GREATER_OR_EQUAL:
      return val1 >= val2;
    case GREATER_THAN:
      return val1 > val2;
    default:
      throw new RuntimeException("Unknown TimelineCompareOp " +
          compareOp.name());
    }
  }

  /**
   * Matches compare filter. Used for metric filters.
   *
   * @param entity entity which holds the metrics which we will match against.
   * @param compareFilter compare filter.
   * @param entityFiltersType type of filters we are trying to match.
   * @return true, if filter matches, false otherwise.
   * @throws IOException if metric filters holds non integral values.
   */
  private static boolean matchCompareFilter(TimelineEntity entity,
      TimelineCompareFilter compareFilter,
      TimelineEntityFiltersType entityFiltersType) throws IOException {
    // Currently exists filter is only supported for metric filters.
    if (entityFiltersType != TimelineEntityFiltersType.METRIC) {
      return false;
    }
    // We expect only integral values(short/int/long) for metric filters.
    if (!isIntegralValue(compareFilter.getValue())) {
      throw new IOException("Metric filters has non integral values");
    }
    Map<String, TimelineMetric> metricMap =
        new HashMap<String, TimelineMetric>();
    for (TimelineMetric metric : entity.getMetrics()) {
      metricMap.put(metric.getId(), metric);
    }
    TimelineMetric metric = metricMap.get(compareFilter.getKey());
    if (metric == null) {
      return false;
    }
    // We will be using the latest value of metric to compare.
    return compareValues(compareFilter.getCompareOp(),
        metric.getValuesJAXB().firstEntry().getValue().longValue(),
        ((Number)compareFilter.getValue()).longValue());
  }

  /**
   * Matches metric filters.
   *
   * @param entity entity which holds a set of metric objects.
   * @param metricFilters list of metric filters.
   * @return a boolean flag to indicate if both match.
   * @throws IOException if an unsupported filter for matching metric filters is
   *     being matched.
   */
  public static boolean matchMetricFilters(TimelineEntity entity,
      TimelineFilterList metricFilters) throws IOException {
    return matchFilters(
        entity, metricFilters, TimelineEntityFiltersType.METRIC);
  }

  /**
   * Common routine to match different filters. Iterates over a filter list and
   * calls routines based on filter type.
   *
   * @param entity Timeline entity.
   * @param filters filter list.
   * @param entityFiltersType type of filters which are being matched.
   * @return a boolean flag to indicate if filter matches.
   * @throws IOException if an unsupported filter for matching this specific
   *     filter is being matched.
   */
  private static boolean matchFilters(TimelineEntity entity,
      TimelineFilterList filters, TimelineEntityFiltersType entityFiltersType)
      throws IOException {
    if (filters == null || filters.getFilterList().isEmpty()) {
      return false;
    }
    TimelineFilterList.Operator operator = filters.getOperator();
    for (TimelineFilter filter : filters.getFilterList()) {
      TimelineFilterType filterType = filter.getFilterType();
      if (!entityFiltersType.isValidFilter(filterType)) {
        throw new IOException("Unsupported filter " + filterType);
      }
      boolean matched = false;
      switch (filterType) {
      case LIST:
        matched = matchFilters(entity, (TimelineFilterList)filter,
            entityFiltersType);
        break;
      case COMPARE:
        matched = matchCompareFilter(entity, (TimelineCompareFilter)filter,
            entityFiltersType);
        break;
      case EXISTS:
        matched = matchExistsFilter(entity, (TimelineExistsFilter)filter,
            entityFiltersType);
        break;
      case KEY_VALUE:
        matched = matchKeyValueFilter(entity, (TimelineKeyValueFilter)filter,
            entityFiltersType);
        break;
      case KEY_VALUES:
        matched = matchKeyValuesFilter(entity, (TimelineKeyValuesFilter)filter,
            entityFiltersType);
        break;
      default:
        throw new IOException("Unsupported filter " + filterType);
      }
      if (!matched) {
        if(operator == TimelineFilterList.Operator.AND) {
          return false;
        }
      } else {
        if(operator == TimelineFilterList.Operator.OR) {
          return true;
        }
      }
    }
    return operator == TimelineFilterList.Operator.AND;
  }

  /**
   * Checks if passed object is of integral type(Short/Integer/Long).
   *
   * @param obj Object to be checked.
   * @return true if object passed is of type Short or Integer or Long, false
   * otherwise.
   */
  public static boolean isIntegralValue(Object obj) {
    return (obj instanceof Short) || (obj instanceof Integer) ||
        (obj instanceof Long);
  }
}
