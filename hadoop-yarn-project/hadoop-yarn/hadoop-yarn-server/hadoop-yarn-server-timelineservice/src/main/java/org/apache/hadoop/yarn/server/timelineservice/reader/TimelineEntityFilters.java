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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Encapsulates information regarding the filters to apply while querying. These
 * filters restrict the number of entities to return.<br>
 * Filters contain the following :<br>
 * <ul>
 * <li><b>limit</b> - A limit on the number of entities to return. If null
 * or {@literal <=0}, defaults to {@link #DEFAULT_LIMIT}.</li>
 * <li><b>createdTimeBegin</b> - Matched entities should not be created
 * before this timestamp. If null or {@literal <=0}, defaults to 0.</li>
 * <li><b>createdTimeEnd</b> - Matched entities should not be created after
 * this timestamp. If null or {@literal <=0}, defaults to
 * {@link Long#MAX_VALUE}.</li>
 * <li><b>relatesTo</b> - Matched entities should relate to given entities.
 * If null or empty, the relations are not matched.</li>
 * <li><b>isRelatedTo</b> - Matched entities should be related to given
 * entities. If null or empty, the relations are not matched.</li>
 * <li><b>infoFilters</b> - Matched entities should have exact matches to
 * the given info represented as key-value pairs. If null or empty, the
 * filter is not applied.</li>
 * <li><b>configFilters</b> - Matched entities should have exact matches to
 * the given configs represented as key-value pairs. If null or empty, the
 * filter is not applied.</li>
 * <li><b>metricFilters</b> - Matched entities should contain the given
 * metrics. If null or empty, the filter is not applied.</li>
 * <li><b>eventFilters</b> - Matched entities should contain the given
 * events. If null or empty, the filter is not applied.</li>
 * </ul>
 */
@Private
@Unstable
public class TimelineEntityFilters {
  private Long limit;
  private Long createdTimeBegin;
  private Long createdTimeEnd;
  private Map<String, Set<String>> relatesTo;
  private Map<String, Set<String>> isRelatedTo;
  private Map<String, Object> infoFilters;
  private Map<String, String> configFilters;
  private Set<String>  metricFilters;
  private Set<String> eventFilters;
  private static final Long DEFAULT_BEGIN_TIME = 0L;
  private static final Long DEFAULT_END_TIME = Long.MAX_VALUE;

  /**
   * Default limit of number of entities to return for getEntities API.
   */
  public static final long DEFAULT_LIMIT = 100;

  public TimelineEntityFilters() {
    this(null, null, null, null, null, null, null, null, null);
  }

  public TimelineEntityFilters(
      Long entityLimit, Long timeBegin, Long timeEnd,
      Map<String, Set<String>> entityRelatesTo,
      Map<String, Set<String>> entityIsRelatedTo,
      Map<String, Object> entityInfoFilters,
      Map<String, String> entityConfigFilters,
      Set<String>  entityMetricFilters,
      Set<String> entityEventFilters) {
    this.limit = entityLimit;
    if (this.limit == null || this.limit < 0) {
      this.limit = DEFAULT_LIMIT;
    }
    this.createdTimeBegin = timeBegin;
    if (this.createdTimeBegin == null || this.createdTimeBegin < 0) {
      this.createdTimeBegin = DEFAULT_BEGIN_TIME;
    }
    this.createdTimeEnd = timeEnd;
    if (this.createdTimeEnd == null || this.createdTimeEnd < 0) {
      this.createdTimeEnd = DEFAULT_END_TIME;
    }
    this.relatesTo = entityRelatesTo;
    this.isRelatedTo = entityIsRelatedTo;
    this.infoFilters = entityInfoFilters;
    this.configFilters = entityConfigFilters;
    this.metricFilters = entityMetricFilters;
    this.eventFilters = entityEventFilters;
  }

  public Long getLimit() {
    return limit;
  }

  public void setLimit(Long entityLimit) {
    this.limit = entityLimit;
    if (this.limit == null || this.limit < 0) {
      this.limit = DEFAULT_LIMIT;
    }
  }

  public Long getCreatedTimeBegin() {
    return createdTimeBegin;
  }

  public void setCreatedTimeBegin(Long timeBegin) {
    this.createdTimeBegin = timeBegin;
    if (this.createdTimeBegin == null || this.createdTimeBegin < 0) {
      this.createdTimeBegin = DEFAULT_BEGIN_TIME;
    }
  }

  public Long getCreatedTimeEnd() {
    return createdTimeEnd;
  }

  public void setCreatedTimeEnd(Long timeEnd) {
    this.createdTimeEnd = timeEnd;
    if (this.createdTimeEnd == null || this.createdTimeEnd < 0) {
      this.createdTimeEnd = DEFAULT_END_TIME;
    }
  }

  public Map<String, Set<String>> getRelatesTo() {
    return relatesTo;
  }

  public void setRelatesTo(Map<String, Set<String>> relations) {
    this.relatesTo = relations;
  }

  public Map<String, Set<String>> getIsRelatedTo() {
    return isRelatedTo;
  }

  public void setIsRelatedTo(Map<String, Set<String>> relations) {
    this.isRelatedTo = relations;
  }

  public Map<String, Object> getInfoFilters() {
    return infoFilters;
  }

  public void setInfoFilters(Map<String, Object> filters) {
    this.infoFilters = filters;
  }

  public Map<String, String> getConfigFilters() {
    return configFilters;
  }

  public void setConfigFilters(Map<String, String> filters) {
    this.configFilters = filters;
  }

  public Set<String> getMetricFilters() {
    return metricFilters;
  }

  public void setMetricFilters(Set<String> filters) {
    this.metricFilters = filters;
  }

  public Set<String> getEventFilters() {
    return eventFilters;
  }

  public void setEventFilters(Set<String> filters) {
    this.eventFilters = filters;
  }
}
