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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;

/**
 * Encapsulates information regarding the filters to apply while querying. These
 * filters restrict the number of entities to return.<br>
 * Filters contain the following :<br>
 * <ul>
 * <li><b>limit</b> - A limit on the number of entities to return. If null or
 * {@literal < 0}, defaults to {@link #DEFAULT_LIMIT}. The maximum possible
 * value for limit can be {@link Long#MAX_VALUE}.</li>
 * <li><b>createdTimeBegin</b> - Matched entities should not be created before
 * this timestamp. If null or {@literal <=0}, defaults to 0.</li>
 * <li><b>createdTimeEnd</b> - Matched entities should not be created after this
 * timestamp. If null or {@literal <=0}, defaults to
 * {@link Long#MAX_VALUE}.</li>
 * <li><b>relatesTo</b> - Matched entities should or should not relate to given
 * entities depending on what's specified in the filter. The entities in
 * relatesTo are identified by entity type and id. This is represented as
 * a {@link TimelineFilterList} object containing
 * {@link TimelineKeyValuesFilter} objects, each of which contains a
 * set of values for a key and the comparison operator (equals/not equals). The
 * key which represents the entity type is a string and values are a set of
 * entity identifiers (also string). As it is a filter list, relatesTo can be
 * evaluated with logical AND/OR and we can create a hierarchy of these
 * {@link TimelineKeyValuesFilter} objects. If null or empty, the relations are
 * not matched.</li>
 * <li><b>isRelatedTo</b> - Matched entities should or should not be related
 * to given entities depending on what's specified in the filter. The entities
 * in isRelatedTo are identified by entity type and id.  This is represented as
 * a {@link TimelineFilterList} object containing
 * {@link TimelineKeyValuesFilter} objects, each of which contains a
 * set of values for a key and the comparison operator (equals/not equals). The
 * key which represents the entity type is a string and values are a set of
 * entity identifiers (also string). As it is a filter list, relatesTo can be
 * evaluated with logical AND/OR and we can create a hierarchy of these
 * {@link TimelineKeyValuesFilter} objects. If null or empty, the relations are
 * not matched.</li>
 * <li><b>infoFilters</b> - Matched entities should have exact matches to
 * the given info and should be either equal or not equal to given value
 * depending on what's specified in the filter. This is represented as a
 * {@link TimelineFilterList} object containing {@link TimelineKeyValueFilter}
 * objects, each of which contains key-value pairs with a comparison operator
 * (equals/not equals). The key which represents the info key is a string but
 * value can be any object. As it is a filter list, info filters can be
 * evaluated with logical AND/OR and we can create a hierarchy of these
 * key-value pairs. If null or empty, the filter is not applied.</li>
 * <li><b>configFilters</b> - Matched entities should have exact matches to
 * the given configurations and should be either equal or not equal to given
 * value depending on what's specified in the filter. This is represented as a
 * {@link TimelineFilterList} object containing {@link TimelineKeyValueFilter}
 * objects, each of which contains key-value pairs with a comparison operator
 * (equals/not equals). Both key (which represents config name) and value (which
 * is config value) are strings. As it is a filter list, config filters can be
 * evaluated with logical AND/OR and we can create a hierarchy of these
 * {@link TimelineKeyValueFilter} objects. If null or empty, the filter is not
 * applied.</li>
 * <li><b>metricFilters</b> - Matched entities should contain the given
 * metrics and satisfy the specified relation with the value. This is
 * represented as a {@link TimelineFilterList} object containing
 * {@link TimelineCompareFilter} objects, each of which contains key-value pairs
 * along with the specified relational/comparison operator represented by
 * {@link TimelineCompareOp}.  The key is a string and value is integer
 * (Short/Integer/Long). As it is a filter list, metric filters can be evaluated
 * with logical AND/OR and we can create a hierarchy of these
 * {@link TimelineCompareFilter} objects. If null or empty, the filter is not
 * applied.</li>
 * <li><b>eventFilters</b> - Matched entities should contain or not contain the
 * given events. This is represented as a {@link TimelineFilterList} object
 * containing {@link TimelineExistsFilter} objects, each of which contains a
 * value which must or must not exist depending on comparison operator specified
 * in the filter. For event filters, the value represents a event id. As it is a
 * filter list, event filters can be evaluated with logical AND/OR and we can
 * create a hierarchy of these {@link TimelineExistsFilter} objects. If null or
 * empty, the filter is not applied.</li>
 * <li><b>fromId</b> - If specified, retrieve the next set of entities from the
 * given fromId. The set of entities retrieved is inclusive of specified fromId.
 * fromId should be taken from the value associated with FROM_ID info key in
 * entity response which was sent earlier.</li>
 * </ul>
 */
@Private
@Unstable
public final class TimelineEntityFilters {
  private final long limit;
  private long createdTimeBegin;
  private long createdTimeEnd;
  private final TimelineFilterList relatesTo;
  private final TimelineFilterList isRelatedTo;
  private final TimelineFilterList infoFilters;
  private final TimelineFilterList configFilters;
  private final TimelineFilterList metricFilters;
  private final TimelineFilterList eventFilters;
  private final String fromId;
  private static final long DEFAULT_BEGIN_TIME = 0L;
  private static final long DEFAULT_END_TIME = Long.MAX_VALUE;


  /**
   * Default limit of number of entities to return for getEntities API.
   */
  public static final long DEFAULT_LIMIT = 100;

  private TimelineEntityFilters(
      Long entityLimit, Long timeBegin, Long timeEnd,
      TimelineFilterList entityRelatesTo,
      TimelineFilterList entityIsRelatedTo,
      TimelineFilterList entityInfoFilters,
      TimelineFilterList entityConfigFilters,
      TimelineFilterList  entityMetricFilters,
      TimelineFilterList entityEventFilters, String fromId) {
    if (entityLimit == null || entityLimit < 0) {
      this.limit = DEFAULT_LIMIT;
    } else {
      this.limit = entityLimit;
    }
    if (timeBegin == null || timeBegin < 0) {
      this.createdTimeBegin = DEFAULT_BEGIN_TIME;
    } else {
      this.createdTimeBegin = timeBegin;
    }
    if (timeEnd == null || timeEnd < 0) {
      this.createdTimeEnd = DEFAULT_END_TIME;
    } else {
      this.createdTimeEnd = timeEnd;
    }
    this.relatesTo = entityRelatesTo;
    this.isRelatedTo = entityIsRelatedTo;
    this.infoFilters = entityInfoFilters;
    this.configFilters = entityConfigFilters;
    this.metricFilters = entityMetricFilters;
    this.eventFilters = entityEventFilters;
    this.fromId = fromId;
  }

  public long getLimit() {
    return limit;
  }

  public long getCreatedTimeBegin() {
    return createdTimeBegin;
  }

  public long getCreatedTimeEnd() {
    return createdTimeEnd;
  }

  public TimelineFilterList getRelatesTo() {
    return relatesTo;
  }

  public TimelineFilterList getIsRelatedTo() {
    return isRelatedTo;
  }

  public TimelineFilterList getInfoFilters() {
    return infoFilters;
  }

  public TimelineFilterList getConfigFilters() {
    return configFilters;
  }

  public TimelineFilterList getMetricFilters() {
    return metricFilters;
  }

  public TimelineFilterList getEventFilters() {
    return eventFilters;
  }

  public String getFromId() {
    return fromId;
  }

  /**
   * A builder class to build an instance of TimelineEntityFilters.
   */
  public static class Builder {
    private Long entityLimit;
    private Long createdTimeBegin;
    private Long createdTimeEnd;
    private TimelineFilterList relatesToFilters;
    private TimelineFilterList isRelatedToFilters;
    private TimelineFilterList entityInfoFilters;
    private TimelineFilterList entityConfigFilters;
    private TimelineFilterList entityMetricFilters;
    private TimelineFilterList entityEventFilters;
    private String entityFromId;

    public Builder entityLimit(Long limit) {
      this.entityLimit = limit;
      return this;
    }

    public Builder createdTimeBegin(Long timeBegin) {
      this.createdTimeBegin = timeBegin;
      return this;
    }

    public Builder createTimeEnd(Long timeEnd) {
      this.createdTimeEnd = timeEnd;
      return this;
    }

    public Builder relatesTo(TimelineFilterList relatesTo) {
      this.relatesToFilters = relatesTo;
      return this;
    }

    public Builder isRelatedTo(TimelineFilterList isRelatedTo) {
      this.isRelatedToFilters = isRelatedTo;
      return this;
    }

    public Builder infoFilters(TimelineFilterList infoFilters) {
      this.entityInfoFilters = infoFilters;
      return this;
    }

    public Builder configFilters(TimelineFilterList configFilters) {
      this.entityConfigFilters = configFilters;
      return this;
    }

    public Builder metricFilters(TimelineFilterList metricFilters) {
      this.entityMetricFilters = metricFilters;
      return this;
    }

    public Builder eventFilters(TimelineFilterList eventFilters) {
      this.entityEventFilters = eventFilters;
      return this;
    }

    public Builder fromId(String fromId) {
      this.entityFromId = fromId;
      return this;
    }

    public TimelineEntityFilters build() {
      return new TimelineEntityFilters(entityLimit, createdTimeBegin,
          createdTimeEnd, relatesToFilters, isRelatedToFilters,
          entityInfoFilters, entityConfigFilters, entityMetricFilters,
          entityEventFilters, entityFromId);
    }
  }
}
