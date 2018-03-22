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

import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;

/**
 * Encapsulates information regarding which data to retrieve for each entity
 * while querying.<br>
 * Data to retrieve contains the following :<br>
 * <ul>
 * <li><b>confsToRetrieve</b> - Used for deciding which configs to return
 * in response. This is represented as a {@link TimelineFilterList} object
 * containing {@link TimelinePrefixFilter} objects. These can either be
 * exact config keys' or prefixes which are then compared against config
 * keys' to decide configs(inside entities) to return in response. If null
 * or empty, all configurations will be fetched if fieldsToRetrieve
 * contains {@link Field#CONFIGS} or {@link Field#ALL}. This should not be
 * confused with configFilters which is used to decide which entities to
 * return instead.</li>
 * <li><b>metricsToRetrieve</b> - Used for deciding which metrics to return
 * in response. This is represented as a {@link TimelineFilterList} object
 * containing {@link TimelinePrefixFilter} objects. These can either be
 * exact metric ids' or prefixes which are then compared against metric
 * ids' to decide metrics(inside entities) to return in response. If null
 * or empty, all metrics will be fetched if fieldsToRetrieve contains
 * {@link Field#METRICS} or {@link Field#ALL}. This should not be confused
 * with metricFilters which is used to decide which entities to return
 * instead.</li>
 * <li><b>fieldsToRetrieve</b> - Specifies which fields of the entity
 * object to retrieve, see {@link Field}. If null, retrieves 3 fields,
 * namely entity id, entity type and entity created time. All fields will
 * be returned if {@link Field#ALL} is specified.</li>
 * <li><b>metricsLimit</b> - If fieldsToRetrieve contains METRICS/ALL or
 * metricsToRetrieve is specified, this limit defines an upper limit to the
 * number of metrics to return. This parameter is ignored if METRICS are not to
 * be fetched.</li>
 * <li><b>metricsTimeStart</b> - Metric values before this timestamp would not
 * be retrieved. If null or {@literal <0}, defaults to 0.</li>
 * <li><b>metricsTimeEnd</b> - Metric values after this timestamp would not
 * be retrieved. If null or {@literal <0}, defaults to {@link Long#MAX_VALUE}.
 * </li>
 * </ul>
 */
@Private
@Unstable
public class TimelineDataToRetrieve {
  private TimelineFilterList confsToRetrieve;
  private TimelineFilterList metricsToRetrieve;
  private EnumSet<Field> fieldsToRetrieve;
  private Integer metricsLimit;
  private Long metricsTimeBegin;
  private Long metricsTimeEnd;
  private static final long DEFAULT_METRICS_BEGIN_TIME = 0L;
  private static final long DEFAULT_METRICS_END_TIME = Long.MAX_VALUE;

  /**
   * Default limit of number of metrics to return.
   */
  public static final Integer DEFAULT_METRICS_LIMIT = 1;

  public TimelineDataToRetrieve() {
    this(null, null, null, null, null, null);
  }

  public TimelineDataToRetrieve(TimelineFilterList confs,
      TimelineFilterList metrics, EnumSet<Field> fields,
      Integer limitForMetrics, Long metricTimeBegin, Long metricTimeEnd) {
    this.confsToRetrieve = confs;
    this.metricsToRetrieve = metrics;
    this.fieldsToRetrieve = fields;
    if (limitForMetrics == null || limitForMetrics < 1) {
      this.metricsLimit = DEFAULT_METRICS_LIMIT;
    } else {
      this.metricsLimit = limitForMetrics;
    }

    if (this.fieldsToRetrieve == null) {
      this.fieldsToRetrieve = EnumSet.noneOf(Field.class);
    }
    if (metricTimeBegin == null || metricTimeBegin < 0) {
      this.metricsTimeBegin = DEFAULT_METRICS_BEGIN_TIME;
    } else {
      this.metricsTimeBegin = metricTimeBegin;
    }
    if (metricTimeEnd == null || metricTimeEnd < 0) {
      this.metricsTimeEnd = DEFAULT_METRICS_END_TIME;
    } else {
      this.metricsTimeEnd = metricTimeEnd;
    }
    if (this.metricsTimeBegin > this.metricsTimeEnd) {
      throw new IllegalArgumentException("metricstimebegin should not be " +
          "greater than metricstimeend");
    }
  }

  public TimelineFilterList getConfsToRetrieve() {
    return confsToRetrieve;
  }

  public void setConfsToRetrieve(TimelineFilterList confs) {
    this.confsToRetrieve = confs;
  }

  public TimelineFilterList getMetricsToRetrieve() {
    return metricsToRetrieve;
  }

  public void setMetricsToRetrieve(TimelineFilterList metrics) {
    this.metricsToRetrieve = metrics;
  }

  public EnumSet<Field> getFieldsToRetrieve() {
    return fieldsToRetrieve;
  }

  public void setFieldsToRetrieve(EnumSet<Field> fields) {
    this.fieldsToRetrieve = fields;
  }

  /**
   * Adds configs and metrics fields to fieldsToRetrieve(if they are not
   * present) if confsToRetrieve and metricsToRetrieve are specified.
   */
  public void addFieldsBasedOnConfsAndMetricsToRetrieve() {
    if (!fieldsToRetrieve.contains(Field.CONFIGS) && confsToRetrieve != null &&
        !confsToRetrieve.getFilterList().isEmpty()) {
      fieldsToRetrieve.add(Field.CONFIGS);
    }
    if (!fieldsToRetrieve.contains(Field.METRICS) &&
        metricsToRetrieve != null &&
        !metricsToRetrieve.getFilterList().isEmpty()) {
      fieldsToRetrieve.add(Field.METRICS);
    }
  }

  public Integer getMetricsLimit() {
    return metricsLimit;
  }

  public Long getMetricsTimeBegin() {
    return this.metricsTimeBegin;
  }

  public Long getMetricsTimeEnd() {
    return metricsTimeEnd;
  }

  public void setMetricsLimit(Integer limit) {
    if (limit == null || limit < 1) {
      this.metricsLimit = DEFAULT_METRICS_LIMIT;
    } else {
      this.metricsLimit = limit;
    }
  }
}
