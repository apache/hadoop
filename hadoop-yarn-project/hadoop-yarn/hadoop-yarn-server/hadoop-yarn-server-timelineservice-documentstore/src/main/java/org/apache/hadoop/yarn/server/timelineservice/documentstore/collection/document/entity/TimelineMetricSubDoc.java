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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;

import java.util.Map;
import java.util.TreeMap;

/**
 * This class represents a Sub Document for {@link TimelineMetric} that will be
 * used when creating new {@link TimelineEntityDocument}.
 */
public class TimelineMetricSubDoc {

  private final TimelineMetric timelineMetric;
  private long singleDataTimestamp;
  private Number singleDataValue = 0;

  public TimelineMetricSubDoc() {
    this.timelineMetric = new TimelineMetric();
  }

  public TimelineMetricSubDoc(TimelineMetric timelineMetric) {
    this.timelineMetric = timelineMetric;
    if (timelineMetric.getType() == TimelineMetric.Type.SINGLE_VALUE &&
        timelineMetric.getValues().size() > 0) {
      this.singleDataTimestamp = timelineMetric.getSingleDataTimestamp();
      this.singleDataValue = timelineMetric.getSingleDataValue();
    }
  }

  /**
   * Get the real time aggregation operation of this metric.
   *
   * @return Real time aggregation operation
   */
  public TimelineMetricOperation getRealtimeAggregationOp() {
    return timelineMetric.getRealtimeAggregationOp();
  }

  /**
   * Set the real time aggregation operation of this metric.
   *
   * @param op A timeline metric operation that the metric should perform on
   *           real time aggregations
   */
  public void setRealtimeAggregationOp(
      final TimelineMetricOperation op) {
    timelineMetric.setRealtimeAggregationOp(op);
  }

  public String getId() {
    return timelineMetric.getId();
  }

  public void setId(String metricId) {
    timelineMetric.setId(metricId);
  }

  public void setSingleDataTimestamp(long singleDataTimestamp) {
    this.singleDataTimestamp = singleDataTimestamp;
  }

  /**
   * Get single data timestamp of the metric.
   *
   * @return the single data timestamp
   */
  public long getSingleDataTimestamp() {
    if (timelineMetric.getType() == TimelineMetric.Type.SINGLE_VALUE) {
      return singleDataTimestamp;
    }
    return 0;
  }

  /**
   * Get single data value of the metric.
   *
   * @return the single data value
   */
  public Number getSingleDataValue() {
    if (timelineMetric.getType() == TimelineMetric.Type.SINGLE_VALUE) {
      return singleDataValue;
    }
    return null;
  }

  public void setSingleDataValue(Number singleDataValue) {
    this.singleDataValue = singleDataValue;
  }

  public Map<Long, Number> getValues() {
    return timelineMetric.getValues();
  }

  public void setValues(Map<Long, Number> vals) {
    timelineMetric.setValues(vals);
  }

  // required by JAXB
  public TreeMap<Long, Number> getValuesJAXB() {
    return timelineMetric.getValuesJAXB();
  }

  public TimelineMetric.Type getType() {
    return timelineMetric.getType();
  }

  public void setType(TimelineMetric.Type metricType) {
    timelineMetric.setType(metricType);
  }

  public boolean isValid() {
    return (timelineMetric.getId() != null);
  }

  @Override
  public int hashCode() {
    int result = timelineMetric.getId().hashCode();
    result = 31 * result + timelineMetric.getType().hashCode();
    return result;
  }

  // Only check if timestamp and id are equal
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TimelineMetricSubDoc)) {
      return false;
    }
    TimelineMetricSubDoc otherTimelineMetric = (TimelineMetricSubDoc) obj;
    if (!this.timelineMetric.getId().equals(otherTimelineMetric.getId())) {
      return false;
    }
    return this.timelineMetric.getType() == otherTimelineMetric.getType();
  }

  public TimelineMetric fetchTimelineMetric() {
    return timelineMetric;
  }
}