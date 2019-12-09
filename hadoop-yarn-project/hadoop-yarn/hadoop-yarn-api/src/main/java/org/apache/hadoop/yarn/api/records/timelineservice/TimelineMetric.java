/*
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
package org.apache.hadoop.yarn.api.records.timelineservice;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * This class contains the information of a metric that is related to some
 * entity. Metric can either be a time series or single value.
 */
@XmlRootElement(name = "metric")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineMetric {

  /**
   * Type of metric.
   */
  public enum Type {
    SINGLE_VALUE,
    TIME_SERIES
  }

  private Type type;
  private String id;
  // By default, not to do any aggregation operations. This field will NOT be
  // persisted (like a "transient" member).
  private TimelineMetricOperation realtimeAggregationOp
      = TimelineMetricOperation.NOP;

  private TreeMap<Long, Number> values
      = new TreeMap<>(Collections.reverseOrder());

  public TimelineMetric() {
    this(Type.SINGLE_VALUE);
  }

  public TimelineMetric(Type type) {
    this.type = type;
  }


  @XmlElement(name = "type")
  public Type getType() {
    return type;
  }

  public void setType(Type metricType) {
    this.type = metricType;
  }

  @XmlElement(name = "id")
  public String getId() {
    return id;
  }

  public void setId(String metricId) {
    this.id = metricId;
  }

  /**
   * Get the real time aggregation operation of this metric.
   *
   * @return Real time aggregation operation
   */
  // required by JAXB
  @XmlElement(name = "aggregationOp")
  public TimelineMetricOperation getRealtimeAggregationOp() {
    return realtimeAggregationOp;
  }

  /**
   * Set the real time aggregation operation of this metric.
   *
   * @param op A timeline metric operation that the metric should perform on
   *           real time aggregations
   */
  public void setRealtimeAggregationOp(
      final TimelineMetricOperation op) {
    this.realtimeAggregationOp = op;
  }

  // required by JAXB
  @InterfaceAudience.Private
  @XmlElement(name = "values")
  public TreeMap<Long, Number> getValuesJAXB() {
    return values;
  }

  public Map<Long, Number> getValues() {
    return values;
  }

  public void setValues(Map<Long, Number> vals) {
    if (type == Type.SINGLE_VALUE) {
      overwrite(vals);
    } else {
      if (vals != null) {
        this.values = new TreeMap<>(Collections.reverseOrder());
        this.values.putAll(vals);
      } else {
        this.values = null;
      }
    }
  }

  public void addValues(Map<Long, Number> vals) {
    if (type == Type.SINGLE_VALUE) {
      overwrite(vals);
    } else {
      this.values.putAll(vals);
    }
  }

  public void addValue(long timestamp, Number value) {
    if (type == Type.SINGLE_VALUE) {
      values.clear();
    }
    values.put(timestamp, value);
  }

  private void overwrite(Map<Long, Number> vals) {
    if (vals.size() > 1) {
      throw new IllegalArgumentException(
          "Values cannot contain more than one point in " +
              Type.SINGLE_VALUE + " mode");
    }
    this.values.clear();
    this.values.putAll(vals);
  }

  public boolean isValid() {
    return (id != null);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }

  // Only check if type and id are equal
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimelineMetric)) {
      return false;
    }

    TimelineMetric m = (TimelineMetric) o;

    if (!id.equals(m.id)) {
      return false;
    }
    if (type != m.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "{id: " + id + ", type: " + type +
        ", realtimeAggregationOp: " +
        realtimeAggregationOp + "; " + values.toString() +
        "}";
  }

  /**
   * Get the latest timeline metric as single value type.
   *
   * @param metric Incoming timeline metric
   * @return The latest metric in the incoming metric
   */
  public static TimelineMetric getLatestSingleValueMetric(
      TimelineMetric metric) {
    if (metric.getType() == Type.SINGLE_VALUE) {
      return metric;
    } else {
      TimelineMetric singleValueMetric = new TimelineMetric(Type.SINGLE_VALUE);
      Long firstKey = metric.values.firstKey();
      if (firstKey != null) {
        Number firstValue = metric.values.get(firstKey);
        singleValueMetric.addValue(firstKey, firstValue);
      }
      return singleValueMetric;
    }
  }

  /**
   * Get single data timestamp of the metric.
   *
   * @return the single data timestamp
   */
  public long getSingleDataTimestamp() {
    if (this.type == Type.SINGLE_VALUE) {
      if (values.size() == 0) {
        throw new YarnRuntimeException("Values for this timeline metric is " +
            "empty.");
      } else {
        return values.firstKey();
      }
    } else {
      throw new YarnRuntimeException("Type for this timeline metric is not " +
          "SINGLE_VALUE.");
    }
  }

  /**
   * Get single data value of the metric.
   *
   * @return the single data value
   */
  public Number getSingleDataValue() {
    if (this.type == Type.SINGLE_VALUE) {
      if (values.size() == 0) {
        return null;
      } else {
        return values.get(values.firstKey());
      }
    } else {
      throw new YarnRuntimeException("Type for this timeline metric is not " +
          "SINGLE_VALUE.");
    }
  }

  /**
   * Aggregate an incoming metric to the base aggregated metric with the given
   * operation state in a stateless fashion. The assumption here is
   * baseAggregatedMetric and latestMetric should be single value data if not
   * null.
   *
   * @param incomingMetric Incoming timeline metric to aggregate
   * @param baseAggregatedMetric Base timeline metric
   * @return Result metric after aggregation
   */
  public static TimelineMetric aggregateTo(TimelineMetric incomingMetric,
      TimelineMetric baseAggregatedMetric) {
    return aggregateTo(incomingMetric, baseAggregatedMetric, null);
  }

  /**
   * Aggregate an incoming metric to the base aggregated metric with the given
   * operation state. The assumption here is baseAggregatedMetric and
   * latestMetric should be single value data if not null.
   *
   * @param incomingMetric Incoming timeline metric to aggregate
   * @param baseAggregatedMetric Base timeline metric
   * @param state Operation state
   * @return Result metric after aggregation
   */
  public static TimelineMetric aggregateTo(TimelineMetric incomingMetric,
      TimelineMetric baseAggregatedMetric, Map<Object, Object> state) {
    TimelineMetricOperation operation
        = incomingMetric.getRealtimeAggregationOp();
    return operation.aggregate(incomingMetric, baseAggregatedMetric, state);
  }

}
