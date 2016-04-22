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

import java.util.Map;

/**
 * Aggregation operations.
 */
public enum TimelineMetricOperation {
  NOP("NOP") {
    /**
     * Do nothing on the base metric.
     *
     * @param incoming Metric a
     * @param base Metric b
     * @param state Operation state (not used)
     * @return Metric b
     */
    @Override
    public TimelineMetric exec(TimelineMetric incoming,
        TimelineMetric base, Map<Object, Object> state) {
      return base;
    }
  },
  MAX("MAX") {
    /**
     * Keep the greater value of incoming and base. Stateless operation.
     *
     * @param incoming Metric a
     * @param base Metric b
     * @param state Operation state (not used)
     * @return the greater value of a and b
     */
    @Override
    public TimelineMetric exec(TimelineMetric incoming,
        TimelineMetric base, Map<Object, Object> state) {
      if (base == null) {
        return incoming;
      }
      Number incomingValue = incoming.getSingleDataValue();
      Number aggregateValue = base.getSingleDataValue();
      if (aggregateValue == null) {
        aggregateValue = Long.MIN_VALUE;
      }
      if (TimelineMetricCalculator.compare(incomingValue, aggregateValue) > 0) {
        base.addValue(incoming.getSingleDataTimestamp(), incomingValue);
      }
      return base;
    }
  },
  REPLACE("REPLACE") {
    /**
     * Replace the base metric with the incoming value. Stateless operation.
     *
     * @param incoming Metric a
     * @param base Metric b
     * @param state Operation state (not used)
     * @return Metric a
     */
    @Override
    public TimelineMetric exec(TimelineMetric incoming,
        TimelineMetric base,
        Map<Object, Object> state) {
      return incoming;
    }
  },
  SUM("SUM") {
    /**
     * Return the sum of the incoming metric and the base metric if the
     * operation is stateless. For stateful operations, also subtract the
     * value of the timeline metric mapped to the PREV_METRIC_STATE_KEY
     * in the state object.
     *
     * @param incoming Metric a
     * @param base Metric b
     * @param state Operation state (PREV_METRIC_STATE_KEY's value as Metric p)
     * @return A metric with value a + b - p
     */
    @Override
    public TimelineMetric exec(TimelineMetric incoming, TimelineMetric base,
        Map<Object, Object> state) {
      if (base == null) {
        return incoming;
      }
      Number incomingValue = incoming.getSingleDataValue();
      Number aggregateValue = base.getSingleDataValue();
      Number result
          = TimelineMetricCalculator.sum(incomingValue, aggregateValue);

      // If there are previous value in the state, we will take it off from the
      // sum
      if (state != null) {
        Object prevMetric = state.get(PREV_METRIC_STATE_KEY);
        if (prevMetric instanceof TimelineMetric) {
          result = TimelineMetricCalculator.sub(result,
              ((TimelineMetric) prevMetric).getSingleDataValue());
        }
      }
      base.addValue(incoming.getSingleDataTimestamp(), result);
      return base;
    }
  },
  AVG("AVERAGE") {
    /**
     * Return the average value of the incoming metric and the base metric,
     * with a given state. Not supported yet.
     *
     * @param incoming Metric a
     * @param base Metric b
     * @param state Operation state
     * @return Not finished yet
     */
    @Override
    public TimelineMetric exec(TimelineMetric incoming, TimelineMetric base,
        Map<Object, Object> state) {
      // Not supported yet
      throw new UnsupportedOperationException(
          "Unsupported aggregation operation: AVERAGE");
    }
  };

  public static final String PREV_METRIC_STATE_KEY = "PREV_METRIC";

  /**
   * Perform the aggregation operation.
   *
   * @param incoming Incoming metric
   * @param aggregate Base aggregation metric
   * @param state Operation state
   * @return Result metric for this aggregation operation
   */
  public TimelineMetric aggregate(TimelineMetric incoming,
      TimelineMetric aggregate, Map<Object, Object> state) {
    return exec(incoming, aggregate, state);
  }

  private final String opName;

  TimelineMetricOperation(String opString) {
    opName = opString;
  }

  @Override
  public String toString() {
    return this.opName;
  }

  abstract TimelineMetric exec(TimelineMetric incoming, TimelineMetric base,
      Map<Object, Object> state);
}
