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
package org.apache.hadoop.yarn.api.records.timelineservice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;

import org.junit.Test;

public class TestTimelineMetric {

  @Test
  public void testTimelineMetricAggregation() {
    long ts = System.currentTimeMillis();
    // single_value metric add against null metric
    TimelineMetric m1 = getSingleValueMetric("MEGA_BYTES_MILLIS",
        TimelineMetricOperation.SUM, ts, 10000L);
    TimelineMetric aggregatedMetric = TimelineMetric.aggregateTo(m1, null);
    assertEquals(10000L, aggregatedMetric.getSingleDataValue());

    TimelineMetric m2 = getSingleValueMetric("MEGA_BYTES_MILLIS",
        TimelineMetricOperation.SUM, ts, 20000L);
    aggregatedMetric = TimelineMetric.aggregateTo(m2, aggregatedMetric);
    assertEquals(30000L, aggregatedMetric.getSingleDataValue());

    // stateful sum test
    Map<Object, Object> state = new HashMap<>();
    state.put(TimelineMetricOperation.PREV_METRIC_STATE_KEY, m2);
    TimelineMetric m2New = getSingleValueMetric("MEGA_BYTES_MILLIS",
        TimelineMetricOperation.SUM, ts, 10000L);
    aggregatedMetric = TimelineMetric.aggregateTo(m2New, aggregatedMetric,
        state);
    assertEquals(20000L, aggregatedMetric.getSingleDataValue());

    // single_value metric max against single_value metric
    TimelineMetric m3 = getSingleValueMetric("TRANSFER_RATE",
        TimelineMetricOperation.MAX, ts, 150L);
    TimelineMetric aggregatedMax = TimelineMetric.aggregateTo(m3, null);
    assertEquals(150L, aggregatedMax.getSingleDataValue());

    TimelineMetric m4 = getSingleValueMetric("TRANSFER_RATE",
        TimelineMetricOperation.MAX, ts, 170L);
    aggregatedMax = TimelineMetric.aggregateTo(m4, aggregatedMax);
    assertEquals(170L, aggregatedMax.getSingleDataValue());

    // single_value metric avg against single_value metric
    TimelineMetric m5 = getSingleValueMetric("TRANSFER_RATE",
        TimelineMetricOperation.AVG, ts, 150L);
    try {
      TimelineMetric.aggregateTo(m5, null);
      fail("Taking average among metrics is not supported! ");
    } catch (UnsupportedOperationException e) {
      // Expected
    }

  }

  private static TimelineMetric getSingleValueMetric(String id,
      TimelineMetricOperation op, long timestamp, long value) {
    TimelineMetric m = new TimelineMetric();
    m.setId(id);
    m.setType(Type.SINGLE_VALUE);
    m.setRealtimeAggregationOp(op);
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    metricValues.put(timestamp, value);
    m.setValues(metricValues);
    return m;
  }

  private static TimelineMetric getTimeSeriesMetric(String id,
      TimelineMetricOperation op, Map<Long, Number> metricValues) {
    TimelineMetric m = new TimelineMetric();
    m.setId(id);
    m.setType(Type.TIME_SERIES);
    m.setRealtimeAggregationOp(op);
    m.setValues(metricValues);
    return m;
  }

}
