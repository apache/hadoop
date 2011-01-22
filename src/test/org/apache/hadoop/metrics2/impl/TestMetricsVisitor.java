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

package org.apache.hadoop.metrics2.impl;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.metrics2.MetricsVisitor;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricCounter;
import org.apache.hadoop.metrics2.MetricGauge;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Test the metric visitor interface
 */
@RunWith(MockitoJUnitRunner.class)
public class TestMetricsVisitor {

  @Captor private ArgumentCaptor<MetricCounter<Integer>> c1;
  @Captor private ArgumentCaptor<MetricCounter<Long>> c2;
  @Captor private ArgumentCaptor<MetricGauge<Integer>> g1;
  @Captor private ArgumentCaptor<MetricGauge<Long>> g2;
  @Captor private ArgumentCaptor<MetricGauge<Float>> g3;
  @Captor private ArgumentCaptor<MetricGauge<Double>> g4;

  /**
   * Test the common use cases
   */
  @Test public void testCommon() {
    MetricsVisitor visitor = mock(MetricsVisitor.class);
    List<Metric> metrics = Arrays.asList(
        new MetricCounterInt("c1", "int counter", 1),
        new MetricCounterLong("c2", "long counter", 2L),
        new MetricGaugeInt("g1", "int gauge", 5),
        new MetricGaugeLong("g2", "long gauge", 6L),
        new MetricGaugeFloat("g3", "float gauge", 7f),
        new MetricGaugeDouble("g4", "double gauge", 8d));

    for (Metric metric : metrics) {
      metric.visit(visitor);
    }

    verify(visitor).counter(c1.capture(), eq(1));
    assertEquals("c1 name", "c1", c1.getValue().name());
    assertEquals("c1 description", "int counter", c1.getValue().description());
    verify(visitor).counter(c2.capture(), eq(2L));
    assertEquals("c2 name", "c2", c2.getValue().name());
    assertEquals("c2 description", "long counter", c2.getValue().description());
    verify(visitor).gauge(g1.capture(), eq(5));
    assertEquals("g1 name", "g1", g1.getValue().name());
    assertEquals("g1 description", "int gauge", g1.getValue().description());
    verify(visitor).gauge(g2.capture(), eq(6L));
    assertEquals("g2 name", "g2", g2.getValue().name());
    assertEquals("g2 description", "long gauge", g2.getValue().description());
    verify(visitor).gauge(g3.capture(), eq(7f));
    assertEquals("g3 name", "g3", g3.getValue().name());
    assertEquals("g3 description", "float gauge", g3.getValue().description());
    verify(visitor).gauge(g4.capture(), eq(8d));
    assertEquals("g4 name", "g4", g4.getValue().name());
    assertEquals("g4 description", "double gauge", g4.getValue().description());
  }

}
