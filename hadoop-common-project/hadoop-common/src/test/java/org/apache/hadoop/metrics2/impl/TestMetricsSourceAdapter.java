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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricsSourceBuilder;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.junit.Test;

public class TestMetricsSourceAdapter {

  @Test
  public void testGetMetricsAndJmx() throws Exception {
    // create test source with a single metric counter of value 0
    TestSource source = new TestSource("test");
    MetricsSourceBuilder sb = MetricsAnnotations.newSourceBuilder(source);
    final MetricsSource s = sb.build();

    List<MetricsTag> injectedTags = new ArrayList<MetricsTag>();
    MetricsSourceAdapter sa = new MetricsSourceAdapter(
        "test", "test", "test desc", s, injectedTags, null, null, 1, false);

    // all metrics are initially assumed to have changed
    MetricsCollectorImpl builder = new MetricsCollectorImpl();
    Iterable<MetricsRecordImpl> metricsRecords = sa.getMetrics(builder, true);

    // Validate getMetrics and JMX initial values
    MetricsRecordImpl metricsRecord = metricsRecords.iterator().next();
    assertEquals(0L,
        metricsRecord.metrics().iterator().next().value().longValue());

    Thread.sleep(100); // skip JMX cache TTL
    assertEquals(0L, (Number)sa.getAttribute("C1"));

    // change metric value
    source.incrementCnt();

    // validate getMetrics and JMX
    builder = new MetricsCollectorImpl();
    metricsRecords = sa.getMetrics(builder, true);
    metricsRecord = metricsRecords.iterator().next();
    assertTrue(metricsRecord.metrics().iterator().hasNext());
    Thread.sleep(100); // skip JMX cache TTL
    assertEquals(1L, (Number)sa.getAttribute("C1"));
  }

  @SuppressWarnings("unused")
  @Metrics(context="test")
  private static class TestSource {
    @Metric("C1 desc") MutableCounterLong c1;
    final MetricsRegistry registry;

    TestSource(String recName) {
      registry = new MetricsRegistry(recName);
    }

    public void incrementCnt() {
      c1.incr();
    }
  }
}
