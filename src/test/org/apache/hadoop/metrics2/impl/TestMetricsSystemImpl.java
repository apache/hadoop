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

import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;
import org.apache.hadoop.metrics2.lib.AbstractMetricsSource;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import static org.apache.hadoop.test.MoreAsserts.*;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * Test the MetricsSystemImpl class
 */
@RunWith(MockitoJUnitRunner.class)
public class TestMetricsSystemImpl {
  private static final Log LOG = LogFactory.getLog(TestMetricsSystemImpl.class);
  @Captor private ArgumentCaptor<MetricsRecord> r1;
  @Captor private ArgumentCaptor<MetricsRecord> r2;
  private static String hostname = MetricsSystemImpl.getHostname();

  @Test public void testInitFirst() throws Exception {
    ConfigBuilder cb = new ConfigBuilder().add("default.period", 8)
        .add("Test.sink.0.class", "org.apache.hadoop.metrics.sink.FileSink")
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.s1.add(0);
    MetricsSink sink1 = mock(MetricsSink.class);
    MetricsSink sink2 = mock(MetricsSink.class);
    ms.register("sink1", "sink1 desc", sink1);
    ms.register("sink2", "sink2 desc", sink2);
    ms.onTimerEvent();  // trigger something interesting
    ms.stop();

    verify(sink1, times(2)).putMetrics(r1.capture());
    List<MetricsRecord> mr1 = r1.getAllValues();
    verify(sink2, times(2)).putMetrics(r2.capture());
    List<MetricsRecord> mr2 = r2.getAllValues();
    checkMetricsRecords(mr1);
    assertEquals("output", mr1, mr2);
  }

  static void checkMetricsRecords(List<MetricsRecord> recs) {
    LOG.debug(recs);
    MetricsRecord r = recs.get(0);
    assertEquals("name", "s1rec", r.name());
    assertEquals("tags", new MetricsTag[] {
      new MetricsTag("context", "Metrics context", "test"),
      new MetricsTag("hostName", "Local hostname", hostname)}, r.tags());
    assertEquals("metrics", new Metric[] {
      new MetricCounterLong("c1", "c1 desc", 1),
      new MetricGaugeLong("g1", "g1 desc", 2),
      new MetricCounterLong("s1_num_ops", "Number of ops for s1 desc", 1),
      new MetricGaugeDouble("s1_avg_time", "Average time for s1 desc", 0)},
      r.metrics());

    // Skip the system metrics for now.
    // MetricsRecord r1 = recs.get(1);
  }

  private static class TestSource extends AbstractMetricsSource {
    final MetricMutableCounterLong c1;
    final MetricMutableGaugeLong g1;
    final MetricMutableStat s1;

    TestSource(String name) {
      super(name);
      registry.setContext("test");
      c1 = registry.newCounter("c1", "c1 desc", 1L);
      g1 = registry.newGauge("g1", "g1 desc", 2L);
      s1 = registry.newStat("s1", "s1 desc", "ops", "time");
    }
  }

}
