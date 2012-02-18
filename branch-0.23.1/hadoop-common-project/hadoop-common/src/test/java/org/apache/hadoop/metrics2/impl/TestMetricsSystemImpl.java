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

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.Iterables;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsException;
import static org.apache.hadoop.test.MoreAsserts.*;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.*;
import static org.apache.hadoop.metrics2.lib.Interns.*;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Test the MetricsSystemImpl class
 */
@RunWith(MockitoJUnitRunner.class)
public class TestMetricsSystemImpl {
  private static final Log LOG = LogFactory.getLog(TestMetricsSystemImpl.class);
  @Captor private ArgumentCaptor<MetricsRecord> r1;
  @Captor private ArgumentCaptor<MetricsRecord> r2;
  private static String hostname = MetricsSystemImpl.getHostname();

  public static class TestSink implements MetricsSink {

    @Override public void putMetrics(MetricsRecord record) {
      LOG.debug(record);
    }

    @Override public void flush() {}

    @Override public void init(SubsetConfiguration conf) {
      LOG.debug(MetricsConfig.toString(conf));
    }
  }

  @Test public void testInitFirst() throws Exception {
    ConfigBuilder cb = new ConfigBuilder().add("*.period", 8)
        //.add("test.sink.plugin.urls", getPluginUrlsAsString())
        .add("test.sink.test.class", TestSink.class.getName())
        .add("test.*.source.filter.exclude", "s0")
        .add("test.source.s1.metric.filter.exclude", "X*")
        .add("test.sink.sink1.metric.filter.exclude", "Y*")
        .add("test.sink.sink2.metric.filter.exclude", "Y*")
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    ms.register("s0", "s0 desc", new TestSource("s0rec"));
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.c1.incr();
    s1.xxx.incr();
    s1.g1.set(2);
    s1.yyy.incr(2);
    s1.s1.add(0);
    MetricsSink sink1 = mock(MetricsSink.class);
    MetricsSink sink2 = mock(MetricsSink.class);
    ms.registerSink("sink1", "sink1 desc", sink1);
    ms.registerSink("sink2", "sink2 desc", sink2);
    ms.onTimerEvent();  // trigger something interesting
    ms.stop();

    verify(sink1, times(2)).putMetrics(r1.capture());
    List<MetricsRecord> mr1 = r1.getAllValues();
    verify(sink2, times(2)).putMetrics(r2.capture());
    List<MetricsRecord> mr2 = r2.getAllValues();
    checkMetricsRecords(mr1);
    assertEquals("output", mr1, mr2);
  }

  @Test public void testRegisterDups() {
    MetricsSystem ms = new MetricsSystemImpl();
    TestSource ts1 = new TestSource("ts1");
    TestSource ts2 = new TestSource("ts2");
    ms.register("ts1", "", ts1);
    MetricsSource s1 = ms.getSource("ts1");
    assertNotNull(s1);
    // should work when metrics system is not started
    ms.register("ts1", "", ts2);
    MetricsSource s2 = ms.getSource("ts1");
    assertNotNull(s2);
    assertNotSame(s1, s2);
  }

  @Test(expected=MetricsException.class) public void testRegisterDupError() {
    MetricsSystem ms = new MetricsSystemImpl("test");
    TestSource ts = new TestSource("ts");
    ms.register(ts);
    ms.register(ts);
  }


  private void checkMetricsRecords(List<MetricsRecord> recs) {
    LOG.debug(recs);
    MetricsRecord r = recs.get(0);
    assertEquals("name", "s1rec", r.name());
    assertEquals("tags", new MetricsTag[] {
      tag(MsInfo.Context, "test"),
      tag(MsInfo.Hostname, hostname)}, r.tags());
    assertEquals("metrics", MetricsLists.builder("")
      .addCounter(info("C1", "C1 desc"), 1L)
      .addGauge(info("G1", "G1 desc"), 2L)
      .addCounter(info("S1NumOps", "Number of ops for s1"), 1L)
      .addGauge(info("S1AvgTime", "Average time for s1"), 0.0)
      .metrics(), r.metrics());

    r = recs.get(1);
    assertTrue("NumActiveSinks should be 3", Iterables.contains(r.metrics(),
               new MetricGaugeInt(MsInfo.NumActiveSinks, 3)));
  }

  @Metrics(context="test")
  private static class TestSource {
    @Metric("C1 desc") MutableCounterLong c1;
    @Metric("XXX desc") MutableCounterLong xxx;
    @Metric("G1 desc") MutableGaugeLong g1;
    @Metric("YYY desc") MutableGaugeLong yyy;
    @Metric MutableRate s1;
    final MetricsRegistry registry;

    TestSource(String recName) {
      registry = new MetricsRegistry(recName);
    }
  }

  private static String getPluginUrlsAsString() {
    return "file:metrics2-test-plugin.jar";
  }
}
