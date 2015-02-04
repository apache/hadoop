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

package org.apache.hadoop.metrics2.lib;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.metrics2.lib.Interns.*;
import static org.apache.hadoop.test.MetricsAsserts.*;

/**
 * Test the metric registry class
 */
public class TestMetricsRegistry {

  /**
   * Test various factory methods
   */
  @Test public void testNewMetrics() {
    final MetricsRegistry r = new MetricsRegistry("test");
    r.newCounter("c1", "c1 desc", 1);
    r.newCounter("c2", "c2 desc", 2L);
    r.newGauge("g1", "g1 desc", 3);
    r.newGauge("g2", "g2 desc", 4L);
    r.newStat("s1", "s1 desc", "ops", "time");

    assertEquals("num metrics in registry", 5, r.metrics().size());
    assertTrue("c1 found", r.get("c1") instanceof MutableCounterInt);
    assertTrue("c2 found", r.get("c2") instanceof MutableCounterLong);
    assertTrue("g1 found", r.get("g1") instanceof MutableGaugeInt);
    assertTrue("g2 found", r.get("g2") instanceof MutableGaugeLong);
    assertTrue("s1 found", r.get("s1") instanceof MutableStat);

    expectMetricsException("Metric name c1 already exists", new Runnable() {
      @Override
      public void run() { r.newCounter("c1", "test dup", 0); }
    });
  }

  /**
   * Test adding metrics with whitespace in the name
   */
  @Test
  public void testMetricsRegistryIllegalMetricNames() {
    final MetricsRegistry r = new MetricsRegistry("test");
    // Fill up with some basics
    r.newCounter("c1", "c1 desc", 1);
    r.newGauge("g1", "g1 desc", 1);
    r.newQuantiles("q1", "q1 desc", "q1 name", "q1 val type", 1);
    // Add some illegal names
    expectMetricsException("Metric name 'badcount 2' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("badcount 2", "c2 desc", 2); }
    });
    expectMetricsException("Metric name 'badcount3  ' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("badcount3  ", "c3 desc", 3); }
    });
    expectMetricsException("Metric name '  badcount4' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("  badcount4", "c4 desc", 4); }
    });
    expectMetricsException("Metric name 'withtab5	' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("withtab5	", "c5 desc", 5); }
    });
    expectMetricsException("Metric name 'withnewline6\n' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("withnewline6\n", "c6 desc", 6); }
    });
    // Final validation
    assertEquals("num metrics in registry", 3, r.metrics().size());
  }

  /**
   * Test the add by name method
   */
  @Test public void testAddByName() {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    final MetricsRegistry r = new MetricsRegistry("test");
    r.add("s1", 42);
    r.get("s1").snapshot(rb);
    verify(rb).addCounter(info("S1NumOps", "Number of ops for s1"), 1L);
    verify(rb).addGauge(info("S1AvgTime", "Average time for s1"), 42.0);

    r.newCounter("c1", "test add", 1);
    r.newGauge("g1", "test add", 1);

    expectMetricsException("Unsupported add", new Runnable() {
      @Override
      public void run() { r.add("c1", 42); }
    });

    expectMetricsException("Unsupported add", new Runnable() {
      @Override
      public void run() { r.add("g1", 42); }
    });
  }

  @Ignore
  private void expectMetricsException(String prefix, Runnable fun) {
    try {
      fun.run();
    }
    catch (MetricsException e) {
      assertTrue("expected exception", e.getMessage().startsWith(prefix));
      return;
    }
    fail("should've thrown '"+ prefix +"...'");
  }
}
