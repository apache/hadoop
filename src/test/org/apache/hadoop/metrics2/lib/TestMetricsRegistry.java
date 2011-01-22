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

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

/**
 * Test the metric registry class
 */
public class TestMetricsRegistry {

  /**
   * Test various factory methods
   */
  @Test public void testNewMetrics() {
    MetricMutableFactory factory = spy(new MetricMutableFactory());
    final MetricsRegistry r = new MetricsRegistry("test", factory);
    r.newCounter("c1", "c1 desc", 1);
    r.newCounter("c2", "c2 desc", 2L);
    r.newGauge("g1", "g1 desc", 3);
    r.newGauge("g2", "g2 desc", 4L);
    r.newStat("s1", "s1 desc", "ops", "time");

    assertEquals("num metrics in registry", 5, r.metrics().size());
    verify(factory).newCounter("c1", "c1 desc", 1);
    verify(factory).newCounter("c2", "c2 desc", 2L);
    verify(factory).newGauge("g1", "g1 desc", 3);
    verify(factory).newGauge("g2", "g2 desc", 4L);
    verify(factory).newStat("s1", "s1 desc", "ops", "time", false);
    assertTrue("c1 found", r.get("c1") instanceof MetricMutableCounterInt);
    assertTrue("c2 found", r.get("c2") instanceof MetricMutableCounterLong);
    assertTrue("g1 found", r.get("g1") instanceof MetricMutableGaugeInt);
    assertTrue("g2 found", r.get("g2") instanceof MetricMutableGaugeLong);
    assertTrue("s1 found", r.get("s1") instanceof MetricMutableStat);

    expectMetricsException("Metric name c1 already exists", new Runnable() {
      public void run() { r.newCounter("c1", "test dup", 0); }
    });
  }

  /**
   * Test the incr by name method
   */
  @Test public void testIncrByName() {
    MetricsRecordBuilder builder = mock(MetricsRecordBuilder.class);
    MetricMutableFactory factory = new MetricMutableFactory() {
      @Override public MetricMutable newMetric(String name) {
        return new MetricMutableGaugeInt(name, "test incr", 1);
      }
    };
    final MetricsRegistry r = new MetricsRegistry("test", factory);
    r.incr("g1");
    r.get("g1").snapshot(builder);
    verify(builder).addGauge("g1", "test incr", 2);

    r.incr("c1", new MetricMutableFactory() {
      @Override public MetricMutable newMetric(String name) {
        return new MetricMutableCounterInt(name, "test incr", 2);
      }
    });
    r.get("c1").snapshot(builder);
    verify(builder).addCounter("c1", "test incr", 3);

    r.newStat("s1", "test incr", "ops", "time");
    expectMetricsException("Unsupported incr", new Runnable() {
      public void run() { r.incr("s1"); }
    });

    expectMetricsException("Metric n1 doesn't exist", new Runnable() {
      public void run() { r.incr("n1", null); }
    });
  }

  /**
   * Test the decr by name method
   */
  @Test public void testDecrByName() {
    MetricsRecordBuilder builder = mock(MetricsRecordBuilder.class);
    MetricMutableFactory factory = new MetricMutableFactory() {
      @Override public MetricMutable newMetric(String name) {
        return new MetricMutableGaugeInt(name, "test decr", 1);
      }
    };
    final MetricsRegistry r = new MetricsRegistry("test", factory);
    r.decr("g1");
    r.get("g1").snapshot(builder);
    verify(builder).addGauge("g1", "test decr", 0);

    expectMetricsException("Unsupported decr", new Runnable() {
      public void run() {
        r.decr("c1", new MetricMutableFactory() {
          @Override public MetricMutable newMetric(String name) {
            return new MetricMutableCounterInt(name, "test decr", 2);
          }
        });
      }
    });

    r.newStat("s1", "test decr", "ops", "time");
    expectMetricsException("Unsupported decr", new Runnable() {
      public void run() { r.decr("s1"); }
    });

    expectMetricsException("Metric n1 doesn't exist", new Runnable() {
      public void run() { r.decr("n1", null); }
    });
  }

  /**
   * Test the add by name method
   */
  @Test public void testAddByName() {
    MetricsRecordBuilder builder = mock(MetricsRecordBuilder.class);
    MetricMutableFactory factory = new MetricMutableFactory() {
      @Override public MetricMutableStat newStat(String name) {
        return new MetricMutableStat(name, "test add", "ops", "time");
      }
    };
    final MetricsRegistry r = new MetricsRegistry("test", factory);
    r.add("s1", 42);
    r.get("s1").snapshot(builder);
    verify(builder).addCounter("s1_num_ops", "Number of ops for test add", 1L);
    verify(builder).addGauge("s1_avg_time", "Average time for test add", 42.0);

    r.newCounter("c1", "test add", 1);
    r.newGauge("g1", "test add", 1);

    expectMetricsException("Unsupported add", new Runnable() {
      public void run() { r.add("c1", 42); }
    });

    expectMetricsException("Unsupported add", new Runnable() {
      public void run() { r.add("g1", 42); }
    });

    expectMetricsException("Metric n1 doesn't exist", new Runnable() {
      public void run() { r.add("n1", 42, null); }
    });
  }

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
