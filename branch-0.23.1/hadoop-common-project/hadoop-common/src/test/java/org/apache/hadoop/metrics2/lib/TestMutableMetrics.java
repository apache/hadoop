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
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalMatchers.*;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.metrics2.lib.Interns.*;
import static org.apache.hadoop.test.MetricsAsserts.*;

/**
 * Test metrics record builder interface and mutable metrics
 */
public class TestMutableMetrics {

  private final double EPSILON = 1e-42;

  /**
   * Test the snapshot method
   */
  @Test public void testSnapshot() {
    MetricsRecordBuilder mb = mockMetricsRecordBuilder();

    MetricsRegistry registry = new MetricsRegistry("test");
    registry.newCounter("c1", "int counter", 1);
    registry.newCounter("c2", "long counter", 2L);
    registry.newGauge("g1", "int gauge", 3);
    registry.newGauge("g2", "long gauge", 4L);
    registry.newStat("s1", "stat", "Ops", "Time", true).add(0);
    registry.newRate("s2", "stat", false).add(0);

    registry.snapshot(mb, true);

    MutableStat s2 = (MutableStat) registry.get("s2");

    s2.snapshot(mb, true); // should get the same back.
    s2.add(1);
    s2.snapshot(mb, true); // should get new interval values back

    verify(mb).addCounter(info("c1", "int counter"), 1);
    verify(mb).addCounter(info("c2", "long counter"), 2L);
    verify(mb).addGauge(info("g1", "int gauge"), 3);
    verify(mb).addGauge(info("g2", "long gauge"), 4L);
    verify(mb).addCounter(info("S1NumOps", "Number of ops for stat"), 1L);
    verify(mb).addGauge(eq(info("S1AvgTime", "Average time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1StdevTime",
                                "Standard deviation of time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1IMinTime",
                                "Interval min time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1IMaxTime",
                                "Interval max time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1MinTime","Min time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1MaxTime","Max time for stat")),
                           eq(0.0, EPSILON));
    verify(mb, times(2))
        .addCounter(info("S2NumOps", "Number of ops for stat"), 1L);
    verify(mb, times(2)).addGauge(eq(info("S2AvgTime",
                                          "Average time for stat")),
                                  eq(0.0, EPSILON));
    verify(mb).addCounter(info("S2NumOps", "Number of ops for stat"), 2L);
    verify(mb).addGauge(eq(info("S2AvgTime", "Average time for stat")),
                           eq(1.0, EPSILON));
  }

  interface TestProtocol {
    void foo();
    void bar();
  }

  @Test public void testMutableRates() {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    MutableRates rates = new MutableRates(registry);

    rates.init(TestProtocol.class);
    registry.snapshot(rb, false);

    assertCounter("FooNumOps", 0L, rb);
    assertGauge("FooAvgTime", 0.0, rb);
    assertCounter("BarNumOps", 0L, rb);
    assertGauge("BarAvgTime", 0.0, rb);
  }
}
