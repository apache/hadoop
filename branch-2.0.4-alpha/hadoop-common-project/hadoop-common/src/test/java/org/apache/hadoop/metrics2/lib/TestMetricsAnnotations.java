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

package org.apache.hadoop.metrics2.lib;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metric.*;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.impl.MsInfo;
import static org.apache.hadoop.metrics2.lib.Interns.*;
import static org.apache.hadoop.test.MetricsAsserts.*;

public class TestMetricsAnnotations {

  static class MyMetrics {
    @Metric MutableCounterInt c1;
    @Metric({"Counter2", "Counter2 desc"}) MutableCounterLong c2;
    @Metric MutableGaugeInt g1, g2;
    @Metric("g3 desc") MutableGaugeLong g3;
    @Metric MutableRate r1;
    @Metric MutableStat s1;
    @Metric MutableRates rs1;
  }

  @Test public void testFields() {
    MyMetrics metrics = new MyMetrics();
    MetricsSource source = MetricsAnnotations.makeSource(metrics);

    metrics.c1.incr();
    metrics.c2.incr();
    metrics.g1.incr();
    metrics.g2.incr();
    metrics.g3.incr();
    metrics.r1.add(1);
    metrics.s1.add(1);
    metrics.rs1.add("rs1", 1);

    MetricsRecordBuilder rb = getMetrics(source);

    verify(rb).addCounter(info("C1", "C1"), 1);
    verify(rb).addCounter(info("Counter2", "Counter2 desc"), 1L);
    verify(rb).addGauge(info("G1", "G1"), 1);
    verify(rb).addGauge(info("G2", "G2"), 1);
    verify(rb).addGauge(info("G3", "g3 desc"), 1L);
    verify(rb).addCounter(info("R1NumOps", "Number of ops for r1"), 1L);
    verify(rb).addGauge(info("R1AvgTime", "Average time for r1"), 1.0);
    verify(rb).addCounter(info("S1NumOps", "Number of ops for s1"), 1L);
    verify(rb).addGauge(info("S1AvgTime", "Average time for s1"), 1.0);
    verify(rb).addCounter(info("Rs1NumOps", "Number of ops for rs1"), 1L);
    verify(rb).addGauge(info("Rs1AvgTime", "Average time for rs1"), 1.0);
  }

  static class BadMetrics {
    @Metric Integer i0;
  }

  @Test(expected=MetricsException.class) public void testBadFields() {
    MetricsAnnotations.makeSource(new BadMetrics());
  }

  static class MyMetrics2 {
    @Metric int getG1() { return 1; }
    @Metric long getG2() { return 2; }
    @Metric float getG3() { return 3; }
    @Metric double getG4() { return 4; }
    @Metric(type=Type.COUNTER) int getC1() { return 1; }
    @Metric(type=Type.COUNTER) long getC2() { return 2; }
    @Metric(type=Type.TAG) String getT1() { return "t1"; }
  }

  @Test public void testMethods() {
    MyMetrics2 metrics = new MyMetrics2();
    MetricsSource source = MetricsAnnotations.makeSource(metrics);
    MetricsRecordBuilder rb = getMetrics(source);

    verify(rb).addGauge(info("G1", "G1"), 1);
    verify(rb).addGauge(info("G2", "G2"), 2L);
    verify(rb).addGauge(info("G3", "G3"), 3.0f);
    verify(rb).addGauge(info("G4", "G4"), 4.0);
    verify(rb).addCounter(info("C1", "C1"), 1);
    verify(rb).addCounter(info("C2", "C2"), 2L);
    verify(rb).tag(info("T1", "T1"), "t1");
  }

  static class BadMetrics2 {
    @Metric int foo(int i) { return i; }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testBadMethodWithArgs() {
    MetricsAnnotations.makeSource(new BadMetrics2());
  }

  static class BadMetrics3 {
    @Metric boolean foo() { return true; }
  }

  @Test(expected=MetricsException.class)
  public void testBadMethodReturnType() {
    MetricsAnnotations.makeSource(new BadMetrics3());
  }

  @Metrics(about="My metrics", context="foo")
  static class MyMetrics3 {
    @Metric int getG1() { return 1; }
  }

  @Test public void testClasses() {
    MetricsRecordBuilder rb = getMetrics(
        MetricsAnnotations.makeSource(new MyMetrics3()));
    MetricsCollector collector = rb.parent();

    verify(collector).addRecord(info("MyMetrics3", "My metrics"));
    verify(rb).add(tag(MsInfo.Context, "foo"));
  }

  static class HybridMetrics implements MetricsSource {
    final MetricsRegistry registry = new MetricsRegistry("HybridMetrics")
        .setContext("hybrid");
    @Metric("C0 desc") MutableCounterInt C0;

    @Metric int getG0() { return 0; }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      collector.addRecord("foo")
                  .setContext("foocontext")
                  .addCounter(info("C1", "C1 desc"), 1)
                  .endRecord()
               .addRecord("bar")
                  .setContext("barcontext")
                  .addGauge(info("G1", "G1 desc"), 1);
      registry.snapshot(collector.addRecord(registry.info()), all);
    }
  }

  @Test public void testHybrid() {
    HybridMetrics metrics = new HybridMetrics();
    MetricsSource source = MetricsAnnotations.makeSource(metrics);

    assertSame(metrics, source);
    metrics.C0.incr();
    MetricsRecordBuilder rb = getMetrics(source);
    MetricsCollector collector = rb.parent();

    verify(collector).addRecord("foo");
    verify(collector).addRecord("bar");
    verify(collector).addRecord(info("HybridMetrics", "HybridMetrics"));
    verify(rb).setContext("foocontext");
    verify(rb).addCounter(info("C1", "C1 desc"), 1);
    verify(rb).setContext("barcontext");
    verify(rb).addGauge(info("G1", "G1 desc"), 1);
    verify(rb).add(tag(MsInfo.Context, "hybrid"));
    verify(rb).addCounter(info("C0", "C0 desc"), 1);
    verify(rb).addGauge(info("G0", "G0"), 0);
  }

  @Metrics(context="hybrid")
  static class BadHybridMetrics implements MetricsSource {

    @Metric MutableCounterInt c1;

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      collector.addRecord("foo");
    }
  }

  @Test(expected=MetricsException.class) public void testBadHybrid() {
    MetricsAnnotations.makeSource(new BadHybridMetrics());
  }

  static class EmptyMetrics {
    int foo;
  }

  @Test(expected=MetricsException.class) public void testEmptyMetrics() {
    MetricsAnnotations.makeSource(new EmptyMetrics());
  }
}
