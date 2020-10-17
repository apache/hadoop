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

package org.apache.hadoop.test;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.*;

import org.junit.Assert;

import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.Mockito.*;

import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;

import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.util.Quantile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.metrics2.lib.Interns.*;

/**
 * Helpers for metrics source tests
 */
public class MetricsAsserts {

  final static Logger LOG = LoggerFactory.getLogger(MetricsAsserts.class);
  private static final double EPSILON = 0.00001;

  public static MetricsSystem mockMetricsSystem() {
    MetricsSystem ms = mock(MetricsSystem.class);
    DefaultMetricsSystem.setInstance(ms);
    return ms;
  }

  public static MetricsRecordBuilder mockMetricsRecordBuilder() {
    final MetricsCollector mc = mock(MetricsCollector.class);
    MetricsRecordBuilder rb = mock(MetricsRecordBuilder.class,
        new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        StringBuilder sb = new StringBuilder();
        for (Object o : args) {
          if (sb.length() > 0) sb.append(", ");
          sb.append(String.valueOf(o));
        }
        String methodName = invocation.getMethod().getName();
        LOG.debug(methodName +": "+ sb);
        return methodName.equals("parent") || methodName.equals("endRecord") ?
               mc : invocation.getMock();
      }
    });
    when(mc.addRecord(anyString())).thenReturn(rb);
    when(mc.addRecord(anyInfo())).thenReturn(rb);
    return rb;
  }

  /**
   * Call getMetrics on source and get a record builder mock to verify
   * @param source  the metrics source
   * @param all     if true, return all metrics even if not changed
   * @return the record builder mock to verifyÏ
   */
  public static MetricsRecordBuilder getMetrics(MetricsSource source,
                                                boolean all) {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    MetricsCollector mc = rb.parent();
    source.getMetrics(mc, all);
    return rb;
  }

  public static MetricsRecordBuilder getMetrics(String name) {
    return getMetrics(DefaultMetricsSystem.instance().getSource(name));
  }

  public static MetricsRecordBuilder getMetrics(MetricsSource source) {
    return getMetrics(source, true);
  }

  private static class InfoWithSameName
      implements ArgumentMatcher<MetricsInfo>{
    private final String expected;

    InfoWithSameName(MetricsInfo info) {
      expected = checkNotNull(info.name(), "info name");
    }

    @Override
    public boolean matches(MetricsInfo info) {
      return expected.equals(info.name());
    }

    @Override
    public String toString() {
      return "Info with name=" + expected;
    }
  }

  /**
   * MetricInfo with the same name
   * @param info to match
   * @return <code>null</code>
   */
  public static MetricsInfo eqName(MetricsInfo info) {
    return argThat(new InfoWithSameName(info));
  }

  private static class AnyInfo implements ArgumentMatcher<MetricsInfo> {
    @Override
    public boolean matches(MetricsInfo info) {
      return info != null;
    }
  }

  public static MetricsInfo anyInfo() {
    return argThat(new AnyInfo());
  }

  /**
   * Assert an int gauge metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertGauge(String name, int expected,
                                 MetricsRecordBuilder rb) {
    Assert.assertEquals("Bad value for metric " + name,
        expected, getIntGauge(name, rb));
  }

  public static int getIntGauge(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
    verify(rb, atLeast(0)).addGauge(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

  /**
   * Assert an int counter metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertCounter(String name, int expected,
                                   MetricsRecordBuilder rb) {
    Assert.assertEquals("Bad value for metric " + name,
        expected, getIntCounter(name, rb));
  }

  public static int getIntCounter(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(
        Integer.class);
    verify(rb, atLeast(0)).addCounter(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }
  
  /**
   * Assert a long gauge metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertGauge(String name, long expected,
                                 MetricsRecordBuilder rb) {
    Assert.assertEquals("Bad value for metric " + name,
        expected, getLongGauge(name, rb));
  }

  public static long getLongGauge(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(rb, atLeast(0)).addGauge(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

   /**
   * Assert a double gauge metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertGauge(String name, double expected,
                                 MetricsRecordBuilder rb) {
    Assert.assertEquals("Bad value for metric " + name,
        expected, getDoubleGauge(name, rb), EPSILON);
  }

  public static double getDoubleGauge(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Double> captor = ArgumentCaptor.forClass(Double.class);
    verify(rb, atLeast(0)).addGauge(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

  /**
   * Assert a long counter metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertCounter(String name, long expected,
                                   MetricsRecordBuilder rb) {
    Assert.assertEquals("Bad value for metric " + name,
        expected, getLongCounter(name, rb));
  }

  public static long getLongCounter(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(rb, atLeast(0)).addCounter(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

  public static long getLongCounterWithoutCheck(String name,
    MetricsRecordBuilder rb) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(rb, atLeast(0)).addCounter(eqName(info(name, "")), captor.capture());
    return captor.getValue();
  }

  public static String getStringMetric(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(rb, atLeast(0)).tag(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

   /**
   * Assert a float gauge metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertGauge(String name, float expected,
                                 MetricsRecordBuilder rb) {
    Assert.assertEquals("Bad value for metric " + name,
        expected, getFloatGauge(name, rb), EPSILON);
  }

  public static float getFloatGauge(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Float> captor = ArgumentCaptor.forClass(Float.class);
    verify(rb, atLeast(0)).addGauge(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

  /**
   * Check that this metric was captured exactly once.
   */
  private static void checkCaptured(ArgumentCaptor<?> captor, String name) {
    Assert.assertEquals("Expected exactly one metric for name " + name,
        1, captor.getAllValues().size());
  }

  /**
   * Assert an int gauge metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param source  to get metrics from
   */
  public static void assertGauge(String name, int expected,
                                 MetricsSource source) {
    assertGauge(name, expected, getMetrics(source));
  }

  /**
   * Assert an int counter metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param source  to get metrics from
   */
  public static void assertCounter(String name, int expected,
                                   MetricsSource source) {
    assertCounter(name, expected, getMetrics(source));
  }

  /**
   * Assert a long gauge metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param source  to get metrics from
   */
  public static void assertGauge(String name, long expected,
                                 MetricsSource source) {
    assertGauge(name, expected, getMetrics(source));
  }

  /**
   * Assert a long counter metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param source  to get metrics from
   */
  public static void assertCounter(String name, long expected,
                                   MetricsSource source) {
    assertCounter(name, expected, getMetrics(source));
  }

  /**
   * Assert that a long counter metric is greater than a value
   * @param name  of the metric
   * @param greater value of the metric should be greater than this
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertCounterGt(String name, long greater,
                                     MetricsRecordBuilder rb) {
    Assert.assertTrue("Bad value for metric " + name,
        getLongCounter(name, rb) > greater);
  }

  /**
   * Assert that a long counter metric is greater than a value
   * @param name  of the metric
   * @param greater value of the metric should be greater than this
   * @param source  the metrics source
   */
  public static void assertCounterGt(String name, long greater,
                                     MetricsSource source) {
    assertCounterGt(name, greater, getMetrics(source));
  }

  /**
   * Assert that a double gauge metric is greater than a value
   * @param name  of the metric
   * @param greater value of the metric should be greater than this
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertGaugeGt(String name, double greater,
                                   MetricsRecordBuilder rb) {
    Assert.assertTrue("Bad value for metric " + name,
        getDoubleGauge(name, rb) > greater);
  }

  /**
   * Assert that a double gauge metric is greater than a value
   * @param name  of the metric
   * @param greater value of the metric should be greater than this
   * @param source  the metrics source
   */
  public static void assertGaugeGt(String name, double greater,
                                   MetricsSource source) {
    assertGaugeGt(name, greater, getMetrics(source));
  }
  
  /**
   * Asserts that the NumOps and quantiles for a metric with value name
   * "Latency" have been changed at some point to a non-zero value.
   * 
   * @param prefix of the metric
   * @param rb MetricsRecordBuilder with the metric
   */
  public static void assertQuantileGauges(String prefix,
      MetricsRecordBuilder rb) {
    assertQuantileGauges(prefix, rb, "Latency");
  }

  /**
   * Asserts that the NumOps and quantiles for a metric have been changed at
   * some point to a non-zero value, for the specified value name of the
   * metrics (e.g., "Latency", "Count").
   *
   * @param prefix of the metric
   * @param rb MetricsRecordBuilder with the metric
   * @param valueName the value name for the metric
   */
  public static void assertQuantileGauges(String prefix,
      MetricsRecordBuilder rb, String valueName) {
    verify(rb).addGauge(eqName(info(prefix + "NumOps", "")), geq(0l));
    for (Quantile q : MutableQuantiles.quantiles) {
      String nameTemplate = prefix + "%dthPercentile" + valueName;
      int percentile = (int) (100 * q.quantile);
      verify(rb).addGauge(
          eqName(info(String.format(nameTemplate, percentile), "")),
          geq(0l));
    }
  }
}
