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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.mockito.Mockito.*;
import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;
import static org.mockito.AdditionalMatchers.*;

import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.util.StringUtils;

/**
 * Helpers for metrics source tests
 */
public class MetricsAsserts {

  final static Log LOG = LogFactory.getLog(MetricsAsserts.class);

  /**
   * Call getMetrics on source and get a record builder mock to verify
   * @param source  the metrics source
   * @return the record builder mock to verify
   */
  public static MetricsRecordBuilder getMetrics(MetricsSource source) {
    MetricsBuilder mb = mock(MetricsBuilder.class);
    final MetricsRecordBuilder rb = mock(MetricsRecordBuilder.class,
        new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        StringBuilder sb = new StringBuilder();
        for (Object o : args) {
          if (sb.length() > 0) sb.append(", ");
          sb.append(String.valueOf(o));
        }
        LOG.debug(invocation.getMethod().getName() +": "+ sb);
        return invocation.getMock();
      }
    });
    when(mb.addRecord(anyString())).thenReturn(rb);
    source.getMetrics(mb, true);
    return rb;
  }

  /**
   * Assert an int gauge metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertGauge(String name, int expected,
                                 MetricsRecordBuilder rb) {
    verify(rb).addGauge(eq(name), anyString(), eq(expected));
  }

  /**
   * Assert an int counter metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertCounter(String name, int expected,
                                   MetricsRecordBuilder rb) {
    verify(rb).addCounter(eq(name), anyString(), eq(expected));
  }

  /**
   * Assert a long gauge metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertGauge(String name, long expected,
                                 MetricsRecordBuilder rb) {
    verify(rb).addGauge(eq(name), anyString(), eq(expected));
  }

  /**
   * Assert a long counter metric as expected
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertCounter(String name, long expected,
                                   MetricsRecordBuilder rb) {
    verify(rb).addCounter(eq(name), anyString(), eq(expected));
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
    verify(rb).addCounter(eq(name), anyString(), gt(greater));
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
    verify(rb).addGauge(eq(name), anyString(), gt(greater));
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

}
