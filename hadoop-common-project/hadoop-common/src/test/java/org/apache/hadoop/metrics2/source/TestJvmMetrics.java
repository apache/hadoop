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

package org.apache.hadoop.metrics2.source;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.mockito.Mockito.*;
import static org.apache.hadoop.test.MetricsAsserts.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.JvmPauseMonitor;

import static org.apache.hadoop.metrics2.source.JvmMetricsInfo.*;
import static org.apache.hadoop.metrics2.impl.MsInfo.*;

public class TestJvmMetrics {

  @Rule
  public Timeout timeout = new Timeout(30000);
  private JvmPauseMonitor pauseMonitor;

  /**
   * Robust shutdown of the pause monitor if it hasn't been stopped already.
   */
  @After
  public void teardown() {
    ServiceOperations.stop(pauseMonitor);
  }

  @Test
  public void testPresence() {
    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(new Configuration());
    pauseMonitor.start();
    JvmMetrics jvmMetrics = new JvmMetrics("test", "test");
    jvmMetrics.setPauseMonitor(pauseMonitor);
    MetricsRecordBuilder rb = getMetrics(jvmMetrics);
    MetricsCollector mc = rb.parent();

    verify(mc).addRecord(JvmMetrics);
    verify(rb).tag(ProcessName, "test");
    verify(rb).tag(SessionId, "test");
    for (JvmMetricsInfo info : JvmMetricsInfo.values()) {
      if (info.name().startsWith("Mem"))
        verify(rb).addGauge(eq(info), anyFloat());
      else if (info.name().startsWith("Gc"))
        verify(rb).addCounter(eq(info), anyLong());
      else if (info.name().startsWith("Threads"))
        verify(rb).addGauge(eq(info), anyInt());
      else if (info.name().startsWith("Log"))
        verify(rb).addCounter(eq(info), anyLong());
    }
  }

  @Test
  public void testDoubleStop() throws Throwable {
    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(new Configuration());
    pauseMonitor.start();
    pauseMonitor.stop();
    pauseMonitor.stop();
  }

  @Test
  public void testDoubleStart() throws Throwable {
    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(new Configuration());
    pauseMonitor.start();
    pauseMonitor.start();
    pauseMonitor.stop();
  }

  @Test
  public void testStopBeforeStart() throws Throwable {
    pauseMonitor = new JvmPauseMonitor();
    try {
      pauseMonitor.init(new Configuration());
      pauseMonitor.stop();
      pauseMonitor.start();
      Assert.fail("Expected an exception, got " + pauseMonitor);
    } catch (ServiceStateException e) {
      GenericTestUtils.assertExceptionContains("cannot enter state", e);
    }
  }

  @Test
  public void testStopBeforeInit() throws Throwable {
    pauseMonitor = new JvmPauseMonitor();
    try {
      pauseMonitor.stop();
      pauseMonitor.init(new Configuration());
      Assert.fail("Expected an exception, got " + pauseMonitor);
    } catch (ServiceStateException e) {
      GenericTestUtils.assertExceptionContains("cannot enter state", e);
    }
  }

}
