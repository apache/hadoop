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

package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.Arrays;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.CommonConfigurationKeys;

public class TestDecayRpcScheduler {
  private Schedulable mockCall(String id) {
    Schedulable mockCall = mock(Schedulable.class);
    UserGroupInformation ugi = mock(UserGroupInformation.class);

    when(ugi.getUserName()).thenReturn(id);
    when(mockCall.getUserGroupInformation()).thenReturn(ugi);

    return mockCall;
  }

  private DecayRpcScheduler scheduler;

  @Test(expected=IllegalArgumentException.class)
  public void testNegativeScheduler() {
    scheduler = new DecayRpcScheduler(-1, "", new Configuration());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testZeroScheduler() {
    scheduler = new DecayRpcScheduler(0, "", new Configuration());
  }

  @Test
  public void testParsePeriod() {
    // By default
    scheduler = new DecayRpcScheduler(1, "", new Configuration());
    assertEquals(DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_DEFAULT,
      scheduler.getDecayPeriodMillis());

    // Custom
    Configuration conf = new Configuration();
    conf.setLong("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY,
      1058);
    scheduler = new DecayRpcScheduler(1, "ns", conf);
    assertEquals(1058L, scheduler.getDecayPeriodMillis());
  }

  @Test
  public void testParseFactor() {
    // Default
    scheduler = new DecayRpcScheduler(1, "", new Configuration());
    assertEquals(DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_DEFAULT,
      scheduler.getDecayFactor(), 0.00001);

    // Custom
    Configuration conf = new Configuration();
    conf.set("prefix." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY,
      "0.125");
    scheduler = new DecayRpcScheduler(1, "prefix", conf);
    assertEquals(0.125, scheduler.getDecayFactor(), 0.00001);
  }

  public void assertEqualDecimalArrays(double[] a, double[] b) {
    assertEquals(a.length, b.length);
    for(int i = 0; i < a.length; i++) {
      assertEquals(a[i], b[i], 0.00001);
    }
  }

  @Test
  public void testParseThresholds() {
    // Defaults vary by number of queues
    Configuration conf = new Configuration();
    scheduler = new DecayRpcScheduler(1, "", conf);
    assertEqualDecimalArrays(new double[]{}, scheduler.getThresholds());

    scheduler = new DecayRpcScheduler(2, "", conf);
    assertEqualDecimalArrays(new double[]{0.5}, scheduler.getThresholds());

    scheduler = new DecayRpcScheduler(3, "", conf);
    assertEqualDecimalArrays(new double[]{0.25, 0.5}, scheduler.getThresholds());

    scheduler = new DecayRpcScheduler(4, "", conf);
    assertEqualDecimalArrays(new double[]{0.125, 0.25, 0.5}, scheduler.getThresholds());

    // Custom
    conf = new Configuration();
    conf.set("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_THRESHOLDS_KEY,
      "1, 10, 20, 50, 85");
    scheduler = new DecayRpcScheduler(6, "ns", conf);
    assertEqualDecimalArrays(new double[]{0.01, 0.1, 0.2, 0.5, 0.85}, scheduler.getThresholds());
  }

  @Test
  public void testAccumulate() {
    Configuration conf = new Configuration();
    conf.set("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY, "99999999"); // Never flush
    scheduler = new DecayRpcScheduler(1, "ns", conf);

    assertEquals(0, scheduler.getCallCountSnapshot().size()); // empty first

    scheduler.getPriorityLevel(mockCall("A"));
    assertEquals(1, scheduler.getCallCountSnapshot().get("A").longValue());
    assertEquals(1, scheduler.getCallCountSnapshot().get("A").longValue());

    scheduler.getPriorityLevel(mockCall("A"));
    scheduler.getPriorityLevel(mockCall("B"));
    scheduler.getPriorityLevel(mockCall("A"));

    assertEquals(3, scheduler.getCallCountSnapshot().get("A").longValue());
    assertEquals(1, scheduler.getCallCountSnapshot().get("B").longValue());
  }

  @Test
  public void testDecay() {
    Configuration conf = new Configuration();
    conf.set("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY, "999999999"); // Never
    conf.set("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY, "0.5");
    scheduler = new DecayRpcScheduler(1, "ns", conf);

    assertEquals(0, scheduler.getTotalCallSnapshot());

    for (int i = 0; i < 4; i++) {
      scheduler.getPriorityLevel(mockCall("A"));
    }

    for (int i = 0; i < 8; i++) {
      scheduler.getPriorityLevel(mockCall("B"));
    }

    assertEquals(12, scheduler.getTotalCallSnapshot());
    assertEquals(4, scheduler.getCallCountSnapshot().get("A").longValue());
    assertEquals(8, scheduler.getCallCountSnapshot().get("B").longValue());

    scheduler.forceDecay();

    assertEquals(6, scheduler.getTotalCallSnapshot());
    assertEquals(2, scheduler.getCallCountSnapshot().get("A").longValue());
    assertEquals(4, scheduler.getCallCountSnapshot().get("B").longValue());

    scheduler.forceDecay();

    assertEquals(3, scheduler.getTotalCallSnapshot());
    assertEquals(1, scheduler.getCallCountSnapshot().get("A").longValue());
    assertEquals(2, scheduler.getCallCountSnapshot().get("B").longValue());

    scheduler.forceDecay();

    assertEquals(1, scheduler.getTotalCallSnapshot());
    assertEquals(null, scheduler.getCallCountSnapshot().get("A"));
    assertEquals(1, scheduler.getCallCountSnapshot().get("B").longValue());

    scheduler.forceDecay();

    assertEquals(0, scheduler.getTotalCallSnapshot());
    assertEquals(null, scheduler.getCallCountSnapshot().get("A"));
    assertEquals(null, scheduler.getCallCountSnapshot().get("B"));
  }

  @Test
  public void testPriority() {
    Configuration conf = new Configuration();
    conf.set("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY, "99999999"); // Never flush
    conf.set("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_THRESHOLDS_KEY,
      "25, 50, 75");
    scheduler = new DecayRpcScheduler(4, "ns", conf);

    assertEquals(0, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(2, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("B")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("B")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("C")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("C")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(2, scheduler.getPriorityLevel(mockCall("A")));
  }

  @Test(timeout=2000)
  public void testPeriodic() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.set("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY, "10");
    conf.set("ns." + DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY, "0.5");
    scheduler = new DecayRpcScheduler(1, "ns", conf);

    assertEquals(10, scheduler.getDecayPeriodMillis());
    assertEquals(0, scheduler.getTotalCallSnapshot());

    for (int i = 0; i < 64; i++) {
      scheduler.getPriorityLevel(mockCall("A"));
    }

    // It should eventually decay to zero
    while (scheduler.getTotalCallSnapshot() > 0) {
      Thread.sleep(10);
    }
  }
}