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

import static java.lang.Thread.sleep;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

public class TestDecayRpcScheduler {
  private Schedulable mockCall(String id) {
    Schedulable mockCall = mock(Schedulable.class);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(id);

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
  @SuppressWarnings("deprecation")
  public void testParsePeriod() {
    // By default
    scheduler = new DecayRpcScheduler(1, "ipc.1", new Configuration());
    assertEquals(DecayRpcScheduler.IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_DEFAULT,
      scheduler.getDecayPeriodMillis());

    // Custom
    Configuration conf = new Configuration();
    conf.setLong("ipc.2." + DecayRpcScheduler.IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY,
      1058);
    scheduler = new DecayRpcScheduler(1, "ipc.2", conf);
    assertEquals(1058L, scheduler.getDecayPeriodMillis());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testParseFactor() {
    // Default
    scheduler = new DecayRpcScheduler(1, "ipc.3", new Configuration());
    assertEquals(DecayRpcScheduler.IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_DEFAULT,
      scheduler.getDecayFactor(), 0.00001);

    // Custom
    Configuration conf = new Configuration();
    conf.set("ipc.4." + DecayRpcScheduler.IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY,
      "0.125");
    scheduler = new DecayRpcScheduler(1, "ipc.4", conf);
    assertEquals(0.125, scheduler.getDecayFactor(), 0.00001);
  }

  public void assertEqualDecimalArrays(double[] a, double[] b) {
    assertEquals(a.length, b.length);
    for(int i = 0; i < a.length; i++) {
      assertEquals(a[i], b[i], 0.00001);
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testParseThresholds() {
    // Defaults vary by number of queues
    Configuration conf = new Configuration();
    scheduler = new DecayRpcScheduler(1, "ipc.5", conf);
    assertEqualDecimalArrays(new double[]{}, scheduler.getThresholds());

    scheduler = new DecayRpcScheduler(2, "ipc.6", conf);
    assertEqualDecimalArrays(new double[]{0.5}, scheduler.getThresholds());

    scheduler = new DecayRpcScheduler(3, "ipc.7", conf);
    assertEqualDecimalArrays(new double[]{0.25, 0.5}, scheduler.getThresholds());

    scheduler = new DecayRpcScheduler(4, "ipc.8", conf);
    assertEqualDecimalArrays(new double[]{0.125, 0.25, 0.5}, scheduler.getThresholds());

    // Custom
    conf = new Configuration();
    conf.set("ipc.9." + DecayRpcScheduler.IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY,
      "1, 10, 20, 50, 85");
    scheduler = new DecayRpcScheduler(6, "ipc.9", conf);
    assertEqualDecimalArrays(new double[]{0.01, 0.1, 0.2, 0.5, 0.85}, scheduler.getThresholds());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAccumulate() {
    Configuration conf = new Configuration();
    conf.set("ipc.10." + DecayRpcScheduler.IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY,
        "99999999"); // Never flush
    scheduler = new DecayRpcScheduler(1, "ipc.10", conf);

    assertEquals(0, scheduler.getCallCostSnapshot().size()); // empty first

    getPriorityIncrementCallCount("A");
    assertEquals(1, scheduler.getCallCostSnapshot().get("A").longValue());
    assertEquals(1, scheduler.getCallCostSnapshot().get("A").longValue());

    getPriorityIncrementCallCount("A");
    getPriorityIncrementCallCount("B");
    getPriorityIncrementCallCount("A");

    assertEquals(3, scheduler.getCallCostSnapshot().get("A").longValue());
    assertEquals(1, scheduler.getCallCostSnapshot().get("B").longValue());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDecay() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("ipc.11." // Never decay
        + DecayRpcScheduler.IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY, 999999999);
    conf.setDouble("ipc.11."
        + DecayRpcScheduler.IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_KEY, 0.5);
    scheduler = new DecayRpcScheduler(1, "ipc.11", conf);

    assertEquals(0, scheduler.getTotalCallSnapshot());

    for (int i = 0; i < 4; i++) {
      getPriorityIncrementCallCount("A");
    }

    sleep(1000);

    for (int i = 0; i < 8; i++) {
      getPriorityIncrementCallCount("B");
    }

    assertEquals(12, scheduler.getTotalCallSnapshot());
    assertEquals(4, scheduler.getCallCostSnapshot().get("A").longValue());
    assertEquals(8, scheduler.getCallCostSnapshot().get("B").longValue());

    scheduler.forceDecay();

    assertEquals(6, scheduler.getTotalCallSnapshot());
    assertEquals(2, scheduler.getCallCostSnapshot().get("A").longValue());
    assertEquals(4, scheduler.getCallCostSnapshot().get("B").longValue());

    scheduler.forceDecay();

    assertEquals(3, scheduler.getTotalCallSnapshot());
    assertEquals(1, scheduler.getCallCostSnapshot().get("A").longValue());
    assertEquals(2, scheduler.getCallCostSnapshot().get("B").longValue());

    scheduler.forceDecay();

    assertEquals(1, scheduler.getTotalCallSnapshot());
    assertEquals(null, scheduler.getCallCostSnapshot().get("A"));
    assertEquals(1, scheduler.getCallCostSnapshot().get("B").longValue());

    scheduler.forceDecay();

    assertEquals(0, scheduler.getTotalCallSnapshot());
    assertEquals(null, scheduler.getCallCostSnapshot().get("A"));
    assertEquals(null, scheduler.getCallCostSnapshot().get("B"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testPriority() throws Exception {
    Configuration conf = new Configuration();
    final String namespace = "ipc.12";
    conf.set(namespace + "." + DecayRpcScheduler
        .IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY, "99999999"); // Never flush
    conf.set(namespace + "." + DecayRpcScheduler
        .IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY, "25, 50, 75");
    scheduler = new DecayRpcScheduler(4, namespace, conf);

    assertEquals(0, getPriorityIncrementCallCount("A")); // 0 out of 0 calls
    assertEquals(3, getPriorityIncrementCallCount("A")); // 1 out of 1 calls
    assertEquals(0, getPriorityIncrementCallCount("B")); // 0 out of 2 calls
    assertEquals(1, getPriorityIncrementCallCount("B")); // 1 out of 3 calls
    assertEquals(0, getPriorityIncrementCallCount("C")); // 0 out of 4 calls
    assertEquals(0, getPriorityIncrementCallCount("C")); // 1 out of 5 calls
    assertEquals(1, getPriorityIncrementCallCount("A")); // 2 out of 6 calls
    assertEquals(1, getPriorityIncrementCallCount("A")); // 3 out of 7 calls
    assertEquals(2, getPriorityIncrementCallCount("A")); // 4 out of 8 calls
    assertEquals(2, getPriorityIncrementCallCount("A")); // 5 out of 9 calls

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service="+ namespace + ",name=DecayRpcScheduler");

    String cvs1 = (String) mbs.getAttribute(mxbeanName, "CallVolumeSummary");
    assertTrue("Get expected JMX of CallVolumeSummary before decay",
        cvs1.equals("{\"A\":6,\"B\":2,\"C\":2}"));

    scheduler.forceDecay();

    String cvs2 = (String) mbs.getAttribute(mxbeanName, "CallVolumeSummary");
    assertTrue("Get expected JMX for CallVolumeSummary after decay",
        cvs2.equals("{\"A\":3,\"B\":1,\"C\":1}"));
  }

  @Test(timeout=2000)
  @SuppressWarnings("deprecation")
  public void testPeriodic() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.set(
        "ipc.13." + DecayRpcScheduler.IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY, "10");
    conf.set(
        "ipc.13." + DecayRpcScheduler.IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY, "0.5");
    scheduler = new DecayRpcScheduler(1, "ipc.13", conf);

    assertEquals(10, scheduler.getDecayPeriodMillis());
    assertEquals(0, scheduler.getTotalCallSnapshot());

    for (int i = 0; i < 64; i++) {
      getPriorityIncrementCallCount("A");
    }

    // It should eventually decay to zero
    while (scheduler.getTotalCallSnapshot() > 0) {
      sleep(10);
    }
  }

  @Test(timeout=60000)
  public void testNPEatInitialization() throws InterruptedException {
    // redirect the LOG to and check if there is NPE message while initializing
    // the DecayRpcScheduler
    PrintStream output = System.out;
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      System.setOut(new PrintStream(bytes));

      // initializing DefaultMetricsSystem here would set "monitoring" flag in
      // MetricsSystemImpl to true
      DefaultMetricsSystem.initialize("NameNode");
      Configuration conf = new Configuration();
      scheduler = new DecayRpcScheduler(1, "ipc.14", conf);
      // check if there is npe in log
      assertFalse(bytes.toString().contains("NullPointerException"));
    } finally {
      //set systout back
      System.setOut(output);
    }
  }

  @Test
  public void testUsingWeightedTimeCostProvider() {
    scheduler = getSchedulerWithWeightedTimeCostProvider(3, "ipc.15");

    // 3 details in increasing order of cost. Although medium has a longer
    // duration, the shared lock is weighted less than the exclusive lock
    ProcessingDetails callDetailsLow =
        new ProcessingDetails(TimeUnit.MILLISECONDS);
    callDetailsLow.set(ProcessingDetails.Timing.LOCKFREE, 1);
    ProcessingDetails callDetailsMedium =
        new ProcessingDetails(TimeUnit.MILLISECONDS);
    callDetailsMedium.set(ProcessingDetails.Timing.LOCKSHARED, 500);
    ProcessingDetails callDetailsHigh =
        new ProcessingDetails(TimeUnit.MILLISECONDS);
    callDetailsHigh.set(ProcessingDetails.Timing.LOCKEXCLUSIVE, 100);

    for (int i = 0; i < 10; i++) {
      scheduler.addResponseTime("ignored", mockCall("LOW"), callDetailsLow);
    }
    scheduler.addResponseTime("ignored", mockCall("MED"), callDetailsMedium);
    scheduler.addResponseTime("ignored", mockCall("HIGH"), callDetailsHigh);

    assertEquals(0, scheduler.getPriorityLevel(mockCall("LOW")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("MED")));
    assertEquals(2, scheduler.getPriorityLevel(mockCall("HIGH")));

    assertEquals(3, scheduler.getUniqueIdentityCount());
    long totalCallInitial = scheduler.getTotalRawCallVolume();
    assertEquals(totalCallInitial, scheduler.getTotalCallVolume());

    scheduler.forceDecay();

    // Relative priorities should stay the same after a single decay
    assertEquals(0, scheduler.getPriorityLevel(mockCall("LOW")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("MED")));
    assertEquals(2, scheduler.getPriorityLevel(mockCall("HIGH")));

    assertEquals(3, scheduler.getUniqueIdentityCount());
    assertEquals(totalCallInitial, scheduler.getTotalRawCallVolume());
    assertTrue(scheduler.getTotalCallVolume() < totalCallInitial);

    for (int i = 0; i < 100; i++) {
      scheduler.forceDecay();
    }
    // After enough decay cycles, all callers should be high priority again
    assertEquals(0, scheduler.getPriorityLevel(mockCall("LOW")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("MED")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("HIGH")));
  }

  @Test
  public void testUsingWeightedTimeCostProviderWithZeroCostCalls() {
    scheduler = getSchedulerWithWeightedTimeCostProvider(2, "ipc.16");

    ProcessingDetails emptyDetails =
        new ProcessingDetails(TimeUnit.MILLISECONDS);

    for (int i = 0; i < 1000; i++) {
      scheduler.addResponseTime("ignored", mockCall("MANY"), emptyDetails);
    }
    scheduler.addResponseTime("ignored", mockCall("FEW"), emptyDetails);

    // Since the calls are all "free", they should have the same priority
    assertEquals(0, scheduler.getPriorityLevel(mockCall("MANY")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("FEW")));
  }

  @Test
  public void testUsingWeightedTimeCostProviderNoRequests() {
    scheduler = getSchedulerWithWeightedTimeCostProvider(2, "ipc.18");

    assertEquals(0, scheduler.getPriorityLevel(mockCall("A")));
  }

  /**
   * Get a scheduler that uses {@link WeightedTimeCostProvider} and has
   * normal decaying disabled.
   */
  private static DecayRpcScheduler getSchedulerWithWeightedTimeCostProvider(
      int priorityLevels, String ns) {
    Configuration conf = new Configuration();
    conf.setClass(ns + "." + CommonConfigurationKeys.IPC_COST_PROVIDER_KEY,
        WeightedTimeCostProvider.class, CostProvider.class);
    conf.setLong(ns + "."
        + DecayRpcScheduler.IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY, 999999);
    return new DecayRpcScheduler(priorityLevels, ns, conf);
  }

  /**
   * Get the priority and increment the call count, assuming that
   * {@link DefaultCostProvider} is in use.
   */
  private int getPriorityIncrementCallCount(String callId) {
    Schedulable mockCall = mockCall(callId);
    int priority = scheduler.getPriorityLevel(mockCall);
    // The DefaultCostProvider uses a cost of 1 for all calls, ignoring
    // the processing details, so an empty one is fine
    ProcessingDetails emptyProcessingDetails =
        new ProcessingDetails(TimeUnit.MILLISECONDS);
    scheduler.addResponseTime("ignored", mockCall, emptyProcessingDetails);
    return priority;
  }
}