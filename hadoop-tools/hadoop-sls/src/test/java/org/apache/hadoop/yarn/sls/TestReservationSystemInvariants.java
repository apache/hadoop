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

package org.apache.hadoop.yarn.sls;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants.InvariantsChecker;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants.ReservationInvariantsChecker;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import net.jcip.annotations.NotThreadSafe;

/**
 * This test performs an SLS run enabling a
 * {@code ReservationInvariantsChecker}.
 */
@NotThreadSafe
public class TestReservationSystemInvariants extends BaseSLSRunnerTest {

  public static Collection<Object[]> data() {
    // Test with both schedulers, and all three trace types
    return Arrays.asList(new Object[][] {
        {CapacityScheduler.class.getCanonicalName(), "SYNTH",
            "src/test/resources/syn.json", null},
        {FairScheduler.class.getCanonicalName(), "SYNTH",
            "src/test/resources/syn.json", null}
    });
  }

  public void initTestReservationSystemInvariants(String pSchedulerType,
      String pTraceType, String pTraceLocation, String pNodeFile) {
    this.schedulerType = pSchedulerType;
    this.traceType = pTraceType;
    this.traceLocation = pTraceLocation;
    this.nodeFile = pNodeFile;
    setup();
  }

  @ParameterizedTest(name = "Testing with: {1}, {0}, (nodeFile {3})")
  @MethodSource("data")
  @Timeout(value = 120)
  @SuppressWarnings("all")
  public void testSimulatorRunning(String pSchedulerType,
      String pTraceType, String pTraceLocation, String pNodeFile) throws Exception {
    initTestReservationSystemInvariants(pSchedulerType, pTraceType, pTraceLocation, pNodeFile);
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.RM_SCHEDULER, schedulerType);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        ReservationInvariantsChecker.class.getCanonicalName());
    conf.setBoolean(InvariantsChecker.THROW_ON_VIOLATION, true);


    long timeTillShutDownInSec = 90;
    runSLS(conf, timeTillShutDownInSec);

  }

  @Override
  public void setup() {

  }
}
