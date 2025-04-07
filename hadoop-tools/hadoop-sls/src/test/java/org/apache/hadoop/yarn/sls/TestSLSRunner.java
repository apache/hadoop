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

import net.jcip.annotations.NotThreadSafe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.security.Security;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test performs simple runs of the SLS with different trace types and
 * schedulers.
 */
@NotThreadSafe
public class TestSLSRunner extends BaseSLSRunnerTest {

  public static Collection<Object[]> data() {

    String capScheduler = CapacityScheduler.class.getCanonicalName();
    String fairScheduler = FairScheduler.class.getCanonicalName();
    String slsTraceFile = "src/test/resources/inputsls.json";
    String rumenTraceFile = "src/main/data/2jobs2min-rumen-jh.json";
    String synthTraceFile = "src/test/resources/syn.json";
    String nodeFile = "src/test/resources/nodes.json";

    // Test with both schedulers, and all three load producers.
    return Arrays.asList(new Object[][] {

        // covering old commandline in tests
        {capScheduler, "OLD_RUMEN", rumenTraceFile, nodeFile },
        {capScheduler, "OLD_SLS", slsTraceFile, nodeFile },

        // covering the no nodeFile case
        {capScheduler, "SYNTH", synthTraceFile, null },
        {capScheduler, "RUMEN", rumenTraceFile, null },
        {capScheduler, "SLS", slsTraceFile, null },

        // covering new commandline and CapacityScheduler
        {capScheduler, "SYNTH", synthTraceFile, nodeFile },
        {capScheduler, "RUMEN", rumenTraceFile, nodeFile },
        {capScheduler, "SLS", slsTraceFile, nodeFile },

        // covering FairScheduler
        {fairScheduler, "SYNTH", synthTraceFile, nodeFile },
        {fairScheduler, "RUMEN", rumenTraceFile, nodeFile },
        {fairScheduler, "SLS", slsTraceFile, nodeFile }
    });
  }

  public void setup() {
    ongoingInvariantFile = "src/test/resources/ongoing-invariants.txt";
    exitInvariantFile = "src/test/resources/exit-invariants.txt";
  }

  public void initTestSLSRunner(String pSchedulerType,
      String pTraceType, String pTraceLocation, String pNodeFile) {
    this.schedulerType = pSchedulerType;
    this.traceType = pTraceType;
    this.traceLocation = pTraceLocation;
    this.nodeFile = pNodeFile;
    setup();
  }

  @ParameterizedTest(name = "Testing with: {1}, {0}, (nodeFile {3})")
  @Timeout(value = 90)
  @MethodSource("data")
  @SuppressWarnings("all")
  public void testSimulatorRunning(String pSchedulerType,
    String pTraceType, String pTraceLocation, String pNodeFile) throws Exception {
    initTestSLSRunner(pSchedulerType, pTraceType, pTraceLocation, pNodeFile);
    Configuration conf = new Configuration(false);
    long timeTillShutdownInsec = 20L;
    runSLS(conf, timeTillShutdownInsec);
  }

  /**
   * Test to check whether caching is enabled based on config.
   */
  @ParameterizedTest(name = "Testing with: {1}, {0}, (nodeFile {3})")
  @MethodSource("data")
  public void testEnableCaching(String pSchedulerType,
    String pTraceType, String pTraceLocation, String pNodeFile) {
    initTestSLSRunner(pSchedulerType, pTraceType, pTraceLocation, pNodeFile);
    String networkCacheDefault = Security.getProperty(
        SLSRunner.NETWORK_CACHE_TTL);
    String networkNegativeCacheDefault =
        Security.getProperty(SLSRunner.NETWORK_NEGATIVE_CACHE_TTL);

    try {
      Configuration conf = new Configuration(false);
      // check when dns caching is disabled
      conf.setBoolean(SLSConfiguration.DNS_CACHING_ENABLED, false);
      SLSRunner.enableDNSCaching(conf);
      assertEquals(networkCacheDefault,
          Security.getProperty(SLSRunner.NETWORK_CACHE_TTL));
      assertEquals(networkNegativeCacheDefault,
          Security.getProperty(SLSRunner.NETWORK_NEGATIVE_CACHE_TTL));

      // check when dns caching is enabled
      conf.setBoolean(SLSConfiguration.DNS_CACHING_ENABLED, true);
      SLSRunner.enableDNSCaching(conf);
      assertEquals("-1",
          Security.getProperty(SLSRunner.NETWORK_CACHE_TTL));
      assertEquals("-1",
          Security.getProperty(SLSRunner.NETWORK_NEGATIVE_CACHE_TTL));
    } finally {
      // set security settings back to default
      Security.setProperty(SLSRunner.NETWORK_CACHE_TTL,
          String.valueOf(networkCacheDefault));
      Security.setProperty(SLSRunner.NETWORK_NEGATIVE_CACHE_TTL,
          String.valueOf(networkNegativeCacheDefault));
    }
  }
}
