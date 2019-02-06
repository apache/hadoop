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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

/**
 * This test performs simple runs of the SLS with the generic syn json format.
 */
@RunWith(value = Parameterized.class)
@NotThreadSafe
public class TestSLSStreamAMSynth extends BaseSLSRunnerTest {

  @Parameters(name = "Testing with: {1}, {0}, (nodeFile {3})")
  public static Collection<Object[]> data() {

    String capScheduler = CapacityScheduler.class.getCanonicalName();
    String fairScheduler = FairScheduler.class.getCanonicalName();
    String synthTraceFile = "src/test/resources/syn_stream.json";
    String nodeFile = "src/test/resources/nodes.json";

    // Test with both schedulers
    return Arrays.asList(new Object[][] {

        // covering the no nodeFile case
        {capScheduler, "SYNTH", synthTraceFile, null },

        // covering new commandline and CapacityScheduler
        {capScheduler, "SYNTH", synthTraceFile, nodeFile },

        // covering FairScheduler
        {fairScheduler, "SYNTH", synthTraceFile, nodeFile },
    });
  }

  @Before
  public void setup() {
    ongoingInvariantFile = "src/test/resources/ongoing-invariants.txt";
    exitInvariantFile = "src/test/resources/exit-invariants.txt";
  }

  @Test(timeout = 90000)
  @SuppressWarnings("all")
  public void testSimulatorRunning() throws Exception {
    Configuration conf = new Configuration(false);
    long timeTillShutdownInsec = 20L;
    runSLS(conf, timeTillShutdownInsec);
  }
}
