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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.fail;

/**
 * This class tests the {@code MetricsInvariantChecker} by running it multiple
 * time and reporting the time it takes to execute, as well as verifying that
 * the invariant throws in case the invariants are not respected.
 */
public class TestMetricsInvariantChecker {
  public final static Logger LOG =
      Logger.getLogger(TestMetricsInvariantChecker.class);

  private MetricsSystem metricsSystem;
  private MetricsInvariantChecker ic;
  private Configuration conf;

  @Before
  public void setup() {
    this.metricsSystem = DefaultMetricsSystem.instance();
    JvmMetrics.initSingleton("ResourceManager", null);
    this.ic = new MetricsInvariantChecker();
    this.conf = new Configuration();
    if (Shell.isJavaVersionAtLeast(9)) {
      conf.set(MetricsInvariantChecker.INVARIANTS_FILE,
          "src/test/resources/invariants_jdk9.txt");
    } else {
      conf.set(MetricsInvariantChecker.INVARIANTS_FILE,
          "src/test/resources/invariants.txt");
    }
    conf.setBoolean(MetricsInvariantChecker.THROW_ON_VIOLATION, true);
    ic.init(conf, null, null);
  }

  @Test(timeout = 5000)
  public void testManyRuns() {

    QueueMetrics qm =
        QueueMetrics.forQueue(metricsSystem, "root", null, false, conf);
    qm.setAvailableResourcesToQueue(RMNodeLabelsManager.NO_LABEL,
        Resource.newInstance(1, 1));

    int numIterations = 1000;
    long start = System.currentTimeMillis();
    for (int i = 0; i < numIterations; i++) {
      ic.editSchedule();
    }
    long end = System.currentTimeMillis();

    System.out.println("Runtime per iteration (avg of " + numIterations
        + " iterations): " + (end - start) + " tot time");

  }

  @Test
  public void testViolation() {

    // create a "wrong" condition in which the invariants are not respected
    QueueMetrics qm =
        QueueMetrics.forQueue(metricsSystem, "root", null, false, conf);
    qm.setAvailableResourcesToQueue(RMNodeLabelsManager.NO_LABEL,
        Resource.newInstance(-1, -1));

    // test with throwing exception turned on
    try {
      ic.editSchedule();
      fail();
    } catch (InvariantViolationException i) {
      // expected
    }

    // test log-only mode
    conf.setBoolean(MetricsInvariantChecker.THROW_ON_VIOLATION, false);
    ic.init(conf, null, null);
    ic.editSchedule();

  }

}
