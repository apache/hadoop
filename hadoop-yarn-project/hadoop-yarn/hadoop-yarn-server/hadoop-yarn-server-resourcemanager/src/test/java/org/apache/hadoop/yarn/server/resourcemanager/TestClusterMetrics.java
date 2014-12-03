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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;

public class TestClusterMetrics {

  private ClusterMetrics metrics;
  /**
   * Test aMLaunchDelay and aMRegisterDelay Metrics
   */
  @Test
  public void testAmMetrics() throws Exception {
    assert(metrics != null);
    Assert.assertTrue(!metrics.aMLaunchDelay.changed());
    Assert.assertTrue(!metrics.aMRegisterDelay.changed());
    metrics.addAMLaunchDelay(1);
    metrics.addAMRegisterDelay(1);
    Assert.assertTrue(metrics.aMLaunchDelay.changed());
    Assert.assertTrue(metrics.aMRegisterDelay.changed());
  }

  @Before
  public void setup() {
    DefaultMetricsSystem.initialize("ResourceManager");
    metrics = ClusterMetrics.getMetrics();
  }

  @After
  public void tearDown() {
    ClusterMetrics.destroy();

    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource("ClusterMetrics") != null) {
      DefaultMetricsSystem.shutdown();
    }
  }
}
