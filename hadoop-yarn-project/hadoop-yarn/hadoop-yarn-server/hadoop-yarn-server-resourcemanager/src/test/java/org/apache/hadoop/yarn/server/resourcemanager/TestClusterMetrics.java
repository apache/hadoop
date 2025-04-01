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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

public class TestClusterMetrics {

  private ClusterMetrics metrics;
  /**
   * Test below metrics
   *  - aMLaunchDelay
   *  - aMRegisterDelay
   *  - aMContainerAllocationDelay
   */
  @Test
  public void testAmMetrics() throws Exception {
    assert(metrics != null);
    assertTrue(!metrics.aMLaunchDelay.changed());
    assertTrue(!metrics.aMRegisterDelay.changed());
    assertTrue(!metrics.getAMContainerAllocationDelay().changed());
    metrics.addAMLaunchDelay(1);
    metrics.addAMRegisterDelay(1);
    metrics.addAMContainerAllocationDelay(1);
    assertTrue(metrics.aMLaunchDelay.changed());
    assertTrue(metrics.aMRegisterDelay.changed());
    assertTrue(metrics.getAMContainerAllocationDelay().changed());
  }

  @BeforeEach
  public void setup() {
    DefaultMetricsSystem.initialize("ResourceManager");
    metrics = ClusterMetrics.getMetrics();
  }

  @AfterEach
  public void tearDown() {
    ClusterMetrics.destroy();

    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource("ClusterMetrics") != null) {
      DefaultMetricsSystem.shutdown();
    }
  }

  @Test
  public void testClusterMetrics() throws Exception {
    assertTrue(!metrics.containerAssignedPerSecond.changed());
    metrics.incrNumContainerAssigned();
    metrics.incrNumContainerAssigned();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return metrics.getContainerAssignedPerSecond() == 2;
      }
    }, 500, 5000);
  }

}
