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
package org.apache.hadoop.ipc.metrics;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.DecayRpcScheduler;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.Test;

public class TestDecayRpcSchedulerDetailedMetrics {

  @Test
  public void metricsRegistered() {
    Configuration conf = new Configuration();
    DecayRpcScheduler scheduler = new DecayRpcScheduler(4, "ipc.8020", conf);
    MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    DecayRpcSchedulerDetailedMetrics metrics =
        scheduler.getDecayRpcSchedulerDetailedMetrics();

    assertNotNull(metricsSystem.getSource(metrics.getName()));

    scheduler.stop();

    assertNull(metricsSystem.getSource(metrics.getName()));
  }
}
