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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestQueueMetrics;
import org.junit.Before;
import org.junit.Test;

/**
 * The test class for {@link FSQueueMetrics}.
 */
public class TestFSQueueMetrics {
  private static final Configuration CONF = new Configuration();

  private MetricsSystem ms;

  @Before public void setUp() {
    ms = new MetricsSystemImpl();
    QueueMetrics.clearQueueMetrics();
  }

  /**
   * Test if the metric scheduling policy is set correctly.
   */
  @Test
  public void testSchedulingPolicy() {
    String queueName = "single";

    FSQueueMetrics metrics = FSQueueMetrics.forQueue(ms, queueName, null, false,
        CONF);
    metrics.setSchedulingPolicy("drf");
    checkSchedulingPolicy(queueName, "drf");

    // test resetting the scheduling policy
    metrics.setSchedulingPolicy("fair");
    checkSchedulingPolicy(queueName, "fair");
  }

  private void checkSchedulingPolicy(String queueName, String policy) {
    MetricsSource queueSource = TestQueueMetrics.queueSource(ms, queueName);
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    queueSource.getMetrics(collector, true);
    MetricsRecords.assertTag(collector.getRecords().get(0), "SchedulingPolicy",
        policy);
  }
}
