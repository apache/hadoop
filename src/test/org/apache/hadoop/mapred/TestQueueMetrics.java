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
package org.apache.hadoop.mapred;

import java.io.IOException;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.jasper.tagplugins.jstl.core.When;
import org.mockito.Mockito;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
public class TestQueueMetrics extends TestCase {

  QueueMetrics metrics = Mockito.mock(QueueMetrics.class);
  static int jobIdCounter = 0;
  static final String jtIdentifier = "queue_jt";

  private static JobID getJobId() {
    return new JobID(TestQueueMetrics.jtIdentifier, jobIdCounter++);
  }

  public void testDefaultSingleQueueMetrics() {
    String queueName = "single";
    TaskAttemptID taskAttemptID = Mockito.mock(TaskAttemptID.class);
    when(taskAttemptID.getJobID()).thenReturn(TestQueueMetrics.getJobId());

    QueueMetrics metrics = QueueMetrics.create(queueName, new Configuration());

    assertEquals(metrics.getQueueName(), "single");
    metrics.launchMap(taskAttemptID);
    checkMaps(metrics, 1, 0, 0, 0, -1, 0);
    metrics.addWaitingMaps(taskAttemptID.getJobID(), 5);
    metrics.launchMap(taskAttemptID);
    checkMaps(metrics, 2, 0, 0, 0, 3, 0);
    checkReduces(metrics, 0, 0, 0, 0, 0, 0);

    metrics.completeMap(taskAttemptID);
    metrics.failedMap(taskAttemptID);
    checkMaps(metrics, 2, 1, 1, 0, 4, 0);
    checkReduces(metrics, 0, 0, 0, 0, 0, 0);

    metrics.launchReduce(taskAttemptID);
    metrics.completeReduce(taskAttemptID);
    metrics.failedReduce(taskAttemptID);
    checkMaps(metrics, 2, 1, 1, 0, 4, 0);
    checkReduces(metrics, 1, 1, 1, 0, 0, 0);

    metrics.addWaitingMaps(null, 20);
    metrics.decWaitingMaps(null, 10);
    metrics.addWaitingReduces(null, 20);
    metrics.decWaitingReduces(null, 10);
    checkMaps(metrics, 2, 1, 1, 0, 14, 0);
    checkReduces(metrics, 1, 1, 1, 0, 10, 0);

    metrics.addReservedMapSlots(10);
    metrics.addReservedReduceSlots(10);
    checkMaps(metrics, 2, 1, 1, 0, 14, 10);
    checkReduces(metrics, 1, 1, 1, 0, 10, 10);
    metrics.decReservedReduceSlots(5);
    metrics.decReservedMapSlots(5);
    checkMaps(metrics, 2, 1, 1, 0, 14, 5);
    checkReduces(metrics, 1, 1, 1, 0, 10, 5);

    metrics.killedMap(taskAttemptID);
    metrics.killedReduce(taskAttemptID);
    checkMaps(metrics, 2, 1, 1, 1, 14, 5);
    checkReduces(metrics, 1, 1, 1, 1, 10, 5);
    checkJobs(metrics, 0, 0, 0, 0, 0, 0);  

    metrics.submitJob(null, null);
    metrics.completeJob(null, null);
    metrics.failedJob(null, null);
    metrics.killedJob(null, null);
    checkJobs(metrics, 1, 1, 1, 1, 0, 0);

    metrics.addPrepJob(null, null);
    metrics.addRunningJob(null, null);
    metrics.addPrepJob(null, null);
    metrics.addRunningJob(null, null);
    checkJobs(metrics, 1, 1, 1, 1, 2, 2);
    metrics.decPrepJob(null, null);
    metrics.decRunningJob(null, null);
    checkJobs(metrics, 1, 1, 1, 1, 1, 1);
    checkMaps(metrics, 2, 1, 1, 1, 14, 5);
    checkReduces(metrics, 1, 1, 1, 1, 10, 5);
  }

  public static void checkMaps(QueueMetrics metrics,
      int maps_launched, int maps_completed, int maps_failed, int maps_killed,
      int waiting_maps, int reserved_map_slots) {
    assertCounter("maps_launched", maps_launched, metrics);
    assertCounter("maps_completed", maps_completed, metrics);
    assertCounter("maps_failed", maps_failed, metrics);
    assertCounter("maps_killed", maps_killed, metrics);
    assertGauge("waiting_maps", waiting_maps, metrics);
    assertGauge("reserved_map_slots", reserved_map_slots, metrics);
  }

  public static void checkReduces(QueueMetrics metrics,
      int reduces_launched, int reduces_completed, int reduces_failed,
      int reduces_killed, int waiting_reduces, int reserved_reduce_slots) {
    assertCounter("reduces_launched", reduces_launched, metrics);
    assertCounter("reduces_completed", reduces_completed, metrics);
    assertCounter("reduces_failed", reduces_failed, metrics);
    assertCounter("reduces_killed", reduces_killed, metrics);
    assertGauge("waiting_reduces", waiting_reduces, metrics);
    assertGauge("reserved_reduce_slots", reserved_reduce_slots, metrics);
  }

  public static void checkJobs(QueueMetrics metrics, int jobs_submitted, int jobs_completed,
      int jobs_failed, int jobs_killed, int jobs_preparing, int jobs_running) {
    assertCounter("jobs_submitted", jobs_submitted, metrics);
    assertCounter("jobs_completed", jobs_completed, metrics);
    assertCounter("jobs_failed", jobs_failed, metrics);
    assertCounter("jobs_killed", jobs_killed, metrics);
    assertGauge("jobs_preparing", jobs_preparing, metrics);
    assertGauge("jobs_running", jobs_running, metrics);    
  }
}
