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

import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.source.JvmMetricsSource;

/**
 * Instrumentation for metrics v2
 */
@SuppressWarnings("deprecation")
public class TaskTrackerMetricsSource extends TaskTrackerInstrumentation
                                  implements MetricsSource {

  final MetricsRegistry registry = new MetricsRegistry("tasktracker");
  final MetricMutableGaugeInt mapsRunning =
      registry.newGauge("maps_running", "", 0);
  final MetricMutableGaugeInt redsRunning =
      registry.newGauge("reduces_running", "", 0);
  final MetricMutableGaugeInt mapSlots =
      registry.newGauge("mapTaskSlots", "", 0);
  final MetricMutableGaugeInt redSlots =
      registry.newGauge("reduceTaskSlots", "", 0);
  final MetricMutableCounterInt completedTasks =
      registry.newCounter("tasks_completed", "", 0);
  final MetricMutableCounterInt timedoutTasks =
      registry.newCounter("tasks_failed_timeout", "", 0);
  final MetricMutableCounterInt pingFailedTasks =
      registry.newCounter("tasks_failed_ping", "", 0);

  public TaskTrackerMetricsSource(TaskTracker tt) {
    super(tt);
    String sessionId = tt.getJobConf().getSessionId();
    JvmMetricsSource.create("TaskTracker", sessionId);
    registry.setContext("mapred").tag("sessionId", "", sessionId);
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    mapsRunning.set(tt.mapTotal);
    redsRunning.set(tt.reduceTotal);
    mapSlots.set(tt.getMaxCurrentMapTasks());
    redSlots.set(tt.getMaxCurrentReduceTasks());
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

  @Override
  public void completeTask(TaskAttemptID t) {
    completedTasks.incr();
  }

  @Override
  public void timedoutTask(TaskAttemptID t) {
    timedoutTasks.incr();
  }

  @Override
  public void taskFailedPing(TaskAttemptID t) {
    pingFailedTasks.incr();
  }

}
