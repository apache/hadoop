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

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import static org.apache.hadoop.metrics2.impl.MsInfo.*;

@Metrics(name="TaskTrackerMetrics", context="mapred")
class TaskTrackerMetricsInst extends TaskTrackerInstrumentation {

  final MetricsRegistry registry = new MetricsRegistry("tasktracker");
  @Metric MutableCounterInt tasksCompleted;
  @Metric MutableCounterInt tasksFailedTimedout;
  @Metric MutableCounterInt tasksFailedPing;
    
  public TaskTrackerMetricsInst(TaskTracker t) {
    super(t);
    String sessionId = tt.getJobConf().getSessionId();
    registry.tag(SessionId, sessionId);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics.create("TaskTracker", sessionId, ms);
    ms.register(this);
  }

  @Override
  public void completeTask(TaskAttemptID t) {
    tasksCompleted.incr();
  }

  @Override
  public void timedoutTask(TaskAttemptID t) {
    tasksFailedTimedout.incr();
  }

  @Override
  public void taskFailedPing(TaskAttemptID t) {
    tasksFailedPing.incr();
  }

  @Metric int getMapsRunning() { return tt.mapTotal; }
  @Metric int getReducesRunning() { return tt.reduceTotal; }
  @Metric int getMapTaskSlots() { return tt.getMaxCurrentMapTasks(); }
  @Metric int getReduceTaskSlots() { return tt.getMaxCurrentReduceTasks(); }
}
