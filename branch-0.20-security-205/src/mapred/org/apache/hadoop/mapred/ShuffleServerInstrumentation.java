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
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

class ShuffleServerInstrumentation implements MetricsSource {
  final int ttWorkerThreads;
  final MetricsRegistry registry = new MetricsRegistry("shuffleOutput");
  private volatile int serverHandlerBusy = 0;
  final MetricMutableCounterLong outputBytes =
      registry.newCounter("shuffle_output_bytes", "", 0L);
  final MetricMutableCounterInt failedOutputs =
      registry.newCounter("shuffle_failed_outputs", "", 0);
  final MetricMutableCounterInt successOutputs =
      registry.newCounter("shuffle_success_outputs", "", 0);
  final MetricMutableCounterInt exceptionsCaught =
    registry.newCounter("shuffle_exceptions_caught", "", 0);

  ShuffleServerInstrumentation(TaskTracker tt) {
    ttWorkerThreads = tt.workerThreads;
    registry.setContext("mapred")
        .tag("sessionId", "session id", tt.getJobConf().getSessionId());
  }

  //@Override
  synchronized void serverHandlerBusy() {
    ++serverHandlerBusy;
  }

  //@Override
  synchronized void serverHandlerFree() {
    --serverHandlerBusy;
  }

  //@Override
  void outputBytes(long bytes) {
    outputBytes.incr(bytes);
  }

  //@Override
  void failedOutput() {
    failedOutputs.incr();
  }

  //@Override
  void successOutput() {
    successOutputs.incr();
  }

  //@Override
  void exceptionsCaught() {
    exceptionsCaught.incr();
  }


  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    MetricsRecordBuilder rb = builder.addRecord(registry.name());
    rb.addGauge("shuffle_handler_busy_percent", "", ttWorkerThreads == 0 ? 0
        : 100. * serverHandlerBusy / ttWorkerThreads);
    registry.snapshot(rb, all);
  }

  static ShuffleServerInstrumentation create(TaskTracker tt) {
    return create(tt, DefaultMetricsSystem.INSTANCE);
  }

  static ShuffleServerInstrumentation create(TaskTracker tt, MetricsSystem ms) {
    return ms.register("ShuffleServerMetrics", "Shuffle output metrics",
                      new ShuffleServerInstrumentation(tt));
  }
  
}
