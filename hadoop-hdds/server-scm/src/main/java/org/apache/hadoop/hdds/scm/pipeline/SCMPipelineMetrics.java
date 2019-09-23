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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class maintains Pipeline related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "SCM PipelineManager Metrics", context = "ozone")
public final class SCMPipelineMetrics implements MetricsSource {

  private static final String SOURCE_NAME =
      SCMPipelineMetrics.class.getSimpleName();

  private MetricsRegistry registry;

  private @Metric MutableCounterLong numPipelineCreated;
  private @Metric MutableCounterLong numPipelineCreationFailed;
  private @Metric MutableCounterLong numPipelineDestroyed;
  private @Metric MutableCounterLong numPipelineDestroyFailed;
  private @Metric MutableCounterLong numPipelineReportProcessed;
  private @Metric MutableCounterLong numPipelineReportProcessingFailed;
  private Map<PipelineID, MutableCounterLong> numBlocksAllocated;

  /** Private constructor. */
  private SCMPipelineMetrics() {
    this.registry = new MetricsRegistry(SOURCE_NAME);
    numBlocksAllocated = new ConcurrentHashMap<>();
  }

  /**
   * Create and returns SCMPipelineMetrics instance.
   *
   * @return SCMPipelineMetrics
   */
  public static SCMPipelineMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "SCM PipelineManager Metrics",
        new SCMPipelineMetrics());
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Override
  @SuppressWarnings("SuspiciousMethodCalls")
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);
    numPipelineCreated.snapshot(recordBuilder, true);
    numPipelineCreationFailed.snapshot(recordBuilder, true);
    numPipelineDestroyed.snapshot(recordBuilder, true);
    numPipelineDestroyFailed.snapshot(recordBuilder, true);
    numPipelineReportProcessed.snapshot(recordBuilder, true);
    numPipelineReportProcessingFailed.snapshot(recordBuilder, true);
    numBlocksAllocated
        .forEach((pid, metric) -> metric.snapshot(recordBuilder, true));
  }

  void createPerPipelineMetrics(Pipeline pipeline) {
    numBlocksAllocated.put(pipeline.getId(), new MutableCounterLong(Interns
        .info(getBlockAllocationMetricName(pipeline),
            "Number of blocks allocated in pipeline " + pipeline.getId()), 0L));
  }

  public static String getBlockAllocationMetricName(Pipeline pipeline) {
    return "NumBlocksAllocated-" + pipeline.getType() + "-" + pipeline
        .getFactor() + "-" + pipeline.getId().getId();
  }

  void removePipelineMetrics(PipelineID pipelineID) {
    numBlocksAllocated.remove(pipelineID);
  }

  /**
   * Increments number of blocks allocated for the pipeline.
   */
  void incNumBlocksAllocated(PipelineID pipelineID) {
    Optional.of(numBlocksAllocated.get(pipelineID)).ifPresent(
        MutableCounterLong::incr);
  }

  /**
   * Increments number of successful pipeline creation count.
   */
  void incNumPipelineCreated() {
    numPipelineCreated.incr();
  }

  /**
   * Increments number of failed pipeline creation count.
   */
  void incNumPipelineCreationFailed() {
    numPipelineCreationFailed.incr();
  }

  /**
   * Increments number of successful pipeline destroy count.
   */
  void incNumPipelineDestroyed() {
    numPipelineDestroyed.incr();
  }

  /**
   * Increments number of failed pipeline destroy count.
   */
  void incNumPipelineDestroyFailed() {
    numPipelineDestroyFailed.incr();
  }

  /**
   * Increments number of pipeline report processed count.
   */
  void incNumPipelineReportProcessed() {
    numPipelineReportProcessed.incr();
  }

  /**
   * Increments number of pipeline report processing failed count.
   */
  void incNumPipelineReportProcessingFailed() {
    numPipelineReportProcessingFailed.incr();
  }
}
