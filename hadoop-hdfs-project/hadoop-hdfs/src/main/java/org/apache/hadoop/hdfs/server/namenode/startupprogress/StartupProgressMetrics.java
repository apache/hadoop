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
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * Links {@link StartupProgress} to a {@link MetricsSource} to expose its
 * information via JMX.
 */
@InterfaceAudience.Private
public class StartupProgressMetrics implements MetricsSource {

  private static final MetricsInfo STARTUP_PROGRESS_METRICS_INFO =
    info("StartupProgress", "NameNode startup progress");

  private final StartupProgress startupProgress;

  /**
   * Registers StartupProgressMetrics linked to the given StartupProgress.
   * 
   * @param prog StartupProgress to link
   */
  public static void register(StartupProgress prog) {
    new StartupProgressMetrics(prog);
  }

  /**
   * Creates a new StartupProgressMetrics registered with the metrics system.
   * 
   * @param startupProgress StartupProgress to link
   */
  public StartupProgressMetrics(StartupProgress startupProgress) {
    this.startupProgress = startupProgress;
    DefaultMetricsSystem.instance().register(
      STARTUP_PROGRESS_METRICS_INFO.name(),
      STARTUP_PROGRESS_METRICS_INFO.description(), this);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    StartupProgressView prog = startupProgress.createView();
    MetricsRecordBuilder builder = collector.addRecord(
      STARTUP_PROGRESS_METRICS_INFO);

    builder.addCounter(info("ElapsedTime", "overall elapsed time"),
      prog.getElapsedTime());
    builder.addGauge(info("PercentComplete", "overall percent complete"),
      prog.getPercentComplete());

    for (Phase phase: prog.getPhases()) {
      addCounter(builder, phase, "Count", " count", prog.getCount(phase));
      addCounter(builder, phase, "ElapsedTime", " elapsed time",
        prog.getElapsedTime(phase));
      addCounter(builder, phase, "Total", " total", prog.getTotal(phase));
      addGauge(builder, phase, "PercentComplete", " percent complete",
        prog.getPercentComplete(phase));
    }
  }

  /**
   * Adds a counter with a name built by using the specified phase's name as
   * prefix and then appending the specified suffix.
   * 
   * @param builder MetricsRecordBuilder to receive counter
   * @param phase Phase to add
   * @param nameSuffix String suffix of metric name
   * @param descSuffix String suffix of metric description
   * @param value long counter value
   */
  private static void addCounter(MetricsRecordBuilder builder, Phase phase,
      String nameSuffix, String descSuffix, long value) {
    MetricsInfo metricsInfo = info(phase.getName() + nameSuffix,
      phase.getDescription() + descSuffix);
    builder.addCounter(metricsInfo, value);
  }

  /**
   * Adds a gauge with a name built by using the specified phase's name as prefix
   * and then appending the specified suffix.
   * 
   * @param builder MetricsRecordBuilder to receive counter
   * @param phase Phase to add
   * @param nameSuffix String suffix of metric name
   * @param descSuffix String suffix of metric description
   * @param value float gauge value
   */
  private static void addGauge(MetricsRecordBuilder builder, Phase phase,
      String nameSuffix, String descSuffix, float value) {
    MetricsInfo metricsInfo = info(phase.getName() + nameSuffix,
      phase.getDescription() + descSuffix);
    builder.addGauge(metricsInfo, value);
  }
}
