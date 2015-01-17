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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@InterfaceAudience.Private
@Metrics(context="container")
public class ContainerMetrics implements MetricsSource {

  @Metric
  public MutableStat pMemMBsStat;

  static final MetricsInfo RECORD_INFO =
      info("ContainerUsage", "Resource usage by container");

  final MetricsInfo recordInfo;
  final MetricsRegistry registry;
  final ContainerId containerId;
  final MetricsSystem metricsSystem;

  // Metrics publishing status
  private long flushPeriodMs;
  private boolean flushOnPeriod = false; // true if period elapsed
  private boolean finished = false; // true if container finished
  private boolean unregister = false; // unregister
  private Timer timer; // lazily initialized

  /**
   * Simple metrics cache to help prevent re-registrations.
   */
  protected final static Map<ContainerId, ContainerMetrics>
      usageMetrics = new HashMap<>();

  ContainerMetrics(
      MetricsSystem ms, ContainerId containerId, long flushPeriodMs) {
    this.recordInfo =
        info(sourceName(containerId), RECORD_INFO.description());
    this.registry = new MetricsRegistry(recordInfo);
    this.metricsSystem = ms;
    this.containerId = containerId;
    this.flushPeriodMs = flushPeriodMs;
    scheduleTimerTaskIfRequired();

    this.pMemMBsStat = registry.newStat(
        "pMem", "Physical memory stats", "Usage", "MBs", true);
  }

  ContainerMetrics tag(MetricsInfo info, ContainerId containerId) {
    registry.tag(info, containerId.toString());
    return this;
  }

  static String sourceName(ContainerId containerId) {
    return RECORD_INFO.name() + "_" + containerId.toString();
  }

  public static ContainerMetrics forContainer(ContainerId containerId) {
    return forContainer(containerId, -1L);
  }

  public static ContainerMetrics forContainer(
      ContainerId containerId, long flushPeriodMs) {
    return forContainer(
        DefaultMetricsSystem.instance(), containerId, flushPeriodMs);
  }

  synchronized static ContainerMetrics forContainer(
      MetricsSystem ms, ContainerId containerId, long flushPeriodMs) {
    ContainerMetrics metrics = usageMetrics.get(containerId);
    if (metrics == null) {
      metrics = new ContainerMetrics(
          ms, containerId, flushPeriodMs).tag(RECORD_INFO, containerId);

      // Register with the MetricsSystems
      if (ms != null) {
        metrics =
            ms.register(sourceName(containerId),
                "Metrics for container: " + containerId, metrics);
      }
      usageMetrics.put(containerId, metrics);
    }

    return metrics;
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    //Container goes through registered -> finished -> unregistered.
    if (unregister) {
      metricsSystem.unregisterSource(recordInfo.name());
      usageMetrics.remove(containerId);
      return;
    }

    if (finished || flushOnPeriod) {
      registry.snapshot(collector.addRecord(registry.info()), all);
    }

    if (finished) {
      this.unregister = true;
    } else if (flushOnPeriod) {
      flushOnPeriod = false;
      scheduleTimerTaskIfRequired();
    }
  }

  public synchronized void finished() {
    this.finished = true;
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }

  public void recordMemoryUsage(int memoryMBs) {
    this.pMemMBsStat.add(memoryMBs);
  }

  private synchronized void scheduleTimerTaskIfRequired() {
    if (flushPeriodMs > 0) {
      // Lazily initialize timer
      if (timer == null) {
        this.timer = new Timer("Metrics flush checker", true);
      }
      TimerTask timerTask = new TimerTask() {
        @Override
        public void run() {
          synchronized (ContainerMetrics.this) {
            if (!finished) {
              flushOnPeriod = true;
            }
          }
        }
      };
      timer.schedule(timerTask, flushPeriodMs);
    }
  }
}
