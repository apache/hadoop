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
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
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

  public static final String PMEM_LIMIT_METRIC_NAME = "pMemLimitMBs";
  public static final String VMEM_LIMIT_METRIC_NAME = "vMemLimitMBs";
  public static final String VCORE_LIMIT_METRIC_NAME = "vCoreLimit";
  public static final String PMEM_USAGE_METRIC_NAME = "pMemUsageMBs";
  private static final String PHY_CPU_USAGE_METRIC_NAME = "pCpuUsagePercent";

  // Use a multiplier of 1000 to avoid losing too much precision when
  // converting to integers
  private static final String VCORE_USAGE_METRIC_NAME = "milliVcoreUsage";

  @Metric
  public MutableStat pMemMBsStat;

  // This tracks overall CPU percentage of the machine in terms of percentage
  // of 1 core similar to top
  // Thus if you use 2 cores completely out of 4 available cores this value
  // will be 200
  @Metric
  public MutableStat cpuCoreUsagePercent;

  @Metric
  public MutableStat milliVcoresUsed;

  @Metric
  public MutableGaugeInt pMemLimitMbs;

  @Metric
  public MutableGaugeInt vMemLimitMbs;

  @Metric
  public MutableGaugeInt cpuVcoreLimit;

  static final MetricsInfo RECORD_INFO =
      info("ContainerResource", "Resource limit and usage by container");

  public static final MetricsInfo PROCESSID_INFO =
      info("ContainerPid", "Container Process Id");

  final MetricsInfo recordInfo;
  final MetricsRegistry registry;
  final ContainerId containerId;
  final MetricsSystem metricsSystem;

  // Metrics publishing status
  private long flushPeriodMs;
  private boolean flushOnPeriod = false; // true if period elapsed
  private boolean finished = false; // true if container finished
  private boolean unregister = false; // unregister
  private long unregisterDelayMs;
  private Timer timer; // lazily initialized

  /**
   * Simple metrics cache to help prevent re-registrations.
   */
  protected final static Map<ContainerId, ContainerMetrics>
      usageMetrics = new HashMap<>();
  // Create a timer to unregister container metrics,
  // whose associated thread run as a daemon.
  private final static Timer unregisterContainerMetricsTimer =
      new Timer("Container metrics unregistration", true);

  ContainerMetrics(
      MetricsSystem ms, ContainerId containerId, long flushPeriodMs,
      long delayMs) {
    this.recordInfo =
        info(sourceName(containerId), RECORD_INFO.description());
    this.registry = new MetricsRegistry(recordInfo);
    this.metricsSystem = ms;
    this.containerId = containerId;
    this.flushPeriodMs = flushPeriodMs;
    this.unregisterDelayMs = delayMs < 0 ? 0 : delayMs;
    scheduleTimerTaskIfRequired();

    this.pMemMBsStat = registry.newStat(
        PMEM_USAGE_METRIC_NAME, "Physical memory stats", "Usage", "MBs", true);
    this.cpuCoreUsagePercent = registry.newStat(
        PHY_CPU_USAGE_METRIC_NAME, "Physical Cpu core percent usage stats",
        "Usage", "Percents", true);
    this.milliVcoresUsed = registry.newStat(
        VCORE_USAGE_METRIC_NAME, "1000 times Vcore usage", "Usage",
        "MilliVcores", true);
    this.pMemLimitMbs = registry.newGauge(
        PMEM_LIMIT_METRIC_NAME, "Physical memory limit in MBs", 0);
    this.vMemLimitMbs = registry.newGauge(
        VMEM_LIMIT_METRIC_NAME, "Virtual memory limit in MBs", 0);
    this.cpuVcoreLimit = registry.newGauge(
        VCORE_LIMIT_METRIC_NAME, "CPU limit in number of vcores", 0);
  }

  ContainerMetrics tag(MetricsInfo info, ContainerId containerId) {
    registry.tag(info, containerId.toString());
    return this;
  }

  static String sourceName(ContainerId containerId) {
    return RECORD_INFO.name() + "_" + containerId.toString();
  }

  public static ContainerMetrics forContainer(
      ContainerId containerId, long flushPeriodMs, long delayMs) {
    return forContainer(
        DefaultMetricsSystem.instance(), containerId, flushPeriodMs, delayMs);
  }

  synchronized static ContainerMetrics forContainer(
      MetricsSystem ms, ContainerId containerId, long flushPeriodMs,
      long delayMs) {
    ContainerMetrics metrics = usageMetrics.get(containerId);
    if (metrics == null) {
      metrics = new ContainerMetrics(ms, containerId, flushPeriodMs,
          delayMs).tag(RECORD_INFO, containerId);

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

  synchronized static void unregisterContainerMetrics(ContainerMetrics cm) {
    cm.metricsSystem.unregisterSource(cm.recordInfo.name());
    usageMetrics.remove(cm.containerId);
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    //Container goes through registered -> finished -> unregistered.
    if (unregister) {
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
    scheduleTimerTaskForUnregistration();
  }

  public void recordMemoryUsage(int memoryMBs) {
    if (memoryMBs >= 0) {
      this.pMemMBsStat.add(memoryMBs);
    }
  }

  public void recordCpuUsage(
      int totalPhysicalCpuPercent, int milliVcoresUsed) {
    if (totalPhysicalCpuPercent >=0) {
      this.cpuCoreUsagePercent.add(totalPhysicalCpuPercent);
    }
    if (milliVcoresUsed >= 0) {
      this.milliVcoresUsed.add(milliVcoresUsed);
    }
  }

  public void recordProcessId(String processId) {
    registry.tag(PROCESSID_INFO, processId);
  }

  public void recordResourceLimit(int vmemLimit, int pmemLimit, int cpuVcores) {
    this.vMemLimitMbs.set(vmemLimit);
    this.pMemLimitMbs.set(pmemLimit);
    this.cpuVcoreLimit.set(cpuVcores);
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

  private void scheduleTimerTaskForUnregistration() {
    TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        ContainerMetrics.unregisterContainerMetrics(ContainerMetrics.this);
      }
    };
    unregisterContainerMetricsTimer.schedule(timerTask, unregisterDelayMs);
  }
}
