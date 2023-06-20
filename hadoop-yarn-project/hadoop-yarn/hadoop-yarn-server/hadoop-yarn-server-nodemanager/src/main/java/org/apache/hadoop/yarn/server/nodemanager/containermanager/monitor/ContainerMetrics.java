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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
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
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.util.Quantile;
import org.apache.hadoop.metrics2.util.QuantileEstimator;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@InterfaceAudience.Private
@Metrics(context="container")
public class ContainerMetrics implements MetricsSource {

  public static final String PMEM_LIMIT_METRIC_NAME = "pMemLimitMBs";
  public static final String VMEM_LIMIT_METRIC_NAME = "vMemLimitMBs";
  public static final String VCORE_LIMIT_METRIC_NAME = "vCoreLimit";
  public static final String PMEM_USAGE_METRIC_NAME = "pMemUsageMBs";
  public static final String PMEM_USAGE_QUANTILES_NAME = "pMemUsageMBHistogram";
  public static final String LAUNCH_DURATION_METRIC_NAME = "launchDurationMs";
  public static final String LOCALIZATION_DURATION_METRIC_NAME =
      "localizationDurationMs";
  private static final String PHY_CPU_USAGE_METRIC_NAME = "pCpuUsagePercent";
  private static final String PHY_CPU_USAGE_QUANTILES_NAME =
      "pCpuUsagePercentHistogram";

  // Use a multiplier of 1000 to avoid losing too much precision when
  // converting to integers
  private static final String VCORE_USAGE_METRIC_NAME = "milliVcoreUsage";

  @Metric
  public MutableStat pMemMBsStat;

  @Metric
  public  MutableQuantiles pMemMBQuantiles;

  // This tracks overall CPU percentage of the machine in terms of percentage
  // of 1 core similar to top
  // Thus if you use 2 cores completely out of 4 available cores this value
  // will be 200
  @Metric
  public MutableStat cpuCoreUsagePercent;

  @Metric
  public  MutableQuantiles cpuCoreUsagePercentQuantiles;

  @Metric
  public MutableStat milliVcoresUsed;

  @Metric
  public MutableGaugeInt pMemLimitMbs;

  @Metric
  public MutableGaugeInt vMemLimitMbs;

  @Metric
  public MutableGaugeInt cpuVcoreLimit;

  @Metric
  public MutableGaugeLong launchDurationMs;

  @Metric
  public MutableGaugeLong localizationDurationMs;

  @Metric
  public MutableGaugeLong startTime;

  @Metric
  public MutableGaugeLong finishTime;

  @Metric
  public MutableGaugeInt exitCode;

  static final MetricsInfo RECORD_INFO =
      info("ContainerResource", "Resource limit and usage by container");

  public static final MetricsInfo PROCESSID_INFO =
      info("ContainerPid", "Container Process Id");

  final MetricsInfo recordInfo;
  final MetricsRegistry registry;
  final ContainerId containerId;
  final MetricsSystem metricsSystem;

  // Metrics publishing status
  private final long flushPeriodMs;
  private final long unregisterDelayMs;
  private boolean flushOnPeriod = false; // true if period elapsed
  private boolean finished = false; // true if container finished
  private Timer timer; // lazily initialized

  /**
   * Simple metrics cache to help prevent re-registrations.
   */
  private final static Map<ContainerId, ContainerMetrics>
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
    this.pMemMBQuantiles = registry
        .newQuantiles(PMEM_USAGE_QUANTILES_NAME, "Physical memory quantiles",
            "Usage", "MBs", 1);
    ContainerMetricsQuantiles memEstimator =
        new ContainerMetricsQuantiles(MutableQuantiles.QUANTILES);
    pMemMBQuantiles.setEstimator(memEstimator);

    this.cpuCoreUsagePercent = registry.newStat(
        PHY_CPU_USAGE_METRIC_NAME, "Physical Cpu core percent usage stats",
        "Usage", "Percents", true);
    this.cpuCoreUsagePercentQuantiles = registry
        .newQuantiles(PHY_CPU_USAGE_QUANTILES_NAME,
            "Physical Cpu core percent usage quantiles", "Usage", "Percents",
            1);
    ContainerMetricsQuantiles cpuEstimator =
        new ContainerMetricsQuantiles(MutableQuantiles.QUANTILES);
    cpuCoreUsagePercentQuantiles.setEstimator(cpuEstimator);
    this.milliVcoresUsed = registry.newStat(
        VCORE_USAGE_METRIC_NAME, "1000 times Vcore usage", "Usage",
        "MilliVcores", true);
    this.pMemLimitMbs = registry.newGauge(
        PMEM_LIMIT_METRIC_NAME, "Physical memory limit in MBs", 0);
    this.vMemLimitMbs = registry.newGauge(
        VMEM_LIMIT_METRIC_NAME, "Virtual memory limit in MBs", 0);
    this.cpuVcoreLimit = registry.newGauge(
        VCORE_LIMIT_METRIC_NAME, "CPU limit in number of vcores", 0);
    this.launchDurationMs = registry.newGauge(
        LAUNCH_DURATION_METRIC_NAME, "Launch duration in MS", 0L);
    this.localizationDurationMs = registry.newGauge(
        LOCALIZATION_DURATION_METRIC_NAME, "Localization duration in MS", 0L);
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

  public synchronized static ContainerMetrics getContainerMetrics(
      ContainerId containerId) {
    // could be null
    return usageMetrics.get(containerId);
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
    if (finished || flushOnPeriod) {
      registry.snapshot(collector.addRecord(registry.info()), all);
    }

    if (!finished && flushOnPeriod) {
      flushOnPeriod = false;
      scheduleTimerTaskIfRequired();
    }
  }

  public synchronized void finished(boolean unregisterWithoutDelay) {
    if (!finished) {
      this.finished = true;
      if (timer != null) {
        timer.cancel();
        timer = null;
      }
      if (!unregisterWithoutDelay) {
        scheduleTimerTaskForUnregistration();
      } else {
        ContainerMetrics.unregisterContainerMetrics(ContainerMetrics.this);
      }
      this.pMemMBQuantiles.stop();
      this.cpuCoreUsagePercentQuantiles.stop();
    }
  }

  public void recordMemoryUsage(int memoryMBs) {
    if (memoryMBs >= 0) {
      this.pMemMBsStat.add(memoryMBs);
      this.pMemMBQuantiles.add(memoryMBs);
    }
  }

  public void recordCpuUsage(
      int totalPhysicalCpuPercent, int milliVcoresUsed) {
    if (totalPhysicalCpuPercent >=0) {
      this.cpuCoreUsagePercent.add(totalPhysicalCpuPercent);
      this.cpuCoreUsagePercentQuantiles.add(totalPhysicalCpuPercent);
    }
    if (milliVcoresUsed >= 0) {
      this.milliVcoresUsed.add(milliVcoresUsed);
    }
  }

  public void recordProcessId(String processId) {
    registry.tag(PROCESSID_INFO, processId, true);
  }

  public void recordResourceLimit(int vmemLimit, int pmemLimit, int cpuVcores) {
    this.vMemLimitMbs.set(vmemLimit);
    this.pMemLimitMbs.set(pmemLimit);
    this.cpuVcoreLimit.set(cpuVcores);
  }

  public void recordStateChangeDurations(long launchDuration,
      long localizationDuration) {
    this.launchDurationMs.set(launchDuration);
    this.localizationDurationMs.set(localizationDuration);
  }

  public void recordStartTime(long startTime) {
    this.startTime.set(startTime);
  }

  public void recordFinishTimeAndExitCode(long finishTime, int exitCode) {
    this.finishTime.set(finishTime);
    this.exitCode.set(exitCode);
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

  public static class ContainerMetricsQuantiles implements QuantileEstimator {

    private final Histogram histogram = new Histogram(new UniformReservoir());

    private Quantile[] quantiles;

    ContainerMetricsQuantiles(Quantile[] q) {
      quantiles = q;
    }

    @Override
    public synchronized void insert(long value) {
      histogram.update(value);
    }

    @Override
    synchronized public long getCount() {
      return histogram.getCount();
    }

    @Override
    synchronized public void clear() {
      // don't do anything because we want metrics over the lifetime of the
      // container
    }

    @Override
    public synchronized Map<Quantile, Long> snapshot() {
      Snapshot snapshot = histogram.getSnapshot();
      Map<Quantile, Long> values = new TreeMap<>();
      for (Quantile quantile : quantiles) {
        values.put(quantile, (long) snapshot.getValue(quantile.quantile));
      }
      return values;
    }
  }
}
