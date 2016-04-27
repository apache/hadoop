/*
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

package org.apache.hadoop.metrics2.source;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.log.metrics.EventCounter;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import static org.apache.hadoop.metrics2.source.JvmMetricsInfo.*;
import static org.apache.hadoop.metrics2.impl.MsInfo.*;
import org.apache.hadoop.util.JvmPauseMonitor;

/**
 * JVM and logging related metrics.
 * Mostly used by various servers as a part of the metrics they export.
 */
@InterfaceAudience.Private
public class JvmMetrics implements MetricsSource {
  enum Singleton {
    INSTANCE;

    JvmMetrics impl;

    synchronized JvmMetrics init(String processName, String sessionId) {
      if (impl == null) {
        impl = create(processName, sessionId, DefaultMetricsSystem.instance());
      }
      return impl;
    }
  }

  static final float M = 1024*1024;
  static public final float MEMORY_MAX_UNLIMITED_MB = -1;

  final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
  final List<GarbageCollectorMXBean> gcBeans =
      ManagementFactory.getGarbageCollectorMXBeans();
  final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  final String processName, sessionId;
  private JvmPauseMonitor pauseMonitor = null;
  final ConcurrentHashMap<String, MetricsInfo[]> gcInfoCache =
      new ConcurrentHashMap<String, MetricsInfo[]>();

  JvmMetrics(String processName, String sessionId) {
    this.processName = processName;
    this.sessionId = sessionId;
  }

  public void setPauseMonitor(final JvmPauseMonitor pauseMonitor) {
    this.pauseMonitor = pauseMonitor;
  }

  public static JvmMetrics create(String processName, String sessionId,
                                  MetricsSystem ms) {
    return ms.register(JvmMetrics.name(), JvmMetrics.description(),
                       new JvmMetrics(processName, sessionId));
  }

  public static void reattach(MetricsSystem ms, JvmMetrics jvmMetrics) {
    ms.register(JvmMetrics.name(), JvmMetrics.description(), jvmMetrics);
  }

  public static JvmMetrics initSingleton(String processName, String sessionId) {
    return Singleton.INSTANCE.init(processName, sessionId);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(JvmMetrics)
        .setContext("jvm").tag(ProcessName, processName)
        .tag(SessionId, sessionId);
    getMemoryUsage(rb);
    getGcUsage(rb);
    getThreadUsage(rb);
    getEventCounters(rb);
  }

  private void getMemoryUsage(MetricsRecordBuilder rb) {
    MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage();
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    Runtime runtime = Runtime.getRuntime();
    rb.addGauge(MemNonHeapUsedM, memNonHeap.getUsed() / M)
      .addGauge(MemNonHeapCommittedM, memNonHeap.getCommitted() / M)
      .addGauge(MemNonHeapMaxM, calculateMaxMemoryUsage(memNonHeap))
      .addGauge(MemHeapUsedM, memHeap.getUsed() / M)
      .addGauge(MemHeapCommittedM, memHeap.getCommitted() / M)
      .addGauge(MemHeapMaxM, calculateMaxMemoryUsage(memHeap))
      .addGauge(MemMaxM, runtime.maxMemory() / M);
  }

  private float calculateMaxMemoryUsage(MemoryUsage memHeap) {
    long max =  memHeap.getMax() ;

     if (max == -1) {
       return MEMORY_MAX_UNLIMITED_MB;
     }

    return max / M;
  }

  private void getGcUsage(MetricsRecordBuilder rb) {
    long count = 0;
    long timeMillis = 0;
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      long c = gcBean.getCollectionCount();
      long t = gcBean.getCollectionTime();
      MetricsInfo[] gcInfo = getGcInfo(gcBean.getName());
      rb.addCounter(gcInfo[0], c).addCounter(gcInfo[1], t);
      count += c;
      timeMillis += t;
    }
    rb.addCounter(GcCount, count)
      .addCounter(GcTimeMillis, timeMillis);
    
    if (pauseMonitor != null) {
      rb.addCounter(GcNumWarnThresholdExceeded,
          pauseMonitor.getNumGcWarnThresholdExceeded());
      rb.addCounter(GcNumInfoThresholdExceeded,
          pauseMonitor.getNumGcInfoThresholdExceeded());
      rb.addCounter(GcTotalExtraSleepTime,
          pauseMonitor.getTotalGcExtraSleepTime());
    }
  }

  private MetricsInfo[] getGcInfo(String gcName) {
    MetricsInfo[] gcInfo = gcInfoCache.get(gcName);
    if (gcInfo == null) {
      gcInfo = new MetricsInfo[2];
      gcInfo[0] = Interns.info("GcCount" + gcName, "GC Count for " + gcName);
      gcInfo[1] = Interns
          .info("GcTimeMillis" + gcName, "GC Time for " + gcName);
      MetricsInfo[] previousGcInfo = gcInfoCache.putIfAbsent(gcName, gcInfo);
      if (previousGcInfo != null) {
        return previousGcInfo;
      }
    }
    return gcInfo;
  }

  private void getThreadUsage(MetricsRecordBuilder rb) {
    int threadsNew = 0;
    int threadsRunnable = 0;
    int threadsBlocked = 0;
    int threadsWaiting = 0;
    int threadsTimedWaiting = 0;
    int threadsTerminated = 0;
    long threadIds[] = threadMXBean.getAllThreadIds();
    for (ThreadInfo threadInfo : threadMXBean.getThreadInfo(threadIds, 0)) {
      if (threadInfo == null) continue; // race protection
      switch (threadInfo.getThreadState()) {
        case NEW:           threadsNew++;           break;
        case RUNNABLE:      threadsRunnable++;      break;
        case BLOCKED:       threadsBlocked++;       break;
        case WAITING:       threadsWaiting++;       break;
        case TIMED_WAITING: threadsTimedWaiting++;  break;
        case TERMINATED:    threadsTerminated++;    break;
      }
    }
    rb.addGauge(ThreadsNew, threadsNew)
      .addGauge(ThreadsRunnable, threadsRunnable)
      .addGauge(ThreadsBlocked, threadsBlocked)
      .addGauge(ThreadsWaiting, threadsWaiting)
      .addGauge(ThreadsTimedWaiting, threadsTimedWaiting)
      .addGauge(ThreadsTerminated, threadsTerminated);
  }

  private void getEventCounters(MetricsRecordBuilder rb) {
    rb.addCounter(LogFatal, EventCounter.getFatal())
      .addCounter(LogError, EventCounter.getError())
      .addCounter(LogWarn, EventCounter.getWarn())
      .addCounter(LogInfo, EventCounter.getInfo());
  }
}
