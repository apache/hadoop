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
package org.apache.hadoop.metrics2.source;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import static java.lang.Thread.State.*;
import java.lang.management.GarbageCollectorMXBean;
import java.util.List;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import org.apache.hadoop.log.EventCounter;

/**
 *
 */
public class JvmMetricsSource implements MetricsSource {

  private static final float M = 1024*1024;

  static final String SOURCE_NAME = "jvm";
  static final String CONTEXT = "jvm";
  static final String RECORD_NAME = "metrics";
  static final String SOURCE_DESC = "JVM metrics etc.";

  // tags
  static final String PROCESSNAME_KEY = "processName";
  static final String PROCESSNAME_DESC = "Process name";
  static final String SESSIONID_KEY = "sessionId";
  static final String SESSIONID_DESC = "Session ID";
  private final String processName, sessionId;

  // metrics
  static final String NONHEAP_USED_KEY = "memNonHeapUsedM";
  static final String NONHEAP_USED_DESC = "Non-heap memory used in MB";
  static final String NONHEAP_COMMITTED_KEY = "memNonHeapCommittedM";
  static final String NONHEAP_COMMITTED_DESC = "Non-heap committed in MB";
  static final String HEAP_USED_KEY = "memHeapUsedM";
  static final String HEAP_USED_DESC = "Heap memory used in MB";
  static final String HEAP_COMMITTED_KEY = "memHeapCommittedM";
  static final String HEAP_COMMITTED_DESC = "Heap memory committed in MB";
  static final String GC_COUNT_KEY = "gcCount";
  static final String GC_COUNT_DESC = "Total GC count";
  static final String GC_TIME_KEY = "gcTimeMillis";
  static final String GC_TIME_DESC = "Total GC time in milliseconds";
  static final String THREADS_NEW_KEY = "threadsNew";
  static final String THREADS_NEW_DESC = "Number of new threads";
  static final String THREADS_RUNNABLE_KEY = "threadsRunnable";
  static final String THREADS_RUNNABLE_DESC = "Number of runnable threads";
  static final String THREADS_BLOCKED_KEY = "threadsBlocked";
  static final String THREADS_BLOCKED_DESC = "Number of blocked threads";
  static final String THREADS_WAITING_KEY = "threadsWaiting";
  static final String THREADS_WAITING_DESC = "Number of waiting threads";
  static final String THREADS_TIMEDWAITING_KEY = "threadsTimedWaiting";
  static final String THREADS_TIMEDWAITING_DESC =
      "Number of timed waiting threads";
  static final String THREADS_TERMINATED_KEY = "threadsTerminated";
  static final String THREADS_TERMINATED_DESC = "Number of terminated threads";
  static final String LOG_FATAL_KEY = "logFatal";
  static final String LOG_FATAL_DESC = "Total number of fatal log events";
  static final String LOG_ERROR_KEY = "logError";
  static final String LOG_ERROR_DESC = "Total number of error log events";
  static final String LOG_WARN_KEY = "logWarn";
  static final String LOG_WARN_DESC = "Total number of warning log events";
  static final String LOG_INFO_KEY = "logInfo";
  static final String LOG_INFO_DESC = "Total number of info log events";

  private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
  private final List<GarbageCollectorMXBean> gcBeans =
      ManagementFactory.getGarbageCollectorMXBeans();
  private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

  JvmMetricsSource(String processName, String sessionId) {
    this.processName = processName;
    this.sessionId = sessionId;
  }

  public static JvmMetricsSource create(String processName, String sessionId,
                                        MetricsSystem ms) {
    return ms.register(SOURCE_NAME, SOURCE_DESC,
                       new JvmMetricsSource(processName, sessionId));
  }

  public static JvmMetricsSource create(String processName, String sessionId) {
    return create(processName, sessionId, DefaultMetricsSystem.INSTANCE);
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    MetricsRecordBuilder rb = builder.addRecord(RECORD_NAME)
        .setContext(CONTEXT)
        .tag(PROCESSNAME_KEY, PROCESSNAME_DESC, processName)
        .tag(SESSIONID_KEY, SESSIONID_DESC, sessionId);
    getMemoryUsage(rb);
    getGcUsage(rb);
    getThreadUsage(rb);
    getEventCounters(rb);
  }

  private void getMemoryUsage(MetricsRecordBuilder rb) {
    MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage();
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    rb.addGauge(NONHEAP_USED_KEY, NONHEAP_USED_DESC, memNonHeap.getUsed() / M)
      .addGauge(NONHEAP_COMMITTED_KEY, NONHEAP_COMMITTED_DESC,
                memNonHeap.getCommitted() / M)
      .addGauge(HEAP_USED_KEY, HEAP_USED_DESC, memHeap.getUsed() / M)
      .addGauge(HEAP_COMMITTED_KEY, HEAP_COMMITTED_DESC,
                memHeap.getCommitted() / M);
  }

  private void getGcUsage(MetricsRecordBuilder rb) {
    long count = 0;
    long timeMillis = 0;
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      count += gcBean.getCollectionCount();
      timeMillis += gcBean.getCollectionTime();
    }
    rb.addCounter(GC_COUNT_KEY, GC_COUNT_DESC, count)
      .addCounter(GC_TIME_KEY, GC_TIME_DESC, timeMillis);
  }

  private void getThreadUsage(MetricsRecordBuilder rb) {
    long threadIds[] = threadMXBean.getAllThreadIds();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, 0);
    int threadsNew = 0;
    int threadsRunnable = 0;
    int threadsBlocked = 0;
    int threadsWaiting = 0;
    int threadsTimedWaiting = 0;
    int threadsTerminated = 0;

    for (ThreadInfo threadInfo : threadInfos) {
      // threadInfo is null if the thread is not alive or doesn't exist
      if (threadInfo == null) {
        continue;
      }
      Thread.State state = threadInfo.getThreadState();
      if (state == NEW) {
        threadsNew++;
      } else if (state == RUNNABLE) {
        threadsRunnable++;
      } else if (state == BLOCKED) {
        threadsBlocked++;
      } else if (state == WAITING) {
        threadsWaiting++;
      } else if (state == TIMED_WAITING) {
        threadsTimedWaiting++;
      } else if (state == TERMINATED) {
        threadsTerminated++;
      }
    }
    rb.addGauge(THREADS_NEW_KEY, THREADS_NEW_DESC, threadsNew)
      .addGauge(THREADS_RUNNABLE_KEY, THREADS_RUNNABLE_DESC, threadsRunnable)
      .addGauge(THREADS_BLOCKED_KEY, THREADS_BLOCKED_DESC, threadsBlocked)
      .addGauge(THREADS_WAITING_KEY, THREADS_WAITING_DESC, threadsWaiting)
      .addGauge(THREADS_TIMEDWAITING_KEY, THREADS_TIMEDWAITING_DESC,
                threadsTimedWaiting)
      .addGauge(THREADS_TERMINATED_KEY, THREADS_TERMINATED_DESC,
                threadsTerminated);
  }

  private void getEventCounters(MetricsRecordBuilder rb) {
    rb.addCounter(LOG_FATAL_KEY, LOG_FATAL_DESC, EventCounter.getFatal())
      .addCounter(LOG_ERROR_KEY, LOG_ERROR_DESC, EventCounter.getError())
      .addCounter(LOG_WARN_KEY, LOG_WARN_DESC, EventCounter.getWarn())
      .addCounter(LOG_INFO_KEY, LOG_INFO_DESC, EventCounter.getInfo());
  }

}
