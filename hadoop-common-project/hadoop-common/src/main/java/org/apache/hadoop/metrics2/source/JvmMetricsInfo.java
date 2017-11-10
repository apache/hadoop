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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * JVM and logging related metrics info instances
 */
@InterfaceAudience.Private
public enum JvmMetricsInfo implements MetricsInfo {
  JvmMetrics("JVM related metrics etc."), // record info
  // metrics
  MemNonHeapUsedM("Non-heap memory used in MB"),
  MemNonHeapCommittedM("Non-heap memory committed in MB"),
  MemNonHeapMaxM("Non-heap memory max in MB"),
  MemHeapUsedM("Heap memory used in MB"),
  MemHeapCommittedM("Heap memory committed in MB"),
  MemHeapMaxM("Heap memory max in MB"),
  MemMaxM("Max memory size in MB"),
  GcCount("Total GC count"),
  GcTimeMillis("Total GC time in milliseconds"),
  ThreadsNew("Number of new threads"),
  ThreadsRunnable("Number of runnable threads"),
  ThreadsBlocked("Number of blocked threads"),
  ThreadsWaiting("Number of waiting threads"),
  ThreadsTimedWaiting("Number of timed waiting threads"),
  ThreadsTerminated("Number of terminated threads"),
  LogFatal("Total number of fatal log events"),
  LogError("Total number of error log events"),
  LogWarn("Total number of warning log events"),
  LogInfo("Total number of info log events"),
  GcNumWarnThresholdExceeded("Number of times that the GC warn threshold is exceeded"),
  GcNumInfoThresholdExceeded("Number of times that the GC info threshold is exceeded"),
  GcTotalExtraSleepTime("Total GC extra sleep time in milliseconds"),
  GcTimePercentage("Percentage of time the JVM was paused in GC");

  private final String desc;

  JvmMetricsInfo(String desc) { this.desc = desc; }

  @Override public String description() { return desc; }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append(this.getClass().getSimpleName());
    sb.append("{name=");
    sb.append(name());
    sb.append(", description=");
    sb.append(desc);
    return sb.append('}').toString();
  }
}
