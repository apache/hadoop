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

package org.apache.hadoop.metrics2.lib;

import com.google.common.collect.Sets;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.SampleStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to manage a group of mutable rate metrics.
 *
 * Each thread will maintain a local rate count, and upon snapshot,
 * these values will be aggregated into a global rate. This class
 * should only be used for long running threads, as any metrics
 * produced between the last snapshot and the death of a thread
 * will be lost. This allows for significantly higher concurrency
 * than {@link MutableRates}. See HADOOP-24420.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableRatesWithAggregation extends MutableMetric {
  static final Logger LOG =
      LoggerFactory.getLogger(MutableRatesWithAggregation.class);
  private final Map<String, MutableRate> globalMetrics =
      new ConcurrentHashMap<>();
  private final Set<Class<?>> protocolCache = Sets.newHashSet();

  private final ConcurrentLinkedDeque<WeakReference<ConcurrentMap<String, ThreadSafeSampleStat>>>
      weakReferenceQueue = new ConcurrentLinkedDeque<>();
  private final ThreadLocal<ConcurrentMap<String, ThreadSafeSampleStat>>
      threadLocalMetricsMap = new ThreadLocal<>();

  /**
   * Initialize the registry with all the methods in a protocol
   * so they all show up in the first snapshot.
   * Convenient for JMX implementations.
   * @param protocol the protocol class
   */
  public void init(Class<?> protocol) {
    if (protocolCache.contains(protocol)) {
      return;
    }
    protocolCache.add(protocol);
    for (Method method : protocol.getDeclaredMethods()) {
      String name = method.getName();
      LOG.debug(name);
      addMetricIfNotExists(name);
    }
  }

  /**
   * Add a rate sample for a rate metric.
   * @param name of the rate metric
   * @param elapsed time
   */
  public void add(String name, long elapsed) {
    ConcurrentMap<String, ThreadSafeSampleStat> localStats =
        threadLocalMetricsMap.get();
    if (localStats == null) {
      localStats = new ConcurrentHashMap<>();
      threadLocalMetricsMap.set(localStats);
      weakReferenceQueue.add(new WeakReference<>(localStats));
    }
    ThreadSafeSampleStat stat = localStats.get(name);
    if (stat == null) {
      stat = new ThreadSafeSampleStat();
      localStats.put(name, stat);
    }
    stat.add(elapsed);
  }

  @Override
  public synchronized void snapshot(MetricsRecordBuilder rb, boolean all) {
    Iterator<WeakReference<ConcurrentMap<String, ThreadSafeSampleStat>>> iter =
        weakReferenceQueue.iterator();
    while (iter.hasNext()) {
      ConcurrentMap<String, ThreadSafeSampleStat> map = iter.next().get();
      if (map == null) {
        // Thread has died; clean up its state
        iter.remove();
      } else {
        aggregateLocalStatesToGlobalMetrics(map);
      }
    }
    for (MutableRate globalMetric : globalMetrics.values()) {
      globalMetric.snapshot(rb, all);
    }
  }

  /**
   * Collects states maintained in {@link ThreadLocal}, if any.
   */
  synchronized void collectThreadLocalStates() {
    final ConcurrentMap<String, ThreadSafeSampleStat> localStats =
        threadLocalMetricsMap.get();
    if (localStats != null) {
      aggregateLocalStatesToGlobalMetrics(localStats);
    }
  }

  /**
   * Aggregates the thread's local samples into the global metrics. The caller
   * should ensure its thread safety.
   */
  private void aggregateLocalStatesToGlobalMetrics(
      final ConcurrentMap<String, ThreadSafeSampleStat> localStats) {
    for (Map.Entry<String, ThreadSafeSampleStat> entry : localStats
        .entrySet()) {
      String name = entry.getKey();
      MutableRate globalMetric = addMetricIfNotExists(name);
      entry.getValue().snapshotInto(globalMetric);
    }
  }

  Map<String, MutableRate> getGlobalMetrics() {
    return globalMetrics;
  }

  private synchronized MutableRate addMetricIfNotExists(String name) {
    MutableRate metric = globalMetrics.get(name);
    if (metric == null) {
      metric = new MutableRate(name, name, false);
      globalMetrics.put(name, metric);
    }
    return metric;
  }

  private static class ThreadSafeSampleStat {

    private SampleStat stat = new SampleStat();

    synchronized void add(double x) {
      stat.add(x);
    }

    synchronized void snapshotInto(MutableRate metric) {
      if (stat.numSamples() > 0) {
        metric.add(stat.numSamples(), Math.round(stat.total()));
        stat.reset();
      }
    }
  }

}
