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
package org.apache.hadoop.hdfs.server.federation.metrics;

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableRate;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementations of the JMX interface for the State Store metrics.
 */
@Metrics(name = "StateStoreActivity", about = "Router metrics",
    context = "dfs")
public final class StateStoreMetrics implements StateStoreMBean {

  private final MetricsRegistry registry = new MetricsRegistry("router");

  @Metric("GET transactions")
  private MutableRate reads;
  @Metric("PUT transactions")
  private MutableRate writes;
  @Metric("REMOVE transactions")
  private MutableRate removes;
  @Metric("Failed transactions")
  private MutableRate failures;

  private Map<String, MutableGaugeInt> cacheSizes;

  private StateStoreMetrics(Configuration conf) {
    registry.tag(SessionId, "RouterSession");
    registry.tag(ProcessName, "Router");
    cacheSizes = new HashMap<>();
  }

  public static StateStoreMetrics create(Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(new StateStoreMetrics(conf));
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
    reset();
  }

  public void addRead(long latency) {
    reads.add(latency);
  }

  public long getReadOps() {
    return reads.lastStat().numSamples();
  }

  public double getReadAvg() {
    return reads.lastStat().mean();
  }

  public void addWrite(long latency) {
    writes.add(latency);
  }

  public long getWriteOps() {
    return writes.lastStat().numSamples();
  }

  public double getWriteAvg() {
    return writes.lastStat().mean();
  }

  public void addFailure(long latency) {
    failures.add(latency);
  }

  public long getFailureOps() {
    return failures.lastStat().numSamples();
  }

  public double getFailureAvg() {
    return failures.lastStat().mean();
  }

  public void addRemove(long latency) {
    removes.add(latency);
  }

  public long getRemoveOps() {
    return removes.lastStat().numSamples();
  }

  public double getRemoveAvg() {
    return removes.lastStat().mean();
  }

  /**
   * Set the size of the cache for a State Store interface.
   *
   * @param name Name of the record to cache.
   * @param size Number of records.
   */
  public void setCacheSize(String name, int size) {
    String counterName = "Cache" + name + "Size";
    MutableGaugeInt counter = cacheSizes.get(counterName);
    if (counter == null) {
      counter = registry.newGauge(counterName, name, size);
      cacheSizes.put(counterName, counter);
    }
    counter.set(size);
  }

  @VisibleForTesting
  public void reset() {
    reads.resetMinMax();
    writes.resetMinMax();
    removes.resetMinMax();
    failures.resetMinMax();

    reads.lastStat().reset();
    writes.lastStat().reset();
    removes.lastStat().reset();
    failures.lastStat().reset();
  }
}
