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

package org.apache.hadoop.cblock.jscsiHelper;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This class is for maintaining  the various Cblock Target statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 *
 * This class maintains stats like cache hit and miss ratio
 * as well as the latency time of read and write ops.
 */
public class CBlockTargetMetrics {
  @Metric private MutableCounterLong numReadOps;
  @Metric private MutableCounterLong numWriteOps;
  @Metric private MutableCounterLong numReadCacheHits;
  @Metric private MutableCounterLong numReadCacheMiss;
  @Metric private MutableCounterLong numReadLostBlocks;
  @Metric private MutableCounterLong numDirectBlockWrites;
  @Metric private MutableCounterLong numFailedDirectBlockWrites;

  @Metric private MutableRate dbReadLatency;
  @Metric private MutableRate containerReadLatency;

  @Metric private MutableRate dbWriteLatency;
  @Metric private MutableRate containerWriteLatency;

  @Metric private MutableRate directBlockWriteLatency;

  public CBlockTargetMetrics() {
  }

  public static CBlockTargetMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register("CBlockTargetMetrics",
        "CBlock Target Metrics",
        new CBlockTargetMetrics());
  }

  public void incNumReadOps() {
    numReadOps.incr();
  }

  public void incNumWriteOps() {
    numWriteOps.incr();
  }

  public void incNumReadCacheHits() {
    numReadCacheHits.incr();
  }

  public void incNumReadCacheMiss() {
    numReadCacheMiss.incr();
  }

  public void incNumReadLostBlocks() {
    numReadLostBlocks.incr();
  }

  public void incNumDirectBlockWrites() {
    numDirectBlockWrites.incr();
  }

  public void incNumFailedDirectBlockWrites() {
    numFailedDirectBlockWrites.incr();
  }

  public void updateDBReadLatency(long latency) {
    dbReadLatency.add(latency);
  }

  public void updateContainerReadLatency(long latency) {
    containerReadLatency.add(latency);
  }

  public void updateDBWriteLatency(long latency) {
    dbWriteLatency.add(latency);
  }

  public void updateContainerWriteLatency(long latency) {
    containerWriteLatency.add(latency);
  }

  public void updateDirectBlockWriteLatency(long latency) {
    directBlockWriteLatency.add(latency);
  }

  @VisibleForTesting
  public long getNumReadOps() {
    return numReadOps.value();
  }

  @VisibleForTesting
  public long getNumWriteOps() {
    return numWriteOps.value();
  }

  @VisibleForTesting
  public long getNumReadCacheHits() {
    return numReadCacheHits.value();
  }

  @VisibleForTesting
  public long getNumReadCacheMiss() {
    return numReadCacheMiss.value();
  }

  @VisibleForTesting
  public long getNumReadLostBlocks() {
    return numReadLostBlocks.value();
  }

  @VisibleForTesting
  public long getNumDirectBlockWrites() {
    return numDirectBlockWrites.value();
  }

  @VisibleForTesting
  public long getNumFailedDirectBlockWrites() {
    return numFailedDirectBlockWrites.value();
  }
}
