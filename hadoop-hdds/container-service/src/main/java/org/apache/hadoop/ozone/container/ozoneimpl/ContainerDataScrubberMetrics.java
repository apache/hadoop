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
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.ThreadLocalRandom;

/**
 * This class captures the container data scrubber metrics on the data-node.
 **/
@InterfaceAudience.Private
@Metrics(about="DataNode container data scrubber metrics", context="dfs")
public final class ContainerDataScrubberMetrics {
  private final String name;
  private final MetricsSystem ms;
  @Metric("number of containers scanned in the current iteration")
  private MutableGaugeInt numContainersScanned;
  @Metric("number of unhealthy containers found in the current iteration")
  private MutableGaugeInt numUnHealthyContainers;
  @Metric("number of iterations of scanner completed since the restart")
  private MutableCounterInt numScanIterations;
  @Metric("disk bandwidth used by the container data scrubber per volume")
  private MutableRate numBytesScanned;

  public int getNumContainersScanned() {
    return numContainersScanned.value();
  }

  public void incNumContainersScanned() {
    numContainersScanned.incr();
  }

  public void resetNumContainersScanned() {
    numContainersScanned.decr(getNumContainersScanned());
  }

  public int getNumUnHealthyContainers() {
    return numUnHealthyContainers.value();
  }

  public void incNumUnHealthyContainers() {
    numUnHealthyContainers.incr();
  }

  public void resetNumUnhealthyContainers() {
    numUnHealthyContainers.decr(getNumUnHealthyContainers());
  }

  public int getNumScanIterations() {
    return numScanIterations.value();
  }

  public void incNumScanIterations() {
    numScanIterations.incr();
  }

  public double getNumBytesScannedMean() {
    return numBytesScanned.lastStat().mean();
  }

  public long getNumBytesScannedSampleCount() {
    return numBytesScanned.lastStat().numSamples();
  }

  public double getNumBytesScannedStdDev() {
    return numBytesScanned.lastStat().stddev();
  }

  public void incNumBytesScanned(long bytes) {
    numBytesScanned.add(bytes);
  }

  public void unregister() {
    ms.unregisterSource(name);
  }

  private ContainerDataScrubberMetrics(String name, MetricsSystem ms) {
    this.name = name;
    this.ms = ms;
  }

  public static ContainerDataScrubberMetrics create(final String volumeName) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = "ContainerDataScrubberMetrics-"+ (volumeName.isEmpty()
        ? "UndefinedDataNodeVolume"+ ThreadLocalRandom.current().nextInt()
        : volumeName.replace(':', '-'));

    return ms.register(name, null, new ContainerDataScrubberMetrics(name, ms));
  }
}
