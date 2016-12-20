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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.ThreadLocalRandom;

/**
 * This class is for maintaining Datanode Volume IO related statistics and
 * publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@Metrics(name = "DataNodeVolume", about = "DataNode Volume metrics",
    context = "dfs")
public class DataNodeVolumeMetrics {
  private final MetricsRegistry registry = new MetricsRegistry("FsVolume");

  @Metric("number of metadata operations")
  private MutableCounterLong totalMetadataOperations;
  @Metric("metadata operation rate")
  private MutableRate metadataOperationRate;
  private MutableQuantiles[] metadataOperationLatencyQuantiles;

  @Metric("number of data file io operations")
  private MutableCounterLong totalDataFileIos;
  @Metric("data file io operation rate")
  private MutableRate dataFileIoRate;
  private MutableQuantiles[] dataFileIoLatencyQuantiles;

  @Metric("file io flush rate")
  private MutableRate flushIoRate;
  private MutableQuantiles[] flushIoLatencyQuantiles;

  @Metric("file io sync rate")
  private MutableRate syncIoRate;
  private MutableQuantiles[] syncIoLatencyQuantiles;

  @Metric("file io read rate")
  private MutableRate readIoRate;
  private MutableQuantiles[] readIoLatencyQuantiles;

  @Metric("file io write rate")
  private MutableRate writeIoRate;
  private MutableQuantiles[] writeIoLatencyQuantiles;

  @Metric("number of file io errors")
  private MutableCounterLong totalFileIoErrors;
  @Metric("file io error rate")
  private MutableRate fileIoErrorRate;

  public long getTotalMetadataOperations() {
    return totalMetadataOperations.value();
  }

  // Based on metadataOperationRate
  public long getMetadataOperationSampleCount() {
    return metadataOperationRate.lastStat().numSamples();
  }

  public double getMetadataOperationMean() {
    return metadataOperationRate.lastStat().mean();
  }

  public double getMetadataOperationStdDev() {
    return metadataOperationRate.lastStat().stddev();
  }

  public long getTotalDataFileIos() {
    return totalDataFileIos.value();
  }

  // Based on dataFileIoRate
  public long getDataFileIoSampleCount() {
    return dataFileIoRate.lastStat().numSamples();
  }

  public double getDataFileIoMean() {
    return dataFileIoRate.lastStat().mean();
  }

  public double getDataFileIoStdDev() {
    return dataFileIoRate.lastStat().stddev();
  }

  // Based on flushIoRate
  public long getFlushIoSampleCount() {
    return flushIoRate.lastStat().numSamples();
  }

  public double getFlushIoMean() {
    return flushIoRate.lastStat().mean();
  }

  public double getFlushIoStdDev() {
    return flushIoRate.lastStat().stddev();
  }

  // Based on syncIoRate
  public long getSyncIoSampleCount() {
    return syncIoRate.lastStat().numSamples();
  }

  public double getSyncIoMean() {
    return syncIoRate.lastStat().mean();
  }

  public double getSyncIoStdDev() {
    return syncIoRate.lastStat().stddev();
  }

  // Based on readIoRate
  public long getReadIoSampleCount() {
    return readIoRate.lastStat().numSamples();
  }

  public double getReadIoMean() {
    return readIoRate.lastStat().mean();
  }

  public double getReadIoStdDev() {
    return readIoRate.lastStat().stddev();
  }

  // Based on writeIoRate
  public long getWriteIoSampleCount() {
    return syncIoRate.lastStat().numSamples();
  }

  public double getWriteIoMean() {
    return syncIoRate.lastStat().mean();
  }

  public double getWriteIoStdDev() {
    return syncIoRate.lastStat().stddev();
  }

  public long getTotalFileIoErrors() {
    return totalFileIoErrors.value();
  }

  // Based on fileIoErrorRate
  public long getFileIoErrorSampleCount() {
    return fileIoErrorRate.lastStat().numSamples();
  }

  public double getFileIoErrorMean() {
    return fileIoErrorRate.lastStat().mean();
  }

  public double getFileIoErrorStdDev() {
    return fileIoErrorRate.lastStat().stddev();
  }

  private final String name;
  private final MetricsSystem ms;

  public DataNodeVolumeMetrics(final MetricsSystem metricsSystem,
      final String volumeName, final int[] intervals) {
    this.ms = metricsSystem;
    this.name = volumeName;
    final int len = intervals.length;
    metadataOperationLatencyQuantiles = new MutableQuantiles[len];
    dataFileIoLatencyQuantiles = new MutableQuantiles[len];
    flushIoLatencyQuantiles = new MutableQuantiles[len];
    syncIoLatencyQuantiles = new MutableQuantiles[len];
    readIoLatencyQuantiles = new MutableQuantiles[len];
    writeIoLatencyQuantiles = new MutableQuantiles[len];
    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      metadataOperationLatencyQuantiles[i] = registry.newQuantiles(
          "metadataOperationLatency" + interval + "s",
          "Meatadata Operation Latency in ms", "ops", "latency", interval);
      dataFileIoLatencyQuantiles[i] = registry.newQuantiles(
          "dataFileIoLatency" + interval + "s",
          "Data File Io Latency in ms", "ops", "latency", interval);
      flushIoLatencyQuantiles[i] = registry.newQuantiles(
          "flushIoLatency" + interval + "s",
          "Data flush Io Latency in ms", "ops", "latency", interval);
      syncIoLatencyQuantiles[i] = registry.newQuantiles(
          "syncIoLatency" + interval + "s",
          "Data sync Io Latency in ms", "ops", "latency", interval);
      readIoLatencyQuantiles[i] = registry.newQuantiles(
          "readIoLatency" + interval + "s",
          "Data read Io Latency in ms", "ops", "latency", interval);
      writeIoLatencyQuantiles[i] = registry.newQuantiles(
          "writeIoLatency" + interval + "s",
          "Data write Io Latency in ms", "ops", "latency", interval);
    }
  }

  public static DataNodeVolumeMetrics create(final Configuration conf,
      final String volumeName) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = "DataNodeVolume-"+ (volumeName.isEmpty()
        ? "UndefinedDataNodeVolume"+ ThreadLocalRandom.current().nextInt()
        : volumeName.replace(':', '-'));

    // Percentile measurement is off by default, by watching no intervals
    int[] intervals =
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    return ms.register(name, null, new DataNodeVolumeMetrics(ms, name,
        intervals));
  }

  public String name() {
    return name;
  }

  public void unRegister() {
    ms.unregisterSource(name);
  }

  public void addMetadastaOperationLatency(final long latency) {
    totalMetadataOperations.incr();
    metadataOperationRate.add(latency);
    for (MutableQuantiles q : metadataOperationLatencyQuantiles) {
      q.add(latency);
    }
  }

  public void addDataFileIoLatency(final long latency) {
    totalDataFileIos.incr();
    dataFileIoRate.add(latency);
    for (MutableQuantiles q : dataFileIoLatencyQuantiles) {
      q.add(latency);
    }
  }

  public void addSyncIoLatency(final long latency) {
    syncIoRate.add(latency);
    for (MutableQuantiles q : syncIoLatencyQuantiles) {
      q.add(latency);
    }
  }

  public void addFlushIoLatency(final long latency) {
    flushIoRate.add(latency);
    for (MutableQuantiles q : flushIoLatencyQuantiles) {
      q.add(latency);
    }
  }

  public void addReadIoLatency(final long latency) {
    readIoRate.add(latency);
    for (MutableQuantiles q : readIoLatencyQuantiles) {
      q.add(latency);
    }
  }

  public void addWriteIoLatency(final long latency) {
    writeIoRate.add(latency);
    for (MutableQuantiles q: writeIoLatencyQuantiles) {
      q.add(latency);
    }
  }

  public void addFileIoError(final long latency) {
    totalFileIoErrors.incr();
    metadataOperationRate.add(latency);
  }
}