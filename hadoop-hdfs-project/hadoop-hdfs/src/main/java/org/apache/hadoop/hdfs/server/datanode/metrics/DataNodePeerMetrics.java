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

package org.apache.hadoop.hdfs.server.datanode.metrics;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;
import org.apache.hadoop.metrics2.MetricsJsonBuilder;
import org.apache.hadoop.metrics2.lib.MutableRollingAverages;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY;

/**
 * This class maintains DataNode peer metrics (e.g. numOps, AvgTime, etc.) for
 * various peer operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DataNodePeerMetrics {

  public static final Logger LOG = LoggerFactory.getLogger(
      DataNodePeerMetrics.class);

  private final MutableRollingAverages sendPacketDownstreamRollingAverages;

  private final String name;

  // Strictly to be used by test code only. Source code is not supposed to use this.
  private Map<String, OutlierMetrics> testOutlier = null;

  private final OutlierDetector slowNodeDetector;

  /**
   * Minimum number of packet send samples which are required to qualify
   * for outlier detection. If the number of samples is below this then
   * outlier detection is skipped.
   */
  private volatile long minOutlierDetectionSamples;
  /**
   * Threshold in milliseconds below which a DataNode is definitely not slow.
   */
  private volatile long lowThresholdMs;
  /**
   * Minimum number of nodes to run outlier detection.
   */
  private volatile long minOutlierDetectionNodes;

  public DataNodePeerMetrics(final String name, Configuration conf) {
    this.name = name;
    minOutlierDetectionSamples = conf.getLong(
        DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY,
        DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_DEFAULT);
    lowThresholdMs =
        conf.getLong(DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY,
            DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT);
    minOutlierDetectionNodes =
        conf.getLong(DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY,
            DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT);
    this.slowNodeDetector =
        new OutlierDetector(minOutlierDetectionNodes, lowThresholdMs);
    sendPacketDownstreamRollingAverages = new MutableRollingAverages("Time");
  }

  public String name() {
    return name;
  }

  public long getMinOutlierDetectionSamples() {
    return minOutlierDetectionSamples;
  }

  /**
   * Creates an instance of DataNodePeerMetrics, used for registration.
   */
  public static DataNodePeerMetrics create(String dnName, Configuration conf) {
    final String name = "DataNodePeerActivity-" + (dnName.isEmpty()
        ? "UndefinedDataNodeName" + ThreadLocalRandom.current().nextInt()
        : dnName.replace(':', '-'));

    return new DataNodePeerMetrics(name, conf);
  }

  /**
   * Adds invocation and elapsed time of SendPacketDownstream for peer.
   * <p>
   * The caller should pass in a well-formatted peerAddr. e.g.
   * "[192.168.1.110:1010]" is good. This will be translated into a full
   * qualified metric name, e.g. "[192.168.1.110:1010]AvgTime".
   * </p>
   */
  public void addSendPacketDownstream(
      final String peerAddr,
      final long elapsedMs) {
    sendPacketDownstreamRollingAverages.add(peerAddr, elapsedMs);
  }

  /**
   * Dump SendPacketDownstreamRollingAvgTime metrics as JSON.
   */
  public String dumpSendPacketDownstreamAvgInfoAsJson() {
    final MetricsJsonBuilder builder = new MetricsJsonBuilder(null);
    sendPacketDownstreamRollingAverages.snapshot(builder, true);
    return builder.toString();
  }

  /**
   * Collects states maintained in {@link ThreadLocal}, if any.
   */
  public void collectThreadLocalStates() {
    sendPacketDownstreamRollingAverages.collectThreadLocalStates();
  }

  /**
   * Retrieve the set of dataNodes that look significantly slower
   * than their peers.
   */
  public Map<String, OutlierMetrics> getOutliers() {
    // outlier must be null for source code.
    if (testOutlier == null) {
      // This maps the metric name to the aggregate latency.
      // The metric name is the datanode ID.
      final Map<String, Double> stats =
          sendPacketDownstreamRollingAverages.getStats(minOutlierDetectionSamples);
      LOG.trace("DataNodePeerMetrics: Got stats: {}", stats);
      return slowNodeDetector.getOutlierMetrics(stats);
    } else {
      // this happens only for test code.
      return testOutlier;
    }
  }

  /**
   * Strictly to be used by test code only. Source code is not supposed to use this. This method
   * directly sets outlier mapping so that aggregate latency metrics are not calculated for tests.
   *
   * @param outlier outlier directly set by tests.
   */
  public void setTestOutliers(Map<String, OutlierMetrics> outlier) {
    this.testOutlier = outlier;
  }

  public MutableRollingAverages getSendPacketDownstreamRollingAverages() {
    return sendPacketDownstreamRollingAverages;
  }

  public void setMinOutlierDetectionNodes(long minNodes) {
    Preconditions.checkArgument(minNodes > 0,
        DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY + " should be larger than 0");
    minOutlierDetectionNodes = minNodes;
    this.slowNodeDetector.setMinNumResources(minNodes);
  }

  public long getMinOutlierDetectionNodes() {
    return minOutlierDetectionNodes;
  }

  public void setLowThresholdMs(long thresholdMs) {
    Preconditions.checkArgument(thresholdMs > 0,
        DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY + " should be larger than 0");
    lowThresholdMs = thresholdMs;
    this.slowNodeDetector.setLowThresholdMs(thresholdMs);
  }

  public long getLowThresholdMs() {
    return lowThresholdMs;
  }

  public void setMinOutlierDetectionSamples(long minSamples) {
    Preconditions.checkArgument(minSamples > 0,
        DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY +
            " should be larger than 0");
    minOutlierDetectionSamples = minSamples;
  }

  @VisibleForTesting
  public OutlierDetector getSlowNodeDetector() {
    return this.slowNodeDetector;
  }
}
