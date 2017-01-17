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

import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsJsonBuilder;
import org.apache.hadoop.metrics2.lib.RollingAverages;

/**
 * This class maintains DataNode peer metrics (e.g. numOps, AvgTime, etc.) for
 * various peer operations.
 */
@InterfaceAudience.Private
public class DataNodePeerMetrics {

  static final Log LOG = LogFactory.getLog(DataNodePeerMetrics.class);

  private final RollingAverages sendPacketDownstreamRollingAvgerages;

  private final String name;
  private final boolean peerStatsEnabled;

  public DataNodePeerMetrics(
      final String name,
      final int windowSize,
      final int numWindows,
      final boolean peerStatsEnabled) {
    this.name = name;
    this.peerStatsEnabled = peerStatsEnabled;
    sendPacketDownstreamRollingAvgerages = new RollingAverages(
        windowSize,
        numWindows);
  }

  public String name() {
    return name;
  }

  /**
   * Creates an instance of DataNodePeerMetrics, used for registration.
   */
  public static DataNodePeerMetrics create(Configuration conf, String dnName) {
    final String name = "DataNodePeerActivity-" + (dnName.isEmpty()
        ? "UndefinedDataNodeName" + ThreadLocalRandom.current().nextInt()
        : dnName.replace(':', '-'));

    final int windowSize = conf.getInt(
            DFSConfigKeys.DFS_METRICS_ROLLING_AVERAGES_WINDOW_SIZE_KEY,
            DFSConfigKeys.DFS_METRICS_ROLLING_AVERAGES_WINDOW_SIZE_DEFAULT);
    final int numWindows = conf.getInt(
        DFSConfigKeys.DFS_METRICS_ROLLING_AVERAGES_WINDOW_NUMBERS_KEY,
        DFSConfigKeys.DFS_METRICS_ROLLING_AVERAGES_WINDOW_NUMBERS_DEFAULT);
    final boolean peerStatsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY,
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT);

    return new DataNodePeerMetrics(
        name,
        windowSize,
        numWindows,
        peerStatsEnabled);
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
    if (peerStatsEnabled) {
      sendPacketDownstreamRollingAvgerages.add(peerAddr, elapsedMs);
    }
  }

  /**
   * Dump SendPacketDownstreamRollingAvgTime metrics as JSON.
   */
  public String dumpSendPacketDownstreamAvgInfoAsJson() {
    final MetricsJsonBuilder builder = new MetricsJsonBuilder(null);
    sendPacketDownstreamRollingAvgerages.snapshot(builder, true);
    return builder.toString();
  }

  /**
   * Collects states maintained in {@link ThreadLocal}, if any.
   */
  public void collectThreadLocalStates() {
    sendPacketDownstreamRollingAvgerages.collectThreadLocalStates();
  }
}
