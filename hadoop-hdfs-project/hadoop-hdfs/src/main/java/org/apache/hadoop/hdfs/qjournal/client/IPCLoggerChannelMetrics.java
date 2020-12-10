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
package org.apache.hadoop.hdfs.qjournal.client;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * The metrics for a journal from the writer's perspective.
 */
@Metrics(about="Journal client metrics", context="dfs")
class IPCLoggerChannelMetrics {
  final MetricsRegistry registry = new MetricsRegistry("NameNode");

  private volatile IPCLoggerChannel ch;
  
  private final MutableQuantiles[] writeEndToEndLatencyQuantiles;
  private final MutableQuantiles[] writeRpcLatencyQuantiles;

  
  /**
   * In the case of the NN transitioning between states, edit logs are closed
   * and reopened. Thus, the IPCLoggerChannel instance that writes to a
   * given JournalNode may change over the lifetime of the process.
   * However, metrics2 doesn't have a function to unregister a set of metrics
   * and fails if a new metrics class is registered with the same name
   * as the existing one. Hence, we have to maintain our own registry
   * ("multiton") here, so that we have exactly one metrics instance
   * per JournalNode, and switch out the pointer to the underlying
   * IPCLoggerChannel instance.
   */
  private static final Map<String, IPCLoggerChannelMetrics> REGISTRY =
      Maps.newHashMap();
  
  private IPCLoggerChannelMetrics(IPCLoggerChannel ch) {
    this.ch = ch;
    
    Configuration conf = new HdfsConfiguration();
    int[] intervals = 
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    if (intervals != null) {
      writeEndToEndLatencyQuantiles = new MutableQuantiles[intervals.length];
      writeRpcLatencyQuantiles = new MutableQuantiles[intervals.length];
      for (int i = 0; i < writeEndToEndLatencyQuantiles.length; i++) {
        int interval = intervals[i];
        writeEndToEndLatencyQuantiles[i] = registry.newQuantiles(
            "writesE2E" + interval + "s",
            "End-to-end time for write operations", "ops", "LatencyMicros", interval);
        writeRpcLatencyQuantiles[i] = registry.newQuantiles(
            "writesRpc" + interval + "s",
            "RPC RTT for write operations", "ops", "LatencyMicros", interval);
      }
    } else {
      writeEndToEndLatencyQuantiles = null;
      writeRpcLatencyQuantiles = null;
    }
  }
  
  private void setChannel(IPCLoggerChannel ch) {
    assert ch.getRemoteAddress().equals(this.ch.getRemoteAddress());
    this.ch = ch;
  }

  static IPCLoggerChannelMetrics create(IPCLoggerChannel ch) {
    String name = getName(ch);
    synchronized (REGISTRY) {
      IPCLoggerChannelMetrics m = REGISTRY.get(name);
      if (m != null) {
        m.setChannel(ch);
      } else {
        m = new IPCLoggerChannelMetrics(ch);
        DefaultMetricsSystem.instance().register(name, null, m);
        REGISTRY.put(name, m);
      }
      return m;
    }
  }

  private static String getName(IPCLoggerChannel ch) {
    InetSocketAddress addr = ch.getRemoteAddress();
    String addrStr = addr.getAddress().getHostAddress();
    
    // IPv6 addresses have colons, which aren't allowed as part of
    // MBean names. Replace with '.'
    addrStr = addrStr.replace(':', '.');
    
    return "IPCLoggerChannel-" + addrStr +
        "-" + addr.getPort();
  }

  @Metric("Is the remote logger out of sync with the quorum")
  public String isOutOfSync() {
    return Boolean.toString(ch.isOutOfSync()); 
  }
  
  @Metric("The number of transactions the remote log is lagging behind the " +
          "quorum")
  public long getCurrentLagTxns() {
    return ch.getLagTxns();
  }
  
  @Metric("The number of milliseconds the remote log is lagging behind the " +
          "quorum")
  public long getLagTimeMillis() {
    return ch.getLagTimeMillis();
  }
  
  @Metric("The number of bytes of pending data to be sent to the remote node")
  public int getQueuedEditsSize() {
    return ch.getQueuedEditsSize();
  }

  public void addWriteEndToEndLatency(long micros) {
    if (writeEndToEndLatencyQuantiles != null) {
      for (MutableQuantiles q : writeEndToEndLatencyQuantiles) {
        q.add(micros);
      }
    }
  }
  
  public void addWriteRpcLatency(long micros) {
    if (writeRpcLatencyQuantiles != null) {
      for (MutableQuantiles q : writeRpcLatencyQuantiles) {
        q.add(micros);
      }
    }
  }
}
