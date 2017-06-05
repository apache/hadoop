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
package org.apache.hadoop.ipc.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about="Aggregate RPC metrics", context="rpc")
public class RpcMetrics {

  static final Log LOG = LogFactory.getLog(RpcMetrics.class);
  final Server server;
  final MetricsRegistry registry;
  final String name;
  final boolean rpcQuantileEnable;
  
  RpcMetrics(Server server, Configuration conf) {
    String port = String.valueOf(server.getListenerAddress().getPort());
    name = "RpcActivityForPort" + port;
    this.server = server;
    registry = new MetricsRegistry("rpc").tag("port", "RPC port", port);
    int[] intervals = conf.getInts(
        CommonConfigurationKeys.RPC_METRICS_PERCENTILES_INTERVALS_KEY);
    rpcQuantileEnable = (intervals.length > 0) && conf.getBoolean(
        CommonConfigurationKeys.RPC_METRICS_QUANTILE_ENABLE,
        CommonConfigurationKeys.RPC_METRICS_QUANTILE_ENABLE_DEFAULT);
    if (rpcQuantileEnable) {
      rpcQueueTimeMillisQuantiles =
          new MutableQuantiles[intervals.length];
      rpcProcessingTimeMillisQuantiles =
          new MutableQuantiles[intervals.length];
      for (int i = 0; i < intervals.length; i++) {
        int interval = intervals[i];
        rpcQueueTimeMillisQuantiles[i] = registry.newQuantiles("rpcQueueTime"
            + interval + "s", "rpc queue time in milli second", "ops",
            "latency", interval);
        rpcProcessingTimeMillisQuantiles[i] = registry.newQuantiles(
            "rpcProcessingTime" + interval + "s",
            "rpc processing time in milli second", "ops", "latency", interval);
      }
    }
    LOG.debug("Initialized " + registry);
  }

  public String name() { return name; }

  public static RpcMetrics create(Server server, Configuration conf) {
    RpcMetrics m = new RpcMetrics(server, conf);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  @Metric("Number of received bytes") MutableCounterLong receivedBytes;
  @Metric("Number of sent bytes") MutableCounterLong sentBytes;
  @Metric("Queue time") MutableRate rpcQueueTime;
  MutableQuantiles[] rpcQueueTimeMillisQuantiles;
  @Metric("Processsing time") MutableRate rpcProcessingTime;
  MutableQuantiles[] rpcProcessingTimeMillisQuantiles;
  @Metric("Number of authentication failures")
  MutableCounterLong rpcAuthenticationFailures;
  @Metric("Number of authentication successes")
  MutableCounterLong rpcAuthenticationSuccesses;
  @Metric("Number of authorization failures")
  MutableCounterLong rpcAuthorizationFailures;
  @Metric("Number of authorization sucesses")
  MutableCounterLong rpcAuthorizationSuccesses;
  @Metric("Number of Slow RPC calls")
  MutableCounterLong rpcSlowCalls;
  @Metric("Number of client backoff requests")
  MutableCounterLong rpcClientBackoff;

  @Metric("Number of open connections") public int numOpenConnections() {
    return server.getNumOpenConnections();
  }

  @Metric("Number of open connections per user")
  public String numOpenConnectionsPerUser() {
    return server.getNumOpenConnectionsPerUser();
  }

  @Metric("Length of the call queue") public int callQueueLength() {
    return server.getCallQueueLen();
  }

  @Metric("Number of dropped connections") public long numDroppedConnections() {
    return server.getNumDroppedConnections();
  }

  // Public instrumentation methods that could be extracted to an
  // abstract class if we decide to do custom instrumentation classes a la
  // JobTrackerInstrumenation. The methods with //@Override comment are
  // candidates for abstract methods in a abstract instrumentation class.

  /**
   * One authentication failure event
   */
  //@Override
  public void incrAuthenticationFailures() {
    rpcAuthenticationFailures.incr();
  }

  /**
   * One authentication success event
   */
  //@Override
  public void incrAuthenticationSuccesses() {
    rpcAuthenticationSuccesses.incr();
  }

  /**
   * One authorization success event
   */
  //@Override
  public void incrAuthorizationSuccesses() {
    rpcAuthorizationSuccesses.incr();
  }

  /**
   * One authorization failure event
   */
  //@Override
  public void incrAuthorizationFailures() {
    rpcAuthorizationFailures.incr();
  }

  /**
   * Shutdown the instrumentation for the process
   */
  //@Override
  public void shutdown() {}

  /**
   * Increment sent bytes by count
   * @param count to increment
   */
  //@Override
  public void incrSentBytes(int count) {
    sentBytes.incr(count);
  }

  /**
   * Increment received bytes by count
   * @param count to increment
   */
  //@Override
  public void incrReceivedBytes(int count) {
    receivedBytes.incr(count);
  }

  /**
   * Add an RPC queue time sample
   * @param qTime the queue time
   */
  //@Override
  public void addRpcQueueTime(int qTime) {
    rpcQueueTime.add(qTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : rpcQueueTimeMillisQuantiles) {
        q.add(qTime);
      }
    }
  }

  /**
   * Add an RPC processing time sample
   * @param processingTime the processing time
   */
  //@Override
  public void addRpcProcessingTime(int processingTime) {
    rpcProcessingTime.add(processingTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : rpcProcessingTimeMillisQuantiles) {
        q.add(processingTime);
      }
    }
  }

  /**
   * One client backoff event
   */
  //@Override
  public void incrClientBackoff() {
    rpcClientBackoff.incr();
  }

  /**
   * Increments the Slow RPC counter.
   */
  public  void incrSlowRpc() {
    rpcSlowCalls.incr();
  }
  /**
   * Returns a MutableRate Counter.
   * @return Mutable Rate
   */
  public MutableRate getRpcProcessingTime() {
    return rpcProcessingTime;
  }

  /**
   * Returns the number of samples that we have seen so far.
   * @return long
   */
  public long getProcessingSampleCount() {
    return rpcProcessingTime.lastStat().numSamples();
  }

  /**
   * Returns mean of RPC Processing Times.
   * @return double
   */
  public double getProcessingMean() {
    return  rpcProcessingTime.lastStat().mean();
  }

  /**
   * Return Standard Deviation of the Processing Time.
   * @return  double
   */
  public double getProcessingStdDev() {
    return rpcProcessingTime.lastStat().stddev();
  }

  /**
   * Returns the number of slow calls.
   * @return long
   */
  public long getRpcSlowCalls() {
    return rpcSlowCalls.value();
  }
}
