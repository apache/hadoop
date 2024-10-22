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

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about="Aggregate RPC metrics", context="rpc")
public class RpcMetrics {

  static final Logger LOG = LoggerFactory.getLogger(RpcMetrics.class);
  final Server server;
  final MetricsRegistry registry;
  final String name;
  final boolean rpcQuantileEnable;

  public static final TimeUnit DEFAULT_METRIC_TIME_UNIT =
      TimeUnit.MILLISECONDS;
  /** The time unit used when storing/accessing time durations. */
  private final TimeUnit metricsTimeUnit;

  RpcMetrics(Server server, Configuration conf) {
    String port = String.valueOf(server.getListenerAddress().getPort());
    name = "RpcActivityForPort" + port;
    this.server = server;
    registry = new MetricsRegistry("rpc")
        .tag("port", "RPC port", port)
        .tag("serverName", "Name of the RPC server", server.getServerName());
    int[] intervals = conf.getInts(
        CommonConfigurationKeys.RPC_METRICS_PERCENTILES_INTERVALS_KEY);
    rpcQuantileEnable = (intervals.length > 0) && conf.getBoolean(
        CommonConfigurationKeys.RPC_METRICS_QUANTILE_ENABLE,
        CommonConfigurationKeys.RPC_METRICS_QUANTILE_ENABLE_DEFAULT);
    metricsTimeUnit = getMetricsTimeUnit(conf);
    if (rpcQuantileEnable) {
      rpcEnQueueTimeQuantiles =
          new MutableQuantiles[intervals.length];
      rpcQueueTimeQuantiles =
          new MutableQuantiles[intervals.length];
      rpcLockWaitTimeQuantiles =
          new MutableQuantiles[intervals.length];
      rpcProcessingTimeQuantiles =
          new MutableQuantiles[intervals.length];
      rpcResponseTimeQuantiles =
          new MutableQuantiles[intervals.length];
      deferredRpcProcessingTimeQuantiles =
          new MutableQuantiles[intervals.length];
      for (int i = 0; i < intervals.length; i++) {
        int interval = intervals[i];
        rpcEnQueueTimeQuantiles[i] = registry.newQuantiles("rpcEnQueueTime"
            + interval + "s", "rpc enqueue time in " + metricsTimeUnit, "ops",
            "latency", interval);
        rpcQueueTimeQuantiles[i] = registry.newQuantiles("rpcQueueTime"
            + interval + "s", "rpc queue time in " + metricsTimeUnit, "ops",
            "latency", interval);
        rpcLockWaitTimeQuantiles[i] = registry.newQuantiles(
            "rpcLockWaitTime" + interval + "s",
            "rpc lock wait time in " + metricsTimeUnit, "ops",
            "latency", interval);
        rpcProcessingTimeQuantiles[i] = registry.newQuantiles(
            "rpcProcessingTime" + interval + "s",
            "rpc processing time in " + metricsTimeUnit, "ops",
            "latency", interval);
        rpcResponseTimeQuantiles[i] = registry.newQuantiles(
            "rpcResponseTime" + interval + "s",
            "rpc response time in " + metricsTimeUnit, "ops",
            "latency", interval);
        deferredRpcProcessingTimeQuantiles[i] = registry.newQuantiles(
            "deferredRpcProcessingTime" + interval + "s",
            "deferred rpc processing time in " + metricsTimeUnit, "ops",
            "latency", interval);
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
  @Metric("EQueue time") MutableRate rpcEnQueueTime;
  MutableQuantiles[] rpcEnQueueTimeQuantiles;
  @Metric("Queue time") MutableRate rpcQueueTime;
  MutableQuantiles[] rpcQueueTimeQuantiles;
  @Metric("Lock wait time") MutableRate rpcLockWaitTime;
  MutableQuantiles[] rpcLockWaitTimeQuantiles;
  @Metric("Processing time") MutableRate rpcProcessingTime;
  MutableQuantiles[] rpcProcessingTimeQuantiles;
  @Metric("Response time") MutableRate rpcResponseTime;
  MutableQuantiles[] rpcResponseTimeQuantiles;
  @Metric("Deferred Processing time") MutableRate deferredRpcProcessingTime;
  MutableQuantiles[] deferredRpcProcessingTimeQuantiles;
  @Metric("Number of authentication failures")
  MutableCounterLong rpcAuthenticationFailures;
  @Metric("Number of authentication successes")
  MutableCounterLong rpcAuthenticationSuccesses;
  @Metric("Number of authorization failures")
  MutableCounterLong rpcAuthorizationFailures;
  @Metric("Number of authorization successes")
  MutableCounterLong rpcAuthorizationSuccesses;
  @Metric("Number of client backoff requests")
  MutableCounterLong rpcClientBackoff;
  @Metric("Number of disconnected client backoff requests")
  MutableCounterLong rpcClientBackoffDisconnected;
  @Metric("Number of slow RPC calls")
  MutableCounterLong rpcSlowCalls;
  @Metric("Number of requeue calls")
  MutableCounterLong rpcRequeueCalls;
  @Metric("Number of successful RPC calls")
  MutableCounterLong rpcCallSuccesses;
  @Metric("Number of observer namenode rejected RPC calls")
  MutableCounterLong rpcCallsRejectedByObserver;

  @Metric("Number of open connections") public int numOpenConnections() {
    return server.getNumOpenConnections();
  }

  @Metric("Number of in process handlers")
  public int getNumInProcessHandler() {
    return server.getNumInProcessHandler();
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

  @Metric("Number of total requests")
  public long getTotalRequests() {
    return server.getTotalRequests();
  }

  @Metric("Number of total requests per second")
  public long getTotalRequestsPerSecond() {
    return server.getTotalRequestsPerSecond();
  }

  public TimeUnit getMetricsTimeUnit() {
    return metricsTimeUnit;
  }

  public static TimeUnit getMetricsTimeUnit(Configuration conf) {
    TimeUnit metricsTimeUnit = RpcMetrics.DEFAULT_METRIC_TIME_UNIT;
    String timeunit = conf.get(CommonConfigurationKeys.RPC_METRICS_TIME_UNIT);
    if (StringUtils.isNotEmpty(timeunit)) {
      try {
        metricsTimeUnit = TimeUnit.valueOf(timeunit);
      } catch (IllegalArgumentException e) {
        LOG.info("Config key {} 's value {} does not correspond to enum values"
                + " of java.util.concurrent.TimeUnit. Hence default unit"
                + " {} will be used",
            CommonConfigurationKeys.RPC_METRICS_TIME_UNIT, timeunit,
            RpcMetrics.DEFAULT_METRIC_TIME_UNIT);
      }
    }
    return metricsTimeUnit;
  }

  // Public instrumentation methods that could be extracted to an
  // abstract class if we decide to do custom instrumentation classes a la
  // JobTrackerInstrumentation. The methods with //@Override comment are
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
  public void shutdown() {
    DefaultMetricsSystem.instance().unregisterSource(name);
  }

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
   * Sometimes, the request time observed by the client is much longer than
   * the queue + process time on the RPC server.Perhaps the RPC request
   * 'waiting enQueue' took too long on the RPC server, so we should add
   * enQueue time to RpcMetrics. See HADOOP-18840 for details.
   * Add an RPC enqueue time sample
   * @param enQTime the queue time
   */
  public void addRpcEnQueueTime(long enQTime) {
    rpcEnQueueTime.add(enQTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : rpcEnQueueTimeQuantiles) {
        q.add(enQTime);
      }
    }
  }

  /**
   * Add an RPC queue time sample
   * @param qTime the queue time
   */
  public void addRpcQueueTime(long qTime) {
    rpcQueueTime.add(qTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : rpcQueueTimeQuantiles) {
        q.add(qTime);
      }
    }
  }

  public void addRpcLockWaitTime(long waitTime) {
    rpcLockWaitTime.add(waitTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : rpcLockWaitTimeQuantiles) {
        q.add(waitTime);
      }
    }
  }

  /**
   * Add an RPC processing time sample
   * @param processingTime the processing time
   */
  public void addRpcProcessingTime(long processingTime) {
    rpcProcessingTime.add(processingTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : rpcProcessingTimeQuantiles) {
        q.add(processingTime);
      }
    }
  }

  public void addRpcResponseTime(long responseTime) {
    rpcResponseTime.add(responseTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : rpcResponseTimeQuantiles) {
        q.add(responseTime);
      }
    }
  }

  public void addDeferredRpcProcessingTime(long processingTime) {
    deferredRpcProcessingTime.add(processingTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : deferredRpcProcessingTimeQuantiles) {
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
   * Client was disconnected due to backoff
   */
  public void incrClientBackoffDisconnected() {
    rpcClientBackoffDisconnected.incr();
  }

  /**
   * Returns the number of disconnected backoffs.
   * @return long
   */
  public long getClientBackoffDisconnected() {
    return rpcClientBackoffDisconnected.value();
  }


  /**
   * Increments the Slow RPC counter.
   */
  public  void incrSlowRpc() {
    rpcSlowCalls.incr();
  }

  /**
   * Increments the Requeue Calls counter.
   */
  public void incrRequeueCalls() {
    rpcRequeueCalls.incr();
  }

  /**
   * One RPC call success event.
   */
  public void incrRpcCallSuccesses() {
    rpcCallSuccesses.incr();
  }

  /**
   * Increments the Observer NameNode rejected RPC Calls Counter.
   */
  public void incrRpcCallsRejectedByObserver() {
    rpcCallsRejectedByObserver.incr();
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

  /**
   * Returns the number of requeue calls.
   * @return long
   */
  @VisibleForTesting
  public long getRpcRequeueCalls() {
    return rpcRequeueCalls.value();
  }

  /**
   * Returns the number of observer namenode rejected RPC calls.
   * @return long
   */
  @VisibleForTesting
  public long getRpcCallsRejectedByObserver() {
    return rpcCallsRejectedByObserver.value();
  }

  public MutableRate getDeferredRpcProcessingTime() {
    return deferredRpcProcessingTime;
  }

  public long getDeferredRpcProcessingSampleCount() {
    return deferredRpcProcessingTime.lastStat().numSamples();
  }

  public double getDeferredRpcProcessingMean() {
    return deferredRpcProcessingTime.lastStat().mean();
  }

  public double getDeferredRpcProcessingStdDev() {
    return deferredRpcProcessingTime.lastStat().stddev();
  }

  @VisibleForTesting
  public MetricsTag getTag(String tagName) {
    return registry.getTag(tagName);
  }

  @VisibleForTesting
  public MutableCounterLong getRpcAuthorizationSuccesses() {
    return rpcAuthorizationSuccesses;
  }
}
