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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Implementation of the RPC metrics collector.
 */
@Metrics(name = "RouterRPCActivity", about = "Router RPC Activity",
    context = "dfs")
public class FederationRPCMetrics implements FederationRPCMBean {

  private final MetricsRegistry registry = new MetricsRegistry("router");

  private RouterRpcServer rpcServer;

  @Metric("Time for the router to process an operation internally")
  private MutableRate processing;
  @Metric("Number of operations the Router processed internally")
  private MutableCounterLong processingOp;
  @Metric("Time for the Router to proxy an operation to the Namenodes")
  private MutableRate proxy;
  @Metric("Number of operations the Router proxied to a Namenode")
  private MutableCounterLong proxyOp;

  @Metric("Number of operations to fail to reach NN")
  private MutableCounterLong proxyOpFailureStandby;
  @Metric("Number of operations to hit a standby NN")
  private MutableCounterLong proxyOpFailureCommunicate;
  @Metric("Number of operations to hit a client overloaded Router")
  private MutableCounterLong proxyOpFailureClientOverloaded;
  @Metric("Number of operations not implemented")
  private MutableCounterLong proxyOpNotImplemented;
  @Metric("Number of operation retries")
  private MutableCounterLong proxyOpRetries;
  @Metric("Number of operations to hit no namenodes available")
  private MutableCounterLong proxyOpNoNamenodes;

  @Metric("Failed requests due to State Store unavailable")
  private MutableCounterLong routerFailureStateStore;
  @Metric("Failed requests due to read only mount point")
  private MutableCounterLong routerFailureReadOnly;
  @Metric("Failed requests due to locked path")
  private MutableCounterLong routerFailureLocked;
  @Metric("Failed requests due to safe mode")
  private MutableCounterLong routerFailureSafemode;

  public FederationRPCMetrics(Configuration conf, RouterRpcServer rpcServer) {
    this.rpcServer = rpcServer;

    registry.tag(SessionId, "RouterRPCSession");
    registry.tag(ProcessName, "Router");
  }

  public static FederationRPCMetrics create(Configuration conf,
      RouterRpcServer rpcServer) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(FederationRPCMetrics.class.getName(),
        "HDFS Federation RPC Metrics",
        new FederationRPCMetrics(conf, rpcServer));
  }

  /**
   * Reset the metrics system.
   */
  public static void reset() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(FederationRPCMetrics.class.getName());
  }

  public void incrProxyOpFailureStandby() {
    proxyOpFailureStandby.incr();
  }

  @Override
  public long getProxyOpFailureStandby() {
    return proxyOpFailureStandby.value();
  }

  public void incrProxyOpFailureCommunicate() {
    proxyOpFailureCommunicate.incr();
  }

  @Override
  public long getProxyOpFailureCommunicate() {
    return proxyOpFailureCommunicate.value();
  }

  public void incrProxyOpFailureClientOverloaded() {
    proxyOpFailureClientOverloaded.incr();
  }

  @Override
  public long getProxyOpFailureClientOverloaded() {
    return proxyOpFailureClientOverloaded.value();
  }

  public void incrProxyOpNotImplemented() {
    proxyOpNotImplemented.incr();
  }

  @Override
  public long getProxyOpNotImplemented() {
    return proxyOpNotImplemented.value();
  }

  public void incrProxyOpRetries() {
    proxyOpRetries.incr();
  }

  @Override
  public long getProxyOpRetries() {
    return proxyOpRetries.value();
  }

  public void incrProxyOpNoNamenodes() {
    proxyOpNoNamenodes.incr();
  }

  @Override
  public long getProxyOpNoNamenodes() {
    return proxyOpNoNamenodes.value();
  }

  public void incrRouterFailureStateStore() {
    routerFailureStateStore.incr();
  }

  @Override
  public long getRouterFailureStateStoreOps() {
    return routerFailureStateStore.value();
  }

  public void incrRouterFailureSafemode() {
    routerFailureSafemode.incr();
  }

  @Override
  public long getRouterFailureSafemodeOps() {
    return routerFailureSafemode.value();
  }

  public void incrRouterFailureReadOnly() {
    routerFailureReadOnly.incr();
  }

  @Override
  public long getRouterFailureReadOnlyOps() {
    return routerFailureReadOnly.value();
  }

  public void incrRouterFailureLocked() {
    routerFailureLocked.incr();
  }

  @Override
  public long getRouterFailureLockedOps() {
    return routerFailureLocked.value();
  }

  @Override
  public int getRpcServerCallQueue() {
    return rpcServer.getServer().getCallQueueLen();
  }

  @Override
  public int getRpcServerNumOpenConnections() {
    return rpcServer.getServer().getNumOpenConnections();
  }

  @Override
  public int getRpcClientNumConnections() {
    return rpcServer.getRPCClient().getNumConnections();
  }

  @Override
  public int getRpcClientNumActiveConnections() {
    return rpcServer.getRPCClient().getNumActiveConnections();
  }

  @Override
  public int getRpcClientNumCreatingConnections() {
    return rpcServer.getRPCClient().getNumCreatingConnections();
  }

  @Override
  public int getRpcClientNumConnectionPools() {
    return rpcServer.getRPCClient().getNumConnectionPools();
  }

  @Override
  public String getRpcClientConnections() {
    return rpcServer.getRPCClient().getJSON();
  }

  @Override
  public String getAsyncCallerPool() {
    return rpcServer.getRPCClient().getAsyncCallerPoolJson();
  }

  /**
   * Add the time to proxy an operation from the moment the Router sends it to
   * the Namenode until it replied.
   * @param time Proxy time of an operation in nanoseconds.
   */
  public void addProxyTime(long time) {
    proxy.add(time);
    proxyOp.incr();
  }

  @Override
  public double getProxyAvg() {
    return proxy.lastStat().mean();
  }

  @Override
  public long getProxyOps() {
    return proxyOp.value();
  }

  /**
   * Add the time to process a request in the Router from the time we receive
   * the call until we send it to the Namenode.
   * @param time Process time of an operation in nanoseconds.
   */
  public void addProcessingTime(long time) {
    processing.add(time);
    processingOp.incr();
  }

  @Override
  public double getProcessingAvg() {
    return processing.lastStat().mean();
  }

  @Override
  public long getProcessingOps() {
    return processingOp.value();
  }
}
