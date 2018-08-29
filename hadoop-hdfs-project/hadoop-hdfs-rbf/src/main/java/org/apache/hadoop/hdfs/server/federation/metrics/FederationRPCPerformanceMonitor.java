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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcMonitor;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.metrics2.util.MBeans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Customizable RPC performance monitor. Receives events from the RPC server
 * and aggregates them via JMX.
 */
public class FederationRPCPerformanceMonitor implements RouterRpcMonitor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRPCPerformanceMonitor.class);


  /** Time for an operation to be received in the Router. */
  private static final ThreadLocal<Long> START_TIME = new ThreadLocal<>();
  /** Time for an operation to be send to the Namenode. */
  private static final ThreadLocal<Long> PROXY_TIME = new ThreadLocal<>();

  /** Configuration for the performance monitor. */
  private Configuration conf;
  /** RPC server for the Router. */
  private RouterRpcServer server;
  /** State Store. */
  private StateStoreService store;

  /** JMX interface to monitor the RPC metrics. */
  private FederationRPCMetrics metrics;
  private ObjectName registeredBean;

  /** Thread pool for logging stats. */
  private ExecutorService executor;


  @Override
  public void init(Configuration configuration, RouterRpcServer rpcServer,
      StateStoreService stateStore) {

    this.conf = configuration;
    this.server = rpcServer;
    this.store = stateStore;

    // Create metrics
    this.metrics = FederationRPCMetrics.create(conf, server);

    // Create thread pool
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("Federation RPC Performance Monitor-%d").build();
    this.executor = Executors.newFixedThreadPool(1, threadFactory);

    // Adding JMX interface
    try {
      StandardMBean bean =
          new StandardMBean(this.metrics, FederationRPCMBean.class);
      registeredBean = MBeans.register("Router", "FederationRPC", bean);
      LOG.info("Registered FederationRPCMBean: {}", registeredBean);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad FederationRPCMBean setup", e);
    }
  }

  @Override
  public void close() {
    if (registeredBean != null) {
      MBeans.unregister(registeredBean);
      registeredBean = null;
    }
    if (this.executor != null) {
      this.executor.shutdown();
    }
  }

  /**
   * Resets all RPC service performance counters to their defaults.
   */
  public void resetPerfCounters() {
    if (registeredBean != null) {
      MBeans.unregister(registeredBean);
      registeredBean = null;
    }
    if (metrics != null) {
      FederationRPCMetrics.reset();
      metrics = null;
    }
    init(conf, server, store);
  }

  @Override
  public void startOp() {
    START_TIME.set(monotonicNow());
  }

  @Override
  public long proxyOp() {
    PROXY_TIME.set(monotonicNow());
    long processingTime = getProcessingTime();
    if (processingTime >= 0) {
      metrics.addProcessingTime(processingTime);
    }
    return Thread.currentThread().getId();
  }

  @Override
  public void proxyOpComplete(boolean success) {
    if (success) {
      long proxyTime = getProxyTime();
      if (proxyTime >= 0) {
        metrics.addProxyTime(proxyTime);
      }
    }
  }

  @Override
  public void proxyOpFailureStandby() {
    metrics.incrProxyOpFailureStandby();
  }

  @Override
  public void proxyOpFailureCommunicate() {
    metrics.incrProxyOpFailureCommunicate();
  }

  @Override
  public void proxyOpFailureClientOverloaded() {
    metrics.incrProxyOpFailureClientOverloaded();
  }

  @Override
  public void proxyOpNotImplemented() {
    metrics.incrProxyOpNotImplemented();
  }

  @Override
  public void proxyOpRetries() {
    metrics.incrProxyOpRetries();
  }

  @Override
  public void routerFailureStateStore() {
    metrics.incrRouterFailureStateStore();
  }

  @Override
  public void routerFailureSafemode() {
    metrics.incrRouterFailureSafemode();
  }

  @Override
  public void routerFailureReadOnly() {
    metrics.incrRouterFailureReadOnly();
  }

  @Override
  public void routerFailureLocked() {
    metrics.incrRouterFailureLocked();
  }


  /**
   * Get time between we receiving the operation and sending it to the Namenode.
   * @return Processing time in nanoseconds.
   */
  private long getProcessingTime() {
    if (START_TIME.get() != null && START_TIME.get() > 0 &&
        PROXY_TIME.get() != null && PROXY_TIME.get() > 0) {
      return PROXY_TIME.get() - START_TIME.get();
    }
    return -1;
  }

  /**
   * Get time between now and when the operation was forwarded to the Namenode.
   * @return Current proxy time in nanoseconds.
   */
  private long getProxyTime() {
    if (PROXY_TIME.get() != null && PROXY_TIME.get() > 0) {
      return monotonicNow() - PROXY_TIME.get();
    }
    return -1;
  }

  @Override
  public FederationRPCMetrics getRPCMetrics() {
    return this.metrics;
  }
}