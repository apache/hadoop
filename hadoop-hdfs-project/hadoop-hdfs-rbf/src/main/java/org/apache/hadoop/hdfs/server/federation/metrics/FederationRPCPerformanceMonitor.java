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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcMonitor;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.metrics2.util.MBeans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

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
  /** Time for an operation to be sent to the Namenode. */
  private static final ThreadLocal<Long> PROXY_TIME = new ThreadLocal<>();

  /** Configuration for the performance monitor. */
  private Configuration conf;
  /** RPC server for the Router. */
  private RouterRpcServer server;
  /** State Store. */
  private StateStoreService store;

  /** JMX interface to monitor the RPC metrics. */
  private FederationRPCMetrics metrics;
  /** JMX interface to monitor each Nameservice RPC metrics. */
  private Map<String, NameserviceRPCMetrics> nameserviceRPCMetricsMap =
      new ConcurrentHashMap<>();
  private ObjectName registeredBean;

  /** Thread pool for logging stats. */
  private ExecutorService executor;

  public static final String CONCURRENT = "concurrent";

  @Override
  public void init(Configuration configuration, RouterRpcServer rpcServer,
      StateStoreService stateStore) {

    this.conf = configuration;
    this.server = rpcServer;
    this.store = stateStore;

    // Create metrics
    this.metrics = FederationRPCMetrics.create(conf, server);
    for (String nameservice : FederationUtil.getAllConfiguredNS(conf)) {
      LOG.info("Create Nameservice RPC Metrics for {}", nameservice);
      this.nameserviceRPCMetricsMap.computeIfAbsent(nameservice,
          k -> NameserviceRPCMetrics.create(conf, k));
    }
    LOG.info("Create Nameservice RPC Metrics for {}", CONCURRENT);
    this.nameserviceRPCMetricsMap.computeIfAbsent(CONCURRENT,
        k -> NameserviceRPCMetrics.create(conf, k));

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
    if (metrics != null && processingTime >= 0) {
      metrics.addProcessingTime(processingTime);
    }
    return Thread.currentThread().getId();
  }

  @Override
  public void proxyOpComplete(boolean success, String nsId,
      FederationNamenodeServiceState state) {
    if (success) {
      long proxyTime = getProxyTime();
      if (proxyTime >= 0) {
        if (metrics != null && !CONCURRENT.equals(nsId)) {
          metrics.addProxyTime(proxyTime, state);
        }
        if (nameserviceRPCMetricsMap != null &&
            nameserviceRPCMetricsMap.containsKey(nsId)) {
          nameserviceRPCMetricsMap.get(nsId).addProxyTime(proxyTime);
        }
      }
    }
  }

  @Override
  public void proxyOpFailureStandby(String nsId) {
    if (metrics != null) {
      metrics.incrProxyOpFailureStandby();
    }
    if (nameserviceRPCMetricsMap != null &&
        nameserviceRPCMetricsMap.containsKey(nsId)) {
      nameserviceRPCMetricsMap.get(nsId).incrProxyOpFailureStandby();
    }
  }

  @Override
  public void proxyOpFailureCommunicate(String nsId) {
    if (metrics != null) {
      metrics.incrProxyOpFailureCommunicate();
    }
    if (nameserviceRPCMetricsMap != null &&
        nameserviceRPCMetricsMap.containsKey(nsId)) {
      nameserviceRPCMetricsMap.get(nsId).incrProxyOpFailureCommunicate();
    }
  }

  @Override
  public void proxyOpPermitRejected(String nsId) {
    if (metrics != null) {
      metrics.incrProxyOpPermitRejected();
    }
    if (nameserviceRPCMetricsMap != null &&
        nameserviceRPCMetricsMap.containsKey(nsId)) {
      nameserviceRPCMetricsMap.get(nsId).incrProxyOpPermitRejected();
    }
  }

  @Override
  public void proxyOpPermitAccepted(String nsId) {
    if (nameserviceRPCMetricsMap != null &&
        nameserviceRPCMetricsMap.containsKey(nsId)) {
      nameserviceRPCMetricsMap.get(nsId).incrProxyOpPermitAccepted();
    }
  }

  @Override
  public void proxyOpFailureClientOverloaded() {
    if (metrics != null) {
      metrics.incrProxyOpFailureClientOverloaded();
    }
  }

  @Override
  public void proxyOpNotImplemented() {
    if (metrics != null) {
      metrics.incrProxyOpNotImplemented();
    }
  }

  @Override
  public void proxyOpRetries() {
    if (metrics != null) {
      metrics.incrProxyOpRetries();
    }
  }

  @Override
  public void proxyOpNoNamenodes(String nsId) {
    if (metrics != null) {
      metrics.incrProxyOpNoNamenodes();
    }
    if (nameserviceRPCMetricsMap != null &&
        nameserviceRPCMetricsMap.containsKey(nsId)) {
      nameserviceRPCMetricsMap.get(nsId).incrProxyOpNoNamenodes();
    }
  }

  @Override
  public void routerFailureStateStore() {
    if (metrics != null) {
      metrics.incrRouterFailureStateStore();
    }
  }

  @Override
  public void routerFailureSafemode() {
    if (metrics != null) {
      metrics.incrRouterFailureSafemode();
    }
  }

  @Override
  public void routerFailureReadOnly() {
    if (metrics != null) {
      metrics.incrRouterFailureReadOnly();
    }
  }

  @Override
  public void routerFailureLocked() {
    if (metrics != null) {
      metrics.incrRouterFailureLocked();
    }
  }


  /**
   * Get time between we receiving the operation and sending it to the Namenode.
   * @return Processing time in milliseconds.
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
   * @return Current proxy time in milliseconds.
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