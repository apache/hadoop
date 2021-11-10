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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Implementation of the RPC metrics collector.
 */
@Metrics(name = "SubClusterRPCActivity", about = "SubCluster RPC Activity",
    context = "dfs")
public class SubClusterRPCMetrics implements SubClusterRPCMBean {

  public final static String SUB_CLUSTER_RPC_METRICS_PREFIX = "SubClusterActivity-";

  private final String name;

  @Metric("Time for the Router to proxy an operation to the Nameservice")
  private MutableRate proxy;
  @Metric("Number of operations the Router proxied to a Nameservice")
  private MutableCounterLong proxyOp;

  @Metric("Number of operations to hit a standby NN")
  private MutableCounterLong proxyOpFailureStandby;
  @Metric("Number of operations to fail to reach NN")
  private MutableCounterLong proxyOpFailureCommunicate;
  @Metric("Number of operations to hit no namenodes available")
  private MutableCounterLong proxyOpNoNamenodes;

  public SubClusterRPCMetrics(Configuration conf, String name) {
    this.name = name;
  }

  public static SubClusterRPCMetrics create(Configuration conf,
      String subClusterName) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = SUB_CLUSTER_RPC_METRICS_PREFIX + (subClusterName.isEmpty()
        ? "UndefinedSubClusterName"+ ThreadLocalRandom.current().nextInt()
        : subClusterName);
    return ms.register(name, "HDFS Federation SubCluster RPC Metrics",
        new SubClusterRPCMetrics(conf, name));
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

  public void incrProxyOpNoNamenodes() {
    proxyOpNoNamenodes.incr();
  }

  @Override
  public long getProxyOpNoNamenodes() {
    return proxyOpNoNamenodes.value();
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

  public String getName() {
    return name;
  }
}