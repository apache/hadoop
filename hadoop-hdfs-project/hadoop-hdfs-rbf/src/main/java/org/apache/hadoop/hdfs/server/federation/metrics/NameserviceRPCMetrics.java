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
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Implementation of the Nameservice RPC metrics collector.
 */
@Metrics(name = "NameserviceRPCActivity", about = "Nameservice RPC Activity",
    context = "dfs")
public class NameserviceRPCMetrics implements NameserviceRPCMBean {

  public final static String NAMESERVICE_RPC_METRICS_PREFIX = "NameserviceActivity-";

  private final String nsId;

  @Metric("Time for the Router to proxy an operation to the Nameservice")
  private MutableRate proxy;
  @Metric("Number of operations the Router proxied to a NameService")
  private MutableCounterLong proxyOp;

  @Metric("Number of operations to hit a standby NN")
  private MutableCounterLong proxyOpFailureStandby;
  @Metric("Number of operations to fail to reach NN")
  private MutableCounterLong proxyOpFailureCommunicate;
  @Metric("Number of operations to hit no namenodes available")
  private MutableCounterLong proxyOpNoNamenodes;

  public NameserviceRPCMetrics(Configuration conf, String nsId) {
    this.nsId = nsId;
  }

  public static NameserviceRPCMetrics create(Configuration conf,
      String nameService) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = NAMESERVICE_RPC_METRICS_PREFIX + (nameService.isEmpty()
        ? "UndefinedNameService"+ ThreadLocalRandom.current().nextInt()
        : nameService);
    return ms.register(name, "HDFS Federation NameService RPC Metrics",
        new NameserviceRPCMetrics(conf, name));
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

  public String getNsId() {
    return this.nsId;
  }
}