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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for maintaining RPC method related statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about="Per method RPC metrics", context="rpcdetailed")
public class RpcDetailedMetrics {
  static final String DEFERRED_PREFIX = "Deferred";
  static final String OVERALL_PROCESSING_PREFIX = "Overall";

  // per-method RPC processing time
  @Metric MutableRatesWithAggregation rates;
  @Metric MutableRatesWithAggregation deferredRpcRates;
  /**
   * per-method overall RPC processing time, from request arrival to when the
   * response is sent back.
   */
  @Metric MutableRatesWithAggregation overallRpcProcessingRates;

  static final Logger LOG = LoggerFactory.getLogger(RpcDetailedMetrics.class);
  final MetricsRegistry registry;
  final String name;

  // Mainly to facilitate testing in TestRPC.java
  public MutableRatesWithAggregation getOverallRpcProcessingRates() {
    return overallRpcProcessingRates;
  }

  RpcDetailedMetrics(int port) {
    name = "RpcDetailedActivityForPort"+ port;
    registry = new MetricsRegistry("rpcdetailed")
        .tag("port", "RPC port", String.valueOf(port));
    LOG.debug(registry.info().toString());
  }

  public String name() { return name; }

  public static RpcDetailedMetrics create(int port) {
    RpcDetailedMetrics m = new RpcDetailedMetrics(port);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  /**
   * Initialize the metrics for JMX with protocol methods
   * @param protocol the protocol class
   */
  public void init(Class<?> protocol) {
    rates.init(protocol);
    deferredRpcRates.init(protocol, DEFERRED_PREFIX);
    overallRpcProcessingRates.init(protocol, OVERALL_PROCESSING_PREFIX);
  }

  /**
   * Add an RPC processing time sample
   * @param rpcCallName of the RPC call
   * @param processingTime  the processing time
   */
  //@Override // some instrumentation interface
  public void addProcessingTime(String rpcCallName, long processingTime) {
    rates.add(rpcCallName, processingTime);
  }

  public void addDeferredProcessingTime(String name, long processingTime) {
    deferredRpcRates.add(name, processingTime);
  }

  /**
   * Add an overall RPC processing time sample.
   * @param rpcCallName of the RPC call
   * @param overallProcessingTime  the overall RPC processing time
   */
  public void addOverallProcessingTime(String rpcCallName, long overallProcessingTime) {
    overallRpcProcessingRates.add(rpcCallName, overallProcessingTime);
  }

  /**
   * Shutdown the instrumentation for the process
   */
  //@Override // some instrumentation interface
  public void shutdown() {
    DefaultMetricsSystem.instance().unregisterSource(name);
  }
}
