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
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.AbstractMetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

/**
 * The RPC metrics instrumentation
 */
public class RpcInstrumentation implements MetricsSource {

  static final Log LOG = LogFactory.getLog(RpcInstrumentation.class);

  final MetricsRegistry registry = new MetricsRegistry("rpc");
  final MetricMutableCounterInt authenticationSuccesses =
      registry.newCounter("rpcAuthenticationSuccesses",
                          "RPC authentication successes count", 0);
  final MetricMutableCounterInt authenticationFailures =
      registry.newCounter("rpcAuthenticationFailures",
                          "RPC authentication failures count", 0);
  final MetricMutableCounterInt authorizationSuccesses =
      registry.newCounter("rpcAuthorizationSuccesses",
                          "RPC authorization successes count", 0);
  final MetricMutableCounterInt authorizationFailures =
      registry.newCounter("rpcAuthorizationFailures",
                          "RPC authorization failures count", 0);
  final MetricMutableCounterLong receivedBytes =
      registry.newCounter("ReceivedBytes", "RPC received bytes count", 0L);
  final MetricMutableCounterLong sentBytes =
      registry.newCounter("SentBytes", "RPC sent bytes count", 0L);
  final MetricMutableStat rpcQueueTime = registry.newStat("RpcQueueTime",
      "RPC queue time stats", "ops", "time");
  final MetricMutableStat rpcProcessingTime = registry.newStat(
      "RpcProcessingTime", "RPC processing time", "ops", "time");
  final MetricMutableGaugeInt numOpenConnections = registry.newGauge(
      "NumOpenConnections", "Number of open connections", 0);
  final MetricMutableGaugeInt callQueueLen = registry.newGauge("callQueueLen",
      "RPC call queue length", 0);

  final Detailed detailed;

  RpcInstrumentation(String serverName, int port) {
    String portStr = String.valueOf(port);
    registry.setContext("rpc").tag("port", "RPC port", portStr);
    detailed = new Detailed(portStr);
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

  /**
   * Create an RPC instrumentation object
   * @param serverName  name of the server
   * @param port  the RPC port
   * @return the instrumentation object
   */
  public static RpcInstrumentation create(String serverName, int port) {
    return create(serverName, port, DefaultMetricsSystem.INSTANCE);
  }

  /**
   * Create an RPC instrumentation object
   * Mostly useful for testing.
   * @param serverName  name of the server
   * @param port  the RPC port
   * @param ms  the metrics system object
   * @return the instrumentation object
   */
  public static RpcInstrumentation create(String serverName, int port,
                                          MetricsSystem ms) {
    RpcInstrumentation rpc = new RpcInstrumentation(serverName, port);
    ms.register("RpcDetailedActivityForPort"+ port, "Per call", rpc.detailed());
    return ms.register("RpcActivityForPort"+ port, "Aggregate metrics", rpc);
  }

  /**
   * @return the detailed (per call) metrics source for RPC
   */
  public MetricsSource detailed() {
    return detailed;
  }

  // Start of public instrumentation methods that could be extracted to an
  // abstract class if we decide to allow custom instrumentation classes a la
  // JobTrackerInstrumenation. The methods with //@Override comment are
  // candidates for abstract methods in a abstract instrumentation class

  /**
   * One authentication failure event
   */
  //@Override
  public void incrAuthenticationFailures() {
    this.authenticationFailures.incr();
  }

  /**
   * One authentication success event
   */
  //@Override
  public void incrAuthenticationSuccesses() {
    this.authenticationSuccesses.incr();
  }

  /**
   * One authorization success event
   */
  //@Override
  public void incrAuthorizationSuccesses() {
    this.authorizationSuccesses.incr();
  }

  /**
   * One authorization failure event
   */
  //@Override
  public void incrAuthorizationFailures() {
    this.authorizationFailures.incr();
  }

  /**
   * Shutdown the instrumentation for the process
   */
  //@Override
  public void shutdown() {
    LOG.info("shut down");
  }

  /**
   * Increment sent bytes by count
   * @param count to increment
   */
  //@Override
  public void incrSentBytes(int count) {
    this.sentBytes.incr(count);
  }

  /**
   * Increment received bytes by count
   * @param count to increment
   */
  //@Override
  public void incrReceivedBytes(int count) {
    this.receivedBytes.incr(count);
  }

  /**
   * Add an RPC queue time sample
   * @param qTime
   */
  //@Override
  public void addRpcQueueTime(int qTime) {
    this.rpcQueueTime.add(qTime);
  }

  /**
   * Add an RPC processing time sample
   * @param processingTime
   */
  //@Override
  public void addRpcProcessingTime(int processingTime) {
    this.rpcProcessingTime.add(processingTime);
  }

  /**
   * Add an RPC processing time sample for a particular RPC method
   * @param methodName  method name of the RPC
   * @param processingTime  elapsed processing time of the RPC
   */
  //@Override
  public void addRpcProcessingTime(String methodName, int processingTime) {
    detailed.addRpcProcessingTime(methodName, processingTime);
  }

  /**
   * Use a separate source for detailed (per call) RPC metrics for
   * easy and efficient filtering
   */
  public static class Detailed extends AbstractMetricsSource {

    Detailed(String port) {
      super("rpcdetailed");
      registry.setContext("rpcdetailed").tag("port", "RPC port", port);
    }

    public synchronized void addRpcProcessingTime(String methodName,
                                                  int processingTime) {
      registry.add(methodName, processingTime);
    }

  }

}
