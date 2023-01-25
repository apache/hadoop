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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * JMX interface for the RPC server.
 * TODO use the default RPC MBean.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FederationRPCMBean {

  long getProxyOps();

  long getActiveProxyOps();

  long getObserverProxyOps();

  double getProxyAvg();

  long getProcessingOps();

  double getProcessingAvg();

  long getProxyOpFailureCommunicate();

  long getProxyOpFailureStandby();

  long getProxyOpFailureClientOverloaded();

  long getProxyOpNotImplemented();

  long getProxyOpRetries();

  long getProxyOpNoNamenodes();

  long getRouterFailureStateStoreOps();

  long getRouterFailureReadOnlyOps();

  long getRouterFailureLockedOps();

  long getRouterFailureSafemodeOps();

  int getRpcServerCallQueue();

  /**
   * Get the number of RPC connections between the clients and the Router.
   * @return Number of RPC connections between the clients and the Router.
   */
  int getRpcServerNumOpenConnections();

  /**
   * Get the number of RPC connections between the Router and the NNs.
   * @return Number of RPC connections between the Router and the NNs.
   */
  int getRpcClientNumConnections();

  /**
   * Get the number of active RPC connections between the Router and the NNs.
   * @return Number of active RPC connections between the Router and the NNs.
   */
  int getRpcClientNumActiveConnections();

  /**
   * Get the number of idle RPC connections between the Router and the NNs.
   * @return Number of idle RPC connections between the Router and the NNs.
   */
  int getRpcClientNumIdleConnections();

  /**
   * Get the number of recently active RPC connections between
   * the Router and the NNs.
   *
   * @return Number of recently active RPC connections between
   * the Router and the NNs.
   */
  int getRpcClientNumActiveConnectionsRecently();

  /**
   * Get the number of RPC connections to be created.
   * @return Number of RPC connections to be created.
   */
  int getRpcClientNumCreatingConnections();

  /**
   * Get the number of connection pools between the Router and a NNs.
   * @return Number of connection pools between the Router and a NNs.
   */
  int getRpcClientNumConnectionPools();

  /**
   * JSON representation of the RPC connections from the Router to the NNs.
   * @return JSON string representation.
   */
  String getRpcClientConnections();

  /**
   * JSON representation of the available handler per Ns.
   * @return JSON string representation.
   */
  String getAvailableHandlerOnPerNs();

  /**
   * Get the JSON representation of the async caller thread pool.
   * @return JSON string representation of the async caller thread pool.
   */
  String getAsyncCallerPool();

  /**
   * Get the number of operations rejected due to lack of permits.
   * @return Number of operations rejected due to lack of permits.
   */
  long getProxyOpPermitRejected();

  /**
   * Get the number of operations rejected due to lack of permits of each namespace.
   * @return Number of operations rejected due to lack of permits of each namespace.
   */
  String getProxyOpPermitRejectedPerNs();

  /**
   * Get the number of operations accepted of each namespace.
   * @return Number of operations accepted of each namespace.
   */
  String getProxyOpPermitAcceptedPerNs();
}
