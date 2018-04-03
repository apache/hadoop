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

  double getProxyAvg();

  long getProcessingOps();

  double getProcessingAvg();

  long getProxyOpFailureCommunicate();

  long getProxyOpFailureStandby();

  long getProxyOpNotImplemented();

  long getProxyOpRetries();

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
}
