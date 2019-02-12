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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;

/**
 * Metrics and monitoring interface for the router RPC server. Allows pluggable
 * diagnostics and monitoring services to be attached.
 */
public interface RouterRpcMonitor {

  /**
   * Initialize the monitor.
   * @param conf Configuration for the monitor.
   * @param server RPC server.
   * @param store State Store.
   */
  void init(
      Configuration conf, RouterRpcServer server, StateStoreService store);

  /**
   * Get Router RPC metrics info.
   * @return The instance of FederationRPCMetrics.
   */
  FederationRPCMetrics getRPCMetrics();

  /**
   * Close the monitor.
   */
  void close();

  /**
   * Start processing an operation on the Router.
   */
  void startOp();

  /**
   * Start proxying an operation to the Namenode.
   * @return Id of the thread doing the proxying.
   */
  long proxyOp();

  /**
   * Mark a proxy operation as completed.
   * @param success If the operation was successful.
   */
  void proxyOpComplete(boolean success);

  /**
   * Failed to proxy an operation to a Namenode because it was in standby.
   */
  void proxyOpFailureStandby();

  /**
   * Failed to proxy an operation to a Namenode because of an unexpected
   * exception.
   */
  void proxyOpFailureCommunicate();

  /**
   * Failed to proxy an operation to a Namenode because the client was
   * overloaded.
   */
  void proxyOpFailureClientOverloaded();

  /**
   * Failed to proxy an operation because it is not implemented.
   */
  void proxyOpNotImplemented();

  /**
   * Retry to proxy an operation to a Namenode because of an unexpected
   * exception.
   */
  void proxyOpRetries();

  /**
   * Failed to proxy an operation because of no namenodes available.
   */
  void proxyOpNoNamenodes();

  /**
   * If the Router cannot contact the State Store in an operation.
   */
  void routerFailureStateStore();

  /**
   * If the Router is in safe mode.
   */
  void routerFailureSafemode();

  /**
   * If a path is locked.
   */
  void routerFailureLocked();

  /**
   * If a path is in a read only mount point.
   */
  void routerFailureReadOnly();
}
