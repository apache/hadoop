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
package org.apache.hadoop.hdfs.server.federation.resolver;

import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

/**
 * Namenode state in the federation. The order of this enum is used to evaluate
 * NN priority for RPC calls.
 */
public enum FederationNamenodeServiceState {
  ACTIVE, // HAServiceState.ACTIVE or operational.
  STANDBY, // HAServiceState.STANDBY.
  UNAVAILABLE, // When the namenode cannot be reached.
  EXPIRED, // When the last update is too old.
  DISABLED; // When the nameservice is disabled.

  public static FederationNamenodeServiceState getState(HAServiceState state) {
    switch(state) {
    case ACTIVE:
      return FederationNamenodeServiceState.ACTIVE;
    case STANDBY:
    // TODO: we should probably have a separate state OBSERVER for RBF and
    // treat it differently.
    case OBSERVER:
      return FederationNamenodeServiceState.STANDBY;
    case INITIALIZING:
      return FederationNamenodeServiceState.UNAVAILABLE;
    case STOPPING:
      return FederationNamenodeServiceState.UNAVAILABLE;
    default:
      return FederationNamenodeServiceState.UNAVAILABLE;
    }
  }
}