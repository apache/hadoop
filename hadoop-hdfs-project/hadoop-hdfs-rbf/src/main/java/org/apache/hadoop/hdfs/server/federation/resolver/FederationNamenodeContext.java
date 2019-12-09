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

/**
 * Interface for a discovered NN and its current server endpoints.
 */
public interface FederationNamenodeContext {

  /**
   * Get the RPC server address of the namenode.
   *
   * @return RPC server address in the form of host:port.
   */
  String getRpcAddress();

  /**
   * Get the Service RPC server address of the namenode.
   *
   * @return Service RPC server address in the form of host:port.
   */
  String getServiceAddress();

  /**
   * Get the Lifeline RPC server address of the namenode.
   *
   * @return Lifeline RPC server address in the form of host:port.
   */
  String getLifelineAddress();

  /**
   * Get the HTTP server address of the namenode.
   *
   * @return HTTP address in the form of host:port.
   */
  String getWebAddress();

  /**
   * Get the unique key representing the namenode.
   *
   * @return Combination of the nameservice and the namenode IDs.
   */
  String getNamenodeKey();

  /**
   * Identifier for the nameservice/namespace.
   *
   * @return Namenode nameservice identifier.
   */
  String getNameserviceId();

  /**
   * Identifier for the namenode.
   *
   * @return String
   */
  String getNamenodeId();

  /**
   * The current state of the namenode (active, standby, etc).
   *
   * @return FederationNamenodeServiceState State of the namenode.
   */
  FederationNamenodeServiceState getState();

  /**
   * The update date.
   *
   * @return Long with the update date.
   */
  long getDateModified();
}
