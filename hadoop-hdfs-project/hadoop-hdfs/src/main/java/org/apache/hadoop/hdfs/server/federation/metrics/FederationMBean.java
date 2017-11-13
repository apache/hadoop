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
 * JMX interface for the federation statistics.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FederationMBean {

  /**
   * Get information about all the namenodes in the federation or null if
   * failure.
   * @return JSON with all the Namenodes.
   */
  String getNamenodes();

  /**
   * Get the latest info for each registered nameservice.
   * @return JSON with all the nameservices.
   */
  String getNameservices();

  /**
   * Get the mount table for the federated filesystem or null if failure.
   * @return JSON with the mount table.
   */
  String getMountTable();

  /**
   * Get the total capacity of the federated cluster.
   * @return Total capacity of the federated cluster.
   */
  long getTotalCapacity();

  /**
   * Get the used capacity of the federated cluster.
   * @return Used capacity of the federated cluster.
   */
  long getUsedCapacity();

  /**
   * Get the remaining capacity of the federated cluster.
   * @return Remaining capacity of the federated cluster.
   */
  long getRemainingCapacity();

  /**
   * Get the number of nameservices in the federation.
   * @return Number of nameservices in the federation.
   */
  int getNumNameservices();

  /**
   * Get the number of namenodes.
   * @return Number of namenodes.
   */
  int getNumNamenodes();

  /**
   * Get the number of expired namenodes.
   * @return Number of expired namenodes.
   */
  int getNumExpiredNamenodes();

  /**
   * Get the number of live datanodes.
   * @return Number of live datanodes.
   */
  int getNumLiveNodes();

  /**
   * Get the number of dead datanodes.
   * @return Number of dead datanodes.
   */
  int getNumDeadNodes();

  /**
   * Get the number of decommissioning datanodes.
   * @return Number of decommissioning datanodes.
   */
  int getNumDecommissioningNodes();

  /**
   * Get the number of live decommissioned datanodes.
   * @return Number of live decommissioned datanodes.
   */
  int getNumDecomLiveNodes();

  /**
   * Get the number of dead decommissioned datanodes.
   * @return Number of dead decommissioned datanodes.
   */
  int getNumDecomDeadNodes();

  /**
   * Get Max, Median, Min and Standard Deviation of DataNodes usage.
   * @return the DataNode usage information, as a JSON string.
   */
  String getNodeUsage();

  /**
   * Get the number of blocks in the federation.
   * @return Number of blocks in the federation.
   */
  long getNumBlocks();

  /**
   * Get the number of missing blocks in the federation.
   * @return Number of missing blocks in the federation.
   */
  long getNumOfMissingBlocks();

  /**
   * Get the number of pending replication blocks in the federation.
   * @return Number of pending replication blocks in the federation.
   */
  long getNumOfBlocksPendingReplication();

  /**
   * Get the number of under replicated blocks in the federation.
   * @return Number of under replicated blocks in the federation.
   */
  long getNumOfBlocksUnderReplicated();

  /**
   * Get the number of pending deletion blocks in the federation.
   * @return Number of pending deletion blocks in the federation.
   */
  long getNumOfBlocksPendingDeletion();

  /**
   * Get the number of files in the federation.
   * @return Number of files in the federation.
   */
  long getNumFiles();

  /**
   * When the router started.
   * @return Date as a string the router started.
   */
  String getRouterStarted();

  /**
   * Get the version of the router.
   * @return Version of the router.
   */
  String getVersion();

  /**
   * Get the compilation date of the router.
   * @return Compilation date of the router.
   */
  String getCompiledDate();

  /**
   * Get the compilation info of the router.
   * @return Compilation info of the router.
   */
  String getCompileInfo();

  /**
   * Get the host and port of the router.
   * @return Host and port of the router.
   */
  String getHostAndPort();

  /**
   * Get the identifier of the router.
   * @return Identifier of the router.
   */
  String getRouterId();

  /**
   * Get the host and port of the router.
   * @return Host and port of the router.
   */
  String getClusterId();

  /**
   * Get the host and port of the router.
   * @return Host and port of the router.
   */
  String getBlockPoolId();

  /**
   * Get the current state of the router.
   *
   * @return String label for the current router state.
   */
  String getRouterStatus();
}
