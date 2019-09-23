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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;

/**
 * This is the JMX management interface for namenode information.
 * End users shouldn't be implementing these interfaces, and instead
 * access this information through the JMX APIs.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface NameNodeMXBean {

  /**
   * Gets the version of Hadoop.
   * 
   * @return the version.
   */
  String getVersion();

  /**
   * Get the version of software running on the Namenode.
   *
   * @return a string representing the version.
   */
  String getSoftwareVersion();

  /**
   * Gets the used space by data nodes.
   * 
   * @return the used space by data nodes.
   */
  long getUsed();
  
  /**
   * Gets total non-used raw bytes.
   * 
   * @return total non-used raw bytes.
   */
  long getFree();
  
  /**
   * Gets total raw bytes including non-dfs used space.
   * 
   * @return the total raw bytes including non-dfs used space.
   */
  long getTotal();

  /**
   * Gets capacity of the provided storage mounted, in bytes.
   *
   * @return the total raw bytes present in the provided storage.
   */
  long getProvidedCapacity();

  /**
   * Gets the safemode status.
   * 
   * @return the safemode status.
   */
  String getSafemode();
  
  /**
   * Checks if upgrade is finalized.
   * 
   * @return true, if upgrade is finalized.
   */
  boolean isUpgradeFinalized();

  /**
   * Gets the RollingUpgrade information.
   *
   * @return Rolling upgrade information if an upgrade is in progress. Else
   * (e.g. if there is no upgrade or the upgrade is finalized), returns null.
   */
  RollingUpgradeInfo.Bean getRollingUpgradeStatus();

  /**
   * Gets total used space by data nodes for non DFS purposes such as storing
   * temporary files on the local file system.
   * 
   * @return the non dfs space of the cluster.
   */
  long getNonDfsUsedSpace();
  
  /**
   * Gets the total used space by data nodes as percentage of total capacity.
   * 
   * @return the percentage of used space on the cluster.
   */
  float getPercentUsed();
  
  /**
   * Gets the total remaining space by data nodes as percentage of total 
   * capacity.
   * 
   * @return the percentage of the remaining space on the cluster.
   */
  float getPercentRemaining();

  /**
   * Gets the amount of cache used by the datanode (in bytes).
   *
   * @return the amount of cache used by the datanode (in bytes).
   */
  long getCacheUsed();

  /**
   * Gets the total cache capacity of the datanode (in bytes).
   *
   * @return the total cache capacity of the datanode (in bytes).
   */
  long getCacheCapacity();
  
  /**
   * Get the total space used by the block pools of this namenode.
   *
   * @return the total space used by the block pools of this namenode.
   */
  long getBlockPoolUsedSpace();
  
  /**
   * Get the total space used by the block pool as percentage of total capacity.
   *
   * @return the total space used by the block pool as percentage of total
   * capacity.
   */
  float getPercentBlockPoolUsed();
    
  /**
   * Gets the total numbers of blocks on the cluster.
   * 
   * @return the total number of blocks of the cluster.
   */
  long getTotalBlocks();
  
  /**
   * Gets the total number of missing blocks on the cluster.
   * 
   * @return the total number of missing blocks on the cluster.
   */
  long getNumberOfMissingBlocks();
  
  /**
   * Gets the total number of missing blocks on the cluster with
   * replication factor 1.
   *
   * @return the total number of missing blocks on the cluster with
   * replication factor 1.
   */
  long getNumberOfMissingBlocksWithReplicationFactorOne();

  /**
   * Gets the total number of replicated low redundancy blocks on the cluster
   * with the highest risk of loss.
   *
   * @return the total number of low redundancy blocks on the cluster
   * with the highest risk of loss.
   */
  long getHighestPriorityLowRedundancyReplicatedBlocks();

  /**
   * Gets the total number of erasure coded low redundancy blocks on the cluster
   * with the highest risk of loss.
   *
   * @return the total number of low redundancy blocks on the cluster
   * with the highest risk of loss.
   */
  long getHighestPriorityLowRedundancyECBlocks();

  /**
   * Gets the total number of snapshottable dirs in the system.
   *
   * @return the total number of snapshottable dirs in the system.
   */
  long getNumberOfSnapshottableDirs();

  /**
   * Gets the number of threads.
   * 
   * @return the number of threads.
   */
  int getThreads();

  /**
   * Gets the live node information of the cluster.
   * 
   * @return the live node information.
   */
  String getLiveNodes();
  
  /**
   * Gets the dead node information of the cluster.
   * 
   * @return the dead node information.
   */
  String getDeadNodes();
  
  /**
   * Gets the decommissioning node information of the cluster.
   * 
   * @return the decommissioning node information.
   */
  String getDecomNodes();

  /**
   * Gets the information on nodes entering maintenance.
   *
   * @return the information on nodes entering maintenance.
   */
  String getEnteringMaintenanceNodes();

  /**
   * Gets the cluster id.
   * 
   * @return the cluster id.
   */
  String getClusterId();
  
  /**
   * Gets the block pool id.
   * 
   * @return the block pool id.
   */
  String getBlockPoolId();

  /**
   * Get status information about the directories storing image and edits logs
   * of the NN.
   * 
   * @return the name dir status information, as a JSON string.
   */
  String getNameDirStatuses();

  /**
   * Get Max, Median, Min and Standard Deviation of DataNodes usage.
   *
   * @return the DataNode usage information, as a JSON string.
   */
  String getNodeUsage();

  /**
   * Get status information about the journals of the NN.
   *
   * @return the name journal status information, as a JSON string.
   */
  String getNameJournalStatus();
  
  /**
   * Get information about the transaction ID, including the last applied 
   * transaction ID and the most recent checkpoint's transaction ID.
   *
   * @return information about the transaction ID.
   */
  String getJournalTransactionInfo();

  /**
   * Gets the NN start time in milliseconds.
   *
   * @return the NN start time in msec.
   */
  long getNNStartedTimeInMillis();

  /**
   * Get the compilation information which contains date, user and branch.
   *
   * @return the compilation information, as a JSON string.
   */
  String getCompileInfo();

  /**
   * Get the list of corrupt files.
   *
   * @return the list of corrupt files, as a JSON string.
   */
  String getCorruptFiles();

  /**
   * Get the length of the list of corrupt files.
   *
   * @return the length of the list of corrupt files.
   */
  int getCorruptFilesCount();

  /**
   * Get the number of distinct versions of live datanodes.
   * 
   * @return the number of distinct versions of live datanodes.
   */
  int getDistinctVersionCount();

  /**
   * Get the number of live datanodes for each distinct versions.
   * 
   * @return the number of live datanodes for each distinct versions.
   */
  Map<String, Integer> getDistinctVersions();
  
  /**
   * Get namenode directory size.
   *
   * @return namenode directory size.
   */
  String getNameDirSize();

  /**
   * Verifies whether the cluster setup can support all enabled EC policies.
   *
   * @return the result of the verification.
   */
  String getVerifyECWithTopologyResult();

}
