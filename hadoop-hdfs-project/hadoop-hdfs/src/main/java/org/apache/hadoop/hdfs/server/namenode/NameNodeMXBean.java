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
 * This is the JMX management interface for namenode information
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface NameNodeMXBean {

  /**
   * Gets the version of Hadoop.
   * 
   * @return the version
   */
  public String getVersion();

  /**
   * Get the version of software running on the Namenode
   * @return a string representing the version
   */
  public String getSoftwareVersion();

  /**
   * Gets the used space by data nodes.
   * 
   * @return the used space by data nodes
   */
  public long getUsed();
  
  /**
   * Gets total non-used raw bytes.
   * 
   * @return total non-used raw bytes
   */
  public long getFree();
  
  /**
   * Gets total raw bytes including non-dfs used space.
   * 
   * @return the total raw bytes including non-dfs used space
   */
  public long getTotal();
  
  
  /**
   * Gets the safemode status
   * 
   * @return the safemode status
   * 
   */
  public String getSafemode();
  
  /**
   * Checks if upgrade is finalized.
   * 
   * @return true, if upgrade is finalized
   */
  public boolean isUpgradeFinalized();

  /**
   * Gets the RollingUpgrade information.
   *
   * @return Rolling upgrade information if an upgrade is in progress. Else
   * (e.g. if there is no upgrade or the upgrade is finalized), returns null.
   */
  public RollingUpgradeInfo.Bean getRollingUpgradeStatus();

  /**
   * Gets total used space by data nodes for non DFS purposes such as storing
   * temporary files on the local file system
   * 
   * @return the non dfs space of the cluster
   */
  public long getNonDfsUsedSpace();
  
  /**
   * Gets the total used space by data nodes as percentage of total capacity
   * 
   * @return the percentage of used space on the cluster.
   */
  public float getPercentUsed();
  
  /**
   * Gets the total remaining space by data nodes as percentage of total 
   * capacity
   * 
   * @return the percentage of the remaining space on the cluster
   */
  public float getPercentRemaining();

  /**
   * Returns the amount of cache used by the datanode (in bytes).
   */
  public long getCacheUsed();

  /**
   * Returns the total cache capacity of the datanode (in bytes).
   */
  public long getCacheCapacity();
  
  /**
   * Get the total space used by the block pools of this namenode
   */
  public long getBlockPoolUsedSpace();
  
  /**
   * Get the total space used by the block pool as percentage of total capacity
   */
  public float getPercentBlockPoolUsed();
    
  /**
   * Gets the total numbers of blocks on the cluster.
   * 
   * @return the total number of blocks of the cluster
   */
  public long getTotalBlocks();
  
  /**
   * Gets the total number of files on the cluster
   * 
   * @return the total number of files on the cluster
   */
  public long getTotalFiles();
  
  /**
   * Gets the total number of missing blocks on the cluster
   * 
   * @return the total number of missing blocks on the cluster
   */
  public long getNumberOfMissingBlocks();
  
  /**
   * Gets the total number of missing blocks on the cluster with
   * replication factor 1
   *
   * @return the total number of missing blocks on the cluster with
   * replication factor 1
   */
  public long getNumberOfMissingBlocksWithReplicationFactorOne();

  /**
   * Gets the number of threads.
   * 
   * @return the number of threads
   */
  public int getThreads();

  /**
   * Gets the live node information of the cluster.
   * 
   * @return the live node information
   */
  public String getLiveNodes();
  
  /**
   * Gets the dead node information of the cluster.
   * 
   * @return the dead node information
   */
  public String getDeadNodes();
  
  /**
   * Gets the decommissioning node information of the cluster.
   * 
   * @return the decommissioning node information
   */
  public String getDecomNodes();
  
  /**
   * Gets the cluster id.
   * 
   * @return the cluster id
   */
  public String getClusterId();
  
  /**
   * Gets the block pool id.
   * 
   * @return the block pool id
   */
  public String getBlockPoolId();

  /**
   * Get status information about the directories storing image and edits logs
   * of the NN.
   * 
   * @return the name dir status information, as a JSON string.
   */
  public String getNameDirStatuses();

  /**
   * Get Max, Median, Min and Standard Deviation of DataNodes usage.
   *
   * @return the DataNode usage information, as a JSON string.
   */
  public String getNodeUsage();

  /**
   * Get status information about the journals of the NN.
   *
   * @return the name journal status information, as a JSON string.
   */
  public String getNameJournalStatus();
  
  /**
   * Get information about the transaction ID, including the last applied 
   * transaction ID and the most recent checkpoint's transaction ID
   */
  public String getJournalTransactionInfo();

  /**
   * Gets the NN start time
   *
   * @return the NN start time
   */
  public String getNNStarted();

  /**
   * Get the compilation information which contains date, user and branch
   *
   * @return the compilation information, as a JSON string.
   */
  public String getCompileInfo();

  /**
   * Get the list of corrupt files
   *
   * @return the list of corrupt files, as a JSON string.
   */
  public String getCorruptFiles();

  /**
   * Get the number of distinct versions of live datanodes
   * 
   * @return the number of distinct versions of live datanodes
   */
  public int getDistinctVersionCount();

  /**
   * Get the number of live datanodes for each distinct versions
   * 
   * @return the number of live datanodes for each distinct versions
   */
  public Map<String, Integer> getDistinctVersions();
  
}
