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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 
 * This Interface defines the methods to get the status of a the FSNamesystem of
 * a name node.
 * It is also used for publishing via JMX (hence we follow the JMX naming
 * convention.)
 * 
 * Note we have not used the MetricsDynamicMBeanBase to implement this
 * because the interface for the NameNodeStateMBean is stable and should
 * be published as an interface.
 * 
 * <p>
 * Name Node runtime activity statistic  info is reported in
 * @see org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics
 *
 */
@InterfaceAudience.Private
public interface FSNamesystemMBean {

  /**
   * The state of the file system: Safemode or Operational
   * @return the state
   */
  public String getFSState();
  
  
  /**
   * Number of allocated blocks in the system
   * @return -  number of allocated blocks
   */
  public long getBlocksTotal();

  /**
   * Total storage capacity
   * @return -  total capacity in bytes
   */
  public long getCapacityTotal();


  /**
   * Free (unused) storage capacity
   * @return -  free capacity in bytes
   */
  public long getCapacityRemaining();
 
  /**
   * Used storage capacity
   * @return -  used capacity in bytes
   */
  public long getCapacityUsed();

  /**
   * Total PROVIDED storage capacity.
   * @return -  total PROVIDED storage capacity in bytes
   */
  public long getProvidedCapacityTotal();

  /**
   * Total number of files and directories
   * @return -  num of files and directories
   */
  public long getFilesTotal();
 
  /**
   * Get aggregated count of all blocks pending to be reconstructed.
   * @deprecated Use {@link #getPendingReconstructionBlocks()} instead.
   */
  @Deprecated
  public long getPendingReplicationBlocks();

  /**
   * Get aggregated count of all blocks pending to be reconstructed.
   * @return Number of blocks to be replicated.
   */
  public long getPendingReconstructionBlocks();

  /**
   * Get aggregated count of all blocks with low redundancy.
   * @deprecated Use {@link #getLowRedundancyBlocks()} instead.
   */
  @Deprecated
  public long getUnderReplicatedBlocks();

  /**
   * Get aggregated count of all blocks with low redundancy.
   * @return Number of blocks with low redundancy.
   */
  public long getLowRedundancyBlocks();

  /**
   * Blocks scheduled for replication
   * @return -  num of blocks scheduled for replication
   */
  public long getScheduledReplicationBlocks();

  /**
   * Total Load on the FSNamesystem
   * @return -  total load of FSNamesystem
   */
  public int getTotalLoad();

  /**
   * Number of Live data nodes
   * @return number of live data nodes
   */
  public int getNumLiveDataNodes();
  
  /**
   * Number of dead data nodes
   * @return number of dead data nodes
   */
  public int getNumDeadDataNodes();
  
  /**
   * Number of stale data nodes
   * @return number of stale data nodes
   */
  public int getNumStaleDataNodes();

  /**
   * Number of decommissioned Live data nodes
   * @return number of decommissioned live data nodes
   */
  public int getNumDecomLiveDataNodes();

  /**
   * Number of decommissioned dead data nodes
   * @return number of decommissioned dead data nodes
   */
  public int getNumDecomDeadDataNodes();

  /**
   * @return Number of in-service data nodes, where NumInServiceDataNodes =
   * NumLiveDataNodes - NumDecomLiveDataNodes - NumInMaintenanceLiveDataNodes
   */
  int getNumInServiceLiveDataNodes();

  /**
   * Number of failed data volumes across all live data nodes.
   * @return number of failed data volumes across all live data nodes
   */
  int getVolumeFailuresTotal();

  /**
   * Returns an estimate of total capacity lost due to volume failures in bytes
   * across all live data nodes.
   * @return estimate of total capacity lost in bytes
   */
  long getEstimatedCapacityLostTotal();

  /**
   * Number of data nodes that are in the decommissioning state
   */
  public int getNumDecommissioningDataNodes();

  /**
   * The statistics of snapshots
   */
  public String getSnapshotStats();

  /**
   * Return the maximum number of inodes in the file system
   */
  public long getMaxObjects();

  /**
   * Number of blocks pending deletion
   * @return number of blocks pending deletion
   */
  long getPendingDeletionBlocks();

  /**
   * Time when block deletions will begin
   * @return time when block deletions will begin
   */
  long getBlockDeletionStartTime();

  /**
   * Number of content stale storages.
   * @return number of content stale storages
   */
  public int getNumStaleStorages();

  /**
   * Returns a nested JSON object listing the top users for different RPC 
   * operations over tracked time windows.
   * 
   * @return JSON string
   */
  public String getTopUserOpCounts();

  /**
   * Return the number of encryption zones in the system.
   */
  int getNumEncryptionZones();

  /**
   * Returns the length of the wait Queue for the FSNameSystemLock.
   *
   * A larger number here indicates lots of threads are waiting for
   * FSNameSystemLock.
   * @return int - Number of Threads waiting to acquire FSNameSystemLock
   */
  int getFsLockQueueLength();

  /**
   * Return total number of Sync Operations on FSEditLog.
   */
  long getTotalSyncCount();

  /**
   * Return total time spent doing sync operations on FSEditLog.
   */
  String getTotalSyncTimes();

  /**
   * @return Number of IN_MAINTENANCE live data nodes
   */
  int getNumInMaintenanceLiveDataNodes();

  /**
   * @return Number of IN_MAINTENANCE dead data nodes
   */
  int getNumInMaintenanceDeadDataNodes();

  /**
   * @return Number of ENTERING_MAINTENANCE data nodes
   */
  int getNumEnteringMaintenanceDataNodes();

  /**
   * Get the current number of delegation tokens in memory.
   * @return number of DTs
   */
  long getCurrentTokensCount();
}
