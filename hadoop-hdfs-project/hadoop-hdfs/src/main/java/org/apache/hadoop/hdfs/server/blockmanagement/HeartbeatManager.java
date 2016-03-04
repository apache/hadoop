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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Manage the heartbeats received from datanodes.
 * The datanode list and statistics are synchronized
 * by the heartbeat manager lock.
 */
class HeartbeatManager implements DatanodeStatistics {
  static final Logger LOG = LoggerFactory.getLogger(HeartbeatManager.class);

  /**
   * Stores a subset of the datanodeMap in DatanodeManager,
   * containing nodes that are considered alive.
   * The HeartbeatMonitor periodically checks for out-dated entries,
   * and removes them from the list.
   * It is synchronized by the heartbeat manager lock.
   */
  private final List<DatanodeDescriptor> datanodes = new ArrayList<>();

  /** Statistics, which are synchronized by the heartbeat manager lock. */
  private final DatanodeStats stats = new DatanodeStats();

  /** The time period to check for expired datanodes */
  private final long heartbeatRecheckInterval;
  /** Heartbeat monitor thread */
  private final Daemon heartbeatThread = new Daemon(new Monitor());
  private final StopWatch heartbeatStopWatch = new StopWatch();

  final Namesystem namesystem;
  final BlockManager blockManager;

  HeartbeatManager(final Namesystem namesystem,
      final BlockManager blockManager, final Configuration conf) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    boolean avoidStaleDataNodesForWrite = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT);
    long recheckInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 min
    long staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);// 30s

    if (avoidStaleDataNodesForWrite && staleInterval < recheckInterval) {
      this.heartbeatRecheckInterval = staleInterval;
      LOG.info("Setting heartbeat recheck interval to " + staleInterval
          + " since " + DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY
          + " is less than "
          + DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY);
    } else {
      this.heartbeatRecheckInterval = recheckInterval;
    }
  }

  void activate() {
    heartbeatThread.start();
  }

  void close() {
    heartbeatThread.interrupt();
    try {
      // This will no effect if the thread hasn't yet been started.
      heartbeatThread.join(3000);
    } catch (InterruptedException ignored) {
    }
  }
  
  synchronized int getLiveDatanodeCount() {
    return datanodes.size();
  }

  @Override
  public long getCapacityTotal() {
    return stats.getCapacityTotal();
  }

  @Override
  public long getCapacityUsed() {
    return stats.getCapacityUsed();
  }

  @Override
  public float getCapacityUsedPercent() {
    return stats.getCapacityUsedPercent();
  }

  @Override
  public long getCapacityRemaining() {
    return stats.getCapacityRemaining();
  }

  @Override
  public float getCapacityRemainingPercent() {
    return stats.getCapacityRemainingPercent();
  }

  @Override
  public long getBlockPoolUsed() {
    return stats.getBlockPoolUsed();
  }

  @Override
  public float getPercentBlockPoolUsed() {
    return stats.getPercentBlockPoolUsed();
  }

  @Override
  public long getCapacityUsedNonDFS() {
    return stats.getCapacityUsedNonDFS();
  }

  @Override
  public int getXceiverCount() {
    return stats.getXceiverCount();
  }
  
  @Override
  public int getInServiceXceiverCount() {
    return stats.getNodesInServiceXceiverCount();
  }
  
  @Override
  public int getNumDatanodesInService() {
    return stats.getNodesInService();
  }
  
  @Override
  public long getCacheCapacity() {
    return stats.getCacheCapacity();
  }

  @Override
  public long getCacheUsed() {
    return stats.getCacheUsed();
  }

  @Override
  public synchronized long[] getStats() {
    return new long[] {getCapacityTotal(),
                       getCapacityUsed(),
                       getCapacityRemaining(),
                       -1L,
                       -1L,
                       -1L,
                       -1L,
                       -1L,
                       -1L};
  }

  @Override
  public int getExpiredHeartbeats() {
    return stats.getExpiredHeartbeats();
  }

  @Override
  public Map<StorageType, StorageTypeStats> getStorageTypeStats() {
    return stats.getStatsMap();
  }

  synchronized void register(final DatanodeDescriptor d) {
    if (!d.isAlive()) {
      addDatanode(d);

      //update its timestamp
      d.updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0L, 0L, 0, 0, null);
      stats.add(d);
    }
  }

  synchronized DatanodeDescriptor[] getDatanodes() {
    return datanodes.toArray(new DatanodeDescriptor[datanodes.size()]);
  }

  synchronized void addDatanode(final DatanodeDescriptor d) {
    // update in-service node count
    datanodes.add(d);
    d.setAlive(true);
  }

  void updateDnStat(final DatanodeDescriptor d){
    stats.add(d);
  }

  synchronized void removeDatanode(DatanodeDescriptor node) {
    if (node.isAlive()) {
      stats.subtract(node);
      datanodes.remove(node);
      node.setAlive(false);
    }
  }

  synchronized void updateHeartbeat(final DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) {
    stats.subtract(node);
    node.updateHeartbeat(reports, cacheCapacity, cacheUsed,
      xceiverCount, failedVolumes, volumeFailureSummary);
    stats.add(node);
  }

  synchronized void updateLifeline(final DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) {
    stats.subtract(node);
    // This intentionally calls updateHeartbeatState instead of
    // updateHeartbeat, because we don't want to modify the
    // heartbeatedSinceRegistration flag.  Arrival of a lifeline message does
    // not count as arrival of the first heartbeat.
    node.updateHeartbeatState(reports, cacheCapacity, cacheUsed,
        xceiverCount, failedVolumes, volumeFailureSummary);
    stats.add(node);
  }

  synchronized void startDecommission(final DatanodeDescriptor node) {
    if (!node.isAlive()) {
      LOG.info("Dead node {} is decommissioned immediately.", node);
      node.setDecommissioned();
    } else {
      stats.subtract(node);
      node.startDecommission();
      stats.add(node);
    }
  }

  synchronized void stopDecommission(final DatanodeDescriptor node) {
    LOG.info("Stopping decommissioning of {} node {}",
        node.isAlive() ? "live" : "dead", node);
    if (!node.isAlive()) {
      node.stopDecommission();
    } else {
      stats.subtract(node);
      node.stopDecommission();
      stats.add(node);
    }
  }

  @VisibleForTesting
  void restartHeartbeatStopWatch() {
    heartbeatStopWatch.reset().start();
  }

  @VisibleForTesting
  boolean shouldAbortHeartbeatCheck(long offset) {
    long elapsed = heartbeatStopWatch.now(TimeUnit.MILLISECONDS);
    return elapsed + offset > heartbeatRecheckInterval;
  }

  /**
   * Check if there are any expired heartbeats, and if so,
   * whether any blocks have to be re-replicated.
   * While removing dead datanodes, make sure that only one datanode is marked
   * dead at a time within the synchronized section. Otherwise, a cascading
   * effect causes more datanodes to be declared dead.
   * Check if there are any failed storage and if so,
   * Remove all the blocks on the storage. It also covers the following less
   * common scenarios. After DatanodeStorage is marked FAILED, it is still
   * possible to receive IBR for this storage.
   * 1) DN could deliver IBR for failed storage due to its implementation.
   *    a) DN queues a pending IBR request.
   *    b) The storage of the block fails.
   *    c) DN first sends HB, NN will mark the storage FAILED.
   *    d) DN then sends the pending IBR request.
   * 2) SBN processes block request from pendingDNMessages.
   *    It is possible to have messages in pendingDNMessages that refer
   *    to some failed storage.
   *    a) SBN receives a IBR and put it in pendingDNMessages.
   *    b) The storage of the block fails.
   *    c) Edit log replay get the IBR from pendingDNMessages.
   * Alternatively, we can resolve these scenarios with the following approaches.
   * A. Make sure DN don't deliver IBR for failed storage.
   * B. Remove all blocks in PendingDataNodeMessages for the failed storage
   *    when we remove all blocks from BlocksMap for that storage.
   */
  @VisibleForTesting
  void heartbeatCheck() {
    final DatanodeManager dm = blockManager.getDatanodeManager();
    // It's OK to check safe mode w/o taking the lock here, we re-check
    // for safe mode after taking the lock before removing a datanode.
    if (namesystem.isInStartupSafeMode()) {
      return;
    }
    boolean allAlive = false;
    while (!allAlive) {
      // locate the first dead node.
      DatanodeID dead = null;

      // locate the first failed storage that isn't on a dead node.
      DatanodeStorageInfo failedStorage = null;

      // check the number of stale nodes
      int numOfStaleNodes = 0;
      int numOfStaleStorages = 0;
      synchronized(this) {
        for (DatanodeDescriptor d : datanodes) {
          // check if an excessive GC pause has occurred
          if (shouldAbortHeartbeatCheck(0)) {
            return;
          }
          if (dead == null && dm.isDatanodeDead(d)) {
            stats.incrExpiredHeartbeats();
            dead = d;
          }
          if (d.isStale(dm.getStaleInterval())) {
            numOfStaleNodes++;
          }
          DatanodeStorageInfo[] storageInfos = d.getStorageInfos();
          for(DatanodeStorageInfo storageInfo : storageInfos) {
            if (storageInfo.areBlockContentsStale()) {
              numOfStaleStorages++;
            }

            if (failedStorage == null &&
                storageInfo.areBlocksOnFailedStorage() &&
                d != dead) {
              failedStorage = storageInfo;
            }
          }

        }
        
        // Set the number of stale nodes in the DatanodeManager
        dm.setNumStaleNodes(numOfStaleNodes);
        dm.setNumStaleStorages(numOfStaleStorages);
      }

      allAlive = dead == null && failedStorage == null;
      if (!allAlive && namesystem.isInStartupSafeMode()) {
        return;
      }
      if (dead != null) {
        // acquire the fsnamesystem lock, and then remove the dead node.
        namesystem.writeLock();
        try {
          dm.removeDeadDatanode(dead);
        } finally {
          namesystem.writeUnlock();
        }
      }
      if (failedStorage != null) {
        // acquire the fsnamesystem lock, and remove blocks on the storage.
        namesystem.writeLock();
        try {
          blockManager.removeBlocksAssociatedTo(failedStorage);
        } finally {
          namesystem.writeUnlock();
        }
      }
    }
  }

  /** Periodically check heartbeat and update block key */
  private class Monitor implements Runnable {
    private long lastHeartbeatCheck;
    private long lastBlockKeyUpdate;

    @Override
    public void run() {
      while(namesystem.isRunning()) {
        restartHeartbeatStopWatch();
        try {
          final long now = Time.monotonicNow();
          if (lastHeartbeatCheck + heartbeatRecheckInterval < now) {
            heartbeatCheck();
            lastHeartbeatCheck = now;
          }
          if (blockManager.shouldUpdateBlockKey(now - lastBlockKeyUpdate)) {
            synchronized(HeartbeatManager.this) {
              for(DatanodeDescriptor d : datanodes) {
                d.setNeedKeyUpdate(true);
              }
            }
            lastBlockKeyUpdate = now;
          }
        } catch (Exception e) {
          LOG.error("Exception while checking heartbeat", e);
        }
        try {
          Thread.sleep(5000);  // 5 seconds
        } catch (InterruptedException ignored) {
        }
        // avoid declaring nodes dead for another cycle if a GC pause lasts
        // longer than the node recheck interval
        if (shouldAbortHeartbeatCheck(-5000)) {
          LOG.warn("Skipping next heartbeat scan due to excessive pause");
          lastHeartbeatCheck = Time.monotonicNow();
        }
      }
    }
  }
}
