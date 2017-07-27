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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final List<DatanodeDescriptor> datanodes = new ArrayList<DatanodeDescriptor>();

  /** Statistics, which are synchronized by the heartbeat manager lock. */
  private final Stats stats = new Stats();

  /** The time period to check for expired datanodes */
  private final long heartbeatRecheckInterval;
  /** Heartbeat monitor thread */
  private final Daemon heartbeatThread = new Daemon(new Monitor());

    
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

  void activate(Configuration conf) {
    heartbeatThread.start();
  }

  void close() {
    heartbeatThread.interrupt();
    try {
      // This will no effect if the thread hasn't yet been started.
      heartbeatThread.join(3000);
    } catch (InterruptedException e) {
    }
  }
  
  synchronized int getLiveDatanodeCount() {
    return datanodes.size();
  }

  @Override
  public synchronized long getCapacityTotal() {
    return stats.capacityTotal;
  }

  @Override
  public synchronized long getCapacityUsed() {
    return stats.capacityUsed;
  }

  @Override
  public synchronized float getCapacityUsedPercent() {
    return DFSUtil.getPercentUsed(stats.capacityUsed, stats.capacityTotal);
  }

  @Override
  public synchronized long getCapacityRemaining() {
    return stats.capacityRemaining;
  }

  @Override
  public synchronized float getCapacityRemainingPercent() {
    return DFSUtil.getPercentRemaining(
        stats.capacityRemaining, stats.capacityTotal);
  }

  @Override
  public synchronized long getBlockPoolUsed() {
    return stats.blockPoolUsed;
  }

  @Override
  public synchronized float getPercentBlockPoolUsed() {
    return DFSUtil.getPercentUsed(stats.blockPoolUsed, stats.capacityTotal);
  }

  @Override
  public synchronized long getCapacityUsedNonDFS() {
    return stats.capacityUsedNonDfs;
  }

  @Override
  public synchronized int getXceiverCount() {
    return stats.xceiverCount;
  }
  
  @Override
  public synchronized int getInServiceXceiverCount() {
    return stats.nodesInServiceXceiverCount;
  }
  
  @Override
  public synchronized int getNumDatanodesInService() {
    return stats.nodesInService;
  }
  
  @Override
  public synchronized long getCacheCapacity() {
    return stats.cacheCapacity;
  }

  @Override
  public synchronized long getCacheUsed() {
    return stats.cacheUsed;
  }
  

  @Override
  public synchronized long[] getStats() {
    return new long[] {getCapacityTotal(),
                       getCapacityUsed(),
                       getCapacityRemaining(),
                       -1L,
                       -1L,
                       -1L,
                       -1L};
  }

  @Override
  public synchronized int getExpiredHeartbeats() {
    return stats.expiredHeartbeats;
  }

  synchronized void register(final DatanodeDescriptor d) {
    if (!d.isAlive) {
      //update its timestamp
      d.updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0L, 0L, 0, 0, null);
      addDatanode(d);
    }
  }

  synchronized DatanodeDescriptor[] getDatanodes() {
    return datanodes.toArray(new DatanodeDescriptor[datanodes.size()]);
  }

  synchronized void addDatanode(final DatanodeDescriptor d) {
    // update in-service node count
    stats.add(d);
    datanodes.add(d);
    d.isAlive = true;
  }

  synchronized void removeDatanode(DatanodeDescriptor node) {
    if (node.isAlive) {
      stats.subtract(node);
      datanodes.remove(node);
      node.isAlive = false;
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

  synchronized void startDecommission(final DatanodeDescriptor node) {
    if (!node.isAlive) {
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
        node.isAlive ? "live" : "dead", node);
    if (!node.isAlive) {
      node.stopDecommission();
    } else {
      stats.subtract(node);
      node.stopDecommission();
      stats.add(node);
    }
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
      if (dead != null) {
        // acquire the fsnamesystem lock, and then remove the dead node.
        namesystem.writeLock();
        try {
          if (namesystem.isInStartupSafeMode()) {
            return;
          }
          synchronized(this) {
            dm.removeDeadDatanode(dead);
          }
        } finally {
          namesystem.writeUnlock();
        }
      }
      if (failedStorage != null) {
        // acquire the fsnamesystem lock, and remove blocks on the storage.
        namesystem.writeLock();
        try {
          if (namesystem.isInStartupSafeMode()) {
            return;
          }
          synchronized(this) {
            blockManager.removeBlocksAssociatedTo(failedStorage);
          }
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
        try {
          final long now = Time.monotonicNow();
          if (lastHeartbeatCheck + heartbeatRecheckInterval < now) {
            heartbeatCheck();
            lastHeartbeatCheck = now;
          }
          if (blockManager.shouldUpdateBlockKey(now - lastBlockKeyUpdate)) {
            synchronized(HeartbeatManager.this) {
              for(DatanodeDescriptor d : datanodes) {
                d.needKeyUpdate = true;
              }
            }
            lastBlockKeyUpdate = now;
          }
        } catch (Exception e) {
          LOG.error("Exception while checking heartbeat", e);
        }
        try {
          Thread.sleep(5000);  // 5 seconds
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /** Datanode statistics.
   * For decommissioning/decommissioned nodes, only used capacity is counted.
   */
  private static class Stats {
    private long capacityTotal = 0L;
    private long capacityUsed = 0L;
    private long capacityUsedNonDfs = 0L;
    private long capacityRemaining = 0L;
    private long blockPoolUsed = 0L;
    private int xceiverCount = 0;
    private long cacheCapacity = 0L;
    private long cacheUsed = 0L;

    private int nodesInService = 0;
    private int nodesInServiceXceiverCount = 0;

    private int expiredHeartbeats = 0;

    private void add(final DatanodeDescriptor node) {
      capacityUsed += node.getDfsUsed();
      capacityUsedNonDfs += node.getNonDfsUsed();
      blockPoolUsed += node.getBlockPoolUsed();
      xceiverCount += node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        nodesInService++;
        nodesInServiceXceiverCount += node.getXceiverCount();
        capacityTotal += node.getCapacity();
        capacityRemaining += node.getRemaining();
      } else {
        capacityTotal += node.getDfsUsed();
      }
      cacheCapacity += node.getCacheCapacity();
      cacheUsed += node.getCacheUsed();
    }

    private void subtract(final DatanodeDescriptor node) {
      capacityUsed -= node.getDfsUsed();
      capacityUsedNonDfs -= node.getNonDfsUsed();
      blockPoolUsed -= node.getBlockPoolUsed();
      xceiverCount -= node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        nodesInService--;
        nodesInServiceXceiverCount -= node.getXceiverCount();
        capacityTotal -= node.getCapacity();
        capacityRemaining -= node.getRemaining();
      } else {
        capacityTotal -= node.getDfsUsed();
      }
      cacheCapacity -= node.getCacheCapacity();
      cacheUsed -= node.getCacheUsed();
    }
    
    /** Increment expired heartbeat counter. */
    private void incrExpiredHeartbeats() {
      expiredHeartbeats++;
    }
  }
}

