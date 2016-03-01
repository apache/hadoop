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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.Time;

/**
 * This class extends the DatanodeInfo class with ephemeral information (eg
 * health, capacity, what blocks are associated with the Datanode) that is
 * private to the Namenode, ie this class is not exposed to clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeDescriptor extends DatanodeInfo {
  public static final Log LOG = LogFactory.getLog(DatanodeDescriptor.class);
  public static final DatanodeDescriptor[] EMPTY_ARRAY = {};

  // Stores status of decommissioning.
  // If node is not decommissioning, do not use this object for anything.
  public final DecommissioningStatus decommissioningStatus = new DecommissioningStatus();

  private long curBlockReportId = 0;

  private BitSet curBlockReportRpcsSeen = null;

  public int updateBlockReportContext(BlockReportContext context) {
    if (curBlockReportId != context.getReportId()) {
      curBlockReportId = context.getReportId();
      curBlockReportRpcsSeen = new BitSet(context.getTotalRpcs());
    }
    curBlockReportRpcsSeen.set(context.getCurRpc());
    return curBlockReportRpcsSeen.cardinality();
  }

  public void clearBlockReportContext() {
    curBlockReportId = 0;
    curBlockReportRpcsSeen = null;
  }

  /** Block and targets pair */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeStorageInfo[] targets;    

    BlockTargetPair(Block block, DatanodeStorageInfo[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /** A BlockTargetPair queue. */
  private static class BlockQueue<E> {
    private final Queue<E> blockq = new LinkedList<E>();

    /** Size of the queue */
    synchronized int size() {return blockq.size();}

    /** Enqueue */
    synchronized boolean offer(E e) { 
      return blockq.offer(e);
    }

    /** Dequeue */
    synchronized List<E> poll(int numBlocks) {
      if (numBlocks <= 0 || blockq.isEmpty()) {
        return null;
      }

      List<E> results = new ArrayList<E>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }

    /**
     * Returns <tt>true</tt> if the queue contains the specified element.
     */
    boolean contains(E e) {
      return blockq.contains(e);
    }

    synchronized void clear() {
      blockq.clear();
    }
  }

  private final Map<String, DatanodeStorageInfo> storageMap = 
      new HashMap<String, DatanodeStorageInfo>();

  /**
   * A list of CachedBlock objects on this datanode.
   */
  public static class CachedBlocksList extends IntrusiveCollection<CachedBlock> {
    public enum Type {
      PENDING_CACHED,
      CACHED,
      PENDING_UNCACHED
    }

    private final DatanodeDescriptor datanode;

    private final Type type;

    CachedBlocksList(DatanodeDescriptor datanode, Type type) {
      this.datanode = datanode;
      this.type = type;
    }

    public DatanodeDescriptor getDatanode() {
      return datanode;
    }

    public Type getType() {
      return type;
    }
  }

  /**
   * The blocks which we want to cache on this DataNode.
   */
  private final CachedBlocksList pendingCached = 
      new CachedBlocksList(this, CachedBlocksList.Type.PENDING_CACHED);

  /**
   * The blocks which we know are cached on this datanode.
   * This list is updated by periodic cache reports.
   */
  private final CachedBlocksList cached = 
      new CachedBlocksList(this, CachedBlocksList.Type.CACHED);

  /**
   * The blocks which we want to uncache on this DataNode.
   */
  private final CachedBlocksList pendingUncached = 
      new CachedBlocksList(this, CachedBlocksList.Type.PENDING_UNCACHED);

  public CachedBlocksList getPendingCached() {
    return pendingCached;
  }

  public CachedBlocksList getCached() {
    return cached;
  }

  public CachedBlocksList getPendingUncached() {
    return pendingUncached;
  }

  /**
   * The time when the last batch of caching directives was sent, in
   * monotonic milliseconds.
   */
  private long lastCachingDirectiveSentTimeMs;

  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  public boolean isAlive = false;
  public boolean needKeyUpdate = false;

  private boolean forceRegistration = false;

  // A system administrator can tune the balancer bandwidth parameter
  // (dfs.balance.bandwidthPerSec) dynamically by calling
  // "dfsadmin -setBalanacerBandwidth <newbandwidth>", at which point the
  // following 'bandwidth' variable gets updated with the new value for each
  // node. Once the heartbeat command is issued to update the value on the
  // specified datanode, this value will be set back to 0.
  private long bandwidth;

  /** A queue of blocks to be replicated by this datanode */
  private final BlockQueue<BlockTargetPair> replicateBlocks = new BlockQueue<BlockTargetPair>();
  /** A queue of blocks to be recovered by this datanode */
  private final BlockQueue<BlockInfoContiguousUnderConstruction> recoverBlocks =
                                new BlockQueue<BlockInfoContiguousUnderConstruction>();
  /** A set of blocks to be invalidated by this datanode */
  private final LightWeightHashSet<Block> invalidateBlocks = new LightWeightHashSet<Block>();

  /* Variables for maintaining number of blocks scheduled to be written to
   * this storage. This count is approximate and might be slightly bigger
   * in case of errors (e.g. datanode does not report if an error occurs
   * while writing the block).
   */
  private EnumCounters<StorageType> currApproxBlocksScheduled
      = new EnumCounters<StorageType>(StorageType.class);
  private EnumCounters<StorageType> prevApproxBlocksScheduled
      = new EnumCounters<StorageType>(StorageType.class);
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min
  private int volumeFailures = 0;
  private VolumeFailureSummary volumeFailureSummary = null;
  
  /** 
   * When set to true, the node is not in include list and is not allowed
   * to communicate with the namenode
   */
  private boolean disallowed = false;

  // The number of replication work pending before targets are determined
  private int PendingReplicationWithoutTargets = 0;

  // HB processing can use it to tell if it is the first HB since DN restarted
  private boolean heartbeatedSinceRegistration = false;

  /**
   * DatanodeDescriptor constructor
   * @param nodeID id of the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
    super(nodeID);
    updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0L, 0L, 0, 0, null);
  }

  /**
   * DatanodeDescriptor constructor
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation) {
    super(nodeID, networkLocation);
    updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0L, 0L, 0, 0, null);
  }

  @VisibleForTesting
  public DatanodeStorageInfo getStorageInfo(String storageID) {
    synchronized (storageMap) {
      return storageMap.get(storageID);
    }
  }
  @VisibleForTesting
  public DatanodeStorageInfo[] getStorageInfos() {
    synchronized (storageMap) {
      final Collection<DatanodeStorageInfo> storages = storageMap.values();
      return storages.toArray(new DatanodeStorageInfo[storages.size()]);
    }
  }

  public StorageReport[] getStorageReports() {
    final DatanodeStorageInfo[] infos = getStorageInfos();
    final StorageReport[] reports = new StorageReport[infos.length];
    for(int i = 0; i < infos.length; i++) {
      reports[i] = infos[i].toStorageReport();
    }
    return reports;
  }

  boolean hasStaleStorages() {
    synchronized (storageMap) {
      for (DatanodeStorageInfo storage : storageMap.values()) {
        if (storage.areBlockContentsStale()) {
          return true;
        }
      }
      return false;
    }
  }

  static final private List<DatanodeStorageInfo> EMPTY_STORAGE_INFO_LIST =
      ImmutableList.of();

  List<DatanodeStorageInfo> removeZombieStorages() {
    List<DatanodeStorageInfo> zombies = null;
    synchronized (storageMap) {
      Iterator<Map.Entry<String, DatanodeStorageInfo>> iter =
          storageMap.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, DatanodeStorageInfo> entry = iter.next();
        DatanodeStorageInfo storageInfo = entry.getValue();
        if (storageInfo.getLastBlockReportId() != curBlockReportId) {
          LOG.info(storageInfo.getStorageID() + " had lastBlockReportId 0x" +
              Long.toHexString(storageInfo.getLastBlockReportId()) +
              ", but curBlockReportId = 0x" +
              Long.toHexString(curBlockReportId));
          iter.remove();
          if (zombies == null) {
            zombies = new LinkedList<DatanodeStorageInfo>();
          }
          zombies.add(storageInfo);
        }
        storageInfo.setLastBlockReportId(0);
      }
    }
    return zombies == null ? EMPTY_STORAGE_INFO_LIST : zombies;
  }

  /**
   * Remove block from the list of blocks belonging to the data-node. Remove
   * data-node from the block.
   */
  boolean removeBlock(BlockInfoContiguous b) {
    final DatanodeStorageInfo s = b.findStorageInfo(this);
    // if block exists on this datanode
    if (s != null) {
      return s.removeBlock(b);
    }
    return false;
  }
  
  /**
   * Remove block from the list of blocks belonging to the data-node. Remove
   * data-node from the block.
   */
  boolean removeBlock(String storageID, BlockInfoContiguous b) {
    DatanodeStorageInfo s = getStorageInfo(storageID);
    if (s != null) {
      return s.removeBlock(b);
    }
    return false;
  }

  public void resetBlocks() {
    setCapacity(0);
    setRemaining(0);
    setBlockPoolUsed(0);
    setDfsUsed(0);
    setXceiverCount(0);
    this.invalidateBlocks.clear();
    this.volumeFailures = 0;
    // pendingCached, cached, and pendingUncached are protected by the
    // FSN lock.
    this.pendingCached.clear();
    this.cached.clear();
    this.pendingUncached.clear();
  }
  
  public void clearBlockQueues() {
    synchronized (invalidateBlocks) {
      this.invalidateBlocks.clear();
      this.recoverBlocks.clear();
      this.replicateBlocks.clear();
    }
    // pendingCached, cached, and pendingUncached are protected by the
    // FSN lock.
    this.pendingCached.clear();
    this.cached.clear();
    this.pendingUncached.clear();
  }

  public int numBlocks() {
    int blocks = 0;
    for (DatanodeStorageInfo entry : getStorageInfos()) {
      blocks += entry.numBlocks();
    }
    return blocks;
  }

  /**
   * Updates stats from datanode heartbeat.
   */
  public void updateHeartbeat(StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int volFailures,
      VolumeFailureSummary volumeFailureSummary) {
    updateHeartbeatState(reports, cacheCapacity, cacheUsed, xceiverCount,
        volFailures, volumeFailureSummary);
    heartbeatedSinceRegistration = true;
  }

  /**
   * process datanode heartbeat or stats initialization.
   */
  public void updateHeartbeatState(StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int volFailures,
      VolumeFailureSummary volumeFailureSummary) {
    long totalCapacity = 0;
    long totalRemaining = 0;
    long totalBlockPoolUsed = 0;
    long totalDfsUsed = 0;
    Set<DatanodeStorageInfo> failedStorageInfos = null;

    // Decide if we should check for any missing StorageReport and mark it as
    // failed. There are different scenarios.
    // 1. When DN is running, a storage failed. Given the current DN
    //    implementation doesn't add recovered storage back to its storage list
    //    until DN restart, we can assume volFailures won't decrease
    //    during the current DN registration session.
    //    When volumeFailures == this.volumeFailures, it implies there is no
    //    state change. No need to check for failed storage. This is an
    //    optimization.  Recent versions of the DataNode report a
    //    VolumeFailureSummary containing the date/time of the last volume
    //    failure.  If that's available, then we check that instead for greater
    //    accuracy.
    // 2. After DN restarts, volFailures might not increase and it is possible
    //    we still have new failed storage. For example, admins reduce
    //    available storages in configuration. Another corner case
    //    is the failed volumes might change after restart; a) there
    //    is one good storage A, one restored good storage B, so there is
    //    one element in storageReports and that is A. b) A failed. c) Before
    //    DN sends HB to NN to indicate A has failed, DN restarts. d) After DN
    //    restarts, storageReports has one element which is B.
    final boolean checkFailedStorages;
    if (volumeFailureSummary != null && this.volumeFailureSummary != null) {
      checkFailedStorages = volumeFailureSummary.getLastVolumeFailureDate() >
          this.volumeFailureSummary.getLastVolumeFailureDate();
    } else {
      checkFailedStorages = (volFailures > this.volumeFailures) ||
          !heartbeatedSinceRegistration;
    }

    if (checkFailedStorages) {
      LOG.info("Number of failed storage changes from "
          + this.volumeFailures + " to " + volFailures);
      failedStorageInfos = new HashSet<DatanodeStorageInfo>(
          storageMap.values());
    }

    setCacheCapacity(cacheCapacity);
    setCacheUsed(cacheUsed);
    setXceiverCount(xceiverCount);
    setLastUpdate(Time.now());
    setLastUpdateMonotonic(Time.monotonicNow());
    this.volumeFailures = volFailures;
    this.volumeFailureSummary = volumeFailureSummary;
    for (StorageReport report : reports) {
      DatanodeStorageInfo storage = updateStorage(report.getStorage());
      if (checkFailedStorages) {
        failedStorageInfos.remove(storage);
      }

      storage.receivedHeartbeat(report);
      totalCapacity += report.getCapacity();
      totalRemaining += report.getRemaining();
      totalBlockPoolUsed += report.getBlockPoolUsed();
      totalDfsUsed += report.getDfsUsed();
    }
    rollBlocksScheduled(getLastUpdateMonotonic());

    // Update total metrics for the node.
    setCapacity(totalCapacity);
    setRemaining(totalRemaining);
    setBlockPoolUsed(totalBlockPoolUsed);
    setDfsUsed(totalDfsUsed);
    if (checkFailedStorages) {
      updateFailedStorage(failedStorageInfos);
    }

    if (storageMap.size() != reports.length) {
      pruneStorageMap(reports);
    }
  }

  /**
   * Remove stale storages from storageMap. We must not remove any storages
   * as long as they have associated block replicas.
   */
  private void pruneStorageMap(final StorageReport[] reports) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Number of storages reported in heartbeat=" + reports.length +
                    "; Number of storages in storageMap=" + storageMap.size());
    }

    HashMap<String, DatanodeStorageInfo> excessStorages;

    synchronized (storageMap) {
      // Init excessStorages with all known storages.
      excessStorages = new HashMap<String, DatanodeStorageInfo>(storageMap);

      // Remove storages that the DN reported in the heartbeat.
      for (final StorageReport report : reports) {
        excessStorages.remove(report.getStorage().getStorageID());
      }

      // For each remaining storage, remove it if there are no associated
      // blocks.
      for (final DatanodeStorageInfo storageInfo : excessStorages.values()) {
        if (storageInfo.numBlocks() == 0) {
          storageMap.remove(storageInfo.getStorageID());
          LOG.info("Removed storage " + storageInfo + " from DataNode" + this);
        } else if (LOG.isDebugEnabled()) {
          // This can occur until all block reports are received.
          LOG.debug("Deferring removal of stale storage " + storageInfo +
                        " with " + storageInfo.numBlocks() + " blocks");
        }
      }
    }
  }

  private void updateFailedStorage(
      Set<DatanodeStorageInfo> failedStorageInfos) {
    for (DatanodeStorageInfo storageInfo : failedStorageInfos) {
      if (storageInfo.getState() != DatanodeStorage.State.FAILED) {
        LOG.info(storageInfo + " failed.");
        storageInfo.setState(DatanodeStorage.State.FAILED);
      }
    }
  }

  private static class BlockIterator implements Iterator<BlockInfoContiguous> {
    private int index = 0;
    private final List<Iterator<BlockInfoContiguous>> iterators;
    
    private BlockIterator(final DatanodeStorageInfo... storages) {
      List<Iterator<BlockInfoContiguous>> iterators = new ArrayList<Iterator<BlockInfoContiguous>>();
      for (DatanodeStorageInfo e : storages) {
        iterators.add(e.getBlockIterator());
      }
      this.iterators = Collections.unmodifiableList(iterators);
    }

    @Override
    public boolean hasNext() {
      update();
      return !iterators.isEmpty() && iterators.get(index).hasNext();
    }

    @Override
    public BlockInfoContiguous next() {
      update();
      return iterators.get(index).next();
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove unsupported.");
    }
    
    private void update() {
      while(index < iterators.size() - 1 && !iterators.get(index).hasNext()) {
        index++;
      }
    }
  }

  Iterator<BlockInfoContiguous> getBlockIterator() {
    return new BlockIterator(getStorageInfos());
  }
  Iterator<BlockInfoContiguous> getBlockIterator(final String storageID) {
    return new BlockIterator(getStorageInfo(storageID));
  }

  void incrementPendingReplicationWithoutTargets() {
    PendingReplicationWithoutTargets++;
  }

  void decrementPendingReplicationWithoutTargets() {
    PendingReplicationWithoutTargets--;
  }

  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeStorageInfo[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(new BlockTargetPair(block, targets));
  }

  /**
   * Store block recovery work.
   */
  void addBlockToBeRecovered(BlockInfoContiguousUnderConstruction block) {
    if(recoverBlocks.contains(block)) {
      // this prevents adding the same block twice to the recovery queue
      BlockManager.LOG.info(block + " is already in the recovery queue");
      return;
    }
    recoverBlocks.offer(block);
  }

  /**
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(List<Block> blocklist) {
    assert(blocklist != null && blocklist.size() > 0);
    synchronized (invalidateBlocks) {
      for(Block blk : blocklist) {
        invalidateBlocks.add(blk);
      }
    }
  }

  /**
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    return PendingReplicationWithoutTargets + replicateBlocks.size();
  }

  /**
   * The number of block invalidation items that are pending to 
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }

  public List<BlockTargetPair> getReplicationCommand(int maxTransfers) {
    return replicateBlocks.poll(maxTransfers);
  }

  public BlockInfoContiguousUnderConstruction[] getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockInfoContiguousUnderConstruction> blocks = recoverBlocks.poll(maxTransfers);
    if(blocks == null)
      return null;
    return blocks.toArray(new BlockInfoContiguousUnderConstruction[blocks.size()]);
  }

  /**
   * Remove the specified number of blocks to be invalidated
   */
  public Block[] getInvalidateBlocks(int maxblocks) {
    synchronized (invalidateBlocks) {
      Block[] deleteList = invalidateBlocks.pollToArray(new Block[Math.min(
          invalidateBlocks.size(), maxblocks)]);
      return deleteList.length == 0 ? null : deleteList;
    }
  }

  /**
   * Return the sum of remaining spaces of the specified type. If the remaining
   * space of a storage is less than minSize, it won't be counted toward the
   * sum.
   *
   * @param t The storage type. If null, the type is ignored.
   * @param minSize The minimum free space required.
   * @return the sum of remaining spaces that are bigger than minSize.
   */
  public long getRemaining(StorageType t, long minSize) {
    long remaining = 0;
    for (DatanodeStorageInfo s : getStorageInfos()) {
      if (s.getState() == State.NORMAL &&
          (t == null || s.getStorageType() == t)) {
        long r = s.getRemaining();
        if (r >= minSize) {
          remaining += r;
        }
      }
    }
    return remaining;
  }

  /**
   * @return Approximate number of blocks currently scheduled to be written 
   * to the given storage type of this datanode.
   */
  public int getBlocksScheduled(StorageType t) {
    return (int)(currApproxBlocksScheduled.get(t)
        + prevApproxBlocksScheduled.get(t));
  }

  /**
   * @return Approximate number of blocks currently scheduled to be written 
   * to this datanode.
   */
  public int getBlocksScheduled() {
    return (int)(currApproxBlocksScheduled.sum()
        + prevApproxBlocksScheduled.sum());
  }

  /** Increment the number of blocks scheduled. */
  void incrementBlocksScheduled(StorageType t) {
    currApproxBlocksScheduled.add(t, 1);;
  }
  
  /** Decrement the number of blocks scheduled. */
  void decrementBlocksScheduled(StorageType t) {
    if (prevApproxBlocksScheduled.get(t) > 0) {
      prevApproxBlocksScheduled.subtract(t, 1);
    } else if (currApproxBlocksScheduled.get(t) > 0) {
      currApproxBlocksScheduled.subtract(t, 1);
    } 
    // its ok if both counters are zero.
  }
  
  /** Adjusts curr and prev number of blocks scheduled every few minutes. */
  private void rollBlocksScheduled(long now) {
    if (now - lastBlocksScheduledRollTime > BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled.set(currApproxBlocksScheduled);
      currApproxBlocksScheduled.reset();
      lastBlocksScheduledRollTime = now;
    }
  }
  
  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    // Sufficient to use super equality as datanodes are uniquely identified
    // by DatanodeID
    return (this == obj) || super.equals(obj);
  }

  /** Decommissioning status */
  public class DecommissioningStatus {
    private int underReplicatedBlocks;
    private int decommissionOnlyReplicas;
    private int underReplicatedInOpenFiles;
    private long startTime;
    
    synchronized void set(int underRep,
        int onlyRep, int underConstruction) {
      if (isDecommissionInProgress() == false) {
        return;
      }
      underReplicatedBlocks = underRep;
      decommissionOnlyReplicas = onlyRep;
      underReplicatedInOpenFiles = underConstruction;
    }

    /** @return the number of under-replicated blocks */
    public synchronized int getUnderReplicatedBlocks() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedBlocks;
    }
    /** @return the number of decommission-only replicas */
    public synchronized int getDecommissionOnlyReplicas() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return decommissionOnlyReplicas;
    }
    /** @return the number of under-replicated blocks in open files */
    public synchronized int getUnderReplicatedInOpenFiles() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedInOpenFiles;
    }
    /** Set start time */
    public synchronized void setStartTime(long time) {
      startTime = time;
    }
    /** @return start time */
    public synchronized long getStartTime() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return startTime;
    }
  }  // End of class DecommissioningStatus

  /**
   * Set the flag to indicate if this datanode is disallowed from communicating
   * with the namenode.
   */
  public void setDisallowed(boolean flag) {
    disallowed = flag;
  }
  /** Is the datanode disallowed from communicating with the namenode? */
  public boolean isDisallowed() {
    return disallowed;
  }

  /**
   * @return number of failed volumes in the datanode.
   */
  public int getVolumeFailures() {
    return volumeFailures;
  }

  /**
   * Returns info about volume failures.
   *
   * @return info about volume failures, possibly null
   */
  public VolumeFailureSummary getVolumeFailureSummary() {
    return volumeFailureSummary;
  }

  /**
   * @param nodeReg DatanodeID to update registration for.
   */
  @Override
  public void updateRegInfo(DatanodeID nodeReg) {
    super.updateRegInfo(nodeReg);
    
    // must re-process IBR after re-registration
    for(DatanodeStorageInfo storage : getStorageInfos()) {
      storage.setBlockReportCount(0);
    }
    heartbeatedSinceRegistration = false;
    forceRegistration = false;
  }

  /**
   * @return balancer bandwidth in bytes per second for this datanode
   */
  public long getBalancerBandwidth() {
    return this.bandwidth;
  }

  /**
   * @param bandwidth balancer bandwidth in bytes per second for this datanode
   */
  public void setBalancerBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  @Override
  public String dumpDatanode() {
    StringBuilder sb = new StringBuilder(super.dumpDatanode());
    int repl = replicateBlocks.size();
    if (repl > 0) {
      sb.append(" ").append(repl).append(" blocks to be replicated;");
    }
    int inval = invalidateBlocks.size();
    if (inval > 0) {
      sb.append(" ").append(inval).append(" blocks to be invalidated;");      
    }
    int recover = recoverBlocks.size();
    if (recover > 0) {
      sb.append(" ").append(recover).append(" blocks to be recovered;");
    }
    return sb.toString();
  }

  DatanodeStorageInfo updateStorage(DatanodeStorage s) {
    synchronized (storageMap) {
      DatanodeStorageInfo storage = storageMap.get(s.getStorageID());
      if (storage == null) {
        LOG.info("Adding new storage ID " + s.getStorageID() +
                 " for DN " + getXferAddr());
        storage = new DatanodeStorageInfo(this, s);
        storageMap.put(s.getStorageID(), storage);
      } else if (storage.getState() != s.getState() ||
                 storage.getStorageType() != s.getStorageType()) {
        // For backwards compatibility, make sure that the type and
        // state are updated. Some reports from older datanodes do
        // not include these fields so we may have assumed defaults.
        storage.updateFromStorage(s);
        storageMap.put(storage.getStorageID(), storage);
      }
      return storage;
    }
  }

  /**
   * @return   The time at which we last sent caching directives to this 
   *           DataNode, in monotonic milliseconds.
   */
  public long getLastCachingDirectiveSentTimeMs() {
    return this.lastCachingDirectiveSentTimeMs;
  }

  /**
   * @param time  The time at which we last sent caching directives to this 
   *              DataNode, in monotonic milliseconds.
   */
  public void setLastCachingDirectiveSentTimeMs(long time) {
    this.lastCachingDirectiveSentTimeMs = time;
  }
  
  /**
   * checks whether atleast first block report has been received
   * @return
   */
  public boolean checkBlockReportReceived() {
    if(this.getStorageInfos().length == 0) {
      return false;
    }
    for(DatanodeStorageInfo storageInfo: this.getStorageInfos()) {
      if(storageInfo.getBlockReportCount() == 0 )
        return false;
    }
    return true;
  }

  public void setForceRegistration(boolean force) {
    forceRegistration = force;
  }

  public boolean isRegistered() {
    return isAlive && !forceRegistration;
  }
}

