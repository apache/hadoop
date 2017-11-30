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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.net.DFSTopologyNodeImpl;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends the DatanodeInfo class with ephemeral information (eg
 * health, capacity, what blocks are associated with the Datanode) that is
 * private to the Namenode, ie this class is not exposed to clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeDescriptor extends DatanodeInfo {
  public static final Logger LOG =
      LoggerFactory.getLogger(DatanodeDescriptor.class);
  public static final DatanodeDescriptor[] EMPTY_ARRAY = {};
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min

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
    private final Queue<E> blockq = new LinkedList<>();

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

      List<E> results = new ArrayList<>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }

    /**
     * Returns <tt>true</tt> if the queue contains the specified element.
     */
    synchronized boolean contains(E e) {
      return blockq.contains(e);
    }

    synchronized void clear() {
      blockq.clear();
    }
  }

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

  // Stores status of decommissioning.
  // If node is not decommissioning, do not use this object for anything.
  private final LeavingServiceStatus leavingServiceStatus =
      new LeavingServiceStatus();

  private final Map<String, DatanodeStorageInfo> storageMap =
      new HashMap<>();

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

  /**
   * The time when the last batch of caching directives was sent, in
   * monotonic milliseconds.
   */
  private long lastCachingDirectiveSentTimeMs;

  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  private boolean isAlive = false;
  private boolean needKeyUpdate = false;
  private boolean forceRegistration = false;

  // A system administrator can tune the balancer bandwidth parameter
  // (dfs.datanode.balance.bandwidthPerSec) dynamically by calling
  // "dfsadmin -setBalanacerBandwidth <newbandwidth>", at which point the
  // following 'bandwidth' variable gets updated with the new value for each
  // node. Once the heartbeat command is issued to update the value on the
  // specified datanode, this value will be set back to 0.
  private long bandwidth;

  /** A queue of blocks to be replicated by this datanode */
  private final BlockQueue<BlockTargetPair> replicateBlocks =
      new BlockQueue<>();
  /** A queue of blocks to be erasure coded by this datanode */
  private final BlockQueue<BlockECReconstructionInfo> erasurecodeBlocks =
      new BlockQueue<>();
  /** A queue of blocks to be recovered by this datanode */
  private final BlockQueue<BlockInfo> recoverBlocks = new BlockQueue<>();
  /** A set of blocks to be invalidated by this datanode */
  private final LightWeightHashSet<Block> invalidateBlocks =
      new LightWeightHashSet<>();

  /* Variables for maintaining number of blocks scheduled to be written to
   * this storage. This count is approximate and might be slightly bigger
   * in case of errors (e.g. datanode does not report if an error occurs
   * while writing the block).
   */
  private EnumCounters<StorageType> currApproxBlocksScheduled
      = new EnumCounters<>(StorageType.class);
  private EnumCounters<StorageType> prevApproxBlocksScheduled
      = new EnumCounters<>(StorageType.class);
  private long lastBlocksScheduledRollTime = 0;
  private int volumeFailures = 0;
  private VolumeFailureSummary volumeFailureSummary = null;
  
  /** 
   * When set to true, the node is not in include list and is not allowed
   * to communicate with the namenode
   */
  private boolean disallowed = false;

  // The number of replication work pending before targets are determined
  private int pendingReplicationWithoutTargets = 0;

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

  public CachedBlocksList getPendingCached() {
    return pendingCached;
  }

  public CachedBlocksList getCached() {
    return cached;
  }

  public CachedBlocksList getPendingUncached() {
    return pendingUncached;
  }

  public boolean isAlive() {
    return isAlive;
  }

  public void setAlive(boolean isAlive) {
    this.isAlive = isAlive;
  }

  public synchronized boolean needKeyUpdate() {
    return needKeyUpdate;
  }

  public synchronized void setNeedKeyUpdate(boolean needKeyUpdate) {
    this.needKeyUpdate = needKeyUpdate;
  }

  public LeavingServiceStatus getLeavingServiceStatus() {
    return leavingServiceStatus;
  }

  @VisibleForTesting
  public boolean isHeartbeatedSinceRegistration() {
   return heartbeatedSinceRegistration;
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

  public EnumSet<StorageType> getStorageTypes() {
    EnumSet<StorageType> storageTypes = EnumSet.noneOf(StorageType.class);
    for (DatanodeStorageInfo dsi : getStorageInfos()) {
      storageTypes.add(dsi.getStorageType());
    }
    return storageTypes;
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

  public void resetBlocks() {
    updateStorageStats(this.getStorageReports(), 0L, 0L, 0, 0, null);
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
    }
    this.recoverBlocks.clear();
    this.replicateBlocks.clear();
    this.erasurecodeBlocks.clear();
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
    updateStorageStats(reports, cacheCapacity, cacheUsed, xceiverCount,
        volFailures, volumeFailureSummary);
    setLastUpdate(Time.now());
    setLastUpdateMonotonic(Time.monotonicNow());
    rollBlocksScheduled(getLastUpdateMonotonic());
  }

  private void updateStorageStats(StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int volFailures,
      VolumeFailureSummary volumeFailureSummary) {
    long totalCapacity = 0;
    long totalRemaining = 0;
    long totalBlockPoolUsed = 0;
    long totalDfsUsed = 0;
    long totalNonDfsUsed = 0;
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
      if (this.volumeFailures != volFailures) {
        LOG.info("Number of failed storages changes from {} to {}",
            this.volumeFailures, volFailures);
      }
      synchronized (storageMap) {
        failedStorageInfos =
            new HashSet<>(storageMap.values());
      }
    }

    setCacheCapacity(cacheCapacity);
    setCacheUsed(cacheUsed);
    setXceiverCount(xceiverCount);
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
      totalNonDfsUsed += report.getNonDfsUsed();
    }

    // Update total metrics for the node.
    setCapacity(totalCapacity);
    setRemaining(totalRemaining);
    setBlockPoolUsed(totalBlockPoolUsed);
    setDfsUsed(totalDfsUsed);
    setNonDfsUsed(totalNonDfsUsed);
    if (checkFailedStorages) {
      updateFailedStorage(failedStorageInfos);
    }
    long storageMapSize;
    synchronized (storageMap) {
      storageMapSize = storageMap.size();
    }
    if (storageMapSize != reports.length) {
      pruneStorageMap(reports);
    }
  }

  /**
   * Remove stale storages from storageMap. We must not remove any storages
   * as long as they have associated block replicas.
   */
  private void pruneStorageMap(final StorageReport[] reports) {
    synchronized (storageMap) {
      LOG.debug("Number of storages reported in heartbeat={};"
              + " Number of storages in storageMap={}", reports.length,
          storageMap.size());

      HashMap<String, DatanodeStorageInfo> excessStorages;

      // Init excessStorages with all known storages.
      excessStorages = new HashMap<>(storageMap);

      // Remove storages that the DN reported in the heartbeat.
      for (final StorageReport report : reports) {
        excessStorages.remove(report.getStorage().getStorageID());
      }

      // For each remaining storage, remove it if there are no associated
      // blocks.
      for (final DatanodeStorageInfo storageInfo : excessStorages.values()) {
        if (storageInfo.numBlocks() == 0) {
          DatanodeStorageInfo info =
              storageMap.remove(storageInfo.getStorageID());
          if (!hasStorageType(info.getStorageType())) {
            // we removed a storage, and as result there is no more such storage
            // type, inform the parent about this.
            if (getParent() instanceof DFSTopologyNodeImpl) {
              ((DFSTopologyNodeImpl) getParent()).childRemoveStorage(getName(),
                  info.getStorageType());
            }
          }
          LOG.info("Removed storage {} from DataNode {}", storageInfo, this);
        } else {
          // This can occur until all block reports are received.
          LOG.debug("Deferring removal of stale storage {} with {} blocks",
              storageInfo, storageInfo.numBlocks());
        }
      }
    }
  }

  private void updateFailedStorage(
      Set<DatanodeStorageInfo> failedStorageInfos) {
    for (DatanodeStorageInfo storageInfo : failedStorageInfos) {
      if (storageInfo.getState() != DatanodeStorage.State.FAILED) {
        LOG.info("{} failed.", storageInfo);
        storageInfo.setState(DatanodeStorage.State.FAILED);
      }
    }
  }

  private static class BlockIterator implements Iterator<BlockInfo> {
    private int index = 0;
    private final List<Iterator<BlockInfo>> iterators;
    
    private BlockIterator(final int startBlock,
                          final DatanodeStorageInfo... storages) {
      if(startBlock < 0) {
        throw new IllegalArgumentException(
            "Illegal value startBlock = " + startBlock);
      }
      List<Iterator<BlockInfo>> iterators = new ArrayList<>();
      int s = startBlock;
      int sumBlocks = 0;
      for (DatanodeStorageInfo e : storages) {
        int numBlocks = e.numBlocks();
        sumBlocks += numBlocks;
        if(sumBlocks <= startBlock) {
          s -= numBlocks;
        } else {
          iterators.add(e.getBlockIterator());
        }
      }
      this.iterators = Collections.unmodifiableList(iterators);
      // skip to the storage containing startBlock
      for(; s > 0 && hasNext(); s--) {
        next();
      }
    }

    @Override
    public boolean hasNext() {
      update();
      return index < iterators.size() && iterators.get(index).hasNext();
    }

    @Override
    public BlockInfo next() {
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

  Iterator<BlockInfo> getBlockIterator() {
    return getBlockIterator(0);
  }

  /**
   * Get iterator, which starts iterating from the specified block.
   */
  Iterator<BlockInfo> getBlockIterator(final int startBlock) {
    return new BlockIterator(startBlock, getStorageInfos());
  }

  void incrementPendingReplicationWithoutTargets() {
    pendingReplicationWithoutTargets++;
  }

  void decrementPendingReplicationWithoutTargets() {
    pendingReplicationWithoutTargets--;
  }

  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeStorageInfo[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(new BlockTargetPair(block, targets));
  }

  /**
   * Store block erasure coding work.
   */
  void addBlockToBeErasureCoded(ExtendedBlock block,
      DatanodeDescriptor[] sources, DatanodeStorageInfo[] targets,
      byte[] liveBlockIndices, ErasureCodingPolicy ecPolicy) {
    assert (block != null && sources != null && sources.length > 0);
    BlockECReconstructionInfo task = new BlockECReconstructionInfo(block,
        sources, targets, liveBlockIndices, ecPolicy);
    erasurecodeBlocks.offer(task);
    BlockManager.LOG.debug("Adding block reconstruction task " + task + "to "
        + getName() + ", current queue size is " + erasurecodeBlocks.size());
  }

  /**
   * Store block recovery work.
   */
  void addBlockToBeRecovered(BlockInfo block) {
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
   * The number of work items that are pending to be replicated.
   */
  int getNumberOfBlocksToBeReplicated() {
    return pendingReplicationWithoutTargets + replicateBlocks.size();
  }

  /**
   * The number of work items that are pending to be reconstructed.
   */
  @VisibleForTesting
  public int getNumberOfBlocksToBeErasureCoded() {
    return erasurecodeBlocks.size();
  }

  int getNumberOfReplicateBlocks() {
    return replicateBlocks.size();
  }

  List<BlockTargetPair> getReplicationCommand(int maxTransfers) {
    return replicateBlocks.poll(maxTransfers);
  }

  public List<BlockECReconstructionInfo> getErasureCodeCommand(
      int maxTransfers) {
    return erasurecodeBlocks.poll(maxTransfers);
  }

  public BlockInfo[] getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockInfo> blocks = recoverBlocks.poll(maxTransfers);
    if(blocks == null)
      return null;
    return blocks.toArray(new BlockInfo[blocks.size()]);
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

  @VisibleForTesting
  public boolean containsInvalidateBlock(Block block) {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.contains(block);
    }
  }

  /**
   * Find whether the datanode contains good storage of given type to
   * place block of size <code>blockSize</code>.
   *
   * <p>Currently datanode only cares about the storage type, in this
   * method, the first storage of given type we see is returned.
   *
   * @param t requested storage type
   * @param blockSize requested block size
   */
  public DatanodeStorageInfo chooseStorage4Block(StorageType t,
      long blockSize) {
    final long requiredSize =
        blockSize * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE;
    final long scheduledSize = blockSize * getBlocksScheduled(t);
    long remaining = 0;
    DatanodeStorageInfo storage = null;
    for (DatanodeStorageInfo s : getStorageInfos()) {
      if (s.getState() == State.NORMAL && s.getStorageType() == t) {
        if (storage == null) {
          storage = s;
        }
        long r = s.getRemaining();
        if (r >= requiredSize) {
          remaining += r;
        }
      }
    }
    if (requiredSize > remaining - scheduledSize) {
      LOG.debug(
          "The node {} does not have enough {} space (required={},"
          + " scheduled={}, remaining={}).",
          this, t, requiredSize, scheduledSize, remaining);
      return null;
    }
    return storage;
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
    currApproxBlocksScheduled.add(t, 1);
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

  /** Leaving service status. */
  public class LeavingServiceStatus {
    private int underReplicatedBlocks;
    private int outOfServiceOnlyReplicas;
    private int underReplicatedInOpenFiles;
    private long startTime;
    
    synchronized void set(int underRepInOpenFiles, int underRepBlocks,
        int outOfServiceOnlyRep) {
      if (!isDecommissionInProgress() && !isEnteringMaintenance()) {
        return;
      }
      underReplicatedInOpenFiles = underRepInOpenFiles;
      underReplicatedBlocks = underRepBlocks;
      outOfServiceOnlyReplicas = outOfServiceOnlyRep;
    }

    /** @return the number of under-replicated blocks */
    public synchronized int getUnderReplicatedBlocks() {
      if (!isDecommissionInProgress() && !isEnteringMaintenance()) {
        return 0;
      }
      return underReplicatedBlocks;
    }
    /** @return the number of blocks with out-of-service-only replicas */
    public synchronized int getOutOfServiceOnlyReplicas() {
      if (!isDecommissionInProgress() && !isEnteringMaintenance()) {
        return 0;
      }
      return outOfServiceOnlyReplicas;
    }
    /** @return the number of under-replicated blocks in open files */
    public synchronized int getUnderReplicatedInOpenFiles() {
      if (!isDecommissionInProgress() && !isEnteringMaintenance()) {
        return 0;
      }
      return underReplicatedInOpenFiles;
    }
    /** Set start time */
    public synchronized void setStartTime(long time) {
      if (!isDecommissionInProgress() && !isEnteringMaintenance()) {
        return;
      }
      startTime = time;
    }
    /** @return start time */
    public synchronized long getStartTime() {
      if (!isDecommissionInProgress() && !isEnteringMaintenance()) {
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
  public synchronized long getBalancerBandwidth() {
    return this.bandwidth;
  }

  /**
   * @param bandwidth balancer bandwidth in bytes per second for this datanode
   */
  public synchronized void setBalancerBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  @Override
  public String dumpDatanode() {
    StringBuilder sb = new StringBuilder(super.dumpDatanode());
    int repl = replicateBlocks.size();
    if (repl > 0) {
      sb.append(" ").append(repl).append(" blocks to be replicated;");
    }
    int ec = erasurecodeBlocks.size();
    if(ec > 0) {
      sb.append(" ").append(ec).append(" blocks to be erasure coded;");
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
      DFSTopologyNodeImpl parent = null;
      if (getParent() instanceof DFSTopologyNodeImpl) {
        parent = (DFSTopologyNodeImpl) getParent();
      }

      if (storage == null) {
        LOG.info("Adding new storage ID {} for DN {}", s.getStorageID(),
            getXferAddr());
        StorageType type = s.getStorageType();
        if (!hasStorageType(type) && parent != null) {
          // we are about to add a type this node currently does not have,
          // inform the parent that a new type is added to this datanode
          parent.childAddStorage(getName(), s.getStorageType());
        }
        storage = new DatanodeStorageInfo(this, s);
        storageMap.put(s.getStorageID(), storage);
      } else if (storage.getState() != s.getState() ||
                 storage.getStorageType() != s.getStorageType()) {
        // For backwards compatibility, make sure that the type and
        // state are updated. Some reports from older datanodes do
        // not include these fields so we may have assumed defaults.
        StorageType oldType = storage.getStorageType();
        StorageType newType = s.getStorageType();
        if (oldType != newType && !hasStorageType(newType) && parent != null) {
          // we are about to add a type this node currently does not have
          // inform the parent that a new type is added to this datanode
          // if old == new, nothing's changed. don't bother
          parent.childAddStorage(getName(), newType);
        }
        storage.updateFromStorage(s);
        storageMap.put(storage.getStorageID(), storage);
        if (oldType != newType && !hasStorageType(oldType) && parent != null) {
          // there is no more old type storage on this datanode, inform parent
          // about this change.
          parent.childRemoveStorage(getName(), oldType);
        }
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
   * @return whether at least first block report has been received
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
    return isAlive() && !forceRegistration;
  }

  public boolean hasStorageType(StorageType type) {
    for (DatanodeStorageInfo dnStorage : getStorageInfos()) {
      if (dnStorage.getStorageType() == type) {
        return true;
      }
    }
    return false;
  }
}

