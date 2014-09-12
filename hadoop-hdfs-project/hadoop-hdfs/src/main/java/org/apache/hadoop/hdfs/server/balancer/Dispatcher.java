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
package org.apache.hadoop.hdfs.server.balancer;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

/** Dispatching block replica moves between datanodes. */
@InterfaceAudience.Private
public class Dispatcher {
  static final Log LOG = LogFactory.getLog(Dispatcher.class);

  private static final long GB = 1L << 30; // 1GB
  private static final long MAX_BLOCKS_SIZE_TO_FETCH = 2 * GB;

  private static final int MAX_NO_PENDING_MOVE_ITERATIONS = 5;
  private static final long DELAY_AFTER_ERROR = 10 * 1000L; // 10 seconds

  private final NameNodeConnector nnc;
  private final SaslDataTransferClient saslClient;

  /** Set of datanodes to be excluded. */
  private final Set<String> excludedNodes;
  /** Restrict to the following nodes. */
  private final Set<String> includedNodes;

  private final Collection<Source> sources = new HashSet<Source>();
  private final Collection<StorageGroup> targets = new HashSet<StorageGroup>();

  private final GlobalBlockMap globalBlocks = new GlobalBlockMap();
  private final MovedBlocks<StorageGroup> movedBlocks;

  /** Map (datanodeUuid,storageType -> StorageGroup) */
  private final StorageGroupMap storageGroupMap = new StorageGroupMap();

  private NetworkTopology cluster;

  private final ExecutorService moveExecutor;
  private final ExecutorService dispatchExecutor;
  /** The maximum number of concurrent blocks moves at a datanode */
  private final int maxConcurrentMovesPerNode;

  private final AtomicLong bytesMoved = new AtomicLong();

  private static class GlobalBlockMap {
    private final Map<Block, DBlock> map = new HashMap<Block, DBlock>();

    /**
     * Get the block from the map;
     * if the block is not found, create a new block and put it in the map.
     */
    private DBlock get(Block b) {
      DBlock block = map.get(b);
      if (block == null) {
        block = new DBlock(b);
        map.put(b, block);
      }
      return block;
    }
    
    /** Remove all blocks except for the moved blocks. */
    private void removeAllButRetain(MovedBlocks<StorageGroup> movedBlocks) {
      for (Iterator<Block> i = map.keySet().iterator(); i.hasNext();) {
        if (!movedBlocks.contains(i.next())) {
          i.remove();
        }
      }
    }
  }

  static class StorageGroupMap {
    private static String toKey(String datanodeUuid, StorageType storageType) {
      return datanodeUuid + ":" + storageType;
    }

    private final Map<String, StorageGroup> map = new HashMap<String, StorageGroup>();

    StorageGroup get(String datanodeUuid, StorageType storageType) {
      return map.get(toKey(datanodeUuid, storageType));
    }

    void put(StorageGroup g) {
      final String key = toKey(g.getDatanodeInfo().getDatanodeUuid(), g.storageType);
      final StorageGroup existing = map.put(key, g);
      Preconditions.checkState(existing == null);
    }

    int size() {
      return map.size();
    }

    void clear() {
      map.clear();
    }
  }

  /** This class keeps track of a scheduled block move */
  private class PendingMove {
    private DBlock block;
    private Source source;
    private DDatanode proxySource;
    private StorageGroup target;

    private PendingMove() {
    }

    @Override
    public String toString() {
      final Block b = block.getBlock();
      return b + " with size=" + b.getNumBytes() + " from "
          + source.getDisplayName() + " to " + target.getDisplayName()
          + " through " + proxySource.datanode;
    }

    /**
     * Choose a block & a proxy source for this pendingMove whose source &
     * target have already been chosen.
     * 
     * @return true if a block and its proxy are chosen; false otherwise
     */
    private boolean chooseBlockAndProxy() {
      // iterate all source's blocks until find a good one
      for (Iterator<DBlock> i = source.getBlockIterator(); i.hasNext();) {
        if (markMovedIfGoodBlock(i.next())) {
          i.remove();
          return true;
        }
      }
      return false;
    }

    /**
     * @return true if the given block is good for the tentative move.
     */
    private boolean markMovedIfGoodBlock(DBlock block) {
      synchronized (block) {
        synchronized (movedBlocks) {
          if (isGoodBlockCandidate(source, target, block)) {
            this.block = block;
            if (chooseProxySource()) {
              movedBlocks.put(block);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Decided to move " + this);
              }
              return true;
            }
          }
        }
      }
      return false;
    }

    /**
     * Choose a proxy source.
     * 
     * @return true if a proxy is found; otherwise false
     */
    private boolean chooseProxySource() {
      final DatanodeInfo targetDN = target.getDatanodeInfo();
      // if node group is supported, first try add nodes in the same node group
      if (cluster.isNodeGroupAware()) {
        for (StorageGroup loc : block.getLocations()) {
          if (cluster.isOnSameNodeGroup(loc.getDatanodeInfo(), targetDN)
              && addTo(loc)) {
            return true;
          }
        }
      }
      // check if there is replica which is on the same rack with the target
      for (StorageGroup loc : block.getLocations()) {
        if (cluster.isOnSameRack(loc.getDatanodeInfo(), targetDN) && addTo(loc)) {
          return true;
        }
      }
      // find out a non-busy replica
      for (StorageGroup loc : block.getLocations()) {
        if (addTo(loc)) {
          return true;
        }
      }
      return false;
    }

    /** add to a proxy source for specific block movement */
    private boolean addTo(StorageGroup g) {
      final DDatanode dn = g.getDDatanode();
      if (dn.addPendingBlock(this)) {
        proxySource = dn;
        return true;
      }
      return false;
    }

    /** Dispatch the move to the proxy source & wait for the response. */
    private void dispatch() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Start moving " + this);
      }

      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock.connect(
            NetUtils.createSocketAddr(target.getDatanodeInfo().getXferAddr()),
            HdfsServerConstants.READ_TIMEOUT);

        sock.setKeepAlive(true);

        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(),
            block.getBlock());
        final KeyManager km = nnc.getKeyManager(); 
        Token<BlockTokenIdentifier> accessToken = km.getAccessToken(eb);
        IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
            unbufIn, km, accessToken, target.getDatanodeInfo());
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.IO_FILE_BUFFER_SIZE));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            HdfsConstants.IO_FILE_BUFFER_SIZE));

        sendRequest(out, eb, accessToken);
        receiveResponse(in);
        bytesMoved.addAndGet(block.getNumBytes());
        LOG.info("Successfully moved " + this);
      } catch (IOException e) {
        LOG.warn("Failed to move " + this + ": " + e.getMessage());
        // Proxy or target may have some issues, delay before using these nodes
        // further in order to avoid a potential storm of "threads quota
        // exceeded" warnings when the dispatcher gets out of sync with work
        // going on in datanodes.
        proxySource.activateDelay(DELAY_AFTER_ERROR);
        target.getDDatanode().activateDelay(DELAY_AFTER_ERROR);
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);

        proxySource.removePendingBlock(this);
        target.getDDatanode().removePendingBlock(this);

        synchronized (this) {
          reset();
        }
        synchronized (Dispatcher.this) {
          Dispatcher.this.notifyAll();
        }
      }
    }

    /** Send a block replace request to the output stream */
    private void sendRequest(DataOutputStream out, ExtendedBlock eb,
        Token<BlockTokenIdentifier> accessToken) throws IOException {
      new Sender(out).replaceBlock(eb, target.storageType, accessToken,
          source.getDatanodeInfo().getDatanodeUuid(), proxySource.datanode);
    }

    /** Receive a block copy response from the input stream */
    private void receiveResponse(DataInputStream in) throws IOException {
      BlockOpResponseProto response =
          BlockOpResponseProto.parseFrom(vintPrefixed(in));
      while (response.getStatus() == Status.IN_PROGRESS) {
        // read intermediate responses
        response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
      }
      if (response.getStatus() != Status.SUCCESS) {
        if (response.getStatus() == Status.ERROR_ACCESS_TOKEN) {
          throw new IOException("block move failed due to access token error");
        }
        throw new IOException("block move is failed: " + response.getMessage());
      }
    }

    /** reset the object */
    private void reset() {
      block = null;
      source = null;
      proxySource = null;
      target = null;
    }
  }

  /** A class for keeping track of block locations in the dispatcher. */
  private static class DBlock extends MovedBlocks.Locations<StorageGroup> {
    DBlock(Block block) {
      super(block);
    }
  }

  /** The class represents a desired move. */
  static class Task {
    private final StorageGroup target;
    private long size; // bytes scheduled to move

    Task(StorageGroup target, long size) {
      this.target = target;
      this.size = size;
    }

    long getSize() {
      return size;
    }
  }

  /** A class that keeps track of a datanode. */
  static class DDatanode {

    /** A group of storages in a datanode with the same storage type. */
    class StorageGroup {
      final StorageType storageType;
      final long maxSize2Move;
      private long scheduledSize = 0L;

      private StorageGroup(StorageType storageType, long maxSize2Move) {
        this.storageType = storageType;
        this.maxSize2Move = maxSize2Move;
      }

      private DDatanode getDDatanode() {
        return DDatanode.this;
      }

      DatanodeInfo getDatanodeInfo() {
        return DDatanode.this.datanode;
      }

      /** Decide if still need to move more bytes */
      synchronized boolean hasSpaceForScheduling() {
        return availableSizeToMove() > 0L;
      }

      /** @return the total number of bytes that need to be moved */
      synchronized long availableSizeToMove() {
        return maxSize2Move - scheduledSize;
      }

      /** increment scheduled size */
      synchronized void incScheduledSize(long size) {
        scheduledSize += size;
      }

      /** @return scheduled size */
      synchronized long getScheduledSize() {
        return scheduledSize;
      }

      /** Reset scheduled size to zero. */
      synchronized void resetScheduledSize() {
        scheduledSize = 0L;
      }

      /** @return the name for display */
      String getDisplayName() {
        return datanode + ":" + storageType;
      }

      @Override
      public String toString() {
        return getDisplayName();
      }
    }

    final DatanodeInfo datanode;
    final EnumMap<StorageType, StorageGroup> storageMap
        = new EnumMap<StorageType, StorageGroup>(StorageType.class);
    protected long delayUntil = 0L;
    /** blocks being moved but not confirmed yet */
    private final List<PendingMove> pendings;
    private final int maxConcurrentMoves;

    @Override
    public String toString() {
      return getClass().getSimpleName() + ":" + datanode + ":" + storageMap.values();
    }

    private DDatanode(DatanodeStorageReport r, int maxConcurrentMoves) {
      this.datanode = r.getDatanodeInfo();
      this.maxConcurrentMoves = maxConcurrentMoves;
      this.pendings = new ArrayList<PendingMove>(maxConcurrentMoves);
    }

    private void put(StorageType storageType, StorageGroup g) {
      final StorageGroup existing = storageMap.put(storageType, g);
      Preconditions.checkState(existing == null);
    }

    StorageGroup addStorageGroup(StorageType storageType, long maxSize2Move) {
      final StorageGroup g = new StorageGroup(storageType, maxSize2Move);
      put(storageType, g);
      return g;
    }

    Source addSource(StorageType storageType, long maxSize2Move, Dispatcher d) {
      final Source s = d.new Source(storageType, maxSize2Move, this);
      put(storageType, s);
      return s;
    }

    synchronized private void activateDelay(long delta) {
      delayUntil = Time.monotonicNow() + delta;
    }

    synchronized private boolean isDelayActive() {
      if (delayUntil == 0 || Time.monotonicNow() > delayUntil) {
        delayUntil = 0;
        return false;
      }
      return true;
    }

    /** Check if the node can schedule more blocks to move */
    synchronized boolean isPendingQNotFull() {
      return pendings.size() < maxConcurrentMoves;
    }

    /** Check if all the dispatched moves are done */
    synchronized boolean isPendingQEmpty() {
      return pendings.isEmpty();
    }

    /** Add a scheduled block move to the node */
    synchronized boolean addPendingBlock(PendingMove pendingBlock) {
      if (!isDelayActive() && isPendingQNotFull()) {
        return pendings.add(pendingBlock);
      }
      return false;
    }

    /** Remove a scheduled block move from the node */
    synchronized boolean removePendingBlock(PendingMove pendingBlock) {
      return pendings.remove(pendingBlock);
    }
  }

  /** A node that can be the sources of a block move */
  class Source extends DDatanode.StorageGroup {

    private final List<Task> tasks = new ArrayList<Task>(2);
    private long blocksToReceive = 0L;
    /**
     * Source blocks point to the objects in {@link Dispatcher#globalBlocks}
     * because we want to keep one copy of a block and be aware that the
     * locations are changing over time.
     */
    private final List<DBlock> srcBlocks = new ArrayList<DBlock>();

    private Source(StorageType storageType, long maxSize2Move, DDatanode dn) {
      dn.super(storageType, maxSize2Move);
    }

    /** Add a task */
    void addTask(Task task) {
      Preconditions.checkState(task.target != this,
          "Source and target are the same storage group " + getDisplayName());
      incScheduledSize(task.size);
      tasks.add(task);
    }

    /** @return an iterator to this source's blocks */
    Iterator<DBlock> getBlockIterator() {
      return srcBlocks.iterator();
    }

    /**
     * Fetch new blocks of this source from namenode and update this source's
     * block list & {@link Dispatcher#globalBlocks}.
     * 
     * @return the total size of the received blocks in the number of bytes.
     */
    private long getBlockList() throws IOException {
      final long size = Math.min(MAX_BLOCKS_SIZE_TO_FETCH, blocksToReceive);
      final BlocksWithLocations newBlocks = nnc.getBlocks(getDatanodeInfo(), size);

      long bytesReceived = 0;
      for (BlockWithLocations blk : newBlocks.getBlocks()) {
        bytesReceived += blk.getBlock().getNumBytes();
        synchronized (globalBlocks) {
          final DBlock block = globalBlocks.get(blk.getBlock());
          synchronized (block) {
            block.clearLocations();

            // update locations
            final String[] datanodeUuids = blk.getDatanodeUuids();
            final StorageType[] storageTypes = blk.getStorageTypes();
            for (int i = 0; i < datanodeUuids.length; i++) {
              final StorageGroup g = storageGroupMap.get(
                  datanodeUuids[i], storageTypes[i]);
              if (g != null) { // not unknown
                block.addLocation(g);
              }
            }
          }
          if (!srcBlocks.contains(block) && isGoodBlockCandidate(block)) {
            // filter bad candidates
            srcBlocks.add(block);
          }
        }
      }
      return bytesReceived;
    }

    /** Decide if the given block is a good candidate to move or not */
    private boolean isGoodBlockCandidate(DBlock block) {
      for (Task t : tasks) {
        if (Dispatcher.this.isGoodBlockCandidate(this, t.target, block)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Choose a move for the source. The block's source, target, and proxy
     * are determined too. When choosing proxy and target, source &
     * target throttling has been considered. They are chosen only when they
     * have the capacity to support this block move. The block should be
     * dispatched immediately after this method is returned.
     * 
     * @return a move that's good for the source to dispatch immediately.
     */
    private PendingMove chooseNextMove() {
      for (Iterator<Task> i = tasks.iterator(); i.hasNext();) {
        final Task task = i.next();
        final DDatanode target = task.target.getDDatanode();
        PendingMove pendingBlock = new PendingMove();
        if (target.addPendingBlock(pendingBlock)) {
          // target is not busy, so do a tentative block allocation
          pendingBlock.source = this;
          pendingBlock.target = task.target;
          if (pendingBlock.chooseBlockAndProxy()) {
            long blockSize = pendingBlock.block.getNumBytes();
            incScheduledSize(-blockSize);
            task.size -= blockSize;
            if (task.size == 0) {
              i.remove();
            }
            return pendingBlock;
          } else {
            // cancel the tentative move
            target.removePendingBlock(pendingBlock);
          }
        }
      }
      return null;
    }

    /** Iterate all source's blocks to remove moved ones */
    private void removeMovedBlocks() {
      for (Iterator<DBlock> i = getBlockIterator(); i.hasNext();) {
        if (movedBlocks.contains(i.next().getBlock())) {
          i.remove();
        }
      }
    }

    private static final int SOURCE_BLOCKS_MIN_SIZE = 5;

    /** @return if should fetch more blocks from namenode */
    private boolean shouldFetchMoreBlocks() {
      return srcBlocks.size() < SOURCE_BLOCKS_MIN_SIZE && blocksToReceive > 0;
    }

    private static final long MAX_ITERATION_TIME = 20 * 60 * 1000L; // 20 mins

    /**
     * This method iteratively does the following: it first selects a block to
     * move, then sends a request to the proxy source to start the block move
     * when the source's block list falls below a threshold, it asks the
     * namenode for more blocks. It terminates when it has dispatch enough block
     * move tasks or it has received enough blocks from the namenode, or the
     * elapsed time of the iteration has exceeded the max time limit.
     */
    private void dispatchBlocks() {
      final long startTime = Time.monotonicNow();
      this.blocksToReceive = 2 * getScheduledSize();
      boolean isTimeUp = false;
      int noPendingMoveIteration = 0;
      while (!isTimeUp && getScheduledSize() > 0
          && (!srcBlocks.isEmpty() || blocksToReceive > 0)) {
        final PendingMove p = chooseNextMove();
        if (p != null) {
          // Reset no pending move counter
          noPendingMoveIteration=0;
          // move the block
          moveExecutor.execute(new Runnable() {
            @Override
            public void run() {
              p.dispatch();
            }
          });
          continue;
        }

        // Since we cannot schedule any block to move,
        // remove any moved blocks from the source block list and
        removeMovedBlocks(); // filter already moved blocks
        // check if we should fetch more blocks from the namenode
        if (shouldFetchMoreBlocks()) {
          // fetch new blocks
          try {
            blocksToReceive -= getBlockList();
            continue;
          } catch (IOException e) {
            LOG.warn("Exception while getting block list", e);
            return;
          }
        } else {
          // source node cannot find a pending block to move, iteration +1
          noPendingMoveIteration++;
          // in case no blocks can be moved for source node's task,
          // jump out of while-loop after 5 iterations.
          if (noPendingMoveIteration >= MAX_NO_PENDING_MOVE_ITERATIONS) {
            resetScheduledSize();
          }
        }

        // check if time is up or not
        if (Time.monotonicNow() - startTime > MAX_ITERATION_TIME) {
          isTimeUp = true;
          continue;
        }

        // Now we can not schedule any block to move and there are
        // no new blocks added to the source block list, so we wait.
        try {
          synchronized (Dispatcher.this) {
            Dispatcher.this.wait(1000); // wait for targets/sources to be idle
          }
        } catch (InterruptedException ignored) {
        }
      }
    }
  }

  public Dispatcher(NameNodeConnector nnc, Set<String> includedNodes,
      Set<String> excludedNodes, long movedWinWidth, int moverThreads,
      int dispatcherThreads, int maxConcurrentMovesPerNode, Configuration conf) {
    this.nnc = nnc;
    this.excludedNodes = excludedNodes;
    this.includedNodes = includedNodes;
    this.movedBlocks = new MovedBlocks<StorageGroup>(movedWinWidth);

    this.cluster = NetworkTopology.getInstance(conf);

    this.moveExecutor = Executors.newFixedThreadPool(moverThreads);
    this.dispatchExecutor = Executors.newFixedThreadPool(dispatcherThreads);
    this.maxConcurrentMovesPerNode = maxConcurrentMovesPerNode;

    final boolean fallbackToSimpleAuthAllowed = conf.getBoolean(
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.saslClient = new SaslDataTransferClient(
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf), fallbackToSimpleAuthAllowed);
  }

  StorageGroupMap getStorageGroupMap() {
    return storageGroupMap;
  }

  NetworkTopology getCluster() {
    return cluster;
  }
  
  long getBytesMoved() {
    return bytesMoved.get();
  }

  long bytesToMove() {
    Preconditions.checkState(
        storageGroupMap.size() >= sources.size() + targets.size(),
        "Mismatched number of storage groups (" + storageGroupMap.size()
            + " < " + sources.size() + " sources + " + targets.size()
            + " targets)");

    long b = 0L;
    for (Source src : sources) {
      b += src.getScheduledSize();
    }
    return b;
  }

  void add(Source source, StorageGroup target) {
    sources.add(source);
    targets.add(target);
  }

  private boolean shouldIgnore(DatanodeInfo dn) {
    // ignore decommissioned nodes
    final boolean decommissioned = dn.isDecommissioned();
    // ignore decommissioning nodes
    final boolean decommissioning = dn.isDecommissionInProgress();
    // ignore nodes in exclude list
    final boolean excluded = Util.isExcluded(excludedNodes, dn);
    // ignore nodes not in the include list (if include list is not empty)
    final boolean notIncluded = !Util.isIncluded(includedNodes, dn);

    if (decommissioned || decommissioning || excluded || notIncluded) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Excluding datanode " + dn + ": " + decommissioned + ", "
            + decommissioning + ", " + excluded + ", " + notIncluded);
      }
      return true;
    }
    return false;
  }

  /** Get live datanode storage reports and then build the network topology. */
  List<DatanodeStorageReport> init() throws IOException {
    final DatanodeStorageReport[] reports = nnc.getLiveDatanodeStorageReport();
    final List<DatanodeStorageReport> trimmed = new ArrayList<DatanodeStorageReport>(); 
    // create network topology and classify utilization collections:
    // over-utilized, above-average, below-average and under-utilized.
    for (DatanodeStorageReport r : DFSUtil.shuffle(reports)) {
      final DatanodeInfo datanode = r.getDatanodeInfo();
      if (shouldIgnore(datanode)) {
        continue;
      }
      trimmed.add(r);
      cluster.add(datanode);
    }
    return trimmed;
  }

  public DDatanode newDatanode(DatanodeStorageReport r) {
    return new DDatanode(r, maxConcurrentMovesPerNode);
  }

  public boolean dispatchAndCheckContinue() throws InterruptedException {
    return nnc.shouldContinue(dispatchBlockMoves());
  }

  /**
   * Dispatch block moves for each source. The thread selects blocks to move &
   * sends request to proxy source to initiate block move. The process is flow
   * controlled. Block selection is blocked if there are too many un-confirmed
   * block moves.
   * 
   * @return the total number of bytes successfully moved in this iteration.
   */
  private long dispatchBlockMoves() throws InterruptedException {
    final long bytesLastMoved = bytesMoved.get();
    final Future<?>[] futures = new Future<?>[sources.size()];

    final Iterator<Source> i = sources.iterator();
    for (int j = 0; j < futures.length; j++) {
      final Source s = i.next();
      futures[j] = dispatchExecutor.submit(new Runnable() {
        @Override
        public void run() {
          s.dispatchBlocks();
        }
      });
    }

    // wait for all dispatcher threads to finish
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.warn("Dispatcher thread failed", e.getCause());
      }
    }

    // wait for all block moving to be done
    waitForMoveCompletion();

    return bytesMoved.get() - bytesLastMoved;
  }

  /** The sleeping period before checking if block move is completed again */
  static private long blockMoveWaitTime = 30000L;

  /** set the sleeping period for block move completion check */
  static void setBlockMoveWaitTime(long time) {
    blockMoveWaitTime = time;
  }

  /** Wait for all block move confirmations. */
  private void waitForMoveCompletion() {
    for(;;) {
      boolean empty = true;
      for (StorageGroup t : targets) {
        if (!t.getDDatanode().isPendingQEmpty()) {
          empty = false;
          break;
        }
      }
      if (empty) {
        return; //all pending queues are empty
      }
      try {
        Thread.sleep(blockMoveWaitTime);
      } catch (InterruptedException ignored) {
      }
    }
  }

  /**
   * Decide if the block is a good candidate to be moved from source to target.
   * A block is a good candidate if 
   * 1. the block is not in the process of being moved/has not been moved;
   * 2. the block does not have a replica on the target;
   * 3. doing the move does not reduce the number of racks that the block has
   */
  private boolean isGoodBlockCandidate(Source source, StorageGroup target,
      DBlock block) {
    if (source.storageType != target.storageType) {
      return false;
    }
    // check if the block is moved or not
    if (movedBlocks.contains(block.getBlock())) {
      return false;
    }
    if (block.isLocatedOn(target)) {
      return false;
    }
    if (cluster.isNodeGroupAware()
        && isOnSameNodeGroupWithReplicas(target, block, source)) {
      return false;
    }
    if (reduceNumOfRacks(source, target, block)) {
      return false;
    }
    return true;
  }

  /**
   * Determine whether moving the given block replica from source to target
   * would reduce the number of racks of the block replicas.
   */
  private boolean reduceNumOfRacks(Source source, StorageGroup target,
      DBlock block) {
    final DatanodeInfo sourceDn = source.getDatanodeInfo();
    if (cluster.isOnSameRack(sourceDn, target.getDatanodeInfo())) {
      // source and target are on the same rack
      return false;
    }
    boolean notOnSameRack = true;
    synchronized (block) {
      for (StorageGroup loc : block.getLocations()) {
        if (cluster.isOnSameRack(loc.getDatanodeInfo(), target.getDatanodeInfo())) {
          notOnSameRack = false;
          break;
        }
      }
    }
    if (notOnSameRack) {
      // target is not on the same rack as any replica
      return false;
    }
    for (StorageGroup g : block.getLocations()) {
      if (g != source && cluster.isOnSameRack(g.getDatanodeInfo(), sourceDn)) {
        // source is on the same rack of another replica
        return false;
      }
    }
    return true;
  }

  /**
   * Check if there are any replica (other than source) on the same node group
   * with target. If true, then target is not a good candidate for placing
   * specific replica as we don't want 2 replicas under the same nodegroup.
   * 
   * @return true if there are any replica (other than source) on the same node
   *         group with target
   */
  private boolean isOnSameNodeGroupWithReplicas(
      StorageGroup target, DBlock block, Source source) {
    final DatanodeInfo targetDn = target.getDatanodeInfo();
    for (StorageGroup g : block.getLocations()) {
      if (g != source && cluster.isOnSameNodeGroup(g.getDatanodeInfo(), targetDn)) {
        return true;
      }
    }
    return false;
  }

  /** Reset all fields in order to prepare for the next iteration */
  void reset(Configuration conf) {
    cluster = NetworkTopology.getInstance(conf);
    storageGroupMap.clear();
    sources.clear();
    targets.clear();
    globalBlocks.removeAllButRetain(movedBlocks);
    movedBlocks.cleanup();
  }

  /** shutdown thread pools */
  void shutdownNow() {
    dispatchExecutor.shutdownNow();
    moveExecutor.shutdownNow();
  }

  static class Util {
    /** @return true if data node is part of the excludedNodes. */
    static boolean isExcluded(Set<String> excludedNodes, DatanodeInfo dn) {
      return isIn(excludedNodes, dn);
    }

    /**
     * @return true if includedNodes is empty or data node is part of the
     *         includedNodes.
     */
    static boolean isIncluded(Set<String> includedNodes, DatanodeInfo dn) {
      return (includedNodes.isEmpty() || isIn(includedNodes, dn));
    }

    /**
     * Match is checked using host name , ip address with and without port
     * number.
     * 
     * @return true if the datanode's transfer address matches the set of nodes.
     */
    private static boolean isIn(Set<String> datanodes, DatanodeInfo dn) {
      return isIn(datanodes, dn.getPeerHostName(), dn.getXferPort())
          || isIn(datanodes, dn.getIpAddr(), dn.getXferPort())
          || isIn(datanodes, dn.getHostName(), dn.getXferPort());
    }

    /** @return true if nodes contains host or host:port */
    private static boolean isIn(Set<String> nodes, String host, int port) {
      if (host == null) {
        return false;
      }
      return (nodes.contains(host) || nodes.contains(host + ":" + port));
    }

    /**
     * Parse a comma separated string to obtain set of host names
     * 
     * @return set of host names
     */
    static Set<String> parseHostList(String string) {
      String[] addrs = StringUtils.getTrimmedStrings(string);
      return new HashSet<String>(Arrays.asList(addrs));
    }

    /**
     * Read set of host names from a file
     * 
     * @return set of host names
     */
    static Set<String> getHostListFromFile(String fileName, String type) {
      Set<String> nodes = new HashSet<String>();
      try {
        HostsFileReader.readFileToSet(type, fileName, nodes);
        return StringUtils.getTrimmedStrings(nodes);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Failed to read host list from file: " + fileName);
      }
    }
  }
}
