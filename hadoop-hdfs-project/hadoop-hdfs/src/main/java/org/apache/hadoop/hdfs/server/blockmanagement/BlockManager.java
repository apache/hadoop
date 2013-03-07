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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportIterator;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;

import static org.apache.hadoop.util.ExitUtil.terminate;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.Daemon;

import com.google.common.annotations.VisibleForTesting;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 */
@InterfaceAudience.Private
public class BlockManager {

  static final Log LOG = LogFactory.getLog(BlockManager.class);
  static final Log blockLog = NameNode.blockStateChangeLog;

  /** Default load factor of map */
  public static final float DEFAULT_MAP_LOAD_FACTOR = 0.75f;

  private final Namesystem namesystem;

  private final DatanodeManager datanodeManager;
  private final HeartbeatManager heartbeatManager;
  private final BlockTokenSecretManager blockTokenSecretManager;

  private volatile long pendingReplicationBlocksCount = 0L;
  private volatile long corruptReplicaBlocksCount = 0L;
  private volatile long underReplicatedBlocksCount = 0L;
  private volatile long scheduledReplicationBlocksCount = 0L;
  private volatile long excessBlocksCount = 0L;
  
  /** Used by metrics */
  public long getPendingReplicationBlocksCount() {
    return pendingReplicationBlocksCount;
  }
  /** Used by metrics */
  public long getUnderReplicatedBlocksCount() {
    return underReplicatedBlocksCount;
  }
  /** Used by metrics */
  public long getCorruptReplicaBlocksCount() {
    return corruptReplicaBlocksCount;
  }
  /** Used by metrics */
  public long getScheduledReplicationBlocksCount() {
    return scheduledReplicationBlocksCount;
  }
  /** Used by metrics */
  public long getPendingDeletionBlocksCount() {
    return invalidateBlocks.numBlocks();
  }
  /** Used by metrics */
  public long getExcessBlocksCount() {
    return excessBlocksCount;
  }

  /**replicationRecheckInterval is how often namenode checks for new replication work*/
  private final long replicationRecheckInterval;
  
  /**
   * Mapping: Block -> { INode, datanodes, self ref }
   * Updated only in response to client-sent information.
   */
  final BlocksMap blocksMap;

  /** Replication thread. */
  final Daemon replicationThread = new Daemon(new ReplicationMonitor());
  
  /** Store blocks -> datanodedescriptor(s) map of corrupt replicas */
  final CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();

  /** Blocks to be invalidated. */
  private final InvalidateBlocks invalidateBlocks;

  //
  // Keeps a TreeSet for every named node. Each treeset contains
  // a list of the blocks that are "extra" at that location. We'll
  // eventually remove these extras.
  // Mapping: StorageID -> TreeSet<Block>
  //
  public final Map<String, LightWeightLinkedSet<Block>> excessReplicateMap =
    new TreeMap<String, LightWeightLinkedSet<Block>>();

  //
  // Store set of Blocks that need to be replicated 1 or more times.
  // We also store pending replication-orders.
  //
  public final UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();
  @VisibleForTesting
  final PendingReplicationBlocks pendingReplications;

  /** The maximum number of replicas allowed for a block */
  public final short maxReplication;
  /**
   * The maximum number of outgoing replication streams a given node should have
   * at one time considering all but the highest priority replications needed.
   */
  int maxReplicationStreams;
  /**
   * The maximum number of outgoing replication streams a given node should have
   * at one time.
   */
  int replicationStreamsHardLimit;
  /** Minimum copies needed or else write is disallowed */
  public final short minReplication;
  /** Default number of replicas */
  public final int defaultReplication;
  /** The maximum number of entries returned by getCorruptInodes() */
  final int maxCorruptFilesReturned;

  /** variable to enable check for enough racks */
  final boolean shouldCheckForEnoughRacks;

  /** for block replicas placement */
  private BlockPlacementPolicy blockplacement;
  
  public BlockManager(final Namesystem namesystem, final FSClusterStats stats,
      final Configuration conf) throws IOException {
    this.namesystem = namesystem;
    datanodeManager = new DatanodeManager(this, namesystem, conf);
    heartbeatManager = datanodeManager.getHeartbeatManager();
    invalidateBlocks = new InvalidateBlocks(datanodeManager);

    blocksMap = new BlocksMap(DEFAULT_MAP_LOAD_FACTOR);
    blockplacement = BlockPlacementPolicy.getInstance(
        conf, stats, datanodeManager.getNetworkTopology());
    pendingReplications = new PendingReplicationBlocks(conf.getInt(
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT) * 1000L);

    blockTokenSecretManager = createBlockTokenSecretManager(conf);

    this.maxCorruptFilesReturned = conf.getInt(
      DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY,
      DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED);
    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 
                                          DFSConfigKeys.DFS_REPLICATION_DEFAULT);

    final int maxR = conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY, 
                                 DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
    final int minR = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
                                 DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    if (minR <= 0)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " <= 0");
    if (maxR > Short.MAX_VALUE)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR + " > " + Short.MAX_VALUE);
    if (minR > maxR)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " > "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR);
    this.minReplication = (short)minR;
    this.maxReplication = (short)maxR;

    this.maxReplicationStreams =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT);
    this.replicationStreamsHardLimit =
        conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT);
    this.shouldCheckForEnoughRacks =
        conf.get(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY) == null
            ? false : true;
    
    this.replicationRecheckInterval = 
      conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 
                  DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000L;
    LOG.info("defaultReplication = " + defaultReplication);
    LOG.info("maxReplication     = " + maxReplication);
    LOG.info("minReplication     = " + minReplication);
    LOG.info("maxReplicationStreams      = " + maxReplicationStreams);
    LOG.info("shouldCheckForEnoughRacks  = " + shouldCheckForEnoughRacks);
    LOG.info("replicationRecheckInterval = " + replicationRecheckInterval);
  }

  private static BlockTokenSecretManager createBlockTokenSecretManager(
      final Configuration conf) throws IOException {
    final boolean isEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + "=" + isEnabled);

    if (!isEnabled) {
      return null;
    }

    final long updateMin = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT);
    final long lifetimeMin = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY
        + "=" + updateMin + " min(s), "
        + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY
        + "=" + lifetimeMin + " min(s)");
    return new BlockTokenSecretManager(true,
        updateMin*60*1000L, lifetimeMin*60*1000L);
  }

  /** get the BlockTokenSecretManager */
  BlockTokenSecretManager getBlockTokenSecretManager() {
    return blockTokenSecretManager;
  }

  private boolean isBlockTokenEnabled() {
    return blockTokenSecretManager != null;
  }

  /** Should the access keys be updated? */
  boolean shouldUpdateBlockKey(final long updateTime) throws IOException {
    return isBlockTokenEnabled()? blockTokenSecretManager.updateKeys(updateTime)
        : false;
  }

  public void activate(Configuration conf) {
    pendingReplications.start();
    datanodeManager.activate(conf);
    this.replicationThread.start();
  }

  public void close() {
    if (pendingReplications != null) pendingReplications.stop();
    blocksMap.close();
    datanodeManager.close();
    if (replicationThread != null) replicationThread.interrupt();
  }

  /** @return the datanodeManager */
  public DatanodeManager getDatanodeManager() {
    return datanodeManager;
  }

  /** @return the BlockPlacementPolicy */
  public BlockPlacementPolicy getBlockPlacementPolicy() {
    return blockplacement;
  }

  /** Set BlockPlacementPolicy */
  public void setBlockPlacementPolicy(BlockPlacementPolicy newpolicy) {
    if (newpolicy == null) {
      throw new HadoopIllegalArgumentException("newpolicy == null");
    }
    this.blockplacement = newpolicy;
  }

  /** Dump meta data to out. */
  public void metaSave(PrintWriter out) {
    assert namesystem.hasWriteLock();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    datanodeManager.fetchDatanodes(live, dead, false);
    out.println("Live Datanodes: " + live.size());
    out.println("Dead Datanodes: " + dead.size());
    //
    // Dump contents of neededReplication
    //
    synchronized (neededReplications) {
      out.println("Metasave: Blocks waiting for replication: " + 
                  neededReplications.size());
      for (Block block : neededReplications) {
        List<DatanodeDescriptor> containingNodes =
                                          new ArrayList<DatanodeDescriptor>();
        List<DatanodeDescriptor> containingLiveReplicasNodes =
          new ArrayList<DatanodeDescriptor>();
        
        NumberReplicas numReplicas = new NumberReplicas();
        // source node returned is not used
        chooseSourceDatanode(block, containingNodes,
            containingLiveReplicasNodes, numReplicas,
            UnderReplicatedBlocks.LEVEL);
        assert containingLiveReplicasNodes.size() == numReplicas.liveReplicas();
        int usableReplicas = numReplicas.liveReplicas() +
                             numReplicas.decommissionedReplicas();
       
        if (block instanceof BlockInfo) {
          String fileName = ((BlockInfo)block).getINode().getFullPathName();
          out.print(fileName + ": ");
        }
        // l: == live:, d: == decommissioned c: == corrupt e: == excess
        out.print(block + ((usableReplicas > 0)? "" : " MISSING") + 
                  " (replicas:" +
                  " l: " + numReplicas.liveReplicas() +
                  " d: " + numReplicas.decommissionedReplicas() +
                  " c: " + numReplicas.corruptReplicas() +
                  " e: " + numReplicas.excessReplicas() + ") "); 

        Collection<DatanodeDescriptor> corruptNodes = 
                                      corruptReplicas.getNodes(block);
        
        for (Iterator<DatanodeDescriptor> jt = blocksMap.nodeIterator(block);
             jt.hasNext();) {
          DatanodeDescriptor node = jt.next();
          String state = "";
          if (corruptNodes != null && corruptNodes.contains(node)) {
            state = "(corrupt)";
          } else if (node.isDecommissioned() || 
              node.isDecommissionInProgress()) {
            state = "(decommissioned)";
          }          
          out.print(" " + node + state + " : ");
        }
        out.println("");
      }
    }

    // Dump blocks from pendingReplication
    pendingReplications.metaSave(out);

    // Dump blocks that are waiting to be deleted
    invalidateBlocks.dump(out);

    // Dump all datanodes
    getDatanodeManager().datanodeDump(out);
  }

  /** @return maxReplicationStreams */
  public int getMaxReplicationStreams() {
    return maxReplicationStreams;
  }

  /**
   * @param block
   * @return true if the block has minimum replicas
   */
  public boolean checkMinReplication(Block block) {
    return (countNodes(block).liveReplicas() >= minReplication);
  }

  /**
   * Commit a block of a file
   * 
   * @param block block to be committed
   * @param commitBlock - contains client reported block length and generation
   * @return true if the block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private boolean commitBlock(final BlockInfoUnderConstruction block,
      final Block commitBlock) throws IOException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED)
      return false;
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
      "commitBlock length is less than the stored one "
      + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
    block.commitBlock(commitBlock);
    return true;
  }
  
  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum replication requirement
   * 
   * @param fileINode file inode
   * @param commitBlock - contains client reported block length and generation
   * @return true if the last block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  public boolean commitOrCompleteLastBlock(INodeFileUnderConstruction fileINode, 
      Block commitBlock) throws IOException {
    if(commitBlock == null)
      return false; // not committing, this is a block allocation retry
    BlockInfo lastBlock = fileINode.getLastBlock();
    if(lastBlock == null)
      return false; // no blocks in file yet
    if(lastBlock.isComplete())
      return false; // already completed (e.g. by syncBlock)
    
    final boolean b = commitBlock((BlockInfoUnderConstruction)lastBlock, commitBlock);
    if(countNodes(lastBlock).liveReplicas() >= minReplication)
      completeBlock(fileINode,fileINode.numBlocks()-1);
    return b;
  }

  /**
   * Convert a specified block of the file to a complete block.
   * @param fileINode file
   * @param blkIndex  block index in the file
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private BlockInfo completeBlock(final INodeFile fileINode,
      final int blkIndex) throws IOException {
    return completeBlock(fileINode, blkIndex, false);
  }

  public BlockInfo completeBlock(final INodeFile fileINode, 
      final int blkIndex, final boolean force) throws IOException {
    if(blkIndex < 0)
      return null;
    BlockInfo curBlock = fileINode.getBlocks()[blkIndex];
    if(curBlock.isComplete())
      return curBlock;
    BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction)curBlock;
    if(!force && ucBlock.numNodes() < minReplication)
      throw new IOException("Cannot complete block: " +
          "block does not satisfy minimal replication requirement.");
    if(!force && ucBlock.getBlockUCState() != BlockUCState.COMMITTED)
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
    // replace penultimate block in file
    fileINode.setBlock(blkIndex, completeBlock);
    // replace block in the blocksMap
    return blocksMap.replaceBlock(completeBlock);
  }

  private BlockInfo completeBlock(final INodeFile fileINode,
      final BlockInfo block) throws IOException {
    BlockInfo[] fileBlocks = fileINode.getBlocks();
    for(int idx = 0; idx < fileBlocks.length; idx++)
      if(fileBlocks[idx] == block) {
        return completeBlock(fileINode, idx);
      }
    return block;
  }

  /**
   * Convert the last block of the file to an under construction block.<p>
   * The block is converted only if the file has blocks and the last one
   * is a partial block (its size is less than the preferred block size).
   * The converted block is returned to the client.
   * The client uses the returned block locations to form the data pipeline
   * for this block.<br>
   * The methods returns null if there is no partial block at the end.
   * The client is supposed to allocate a new block with the next call.
   *
   * @param fileINode file
   * @return the last block locations if the block is partial or null otherwise
   */
  public LocatedBlock convertLastBlockToUnderConstruction(
      INodeFileUnderConstruction fileINode) throws IOException {
    BlockInfo oldBlock = fileINode.getLastBlock();
    if(oldBlock == null ||
        fileINode.getPreferredBlockSize() == oldBlock.getNumBytes())
      return null;
    assert oldBlock == getStoredBlock(oldBlock) :
      "last block of the file is not in blocksMap";

    DatanodeDescriptor[] targets = getNodes(oldBlock);

    BlockInfoUnderConstruction ucBlock =
      fileINode.setLastBlock(oldBlock, targets);
    blocksMap.replaceBlock(ucBlock);

    // Remove block from replication queue.
    updateNeededReplications(oldBlock, 0, 0);

    // remove this block from the list of pending blocks to be deleted. 
    for (DatanodeDescriptor dd : targets) {
      String datanodeId = dd.getStorageID();
      invalidateBlocks.remove(datanodeId, oldBlock);
    }

    final long fileLength = fileINode.computeContentSummary().getLength();
    final long pos = fileLength - ucBlock.getNumBytes();
    return createLocatedBlock(ucBlock, pos, AccessMode.WRITE);
  }

  /**
   * Get all valid locations of the block
   */
  private List<String> getValidLocations(Block block) {
    ArrayList<String> machineSet =
      new ArrayList<String>(blocksMap.numNodes(block));
    for(Iterator<DatanodeDescriptor> it =
      blocksMap.nodeIterator(block); it.hasNext();) {
      String storageID = it.next().getStorageID();
      // filter invalidate replicas
      if(!invalidateBlocks.contains(storageID, block)) {
        machineSet.add(storageID);
      }
    }
    return machineSet;
  }

  private List<LocatedBlock> createLocatedBlockList(final BlockInfo[] blocks,
      final long offset, final long length, final int nrBlocksToReturn,
      final AccessMode mode) throws IOException {
    int curBlk = 0;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return Collections.<LocatedBlock>emptyList();

    long endOff = offset + length;
    List<LocatedBlock> results = new ArrayList<LocatedBlock>(blocks.length);
    do {
      results.add(createLocatedBlock(blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff 
          && curBlk < blocks.length
          && results.size() < nrBlocksToReturn);
    return results;
  }

  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos,
    final BlockTokenSecretManager.AccessMode mode) throws IOException {
    final LocatedBlock lb = createLocatedBlock(blk, pos);
    if (mode != null) {
      setBlockToken(lb, mode);
    }
    return lb;
  }

  /** @return a LocatedBlock for the given block */
  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos
      ) throws IOException {
    if (blk instanceof BlockInfoUnderConstruction) {
      if (blk.isComplete()) {
        throw new IOException(
            "blk instanceof BlockInfoUnderConstruction && blk.isComplete()"
            + ", blk=" + blk);
      }
      final BlockInfoUnderConstruction uc = (BlockInfoUnderConstruction)blk;
      final DatanodeDescriptor[] locations = uc.getExpectedLocations();
      final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
      return new LocatedBlock(eb, locations, pos, false);
    }

    // get block locations
    final int numCorruptNodes = countNodes(blk).corruptReplicas();
    final int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blk);
    if (numCorruptNodes != numCorruptReplicas) {
      LOG.warn("Inconsistent number of corrupt replicas for "
          + blk + " blockMap has " + numCorruptNodes
          + " but corrupt replicas map has " + numCorruptReplicas);
    }

    final int numNodes = blocksMap.numNodes(blk);
    final boolean isCorrupt = numCorruptNodes == numNodes;
    final int numMachines = isCorrupt ? numNodes: numNodes - numCorruptNodes;
    final DatanodeDescriptor[] machines = new DatanodeDescriptor[numMachines];
    if (numMachines > 0) {
      int j = 0;
      for(Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(blk);
          it.hasNext();) {
        final DatanodeDescriptor d = it.next();
        final boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blk, d);
        if (isCorrupt || (!isCorrupt && !replicaCorrupt))
          machines[j++] = d;
      }
    }
    final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
    return new LocatedBlock(eb, machines, pos, isCorrupt);
  }

  /** Create a LocatedBlocks. */
  public LocatedBlocks createLocatedBlocks(final BlockInfo[] blocks,
      final long fileSizeExcludeBlocksUnderConstruction,
      final boolean isFileUnderConstruction,
      final long offset, final long length, final boolean needBlockToken
      ) throws IOException {
    assert namesystem.hasReadOrWriteLock();
    if (blocks == null) {
      return null;
    } else if (blocks.length == 0) {
      return new LocatedBlocks(0, isFileUnderConstruction,
          Collections.<LocatedBlock>emptyList(), null, false);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("blocks = " + java.util.Arrays.asList(blocks));
      }
      final AccessMode mode = needBlockToken? AccessMode.READ: null;
      final List<LocatedBlock> locatedblocks = createLocatedBlockList(
          blocks, offset, length, Integer.MAX_VALUE, mode);

      final BlockInfo last = blocks[blocks.length - 1];
      final long lastPos = last.isComplete()?
          fileSizeExcludeBlocksUnderConstruction - last.getNumBytes()
          : fileSizeExcludeBlocksUnderConstruction;
      final LocatedBlock lastlb = createLocatedBlock(last, lastPos, mode);
      return new LocatedBlocks(
          fileSizeExcludeBlocksUnderConstruction, isFileUnderConstruction,
          locatedblocks, lastlb, last.isComplete());
    }
  }

  /** @return current access keys. */
  public ExportedBlockKeys getBlockKeys() {
    return isBlockTokenEnabled()? blockTokenSecretManager.exportKeys()
        : ExportedBlockKeys.DUMMY_KEYS;
  }

  /** Generate a block token for the located block. */
  public void setBlockToken(final LocatedBlock b,
      final BlockTokenSecretManager.AccessMode mode) throws IOException {
    if (isBlockTokenEnabled()) {
      b.setBlockToken(blockTokenSecretManager.generateToken(b.getBlock(), 
          EnumSet.of(mode)));
    }    
  }

  void addKeyUpdateCommand(final List<DatanodeCommand> cmds,
      final DatanodeDescriptor nodeinfo) {
    // check access key update
    if (isBlockTokenEnabled() && nodeinfo.needKeyUpdate) {
      cmds.add(new KeyUpdateCommand(blockTokenSecretManager.exportKeys()));
      nodeinfo.needKeyUpdate = false;
    }
  }

  /**
   * Clamp the specified replication between the minimum and the maximum
   * replication levels.
   */
  public short adjustReplication(short replication) {
    return replication < minReplication? minReplication
        : replication > maxReplication? maxReplication: replication;
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration.
   */
   public void verifyReplication(String src,
                          short replication,
                          String clientName) throws IOException {

    if (replication >= minReplication && replication <= maxReplication) {
      //common case. avoid building 'text'
      return;
    }
    
    String text = "file " + src 
      + ((clientName != null) ? " on client " + clientName : "")
      + ".\n"
      + "Requested replication " + replication;

    if (replication > maxReplication)
      throw new IOException(text + " exceeds maximum " + maxReplication);

    if (replication < minReplication)
      throw new IOException(text + " is less than the required minimum " +
                            minReplication);
  }

  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   * 
   * @param datanode on which blocks are located
   * @param size total size of blocks
   */
  public BlocksWithLocations getBlocks(DatanodeID datanode, long size
      ) throws IOException {
    namesystem.readLock();
    try {
      namesystem.checkSuperuserPrivilege();
      return getBlocksWithLocations(datanode, size);  
    } finally {
      namesystem.readUnlock();
    }
  }

  /** Get all blocks with location information from a datanode. */
  private BlocksWithLocations getBlocksWithLocations(final DatanodeID datanode,
      final long size) throws UnregisteredNodeException {
    final DatanodeDescriptor node = getDatanodeManager().getDatanode(datanode);
    if (node == null) {
      blockLog.warn("BLOCK* getBlocks: "
          + "Asking for blocks from an unrecorded node " + datanode.getName());
      throw new HadoopIllegalArgumentException(
          "Datanode " + datanode.getName() + " not found.");
    }

    int numBlocks = node.numBlocks();
    if(numBlocks == 0) {
      return new BlocksWithLocations(new BlockWithLocations[0]);
    }
    Iterator<BlockInfo> iter = node.getBlockIterator();
    int startBlock = DFSUtil.getRandom().nextInt(numBlocks); // starting from a random block
    // skip blocks
    for(int i=0; i<startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    long totalSize = 0;
    BlockInfo curBlock;
    while(totalSize<size && iter.hasNext()) {
      curBlock = iter.next();
      if(!curBlock.isComplete())  continue;
      totalSize += addBlock(curBlock, results);
    }
    if(totalSize<size) {
      iter = node.getBlockIterator(); // start from the beginning
      for(int i=0; i<startBlock&&totalSize<size; i++) {
        curBlock = iter.next();
        if(!curBlock.isComplete())  continue;
        totalSize += addBlock(curBlock, results);
      }
    }

    return new BlocksWithLocations(
        results.toArray(new BlockWithLocations[results.size()]));
  }

   
  /** Remove the blocks associated to the given datanode. */
  void removeBlocksAssociatedTo(final DatanodeDescriptor node) {
    final Iterator<? extends Block> it = node.getBlockIterator();
    while(it.hasNext()) {
      removeStoredBlock(it.next(), node);
    }

    node.resetBlocks();
    invalidateBlocks.remove(node.getStorageID());
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the operation
   */
  void addToInvalidates(final Block block, final DatanodeInfo datanode) {
    invalidateBlocks.add(block, datanode, true);
  }

  /**
   * Adds block to list of blocks which will be invalidated on all its
   * datanodes.
   */
  private void addToInvalidates(Block b) {
    StringBuilder datanodes = new StringBuilder();
    for (Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(b); it
        .hasNext();) {
      DatanodeDescriptor node = it.next();
      invalidateBlocks.add(b, node, false);
      datanodes.append(node.getName()).append(" ");
    }
    if (datanodes.length() != 0) {
      blockLog.info("BLOCK* addToInvalidates: "
          + b + " to " + datanodes.toString());
    }
  }

  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   * @param reason a textual reason why the block should be marked corrupt,
   * for logging purposes
   */
  public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
      final DatanodeInfo dn, String reason) throws IOException {
    namesystem.writeLock();
    try {
      final BlockInfo storedBlock = getStoredBlock(blk.getLocalBlock());
      if (storedBlock == null) {
        // Check if the replica is in the blockMap, if not
        // ignore the request for now. This could happen when BlockScanner
        // thread of Datanode reports bad block before Block reports are sent
        // by the Datanode on startup
        blockLog.info("BLOCK* findAndMarkBlockAsCorrupt: "
            + blk + " not found.");
        return;
      }
      markBlockAsCorrupt(storedBlock, dn, reason);
    } finally {
      namesystem.writeUnlock();
    }
  }

  private void markBlockAsCorrupt(BlockInfo storedBlock,
                                  DatanodeInfo dn,
                                  String reason) throws IOException {
    assert storedBlock != null : "storedBlock should not be null";
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot mark block " + 
                            storedBlock.getBlockName() +
                            " as corrupt because datanode " + dn.getName() +
                            " does not exist. ");
    }

    INodeFile inode = storedBlock.getINode();
    if (inode == null) {
      blockLog.info("BLOCK markBlockAsCorrupt: " +
                                   "block " + storedBlock +
                                   " could not be marked as corrupt as it" +
                                   " does not belong to any file");
      addToInvalidates(storedBlock, node);
      return;
    } 

    // Add replica to the data-node if it is not already there
    node.addBlock(storedBlock);

    // Add this replica to corruptReplicas Map
    corruptReplicas.addToCorruptReplicasMap(storedBlock, node, reason);
    if (countNodes(storedBlock).liveReplicas() >= inode.getReplication()) {
      // the block is over-replicated so invalidate the replicas immediately
      invalidateBlock(storedBlock, node);
    } else if (namesystem.isPopulatingReplQueues()) {
      // add the block to neededReplication
      updateNeededReplications(storedBlock, -1, 0);
    }
  }

  /**
   * Invalidates the given block on the given datanode.
   */
  private void invalidateBlock(Block blk, DatanodeInfo dn)
      throws IOException {
    blockLog.info("BLOCK* invalidateBlock: "
                                 + blk + " on " + dn.getName());
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot invalidate block " + blk
          + " because datanode " + dn.getName() + " does not exist.");
    }

    // Check how many copies we have of the block. If we have at least one
    // copy on a live node, then we can delete it.
    int count = countNodes(blk).liveReplicas();
    if (count >= 1) {
      addToInvalidates(blk, dn);
      removeStoredBlock(blk, node);
      if(blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* invalidateBlocks: "
            + blk + " on " + dn.getName() + " listed for deletion.");
      }
    } else {
      blockLog.info("BLOCK* invalidateBlocks: " + blk + " on "
          + dn.getName() + " is the only copy and was not deleted.");
    }
  }

  void updateState() {
    pendingReplicationBlocksCount = pendingReplications.size();
    underReplicatedBlocksCount = neededReplications.size();
    corruptReplicaBlocksCount = corruptReplicas.size();
  }

  /** Return number of under-replicated but not missing blocks */
  public int getUnderReplicatedNotMissingBlocks() {
    return neededReplications.getUnderReplicatedBlockCount();
  }
  
  /**
   * Schedule blocks for deletion at datanodes
   * @param nodesToProcess number of datanodes to schedule deletion work
   * @return total number of block for deletion
   */
  int computeInvalidateWork(int nodesToProcess) {
    final List<String> nodes = invalidateBlocks.getStorageIDs();
    Collections.shuffle(nodes);

    nodesToProcess = Math.min(nodes.size(), nodesToProcess);

    int blockCnt = 0;
    for(int nodeCnt = 0; nodeCnt < nodesToProcess; nodeCnt++ ) {
      blockCnt += invalidateWorkForOneNode(nodes.get(nodeCnt));
    }
    return blockCnt;
  }

  /**
   * Scan blocks in {@link #neededReplications} and assign replication
   * work to data-nodes they belong to.
   *
   * The number of process blocks equals either twice the number of live
   * data-nodes or the number of under-replicated blocks whichever is less.
   *
   * @return number of blocks scheduled for replication during this iteration.
   */
  private int computeReplicationWork(int blocksToProcess) throws IOException {
    List<List<Block>> blocksToReplicate = null;
    namesystem.writeLock();
    try {
      // Choose the blocks to be replicated
      blocksToReplicate = neededReplications
          .chooseUnderReplicatedBlocks(blocksToProcess);
    } finally {
      namesystem.writeUnlock();
    }
    return computeReplicationWorkForBlocks(blocksToReplicate);
  }

  /** Replicate a set of blocks
   *
   * @param blocksToReplicate blocks to be replicated, for each priority
   * @return the number of blocks scheduled for replication
   */
  @VisibleForTesting
  int computeReplicationWorkForBlocks(List<List<Block>> blocksToReplicate) {
    int requiredReplication, numEffectiveReplicas;
    List<DatanodeDescriptor> containingNodes, liveReplicaNodes;
    DatanodeDescriptor srcNode;
    INodeFile fileINode = null;
    int additionalReplRequired;

    int scheduledWork = 0;
    List<ReplicationWork> work = new LinkedList<ReplicationWork>();

    namesystem.writeLock();
    try {
      synchronized (neededReplications) {
        for (int priority = 0; priority < blocksToReplicate.size(); priority++) {
          for (Block block : blocksToReplicate.get(priority)) {
            // block should belong to a file
            fileINode = blocksMap.getINode(block);
            // abandoned block or block reopened for append
            if(fileINode == null || fileINode.isUnderConstruction()) {
              neededReplications.remove(block, priority); // remove from neededReplications
              neededReplications.decrementReplicationIndex(priority);
              continue;
            }

            requiredReplication = fileINode.getReplication();

            // get a source data-node
            containingNodes = new ArrayList<DatanodeDescriptor>();
            liveReplicaNodes = new ArrayList<DatanodeDescriptor>();
            NumberReplicas numReplicas = new NumberReplicas();
            srcNode = chooseSourceDatanode(
                block, containingNodes, liveReplicaNodes, numReplicas, priority);
            if(srcNode == null) // block can not be replicated from any node
              continue;

            assert liveReplicaNodes.size() == numReplicas.liveReplicas();
            // do not schedule more if enough replicas is already pending
            numEffectiveReplicas = numReplicas.liveReplicas() +
                                    pendingReplications.getNumReplicas(block);
      
            if (numEffectiveReplicas >= requiredReplication) {
              if ( (pendingReplications.getNumReplicas(block) > 0) ||
                   (blockHasEnoughRacks(block)) ) {
                neededReplications.remove(block, priority); // remove from neededReplications
                neededReplications.decrementReplicationIndex(priority);
                blockLog.info("BLOCK* "
                    + "Removing block " + block
                    + " from neededReplications as it has enough replicas.");
                continue;
              }
            }

            if (numReplicas.liveReplicas() < requiredReplication) {
              additionalReplRequired = requiredReplication
                  - numEffectiveReplicas;
            } else {
              additionalReplRequired = 1; // Needed on a new rack
            }
            work.add(new ReplicationWork(block, fileINode, srcNode,
                containingNodes, liveReplicaNodes, additionalReplRequired,
                priority));
          }
        }
      }
    } finally {
      namesystem.writeUnlock();
    }

    HashMap<Node, Node> excludedNodes
        = new HashMap<Node, Node>();
    for(ReplicationWork rw : work){
      // Exclude all of the containing nodes from being targets.
      // This list includes decommissioning or corrupt nodes.
      excludedNodes.clear();
      for (DatanodeDescriptor dn : rw.containingNodes) {
        excludedNodes.put(dn, dn);
      }

      // choose replication targets: NOT HOLDING THE GLOBAL LOCK
      // It is costly to extract the filename for which chooseTargets is called,
      // so for now we pass in the Inode itself.
      rw.targets = blockplacement.chooseTarget(rw.fileINode,
          rw.additionalReplRequired, rw.srcNode, rw.liveReplicaNodes,
          excludedNodes, rw.block.getNumBytes());
    }

    namesystem.writeLock();
    try {
      for(ReplicationWork rw : work){
        DatanodeDescriptor[] targets = rw.targets;
        if(targets == null || targets.length == 0){
          rw.targets = null;
          continue;
        }

        synchronized (neededReplications) {
          Block block = rw.block;
          int priority = rw.priority;
          // Recheck since global lock was released
          // block should belong to a file
          fileINode = blocksMap.getINode(block);
          // abandoned block or block reopened for append
          if(fileINode == null || fileINode.isUnderConstruction()) {
            neededReplications.remove(block, priority); // remove from neededReplications
            rw.targets = null;
            neededReplications.decrementReplicationIndex(priority);
            continue;
          }
          requiredReplication = fileINode.getReplication();

          // do not schedule more if enough replicas is already pending
          NumberReplicas numReplicas = countNodes(block);
          numEffectiveReplicas = numReplicas.liveReplicas() +
            pendingReplications.getNumReplicas(block);

          if (numEffectiveReplicas >= requiredReplication) {
            if ( (pendingReplications.getNumReplicas(block) > 0) ||
                 (blockHasEnoughRacks(block)) ) {
              neededReplications.remove(block, priority); // remove from neededReplications
              neededReplications.decrementReplicationIndex(priority);
              rw.targets = null;
              NameNode.stateChangeLog.info("BLOCK* "
                  + "Removing block " + block
                  + " from neededReplications as it has enough replicas.");
              continue;
            }
          }

          if ( (numReplicas.liveReplicas() >= requiredReplication) &&
               (!blockHasEnoughRacks(block)) ) {
            if (rw.srcNode.getNetworkLocation().equals(targets[0].getNetworkLocation())) {
              //No use continuing, unless a new rack in this case
              continue;
            }
          }

          // Add block to the to be replicated list
          rw.srcNode.addBlockToBeReplicated(block, targets);
          scheduledWork++;

          for (DatanodeDescriptor dn : targets) {
            dn.incBlocksScheduled();
          }

          // Move the block-replication into a "pending" state.
          // The reason we use 'pending' is so we can retry
          // replications that fail after an appropriate amount of time.
          pendingReplications.increment(block, targets.length);
          if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug(
                "BLOCK* block " + block
                + " is moved from neededReplications to pendingReplications");
          }

          // remove from neededReplications
          if(numEffectiveReplicas + targets.length >= requiredReplication) {
            neededReplications.remove(block, priority); // remove from neededReplications
            neededReplications.decrementReplicationIndex(priority);
          }
        }
      }
    } finally {
      namesystem.writeUnlock();
    }

    if (blockLog.isInfoEnabled()) {
      // log which blocks have been scheduled for replication
      for(ReplicationWork rw : work){
        DatanodeDescriptor[] targets = rw.targets;
        if (targets != null && targets.length != 0) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (int k = 0; k < targets.length; k++) {
            targetList.append(' ');
            targetList.append(targets[k].getName());
          }
          blockLog.info(
                  "BLOCK* ask "
                  + rw.srcNode.getName() + " to replicate "
                  + rw.block + " to " + targetList);
        }
      }
    }
    if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
          "BLOCK* neededReplications = " + neededReplications.size()
          + " pendingReplications = " + pendingReplications.size());
    }

    return scheduledWork;
  }

  /**
   * Choose target datanodes according to the replication policy.
   * 
   * @throws IOException
   *           if the number of targets < minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, DatanodeDescriptor,
   *      List, boolean, HashMap, long)
   */
  public DatanodeDescriptor[] chooseTarget(final String src,
      final int numOfReplicas, final DatanodeDescriptor client,
      final HashMap<Node, Node> excludedNodes,
      final long blocksize) throws IOException {
    // choose targets for the new block to be allocated.
    final DatanodeDescriptor targets[] = blockplacement.chooseTarget(
        src, numOfReplicas, client, excludedNodes, blocksize);
    if (targets.length < minReplication) {
      throw new IOException("File " + src + " could only be replicated to "
          + targets.length + " nodes instead of minReplication (="
          + minReplication + ").  There are "
          + getDatanodeManager().getNetworkTopology().getNumOfLeaves()
          + " datanode(s) running and "
          + (excludedNodes == null? "no": excludedNodes.size())
          + " node(s) are excluded in this operation.");
    }
    return targets;
  }

  /**
   * Parse the data-nodes the block belongs to and choose one,
   * which will be the replication source.
   *
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy.
   * We do not use already decommissioned nodes as a source.
   * Otherwise we choose a random node among those that did not reach their
   * replication limits.  However, if the replication is of the highest priority
   * and all nodes have reached their replication limits, we will choose a
   * random node despite the replication limit.
   *
   * In addition form a list of all nodes containing the block
   * and calculate its replication numbers.
   *
   * @param block Block for which a replication source is needed
   * @param containingNodes List to be populated with nodes found to contain the
   *                        given block
   * @param nodesContainingLiveReplicas List to be populated with nodes found to
   *                                    contain live replicas of the given block
   * @param numReplicas NumberReplicas instance to be initialized with the
   *                                   counts of live, corrupt, excess, and
   *                                   decommissioned replicas of the given
   *                                   block.
   * @param priority integer representing replication priority of the given
   *                 block
   * @return the DatanodeDescriptor of the chosen node from which to replicate
   *         the given block
   */
  @VisibleForTesting
  DatanodeDescriptor chooseSourceDatanode(
                                    Block block,
                                    List<DatanodeDescriptor> containingNodes,
                                    List<DatanodeDescriptor> nodesContainingLiveReplicas,
                                    NumberReplicas numReplicas,
                                    int priority) {
    containingNodes.clear();
    nodesContainingLiveReplicas.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;
    Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    while(it.hasNext()) {
      DatanodeDescriptor node = it.next();
      LightWeightLinkedSet<Block> excessBlocks =
        excessReplicateMap.get(node.getStorageID());
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node)))
        corrupt++;
      else if (node.isDecommissionInProgress() || node.isDecommissioned())
        decommissioned++;
      else if (excessBlocks != null && excessBlocks.contains(block)) {
        excess++;
      } else {
        nodesContainingLiveReplicas.add(node);
        live++;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node))
        continue;
      if(priority != UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY
          && node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams)
      {
        continue; // already reached replication limit
      }
      if (node.getNumberOfBlocksToBeReplicated() >= replicationStreamsHardLimit)
      {
        continue;
      }
      // the block must not be scheduled for removal on srcNode
      if(excessBlocks != null && excessBlocks.contains(block))
        continue;
      // never use already decommissioned nodes
      if(node.isDecommissioned())
        continue;
      // we prefer nodes that are in DECOMMISSION_INPROGRESS state
      if(node.isDecommissionInProgress() || srcNode == null) {
        srcNode = node;
        continue;
      }
      if(srcNode.isDecommissionInProgress())
        continue;
      // switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if(DFSUtil.getRandom().nextBoolean())
        srcNode = node;
    }
    if(numReplicas != null)
      numReplicas.initialize(live, decommissioned, corrupt, excess);
    return srcNode;
  }

  /**
   * If there were any replication requests that timed out, reap them
   * and put them back into the neededReplication queue
   */
  private void processPendingReplications() {
    Block[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      namesystem.writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          NumberReplicas num = countNodes(timedOutItems[i]);
          if (isNeededReplication(timedOutItems[i], getReplication(timedOutItems[i]),
                                 num.liveReplicas())) {
            neededReplications.add(timedOutItems[i],
                                   num.liveReplicas(),
                                   num.decommissionedReplicas(),
                                   getReplication(timedOutItems[i]));
          }
        }
      } finally {
        namesystem.writeUnlock();
      }
      /* If we know the target datanodes where the replication timedout,
       * we could invoke decBlocksScheduled() on it. Its ok for now.
       */
    }
  }
  
  /**
   * StatefulBlockInfo is used to build the "toUC" list, which is a list of
   * updates to the information about under-construction blocks.
   * Besides the block in question, it provides the ReplicaState
   * reported by the datanode in the block report. 
   */
  private static class StatefulBlockInfo {
    final BlockInfoUnderConstruction storedBlock;
    final ReplicaState reportedState;
    
    StatefulBlockInfo(BlockInfoUnderConstruction storedBlock, 
        ReplicaState reportedState) {
      this.storedBlock = storedBlock;
      this.reportedState = reportedState;
    }
  }
  
  /**
   * BlockToMarkCorrupt is used to build the "toCorrupt" list, which is a
   * list of blocks that should be considered corrupt due to a block report.
   */
  private static class BlockToMarkCorrupt {
    final BlockInfo blockInfo;
    final String reason;
    
    BlockToMarkCorrupt(BlockInfo blockInfo, String reason) {
      super();
      this.blockInfo = blockInfo;
      this.reason = reason;
    }
  }

  /**
   * The given datanode is reporting all its blocks.
   * Update the (machine-->blocklist) and (block-->machinelist) maps.
   */
  public void processReport(final DatanodeID nodeID, final String poolId,
      final BlockListAsLongs newReport) throws IOException {
    namesystem.writeLock();
    final long startTime = Util.now(); //after acquiring write lock
    final long endTime;
    try {
      final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
      if (node == null || !node.isAlive) {
        throw new IOException("ProcessReport from dead or unregistered node: "
                              + nodeID.getName());
      }

      // To minimize startup time, we discard any second (or later) block reports
      // that we receive while still in startup phase.
      if (namesystem.isInStartupSafeMode() && !node.isFirstBlockReport()) {
        blockLog.info("BLOCK* processReport: "
            + "discarded non-initial block report from " + nodeID.getName()
            + " because namenode still in startup phase");
        return;
      }

      if (node.numBlocks() == 0) {
        // The first block report can be processed a lot more efficiently than
        // ordinary block reports.  This shortens restart times.
        processFirstBlockReport(node, newReport);
      } else {
        processReport(node, newReport);
      }
      node.receivedBlockReport();
    } finally {
      endTime = Util.now();
      namesystem.writeUnlock();
    }

    // Log the block report processing stats from Namenode perspective
    final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
    if (metrics != null) {
      metrics.addBlockReport((int) (endTime - startTime));
    }
    blockLog.info("BLOCK* processReport: from "
        + nodeID.getName() + ", blocks: " + newReport.getNumberOfBlocks()
        + ", processing time: " + (endTime - startTime) + " msecs");
  }

  private void processReport(final DatanodeDescriptor node,
      final BlockListAsLongs report) throws IOException {
    // Normal case:
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
    Collection<Block> toRemove = new LinkedList<Block>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
    reportDiff(node, report, toAdd, toRemove, toInvalidate, toCorrupt, toUC);

    // Process the blocks on each queue
    for (StatefulBlockInfo b : toUC) { 
      addStoredBlockUnderConstruction(b.storedBlock, node, b.reportedState);
    }
    for (Block b : toRemove) {
      removeStoredBlock(b, node);
    }
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, node, null, true);
    }
    for (Block b : toInvalidate) {
      blockLog.info("BLOCK* processReport: block "
          + b + " on " + node.getName() + " size " + b.getNumBytes()
          + " does not belong to any file.");
      addToInvalidates(b, node);
    }
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b.blockInfo, node, b.reason);
    }
  }

  /**
   * processFirstBlockReport is intended only for processing "initial" block
   * reports, the first block report received from a DN after it registers.
   * It just adds all the valid replicas to the datanode, without calculating 
   * a toRemove list (since there won't be any).  It also silently discards 
   * any invalid blocks, thereby deferring their processing until 
   * the next block report.
   * @param node - DatanodeDescriptor of the node that sent the report
   * @param report - the initial block report, to be processed
   * @throws IOException 
   */
  private void processFirstBlockReport(final DatanodeDescriptor node,
      final BlockListAsLongs report) throws IOException {
    if (report == null) return;
    assert (namesystem.hasWriteLock());
    assert (node.numBlocks() == 0);
    BlockReportIterator itBR = report.getBlockReportIterator();

    while(itBR.hasNext()) {
      Block iblk = itBR.next();
      ReplicaState reportedState = itBR.getCurrentReplicaState();
      BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
      // If block does not belong to any file, we are done.
      if (storedBlock == null) continue;
      
      // If block is corrupt, mark it and continue to next block.
      BlockUCState ucState = storedBlock.getBlockUCState();
      BlockToMarkCorrupt c = checkReplicaCorrupt(
          iblk, reportedState, storedBlock, ucState, node);
      if (c != null) {
        markBlockAsCorrupt(c.blockInfo, node, c.reason);
        continue;
      }
      
      // If block is under construction, add this replica to its list
      if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
        ((BlockInfoUnderConstruction)storedBlock).addReplicaIfNotPresent(
            node, iblk, reportedState);
        //and fall through to next clause
      }      
      //add replica if appropriate
      if (reportedState == ReplicaState.FINALIZED) {
        addStoredBlockImmediate(storedBlock, node);
      }
    }
  }

  private void reportDiff(DatanodeDescriptor dn, 
      BlockListAsLongs newReport, 
      Collection<BlockInfo> toAdd,              // add to DatanodeDescriptor
      Collection<Block> toRemove,           // remove from DatanodeDescriptor
      Collection<Block> toInvalidate,       // should be removed from DN
      Collection<BlockToMarkCorrupt> toCorrupt, // add to corrupt replicas list
      Collection<StatefulBlockInfo> toUC) { // add to under-construction list
    // place a delimiter in the list which separates blocks 
    // that have been reported from those that have not
    BlockInfo delimiter = new BlockInfo(new Block(), 1);
    boolean added = dn.addBlock(delimiter);
    assert added : "Delimiting block cannot be present in the node";
    int headIndex = 0; //currently the delimiter is in the head of the list
    int curIndex;

    if (newReport == null)
      newReport = new BlockListAsLongs();
    // scan the report and process newly reported blocks
    BlockReportIterator itBR = newReport.getBlockReportIterator();
    while(itBR.hasNext()) {
      Block iblk = itBR.next();
      ReplicaState iState = itBR.getCurrentReplicaState();
      BlockInfo storedBlock = processReportedBlock(dn, iblk, iState,
                                  toAdd, toInvalidate, toCorrupt, toUC);
      // move block to the head of the list
      if (storedBlock != null && (curIndex = storedBlock.findDatanode(dn)) >= 0) {
        headIndex = dn.moveBlockToHead(storedBlock, curIndex, headIndex);
      }
    }
    // collect blocks that have not been reported
    // all of them are next to the delimiter
    Iterator<? extends Block> it = new DatanodeDescriptor.BlockIterator(
        delimiter.getNext(0), dn);
    while(it.hasNext())
      toRemove.add(it.next());
    dn.removeBlock(delimiter);
  }

  /**
   * Process a block replica reported by the data-node.
   * No side effects except adding to the passed-in Collections.
   * 
   * <ol>
   * <li>If the block is not known to the system (not in blocksMap) then the
   * data-node should be notified to invalidate this block.</li>
   * <li>If the reported replica is valid that is has the same generation stamp
   * and length as recorded on the name-node, then the replica location should
   * be added to the name-node.</li>
   * <li>If the reported replica is not valid, then it is marked as corrupt,
   * which triggers replication of the existing valid replicas.
   * Corrupt replicas are removed from the system when the block
   * is fully replicated.</li>
   * <li>If the reported replica is for a block currently marked "under
   * construction" in the NN, then it should be added to the 
   * BlockInfoUnderConstruction's list of replicas.</li>
   * </ol>
   * 
   * @param dn descriptor for the datanode that made the report
   * @param block reported block replica
   * @param reportedState reported replica state
   * @param toAdd add to DatanodeDescriptor
   * @param toInvalidate missing blocks (not in the blocks map)
   *        should be removed from the data-node
   * @param toCorrupt replicas with unexpected length or generation stamp;
   *        add to corrupt replicas
   * @param toUC replicas of blocks currently under construction
   * @return
   */
  private BlockInfo processReportedBlock(final DatanodeDescriptor dn, 
      final Block block, final ReplicaState reportedState, 
      final Collection<BlockInfo> toAdd, 
      final Collection<Block> toInvalidate, 
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC) {
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Reported block " + block
          + " on " + dn.getName() + " size " + block.getNumBytes()
          + " replicaState = " + reportedState);
    }
  
    // find block by blockId
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if(storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // the replica should be removed from the data-node.
      toInvalidate.add(new Block(block));
      return null;
    }
    BlockUCState ucState = storedBlock.getBlockUCState();
    
    // Block is on the NN
    if(LOG.isDebugEnabled()) {
      LOG.debug("In memory blockUCState = " + ucState);
    }

    // Ignore replicas already scheduled to be removed from the DN
    if(invalidateBlocks.contains(dn.getStorageID(), block)) {
      return storedBlock;
    }

    BlockToMarkCorrupt c = checkReplicaCorrupt(
        block, reportedState, storedBlock, ucState, dn);
    if (c != null) {
      toCorrupt.add(c);
      return storedBlock;
    }

    if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
      toUC.add(new StatefulBlockInfo(
          (BlockInfoUnderConstruction)storedBlock, reportedState));
      return storedBlock;
    }

    //add replica if appropriate
    if (reportedState == ReplicaState.FINALIZED
        && storedBlock.findDatanode(dn) < 0) {
      toAdd.add(storedBlock);
    }
    return storedBlock;
  }

  /**
   * The next two methods test the various cases under which we must conclude
   * the replica is corrupt, or under construction.  These are laid out
   * as switch statements, on the theory that it is easier to understand
   * the combinatorics of reportedState and ucState that way.  It should be
   * at least as efficient as boolean expressions.
   * 
   * @return a BlockToMarkCorrupt object, or null if the replica is not corrupt
   */
  private BlockToMarkCorrupt checkReplicaCorrupt(
      Block iblk, ReplicaState reportedState, 
      BlockInfo storedBlock, BlockUCState ucState, 
      DatanodeDescriptor dn) {
    switch(reportedState) {
    case FINALIZED:
      switch(ucState) {
      case COMPLETE:
      case COMMITTED:
        if (storedBlock.getGenerationStamp() != iblk.getGenerationStamp()) {
          return new BlockToMarkCorrupt(storedBlock,
              "block is " + ucState + " and reported genstamp " +
              iblk.getGenerationStamp() + " does not match " +
              "genstamp in block map " + storedBlock.getGenerationStamp());
        } else if (storedBlock.getNumBytes() != iblk.getNumBytes()) {
          return new BlockToMarkCorrupt(storedBlock,
              "block is " + ucState + " and reported length " +
              iblk.getNumBytes() + " does not match " +
              "length in block map " + storedBlock.getNumBytes());
        } else {
          return null; // not corrupt
        }
      default:
        return null;
      }
    case RBW:
    case RWR:
      if (!storedBlock.isComplete()) {
        return null; // not corrupt
      } else if (storedBlock.getGenerationStamp() != iblk.getGenerationStamp()) {
        return new BlockToMarkCorrupt(storedBlock,
            "reported " + reportedState + " replica with genstamp " +
            iblk.getGenerationStamp() + " does not match COMPLETE block's " +
            "genstamp in block map " + storedBlock.getGenerationStamp());
      } else { // COMPLETE block, same genstamp
        if (reportedState == ReplicaState.RBW) {
          // If it's a RBW report for a COMPLETE block, it may just be that
          // the block report got a little bit delayed after the pipeline
          // closed. So, ignore this report, assuming we will get a
          // FINALIZED replica later. See HDFS-2791
          LOG.info("Received an RBW replica for block " + storedBlock +
              " on " + dn.getName() + ": ignoring it, since the block is " +
              "complete with the same generation stamp.");
          return null;
        } else {
          return new BlockToMarkCorrupt(storedBlock,
              "reported replica has invalid state " + reportedState);
        }
      }
    case RUR:       // should not be reported
    case TEMPORARY: // should not be reported
    default:
      String msg = "Unexpected replica state " + reportedState
      + " for block: " + storedBlock + 
      " on " + dn.getName() + " size " + storedBlock.getNumBytes();
      // log here at WARN level since this is really a broken HDFS
      // invariant
      LOG.warn(msg);
      return new BlockToMarkCorrupt(storedBlock, msg);
    }
  }

  private boolean isBlockUnderConstruction(BlockInfo storedBlock, 
      BlockUCState ucState, ReplicaState reportedState) {
    switch(reportedState) {
    case FINALIZED:
      switch(ucState) {
      case UNDER_CONSTRUCTION:
      case UNDER_RECOVERY:
        return true;
      default:
        return false;
      }
    case RBW:
    case RWR:
      return (!storedBlock.isComplete());
    case RUR:       // should not be reported                                                                                             
    case TEMPORARY: // should not be reported                                                                                             
    default:
      return false;
    }
  }
  
  void addStoredBlockUnderConstruction(
      BlockInfoUnderConstruction block, 
      DatanodeDescriptor node, 
      ReplicaState reportedState) 
  throws IOException {
    block.addReplicaIfNotPresent(node, block, reportedState);
    if (reportedState == ReplicaState.FINALIZED && block.findDatanode(node) < 0) {
      addStoredBlock(block, node, null, true);
    }
  }
  
  /**
   * Faster version of
   * {@link #addStoredBlock(BlockInfo, DatanodeDescriptor, DatanodeDescriptor, boolean)}
   * , intended for use with initial block report at startup. If not in startup
   * safe mode, will call standard addStoredBlock(). Assumes this method is
   * called "immediately" so there is no need to refresh the storedBlock from
   * blocksMap. Doesn't handle underReplication/overReplication, or worry about
   * pendingReplications or corruptReplicas, because it's in startup safe mode.
   * Doesn't log every block, because there are typically millions of them.
   * 
   * @throws IOException
   */
  private void addStoredBlockImmediate(BlockInfo storedBlock,
                               DatanodeDescriptor node)
  throws IOException {
    assert (storedBlock != null && namesystem.hasWriteLock());
    if (!namesystem.isInStartupSafeMode() 
        || namesystem.isPopulatingReplQueues()) {
      addStoredBlock(storedBlock, node, null, false);
      return;
    }

    // just add it
    node.addBlock(storedBlock);

    // Now check for completion of blocks and safe block count
    int numCurrentReplica = countLiveNodes(storedBlock);
    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED
        && numCurrentReplica >= minReplication)
      storedBlock = completeBlock(storedBlock.getINode(), storedBlock);

    // check whether safe replication is reached for the block
    // only complete blocks are counted towards that
    if(storedBlock.isComplete())
      namesystem.incrementSafeBlockCount(numCurrentReplica);
  }

  /**
   * Modify (block-->datanode) map. Remove block from set of
   * needed replications if this takes care of the problem.
   * @return the block that is stored in blockMap.
   */
  private Block addStoredBlock(final BlockInfo block,
                               DatanodeDescriptor node,
                               DatanodeDescriptor delNodeHint,
                               boolean logEveryBlock)
  throws IOException {
    assert block != null && namesystem.hasWriteLock();
    BlockInfo storedBlock;
    if (block instanceof BlockInfoUnderConstruction) {
      //refresh our copy in case the block got completed in another thread
      storedBlock = blocksMap.getStoredBlock(block);
    } else {
      storedBlock = block;
    }
    if (storedBlock == null || storedBlock.getINode() == null) {
      // If this block does not belong to anyfile, then we are done.
      blockLog.info("BLOCK* addStoredBlock: " + block + " on "
          + node.getName() + " size " + block.getNumBytes()
          + " but it does not belong to any file.");
      // we could add this block to invalidate set of this datanode.
      // it will happen in next block report otherwise.
      return block;
    }
    assert storedBlock != null : "Block must be stored by now";
    INodeFile fileINode = storedBlock.getINode();
    assert fileINode != null : "Block must belong to a file";

    // add block to the datanode
    boolean added = node.addBlock(storedBlock);

    int curReplicaDelta;
    if (added) {
      curReplicaDelta = 1;
      if (logEveryBlock) {
        blockLog.info("BLOCK* addStoredBlock: "
            + "blockMap updated: " + node.getName() + " is added to " + 
            storedBlock + " size " + storedBlock.getNumBytes());
      }
    } else {
      curReplicaDelta = 0;
      blockLog.warn("BLOCK* addStoredBlock: "
          + "Redundant addStoredBlock request received for " + storedBlock
          + " on " + node.getName() + " size " + storedBlock.getNumBytes());
    }

    // Now check for completion of blocks and safe block count
    NumberReplicas num = countNodes(storedBlock);
    int numLiveReplicas = num.liveReplicas();
    int numCurrentReplica = numLiveReplicas
      + pendingReplications.getNumReplicas(storedBlock);

    if(storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        numLiveReplicas >= minReplication)
      storedBlock = completeBlock(fileINode, storedBlock);

    // check whether safe replication is reached for the block
    // only complete blocks are counted towards that
    // Is no-op if not in safe mode.
    if(storedBlock.isComplete())
      namesystem.incrementSafeBlockCount(numCurrentReplica);

    // if file is under construction, then done for now
    if (fileINode.isUnderConstruction()) {
      return storedBlock;
    }

    // do not try to handle over/under-replicated blocks during safe mode
    if (!namesystem.isPopulatingReplQueues()) {
      return storedBlock;
    }

    // handle underReplication/overReplication
    short fileReplication = fileINode.getReplication();
    if (!isNeededReplication(storedBlock, fileReplication, numCurrentReplica)) {
      neededReplications.remove(storedBlock, numCurrentReplica,
          num.decommissionedReplicas(), fileReplication);
    } else {
      updateNeededReplications(storedBlock, curReplicaDelta, 0);
    }
    if (numCurrentReplica > fileReplication) {
      processOverReplicatedBlock(storedBlock, fileReplication, node, delNodeHint);
    }
    // If the file replication has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(storedBlock);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " +
          storedBlock + "blockMap has " + numCorruptNodes + 
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication))
      invalidateCorruptReplicas(storedBlock);
    return storedBlock;
  }

  /**
   * Invalidate corrupt replicas.
   * <p>
   * This will remove the replicas from the block's location list,
   * add them to {@link #invalidateBlocks} so that they could be further
   * deleted from the respective data-nodes,
   * and remove the block from corruptReplicasMap.
   * <p>
   * This method should be called when the block has sufficient
   * number of live replicas.
   *
   * @param blk Block whose corrupt replicas need to be invalidated
   */
  private void invalidateCorruptReplicas(Block blk) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean gotException = false;
    if (nodes == null)
      return;
    // make a copy of the array of nodes in order to avoid
    // ConcurrentModificationException, when the block is removed from the node
    DatanodeDescriptor[] nodesCopy = nodes.toArray(new DatanodeDescriptor[0]);
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        invalidateBlock(blk, node);
      } catch (IOException e) {
        blockLog.info("NameNode.invalidateCorruptReplicas " +
                                      "error in deleting bad block " + blk +
                                      " on " + node + e);
        gotException = true;
      }
    }
    // Remove the block from corruptReplicasMap
    if (!gotException)
      corruptReplicas.removeFromCorruptReplicasMap(blk);
  }

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * over or under replicated. Place it into the respective queue.
   */
  public void processMisReplicatedBlocks() {
    assert namesystem.hasWriteLock();

    long nrInvalid = 0, nrOverReplicated = 0, nrUnderReplicated = 0,
         nrUnderConstruction = 0;
    neededReplications.clear();
    for (BlockInfo block : blocksMap.getBlocks()) {
      INodeFile fileINode = block.getINode();
      if (fileINode == null) {
        // block does not belong to any file
        nrInvalid++;
        addToInvalidates(block);
        continue;
      }
      if (!block.isComplete()) {
        // Incomplete blocks are never considered mis-replicated --
        // they'll be reached when they are completed or recovered.
        nrUnderConstruction++;
        continue;
      }
      // calculate current replication
      short expectedReplication = fileINode.getReplication();
      NumberReplicas num = countNodes(block);
      int numCurrentReplica = num.liveReplicas();
      // add to under-replicated queue if need to be
      if (isNeededReplication(block, expectedReplication, numCurrentReplica)) {
        if (neededReplications.add(block, numCurrentReplica, num
            .decommissionedReplicas(), expectedReplication)) {
          nrUnderReplicated++;
        }
      }

      if (numCurrentReplica > expectedReplication) {
        // over-replicated block
        nrOverReplicated++;
        processOverReplicatedBlock(block, expectedReplication, null, null);
      }
    }

    LOG.info("Total number of blocks            = " + blocksMap.size());
    LOG.info("Number of invalid blocks          = " + nrInvalid);
    LOG.info("Number of under-replicated blocks = " + nrUnderReplicated);
    LOG.info("Number of  over-replicated blocks = " + nrOverReplicated);
    LOG.info("Number of blocks being written    = " + nrUnderConstruction);
  }

  /** Set replication for the blocks. */
  public void setReplication(final short oldRepl, final short newRepl,
      final String src, final Block... blocks) throws IOException {
    if (newRepl == oldRepl) {
      return;
    }

    // update needReplication priority queues
    for(Block b : blocks) {
      updateNeededReplications(b, 0, newRepl-oldRepl);
    }
      
    if (oldRepl > newRepl) {
      // old replication > the new one; need to remove copies
      LOG.info("Decreasing replication from " + oldRepl + " to " + newRepl
          + " for " + src);
      for(Block b : blocks) {
        processOverReplicatedBlock(b, newRepl, null, null);
      }
    } else { // replication factor is increased
      LOG.info("Increasing replication from " + oldRepl + " to " + newRepl
          + " for " + src);
    }
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   */
  private void processOverReplicatedBlock(final Block block,
      final short replication, final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    assert namesystem.hasWriteLock();
    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeDescriptor> nonExcess = new ArrayList<DatanodeDescriptor>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas
        .getNodes(block);
    for (Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
         it.hasNext();) {
      DatanodeDescriptor cur = it.next();
      LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(cur
          .getStorageID());
      if (excessBlocks == null || !excessBlocks.contains(block)) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
          // exclude corrupt replicas
          if (corruptNodes == null || !corruptNodes.contains(cur)) {
            nonExcess.add(cur);
          }
        }
      }
    }
    chooseExcessReplicates(nonExcess, block, replication, 
        addedNode, delNodeHint, blockplacement);
  }


  /**
   * We want "replication" replicates for the block, but we now have too many.  
   * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
   *
   * srcNodes.size() - dstNodes.size() == replication
   *
   * We pick node that make sure that replicas are spread across racks and
   * also try hard to pick one with least free space.
   * The algorithm is first to pick a node with least free space from nodes
   * that are on a rack holding more than one replicas of the block.
   * So removing such a replica won't remove a rack. 
   * If no such a node is available,
   * then pick a node with least free space
   */
  private void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess, 
                              Block b, short replication,
                              DatanodeDescriptor addedNode,
                              DatanodeDescriptor delNodeHint,
                              BlockPlacementPolicy replicator) {
    assert namesystem.hasWriteLock();
    // first form a rack to datanodes map and
    INodeFile inode = getINode(b);
    final Map<String, List<DatanodeDescriptor>> rackMap
        = new HashMap<String, List<DatanodeDescriptor>>();
    for(final Iterator<DatanodeDescriptor> iter = nonExcess.iterator();
        iter.hasNext(); ) {
      final DatanodeDescriptor node = iter.next();
      final String rackName = node.getNetworkLocation();
      List<DatanodeDescriptor> datanodeList = rackMap.get(rackName);
      if (datanodeList == null) {
        datanodeList = new ArrayList<DatanodeDescriptor>();
        rackMap.put(rackName, datanodeList);
      }
      datanodeList.add(node);
    }
    
    // split nodes into two sets
    // priSet contains nodes on rack with more than one replica
    // remains contains the remaining nodes
    final List<DatanodeDescriptor> priSet = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> remains = new ArrayList<DatanodeDescriptor>();
    for(List<DatanodeDescriptor> datanodeList : rackMap.values()) {
      if (datanodeList.size() == 1 ) {
        remains.add(datanodeList.get(0));
      } else {
        priSet.addAll(datanodeList);
      }
    }
    
    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    while (nonExcess.size() - replication > 0) {
      // check if we can delete delNodeHint
      final DatanodeInfo cur;
      if (firstOne && delNodeHint !=null && nonExcess.contains(delNodeHint)
          && (priSet.contains(delNodeHint)
              || (addedNode != null && !priSet.contains(addedNode))) ) {
        cur = delNodeHint;
      } else { // regular excessive replica removal
        cur = replicator.chooseReplicaToDelete(inode, b, replication,
            priSet, remains);
      }
      firstOne = false;

      // adjust rackmap, priSet, and remains
      String rack = cur.getNetworkLocation();
      final List<DatanodeDescriptor> datanodes = rackMap.get(rack);
      datanodes.remove(cur);
      if (datanodes.isEmpty()) {
        rackMap.remove(rack);
      }
      if (priSet.remove(cur)) {
        if (datanodes.size() == 1) {
          priSet.remove(datanodes.get(0));
          remains.add(datanodes.get(0));
        }
      } else {
        remains.remove(cur);
      }

      nonExcess.remove(cur);
      addToExcessReplicate(cur, b);

      //
      // The 'excessblocks' tracks blocks until we get confirmation
      // that the datanode has deleted them; the only way we remove them
      // is when we get a "removeBlock" message.  
      //
      // The 'invalidate' list is used to inform the datanode the block 
      // should be deleted.  Items are removed from the invalidate list
      // upon giving instructions to the namenode.
      //
      addToInvalidates(b, cur);
      blockLog.info("BLOCK* chooseExcessReplicates: "
                +"("+cur.getName()+", "+b+") is added to invalidated blocks set.");
    }
  }

  private void addToExcessReplicate(DatanodeInfo dn, Block block) {
    assert namesystem.hasWriteLock();
    LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(dn.getStorageID());
    if (excessBlocks == null) {
      excessBlocks = new LightWeightLinkedSet<Block>();
      excessReplicateMap.put(dn.getStorageID(), excessBlocks);
    }
    if (excessBlocks.add(block)) {
      excessBlocksCount++;
      if(blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* addToExcessReplicate:"
            + " (" + dn.getName() + ", " + block
            + ") is added to excessReplicateMap");
      }
    }
  }

  /**
   * Modify (block-->datanode) map. Possibly generate replication tasks, if the
   * removed block is still valid.
   */
  public void removeStoredBlock(Block block, DatanodeDescriptor node) {
    if(blockLog.isDebugEnabled()) {
      blockLog.debug("BLOCK* removeStoredBlock: "
          + block + " from " + node.getName());
    }
    assert (namesystem.hasWriteLock());
    {
      if (!blocksMap.removeNode(block, node)) {
        if(blockLog.isDebugEnabled()) {
          blockLog.debug("BLOCK* removeStoredBlock: "
              + block + " has already been removed from node " + node);
        }
        return;
      }

      //
      // It's possible that the block was removed because of a datanode
      // failure. If the block is still valid, check if replication is
      // necessary. In that case, put block on a possibly-will-
      // be-replicated list.
      //
      INode fileINode = blocksMap.getINode(block);
      if (fileINode != null) {
        namesystem.decrementSafeBlockCount(block);
        updateNeededReplications(block, -1, 0);
      }

      //
      // We've removed a block from a node, so it's definitely no longer
      // in "excess" there.
      //
      LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(node
          .getStorageID());
      if (excessBlocks != null) {
        if (excessBlocks.remove(block)) {
          excessBlocksCount--;
          if(blockLog.isDebugEnabled()) {
            blockLog.debug("BLOCK* removeStoredBlock: "
                + block + " is removed from excessBlocks");
          }
          if (excessBlocks.size() == 0) {
            excessReplicateMap.remove(node.getStorageID());
          }
        }
      }

      // Remove the replica from corruptReplicas
      corruptReplicas.removeFromCorruptReplicasMap(block, node);
    }
  }

  /**
   * Get all valid locations of the block & add the block to results
   * return the length of the added block; 0 if the block is not added
   */
  private long addBlock(Block block, List<BlockWithLocations> results) {
    final List<String> machineSet = getValidLocations(block);
    if(machineSet.size() == 0) {
      return 0;
    } else {
      results.add(new BlockWithLocations(block, 
          machineSet.toArray(new String[machineSet.size()])));
      return block.getNumBytes();
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  @VisibleForTesting
  void addBlock(DatanodeDescriptor node, Block block, String delHint)
      throws IOException {
    // decrement number of blocks scheduled to this datanode.
    node.decBlocksScheduled();

    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if (delHint != null && delHint.length() != 0) {
      delHintNode = datanodeManager.getDatanode(delHint);
      if (delHintNode == null) {
        blockLog.warn("BLOCK* blockReceived: " + block
            + " is expected to be removed from an unrecorded node " + delHint);
      }
    }

    //
    // Modify the blocks->datanode map and node's map.
    //
    pendingReplications.decrement(block);

    // blockReceived reports a finalized block
    Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
    processReportedBlock(node, block, ReplicaState.FINALIZED,
                              toAdd, toInvalidate, toCorrupt, toUC);
    // the block is only in one of the to-do lists
    // if it is in none then data-node already has it
    assert toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt.size() <= 1
      : "The block should be only in one of the lists.";

    for (StatefulBlockInfo b : toUC) { 
      addStoredBlockUnderConstruction(b.storedBlock, node, b.reportedState);
    }
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, node, delHintNode, true);
    }
    for (Block b : toInvalidate) {
      blockLog.info("BLOCK* addBlock: block "
          + b + " on " + node.getName() + " size " + b.getNumBytes()
          + " does not belong to any file.");
      addToInvalidates(b, node);
    }
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b.blockInfo, node, b.reason);
    }
  }

  /** The given node is reporting that it received/deleted certain blocks. */
  public void blockReceivedAndDeleted(final DatanodeID nodeID, 
     final String poolId, 
     final ReceivedDeletedBlockInfo receivedAndDeletedBlocks[]
  ) throws IOException {
    namesystem.writeLock();
    int received = 0;
    int deleted = 0;
    try {
      final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
      if (node == null || !node.isAlive) {
        blockLog.warn("BLOCK* blockReceivedDeleted"
                + " is received from dead or unregistered node "
                + nodeID.getName());
        throw new IOException(
            "Got blockReceivedDeleted message from unregistered or dead node");
      }

      for (int i = 0; i < receivedAndDeletedBlocks.length; i++) {
        if (receivedAndDeletedBlocks[i].isDeletedBlock()) {
          removeStoredBlock(
              receivedAndDeletedBlocks[i].getBlock(), node);
          deleted++;
        } else {
          addBlock(node, receivedAndDeletedBlocks[i].getBlock(),
              receivedAndDeletedBlocks[i].getDelHints());
          received++;
        }
        if (blockLog.isDebugEnabled()) {
          blockLog.debug("BLOCK* block"
              + (receivedAndDeletedBlocks[i].isDeletedBlock() ? "Deleted"
                  : "Received") + ": " + receivedAndDeletedBlocks[i].getBlock()
              + " is received from " + nodeID.getName());
        }
      }
    } finally {
      namesystem.writeUnlock();
      if (blockLog.isDebugEnabled()) {
          blockLog.debug("*BLOCK* NameNode.blockReceivedAndDeleted: " + "from "
              + nodeID.getName() + " received: " + received + ", "
              + " deleted: " + deleted);
      }
    }
  }

  /**
   * Return the number of nodes that are live and decommissioned.
   */
  public NumberReplicas countNodes(Block b) {
    int count = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;
    Iterator<DatanodeDescriptor> nodeIter = blocksMap.nodeIterator(b);
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    while (nodeIter.hasNext()) {
      DatanodeDescriptor node = nodeIter.next();
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        count++;
      } else {
        LightWeightLinkedSet<Block> blocksExcess = excessReplicateMap.get(node
            .getStorageID());
        if (blocksExcess != null && blocksExcess.contains(b)) {
          excess++;
        } else {
          live++;
        }
      }
    }
    return new NumberReplicas(live, count, corrupt, excess);
  }

  /** 
   * Simpler, faster form of {@link #countNodes(Block)} that only returns the number
   * of live nodes.  If in startup safemode (or its 30-sec extension period),
   * then it gains speed by ignoring issues of excess replicas or nodes
   * that are decommissioned or in process of becoming decommissioned.
   * If not in startup, then it calls {@link countNodes()} instead.
   * 
   * @param b - the block being tested
   * @return count of live nodes for this block
   */
  int countLiveNodes(BlockInfo b) {
    if (!namesystem.isInStartupSafeMode()) {
      return countNodes(b).liveReplicas();
    }
    // else proceed with fast case
    int live = 0;
    Iterator<DatanodeDescriptor> nodeIter = blocksMap.nodeIterator(b);
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    while (nodeIter.hasNext()) {
      DatanodeDescriptor node = nodeIter.next();
      if ((nodesCorrupt == null) || (!nodesCorrupt.contains(node)))
        live++;
    }
    return live;
  }

  private void logBlockReplicationInfo(Block block, DatanodeDescriptor srcNode,
      NumberReplicas num) {
    int curReplicas = num.liveReplicas();
    int curExpectedReplicas = getReplication(block);
    INode fileINode = blocksMap.getINode(block);
    Iterator<DatanodeDescriptor> nodeIter = blocksMap.nodeIterator(block);
    StringBuilder nodeList = new StringBuilder();
    while (nodeIter.hasNext()) {
      DatanodeDescriptor node = nodeIter.next();
      nodeList.append(node.name);
      nodeList.append(" ");
    }
    LOG.info("Block: " + block + ", Expected Replicas: "
        + curExpectedReplicas + ", live replicas: " + curReplicas
        + ", corrupt replicas: " + num.corruptReplicas()
        + ", decommissioned replicas: " + num.decommissionedReplicas()
        + ", excess replicas: " + num.excessReplicas()
        + ", Is Open File: " + fileINode.isUnderConstruction()
        + ", Datanodes having this block: " + nodeList + ", Current Datanode: "
        + srcNode.name + ", Is current datanode decommissioning: "
        + srcNode.isDecommissionInProgress());
  }
  
  /**
   * On stopping decommission, check if the node has excess replicas.
   * If there are any excess replicas, call processOverReplicatedBlock()
   */
  void processOverReplicatedBlocksOnReCommission(
      final DatanodeDescriptor srcNode) {
    final Iterator<? extends Block> it = srcNode.getBlockIterator();
    int numOverReplicated = 0;
    while(it.hasNext()) {
      final Block block = it.next();
      INodeFile fileINode = blocksMap.getINode(block);
      short expectedReplication = fileINode.getReplication();
      NumberReplicas num = countNodes(block);
      int numCurrentReplica = num.liveReplicas();
      if (numCurrentReplica > expectedReplication) {
        // over-replicated block 
        processOverReplicatedBlock(block, expectedReplication, null, null);
        numOverReplicated++;
      }
    }
    LOG.info("Invalidated " + numOverReplicated + " over-replicated blocks on " +
        srcNode + " during recommissioning");
  }

  /**
   * Return true if there are any blocks on this node that have not
   * yet reached their replication factor. Otherwise returns false.
   */
  boolean isReplicationInProgress(DatanodeDescriptor srcNode) {
    boolean status = false;
    int underReplicatedBlocks = 0;
    int decommissionOnlyReplicas = 0;
    int underReplicatedInOpenFiles = 0;
    final Iterator<? extends Block> it = srcNode.getBlockIterator();
    while(it.hasNext()) {
      final Block block = it.next();
      INode fileINode = blocksMap.getINode(block);

      if (fileINode != null) {
        NumberReplicas num = countNodes(block);
        int curReplicas = num.liveReplicas();
        int curExpectedReplicas = getReplication(block);
        if (isNeededReplication(block, curExpectedReplicas, curReplicas)) {
          if (curExpectedReplicas > curReplicas) {
            //Log info about one block for this node which needs replication
            if (!status) {
              status = true;
              logBlockReplicationInfo(block, srcNode, num);
            }
            underReplicatedBlocks++;
            if ((curReplicas == 0) && (num.decommissionedReplicas() > 0)) {
              decommissionOnlyReplicas++;
            }
            if (fileINode.isUnderConstruction()) {
              underReplicatedInOpenFiles++;
            }
          }
          if (!neededReplications.contains(block) &&
            pendingReplications.getNumReplicas(block) == 0) {
            //
            // These blocks have been reported from the datanode
            // after the startDecommission method has been executed. These
            // blocks were in flight when the decommissioning was started.
            //
            neededReplications.add(block,
                                   curReplicas,
                                   num.decommissionedReplicas(),
                                   curExpectedReplicas);
          }
        }
      }
    }
    srcNode.decommissioningStatus.set(underReplicatedBlocks,
        decommissionOnlyReplicas, 
        underReplicatedInOpenFiles);
    return status;
  }

  public int getActiveBlockCount() {
    return blocksMap.size() - (int)invalidateBlocks.numBlocks();
  }

  public DatanodeDescriptor[] getNodes(BlockInfo block) {
    DatanodeDescriptor[] nodes =
      new DatanodeDescriptor[block.numNodes()];
    Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
    for (int i = 0; it != null && it.hasNext(); i++) {
      nodes[i] = it.next();
    }
    return nodes;
  }

  public int getTotalBlocks() {
    return blocksMap.size();
  }

  public void removeBlock(Block block) {
    block.setNumBytes(BlockCommand.NO_ACK);
    addToInvalidates(block);
    corruptReplicas.removeFromCorruptReplicasMap(block);
    blocksMap.removeBlock(block);
    // Remove the block from pendingReplications
    pendingReplications.remove(block);
  }

  public BlockInfo getStoredBlock(Block block) {
    return blocksMap.getStoredBlock(block);
  }

  /** updates a block in under replication queue */
  private void updateNeededReplications(final Block block,
      final int curReplicasDelta, int expectedReplicasDelta) {
    namesystem.writeLock();
    try {
      NumberReplicas repl = countNodes(block);
      int curExpectedReplicas = getReplication(block);
      if (isNeededReplication(block, curExpectedReplicas, repl.liveReplicas())) {
        neededReplications.update(block, repl.liveReplicas(), repl
            .decommissionedReplicas(), curExpectedReplicas, curReplicasDelta,
            expectedReplicasDelta);
      } else {
        int oldReplicas = repl.liveReplicas()-curReplicasDelta;
        int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
        neededReplications.remove(block, oldReplicas, repl.decommissionedReplicas(),
                                  oldExpectedReplicas);
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  public void checkReplication(Block block, short numExpectedReplicas) {
    // filter out containingNodes that are marked for decommission.
    NumberReplicas number = countNodes(block);
    if (isNeededReplication(block, numExpectedReplicas, number.liveReplicas())) { 
      neededReplications.add(block,
                             number.liveReplicas(),
                             number.decommissionedReplicas(),
                             numExpectedReplicas);
      return;
    }
    if (number.liveReplicas() > numExpectedReplicas) {
      processOverReplicatedBlock(block, numExpectedReplicas, null, null);
    }
  }

  /* get replication factor of a block */
  private int getReplication(Block block) {
    INodeFile fileINode = blocksMap.getINode(block);
    if (fileINode == null) { // block does not belong to any file
      return 0;
    }
    assert !fileINode.isDirectory() : "Block cannot belong to a directory.";
    return fileINode.getReplication();
  }


  /**
   * Get blocks to invalidate for <i>nodeId</i>
   * in {@link #invalidateBlocks}.
   *
   * @return number of blocks scheduled for removal during this iteration.
   */
  private int invalidateWorkForOneNode(String nodeId) {
    namesystem.writeLock();
    try {
      // blocks should not be replicated or removed if safe mode is on
      if (namesystem.isInSafeMode())
        return 0;
      // get blocks to invalidate for the nodeId
      assert nodeId != null;
      return invalidateBlocks.invalidateWork(nodeId);
    } finally {
      namesystem.writeUnlock();
    }
  }

  boolean blockHasEnoughRacks(Block b) {
    if (!this.shouldCheckForEnoughRacks) {
      return true;
    }
    boolean enoughRacks = false;;
    Collection<DatanodeDescriptor> corruptNodes = 
                                  corruptReplicas.getNodes(b);
    int numExpectedReplicas = getReplication(b);
    String rackName = null;
    for (Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(b); 
         it.hasNext();) {
      DatanodeDescriptor cur = it.next();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
        if ((corruptNodes == null ) || !corruptNodes.contains(cur)) {
          if (numExpectedReplicas == 1) {
            enoughRacks = true;
            break;
          }
          String rackNameNew = cur.getNetworkLocation();
          if (rackName == null) {
            rackName = rackNameNew;
          } else if (!rackName.equals(rackNameNew)) {
            enoughRacks = true;
            break;
          }
        }
      }
    }
    return enoughRacks;
  }

  boolean isNeededReplication(Block b, int expectedReplication, int curReplicas) {
    if ((curReplicas >= expectedReplication) && (blockHasEnoughRacks(b))) {
      return false;
    } else {
      return true;
    }
  }
  
  public long getMissingBlocksCount() {
    // not locking
    return this.neededReplications.getCorruptBlockSize();
  }

  public BlockInfo addINode(BlockInfo block, INodeFile iNode) {
    return blocksMap.addINode(block, iNode);
  }

  public INodeFile getINode(Block b) {
    return blocksMap.getINode(b);
  }

  /** @return an iterator of the datanodes. */
  public Iterator<DatanodeDescriptor> datanodeIterator(final Block block) {
    return blocksMap.nodeIterator(block);
  }

  public int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }

  public void removeBlockFromMap(Block block) {
    blocksMap.removeBlock(block);
    // If block is removed from blocksMap remove it from corruptReplicasMap
    corruptReplicas.removeFromCorruptReplicasMap(block);
  }

  public int getCapacity() {
    namesystem.readLock();
    try {
      return blocksMap.getCapacity();
    } finally {
      namesystem.readUnlock();
    }
  }
  
  /**
   * Return a range of corrupt replica block ids. Up to numExpectedBlocks 
   * blocks starting at the next block after startingBlockId are returned
   * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId 
   * is null, up to numExpectedBlocks blocks are returned from the beginning.
   * If startingBlockId cannot be found, null is returned.
   *
   * @param numExpectedBlocks Number of block ids to return.
   *  0 <= numExpectedBlocks <= 100
   * @param startingBlockId Block id from which to start. If null, start at
   *  beginning.
   * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
   *
   */
  public long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {
    return corruptReplicas.getCorruptReplicaBlockIds(numExpectedBlocks,
                                                     startingBlockId);
  }

  /**
   * Return an iterator over the set of blocks for which there are no replicas.
   */
  public Iterator<Block> getCorruptReplicaBlockIterator() {
    return neededReplications.iterator(
        UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }

  /** @return the size of UnderReplicatedBlocks */
  public int numOfUnderReplicatedBlocks() {
    return neededReplications.size();
  }

  /**
   * Periodically calls computeReplicationWork().
   */
  private class ReplicationMonitor implements Runnable {
    private static final int INVALIDATE_WORK_PCT_PER_ITERATION = 32;
    private static final int REPLICATION_WORK_MULTIPLIER_PER_ITERATION = 2;

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          computeDatanodeWork();
          processPendingReplications();
          Thread.sleep(replicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("ReplicationMonitor thread received InterruptedException.", ie);
          break;
        } catch (IOException ie) {
          LOG.warn("ReplicationMonitor thread received exception. " , ie);
        } catch (Throwable t) {
          LOG.fatal("ReplicationMonitor thread received Runtime exception. ", t);
          terminate(1);
        }
      }
    }
  }


  /**
   * Compute block replication and block invalidation work that can be scheduled
   * on data-nodes. The datanode will be informed of this work at the next
   * heartbeat.
   * 
   * @return number of blocks scheduled for replication or removal.
   * @throws IOException
   */
  int computeDatanodeWork() throws IOException {
    int workFound = 0;
    // Blocks should not be replicated or removed if in safe mode.
    // It's OK to check safe mode here w/o holding lock, in the worst
    // case extra replications will be scheduled, and these will get
    // fixed up later.
    if (namesystem.isInSafeMode())
      return workFound;

    final int numlive = heartbeatManager.getLiveDatanodeCount();
    final int blocksToProcess = numlive
        * ReplicationMonitor.REPLICATION_WORK_MULTIPLIER_PER_ITERATION;
    final int nodesToProcess = (int) Math.ceil(numlive
        * ReplicationMonitor.INVALIDATE_WORK_PCT_PER_ITERATION / 100.0);

    workFound = this.computeReplicationWork(blocksToProcess);

    // Update counters
    namesystem.writeLock();
    try {
      this.updateState();
      this.scheduledReplicationBlocksCount = workFound;
    } finally {
      namesystem.writeUnlock();
    }
    workFound += this.computeInvalidateWork(nodesToProcess);
    return workFound;
  }

  private static class ReplicationWork {

    private Block block;
    private INodeFile fileINode;

    private DatanodeDescriptor srcNode;
    private List<DatanodeDescriptor> containingNodes;
    private List<DatanodeDescriptor> liveReplicaNodes;
    private int additionalReplRequired;

    private DatanodeDescriptor targets[];
    private int priority;

    public ReplicationWork(Block block,
        INodeFile fileINode,
        DatanodeDescriptor srcNode,
        List<DatanodeDescriptor> containingNodes,
        List<DatanodeDescriptor> liveReplicaNodes,
        int additionalReplRequired,
        int priority) {
      this.block = block;
      this.fileINode = fileINode;
      this.srcNode = srcNode;
      this.containingNodes = containingNodes;
      this.liveReplicaNodes = liveReplicaNodes;
      this.additionalReplRequired = additionalReplRequired;
      this.priority = priority;
      this.targets = null;
    }
  }
}
