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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportIterator;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlocks.BlockIterator;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.Node;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 * This class is a helper class for {@link FSNamesystem} and requires several
 * methods to be called with lock held on {@link FSNamesystem}.
 */
@InterfaceAudience.Private
public class BlockManager {
  static final Log LOG = LogFactory.getLog(BlockManager.class);

  /** Default load factor of map */
  public static final float DEFAULT_MAP_LOAD_FACTOR = 0.75f;

  private final FSNamesystem namesystem;

  private volatile long pendingReplicationBlocksCount = 0L;
  private volatile long corruptReplicaBlocksCount = 0L;
  private volatile long underReplicatedBlocksCount = 0L;
  public volatile long scheduledReplicationBlocksCount = 0L;
  private volatile long excessBlocksCount = 0L;
  private volatile long pendingDeletionBlocksCount = 0L;

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
    return pendingDeletionBlocksCount;
  }
  /** Used by metrics */
  public long getExcessBlocksCount() {
    return excessBlocksCount;
  }

  /**
   * Mapping: Block -> { INode, datanodes, self ref }
   * Updated only in response to client-sent information.
   */
  public final BlocksMap blocksMap;

  private final DatanodeManager datanodeManager;

  //
  // Store blocks-->datanodedescriptor(s) map of corrupt replicas
  //
  private final CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();

  //
  // Keeps a Collection for every named machine containing
  // blocks that have recently been invalidated and are thought to live
  // on the machine in question.
  // Mapping: StorageID -> ArrayList<Block>
  //
  private final Map<String, Collection<Block>> recentInvalidateSets =
    new TreeMap<String, Collection<Block>>();

  //
  // Keeps a TreeSet for every named node. Each treeset contains
  // a list of the blocks that are "extra" at that location. We'll
  // eventually remove these extras.
  // Mapping: StorageID -> TreeSet<Block>
  //
  public final Map<String, Collection<Block>> excessReplicateMap =
    new TreeMap<String, Collection<Block>>();

  //
  // Store set of Blocks that need to be replicated 1 or more times.
  // We also store pending replication-orders.
  //
  public final UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();
  private final PendingReplicationBlocks pendingReplications;

  //  The maximum number of replicas allowed for a block
  public final int maxReplication;
  //  How many outgoing replication streams a given node should have at one time
  public int maxReplicationStreams;
  // Minimum copies needed or else write is disallowed
  public final int minReplication;
  // Default number of replicas
  public final int defaultReplication;
  // How many entries are returned by getCorruptInodes()
  final int maxCorruptFilesReturned;
  
  // variable to enable check for enough racks 
  final boolean shouldCheckForEnoughRacks;

  /**
   * Last block index used for replication work.
   */
  private int replIndex = 0;
  Random r = new Random();

  // for block replicas placement
  public final BlockPlacementPolicy replicator;

  public BlockManager(FSNamesystem fsn, Configuration conf) throws IOException {
    namesystem = fsn;
    datanodeManager = new DatanodeManager(fsn);

    blocksMap = new BlocksMap(DEFAULT_MAP_LOAD_FACTOR);
    replicator = BlockPlacementPolicy.getInstance(
        conf, namesystem, datanodeManager.getNetworkTopology());
    pendingReplications = new PendingReplicationBlocks(conf.getInt(
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT) * 1000L);

    this.maxCorruptFilesReturned = conf.getInt(
      DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY,
      DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED);
    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 
                                          DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    this.maxReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY, 
                                      DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
    this.minReplication = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
                                      DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    if (minReplication <= 0)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.namenode.replication.min = "
                            + minReplication
                            + " must be greater than 0");
    if (maxReplication >= (int)Short.MAX_VALUE)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.max = "
                            + maxReplication + " must be less than " + (Short.MAX_VALUE));
    if (maxReplication < minReplication)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.namenode.replication.min = "
                            + minReplication
                            + " must be less than dfs.replication.max = "
                            + maxReplication);
    this.maxReplicationStreams = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
                                             DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT);
    this.shouldCheckForEnoughRacks = conf.get(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY) == null ? false
                                                                             : true;
    FSNamesystem.LOG.info("defaultReplication = " + defaultReplication);
    FSNamesystem.LOG.info("maxReplication = " + maxReplication);
    FSNamesystem.LOG.info("minReplication = " + minReplication);
    FSNamesystem.LOG.info("maxReplicationStreams = " + maxReplicationStreams);
    FSNamesystem.LOG.info("shouldCheckForEnoughRacks = " + shouldCheckForEnoughRacks);
  }

  public void activate(Configuration conf) {
    pendingReplications.start();
    datanodeManager.activate(conf);
  }

  public void close() {
    if (pendingReplications != null) pendingReplications.stop();
    blocksMap.close();
    datanodeManager.close();
  }

  /** @return the datanodeManager */
  public DatanodeManager getDatanodeManager() {
    return datanodeManager;
  }

  public void metaSave(PrintWriter out) {
    //
    // Dump contents of neededReplication
    //
    synchronized (neededReplications) {
      out.println("Metasave: Blocks waiting for replication: " + 
                  neededReplications.size());
      for (Block block : neededReplications) {
        List<DatanodeDescriptor> containingNodes =
                                          new ArrayList<DatanodeDescriptor>();
        NumberReplicas numReplicas = new NumberReplicas();
        // source node returned is not used
        chooseSourceDatanode(block, containingNodes, numReplicas);
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

    //
    // Dump blocks from pendingReplication
    //
    pendingReplications.metaSave(out);

    //
    // Dump blocks that are waiting to be deleted
    //
    dumpRecentInvalidateSets(out);
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
   * @param fileINode file inode
   * @param block block to be committed
   * @param commitBlock - contains client reported block length and generation
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private void commitBlock(INodeFileUnderConstruction fileINode,
                       BlockInfoUnderConstruction block,
                       Block commitBlock) throws IOException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED)
      return;
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
      "commitBlock length is less than the stored one "
      + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
    block.commitBlock(commitBlock);
    
    // Adjust disk space consumption if required
    long diff = fileINode.getPreferredBlockSize() - commitBlock.getNumBytes();    
    if (diff > 0) {
      try {
        String path = /* For finding parents */
        namesystem.leaseManager.findPath(fileINode);
        namesystem.dir.updateSpaceConsumed(path, 0, -diff
            * fileINode.getReplication());
      } catch (IOException e) {
        FSNamesystem.LOG
            .warn("Unexpected exception while updating disk space : "
                + e.getMessage());
      }
    }
  }
  
  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum replication requirement
   * 
   * @param fileINode file inode
   * @param commitBlock - contains client reported block length and generation
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  public void commitOrCompleteLastBlock(INodeFileUnderConstruction fileINode, 
      Block commitBlock) throws IOException {
    
    if(commitBlock == null)
      return; // not committing, this is a block allocation retry
    BlockInfo lastBlock = fileINode.getLastBlock();
    if(lastBlock == null)
      return; // no blocks in file yet
    if(lastBlock.isComplete())
      return; // already completed (e.g. by syncBlock)
    
    commitBlock(fileINode, (BlockInfoUnderConstruction)lastBlock, commitBlock);

    if(countNodes(lastBlock).liveReplicas() >= minReplication)
      completeBlock(fileINode,fileINode.numBlocks()-1);
  }

  /**
   * Convert a specified block of the file to a complete block.
   * @param fileINode file
   * @param blkIndex  block index in the file
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  BlockInfo completeBlock(INodeFile fileINode, int blkIndex)
  throws IOException {
    if(blkIndex < 0)
      return null;
    BlockInfo curBlock = fileINode.getBlocks()[blkIndex];
    if(curBlock.isComplete())
      return curBlock;
    BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction)curBlock;
    if(ucBlock.numNodes() < minReplication)
      throw new IOException("Cannot complete block: " +
          "block does not satisfy minimal replication requirement.");
    BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
    // replace penultimate block in file
    fileINode.setBlock(blkIndex, completeBlock);
    // replace block in the blocksMap
    return blocksMap.replaceBlock(completeBlock);
  }

  BlockInfo completeBlock(INodeFile fileINode, BlockInfo block)
  throws IOException {
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
      removeFromInvalidates(datanodeId, oldBlock);
    }

    long fileLength = fileINode.computeContentSummary().getLength();
    return getBlockLocation(ucBlock, fileLength - ucBlock.getNumBytes());
  }

  /**
   * Get all valid locations of the block
   */
  public ArrayList<String> getValidLocations(Block block) {
    ArrayList<String> machineSet =
      new ArrayList<String>(blocksMap.numNodes(block));
    for(Iterator<DatanodeDescriptor> it =
      blocksMap.nodeIterator(block); it.hasNext();) {
      String storageID = it.next().getStorageID();
      // filter invalidate replicas
      if( ! belongsToInvalidates(storageID, block)) {
        machineSet.add(storageID);
      }
    }
    return machineSet;
  }

  public List<LocatedBlock> getBlockLocations(BlockInfo[] blocks, long offset,
      long length, int nrBlocksToReturn) throws IOException {
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
      results.add(getBlockLocation(blocks[curBlk], curPos));
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff 
          && curBlk < blocks.length
          && results.size() < nrBlocksToReturn);
    return results;
  }

  /** @return a LocatedBlock for the given block */
  public LocatedBlock getBlockLocation(final BlockInfo blk, final long pos
      ) throws IOException {
    if (blk instanceof BlockInfoUnderConstruction) {
      if (blk.isComplete()) {
        throw new IOException(
            "blk instanceof BlockInfoUnderConstruction && blk.isComplete()"
            + ", blk=" + blk);
      }
      final BlockInfoUnderConstruction uc = (BlockInfoUnderConstruction)blk;
      final DatanodeDescriptor[] locations = uc.getExpectedLocations();
      return namesystem.createLocatedBlock(uc, locations, pos, false);
    }

    // get block locations
    final int numCorruptNodes = countNodes(blk).corruptReplicas();
    final int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blk);
    if (numCorruptNodes != numCorruptReplicas) {
      FSNamesystem.LOG.warn("Inconsistent number of corrupt replicas for "
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
    return namesystem.createLocatedBlock(blk, machines, pos, isCorrupt);
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

  /** Remove a datanode. */
  public void removeDatanode(final DatanodeDescriptor node) {
    final Iterator<? extends Block> it = node.getBlockIterator();
    while(it.hasNext()) {
      removeStoredBlock(it.next(), node);
    }

    node.resetBlocks();
    removeFromInvalidates(node.getStorageID());
    datanodeManager.getNetworkTopology().remove(node);

    if (LOG.isDebugEnabled()) {
      LOG.debug("remove datanode " + node.getName());
    }
  }
  
  void removeFromInvalidates(String storageID, Block block) {
    Collection<Block> v = recentInvalidateSets.get(storageID);
    if (v != null && v.remove(block)) {
      pendingDeletionBlocksCount--;
      if (v.isEmpty()) {
        recentInvalidateSets.remove(storageID);
      }
    }
  }

  boolean belongsToInvalidates(String storageID, Block block) {
    Collection<Block> invalidateSet = recentInvalidateSets.get(storageID);
    return invalidateSet != null && invalidateSet.contains(block);
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode
   *
   * @param b block
   * @param dn datanode
   * @param log true to create an entry in the log 
   */
  void addToInvalidates(Block b, DatanodeInfo dn, boolean log) {
    Collection<Block> invalidateSet = recentInvalidateSets
        .get(dn.getStorageID());
    if (invalidateSet == null) {
      invalidateSet = new HashSet<Block>();
      recentInvalidateSets.put(dn.getStorageID(), invalidateSet);
    }
    if (invalidateSet.add(b)) {
      pendingDeletionBlocksCount++;
      if (log) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.addToInvalidates: "
            + b + " to " + dn.getName());
      }
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the operation
   *
   * @param b block
   * @param dn datanode
   */
  public void addToInvalidates(Block b, DatanodeInfo dn) {
    addToInvalidates(b, dn, true);
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
      addToInvalidates(b, node, false);
      datanodes.append(node.getName()).append(" ");
    }
    if (datanodes.length() != 0) {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.addToInvalidates: "
          + b + " to " + datanodes.toString());
    }
  }

  /**
   * dumps the contents of recentInvalidateSets
   */
  private void dumpRecentInvalidateSets(PrintWriter out) {
    assert namesystem.hasWriteLock();
    int size = recentInvalidateSets.values().size();
    out.println("Metasave: Blocks " + pendingDeletionBlocksCount 
        + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }
    for(Map.Entry<String,Collection<Block>> entry : recentInvalidateSets.entrySet()) {
      Collection<Block> blocks = entry.getValue();
      if (blocks.size() > 0) {
        out.println(namesystem.getDatanode(entry.getKey()).getName() + blocks);
      }
    }
  }

  public void findAndMarkBlockAsCorrupt(Block blk,
                                 DatanodeInfo dn) throws IOException {
    BlockInfo storedBlock = getStoredBlock(blk);
    if (storedBlock == null) {
      // Check if the replica is in the blockMap, if not
      // ignore the request for now. This could happen when BlockScanner
      // thread of Datanode reports bad block before Block reports are sent
      // by the Datanode on startup
      NameNode.stateChangeLog.info("BLOCK* NameSystem.markBlockAsCorrupt: " +
                                   "block " + blk + " could not be marked as " +
                                   "corrupt as it does not exist in blocksMap");
      return;
    }
    markBlockAsCorrupt(storedBlock, dn);
  }

  private void markBlockAsCorrupt(BlockInfo storedBlock,
                                  DatanodeInfo dn) throws IOException {
    assert storedBlock != null : "storedBlock should not be null";
    DatanodeDescriptor node = namesystem.getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot mark block " + 
                            storedBlock.getBlockName() +
                            " as corrupt because datanode " + dn.getName() +
                            " does not exist. ");
    }

    INodeFile inode = storedBlock.getINode();
    if (inode == null) {
      NameNode.stateChangeLog.info("BLOCK NameSystem.markBlockAsCorrupt: " +
                                   "block " + storedBlock +
                                   " could not be marked as corrupt as it" +
                                   " does not belong to any file");
      addToInvalidates(storedBlock, node);
      return;
    } 

    // Add replica to the data-node if it is not already there
    node.addBlock(storedBlock);

    // Add this replica to corruptReplicas Map
    corruptReplicas.addToCorruptReplicasMap(storedBlock, node);
    if (countNodes(storedBlock).liveReplicas() > inode.getReplication()) {
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
    NameNode.stateChangeLog.info("DIR* NameSystem.invalidateBlock: "
                                 + blk + " on " + dn.getName());
    DatanodeDescriptor node = namesystem.getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot invalidate block " + blk +
                            " because datanode " + dn.getName() +
                            " does not exist.");
    }

    // Check how many copies we have of the block. If we have at least one
    // copy on a live node, then we can delete it.
    int count = countNodes(blk).liveReplicas();
    if (count > 1) {
      addToInvalidates(blk, dn);
      removeStoredBlock(blk, node);
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.invalidateBlocks: "
            + blk + " on "
            + dn.getName() + " listed for deletion.");
      }
    } else {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.invalidateBlocks: "
          + blk + " on " + dn.getName()
          + " is the only copy and was not deleted.");
    }
  }

  public void updateState() {
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
  public int computeInvalidateWork(int nodesToProcess) {
    int numOfNodes = recentInvalidateSets.size();
    nodesToProcess = Math.min(numOfNodes, nodesToProcess);

    // TODO should using recentInvalidateSets be synchronized?
    // get an array of the keys
    ArrayList<String> keyArray =
      new ArrayList<String>(recentInvalidateSets.keySet());

    // randomly pick up <i>nodesToProcess</i> nodes
    // and put them at [0, nodesToProcess)
    int remainingNodes = numOfNodes - nodesToProcess;
    if (nodesToProcess < remainingNodes) {
      for(int i=0; i<nodesToProcess; i++) {
        int keyIndex = r.nextInt(numOfNodes-i)+i;
        Collections.swap(keyArray, keyIndex, i); // swap to front
      }
    } else {
      for(int i=0; i<remainingNodes; i++) {
        int keyIndex = r.nextInt(numOfNodes-i);
        Collections.swap(keyArray, keyIndex, numOfNodes-i-1); // swap to end
      }
    }

    int blockCnt = 0;
    for(int nodeCnt = 0; nodeCnt < nodesToProcess; nodeCnt++ ) {
      blockCnt += invalidateWorkForOneNode(keyArray.get(nodeCnt));
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
  public int computeReplicationWork(int blocksToProcess) throws IOException {
    // Choose the blocks to be replicated
    List<List<Block>> blocksToReplicate =
      chooseUnderReplicatedBlocks(blocksToProcess);

    // replicate blocks
    int scheduledReplicationCount = 0;
    for (int i=0; i<blocksToReplicate.size(); i++) {
      for(Block block : blocksToReplicate.get(i)) {
        if (computeReplicationWorkForBlock(block, i)) {
          scheduledReplicationCount++;
        }
      }
    }
    return scheduledReplicationCount;
  }

  /**
   * Get a list of block lists to be replicated The index of block lists
   * represents the
   *
   * @param blocksToProcess
   * @return Return a list of block lists to be replicated. The block list index
   *         represents its replication priority.
   */
  private List<List<Block>> chooseUnderReplicatedBlocks(int blocksToProcess) {
    // initialize data structure for the return value
    List<List<Block>> blocksToReplicate = new ArrayList<List<Block>>(
        UnderReplicatedBlocks.LEVEL);
    for (int i = 0; i < UnderReplicatedBlocks.LEVEL; i++) {
      blocksToReplicate.add(new ArrayList<Block>());
    }
    namesystem.writeLock();
    try {
      synchronized (neededReplications) {
        if (neededReplications.size() == 0) {
          return blocksToReplicate;
        }

        // Go through all blocks that need replications.
        UnderReplicatedBlocks.BlockIterator neededReplicationsIterator = 
            neededReplications.iterator();
        // skip to the first unprocessed block, which is at replIndex
        for (int i = 0; i < replIndex && neededReplicationsIterator.hasNext(); i++) {
          neededReplicationsIterator.next();
        }
        // # of blocks to process equals either twice the number of live
        // data-nodes or the number of under-replicated blocks whichever is less
        blocksToProcess = Math.min(blocksToProcess, neededReplications.size());

        for (int blkCnt = 0; blkCnt < blocksToProcess; blkCnt++, replIndex++) {
          if (!neededReplicationsIterator.hasNext()) {
            // start from the beginning
            replIndex = 0;
            blocksToProcess = Math.min(blocksToProcess, neededReplications
                .size());
            if (blkCnt >= blocksToProcess)
              break;
            neededReplicationsIterator = neededReplications.iterator();
            assert neededReplicationsIterator.hasNext() : "neededReplications should not be empty.";
          }

          Block block = neededReplicationsIterator.next();
          int priority = neededReplicationsIterator.getPriority();
          if (priority < 0 || priority >= blocksToReplicate.size()) {
            FSNamesystem.LOG.warn("Unexpected replication priority: "
                + priority + " " + block);
          } else {
            blocksToReplicate.get(priority).add(block);
          }
        } // end for
      } // end synchronized neededReplication
    } finally {
      namesystem.writeUnlock();
    }

    return blocksToReplicate;
  }

  /** Replicate a block
   *
   * @param block block to be replicated
   * @param priority a hint of its priority in the neededReplication queue
   * @return if the block gets replicated or not
   */
  private boolean computeReplicationWorkForBlock(Block block, int priority) {
    int requiredReplication, numEffectiveReplicas;
    List<DatanodeDescriptor> containingNodes;
    DatanodeDescriptor srcNode;
    INodeFile fileINode = null;
    int additionalReplRequired;

    namesystem.writeLock();
    try {
      synchronized (neededReplications) {
        // block should belong to a file
        fileINode = blocksMap.getINode(block);
        // abandoned block or block reopened for append
        if(fileINode == null || fileINode.isUnderConstruction()) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
          return false;
        }

        requiredReplication = fileINode.getReplication();

        // get a source data-node
        containingNodes = new ArrayList<DatanodeDescriptor>();
        NumberReplicas numReplicas = new NumberReplicas();
        srcNode = chooseSourceDatanode(block, containingNodes, numReplicas);
        if(srcNode == null) // block can not be replicated from any node
          return false;

        // do not schedule more if enough replicas is already pending
        numEffectiveReplicas = numReplicas.liveReplicas() +
                                pendingReplications.getNumReplicas(block);
      
        if (numEffectiveReplicas >= requiredReplication) {
          if ( (pendingReplications.getNumReplicas(block) > 0) ||
               (blockHasEnoughRacks(block)) ) {
            neededReplications.remove(block, priority); // remove from neededReplications
            replIndex--;
            NameNode.stateChangeLog.info("BLOCK* "
                + "Removing block " + block
                + " from neededReplications as it has enough replicas.");
            return false;
          }
        }

        if (numReplicas.liveReplicas() < requiredReplication) {
          additionalReplRequired = requiredReplication - numEffectiveReplicas;
        } else {
          additionalReplRequired = 1; //Needed on a new rack
        }

      }
    } finally {
      namesystem.writeUnlock();
    }

    // choose replication targets: NOT HOLDING THE GLOBAL LOCK
    // It is costly to extract the filename for which chooseTargets is called,
    // so for now we pass in the Inode itself.
    DatanodeDescriptor targets[] = 
                       replicator.chooseTarget(fileINode, additionalReplRequired,
                       srcNode, containingNodes, block.getNumBytes());
    if(targets.length == 0)
      return false;

    namesystem.writeLock();
    try {
      synchronized (neededReplications) {
        // Recheck since global lock was released
        // block should belong to a file
        fileINode = blocksMap.getINode(block);
        // abandoned block or block reopened for append
        if(fileINode == null || fileINode.isUnderConstruction()) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
          return false;
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
            replIndex--;
            NameNode.stateChangeLog.info("BLOCK* "
                + "Removing block " + block
                + " from neededReplications as it has enough replicas.");
            return false;
          }
        }

        if ( (numReplicas.liveReplicas() >= requiredReplication) &&
             (!blockHasEnoughRacks(block)) ) {
          if (srcNode.getNetworkLocation().equals(targets[0].getNetworkLocation())) {
            //No use continuing, unless a new rack in this case
            return false;
          }
        }

        // Add block to the to be replicated list
        srcNode.addBlockToBeReplicated(block, targets);

        for (DatanodeDescriptor dn : targets) {
          dn.incBlocksScheduled();
        }

        // Move the block-replication into a "pending" state.
        // The reason we use 'pending' is so we can retry
        // replications that fail after an appropriate amount of time.
        pendingReplications.add(block, targets.length);
        if(NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "BLOCK* block " + block
              + " is moved from neededReplications to pendingReplications");
        }

        // remove from neededReplications
        if(numEffectiveReplicas + targets.length >= requiredReplication) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
        }
        if (NameNode.stateChangeLog.isInfoEnabled()) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (int k = 0; k < targets.length; k++) {
            targetList.append(' ');
            targetList.append(targets[k].getName());
          }
          NameNode.stateChangeLog.info(
                    "BLOCK* ask "
                    + srcNode.getName() + " to replicate "
                    + block + " to " + targetList);
          if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug(
                "BLOCK* neededReplications = " + neededReplications.size()
                + " pendingReplications = " + pendingReplications.size());
          }
        }
      }
    } finally {
      namesystem.writeUnlock();
    }

    return true;
  }

  /**
   * Choose target datanodes according to the replication policy.
   * @throws IOException if the number of targets < minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, DatanodeDescriptor, HashMap, long)
   */
  public DatanodeDescriptor[] chooseTarget(final String src,
      final int numOfReplicas, final DatanodeDescriptor client,
      final HashMap<Node, Node> excludedNodes,
      final long blocksize) throws IOException {
    // choose targets for the new block to be allocated.
    final DatanodeDescriptor targets[] = replicator.chooseTarget(
        src, numOfReplicas, client, excludedNodes, blocksize);
    if (targets.length < minReplication) {
      throw new IOException("File " + src + " could only be replicated to " +
                            targets.length + " nodes, instead of " +
                            minReplication + ". There are "
                            + getDatanodeManager().getNetworkTopology().getNumOfLeaves()
                            + " datanode(s) running but "+excludedNodes.size() +
                            " node(s) are excluded in this operation.");
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
   * replication limit.
   *
   * In addition form a list of all nodes containing the block
   * and calculate its replication numbers.
   */
  private DatanodeDescriptor chooseSourceDatanode(
                                    Block block,
                                    List<DatanodeDescriptor> containingNodes,
                                    NumberReplicas numReplicas) {
    containingNodes.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;
    Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    while(it.hasNext()) {
      DatanodeDescriptor node = it.next();
      Collection<Block> excessBlocks =
        excessReplicateMap.get(node.getStorageID());
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node)))
        corrupt++;
      else if (node.isDecommissionInProgress() || node.isDecommissioned())
        decommissioned++;
      else if (excessBlocks != null && excessBlocks.contains(block)) {
        excess++;
      } else {
        live++;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node))
        continue;
      if(node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams)
        continue; // already reached replication limit
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
      if(r.nextBoolean())
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
  public void processPendingReplications() {
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
   * The given node is reporting all its blocks.  Use this info to
   * update the (datanode-->blocklist) and (block-->nodelist) tables.
   */
  public void processReport(DatanodeDescriptor node, BlockListAsLongs report) 
  throws IOException {
    
    boolean isFirstBlockReport = (node.numBlocks() == 0);
    if (isFirstBlockReport) {
      // Initial block reports can be processed a lot more efficiently than
      // ordinary block reports.  This shortens NN restart times.
      processFirstBlockReport(node, report);
      return;
    } 

    // Normal case:
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
    Collection<Block> toRemove = new LinkedList<Block>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockInfo> toCorrupt = new LinkedList<BlockInfo>();
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
      NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: block "
          + b + " on " + node.getName() + " size " + b.getNumBytes()
          + " does not belong to any file.");
      addToInvalidates(b, node);
    }
    for (BlockInfo b : toCorrupt) {
      markBlockAsCorrupt(b, node);
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
  void processFirstBlockReport(DatanodeDescriptor node, BlockListAsLongs report) 
  throws IOException {
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
      if (isReplicaCorrupt(iblk, reportedState, storedBlock, ucState, node)) {
        markBlockAsCorrupt(storedBlock, node);
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

  void reportDiff(DatanodeDescriptor dn, 
      BlockListAsLongs newReport, 
      Collection<BlockInfo> toAdd,              // add to DatanodeDescriptor
      Collection<Block> toRemove,           // remove from DatanodeDescriptor
      Collection<Block> toInvalidate,       // should be removed from DN
      Collection<BlockInfo> toCorrupt,      // add to corrupt replicas list
      Collection<StatefulBlockInfo> toUC) { // add to under-construction list
    // place a delimiter in the list which separates blocks 
    // that have been reported from those that have not
    BlockInfo delimiter = new BlockInfo(new Block(), 1);
    boolean added = dn.addBlock(delimiter);
    assert added : "Delimiting block cannot be present in the node";
    if(newReport == null)
      newReport = new BlockListAsLongs();
    // scan the report and process newly reported blocks
    BlockReportIterator itBR = newReport.getBlockReportIterator();
    while(itBR.hasNext()) {
      Block iblk = itBR.next();
      ReplicaState iState = itBR.getCurrentReplicaState();
      BlockInfo storedBlock = processReportedBlock(dn, iblk, iState,
                                  toAdd, toInvalidate, toCorrupt, toUC);
      // move block to the head of the list
      if(storedBlock != null && storedBlock.findDatanode(dn) >= 0)
        dn.moveBlockToHead(storedBlock);
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
  BlockInfo processReportedBlock(DatanodeDescriptor dn, 
      Block block, ReplicaState reportedState, 
      Collection<BlockInfo> toAdd, 
      Collection<Block> toInvalidate, 
      Collection<BlockInfo> toCorrupt,
      Collection<StatefulBlockInfo> toUC) {
    
    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("Reported block " + block
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
    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("In memory blockUCState = " + ucState);
    }

    // Ignore replicas already scheduled to be removed from the DN
    if(belongsToInvalidates(dn.getStorageID(), block)) {
      assert storedBlock.findDatanode(dn) < 0 : "Block " + block
        + " in recentInvalidatesSet should not appear in DN " + dn;
      return storedBlock;
    }

    if (isReplicaCorrupt(block, reportedState, storedBlock, ucState, dn)) {
      toCorrupt.add(storedBlock);
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

  /*
   * The next two methods test the various cases under which we must conclude
   * the replica is corrupt, or under construction.  These are laid out
   * as switch statements, on the theory that it is easier to understand
   * the combinatorics of reportedState and ucState that way.  It should be
   * at least as efficient as boolean expressions.
   */
  private boolean isReplicaCorrupt(Block iblk, ReplicaState reportedState, 
      BlockInfo storedBlock, BlockUCState ucState, 
      DatanodeDescriptor dn) {
    switch(reportedState) {
    case FINALIZED:
      switch(ucState) {
      case COMPLETE:
      case COMMITTED:
        return (storedBlock.getGenerationStamp() != iblk.getGenerationStamp()
            || storedBlock.getNumBytes() != iblk.getNumBytes());
      default:
        return false;
      }
    case RBW:
    case RWR:
      return storedBlock.isComplete();
    case RUR:       // should not be reported
    case TEMPORARY: // should not be reported
    default:
      FSNamesystem.LOG.warn("Unexpected replica state " + reportedState
          + " for block: " + storedBlock + 
          " on " + dn.getName() + " size " + storedBlock.getNumBytes());
      return true;
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
   * Faster version of {@link addStoredBlock()}, intended for use with 
   * initial block report at startup.  If not in startup safe mode, will
   * call standard addStoredBlock().
   * Assumes this method is called "immediately" so there is no need to
   * refresh the storedBlock from blocksMap.
   * Doesn't handle underReplication/overReplication, or worry about
   * pendingReplications or corruptReplicas, because it's in startup safe mode.
   * Doesn't log every block, because there are typically millions of them.
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
      NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
                                   + "addStoredBlock request received for "
                                   + block + " on " + node.getName()
                                   + " size " + block.getNumBytes()
                                   + " But it does not belong to any file.");
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
        NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
            + "blockMap updated: " + node.getName() + " is added to " + 
            storedBlock + " size " + storedBlock.getNumBytes());
      }
    } else {
      curReplicaDelta = 0;
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.addStoredBlock: "
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
      FSNamesystem.LOG.warn("Inconsistent number of corrupt replicas for " +
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
   * add them to {@link #recentInvalidateSets} so that they could be further
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
        NameNode.stateChangeLog.info("NameNode.invalidateCorruptReplicas " +
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
    long nrInvalid = 0, nrOverReplicated = 0, nrUnderReplicated = 0;
    namesystem.writeLock();
    try {
      neededReplications.clear();
      for (BlockInfo block : blocksMap.getBlocks()) {
        INodeFile fileINode = block.getINode();
        if (fileINode == null) {
          // block does not belong to any file
          nrInvalid++;
          addToInvalidates(block);
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
    } finally {
      namesystem.writeUnlock();
    }
    FSNamesystem.LOG.info("Total number of blocks = " + blocksMap.size());
    FSNamesystem.LOG.info("Number of invalid blocks = " + nrInvalid);
    FSNamesystem.LOG.info("Number of under-replicated blocks = " + nrUnderReplicated);
    FSNamesystem.LOG.info("Number of  over-replicated blocks = " + nrOverReplicated);
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   */
  public void processOverReplicatedBlock(Block block, short replication,
      DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {
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
      Collection<Block> excessBlocks = excessReplicateMap.get(cur
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
    namesystem.chooseExcessReplicates(nonExcess, block, replication, 
        addedNode, delNodeHint, replicator);
  }

  public void addToExcessReplicate(DatanodeInfo dn, Block block) {
    assert namesystem.hasWriteLock();
    Collection<Block> excessBlocks = excessReplicateMap.get(dn.getStorageID());
    if (excessBlocks == null) {
      excessBlocks = new TreeSet<Block>();
      excessReplicateMap.put(dn.getStorageID(), excessBlocks);
    }
    if (excessBlocks.add(block)) {
      excessBlocksCount++;
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates:"
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
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
          + block + " from " + node.getName());
    }
    assert (namesystem.hasWriteLock());
    {
      if (!blocksMap.removeNode(block, node)) {
        if(NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
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
      Collection<Block> excessBlocks = excessReplicateMap.get(node
          .getStorageID());
      if (excessBlocks != null) {
        if (excessBlocks.remove(block)) {
          excessBlocksCount--;
          if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug(
                "BLOCK* NameSystem.removeStoredBlock: "
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
   * The given node is reporting that it received a certain block.
   */
  public void addBlock(DatanodeDescriptor node, Block block, String delHint)
      throws IOException {
    // decrement number of blocks scheduled to this datanode.
    node.decBlocksScheduled();

    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if (delHint != null && delHint.length() != 0) {
      delHintNode = namesystem.getDatanode(delHint);
      if (delHintNode == null) {
        NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: "
            + block + " is expected to be removed from an unrecorded node "
            + delHint);
      }
    }

    //
    // Modify the blocks->datanode map and node's map.
    //
    pendingReplications.remove(block);

    // blockReceived reports a finalized block
    Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockInfo> toCorrupt = new LinkedList<BlockInfo>();
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
      NameNode.stateChangeLog.info("BLOCK* NameSystem.addBlock: block "
          + b + " on " + node.getName() + " size " + b.getNumBytes()
          + " does not belong to any file.");
      addToInvalidates(b, node);
    }
    for (BlockInfo b : toCorrupt) {
      markBlockAsCorrupt(b, node);
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
        Collection<Block> blocksExcess =
          excessReplicateMap.get(node.getStorageID());
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
   * Simpler, faster form of {@link countNodes()} that only returns the number
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
    FSNamesystem.LOG.info("Block: " + block + ", Expected Replicas: "
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
   * Return true if there are any blocks on this node that have not
   * yet reached their replication factor. Otherwise returns false.
   */
  public boolean isReplicationInProgress(DatanodeDescriptor srcNode) {
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
    return blocksMap.size() - (int)pendingDeletionBlocksCount;
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
    addToInvalidates(block);
    corruptReplicas.removeFromCorruptReplicasMap(block);
    blocksMap.removeBlock(block);
  }

  public BlockInfo getStoredBlock(Block block) {
    return blocksMap.getStoredBlock(block);
  }

  /* updates a block in under replication queue */
  public void updateNeededReplications(Block block, int curReplicasDelta,
      int expectedReplicasDelta) {
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

  public void checkReplication(Block block, int numExpectedReplicas) {
    // filter out containingNodes that are marked for decommission.
    NumberReplicas number = countNodes(block);
    if (isNeededReplication(block, numExpectedReplicas, number.liveReplicas())) { 
      neededReplications.add(block,
                             number.liveReplicas(),
                             number.decommissionedReplicas(),
                             numExpectedReplicas);
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

  /** Remove a datanode from the invalidatesSet */
  public void removeFromInvalidates(String storageID) {
    Collection<Block> blocks = recentInvalidateSets.remove(storageID);
    if (blocks != null) {
      pendingDeletionBlocksCount -= blocks.size();
    }
  }

  /**
   * Get blocks to invalidate for <i>nodeId</i>
   * in {@link #recentInvalidateSets}.
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
      DatanodeDescriptor dn = namesystem.getDatanode(nodeId);
      if (dn == null) {
        removeFromInvalidates(nodeId);
        return 0;
      }

      Collection<Block> invalidateSet = recentInvalidateSets.get(nodeId);
      if (invalidateSet == null)
        return 0;

      ArrayList<Block> blocksToInvalidate = new ArrayList<Block>(
          namesystem.blockInvalidateLimit);

      // # blocks that can be sent in one message is limited
      Iterator<Block> it = invalidateSet.iterator();
      for (int blkCount = 0; blkCount < namesystem.blockInvalidateLimit
          && it.hasNext(); blkCount++) {
        blocksToInvalidate.add(it.next());
        it.remove();
      }

      // If we send everything in this message, remove this node entry
      if (!it.hasNext()) {
        removeFromInvalidates(nodeId);
      }

      dn.addBlocksToBeInvalidated(blocksToInvalidate);

      if (NameNode.stateChangeLog.isInfoEnabled()) {
        StringBuilder blockList = new StringBuilder();
        for (Block blk : blocksToInvalidate) {
          blockList.append(' ');
          blockList.append(blk);
        }
        NameNode.stateChangeLog.info("BLOCK* ask " + dn.getName()
            + " to delete " + blockList);
      }
      pendingDeletionBlocksCount -= blocksToInvalidate.size();
      return blocksToInvalidate.size();
    } finally {
      namesystem.writeUnlock();
    }
  }
  
  //Returns the number of racks over which a given block is replicated
  //decommissioning/decommissioned nodes are not counted. corrupt replicas 
  //are also ignored
  public int getNumberOfRacks(Block b) {
    HashSet<String> rackSet = new HashSet<String>(0);
    Collection<DatanodeDescriptor> corruptNodes = 
                                  corruptReplicas.getNodes(b);
    for (Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(b); 
         it.hasNext();) {
      DatanodeDescriptor cur = it.next();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
        if ((corruptNodes == null ) || !corruptNodes.contains(cur)) {
          String rackName = cur.getNetworkLocation();
          if (!rackSet.contains(rackName)) {
            rackSet.add(rackName);
          }
        }
      }
    }
    return rackSet.size();
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

  public void removeFromCorruptReplicasMap(Block block) {
    corruptReplicas.removeFromCorruptReplicasMap(block);
  }

  public int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }

  public void removeBlockFromMap(Block block) {
    blocksMap.removeBlock(block);
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
  public BlockIterator getCorruptReplicaBlockIterator() {
    return neededReplications
        .iterator(UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }
}
