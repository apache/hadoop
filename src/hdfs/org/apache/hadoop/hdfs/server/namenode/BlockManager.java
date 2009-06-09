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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NumberReplicas;
import org.apache.hadoop.hdfs.server.namenode.UnderReplicatedBlocks.BlockIterator;
import org.apache.hadoop.security.AccessTokenHandler;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 * This class is a helper class for {@link FSNamesystem} and requires several
 * methods to be called with lock held on {@link FSNamesystem}.
 */
public class BlockManager {
  private final FSNamesystem namesystem;

  long pendingReplicationBlocksCount = 0L, corruptReplicaBlocksCount,
  underReplicatedBlocksCount = 0L, scheduledReplicationBlocksCount = 0L;

  //
  // Mapping: Block -> { INode, datanodes, self ref }
  // Updated only in response to client-sent information.
  //
  BlocksMap blocksMap = new BlocksMap();

  //
  // Store blocks-->datanodedescriptor(s) map of corrupt replicas
  //
  CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();

  //
  // Keeps a Collection for every named machine containing
  // blocks that have recently been invalidated and are thought to live
  // on the machine in question.
  // Mapping: StorageID -> ArrayList<Block>
  //
  Map<String, Collection<Block>> recentInvalidateSets =
    new TreeMap<String, Collection<Block>>();

  //
  // Keeps a TreeSet for every named node. Each treeset contains
  // a list of the blocks that are "extra" at that location. We'll
  // eventually remove these extras.
  // Mapping: StorageID -> TreeSet<Block>
  //
  Map<String, Collection<Block>> excessReplicateMap =
    new TreeMap<String, Collection<Block>>();

  //
  // Store set of Blocks that need to be replicated 1 or more times.
  // We also store pending replication-orders.
  //
  UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();
  private PendingReplicationBlocks pendingReplications;

  //  The maximum number of replicas allowed for a block
  int maxReplication;
  //  How many outgoing replication streams a given node should have at one time
  int maxReplicationStreams;
  // Minimum copies needed or else write is disallowed
  int minReplication;
  // Default number of replicas
  int defaultReplication;

  /**
   * Last block index used for replication work.
   */
  private int replIndex = 0;
  private long missingBlocksInCurIter = 0;
  private long missingBlocksInPrevIter = 0;
  Random r = new Random();

  // for block replicas placement
  ReplicationTargetChooser replicator;

  BlockManager(FSNamesystem fsn, Configuration conf) throws IOException {
    namesystem = fsn;
    pendingReplications = new PendingReplicationBlocks(
        conf.getInt("dfs.replication.pending.timeout.sec",
                    -1) * 1000L);
    setConfigurationParameters(conf);
  }

  void setConfigurationParameters(Configuration conf) throws IOException {
    this.replicator = new ReplicationTargetChooser(
                         conf.getBoolean("dfs.replication.considerLoad", true),
                         namesystem,
                         namesystem.clusterMap);

    this.defaultReplication = conf.getInt("dfs.replication", 3);
    this.maxReplication = conf.getInt("dfs.replication.max", 512);
    this.minReplication = conf.getInt("dfs.replication.min", 1);
    if (minReplication <= 0)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.min = "
                            + minReplication
                            + " must be greater than 0");
    if (maxReplication >= (int)Short.MAX_VALUE)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.max = "
                            + maxReplication + " must be less than " + (Short.MAX_VALUE));
    if (maxReplication < minReplication)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.min = "
                            + minReplication
                            + " must be less than dfs.replication.max = "
                            + maxReplication);
    this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
    FSNamesystem.LOG.info("defaultReplication = " + defaultReplication);
    FSNamesystem.LOG.info("maxReplication = " + maxReplication);
    FSNamesystem.LOG.info("minReplication = " + minReplication);
    FSNamesystem.LOG.info("maxReplicationStreams = " + maxReplicationStreams);
  }

  void activate() {
    pendingReplications.start();
  }

  void close() {
    if (pendingReplications != null) pendingReplications.stop();
  }

  void metaSave(PrintWriter out) {
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
  boolean checkMinReplication(Block block) {
    return (blocksMap.numNodes(block) >= minReplication);
  }

  /**
   * Get all valid locations of the block
   */
  ArrayList<String> addBlock(Block block) {
    ArrayList<String> machineSet =
      new ArrayList<String>(blocksMap.numNodes(block));
    for(Iterator<DatanodeDescriptor> it =
      blocksMap.nodeIterator(block); it.hasNext();) {
      String storageID = it.next().getStorageID();
      // filter invalidate replicas
      Collection<Block> blocks = recentInvalidateSets.get(storageID);
      if(blocks==null || !blocks.contains(block)) {
        machineSet.add(storageID);
      }
    }
    return machineSet;
  }


  List<LocatedBlock> getBlockLocations(Block[] blocks, long offset,
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
      return null;

    long endOff = offset + length;
    List<LocatedBlock> results = new ArrayList<LocatedBlock>(blocks.length);
    do {
      // get block locations
      int numNodes = blocksMap.numNodes(blocks[curBlk]);
      int numCorruptNodes = countNodes(blocks[curBlk]).corruptReplicas();
      int numCorruptReplicas = corruptReplicas
          .numCorruptReplicas(blocks[curBlk]);
      if (numCorruptNodes != numCorruptReplicas) {
        FSNamesystem.LOG.warn("Inconsistent number of corrupt replicas for "
            + blocks[curBlk] + "blockMap has " + numCorruptNodes
            + " but corrupt replicas map has " + numCorruptReplicas);
      }
      boolean blockCorrupt = (numCorruptNodes == numNodes);
      int numMachineSet = blockCorrupt ? numNodes :
                          (numNodes - numCorruptNodes);
      DatanodeDescriptor[] machineSet = new DatanodeDescriptor[numMachineSet];
      if (numMachineSet > 0) {
        numNodes = 0;
        for (Iterator<DatanodeDescriptor> it = 
             blocksMap.nodeIterator(blocks[curBlk]); it.hasNext();) {
          DatanodeDescriptor dn = it.next();
          boolean replicaCorrupt = 
            corruptReplicas.isReplicaCorrupt(blocks[curBlk], dn);
          if (blockCorrupt || (!blockCorrupt && !replicaCorrupt))
            machineSet[numNodes++] = dn;
        }
      }
      LocatedBlock b = new LocatedBlock(blocks[curBlk], machineSet, curPos,
          blockCorrupt);
      if (namesystem.isAccessTokenEnabled) {
        b.setAccessToken(namesystem.accessTokenHandler.generateToken(b.getBlock()
            .getBlockId(), EnumSet.of(AccessTokenHandler.AccessMode.READ)));
      }
      results.add(b);
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff 
          && curBlk < blocks.length
          && results.size() < nrBlocksToReturn);
    return results;
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration.
   */
   void verifyReplication(String src,
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

  void removeFromInvalidates(String datanodeId, Block block) {
    Collection<Block> v = recentInvalidateSets.get(datanodeId);
    if (v != null && v.remove(block) && v.isEmpty()) {
      recentInvalidateSets.remove(datanodeId);
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the move
   *
   * @param b block
   * @param dn datanode
   */
  void addToInvalidates(Block b, DatanodeInfo dn) {
    Collection<Block> invalidateSet = recentInvalidateSets
        .get(dn.getStorageID());
    if (invalidateSet == null) {
      invalidateSet = new HashSet<Block>();
      recentInvalidateSets.put(dn.getStorageID(), invalidateSet);
    }
    if (invalidateSet.add(b)) {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.addToInvalidates: "
          + b.getBlockName() + " is added to invalidSet of " + dn.getName());
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on all its
   * datanodes.
   */
  private void addToInvalidates(Block b) {
    for (Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(b); it
        .hasNext();) {
      DatanodeDescriptor node = it.next();
      addToInvalidates(b, node);
    }
  }

  /**
   * dumps the contents of recentInvalidateSets
   */
  private void dumpRecentInvalidateSets(PrintWriter out) {
    int size = recentInvalidateSets.values().size();
    out.println("Metasave: Blocks waiting deletion from "+size+" datanodes.");
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

  void markBlockAsCorrupt(Block blk, DatanodeInfo dn) throws IOException {
    DatanodeDescriptor node = namesystem.getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot mark block" + blk.getBlockName() +
                            " as corrupt because datanode " + dn.getName() +
                            " does not exist. ");
    }

    final BlockInfo storedBlockInfo = blocksMap.getStoredBlock(blk);
    if (storedBlockInfo == null) {
      // Check if the replica is in the blockMap, if not
      // ignore the request for now. This could happen when BlockScanner
      // thread of Datanode reports bad block before Block reports are sent
      // by the Datanode on startup
      NameNode.stateChangeLog.info("BLOCK NameSystem.markBlockAsCorrupt: " +
                                   "block " + blk + " could not be marked " +
                                   "as corrupt as it does not exists in " +
                                   "blocksMap");
    } else {
      INodeFile inode = storedBlockInfo.getINode();
      if (inode == null) {
        NameNode.stateChangeLog.info("BLOCK NameSystem.markBlockAsCorrupt: " +
                                     "block " + blk + " could not be marked " +
                                     "as corrupt as it does not belong to " +
                                     "any file");
        addToInvalidates(storedBlockInfo, node);
        return;
      } 
      // Add this replica to corruptReplicas Map
      corruptReplicas.addToCorruptReplicasMap(storedBlockInfo, node);
      if (countNodes(storedBlockInfo).liveReplicas() > inode.getReplication()) {
        // the block is over-replicated so invalidate the replicas immediately
        invalidateBlock(storedBlockInfo, node);
      } else {
        // add the block to neededReplication
        updateNeededReplications(storedBlockInfo, -1, 0);
      }
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
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.invalidateBlocks: "
                                   + blk + " on "
                                   + dn.getName() + " listed for deletion.");
    } else {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.invalidateBlocks: "
          + blk + " on " + dn.getName()
          + " is the only copy and was not deleted.");
    }
  }

  void updateState() {
    pendingReplicationBlocksCount = pendingReplications.size();
    underReplicatedBlocksCount = neededReplications.size();
    corruptReplicaBlocksCount = corruptReplicas.size();
  }

  /**
   * Schedule blocks for deletion at datanodes
   * @param nodesToProcess number of datanodes to schedule deletion work
   * @return total number of block for deletion
   */
  int computeInvalidateWork(int nodesToProcess) {
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
  int computeReplicationWork(int blocksToProcess) throws IOException {
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
    synchronized (namesystem) {
      for (int i = 0; i < UnderReplicatedBlocks.LEVEL; i++) {
        blocksToReplicate.add(new ArrayList<Block>());
      }

      synchronized (neededReplications) {
        if (neededReplications.size() == 0) {
          missingBlocksInCurIter = 0;
          missingBlocksInPrevIter = 0;
          return blocksToReplicate;
        }

        // Go through all blocks that need replications.
        BlockIterator neededReplicationsIterator = neededReplications
            .iterator();
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
            missingBlocksInPrevIter = missingBlocksInCurIter;
            missingBlocksInCurIter = 0;
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
    } // end synchronized namesystem

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

    synchronized (namesystem) {
      synchronized (neededReplications) {
        // block should belong to a file
        INodeFile fileINode = blocksMap.getINode(block);
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
        if ((numReplicas.liveReplicas() + numReplicas.decommissionedReplicas())
            <= 0) {
          missingBlocksInCurIter++;
        }
        if(srcNode == null) // block can not be replicated from any node
          return false;

        // do not schedule more if enough replicas is already pending
        numEffectiveReplicas = numReplicas.liveReplicas() +
                                pendingReplications.getNumReplicas(block);
        if(numEffectiveReplicas >= requiredReplication) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
          NameNode.stateChangeLog.info("BLOCK* "
              + "Removing block " + block
              + " from neededReplications as it has enough replicas.");
          return false;
        }
      }
    }

    // choose replication targets: NOT HOLDING THE GLOBAL LOCK
    DatanodeDescriptor targets[] = replicator.chooseTarget(
        requiredReplication - numEffectiveReplicas,
        srcNode, containingNodes, null, block.getNumBytes());
    if(targets.length == 0)
      return false;

    synchronized (namesystem) {
      synchronized (neededReplications) {
        // Recheck since global lock was released
        // block should belong to a file
        INodeFile fileINode = blocksMap.getINode(block);
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
        if(numEffectiveReplicas >= requiredReplication) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
          NameNode.stateChangeLog.info("BLOCK* "
              + "Removing block " + block
              + " from neededReplications as it has enough replicas.");
          return false;
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
        NameNode.stateChangeLog.debug(
            "BLOCK* block " + block
            + " is moved from neededReplications to pendingReplications");

        // remove from neededReplications
        if(numEffectiveReplicas + targets.length >= requiredReplication) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
        }
        if (NameNode.stateChangeLog.isInfoEnabled()) {
          StringBuffer targetList = new StringBuffer("datanode(s)");
          for (int k = 0; k < targets.length; k++) {
            targetList.append(' ');
            targetList.append(targets[k].getName());
          }
          NameNode.stateChangeLog.info(
                    "BLOCK* ask "
                    + srcNode.getName() + " to replicate "
                    + block + " to " + targetList);
          NameNode.stateChangeLog.debug(
                    "BLOCK* neededReplications = " + neededReplications.size()
                    + " pendingReplications = " + pendingReplications.size());
        }
      }
    }

    return true;
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
  void processPendingReplications() {
    Block[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      synchronized (namesystem) {
        for (int i = 0; i < timedOutItems.length; i++) {
          NumberReplicas num = countNodes(timedOutItems[i]);
          neededReplications.add(timedOutItems[i],
                                 num.liveReplicas(),
                                 num.decommissionedReplicas(),
                                 getReplication(timedOutItems[i]));
        }
      }
      /* If we know the target datanodes where the replication timedout,
       * we could invoke decBlocksScheduled() on it. Its ok for now.
       */
    }
  }

  /**
   * The given node is reporting all its blocks.  Use this info to
   * update the (machine-->blocklist) and (block-->machinelist) tables.
   */
  public void processReport(DatanodeDescriptor node,
                            BlockListAsLongs report) throws IOException {
    //
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    Collection<Block> toAdd = new LinkedList<Block>();
    Collection<Block> toRemove = new LinkedList<Block>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    node.reportDiff(blocksMap, report, toAdd, toRemove, toInvalidate);

    for (Block b : toRemove) {
      removeStoredBlock(b, node);
    }
    for (Block b : toAdd) {
      addStoredBlock(b, node, null);
    }
    for (Block b : toInvalidate) {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: block "
          + b + " on " + node.getName() + " size " + b.getNumBytes()
          + " does not belong to any file.");
      addToInvalidates(b, node);
    }
  }

  /**
   * Modify (block-->datanode) map. Remove block from set of
   * needed replications if this takes care of the problem.
   * @return the block that is stored in blockMap.
   */
  private Block addStoredBlock(Block block, DatanodeDescriptor node,
      DatanodeDescriptor delNodeHint) {
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
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

    // add block to the data-node
    boolean added = node.addBlock(storedBlock);

    assert storedBlock != null : "Block must be stored by now";

    if (block != storedBlock) {
      if (block.getNumBytes() >= 0) {
        long cursize = storedBlock.getNumBytes();
        if (cursize == 0) {
          storedBlock.setNumBytes(block.getNumBytes());
        } else if (cursize != block.getNumBytes()) {
          FSNamesystem.LOG.warn("Inconsistent size for block " + block +
                   " reported from " + node.getName() +
                   " current size is " + cursize +
                   " reported size is " + block.getNumBytes());
          try {
            if (cursize > block.getNumBytes()) {
              // new replica is smaller in size than existing block.
              // Mark the new replica as corrupt.
              FSNamesystem.LOG.warn("Mark new replica "
                  + block + " from " + node.getName() + " as corrupt "
                  + "because length is shorter than existing ones");
              markBlockAsCorrupt(block, node);
            } else {
              // new replica is larger in size than existing block.
              // Mark pre-existing replicas as corrupt.
              int numNodes = blocksMap.numNodes(block);
              int count = 0;
              DatanodeDescriptor nodes[] = new DatanodeDescriptor[numNodes];
              Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
              for (; it != null && it.hasNext(); ) {
                DatanodeDescriptor dd = it.next();
                if (!dd.equals(node)) {
                  nodes[count++] = dd;
                }
              }
              for (int j = 0; j < count; j++) {
                FSNamesystem.LOG.warn("Mark existing replica "
                        + block + " from " + node.getName() + " as corrupt "
                        + "because its length is shorter than the new one");
                markBlockAsCorrupt(block, nodes[j]);
              }
              //
              // change the size of block in blocksMap
              //
              storedBlock = blocksMap.getStoredBlock(block); // extra look up!
              if (storedBlock == null) {
                FSNamesystem.LOG.warn("Block " + block + " reported from "
                    + node.getName()
                    + " does not exist in blockMap. Surprise! Surprise!");
              } else {
                storedBlock.setNumBytes(block.getNumBytes());
              }
            }
          } catch (IOException e) {
            FSNamesystem.LOG.warn("Error in deleting bad block " + block + e);
          }
        }

        // Updated space consumed if required.
        INodeFile file = (storedBlock != null) ? storedBlock.getINode() : null;
        long diff = (file == null) ? 0 :
                    (file.getPreferredBlockSize() - storedBlock.getNumBytes());
        
        if (diff > 0 && file.isUnderConstruction() &&
            cursize < storedBlock.getNumBytes()) {
          try {
            String path = /* For finding parents */
            namesystem.leaseManager.findPath((INodeFileUnderConstruction) file);
            namesystem.dir.updateSpaceConsumed(path, 0, -diff
                * file.getReplication());
          } catch (IOException e) {
            FSNamesystem.LOG
                .warn("Unexpected exception while updating disk space : "
                    + e.getMessage());
          }
        }
      }
      block = storedBlock;
    }
    assert storedBlock == block : "Block must be stored by now";

    int curReplicaDelta = 0;

    if (added) {
      curReplicaDelta = 1;
      //
      // At startup time, because too many new blocks come in
      // they take up lots of space in the log file.
      // So, we log only when namenode is out of safemode.
      //
      if (!namesystem.isInSafeMode()) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
            + "blockMap updated: " + node.getName() + " is added to " + block
            + " size " + block.getNumBytes());
      }
    } else {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.addStoredBlock: "
          + "Redundant addStoredBlock request received for " + block + " on "
          + node.getName() + " size " + block.getNumBytes());
    }

    // filter out containingNodes that are marked for decommission.
    NumberReplicas num = countNodes(storedBlock);
    int numLiveReplicas = num.liveReplicas();
    int numCurrentReplica = numLiveReplicas
      + pendingReplications.getNumReplicas(block);

    // check whether safe replication is reached for the block
    namesystem.incrementSafeBlockCount(numCurrentReplica);

    //
    // if file is being actively written to, then do not check
    // replication-factor here. It will be checked when the file is closed.
    //
    INodeFile fileINode = null;
    fileINode = storedBlock.getINode();
    if (fileINode.isUnderConstruction()) {
      return block;
    }

    // do not handle mis-replicated blocks during startup
    if (namesystem.isInSafeMode())
      return block;

    // handle underReplication/overReplication
    short fileReplication = fileINode.getReplication();
    if (numCurrentReplica >= fileReplication) {
      neededReplications.remove(block, numCurrentReplica,
          num.decommissionedReplicas, fileReplication);
    } else {
      updateNeededReplications(block, curReplicaDelta, 0);
    }
    if (numCurrentReplica > fileReplication) {
      processOverReplicatedBlock(block, fileReplication, node, delNodeHint);
    }
    // If the file replication has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(block);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      FSNamesystem.LOG.warn("Inconsistent number of corrupt replicas for " +
          block + "blockMap has " + numCorruptNodes + 
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication))
      invalidateCorruptReplicas(block);
    return block;
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
    for (Iterator<DatanodeDescriptor> it = nodes.iterator(); it.hasNext(); ) {
      DatanodeDescriptor node = it.next();
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
  void processMisReplicatedBlocks() {
    long nrInvalid = 0, nrOverReplicated = 0, nrUnderReplicated = 0;
    synchronized (namesystem) {
      neededReplications.clear();
      for (BlocksMap.BlockInfo block : blocksMap.getBlocks()) {
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
        if (neededReplications.add(block, numCurrentReplica, num
            .decommissionedReplicas(), expectedReplication)) {
          nrUnderReplicated++;
        }

        if (numCurrentReplica > expectedReplication) {
          // over-replicated block
          nrOverReplicated++;
          processOverReplicatedBlock(block, expectedReplication, null, null);
        }
      }
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
  void processOverReplicatedBlock(Block block, short replication,
      DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {
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
        addedNode, delNodeHint);
  }

  void addToExcessReplicate(DatanodeInfo dn, Block block) {
    Collection<Block> excessBlocks = excessReplicateMap.get(dn.getStorageID());
    if (excessBlocks == null) {
      excessBlocks = new TreeSet<Block>();
      excessReplicateMap.put(dn.getStorageID(), excessBlocks);
    }
    excessBlocks.add(block);
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates: "
        + "(" + dn.getName() + ", " + block
        + ") is added to excessReplicateMap");
  }

  /**
   * Modify (block-->datanode) map. Possibly generate replication tasks, if the
   * removed block is still valid.
   */
  void removeStoredBlock(Block block, DatanodeDescriptor node) {
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
        + block + " from " + node.getName());
    synchronized (namesystem) {
      if (!blocksMap.removeNode(block, node)) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
            + block + " has already been removed from node " + node);
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
        excessBlocks.remove(block);
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
            + block + " is removed from excessBlocks");
        if (excessBlocks.size() == 0) {
          excessReplicateMap.remove(node.getStorageID());
        }
      }

      // Remove the replica from corruptReplicas
      corruptReplicas.removeFromCorruptReplicasMap(block, node);
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  void addBlock(DatanodeDescriptor node, Block block, String delHint)
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
    addStoredBlock(block, node, delHintNode);
  }

  /**
   * Return the number of nodes that are live and decommissioned.
   */
  NumberReplicas countNodes(Block b) {
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
   * Return true if there are any blocks on this node that have not
   * yet reached their replication factor. Otherwise returns false.
   */
  boolean isReplicationInProgress(DatanodeDescriptor srcNode) {
    boolean status = false;
    for(final Iterator<Block> i = srcNode.getBlockIterator(); i.hasNext(); ) {
      final Block block = i.next();
      INode fileINode = blocksMap.getINode(block);

      if (fileINode != null) {
        NumberReplicas num = countNodes(block);
        int curReplicas = num.liveReplicas();
        int curExpectedReplicas = getReplication(block);
        if (curExpectedReplicas > curReplicas) {
          status = true;
          if (!neededReplications.contains(block) &&
            pendingReplications.getNumReplicas(block) == 0) {
            //
            // These blocks have been reported from the datanode
            // after the startDecommission method has been executed. These
            // blocks were in flight when the decommission was started.
            //
            neededReplications.add(block,
                                   curReplicas,
                                   num.decommissionedReplicas(),
                                   curExpectedReplicas);
          }
        }
      }
    }
    return status;
  }

  int getActiveBlockCount() {
    int activeBlocks = blocksMap.size();
    for(Iterator<Collection<Block>> it =
          recentInvalidateSets.values().iterator(); it.hasNext();) {
      activeBlocks -= it.next().size();
    }
    return activeBlocks;
  }

  DatanodeDescriptor[] getNodes(Block block) {
    DatanodeDescriptor[] nodes =
      new DatanodeDescriptor[blocksMap.numNodes(block)];
    Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
    for (int i = 0; it != null && it.hasNext(); i++) {
      nodes[i] = it.next();
    }
    return nodes;
  }

  int getTotalBlocks() {
    return blocksMap.size();
  }

  void removeBlock(Block block) {
    blocksMap.removeINode(block);
    corruptReplicas.removeFromCorruptReplicasMap(block);
    addToInvalidates(block);
  }

  BlockInfo getStoredBlock(Block block) {
    return blocksMap.getStoredBlock(block);
  }

  /* updates a block in under replication queue */
  void updateNeededReplications(Block block, int curReplicasDelta,
      int expectedReplicasDelta) {
    synchronized (namesystem) {
      NumberReplicas repl = countNodes(block);
      int curExpectedReplicas = getReplication(block);
      neededReplications.update(block, repl.liveReplicas(), repl
          .decommissionedReplicas(), curExpectedReplicas, curReplicasDelta,
          expectedReplicasDelta);
    }
  }

  void checkReplication(Block block, int numExpectedReplicas) {
    // filter out containingNodes that are marked for decommission.
    NumberReplicas number = countNodes(block);
    if (number.liveReplicas() < numExpectedReplicas) {
      neededReplications.add(block,
                             number.liveReplicas(),
                             number.decommissionedReplicas,
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

  /**
   * Remove a datanode from the invalidatesSet
   * @param n datanode
   */
  void removeFromInvalidates(DatanodeInfo n) {
    recentInvalidateSets.remove(n.getStorageID());
  }

  /**
   * Get blocks to invalidate for <i>nodeId</i>
   * in {@link #recentInvalidateSets}.
   *
   * @return number of blocks scheduled for removal during this iteration.
   */
  private int invalidateWorkForOneNode(String nodeId) {
    synchronized (namesystem) {
      // blocks should not be replicated or removed if safe mode is on
      if (namesystem.isInSafeMode())
        return 0;
      // get blocks to invalidate for the nodeId
      assert nodeId != null;
      DatanodeDescriptor dn = namesystem.getDatanode(nodeId);
      if (dn == null) {
        recentInvalidateSets.remove(nodeId);
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
      if (!it.hasNext())
        recentInvalidateSets.remove(nodeId);

      dn.addBlocksToBeInvalidated(blocksToInvalidate);

      if (NameNode.stateChangeLog.isInfoEnabled()) {
        StringBuffer blockList = new StringBuffer();
        for (Block blk : blocksToInvalidate) {
          blockList.append(' ');
          blockList.append(blk);
        }
        NameNode.stateChangeLog.info("BLOCK* ask " + dn.getName()
            + " to delete " + blockList);
      }
      return blocksToInvalidate.size();
    }
  }

  long getMissingBlocksCount() {
    // not locking
    return Math.max(missingBlocksInPrevIter, missingBlocksInCurIter);
  }

  BlockInfo addINode(Block block, INodeFile iNode) {
    return blocksMap.addINode(block, iNode);
  }

  void removeINode(Block block) {
    blocksMap.removeINode(block);
  }

  INodeFile getINode(Block b) {
    return blocksMap.getINode(b);
  }

  void removeFromCorruptReplicasMap(Block block) {
    corruptReplicas.removeFromCorruptReplicasMap(block);
  }

  int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }

  void removeBlockFromMap(BlockInfo blockInfo) {
    blocksMap.removeBlock(blockInfo);
  }
}
