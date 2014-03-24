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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

import java.util.*;

/**
 * Stores information about all corrupt blocks in the File System.
 * A Block is considered corrupt only if all of its replicas are
 * corrupt. While reporting replicas of a Block, we hide any corrupt
 * copies. These copies are removed once Block is found to have 
 * expected number of good replicas.
 * Mapping: Block -> TreeSet<DatanodeDescriptor> 
 */

@InterfaceAudience.Private
public class CorruptReplicasMap{

  /** The corruption reason code */
  public static enum Reason {
    NONE,                // not specified.
    ANY,                 // wildcard reason
    GENSTAMP_MISMATCH,   // mismatch in generation stamps
    SIZE_MISMATCH,       // mismatch in sizes
    INVALID_STATE,       // invalid state
    CORRUPTION_REPORTED  // client or datanode reported the corruption
  }

  private final SortedMap<Block, Map<DatanodeDescriptor, Reason>> corruptReplicasMap =
    new TreeMap<Block, Map<DatanodeDescriptor, Reason>>();
  
  /**
   * Mark the block belonging to datanode as corrupt.
   *
   * @param blk Block to be added to CorruptReplicasMap
   * @param dn DatanodeDescriptor which holds the corrupt replica
   * @param reason a textual reason (for logging purposes)
   */
  public void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn,
      String reason) {
    addToCorruptReplicasMap(blk, dn, reason, Reason.NONE);
  }

  /**
   * Mark the block belonging to datanode as corrupt.
   *
   * @param blk Block to be added to CorruptReplicasMap
   * @param dn DatanodeDescriptor which holds the corrupt replica
   * @param reason a textual reason (for logging purposes)
   * @param reasonCode the enum representation of the reason
   */
  public void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn,
      String reason, Reason reasonCode) {
    Map <DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null) {
      nodes = new HashMap<DatanodeDescriptor, Reason>();
      corruptReplicasMap.put(blk, nodes);
    }
    
    String reasonText;
    if (reason != null) {
      reasonText = " because " + reason;
    } else {
      reasonText = "";
    }
    
    if (!nodes.keySet().contains(dn)) {
      NameNode.blockStateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   blk.getBlockName() +
                                   " added as corrupt on " + dn +
                                   " by " + Server.getRemoteIp() +
                                   reasonText);
    } else {
      NameNode.blockStateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   "duplicate requested for " + 
                                   blk.getBlockName() + " to add as corrupt " +
                                   "on " + dn +
                                   " by " + Server.getRemoteIp() +
                                   reasonText);
    }
    // Add the node or update the reason.
    nodes.put(dn, reasonCode);
  }

  /**
   * Remove Block from CorruptBlocksMap
   *
   * @param blk Block to be removed
   */
  void removeFromCorruptReplicasMap(Block blk) {
    if (corruptReplicasMap != null) {
      corruptReplicasMap.remove(blk);
    }
  }

  /**
   * Remove the block at the given datanode from CorruptBlockMap
   * @param blk block to be removed
   * @param datanode datanode where the block is located
   * @return true if the removal is successful; 
             false if the replica is not in the map
   */ 
  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode) {
    return removeFromCorruptReplicasMap(blk, datanode, Reason.ANY);
  }

  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode,
      Reason reason) {
    Map <DatanodeDescriptor, Reason> datanodes = corruptReplicasMap.get(blk);
    boolean removed = false;
    if (datanodes==null)
      return false;

    // if reasons can be compared but don't match, return false.
    Reason storedReason = datanodes.get(datanode);
    if (reason != Reason.ANY && storedReason != null &&
        reason != storedReason) {
      return false;
    }

    if (datanodes.remove(datanode) != null) { // remove the replicas
      if (datanodes.isEmpty()) {
        // remove the block if there is no more corrupted replicas
        corruptReplicasMap.remove(blk);
      }
      return true;
    }
    return false;
  }
    

  /**
   * Get Nodes which have corrupt replicas of Block
   * 
   * @param blk Block for which nodes are requested
   * @return collection of nodes. Null if does not exists
   */
  Collection<DatanodeDescriptor> getNodes(Block blk) {
    Map <DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null)
      return null;
    return nodes.keySet();
  }

  /**
   * Check if replica belonging to Datanode is corrupt
   *
   * @param blk Block to check
   * @param node DatanodeDescriptor which holds the replica
   * @return true if replica is corrupt, false if does not exists in this map
   */
  boolean isReplicaCorrupt(Block blk, DatanodeDescriptor node) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return ((nodes != null) && (nodes.contains(node)));
  }

  public int numCorruptReplicas(Block blk) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return (nodes == null) ? 0 : nodes.size();
  }
  
  public int size() {
    return corruptReplicasMap.size();
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
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    
    Iterator<Block> blockIt = corruptReplicasMap.keySet().iterator();
    
    // if the starting block id was specified, iterate over keys until
    // we find the matching block. If we find a matching block, break
    // to leave the iterator on the next block after the specified block. 
    if (startingBlockId != null) {
      boolean isBlockFound = false;
      while (blockIt.hasNext()) {
        Block b = blockIt.next();
        if (b.getBlockId() == startingBlockId) {
          isBlockFound = true;
          break; 
        }
      }
      
      if (!isBlockFound) {
        return null;
      }
    }

    ArrayList<Long> corruptReplicaBlockIds = new ArrayList<Long>();

    // append up to numExpectedBlocks blockIds to our list
    for(int i=0; i<numExpectedBlocks && blockIt.hasNext(); i++) {
      corruptReplicaBlockIds.add(blockIt.next().getBlockId());
    }
    
    long[] ret = new long[corruptReplicaBlockIds.size()];
    for(int i=0; i<ret.length; i++) {
      ret[i] = corruptReplicaBlockIds.get(i);
    }
    
    return ret;
  }  
}
