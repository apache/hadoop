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
package org.apache.hadoop.dfs;

import org.apache.hadoop.ipc.Server;

import java.util.*;
import java.io.IOException;

/**
 * Stores information about all corrupt blocks in the File System.
 * A Block is considered corrupt only if all of its replicas are
 * corrupt. While reporting replicas of a Block, we hide any corrupt
 * copies. These copies are removed once Block is found to have 
 * expected number of good replicas.
 * Mapping: Block -> TreeSet<DatanodeDescriptor> 
 */

class CorruptReplicasMap{

  private Map<Block, Collection<DatanodeDescriptor>> corruptReplicasMap =
    new TreeMap<Block, Collection<DatanodeDescriptor>>();
  
  /**
   * Mark the block belonging to datanode as corrupt.
   *
   * @param blk Block to be added to CorruptReplicasMap
   * @param dn DatanodeDescriptor which holds the corrupt replica
   */
  public void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    if (nodes == null) {
      nodes = new TreeSet<DatanodeDescriptor>();
      corruptReplicasMap.put(blk, nodes);
    }
    if (!nodes.contains(dn)) {
      nodes.add(dn);
      NameNode.stateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   blk.getBlockName() +
                                   " added as corrupt on " + dn.getName() +
                                   " by " + Server.getRemoteIp());
    } else {
      NameNode.stateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   "duplicate requested for " + 
                                   blk.getBlockName() + " to add as corrupt " +
                                   "on " + dn.getName() +
                                   " by " + Server.getRemoteIp());
    }
    NameNode.getNameNodeMetrics().numBlocksCorrupted.set(
        corruptReplicasMap.size());
  }

  /**
   * Remove Block from CorruptBlocksMap
   *
   * @param blk Block to be removed
   */
  void removeFromCorruptReplicasMap(Block blk) {
    FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
    if (fsNamesystem.blocksMap.contains(blk))
      return;
    if (corruptReplicasMap != null) {
      corruptReplicasMap.remove(blk);
      NameNode.getNameNodeMetrics().numBlocksCorrupted.set(
          corruptReplicasMap.size());
    }
  }

  /**
   * Get Nodes which have corrupt replicas of Block
   * 
   * @param blk Block for which nodes are requested
   * @return collection of nodes. Null if does not exists
   */
  Collection<DatanodeDescriptor> getNodes(Block blk) {
    return corruptReplicasMap.get(blk);
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

  /**
   * Invalidate corrupt replicas
   *
   * @param blk Block whose corrupt replicas need to be invalidated
   */
  void invalidateCorruptReplicas(Block blk) {
    FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    boolean gotException = false;
    if (nodes == null)
      return;
    for (Iterator<DatanodeDescriptor> it = nodes.iterator(); it.hasNext(); ) {
      DatanodeDescriptor node = it.next();
      try {
        fsNamesystem.invalidateBlock(blk, node);
      } catch (IOException e) {
        NameNode.stateChangeLog.info("NameNode.invalidateCorruptReplicas " +
                                      "error in deleting bad block " + blk +
                                      " on " + node + e);
      }
    }
    // Remove the block from corruptReplicasMap if empty
    if (!gotException)
      removeFromCorruptReplicasMap(blk);
  }
}
