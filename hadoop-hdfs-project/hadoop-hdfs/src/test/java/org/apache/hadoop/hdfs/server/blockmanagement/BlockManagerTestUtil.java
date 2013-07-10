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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.Daemon;
import org.junit.Assert;

import com.google.common.base.Preconditions;

public class BlockManagerTestUtil {
  public static void setNodeReplicationLimit(final BlockManager blockManager,
      final int limit) {
    blockManager.maxReplicationStreams = limit;
  }

  /** @return the datanode descriptor for the given the given storageID. */
  public static DatanodeDescriptor getDatanode(final FSNamesystem ns,
      final String storageID) {
    ns.readLock();
    try {
      return ns.getBlockManager().getDatanodeManager().getDatanode(storageID);
    } finally {
      ns.readUnlock();
    }
  }


  /**
   * Refresh block queue counts on the name-node.
   */
  public static void updateState(final BlockManager blockManager) {
    blockManager.updateState();
  }

  /**
   * @return a tuple of the replica state (number racks, number live
   * replicas, and number needed replicas) for the given block.
   */
  public static int[] getReplicaInfo(final FSNamesystem namesystem, final Block b) {
    final BlockManager bm = namesystem.getBlockManager();
    namesystem.readLock();
    try {
      return new int[]{getNumberOfRacks(bm, b),
          bm.countNodes(b).liveReplicas(),
          bm.neededReplications.contains(b) ? 1 : 0};
    } finally {
      namesystem.readUnlock();
    }
  }

  /**
   * @return the number of racks over which a given block is replicated
   * decommissioning/decommissioned nodes are not counted. corrupt replicas 
   * are also ignored
   */
  private static int getNumberOfRacks(final BlockManager blockManager,
      final Block b) {
    final Set<String> rackSet = new HashSet<String>(0);
    final Collection<DatanodeDescriptor> corruptNodes = 
       getCorruptReplicas(blockManager).getNodes(b);
    for (Iterator<DatanodeDescriptor> it = blockManager.blocksMap.nodeIterator(b); 
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

  /**
   * @param blockManager
   * @return replication monitor thread instance from block manager.
   */
  public static Daemon getReplicationThread(final BlockManager blockManager)
  {
    return blockManager.replicationThread;
  }
  
  /**
   * Stop the replication monitor thread
   * @param blockManager
   */
  public static void stopReplicationThread(final BlockManager blockManager) 
      throws IOException {
    blockManager.enableRMTerminationForTesting();
    blockManager.replicationThread.interrupt();
    try {
      blockManager.replicationThread.join();
    } catch(InterruptedException ie) {
      throw new IOException(
          "Interrupted while trying to stop ReplicationMonitor");
    }
  }

  /**
   * @param blockManager
   * @return corruptReplicas from block manager
   */
  public static  CorruptReplicasMap getCorruptReplicas(final BlockManager blockManager){
    return blockManager.corruptReplicas;
    
  }

  /**
   * @param blockManager
   * @return computed block replication and block invalidation work that can be
   *         scheduled on data-nodes.
   * @throws IOException
   */
  public static int getComputedDatanodeWork(final BlockManager blockManager) throws IOException
  {
    return blockManager.computeDatanodeWork();
  }
  
  public static int computeInvalidationWork(BlockManager bm) {
    return bm.computeInvalidateWork(Integer.MAX_VALUE);
  }
  
  /**
   * Compute all the replication and invalidation work for the
   * given BlockManager.
   * 
   * This differs from the above functions in that it computes
   * replication work for all DNs rather than a particular subset,
   * regardless of invalidation/replication limit configurations.
   * 
   * NB: you may want to set
   * {@link DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY} to
   * a high value to ensure that all work is calculated.
   */
  public static int computeAllPendingWork(BlockManager bm) {
    int work = computeInvalidationWork(bm);
    work += bm.computeReplicationWork(Integer.MAX_VALUE);
    return work;
  }

  /**
   * Ensure that the given NameNode marks the specified DataNode as
   * entirely dead/expired.
   * @param nn the NameNode to manipulate
   * @param dnName the name of the DataNode
   */
  public static void noticeDeadDatanode(NameNode nn, String dnName) {
    FSNamesystem namesystem = nn.getNamesystem();
    namesystem.writeLock();
    try {
      DatanodeManager dnm = namesystem.getBlockManager().getDatanodeManager();
      HeartbeatManager hbm = dnm.getHeartbeatManager();
      DatanodeDescriptor[] dnds = hbm.getDatanodes();
      DatanodeDescriptor theDND = null;
      for (DatanodeDescriptor dnd : dnds) {
        if (dnd.getXferAddr().equals(dnName)) {
          theDND = dnd;
        }
      }
      Assert.assertNotNull("Could not find DN with name: " + dnName, theDND);
      
      synchronized (hbm) {
        theDND.setLastUpdate(0);
        hbm.heartbeatCheck();
      }
    } finally {
      namesystem.writeUnlock();
    }
  }
  
  /**
   * Change whether the block placement policy will prefer the writer's
   * local Datanode or not.
   * @param prefer
   */
  public static void setWritingPrefersLocalNode(
      BlockManager bm, boolean prefer) {
    BlockPlacementPolicy bpp = bm.getBlockPlacementPolicy();
    Preconditions.checkState(bpp instanceof BlockPlacementPolicyDefault,
        "Must use default policy, got %s", bpp.getClass());
    ((BlockPlacementPolicyDefault)bpp).setPreferLocalNode(prefer);
  }
  
  /**
   * Call heartbeat check function of HeartbeatManager
   * @param bm the BlockManager to manipulate
   */
  public static void checkHeartbeat(BlockManager bm) {
    bm.getDatanodeManager().getHeartbeatManager().heartbeatCheck();
  }
}
