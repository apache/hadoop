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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerSafeMode.BMSafeModeStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.util.Daemon;
import org.junit.Assert;

import com.google.common.base.Preconditions;
import org.mockito.internal.util.reflection.Whitebox;

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
      final BlockInfo storedBlock = bm.getStoredBlock(b);
      return new int[]{getNumberOfRacks(bm, b),
          bm.countNodes(storedBlock).liveReplicas(),
          bm.neededReconstruction.contains(storedBlock) ? 1 : 0};
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
    for(DatanodeStorageInfo storage : blockManager.blocksMap.getStorages(b)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
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
   * @return replication monitor thread instance from block manager.
   */
  public static Daemon getReplicationThread(final BlockManager blockManager)
  {
    return blockManager.replicationThread;
  }
  
  /**
   * Stop the replication monitor thread
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
   * @return corruptReplicas from block manager
   */
  public static  CorruptReplicasMap getCorruptReplicas(final BlockManager blockManager){
    return blockManager.corruptReplicas;
    
  }

  /**
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
   * {@link DFSConfigKeys#DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY} to
   * a high value to ensure that all work is calculated.
   */
  public static int computeAllPendingWork(BlockManager bm) {
    int work = computeInvalidationWork(bm);
    work += bm.computeBlockReconstructionWork(Integer.MAX_VALUE);
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
        DFSTestUtil.setDatanodeDead(theDND);
        hbm.heartbeatCheck();
      }
    } finally {
      namesystem.writeUnlock();
    }
  }
  
  /**
   * Change whether the block placement policy will prefer the writer's
   * local Datanode or not.
   * @param prefer if true, prefer local node
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

  /**
   * Call heartbeat check function of HeartbeatManager and get
   * under replicated blocks count within write lock to make sure
   * computeDatanodeWork doesn't interfere.
   * @param namesystem the FSNamesystem
   * @param bm the BlockManager to manipulate
   * @return the number of under replicated blocks
   */
  public static int checkHeartbeatAndGetUnderReplicatedBlocksCount(
      FSNamesystem namesystem, BlockManager bm) {
    namesystem.writeLock();
    try {
      bm.getDatanodeManager().getHeartbeatManager().heartbeatCheck();
      return bm.getUnderReplicatedNotMissingBlocks();
    } finally {
      namesystem.writeUnlock();
    }
  }

  public static DatanodeStorageInfo updateStorage(DatanodeDescriptor dn,
      DatanodeStorage s) {
    return dn.updateStorage(s);
  }

  /**
   * Call heartbeat check function of HeartbeatManager
   * @param bm the BlockManager to manipulate
   */
  public static void rescanPostponedMisreplicatedBlocks(BlockManager bm) {
    bm.rescanPostponedMisreplicatedBlocks();
  }

  public static DatanodeDescriptor getLocalDatanodeDescriptor(
      boolean initializeStorage) {
    DatanodeDescriptor dn = new DatanodeDescriptor(DFSTestUtil.getLocalDatanodeID());
    if (initializeStorage) {
      dn.updateStorage(new DatanodeStorage(DatanodeStorage.generateUuid()));
    }
    return dn;
  }
  
  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, boolean initializeStorage) {
    return getDatanodeDescriptor(ipAddr, rackLocation,
        initializeStorage? new DatanodeStorage(DatanodeStorage.generateUuid()): null);
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, DatanodeStorage storage) {
    return getDatanodeDescriptor(ipAddr, rackLocation, storage, "host");
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, DatanodeStorage storage, String hostname) {
      DatanodeDescriptor dn = DFSTestUtil.getDatanodeDescriptor(ipAddr,
          DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT, rackLocation, hostname);
      if (storage != null) {
        dn.updateStorage(storage);
      }
      return dn;
  }

  public static DatanodeStorageInfo newDatanodeStorageInfo(
      DatanodeDescriptor dn, DatanodeStorage s) {
    return new DatanodeStorageInfo(dn, s);
  }

  public static StorageReport[] getStorageReportsForDatanode(
      DatanodeDescriptor dnd) {
    ArrayList<StorageReport> reports = new ArrayList<StorageReport>();
    for (DatanodeStorageInfo storage : dnd.getStorageInfos()) {
      DatanodeStorage dns = new DatanodeStorage(
          storage.getStorageID(), storage.getState(), storage.getStorageType());
      StorageReport report = new StorageReport(
          dns ,false, storage.getCapacity(),
          storage.getDfsUsed(), storage.getRemaining(),
          storage.getBlockPoolUsed());
      reports.add(report);
    }
    return reports.toArray(StorageReport.EMPTY_ARRAY);
  }

  /**
   * Have DatanodeManager check decommission state.
   * @param dm the DatanodeManager to manipulate
   */
  public static void recheckDecommissionState(DatanodeManager dm)
      throws ExecutionException, InterruptedException {
    dm.getDecomManager().runMonitorForTest();
  }

  /**
   * add block to the replicateBlocks queue of the Datanode
   */
  public static void addBlockToBeReplicated(DatanodeDescriptor node,
      Block block, DatanodeStorageInfo[] targets) {
    node.addBlockToBeReplicated(block, targets);
  }

  public static void setStartupSafeModeForTest(BlockManager bm) {
    BlockManagerSafeMode bmSafeMode = (BlockManagerSafeMode)Whitebox
        .getInternalState(bm, "bmSafeMode");
    Whitebox.setInternalState(bmSafeMode, "extension", Integer.MAX_VALUE);
    Whitebox.setInternalState(bmSafeMode, "status", BMSafeModeStatus.EXTENSION);
  }

  /**
   * Check if a given Datanode (specified by uuid) is removed. Removed means the
   * Datanode is no longer present in HeartbeatManager and NetworkTopology.
   * @param nn Namenode
   * @param dnUuid Datanode UUID
   * @return true if datanode is removed.
   */
  public static boolean isDatanodeRemoved(NameNode nn, String dnUuid){
      final DatanodeManager dnm =
          nn.getNamesystem().getBlockManager().getDatanodeManager();
      return !dnm.getNetworkTopology().contains(dnm.getDatanode(dnUuid));
  }
}
