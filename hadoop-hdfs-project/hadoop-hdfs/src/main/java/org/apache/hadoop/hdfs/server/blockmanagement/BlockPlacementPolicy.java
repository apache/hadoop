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
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * This interface is used for choosing the desired number of targets
 * for placing block replicas.
 */
@InterfaceAudience.Private
public abstract class BlockPlacementPolicy {
  public static final Logger LOG = LoggerFactory.getLogger(
      BlockPlacementPolicy.class);

  @InterfaceAudience.Private
  public static class NotEnoughReplicasException extends Exception {
    private static final long serialVersionUID = 1L;
    NotEnoughReplicasException(String msg) {
      super(msg);
    }
  }
    
  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i> 
   * to re-replicate a block with size <i>blocksize</i> 
   * If not, return as many as we can.
   *
   * @param srcPath the file to which this chooseTargets is being invoked.
   * @param numOfReplicas additional number of replicas wanted.
   * @param writer the writer's machine, null if not in the cluster.
   * @param chosen datanodes that have been chosen as targets.
   * @param returnChosenNodes decide if the chosenNodes are returned.
   * @param excludedNodes datanodes that should not be considered as targets.
   * @param blocksize size of the data to be written.
   * @param flags Block placement flags.
   * @return array of DatanodeDescriptor instances chosen as target
   * and sorted as a pipeline.
   */
  public abstract DatanodeStorageInfo[] chooseTarget(String srcPath,
                                             int numOfReplicas,
                                             Node writer,
                                             List<DatanodeStorageInfo> chosen,
                                             boolean returnChosenNodes,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             BlockStoragePolicy storagePolicy,
                                             EnumSet<AddBlockFlag> flags);
  
  /**
   * @param favoredNodes datanodes that should be favored as targets. This
   *          is only a hint and due to cluster state, namenode may not be 
   *          able to place the blocks on these datanodes.
   */
  DatanodeStorageInfo[] chooseTarget(String src,
      int numOfReplicas, Node writer,
      Set<Node> excludedNodes,
      long blocksize,
      List<DatanodeDescriptor> favoredNodes,
      BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags) {
    // This class does not provide the functionality of placing
    // a block in favored datanodes. The implementations of this class
    // are expected to provide this functionality

    return chooseTarget(src, numOfReplicas, writer, 
        new ArrayList<DatanodeStorageInfo>(numOfReplicas), false,
        excludedNodes, blocksize, storagePolicy, flags);
  }

  /**
   * @param storageTypes storage types that should be used as targets.
   */
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
      Node writer, List<DatanodeStorageInfo> chosen, boolean returnChosenNodes,
      Set<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags, EnumMap<StorageType, Integer> storageTypes) {
    return chooseTarget(srcPath, numOfReplicas, writer, chosen,
        returnChosenNodes, excludedNodes, blocksize, storagePolicy, flags);
  }

  /**
   * Verify if the block's placement meets requirement of placement policy,
   * i.e. replicas are placed on no less than minRacks racks in the system.
   * 
   * @param locs block with locations
   * @param numOfReplicas replica number of file to be verified
   * @return the result of verification
   */
  public abstract BlockPlacementStatus verifyBlockPlacement(
      DatanodeInfo[] locs, int numOfReplicas);

  /**
   * Select the excess replica storages for deletion based on either
   * delNodehint/Excess storage types.
   *
   * @param availableReplicas
   *          available replicas
   * @param delCandidates
   *          Candidates for deletion. For normal replication, this set is the
   *          same with availableReplicas. For striped blocks, this set is a
   *          subset of availableReplicas.
   * @param expectedNumOfReplicas
   *          The expected number of replicas remaining in the delCandidates
   * @param excessTypes
   *          type of the storagepolicy
   * @param addedNode
   *          New replica reported
   * @param delNodeHint
   *          Hint for excess storage selection
   * @return Returns the list of excess replicas chosen for deletion
   */
  public abstract List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates, int expectedNumOfReplicas,
      List<StorageType> excessTypes, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint);

  /**
   * Used to setup a BlockPlacementPolicy object. This should be defined by 
   * all implementations of a BlockPlacementPolicy.
   * 
   * @param conf the configuration object
   * @param stats retrieve cluster status from here
   * @param clusterMap cluster topology
   */
  protected abstract void initialize(Configuration conf,  FSClusterStats stats,
                                     NetworkTopology clusterMap, 
                                     Host2NodesMap host2datanodeMap);

  /**
   * Check if the move is allowed. Used by balancer and other tools.
   *
   * @param candidates all replicas including source and target
   * @param source source replica of the move
   * @param target target replica of the move
   */
  public abstract boolean isMovable(Collection<DatanodeInfo> candidates,
      DatanodeInfo source, DatanodeInfo target);

  /**
   * Adjust rackmap, moreThanOne, and exactlyOne after removing replica on cur.
   *
   * @param rackMap a map from rack to replica
   * @param moreThanOne The List of replica nodes on rack which has more than 
   *        one replica
   * @param exactlyOne The List of replica nodes on rack with only one replica
   * @param cur current replica to remove
   */
  public void adjustSetsWithChosenReplica(
      final Map<String, List<DatanodeStorageInfo>> rackMap,
      final List<DatanodeStorageInfo> moreThanOne,
      final List<DatanodeStorageInfo> exactlyOne,
      final DatanodeStorageInfo cur) {
    
    final String rack = getRack(cur.getDatanodeDescriptor());
    final List<DatanodeStorageInfo> storages = rackMap.get(rack);
    storages.remove(cur);
    if (storages.isEmpty()) {
      rackMap.remove(rack);
    }
    if (moreThanOne.remove(cur)) {
      if (storages.size() == 1) {
        final DatanodeStorageInfo remaining = storages.get(0);
        if (moreThanOne.remove(remaining)) {
          exactlyOne.add(remaining);
        }
      }
    } else {
      exactlyOne.remove(cur);
    }
  }

  protected <T> DatanodeInfo getDatanodeInfo(T datanode) {
    Preconditions.checkArgument(
        datanode instanceof DatanodeInfo ||
        datanode instanceof DatanodeStorageInfo,
        "class " + datanode.getClass().getName() + " not allowed");
    if (datanode instanceof DatanodeInfo) {
      return ((DatanodeInfo)datanode);
    } else {
      return ((DatanodeStorageInfo)datanode).getDatanodeDescriptor();
    }
  }

  /**
   * Get rack string from a data node
   * @return rack of data node
   */
  protected String getRack(final DatanodeInfo datanode) {
    return datanode.getNetworkLocation();
  }

  /**
   * Split data nodes into two sets, one set includes nodes on rack with
   * more than one  replica, the other set contains the remaining nodes.
   *
   * @param availableSet all the available DataNodes/storages of the block
   * @param candidates DatanodeStorageInfo/DatanodeInfo to be split
   *        into two sets
   * @param rackMap a map from rack to datanodes
   * @param moreThanOne contains nodes on rack with more than one replica
   * @param exactlyOne remains contains the remaining nodes
   */
  public <T> void splitNodesWithRack(
      final Iterable<T> availableSet,
      final Collection<T> candidates,
      final Map<String, List<T>> rackMap,
      final List<T> moreThanOne,
      final List<T> exactlyOne) {
    for(T s: availableSet) {
      final String rackName = getRack(getDatanodeInfo(s));
      List<T> storageList = rackMap.get(rackName);
      if (storageList == null) {
        storageList = new ArrayList<>();
        rackMap.put(rackName, storageList);
      }
      storageList.add(s);
    }
    for (T candidate : candidates) {
      final String rackName = getRack(getDatanodeInfo(candidate));
      if (rackMap.get(rackName).size() == 1) {
        // exactlyOne contains nodes on rack with only one replica
        exactlyOne.add(candidate);
      } else {
        // moreThanOne contains nodes on rack with more than one replica
        moreThanOne.add(candidate);
      }
    }
  }

  /**
   * Updates the value used for excludeSlowNodesEnabled, which is set by
   * {@code DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY}
   * initially.
   *
   * @param enable true, we will filter out slow nodes
   * when choosing targets for blocks, otherwise false not filter.
   */
  public abstract void setExcludeSlowNodesEnabled(boolean enable);

  public abstract boolean getExcludeSlowNodesEnabled();

  /**
   * Updates the value used for minBlocksForWrite, which is set by
   * {@code DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY}.
   *
   * @param minBlocksForWrite the minimum number of blocks required for write operations.
   */
  public abstract void setMinBlocksForWrite(int minBlocksForWrite);

  public abstract int getMinBlocksForWrite();
}
