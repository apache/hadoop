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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

/**
 * Space balanced block placement policy.
 */
public class AvailableSpaceBlockPlacementPolicy extends
    BlockPlacementPolicyDefault {
  private static final Logger LOG = LoggerFactory
      .getLogger(AvailableSpaceBlockPlacementPolicy.class);
  private static final Random RAND = new Random();
  private int balancedPreference =
      (int) (100 * DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);
  private boolean optimizeLocal;

  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    float balancedPreferencePercent =
        conf.getFloat(
          DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
          DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);

    LOG.info("Available space block placement policy initialized: "
        + DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
        + " = " + balancedPreferencePercent);

    optimizeLocal = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCE_LOCAL_NODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCE_LOCAL_NODE_DEFAULT);

    if (balancedPreferencePercent > 1.0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is greater than 1.0 but should be in the range 0.0 - 1.0");
    }
    if (balancedPreferencePercent < 0.5) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is less than 0.5 so datanodes with more used percent will"
          + " receive  more block allocations.");
    }
    balancedPreference = (int) (100 * balancedPreferencePercent);
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode, StorageType type) {
    // only the code that uses DFSNetworkTopology should trigger this code path.
    Preconditions.checkArgument(clusterMap instanceof DFSNetworkTopology);
    DFSNetworkTopology dfsClusterMap = (DFSNetworkTopology)clusterMap;
    DatanodeDescriptor a = (DatanodeDescriptor) dfsClusterMap
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    DatanodeDescriptor b = (DatanodeDescriptor) dfsClusterMap
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    return select(a, b, false);
  }

  @Override
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, boolean fallbackToLocalRack)
      throws NotEnoughReplicasException {
    if (!optimizeLocal) {
      return super.chooseLocalStorage(localMachine, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes,
          fallbackToLocalRack);
    }
    final EnumMap<StorageType, Integer> initialStorageTypesLocal =
        storageTypes.clone();
    final EnumMap<StorageType, Integer> initialStorageTypesLocalRack =
        storageTypes.clone();
    DatanodeStorageInfo local =
        chooseLocalStorage(localMachine, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes,
            initialStorageTypesLocal);
    if (!fallbackToLocalRack) {
      return local;
    }
    if (local != null) {
      results.remove(local);
    }
    DatanodeStorageInfo localRack =
        chooseLocalRack(localMachine, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, initialStorageTypesLocalRack);
    if (local != null && localRack != null) {
      if (select(local.getDatanodeDescriptor(),
          localRack.getDatanodeDescriptor(), true) == local
          .getDatanodeDescriptor()) {
        results.remove(localRack);
        results.add(local);
        swapStorageTypes(initialStorageTypesLocal, storageTypes);
        excludedNodes.remove(localRack.getDatanodeDescriptor());
        return local;
      } else {
        swapStorageTypes(initialStorageTypesLocalRack, storageTypes);
        excludedNodes.remove(local.getDatanodeDescriptor());
        return localRack;
      }
    } else if (localRack == null && local != null) {
      results.add(local);
      swapStorageTypes(initialStorageTypesLocal, storageTypes);
      return local;
    } else {
      swapStorageTypes(initialStorageTypesLocalRack, storageTypes);
      return localRack;
    }
  }

  private void swapStorageTypes(EnumMap<StorageType, Integer> fromStorageTypes,
      EnumMap<StorageType, Integer> toStorageTypes) {
    toStorageTypes.clear();
    toStorageTypes.putAll(fromStorageTypes);
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode) {
    DatanodeDescriptor a =
        (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNode);
    DatanodeDescriptor b =
        (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNode);
    return select(a, b, false);
  }

  private DatanodeDescriptor select(DatanodeDescriptor a, DatanodeDescriptor b,
      boolean isBalanceLocal) {
    if (a != null && b != null){
      int ret = compareDataNode(a, b, isBalanceLocal);
      if (ret == 0) {
        return a;
      } else if (ret < 0) {
        return (RAND.nextInt(100) < balancedPreference) ? a : b;
      } else {
        return (RAND.nextInt(100) < balancedPreference) ? b : a;
      }
    } else {
      return a == null ? b : a;
    }
  }

  /**
   * Compare the two data nodes.
   */
  protected int compareDataNode(final DatanodeDescriptor a,
      final DatanodeDescriptor b, boolean isBalanceLocal) {
    if (a.equals(b)
        || Math.abs(a.getDfsUsedPercent() - b.getDfsUsedPercent()) < 5 || ((
        isBalanceLocal && a.getDfsUsedPercent() < 50))) {
      return 0;
    }
    return a.getDfsUsedPercent() < b.getDfsUsedPercent() ? -1 : 1;
  }
}
