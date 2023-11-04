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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.net.Node;

import java.util.EnumMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Utility class for the Available Space Block Placement Policy.
 * This class contains methods that help with block placement
 * decision-making based on available storage space.
 */
public class AvailableSpaceBlockPlacementPolicyUtils {

  private AvailableSpaceBlockPlacementPolicyUtils() {
    /* Hidden constructor */
  }

  private static final Random RAND = new Random();

  protected static DatanodeStorageInfo chooseLocalStorage(
      Node localMachine, Set<Node> excludedNodes, long blocksize,
      int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes,
      boolean fallbackToLocalRack, AvailableSpaceContext context)
      throws BlockPlacementPolicy.NotEnoughReplicasException {
    final EnumMap<StorageType, Integer> initialStorageTypesLocal =
        storageTypes.clone();
    final EnumMap<StorageType, Integer> initialStorageTypesLocalRack =
        storageTypes.clone();
    DatanodeStorageInfo local =
        context.getPlacementPolicy().chooseLocalStorage(localMachine, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes,
            initialStorageTypesLocal);
    if (!fallbackToLocalRack) {
      return local;
    }
    if (local != null) {
      results.remove(local);
    }
    DatanodeStorageInfo localRack =
        context.getPlacementPolicy().chooseLocalRack(localMachine, excludedNodes,
            blocksize, maxNodesPerRack, results, avoidStaleNodes, initialStorageTypesLocalRack);
    if (local != null && localRack != null) {
      if (select(local.getDatanodeDescriptor(),
          localRack.getDatanodeDescriptor(), true, context) == local
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

  private static void swapStorageTypes(EnumMap<StorageType, Integer> fromStorageTypes,
      EnumMap<StorageType, Integer> toStorageTypes) {
    toStorageTypes.clear();
    toStorageTypes.putAll(fromStorageTypes);
  }

  protected static DatanodeDescriptor select(DatanodeDescriptor a, DatanodeDescriptor b,
      boolean isBalanceLocal, AvailableSpaceContext context) {
    if (a != null && b != null){
      int ret = compareDataNode(a, b, isBalanceLocal, context.getBalancedSpaceToleranceLimit(),
          context.getBalancedSpaceTolerance());
      if (ret == 0) {
        return a;
      } else if (ret < 0) {
        return (RAND.nextInt(100) < context.getBalancedPreference()) ? a : b;
      } else {
        return (RAND.nextInt(100) < context.getBalancedPreference()) ? b : a;
      }
    } else {
      return a == null ? b : a;
    }
  }

  /**
   * Compare the two data nodes.
   */
  @VisibleForTesting
  protected static int compareDataNode(final DatanodeDescriptor a,
      final DatanodeDescriptor b, boolean isBalanceLocal,
      int balancedSpaceToleranceLimit, int balancedSpaceTolerance) {

    boolean toleranceLimit = Math.max(a.getDfsUsedPercent(), b.getDfsUsedPercent())
        < balancedSpaceToleranceLimit;
    if (a.equals(b)
        || (toleranceLimit && Math.abs(a.getDfsUsedPercent() - b.getDfsUsedPercent())
        < balancedSpaceTolerance) || ((
        isBalanceLocal && a.getDfsUsedPercent() < 50))) {
      return 0;
    }
    return a.getDfsUsedPercent() < b.getDfsUsedPercent() ? -1 : 1;
  }

  protected final static class AvailableSpaceContext {
    private final int balancedPreference;
    private final int balancedSpaceToleranceLimit;
    private final int balancedSpaceTolerance;
    private BlockPlacementPolicyDefault placementPolicy;

    public AvailableSpaceContext(int balancedPreference, int balancedSpaceToleranceLimit,
        int balancedSpaceTolerance) {
      this.balancedPreference = balancedPreference;
      this.balancedSpaceToleranceLimit = balancedSpaceToleranceLimit;
      this.balancedSpaceTolerance = balancedSpaceTolerance;
    }

    public AvailableSpaceContext(int balancedPreference, int balancedSpaceToleranceLimit,
        int balancedSpaceTolerance, BlockPlacementPolicyDefault placementPolicy) {
      this.balancedPreference = balancedPreference;
      this.balancedSpaceToleranceLimit = balancedSpaceToleranceLimit;
      this.balancedSpaceTolerance = balancedSpaceTolerance;
      this.placementPolicy = placementPolicy;
    }

    public int getBalancedPreference() {
      return balancedPreference;
    }

    public int getBalancedSpaceToleranceLimit() {
      return balancedSpaceToleranceLimit;
    }

    public int getBalancedSpaceTolerance() {
      return balancedSpaceTolerance;
    }

    public BlockPlacementPolicyDefault getPlacementPolicy() {
      return placementPolicy;
    }
  }
}
