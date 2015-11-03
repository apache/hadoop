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
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas that honors upgrade domain policy.
 * Here is the replica placement strategy. If the writer is on a datanode,
 * the 1st replica is placed on the local machine,
 * otherwise a random datanode. The 2nd replica is placed on a datanode
 * that is on a different rack. The 3rd replica is placed on a datanode
 * which is on a different node of the rack as the second replica.
 * All 3 replicas have unique upgrade domains.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockPlacementPolicyWithUpgradeDomain extends
    BlockPlacementPolicyDefault {

  private int upgradeDomainFactor;

  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    upgradeDomainFactor = conf.getInt(
        DFSConfigKeys.DFS_UPGRADE_DOMAIN_FACTOR,
        DFSConfigKeys.DFS_UPGRADE_DOMAIN_FACTOR_DEFAULT);
  }

  @Override
  protected boolean isGoodDatanode(DatanodeDescriptor node,
      int maxTargetPerRack, boolean considerLoad,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes) {
    boolean isGoodTarget = super.isGoodDatanode(node,
        maxTargetPerRack, considerLoad, results, avoidStaleNodes);
    if (isGoodTarget) {
      if (results.size() > 0 && results.size() < upgradeDomainFactor) {
        // Each node in "results" has a different upgrade domain. Make sure
        // the candidate node introduces a new upgrade domain.
        Set<String> upgradeDomains = getUpgradeDomains(results);
        if (upgradeDomains.contains(node.getUpgradeDomain())) {
          isGoodTarget = false;
        }
      }
    }
    return isGoodTarget;
  }

  // If upgrade domain isn't specified, uses its XferAddr as upgrade domain.
  // Such fallback is useful to test the scenario where upgrade domain isn't
  // defined but the block placement is set to upgrade domain policy.
  public String getUpgradeDomainWithDefaultValue(DatanodeInfo datanodeInfo) {
    String upgradeDomain = datanodeInfo.getUpgradeDomain();
    if (upgradeDomain == null) {
      LOG.warn("Upgrade domain isn't defined for " + datanodeInfo);
      upgradeDomain = datanodeInfo.getXferAddr();
    }
    return upgradeDomain;
  }

  private String getUpgradeDomain(DatanodeStorageInfo storage) {
    return getUpgradeDomainWithDefaultValue(storage.getDatanodeDescriptor());
  }

  private Set<String> getUpgradeDomains(List<DatanodeStorageInfo> results) {
    Set<String> upgradeDomains = new HashSet<>();
    if (results == null) {
      return upgradeDomains;
    }
    for(DatanodeStorageInfo storageInfo : results) {
      upgradeDomains.add(getUpgradeDomain(storageInfo));
    }
    return upgradeDomains;
  }

  private Set<String> getUpgradeDomainsFromNodes(DatanodeInfo[] nodes) {
    Set<String> upgradeDomains = new HashSet<>();
    if (nodes == null) {
      return upgradeDomains;
    }
    for(DatanodeInfo node : nodes) {
      upgradeDomains.add(getUpgradeDomainWithDefaultValue(node));
    }
    return upgradeDomains;
  }

  private <T> Map<String, List<T>> getUpgradeDomainMap(
      Collection<T> storagesOrDataNodes) {
    Map<String, List<T>> upgradeDomainMap = new HashMap<>();
    for(T storage : storagesOrDataNodes) {
      String upgradeDomain = getUpgradeDomainWithDefaultValue(
          getDatanodeInfo(storage));
      List<T> storages = upgradeDomainMap.get(upgradeDomain);
      if (storages == null) {
        storages = new ArrayList<>();
        upgradeDomainMap.put(upgradeDomain, storages);
      }
      storages.add(storage);
    }
    return upgradeDomainMap;
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    BlockPlacementStatus defaultStatus = super.verifyBlockPlacement(locs,
        numberOfReplicas);
    BlockPlacementStatusWithUpgradeDomain upgradeDomainStatus =
        new BlockPlacementStatusWithUpgradeDomain(defaultStatus,
            getUpgradeDomainsFromNodes(locs),
                numberOfReplicas, upgradeDomainFactor);
    return upgradeDomainStatus;
  }

  private <T> List<T> getShareUDSet(
      Map<String, List<T>> upgradeDomains) {
    List<T> getShareUDSet = new ArrayList<>();
    for (Map.Entry<String, List<T>> e : upgradeDomains.entrySet()) {
      if (e.getValue().size() > 1) {
        getShareUDSet.addAll(e.getValue());
      }
    }
    return getShareUDSet;
  }

  private Collection<DatanodeStorageInfo> combine(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne) {
    List<DatanodeStorageInfo> all = new ArrayList<>();
    if (moreThanOne != null) {
      all.addAll(moreThanOne);
    }
    if (exactlyOne != null) {
      all.addAll(exactlyOne);
    }
    return all;
  }

  /*
   * The policy to pick the replica set for deleting the over-replicated
   * replica which meet the rack and upgrade domain requirements.
   * The algorithm:
   * a. Each replica has a boolean attribute "shareRack" that defines
   *    whether it shares its rack with another replica of the same block.
   * b. Each replica has another boolean attribute "shareUD" that defines
   *    whether it shares its upgrade domain with another replica of the same
   *    block.
   * c. Partition the replicas into 4 sets (some might be empty.):
   *    shareRackAndUDSet: {shareRack==true, shareUD==true}
   *    shareUDNotRackSet: {shareRack==false, shareUD==true}
   *    shareRackNotUDSet: {shareRack==true, shareUD==false}
   *    NoShareRackOrUDSet: {shareRack==false, shareUD==false}
   * d. Pick the first not-empty replica set in the following order.
   *    shareRackAndUDSet, shareUDNotRackSet, shareRackNotUDSet,
   *    NoShareRackOrUDSet
   * e. Proof this won't degrade the existing rack-based data
   *    availability model under different scenarios.
   *    1. shareRackAndUDSet isn't empty. Removing a node
   *       from shareRackAndUDSet won't change # of racks and # of UD.
   *       The followings cover empty shareUDNotRackSet scenarios.
   *    2. shareUDNotRackSet isn't empty and shareRackNotUDSet isn't empty.
   *       Let us proof that # of racks >= 3 before the deletion and thus
   *       after deletion # of racks >= 2.
   *         Given shareUDNotRackSet is empty, there won't be overlap between
   *       shareUDNotRackSet and shareRackNotUDSet. It means DNs in
   *       shareRackNotUDSet should be on at least a rack
   *       different from any DN' rack in shareUDNotRackSet.
   *         Given shareUDNotRackSet.size() >= 2 and each DN in the set
   *       doesn't share rack with any other DNs, there are at least 2 racks
   *       coming from shareUDNotRackSet.
   *         Thus the # of racks from DNs in {shareUDNotRackSet,
   *       shareRackNotUDSet} >= 3. Removing a node from shareUDNotRackSet
   *       will reduce the # of racks by 1 and won't change # of upgrade
   *       domains.
   *         Note that this is different from BlockPlacementPolicyDefault which
   *       will keep the # of racks after deletion. With upgrade domain policy,
   *       given # of racks is still >= 2 after deletion, the data availability
   *       model remains the same as BlockPlacementPolicyDefault (only supports
   *       one rack failure).
   *         For example, assume we have 4 replicas: d1(rack1, ud1),
   *       d2(rack2, ud1), d3(rack3, ud3), d4(rack3, ud4). Thus we have
   *       shareUDNotRackSet: {d1, d2} and shareRackNotUDSet: {d3, d4}.
   *       With upgrade domain policy, the remaining replicas after deletion
   *       are {d1(or d2), d3, d4} which has 2 racks.
   *       With BlockPlacementPolicyDefault policy, the remaining replicas
   *       after deletion are {d1, d2, d3(or d4)} which has 3 racks.
   *    3. shareUDNotRackSet isn't empty and shareRackNotUDSet is empty. This
   *       implies all replicas are on unique racks. Removing a node from
   *       shareUDNotRackSet will reduce # of racks (no different from
   *       BlockPlacementPolicyDefault) by 1 and won't change #
   *       of upgrade domains.
   *    4. shareUDNotRackSet is empty and shareRackNotUDSet isn't empty.
   *       Removing a node from shareRackNotUDSet is no different from
   *       BlockPlacementPolicyDefault.
   *    5. shareUDNotRackSet is empty and shareRackNotUDSet is empty.
   *       Removing a node from NoShareRackOrUDSet is no different from
   *       BlockPlacementPolicyDefault.
   * The implementation:
   * 1. Generate set shareUDSet which includes all DatanodeStorageInfo that
   *    share the same upgrade domain with another DatanodeStorageInfo,
   *    e.g. {shareRackAndUDSet, shareUDNotRackSet}.
   * 2. If shareUDSet is empty, it means shareRackAndUDSet is empty and
   *    shareUDNotRackSet is empty. Use the default rack based policy.
   * 3. If shareUDSet isn't empty, intersect it with moreThanOne(
   *    {shareRackAndUDSet, shareRackNotUDSet})to generate shareRackAndUDSet.
   * 4. If shareRackAndUDSet isn't empty, return
   *    shareRackAndUDSet, otherwise return shareUDSet which is the same as
   *    shareUDNotRackSet.
   */
  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne) {
    // shareUDSet includes DatanodeStorageInfo that share same upgrade
    // domain with another DatanodeStorageInfo.
    Collection<DatanodeStorageInfo> all = combine(moreThanOne, exactlyOne);
    List<DatanodeStorageInfo> shareUDSet = getShareUDSet(
        getUpgradeDomainMap(all));
    // shareRackAndUDSet contains those DatanodeStorageInfo that
    // share rack and upgrade domain with another DatanodeStorageInfo.
    List<DatanodeStorageInfo> shareRackAndUDSet = new ArrayList<>();
    if (shareUDSet.size() == 0) {
      // All upgrade domains are unique, use the parent set.
      return super.pickupReplicaSet(moreThanOne, exactlyOne);
    } else if (moreThanOne != null) {
      for (DatanodeStorageInfo storage : shareUDSet) {
        if (moreThanOne.contains(storage)) {
          shareRackAndUDSet.add(storage);
        }
      }
    }
    return (shareRackAndUDSet.size() > 0) ? shareRackAndUDSet : shareUDSet;
  }

  @Override
  boolean useDelHint(DatanodeStorageInfo delHint,
      DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      List<StorageType> excessTypes) {
    if (!super.useDelHint(delHint, added, moreThanOne, exactlyOne,
        excessTypes)) {
      // If BlockPlacementPolicyDefault doesn't allow useDelHint, there is no
      // point checking with upgrade domain policy.
      return false;
    }
    return isMovableBasedOnUpgradeDomain(combine(moreThanOne, exactlyOne),
        delHint, added);
  }

  // Check if moving from source to target will preserve the upgrade domain
  // policy.
  private <T> boolean isMovableBasedOnUpgradeDomain(Collection<T> all,
      T source, T target) {
    Map<String, List<T>> udMap = getUpgradeDomainMap(all);
    // shareUDSet includes datanodes that share same upgrade
    // domain with another datanode.
    List<T> shareUDSet = getShareUDSet(udMap);
    // check if removing source reduces the number of upgrade domains
    if (notReduceNumOfGroups(shareUDSet, source, target)) {
      return true;
    } else if (udMap.size() > upgradeDomainFactor) {
      return true; // existing number of upgrade domain exceeds the limit.
    } else {
      return false; // removing source reduces the number of UDs.
    }
  }

  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs,
      DatanodeInfo source, DatanodeInfo target) {
    if (super.isMovable(locs, source, target)) {
      return isMovableBasedOnUpgradeDomain(locs, source, target);
    } else {
      return false;
    }
  }
}
