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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.net.Node;
import org.junit.Test;


public class TestReplicationPolicyWithUpgradeDomain
    extends BaseReplicationPolicyTest {
  public TestReplicationPolicyWithUpgradeDomain() {
    this.blockPlacementPolicy =
        BlockPlacementPolicyWithUpgradeDomain.class.getName();
  }

  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    final String[] racks = {
        "/d1/r1",
        "/d1/r1",
        "/d1/r1",
        "/d1/r2",
        "/d1/r2",
        "/d1/r2",
        "/d1/r3",
        "/d1/r3",
        "/d1/r3"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    DatanodeDescriptor dataNodes[] =
        DFSTestUtil.toDatanodeDescriptor(storages);
    for (int i=0; i < dataNodes.length; i++) {
      // each rack has 3 DNs with upgrade domain id 1,2,3 respectively.
      String upgradeDomain = Integer.toString((i%3)+1);
      dataNodes[i].setUpgradeDomain(upgradeDomain);
    }
    return dataNodes;
  }


  /**
   * Verify the targets are chosen to honor both
   * rack and upgrade domain policies when number of replica is
   * 0, 1, 2, 3, 4 respectively.
   * @throws Exception
   */
  @Test
  public void testChooseTarget1() throws Exception {
    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        0L, 0L, 4, 0);

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0);
    assertEquals(targets.length, 0);

    targets = chooseTarget(1);
    assertEquals(targets.length, 1);
    assertEquals(storages[0], targets[0]);

    targets = chooseTarget(2);
    assertEquals(targets.length, 2);
    assertEquals(storages[0], targets[0]);
    assertFalse(isOnSameRack(targets[0], targets[1]));
    assertEquals(getUpgradeDomains(targets).size(), 2);

    targets = chooseTarget(3);
    assertEquals(targets.length, 3);
    assertEquals(storages[0], targets[0]);
    assertFalse(isOnSameRack(targets[0], targets[1]));
    assertTrue(isOnSameRack(targets[1], targets[2]));
    assertEquals(getUpgradeDomains(targets).size(), 3);

    targets = chooseTarget(4);
    assertEquals(targets.length, 4);
    assertEquals(storages[0], targets[0]);
    assertTrue(isOnSameRack(targets[1], targets[2]) ||
        isOnSameRack(targets[2], targets[3]));
    assertFalse(isOnSameRack(targets[0], targets[2]));
    assertEquals(getUpgradeDomains(targets).size(), 3);

    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }

  /**
   * Verify the rack and upgrade domain policies when excludeNodes are
   * specified.
   * @throws Exception
   */
  @Test
  public void testChooseTargetWithExcludeNodes() throws Exception {
    Set<Node> excludedNodes = new HashSet<>();
    DatanodeStorageInfo[] targets;
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[4]);
    targets = chooseTarget(3, chosenNodes, excludedNodes);
    assertEquals(targets.length, 3);
    assertEquals(storages[0], targets[0]);
    assertEquals(getRacks(targets).size(), 2);
    assertEquals(getUpgradeDomains(targets).size(), 3);

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[4]);
    excludedNodes.add(dataNodes[8]);
    targets = chooseTarget(3, chosenNodes, excludedNodes);
    assertEquals(targets.length, 3);
    assertEquals(storages[0], targets[0]);
    assertEquals(getRacks(targets).size(), 2);
    assertEquals(getUpgradeDomains(targets).size(), 3);

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[4]);
    excludedNodes.add(dataNodes[5]);
    excludedNodes.add(dataNodes[8]);
    targets = chooseTarget(3, chosenNodes, excludedNodes);
    assertEquals(targets.length, 3);
    assertEquals(storages[0], targets[0]);
    assertEquals(storages[2], targets[1]);
    assertEquals(storages[7], targets[2]);

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[4]);
    targets = chooseTarget(4, chosenNodes, excludedNodes);
    assertEquals(targets.length, 4);
    assertEquals(storages[0], targets[0]);
    assertTrue(getRacks(targets).size()>=2);
    assertEquals(getUpgradeDomains(targets).size(), 3);

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[4]);
    excludedNodes.add(dataNodes[8]);
    targets = chooseTarget(4, chosenNodes, excludedNodes);
    assertEquals(targets.length, 4);
    assertEquals(storages[0], targets[0]);
    assertTrue(getRacks(targets).size()>=2);
    assertEquals(getUpgradeDomains(targets).size(), 3);

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]);
    chosenNodes.add(storages[2]);
    targets = replicator.chooseTarget(filename, 1, dataNodes[0], chosenNodes,
        true, excludedNodes, BLOCK_SIZE,
        TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);
    System.out.println("targets=" + Arrays.asList(targets));
    assertEquals(2, targets.length);
  }

  /**
   * Verify the correct replica is chosen to satisfy both rack and upgrade
   * domain policy.
   * @throws Exception
   */
  @Test
  public void testChooseReplicaToDelete() throws Exception {
    BlockPlacementPolicyWithUpgradeDomain upgradeDomainPolicy =
        (BlockPlacementPolicyWithUpgradeDomain)replicator;
    List<DatanodeStorageInfo> first = new ArrayList<>();
    List<DatanodeStorageInfo> second = new ArrayList<>();
    List<StorageType> excessTypes = new ArrayList<>();
    excessTypes.add(StorageType.DEFAULT);
    first.add(storages[0]);
    first.add(storages[1]);
    second.add(storages[4]);
    second.add(storages[8]);
    DatanodeStorageInfo chosenStorage =
        upgradeDomainPolicy.chooseReplicaToDelete(
            null, null, (short)3, first, second, excessTypes);
    assertEquals(chosenStorage, storages[1]);
    first.clear();
    second.clear();

    excessTypes.add(StorageType.DEFAULT);
    first.add(storages[0]);
    first.add(storages[1]);
    first.add(storages[4]);
    first.add(storages[5]);
    chosenStorage = upgradeDomainPolicy.chooseReplicaToDelete(
        null, null, (short)3, first, second, excessTypes);
    assertTrue(chosenStorage.equals(storages[1]) ||
        chosenStorage.equals(storages[4]));
  }

  /**
   * Test the scenario where not enough replicas can't satisfy the policy.
   * @throws Exception
   */
  @Test
  public void testChooseTargetWithoutEnoughReplica() throws Exception {
    Set<Node> excludedNodes = new HashSet<>();
    DatanodeStorageInfo[] targets;
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[4]);
    excludedNodes.add(dataNodes[5]);
    excludedNodes.add(dataNodes[7]);
    excludedNodes.add(dataNodes[8]);
    targets = chooseTarget(3, chosenNodes, excludedNodes);
    assertEquals(targets.length, 2);
    assertEquals(storages[0], targets[0]);
    assertTrue(targets[1].equals(storages[1]) ||
        targets[1].equals(storages[2]));
  }

  /**
   * Test the scenario where not enough replicas can't satisfy the policy.
   * @throws Exception
   */
  @Test
  public void testVerifyBlockPlacement() throws Exception {
    LocatedBlock locatedBlock;
    BlockPlacementStatus status;
    ExtendedBlock b = new ExtendedBlock("fake-pool", new Block(12345L));
    List<DatanodeStorageInfo> set = new ArrayList<>();

    // 2 upgrade domains (not enough), 2 racks (enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[1]);
    set.add(storages[4]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement("", locatedBlock, set.size());
    assertFalse(status.isPlacementPolicySatisfied());

    // 3 upgrade domains (enough), 2 racks (enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[1]);
    set.add(storages[5]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement("", locatedBlock, set.size());
    assertTrue(status.isPlacementPolicySatisfied());

    // 3 upgrade domains (enough), 1 rack (not enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[1]);
    set.add(storages[2]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement("", locatedBlock, set.size());
    assertFalse(status.isPlacementPolicySatisfied());
    assertFalse(status.getErrorDescription().contains("upgrade domain"));

    // 2 upgrade domains( not enough), 3 racks (enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[5]);
    set.add(storages[8]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement("", locatedBlock, set.size());
    assertFalse(status.isPlacementPolicySatisfied());
    assertTrue(status.getErrorDescription().contains("upgrade domain"));

    // 3 upgrade domains (enough), 3 racks (enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[4]);
    set.add(storages[8]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement("", locatedBlock, set.size());
    assertTrue(status.isPlacementPolicySatisfied());


    // 3 upgrade domains (enough), 3 racks (enough), 4 replicas
    set.clear();
    set.add(storages[0]);
    set.add(storages[1]);
    set.add(storages[5]);
    set.add(storages[8]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement("", locatedBlock, set.size());
    assertTrue(status.isPlacementPolicySatisfied());

    // 2 upgrade domains (not enough), 3 racks (enough), 4 replicas
    set.clear();
    set.add(storages[0]);
    set.add(storages[3]);
    set.add(storages[5]);
    set.add(storages[8]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement("", locatedBlock, set.size());
    assertFalse(status.isPlacementPolicySatisfied());
  }

  private Set<String> getUpgradeDomains(DatanodeStorageInfo[] nodes) {
    HashSet<String> upgradeDomains = new HashSet<>();
    for (DatanodeStorageInfo node : nodes) {
      upgradeDomains.add(node.getDatanodeDescriptor().getUpgradeDomain());
    }
    return upgradeDomains;
  }

  private Set<String> getRacks(DatanodeStorageInfo[] nodes) {
    HashSet<String> racks = new HashSet<>();
    for (DatanodeStorageInfo node : nodes) {
      String rack = node.getDatanodeDescriptor().getNetworkLocation();
      racks.add(rack);
    }
    return racks;
  }
}
