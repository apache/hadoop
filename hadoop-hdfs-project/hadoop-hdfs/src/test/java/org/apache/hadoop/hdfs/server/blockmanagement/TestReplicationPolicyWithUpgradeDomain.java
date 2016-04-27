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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
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
        TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);
    System.out.println("targets=" + Arrays.asList(targets));
    assertEquals(2, targets.length);
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
   * Test block placement verification.
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
    status = replicator.verifyBlockPlacement(locatedBlock.getLocations(),
        set.size());
    assertFalse(status.isPlacementPolicySatisfied());

    // 3 upgrade domains (enough), 2 racks (enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[1]);
    set.add(storages[5]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement(locatedBlock.getLocations(),
        set.size());
    assertTrue(status.isPlacementPolicySatisfied());

    // 3 upgrade domains (enough), 1 rack (not enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[1]);
    set.add(storages[2]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement(locatedBlock.getLocations(),
        set.size());
    assertFalse(status.isPlacementPolicySatisfied());
    assertFalse(status.getErrorDescription().contains("upgrade domain"));

    // 2 upgrade domains( not enough), 3 racks (enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[5]);
    set.add(storages[8]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement(locatedBlock.getLocations(),
        set.size());
    assertFalse(status.isPlacementPolicySatisfied());
    assertTrue(status.getErrorDescription().contains("upgrade domain"));

    // 3 upgrade domains (enough), 3 racks (enough)
    set.clear();
    set.add(storages[0]);
    set.add(storages[4]);
    set.add(storages[8]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement(locatedBlock.getLocations(),
        set.size());
    assertTrue(status.isPlacementPolicySatisfied());


    // 3 upgrade domains (enough), 3 racks (enough), 4 replicas
    set.clear();
    set.add(storages[0]);
    set.add(storages[1]);
    set.add(storages[5]);
    set.add(storages[8]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement(locatedBlock.getLocations(),
        set.size());
    assertTrue(status.isPlacementPolicySatisfied());

    // 2 upgrade domains (not enough), 3 racks (enough), 4 replicas
    set.clear();
    set.add(storages[0]);
    set.add(storages[3]);
    set.add(storages[5]);
    set.add(storages[8]);
    locatedBlock = BlockManager.newLocatedBlock(b,
        set.toArray(new DatanodeStorageInfo[set.size()]), 0, false);
    status = replicator.verifyBlockPlacement(locatedBlock.getLocations(),
        set.size());
    assertFalse(status.isPlacementPolicySatisfied());
  }

  /**
   * Verify the correct replica is chosen to satisfy both rack and upgrade
   * domain policy.
   * @throws Exception
   */
  @Test
  public void testChooseReplicasToDelete() throws Exception {
    Collection<DatanodeStorageInfo> nonExcess = new ArrayList<>();
    nonExcess.add(storages[0]);
    nonExcess.add(storages[1]);
    nonExcess.add(storages[2]);
    nonExcess.add(storages[3]);
    List<DatanodeStorageInfo> excessReplicas;
    BlockStoragePolicySuite POLICY_SUITE = BlockStoragePolicySuite
        .createDefaultSuite();
    BlockStoragePolicy storagePolicy = POLICY_SUITE.getDefaultPolicy();

    // delete hint accepted.
    DatanodeDescriptor delHintNode = storages[0].getDatanodeDescriptor();
    List<StorageType> excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[3].getDatanodeDescriptor(), delHintNode);
    assertTrue(excessReplicas.size() == 1);
    assertTrue(excessReplicas.contains(storages[0]));

    // delete hint rejected because deleting storages[1] would have
    // cause only two upgrade domains left.
    delHintNode = storages[1].getDatanodeDescriptor();
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[3].getDatanodeDescriptor(), delHintNode);
    assertTrue(excessReplicas.size() == 1);
    assertTrue(excessReplicas.contains(storages[0]));

    // no delete hint, case 1
    nonExcess.clear();
    nonExcess.add(storages[0]);
    nonExcess.add(storages[1]);
    nonExcess.add(storages[4]);
    nonExcess.add(storages[8]);
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[8].getDatanodeDescriptor(), null);
    assertTrue(excessReplicas.size() == 1);
    assertTrue(excessReplicas.contains(storages[1]));

    // no delete hint, case 2
    nonExcess.clear();
    nonExcess.add(storages[0]);
    nonExcess.add(storages[1]);
    nonExcess.add(storages[4]);
    nonExcess.add(storages[5]);
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[8].getDatanodeDescriptor(), null);
    assertTrue(excessReplicas.size() == 1);
    assertTrue(excessReplicas.contains(storages[1]) ||
        excessReplicas.contains(storages[4]));

    // No delete hint, different excess type deletion
    nonExcess.clear();
    nonExcess.add(storages[0]);
    nonExcess.add(storages[1]);
    nonExcess.add(storages[2]);
    nonExcess.add(storages[3]);
    DatanodeStorageInfo excessStorage = DFSTestUtil.createDatanodeStorageInfo(
        "Storage-excess-ID", "localhost", delHintNode.getNetworkLocation(),
        "foo.com", StorageType.ARCHIVE, delHintNode.getUpgradeDomain());
    nonExcess.add(excessStorage);
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[3].getDatanodeDescriptor(), null);
    assertTrue(excessReplicas.size() == 2);
    assertTrue(excessReplicas.contains(storages[0]));
    assertTrue(excessReplicas.contains(excessStorage));

    // Test SSD related deletion. With different rack settings here, but
    // similar to {@link TestReplicationPolicy#testChooseReplicasToDelete}.
    // The block was initially created on excessSSD(rack r1, UD 4),
    // storages[7](rack r3, UD 2) and storages[8](rack r3, UD 3) with
    // ONESSD_STORAGE_POLICY_NAME storage policy. Replication factor = 3.
    // Right after balancer moves the block from storages[7] to
    // storages[3](rack r2, UD 1), the application changes the storage policy
    // from ONESSD_STORAGE_POLICY_NAME to HOT_STORAGE_POLICY_ID. In this case,
    // we should be able to delete excessSSD since the remaining
    // storages ({storages[3]}, {storages[7], storages[8]})
    // are on different racks (r2, r3) and different UDs (1, 2, 3).
    DatanodeStorageInfo excessSSD = DFSTestUtil.createDatanodeStorageInfo(
        "Storage-excess-SSD-ID", "localhost",
        storages[0].getDatanodeDescriptor().getNetworkLocation(), "foo.com",
        StorageType.SSD, null);
    DatanodeStorageInfo[] ssds = { excessSSD };
    DatanodeDescriptor ssdNodes[] = DFSTestUtil.toDatanodeDescriptor(ssds);
    ssdNodes[0].setUpgradeDomain(Integer.toString(4));

    nonExcess.clear();
    nonExcess.add(excessSSD);
    nonExcess.add(storages[3]);
    nonExcess.add(storages[7]);
    nonExcess.add(storages[8]);
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[3].getDatanodeDescriptor(),
        storages[7].getDatanodeDescriptor());
    assertEquals(1, excessReplicas.size());
    assertTrue(excessReplicas.contains(excessSSD));
  }

  @Test
  public void testIsMovable() throws Exception {
    List<DatanodeInfo> candidates = new ArrayList<>();

    // after the move, the number of racks changes from 1 to 2.
    // and number of upgrade domains remains 3.
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[1]);
    candidates.add(dataNodes[2]);
    candidates.add(dataNodes[3]);
    assertTrue(replicator.isMovable(candidates, dataNodes[0], dataNodes[3]));

    // the move would have changed the number of racks from 1 to 2.
    // and the number of UDs from 3 to 2.
    candidates.clear();
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[1]);
    candidates.add(dataNodes[2]);
    candidates.add(dataNodes[4]);
    assertFalse(replicator.isMovable(candidates, dataNodes[0], dataNodes[4]));

    // after the move, the number of racks remains 2.
    // the number of UDs remains 3.
    candidates.clear();
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[4]);
    candidates.add(dataNodes[5]);
    candidates.add(dataNodes[6]);
    assertTrue(replicator.isMovable(candidates, dataNodes[0], dataNodes[6]));

    // after the move, the number of racks remains 2.
    // the number of UDs remains 2.
    candidates.clear();
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[1]);
    candidates.add(dataNodes[3]);
    candidates.add(dataNodes[4]);
    assertTrue(replicator.isMovable(candidates, dataNodes[0], dataNodes[4]));

    // the move would have changed the number of racks from 2 to 3.
    // and the number of UDs from 2 to 1.
    candidates.clear();
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[3]);
    candidates.add(dataNodes[4]);
    candidates.add(dataNodes[6]);
    assertFalse(replicator.isMovable(candidates, dataNodes[4], dataNodes[6]));
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
