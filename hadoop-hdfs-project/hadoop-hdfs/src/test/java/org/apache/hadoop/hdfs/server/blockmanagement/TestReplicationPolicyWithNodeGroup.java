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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestReplicationPolicyWithNodeGroup {
  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 8;
  private static final int NUM_OF_DATANODES_BOUNDARY = 6;
  private static final int NUM_OF_DATANODES_MORE_TARGETS = 12;
  private static final int NUM_OF_DATANODES_FOR_DEPENDENCIES = 6;
  private final Configuration CONF = new HdfsConfiguration();
  private NetworkTopology cluster;
  private NameNode namenode;
  private BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";

  private static final DatanodeStorageInfo[] storages;
  private static final DatanodeDescriptor[] dataNodes;
  static {
    final String[] racks = {
        "/d1/r1/n1",
        "/d1/r1/n1",
        "/d1/r1/n2",
        "/d1/r2/n3",
        "/d1/r2/n3",
        "/d1/r2/n4",
        "/d2/r3/n5",
        "/d2/r3/n6"
    };
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);
  }

  private static final DatanodeStorageInfo[] storagesInBoundaryCase;
  private static final DatanodeDescriptor[] dataNodesInBoundaryCase;
  static {
    final String[] racksInBoundaryCase = {
        "/d1/r1/n1",
        "/d1/r1/n1",
        "/d1/r1/n1",
        "/d1/r1/n2",
        "/d1/r2/n3",
        "/d1/r2/n3"
    };
    storagesInBoundaryCase = DFSTestUtil.createDatanodeStorageInfos(racksInBoundaryCase);
    dataNodesInBoundaryCase = DFSTestUtil.toDatanodeDescriptor(storagesInBoundaryCase);
  }

  private static final DatanodeStorageInfo[] storagesInMoreTargetsCase;
  private final static DatanodeDescriptor[] dataNodesInMoreTargetsCase;
  static {
    final String[] racksInMoreTargetsCase = {
        "/r1/n1",
        "/r1/n1",
        "/r1/n2",
        "/r1/n2",
        "/r1/n3",
        "/r1/n3",
        "/r2/n4",
        "/r2/n4",
        "/r2/n5",
        "/r2/n5",
        "/r2/n6",
        "/r2/n6"
    };
    storagesInMoreTargetsCase = DFSTestUtil.createDatanodeStorageInfos(racksInMoreTargetsCase);
    dataNodesInMoreTargetsCase = DFSTestUtil.toDatanodeDescriptor(storagesInMoreTargetsCase);
  };

  private final static DatanodeDescriptor NODE = 
      new DatanodeDescriptor(DFSTestUtil.getDatanodeDescriptor("9.9.9.9", "/d2/r4/n7"));
  
  private static final DatanodeStorageInfo[] storagesForDependencies;
  private static final DatanodeDescriptor[]  dataNodesForDependencies;
  static {
    final String[] racksForDependencies = {
        "/d1/r1/n1",
        "/d1/r1/n1",
        "/d1/r1/n2",
        "/d1/r1/n2",
        "/d1/r1/n3",
        "/d1/r1/n4"
    };
    final String[] hostNamesForDependencies = {
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6"
    };
    
    storagesForDependencies = DFSTestUtil.createDatanodeStorageInfos(
        racksForDependencies, hostNamesForDependencies);
    dataNodesForDependencies = DFSTestUtil.toDatanodeDescriptor(storagesForDependencies);
    
  };
  
  @Before
  public void setUp() throws Exception {
    FileSystem.setDefaultUri(CONF, "hdfs://localhost:0");
    CONF.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    // Set properties to make HDFS aware of NodeGroup.
    CONF.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY, 
        BlockPlacementPolicyWithNodeGroup.class.getName());
    CONF.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_IMPL_KEY, 
        NetworkTopologyWithNodeGroup.class.getName());
    
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    
    File baseDir = PathUtils.getTestDir(TestReplicationPolicyWithNodeGroup.class);
    
    CONF.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());
    
    DFSTestUtil.formatNameNode(CONF);
    namenode = new NameNode(CONF);
    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    replicator = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    // construct network topology
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
    }
    setupDataNodeCapacity();
  }

  @After
  public void tearDown() throws Exception {
    namenode.stop();
  }
  
  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      long dnCacheCapacity, long dnCacheUsed, int xceiverCount,
      int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures);
  }

  private static void setupDataNodeCapacity() {
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
  }
  
  /**
   * Scan the targets list: all targets should be on different NodeGroups.
   * Return false if two targets are found on the same NodeGroup.
   */
  private static boolean checkTargetsOnDifferentNodeGroup(
      DatanodeStorageInfo[] targets) {
    if(targets.length == 0)
      return true;
    Set<String> targetSet = new HashSet<String>();
    for(DatanodeStorageInfo storage:targets) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      String nodeGroup = NetworkTopology.getLastHalf(node.getNetworkLocation());
      if(targetSet.contains(nodeGroup)) {
        return false;
      } else {
        targetSet.add(nodeGroup);
      }
    }
    return true;
  }

  private boolean isOnSameRack(DatanodeStorageInfo left, DatanodeStorageInfo right) {
    return isOnSameRack(left.getDatanodeDescriptor(), right);
  }

  private boolean isOnSameRack(DatanodeDescriptor left, DatanodeStorageInfo right) {
    return cluster.isOnSameRack(left, right.getDatanodeDescriptor());
  }

  private boolean isOnSameNodeGroup(DatanodeStorageInfo left, DatanodeStorageInfo right) {
    return isOnSameNodeGroup(left.getDatanodeDescriptor(), right);
  }

  private boolean isOnSameNodeGroup(DatanodeDescriptor left, DatanodeStorageInfo right) {
    return cluster.isOnSameNodeGroup(left, right.getDatanodeDescriptor());
  }

  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas) {
    return chooseTarget(numOfReplicas, dataNodes[0]);
  }

  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer) {
    return chooseTarget(numOfReplicas, writer,
        new ArrayList<DatanodeStorageInfo>());
  }

  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      List<DatanodeStorageInfo> chosenNodes) {
    return chooseTarget(numOfReplicas, dataNodes[0], chosenNodes);
  }

  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeStorageInfo> chosenNodes) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null);
  }

  private DatanodeStorageInfo[] chooseTarget(
      int numOfReplicas,
      DatanodeDescriptor writer,
      List<DatanodeStorageInfo> chosenNodes,
      Set<Node> excludedNodes) {
    return replicator.chooseTarget(filename, numOfReplicas, writer, chosenNodes,
        false, excludedNodes, BLOCK_SIZE, StorageType.DEFAULT);
  }

  /**
   * In this testcase, client is dataNodes[0]. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on 
   * different rack and third should be placed on different node (and node group)
   * of rack chosen for 2nd node.
   * The only excpetion is when the <i>numOfReplicas</i> is 2, 
   * the 1st is on dataNodes[0] and the 2nd is on a different rack.
   * @throws Exception
   */
  @Test
  public void testChooseTarget1() throws Exception {
    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        0L, 0L, 4, 0); // overloaded

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

    targets = chooseTarget(3);
    assertEquals(targets.length, 3);
    assertEquals(storages[0], targets[0]);

    assertFalse(isOnSameRack(targets[0], targets[1]));
    assertTrue(isOnSameRack(targets[1], targets[2]));
    assertFalse(isOnSameNodeGroup(targets[1], targets[2]));

    targets = chooseTarget(4);
    assertEquals(targets.length, 4);
    assertEquals(storages[0], targets[0]);

    assertTrue(isOnSameRack(targets[1], targets[2]) ||
               isOnSameRack(targets[2], targets[3]));
    assertFalse(isOnSameRack(targets[0], targets[2]));
    // Make sure no more than one replicas are on the same nodegroup 
    verifyNoTwoTargetsOnSameNodeGroup(targets);

    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }

  private void verifyNoTwoTargetsOnSameNodeGroup(DatanodeStorageInfo[] targets) {
    Set<String> nodeGroupSet = new HashSet<String>();
    for (DatanodeStorageInfo target: targets) {
      nodeGroupSet.add(target.getDatanodeDescriptor().getNetworkLocation());
    }
    assertEquals(nodeGroupSet.size(), targets.length);
  }

  /**
   * In this testcase, client is dataNodes[0], but the dataNodes[1] is
   * not allowed to be chosen. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on a different
   * rack, the 3rd should be on same rack as the 2nd replica but in different
   * node group, and the rest should be placed on a third rack.
   * @throws Exception
   */
  @Test
  public void testChooseTarget2() throws Exception {
    DatanodeStorageInfo[] targets;
    BlockPlacementPolicyDefault repl = (BlockPlacementPolicyDefault)replicator;
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();

    Set<Node> excludedNodes = new HashSet<Node>();
    excludedNodes.add(dataNodes[1]); 
    targets = repl.chooseTarget(filename, 4, dataNodes[0], chosenNodes, false, 
        excludedNodes, BLOCK_SIZE, StorageType.DEFAULT);
    assertEquals(targets.length, 4);
    assertEquals(storages[0], targets[0]);

    assertTrue(cluster.isNodeGroupAware());
    // Make sure no replicas are on the same nodegroup 
    for (int i=1;i<4;i++) {
      assertFalse(isOnSameNodeGroup(targets[0], targets[i]));
    }
    assertTrue(isOnSameRack(targets[1], targets[2]) ||
               isOnSameRack(targets[2], targets[3]));
    assertFalse(isOnSameRack(targets[1], targets[3]));

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    chosenNodes.add(storages[2]);
    targets = repl.chooseTarget(filename, 1, dataNodes[0], chosenNodes, true,
        excludedNodes, BLOCK_SIZE, StorageType.DEFAULT);
    System.out.println("targets=" + Arrays.asList(targets));
    assertEquals(2, targets.length);
    //make sure that the chosen node is in the target.
    int i = 0;
    for(; i < targets.length && !storages[2].equals(targets[i]); i++);
    assertTrue(i < targets.length);
  }

  /**
   * In this testcase, client is dataNodes[0], but dataNodes[0] is not qualified
   * to be chosen. So the 1st replica should be placed on dataNodes[1], 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 2nd replica but in different nodegroup,
   * and the rest should be placed on the third rack.
   * @throws Exception
   */
  @Test
  public void testChooseTarget3() throws Exception {
    // make data node 0 to be not qualified to choose
    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (HdfsConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L,
        0L, 0L, 0, 0); // no space

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0);
    assertEquals(targets.length, 0);

    targets = chooseTarget(1);
    assertEquals(targets.length, 1);
    assertEquals(storages[1], targets[0]);

    targets = chooseTarget(2);
    assertEquals(targets.length, 2);
    assertEquals(storages[1], targets[0]);
    assertFalse(isOnSameRack(targets[0], targets[1]));

    targets = chooseTarget(3);
    assertEquals(targets.length, 3);
    assertEquals(storages[1], targets[0]);
    assertTrue(isOnSameRack(targets[1], targets[2]));
    assertFalse(isOnSameRack(targets[0], targets[1]));

    targets = chooseTarget(4);
    assertEquals(targets.length, 4);
    assertEquals(storages[1], targets[0]);
    assertTrue(cluster.isNodeGroupAware());
    verifyNoTwoTargetsOnSameNodeGroup(targets);
    assertTrue(isOnSameRack(targets[1], targets[2]) ||
               isOnSameRack(targets[2], targets[3]));

    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }

  /**
   * In this testcase, client is dataNodes[0], but none of the nodes on rack 1
   * is qualified to be chosen. So the 1st replica should be placed on either
   * rack 2 or rack 3. 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 1st replica, but 
   * in different node group.
   * @throws Exception
   */
  @Test
  public void testChooseTarget4() throws Exception {
    // make data node 0-2 to be not qualified to choose: not enough disk space
    for(int i=0; i<3; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0);
    assertEquals(targets.length, 0);

    targets = chooseTarget(1);
    assertEquals(targets.length, 1);
    assertFalse(isOnSameRack(dataNodes[0], targets[0]));

    targets = chooseTarget(2);
    assertEquals(targets.length, 2);
    assertFalse(isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(isOnSameRack(targets[0], targets[1]));

    targets = chooseTarget(3);
    assertEquals(targets.length, 3);
    for(int i=0; i<3; i++) {
      assertFalse(isOnSameRack(dataNodes[0], targets[i]));
    }
    verifyNoTwoTargetsOnSameNodeGroup(targets);
    assertTrue(isOnSameRack(targets[0], targets[1]) ||
               isOnSameRack(targets[1], targets[2]));
    assertFalse(isOnSameRack(targets[0], targets[2]));
  }

  /**
   * In this testcase, client is is a node outside of file system.
   * So the 1st replica can be placed on any node. 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 2nd replica,
   * @throws Exception
   */
  @Test
  public void testChooseTarget5() throws Exception {
    setupDataNodeCapacity();
    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, NODE);
    assertEquals(targets.length, 0);

    targets = chooseTarget(1, NODE);
    assertEquals(targets.length, 1);

    targets = chooseTarget(2, NODE);
    assertEquals(targets.length, 2);
    assertFalse(isOnSameRack(targets[0], targets[1]));

    targets = chooseTarget(3, NODE);
    assertEquals(targets.length, 3);
    assertTrue(isOnSameRack(targets[1], targets[2]));
    assertFalse(isOnSameRack(targets[0], targets[1]));
    verifyNoTwoTargetsOnSameNodeGroup(targets);
  }

  /**
   * This testcase tests re-replication, when dataNodes[0] is already chosen.
   * So the 1st replica can be placed on random rack. 
   * the 2nd replica should be placed on different node and nodegroup by same rack as 
   * the 1st replica. The 3rd replica can be placed randomly.
   * @throws Exception
   */
  @Test
  public void testRereplicate1() throws Exception {
    setupDataNodeCapacity();
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    chosenNodes.add(storages[0]);
    DatanodeStorageInfo[] targets;
    
    targets = chooseTarget(0, chosenNodes);
    assertEquals(targets.length, 0);
    
    targets = chooseTarget(1, chosenNodes);
    assertEquals(targets.length, 1);
    assertFalse(isOnSameRack(dataNodes[0], targets[0]));
    
    targets = chooseTarget(2, chosenNodes);
    assertEquals(targets.length, 2);
    assertTrue(isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(isOnSameRack(targets[0], targets[1]));
    
    targets = chooseTarget(3, chosenNodes);
    assertEquals(targets.length, 3);
    assertTrue(isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(isOnSameNodeGroup(dataNodes[0], targets[0]));
    assertFalse(isOnSameRack(targets[0], targets[2]));
  }

  /**
   * This testcase tests re-replication, 
   * when dataNodes[0] and dataNodes[1] are already chosen.
   * So the 1st replica should be placed on a different rack of rack 1. 
   * the rest replicas can be placed randomly,
   * @throws Exception
   */
  @Test
  public void testRereplicate2() throws Exception {
    setupDataNodeCapacity();
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    chosenNodes.add(storages[0]);
    chosenNodes.add(storages[1]);

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, chosenNodes);
    assertEquals(targets.length, 0);

    targets = chooseTarget(1, chosenNodes);
    assertEquals(targets.length, 1);
    assertFalse(isOnSameRack(dataNodes[0], targets[0]));

    targets = chooseTarget(2, chosenNodes);
    assertEquals(targets.length, 2);
    assertFalse(isOnSameRack(dataNodes[0], targets[0]) && 
        isOnSameRack(dataNodes[0], targets[1]));
  }

  /**
   * This testcase tests re-replication, 
   * when dataNodes[0] and dataNodes[3] are already chosen.
   * So the 1st replica should be placed on the rack that the writer resides. 
   * the rest replicas can be placed randomly,
   * @throws Exception
   */
  @Test
  public void testRereplicate3() throws Exception {
    setupDataNodeCapacity();
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    chosenNodes.add(storages[0]);
    chosenNodes.add(storages[3]);

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, chosenNodes);
    assertEquals(targets.length, 0);

    targets = chooseTarget(1, chosenNodes);
    assertEquals(targets.length, 1);
    assertTrue(isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(isOnSameRack(dataNodes[3], targets[0]));

    targets = chooseTarget(1, dataNodes[3], chosenNodes);
    assertEquals(targets.length, 1);
    assertTrue(isOnSameRack(dataNodes[3], targets[0]));
    assertFalse(isOnSameNodeGroup(dataNodes[3], targets[0]));
    assertFalse(isOnSameRack(dataNodes[0], targets[0]));

    targets = chooseTarget(2, chosenNodes);
    assertEquals(targets.length, 2);
    assertTrue(isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(isOnSameNodeGroup(dataNodes[0], targets[0]));

    targets = chooseTarget(2, dataNodes[3], chosenNodes);
    assertEquals(targets.length, 2);
    assertTrue(isOnSameRack(dataNodes[3], targets[0]));
  }
  
  /**
   * Test for the chooseReplicaToDelete are processed based on 
   * block locality and free space
   */
  @Test
  public void testChooseReplicaToDelete() throws Exception {
    List<DatanodeStorageInfo> replicaList = new ArrayList<DatanodeStorageInfo>();
    final Map<String, List<DatanodeStorageInfo>> rackMap
        = new HashMap<String, List<DatanodeStorageInfo>>();
    dataNodes[0].setRemaining(4*1024*1024);
    replicaList.add(storages[0]);

    dataNodes[1].setRemaining(3*1024*1024);
    replicaList.add(storages[1]);

    dataNodes[2].setRemaining(2*1024*1024);
    replicaList.add(storages[2]);

    dataNodes[5].setRemaining(1*1024*1024);
    replicaList.add(storages[5]);

    List<DatanodeStorageInfo> first = new ArrayList<DatanodeStorageInfo>();
    List<DatanodeStorageInfo> second = new ArrayList<DatanodeStorageInfo>();
    replicator.splitNodesWithRack(
        replicaList, rackMap, first, second);
    assertEquals(3, first.size());
    assertEquals(1, second.size());
    DatanodeStorageInfo chosen = replicator.chooseReplicaToDelete(
        null, null, (short)3, first, second);
    // Within first set {dataNodes[0], dataNodes[1], dataNodes[2]}, 
    // dataNodes[0] and dataNodes[1] are in the same nodegroup, 
    // but dataNodes[1] is chosen as less free space
    assertEquals(chosen, storages[1]);

    replicator.adjustSetsWithChosenReplica(rackMap, first, second, chosen);
    assertEquals(2, first.size());
    assertEquals(1, second.size());
    // Within first set {dataNodes[0], dataNodes[2]}, dataNodes[2] is chosen
    // as less free space
    chosen = replicator.chooseReplicaToDelete(
        null, null, (short)2, first, second);
    assertEquals(chosen, storages[2]);

    replicator.adjustSetsWithChosenReplica(rackMap, first, second, chosen);
    assertEquals(0, first.size());
    assertEquals(2, second.size());
    // Within second set, dataNodes[5] with less free space
    chosen = replicator.chooseReplicaToDelete(
        null, null, (short)1, first, second);
    assertEquals(chosen, storages[5]);
  }
  
  /**
   * Test replica placement policy in case of boundary topology.
   * Rack 2 has only 1 node group & can't be placed with two replicas
   * The 1st replica will be placed on writer.
   * The 2nd replica should be placed on a different rack 
   * The 3rd replica should be placed on the same rack with writer, but on a 
   * different node group.
   */
  @Test
  public void testChooseTargetsOnBoundaryTopology() throws Exception {
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      cluster.remove(dataNodes[i]);
    }

    for(int i=0; i<NUM_OF_DATANODES_BOUNDARY; i++) {
      cluster.add(dataNodesInBoundaryCase[i]);
    }
    for(int i=0; i<NUM_OF_DATANODES_BOUNDARY; i++) {
      updateHeartbeatWithUsage(dataNodes[0],
                2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
                (HdfsConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE,
                0L, 0L, 0L, 0, 0);

      updateHeartbeatWithUsage(dataNodesInBoundaryCase[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, dataNodesInBoundaryCase[0]);
    assertEquals(targets.length, 0);
    
    targets = chooseTarget(1, dataNodesInBoundaryCase[0]);
    assertEquals(targets.length, 1);

    targets = chooseTarget(2, dataNodesInBoundaryCase[0]);
    assertEquals(targets.length, 2);
    assertFalse(isOnSameRack(targets[0], targets[1]));
    
    targets = chooseTarget(3, dataNodesInBoundaryCase[0]);
    assertEquals(targets.length, 3);
    assertTrue(checkTargetsOnDifferentNodeGroup(targets));
  }

  /**
   * Test re-replication policy in boundary case.
   * Rack 2 has only one node group & the node in this node group is chosen
   * Rack 1 has two nodegroups & one of them is chosen.
   * Replica policy should choose the node from node group of Rack1 but not the
   * same nodegroup with chosen nodes.
   */
  @Test
  public void testRereplicateOnBoundaryTopology() throws Exception {
    for(int i=0; i<NUM_OF_DATANODES_BOUNDARY; i++) {
      updateHeartbeatWithUsage(dataNodesInBoundaryCase[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    chosenNodes.add(storagesInBoundaryCase[0]);
    chosenNodes.add(storagesInBoundaryCase[5]);
    DatanodeStorageInfo[] targets;
    targets = chooseTarget(1, dataNodesInBoundaryCase[0], chosenNodes);
    assertFalse(isOnSameNodeGroup(dataNodesInBoundaryCase[0], targets[0]));
    assertFalse(isOnSameNodeGroup(dataNodesInBoundaryCase[5], targets[0]));
    assertTrue(checkTargetsOnDifferentNodeGroup(targets));
  }
  
  /**
   * Test replica placement policy in case of targets more than number of 
   * NodeGroups.
   * The 12-nodes cluster only has 6 NodeGroups, but in some cases, like: 
   * placing submitted job file, there is requirement to choose more (10) 
   * targets for placing replica. We should test it can return 6 targets.
   */
  @Test
  public void testChooseMoreTargetsThanNodeGroups() throws Exception {
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      cluster.remove(dataNodes[i]);
    }
    for(int i=0; i<NUM_OF_DATANODES_BOUNDARY; i++) {
      DatanodeDescriptor node = dataNodesInBoundaryCase[i];
      if (cluster.contains(node)) {
        cluster.remove(node);
      }
    }

    for(int i=0; i<NUM_OF_DATANODES_MORE_TARGETS; i++) {
      cluster.add(dataNodesInMoreTargetsCase[i]);
    }

    for(int i=0; i<NUM_OF_DATANODES_MORE_TARGETS; i++) {
      updateHeartbeatWithUsage(dataNodesInMoreTargetsCase[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }

    DatanodeStorageInfo[] targets;
    // Test normal case -- 3 replicas
    targets = chooseTarget(3, dataNodesInMoreTargetsCase[0]);
    assertEquals(targets.length, 3);
    assertTrue(checkTargetsOnDifferentNodeGroup(targets));

    // Test special case -- replica number over node groups.
    targets = chooseTarget(10, dataNodesInMoreTargetsCase[0]);
    assertTrue(checkTargetsOnDifferentNodeGroup(targets));
    // Verify it only can find 6 targets for placing replicas.
    assertEquals(targets.length, 6);
  }

  @Test
  public void testChooseTargetWithDependencies() throws Exception {
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      cluster.remove(dataNodes[i]);
    }
    
    for(int i=0; i<NUM_OF_DATANODES_MORE_TARGETS; i++) {
      DatanodeDescriptor node = dataNodesInMoreTargetsCase[i];
      if (cluster.contains(node)) {
        cluster.remove(node);
      }
    }
    
    Host2NodesMap host2DatanodeMap = namenode.getNamesystem()
        .getBlockManager()
        .getDatanodeManager().getHost2DatanodeMap();
    for(int i=0; i<NUM_OF_DATANODES_FOR_DEPENDENCIES; i++) {
      cluster.add(dataNodesForDependencies[i]);
      host2DatanodeMap.add(dataNodesForDependencies[i]);
    }
    
    //add dependencies (node1 <-> node2, and node3<->node4)
    dataNodesForDependencies[1].addDependentHostName(
        dataNodesForDependencies[2].getHostName());
    dataNodesForDependencies[2].addDependentHostName(
        dataNodesForDependencies[1].getHostName());
    dataNodesForDependencies[3].addDependentHostName(
        dataNodesForDependencies[4].getHostName());
    dataNodesForDependencies[4].addDependentHostName(
        dataNodesForDependencies[3].getHostName());
    
    //Update heartbeat
    for(int i=0; i<NUM_OF_DATANODES_FOR_DEPENDENCIES; i++) {
      updateHeartbeatWithUsage(dataNodesForDependencies[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
    
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    
    DatanodeStorageInfo[] targets;
    Set<Node> excludedNodes = new HashSet<Node>();
    excludedNodes.add(dataNodesForDependencies[5]);
    
    //try to select three targets as there are three node groups
    targets = chooseTarget(3, dataNodesForDependencies[1], chosenNodes, excludedNodes);
    
    //Even there are three node groups, verify that 
    //only two targets are selected due to dependencies
    assertEquals(targets.length, 2);
    assertEquals(targets[0], storagesForDependencies[1]);
    assertTrue(targets[1].equals(storagesForDependencies[3]) || targets[1].equals(storagesForDependencies[4]));
    
    //verify that all data nodes are in the excluded list
    assertEquals(excludedNodes.size(), NUM_OF_DATANODES_FOR_DEPENDENCIES);
    for(int i=0; i<NUM_OF_DATANODES_FOR_DEPENDENCIES; i++) {
      assertTrue(excludedNodes.contains(dataNodesForDependencies[i]));
    }
  }
}
