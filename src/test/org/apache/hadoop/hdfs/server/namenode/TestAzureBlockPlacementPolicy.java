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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

public class TestAzureBlockPlacementPolicy extends TestCase {
  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 7;
  private static final Configuration CONF = new Configuration();
  private static final NetworkTopology cluster;
  private static final NameNode namenode;
  private static final BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";
  private static final Block dummyBlock = new Block();
  private static final DatanodeDescriptor dataNodes[] = new DatanodeDescriptor[] {
      new DatanodeDescriptor(new DatanodeID("h1:5020"), "/fd0/ud0"),
      new DatanodeDescriptor(new DatanodeID("h2:5020"), "/fd0/ud1"),
      new DatanodeDescriptor(new DatanodeID("h3:5020"), "/fd0/ud2"),
      new DatanodeDescriptor(new DatanodeID("h4:5020"), "/fd1/ud0"),
      new DatanodeDescriptor(new DatanodeID("h5:5020"), "/fd1/ud1"),
      new DatanodeDescriptor(new DatanodeID("h6:5020"), "/fd1/ud2"),
      new DatanodeDescriptor(new DatanodeID("h7:5020"), "/fd0/ud0") };

  // using meaningful names for the above so that they are more readable
  private static final DatanodeDescriptor NODE1_FD0_UD0 = dataNodes[0];
  private static final DatanodeDescriptor NODE_FD0_UD1 = dataNodes[1];
  private static final DatanodeDescriptor NODE_FD0_UD2 = dataNodes[2];
  private static final DatanodeDescriptor NODE_FD1_UD0 = dataNodes[3];
  private static final DatanodeDescriptor NODE_FD1_UD1 = dataNodes[4];
  private static final DatanodeDescriptor NODE_FD1_UD2 = dataNodes[5];
  private static final DatanodeDescriptor NODE2_FD0_UD0 = dataNodes[6];

  private final static DatanodeDescriptor NODE_FD1_UD3 = new DatanodeDescriptor(
      new DatanodeID("h8:5020"), "/fd1/ud3");

  static {
    try {
      FileSystem.setDefaultUri(CONF, "hdfs://localhost:0");
      CONF.set("dfs.http.address", "0.0.0.0:0");
      CONF.set("dfs.block.replicator.classname",
          "org.apache.hadoop.hdfs.server.namenode.AzureBlockPlacementPolicy");
      NameNode.format(CONF);
      namenode = new NameNode(CONF);
    } catch (IOException e) {
      e.printStackTrace();
      throw (RuntimeException) new RuntimeException().initCause(e);
    }
    FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
    replicator = fsNamesystem.replicator;
    cluster = fsNamesystem.clusterMap;
    // construct network topology
    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
    }
    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      dataNodes[i].updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
          * BLOCK_SIZE, 0L, 2 * FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
          0);
    }
  }

  /**
   * In this test case, client is NODE1_FD0_UD0. So the 1st replica should be
   * placed on NODE1_FD0_UD0, the 2nd replica should be placed in a different
   * upgrade and fault domain and third should be placed in an upgrade domain
   * different from the first two.
   */
  public void testChooseTarget1() throws Exception {
    NODE1_FD0_UD0.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
        * BLOCK_SIZE, 0L, FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 4); // overloaded

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], NODE1_FD0_UD0);

    targets = replicator.chooseTarget(filename, 2, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], NODE1_FD0_UD0);
    assertTrue(AreFaultAndUpgradeDomainsDifferent(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 3, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], NODE1_FD0_UD0);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 3) == 0);

    targets = replicator.chooseTarget(filename, 4, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], NODE1_FD0_UD0);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 4) == 0);
    
    // Choose 7 targets. Should be able to return 7 nodes
    targets = replicator.chooseTarget(filename, NUM_OF_DATANODES,
        NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, NUM_OF_DATANODES);
    assertEquals(targets[0], NODE1_FD0_UD0);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 7) == 0);

    NODE1_FD0_UD0.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
        * BLOCK_SIZE, 0L, FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0);
  }

  /**
   * In this test case, client is NODE1_FD0_UD0, but the NODE_FD1_UD0 is not
   * allowed to be chosen. So the 1st replica should be placed on NODE1_FD0_UD0,
   * the 2nd replica should be placed on a different fault and upgrade domain
   * from the first, the 3rd should be on an upgrade domain different from the
   * first and second
   * 
   * @throws Exception
   */
  public void testChooseTarget2() throws Exception {
    List<Node> invalidNodes;
    HashMap<Node, Node> excludedNodes;
    DatanodeDescriptor[] targets;
    invalidNodes = new ArrayList<Node>();
    invalidNodes.add(NODE_FD1_UD0);
    BlockPlacementPolicyDefault repl = (BlockPlacementPolicyDefault) replicator;
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    excludedNodes = getMap(invalidNodes);
    targets = repl.chooseTarget(0, NODE1_FD0_UD0, chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 0);

    excludedNodes = getMap(invalidNodes);
    chosenNodes.clear();
    targets = repl.chooseTarget(1, NODE1_FD0_UD0, chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], NODE1_FD0_UD0);
    assertFalse(areInvalidNodesSelected(invalidNodes, targets));

    excludedNodes = getMap(invalidNodes);
    chosenNodes.clear();
    targets = repl.chooseTarget(2, NODE1_FD0_UD0, chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], NODE1_FD0_UD0);
    assertFalse(areInvalidNodesSelected(invalidNodes, targets));
    assertTrue(AreFaultAndUpgradeDomainsDifferent(targets[0], targets[1]));

    excludedNodes = getMap(invalidNodes);
    chosenNodes.clear();
    targets = repl.chooseTarget(3, NODE1_FD0_UD0, chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], NODE1_FD0_UD0);
    assertFalse(areInvalidNodesSelected(invalidNodes, targets));
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 3) == 0);

    excludedNodes = getMap(invalidNodes);
    chosenNodes.clear();
    targets = repl.chooseTarget(4, NODE1_FD0_UD0, chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], NODE1_FD0_UD0);
    assertFalse(areInvalidNodesSelected(invalidNodes, targets));
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 4) == 0);
    
    // Choose 7 targets. Should be able to return 6 nodes
    excludedNodes = getMap(invalidNodes);
    chosenNodes.clear();
    targets = repl.chooseTarget(NUM_OF_DATANODES, NODE1_FD0_UD0, chosenNodes,
        excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, NUM_OF_DATANODES - 1);
    assertEquals(targets[0], NODE1_FD0_UD0);
  }

  /**
   * In this test case, client is NODE1_FD0_UD0, but NODE1_FD0_UD0 is not
   * qualified to be chosen. So the 1st replica should be placed on
   * NODE_FD0_UD1, the 2nd replica should be placed on a different fault and
   * upgrade domain, the 3rd replica should be placed on an upgrade domain
   * different from the first two
   * 
   * @throws Exception
   */
  public void testChooseTarget3() throws Exception {
    // make data node 0 to be not qualified to choose
    NODE1_FD0_UD0.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
        * BLOCK_SIZE, 0L, (FSConstants.MIN_BLOCKS_FOR_WRITE - 1) * BLOCK_SIZE,
        0); // no space

    // nodes that should not be selected
    ArrayList<Node> invalidNodes = new ArrayList<Node>();
    invalidNodes.add(NODE1_FD0_UD0);

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(areInvalidNodesSelected(invalidNodes, targets));

    targets = replicator.chooseTarget(filename, 2, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(areInvalidNodesSelected(invalidNodes, targets));
    assertTrue(AreFaultAndUpgradeDomainsDifferent(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 3, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertFalse(areInvalidNodesSelected(invalidNodes, targets));
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 3) == 0);

    targets = replicator.chooseTarget(filename, 4, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertFalse(areInvalidNodesSelected(invalidNodes, targets));
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 3) == 0);

    NODE1_FD0_UD0.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
        * BLOCK_SIZE, 0L, FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0);
  }

  /**
   * In this testcase, client is NODE1_FD0_UD0, but none of the nodes on fault
   * domain 0 and upgrade domain 0 is qualified to be chosen. So the 1st replica
   * should be placed on other fault and upgrade domains. the 2nd replica should
   * be placed on a fault and upgrade domain different from the first and the
   * 3rd replica should be placed on an upgrade domain different from both.
   * 
   * @throws Exception
   */
  public void testChooseTarget4() throws Exception {
    // make data node 0 & 6 to be not qualified to choose: not enough disk space

    NODE1_FD0_UD0.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
        * BLOCK_SIZE, 0L, (FSConstants.MIN_BLOCKS_FOR_WRITE - 1) * BLOCK_SIZE,
        0);
    NODE2_FD0_UD0.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
        * BLOCK_SIZE, 0L, (FSConstants.MIN_BLOCKS_FOR_WRITE - 1) * BLOCK_SIZE,
        0);

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(targets[0], NODE1_FD0_UD0));

    targets = replicator.chooseTarget(filename, 2, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(targets[0], NODE1_FD0_UD0));
    assertTrue(AreFaultAndUpgradeDomainsDifferent(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 3, NODE1_FD0_UD0, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 3) == 0);

    NODE1_FD0_UD0.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
        * BLOCK_SIZE, 0L, FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0);
    NODE2_FD0_UD0.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
        * BLOCK_SIZE, 0L, FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0);
  }

  /**
   * In this testcase, client is a node outside of file system. So the 1st
   * replica can be placed on any node. the 2nd replica should be placed on a
   * fault and upgrade domain different from the first and the 3rd replica
   * should be placed on an upgrade domain different from both.
   * 
   * @throws Exception
   */
  public void testChooseTarget5() throws Exception {
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, NODE_FD1_UD3, BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE_FD1_UD3, BLOCK_SIZE);
    assertEquals(targets.length, 1);

    targets = replicator.chooseTarget(filename, 2, NODE_FD1_UD3, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(AreFaultAndUpgradeDomainsDifferent(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 3, NODE_FD1_UD3, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(targets), (short) 3) == 0);
  }

  /**
   * This testcase tests re-replication, when NODE1_FD0_UD0 is already chosen.
   * the 1st replica should be placed on a fault and upgrade domain different
   * from the first and the 2nd replica should be placed on an upgrade domain
   * different from both.The 3rd replica can be placed randomly.
   * 
   * @throws Exception
   */
  public void testRereplicate1() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(NODE1_FD0_UD0);
    DatanodeDescriptor[] targets;

    targets = replicator.chooseTarget(filename, 0, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(AreFaultAndUpgradeDomainsDifferent(NODE1_FD0_UD0, targets[0]));

    targets = replicator.chooseTarget(filename, 2, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(getSelectedNodes(chosenNodes, targets)), (short) 3) == 0);

    targets = replicator.chooseTarget(filename, 3, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(getSelectedNodes(chosenNodes, targets)), (short) 4) == 0);
  }

  /**
   * This test case tests re-replication, when NODE1_FD0_UD0 and NODE_FD0_UD1
   * are already chosen. So the 1st replica should be placed in an upgrade and
   * fault domain different from both and the other replicas can be placed
   * randomly.
   * 
   * @throws Exception
   */
  public void testRereplicate2() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(NODE1_FD0_UD0);
    chosenNodes.add(NODE_FD0_UD1);

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 1);

    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(getSelectedNodes(chosenNodes, targets)), (short) 3) == 0);

    targets = replicator.chooseTarget(filename, 2, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(getSelectedNodes(chosenNodes, targets)), (short) 4) == 0);
  }

  /**
   * This testcase tests re-replication, when NODE1_FD0_UD0 and NODE_FD0_UD2 are
   * already chosen. So the 1st replica should be placed on an upgrade domain
   * different from both.
   * 
   * @throws Exception
   */
  public void testRereplicate3() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(NODE1_FD0_UD0);
    chosenNodes.add(NODE_FD0_UD2);

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(getSelectedNodes(chosenNodes, targets)), (short) 3) == 0);

    targets = replicator.chooseTarget(filename, 2, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(getSelectedNodes(chosenNodes, targets)), (short) 4) == 0);
  }

  /**
   * This testcase tests re-replication, when NODE1_FD0_UD0 and NODE2_FD0_UD0
   * are already chosen. So the 1st replica should be placed on a different
   * upgrade and fault domain. The 2nd replica should be placed on an upgrade
   * domain different from previous replicas
   * 
   * @throws Exception
   */
  public void testRereplicate4() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(NODE1_FD0_UD0);
    chosenNodes.add(NODE2_FD0_UD0);

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(getSelectedNodes(chosenNodes, targets)), (short) 3) == 1);

    targets = replicator.chooseTarget(filename, 2, NODE1_FD0_UD0, chosenNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(replicator.verifyBlockPlacement(filename,
        getLocatedBlock(getSelectedNodes(chosenNodes, targets)), (short) 4) == 0);
  }

  /**
   * tests canDelete method for the following cases a) nodes in same network
   * location b) nodes in same upgrade domains c) nodes in same fault domains d)
   * nodes in similar fault and upgrade domains with high replication e) nodes
   * in different fault and upgrade domains with high replication
   */
  public void testCanDelete() {

    AzureBlockPlacementPolicy policy = (AzureBlockPlacementPolicy) replicator;
    short replication = 3;
    List<DatanodeDescriptor> nodesWithReplicas = new ArrayList<DatanodeDescriptor>();
    nodesWithReplicas.add(NODE1_FD0_UD0);
    nodesWithReplicas.add(NODE2_FD0_UD0);
    nodesWithReplicas.add(NODE_FD0_UD1);

    // NODE1_FD0_UD0 can be deleted as it is in the same network location
    // as datanode[6].NODE_FD0_UD1 cannot be deleted as its removal decreases
    // the number of fault and upgrade domains containing the replicas.
    assertTrue(policy.canDelete(NODE1_FD0_UD0, nodesWithReplicas, replication));
    assertFalse(policy.canDelete(NODE_FD0_UD1, nodesWithReplicas, replication));
    assertTrue(policy.canDelete(NODE2_FD0_UD0, nodesWithReplicas, replication));

    nodesWithReplicas.clear();
    nodesWithReplicas.add(NODE1_FD0_UD0);
    nodesWithReplicas.add(NODE_FD0_UD1);
    nodesWithReplicas.add(NODE_FD1_UD0);

    // NODE1_FD0_UD0 can be deleted as its removal does not decrease
    // the number of fault or upgrade domains containing the replicas.
    // NODE_FD0_UD1 cannot be deleted as it would
    // decrease the no of upgrade domains containing the replica.
    // NODE_FD1_UD0 cannot be deleted as it would
    // decrease the no of fault domains containing the replica.
    assertTrue(policy.canDelete(NODE1_FD0_UD0, nodesWithReplicas, replication));
    assertFalse(policy.canDelete(NODE_FD0_UD1, nodesWithReplicas, replication));
    assertFalse(policy.canDelete(NODE_FD1_UD0, nodesWithReplicas, replication));

    nodesWithReplicas.clear();
    nodesWithReplicas.add(NODE1_FD0_UD0);
    nodesWithReplicas.add(NODE_FD0_UD1);
    nodesWithReplicas.add(NODE_FD0_UD2);
    nodesWithReplicas.add(NODE_FD1_UD0);

    // Though the replication factor is greater than 3,
    // datanodes 1,2 cannot be deleted as their removal decreases
    // the number of upgrade domains containing the replicas below the min
    // upgrade domains criteria.
    // NODE_FD1_UD0 cannot be deleted as the removal would
    // decrease the no of fault domains containing the replicas.
    assertTrue(policy.canDelete(NODE1_FD0_UD0, nodesWithReplicas, replication));
    assertFalse(policy.canDelete(NODE_FD0_UD1, nodesWithReplicas, replication));
    assertFalse(policy.canDelete(NODE_FD0_UD2, nodesWithReplicas, replication));
    assertFalse(policy.canDelete(NODE_FD1_UD0, nodesWithReplicas, replication));

    nodesWithReplicas.clear();
    nodesWithReplicas.add(NODE1_FD0_UD0);
    nodesWithReplicas.add(NODE_FD0_UD2);
    nodesWithReplicas.add(NODE_FD1_UD1);
    cluster.add(NODE_FD1_UD3);
    nodesWithReplicas.add(NODE_FD1_UD3);

    // any node can be deleted because, though it could
    // decrease the number of fault and upgrade domains containing
    // the replicas, the min criteria of 2 fault and 3 upgrade
    // domains is being satisfied.
    assertTrue(policy.canDelete(NODE1_FD0_UD0, nodesWithReplicas, replication));
    assertTrue(policy.canDelete(NODE_FD0_UD2, nodesWithReplicas, replication));
    assertTrue(policy.canDelete(NODE_FD1_UD1, nodesWithReplicas, replication));
    assertTrue(policy.canDelete(NODE_FD1_UD3, nodesWithReplicas, replication));

    nodesWithReplicas.clear();
    nodesWithReplicas.add(NODE_FD1_UD0);
    nodesWithReplicas.add(NODE_FD1_UD1);
    nodesWithReplicas.add(NODE_FD1_UD2);
    nodesWithReplicas.add(NODE_FD1_UD3);

    // any node can be deleted because, though it could
    // decrease the number of fault and upgrade domains containing
    // the replicas, the min criteria of (min of 2 or orginal no of
    // fault domains) and (min of 3 or original no of upgrade
    // domains) is being satisfied.
    assertTrue(policy.canDelete(NODE_FD1_UD0, nodesWithReplicas, replication));
    assertTrue(policy.canDelete(NODE_FD1_UD1, nodesWithReplicas, replication));
    assertTrue(policy.canDelete(NODE_FD1_UD2, nodesWithReplicas, replication));
    assertTrue(policy.canDelete(NODE_FD1_UD3, nodesWithReplicas, replication));

    cluster.remove(NODE_FD1_UD3);

    replication = 1;
    nodesWithReplicas.clear();
    nodesWithReplicas.add(NODE1_FD0_UD0);
    nodesWithReplicas.add(NODE_FD0_UD1);

    // Any node can be deleted as replication factor is just 1
    assertTrue(policy.canDelete(NODE1_FD0_UD0, nodesWithReplicas, replication));
    assertTrue(policy.canDelete(NODE_FD0_UD1, nodesWithReplicas, replication));

    replication = 2;
    nodesWithReplicas.clear();
    nodesWithReplicas.add(NODE1_FD0_UD0);
    nodesWithReplicas.add(NODE_FD0_UD1);
    nodesWithReplicas.add(NODE_FD1_UD0);

    // NODE1_FD0_UD0 can be deleted as it does not decrease the number of
    // upgrade
    // domains, containing the replica whereas NODE_FD0_UD1 cannot be as it
    // would decrease the number of upgrade domains. NODE_FD1_UD0 cannot be
    // deleted as it would decrease the fault domains.
    assertTrue(policy.canDelete(NODE1_FD0_UD0, nodesWithReplicas, replication));
    assertFalse(policy.canDelete(NODE_FD0_UD1, nodesWithReplicas, replication));
    assertFalse(policy.canDelete(NODE_FD1_UD0, nodesWithReplicas, replication));
  }

  /**
   * Tests can move for every possible node when replicas are spread as per the
   * default placement i.e. across 2 fault and 3 upgrade domains
   */
  public void testcanMove1() {
    List<DatanodeInfo> blockLocations = new ArrayList<DatanodeInfo>();
    blockLocations.add(NODE1_FD0_UD0);
    blockLocations.add(NODE_FD0_UD1);
    blockLocations.add(NODE_FD1_UD2);
    cluster.add(NODE_FD1_UD3);

    // check if you can move NODE1_FD0_UD0 to various nodes
    assertFalse(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD0_UD1,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD0_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD0,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD1,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE2_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD3,
        blockLocations));

    // check if you can move NODE_FD0_UD1 to various nodes
    assertFalse(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE1_FD0_UD0,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD0_UD2,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD1,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD2,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE2_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD3,
        blockLocations));

    // check if you can move NODE_FD1_UD2 to various nodes
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD2, NODE1_FD0_UD0,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD2, NODE_FD0_UD1,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD2, NODE_FD0_UD2,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD2, NODE_FD1_UD0,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD2, NODE_FD1_UD1,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD2, NODE2_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD1_UD2, NODE_FD1_UD3,
        blockLocations));

    cluster.remove(NODE_FD1_UD3);
  }

  /**
   * Tests can move for every possible node when replicas have lesser spread
   * than the default placement i.e. across 1 fault and 2 upgrade domains
   */
  public void testcanMove2() {
    List<DatanodeInfo> blockLocations = new ArrayList<DatanodeInfo>();
    // replicas are spread across 1 fault domain and 2 upgrade domains
    blockLocations.add(NODE1_FD0_UD0);
    blockLocations.add(NODE_FD0_UD1);
    cluster.add(NODE_FD1_UD3);

    // check if you can move NODE1_FD0_UD0 to various nodes
    assertFalse(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD0_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD0_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD0,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE2_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD3,
        blockLocations));

    // check if you can move NODE_FD0_UD1 to various nodes
    assertFalse(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE1_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD0_UD2,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD2,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE2_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD3,
        blockLocations));

    cluster.remove(NODE_FD1_UD3);
  }

  /**
   * Tests can move for every possible node when replicas have more spread than
   * the default placement i.e. across 2 fault and 4 upgrade domains
   */
  public void testcanMove3() {
    List<DatanodeInfo> blockLocations = new ArrayList<DatanodeInfo>();
    // replicas are spread across 2 fault domains and 4 upgrade domains
    blockLocations.add(NODE1_FD0_UD0);
    blockLocations.add(NODE_FD0_UD1);
    blockLocations.add(NODE_FD0_UD2);
    blockLocations.add(NODE_FD1_UD3);
    cluster.add(NODE_FD1_UD3);

    // check if you can move NODE1_FD0_UD0 to various nodes
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD0_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD0_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE2_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE1_FD0_UD0, NODE_FD1_UD3,
        blockLocations));

    // check if you can move NODE_FD0_UD1 to various nodes
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE1_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD0_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE2_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD1, NODE_FD1_UD3,
        blockLocations));

    // check if you can move NODE_FD0_UD2 to various nodes
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD2, NODE1_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD2, NODE_FD0_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD2, NODE_FD1_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD2, NODE_FD1_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD2, NODE_FD1_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD2, NODE2_FD0_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD0_UD2, NODE_FD1_UD3,
        blockLocations));

    // check if you can move NODE to various nodes
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD3, NODE1_FD0_UD0,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD3, NODE_FD0_UD1,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD3, NODE_FD0_UD2,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD1_UD3, NODE_FD1_UD0,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD1_UD3, NODE_FD1_UD1,
        blockLocations));
    assertTrue(replicator.canMove(dummyBlock, NODE_FD1_UD3, NODE_FD1_UD2,
        blockLocations));
    assertFalse(replicator.canMove(dummyBlock, NODE_FD1_UD3, NODE2_FD0_UD0,
        blockLocations));

    cluster.remove(NODE_FD1_UD3);
  }

  // This is a sanity test to ensure that
  // when there is no script to map ips to racks or the mapping script fails
  // and default-racks are used for all nodes, the azure placement policy
  // handles it gracefully for various replication factors.
  // This tests the replication, re-replication, deletion
  // and movement of nodes for replication factors 1 to 4
  public void testPolicyWithDefaultRacks() {
    // clear the old topology
    for (Node node : dataNodes) {
      cluster.remove(node);
    }

    // create a new topology with same racks for all nodes
    String rackLocation = NetworkTopology.DEFAULT_RACK;

    DatanodeDescriptor newDataNodes[] = new DatanodeDescriptor[] {
        new DatanodeDescriptor(new DatanodeID("n1:5020"), rackLocation),
        new DatanodeDescriptor(new DatanodeID("n2:5020"), rackLocation),
        new DatanodeDescriptor(new DatanodeID("n3:5020"), rackLocation),
        new DatanodeDescriptor(new DatanodeID("n4:5020"), rackLocation),
        new DatanodeDescriptor(new DatanodeID("n5:5020"), rackLocation) };

    for (DatanodeDescriptor node : newDataNodes) {
      cluster.add(node);
      node.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
          0L, 2 * FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0);
    }

    for (int replication = 1; replication < newDataNodes.length - 1; replication++) {

      // test replication
      DatanodeDescriptor[] targets;
      targets = replicator.chooseTarget(filename, replication, newDataNodes[0],
          BLOCK_SIZE);
      assertEquals(targets.length, replication);

      assertTrue(replicator.verifyBlockPlacement(filename,
          getLocatedBlock(targets), (short) replication) == 0);

      // test re-replication
      List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
      chosenNodes.add(newDataNodes[0]);
      targets = replicator.chooseTarget(filename, replication - 1,
          newDataNodes[0], chosenNodes, BLOCK_SIZE);
      assertEquals(targets.length, replication - 1);

      // test deletion and movement
      List<DatanodeDescriptor> nodesWithReplicas = new ArrayList<DatanodeDescriptor>();
      List<DatanodeInfo> blockLocations = new ArrayList<DatanodeInfo>();
      for (DatanodeDescriptor dn : newDataNodes) {
        nodesWithReplicas.add(dn);
        blockLocations.add(dn);
      }

      for (int i = 0; i < newDataNodes.length; i++) {
        // test deletion
        assertTrue(((AzureBlockPlacementPolicy) replicator).canDelete(
            newDataNodes[i], nodesWithReplicas, (short) replication));

        // just pick the next node for move. all of them are equivalent
        DatanodeInfo target = newDataNodes[(i + 1) % newDataNodes.length];

        // test can move
        replicator.canMove(dummyBlock, newDataNodes[i], target, blockLocations);
      }
    }

    // clear the new topology
    for (Node node : newDataNodes) {
      cluster.remove(node);
    }

    // get back to original topology
    for (DatanodeDescriptor node : dataNodes) {
      cluster.add(node);
      node.updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
          0L, 2 * FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0);
    }
  }

  private static boolean AreFaultAndUpgradeDomainsDifferent(
      DatanodeDescriptor d1, DatanodeDescriptor d2) {
    assert ((d1 != null) && (d2 != null));

    Node parent1 = d1.getParent();
    Node parent2 = d2.getParent();

    assert ((parent1 != null) && (parent2 != null));

    Node grandParent1 = parent1.getParent();
    Node grandParent2 = parent2.getParent();

    assert ((grandParent1 != null) && (grandParent2 != null));

    if ((!parent1.getName().equals(parent2.getName()))
        && (!grandParent1.getName().equals(grandParent2.getName()))) {
      return true;
    }

    return false;
  }

  private static LocatedBlock getLocatedBlock(DatanodeDescriptor[] locations) {
    LocatedBlock block = new LocatedBlock(new Block(), locations);
    return block;
  }

  private static boolean areInvalidNodesSelected(List<Node> invalidNodes,
      DatanodeDescriptor[] targets) {
    TreeSet<Node> dataNodeSet = new TreeSet<Node>();
    for (Node dn : invalidNodes) {
      dataNodeSet.add(dn);
    }

    for (DatanodeDescriptor dnd : targets) {
      if (dataNodeSet.contains(dnd))
        return true;
    }
    return false;
  }

  private static DatanodeDescriptor[] getSelectedNodes(
      List<DatanodeDescriptor> chooseNodes, DatanodeDescriptor[] targets) {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[chooseNodes.size()
        + targets.length];
    int i = 0;
    for (DatanodeDescriptor dn : chooseNodes) {
      datanodes[i++] = dn;
    }

    for (int j = 0; j < targets.length; j++) {
      datanodes[i++] = targets[j];
    }
    return datanodes;
  }

  private static HashMap<Node, Node> getMap(List<Node> nodes) {
    HashMap<Node, Node> map = new HashMap<Node, Node>();
    for (Node node : nodes) {
      map.put(node, node);
    }
    return map;
  }
}

