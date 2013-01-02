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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.junit.Test;

public class TestReplicationPolicyWithNodeGroup {
  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 8;
  private static final int NUM_OF_DATANODES_BOUNDARY = 6;
  private static final int NUM_OF_DATANODES_MORE_TARGETS = 12;
  private static final Configuration CONF = new Configuration();
  private static final NetworkTopology cluster;
  private static final NameNode namenode;
  private static final BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";

  private final static DatanodeDescriptor dataNodes[] = new DatanodeDescriptor[] {
      DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1/n1"),
      DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1/n1"),
      DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r1/n2"),
      DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/d1/r2/n3"),
      DFSTestUtil.getDatanodeDescriptor("5.5.5.5", "/d1/r2/n3"),
      DFSTestUtil.getDatanodeDescriptor("6.6.6.6", "/d1/r2/n4"),
      DFSTestUtil.getDatanodeDescriptor("7.7.7.7", "/d2/r3/n5"),
      DFSTestUtil.getDatanodeDescriptor("8.8.8.8", "/d2/r3/n6")
  };
  
  private final static DatanodeDescriptor dataNodesInBoundaryCase[] = 
          new DatanodeDescriptor[] {
      DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1/n1"),
      DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1/n1"),
      DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r1/n1"),
      DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/d1/r1/n2"),
      DFSTestUtil.getDatanodeDescriptor("5.5.5.5", "/d1/r2/n3"),
      DFSTestUtil.getDatanodeDescriptor("6.6.6.6", "/d1/r2/n3")
  };
  
  private final static DatanodeDescriptor dataNodesInMoreTargetsCase[] =
          new DatanodeDescriptor[] {
      DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/r1/n1"),
      DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/r1/n1"),
      DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/r1/n2"),
      DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/r1/n2"),
      DFSTestUtil.getDatanodeDescriptor("5.5.5.5", "/r1/n3"),
      DFSTestUtil.getDatanodeDescriptor("6.6.6.6", "/r1/n3"),
      DFSTestUtil.getDatanodeDescriptor("7.7.7.7", "/r2/n4"),
      DFSTestUtil.getDatanodeDescriptor("8.8.8.8", "/r2/n4"),
      DFSTestUtil.getDatanodeDescriptor("9.9.9.9", "/r2/n5"),
      DFSTestUtil.getDatanodeDescriptor("10.10.10.10", "/r2/n5"),
      DFSTestUtil.getDatanodeDescriptor("11.11.11.11", "/r2/n6"),
      DFSTestUtil.getDatanodeDescriptor("12.12.12.12", "/r2/n6"),
  };

  private final static DatanodeDescriptor NODE = 
      new DatanodeDescriptor(DFSTestUtil.getDatanodeDescriptor("9.9.9.9", "/d2/r4/n7"));

  static {
    try {
      FileSystem.setDefaultUri(CONF, "hdfs://localhost:0");
      CONF.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      // Set properties to make HDFS aware of NodeGroup.
      CONF.set("dfs.block.replicator.classname", 
          "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyWithNodeGroup");
      CONF.set("net.topology.impl", 
          "org.apache.hadoop.net.NetworkTopologyWithNodeGroup");
     
      File baseDir = new File(System.getProperty(
           "test.build.data", "build/test/data"), "dfs/");
      CONF.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
           new File(baseDir, "name").getPath());
           
      NameNode.format(CONF);
      namenode = new NameNode(CONF);
    } catch (IOException e) {
      e.printStackTrace();
      throw (RuntimeException)new RuntimeException().initCause(e);
    }
    FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
    replicator = fsNamesystem.replicator;
    cluster = fsNamesystem.clusterMap;
    // construct network topology
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
    }
    setupDataNodeCapacity();
  }

  private static void setupDataNodeCapacity() {
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0);
    }
  }
  
  /**
   * Scan the targets list: all targets should be on different NodeGroups.
   * Return false if two targets are found on the same NodeGroup.
   */
  private static boolean checkTargetsOnDifferentNodeGroup(
      DatanodeDescriptor[] targets) {
    if(targets.length == 0)
      return true;
    Set<String> targetSet = new HashSet<String>();
    for(DatanodeDescriptor node:targets) {
      String nodeGroup = NetworkTopology.getLastHalf(node.getNetworkLocation());
      if(targetSet.contains(nodeGroup)) {
        return false;
      } else {
        targetSet.add(nodeGroup);
      }
    }
    return true;
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
    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 4); // overloaded

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[0]);

    targets = replicator.chooseTarget(filename, 2, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 3, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameNodeGroup(targets[1], targets[2]));

    targets = replicator.chooseTarget(filename, 4, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[0]);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
    // Make sure no more than one replicas are on the same nodegroup 
    verifyNoTwoTargetsOnSameNodeGroup(targets);

    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0); 
  }

  private void verifyNoTwoTargetsOnSameNodeGroup(DatanodeDescriptor[] targets) {
    Set<String> nodeGroupSet = new HashSet<String>();
    for (DatanodeDescriptor target: targets) {
      nodeGroupSet.add(target.getNetworkLocation());
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
    HashMap<Node, Node> excludedNodes;
    DatanodeDescriptor[] targets;
    BlockPlacementPolicyDefault repl = (BlockPlacementPolicyDefault)replicator;
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();

    excludedNodes = new HashMap<Node, Node>();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = repl.chooseTarget(4, dataNodes[0], chosenNodes, 
        excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[0]);
    assertTrue(cluster.isNodeGroupAware());
    // Make sure no replicas are on the same nodegroup 
    for (int i=1;i<4;i++) {
      assertFalse(cluster.isOnSameNodeGroup(targets[0], targets[i]));
    }
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[1], targets[3]));
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
    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0); // no space

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[1]);

    targets = replicator.chooseTarget(filename, 2, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], dataNodes[1]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 3, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], dataNodes[1]);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 4, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[1]);
    assertTrue(cluster.isNodeGroupAware());
    verifyNoTwoTargetsOnSameNodeGroup(targets);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));

    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0); 
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
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0);
    }

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(targets[0], dataNodes[0]));

    targets = replicator.chooseTarget(filename, 2, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(targets[0], dataNodes[0]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 3, dataNodes[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 3);
    for(int i=0; i<3; i++) {
      assertFalse(cluster.isOnSameRack(targets[i], dataNodes[0]));
    }
    verifyNoTwoTargetsOnSameNodeGroup(targets);
    assertTrue(cluster.isOnSameRack(targets[0], targets[1]) ||
               cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
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
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, NODE,
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename, 1, NODE,
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 1);

    targets = replicator.chooseTarget(filename, 2, NODE,
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));

    targets = replicator.chooseTarget(filename, 3, NODE,
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
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
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);
    DatanodeDescriptor[] targets;
    
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
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
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);
    chosenNodes.add(dataNodes[1]);

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));

    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]) && 
        cluster.isOnSameRack(dataNodes[0], targets[1]));
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
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);
    chosenNodes.add(dataNodes[3]);

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);

    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[3], targets[0]));

    targets = replicator.chooseTarget(filename,
                               1, dataNodes[3], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(cluster.isOnSameRack(dataNodes[3], targets[0]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[3], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));

    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameNodeGroup(dataNodes[0], targets[0]));
    targets = replicator.chooseTarget(filename,
                               2, dataNodes[3], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[3], targets[0]));
  }
  
  /**
   * Test for the chooseReplicaToDelete are processed based on 
   * block locality and free space
   */
  @Test
  public void testChooseReplicaToDelete() throws Exception {
    List<DatanodeDescriptor> replicaNodeList = 
        new ArrayList<DatanodeDescriptor>();
    final Map<String, List<DatanodeDescriptor>> rackMap = 
        new HashMap<String, List<DatanodeDescriptor>>();
    dataNodes[0].setRemaining(4*1024*1024);
    replicaNodeList.add(dataNodes[0]);

    dataNodes[1].setRemaining(3*1024*1024);
    replicaNodeList.add(dataNodes[1]);

    dataNodes[2].setRemaining(2*1024*1024);
    replicaNodeList.add(dataNodes[2]);

    dataNodes[5].setRemaining(1*1024*1024);
    replicaNodeList.add(dataNodes[5]);

    List<DatanodeDescriptor> first = new ArrayList<DatanodeDescriptor>();
    List<DatanodeDescriptor> second = new ArrayList<DatanodeDescriptor>();
    replicator.splitNodesWithRack(
        replicaNodeList, rackMap, first, second);
    assertEquals(3, first.size());
    assertEquals(1, second.size());
    DatanodeDescriptor chosenNode = replicator.chooseReplicaToDelete(
        null, null, (short)3, first, second);
    // Within first set {dataNodes[0], dataNodes[1], dataNodes[2]}, 
    // dataNodes[0] and dataNodes[1] are in the same nodegroup, 
    // but dataNodes[1] is chosen as less free space
    assertEquals(chosenNode, dataNodes[1]);

    replicator.adjustSetsWithChosenReplica(rackMap, first, second, chosenNode);
    assertEquals(2, first.size());
    assertEquals(1, second.size());
    // Within first set {dataNodes[0], dataNodes[2]}, dataNodes[2] is chosen
    // as less free space
    chosenNode = replicator.chooseReplicaToDelete(
        null, null, (short)2, first, second);
    assertEquals(chosenNode, dataNodes[2]);

    replicator.adjustSetsWithChosenReplica(rackMap, first, second, chosenNode);
    assertEquals(0, first.size());
    assertEquals(2, second.size());
    // Within second set, dataNodes[5] with less free space
    chosenNode = replicator.chooseReplicaToDelete(
        null, null, (short)1, first, second);
    assertEquals(chosenNode, dataNodes[5]);
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
      dataNodes[0].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0);
        
      dataNodesInBoundaryCase[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0);
    }

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 0, dataNodesInBoundaryCase[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename, 1, dataNodesInBoundaryCase[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 1);

    targets = replicator.chooseTarget(filename, 2, dataNodesInBoundaryCase[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename, 3, dataNodesInBoundaryCase[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
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
      dataNodesInBoundaryCase[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0);
    }
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodesInBoundaryCase[0]);
    chosenNodes.add(dataNodesInBoundaryCase[5]);
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename, 1, dataNodesInBoundaryCase[0],
        chosenNodes, BLOCK_SIZE);
    assertFalse(cluster.isOnSameNodeGroup(targets[0], 
        dataNodesInBoundaryCase[0]));
    assertFalse(cluster.isOnSameNodeGroup(targets[0],
        dataNodesInBoundaryCase[5]));
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
    // Cleanup nodes in previous tests
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
      dataNodesInMoreTargetsCase[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0);
    }

    DatanodeDescriptor[] targets;
    // Test normal case -- 3 replicas
    targets = replicator.chooseTarget(filename, 3, dataNodesInMoreTargetsCase[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(checkTargetsOnDifferentNodeGroup(targets));

    // Test special case -- replica number over node groups.
    targets = replicator.chooseTarget(filename, 10, dataNodesInMoreTargetsCase[0],
        new ArrayList<DatanodeDescriptor>(), BLOCK_SIZE);
    assertTrue(checkTargetsOnDifferentNodeGroup(targets));
    // Verify it only can find 6 targets for placing replicas.
    assertEquals(targets.length, 6);
  }
  
}
