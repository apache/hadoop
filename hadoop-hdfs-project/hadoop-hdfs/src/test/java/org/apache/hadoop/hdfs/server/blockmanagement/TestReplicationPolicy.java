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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.junit.Test;

public class TestReplicationPolicy {
  private Random random= DFSUtil.getRandom();
  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 6;
  private static final Configuration CONF = new HdfsConfiguration();
  private static final NetworkTopology cluster;
  private static final NameNode namenode;
  private static final BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";
  private static final DatanodeDescriptor dataNodes[] = 
    new DatanodeDescriptor[] {
      new DatanodeDescriptor(new DatanodeID("h1:5020"), "/d1/r1"),
      new DatanodeDescriptor(new DatanodeID("h2:5020"), "/d1/r1"),
      new DatanodeDescriptor(new DatanodeID("h3:5020"), "/d1/r2"),
      new DatanodeDescriptor(new DatanodeID("h4:5020"), "/d1/r2"),
      new DatanodeDescriptor(new DatanodeID("h5:5020"), "/d2/r3"),
      new DatanodeDescriptor(new DatanodeID("h6:5020"), "/d2/r3")
    };
   
  private final static DatanodeDescriptor NODE = 
    new DatanodeDescriptor(new DatanodeID("h7:5020"), "/d2/r4");
  
  static {
    try {
      FileSystem.setDefaultUri(CONF, "hdfs://localhost:0");
      CONF.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      DFSTestUtil.formatNameNode(CONF);
      namenode = new NameNode(CONF);
    } catch (IOException e) {
      e.printStackTrace();
      throw (RuntimeException)new RuntimeException().initCause(e);
    }
    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    replicator = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    // construct network topology
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
    }
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      dataNodes[i].updateHeartbeat(
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0, 0);
    }
  }
  
  /**
   * In this testcase, client is dataNodes[0]. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on 
   * different rack and third should be placed on different node
   * of rack chosen for 2nd node.
   * The only excpetion is when the <i>numOfReplicas</i> is 2, 
   * the 1st is on dataNodes[0] and the 2nd is on a different rack.
   * @throws Exception
   */
  @Test
  public void testChooseTarget1() throws Exception {
    dataNodes[0].updateHeartbeat(
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 4, 0); // overloaded

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[0]);
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));

    targets = replicator.chooseTarget(filename,
                                     4, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[0]);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
    
    dataNodes[0].updateHeartbeat(
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0, 0); 
  }

  private static DatanodeDescriptor[] chooseTarget(
      BlockPlacementPolicyDefault policy,
      int numOfReplicas,
      DatanodeDescriptor writer,
      List<DatanodeDescriptor> chosenNodes,
      HashMap<Node, Node> excludedNodes,
      long blocksize) {
    return policy.chooseTarget(numOfReplicas, writer, chosenNodes, false,
        excludedNodes, blocksize);
  }

  /**
   * In this testcase, client is dataNodes[0], but the dataNodes[1] is
   * not allowed to be chosen. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on a different
   * rack, the 3rd should be on same rack as the 2nd replica, and the rest
   * should be placed on a third rack.
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
    targets = chooseTarget(repl, 0, dataNodes[0], chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = chooseTarget(repl, 1, dataNodes[0], chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[0]);
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = chooseTarget(repl, 2, dataNodes[0], chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = chooseTarget(repl, 3, dataNodes[0], chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = chooseTarget(repl, 4, dataNodes[0], chosenNodes, excludedNodes,
        BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[0]);
    for(int i=1; i<4; i++) {
      assertFalse(cluster.isOnSameRack(targets[0], targets[i]));
    }
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[1], targets[3]));

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    chosenNodes.add(dataNodes[2]);
    targets = repl.chooseTarget(1, dataNodes[0], chosenNodes, true,
        excludedNodes, BLOCK_SIZE);
    System.out.println("targets=" + Arrays.asList(targets));
    assertEquals(2, targets.length);
    //make sure that the chosen node is in the target.
    int i = 0;
    for(; i < targets.length && !dataNodes[2].equals(targets[i]); i++);
    assertTrue(i < targets.length);
  }

  /**
   * In this testcase, client is dataNodes[0], but dataNodes[0] is not qualified
   * to be chosen. So the 1st replica should be placed on dataNodes[1], 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 2nd replica,
   * and the rest should be placed on the third rack.
   * @throws Exception
   */
  @Test
  public void testChooseTarget3() throws Exception {
    // make data node 0 to be not qualified to choose
    dataNodes[0].updateHeartbeat(
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (HdfsConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0, 0); // no space
        
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[1]);
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], dataNodes[1]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], dataNodes[1]);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      4, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[1]);
    for(int i=1; i<4; i++) {
      assertFalse(cluster.isOnSameRack(targets[0], targets[i]));
    }
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[1], targets[3]));

    dataNodes[0].updateHeartbeat(
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0, 0); 
  }
  
  /**
   * In this testcase, client is dataNodes[0], but none of the nodes on rack 1
   * is qualified to be chosen. So the 1st replica should be placed on either
   * rack 2 or rack 3. 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 1st replica,
   * @throws Exception
   */
  @Test
  public void testChoooseTarget4() throws Exception {
    // make data node 0 & 1 to be not qualified to choose: not enough disk space
    for(int i=0; i<2; i++) {
      dataNodes[i].updateHeartbeat(
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0, 0);
    }
      
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(targets[0], dataNodes[0]));
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(targets[0], dataNodes[0]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 3);
    for(int i=0; i<3; i++) {
      assertFalse(cluster.isOnSameRack(targets[i], dataNodes[0]));
    }
    assertTrue(cluster.isOnSameRack(targets[0], targets[1]) ||
               cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
    
    for(int i=0; i<2; i++) {
      dataNodes[i].updateHeartbeat(
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0, 0);
    }
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
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, NODE, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, NODE, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    
    targets = replicator.chooseTarget(filename,
                                      2, NODE, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, NODE, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));    
  }
  
  /**
   * This testcase tests re-replication, when dataNodes[0] is already chosen.
   * So the 1st replica can be placed on random rack. 
   * the 2nd replica should be placed on different node by same rack as 
   * the 1st replica. The 3rd replica can be placed randomly.
   * @throws Exception
   */
  @Test
  public void testRereplicate1() throws Exception {
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
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
  }

  /**
   * This testcase tests re-replication, 
   * when dataNodes[0] and dataNodes[1] are already chosen.
   * So the 1st replica should be placed on a different rack than rack 1. 
   * the rest replicas can be placed randomly,
   * @throws Exception
   */
  @Test
  public void testRereplicate2() throws Exception {
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
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[1]));
  }

  /**
   * This testcase tests re-replication, 
   * when dataNodes[0] and dataNodes[2] are already chosen.
   * So the 1st replica should be placed on the rack that the writer resides. 
   * the rest replicas can be placed randomly,
   * @throws Exception
   */
  @Test
  public void testRereplicate3() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);
    chosenNodes.add(dataNodes[2]);
    
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[2], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                               1, dataNodes[2], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(cluster.isOnSameRack(dataNodes[2], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));

    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                               2, dataNodes[2], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[2], targets[0]));
  }
  
  /**
   * Test for the high priority blocks are processed before the low priority
   * blocks.
   */
  @Test(timeout = 60000)
  public void testReplicationWithPriority() throws Exception {
    int DFS_NAMENODE_REPLICATION_INTERVAL = 1000;
    int HIGH_PRIORITY = 0;
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(true).build();
    try {
      cluster.waitActive();
      final UnderReplicatedBlocks neededReplications = (UnderReplicatedBlocks) cluster
          .getNameNode().getNamesystem().getBlockManager().neededReplications;
      for (int i = 0; i < 100; i++) {
        // Adding the blocks directly to normal priority
        neededReplications.add(new Block(random.nextLong()), 2, 0, 3);
      }
      // Lets wait for the replication interval, to start process normal
      // priority blocks
      Thread.sleep(DFS_NAMENODE_REPLICATION_INTERVAL);
      
      // Adding the block directly to high priority list
      neededReplications.add(new Block(random.nextLong()), 1, 0, 3);
      
      // Lets wait for the replication interval
      Thread.sleep(DFS_NAMENODE_REPLICATION_INTERVAL);

      // Check replication completed successfully. Need not wait till it process
      // all the 100 normal blocks.
      assertFalse("Not able to clear the element from high priority list",
          neededReplications.iterator(HIGH_PRIORITY).hasNext());
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Test for the ChooseUnderReplicatedBlocks are processed based on priority
   */
  @Test
  public void testChooseUnderReplicatedBlocks() throws Exception {
    UnderReplicatedBlocks underReplicatedBlocks = new UnderReplicatedBlocks();

    for (int i = 0; i < 5; i++) {
      // Adding QUEUE_HIGHEST_PRIORITY block
      underReplicatedBlocks.add(new Block(random.nextLong()), 1, 0, 3);

      // Adding QUEUE_VERY_UNDER_REPLICATED block
      underReplicatedBlocks.add(new Block(random.nextLong()), 2, 0, 7);

      // Adding QUEUE_UNDER_REPLICATED block
      underReplicatedBlocks.add(new Block(random.nextLong()), 6, 0, 6);

      // Adding QUEUE_REPLICAS_BADLY_DISTRIBUTED block
      underReplicatedBlocks.add(new Block(random.nextLong()), 5, 0, 6);

      // Adding QUEUE_WITH_CORRUPT_BLOCKS block
      underReplicatedBlocks.add(new Block(random.nextLong()), 0, 0, 3);
    }

    // Choose 6 blocks from UnderReplicatedBlocks. Then it should pick 5 blocks
    // from
    // QUEUE_HIGHEST_PRIORITY and 1 block from QUEUE_VERY_UNDER_REPLICATED.
    List<List<Block>> chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(6);
    assertTheChosenBlocks(chosenBlocks, 5, 1, 0, 0, 0);

    // Choose 10 blocks from UnderReplicatedBlocks. Then it should pick 4 blocks from
    // QUEUE_VERY_UNDER_REPLICATED, 5 blocks from QUEUE_UNDER_REPLICATED and 1
    // block from QUEUE_REPLICAS_BADLY_DISTRIBUTED.
    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(10);
    assertTheChosenBlocks(chosenBlocks, 0, 4, 5, 1, 0);

    // Adding QUEUE_HIGHEST_PRIORITY
    underReplicatedBlocks.add(new Block(random.nextLong()), 1, 0, 3);

    // Choose 10 blocks from UnderReplicatedBlocks. Then it should pick 1 block from
    // QUEUE_HIGHEST_PRIORITY, 4 blocks from QUEUE_REPLICAS_BADLY_DISTRIBUTED
    // and 5 blocks from QUEUE_WITH_CORRUPT_BLOCKS.
    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(10);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 4, 5);

    // Since it is reached to end of all lists,
    // should start picking the blocks from start.
    // Choose 7 blocks from UnderReplicatedBlocks. Then it should pick 6 blocks from
    // QUEUE_HIGHEST_PRIORITY, 1 block from QUEUE_VERY_UNDER_REPLICATED.
    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(7);
    assertTheChosenBlocks(chosenBlocks, 6, 1, 0, 0, 0);
  }
  
  /** asserts the chosen blocks with expected priority blocks */
  private void assertTheChosenBlocks(
      List<List<Block>> chosenBlocks, int firstPrioritySize,
      int secondPrioritySize, int thirdPrioritySize, int fourthPrioritySize,
      int fifthPrioritySize) {
    assertEquals(
        "Not returned the expected number of QUEUE_HIGHEST_PRIORITY blocks",
        firstPrioritySize, chosenBlocks.get(
            UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY).size());
    assertEquals(
        "Not returned the expected number of QUEUE_VERY_UNDER_REPLICATED blocks",
        secondPrioritySize, chosenBlocks.get(
            UnderReplicatedBlocks.QUEUE_VERY_UNDER_REPLICATED).size());
    assertEquals(
        "Not returned the expected number of QUEUE_UNDER_REPLICATED blocks",
        thirdPrioritySize, chosenBlocks.get(
            UnderReplicatedBlocks.QUEUE_UNDER_REPLICATED).size());
    assertEquals(
        "Not returned the expected number of QUEUE_REPLICAS_BADLY_DISTRIBUTED blocks",
        fourthPrioritySize, chosenBlocks.get(
            UnderReplicatedBlocks.QUEUE_REPLICAS_BADLY_DISTRIBUTED).size());
    assertEquals(
        "Not returned the expected number of QUEUE_WITH_CORRUPT_BLOCKS blocks",
        fifthPrioritySize, chosenBlocks.get(
            UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS).size());
  }
}
