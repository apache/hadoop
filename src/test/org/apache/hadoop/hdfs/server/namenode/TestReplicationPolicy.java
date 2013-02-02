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


import static org.apache.hadoop.test.MetricsAsserts.assertGauge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

public class TestReplicationPolicy extends TestCase {
  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 6;
  private static final Configuration CONF = new Configuration();
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
  // The interval for marking a datanode as stale,
  private static final long staleInterval = 
      DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT;
   
  private final static DatanodeDescriptor NODE = 
    new DatanodeDescriptor(new DatanodeID("h7:5020"), "/d2/r4");
  
  static {
    try {
      FileSystem.setDefaultUri(CONF, "hdfs://localhost:0");
      CONF.set("dfs.http.address", "0.0.0.0:0");
      CONF.setBoolean(
          DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
      NameNode.format(CONF);
      namenode = new NameNode(CONF);
    } catch (IOException e) {
      e.printStackTrace();
      throw (RuntimeException)new RuntimeException().initCause(e);
    }
    // Override fsNamesystem to always avoid stale datanodes
    FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
    replicator = fsNamesystem.replicator;
    cluster = fsNamesystem.clusterMap;
    ArrayList<DatanodeDescriptor> heartbeats = fsNamesystem.heartbeats;
    // construct network topology
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      dataNodes[i].isAlive = true;
      cluster.add(dataNodes[i]);
      heartbeats.add(dataNodes[i]);
    }
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0);
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
  public void testChooseTarget1() throws Exception {
    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 4); // overloaded

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
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0); 
  }

  /**
   * In this testcase, client is dataNodes[0], but the dataNodes[1] is
   * not allowed to be chosen. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on a different
   * rack, the 3rd should be on same rack as the 2nd replica, and the rest
   * should be placed on a third rack.
   * @throws Exception
   */
  public void testChooseTarget2() throws Exception { 
    HashMap<Node, Node> excludedNodes;
    DatanodeDescriptor[] targets;
    BlockPlacementPolicyDefault repl = (BlockPlacementPolicyDefault)replicator;
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    
    excludedNodes = new HashMap<Node, Node>();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = repl.chooseTarget(
                                0, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = repl.chooseTarget(
                                1, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[0]);
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = repl.chooseTarget(
                                2, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = repl.chooseTarget(
                                3, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.put(dataNodes[1], dataNodes[1]); 
    targets = repl.chooseTarget(
                                4, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[0]);
    for(int i=1; i<4; i++) {
      assertFalse(cluster.isOnSameRack(targets[0], targets[i]));
    }
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[1], targets[3]));
  }

  /**
   * In this testcase, client is dataNodes[0], but dataNodes[0] is not qualified
   * to be chosen. So the 1st replica should be placed on dataNodes[1], 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 2nd replica,
   * and the rest should be placed on the third rack.
   * @throws Exception
   */
  public void testChooseTarget3() throws Exception {
    // make data node 0 to be not qualified to choose
    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0); // no space
        
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
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0); 
  }
  
  /**
   * In this testcase, client is dataNodes[0], but none of the nodes on rack 1
   * is qualified to be chosen. So the 1st replica should be placed on either
   * rack 2 or rack 3. 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 1st replica,
   * @throws Exception
   */
  public void testChoooseTarget4() throws Exception {
    // make data node 0 & 1 to be not qualified to choose: not enough disk space
    for(int i=0; i<2; i++) {
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0);
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
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0);
    }
  }
  /**
   * In this testcase, client is is a node outside of file system.
   * So the 1st replica can be placed on any node. 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 2nd replica,
   * @throws Exception
   */
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
  
  private boolean containsWithinRange(DatanodeDescriptor target,
      DatanodeDescriptor[] nodes, int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < nodes.length;
    assert endIndex >= startIndex && endIndex < nodes.length;
    for (int i = startIndex; i <= endIndex; i++) {
      if (nodes[i].equals(target)) {
        return true;
      }
    }
    return false;
  }

  /**
   * In this testcase, it tries to choose more targets than available nodes and
   * check the result, with stale node avoidance on the write path enabled.
   * @throws Exception
   */
  public void testChooseTargetWithMoreThanAvailableNodesWithStaleness()
      throws Exception {
    try {
      namenode.getNamesystem().setNumStaleNodes(NUM_OF_DATANODES);
      testChooseTargetWithMoreThanAvailableNodes();
    } finally {
      namenode.getNamesystem().setNumStaleNodes(0);
    }
  }
  
  /**
   * In this testcase, it tries to choose more targets than available nodes and
   * check the result. 
   * @throws Exception
   */
  public void testChooseTargetWithMoreThanAvailableNodes() throws Exception {
    // make data node 0 & 1 to be not qualified to choose: not enough disk space
    for(int i=0; i<2; i++) {
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0);
    }
    
    final TestAppender appender = new TestAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);
    
    // try to choose NUM_OF_DATANODES which is more than actually available
    // nodes.
    DatanodeDescriptor[] targets = replicator.chooseTarget(filename, 
        NUM_OF_DATANODES, dataNodes[0], new ArrayList<DatanodeDescriptor>(),
        BLOCK_SIZE);
    assertEquals(targets.length, NUM_OF_DATANODES - 2);

    final List<LoggingEvent> log = appender.getLog();
    assertNotNull(log);
    assertFalse(log.size() == 0);
    final LoggingEvent lastLogEntry = log.get(log.size() - 1);
    
    assertEquals(lastLogEntry.getLevel(), Level.WARN);
    // Suppose to place replicas on each node but two data nodes are not
    // available for placing replica, so here we expect a short of 2
    assertTrue(((String)lastLogEntry.getMessage()).contains("in need of 2"));
    
    for(int i=0; i<2; i++) {
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0);
    }
  }
  
  class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
      log.add(loggingEvent);
    }

    @Override
    public void close() {
    }

    public List<LoggingEvent> getLog() {
      return new ArrayList<LoggingEvent>(log);
    }
  }

  public void testChooseTargetWithStaleNodes() throws Exception {
    // Set dataNodes[0] as stale
    dataNodes[0].setLastUpdate(System.currentTimeMillis() - staleInterval - 1);
    namenode.getNamesystem().heartbeatCheck();
    assertTrue(namenode.getNamesystem().shouldAvoidStaleDataNodesForWrite());

    DatanodeDescriptor[] targets;
    // We set the dataNodes[0] as stale, thus should choose dataNodes[1] since
    // dataNodes[1] is on the same rack with dataNodes[0] (writer)
    targets = replicator.chooseTarget(filename, 1, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[1]);

    HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
    excludedNodes.put(dataNodes[1], dataNodes[1]);
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    targets = replicator.chooseTarget(filename, 1, dataNodes[0], chosenNodes,
        excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(targets[0], dataNodes[0]));

    // reset
    dataNodes[0].setLastUpdate(System.currentTimeMillis());
    namenode.getNamesystem().heartbeatCheck();
  }

  /**
   * In this testcase, we set 3 nodes (dataNodes[0] ~ dataNodes[2]) as stale,
   * and when the number of replicas is less or equal to 3, all the healthy
   * datanodes should be returned by the chooseTarget method. When the number of
   * replicas is 4, a stale node should be included.
   * 
   * @throws Exception
   */
  public void testChooseTargetWithHalfStaleNodes() throws Exception {
    // Set dataNodes[0], dataNodes[1], and dataNodes[2] as stale
    for (int i = 0; i < 3; i++) {
      dataNodes[i]
          .setLastUpdate(System.currentTimeMillis() - staleInterval - 1);
    }
    namenode.getNamesystem().heartbeatCheck();

    DatanodeDescriptor[] targets;
    // We set the datanode[0~2] as stale, thus should not choose them
    targets = replicator.chooseTarget(filename, 1, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(containsWithinRange(targets[0], dataNodes, 0, 2));

    targets = replicator.chooseTarget(filename, 2, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(containsWithinRange(targets[0], dataNodes, 0, 2));
    assertFalse(containsWithinRange(targets[1], dataNodes, 0, 2));

    targets = replicator.chooseTarget(filename, 3, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(containsWithinRange(targets[0], dataNodes, 3, 5));
    assertTrue(containsWithinRange(targets[1], dataNodes, 3, 5));
    assertTrue(containsWithinRange(targets[2], dataNodes, 3, 5));

    targets = replicator.chooseTarget(filename, 4, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertTrue(containsWithinRange(dataNodes[3], targets, 0, 3));
    assertTrue(containsWithinRange(dataNodes[4], targets, 0, 3));
    assertTrue(containsWithinRange(dataNodes[5], targets, 0, 3));

    // reset
    for (int i = 0; i < dataNodes.length; i++) {
      dataNodes[i].setLastUpdate(System.currentTimeMillis());
    }
    namenode.getNamesystem().heartbeatCheck();
  }
  
  /**
   * This testcase tests re-replication, when dataNodes[0] is already chosen.
   * So the 1st replica can be placed on random rack. 
   * the 2nd replica should be placed on different node by same rack as 
   * the 1st replica. The 3rd replica can be placed randomly.
   * @throws Exception
   */
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
  
  public void testChooseTargetWithMoreThanHalfStaleNodes() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    // DataNode will send out heartbeat every 15 minutes
    // In this way, when we have set a datanode as stale,
    // its heartbeat will not come to refresh its state
    long heartbeatInterval = 15 * 60;
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, heartbeatInterval);
    // Because the stale interval must be at least 3 times of heartbeatInterval,
    // we reset the staleInterval value.
    long longStaleInterval = 3 * heartbeatInterval * 1000;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        longStaleInterval);

    String[] hosts = new String[] { "host1", "host2", "host3", "host4",
        "host5", "host6" };
    String[] racks = new String[] { "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2",
        "/d2/r3", "/d2/r3" };
    MiniDFSCluster miniCluster = new MiniDFSCluster(conf, hosts.length, true,
        racks, hosts);
    miniCluster.waitActive();

    try {
      // Step 1. Make two datanodes as stale, check whether the
      // avoidStaleDataNodesForWrite calculation is correct.
      // First stop the heartbeat of host1 and host2
      for (int i = 0; i < 2; i++) {
        DataNode dn = miniCluster.getDataNodes().get(i);
        miniCluster.getNameNode().getNamesystem()
            .getDatanode(dn.dnRegistration)
            .setLastUpdate(System.currentTimeMillis() - longStaleInterval - 1);
      }
      // Instead of waiting, explicitly call heartbeatCheck to
      // let heartbeat manager to detect stale nodes
      miniCluster.getNameNode().getNamesystem().heartbeatCheck();
      int numStaleNodes = miniCluster.getNameNode().getNamesystem()
          .getNumStaleNodes();
      assertEquals(numStaleNodes, 2);
      assertTrue(miniCluster.getNameNode().getNamesystem()
          .shouldAvoidStaleDataNodesForWrite());
      // Check metrics
      assertGauge("StaleDataNodes", numStaleNodes, miniCluster.getNameNode()
          .getNamesystem());
      // Call chooseTarget
      DatanodeDescriptor staleNodeInfo = miniCluster.getNameNode()
          .getNamesystem()
          .getDatanode(miniCluster.getDataNodes().get(0).dnRegistration);
      BlockPlacementPolicy replicator = miniCluster.getNameNode()
          .getNamesystem().replicator;
      DatanodeDescriptor[] targets = replicator.chooseTarget(filename, 3,
          staleNodeInfo, BLOCK_SIZE);
      assertEquals(targets.length, 3);
      assertFalse(cluster.isOnSameRack(targets[0], staleNodeInfo));

      // Step 2. Set more than half of the datanodes as stale
      for (int i = 0; i < 4; i++) {
        DataNode dn = miniCluster.getDataNodes().get(i);
        miniCluster.getNameNode().getNamesystem()
            .getDatanode(dn.dnRegistration)
            .setLastUpdate(System.currentTimeMillis() - longStaleInterval - 1);
      }
      // Explicitly call heartbeatCheck
      miniCluster.getNameNode().getNamesystem().heartbeatCheck();
      numStaleNodes = miniCluster.getNameNode().getNamesystem()
          .getNumStaleNodes();
      assertEquals(numStaleNodes, 4);
      // According to our strategy, stale datanodes will be included for writing
      // to avoid hotspots
      assertFalse(miniCluster.getNameNode().getNamesystem()
          .shouldAvoidStaleDataNodesForWrite());
      // Check metrics
      assertGauge("StaleDataNodes", numStaleNodes, miniCluster.getNameNode()
          .getNamesystem());
      // Call chooseTarget
      targets = replicator.chooseTarget(filename, 3, staleNodeInfo, BLOCK_SIZE);
      assertEquals(targets.length, 3);
      assertTrue(cluster.isOnSameRack(targets[0], staleNodeInfo));

      // Step 3. Set 2 stale datanodes back to healthy nodes,
      // still have 2 stale nodes
      for (int i = 2; i < 4; i++) {
        DataNode dn = miniCluster.getDataNodes().get(i);
        miniCluster.getNameNode().getNamesystem()
            .getDatanode(dn.dnRegistration)
            .setLastUpdate(System.currentTimeMillis());
      }
      // Explicitly call heartbeatCheck
      miniCluster.getNameNode().getNamesystem().heartbeatCheck();
      numStaleNodes = miniCluster.getNameNode().getNamesystem()
          .getNumStaleNodes();
      assertEquals(numStaleNodes, 2);
      assertTrue(miniCluster.getNameNode().getNamesystem()
          .shouldAvoidStaleDataNodesForWrite());
      // Check metrics
      assertGauge("StaleDataNodes", numStaleNodes, miniCluster.getNameNode()
          .getNamesystem());
      // Call chooseTarget
      targets = replicator.chooseTarget(filename, 3, staleNodeInfo, BLOCK_SIZE);
      assertEquals(targets.length, 3);
      assertFalse(cluster.isOnSameRack(targets[0], staleNodeInfo));
    } finally {
      miniCluster.shutdown();
    }
  }
  
  /**
   * This testcase tests whether the value returned by 
   * DFSUtil.getInvalidateWorkPctPerIteration() is positive
   */
  public void testGetInvalidateWorkPctPerIteration() {
    Configuration conf = new Configuration();
    float blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    assertTrue(blocksInvalidateWorkPct > 0);
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION, "0.0f");
    try {
      blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
      fail("Should throw IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected 
    }
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION, "1.5f");
    try {
      blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
      fail("Should throw IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected 
    }
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION, "-0.5f");
    try {
      blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
      fail("Should throw IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected 
    }
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION, "0.5f");
    blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    assertEquals(blocksInvalidateWorkPct, 0.5f);
  }
  
  /**
   * This testcase tests whether the value returned by 
   * DFSUtil.getReplWorkMultiplier() is positive
   */
  public void testGetReplWorkMultiplier() {
    Configuration conf = new Configuration();
    int blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);
    assertTrue(blocksReplWorkMultiplier > 0);
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, "-1");
    try {
      blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);
      fail("Should throw IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, "3");
    blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);
    assertEquals(blocksReplWorkMultiplier, 3);
  }
    
  /**
   * Test for the chooseReplicaToDelete are processed based on block locality
   * and free space
   */
  public void testChooseReplicaToDelete() throws Exception {
    List<DatanodeDescriptor> replicaNodeList = new ArrayList<DatanodeDescriptor>();
    final Map<String, List<DatanodeDescriptor>> rackMap = new HashMap<String, List<DatanodeDescriptor>>();

    dataNodes[0].setRemaining(4 * 1024 * 1024);
    replicaNodeList.add(dataNodes[0]);

    dataNodes[1].setRemaining(3 * 1024 * 1024);
    replicaNodeList.add(dataNodes[1]);

    dataNodes[2].setRemaining(2 * 1024 * 1024);
    replicaNodeList.add(dataNodes[2]);

    dataNodes[5].setRemaining(1 * 1024 * 1024);
    replicaNodeList.add(dataNodes[5]);

    List<DatanodeDescriptor> first = new ArrayList<DatanodeDescriptor>();
    List<DatanodeDescriptor> second = new ArrayList<DatanodeDescriptor>();
    replicator.splitNodesWithRack(replicaNodeList, rackMap, first, second);
    // dataNodes[0] and dataNodes[1] are in first set as their rack has two
    // replica nodes, while datanodes[2] and dataNodes[5] are in second set.
    assertEquals(2, first.size());
    assertEquals(2, second.size());
    DatanodeDescriptor chosenNode = replicator.chooseReplicaToDelete(null,
        null, (short) 3, first, second);
    // Within first set, dataNodes[1] with less free space
    assertEquals(chosenNode, dataNodes[1]);

    replicator.adjustSetsWithChosenReplica(rackMap, first, second, chosenNode);
    assertEquals(0, first.size());
    assertEquals(3, second.size());
    // Within second set, dataNodes[5] with less free space
    chosenNode = replicator.chooseReplicaToDelete(null, null, (short) 2, first,
        second);
    assertEquals(chosenNode, dataNodes[5]);
  }

}
