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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager.StatefulBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestReplicationPolicy {
  {
    ((Log4JLogger)BlockPlacementPolicy.LOG).getLogger().setLevel(Level.ALL);
  }

  private final Random random = DFSUtil.getRandom();
  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 6;
  private static NetworkTopology cluster;
  private static NameNode namenode;
  private static BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";
  private static DatanodeDescriptor[] dataNodes;
  private static DatanodeStorageInfo[] storages;
  // The interval for marking a datanode as stale,
  private static final long staleInterval =
      DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT;

  @Rule
  public ExpectedException exception = ExpectedException.none();
  
  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
    long capacity, long dfsUsed, long remaining, long blockPoolUsed,
    long dnCacheCapacity, long dnCacheUsed, int xceiverCount, int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures, null);
  }

  private static void updateHeartbeatForExtraStorage(long capacity,
      long dfsUsed, long remaining, long blockPoolUsed) {
    DatanodeDescriptor dn = dataNodes[5];
    dn.getStorageInfos()[1].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        0L, 0L, 0, 0, null);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {
        "/d1/r1",
        "/d1/r1",
        "/d1/r2",
        "/d1/r2",
        "/d2/r3",
        "/d2/r3"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    // create an extra storage for dn5.
    DatanodeStorage extraStorage = new DatanodeStorage(
        storages[5].getStorageID() + "-extra", DatanodeStorage.State.NORMAL,
        StorageType.DEFAULT);
/*    DatanodeStorageInfo si = new DatanodeStorageInfo(
        storages[5].getDatanodeDescriptor(), extraStorage);
*/
    BlockManagerTestUtil.updateStorage(storages[5].getDatanodeDescriptor(),
        extraStorage);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestReplicationPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());

    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    replicator = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    // construct network topology
    for (int i=0; i < NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
      bm.getDatanodeManager().getHeartbeatManager().addDatanode(
          dataNodes[i]);
    }
    resetHeartbeatForStorages();
  }

  private static void resetHeartbeatForStorages() {
    for (int i=0; i < NUM_OF_DATANODES; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }    
    // No available space in the extra storage of dn0
    updateHeartbeatForExtraStorage(0L, 0L, 0L, 0L);
  }

  private static boolean isOnSameRack(DatanodeStorageInfo left, DatanodeStorageInfo right) {
    return isOnSameRack(left, right.getDatanodeDescriptor());
  }

  private static boolean isOnSameRack(DatanodeStorageInfo left, DatanodeDescriptor right) {
    return cluster.isOnSameRack(left.getDatanodeDescriptor(), right);
  }

  /**
   * Test whether the remaining space per storage is individually
   * considered.
   */
  @Test
  public void testChooseNodeWithMultipleStorages() throws Exception {
    updateHeartbeatWithUsage(dataNodes[5],
        2* HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE)/3, 0L,
        0L, 0L, 0, 0);

    updateHeartbeatForExtraStorage(
        2* HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE)/3, 0L);

    DatanodeStorageInfo[] targets;
    targets = chooseTarget (1, dataNodes[5],
        new ArrayList<DatanodeStorageInfo>(), null);
    assertEquals(1, targets.length);
    assertEquals(storages[4], targets[0]);

    resetHeartbeatForStorages();
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

    targets = chooseTarget(4);
    assertEquals(targets.length, 4);
    assertEquals(storages[0], targets[0]);
    assertTrue(isOnSameRack(targets[1], targets[2]) ||
               isOnSameRack(targets[2], targets[3]));
    assertFalse(isOnSameRack(targets[0], targets[2]));
    
    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas) {
    return chooseTarget(numOfReplicas, dataNodes[0]);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer) {
    return chooseTarget(numOfReplicas, writer,
        new ArrayList<DatanodeStorageInfo>());
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      List<DatanodeStorageInfo> chosenNodes) {
    return chooseTarget(numOfReplicas, dataNodes[0], chosenNodes);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeStorageInfo> chosenNodes) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      List<DatanodeStorageInfo> chosenNodes, Set<Node> excludedNodes) {
    return chooseTarget(numOfReplicas, dataNodes[0], chosenNodes, excludedNodes);
  }

  private static DatanodeStorageInfo[] chooseTarget(
      int numOfReplicas,
      DatanodeDescriptor writer,
      List<DatanodeStorageInfo> chosenNodes,
      Set<Node> excludedNodes) {
    return replicator.chooseTarget(filename, numOfReplicas, writer, chosenNodes,
        false, excludedNodes, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);
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
    Set<Node> excludedNodes;
    DatanodeStorageInfo[] targets;
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    
    excludedNodes = new HashSet<Node>();
    excludedNodes.add(dataNodes[1]); 
    targets = chooseTarget(0, chosenNodes, excludedNodes);
    assertEquals(targets.length, 0);
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    targets = chooseTarget(1, chosenNodes, excludedNodes);
    assertEquals(targets.length, 1);
    assertEquals(storages[0], targets[0]);
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    targets = chooseTarget(2, chosenNodes, excludedNodes);
    assertEquals(targets.length, 2);
    assertEquals(storages[0], targets[0]);

    assertFalse(isOnSameRack(targets[0], targets[1]));
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    targets = chooseTarget(3, chosenNodes, excludedNodes);
    assertEquals(targets.length, 3);
    assertEquals(storages[0], targets[0]);

    assertFalse(isOnSameRack(targets[0], targets[1]));
    assertTrue(isOnSameRack(targets[1], targets[2]));
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    targets = chooseTarget(4, chosenNodes, excludedNodes);
    assertEquals(targets.length, 4);
    assertEquals(storages[0], targets[0]);

    for(int i=1; i<4; i++) {
      assertFalse(isOnSameRack(targets[0], targets[i]));
    }
    assertTrue(isOnSameRack(targets[1], targets[2]) ||
               isOnSameRack(targets[2], targets[3]));
    assertFalse(isOnSameRack(targets[1], targets[3]));

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    chosenNodes.add(storages[2]);
    targets = replicator.chooseTarget(filename, 1, dataNodes[0], chosenNodes, true,
        excludedNodes, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);
    System.out.println("targets=" + Arrays.asList(targets));
    assertEquals(2, targets.length);
    //make sure that the chosen node is in the target.
    int i = 0;
    for (; i < targets.length && !storages[2].equals(targets[i]); i++);
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
    for(int i=1; i<4; i++) {
      assertFalse(isOnSameRack(targets[0], targets[i]));
    }
    assertTrue(isOnSameRack(targets[1], targets[2]) ||
               isOnSameRack(targets[2], targets[3]));
    assertFalse(isOnSameRack(targets[1], targets[3]));

    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
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
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
      
    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0);
    assertEquals(targets.length, 0);
    
    targets = chooseTarget(1);
    assertEquals(targets.length, 1);
    assertFalse(isOnSameRack(targets[0], dataNodes[0]));
    
    targets = chooseTarget(2);
    assertEquals(targets.length, 2);
    assertFalse(isOnSameRack(targets[0], dataNodes[0]));
    assertFalse(isOnSameRack(targets[0], targets[1]));
    
    targets = chooseTarget(3);
    assertEquals(targets.length, 3);
    for(int i=0; i<3; i++) {
      assertFalse(isOnSameRack(targets[i], dataNodes[0]));
    }
    assertTrue(isOnSameRack(targets[0], targets[1]) ||
               isOnSameRack(targets[1], targets[2]));
    assertFalse(isOnSameRack(targets[0], targets[2]));
    
    for(int i=0; i<2; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
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
    DatanodeDescriptor writerDesc =
      DFSTestUtil.getDatanodeDescriptor("7.7.7.7", "/d2/r4");

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, writerDesc);
    assertEquals(targets.length, 0);

    targets = chooseTarget(1, writerDesc);
    assertEquals(targets.length, 1);

    targets = chooseTarget(2, writerDesc);
    assertEquals(targets.length, 2);
    assertFalse(isOnSameRack(targets[0], targets[1]));

    targets = chooseTarget(3, writerDesc);
    assertEquals(targets.length, 3);
    assertTrue(isOnSameRack(targets[1], targets[2]));
    assertFalse(isOnSameRack(targets[0], targets[1]));
  }

  /**
   * In this testcase, there are enough total number of nodes, but only
   * one rack is actually available.
   * @throws Exception
   */
  @Test
  public void testChooseTarget6() throws Exception {
    DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo(
        "DS-xxxx", "7.7.7.7", "/d2/r3", "host7");
    DatanodeDescriptor newDn = storage.getDatanodeDescriptor();
    Set<Node> excludedNodes;
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();

    excludedNodes = new HashSet<Node>();
    excludedNodes.add(dataNodes[0]);
    excludedNodes.add(dataNodes[1]);
    excludedNodes.add(dataNodes[2]);
    excludedNodes.add(dataNodes[3]);

    DatanodeStorageInfo[] targets;
    // Only two nodes available in a rack. Try picking two nodes. Only one
    // should return.
    targets = chooseTarget(2, chosenNodes, excludedNodes);
    assertEquals(1, targets.length);

    // Make three nodes available in a rack.
    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    bm.getDatanodeManager().getNetworkTopology().add(newDn);
    bm.getDatanodeManager().getHeartbeatManager().addDatanode(newDn);
    updateHeartbeatWithUsage(newDn,
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);

    // Try picking three nodes. Only two should return.
    excludedNodes.clear();
    excludedNodes.add(dataNodes[0]);
    excludedNodes.add(dataNodes[1]);
    excludedNodes.add(dataNodes[2]);
    excludedNodes.add(dataNodes[3]);
    chosenNodes.clear();
    try {
      targets = chooseTarget(3, chosenNodes, excludedNodes);
      assertEquals(2, targets.length);
    } finally {
      bm.getDatanodeManager().getNetworkTopology().remove(newDn);
    }
  }


  /**
   * In this testcase, it tries to choose more targets than available nodes and
   * check the result, with stale node avoidance on the write path enabled.
   * @throws Exception
   */
  @Test
  public void testChooseTargetWithMoreThanAvailableNodesWithStaleness()
      throws Exception {
    try {
      namenode.getNamesystem().getBlockManager().getDatanodeManager()
        .setNumStaleNodes(NUM_OF_DATANODES);
      testChooseTargetWithMoreThanAvailableNodes();
    } finally {
      namenode.getNamesystem().getBlockManager().getDatanodeManager()
        .setNumStaleNodes(0);
    }
  }
  
  /**
   * In this testcase, it tries to choose more targets than available nodes and
   * check the result. 
   * @throws Exception
   */
  @Test
  public void testChooseTargetWithMoreThanAvailableNodes() throws Exception {
    // make data node 0 & 1 to be not qualified to choose: not enough disk space
    for(int i=0; i<2; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
    
    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);
    
    // try to choose NUM_OF_DATANODES which is more than actually available
    // nodes.
    DatanodeStorageInfo[] targets = chooseTarget(NUM_OF_DATANODES);
    assertEquals(targets.length, NUM_OF_DATANODES - 2);

    final List<LoggingEvent> log = appender.getLog();
    assertNotNull(log);
    assertFalse(log.size() == 0);
    final LoggingEvent lastLogEntry = log.get(log.size() - 1);
    
    assertTrue(Level.WARN.isGreaterOrEqual(lastLogEntry.getLevel()));
    // Suppose to place replicas on each node but two data nodes are not
    // available for placing replica, so here we expect a short of 2
    assertTrue(((String)lastLogEntry.getMessage()).contains("in need of 2"));
    
    for(int i=0; i<2; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
  }

  private boolean containsWithinRange(DatanodeStorageInfo target,
      DatanodeDescriptor[] nodes, int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < nodes.length;
    assert endIndex >= startIndex && endIndex < nodes.length;
    for (int i = startIndex; i <= endIndex; i++) {
      if (nodes[i].equals(target.getDatanodeDescriptor())) {
        return true;
      }
    }
    return false;
  }
  
  private boolean containsWithinRange(DatanodeDescriptor target,
      DatanodeStorageInfo[] nodes, int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < nodes.length;
    assert endIndex >= startIndex && endIndex < nodes.length;
    for (int i = startIndex; i <= endIndex; i++) {
      if (nodes[i].getDatanodeDescriptor().equals(target)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testChooseTargetWithStaleNodes() throws Exception {
    // Set dataNodes[0] as stale
    DFSTestUtil.resetLastUpdatesWithOffset(dataNodes[0], -(staleInterval + 1));
    namenode.getNamesystem().getBlockManager()
      .getDatanodeManager().getHeartbeatManager().heartbeatCheck();
    assertTrue(namenode.getNamesystem().getBlockManager()
        .getDatanodeManager().shouldAvoidStaleDataNodesForWrite());
    DatanodeStorageInfo[] targets;
    // We set the datanode[0] as stale, thus should choose datanode[1] since
    // datanode[1] is on the same rack with datanode[0] (writer)
    targets = chooseTarget(1);
    assertEquals(targets.length, 1);
    assertEquals(storages[1], targets[0]);

    Set<Node> excludedNodes = new HashSet<Node>();
    excludedNodes.add(dataNodes[1]);
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    targets = chooseTarget(1, chosenNodes, excludedNodes);
    assertEquals(targets.length, 1);
    assertFalse(isOnSameRack(targets[0], dataNodes[0]));
    
    // reset
    DFSTestUtil.resetLastUpdatesWithOffset(dataNodes[0], 0);
    namenode.getNamesystem().getBlockManager()
      .getDatanodeManager().getHeartbeatManager().heartbeatCheck();
  }

  /**
   * In this testcase, we set 3 nodes (dataNodes[0] ~ dataNodes[2]) as stale,
   * and when the number of replicas is less or equal to 3, all the healthy
   * datanodes should be returned by the chooseTarget method. When the number 
   * of replicas is 4, a stale node should be included.
   * 
   * @throws Exception
   */
  @Test
  public void testChooseTargetWithHalfStaleNodes() throws Exception {
    // Set dataNodes[0], dataNodes[1], and dataNodes[2] as stale
    for (int i = 0; i < 3; i++) {
      DFSTestUtil
          .resetLastUpdatesWithOffset(dataNodes[i], -(staleInterval + 1));
    }
    namenode.getNamesystem().getBlockManager()
      .getDatanodeManager().getHeartbeatManager().heartbeatCheck();

    DatanodeStorageInfo[] targets = chooseTarget(0);
    assertEquals(targets.length, 0);

    // Since we have 6 datanodes total, stale nodes should
    // not be returned until we ask for more than 3 targets
    targets = chooseTarget(1);
    assertEquals(targets.length, 1);
    assertFalse(containsWithinRange(targets[0], dataNodes, 0, 2));

    targets = chooseTarget(2);
    assertEquals(targets.length, 2);
    assertFalse(containsWithinRange(targets[0], dataNodes, 0, 2));
    assertFalse(containsWithinRange(targets[1], dataNodes, 0, 2));

    targets = chooseTarget(3);
    assertEquals(targets.length, 3);
    assertTrue(containsWithinRange(targets[0], dataNodes, 3, 5));
    assertTrue(containsWithinRange(targets[1], dataNodes, 3, 5));
    assertTrue(containsWithinRange(targets[2], dataNodes, 3, 5));

    targets = chooseTarget(4);
    assertEquals(targets.length, 4);
    assertTrue(containsWithinRange(dataNodes[3], targets, 0, 3));
    assertTrue(containsWithinRange(dataNodes[4], targets, 0, 3));
    assertTrue(containsWithinRange(dataNodes[5], targets, 0, 3));

    for (int i = 0; i < dataNodes.length; i++) {
      DFSTestUtil.resetLastUpdatesWithOffset(dataNodes[i], 0);
    }
    namenode.getNamesystem().getBlockManager()
      .getDatanodeManager().getHeartbeatManager().heartbeatCheck();
  }

  @Test
  public void testChooseTargetWithMoreThanHalfStaleNodes() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    String[] hosts = new String[]{"host1", "host2", "host3", 
                                  "host4", "host5", "host6"};
    String[] racks = new String[]{"/d1/r1", "/d1/r1", "/d1/r2", 
                                  "/d1/r2", "/d2/r3", "/d2/r3"};
    MiniDFSCluster miniCluster = new MiniDFSCluster.Builder(conf).racks(racks)
        .hosts(hosts).numDataNodes(hosts.length).build();
    miniCluster.waitActive();
    
    try {
      // Step 1. Make two datanodes as stale, check whether the 
      // avoidStaleDataNodesForWrite calculation is correct.
      // First stop the heartbeat of host1 and host2
      for (int i = 0; i < 2; i++) {
        DataNode dn = miniCluster.getDataNodes().get(i);
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
        DatanodeDescriptor dnDes = miniCluster.getNameNode().getNamesystem()
            .getBlockManager().getDatanodeManager()
            .getDatanode(dn.getDatanodeId());
        DFSTestUtil.resetLastUpdatesWithOffset(dnDes, -(staleInterval + 1));
      }
      // Instead of waiting, explicitly call heartbeatCheck to 
      // let heartbeat manager to detect stale nodes
      miniCluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().getHeartbeatManager().heartbeatCheck();
      int numStaleNodes = miniCluster.getNameNode().getNamesystem()
          .getBlockManager().getDatanodeManager().getNumStaleNodes();
      assertEquals(numStaleNodes, 2);
      assertTrue(miniCluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().shouldAvoidStaleDataNodesForWrite());
      // Call chooseTarget
      DatanodeDescriptor staleNodeInfo = miniCluster.getNameNode()
          .getNamesystem().getBlockManager().getDatanodeManager()
          .getDatanode(miniCluster.getDataNodes().get(0).getDatanodeId());
      BlockPlacementPolicy replicator = miniCluster.getNameNode()
          .getNamesystem().getBlockManager().getBlockPlacementPolicy();
      DatanodeStorageInfo[] targets = replicator.chooseTarget(filename, 3,
          staleNodeInfo, new ArrayList<DatanodeStorageInfo>(), false, null,
          BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);

      assertEquals(targets.length, 3);
      assertFalse(isOnSameRack(targets[0], staleNodeInfo));
      
      // Step 2. Set more than half of the datanodes as stale
      for (int i = 0; i < 4; i++) {
        DataNode dn = miniCluster.getDataNodes().get(i);
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
        DatanodeDescriptor dnDesc = miniCluster.getNameNode().getNamesystem().getBlockManager()
            .getDatanodeManager().getDatanode(dn.getDatanodeId());
        DFSTestUtil.resetLastUpdatesWithOffset(dnDesc, -(staleInterval + 1));
      }
      // Explicitly call heartbeatCheck
      miniCluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().getHeartbeatManager().heartbeatCheck();
      numStaleNodes = miniCluster.getNameNode().getNamesystem()
          .getBlockManager().getDatanodeManager().getNumStaleNodes();
      assertEquals(numStaleNodes, 4);
      // According to our strategy, stale datanodes will be included for writing
      // to avoid hotspots
      assertFalse(miniCluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().shouldAvoidStaleDataNodesForWrite());
      // Call chooseTarget
      targets = replicator.chooseTarget(filename, 3, staleNodeInfo,
          new ArrayList<DatanodeStorageInfo>(), false, null, BLOCK_SIZE,
          TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);
      assertEquals(targets.length, 3);
      assertTrue(isOnSameRack(targets[0], staleNodeInfo));
      
      // Step 3. Set 2 stale datanodes back to healthy nodes,
      // still have 2 stale nodes
      for (int i = 2; i < 4; i++) {
        DataNode dn = miniCluster.getDataNodes().get(i);
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
        DatanodeDescriptor dnDesc = miniCluster.getNameNode().getNamesystem()
            .getBlockManager().getDatanodeManager()
            .getDatanode(dn.getDatanodeId());
        DFSTestUtil.resetLastUpdatesWithOffset(dnDesc, 0);
      }
      // Explicitly call heartbeatCheck
      miniCluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().getHeartbeatManager().heartbeatCheck();
      numStaleNodes = miniCluster.getNameNode().getNamesystem()
          .getBlockManager().getDatanodeManager().getNumStaleNodes();
      assertEquals(numStaleNodes, 2);
      assertTrue(miniCluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().shouldAvoidStaleDataNodesForWrite());
      // Call chooseTarget
      targets = chooseTarget(3, staleNodeInfo);
      assertEquals(targets.length, 3);
      assertFalse(isOnSameRack(targets[0], staleNodeInfo));
    } finally {
      miniCluster.shutdown();
    }
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
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    chosenNodes.add(storages[0]);    
    DatanodeStorageInfo[] targets;
    
    targets = chooseTarget(0, chosenNodes);
    assertEquals(targets.length, 0);
    
    targets = chooseTarget(1, chosenNodes);
    assertEquals(targets.length, 1);
    assertFalse(isOnSameRack(targets[0], dataNodes[0]));
    
    targets = chooseTarget(2, chosenNodes);
    assertEquals(targets.length, 2);
    assertTrue(isOnSameRack(targets[0], dataNodes[0]));
    assertFalse(isOnSameRack(targets[0], targets[1]));
    
    targets = chooseTarget(3, chosenNodes);
    assertEquals(targets.length, 3);
    assertTrue(isOnSameRack(targets[0], dataNodes[0]));
    assertFalse(isOnSameRack(targets[0], targets[2]));
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
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    chosenNodes.add(storages[0]);
    chosenNodes.add(storages[1]);

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, chosenNodes);
    assertEquals(targets.length, 0);
    
    targets = chooseTarget(1, chosenNodes);
    assertEquals(targets.length, 1);
    assertFalse(isOnSameRack(targets[0], dataNodes[0]));
    
    targets = chooseTarget(2, chosenNodes);
    assertEquals(targets.length, 2);
    assertFalse(isOnSameRack(targets[0], dataNodes[0]));
    assertFalse(isOnSameRack(targets[1], dataNodes[0]));
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
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    chosenNodes.add(storages[0]);
    chosenNodes.add(storages[2]);
    
    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, chosenNodes);
    assertEquals(targets.length, 0);
    
    targets = chooseTarget(1, chosenNodes);
    assertEquals(targets.length, 1);
    assertTrue(isOnSameRack(targets[0], dataNodes[0]));
    assertFalse(isOnSameRack(targets[0], dataNodes[2]));
    
    targets = chooseTarget(1, dataNodes[2], chosenNodes);
    assertEquals(targets.length, 1);
    assertTrue(isOnSameRack(targets[0], dataNodes[2]));
    assertFalse(isOnSameRack(targets[0], dataNodes[0]));

    targets = chooseTarget(2, chosenNodes);
    assertEquals(targets.length, 2);
    assertTrue(isOnSameRack(targets[0], dataNodes[0]));
    
    targets = chooseTarget(2, dataNodes[2], chosenNodes);
    assertEquals(targets.length, 2);
    assertTrue(isOnSameRack(targets[0], dataNodes[2]));
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
      final UnderReplicatedBlocks neededReplications = cluster.getNameNode()
          .getNamesystem().getBlockManager().neededReplications;
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

      // Adding QUEUE_REPLICAS_BADLY_DISTRIBUTED block
      underReplicatedBlocks.add(new Block(random.nextLong()), 6, 0, 6);

      // Adding QUEUE_UNDER_REPLICATED block
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
    
    // Refresh the last update time for all the datanodes
    for (int i = 0; i < dataNodes.length; i++) {
      DFSTestUtil.resetLastUpdatesWithOffset(dataNodes[i], 0);
    }
    
    List<DatanodeStorageInfo> first = new ArrayList<DatanodeStorageInfo>();
    List<DatanodeStorageInfo> second = new ArrayList<DatanodeStorageInfo>();
    replicator.splitNodesWithRack(replicaList, rackMap, first, second);
    // storages[0] and storages[1] are in first set as their rack has two 
    // replica nodes, while storages[2] and dataNodes[5] are in second set.
    assertEquals(2, first.size());
    assertEquals(2, second.size());
    List<StorageType> excessTypes = new ArrayList<StorageType>();
    {
      // test returning null
      excessTypes.add(StorageType.SSD);
      assertNull(replicator.chooseReplicaToDelete(
          null, null, (short)3, first, second, excessTypes));
    }
    excessTypes.add(StorageType.DEFAULT);
    DatanodeStorageInfo chosen = replicator.chooseReplicaToDelete(
        null, null, (short)3, first, second, excessTypes);
    // Within first set, storages[1] with less free space
    assertEquals(chosen, storages[1]);

    replicator.adjustSetsWithChosenReplica(rackMap, first, second, chosen);
    assertEquals(0, first.size());
    assertEquals(3, second.size());
    // Within second set, storages[5] with less free space
    excessTypes.add(StorageType.DEFAULT);
    chosen = replicator.chooseReplicaToDelete(
        null, null, (short)2, first, second, excessTypes);
    assertEquals(chosen, storages[5]);
  }

  /**
   * This testcase tests whether the default value returned by
   * DFSUtil.getInvalidateWorkPctPerIteration() is positive, 
   * and whether an IllegalArgumentException will be thrown 
   * when 0.0f is retrieved
   */
  @Test
  public void testGetInvalidateWorkPctPerIteration() {
    Configuration conf = new Configuration();
    float blocksInvalidateWorkPct = DFSUtil
        .getInvalidateWorkPctPerIteration(conf);
    assertTrue(blocksInvalidateWorkPct > 0);

    conf.set(DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION,
        "0.5f");
    blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    assertEquals(blocksInvalidateWorkPct, 0.5f, blocksInvalidateWorkPct * 1e-7);
    
    conf.set(DFSConfigKeys.
        DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION, "1.0f");
    blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    assertEquals(blocksInvalidateWorkPct, 1.0f, blocksInvalidateWorkPct * 1e-7);
    
    conf.set(DFSConfigKeys.
        DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION, "0.0f");
    exception.expect(IllegalArgumentException.class);
    blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
  }
  
  /**
   * This testcase tests whether an IllegalArgumentException 
   * will be thrown when a negative value is retrieved by 
   * DFSUtil#getInvalidateWorkPctPerIteration
   */
  @Test
  public void testGetInvalidateWorkPctPerIteration_NegativeValue() {
    Configuration conf = new Configuration();
    float blocksInvalidateWorkPct = DFSUtil
        .getInvalidateWorkPctPerIteration(conf);
    assertTrue(blocksInvalidateWorkPct > 0);
    
    conf.set(DFSConfigKeys.
        DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION, "-0.5f");
    exception.expect(IllegalArgumentException.class);
    blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
  }
  
  /**
   * This testcase tests whether an IllegalArgumentException 
   * will be thrown when a value greater than 1 is retrieved by 
   * DFSUtil#getInvalidateWorkPctPerIteration
   */
  @Test
  public void testGetInvalidateWorkPctPerIteration_GreaterThanOne() {
    Configuration conf = new Configuration();
    float blocksInvalidateWorkPct = DFSUtil
        .getInvalidateWorkPctPerIteration(conf);
    assertTrue(blocksInvalidateWorkPct > 0);
    
    conf.set(DFSConfigKeys.
        DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION, "1.5f");
    exception.expect(IllegalArgumentException.class);
    blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
  }

  /**
   * This testcase tests whether the value returned by
   * DFSUtil.getReplWorkMultiplier() is positive,
   * and whether an IllegalArgumentException will be thrown 
   * when a non-positive value is retrieved
   */
  @Test
  public void testGetReplWorkMultiplier() {
    Configuration conf = new Configuration();
    int blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);
    assertTrue(blocksReplWorkMultiplier > 0);

    conf.set(DFSConfigKeys.
        DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,"3");
    blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);
    assertEquals(blocksReplWorkMultiplier, 3);
    
    conf.set(DFSConfigKeys.
        DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,"-1");
    exception.expect(IllegalArgumentException.class);
    blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);
  }
}
