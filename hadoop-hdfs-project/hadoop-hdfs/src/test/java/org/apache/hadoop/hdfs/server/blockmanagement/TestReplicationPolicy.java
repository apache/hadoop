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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager.StatefulBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.TestINodeFile;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestReplicationPolicy extends BaseReplicationPolicyTest {

  private static final String filename = "/dummyfile.txt";
  // The interval for marking a datanode as stale,
  private static final long staleInterval =
      DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  public TestReplicationPolicy(String blockPlacementPolicyClassName) {
    this.blockPlacementPolicy = blockPlacementPolicyClassName;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { BlockPlacementPolicyDefault.class.getName() },
        { BlockPlacementPolicyWithUpgradeDomain.class.getName() } });
  }

  private void updateHeartbeatForExtraStorage(long capacity,
      long dfsUsed, long remaining, long blockPoolUsed) {
    DatanodeDescriptor dn = dataNodes[5];
    dn.getStorageInfos()[1].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        0L, 0L, 0, 0, null);
  }

  private void resetHeartbeatForStorages() {
    for (int i=0; i < dataNodes.length; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L,
          0, 0);
    }
    // No available space in the extra storage of dn0
    updateHeartbeatForExtraStorage(0L, 0L, 0L, 0L);
  }

  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    final String[] racks = {
        "/d1/r1",
        "/d1/r1",
        "/d1/r2",
        "/d1/r2",
        "/d2/r3",
        "/d2/r3"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    // create an extra storage for dn5.
    DatanodeStorage extraStorage = new DatanodeStorage(
        storages[5].getStorageID() + "-extra", DatanodeStorage.State.NORMAL,
        StorageType.DEFAULT);
    BlockManagerTestUtil.updateStorage(storages[5].getDatanodeDescriptor(),
        extraStorage);
    return DFSTestUtil.toDatanodeDescriptor(storages);
  }

  /**
   * Test whether the remaining space per storage is individually
   * considered.
   */
  @Test
  public void testChooseNodeWithMultipleStorages1() throws Exception {
    updateHeartbeatWithUsage(dataNodes[5],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (2*HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE)/3, 0L,
        0L, 0L, 0, 0);

    updateHeartbeatForExtraStorage(
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (2*HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE)/3, 0L);

    DatanodeStorageInfo[] targets;
    targets = chooseTarget (1, dataNodes[5],
        new ArrayList<DatanodeStorageInfo>(), null);
    assertEquals(1, targets.length);
    assertEquals(storages[4], targets[0]);

    resetHeartbeatForStorages();
  }

  /**
   * Test whether all storages on the datanode are considered while
   * choosing target to place block.
   */
  @Test
  public void testChooseNodeWithMultipleStorages2() throws Exception {
    updateHeartbeatWithUsage(dataNodes[5],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (2*HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE)/3, 0L,
        0L, 0L, 0, 0);

    updateHeartbeatForExtraStorage(
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L);

    DatanodeStorageInfo[] targets;
    targets = chooseTarget (1, dataNodes[5],
        new ArrayList<DatanodeStorageInfo>(), null);
    assertEquals(1, targets.length);
    assertEquals(dataNodes[5], targets[0].getDatanodeDescriptor());

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
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
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

    resetHeartbeatForStorages();
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
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();

    excludedNodes = new HashSet<>();
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
        excludedNodes, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY,
        null);
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
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L,
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

    resetHeartbeatForStorages();
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
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
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

    resetHeartbeatForStorages();
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
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();

    excludedNodes = new HashSet<>();
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
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);

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
    resetHeartbeatForStorages();
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
        .setNumStaleNodes(dataNodes.length);
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
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
    
    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);
    
    // try to choose NUM_OF_DATANODES which is more than actually available
    // nodes.
    DatanodeStorageInfo[] targets = chooseTarget(dataNodes.length);
    assertEquals(targets.length, dataNodes.length - 2);

    final List<LoggingEvent> log = appender.getLog();
    assertNotNull(log);
    assertFalse(log.size() == 0);
    final LoggingEvent lastLogEntry = log.get(log.size() - 1);
    
    assertTrue(Level.WARN.isGreaterOrEqual(lastLogEntry.getLevel()));
    // Suppose to place replicas on each node but two data nodes are not
    // available for placing replica, so here we expect a short of 2
    assertTrue(((String)lastLogEntry.getMessage()).contains("in need of 2"));

    resetHeartbeatForStorages();
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

    Set<Node> excludedNodes = new HashSet<>();
    excludedNodes.add(dataNodes[1]);
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
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
          BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY,
          null);

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
          TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);
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
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
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
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
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
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<>();
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

  private BlockInfo genBlockInfo(long id) {
    return new BlockInfoContiguous(new Block(id), (short) 3);
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
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(true).build();
    try {
      cluster.waitActive();
      final LowRedundancyBlocks neededReconstruction = cluster.getNameNode()
          .getNamesystem().getBlockManager().neededReconstruction;
      for (int i = 0; i < 100; i++) {
        // Adding the blocks directly to normal priority

        neededReconstruction.add(genBlockInfo(ThreadLocalRandom.current().
            nextLong()), 2, 0, 0, 3);
      }
      // Lets wait for the replication interval, to start process normal
      // priority blocks
      Thread.sleep(DFS_NAMENODE_REPLICATION_INTERVAL);
      
      // Adding the block directly to high priority list
      neededReconstruction.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 1, 0, 0, 3);

      // Lets wait for the replication interval
      Thread.sleep(DFS_NAMENODE_REPLICATION_INTERVAL);

      // Check replication completed successfully. Need not wait till it process
      // all the 100 normal blocks.
      assertFalse("Not able to clear the element from high priority list",
          neededReconstruction.iterator(HIGH_PRIORITY).hasNext());
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Test for the ChooseLowRedundancyBlocks are processed based on priority
   */
  @Test
  public void testChooseLowRedundancyBlocks() throws Exception {
    LowRedundancyBlocks lowRedundancyBlocks = new LowRedundancyBlocks();

    for (int i = 0; i < 5; i++) {
      // Adding QUEUE_HIGHEST_PRIORITY block
      lowRedundancyBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 1, 0, 0, 3);

      // Adding QUEUE_VERY_LOW_REDUNDANCY block
      lowRedundancyBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 2, 0, 0, 7);

      // Adding QUEUE_REPLICAS_BADLY_DISTRIBUTED block
      lowRedundancyBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 6, 0, 0, 6);

      // Adding QUEUE_LOW_REDUNDANCY block
      lowRedundancyBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 5, 0, 0, 6);

      // Adding QUEUE_WITH_CORRUPT_BLOCKS block
      lowRedundancyBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 0, 0, 0, 3);
    }

    // Choose 6 blocks from lowRedundancyBlocks. Then it should pick 5 blocks
    // from QUEUE_HIGHEST_PRIORITY and 1 block from QUEUE_VERY_LOW_REDUNDANCY.
    List<List<BlockInfo>> chosenBlocks =
        lowRedundancyBlocks.chooseLowRedundancyBlocks(6);
    assertTheChosenBlocks(chosenBlocks, 5, 1, 0, 0, 0);

    // Choose 10 blocks from lowRedundancyBlocks. Then it should pick 4 blocks
    // from QUEUE_VERY_LOW_REDUNDANCY, 5 blocks from QUEUE_LOW_REDUNDANCY and 1
    // block from QUEUE_REPLICAS_BADLY_DISTRIBUTED.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(10);
    assertTheChosenBlocks(chosenBlocks, 0, 4, 5, 1, 0);

    // Adding QUEUE_HIGHEST_PRIORITY
    lowRedundancyBlocks.add(genBlockInfo(ThreadLocalRandom.current().
        nextLong()), 0, 1, 0, 3);

    // Choose 10 blocks from lowRedundancyBlocks. Then it should pick 1 block
    // from QUEUE_HIGHEST_PRIORITY, 4 blocks from
    // QUEUE_REPLICAS_BADLY_DISTRIBUTED
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(10);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 4);

    // Since it is reached to end of all lists,
    // should start picking the blocks from start.
    // Choose 7 blocks from lowRedundancyBlocks. Then it should pick 6 blocks
    // from QUEUE_HIGHEST_PRIORITY, 1 block from QUEUE_VERY_LOW_REDUNDANCY.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(7);
    assertTheChosenBlocks(chosenBlocks, 6, 1, 0, 0, 0);
  }
  
  /** asserts the chosen blocks with expected priority blocks */
  private void assertTheChosenBlocks(
      List<List<BlockInfo>> chosenBlocks, int... expectedSizes) {
    int i = 0;
    for(; i < chosenBlocks.size(); i++) {
      assertEquals("Not returned the expected number for i=" + i,
          expectedSizes[i], chosenBlocks.get(i).size());
    }
    for(; i < expectedSizes.length; i++) {
      assertEquals("Expected size is non-zero for i=" + i, 0, expectedSizes[i]);
    }
  }
  
  /**
   * Test for the chooseReplicaToDelete are processed based on 
   * block locality and free space
   */
  @Test
  public void testChooseReplicaToDelete() throws Exception {
    List<DatanodeStorageInfo> replicaList = new ArrayList<>();
    final Map<String, List<DatanodeStorageInfo>> rackMap
        = new HashMap<String, List<DatanodeStorageInfo>>();

    storages[0].setRemainingForTests(4*1024*1024);
    dataNodes[0].setRemaining(calculateRemaining(dataNodes[0]));
    replicaList.add(storages[0]);

    storages[1].setRemainingForTests(3*1024*1024);
    dataNodes[1].setRemaining(calculateRemaining(dataNodes[1]));
    replicaList.add(storages[1]);

    storages[2].setRemainingForTests(2*1024*1024);
    dataNodes[2].setRemaining(calculateRemaining(dataNodes[2]));
    replicaList.add(storages[2]);

    //Even if this node has the most space, because the storage[5] has
    //the lowest it should be chosen in case of block delete.
    storages[4].setRemainingForTests(100 * 1024 * 1024);
    storages[5].setRemainingForTests(512 * 1024);
    dataNodes[5].setRemaining(calculateRemaining(dataNodes[5]));
    replicaList.add(storages[5]);

    // Refresh the last update time for all the datanodes
    for (int i = 0; i < dataNodes.length; i++) {
      DFSTestUtil.resetLastUpdatesWithOffset(dataNodes[i], 0);
    }

    List<DatanodeStorageInfo> first = new ArrayList<>();
    List<DatanodeStorageInfo> second = new ArrayList<>();
    replicator.splitNodesWithRack(replicaList, replicaList, rackMap, first,
        second);
    // storages[0] and storages[1] are in first set as their rack has two 
    // replica nodes, while storages[2] and dataNodes[5] are in second set.
    assertEquals(2, first.size());
    assertEquals(2, second.size());
    List<StorageType> excessTypes = new ArrayList<>();
    {
      // test returning null
      excessTypes.add(StorageType.SSD);
      assertNull(((BlockPlacementPolicyDefault) replicator)
          .chooseReplicaToDelete(first, second, excessTypes, rackMap));
    }
    excessTypes.add(StorageType.DEFAULT);
    DatanodeStorageInfo chosen = ((BlockPlacementPolicyDefault) replicator)
        .chooseReplicaToDelete(first, second, excessTypes, rackMap);
    // Within all storages, storages[5] with least free space
    assertEquals(chosen, storages[5]);

    replicator.adjustSetsWithChosenReplica(rackMap, first, second, chosen);
    assertEquals(2, first.size());
    assertEquals(1, second.size());
    // Within first set, storages[1] with less free space
    excessTypes.add(StorageType.DEFAULT);
    chosen = ((BlockPlacementPolicyDefault) replicator).chooseReplicaToDelete(
        first, second, excessTypes, rackMap);
    assertEquals(chosen, storages[1]);
  }

  private long calculateRemaining(DatanodeDescriptor dataNode) {
    long sum = 0;
    for (DatanodeStorageInfo storageInfo: dataNode.getStorageInfos()){
      sum += storageInfo.getRemaining();
    }
    return sum;
  }

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
    DatanodeStorageInfo excessSSD = DFSTestUtil.createDatanodeStorageInfo(
        "Storage-excess-SSD-ID", "localhost",
        storages[0].getDatanodeDescriptor().getNetworkLocation(),
        "foo.com", StorageType.SSD, null);
    updateHeartbeatWithUsage(excessSSD.getDatanodeDescriptor(),
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0,
        0);

    // use delete hint case.

    DatanodeDescriptor delHintNode = storages[0].getDatanodeDescriptor();
    List<StorageType> excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[3].getDatanodeDescriptor(), delHintNode);
    assertTrue(excessReplicas.size() == 1);
    assertTrue(excessReplicas.contains(storages[0]));

    // Excess type deletion

    DatanodeStorageInfo excessStorage = DFSTestUtil.createDatanodeStorageInfo(
        "Storage-excess-ID", "localhost", delHintNode.getNetworkLocation(),
        "foo.com", StorageType.ARCHIVE, null);
    nonExcess.add(excessStorage);
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[3].getDatanodeDescriptor(), null);
    assertTrue(excessReplicas.contains(excessStorage));

    // The block was initially created on excessSSD(rack r1),
    // storages[4](rack r3) and storages[5](rack r3) with
    // ONESSD_STORAGE_POLICY_NAME storage policy. Replication factor = 3.
    // Right after balancer moves the block from storages[5] to
    // storages[3](rack r2), the application changes the storage policy from
    // ONESSD_STORAGE_POLICY_NAME to HOT_STORAGE_POLICY_ID. In this case,
    // we should be able to delete excessSSD since the remaining
    // storages ({storages[3]}, {storages[4], storages[5]})
    // are on different racks (r2, r3).
    nonExcess.clear();
    nonExcess.add(excessSSD);
    nonExcess.add(storages[3]);
    nonExcess.add(storages[4]);
    nonExcess.add(storages[5]);
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[3].getDatanodeDescriptor(),
        storages[5].getDatanodeDescriptor());
    assertEquals(1, excessReplicas.size());
    assertTrue(excessReplicas.contains(excessSSD));

    // Similar to above, but after policy change and before deletion,
    // the replicas are located on excessSSD(rack r1), storages[1](rack r1),
    // storages[2](rack r2) and storages[3](rack r2). Replication factor = 3.
    // In this case, we should be able to delete excessSSD since the remaining
    // storages ({storages[1]} , {storages[2], storages[3]})
    // are on different racks (r1, r2).
    nonExcess.clear();
    nonExcess.add(excessSSD);
    nonExcess.add(storages[1]);
    nonExcess.add(storages[2]);
    nonExcess.add(storages[3]);
    excessTypes = storagePolicy.chooseExcess((short) 3,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 3,
        excessTypes, storages[1].getDatanodeDescriptor(),
        storages[3].getDatanodeDescriptor());
    assertEquals(1, excessReplicas.size());
    assertTrue(excessReplicas.contains(excessSSD));

    // Similar to above, but after policy change and before deletion,
    // the replicas are located on excessSSD(rack r1), storages[2](rack r2)
    // Replication factor = 1. We should be able to delete excessSSD.
    nonExcess.clear();
    nonExcess.add(excessSSD);
    nonExcess.add(storages[2]);
    excessTypes = storagePolicy.chooseExcess((short) 1,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 1,
        excessTypes, storages[2].getDatanodeDescriptor(), null);
    assertEquals(1, excessReplicas.size());
    assertTrue(excessReplicas.contains(excessSSD));

    // The block was initially created on excessSSD(rack r1),
    // storages[4](rack r3) and storages[5](rack r3) with
    // ONESSD_STORAGE_POLICY_NAME storage policy. Replication factor = 2.
    // In this case, no replica can be chosen as the excessive replica by
    // chooseReplicasToDelete because if the SSD storage is deleted,
    // the remaining storages[4] and storages[5] are the same rack (r3),
    // violating block placement policy (i.e. the number of racks >= 2).
    // TODO BlockPlacementPolicyDefault should be able to rebalance the replicas
    // and then delete excessSSD.
    nonExcess.clear();
    nonExcess.add(excessSSD);
    nonExcess.add(storages[4]);
    nonExcess.add(storages[5]);
    excessTypes = storagePolicy.chooseExcess((short) 2,
        DatanodeStorageInfo.toStorageTypes(nonExcess));
    excessReplicas = replicator.chooseReplicasToDelete(nonExcess, nonExcess, 2,
        excessTypes, null, null);
    assertEquals(0, excessReplicas.size());
  }

 @Test
  public void testUseDelHint() throws Exception {
    List<StorageType> excessTypes = new ArrayList<>();
    excessTypes.add(StorageType.ARCHIVE);
   BlockPlacementPolicyDefault policyDefault =
       (BlockPlacementPolicyDefault) replicator;
    // no delHint
    assertFalse(policyDefault.useDelHint(null, null, null, null, null));
    // delHint storage type is not an excess type
    assertFalse(policyDefault.useDelHint(storages[0], null, null, null,
        excessTypes));
    // check if removing delHint reduces the number of racks
    List<DatanodeStorageInfo> moreThanOne = new ArrayList<>();
    moreThanOne.add(storages[0]);
    moreThanOne.add(storages[1]);
    List<DatanodeStorageInfo> exactlyOne = new ArrayList<>();
    exactlyOne.add(storages[3]);
    exactlyOne.add(storages[5]);

    excessTypes.add(StorageType.DEFAULT);
    assertTrue(policyDefault.useDelHint(storages[0], null, moreThanOne,
            exactlyOne, excessTypes));
    // the added node adds a new rack
    assertTrue(policyDefault.useDelHint(storages[3], storages[5], moreThanOne,
        exactlyOne, excessTypes));
    // removing delHint reduces the number of racks;
    assertFalse(policyDefault.useDelHint(storages[3], storages[0], moreThanOne,
        exactlyOne, excessTypes));
    assertFalse(policyDefault.useDelHint(storages[3], null, moreThanOne,
        exactlyOne, excessTypes));
  }

  @Test
  public void testIsMovable() throws Exception {
    List<DatanodeInfo> candidates = new ArrayList<>();

    // after the move, the number of racks remains 2.
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[1]);
    candidates.add(dataNodes[2]);
    candidates.add(dataNodes[3]);
    assertTrue(replicator.isMovable(candidates, dataNodes[0], dataNodes[3]));

    // after the move, the number of racks remains 3.
    candidates.clear();
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[1]);
    candidates.add(dataNodes[2]);
    candidates.add(dataNodes[4]);
    assertTrue(replicator.isMovable(candidates, dataNodes[0], dataNodes[1]));

    // after the move, the number of racks changes from 2 to 3.
    candidates.clear();
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[1]);
    candidates.add(dataNodes[2]);
    candidates.add(dataNodes[4]);
    assertTrue(replicator.isMovable(candidates, dataNodes[0], dataNodes[4]));

    // the move would have reduced the number of racks from 3 to 2.
    candidates.clear();
    candidates.add(dataNodes[0]);
    candidates.add(dataNodes[2]);
    candidates.add(dataNodes[3]);
    candidates.add(dataNodes[4]);
    assertFalse(replicator.isMovable(candidates, dataNodes[0], dataNodes[3]));
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

  @Test(timeout = 60000)
  public void testUpdateDoesNotCauseSkippedReplication() {
    LowRedundancyBlocks lowRedundancyBlocks = new LowRedundancyBlocks();

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block2 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block3 = genBlockInfo(ThreadLocalRandom.current().nextLong());

    // Adding QUEUE_VERY_LOW_REDUNDANCY block
    final int block1CurReplicas = 2;
    final int block1ExpectedReplicas = 7;
    lowRedundancyBlocks.add(block1, block1CurReplicas, 0, 0,
        block1ExpectedReplicas);

    // Adding QUEUE_VERY_LOW_REDUNDANCY block
    lowRedundancyBlocks.add(block2, 2, 0, 0, 7);

    // Adding QUEUE_LOW_REDUNDANCY block
    lowRedundancyBlocks.add(block3, 2, 0, 0, 6);

    List<List<BlockInfo>> chosenBlocks;

    // Choose 1 block from lowRedundancyBlocks. Then it should pick 1 block
    // from QUEUE_VERY_LOW_REDUNDANCY.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 0, 1, 0, 0, 0);

    // Increasing the replications will move the block down a
    // priority.  This simulates a replica being completed in between checks.
    lowRedundancyBlocks.update(block1, block1CurReplicas+1, 0, 0,
        block1ExpectedReplicas, 1, 0);

    // Choose 1 block from lowRedundancyBlocks. Then it should pick 1 block
    // from QUEUE_VERY_LOW_REDUNDANCY.
    // This block was moved up a priority and should not be skipped over.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 0, 1, 0, 0, 0);

    // Choose 1 block from lowRedundancyBlocks. Then it should pick 1 block
    // from QUEUE_LOW_REDUNDANCY.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 0, 0, 1, 0, 0);
  }

  @Test(timeout = 60000)
  public void testAddStoredBlockDoesNotCauseSkippedReplication()
      throws IOException {
    FSNamesystem mockNS = mock(FSNamesystem.class);
    when(mockNS.hasWriteLock()).thenReturn(true);
    when(mockNS.hasReadLock()).thenReturn(true);
    BlockManager bm = new BlockManager(mockNS, false, new HdfsConfiguration());
    LowRedundancyBlocks lowRedundancyBlocks = bm.neededReconstruction;

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block2 = genBlockInfo(ThreadLocalRandom.current().nextLong());

    // Adding QUEUE_LOW_REDUNDANCY block
    lowRedundancyBlocks.add(block1, 0, 0, 1, 1);

    // Adding QUEUE_LOW_REDUNDANCY block
    lowRedundancyBlocks.add(block2, 0, 0, 1, 1);

    List<List<BlockInfo>> chosenBlocks;

    // Choose 1 block from lowRedundancyBlocks. Then it should pick 1 block
    // from QUEUE_VERY_LOW_REDUNDANCY.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);

    // Adding this block collection to the BlockManager, so that when we add
    // block under construction, the BlockManager will realize the expected
    // replication has been achieved and remove it from the low redundancy
    // queue.
    BlockInfoContiguous info = new BlockInfoContiguous(block1, (short) 1);
    info.convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION, null);
    info.setBlockCollectionId(1000L);

    final INodeFile file = TestINodeFile.createINodeFile(1000L);
    when(mockNS.getBlockCollection(1000L)).thenReturn(file);
    bm.addBlockCollection(info, file);

    // Adding this block will increase its current replication, and that will
    // remove it from the queue.
    bm.addStoredBlockUnderConstruction(new StatefulBlockInfo(info, info,
        ReplicaState.FINALIZED), storages[0]);

    // Choose 1 block from UnderReplicatedBlocks. Then it should pick 1 block
    // from QUEUE_VERY_LOW_REDUNDANCY.
    // This block remains and should not be skipped over.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);
  }

  @Test(timeout = 60000)
  public void
      testConvertLastBlockToUnderConstructionDoesNotCauseSkippedReplication()
          throws IOException {
    Namesystem mockNS = mock(Namesystem.class);
    when(mockNS.hasWriteLock()).thenReturn(true);

    BlockManager bm = new BlockManager(mockNS, false, new HdfsConfiguration());
    LowRedundancyBlocks lowRedundancyBlocks = bm.neededReconstruction;

    long blkID1 = ThreadLocalRandom.current().nextLong();
    if (blkID1 < 0) {
      blkID1 *= -1;
    }
    long blkID2 = ThreadLocalRandom.current().nextLong();
    if (blkID2 < 0) {
      blkID2 *= -1;
    }

    BlockInfo block1 = genBlockInfo(blkID1);
    BlockInfo block2 = genBlockInfo(blkID2);

    // Adding QUEUE_LOW_REDUNDANCY block
    lowRedundancyBlocks.add(block1, 0, 0, 1, 1);

    // Adding QUEUE_LOW_REDUNDANCY block
    lowRedundancyBlocks.add(block2, 0, 0, 1, 1);

    List<List<BlockInfo>> chosenBlocks;

    // Choose 1 block from lowRedundancyBlocks. Then it should pick 1 block
    // from QUEUE_VERY_LOW_REDUNDANCY.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);

    final BlockInfoContiguous info = new BlockInfoContiguous(block1, (short) 1);
    final BlockCollection mbc = mock(BlockCollection.class);
    when(mbc.getId()).thenReturn(1000L);
    when(mbc.getLastBlock()).thenReturn(info);
    when(mbc.getPreferredBlockSize()).thenReturn(block1.getNumBytes() + 1);
    when(mbc.isUnderConstruction()).thenReturn(true);
    ContentSummary cs = mock(ContentSummary.class);
    when(cs.getLength()).thenReturn((long)1);
    when(mbc.computeContentSummary(bm.getStoragePolicySuite())).thenReturn(cs);
    info.setBlockCollectionId(1000);
    bm.addBlockCollection(info, mbc);

    DatanodeStorageInfo[] storageAry = {new DatanodeStorageInfo(
        dataNodes[0], new DatanodeStorage("s1"))};
    info.convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION,
        storageAry);
    DatanodeStorageInfo storage = mock(DatanodeStorageInfo.class);
    DatanodeDescriptor dn = mock(DatanodeDescriptor.class);
    when(dn.isDecommissioned()).thenReturn(true);
    when(storage.getState()).thenReturn(DatanodeStorage.State.NORMAL);
    when(storage.getDatanodeDescriptor()).thenReturn(dn);
    when(storage.removeBlock(any(BlockInfo.class))).thenReturn(true);
    when(storage.addBlock(any(BlockInfo.class))).thenReturn
        (DatanodeStorageInfo.AddBlockResult.ADDED);
    info.addStorage(storage, info);

    BlockInfo lastBlk = mbc.getLastBlock();
    when(mbc.getLastBlock()).thenReturn(lastBlk, info);

    bm.convertLastBlockToUnderConstruction(mbc, 0L);

    // Choose 1 block from lowRedundancyBlocks. Then it should pick 1 block
    // from QUEUE_VERY_LOW_REDUNDANCY.
    // This block remains and should not be skipped over.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);
  }

  @Test(timeout = 60000)
  public void testupdateNeededReplicationsDoesNotCauseSkippedReplication()
      throws IOException {
    Namesystem mockNS = mock(Namesystem.class);
    when(mockNS.hasReadLock()).thenReturn(true);

    BlockManager bm = new BlockManager(mockNS, false, new HdfsConfiguration());
    LowRedundancyBlocks lowRedundancyBlocks = bm.neededReconstruction;

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block2 = genBlockInfo(ThreadLocalRandom.current().nextLong());

    // Adding QUEUE_LOW_REDUNDANCY block
    lowRedundancyBlocks.add(block1, 0, 0, 1, 1);

    // Adding QUEUE_LOW_REDUNDANCY block
    lowRedundancyBlocks.add(block2, 0, 0, 1, 1);

    List<List<BlockInfo>> chosenBlocks;

    // Choose 1 block from lowRedundancyBlocks. Then it should pick 1 block
    // from QUEUE_VERY_LOW_REDUNDANCY.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);

    bm.setReplication((short)0, (short)1, block1);

    // Choose 1 block from UnderReplicatedBlocks. Then it should pick 1 block
    // from QUEUE_VERY_LOW_REDUNDANCY.
    // This block remains and should not be skipped over.
    chosenBlocks = lowRedundancyBlocks.chooseLowRedundancyBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);
  }

  /**
   * In this testcase, passed 2 favored nodes dataNodes[0],dataNodes[1]
   *
   * Both favored nodes should be chosen as target for placing replication and
   * then should fall into BlockPlacement policy for choosing remaining targets
   * ie. third target as local writer rack , forth target on remote rack and
   * fifth on same rack as second.
   *
   * @throws Exception
   */
  @Test
  public void testChooseExcessReplicaApartFromFavoredNodes() throws Exception {
    DatanodeStorageInfo[] targets;
    List<DatanodeDescriptor> expectedTargets =
        new ArrayList<DatanodeDescriptor>();
    expectedTargets.add(dataNodes[0]);
    expectedTargets.add(dataNodes[1]);
    expectedTargets.add(dataNodes[2]);
    expectedTargets.add(dataNodes[4]);
    expectedTargets.add(dataNodes[5]);
    List<DatanodeDescriptor> favouredNodes =
        new ArrayList<DatanodeDescriptor>();
    favouredNodes.add(dataNodes[0]);
    favouredNodes.add(dataNodes[1]);
    targets = chooseTarget(5, dataNodes[2], null, favouredNodes);
    assertEquals(targets.length, 5);
    for (int i = 0; i < targets.length; i++) {
      assertTrue("Target should be a part of Expected Targets",
          expectedTargets.contains(targets[i].getDatanodeDescriptor()));
    }
  }

  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, Set<Node> excludedNodes,
      List<DatanodeDescriptor> favoredNodes) {
    return chooseTarget(numOfReplicas, writer, excludedNodes,
        favoredNodes, null);
  }

  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
    DatanodeDescriptor writer, Set<Node> excludedNodes,
        List<DatanodeDescriptor> favoredNodes, EnumSet<AddBlockFlag> flags) {
    return replicator.chooseTarget(filename, numOfReplicas, writer,
        excludedNodes, BLOCK_SIZE, favoredNodes,
        TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, flags);
  }

  @Test
  public void testAvoidLocalWrite() throws IOException {
    DatanodeDescriptor writer = dataNodes[2];
    EnumSet<AddBlockFlag> flags = EnumSet.of(AddBlockFlag.NO_LOCAL_WRITE);
    DatanodeStorageInfo[] targets;
    targets = chooseTarget(5, writer, null, null, flags);
    for (DatanodeStorageInfo info : targets) {
      assertNotEquals(info.getDatanodeDescriptor(), writer);
    }
  }

  @Test
  public void testAvoidLocalWriteNoEnoughNodes() throws IOException {
    DatanodeDescriptor writer = dataNodes[2];
    EnumSet<AddBlockFlag> flags = EnumSet.of(AddBlockFlag.NO_LOCAL_WRITE);
    DatanodeStorageInfo[] targets;
    targets = chooseTarget(6, writer, null, null, flags);
    assertEquals(6, targets.length);
    boolean found = false;
    for (DatanodeStorageInfo info : targets) {
      if (info.getDatanodeDescriptor().equals(writer)) {
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test
  public void testMaxLoad() {
    FSClusterStats statistics = mock(FSClusterStats.class);
    DatanodeDescriptor node = mock(DatanodeDescriptor.class);

    when(statistics.getInServiceXceiverAverage()).thenReturn(0.0);
    when(node.getXceiverCount()).thenReturn(1);

    final Configuration conf = new Configuration();
    final Class<? extends BlockPlacementPolicy> replicatorClass = conf
        .getClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
            DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_DEFAULT,
            BlockPlacementPolicy.class);
    BlockPlacementPolicy bpp = ReflectionUtils.
        newInstance(replicatorClass, conf);
    assertTrue(bpp instanceof  BlockPlacementPolicyDefault);

    BlockPlacementPolicyDefault bppd = (BlockPlacementPolicyDefault) bpp;
    bppd.initialize(conf, statistics, null, null);
    assertFalse(bppd.excludeNodeByLoad(node));

    when(statistics.getInServiceXceiverAverage()).thenReturn(1.0);
    when(node.getXceiverCount()).thenReturn(10);
    assertTrue(bppd.excludeNodeByLoad(node));

  }
}
