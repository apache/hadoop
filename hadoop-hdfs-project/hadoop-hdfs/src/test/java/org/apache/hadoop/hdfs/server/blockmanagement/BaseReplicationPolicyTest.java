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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.slf4j.event.Level;

abstract public class BaseReplicationPolicyTest {
  {
    GenericTestUtils.setLogLevel(BlockPlacementPolicy.LOG, Level.TRACE);
  }

  protected NetworkTopology cluster;
  protected DatanodeDescriptor dataNodes[];
  protected static final int BLOCK_SIZE = 1024;
  protected NameNode namenode;
  protected DatanodeManager dnManager;
  protected BlockPlacementPolicy replicator;
  private BlockPlacementPolicy striptedPolicy;
  protected final String filename = "/dummyfile.txt";
  protected DatanodeStorageInfo[] storages;
  protected String blockPlacementPolicy;
  protected NamenodeProtocols nameNodeRpc = null;

  void updateHeartbeatWithUsage(DatanodeDescriptor dn,
    long capacity, long dfsUsed, long remaining, long blockPoolUsed,
    long dnCacheCapacity, long dnCacheUsed, int xceiverCount,
    int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dnManager.getHeartbeatManager().updateHeartbeat(dn,
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures, null);
  }

  abstract DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf);

  @Before
  public void setupCluster() throws Exception {
    Configuration conf = new HdfsConfiguration();
    dataNodes = getDatanodeDescriptors(conf);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestReplicationPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        blockPlacementPolicy);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);
    nameNodeRpc = namenode.getRpcServer();

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    replicator = bm.getBlockPlacementPolicy();
    striptedPolicy = bm.getStriptedBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    dnManager = bm.getDatanodeManager();
    // construct network topology
    for (int i=0; i < dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
      bm.getDatanodeManager().getHeartbeatManager().addDatanode(
          dataNodes[i]);
      bm.getDatanodeManager().getHeartbeatManager().updateDnStat(
          dataNodes[i]);
    }
    updateHeartbeatWithUsage();
  }

  void updateHeartbeatWithUsage() {
    for (int i=0; i < dataNodes.length; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
  }

  public BlockPlacementPolicy getStriptedPolicy() {
    return striptedPolicy;
  }

  @After
  public void tearDown() throws Exception {
    namenode.stop();
  }

  boolean isOnSameRack(DatanodeStorageInfo left, DatanodeStorageInfo right) {
    return isOnSameRack(left, right.getDatanodeDescriptor());
  }

  boolean isOnSameRack(DatanodeStorageInfo left, DatanodeDescriptor right) {
    return cluster.isOnSameRack(left.getDatanodeDescriptor(), right);
  }

  DatanodeStorageInfo[] chooseTarget(int numOfReplicas) {
    return chooseTarget(numOfReplicas, dataNodes[0]);
  }

  DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer) {
    return chooseTarget(numOfReplicas, writer,
        new ArrayList<DatanodeStorageInfo>());
  }

  DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      List<DatanodeStorageInfo> chosenNodes) {
    return chooseTarget(numOfReplicas, dataNodes[0], chosenNodes);
  }

  DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeStorageInfo> chosenNodes) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null);
  }

  DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      List<DatanodeStorageInfo> chosenNodes, Set<Node> excludedNodes) {
    return chooseTarget(numOfReplicas, dataNodes[0], chosenNodes,
        excludedNodes);
  }

  DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
     DatanodeDescriptor writer, List<DatanodeStorageInfo> chosenNodes,
     Set<Node> excludedNodes) {
    return replicator.chooseTarget(filename, numOfReplicas, writer,
        chosenNodes, false, excludedNodes, BLOCK_SIZE,
        TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);
  }
}
