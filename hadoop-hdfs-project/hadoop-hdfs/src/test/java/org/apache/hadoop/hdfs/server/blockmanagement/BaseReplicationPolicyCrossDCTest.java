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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDatacenterCount;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for BlockPlacementPolicyCrossDC tests.
 * Configures DFSNetworkTopologyWithDatacenterCount to enable proper datacenter
 * count tracking for CrossDC block placement policy.
 */
abstract public class BaseReplicationPolicyCrossDCTest extends BaseReplicationPolicyTest {

  public BaseReplicationPolicyCrossDCTest() {
    this.blockPlacementPolicy = BlockPlacementPolicyCrossDC.class.getName();
  }

  @Override
  @BeforeEach
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

    // Use DFSNetworkTopologyWithDatacenterCount for CrossDC policy
    conf.set(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDatacenterCount.class.getName());

    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);
    nameNodeRpc = namenode.getRpcServer();

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    replicator = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    dnManager = bm.getDatanodeManager();
    // construct network topology
    for (int i = 0; i < dataNodes.length; i++) {
      cluster.add(dataNodes[i]);
      bm.getDatanodeManager().getHeartbeatManager().addDatanode(
          dataNodes[i]);
      bm.getDatanodeManager().getHeartbeatManager().updateDnStat(
          dataNodes[i]);
    }
    updateHeartbeatWithUsage();
  }
}