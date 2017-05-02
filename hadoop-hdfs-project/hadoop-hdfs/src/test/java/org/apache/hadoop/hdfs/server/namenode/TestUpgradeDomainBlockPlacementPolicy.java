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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithUpgradeDomain;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.apache.hadoop.net.StaticMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end test case for upgrade domain
 * The test configs upgrade domain for nodes via admin json
 * config file and put some nodes to decommission state.
 * The test then verifies replicas are placed on the nodes that
 * satisfy the upgrade domain policy.
 *
 */
public class TestUpgradeDomainBlockPlacementPolicy {

  private static final short REPLICATION_FACTOR = (short) 3;
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  static final String[] racks =
      { "/RACK1", "/RACK1", "/RACK1", "/RACK2", "/RACK2", "/RACK2" };
  /**
   *  Use host names that can be resolved (
   *  InetSocketAddress#isUnresolved == false). Otherwise,
   *  CombinedHostFileManager won't allow those hosts.
   */
  static final String[] hosts =
      { "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1",
          "127.0.0.1", "127.0.0.1" };
  static final String[] upgradeDomains =
      { "ud1", "ud2", "ud3", "ud1", "ud2", "ud3" };
  static final Set<DatanodeID> expectedDatanodeIDs = new HashSet<>();
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithUpgradeDomain.class,
        BlockPlacementPolicy.class);
    conf.setClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
        CombinedHostFileManager.class, HostConfigManager.class);
    HostsFileWriter hostsFileWriter = new HostsFileWriter();
    hostsFileWriter.initialize(conf, "temp/upgradedomainpolicy");

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(6).racks(racks)
        .hosts(hosts).build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestDefaultBlockPlacementPolicy", null,
        FsPermission.getDefault());
    refreshDatanodeAdminProperties(hostsFileWriter);
    hostsFileWriter.cleanup();
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Define admin properties for these datanodes as follows.
   * dn0 and dn3 have upgrade domain ud1.
   * dn1 and dn4 have upgrade domain ud2.
   * dn2 and dn5 have upgrade domain ud3.
   * dn0 and dn5 are decommissioned.
   * Given dn0, dn1 and dn2 are on rack1 and dn3, dn4 and dn5 are on
   * rack2. Then any block's replicas should be on either
   * {dn1, dn2, d3} or {dn2, dn3, dn4}.
   */
  private void refreshDatanodeAdminProperties(HostsFileWriter hostsFileWriter)
      throws IOException {
    DatanodeAdminProperties[] datanodes = new DatanodeAdminProperties[
        hosts.length];
    for (int i = 0; i < hosts.length; i++) {
      datanodes[i] = new DatanodeAdminProperties();
      DatanodeID datanodeID = cluster.getDataNodes().get(i).getDatanodeId();
      datanodes[i].setHostName(datanodeID.getHostName());
      datanodes[i].setPort(datanodeID.getXferPort());
      datanodes[i].setUpgradeDomain(upgradeDomains[i]);
    }
    datanodes[0].setAdminState(DatanodeInfo.AdminStates.DECOMMISSIONED);
    datanodes[5].setAdminState(DatanodeInfo.AdminStates.DECOMMISSIONED);
    hostsFileWriter.initIncludeHosts(datanodes);
    cluster.getFileSystem().refreshNodes();

    expectedDatanodeIDs.add(cluster.getDataNodes().get(2).getDatanodeId());
    expectedDatanodeIDs.add(cluster.getDataNodes().get(3).getDatanodeId());
  }

  @Test
  public void testPlacement() throws Exception {
    String clientMachine = "127.0.0.1";
    for (int i = 0; i < 5; i++) {
      String src = "/test-" + i;
      // Create the file with client machine
      HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
          clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
          REPLICATION_FACTOR, DEFAULT_BLOCK_SIZE, null, false);
      LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
          null, null, fileStatus.getFileId(), null, null);

      assertEquals("Block should be allocated sufficient locations",
          REPLICATION_FACTOR, locatedBlock.getLocations().length);
      Set<DatanodeInfo> locs = new HashSet<>(Arrays.asList(
          locatedBlock.getLocations()));
      for (DatanodeID datanodeID : expectedDatanodeIDs) {
        locs.contains(datanodeID);
      }

      nameNodeRpc.abandonBlock(locatedBlock.getBlock(), fileStatus.getFileId(),
          src, clientMachine);
    }
  }
}
