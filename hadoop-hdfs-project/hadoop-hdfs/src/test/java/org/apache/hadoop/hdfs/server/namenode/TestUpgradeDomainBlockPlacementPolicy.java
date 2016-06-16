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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithUpgradeDomain;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;

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
      {"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1",
          "127.0.0.1", "127.0.0.1"};
  static final String[] upgradeDomains =
      {"ud5", "ud2", "ud3", "ud1", "ud2", "ud4"};
  static final Set<DatanodeID> expectedDatanodeIDs = new HashSet<>();
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;
  private HostsFileWriter hostsFileWriter = new HostsFileWriter();

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
    hostsFileWriter.initialize(conf, "temp/upgradedomainpolicy");

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(6).racks(racks)
        .hosts(hosts).build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestDefaultBlockPlacementPolicy", null,
        FsPermission.getDefault());
    refreshDatanodeAdminProperties();
  }

  @After
  public void teardown() throws IOException {
    hostsFileWriter.cleanup();
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Define admin properties for these datanodes as follows.
   * dn0's upgrade domain is ud5.
   * dn1's upgrade domain is ud2.
   * dn2's upgrade domain is ud3.
   * dn3's upgrade domain is ud1.
   * dn4's upgrade domain is ud2.
   * dn5's upgrade domain is ud4.
   * dn0 and dn5 are decommissioned.
   * Given dn0, dn1 and dn2 are on rack1 and dn3, dn4 and dn5 are on
   * rack2. Then any block's replicas should be on either
   * {dn1, dn2, d3} or {dn2, dn3, dn4}.
   */
  private void refreshDatanodeAdminProperties()
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

    expectedDatanodeIDs.clear();
    expectedDatanodeIDs.add(cluster.getDataNodes().get(2).getDatanodeId());
    expectedDatanodeIDs.add(cluster.getDataNodes().get(3).getDatanodeId());
  }

  /**
   * Define admin properties for these datanodes as follows.
   * dn0's upgrade domain is ud5.
   * dn1's upgrade domain is ud2.
   * dn2's upgrade domain is ud3.
   * dn3's upgrade domain is ud1.
   * dn4's upgrade domain is ud2.
   * dn5's upgrade domain is ud4.
   * dn2 and dn3 are decommissioned.
   * Given dn0, dn1 and dn2 are on rack1 and dn3, dn4 and dn5 are on
   * rack2. Then any block's replicas should be on either
   * {dn0, dn1, d5} or {dn0, dn4, dn5}.
   */
  private void refreshDatanodeAdminProperties2()
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
    datanodes[2].setAdminState(DatanodeInfo.AdminStates.DECOMMISSIONED);
    datanodes[3].setAdminState(DatanodeInfo.AdminStates.DECOMMISSIONED);
    hostsFileWriter.initIncludeHosts(datanodes);
    cluster.getFileSystem().refreshNodes();

    expectedDatanodeIDs.clear();
    expectedDatanodeIDs.add(cluster.getDataNodes().get(0).getDatanodeId());
    expectedDatanodeIDs.add(cluster.getDataNodes().get(5).getDatanodeId());
  }

  @Test
  public void testPlacement() throws Exception {
    final long fileSize = DEFAULT_BLOCK_SIZE * 5;
    final String testFile = new String("/testfile");
    final Path path = new Path(testFile);
    DFSTestUtil.createFile(cluster.getFileSystem(), path, fileSize,
        REPLICATION_FACTOR, 1000L);
    LocatedBlocks locatedBlocks =
        cluster.getFileSystem().getClient().getLocatedBlocks(
            path.toString(), 0, fileSize);
    for (LocatedBlock block : locatedBlocks.getLocatedBlocks()) {
      Set<DatanodeInfo> locs = new HashSet<>();
      for(DatanodeInfo datanodeInfo : block.getLocations()) {
        if (datanodeInfo.getAdminState() == DatanodeInfo.AdminStates.NORMAL) {
          locs.add(datanodeInfo);
        }
      }
      for (DatanodeID datanodeID : expectedDatanodeIDs) {
        assertTrue(locs.contains(datanodeID));
      }
    }
  }

  @Test(timeout = 300000)
  public void testPlacementAfterDecommission() throws Exception {
    final long fileSize = DEFAULT_BLOCK_SIZE * 5;
    final String testFile = new String("/testfile");
    final Path path = new Path(testFile);
    DFSTestUtil.createFile(cluster.getFileSystem(), path, fileSize,
        REPLICATION_FACTOR, 1000L);

    // Decommission some nodes and wait until decommissions have finished.
    refreshDatanodeAdminProperties2();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        boolean successful = true;
        LocatedBlocks locatedBlocks;
        try {
          locatedBlocks =
              cluster.getFileSystem().getClient().getLocatedBlocks(
                  path.toString(), 0, fileSize);
        } catch (IOException ioe) {
          return false;
        }
        for(LocatedBlock block : locatedBlocks.getLocatedBlocks()) {
          Set<DatanodeInfo> locs = new HashSet<>();
          for (DatanodeInfo datanodeInfo : block.getLocations()) {
            if (datanodeInfo.getAdminState() ==
                DatanodeInfo.AdminStates.NORMAL) {
              locs.add(datanodeInfo);
            }
          }
          for (DatanodeID datanodeID : expectedDatanodeIDs) {
            successful = successful && locs.contains(datanodeID);
          }
        }
        return successful;
      }
    }, 1000, 60000);

    // Verify block placement policy of each block.
    LocatedBlocks locatedBlocks;
    locatedBlocks =
        cluster.getFileSystem().getClient().getLocatedBlocks(
            path.toString(), 0, fileSize);
    for(LocatedBlock block : locatedBlocks.getLocatedBlocks()) {
      BlockPlacementStatus status = cluster.getNamesystem().getBlockManager().
          getBlockPlacementPolicy().verifyBlockPlacement(
              block.getLocations(), REPLICATION_FACTOR);
      assertTrue(status.isPlacementPolicySatisfied());
    }
  }
}
