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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.net.StaticMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultBlockPlacementPolicy {

  private static final short REPLICATION_FACTOR = (short) 3;
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    final String[] racks = { "/RACK0", "/RACK0", "/RACK2", "/RACK3", "/RACK2" };
    final String[] hosts = { "/host0", "/host1", "/host2", "/host3", "/host4" };

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).racks(racks)
        .hosts(hosts).build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestDefaultBlockPlacementPolicy", null,
        FsPermission.getDefault());
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Verify rack-local node selection for the rack-local client in case of no
   * local node
   */
  @Test
  public void testLocalRackPlacement() throws Exception {
    String clientMachine = "client.foo.com";
    // Map client to RACK2
    String clientRack = "/RACK2";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    testPlacement(clientMachine, clientRack, true);
  }

  /**
   * Verify local node selection
   */
  @Test
  public void testLocalStoragePlacement() throws Exception {
    String clientMachine = "/host3";
    testPlacement(clientMachine, "/RACK3", true);
  }

  /**
   * Verify decommissioned nodes should not be selected.
   */
  @Test
  public void testPlacementWithLocalRackNodesDecommissioned() throws Exception {
    String clientMachine = "client.foo.com";
    // Map client to RACK3
    String clientRack = "/RACK3";
    StaticMapping.addNodeToRack(clientMachine, clientRack);
    final DatanodeManager dnm = namesystem.getBlockManager().getDatanodeManager();
    DatanodeDescriptor dnd3 = dnm.getDatanode(
        cluster.getDataNodes().get(3).getDatanodeId());
    assertEquals(dnd3.getNetworkLocation(), clientRack);
    dnm.getDecomManager().startDecommission(dnd3);
    try {
      testPlacement(clientMachine, clientRack, false);
    } finally {
      dnm.getDecomManager().stopDecommission(dnd3);
    }
  }

  /**
   * Verify Random rack node selection for remote client
   */
  @Test
  public void testRandomRackSelectionForRemoteClient() throws Exception {
    String clientMachine = "client.foo.com";
    // Don't map client machine to any rack,
    // so by default it will be treated as /default-rack
    // in that case a random node should be selected as first node.
    testPlacement(clientMachine, null, true);
  }

  private void testPlacement(String clientMachine,
      String clientRack, boolean hasBlockReplicaOnRack) throws IOException {
    // write 5 files and check whether all times block placed
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
      if (clientRack != null) {
        if (hasBlockReplicaOnRack) {
          assertEquals("First datanode should be rack local", clientRack,
              locatedBlock.getLocations()[0].getNetworkLocation());
        } else {
          for (DatanodeInfo dni : locatedBlock.getLocations()) {
            assertNotEquals(clientRack, dni.getNetworkLocation());
          }
        }
      }
      nameNodeRpc.abandonBlock(locatedBlock.getBlock(), fileStatus.getFileId(),
          src, clientMachine);
    }
  }
}
