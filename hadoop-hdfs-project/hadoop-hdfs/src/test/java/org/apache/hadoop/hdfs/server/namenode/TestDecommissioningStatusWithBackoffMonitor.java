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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement
    .DatanodeAdminMonitorInterface;
import org.apache.hadoop.hdfs.server.blockmanagement
    .DatanodeAdminBackoffMonitor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Extends the TestDecommissioningStatus class to provide the same set of
 * tests for the backoff Monitor version.
 */

public class TestDecommissioningStatusWithBackoffMonitor
    extends TestDecommissioningStatus {

  private final long seed = 0xDEADBEEFL;
  private final int blockSize = 8192;
  private final int fileSize = 16384;
  private final int numDatanodes = 2;
  private MiniDFSCluster cluster;
  private FileSystem fileSys;
  private HostsFileWriter hostsFileWriter;
  private Configuration conf;

  @Override
  public void setUp() throws Exception {
    conf = setupConfig();

    conf.setClass(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MONITOR_CLASS,
        DatanodeAdminBackoffMonitor.class, DatanodeAdminMonitorInterface.class);
    createCluster();
    this.cluster = super.getCluster();
    this.fileSys = super.getFileSys();
    this.hostsFileWriter = super.getHostsFileWriter();

  }

  /**
   * This test is almost a copy of the original in the parent class, but due to
   * how the backoff monitor works, it needs to run the check loop twice after a
   * node is decommissioned to get the stats to update.
   * @throws Exception
   */
  @Test
  public void testDecommissionStatus() throws Exception {
    InetSocketAddress addr = new InetSocketAddress("localhost", cluster
        .getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info =
        client.datanodeReport(HdfsConstants.DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", 2, info.length);
    DistributedFileSystem distFileSys = cluster.getFileSystem();
    DFSAdmin admin = new DFSAdmin(cluster.getConfiguration(0));

    short replicas = numDatanodes;
    //
    // Decommission one node. Verify the decommission status
    //
    Path file1 = new Path("decommission.dat");
    DFSTestUtil.createFile(distFileSys, file1, fileSize, fileSize, blockSize,
        replicas, seed);

    Path file2 = new Path("decommission1.dat");
    FSDataOutputStream st1 = AdminStatesBaseTest.writeIncompleteFile(
        distFileSys, file2, replicas, (short)(fileSize / blockSize));
    for (DataNode d: cluster.getDataNodes()) {
      DataNodeTestUtils.triggerBlockReport(d);
    }

    FSNamesystem fsn = cluster.getNamesystem();
    final DatanodeManager dm = fsn.getBlockManager().getDatanodeManager();
    for (int iteration = 0; iteration < numDatanodes; iteration++) {
      String downnode = decommissionNode(client, iteration);
      dm.refreshNodes(conf);
      decommissionedNodes.add(downnode);
      BlockManagerTestUtil.recheckDecommissionState(dm);
      final List<DatanodeDescriptor> decommissioningNodes
          = dm.getDecommissioningNodes();
      if (iteration == 0) {
        assertEquals(decommissioningNodes.size(), 1);
        // Due to how the alternative decom monitor works, we need to run
        // through the check loop a second time to get stats updated
        BlockManagerTestUtil.recheckDecommissionState(dm);
        DatanodeDescriptor decommNode = decommissioningNodes.get(0);
        checkDecommissionStatus(decommNode, 3, 0, 1);
        checkDFSAdminDecommissionStatus(decommissioningNodes.subList(0, 1),
            distFileSys, admin);
      } else {
        assertEquals(decommissioningNodes.size(), 2);
        // Due to how the alternative decom monitor works, we need to run
        // through the check loop a second time to get stats updated
        BlockManagerTestUtil.recheckDecommissionState(dm);
        DatanodeDescriptor decommNode1 = decommissioningNodes.get(0);
        DatanodeDescriptor decommNode2 = decommissioningNodes.get(1);
        // This one is still 3,3,1 since it passed over the UC block
        // earlier, before node 2 was decommed
        checkDecommissionStatus(decommNode1, 3, 3, 1);
        // This one is 4,4,2 since it has the full state
        checkDecommissionStatus(decommNode2, 4, 4, 2);
        checkDFSAdminDecommissionStatus(decommissioningNodes.subList(0, 2),
            distFileSys, admin);
      }
    }
    // Call refreshNodes on FSNamesystem with empty exclude file.
    // This will remove the datanodes from decommissioning list and
    // make them available again.
    hostsFileWriter.initExcludeHost("");
    dm.refreshNodes(conf);
    st1.close();
    AdminStatesBaseTest.cleanupFile(fileSys, file1);
    AdminStatesBaseTest.cleanupFile(fileSys, file2);
  }

}