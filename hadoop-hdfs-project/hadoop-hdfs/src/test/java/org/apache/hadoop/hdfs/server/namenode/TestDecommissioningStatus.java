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
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.TimeoutException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.AdminStatesBaseTest;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAdminManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommissioningStatus {
  private final long seed = 0xDEADBEEFL;
  private final int blockSize = 8192;
  private final int fileSize = 16384;
  private final int numDatanodes = 2;
  private MiniDFSCluster cluster;
  private FileSystem fileSys;
  private HostsFileWriter hostsFileWriter;
  private Configuration conf;
  private Logger LOG;

  final ArrayList<String> decommissionedNodes = new ArrayList<String>(numDatanodes);

  protected MiniDFSCluster getCluster() {
    return cluster;
  }

  protected FileSystem getFileSys() {
    return fileSys;
  }

  protected HostsFileWriter getHostsFileWriter() {
    return hostsFileWriter;
  }

  protected Configuration setupConfig() throws Exception  {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);

    // Set up the hosts/exclude files.
    hostsFileWriter = new HostsFileWriter();
    hostsFileWriter.initialize(conf, "work-dir/decommission");
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY, 4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY, 1);
    Logger.getLogger(DatanodeAdminManager.class).setLevel(Level.DEBUG);
    LOG = Logger.getLogger(TestDecommissioningStatus.class);
    return conf;
  }

  protected void createCluster() throws Exception {
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    cluster.getNamesystem().getBlockManager().getDatanodeManager()
        .setHeartbeatExpireInterval(3000);
  }

  @Before
  public void setUp() throws Exception {
    setupConfig();
    createCluster();
  }

  @After
  public void tearDown() throws Exception {
    if (hostsFileWriter != null) {
      hostsFileWriter.cleanup();
    }
    if(fileSys != null) fileSys.close();
    if(cluster != null) cluster.shutdown();
  }

  /*
   * Decommissions the node at the given index
   */
  protected String decommissionNode(DFSClient client,
      int nodeIndex) throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    String nodename = info[nodeIndex].getXferAddr();
    decommissionNode(nodename);
    return nodename;
  }

  /*
   * Decommissions the node by name
   */
  protected void decommissionNode(String dnName)
      throws IOException {
    System.out.println("Decommissioning node: " + dnName);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>(decommissionedNodes);
    nodes.add(dnName);
    hostsFileWriter.initExcludeHosts(nodes);
  }

  protected void checkDecommissionStatus(DatanodeDescriptor decommNode,
      int expectedUnderRep, int expectedDecommissionOnly,
      int expectedUnderRepInOpenFiles) {
    assertEquals("Unexpected num under-replicated blocks",
        expectedUnderRep,
        decommNode.getLeavingServiceStatus().getUnderReplicatedBlocks());
    assertEquals("Unexpected number of decom-only replicas",
        expectedDecommissionOnly,
        decommNode.getLeavingServiceStatus().getOutOfServiceOnlyReplicas());
    assertEquals(
        "Unexpected number of replicas in under-replicated open files",
        expectedUnderRepInOpenFiles,
        decommNode.getLeavingServiceStatus().getUnderReplicatedInOpenFiles());
  }

  protected void checkDFSAdminDecommissionStatus(
      List<DatanodeDescriptor> expectedDecomm, DistributedFileSystem dfs,
      DFSAdmin admin) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    PrintStream oldOut = System.out;
    System.setOut(ps);
    try {
      // Parse DFSAdmin just to check the count
      admin.report(new String[] {"-decommissioning"}, 0);
      String[] lines = baos.toString().split("\n");
      Integer num = null;
      int count = 0;
      for (String line: lines) {
        if (line.startsWith("Decommissioning datanodes")) {
          // Pull out the "(num)" and parse it into an int
          String temp = line.split(" ")[2];
          num =
              Integer.parseInt((String) temp.subSequence(1, temp.length() - 2));
        }
        if (line.contains("Decommission in progress")) {
          count++;
        }
      }
      assertTrue("No decommissioning output", num != null);
      assertEquals("Unexpected number of decomming DNs", expectedDecomm.size(),
          num.intValue());
      assertEquals("Unexpected number of decomming DNs", expectedDecomm.size(),
          count);

      // Check Java API for correct contents
      List<DatanodeInfo> decomming =
          new ArrayList<DatanodeInfo>(Arrays.asList(dfs
              .getDataNodeStats(DatanodeReportType.DECOMMISSIONING)));
      assertEquals("Unexpected number of decomming DNs", expectedDecomm.size(),
          decomming.size());
      for (DatanodeID id : expectedDecomm) {
        assertTrue("Did not find expected decomming DN " + id,
            decomming.contains(id));
      }
    } finally {
      System.setOut(oldOut);
    }
  }

  /**
   * Allows the main thread to block until the decommission is checked by the
   * admin manager.
   * @param dnAdminMgr admin instance in the datanode manager.
   * @param trackedNumber number of nodes expected to be DECOMMISSIONED or
   *        IN_MAINTENANCE.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForDecommissionedNodes(final DatanodeAdminManager dnAdminMgr,
      final int trackedNumber)
      throws TimeoutException, InterruptedException {
    GenericTestUtils
        .waitFor(() -> dnAdminMgr.getNumTrackedNodes() == trackedNumber,
            100, 2000);
  }

  /**
   * Tests Decommissioning Status in DFS.
   */
  @Test
  public void testDecommissionStatus() throws Exception {
    InetSocketAddress addr = new InetSocketAddress("localhost", cluster
        .getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", 2, info.length);
    DistributedFileSystem fileSys = cluster.getFileSystem();
    DFSAdmin admin = new DFSAdmin(cluster.getConfiguration(0));

    short replicas = numDatanodes;
    //
    // Decommission one node. Verify the decommission status
    //
    Path file1 = new Path("decommission.dat");
    DFSTestUtil.createFile(fileSys, file1, fileSize, fileSize, blockSize,
        replicas, seed);

    Path file2 = new Path("decommission1.dat");
    FSDataOutputStream st1 = AdminStatesBaseTest.writeIncompleteFile(fileSys,
        file2, replicas, (short)(fileSize / blockSize));
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
      // Block until the admin's monitor updates the number of tracked nodes.
      waitForDecommissionedNodes(dm.getDatanodeAdminManager(), iteration + 1);
      final List<DatanodeDescriptor> decommissioningNodes = dm.getDecommissioningNodes();
      if (iteration == 0) {
        assertEquals(decommissioningNodes.size(), 1);
        DatanodeDescriptor decommNode = decommissioningNodes.get(0);
        checkDecommissionStatus(decommNode, 3, 0, 1);
        checkDFSAdminDecommissionStatus(decommissioningNodes.subList(0, 1),
            fileSys, admin);
      } else {
        assertEquals(decommissioningNodes.size(), 2);
        DatanodeDescriptor decommNode1 = decommissioningNodes.get(0);
        DatanodeDescriptor decommNode2 = decommissioningNodes.get(1);
        // This one is still 3,3,1 since it passed over the UC block
        // earlier, before node 2 was decommed
        checkDecommissionStatus(decommNode1, 3, 3, 1);
        // This one is 4,4,2 since it has the full state
        checkDecommissionStatus(decommNode2, 4, 4, 2);
        checkDFSAdminDecommissionStatus(decommissioningNodes.subList(0, 2),
            fileSys, admin);
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

  /**
   * Verify a DN remains in DECOMMISSION_INPROGRESS state if it is marked
   * as dead before decommission has completed. That will allow DN to resume
   * the replication process after it rejoins the cluster.
   */
  @Test(timeout=120000)
  public void testDecommissionStatusAfterDNRestart() throws Exception {
    DistributedFileSystem fileSys =
        (DistributedFileSystem)cluster.getFileSystem();

    // Create a file with one block. That block has one replica.
    Path f = new Path("decommission.dat");
    DFSTestUtil.createFile(fileSys, f, fileSize, fileSize, fileSize,
        (short)1, seed);

    // Find the DN that owns the only replica.
    RemoteIterator<LocatedFileStatus> fileList = fileSys.listLocatedStatus(f);
    BlockLocation[] blockLocations = fileList.next().getBlockLocations();
    String dnName = blockLocations[0].getNames()[0];

    // Decommission the DN.
    FSNamesystem fsn = cluster.getNamesystem();
    final DatanodeManager dm = fsn.getBlockManager().getDatanodeManager();
    decommissionNode(dnName);
    dm.refreshNodes(conf);

    // Stop the DN when decommission is in progress.
    // Given DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY is to 1 and the size of
    // the block, it will take much longer time that test timeout value for
    // the decommission to complete. So when stopDataNode is called,
    // decommission should be in progress.
    DataNodeProperties dataNodeProperties = cluster.stopDataNode(dnName);
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    while (true) {
      dm.fetchDatanodes(null, dead, false);
      if (dead.size() == 1) {
        break;
      }
      Thread.sleep(1000);
    }

    // Force removal of the dead node's blocks.
    BlockManagerTestUtil.checkHeartbeat(fsn.getBlockManager());

    // Force DatanodeManager to check decommission state.
    BlockManagerTestUtil.recheckDecommissionState(dm);
    // Block until the admin's monitor updates the number of tracked nodes.
    waitForDecommissionedNodes(dm.getDatanodeAdminManager(), 1);
    // Verify that the DN remains in DECOMMISSION_INPROGRESS state.
    assertTrue("the node should be DECOMMISSION_IN_PROGRESSS",
        dead.get(0).isDecommissionInProgress());
    // Check DatanodeManager#getDecommissionNodes, make sure it returns
    // the node as decommissioning, even if it's dead
    List<DatanodeDescriptor> decomlist = dm.getDecommissioningNodes();
    assertTrue("The node should be be decommissioning", decomlist.size() == 1);
    
    // Delete the under-replicated file, which should let the 
    // DECOMMISSION_IN_PROGRESS node become DECOMMISSIONED
    AdminStatesBaseTest.cleanupFile(fileSys, f);
    BlockManagerTestUtil.recheckDecommissionState(dm);
    // Block until the admin's monitor updates the number of tracked nodes.
    waitForDecommissionedNodes(dm.getDatanodeAdminManager(), 0);
    assertTrue("the node should be decommissioned",
        dead.get(0).isDecommissioned());

    // Add the node back
    cluster.restartDataNode(dataNodeProperties, true);
    cluster.waitActive();

    // Call refreshNodes on FSNamesystem with empty exclude file.
    // This will remove the datanodes from decommissioning list and
    // make them available again.
    hostsFileWriter.initExcludeHost("");
    dm.refreshNodes(conf);
  }

  /**
   * Verify the support for decommissioning a datanode that is already dead.
   * Under this scenario the datanode should immediately be marked as
   * DECOMMISSIONED
   */
  @Test(timeout=120000)
  public void testDecommissionDeadDN() throws Exception {
    Logger log = Logger.getLogger(DatanodeAdminManager.class);
    log.setLevel(Level.DEBUG);
    DatanodeID dnID = cluster.getDataNodes().get(0).getDatanodeId();
    String dnName = dnID.getXferAddr();
    DataNodeProperties stoppedDN = cluster.stopDataNode(0);
    DFSTestUtil.waitForDatanodeState(cluster, dnID.getDatanodeUuid(),
        false, 30000);
    FSNamesystem fsn = cluster.getNamesystem();
    final DatanodeManager dm = fsn.getBlockManager().getDatanodeManager();
    DatanodeDescriptor dnDescriptor = dm.getDatanode(dnID);
    decommissionNode(dnName);
    dm.refreshNodes(conf);
    BlockManagerTestUtil.recheckDecommissionState(dm);
    // Block until the admin's monitor updates the number of tracked nodes.
    waitForDecommissionedNodes(dm.getDatanodeAdminManager(), 0);
    assertTrue(dnDescriptor.isDecommissioned());

    // Add the node back
    cluster.restartDataNode(stoppedDN, true);
    cluster.waitActive();

    // Call refreshNodes on FSNamesystem with empty exclude file to remove the
    // datanode from decommissioning list and make it available again.
    hostsFileWriter.initExcludeHost("");
    dm.refreshNodes(conf);
  }

  @Test(timeout=120000)
  public void testDecommissionLosingData() throws Exception {
    ArrayList<String> nodes = new ArrayList<String>(2);
    FSNamesystem fsn = cluster.getNamesystem();
    BlockManager bm = fsn.getBlockManager();
    DatanodeManager dm = bm.getDatanodeManager();
    Path file1 = new Path("decommissionLosingData.dat");
    DFSTestUtil.createFile(fileSys, file1, fileSize, fileSize, blockSize,
        (short)2, seed);
    Thread.sleep(1000);

    // Shutdown dn1
    LOG.info("Shutdown dn1");
    DatanodeID dnID = cluster.getDataNodes().get(1).getDatanodeId();
    String dnName = dnID.getXferAddr();
    DatanodeDescriptor dnDescriptor1 = dm.getDatanode(dnID);
    nodes.add(dnName);
    DataNodeProperties stoppedDN1 = cluster.stopDataNode(1);
    DFSTestUtil.waitForDatanodeState(cluster, dnID.getDatanodeUuid(),
        false, 30000);

    // Shutdown dn0
    LOG.info("Shutdown dn0");
    dnID = cluster.getDataNodes().get(0).getDatanodeId();
    dnName = dnID.getXferAddr();
    DatanodeDescriptor dnDescriptor0 = dm.getDatanode(dnID);
    nodes.add(dnName);
    DataNodeProperties stoppedDN0 = cluster.stopDataNode(0);
    DFSTestUtil.waitForDatanodeState(cluster, dnID.getDatanodeUuid(),
        false, 30000);

    // Decommission the nodes.
    LOG.info("Decommissioning nodes");
    hostsFileWriter.initExcludeHosts(nodes);
    dm.refreshNodes(conf);
    BlockManagerTestUtil.recheckDecommissionState(dm);
    // Block until the admin's monitor updates the number of tracked nodes.
    waitForDecommissionedNodes(dm.getDatanodeAdminManager(), 0);
    assertTrue(dnDescriptor0.isDecommissioned());
    assertTrue(dnDescriptor1.isDecommissioned());

    // All nodes are dead and decommed. Blocks should be missing.
    long  missingBlocks = bm.getMissingBlocksCount();
    long underreplicated = bm.getLowRedundancyBlocksCount();
    assertTrue(missingBlocks > 0);
    assertTrue(underreplicated > 0);

    // Bring back dn0
    LOG.info("Bring back dn0");
    cluster.restartDataNode(stoppedDN0, true);
    do {
      dnID = cluster.getDataNodes().get(0).getDatanodeId();
    } while (dnID == null);
    dnDescriptor0 = dm.getDatanode(dnID);
    // Wait until it sends a block report.
    while (dnDescriptor0.numBlocks() == 0) {
      Thread.sleep(100);
    }

    // Bring back dn1
    LOG.info("Bring back dn1");
    cluster.restartDataNode(stoppedDN1, true);
    do {
      dnID = cluster.getDataNodes().get(1).getDatanodeId();
    } while (dnID == null);
    dnDescriptor1 = dm.getDatanode(dnID);
    // Wait until it sends a block report.
    while (dnDescriptor1.numBlocks() == 0) {
      Thread.sleep(100);
    }

    // Blocks should be still be under-replicated
    Thread.sleep(2000);  // Let replication monitor run
    assertEquals(underreplicated, bm.getLowRedundancyBlocksCount());

    // Start up a node.
    LOG.info("Starting two more nodes");
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();
    // Replication should fix it.
    int count = 0;
    while((bm.getLowRedundancyBlocksCount() > 0 ||
        bm.getPendingReconstructionBlocksCount() > 0) &&
        count++ < 10) {
      Thread.sleep(1000);
    }

    assertEquals(0, bm.getLowRedundancyBlocksCount());
    assertEquals(0, bm.getPendingReconstructionBlocksCount());
    assertEquals(0, bm.getMissingBlocksCount());

    // Shutdown the extra nodes.
    dnID = cluster.getDataNodes().get(3).getDatanodeId();
    cluster.stopDataNode(3);
    DFSTestUtil.waitForDatanodeState(cluster, dnID.getDatanodeUuid(),
        false, 30000);

    dnID = cluster.getDataNodes().get(2).getDatanodeId();
    cluster.stopDataNode(2);
    DFSTestUtil.waitForDatanodeState(cluster, dnID.getDatanodeUuid(),
        false, 30000);

    // Call refreshNodes on FSNamesystem with empty exclude file to remove the
    // datanode from decommissioning list and make it available again.
    hostsFileWriter.initExcludeHost("");
    dm.refreshNodes(conf);
    fileSys.delete(file1, false);
  }
}
