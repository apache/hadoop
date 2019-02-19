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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAdminManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommission extends AdminStatesBaseTest {
  public static final Logger LOG = LoggerFactory.getLogger(TestDecommission
      .class);

  /**
   * Verify that the number of replicas are as expected for each block in
   * the given file.
   * For blocks with a decommissioned node, verify that their replication
   * is 1 more than what is specified.
   * For blocks without decommissioned nodes, verify their replication is
   * equal to what is specified.
   * 
   * @param downnode - if null, there is no decommissioned node for this file.
   * @return - null if no failure found, else an error message string.
   */
  private static String checkFile(FileSystem fileSys, Path name, int repl,
    String downnode, int numDatanodes) throws IOException {
    boolean isNodeDown = (downnode != null);
    // need a raw stream
    assertTrue("Not HDFS:"+fileSys.getUri(),
        fileSys instanceof DistributedFileSystem);
    HdfsDataInputStream dis = (HdfsDataInputStream)
        fileSys.open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();
    for (LocatedBlock blk : dinfo) { // for each block
      int hasdown = 0;
      DatanodeInfo[] nodes = blk.getLocations();
      for (int j = 0; j < nodes.length; j++) { // for each replica
        if (isNodeDown && nodes[j].getXferAddr().equals(downnode)) {
          hasdown++;
          //Downnode must actually be decommissioned
          if (!nodes[j].isDecommissioned()) {
            return "For block " + blk.getBlock() + " replica on " +
              nodes[j] + " is given as downnode, " +
              "but is not decommissioned";
          }
          //Decommissioned node (if any) should only be last node in list.
          if (j != nodes.length - 1) {
            return "For block " + blk.getBlock() + " decommissioned node "
              + nodes[j] + " was not last node in list: "
              + (j + 1) + " of " + nodes.length;
          }
          LOG.info("Block " + blk.getBlock() + " replica on " +
            nodes[j] + " is decommissioned.");
        } else {
          //Non-downnodes must not be decommissioned
          if (nodes[j].isDecommissioned()) {
            return "For block " + blk.getBlock() + " replica on " +
              nodes[j] + " is unexpectedly decommissioned";
          }
        }
      }

      LOG.info("Block " + blk.getBlock() + " has " + hasdown
        + " decommissioned replica.");
      if(Math.min(numDatanodes, repl+hasdown) != nodes.length) {
        return "Wrong number of replicas for block " + blk.getBlock() +
          ": " + nodes.length + ", expected " +
          Math.min(numDatanodes, repl+hasdown);
      }
    }
    return null;
  }

  private void verifyStats(NameNode namenode, FSNamesystem fsn,
      DatanodeInfo info, DataNode node, boolean decommissioning)
      throws InterruptedException, IOException {
    // Do the stats check over 10 heartbeats
    for (int i = 0; i < 10; i++) {
      long[] newStats = namenode.getRpcServer().getStats();

      // For decommissioning nodes, ensure capacity of the DN and dfsUsed
      //  is no longer counted towards total
      assertEquals(newStats[0],
          decommissioning ? 0 : info.getCapacity());

      // Ensure cluster used capacity is counted for normal nodes only
      assertEquals(newStats[1], decommissioning ? 0 : info.getDfsUsed());

      // For decommissioning nodes, remaining space from the DN is not counted
      assertEquals(newStats[2], decommissioning ? 0 : info.getRemaining());

      // Ensure transceiver count is same as that DN
      assertEquals(fsn.getTotalLoad(), info.getXceiverCount());
      DataNodeTestUtils.triggerHeartbeat(node);
    }
  }

  /**
   * Tests decommission for non federated cluster
   */
  @Test(timeout=360000)
  public void testDecommission() throws IOException {
    testDecommission(1, 6);
  }

  /**
   * Tests decommission with replicas on the target datanode cannot be migrated
   * to other datanodes and satisfy the replication factor. Make sure the
   * datanode won't get stuck in decommissioning state.
   */
  @Test(timeout = 360000)
  public void testDecommission2() throws IOException {
    LOG.info("Starting test testDecommission");
    int numNamenodes = 1;
    int numDatanodes = 4;
    getConf().setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    startCluster(numNamenodes, numDatanodes);

    ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList = new ArrayList<ArrayList<DatanodeInfo>>(
        numNamenodes);
    namenodeDecomList.add(0, new ArrayList<DatanodeInfo>(numDatanodes));

    Path file1 = new Path("testDecommission2.dat");
    int replicas = 4;

    // Start decommissioning one namenode at a time
    ArrayList<DatanodeInfo> decommissionedNodes = namenodeDecomList.get(0);
    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);

    writeFile(fileSys, file1, replicas);

    int deadDecommissioned = ns.getNumDecomDeadDataNodes();
    int liveDecommissioned = ns.getNumDecomLiveDataNodes();

    // Decommission one node. Verify that node is decommissioned.
    DatanodeInfo decomNode = takeNodeOutofService(0, null, 0,
        decommissionedNodes, AdminStates.DECOMMISSIONED);
    decommissionedNodes.add(decomNode);
    assertEquals(deadDecommissioned, ns.getNumDecomDeadDataNodes());
    assertEquals(liveDecommissioned + 1, ns.getNumDecomLiveDataNodes());

    // Ensure decommissioned datanode is not automatically shutdown
    DFSClient client = getDfsClient(0);
    assertEquals("All datanodes must be alive", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);
    assertNull(checkFile(fileSys, file1, replicas, decomNode.getXferAddr(),
        numDatanodes));
    cleanupFile(fileSys, file1);

    // Restart the cluster and ensure recommissioned datanodes
    // are allowed to register with the namenode
    shutdownCluster();
    startCluster(1, 4);
  }
  
  /**
   * Test decommission for federeated cluster
   */
  @Test(timeout=360000)
  public void testDecommissionFederation() throws IOException {
    testDecommission(2, 2);
  }

  /**
   * Test decommission process on standby NN.
   * Verify admins can run "dfsadmin -refreshNodes" on SBN and decomm
   * process can finish as long as admins run "dfsadmin -refreshNodes"
   * on active NN.
   * SBN used to mark excess replica upon recommission. The SBN's pick
   * for excess replica could be different from the one picked by ANN.
   * That creates inconsistent state and prevent SBN from finishing
   * decommission.
   */
  @Test(timeout=360000)
  public void testDecommissionOnStandby() throws Exception {
    getConf().setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    getConf().setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        30000);
    getConf().setInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY, 2);

    // The time to wait so that the slow DN's heartbeat is considered old
    // by BlockPlacementPolicyDefault and thus will choose that DN for
    // excess replica.
    long slowHeartbeatDNwaitTime =
        getConf().getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000 * (getConf().
        getInt(DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT) + 1);

    startSimpleHACluster(3);

    // Step 1, create a cluster with 4 DNs. Blocks are stored on the
    // first 3 DNs. The last DN is empty. Also configure the last DN to have
    // slow heartbeat so that it will be chosen as excess replica candidate
    // during recommission.

    // Step 1.a, copy blocks to the first 3 DNs. Given the replica count is the
    // same as # of DNs, each DN will have a replica for any block.
    Path file1 = new Path("testDecommissionHA.dat");
    int replicas = 3;
    FileSystem activeFileSys = getCluster().getFileSystem(0);
    writeFile(activeFileSys, file1, replicas);

    HATestUtil.waitForStandbyToCatchUp(getCluster().getNameNode(0),
        getCluster().getNameNode(1));

    // Step 1.b, start a DN with slow heartbeat, so that we can know for sure it
    // will be chosen as the target of excess replica during recommission.
    getConf().setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 30);
    getCluster().startDataNodes(getConf(), 1, true, null, null, null);
    DataNode lastDN = getCluster().getDataNodes().get(3);
    lastDN.getDatanodeUuid();

    // Step 2, decommission the first DN at both ANN and SBN.
    DataNode firstDN = getCluster().getDataNodes().get(0);

    // Step 2.a, ask ANN to decomm the first DN
    DatanodeInfo decommissionedNodeFromANN = takeNodeOutofService(
        0, firstDN.getDatanodeUuid(), 0, null, AdminStates.DECOMMISSIONED);

    // Step 2.b, ask SBN to decomm the first DN
    DatanodeInfo decomNodeFromSBN = takeNodeOutofService(1,
        firstDN.getDatanodeUuid(), 0, null, AdminStates.DECOMMISSIONED);

    // Step 3, recommission the first DN on SBN and ANN to create excess replica
    // It recommissions the node on SBN first to create potential
    // inconsistent state. In production cluster, such insistent state can
    // happen even if recommission command was issued on ANN first given the
    // async nature of the system.

    // Step 3.a, ask SBN to recomm the first DN.
    // SBN has been fixed so that it no longer invalidates excess replica during
    // recommission.
    // Before the fix, SBN could get into the following state.
    //    1. the last DN would have been chosen as excess replica, given its
    //    heartbeat is considered old.
    //    Please refer to BlockPlacementPolicyDefault#chooseReplicaToDelete
    //    2. After recommissionNode finishes, SBN has 3 live replicas (0, 1, 2)
    //    and one excess replica ( 3 )
    // After the fix,
    //    After recommissionNode finishes, SBN has 4 live replicas (0, 1, 2, 3)
    Thread.sleep(slowHeartbeatDNwaitTime);
    putNodeInService(1, decomNodeFromSBN);

    // Step 3.b, ask ANN to recommission the first DN.
    // To verify the fix, the test makes sure the excess replica picked by ANN
    // is different from the one picked by SBN before the fix.
    // To achieve that, we make sure next-to-last DN is chosen as excess replica
    // by ANN.
    // 1. restore LastDNprop's heartbeat interval.
    // 2. Make next-to-last DN's heartbeat slow.
    MiniDFSCluster.DataNodeProperties lastDNprop =
        getCluster().stopDataNode(3);
    lastDNprop.conf.setLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    getCluster().restartDataNode(lastDNprop);

    MiniDFSCluster.DataNodeProperties nextToLastDNprop =
        getCluster().stopDataNode(2);
    nextToLastDNprop.conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        30);
    getCluster().restartDataNode(nextToLastDNprop);
    getCluster().waitActive();
    Thread.sleep(slowHeartbeatDNwaitTime);
    putNodeInService(0, decommissionedNodeFromANN);

    // Step 3.c, make sure the DN has deleted the block and report to NNs
    getCluster().triggerHeartbeats();
    HATestUtil.waitForDNDeletions(getCluster());
    getCluster().triggerDeletionReports();

    // Step 4, decommission the first DN on both ANN and SBN
    // With the fix to make sure SBN no longer marks excess replica
    // during recommission, SBN's decommission can finish properly
    takeNodeOutofService(0, firstDN.getDatanodeUuid(), 0, null,
        AdminStates.DECOMMISSIONED);

    // Ask SBN to decomm the first DN
    takeNodeOutofService(1, firstDN.getDatanodeUuid(), 0, null,
        AdminStates.DECOMMISSIONED);
  }

  private void testDecommission(int numNamenodes, int numDatanodes)
      throws IOException {
    LOG.info("Starting test testDecommission");
    startCluster(numNamenodes, numDatanodes);
    
    ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList = 
      new ArrayList<ArrayList<DatanodeInfo>>(numNamenodes);
    for(int i = 0; i < numNamenodes; i++) {
      namenodeDecomList.add(i, new ArrayList<DatanodeInfo>(numDatanodes));
    }
    Path file1 = new Path("testDecommission.dat");
    for (int iteration = 0; iteration < numDatanodes - 1; iteration++) {
      int replicas = numDatanodes - iteration - 1;
      
      // Start decommissioning one namenode at a time
      for (int i = 0; i < numNamenodes; i++) {
        ArrayList<DatanodeInfo> decommissionedNodes = namenodeDecomList.get(i);
        FileSystem fileSys = getCluster().getFileSystem(i);
        FSNamesystem ns = getCluster().getNamesystem(i);

        writeFile(fileSys, file1, replicas);

        int deadDecommissioned = ns.getNumDecomDeadDataNodes();
        int liveDecommissioned = ns.getNumDecomLiveDataNodes();

        // Decommission one node. Verify that node is decommissioned.
        DatanodeInfo decomNode = takeNodeOutofService(i, null, 0,
            decommissionedNodes, AdminStates.DECOMMISSIONED);
        decommissionedNodes.add(decomNode);
        assertEquals(deadDecommissioned, ns.getNumDecomDeadDataNodes());
        assertEquals(liveDecommissioned + 1, ns.getNumDecomLiveDataNodes());

        // Ensure decommissioned datanode is not automatically shutdown
        DFSClient client = getDfsClient(i);
        assertEquals("All datanodes must be alive", numDatanodes, 
            client.datanodeReport(DatanodeReportType.LIVE).length);
        // wait for the block to be replicated
        int tries = 0;
        while (tries++ < 20) {
          try {
            Thread.sleep(1000);
            if (checkFile(fileSys, file1, replicas, decomNode.getXferAddr(),
                numDatanodes) == null) {
              break;
            }
          } catch (InterruptedException ie) {
          }
        }
        assertTrue("Checked if block was replicated after decommission, tried "
            + tries + " times.", tries < 20);
        cleanupFile(fileSys, file1);
      }
    }

    // Restart the cluster and ensure decommissioned datanodes
    // are allowed to register with the namenode
    shutdownCluster();
    startCluster(numNamenodes, numDatanodes);
  }

  /**
   * Test that over-replicated blocks are deleted on recommission.
   */
  @Test(timeout=120000)
  public void testRecommission() throws Exception {
    final int numDatanodes = 6;
    try {
      LOG.info("Starting test testRecommission");

      startCluster(1, numDatanodes);

      final Path file1 = new Path("testDecommission.dat");
      final int replicas = numDatanodes - 1;

      ArrayList<DatanodeInfo> decommissionedNodes = Lists.newArrayList();
      final FileSystem fileSys = getCluster().getFileSystem();

      // Write a file to n-1 datanodes
      writeFile(fileSys, file1, replicas);

      // Decommission one of the datanodes with a replica
      BlockLocation loc = fileSys.getFileBlockLocations(file1, 0, 1)[0];
      assertEquals("Unexpected number of replicas from getFileBlockLocations",
          replicas, loc.getHosts().length);
      final String toDecomHost = loc.getNames()[0];
      String toDecomUuid = null;
      for (DataNode d : getCluster().getDataNodes()) {
        if (d.getDatanodeId().getXferAddr().equals(toDecomHost)) {
          toDecomUuid = d.getDatanodeId().getDatanodeUuid();
          break;
        }
      }
      assertNotNull("Could not find a dn with the block!", toDecomUuid);
      final DatanodeInfo decomNode = takeNodeOutofService(0, toDecomUuid,
          0, decommissionedNodes, AdminStates.DECOMMISSIONED);
      decommissionedNodes.add(decomNode);
      final BlockManager blockManager =
          getCluster().getNamesystem().getBlockManager();
      final DatanodeManager datanodeManager =
          blockManager.getDatanodeManager();
      BlockManagerTestUtil.recheckDecommissionState(datanodeManager);

      // Ensure decommissioned datanode is not automatically shutdown
      DFSClient client = getDfsClient(0);
      assertEquals("All datanodes must be alive", numDatanodes,
          client.datanodeReport(DatanodeReportType.LIVE).length);

      // wait for the block to be replicated
      final ExtendedBlock b = DFSTestUtil.getFirstBlock(fileSys, file1);
      final String uuid = toDecomUuid;
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          BlockInfo info = blockManager.getStoredBlock(b.getLocalBlock());
          int count = 0;
          StringBuilder sb = new StringBuilder("Replica locations: ");
          for (int i = 0; i < info.numNodes(); i++) {
            DatanodeDescriptor dn = info.getDatanode(i);
            sb.append(dn + ", ");
            if (!dn.getDatanodeUuid().equals(uuid)) {
              count++;
            }
          }
          LOG.info(sb.toString());
          LOG.info("Count: " + count);
          return count == replicas;
        }
      }, 500, 30000);

      // redecommission and wait for over-replication to be fixed
      putNodeInService(0, decomNode);
      BlockManagerTestUtil.recheckDecommissionState(datanodeManager);
      DFSTestUtil.waitForReplication(getCluster(), b, 1, replicas, 0);

      cleanupFile(fileSys, file1);
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Tests cluster storage statistics during decommissioning for non
   * federated cluster
   */
  @Test(timeout=360000)
  public void testClusterStats() throws Exception {
    testClusterStats(1);
  }
  
  /**
   * Tests cluster storage statistics during decommissioning for
   * federated cluster
   */
  @Test(timeout=360000)
  public void testClusterStatsFederation() throws Exception {
    testClusterStats(3);
  }
  
  public void testClusterStats(int numNameNodes) throws IOException,
      InterruptedException {
    LOG.info("Starting test testClusterStats");
    int numDatanodes = 1;
    startCluster(numNameNodes, numDatanodes);
    
    for (int i = 0; i < numNameNodes; i++) {
      FileSystem fileSys = getCluster().getFileSystem(i);
      Path file = new Path("testClusterStats.dat");
      writeFile(fileSys, file, 1);
      
      FSNamesystem fsn = getCluster().getNamesystem(i);
      NameNode namenode = getCluster().getNameNode(i);
      
      DatanodeInfo decomInfo = takeNodeOutofService(i, null, 0, null,
          AdminStates.DECOMMISSION_INPROGRESS);
      DataNode decomNode = getDataNode(decomInfo);
      // Check namenode stats for multiple datanode heartbeats
      verifyStats(namenode, fsn, decomInfo, decomNode, true);
      
      // Stop decommissioning and verify stats
      DatanodeInfo retInfo = NameNodeAdapter.getDatanode(fsn, decomInfo);
      putNodeInService(i, retInfo);
      DataNode retNode = getDataNode(decomInfo);
      verifyStats(namenode, fsn, retInfo, retNode, false);
    }
  }

  private DataNode getDataNode(DatanodeInfo decomInfo) {
    DataNode decomNode = null;
    for (DataNode dn: getCluster().getDataNodes()) {
      if (decomInfo.equals(dn.getDatanodeId())) {
        decomNode = dn;
        break;
      }
    }
    assertNotNull("Could not find decomNode in cluster!", decomNode);
    return decomNode;
  }

  /**
   * Test host/include file functionality. Only datanodes
   * in the include file are allowed to connect to the namenode in a non
   * federated cluster.
   */
  @Test(timeout=360000)
  public void testHostsFile() throws IOException, InterruptedException {
    // Test for a single namenode cluster
    testHostsFile(1);
  }
  
  /**
   * Test host/include file functionality. Only datanodes
   * in the include file are allowed to connect to the namenode in a 
   * federated cluster.
   */
  @Test(timeout=360000)
  public void testHostsFileFederation()
      throws IOException, InterruptedException {
    // Test for 3 namenode federated cluster
    testHostsFile(3);
  }
  
  public void testHostsFile(int numNameNodes) throws IOException,
      InterruptedException {
    int numDatanodes = 1;
    startCluster(numNameNodes, numDatanodes, true, null, false);

    // Now empty hosts file and ensure the datanode is disallowed
    // from talking to namenode, resulting in it's shutdown.
    final String bogusIp = "127.0.30.1";
    initIncludeHost(bogusIp);

    for (int j = 0; j < numNameNodes; j++) {
      refreshNodes(j);
      DFSClient client = getDfsClient(j);
      DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
      for (int i = 0 ; i < 5 && info.length != 0; i++) {
        LOG.info("Waiting for datanode to be marked dead");
        Thread.sleep(HEARTBEAT_INTERVAL * 1000);
        info = client.datanodeReport(DatanodeReportType.LIVE);
      }
      assertEquals("Number of live nodes should be 0", 0, info.length);
      
      // Test that bogus hostnames are considered "dead".
      // The dead report should have an entry for the bogus entry in the hosts
      // file.  The original datanode is excluded from the report because it
      // is no longer in the included list.
      info = client.datanodeReport(DatanodeReportType.DEAD);
      assertEquals("There should be 1 dead node", 1, info.length);
      assertEquals(bogusIp, info[0].getHostName());
    }
  }
  
  @Test(timeout=120000)
  public void testDecommissionWithOpenfile()
      throws IOException, InterruptedException {
    LOG.info("Starting test testDecommissionWithOpenfile");
    
    //At most 4 nodes will be decommissioned
    startCluster(1, 7);
        
    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);

    String openFile = "/testDecommissionWithOpenfile.dat";
           
    writeFile(fileSys, new Path(openFile), (short)3);   
    // make sure the file was open for write
    FSDataOutputStream fdos =  fileSys.append(new Path(openFile)); 
    
    LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(
        getCluster().getNameNode(0), openFile, 0, fileSize);

    DatanodeInfo[] dnInfos4LastBlock = lbs.getLastLocatedBlock().getLocations();
    DatanodeInfo[] dnInfos4FirstBlock = lbs.get(0).getLocations();
    
    ArrayList<String> nodes = new ArrayList<String>();
    ArrayList<DatanodeInfo> dnInfos = new ArrayList<DatanodeInfo>();

    DatanodeManager dm = ns.getBlockManager().getDatanodeManager();
    for (DatanodeInfo datanodeInfo : dnInfos4FirstBlock) {
      DatanodeInfo found = datanodeInfo;
      for (DatanodeInfo dif: dnInfos4LastBlock) {
        if (datanodeInfo.equals(dif)) {
         found = null;
        }
      }
      if (found != null) {
        nodes.add(found.getXferAddr());
        dnInfos.add(dm.getDatanode(found));
      }
    }
    //decommission one of the 3 nodes which have last block
    nodes.add(dnInfos4LastBlock[0].getXferAddr());
    dnInfos.add(dm.getDatanode(dnInfos4LastBlock[0]));

    initExcludeHosts(nodes);
    refreshNodes(0);
    for (DatanodeInfo dn : dnInfos) {
      waitNodeState(dn, AdminStates.DECOMMISSIONED);
    }

    fdos.close();
  }

  private static String scanIntoString(final ByteArrayOutputStream baos) {
    final TextStringBuilder sb = new TextStringBuilder();
    final Scanner scanner = new Scanner(baos.toString());
    while (scanner.hasNextLine()) {
      sb.appendln(scanner.nextLine());
    }
    scanner.close();
    return sb.toString();
  }

  private boolean verifyOpenFilesListing(String message,
      HashSet<Path> closedFileSet,
      HashMap<Path, FSDataOutputStream> openFilesMap,
      ByteArrayOutputStream out, int expOpenFilesListSize) {
    final String outStr = scanIntoString(out);
    LOG.info(message + " - stdout: \n" + outStr);
    for (Path closedFilePath : closedFileSet) {
      if(outStr.contains(closedFilePath.toString())) {
        return false;
      }
    }
    HashSet<Path> openFilesNotListed = new HashSet<>();
    for (Path openFilePath : openFilesMap.keySet()) {
      if(!outStr.contains(openFilePath.toString())) {
        openFilesNotListed.add(openFilePath);
      }
    }
    int actualOpenFilesListedSize =
        openFilesMap.size() - openFilesNotListed.size();
    if (actualOpenFilesListedSize >= expOpenFilesListSize) {
      return true;
    } else {
      LOG.info("Open files that are not listed yet: " + openFilesNotListed);
      return false;
    }
  }

  private void verifyOpenFilesBlockingDecommission(HashSet<Path> closedFileSet,
      HashMap<Path, FSDataOutputStream> openFilesMap, final int maxOpenFiles)
      throws Exception {
    final PrintStream oldStreamOut = System.out;
    try {
      final ByteArrayOutputStream toolOut = new ByteArrayOutputStream();
      System.setOut(new PrintStream(toolOut));
      final DFSAdmin dfsAdmin = new DFSAdmin(getConf());

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            boolean result1 = false;
            boolean result2 = false;
            toolOut.reset();
            assertEquals(0, ToolRunner.run(dfsAdmin,
                new String[]{"-listOpenFiles", "-blockingDecommission"}));
            toolOut.flush();
            result1 = verifyOpenFilesListing(
                "dfsadmin -listOpenFiles -blockingDecommission",
                closedFileSet, openFilesMap, toolOut, maxOpenFiles);

            // test -blockingDecommission with option -path
            if (openFilesMap.size() > 0) {
              String firstOpenFile = null;
              // Construct a new open-file and close-file map.
              // Pick the first open file into new open-file map, remaining
              //  open files move into close-files map.
              HashMap<Path, FSDataOutputStream> newOpenFilesMap =
                  new HashMap<>();
              HashSet<Path> newClosedFileSet = new HashSet<>();
              for (Map.Entry<Path, FSDataOutputStream> entry : openFilesMap
                  .entrySet()) {
                if (firstOpenFile == null) {
                  newOpenFilesMap.put(entry.getKey(), entry.getValue());
                  firstOpenFile = entry.getKey().toString();
                } else {
                  newClosedFileSet.add(entry.getKey());
                }
              }

              toolOut.reset();
              assertEquals(0,
                  ToolRunner.run(dfsAdmin, new String[] {"-listOpenFiles",
                      "-blockingDecommission", "-path", firstOpenFile}));
              toolOut.flush();
              result2 = verifyOpenFilesListing(
                  "dfsadmin -listOpenFiles -blockingDecommission -path"
                      + firstOpenFile,
                  newClosedFileSet, newOpenFilesMap, toolOut, 1);
            } else {
              result2 = true;
            }

            return result1 && result2;
          } catch (Exception e) {
            LOG.warn("Unexpected exception: " + e);
          }
          return false;
        }
      }, 1000, 60000);
    } finally {
      System.setOut(oldStreamOut);
    }
  }

  @Test(timeout=180000)
  public void testDecommissionWithOpenfileReporting()
      throws Exception {
    LOG.info("Starting test testDecommissionWithOpenfileReporting");

    // Disable redundancy monitor check so that open files blocking
    // decommission can be listed and verified.
    getConf().setInt(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1000);
    getConf().setLong(
        DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES, 1);

    //At most 1 node can be decommissioned
    startSimpleCluster(1, 4);

    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);

    final String[] closedFiles = new String[3];
    final String[] openFiles = new String[3];
    HashSet<Path> closedFileSet = new HashSet<>();
    HashMap<Path, FSDataOutputStream> openFilesMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      closedFiles[i] = "/testDecommissionWithOpenfileReporting.closed." + i;
      openFiles[i] = "/testDecommissionWithOpenfileReporting.open." + i;
      writeFile(fileSys, new Path(closedFiles[i]), (short)3, 10);
      closedFileSet.add(new Path(closedFiles[i]));
      writeFile(fileSys, new Path(openFiles[i]), (short)3, 10);
      FSDataOutputStream fdos =  fileSys.append(new Path(openFiles[i]));
      openFilesMap.put(new Path(openFiles[i]), fdos);
    }

    HashMap<DatanodeInfo, Integer> dnInfoMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(
          getCluster().getNameNode(0), openFiles[i], 0, blockSize * 10);
      for (DatanodeInfo dn : lbs.getLastLocatedBlock().getLocations()) {
        if (dnInfoMap.containsKey(dn)) {
          dnInfoMap.put(dn, dnInfoMap.get(dn) + 1);
        } else {
          dnInfoMap.put(dn, 1);
        }
      }
    }

    DatanodeInfo dnToDecommission = null;
    int maxDnOccurance = 0;
    for (Map.Entry<DatanodeInfo, Integer> entry : dnInfoMap.entrySet()) {
      if (entry.getValue() > maxDnOccurance) {
        maxDnOccurance = entry.getValue();
        dnToDecommission = entry.getKey();
      }
    }
    LOG.info("XXX Dn to decommission: " + dnToDecommission + ", max: "
        + maxDnOccurance);

    //decommission one of the 3 nodes which have last block
    DatanodeManager dm = ns.getBlockManager().getDatanodeManager();
    ArrayList<String> nodes = new ArrayList<>();
    dnToDecommission = dm.getDatanode(dnToDecommission.getDatanodeUuid());
    nodes.add(dnToDecommission.getXferAddr());
    initExcludeHosts(nodes);
    refreshNodes(0);
    waitNodeState(dnToDecommission, AdminStates.DECOMMISSION_INPROGRESS);

    // list and verify all the open files that are blocking decommission
    verifyOpenFilesBlockingDecommission(
        closedFileSet, openFilesMap, maxDnOccurance);

    final AtomicBoolean stopRedundancyMonitor = new AtomicBoolean(false);
    Thread monitorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopRedundancyMonitor.get()) {
          try {
            BlockManagerTestUtil.checkRedundancy(
                getCluster().getNamesystem().getBlockManager());
            BlockManagerTestUtil.updateState(
                getCluster().getNamesystem().getBlockManager());
            Thread.sleep(1000);
          } catch (Exception e) {
            LOG.warn("Encountered exception during redundancy monitor: " + e);
          }
        }
      }
    });
    monitorThread.start();

    waitNodeState(dnToDecommission, AdminStates.DECOMMISSIONED);
    stopRedundancyMonitor.set(true);
    monitorThread.join();

    // Open file is no more blocking decommission as all its blocks
    // are re-replicated.
    openFilesMap.clear();
    verifyOpenFilesBlockingDecommission(
        closedFileSet, openFilesMap, 0);
  }

  @Test(timeout = 360000)
  public void testDecommissionWithOpenFileAndBlockRecovery()
      throws IOException, InterruptedException {
    startCluster(1, 6);
    getCluster().waitActive();

    Path file = new Path("/testRecoveryDecommission");

    // Create a file and never close the output stream to trigger recovery
    DistributedFileSystem dfs = getCluster().getFileSystem();
    FSDataOutputStream out = dfs.create(file, true,
        getConf().getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) 3, blockSize);

    // Write data to the file
    long writtenBytes = 0;
    while (writtenBytes < fileSize) {
      out.writeLong(writtenBytes);
      writtenBytes += 8;
    }
    out.hsync();

    DatanodeInfo[] lastBlockLocations = NameNodeAdapter.getBlockLocations(
      getCluster().getNameNode(), "/testRecoveryDecommission", 0, fileSize)
      .getLastLocatedBlock().getLocations();

    // Decommission all nodes of the last block
    ArrayList<String> toDecom = new ArrayList<>();
    for (DatanodeInfo dnDecom : lastBlockLocations) {
      toDecom.add(dnDecom.getXferAddr());
    }
    initExcludeHosts(toDecom);
    refreshNodes(0);

    // Make sure hard lease expires to trigger replica recovery
    getCluster().setLeasePeriod(300L, 300L);
    Thread.sleep(2 * BLOCKREPORT_INTERVAL_MSEC);

    for (DatanodeInfo dnDecom : lastBlockLocations) {
      DatanodeInfo datanode = NameNodeAdapter.getDatanode(
          getCluster().getNamesystem(), dnDecom);
      waitNodeState(datanode, AdminStates.DECOMMISSIONED);
    }

    assertEquals(dfs.getFileStatus(file).getLen(), writtenBytes);
  }

  @Test(timeout=120000)
  public void testCloseWhileDecommission() throws IOException,
      ExecutionException, InterruptedException {
    LOG.info("Starting test testCloseWhileDecommission");

    // min replication = 2
    getConf().setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 2);
    startCluster(1, 3);

    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);

    String openFile = "/testDecommissionWithOpenfile.dat";

    writeFile(fileSys, new Path(openFile), (short)3);
    // make sure the file was open for write
    FSDataOutputStream fdos =  fileSys.append(new Path(openFile));
    byte[] bytes = new byte[1];
    fdos.write(bytes);
    fdos.hsync();

    LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(
        getCluster().getNameNode(0), openFile, 0, fileSize);

    DatanodeInfo[] dnInfos4LastBlock = lbs.getLastLocatedBlock().getLocations();

    ArrayList<String> nodes = new ArrayList<String>();
    ArrayList<DatanodeInfo> dnInfos = new ArrayList<DatanodeInfo>();

    DatanodeManager dm = ns.getBlockManager().getDatanodeManager();
    //decommission 2 of the 3 nodes which have last block
    nodes.add(dnInfos4LastBlock[0].getXferAddr());
    dnInfos.add(dm.getDatanode(dnInfos4LastBlock[0]));
    nodes.add(dnInfos4LastBlock[1].getXferAddr());
    dnInfos.add(dm.getDatanode(dnInfos4LastBlock[1]));

    // because the cluster has only 3 nodes, and 2 of which are decomm'ed,
    // the last block file will remain under replicated.
    initExcludeHosts(nodes);
    refreshNodes(0);

    // the close() should not fail despite the number of live replicas of
    // the last block becomes one.
    fdos.close();

    // make sure the two datanodes remain in decomm in progress state
    BlockManagerTestUtil.recheckDecommissionState(dm);
    assertTrackedAndPending(dm.getDatanodeAdminManager(), 2, 0);
  }
  
  /**
   * Tests restart of namenode while datanode hosts are added to exclude file
   **/
  @Test(timeout=360000)
  public void testDecommissionWithNamenodeRestart()
      throws IOException, InterruptedException {
    LOG.info("Starting test testDecommissionWithNamenodeRestart");
    int numNamenodes = 1;
    int numDatanodes = 1;
    int replicas = 1;
    getConf().setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT);
    getConf().setLong(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY, 5);

    startCluster(numNamenodes, numDatanodes);
    Path file1 = new Path("testDecommissionWithNamenodeRestart.dat");
    FileSystem fileSys = getCluster().getFileSystem();
    writeFile(fileSys, file1, replicas);
        
    DFSClient client = getDfsClient(0);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    DatanodeID excludedDatanodeID = info[0];
    String excludedDatanodeName = info[0].getXferAddr();

    initExcludeHost(excludedDatanodeName);

    //Add a new datanode to cluster
    getCluster().startDataNodes(getConf(), 1, true, null, null, null, null);
    numDatanodes+=1;

    assertEquals("Number of datanodes should be 2 ", 2,
        getCluster().getDataNodes().size());
    //Restart the namenode
    getCluster().restartNameNode();
    DatanodeInfo datanodeInfo = NameNodeAdapter.getDatanode(
        getCluster().getNamesystem(), excludedDatanodeID);
    waitNodeState(datanodeInfo, AdminStates.DECOMMISSIONED);

    // Ensure decommissioned datanode is not automatically shutdown
    assertEquals("All datanodes must be alive", numDatanodes, 
        client.datanodeReport(DatanodeReportType.LIVE).length);
    assertTrue("Checked if block was replicated after decommission.",
        checkFile(fileSys, file1, replicas, datanodeInfo.getXferAddr(),
        numDatanodes) == null);

    cleanupFile(fileSys, file1);
    // Restart the cluster and ensure recommissioned datanodes
    // are allowed to register with the namenode
    shutdownCluster();
    startCluster(numNamenodes, numDatanodes);
  }

  /**
   * Tests dead node count after restart of namenode
   **/
  @Test(timeout=360000)
  public void testDeadNodeCountAfterNamenodeRestart()throws Exception {
    LOG.info("Starting test testDeadNodeCountAfterNamenodeRestart");
    int numNamenodes = 1;
    int numDatanodes = 2;

    startCluster(numNamenodes, numDatanodes);

    DFSClient client = getDfsClient(0);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    DatanodeInfo excludedDatanode = info[0];
    String excludedDatanodeName = info[0].getXferAddr();

    List<String> hosts = new ArrayList<String>(Arrays.asList(
        excludedDatanodeName, info[1].getXferAddr()));
    initIncludeHosts(hosts.toArray(new String[hosts.size()]));
    takeNodeOutofService(0, excludedDatanode.getDatanodeUuid(), 0, null,
        AdminStates.DECOMMISSIONED);

    getCluster().stopDataNode(excludedDatanodeName);
    DFSTestUtil.waitForDatanodeState(
        getCluster(), excludedDatanode.getDatanodeUuid(), false, 20000);

    //Restart the namenode
    getCluster().restartNameNode();

    assertEquals("There should be one node alive", 1,
        client.datanodeReport(DatanodeReportType.LIVE).length);
    assertEquals("There should be one node dead", 1,
        client.datanodeReport(DatanodeReportType.DEAD).length);
  }

  /**
   * Test using a "registration name" in a host include file.
   *
   * Registration names are DataNode names specified in the configuration by
   * dfs.datanode.hostname.  The DataNode will send this name to the NameNode
   * as part of its registration.  Registration names are helpful when you
   * want to override the normal first result of DNS resolution on the
   * NameNode.  For example, a given datanode IP may map to two hostnames,
   * and you may want to choose which hostname is used internally in the
   * cluster.
   *
   * It is not recommended to use a registration name which is not also a
   * valid DNS hostname for the DataNode.  See HDFS-5237 for background.
   */
  @Ignore
  @Test(timeout=360000)
  public void testIncludeByRegistrationName() throws Exception {
    // Any IPv4 address starting with 127 functions as a "loopback" address
    // which is connected to the current host.  So by choosing 127.0.0.100
    // as our registration name, we have chosen a name which is also a valid
    // way of reaching the local DataNode we're going to start.
    // Typically, a registration name would be a hostname, but we don't want
    // to deal with DNS in this test.
    final String registrationName = "127.0.0.100";
    final String nonExistentDn = "127.0.0.10";
    getConf().set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, registrationName);
    startCluster(1, 1, false, null, true);

    // Set up an includes file that doesn't have our datanode.
    initIncludeHost(nonExistentDn);
    refreshNodes(0);

    // Wait for the DN to be marked dead.
    LOG.info("Waiting for DN to be marked as dead.");
    final DFSClient client = getDfsClient(0);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        BlockManagerTestUtil
            .checkHeartbeat(getCluster().getNamesystem().getBlockManager());
        try {
          DatanodeInfo info[] = client.datanodeReport(DatanodeReportType.DEAD);
          return info.length == 1;
        } catch (IOException e) {
          LOG.warn("Failed to check dead DNs", e);
          return false;
        }
      }
    }, 500, 5000);

    // Use a non-empty include file with our registration name.
    // It should work.
    int dnPort = getCluster().getDataNodes().get(0).getXferPort();
    initIncludeHost(registrationName + ":" + dnPort);
    refreshNodes(0);
    getCluster().restartDataNode(0);
    getCluster().triggerHeartbeats();

    // Wait for the DN to come back.
    LOG.info("Waiting for DN to come back.");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        BlockManagerTestUtil
            .checkHeartbeat(getCluster().getNamesystem().getBlockManager());
        try {
          DatanodeInfo info[] = client.datanodeReport(DatanodeReportType.LIVE);
          if (info.length == 1) {
            Assert.assertFalse(info[0].isDecommissioned());
            Assert.assertFalse(info[0].isDecommissionInProgress());
            assertEquals(registrationName, info[0].getHostName());
            return true;
          }
        } catch (IOException e) {
          LOG.warn("Failed to check dead DNs", e);
        }
        return false;
      }
    }, 500, 5000);
  }
  
  @Test(timeout=120000)
  public void testBlocksPerInterval() throws Exception {
    org.apache.log4j.Logger.getLogger(DatanodeAdminManager.class)
        .setLevel(Level.TRACE);
    // Turn the blocks per interval way down
    getConf().setInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        3);
    // Disable the normal monitor runs
    getConf().setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
        Integer.MAX_VALUE);
    startCluster(1, 3);
    final FileSystem fs = getCluster().getFileSystem();
    final DatanodeManager datanodeManager =
        getCluster().getNamesystem().getBlockManager().getDatanodeManager();
    final DatanodeAdminManager decomManager =
        datanodeManager.getDatanodeAdminManager();

    // Write a 3 block file, so each node has one block. Should scan 3 nodes.
    DFSTestUtil.createFile(fs, new Path("/file1"), 64, (short) 3, 0xBAD1DEA);
    doDecomCheck(datanodeManager, decomManager, 3);
    // Write another file, should only scan two
    DFSTestUtil.createFile(fs, new Path("/file2"), 64, (short)3, 0xBAD1DEA);
    doDecomCheck(datanodeManager, decomManager, 2);
    // One more file, should only scan 1
    DFSTestUtil.createFile(fs, new Path("/file3"), 64, (short)3, 0xBAD1DEA);
    doDecomCheck(datanodeManager, decomManager, 1);
    // blocks on each DN now exceeds limit, still scan at least one node
    DFSTestUtil.createFile(fs, new Path("/file4"), 64, (short)3, 0xBAD1DEA);
    doDecomCheck(datanodeManager, decomManager, 1);
  }

  private void doDecomCheck(DatanodeManager datanodeManager,
      DatanodeAdminManager decomManager, int expectedNumCheckedNodes)
      throws IOException, ExecutionException, InterruptedException {
    // Decom all nodes
    ArrayList<DatanodeInfo> decommissionedNodes = Lists.newArrayList();
    for (DataNode d: getCluster().getDataNodes()) {
      DatanodeInfo dn = takeNodeOutofService(0, d.getDatanodeUuid(), 0,
          decommissionedNodes, AdminStates.DECOMMISSION_INPROGRESS);
      decommissionedNodes.add(dn);
    }
    // Run decom scan and check
    BlockManagerTestUtil.recheckDecommissionState(datanodeManager);
    assertEquals("Unexpected # of nodes checked", expectedNumCheckedNodes, 
        decomManager.getNumNodesChecked());
    // Recommission all nodes
    for (DatanodeInfo dn : decommissionedNodes) {
      putNodeInService(0, dn);
    }
  }

  @Test(timeout=120000)
  public void testPendingNodes() throws Exception {
    org.apache.log4j.Logger.getLogger(DatanodeAdminManager.class)
        .setLevel(Level.TRACE);
    // Only allow one node to be decom'd at a time
    getConf().setInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
        1);
    // Disable the normal monitor runs
    getConf().setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
        Integer.MAX_VALUE);
    startCluster(1, 3);
    final FileSystem fs = getCluster().getFileSystem();
    final DatanodeManager datanodeManager =
        getCluster().getNamesystem().getBlockManager().getDatanodeManager();
    final DatanodeAdminManager decomManager =
        datanodeManager.getDatanodeAdminManager();

    // Keep a file open to prevent decom from progressing
    HdfsDataOutputStream open1 =
        (HdfsDataOutputStream) fs.create(new Path("/openFile1"), (short)3);
    // Flush and trigger block reports so the block definitely shows up on NN
    open1.write(123);
    open1.hflush();
    for (DataNode d: getCluster().getDataNodes()) {
      DataNodeTestUtils.triggerBlockReport(d);
    }
    // Decom two nodes, so one is still alive
    ArrayList<DatanodeInfo> decommissionedNodes = Lists.newArrayList();
    for (int i=0; i<2; i++) {
      final DataNode d = getCluster().getDataNodes().get(i);
      DatanodeInfo dn = takeNodeOutofService(0, d.getDatanodeUuid(), 0,
          decommissionedNodes, AdminStates.DECOMMISSION_INPROGRESS);
      decommissionedNodes.add(dn);
    }

    for (int i=2; i>=0; i--) {
      assertTrackedAndPending(decomManager, 0, i);
      BlockManagerTestUtil.recheckDecommissionState(datanodeManager);
    }

    // Close file, try to decom the last node, should get stuck in tracked
    open1.close();
    final DataNode d = getCluster().getDataNodes().get(2);
    DatanodeInfo dn = takeNodeOutofService(0, d.getDatanodeUuid(), 0,
        decommissionedNodes, AdminStates.DECOMMISSION_INPROGRESS);
    decommissionedNodes.add(dn);
    BlockManagerTestUtil.recheckDecommissionState(datanodeManager);
    
    assertTrackedAndPending(decomManager, 1, 0);
  }

  private void assertTrackedAndPending(DatanodeAdminManager decomManager,
      int tracked, int pending) {
    assertEquals("Unexpected number of tracked nodes", tracked,
        decomManager.getNumTrackedNodes());
    assertEquals("Unexpected number of pending nodes", pending,
        decomManager.getNumPendingNodes());
  }

  /**
   * Fetching Live DataNodes by passing removeDecommissionedNode value as
   * false- returns LiveNodeList with Node in Decommissioned state
   * true - returns LiveNodeList without Node in Decommissioned state
   * @throws InterruptedException
   */
  @Test
  public void testCountOnDecommissionedNodeList() throws IOException{
    getConf().setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    getConf().setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        1);
    try {
      startCluster(1, 1);

      ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList =
          new ArrayList<ArrayList<DatanodeInfo>>(1);
      namenodeDecomList.add(0, new ArrayList<DatanodeInfo>(1));

      // Move datanode1 to Decommissioned state
      ArrayList<DatanodeInfo> decommissionedNode = namenodeDecomList.get(0);
      takeNodeOutofService(0, null, 0, decommissionedNode,
          AdminStates.DECOMMISSIONED);

      FSNamesystem ns = getCluster().getNamesystem(0);
      DatanodeManager datanodeManager =
          ns.getBlockManager().getDatanodeManager();
      List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      // fetchDatanode with false should return livedecommisioned node
      datanodeManager.fetchDatanodes(live, null, false);
      assertTrue(1==live.size());
      // fetchDatanode with true should not return livedecommisioned node
      datanodeManager.fetchDatanodes(live, null, true);
      assertTrue(0==live.size());
    }finally {
      shutdownCluster();
    }
  }

  /**
   * Decommissioned node should not be considered while calculating node usage
   * @throws InterruptedException
   */
  @Test
  public void testNodeUsageAfterDecommissioned()
      throws IOException, InterruptedException {
    nodeUsageVerification(2, new long[] { 26384L, 26384L },
        AdminStates.DECOMMISSIONED);
  }

  /**
   * DECOMMISSION_INPROGRESS node should not be considered
   * while calculating node usage
   * @throws InterruptedException
   */
  @Test
  public void testNodeUsageWhileDecommissioining()
      throws IOException, InterruptedException {
    nodeUsageVerification(1, new long[] { 26384L },
        AdminStates.DECOMMISSION_INPROGRESS);
  }

  @SuppressWarnings({ "unchecked" })
  public void nodeUsageVerification(int numDatanodes, long[] nodesCapacity,
      AdminStates decommissionState) throws IOException, InterruptedException {
    Map<String, Map<String, String>> usage = null;
    DatanodeInfo decommissionedNodeInfo = null;
    String zeroNodeUsage = "0.00%";
    getConf().setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    getConf().setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    getConf().setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        1);
    FileSystem fileSys = null;
    Path file1 = new Path("testNodeUsage.dat");
    try {
      SimulatedFSDataset.setFactory(getConf());
      startCluster(1, numDatanodes, false, nodesCapacity, false);

      ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList =
          new ArrayList<ArrayList<DatanodeInfo>>(1);
      namenodeDecomList.add(0, new ArrayList<DatanodeInfo>(numDatanodes));

      if (decommissionState == AdminStates.DECOMMISSIONED) {
        // Move datanode1 to Decommissioned state
        ArrayList<DatanodeInfo> decommissionedNode = namenodeDecomList.get(0);
        decommissionedNodeInfo = takeNodeOutofService(0, null, 0,
            decommissionedNode, decommissionState);
      }
      // Write a file(replica 1).Hence will be written to only one live node.
      fileSys = getCluster().getFileSystem(0);
      FSNamesystem ns = getCluster().getNamesystem(0);
      writeFile(fileSys, file1, 1);
      Thread.sleep(2000);

      // min NodeUsage should not be 0.00%
      usage = (Map<String, Map<String, String>>) JSON.parse(ns.getNodeUsage());
      String minUsageBeforeDecom = usage.get("nodeUsage").get("min");
      assertTrue(!minUsageBeforeDecom.equalsIgnoreCase(zeroNodeUsage));

      if (decommissionState == AdminStates.DECOMMISSION_INPROGRESS) {
        // Start decommissioning datanode
        ArrayList<DatanodeInfo> decommissioningNodes = namenodeDecomList.
            get(0);
        decommissionedNodeInfo = takeNodeOutofService(0, null, 0,
            decommissioningNodes, decommissionState);
        // NodeUsage should not include DECOMMISSION_INPROGRESS node
        // (minUsage should be 0.00%)
        usage = (Map<String, Map<String, String>>)
            JSON.parse(ns.getNodeUsage());
        assertTrue(usage.get("nodeUsage").get("min").
            equalsIgnoreCase(zeroNodeUsage));
      }
      // Recommission node
      putNodeInService(0, decommissionedNodeInfo);

      usage = (Map<String, Map<String, String>>) JSON.parse(ns.getNodeUsage());
      String nodeusageAfterRecommi =
          decommissionState == AdminStates.DECOMMISSION_INPROGRESS
              ? minUsageBeforeDecom
              : zeroNodeUsage;
      assertTrue(usage.get("nodeUsage").get("min").
          equalsIgnoreCase(nodeusageAfterRecommi));
    } finally {
      cleanupFile(fileSys, file1);
    }
  }

  @Test
  public void testUsedCapacity() throws Exception {
    int numNamenodes = 1;
    int numDatanodes = 2;

    startCluster(numNamenodes, numDatanodes);
    FSNamesystem ns = getCluster().getNamesystem(0);
    BlockManager blockManager = ns.getBlockManager();
    DatanodeStatistics datanodeStatistics = blockManager.getDatanodeManager()
        .getDatanodeStatistics();

    long initialUsedCapacity = datanodeStatistics.getCapacityUsed();
    long initialTotalCapacity = datanodeStatistics.getCapacityTotal();
    long initialBlockPoolUsed = datanodeStatistics.getBlockPoolUsed();
    ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList =
        new ArrayList<ArrayList<DatanodeInfo>>(numNamenodes);
    namenodeDecomList.add(0, new ArrayList<>(numDatanodes));
    ArrayList<DatanodeInfo> decommissionedNodes = namenodeDecomList.get(0);
    //decommission one node
    DatanodeInfo decomNode = takeNodeOutofService(0, null, 0,
        decommissionedNodes, AdminStates.DECOMMISSIONED);
    decommissionedNodes.add(decomNode);
    long newUsedCapacity = datanodeStatistics.getCapacityUsed();
    long newTotalCapacity = datanodeStatistics.getCapacityTotal();
    long newBlockPoolUsed = datanodeStatistics.getBlockPoolUsed();

    assertTrue("DfsUsedCapacity should not be the same after a node has " +
        "been decommissioned!", initialUsedCapacity != newUsedCapacity);
    assertTrue("TotalCapacity should not be the same after a node has " +
        "been decommissioned!", initialTotalCapacity != newTotalCapacity);
    assertTrue("BlockPoolUsed should not be the same after a node has " +
        "been decommissioned!",initialBlockPoolUsed != newBlockPoolUsed);
  }

  /**
   * Verify if multiple DataNodes can be decommission at the same time.
   */
  @Test(timeout = 360000)
  public void testMultipleNodesDecommission() throws Exception {
    startCluster(1, 5);
    final Path file = new Path("/testMultipleNodesDecommission.dat");
    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);

    int repl = 3;
    writeFile(fileSys, file, repl, 1);
    // Request Decommission for DataNodes 1 and 2.
    List<DatanodeInfo> decomDataNodes = takeNodeOutofService(0,
        Lists.newArrayList(getCluster().getDataNodes().get(0).getDatanodeUuid(),
            getCluster().getDataNodes().get(1).getDatanodeUuid()),
        Long.MAX_VALUE, null, null, AdminStates.DECOMMISSIONED);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          String errMsg = checkFile(fileSys, file, repl,
              decomDataNodes.get(0).getXferAddr(), 5);
          if (errMsg != null) {
            LOG.warn("Check file: " + errMsg);
          }
          return true;
        } catch (IOException e) {
          LOG.warn("Check file: " + e);
          return false;
        }
      }
    }, 500, 30000);

    // Put the decommissioned nodes back in service.
    for (DatanodeInfo datanodeInfo : decomDataNodes) {
      putNodeInService(0, datanodeInfo);
    }

    cleanupFile(fileSys, file);
  }
}
