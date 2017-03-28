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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DecommissionManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommission {
  public static final Logger LOG = LoggerFactory.getLogger(TestDecommission
      .class);
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int HEARTBEAT_INTERVAL = 1; // heartbeat interval in seconds
  static final int BLOCKREPORT_INTERVAL_MSEC = 1000; //block report in msec
  static final int NAMENODE_REPLICATION_INTERVAL = 1; //replication interval

  final Random myrand = new Random();
  Path dir;
  Path hostsFile;
  Path excludeFile;
  FileSystem localFileSys;
  Configuration conf;
  MiniDFSCluster cluster = null;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    dir = new Path(workingDir, PathUtils.getTestDirName(getClass()) + "/work-dir/decommission");
    hostsFile = new Path(dir, "hosts");
    excludeFile = new Path(dir, "exclude");
    
    // Setup conf
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, BLOCKREPORT_INTERVAL_MSEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, NAMENODE_REPLICATION_INTERVAL);
  
    writeConfigFile(hostsFile, null);
    writeConfigFile(excludeFile, null);
  }
  
  @After
  public void teardown() throws IOException {
    cleanupFile(localFileSys, dir);
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  private void writeConfigFile(Path name, List<String> nodes) 
    throws IOException {
    // delete if it already exists
    if (localFileSys.exists(name)) {
      localFileSys.delete(name, true);
    }

    FSDataOutputStream stm = localFileSys.create(name);
    
    if (nodes != null) {
      for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
        String node = it.next();
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  }

  private void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
    LOG.info("Created file " + name + " with " + repl + " replicas.");
  }

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

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * decommission the DN at index dnIndex or one random node if dnIndex is set
   * to -1 and wait for the node to reach the given {@code waitForState}.
   */
  private DatanodeInfo decommissionNode(int nnIndex,
                                  String datanodeUuid,
                                  ArrayList<DatanodeInfo>decommissionedNodes,
                                  AdminStates waitForState)
    throws IOException {
    DFSClient client = getDfsClient(cluster.getNameNode(nnIndex), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    //
    // pick one datanode randomly unless the caller specifies one.
    //
    int index = 0;
    if (datanodeUuid == null) {
      boolean found = false;
      while (!found) {
        index = myrand.nextInt(info.length);
        if (!info[index].isDecommissioned()) {
          found = true;
        }
      }
    } else {
      // The caller specifies a DN
      for (; index < info.length; index++) {
        if (info[index].getDatanodeUuid().equals(datanodeUuid)) {
          break;
        }
      }
      if (index == info.length) {
        throw new IOException("invalid datanodeUuid " + datanodeUuid);
      }
    }
    String nodename = info[index].getXferAddr();
    LOG.info("Decommissioning node: " + nodename);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>();
    if (decommissionedNodes != null) {
      for (DatanodeInfo dn : decommissionedNodes) {
        nodes.add(dn.getName());
      }
    }
    nodes.add(nodename);
    writeConfigFile(excludeFile, nodes);
    refreshNodes(cluster.getNamesystem(nnIndex), conf);
    DatanodeInfo ret = NameNodeAdapter.getDatanode(
        cluster.getNamesystem(nnIndex), info[index]);
    waitNodeState(ret, waitForState);
    return ret;
  }

  /* Ask a specific NN to stop decommission of the datanode and wait for each
   * to reach the NORMAL state.
   */
  private void recommissionNode(int nnIndex, DatanodeInfo decommissionedNode) throws IOException {
    LOG.info("Recommissioning node: " + decommissionedNode);
    writeConfigFile(excludeFile, null);
    refreshNodes(cluster.getNamesystem(nnIndex), conf);
    waitNodeState(decommissionedNode, AdminStates.NORMAL);

  }

  /* 
   * Wait till node is fully decommissioned.
   */
  private void waitNodeState(DatanodeInfo node,
                             AdminStates state) {
    boolean done = state == node.getAdminState();
    while (!done) {
      LOG.info("Waiting for node " + node + " to change state to "
          + state + " current state: " + node.getAdminState());
      try {
        Thread.sleep(HEARTBEAT_INTERVAL * 500);
      } catch (InterruptedException e) {
        // nothing
      }
      done = state == node.getAdminState();
    }
    LOG.info("node " + node + " reached the state " + state);
  }
  
  /* Get DFSClient to the namenode */
  private static DFSClient getDfsClient(NameNode nn,
      Configuration conf) throws IOException {
    return new DFSClient(nn.getNameNodeAddress(), conf);
  }
  
  /* Validate cluster has expected number of datanodes */
  private static void validateCluster(DFSClient client, int numDNs)
      throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDNs, info.length);
  }
  
  /** Start a MiniDFSCluster 
   * @throws IOException */
  private void startCluster(int numNameNodes, int numDatanodes,
      Configuration conf) throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(numNameNodes))
        .numDataNodes(numDatanodes).build();
    cluster.waitActive();
    for (int i = 0; i < numNameNodes; i++) {
      DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
      validateCluster(client, numDatanodes);
    }
  }

  static void refreshNodes(final FSNamesystem ns, final Configuration conf
      ) throws IOException {
    ns.getBlockManager().getDatanodeManager().refreshNodes(conf);
  }
  
  private void verifyStats(NameNode namenode, FSNamesystem fsn,
      DatanodeInfo info, DataNode node, boolean decommissioning)
      throws InterruptedException, IOException {
    // Do the stats check over 10 heartbeats
    for (int i = 0; i < 10; i++) {
      long[] newStats = namenode.getRpcServer().getStats();

      // For decommissioning nodes, ensure capacity of the DN is no longer
      // counted. Only used space of the DN is counted in cluster capacity
      assertEquals(newStats[0],
          decommissioning ? info.getDfsUsed() : info.getCapacity());

      // Ensure cluster used capacity is counted for both normal and
      // decommissioning nodes
      assertEquals(newStats[1], info.getDfsUsed());

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
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    startCluster(numNamenodes, numDatanodes, conf);

    ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList = new ArrayList<ArrayList<DatanodeInfo>>(
        numNamenodes);
    namenodeDecomList.add(0, new ArrayList<DatanodeInfo>(numDatanodes));

    Path file1 = new Path("testDecommission2.dat");
    int replicas = 4;

    // Start decommissioning one namenode at a time
    ArrayList<DatanodeInfo> decommissionedNodes = namenodeDecomList.get(0);
    FileSystem fileSys = cluster.getFileSystem(0);
    FSNamesystem ns = cluster.getNamesystem(0);

    writeFile(fileSys, file1, replicas);

    int deadDecomissioned = ns.getNumDecomDeadDataNodes();
    int liveDecomissioned = ns.getNumDecomLiveDataNodes();

    // Decommission one node. Verify that node is decommissioned.
    DatanodeInfo decomNode = decommissionNode(0, null, decommissionedNodes,
        AdminStates.DECOMMISSIONED);
    decommissionedNodes.add(decomNode);
    assertEquals(deadDecomissioned, ns.getNumDecomDeadDataNodes());
    assertEquals(liveDecomissioned + 1, ns.getNumDecomLiveDataNodes());

    // Ensure decommissioned datanode is not automatically shutdown
    DFSClient client = getDfsClient(cluster.getNameNode(0), conf);
    assertEquals("All datanodes must be alive", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);
    assertNull(checkFile(fileSys, file1, replicas, decomNode.getXferAddr(),
        numDatanodes));
    cleanupFile(fileSys, file1);

    // Restart the cluster and ensure recommissioned datanodes
    // are allowed to register with the namenode
    cluster.shutdown();
    startCluster(1, 4, conf);
    cluster.shutdown();
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
    Configuration hdfsConf = new HdfsConfiguration(conf);
    hdfsConf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    hdfsConf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 30000);
    hdfsConf.setInt(DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY, 2);

    // The time to wait so that the slow DN's heartbeat is considered old
    // by BlockPlacementPolicyDefault and thus will choose that DN for
    // excess replica.
    long slowHeartbeatDNwaitTime =
        hdfsConf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000 * (hdfsConf.getInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT) + 1);

    cluster = new MiniDFSCluster.Builder(hdfsConf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(3).build();

    cluster.transitionToActive(0);
    cluster.waitActive();


    // Step 1, create a cluster with 4 DNs. Blocks are stored on the first 3 DNs.
    // The last DN is empty. Also configure the last DN to have slow heartbeat
    // so that it will be chosen as excess replica candidate during recommission.

    // Step 1.a, copy blocks to the first 3 DNs. Given the replica count is the
    // same as # of DNs, each DN will have a replica for any block.
    Path file1 = new Path("testDecommissionHA.dat");
    int replicas = 3;
    FileSystem activeFileSys = cluster.getFileSystem(0);
    writeFile(activeFileSys, file1, replicas);

    HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(0),
        cluster.getNameNode(1));

    // Step 1.b, start a DN with slow heartbeat, so that we can know for sure it
    // will be chosen as the target of excess replica during recommission.
    hdfsConf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 30);
    cluster.startDataNodes(hdfsConf, 1, true, null, null, null);
    DataNode lastDN = cluster.getDataNodes().get(3);
    lastDN.getDatanodeUuid();

    // Step 2, decommission the first DN at both ANN and SBN.
    DataNode firstDN = cluster.getDataNodes().get(0);

    // Step 2.a, ask ANN to decomm the first DN
    DatanodeInfo decommissionedNodeFromANN = decommissionNode(
        0, firstDN.getDatanodeUuid(), null, AdminStates.DECOMMISSIONED);

    // Step 2.b, ask SBN to decomm the first DN
    DatanodeInfo decomNodeFromSBN = decommissionNode(1, firstDN.getDatanodeUuid(), null,
        AdminStates.DECOMMISSIONED);

    // Step 3, recommission the first DN on SBN and ANN to create excess replica
    // It recommissions the node on SBN first to create potential
    // inconsistent state. In production cluster, such insistent state can happen
    // even if recommission command was issued on ANN first given the async nature
    // of the system.

    // Step 3.a, ask SBN to recomm the first DN.
    // SBN has been fixed so that it no longer invalidates excess replica during
    // recommission.
    // Before the fix, SBN could get into the following state.
    //    1. the last DN would have been chosen as excess replica, given its
    //    heartbeat is considered old.
    //    Please refer to BlockPlacementPolicyDefault#chooseReplicaToDelete
    //    2. After recommissionNode finishes, SBN has 3 live replicas ( 0, 1, 2 )
    //    and one excess replica ( 3 )
    // After the fix,
    //    After recommissionNode finishes, SBN has 4 live replicas ( 0, 1, 2, 3 )
    Thread.sleep(slowHeartbeatDNwaitTime);
    recommissionNode(1, decomNodeFromSBN);

    // Step 3.b, ask ANN to recommission the first DN.
    // To verify the fix, the test makes sure the excess replica picked by ANN
    // is different from the one picked by SBN before the fix.
    // To achieve that, we make sure next-to-last DN is chosen as excess replica
    // by ANN.
    // 1. restore LastDNprop's heartbeat interval.
    // 2. Make next-to-last DN's heartbeat slow.
    MiniDFSCluster.DataNodeProperties LastDNprop = cluster.stopDataNode(3);
    LastDNprop.conf.setLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    cluster.restartDataNode(LastDNprop);

    MiniDFSCluster.DataNodeProperties nextToLastDNprop = cluster.stopDataNode(2);
    nextToLastDNprop.conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 30);
    cluster.restartDataNode(nextToLastDNprop);
    cluster.waitActive();
    Thread.sleep(slowHeartbeatDNwaitTime);
    recommissionNode(0, decommissionedNodeFromANN);

    // Step 3.c, make sure the DN has deleted the block and report to NNs
    cluster.triggerHeartbeats();
    HATestUtil.waitForDNDeletions(cluster);
    cluster.triggerDeletionReports();

    // Step 4, decommission the first DN on both ANN and SBN
    // With the fix to make sure SBN no longer marks excess replica
    // during recommission, SBN's decommission can finish properly
    decommissionNode(0, firstDN.getDatanodeUuid(), null,
        AdminStates.DECOMMISSIONED);

    // Ask SBN to decomm the first DN
    decommissionNode(1, firstDN.getDatanodeUuid(), null,
        AdminStates.DECOMMISSIONED);

    cluster.shutdown();

  }

  private void testDecommission(int numNamenodes, int numDatanodes)
      throws IOException {
    LOG.info("Starting test testDecommission");
    startCluster(numNamenodes, numDatanodes, conf);
    
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
        FileSystem fileSys = cluster.getFileSystem(i);
        FSNamesystem ns = cluster.getNamesystem(i);

        writeFile(fileSys, file1, replicas);

        int deadDecomissioned = ns.getNumDecomDeadDataNodes();
        int liveDecomissioned = ns.getNumDecomLiveDataNodes();

        // Decommission one node. Verify that node is decommissioned.
        DatanodeInfo decomNode = decommissionNode(i, null, decommissionedNodes,
            AdminStates.DECOMMISSIONED);
        decommissionedNodes.add(decomNode);
        assertEquals(deadDecomissioned, ns.getNumDecomDeadDataNodes());
        assertEquals(liveDecomissioned + 1, ns.getNumDecomLiveDataNodes());

        // Ensure decommissioned datanode is not automatically shutdown
        DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
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
    cluster.shutdown();
    startCluster(numNamenodes, numDatanodes, conf);
    cluster.shutdown();
  }

  /**
   * Test that over-replicated blocks are deleted on recommission.
   */
  @Test(timeout=120000)
  public void testRecommission() throws Exception {
    final int numDatanodes = 6;
    try {
      LOG.info("Starting test testRecommission");

      startCluster(1, numDatanodes, conf);

      final Path file1 = new Path("testDecommission.dat");
      final int replicas = numDatanodes - 1;

      ArrayList<DatanodeInfo> decommissionedNodes = Lists.newArrayList();
      final FileSystem fileSys = cluster.getFileSystem();

      // Write a file to n-1 datanodes
      writeFile(fileSys, file1, replicas);

      // Decommission one of the datanodes with a replica
      BlockLocation loc = fileSys.getFileBlockLocations(file1, 0, 1)[0];
      assertEquals("Unexpected number of replicas from getFileBlockLocations",
          replicas, loc.getHosts().length);
      final String toDecomHost = loc.getNames()[0];
      String toDecomUuid = null;
      for (DataNode d : cluster.getDataNodes()) {
        if (d.getDatanodeId().getXferAddr().equals(toDecomHost)) {
          toDecomUuid = d.getDatanodeId().getDatanodeUuid();
          break;
        }
      }
      assertNotNull("Could not find a dn with the block!", toDecomUuid);
      final DatanodeInfo decomNode =
          decommissionNode(0, toDecomUuid, decommissionedNodes,
              AdminStates.DECOMMISSIONED);
      decommissionedNodes.add(decomNode);
      final BlockManager blockManager =
          cluster.getNamesystem().getBlockManager();
      final DatanodeManager datanodeManager =
          blockManager.getDatanodeManager();
      BlockManagerTestUtil.recheckDecommissionState(datanodeManager);

      // Ensure decommissioned datanode is not automatically shutdown
      DFSClient client = getDfsClient(cluster.getNameNode(), conf);
      assertEquals("All datanodes must be alive", numDatanodes,
          client.datanodeReport(DatanodeReportType.LIVE).length);

      // wait for the block to be replicated
      final ExtendedBlock b = DFSTestUtil.getFirstBlock(fileSys, file1);
      final String uuid = toDecomUuid;
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          BlockInfoContiguous info =
              blockManager.getStoredBlock(b.getLocalBlock());
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
      recommissionNode(0, decomNode);
      BlockManagerTestUtil.recheckDecommissionState(datanodeManager);
      DFSTestUtil.waitForReplication(cluster, b, 1, replicas, 0);

      cleanupFile(fileSys, file1);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
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
    startCluster(numNameNodes, numDatanodes, conf);
    
    for (int i = 0; i < numNameNodes; i++) {
      FileSystem fileSys = cluster.getFileSystem(i);
      Path file = new Path("testClusterStats.dat");
      writeFile(fileSys, file, 1);
      
      FSNamesystem fsn = cluster.getNamesystem(i);
      NameNode namenode = cluster.getNameNode(i);
      
      DatanodeInfo decomInfo = decommissionNode(i, null, null,
          AdminStates.DECOMMISSION_INPROGRESS);
      DataNode decomNode = getDataNode(decomInfo);
      // Check namenode stats for multiple datanode heartbeats
      verifyStats(namenode, fsn, decomInfo, decomNode, true);
      
      // Stop decommissioning and verify stats
      writeConfigFile(excludeFile, null);
      refreshNodes(fsn, conf);
      DatanodeInfo retInfo = NameNodeAdapter.getDatanode(fsn, decomInfo);
      DataNode retNode = getDataNode(decomInfo);
      waitNodeState(retInfo, AdminStates.NORMAL);
      verifyStats(namenode, fsn, retInfo, retNode, false);
    }
  }

  private DataNode getDataNode(DatanodeInfo decomInfo) {
    DataNode decomNode = null;
    for (DataNode dn: cluster.getDataNodes()) {
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
  public void testHostsFileFederation() throws IOException, InterruptedException {
    // Test for 3 namenode federated cluster
    testHostsFile(3);
  }
  
  public void testHostsFile(int numNameNodes) throws IOException,
      InterruptedException {
    int numDatanodes = 1;
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(numNameNodes))
        .numDataNodes(numDatanodes).setupHostsFile(true).build();
    cluster.waitActive();
    
    // Now empty hosts file and ensure the datanode is disallowed
    // from talking to namenode, resulting in it's shutdown.
    ArrayList<String>list = new ArrayList<String>();
    final String bogusIp = "127.0.30.1";
    list.add(bogusIp);
    writeConfigFile(hostsFile, list);
    
    for (int j = 0; j < numNameNodes; j++) {
      refreshNodes(cluster.getNamesystem(j), conf);
      
      DFSClient client = getDfsClient(cluster.getNameNode(j), conf);
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
  public void testDecommissionWithOpenfile() throws IOException, InterruptedException {
    LOG.info("Starting test testDecommissionWithOpenfile");
    
    //At most 4 nodes will be decommissioned
    startCluster(1, 7, conf);
        
    FileSystem fileSys = cluster.getFileSystem(0);
    FSNamesystem ns = cluster.getNamesystem(0);
    
    String openFile = "/testDecommissionWithOpenfile.dat";
           
    writeFile(fileSys, new Path(openFile), (short)3);   
    // make sure the file was open for write
    FSDataOutputStream fdos =  fileSys.append(new Path(openFile)); 
    
    LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(cluster.getNameNode(0), openFile, 0, fileSize);
              
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
    
    writeConfigFile(excludeFile, nodes);
    refreshNodes(ns, conf);  
    for (DatanodeInfo dn : dnInfos) {
      waitNodeState(dn, AdminStates.DECOMMISSIONED);
    }           

    fdos.close();
  }

  @Test(timeout = 360000)
  public void testDecommissionWithOpenFileAndBlockRecovery()
      throws IOException, InterruptedException {
    startCluster(1, 6, conf);
    cluster.waitActive();

    Path file = new Path("/testRecoveryDecommission");

    // Create a file and never close the output stream to trigger recovery
    DistributedFileSystem dfs = cluster.getFileSystem();
    FSNamesystem ns = cluster.getNamesystem(0);
    FSDataOutputStream out = dfs.create(file, true,
        conf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) 3, blockSize);

    // Write data to the file
    long writtenBytes = 0;
    while (writtenBytes < fileSize) {
      out.writeLong(writtenBytes);
      writtenBytes += 8;
    }
    out.hsync();

    DatanodeInfo[] lastBlockLocations = NameNodeAdapter.getBlockLocations(
      cluster.getNameNode(), "/testRecoveryDecommission", 0, fileSize)
      .getLastLocatedBlock().getLocations();

    // Decommission all nodes of the last block
    ArrayList<String> toDecom = new ArrayList<>();
    for (DatanodeInfo dnDecom : lastBlockLocations) {
      toDecom.add(dnDecom.getXferAddr());
    }
    writeConfigFile(excludeFile, toDecom);
    refreshNodes(ns, conf);

    // Make sure hard lease expires to trigger replica recovery
    cluster.setLeasePeriod(300L, 300L);
    Thread.sleep(2 * BLOCKREPORT_INTERVAL_MSEC);

    for (DatanodeInfo dnDecom : lastBlockLocations) {
      DatanodeInfo datanode = NameNodeAdapter.getDatanode(
          cluster.getNamesystem(), dnDecom);
      waitNodeState(datanode, AdminStates.DECOMMISSIONED);
    }

    assertEquals(dfs.getFileStatus(file).getLen(), writtenBytes);
  }

  @Test(timeout=120000)
  public void testCloseWhileDecommission() throws IOException,
      ExecutionException, InterruptedException {
    LOG.info("Starting test testCloseWhileDecommission");

    // min replication = 2
    Configuration hdfsConf = new HdfsConfiguration(conf);
    hdfsConf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 2);
    startCluster(1, 3, hdfsConf);

    FileSystem fileSys = cluster.getFileSystem(0);
    FSNamesystem ns = cluster.getNamesystem(0);

    String openFile = "/testDecommissionWithOpenfile.dat";

    writeFile(fileSys, new Path(openFile), (short)3);
    // make sure the file was open for write
    FSDataOutputStream fdos =  fileSys.append(new Path(openFile));
    byte[] bytes = new byte[1];
    fdos.write(bytes);
    fdos.hsync();

    LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(
        cluster.getNameNode(0), openFile, 0, fileSize);

    DatanodeInfo[] dnInfos4LastBlock = lbs.getLastLocatedBlock().getLocations();

    ArrayList<String> nodes = new ArrayList<String>();
    ArrayList<DatanodeInfo> dnInfos = new ArrayList<DatanodeInfo>();

    DatanodeManager dm = ns.getBlockManager().getDatanodeManager();
    //decommission 2 of the 3 nodes which have last block
    nodes.add(dnInfos4LastBlock[0].getXferAddr());
    dnInfos.add(dm.getDatanode(dnInfos4LastBlock[0]));
    nodes.add(dnInfos4LastBlock[1].getXferAddr());
    dnInfos.add(dm.getDatanode(dnInfos4LastBlock[1]));
    writeConfigFile(excludeFile, nodes);

    // because the cluster has only 3 nodes, and 2 of which are decomm'ed,
    // the last block file will remain under replicated.
    refreshNodes(cluster.getNamesystem(0), cluster.getConfiguration(0));

    // the close() should not fail despite the number of live replicas of
    // the last block becomes one.
    fdos.close();

    // make sure the two datanodes remain in decomm in progress state
    BlockManagerTestUtil.recheckDecommissionState(dm);
    assertTrackedAndPending(dm.getDecomManager(), 2, 0);
  }
  
  /**
   * Tests restart of namenode while datanode hosts are added to exclude file
   **/
  @Test(timeout=360000)
  public void testDecommissionWithNamenodeRestart()throws IOException, InterruptedException {
    LOG.info("Starting test testDecommissionWithNamenodeRestart");
    int numNamenodes = 1;
    int numDatanodes = 1;
    int replicas = 1;
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY, 5);

    startCluster(numNamenodes, numDatanodes, conf);
    Path file1 = new Path("testDecommissionWithNamenodeRestart.dat");
    FileSystem fileSys = cluster.getFileSystem();
    writeFile(fileSys, file1, replicas);
        
    DFSClient client = getDfsClient(cluster.getNameNode(), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    DatanodeID excludedDatanodeID = info[0];
    String excludedDatanodeName = info[0].getXferAddr();

    writeConfigFile(excludeFile, new ArrayList<String>(Arrays.asList(excludedDatanodeName)));

    //Add a new datanode to cluster
    cluster.startDataNodes(conf, 1, true, null, null, null, null);
    numDatanodes+=1;

    assertEquals("Number of datanodes should be 2 ", 2, cluster.getDataNodes().size());
    //Restart the namenode
    cluster.restartNameNode();
    DatanodeInfo datanodeInfo = NameNodeAdapter.getDatanode(
        cluster.getNamesystem(), excludedDatanodeID);
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
    cluster.shutdown();
    startCluster(numNamenodes, numDatanodes, conf);
    cluster.shutdown();
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
    Configuration hdfsConf = new Configuration(conf);
    // Any IPv4 address starting with 127 functions as a "loopback" address
    // which is connected to the current host.  So by choosing 127.0.0.100
    // as our registration name, we have chosen a name which is also a valid
    // way of reaching the local DataNode we're going to start.
    // Typically, a registration name would be a hostname, but we don't want
    // to deal with DNS in this test.
    final String registrationName = "127.0.0.100";
    final String nonExistentDn = "127.0.0.10";
    hdfsConf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, registrationName);
    cluster = new MiniDFSCluster.Builder(hdfsConf)
        .numDataNodes(1).checkDataNodeHostConfig(true)
        .setupHostsFile(true).build();
    cluster.waitActive();

    // Set up an includes file that doesn't have our datanode.
    ArrayList<String> nodes = new ArrayList<String>();
    nodes.add(nonExistentDn);
    writeConfigFile(hostsFile,  nodes);
    refreshNodes(cluster.getNamesystem(0), hdfsConf);

    // Wait for the DN to be marked dead.
    LOG.info("Waiting for DN to be marked as dead.");
    final DFSClient client = getDfsClient(cluster.getNameNode(0), hdfsConf);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        BlockManagerTestUtil
            .checkHeartbeat(cluster.getNamesystem().getBlockManager());
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
    int dnPort = cluster.getDataNodes().get(0).getXferPort();
    nodes = new ArrayList<String>();
    nodes.add(registrationName + ":" + dnPort);
    writeConfigFile(hostsFile,  nodes);
    refreshNodes(cluster.getNamesystem(0), hdfsConf);
    cluster.restartDataNode(0);
    cluster.triggerHeartbeats();

    // Wait for the DN to come back.
    LOG.info("Waiting for DN to come back.");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        BlockManagerTestUtil
            .checkHeartbeat(cluster.getNamesystem().getBlockManager());
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
    Configuration newConf = new Configuration(conf);
    org.apache.log4j.Logger.getLogger(DecommissionManager.class)
        .setLevel(Level.TRACE);
    // Turn the blocks per interval way down
    newConf.setInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        3);
    // Disable the normal monitor runs
    newConf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
        Integer.MAX_VALUE);
    startCluster(1, 3, newConf);
    final FileSystem fs = cluster.getFileSystem();
    final DatanodeManager datanodeManager =
        cluster.getNamesystem().getBlockManager().getDatanodeManager();
    final DecommissionManager decomManager = datanodeManager.getDecomManager();

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

  @Deprecated
  @Test(timeout=120000)
  public void testNodesPerInterval() throws Exception {
    Configuration newConf = new Configuration(conf);
    org.apache.log4j.Logger.getLogger(DecommissionManager.class)
        .setLevel(Level.TRACE);
    // Set the deprecated configuration key which limits the # of nodes per 
    // interval
    newConf.setInt("dfs.namenode.decommission.nodes.per.interval", 1);
    // Disable the normal monitor runs
    newConf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
        Integer.MAX_VALUE);
    startCluster(1, 3, newConf);
    final FileSystem fs = cluster.getFileSystem();
    final DatanodeManager datanodeManager =
        cluster.getNamesystem().getBlockManager().getDatanodeManager();
    final DecommissionManager decomManager = datanodeManager.getDecomManager();

    // Write a 3 block file, so each node has one block. Should scan 1 node 
    // each time.
    DFSTestUtil.createFile(fs, new Path("/file1"), 64, (short) 3, 0xBAD1DEA);
    for (int i=0; i<3; i++) {
      doDecomCheck(datanodeManager, decomManager, 1);
    }
  }

  private void doDecomCheck(DatanodeManager datanodeManager,
      DecommissionManager decomManager, int expectedNumCheckedNodes)
      throws IOException, ExecutionException, InterruptedException {
    // Decom all nodes
    ArrayList<DatanodeInfo> decommissionedNodes = Lists.newArrayList();
    for (DataNode d: cluster.getDataNodes()) {
      DatanodeInfo dn = decommissionNode(0, d.getDatanodeUuid(),
          decommissionedNodes,
          AdminStates.DECOMMISSION_INPROGRESS);
      decommissionedNodes.add(dn);
    }
    // Run decom scan and check
    BlockManagerTestUtil.recheckDecommissionState(datanodeManager);
    assertEquals("Unexpected # of nodes checked", expectedNumCheckedNodes, 
        decomManager.getNumNodesChecked());
    // Recommission all nodes
    for (DatanodeInfo dn : decommissionedNodes) {
      recommissionNode(0, dn);
    }
  }

  @Test(timeout=120000)
  public void testPendingNodes() throws Exception {
    Configuration newConf = new Configuration(conf);
    org.apache.log4j.Logger.getLogger(DecommissionManager.class)
        .setLevel(Level.TRACE);
    // Only allow one node to be decom'd at a time
    newConf.setInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
        1);
    // Disable the normal monitor runs
    newConf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 
        Integer.MAX_VALUE);
    startCluster(1, 3, newConf);
    final FileSystem fs = cluster.getFileSystem();
    final DatanodeManager datanodeManager =
        cluster.getNamesystem().getBlockManager().getDatanodeManager();
    final DecommissionManager decomManager = datanodeManager.getDecomManager();

    // Keep a file open to prevent decom from progressing
    HdfsDataOutputStream open1 =
        (HdfsDataOutputStream) fs.create(new Path("/openFile1"), (short)3);
    // Flush and trigger block reports so the block definitely shows up on NN
    open1.write(123);
    open1.hflush();
    for (DataNode d: cluster.getDataNodes()) {
      DataNodeTestUtils.triggerBlockReport(d);
    }
    // Decom two nodes, so one is still alive
    ArrayList<DatanodeInfo> decommissionedNodes = Lists.newArrayList();
    for (int i=0; i<2; i++) {
      final DataNode d = cluster.getDataNodes().get(i);
      DatanodeInfo dn = decommissionNode(0, d.getDatanodeUuid(), 
          decommissionedNodes, 
          AdminStates.DECOMMISSION_INPROGRESS);
      decommissionedNodes.add(dn);
    }

    for (int i=2; i>=0; i--) {
      assertTrackedAndPending(decomManager, 0, i);
      BlockManagerTestUtil.recheckDecommissionState(datanodeManager);
    }

    // Close file, try to decom the last node, should get stuck in tracked
    open1.close();
    final DataNode d = cluster.getDataNodes().get(2);
    DatanodeInfo dn = decommissionNode(0, d.getDatanodeUuid(),
        decommissionedNodes,
        AdminStates.DECOMMISSION_INPROGRESS);
    decommissionedNodes.add(dn);
    BlockManagerTestUtil.recheckDecommissionState(datanodeManager);
    
    assertTrackedAndPending(decomManager, 1, 0);
  }

  private void assertTrackedAndPending(DecommissionManager decomManager,
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
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1);
    try {
      cluster =
          new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(1))
              .numDataNodes(1).build();
      cluster.waitActive();
      DFSClient client = getDfsClient(cluster.getNameNode(0), conf);
      validateCluster(client, 1);

      ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList =
          new ArrayList<ArrayList<DatanodeInfo>>(1);
      namenodeDecomList.add(0, new ArrayList<DatanodeInfo>(1));

      // Move datanode1 to Decommissioned state
      ArrayList<DatanodeInfo> decommissionedNode = namenodeDecomList.get(0);
      decommissionNode(0, null,
          decommissionedNode, AdminStates.DECOMMISSIONED);

      FSNamesystem ns = cluster.getNamesystem(0);
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
      cluster.shutdown();
    }
  }
}
