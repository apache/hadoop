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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommissioningStatus {
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 8192;
  private static final int fileSize = 16384;
  private static final int numDatanodes = 2;
  private static MiniDFSCluster cluster;
  private static FileSystem fileSys;
  private static Path excludeFile;
  private static FileSystem localFileSys;
  private static Configuration conf;
  private static Path dir;

  final ArrayList<String> decommissionedNodes = new ArrayList<String>(numDatanodes);
  
  @BeforeClass
  public static void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);

    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    dir = new Path(workingDir, "build/test/data/work-dir/decommission");
    assertTrue(localFileSys.mkdirs(dir));
    excludeFile = new Path(dir, "exclude");
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    Path includeFile = new Path(dir, "include");
    conf.set(DFSConfigKeys.DFS_HOSTS, includeFile.toUri().getPath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
        4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY, 1);

    writeConfigFile(localFileSys, excludeFile, null);
    writeConfigFile(localFileSys, includeFile, null);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (localFileSys != null ) cleanupFile(localFileSys, dir);
    if(fileSys != null) fileSys.close();
    if(cluster != null) cluster.shutdown();
  }

  private static void writeConfigFile(FileSystem fs, Path name,
      ArrayList<String> nodes) throws IOException {

    // delete if it already exists
    if (fs.exists(name)) {
      fs.delete(name, true);
    }

    FSDataOutputStream stm = fs.create(name);

    if (nodes != null) {
      for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
        String node = it.next();
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  }

  private void writeFile(FileSystem fileSys, Path name, short repl)
      throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096), repl,
        blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
 
  private FSDataOutputStream writeIncompleteFile(FileSystem fileSys, Path name,
      short repl) throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096), repl,
        blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    // need to make sure that we actually write out both file blocks
    // (see FSOutputSummer#flush)
    stm.flush();
    // Do not close stream, return it
    // so that it is not garbage collected
    return stm;
  }
  
  static private void cleanupFile(FileSystem fileSys, Path name)
      throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * Decommissions the node at the given index
   */
  private String decommissionNode(FSNamesystem namesystem, DFSClient client,
      FileSystem localFileSys, int nodeIndex) throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    String nodename = info[nodeIndex].getXferAddr();
    decommissionNode(namesystem, localFileSys, nodename);
    return nodename;
  }

  /*
   * Decommissions the node by name
   */
  private void decommissionNode(FSNamesystem namesystem,
      FileSystem localFileSys, String dnName) throws IOException {
    System.out.println("Decommissioning node: " + dnName);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>(decommissionedNodes);
    nodes.add(dnName);
    writeConfigFile(localFileSys, excludeFile, nodes);
  }

  private void checkDecommissionStatus(DatanodeDescriptor decommNode,
      int expectedUnderRep, int expectedDecommissionOnly,
      int expectedUnderRepInOpenFiles) {
    assertEquals(decommNode.decommissioningStatus.getUnderReplicatedBlocks(),
        expectedUnderRep);
    assertEquals(
        decommNode.decommissioningStatus.getDecommissionOnlyReplicas(),
        expectedDecommissionOnly);
    assertEquals(decommNode.decommissioningStatus
        .getUnderReplicatedInOpenFiles(), expectedUnderRepInOpenFiles);
  }

  private void checkDFSAdminDecommissionStatus(
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
   * Tests Decommissioning Status in DFS.
   */

  @Test
  public void testDecommissionStatus() throws IOException, InterruptedException {
    InetSocketAddress addr = new InetSocketAddress("localhost", cluster
        .getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", 2, info.length);
    DistributedFileSystem fileSys = cluster.getFileSystem();
    DFSAdmin admin = new DFSAdmin(cluster.getConfiguration(0));

    short replicas = 2;
    //
    // Decommission one node. Verify the decommission status
    // 
    Path file1 = new Path("decommission.dat");
    writeFile(fileSys, file1, replicas);

    Path file2 = new Path("decommission1.dat");
    FSDataOutputStream st1 = writeIncompleteFile(fileSys, file2, replicas);
    Thread.sleep(5000);

    FSNamesystem fsn = cluster.getNamesystem();
    final DatanodeManager dm = fsn.getBlockManager().getDatanodeManager();
    for (int iteration = 0; iteration < numDatanodes; iteration++) {
      String downnode = decommissionNode(fsn, client, localFileSys, iteration);
      dm.refreshNodes(conf);
      decommissionedNodes.add(downnode);
      Thread.sleep(5000);
      final List<DatanodeDescriptor> decommissioningNodes = dm.getDecommissioningNodes();
      if (iteration == 0) {
        assertEquals(decommissioningNodes.size(), 1);
        DatanodeDescriptor decommNode = decommissioningNodes.get(0);
        checkDecommissionStatus(decommNode, 4, 0, 2);
        checkDFSAdminDecommissionStatus(decommissioningNodes.subList(0, 1),
            fileSys, admin);
      } else {
        assertEquals(decommissioningNodes.size(), 2);
        DatanodeDescriptor decommNode1 = decommissioningNodes.get(0);
        DatanodeDescriptor decommNode2 = decommissioningNodes.get(1);
        checkDecommissionStatus(decommNode1, 4, 4, 2);
        checkDecommissionStatus(decommNode2, 4, 4, 2);
        checkDFSAdminDecommissionStatus(decommissioningNodes.subList(0, 2),
            fileSys, admin);
      }
    }
    // Call refreshNodes on FSNamesystem with empty exclude file.
    // This will remove the datanodes from decommissioning list and
    // make them available again.
    writeConfigFile(localFileSys, excludeFile, null);
    dm.refreshNodes(conf);
    st1.close();
    cleanupFile(fileSys, file1);
    cleanupFile(fileSys, file2);
  }

  /**
   * Verify a DN remains in DECOMMISSION_INPROGRESS state if it is marked
   * as dead before decommission has completed. That will allow DN to resume
   * the replication process after it rejoins the cluster.
   */
  @Test(timeout=120000)
  public void testDecommissionStatusAfterDNRestart()
      throws IOException, InterruptedException {
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
    decommissionNode(fsn, localFileSys, dnName);
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
    BlockManagerTestUtil.checkDecommissionState(dm, dead.get(0));

    // Verify that the DN remains in DECOMMISSION_INPROGRESS state.
    assertTrue("the node should be DECOMMISSION_IN_PROGRESSS",
        dead.get(0).isDecommissionInProgress());

    // Delete the under-replicated file, which should let the 
    // DECOMMISSION_IN_PROGRESS node become DECOMMISSIONED
    cleanupFile(fileSys, f);
    BlockManagerTestUtil.checkDecommissionState(dm, dead.get(0));
    assertTrue("the node should be decommissioned",
        dead.get(0).isDecommissioned());

    // Add the node back
    cluster.restartDataNode(dataNodeProperties, true);
    cluster.waitActive();

    // Call refreshNodes on FSNamesystem with empty exclude file.
    // This will remove the datanodes from decommissioning list and
    // make them available again.
    writeConfigFile(localFileSys, excludeFile, null);
    dm.refreshNodes(conf);
  }

  /**
   * Verify the support for decommissioning a datanode that is already dead.
   * Under this scenario the datanode should immediately be marked as
   * DECOMMISSIONED
   */
  @Test(timeout=120000)
  public void testDecommissionDeadDN()
      throws IOException, InterruptedException, TimeoutException {
    DatanodeID dnID = cluster.getDataNodes().get(0).getDatanodeId();
    String dnName = dnID.getXferAddr();
    DataNodeProperties stoppedDN = cluster.stopDataNode(0);
    DFSTestUtil.waitForDatanodeState(cluster, dnID.getDatanodeUuid(),
        false, 30000);
    FSNamesystem fsn = cluster.getNamesystem();
    final DatanodeManager dm = fsn.getBlockManager().getDatanodeManager();
    DatanodeDescriptor dnDescriptor = dm.getDatanode(dnID);
    decommissionNode(fsn, localFileSys, dnName);
    dm.refreshNodes(conf);
    BlockManagerTestUtil.checkDecommissionState(dm, dnDescriptor);
    assertTrue(dnDescriptor.isDecommissioned());

    // Add the node back
    cluster.restartDataNode(stoppedDN, true);
    cluster.waitActive();

    // Call refreshNodes on FSNamesystem with empty exclude file to remove the
    // datanode from decommissioning list and make it available again.
    writeConfigFile(localFileSys, excludeFile, null);
    dm.refreshNodes(conf);
  }
}
