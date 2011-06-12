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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommission {
  public static final Log LOG = LogFactory.getLog(TestDecommission.class);
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int HEARTBEAT_INTERVAL = 1; // heartbeat interval in seconds

  Random myrand = new Random();
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
    Path dir = new Path(workingDir, "build/test/data/work-dir/decommission");
    hostsFile = new Path(dir, "hosts");
    excludeFile = new Path(dir, "exclude");
    
    // Setup conf
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 4);
    writeConfigFile(excludeFile, null);
  }
  
  @After
  public void teardown() throws IOException {
    cleanupFile(localFileSys, excludeFile.getParent());
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  private void writeConfigFile(Path name, ArrayList<String> nodes) 
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
    FSDataOutputStream stm = fileSys.create(name, true, 
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
    LOG.info("Created file " + name + " with " + repl + " replicas.");
  }
  
  /**
   * For blocks that reside on the nodes that are down, verify that their
   * replication factor is 1 more than the specified one.
   */
  private void checkFile(FileSystem fileSys, Path name, int repl,
                         String downnode, int numDatanodes) throws IOException {
    //
    // sleep an additional 10 seconds for the blockreports from the datanodes
    // to arrive. 
    //
    // need a raw stream
    assertTrue("Not HDFS:"+fileSys.getUri(), fileSys instanceof DistributedFileSystem);
        
    DFSClient.DFSDataInputStream dis = (DFSClient.DFSDataInputStream) 
      ((DistributedFileSystem)fileSys).open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();

    for (LocatedBlock blk : dinfo) { // for each block
      int hasdown = 0;
      int firstDecomNodeIndex = -1;
      DatanodeInfo[] nodes = blk.getLocations();
      for (int j = 0; j < nodes.length; j++) {     // for each replica
        if (nodes[j].getName().equals(downnode)) {
          hasdown++;
          LOG.info("Block " + blk.getBlock() + " replica " + nodes[j].getName()
              + " is decommissioned.");
        }
        if (nodes[j].isDecommissioned()) {
          if (firstDecomNodeIndex == -1) {
            firstDecomNodeIndex = j;
          }
          continue;
        }
        assertEquals("Decom node is not at the end", firstDecomNodeIndex, -1);
      }
      LOG.info("Block " + blk.getBlock() + " has " + hasdown
          + " decommissioned replica.");
      assertEquals("Number of replicas for block " + blk.getBlock(),
                   Math.min(numDatanodes, repl+hasdown), nodes.length);  
    }
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * decommission one random node and wait for each to reach the
   * given {@code waitForState}.
   */
  private DatanodeInfo decommissionNode(int nnIndex,
                                  ArrayList<DatanodeInfo>decommissionedNodes,
                                  AdminStates waitForState)
    throws IOException {
    DFSClient client = getDfsClient(cluster.getNameNode(nnIndex), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    //
    // pick one datanode randomly.
    //
    int index = 0;
    boolean found = false;
    while (!found) {
      index = myrand.nextInt(info.length);
      if (!info[index].isDecommissioned()) {
        found = true;
      }
    }
    String nodename = info[index].getName();
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
    cluster.getNamesystem(nnIndex).refreshNodes(conf);
    DatanodeInfo ret = cluster.getNamesystem(nnIndex).getDatanode(info[index]);
    waitNodeState(ret, waitForState);
    return ret;
  }

  /* 
   * Wait till node is fully decommissioned.
   */
  private void waitNodeState(DatanodeInfo node,
                             AdminStates state) throws IOException {
    boolean done = state == node.getAdminState();
    while (!done) {
      LOG.info("Waiting for node " + node + " to change state to "
          + state + " current state: " + node.getAdminState());
      try {
        Thread.sleep(HEARTBEAT_INTERVAL * 1000);
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
    cluster = new MiniDFSCluster.Builder(conf).numNameNodes(numNameNodes)
        .numDataNodes(numDatanodes).build();
    cluster.waitActive();
    for (int i = 0; i < numNameNodes; i++) {
      DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
      validateCluster(client, numDatanodes);
    }
  }
  
  private void verifyStats(NameNode namenode, FSNamesystem fsn,
      DatanodeInfo node, boolean decommissioning) throws InterruptedException {
    // Do the stats check over 10 iterations
    for (int i = 0; i < 10; i++) {
      long[] newStats = namenode.getStats();

      // For decommissioning nodes, ensure capacity of the DN is no longer
      // counted. Only used space of the DN is counted in cluster capacity
      assertEquals(newStats[0], decommissioning ? node.getDfsUsed() : 
        node.getCapacity());

      // Ensure cluster used capacity is counted for both normal and
      // decommissioning nodes
      assertEquals(newStats[1], node.getDfsUsed());

      // For decommissioning nodes, remaining space from the DN is not counted
      assertEquals(newStats[2], decommissioning ? 0 : node.getRemaining());

      // Ensure transceiver count is same as that DN
      assertEquals(fsn.getTotalLoad(), node.getXceiverCount());
      
      Thread.sleep(HEARTBEAT_INTERVAL * 1000); // Sleep heart beat interval
    }
  }

  /**
   * Tests decommission for non federated cluster
   */
  @Test
  public void testDecommission() throws IOException {
    testDecommission(1, 6);
  }
  
  /**
   * Test decommission for federeated cluster
   */
  @Test
  public void testDecommissionFederation() throws IOException {
    testDecommission(2, 2);
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
        writeFile(fileSys, file1, replicas);
        
        // Decommission one node. Verify that node is decommissioned.
        DatanodeInfo decomNode = decommissionNode(i, decommissionedNodes,
            AdminStates.DECOMMISSIONED);
        decommissionedNodes.add(decomNode);
        
        // Ensure decommissioned datanode is not automatically shutdown
        DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
        assertEquals("All datanodes must be alive", numDatanodes, 
            client.datanodeReport(DatanodeReportType.LIVE).length);
        checkFile(fileSys, file1, replicas, decomNode.getName(), numDatanodes);
        cleanupFile(fileSys, file1);
      }
    }
    
    // Restart the cluster and ensure decommissioned datanodes
    // are allowed to register with the namenode
    cluster.shutdown();
    startCluster(numNamenodes, numDatanodes, conf);
  }
  
  /**
   * Tests cluster storage statistics during decommissioning for non
   * federated cluster
   */
  @Test
  public void testClusterStats() throws Exception {
    testClusterStats(1);
  }
  
  /**
   * Tests cluster storage statistics during decommissioning for
   * federated cluster
   */
  @Test
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
      DatanodeInfo downnode = decommissionNode(i, null,
          AdminStates.DECOMMISSION_INPROGRESS);
      // Check namenode stats for multiple datanode heartbeats
      verifyStats(namenode, fsn, downnode, true);
      
      // Stop decommissioning and verify stats
      writeConfigFile(excludeFile, null);
      fsn.refreshNodes(conf);
      DatanodeInfo ret = fsn.getDatanode(downnode);
      waitNodeState(ret, AdminStates.NORMAL);
      verifyStats(namenode, fsn, ret, false);
    }
  }
  
  /**
   * Test host/include file functionality. Only datanodes
   * in the include file are allowed to connect to the namenode in a non
   * federated cluster.
   */
  @Test
  public void testHostsFile() throws IOException, InterruptedException {
    // Test for a single namenode cluster
    testHostsFile(1);
  }
  
  /**
   * Test host/include file functionality. Only datanodes
   * in the include file are allowed to connect to the namenode in a 
   * federated cluster.
   */
  @Test
  public void testHostsFileFederation() throws IOException, InterruptedException {
    // Test for 3 namenode federated cluster
    testHostsFile(3);
  }
  
  public void testHostsFile(int numNameNodes) throws IOException,
      InterruptedException {
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    int numDatanodes = 1;
    cluster = new MiniDFSCluster.Builder(conf).numNameNodes(numNameNodes)
        .numDataNodes(numDatanodes).setupHostsFile(true).build();
    cluster.waitActive();
    
    // Now empty hosts file and ensure the datanode is disallowed
    // from talking to namenode, resulting in it's shutdown.
    ArrayList<String>list = new ArrayList<String>();
    list.add("invalidhost");
    writeConfigFile(hostsFile, list);
    
    for (int j = 0; j < numNameNodes; j++) {
      cluster.getNamesystem(j).refreshNodes(conf);
      
      DFSClient client = getDfsClient(cluster.getNameNode(j), conf);
      DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
      for (int i = 0 ; i < 5 && info.length != 0; i++) {
        LOG.info("Waiting for datanode to be marked dead");
        Thread.sleep(HEARTBEAT_INTERVAL * 1000);
        info = client.datanodeReport(DatanodeReportType.LIVE);
      }
      assertEquals("Number of live nodes should be 0", 0, info.length);
    }
  }
}
