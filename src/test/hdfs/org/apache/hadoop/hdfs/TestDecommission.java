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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
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
    conf.set("dfs.hosts.exclude", excludeFile.toUri().getPath());
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
  }
  
  private void printFileLocations(FileSystem fileSys, Path name)
  throws IOException {
    BlockLocation[] locations = fileSys.getFileBlockLocations(
        fileSys.getFileStatus(name), 0, fileSize);
    for (int idx = 0; idx < locations.length; idx++) {
      String[] loc = locations[idx].getHosts();
      StringBuilder buf = new StringBuilder("Block[" + idx + "] : ");
      for (int j = 0; j < loc.length; j++) {
        buf.append(loc[j] + " ");
      }
      LOG.info(buf.toString());
    }
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
      assertEquals("Number of replicas for block" + blk.getBlock(),
                   Math.min(numDatanodes, repl+hasdown), nodes.length);  
    }
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  private void printDatanodeReport(DatanodeInfo[] info) {
    LOG.info("-------------------------------------------------");
    for (int i = 0; i < info.length; i++) {
      LOG.info(info[i].getDatanodeReport());
      LOG.info("");
    }
  }

  /*
   * decommission one random node.
   */
  private DatanodeInfo decommissionNode(FSNamesystem namesystem,
                                  ArrayList<String>decommissionedNodes,
                                  AdminStates waitForState)
    throws IOException {
    DFSClient client = getDfsClient(cluster, conf);
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
    ArrayList<String> nodes = new ArrayList<String>(decommissionedNodes);
    nodes.add(nodename);
    writeConfigFile(excludeFile, nodes);
    namesystem.refreshNodes(conf);
    DatanodeInfo ret = namesystem.getDatanode(info[index]);
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
  }
  
  /* Get DFSClient to the namenode */
  private static DFSClient getDfsClient(MiniDFSCluster cluster,
      Configuration conf) throws IOException {
    InetSocketAddress addr = new InetSocketAddress("localhost", 
                                                   cluster.getNameNodePort());
    return new DFSClient(addr, conf);
  }
  
  /* Validate cluster has expected number of datanodes */
  private static void validateCluster(DFSClient client, int numDNs)
      throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDNs, info.length);
  }
  
  /** Start a MiniDFSCluster 
   * @throws IOException */
  private void startCluster(int numDatanodes, Configuration conf)
      throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
        .build();
    cluster.waitActive();
    DFSClient client = getDfsClient(cluster, conf);
    validateCluster(client, numDatanodes);
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
   * Tests Decommission in DFS.
   */
  @Test
  public void testDecommission() throws IOException {
    LOG.info("Starting test testDecommission");
    int numDatanodes = 6;
    startCluster(numDatanodes, conf);
    try {
      DFSClient client = getDfsClient(cluster, conf);
      FileSystem fileSys = cluster.getFileSystem();
      FSNamesystem fsn = cluster.getNamesystem();
      ArrayList<String> decommissionedNodes = new ArrayList<String>(numDatanodes);
      for (int iteration = 0; iteration < numDatanodes - 1; iteration++) {
        int replicas = numDatanodes - iteration - 1;
        
        // Decommission one node. Verify that node is decommissioned.
        Path file1 = new Path("testDecommission.dat");
        writeFile(fileSys, file1, replicas);
        LOG.info("Created file decommission.dat with " + replicas
            + " replicas.");
        printFileLocations(fileSys, file1);
        DatanodeInfo downnode = decommissionNode(fsn, decommissionedNodes,
            AdminStates.DECOMMISSIONED);
        decommissionedNodes.add(downnode.getName());
        
        // Ensure decommissioned datanode is not automatically shutdown
        assertEquals("All datanodes must be alive", numDatanodes, 
            client.datanodeReport(DatanodeReportType.LIVE).length);
        
        checkFile(fileSys, file1, replicas, downnode.getName(), numDatanodes);
        cleanupFile(fileSys, file1);
      }
      
      // Restart the cluster and ensure decommissioned datanodes
      // are allowed to register with the namenode
      cluster.shutdown();
      startCluster(numDatanodes, conf);
    } catch (IOException e) {
      DFSClient client = getDfsClient(cluster, conf);
      DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.ALL);
      printDatanodeReport(info);
      throw e;
    }
  }
  
  /**
   * Tests cluster storage statistics during decommissioning
   */
  @Test
  public void testClusterStats() throws IOException, InterruptedException {
    LOG.info("Starting test testClusterStats");
    int numDatanodes = 1;
    startCluster(numDatanodes, conf);
    
    FileSystem fileSys = cluster.getFileSystem();
    Path file = new Path("testClusterStats.dat");
    writeFile(fileSys, file, 1);
    
    FSNamesystem fsn = cluster.getNamesystem();
    NameNode namenode = cluster.getNameNode();
    ArrayList<String> decommissionedNodes = new ArrayList<String>(numDatanodes);
    DatanodeInfo downnode = decommissionNode(fsn, decommissionedNodes,
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
  
  /**
   * Test host file or include file functionality. Only datanodes
   * in the include file are allowed to connect to the namenode.
   */
  @Test
  public void testHostsFile() throws IOException, InterruptedException {
    conf.set("dfs.hosts", hostsFile.toUri().getPath());
    int numDatanodes = 1;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
        .setupHostsFile(true).build();
    cluster.waitActive();
    
    // Now empty hosts file and ensure the datanode is disallowed
    // from talking to namenode, resulting in it's shutdown.
    ArrayList<String>list = new ArrayList<String>();
    list.add("invalidhost");
    writeConfigFile(hostsFile, list);
    cluster.getNamesystem().refreshNodes(conf);
    
    DFSClient client = getDfsClient(cluster, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    for (int i = 0 ; i < 5 && info.length != 0; i++) {
      LOG.info("Waiting for datanode to be marked dead");
      Thread.sleep(HEARTBEAT_INTERVAL * 1000);
      info = client.datanodeReport(DatanodeReportType.LIVE);
    }
    assertEquals("Number of live nodes should be 0", 0, info.length);
  }
}
