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
package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests the decommissioning of nodes.
 * @author Dhruba Borthakur
 */
public class TestDecommission extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int numDatanodes = 4;

  Random myrand = new Random();

  private void writeFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    // create and write a file that contains three blocks of data
    FSOutputStream stm = fileSys.createRaw(name, true, (short)repl,
        (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  
  private void checkFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    String[][] locations = fileSys.getFileCacheHints(name, 0, fileSize);
    for (int idx = 0; idx < locations.length; idx++) {
      assertEquals("Number of replicas for block" + idx,
          Math.min(numDatanodes, repl), locations[idx].length);  
    }
  }

  /**
   * For blocks that reside on the nodes that are down, verify that their
   * replication factor is 1 more than the specified one.
   */
  private void checkFile(FileSystem fileSys, Path name, int repl,
                         String[] downnodes) throws IOException {
    FSInputStream is = fileSys.openRaw(name);
    DFSClient.DFSInputStream dis = (DFSClient.DFSInputStream) is;
    DatanodeInfo[][] dinfo = dis.getDataNodes();

    for (int blk = 0; blk < dinfo.length; blk++) { // for each block
      int hasdown = 0;
      DatanodeInfo[] nodes = dinfo[blk];
      for (int j = 0; j < nodes.length; j++) {     // for each replica
        for (int k = 0; downnodes != null && k < downnodes.length; k++) {
          if (nodes[j].getName().equals(downnodes[k])) {
            hasdown++;
            System.out.println("Block " + blk + " replica " +
                               nodes[j].getName() + " is decommissioned.");
          }
        }
      }
      System.out.println("Block " + blk + " has " + hasdown +
                           " decommissioned replica.");
      assertEquals("Number of replicas for block" + blk,
            Math.min(numDatanodes, repl+hasdown), nodes.length);  
    }
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name);
    assertTrue(!fileSys.exists(name));
  }

  private void printDatanodeReport(DatanodeInfo[] info) {
    System.out.println("-------------------------------------------------");
    for (int i = 0; i < info.length; i++) {
      System.out.println(info[i].getDatanodeReport());
      System.out.println();
    }
  }

  /*
   * decommission one random node.
   */
  private String[] decommissionNode(DFSClient client, FileSystem filesys)
                                    throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    DatanodeInfo[] info = client.datanodeReport();

    //
    // pick one datanode randomly.
    //
    int index = myrand.nextInt(info.length);
    String nodename = info[index].getName();
    System.out.println("Decommissioning node: " + nodename);
    String[] nodes = new String[1];
    nodes[0] = nodename;
    dfs.decommission(FSConstants.DecommissionAction.DECOMMISSION_SET, nodes);
    return nodes;
  }

  /*
   * put node back in action
   */
  private void commissionNode(DFSClient client, FileSystem filesys,
                              String[] nodes) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    DatanodeInfo[] info = client.datanodeReport();

    for (int i = 0; i < nodes.length; i++) {
      System.out.println("Putting node back in action: " + nodes[i]);
    }
    dfs.decommission(FSConstants.DecommissionAction.DECOMMISSION_CLEAR, nodes);
  }

  /* 
   * Check that node(s) were decommissioned
   */
  private void checkNodeDecommission(DFSClient client, FileSystem filesys,
                                     String[] nodes) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    boolean ret = dfs.decommission(
                    FSConstants.DecommissionAction.DECOMMISSION_GET, nodes);
    assertEquals("State of Decommissioned Datanode(s) ", ret, true);
  }

  /* 
   * Wait till node is fully decommissioned.
   */
  private void waitNodeDecommission(DFSClient client, FileSystem filesys,
                                     String[] nodes) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    boolean done = dfs.decommission(
                     FSConstants.DecommissionAction.DECOMMISSION_GET, nodes);
    while (!done) {
      System.out.println("Waiting for nodes " + nodes[0] +
                         " to be fully decommissioned...");
      try {
        Thread.sleep(5000L);
      } catch (InterruptedException e) {
        // nothing
      }
      done = dfs.decommission(FSConstants.DecommissionAction.DECOMMISSION_GET,
                              nodes);
    }
    //
    // sleep an additional 10 seconds for the blockreports from the datanodes
    // to arrive. 
    //
    try {
      Thread.sleep(10 * 1000L);
    } catch (Exception e) {
    }
  }
  
  /**
   * Tests Decommission in DFS.
   */
  public void testDecommission() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(65312, conf, numDatanodes, false);
    // Now wait for 15 seconds to give datanodes chance to register
    // themselves and to report heartbeat
    try {
      Thread.sleep(15000L);
    } catch (InterruptedException e) {
      // nothing
    }
    InetSocketAddress addr = new InetSocketAddress("localhost", 
                                             cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport();
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) fileSys;

    try {
      for (int iteration = 0; iteration < 2; iteration++) {
        //
        // Decommission one node. Verify that node is decommissioned.
        // Verify that replication factor of file has increased from 3
        // to 4. This means one replica is on decommissioned node.
        // 
        Path file1 = new Path("smallblocktest.dat");
        writeFile(fileSys, file1, 3);
        checkFile(fileSys, file1, 3);
        String downnodes[] = decommissionNode(client, fileSys);
        waitNodeDecommission(client, fileSys, downnodes);
        checkFile(fileSys, file1, 3, downnodes);
        commissionNode(client, fileSys, downnodes);
        cleanupFile(fileSys, file1);
      }
    } catch (IOException e) {
      info = client.datanodeReport();
      printDatanodeReport(info);
      throw e;
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
