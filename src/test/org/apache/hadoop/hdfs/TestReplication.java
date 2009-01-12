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

import junit.framework.TestCase;
import java.io.*;
import java.util.Iterator;
import java.util.Random;
import java.net.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests the replication of a DFS file.
 */
public class TestReplication extends TestCase {
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 8192;
  private static final int fileSize = 16384;
  private static final String racks[] = new String[] {
    "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3"
  };
  private static final int numDatanodes = racks.length;
  private static final Log LOG = LogFactory.getLog(
                                       "org.apache.hadoop.hdfs.TestReplication");

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
  
  /* check if there are at least two nodes are on the same rack */
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    Configuration conf = fileSys.getConf();
    ClientProtocol namenode = DFSClient.createNamenode(conf);
      
    waitForBlockReplication(name.toString(), namenode, 
                            Math.min(numDatanodes, repl), -1);
    
    LocatedBlocks locations = namenode.getBlockLocations(name.toString(),0,
                                                         Long.MAX_VALUE);
    boolean isOnSameRack = true, isNotOnSameRack = true;
    for (LocatedBlock blk : locations.getLocatedBlocks()) {
      DatanodeInfo[] datanodes = blk.getLocations();
      if (datanodes.length <= 1) break;
      if (datanodes.length == 2) {
        isNotOnSameRack = !(datanodes[0].getNetworkLocation().equals(
                                                                     datanodes[1].getNetworkLocation()));
        break;
      }
      isOnSameRack = false;
      isNotOnSameRack = false;
      for (int i = 0; i < datanodes.length-1; i++) {
        LOG.info("datanode "+ i + ": "+ datanodes[i].getName());
        boolean onRack = false;
        for( int j=i+1; j<datanodes.length; j++) {
           if( datanodes[i].getNetworkLocation().equals(
            datanodes[j].getNetworkLocation()) ) {
             onRack = true;
           }
        }
        if (onRack) {
          isOnSameRack = true;
        }
        if (!onRack) {
          isNotOnSameRack = true;                      
        }
        if (isOnSameRack && isNotOnSameRack) break;
      }
      if (!isOnSameRack || !isNotOnSameRack) break;
    }
    assertTrue(isOnSameRack);
    assertTrue(isNotOnSameRack);
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /* 
   * Test if Datanode reports bad blocks during replication request
   */
  public void testBadBlockReportOnTransfer() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dfsClient = new DFSClient(new InetSocketAddress("localhost",
                              cluster.getNameNodePort()), conf);
  
    // Create file with replication factor of 1
    Path file1 = new Path("/tmp/testBadBlockReportOnTransfer/file1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)1, 0);
    DFSTestUtil.waitReplication(fs, file1, (short)1);
  
    // Corrupt the block belonging to the created file
    String block = DFSTestUtil.getFirstBlock(fs, file1).getBlockName();
    cluster.corruptBlockOnDataNodes(block);
  
    // Increase replication factor, this should invoke transfer request
    // Receiving datanode fails on checksum and reports it to namenode
    fs.setReplication(file1, (short)2);
  
    // Now get block details and check if the block is corrupt
    blocks = dfsClient.namenode.
              getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    while (blocks.get(0).isCorrupt() != true) {
      try {
        LOG.info("Waiting until block is marked as corrupt...");
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      }
      blocks = dfsClient.namenode.
                getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    }
    replicaCount = blocks.get(0).getLocations().length;
    assertTrue(replicaCount == 1);
    cluster.shutdown();
  }
  
  /**
   * Tests replication in DFS.
   */
  public void runReplication(boolean simulated) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.replication.considerLoad", false);
    if (simulated) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, racks);
    cluster.waitActive();
    
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path file1 = new Path("/smallblocktest.dat");
      writeFile(fileSys, file1, 3);
      checkFile(fileSys, file1, 3);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 10);
      checkFile(fileSys, file1, 10);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 4);
      checkFile(fileSys, file1, 4);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 1);
      checkFile(fileSys, file1, 1);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 2);
      checkFile(fileSys, file1, 2);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }


  public void testReplicationSimulatedStorag() throws IOException {
    runReplication(true);
  }
  
  
  public void testReplication() throws IOException {
    runReplication(false);
  }
  
  // Waits for all of the blocks to have expected replication
  private void waitForBlockReplication(String filename, 
                                       ClientProtocol namenode,
                                       int expected, long maxWaitSec) 
                                       throws IOException {
    long start = System.currentTimeMillis();
    
    //wait for all the blocks to be replicated;
    LOG.info("Checking for block replication for " + filename);
    int iters = 0;
    while (true) {
      boolean replOk = true;
      LocatedBlocks blocks = namenode.getBlockLocations(filename, 0, 
                                                        Long.MAX_VALUE);
      
      for (Iterator<LocatedBlock> iter = blocks.getLocatedBlocks().iterator();
           iter.hasNext();) {
        LocatedBlock block = iter.next();
        int actual = block.getLocations().length;
        if ( actual < expected ) {
          if (true || iters > 0) {
            LOG.info("Not enough replicas for " + block.getBlock() +
                               " yet. Expecting " + expected + ", got " + 
                               actual + ".");
          }
          replOk = false;
          break;
        }
      }
      
      if (replOk) {
        return;
      }
      
      iters++;
      
      if (maxWaitSec > 0 && 
          (System.currentTimeMillis() - start) > (maxWaitSec * 1000)) {
        throw new IOException("Timedout while waiting for all blocks to " +
                              " be replicated for " + filename);
      }
      
      try {
        Thread.sleep(500);
      } catch (InterruptedException ignored) {}
    }
  }
  
  /* This test makes sure that NameNode retries all the available blocks 
   * for under replicated blocks. 
   * 
   * It creates a file with one block and replication of 4. It corrupts 
   * two of the blocks and removes one of the replicas. Expected behaviour is
   * that missing replica will be copied from one valid source.
   */
  public void testPendingReplicationRetry() throws IOException {
    
    MiniDFSCluster cluster = null;
    int numDataNodes = 4;
    String testFile = "/replication-test-file";
    Path testPath = new Path(testFile);
    
    byte buffer[] = new byte[1024];
    for (int i=0; i<buffer.length; i++) {
      buffer[i] = '1';
    }
    
    try {
      Configuration conf = new Configuration();
      conf.set("dfs.replication", Integer.toString(numDataNodes));
      //first time format
      cluster = new MiniDFSCluster(0, conf, numDataNodes, true,
                                   true, null, null);
      cluster.waitActive();
      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                            cluster.getNameNodePort()),
                                            conf);
      
      OutputStream out = cluster.getFileSystem().create(testPath);
      out.write(buffer);
      out.close();
      
      waitForBlockReplication(testFile, dfsClient.namenode, numDataNodes, -1);

      // get first block of the file.
      String block = dfsClient.namenode.
                       getBlockLocations(testFile, 0, Long.MAX_VALUE).
                       get(0).getBlock().getBlockName();
      
      cluster.shutdown();
      cluster = null;
      
      //Now mess up some of the replicas.
      //Delete the first and corrupt the next two.
      File baseDir = new File(System.getProperty("test.build.data"), 
                                                 "dfs/data");
      for (int i=0; i<25; i++) {
        buffer[i] = '0';
      }
      
      int fileCount = 0;
      for (int i=0; i<6; i++) {
        File blockFile = new File(baseDir, "data" + (i+1) + "/current/" + block);
        LOG.info("Checking for file " + blockFile);
        
        if (blockFile.exists()) {
          if (fileCount == 0) {
            LOG.info("Deleting file " + blockFile);
            assertTrue(blockFile.delete());
          } else {
            // corrupt it.
            LOG.info("Corrupting file " + blockFile);
            long len = blockFile.length();
            assertTrue(len > 50);
            RandomAccessFile blockOut = new RandomAccessFile(blockFile, "rw");
            try {
              blockOut.seek(len/3);
              blockOut.write(buffer, 0, 25);
            } finally {
              blockOut.close();
            }
          }
          fileCount++;
        }
      }
      assertEquals(3, fileCount);
      
      /* Start the MiniDFSCluster with more datanodes since once a writeBlock
       * to a datanode node fails, same block can not be written to it
       * immediately. In our case some replication attempts will fail.
       */
      
      LOG.info("Restarting minicluster after deleting a replica and corrupting 2 crcs");
      conf = new Configuration();
      conf.set("dfs.replication", Integer.toString(numDataNodes));
      conf.set("dfs.replication.pending.timeout.sec", Integer.toString(2));
      conf.set("dfs.datanode.block.write.timeout.sec", Integer.toString(5));
      conf.set("dfs.safemode.threshold.pct", "0.75f"); // only 3 copies exist
      
      cluster = new MiniDFSCluster(0, conf, numDataNodes*2, false,
                                   true, null, null);
      cluster.waitActive();
      
      dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                  cluster.getNameNodePort()),
                                  conf);
      
      waitForBlockReplication(testFile, dfsClient.namenode, numDataNodes, -1);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }  
  
}
