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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.util.Time;
import org.junit.Test;

/**
 * This class tests the replication of a DFS file.
 */
public class TestReplication {
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
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
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
    ClientProtocol namenode = NameNodeProxies.createProxy(conf, fileSys.getUri(),
        ClientProtocol.class).getProxy();
      
    waitForBlockReplication(name.toString(), namenode, 
                            Math.min(numDatanodes, repl), -1);
    
    LocatedBlocks locations = namenode.getBlockLocations(name.toString(),0,
                                                         Long.MAX_VALUE);
    FileStatus stat = fileSys.getFileStatus(name);
    BlockLocation[] blockLocations = fileSys.getFileBlockLocations(stat,0L,
                                                         Long.MAX_VALUE);
    // verify that rack locations match
    assertTrue(blockLocations.length == locations.locatedBlockCount());
    for (int i = 0; i < blockLocations.length; i++) {
      LocatedBlock blk = locations.get(i);
      DatanodeInfo[] datanodes = blk.getLocations();
      String[] topologyPaths = blockLocations[i].getTopologyPaths();
      assertTrue(topologyPaths.length == datanodes.length);
      for (int j = 0; j < topologyPaths.length; j++) {
        boolean found = false;
        for (int k = 0; k < racks.length; k++) {
          if (topologyPaths[j].startsWith(racks[k])) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }
    }

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
        LOG.info("datanode "+ i + ": "+ datanodes[i]);
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

  private void testBadBlockReportOnTransfer(
      boolean corruptBlockByDeletingBlockFile) throws Exception {
    Configuration conf = new HdfsConfiguration();
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;
    short replFactor = 1;
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dfsClient = new DFSClient(new InetSocketAddress("localhost",
                              cluster.getNameNodePort()), conf);
  
    // Create file with replication factor of 1
    Path file1 = new Path("/tmp/testBadBlockReportOnTransfer/file1");
    DFSTestUtil.createFile(fs, file1, 1024, replFactor, 0);
    DFSTestUtil.waitReplication(fs, file1, replFactor);
  
    // Corrupt the block belonging to the created file
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file1);

    int blockFilesCorrupted =
        corruptBlockByDeletingBlockFile?
            cluster.corruptBlockOnDataNodesByDeletingBlockFile(block) :
              cluster.corruptBlockOnDataNodes(block);       

    assertEquals("Corrupted too few blocks", replFactor, blockFilesCorrupted); 

    // Increase replication factor, this should invoke transfer request
    // Receiving datanode fails on checksum and reports it to namenode
    replFactor = 2;
    fs.setReplication(file1, replFactor);
  
    // Now get block details and check if the block is corrupt
    blocks = dfsClient.getNamenode().
              getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    while (blocks.get(0).isCorrupt() != true) {
      try {
        LOG.info("Waiting until block is marked as corrupt...");
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      }
      blocks = dfsClient.getNamenode().
                getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    }
    replicaCount = blocks.get(0).getLocations().length;
    assertTrue(replicaCount == 1);
    cluster.shutdown();
  }

  /* 
   * Test if Datanode reports bad blocks during replication request
   */
  @Test
  public void testBadBlockReportOnTransfer() throws Exception {
    testBadBlockReportOnTransfer(false);
  }

  /* 
   * Test if Datanode reports bad blocks during replication request
   * with missing block file
   */
  @Test
  public void testBadBlockReportOnTransferMissingBlockFile() throws Exception {
    testBadBlockReportOnTransfer(true);
  }

  /**
   * Tests replication in DFS.
   */
  public void runReplication(boolean simulated) throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);
    if (simulated) {
      SimulatedFSDataset.setFactory(conf);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .racks(racks).build();
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


  @Test
  public void testReplicationSimulatedStorag() throws IOException {
    runReplication(true);
  }
  
  
  @Test
  public void testReplication() throws IOException {
    runReplication(false);
  }
  
  // Waits for all of the blocks to have expected replication
  private void waitForBlockReplication(String filename, 
                                       ClientProtocol namenode,
                                       int expected, long maxWaitSec) 
                                       throws IOException {
    long start = Time.now();
    
    //wait for all the blocks to be replicated;
    LOG.info("Checking for block replication for " + filename);
    while (true) {
      boolean replOk = true;
      LocatedBlocks blocks = namenode.getBlockLocations(filename, 0, 
                                                        Long.MAX_VALUE);
      
      for (Iterator<LocatedBlock> iter = blocks.getLocatedBlocks().iterator();
           iter.hasNext();) {
        LocatedBlock block = iter.next();
        int actual = block.getLocations().length;
        if ( actual < expected ) {
          LOG.info("Not enough replicas for " + block.getBlock()
              + " yet. Expecting " + expected + ", got " + actual + ".");
          replOk = false;
          break;
        }
      }
      
      if (replOk) {
        return;
      }
      
      if (maxWaitSec > 0 && 
          (Time.now() - start) > (maxWaitSec * 1000)) {
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
   * two of the blocks and removes one of the replicas. Expected behavior is
   * that missing replica will be copied from one valid source.
   */
  @Test
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
      Configuration conf = new HdfsConfiguration();
      conf.set(DFSConfigKeys.DFS_REPLICATION_KEY, Integer.toString(numDataNodes));
      //first time format
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                            cluster.getNameNodePort()),
                                            conf);
      
      OutputStream out = cluster.getFileSystem().create(testPath);
      out.write(buffer);
      out.close();
      
      waitForBlockReplication(testFile, dfsClient.getNamenode(), numDataNodes, -1);

      // get first block of the file.
      ExtendedBlock block = dfsClient.getNamenode().getBlockLocations(testFile,
          0, Long.MAX_VALUE).get(0).getBlock();
      
      cluster.shutdown();
      cluster = null;
      
      for (int i=0; i<25; i++) {
        buffer[i] = '0';
      }
      
      int fileCount = 0;
      // Choose 3 copies of block file - delete 1 and corrupt the remaining 2
      for (int dnIndex=0; dnIndex<3; dnIndex++) {
        File blockFile = MiniDFSCluster.getBlockFile(dnIndex, block);
        LOG.info("Checking for file " + blockFile);
        
        if (blockFile != null && blockFile.exists()) {
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
      conf = new HdfsConfiguration();
      conf.set(DFSConfigKeys.DFS_REPLICATION_KEY, Integer.toString(numDataNodes));
      conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, Integer.toString(2));
      conf.set("dfs.datanode.block.write.timeout.sec", Integer.toString(5));
      conf.set(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, "0.75f"); // only 3 copies exist
      
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(numDataNodes * 2)
                                  .format(false)
                                  .build();
      cluster.waitActive();
      
      dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                  cluster.getNameNodePort()),
                                  conf);
      
      waitForBlockReplication(testFile, dfsClient.getNamenode(), numDataNodes, -1);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }  
  }
  
  /**
   * Test if replication can detect mismatched length on-disk blocks
   * @throws Exception
   */
  @Test
  public void testReplicateLenMismatchedBlock() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).numDataNodes(2).build();
    try {
      cluster.waitActive();
      // test truncated block
      changeBlockLen(cluster, -1);
      // test extended block
      changeBlockLen(cluster, 1);
    } finally {
      cluster.shutdown();
    }
  }
  
  private void changeBlockLen(MiniDFSCluster cluster, int lenDelta)
      throws IOException, InterruptedException, TimeoutException {
    final Path fileName = new Path("/file1");
    final short REPLICATION_FACTOR = (short)1;
    final FileSystem fs = cluster.getFileSystem();
    final int fileLen = fs.getConf().getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    DFSTestUtil.createFile(fs, fileName, fileLen, REPLICATION_FACTOR, 0);
    DFSTestUtil.waitReplication(fs, fileName, REPLICATION_FACTOR);

    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);

    // Change the length of a replica
    for (int i=0; i<cluster.getDataNodes().size(); i++) {
      if (TestDatanodeBlockScanner.changeReplicaLength(block, i, lenDelta)) {
        break;
      }
    }

    // increase the file's replication factor
    fs.setReplication(fileName, (short)(REPLICATION_FACTOR+1));

    // block replication triggers corrupt block detection
    DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost", 
        cluster.getNameNodePort()), fs.getConf());
    LocatedBlocks blocks = dfsClient.getNamenode().getBlockLocations(
        fileName.toString(), 0, fileLen);
    if (lenDelta < 0) { // replica truncated
    	while (!blocks.get(0).isCorrupt() || 
    			REPLICATION_FACTOR != blocks.get(0).getLocations().length) {
    		Thread.sleep(100);
    		blocks = dfsClient.getNamenode().getBlockLocations(
    				fileName.toString(), 0, fileLen);
    	}
    } else { // no corruption detected; block replicated
    	while (REPLICATION_FACTOR+1 != blocks.get(0).getLocations().length) {
    		Thread.sleep(100);
    		blocks = dfsClient.getNamenode().getBlockLocations(
    				fileName.toString(), 0, fileLen);
    	}
    }
    fs.delete(fileName, true);
  }

  /**
   * Test that blocks should get replicated if we have corrupted blocks and
   * having good replicas at least equal or greater to minreplication
   *
   * Simulate rbw blocks by creating dummy copies, then a DN restart to detect
   * those corrupted blocks asap.
   */
  @Test(timeout=30000)
  public void testReplicationWhenBlockCorruption() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(
          DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 1);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      FileSystem fs = cluster.getFileSystem();
      FSDataOutputStream create = fs.create(new Path("/test"));
      fs.setReplication(new Path("/test"), (short) 1);
      create.write(new byte[1024]);
      create.close();

      List<File> nonParticipatedNodeDirs = new ArrayList<File>();
      File participatedNodeDirs = null;
      for (int i = 0; i < cluster.getDataNodes().size(); i++) {
        File storageDir = cluster.getInstanceStorageDir(i, 0);
        String bpid = cluster.getNamesystem().getBlockPoolId();
        File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
        if (data_dir.listFiles().length == 0) {
          nonParticipatedNodeDirs.add(data_dir);
        } else {
          participatedNodeDirs = data_dir;
        }
      }

      String blockFile = null;
      File[] listFiles = participatedNodeDirs.listFiles();
      for (File file : listFiles) {
        if (file.getName().startsWith("blk_")
            && !file.getName().endsWith("meta")) {
          blockFile = file.getName();
          for (File file1 : nonParticipatedNodeDirs) {
            file1.mkdirs();
            new File(file1, blockFile).createNewFile();
            new File(file1, blockFile + "_1000.meta").createNewFile();
          }
          break;
        }
      }

      fs.setReplication(new Path("/test"), (short) 3);
      cluster.restartDataNodes(); // Lets detect all DNs about dummy copied
      // blocks
      cluster.waitActive();
      cluster.triggerBlockReports();
      DFSTestUtil.waitReplication(fs, new Path("/test"), (short) 3);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
