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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Supplier;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils.MaterializedReplica;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.mockito.Mockito;

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
      //writeFile(fileSys, file1, 3);
      DFSTestUtil.createFile(fileSys, file1, fileSize, fileSize, blockSize,
          (short) 3, seed);
      checkFile(fileSys, file1, 3);
      cleanupFile(fileSys, file1);
      DFSTestUtil.createFile(fileSys, file1, fileSize, fileSize, blockSize,
          (short) 10, seed);
      checkFile(fileSys, file1, 10);
      cleanupFile(fileSys, file1);
      DFSTestUtil.createFile(fileSys, file1, fileSize, fileSize, blockSize,
          (short) 4, seed);
      checkFile(fileSys, file1, 4);
      cleanupFile(fileSys, file1);
      DFSTestUtil.createFile(fileSys, file1, fileSize, fileSize, blockSize,
          (short) 1, seed);
      checkFile(fileSys, file1, 1);
      cleanupFile(fileSys, file1);
      DFSTestUtil.createFile(fileSys, file1, fileSize, fileSize, blockSize,
          (short) 2, seed);
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
    waitForBlockReplication(filename, namenode, expected, maxWaitSec, false, false);
  }

  private void waitForBlockReplication(String filename,
      ClientProtocol namenode,
      int expected, long maxWaitSec,
      boolean isUnderConstruction, boolean noOverReplication)
      throws IOException {
    long start = Time.monotonicNow();
    
    //wait for all the blocks to be replicated;
    LOG.info("Checking for block replication for " + filename);
    while (true) {
      boolean replOk = true;
      LocatedBlocks blocks = namenode.getBlockLocations(filename, 0, 
                                                        Long.MAX_VALUE);
      
      for (Iterator<LocatedBlock> iter = blocks.getLocatedBlocks().iterator();
           iter.hasNext();) {
        LocatedBlock block = iter.next();
        if (isUnderConstruction && !iter.hasNext()) {
          break; // do not check the last block
        }
        int actual = block.getLocations().length;
        if (noOverReplication) {
          assertTrue(actual <= expected);
        }
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
          (Time.monotonicNow() - start) > (maxWaitSec * 1000)) {
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

      List<MaterializedReplica> replicas = new ArrayList<>();
      for (int dnIndex=0; dnIndex<3; dnIndex++) {
        replicas.add(cluster.getMaterializedReplica(dnIndex, block));
      }
      assertEquals(3, replicas.size());

      cluster.shutdown();

      int fileCount = 0;
      // Choose 3 copies of block file - delete 1 and corrupt the remaining 2
      for (MaterializedReplica replica : replicas) {
        if (fileCount == 0) {
          LOG.info("Deleting block " + replica);
          replica.deleteData();
        } else {
          // corrupt it.
          LOG.info("Corrupting file " + replica);
          replica.corruptData();
        }
        fileCount++;
      }

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
      if (DFSTestUtil.changeReplicaLength(cluster, block, i, lenDelta)) {
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
      Path filePath = new Path("/test");
      FSDataOutputStream create = fs.create(filePath);
      fs.setReplication(filePath, (short) 1);
      create.write(new byte[1024]);
      create.close();

      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath);
      int numReplicaCreated = 0;
      for (final DataNode dn : cluster.getDataNodes()) {
        if (!dn.getFSDataset().contains(block)) {
          cluster.getFsDatasetTestUtils(dn).injectCorruptReplica(block);
          numReplicaCreated++;
        }
      }
      assertEquals(2, numReplicaCreated);

      fs.setReplication(filePath, (short) 3);
      cluster.restartDataNodes(); // Lets detect all DNs about dummy copied
      // blocks
      cluster.waitActive();
      cluster.triggerBlockReports();
      DFSTestUtil.waitReplication(fs, filePath, (short) 3);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  /**
   * This test makes sure that, when a file is closed before all
   * of the datanodes in the pipeline have reported their replicas,
   * the NameNode doesn't consider the block under-replicated too
   * aggressively. It is a regression test for HDFS-1172.
   */
  @Test(timeout=60000)
  public void testNoExtraReplicationWhenBlockReceivedIsLate()
      throws Exception {
    LOG.info("Test block replication when blockReceived is late" );
    final short numDataNodes = 3;
    final short replication = 3;
    final Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes).build();
    final String testFile = "/replication-test-file";
    final Path testPath = new Path(testFile);
    final BlockManager bm =
        cluster.getNameNode().getNamesystem().getBlockManager();

    try {
      cluster.waitActive();

      // Artificially delay IBR from 1 DataNode.
      // this ensures that the client's completeFile() RPC will get to the
      // NN before some of the replicas are reported.
      NameNode nn = cluster.getNameNode();
      DataNode dn = cluster.getDataNodes().get(0);
      DatanodeProtocolClientSideTranslatorPB spy =
          DataNodeTestUtils.spyOnBposToNN(dn, nn);
      DelayAnswer delayer = new GenericTestUtils.DelayAnswer(LOG);
      Mockito.doAnswer(delayer).when(spy).blockReceivedAndDeleted(
          Mockito.<DatanodeRegistration>anyObject(),
          Mockito.anyString(),
          Mockito.<StorageReceivedDeletedBlocks[]>anyObject());

      FileSystem fs = cluster.getFileSystem();
      // Create and close a small file with two blocks
      DFSTestUtil.createFile(fs, testPath, 1500, replication, 0);

      // schedule replication via BlockManager#computeReplicationWork
      BlockManagerTestUtil.computeAllPendingWork(bm);

      // Initially, should have some pending replication since the close()
      // is earlier than at lease one of the reportReceivedDeletedBlocks calls
      assertTrue(pendingReplicationCount(bm) > 0);

      // release pending IBR.
      delayer.waitForCall();
      delayer.proceed();
      delayer.waitForResult();

      // make sure DataNodes do replication work if exists
      for (DataNode d : cluster.getDataNodes()) {
        DataNodeTestUtils.triggerHeartbeat(d);
      }

      // Wait until there is nothing pending
      try {
        GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return pendingReplicationCount(bm) == 0;
          }
        }, 100, 3000);
      } catch (TimeoutException e) {
        fail("timed out while waiting for no pending replication.");
      }

      // Check that none of the datanodes have serviced a replication request.
      // i.e. that the NameNode didn't schedule any spurious replication.
      assertNoReplicationWasPerformed(cluster);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * This test makes sure that, if a file is under construction, blocks
   * in the middle of that file are properly re-replicated if they
   * become corrupt.
   */
  @Test(timeout=60000)
  public void testReplicationWhileUnderConstruction()
      throws Exception {
    LOG.info("Test block replication in under construction" );
    MiniDFSCluster cluster = null;
    final short numDataNodes = 6;
    final short replication = 3;
    String testFile = "/replication-test-file";
    Path testPath = new Path(testFile);
    FSDataOutputStream stm = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();

      FileSystem fs = cluster.getFileSystem();

      stm = AppendTestUtil.createFile(fs, testPath, replication);

      // Write a full block
      byte[] buffer = AppendTestUtil.initBuffer(AppendTestUtil.BLOCK_SIZE);
      stm.write(buffer); // block 1
      stm.write(buffer); // block 2
      stm.write(buffer, 0, 1); // start block 3
      stm.hflush(); // make sure blocks are persisted, etc

      // Everything should be fully replicated
      waitForBlockReplication(testFile, cluster.getNameNodeRpc(), replication, 30000, true, true);

      // Check that none of the datanodes have serviced a replication request.
      // i.e. that the NameNode didn't schedule any spurious replication.
      assertNoReplicationWasPerformed(cluster);

      // Mark one the blocks corrupt
      List<LocatedBlock> blocks;
      FSDataInputStream in = fs.open(testPath);
      try {
        blocks = DFSTestUtil.getAllBlocks(in);
      } finally {
        in.close();
      }
      LocatedBlock lb = blocks.get(0);
      LocatedBlock lbOneReplica = new LocatedBlock(lb.getBlock(),
          new DatanodeInfo[] { lb.getLocations()[0] });
      cluster.getNameNodeRpc().reportBadBlocks(
          new LocatedBlock[] { lbOneReplica });

      // Everything should be fully replicated
      waitForBlockReplication(testFile, cluster.getNameNodeRpc(), replication, 30000, true, true);
    } finally {
      if (stm != null) {
        IOUtils.closeStream(stm);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private long pendingReplicationCount(BlockManager bm) {
    BlockManagerTestUtil.updateState(bm);
    return bm.getPendingReplicationBlocksCount();
  }

  private void assertNoReplicationWasPerformed(MiniDFSCluster cluster) {
    for (DataNode dn : cluster.getDataNodes()) {
      MetricsRecordBuilder rb = getMetrics(dn.getMetrics().name());
      assertCounter("BlocksReplicated", 0L, rb);
    }
  }
}
