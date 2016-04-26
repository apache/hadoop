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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

/* File Append tests for HDFS-200 & HDFS-142, specifically focused on:
 *  using append()/sync() to recover block information
 */
public class TestFileAppend4 {
  static final Log LOG = LogFactory.getLog(TestFileAppend4.class);
  static final long BLOCK_SIZE = 1024;
  static final long BBW_SIZE = 500; // don't align on bytes/checksum

  static final Object [] NO_ARGS = new Object []{};

  Configuration conf;
  MiniDFSCluster cluster;
  Path file1;
  FSDataOutputStream stm;

  {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }

  @Before
  public void setUp() throws Exception {
    this.conf = new Configuration();

    // lower heartbeat interval for fast recognition of DN death
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);
    // handle under-replicated blocks quickly (for replication asserts)
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY, 5);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    
    // handle failures in the DFSClient pipeline quickly
    // (for cluster.shutdown(); fs.close() idiom)
    conf.setInt("ipc.client.connect.max.retries", 1);
  }
  
  /*
   * Recover file.
   * Try and open file in append mode.
   * Doing this, we get a hold of the file that crashed writer
   * was writing to.  Once we have it, close it.  This will
   * allow subsequent reader to see up to last sync.
   * NOTE: This is the same algorithm that HBase uses for file recovery
   * @param fs
   * @throws Exception
   */
  private void recoverFile(final FileSystem fs) throws Exception {
    LOG.info("Recovering File Lease");

    // set the soft limit to be 1 second so that the
    // namenode triggers lease recovery upon append request
    cluster.setLeasePeriod(1000, HdfsConstants.LEASE_HARDLIMIT_PERIOD);

    // Trying recovery
    int tries = 60;
    boolean recovered = false;
    FSDataOutputStream out = null;
    while (!recovered && tries-- > 0) {
      try {
        out = fs.append(file1);
        LOG.info("Successfully opened for append");
        recovered = true;
      } catch (IOException e) {
        LOG.info("Failed open for append, waiting on lease recovery");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          // ignore it and try again
        }
      }
    }
    if (out != null) {
      out.close();
    }
    if (!recovered) {
      fail("Recovery should take < 1 min");
    }
    LOG.info("Past out lease recovery");
  }
  
  /**
   * Test case that stops a writer after finalizing a block but
   * before calling completeFile, and then tries to recover
   * the lease from another thread.
   */
  @Test(timeout=60000)
  public void testRecoverFinalizedBlock() throws Throwable {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
 
    try {
      cluster.waitActive();
      NamenodeProtocols preSpyNN = cluster.getNameNodeRpc();
      NamenodeProtocols spyNN = spy(preSpyNN);
 
      // Delay completeFile
      GenericTestUtils.DelayAnswer delayer = new GenericTestUtils.DelayAnswer(LOG);
      doAnswer(delayer).when(spyNN).complete(
          anyString(), anyString(), (ExtendedBlock)anyObject(), anyLong());
 
      DFSClient client = new DFSClient(null, spyNN, conf, null);
      file1 = new Path("/testRecoverFinalized");
      final OutputStream stm = client.create("/testRecoverFinalized", true);
 
      // write 1/2 block
      AppendTestUtil.write(stm, 0, 4096);
      final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
      Thread t = new Thread() { 
          @Override
          public void run() {
            try {
              stm.close();
            } catch (Throwable t) {
              err.set(t);
            }
          }};
      t.start();
      LOG.info("Waiting for close to get to latch...");
      delayer.waitForCall();
 
      // At this point, the block is finalized on the DNs, but the file
      // has not been completed in the NN.
      // Lose the leases
      LOG.info("Killing lease checker");
      client.getLeaseRenewer().interruptAndJoin();
 
      FileSystem fs1 = cluster.getFileSystem();
      FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(
        fs1.getConf());
 
      LOG.info("Recovering file");
      recoverFile(fs2);
 
      LOG.info("Telling close to proceed.");
      delayer.proceed();
      LOG.info("Waiting for close to finish.");
      t.join();
      LOG.info("Close finished.");
 
      // We expect that close will get a "File is not open" error.
      Throwable thrownByClose = err.get();
      assertNotNull(thrownByClose);
      assertTrue(thrownByClose instanceof LeaseExpiredException);
      GenericTestUtils.assertExceptionContains("File is not open for writing",
          thrownByClose);
    } finally {
      cluster.shutdown();
    }
  }
 
  /**
   * Test case that stops a writer after finalizing a block but
   * before calling completeFile, recovers a file from another writer,
   * starts writing from that writer, and then has the old lease holder
   * call completeFile
   */
  @Test(timeout=60000)
  public void testCompleteOtherLeaseHoldersFile() throws Throwable {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
 
    try {
      cluster.waitActive();
      NamenodeProtocols preSpyNN = cluster.getNameNodeRpc();
      NamenodeProtocols spyNN = spy(preSpyNN);
 
      // Delay completeFile
      GenericTestUtils.DelayAnswer delayer =
        new GenericTestUtils.DelayAnswer(LOG);
      doAnswer(delayer).when(spyNN).complete(anyString(), anyString(),
          (ExtendedBlock) anyObject(), anyLong());
 
      DFSClient client = new DFSClient(null, spyNN, conf, null);
      file1 = new Path("/testCompleteOtherLease");
      final OutputStream stm = client.create("/testCompleteOtherLease", true);
 
      // write 1/2 block
      AppendTestUtil.write(stm, 0, 4096);
      final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
      Thread t = new Thread() { 
          @Override
          public void run() {
            try {
              stm.close();
            } catch (Throwable t) {
              err.set(t);
            }
          }};
      t.start();
      LOG.info("Waiting for close to get to latch...");
      delayer.waitForCall();
 
      // At this point, the block is finalized on the DNs, but the file
      // has not been completed in the NN.
      // Lose the leases
      LOG.info("Killing lease checker");
      client.getLeaseRenewer().interruptAndJoin();
 
      FileSystem fs1 = cluster.getFileSystem();
      FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(
        fs1.getConf());
 
      LOG.info("Recovering file");
      recoverFile(fs2);
 
      LOG.info("Opening file for append from new fs");
      FSDataOutputStream appenderStream = fs2.append(file1);
      
      LOG.info("Writing some data from new appender");
      AppendTestUtil.write(appenderStream, 0, 4096);
      
      LOG.info("Telling old close to proceed.");
      delayer.proceed();
      LOG.info("Waiting for close to finish.");
      t.join();
      LOG.info("Close finished.");
 
      // We expect that close will get a "Lease mismatch"
      // error.
      Throwable thrownByClose = err.get();
      assertNotNull(thrownByClose);
      assertTrue(thrownByClose instanceof LeaseExpiredException);
      GenericTestUtils.assertExceptionContains("not the lease owner",
          thrownByClose);
      
      // The appender should be able to close properly
      appenderStream.close();
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test the updation of NeededReplications for the Appended Block
   */
  @Test(timeout = 60000)
  public void testUpdateNeededReplicationsForAppendedFile() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    DistributedFileSystem fileSystem = null;
    try {
      // create a file.
      fileSystem = cluster.getFileSystem();
      Path f = new Path("/testAppend");
      FSDataOutputStream create = fileSystem.create(f, (short) 2);
      create.write("/testAppend".getBytes());
      create.close();

      // Append to the file.
      FSDataOutputStream append = fileSystem.append(f);
      append.write("/testAppend".getBytes());
      append.close();

      // Start a new datanode
      cluster.startDataNodes(conf, 1, true, null, null);

      // Check for replications
      DFSTestUtil.waitReplication(fileSystem, f, (short) 2);
    } finally {
      if (null != fileSystem) {
        fileSystem.close();
      }
      cluster.shutdown();
    }
  }

  /**
   * Test that an append with no locations fails with an exception
   * showing insufficient locations.
   */
  @Test(timeout = 60000)
  public void testAppendInsufficientLocations() throws Exception {
    Configuration conf = new Configuration();

    // lower heartbeat interval for fast recognition of DN
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 3000);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4)
        .build();
    DistributedFileSystem fileSystem = null;
    try {
      // create a file with replication 3
      fileSystem = cluster.getFileSystem();
      Path f = new Path("/testAppend");
      FSDataOutputStream create = fileSystem.create(f, (short) 2);
      create.write("/testAppend".getBytes());
      create.close();

      // Check for replications
      DFSTestUtil.waitReplication(fileSystem, f, (short) 2);

      // Shut down all DNs that have the last block location for the file
      LocatedBlocks lbs = fileSystem.dfs.getNamenode().
          getBlockLocations("/testAppend", 0, Long.MAX_VALUE);
      List<DataNode> dnsOfCluster = cluster.getDataNodes();
      DatanodeInfo[] dnsWithLocations = lbs.getLastLocatedBlock().
          getLocations();
      for( DataNode dn : dnsOfCluster) {
        for(DatanodeInfo loc: dnsWithLocations) {
          if(dn.getDatanodeId().equals(loc)){
            dn.shutdown();
            DFSTestUtil.waitForDatanodeDeath(dn);
          }
        }
      }

      // Wait till 0 replication is recognized
      DFSTestUtil.waitReplication(fileSystem, f, (short) 0);

      // Append to the file, at this state there are 3 live DNs but none of them
      // have the block.
      try{
        fileSystem.append(f);
        fail("Append should fail because insufficient locations");
      } catch (IOException e){
        LOG.info("Expected exception: ", e);
      }
      FSDirectory dir = cluster.getNamesystem().getFSDirectory();
      final INodeFile inode = INodeFile.
          valueOf(dir.getINode("/testAppend"), "/testAppend");
      assertTrue("File should remain closed", !inode.isUnderConstruction());
    } finally {
      if (null != fileSystem) {
        fileSystem.close();
      }
      cluster.shutdown();
    }
  }
}
