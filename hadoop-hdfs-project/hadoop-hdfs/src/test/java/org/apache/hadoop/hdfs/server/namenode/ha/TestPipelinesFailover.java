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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Supplier;

/**
 * Test cases regarding pipeline recovery during NN failover.
 */
public class TestPipelinesFailover {
  static {
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(BlockManager.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(
        "org.apache.hadoop.io.retry.RetryInvocationHandler")).getLogger().setLevel(Level.ALL);

    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
  }
  
  protected static final Log LOG = LogFactory.getLog(
      TestPipelinesFailover.class);
  private static final Path TEST_PATH =
    new Path("/test-file");
  private static final int BLOCK_SIZE = 4096;
  private static final int BLOCK_AND_A_HALF = BLOCK_SIZE * 3 / 2;
  
  private static final int STRESS_NUM_THREADS = 25;
  private static final int STRESS_RUNTIME = 40000;
  
  enum TestScenario {
    GRACEFUL_FAILOVER {
      @Override
      void run(MiniDFSCluster cluster) throws IOException {
        cluster.transitionToStandby(0);
        cluster.transitionToActive(1);
      }
    },
    ORIGINAL_ACTIVE_CRASHED {
      @Override
      void run(MiniDFSCluster cluster) throws IOException {
        cluster.restartNameNode(0);
        cluster.transitionToActive(1);
      }
    };

    abstract void run(MiniDFSCluster cluster) throws IOException;
  }
  
  enum MethodToTestIdempotence {
    ALLOCATE_BLOCK,
    COMPLETE_FILE;
  }

  /**
   * Tests continuing a write pipeline over a failover.
   */
  @Test(timeout=30000)
  public void testWriteOverGracefulFailover() throws Exception {
    doWriteOverFailoverTest(TestScenario.GRACEFUL_FAILOVER,
        MethodToTestIdempotence.ALLOCATE_BLOCK);
  }
  
  @Test(timeout=30000)
  public void testAllocateBlockAfterCrashFailover() throws Exception {
    doWriteOverFailoverTest(TestScenario.ORIGINAL_ACTIVE_CRASHED,
        MethodToTestIdempotence.ALLOCATE_BLOCK);
  }

  @Test(timeout=30000)
  public void testCompleteFileAfterCrashFailover() throws Exception {
    doWriteOverFailoverTest(TestScenario.ORIGINAL_ACTIVE_CRASHED,
        MethodToTestIdempotence.COMPLETE_FILE);
  }
  
  private void doWriteOverFailoverTest(TestScenario scenario,
      MethodToTestIdempotence methodToTest) throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    // Don't check replication periodically.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1000);
    
    FSDataOutputStream stm = null;
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    try {
      int sizeWritten = 0;
      
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);

      LOG.info("Starting with NN 0 active");
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      stm = fs.create(TEST_PATH);
      
      // write a block and a half
      AppendTestUtil.write(stm, 0, BLOCK_AND_A_HALF);
      sizeWritten += BLOCK_AND_A_HALF;
      
      // Make sure all of the blocks are written out before failover.
      stm.hflush();

      LOG.info("Failing over to NN 1");
      scenario.run(cluster);

      // NOTE: explicitly do *not* make any further metadata calls
      // to the NN here. The next IPC call should be to allocate the next
      // block. Any other call would notice the failover and not test
      // idempotence of the operation (HDFS-3031)
      
      FSNamesystem ns1 = cluster.getNameNode(1).getNamesystem();
      BlockManagerTestUtil.updateState(ns1.getBlockManager());
      assertEquals(0, ns1.getPendingReplicationBlocks());
      assertEquals(0, ns1.getCorruptReplicaBlocks());
      assertEquals(0, ns1.getMissingBlocksCount());

      // If we're testing allocateBlock()'s idempotence, write another
      // block and a half, so we have to allocate a new block.
      // Otherise, don't write anything, so our next RPC will be
      // completeFile() if we're testing idempotence of that operation.
      if (methodToTest == MethodToTestIdempotence.ALLOCATE_BLOCK) {
        // write another block and a half
        AppendTestUtil.write(stm, sizeWritten, BLOCK_AND_A_HALF);
        sizeWritten += BLOCK_AND_A_HALF;
      }
      
      stm.close();
      stm = null;
      
      AppendTestUtil.check(fs, TEST_PATH, sizeWritten);
    } finally {
      IOUtils.closeStream(stm);
      cluster.shutdown();
    }
  }
  
  /**
   * Tests continuing a write pipeline over a failover when a DN fails
   * after the failover - ensures that updating the pipeline succeeds
   * even when the pipeline was constructed on a different NN.
   */
  @Test(timeout=30000)
  public void testWriteOverGracefulFailoverWithDnFail() throws Exception {
    doTestWriteOverFailoverWithDnFail(TestScenario.GRACEFUL_FAILOVER);
  }
  
  @Test(timeout=30000)
  public void testWriteOverCrashFailoverWithDnFail() throws Exception {
    doTestWriteOverFailoverWithDnFail(TestScenario.ORIGINAL_ACTIVE_CRASHED);
  }

  
  private void doTestWriteOverFailoverWithDnFail(TestScenario scenario)
      throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    
    FSDataOutputStream stm = null;
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(5)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);

      LOG.info("Starting with NN 0 active");
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      stm = fs.create(TEST_PATH);
      
      // write a block and a half
      AppendTestUtil.write(stm, 0, BLOCK_AND_A_HALF);
      
      // Make sure all the blocks are written before failover
      stm.hflush();

      LOG.info("Failing over to NN 1");
      scenario.run(cluster);

      assertTrue(fs.exists(TEST_PATH));
      
      cluster.stopDataNode(0);

      // write another block and a half
      AppendTestUtil.write(stm, BLOCK_AND_A_HALF, BLOCK_AND_A_HALF);
      stm.hflush();
      
      LOG.info("Failing back to NN 0");
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);
      
      cluster.stopDataNode(1);
      
      AppendTestUtil.write(stm, BLOCK_AND_A_HALF*2, BLOCK_AND_A_HALF);
      stm.hflush();
      
      
      stm.close();
      stm = null;
      
      AppendTestUtil.check(fs, TEST_PATH, BLOCK_AND_A_HALF * 3);
    } finally {
      IOUtils.closeStream(stm);
      cluster.shutdown();
    }
  }
  
  /**
   * Tests lease recovery if a client crashes. This approximates the
   * use case of HBase WALs being recovered after a NN failover.
   */
  @Test(timeout=30000)
  public void testLeaseRecoveryAfterFailover() throws Exception {
    final Configuration conf = new Configuration();
    // Disable permissions so that another user can recover the lease.
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    
    FSDataOutputStream stm = null;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);

      LOG.info("Starting with NN 0 active");
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      stm = fs.create(TEST_PATH);
      
      // write a block and a half
      AppendTestUtil.write(stm, 0, BLOCK_AND_A_HALF);
      stm.hflush();
      
      LOG.info("Failing over to NN 1");
      
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      
      assertTrue(fs.exists(TEST_PATH));

      FileSystem fsOtherUser = createFsAsOtherUser(cluster, conf);
      loopRecoverLease(fsOtherUser, TEST_PATH);
      
      AppendTestUtil.check(fs, TEST_PATH, BLOCK_AND_A_HALF);
      
      // Fail back to ensure that the block locations weren't lost on the
      // original node.
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);
      AppendTestUtil.check(fs, TEST_PATH, BLOCK_AND_A_HALF);      
    } finally {
      IOUtils.closeStream(stm);
      cluster.shutdown();
    }
  }

  /**
   * Test the scenario where the NN fails over after issuing a block
   * synchronization request, but before it is committed. The
   * DN running the recovery should then fail to commit the synchronization
   * and a later retry will succeed.
   */
  @Test(timeout=30000)
  public void testFailoverRightBeforeCommitSynchronization() throws Exception {
    final Configuration conf = new Configuration();
    // Disable permissions so that another user can recover the lease.
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    
    FSDataOutputStream stm = null;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);

      LOG.info("Starting with NN 0 active");
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      stm = fs.create(TEST_PATH);
      
      // write a half block
      AppendTestUtil.write(stm, 0, BLOCK_SIZE / 2);
      stm.hflush();
      
      // Look into the block manager on the active node for the block
      // under construction.
      
      NameNode nn0 = cluster.getNameNode(0);
      ExtendedBlock blk = DFSTestUtil.getFirstBlock(fs, TEST_PATH);
      DatanodeDescriptor expectedPrimary =
          DFSTestUtil.getExpectedPrimaryNode(nn0, blk);
      LOG.info("Expecting block recovery to be triggered on DN " +
          expectedPrimary);
      
      // Find the corresponding DN daemon, and spy on its connection to the
      // active.
      DataNode primaryDN = cluster.getDataNode(expectedPrimary.getIpcPort());
      DatanodeProtocolClientSideTranslatorPB nnSpy =
          DataNodeTestUtils.spyOnBposToNN(primaryDN, nn0);
      
      // Delay the commitBlockSynchronization call
      DelayAnswer delayer = new DelayAnswer(LOG);
      Mockito.doAnswer(delayer).when(nnSpy).commitBlockSynchronization(
          Mockito.eq(blk),
          Mockito.anyInt(), // new genstamp
          Mockito.anyLong(), // new length
          Mockito.eq(true), // close file
          Mockito.eq(false), // delete block
          (DatanodeID[]) Mockito.anyObject(), // new targets
          (String[]) Mockito.anyObject()); // new target storages

      DistributedFileSystem fsOtherUser = createFsAsOtherUser(cluster, conf);
      assertFalse(fsOtherUser.recoverLease(TEST_PATH));
      
      LOG.info("Waiting for commitBlockSynchronization call from primary");
      delayer.waitForCall();

      LOG.info("Failing over to NN 1");
      
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      
      // Let the commitBlockSynchronization call go through, and check that
      // it failed with the correct exception.
      delayer.proceed();
      delayer.waitForResult();
      Throwable t = delayer.getThrown();
      if (t == null) {
        fail("commitBlockSynchronization call did not fail on standby");
      }
      GenericTestUtils.assertExceptionContains(
          "Operation category WRITE is not supported",
          t);
      
      // Now, if we try again to recover the block, it should succeed on the new
      // active.
      loopRecoverLease(fsOtherUser, TEST_PATH);
      
      AppendTestUtil.check(fs, TEST_PATH, BLOCK_SIZE/2);
    } finally {
      IOUtils.closeStream(stm);
      cluster.shutdown();
    }
  }
  
  /**
   * Stress test for pipeline/lease recovery. Starts a number of
   * threads, each of which creates a file and has another client
   * break the lease. While these threads run, failover proceeds
   * back and forth between two namenodes.
   */
  @Test(timeout=STRESS_RUNTIME*3)
  public void testPipelineRecoveryStress() throws Exception {
    HAStressTestHarness harness = new HAStressTestHarness();
    // Disable permissions so that another user can recover the lease.
    harness.conf.setBoolean(
        DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    // This test triggers rapid NN failovers.  The client retry policy uses an
    // exponential backoff.  This can quickly lead to long sleep times and even
    // timeout the whole test.  Cap the sleep time at 1s to prevent this.
    harness.conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
      1000);

    final MiniDFSCluster cluster = harness.startCluster();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      FileSystem fs = harness.getFailoverFs();
      DistributedFileSystem fsAsOtherUser = createFsAsOtherUser(
          cluster, harness.conf);
      
      TestContext testers = new TestContext();
      for (int i = 0; i < STRESS_NUM_THREADS; i++) {
        Path p = new Path("/test-" + i);
        testers.addThread(new PipelineTestThread(
            testers, fs, fsAsOtherUser, p));
      }
      
      // Start a separate thread which will make sure that replication
      // happens quickly by triggering deletion reports and replication
      // work calculation frequently.
      harness.addReplicationTriggerThread(500);
      harness.addFailoverThread(5000);
      harness.startThreads();
      testers.startThreads();
      
      testers.waitFor(STRESS_RUNTIME);
      testers.stop();
      harness.stopThreads();
    } finally {
      System.err.println("===========================\n\n\n\n");
      harness.shutdown();
    }
  }

  /**
   * Test thread which creates a file, has another fake user recover
   * the lease on the file, and then ensures that the file's contents
   * are properly readable. If any of these steps fails, propagates
   * an exception back to the test context, causing the test case
   * to fail.
   */
  private static class PipelineTestThread extends RepeatingTestThread {
    private final FileSystem fs;
    private final FileSystem fsOtherUser;
    private final Path path;
    

    public PipelineTestThread(TestContext ctx,
        FileSystem fs, FileSystem fsOtherUser, Path p) {
      super(ctx);
      this.fs = fs;
      this.fsOtherUser = fsOtherUser;
      this.path = p;
    }

    @Override
    public void doAnAction() throws Exception {
      FSDataOutputStream stm = fs.create(path, true);
      try {
        AppendTestUtil.write(stm, 0, 100);
        stm.hflush();
        loopRecoverLease(fsOtherUser, path);
        AppendTestUtil.check(fs, path, 100);
      } finally {
        try {
          stm.close();
        } catch (IOException e) {
          // should expect this since we lost the lease
        }
      }
    }
    
    @Override
    public String toString() {
      return "Pipeline test thread for " + path;
    }
  }

  private DistributedFileSystem createFsAsOtherUser(
      final MiniDFSCluster cluster, final Configuration conf)
      throws IOException, InterruptedException {
    return (DistributedFileSystem) UserGroupInformation.createUserForTesting(
        "otheruser", new String[] { "othergroup"})
    .doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return HATestUtil.configureFailoverFs(
            cluster, conf);
      }
    });
  }
  
  /**
   * Try to recover the lease on the given file for up to 60 seconds.
   * @param fsOtherUser the filesystem to use for the recoverLease call
   * @param testPath the path on which to run lease recovery
   * @throws TimeoutException if lease recover does not succeed within 60
   * seconds
   * @throws InterruptedException if the thread is interrupted
   */
  private static void loopRecoverLease(
      final FileSystem fsOtherUser, final Path testPath)
      throws TimeoutException, InterruptedException {
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          boolean success;
          try {
            success = ((DistributedFileSystem)fsOtherUser)
              .recoverLease(testPath);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          if (!success) {
            LOG.info("Waiting to recover lease successfully");
          }
          return success;
        }
      }, 1000, 60000);
    } catch (TimeoutException e) {
      throw new TimeoutException("Timed out recovering lease for " +
          testPath);
    }
  }
}
