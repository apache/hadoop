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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Supplier;

/**
 * Test cases regarding pipeline recovery during NN failover.
 */
public class TestPipelinesFailover {
  static {
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(RetryInvocationHandler
            .class), org.slf4j.event.Level.DEBUG);
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }
  
  protected static final Logger LOG = LoggerFactory.getLogger(
      TestPipelinesFailover.class);
  private static final Path TEST_PATH =
    new Path("/test-file");
  private static final int BLOCK_SIZE = 4096;
  private static final int BLOCK_AND_A_HALF = BLOCK_SIZE * 3 / 2;
  
  private static final int STRESS_NUM_THREADS = 25;
  private static final int STRESS_RUNTIME = 40000;

  private static final int NN_COUNT = 3;
  private static final long FAILOVER_SEED = System.currentTimeMillis();
  private static final Random failoverRandom = new Random(FAILOVER_SEED);
  static{
    // log the failover seed so we can reproduce the test exactly
    LOG.info("Using random seed: " + FAILOVER_SEED
        + " for selecting active target NN during failover");
  }

  enum TestScenario {
    GRACEFUL_FAILOVER {
      @Override
      void run(MiniDFSCluster cluster, int previousActive, int activeIndex) throws IOException {
        cluster.transitionToStandby(previousActive);
        cluster.transitionToActive(activeIndex);
      }
    },
    ORIGINAL_ACTIVE_CRASHED {
      @Override
      void run(MiniDFSCluster cluster, int previousActive, int activeIndex) throws IOException {
        cluster.restartNameNode(previousActive);
        cluster.transitionToActive(activeIndex);
      }
    };

    abstract void run(MiniDFSCluster cluster, int previousActive, int activeIndex) throws IOException;
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
    // Don't check low redundancy periodically.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1000);
    
    FSDataOutputStream stm = null;
    MiniDFSCluster cluster = newMiniCluster(conf, 3);
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

      LOG.info("Failing over to another NN");
      int activeIndex = failover(cluster, scenario);

      // NOTE: explicitly do *not* make any further metadata calls
      // to the NN here. The next IPC call should be to allocate the next
      // block. Any other call would notice the failover and not test
      // idempotence of the operation (HDFS-3031)
      
      FSNamesystem ns1 = cluster.getNameNode(activeIndex).getNamesystem();
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
    MiniDFSCluster cluster = newMiniCluster(conf, 5);
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

      int nextActive = failover(cluster, scenario);

      assertTrue(fs.exists(TEST_PATH));
      
      cluster.stopDataNode(0);

      // write another block and a half
      AppendTestUtil.write(stm, BLOCK_AND_A_HALF, BLOCK_AND_A_HALF);
      stm.hflush();

      LOG.info("Failing back from NN " + nextActive + " to NN 0");
      cluster.transitionToStandby(nextActive);
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
    final MiniDFSCluster cluster = newMiniCluster(conf, 3);
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      cluster.setBlockRecoveryTimeout(TimeUnit.SECONDS.toMillis(1));
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
    final MiniDFSCluster cluster = newMiniCluster(conf, 3);
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
          InternalDataNodeTestUtils.spyOnBposToNN(primaryDN, nn0);

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
   * Create a MiniCluster with the specified base configuration and the specified number of
   * DataNodes. Helper method to ensure that the we use the same number of NNs across all the tests.
   * @return mini cluster ready to use
   * @throws IOException cluster cannot be started
   */
  private MiniDFSCluster newMiniCluster(Configuration conf, int dnCount) throws IOException {
    return new MiniDFSCluster.Builder(conf)
             .nnTopology(MiniDFSNNTopology.simpleHATopology(NN_COUNT))
             .numDataNodes(dnCount)
             .build();
  }

  /**
   * Stress test for pipeline/lease recovery. Starts a number of
   * threads, each of which creates a file and has another client
   * break the lease. While these threads run, failover proceeds
   * back and forth between two namenodes.
   */
  @Test(timeout=STRESS_RUNTIME*3)
  public void testPipelineRecoveryStress() throws Exception {

    // The following section of code is to help debug HDFS-6694 about
    // this test that fails from time to time due to "too many open files".
    //
    LOG.info("HDFS-6694 Debug Data BEGIN");

    String[][] scmds = new String[][] {
      {"/bin/sh", "-c", "ulimit -a"},
      {"hostname"},
      {"ifconfig", "-a"}
    };

    for (String[] scmd: scmds) {
      String scmd_str = StringUtils.join(" ", scmd);
      try {
        ShellCommandExecutor sce = new ShellCommandExecutor(scmd);
        sce.execute();
        LOG.info("'" + scmd_str + "' output:\n" + sce.getOutput());
      } catch (IOException e) {
        LOG.warn("Error when running '" + scmd_str + "'", e);
      }
    }

    LOG.info("HDFS-6694 Debug Data END");

    HAStressTestHarness harness = new HAStressTestHarness();
    // Disable permissions so that another user can recover the lease.
    harness.conf.setBoolean(
        DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    // This test triggers rapid NN failovers.  The client retry policy uses an
    // exponential backoff.  This can quickly lead to long sleep times and even
    // timeout the whole test.  Cap the sleep time at 1s to prevent this.
    harness.conf.setInt(HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY, 1000);

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
   * Fail-over using the given scenario, assuming NN0 is currently active
   * @param cluster cluster on which to run the scenario
   * @param scenario failure scenario to run
   * @return the index of the new active NN
   * @throws IOException
   */
  private int failover(MiniDFSCluster cluster, TestScenario scenario) throws IOException {
    return failover(cluster, scenario, 0);
  }

  /**
   * Do a fail-over with the given scenario.
   * @param cluster cluster on which to run the scenario
   * @param scenario failure scenario to run
   * @param activeIndex index of the currently active node
   * @throws IOException on failure
   * @return the index of the new active NN
   */
  private int failover(MiniDFSCluster cluster, TestScenario scenario, int activeIndex)
      throws IOException {
    // get index of the next node that should be active, ensuring its not the same as the currently
    // active node
    int nextActive = failoverRandom.nextInt(NN_COUNT);
    if (nextActive == activeIndex) {
      nextActive = (nextActive + 1) % NN_COUNT;
    }
    LOG.info("Failing over to a standby NN:" + nextActive + " from NN " + activeIndex);
    scenario.run(cluster, activeIndex, nextActive);
    return nextActive;
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
