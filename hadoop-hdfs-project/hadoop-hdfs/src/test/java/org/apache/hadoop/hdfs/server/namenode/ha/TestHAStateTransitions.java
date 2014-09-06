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

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.*;

/**
 * Tests state transition from active->standby, and manual failover
 * and failback between two namenodes.
 */
public class TestHAStateTransitions {
  protected static final Log LOG = LogFactory.getLog(
      TestStandbyIsHot.class);
  private static final Path TEST_DIR = new Path("/test");
  private static final Path TEST_FILE_PATH = new Path(TEST_DIR, "foo");
  private static final String TEST_FILE_STR = TEST_FILE_PATH.toUri().getPath();
  private static final String TEST_FILE_DATA =
    "Hello state transitioning world";
  private static final StateChangeRequestInfo REQ_INFO = new StateChangeRequestInfo(
      RequestSource.REQUEST_BY_USER_FORCED);
  
  static {
    ((Log4JLogger)EditLogTailer.LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Test which takes a single node and flip flops between
   * active and standby mode, making sure it doesn't
   * double-play any edits.
   */
  @Test(timeout = 300000)
  public void testTransitionActiveToStandby() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(1)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      FileSystem fs = cluster.getFileSystem(0);
      
      fs.mkdirs(TEST_DIR);
      cluster.transitionToStandby(0);
      try {
        fs.mkdirs(new Path("/x"));
        fail("Didn't throw trying to mutate FS in standby state");
      } catch (Throwable t) {
        GenericTestUtils.assertExceptionContains(
            "Operation category WRITE is not supported", t);
      }
      cluster.transitionToActive(0);
      
      // Create a file, then delete the whole directory recursively.
      DFSTestUtil.createFile(fs, new Path(TEST_DIR, "foo"),
          10, (short)1, 1L);
      fs.delete(TEST_DIR, true);
      
      // Now if the standby tries to replay the last segment that it just
      // wrote as active, it would fail since it's trying to create a file
      // in a non-existent directory.
      cluster.transitionToStandby(0);
      cluster.transitionToActive(0);
      
      assertFalse(fs.exists(TEST_DIR));

    } finally {
      cluster.shutdown();
    }
  }

  private void addCrmThreads(MiniDFSCluster cluster,
      LinkedList<Thread> crmThreads) {
    for (int nn = 0; nn <= 1; nn++) {
      Thread thread = cluster.getNameNode(nn).getNamesystem().
          getCacheManager().getCacheReplicationMonitor();
      if (thread != null) {
        crmThreads.add(thread);
      }
    }
  }

  /**
   * Test that transitioning a service to the state that it is already
   * in is a nop, specifically, an exception is not thrown.
   */
  @Test(timeout = 300000)
  public void testTransitionToCurrentStateIsANop() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS, 1L);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(1)
      .build();
    LinkedList<Thread> crmThreads = new LinkedList<Thread>();
    try {
      cluster.waitActive();
      addCrmThreads(cluster, crmThreads);
      cluster.transitionToActive(0);
      addCrmThreads(cluster, crmThreads);
      cluster.transitionToActive(0);
      addCrmThreads(cluster, crmThreads);
      cluster.transitionToStandby(0);
      addCrmThreads(cluster, crmThreads);
      cluster.transitionToStandby(0);
      addCrmThreads(cluster, crmThreads);
    } finally {
      cluster.shutdown();
    }
    // Verify that all cacheReplicationMonitor threads shut down
    for (Thread thread : crmThreads) {
      Uninterruptibles.joinUninterruptibly(thread);
    }
  }

  /**
   * Test manual failover failback for one namespace
   * @param cluster single process test cluster
   * @param conf cluster configuration
   * @param nsIndex namespace index starting from zero
   * @throws Exception
   */
  private void testManualFailoverFailback(MiniDFSCluster cluster, 
		  Configuration conf, int nsIndex) throws Exception {
      int nn0 = 2 * nsIndex, nn1 = 2 * nsIndex + 1;

      cluster.transitionToActive(nn0);
      
      LOG.info("Starting with NN 0 active in namespace " + nsIndex);
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      fs.mkdirs(TEST_DIR);

      LOG.info("Failing over to NN 1 in namespace " + nsIndex);
      cluster.transitionToStandby(nn0);
      cluster.transitionToActive(nn1);
      assertTrue(fs.exists(TEST_DIR));
      DFSTestUtil.writeFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);

      LOG.info("Failing over to NN 0 in namespace " + nsIndex);
      cluster.transitionToStandby(nn1);
      cluster.transitionToActive(nn0);
      assertTrue(fs.exists(TEST_DIR));
      assertEquals(TEST_FILE_DATA, 
          DFSTestUtil.readFile(fs, TEST_FILE_PATH));

      LOG.info("Removing test file");
      fs.delete(TEST_DIR, true);
      assertFalse(fs.exists(TEST_DIR));

      LOG.info("Failing over to NN 1 in namespace " + nsIndex);
      cluster.transitionToStandby(nn0);
      cluster.transitionToActive(nn1);
      assertFalse(fs.exists(TEST_DIR));
  }
  
  /**
   * Tests manual failover back and forth between two NameNodes.
   */
  @Test(timeout = 300000)
  public void testManualFailoverAndFailback() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(1)
      .build();
    try {
      cluster.waitActive();
      // test the only namespace
      testManualFailoverFailback(cluster, conf, 0);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Regression test for HDFS-2693: when doing state transitions, we need to
   * lock the FSNamesystem so that we don't end up doing any writes while it's
   * "in between" states.
   * This test case starts up several client threads which do mutation operations
   * while flipping a NN back and forth from active to standby.
   */
  @Test(timeout=120000)
  public void testTransitionSynchronization() throws Exception {
    Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    try {
      cluster.waitActive();
      ReentrantReadWriteLock spyLock = NameNodeAdapter.spyOnFsLock(
          cluster.getNameNode(0).getNamesystem());
      Mockito.doAnswer(new GenericTestUtils.SleepAnswer(50))
        .when(spyLock).writeLock();
      
      final FileSystem fs = HATestUtil.configureFailoverFs(
          cluster, conf);
      
      TestContext ctx = new TestContext();
      for (int i = 0; i < 50; i++) {
        final int finalI = i;
        ctx.addThread(new RepeatingTestThread(ctx) {
          @Override
          public void doAnAction() throws Exception {
            Path p = new Path("/test-" + finalI);
            fs.mkdirs(p);
            fs.delete(p, true);
          }
        });
      }
      
      ctx.addThread(new RepeatingTestThread(ctx) {
        @Override
        public void doAnAction() throws Exception {
          cluster.transitionToStandby(0);
          Thread.sleep(50);
          cluster.transitionToActive(0);
        }
      });
      ctx.startThreads();
      ctx.waitFor(20000);
      ctx.stop();
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Test for HDFS-2812. Since lease renewals go from the client
   * only to the active NN, the SBN will have out-of-date lease
   * info when it becomes active. We need to make sure we don't
   * accidentally mark the leases as expired when the failover
   * proceeds.
   */
  @Test(timeout=120000)
  public void testLeasesRenewedOnTransition() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(1)
      .build();
    FSDataOutputStream stm = null;
    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    NameNode nn0 = cluster.getNameNode(0);
    NameNode nn1 = cluster.getNameNode(1);

    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      LOG.info("Starting with NN 0 active");

      stm = fs.create(TEST_FILE_PATH);
      long nn0t0 = NameNodeAdapter.getLeaseRenewalTime(nn0, TEST_FILE_STR);
      assertTrue(nn0t0 > 0);
      long nn1t0 = NameNodeAdapter.getLeaseRenewalTime(nn1, TEST_FILE_STR);
      assertEquals("Lease should not yet exist on nn1",
          -1, nn1t0);
      
      Thread.sleep(5); // make sure time advances!

      HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
      long nn1t1 = NameNodeAdapter.getLeaseRenewalTime(nn1, TEST_FILE_STR);
      assertTrue("Lease should have been created on standby. Time was: " +
          nn1t1, nn1t1 > nn0t0);
          
      Thread.sleep(5); // make sure time advances!
      
      LOG.info("Failing over to NN 1");
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      long nn1t2 = NameNodeAdapter.getLeaseRenewalTime(nn1, TEST_FILE_STR);
      assertTrue("Lease should have been renewed by failover process",
          nn1t2 > nn1t1);
    } finally {
      IOUtils.closeStream(stm);
      cluster.shutdown();
    }
  }
  
  /**
   * Test that delegation tokens continue to work after the failover.
   */
  @Test(timeout = 300000)
  public void testDelegationTokensAfterFailover() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);

      String renewer = UserGroupInformation.getLoginUser().getUserName();
      Token<DelegationTokenIdentifier> token = nn1.getRpcServer()
          .getDelegationToken(new Text(renewer));

      LOG.info("Failing over to NN 1");
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);

      nn2.getRpcServer().renewDelegationToken(token);
      nn2.getRpcServer().cancelDelegationToken(token);
      token = nn2.getRpcServer().getDelegationToken(new Text(renewer));
      Assert.assertTrue(token != null);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Tests manual failover back and forth between two NameNodes
   * for federation cluster with two namespaces.
   */
  @Test(timeout = 300000)
  public void testManualFailoverFailbackFederationHA() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(2))
      .numDataNodes(1)
      .build();
    try {
      cluster.waitActive();
   
      // test for namespace 0
      testManualFailoverFailback(cluster, conf, 0);
      
      // test for namespace 1
      testManualFailoverFailback(cluster, conf, 1); 
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testFailoverWithEmptyInProgressEditLog() throws Exception {
    testFailoverAfterCrashDuringLogRoll(false);
  }

  @Test(timeout = 300000)
  public void testFailoverWithEmptyInProgressEditLogWithHeader()
      throws Exception {
    testFailoverAfterCrashDuringLogRoll(true);
  }
  
  private static void testFailoverAfterCrashDuringLogRoll(boolean writeHeader)
      throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, Integer.MAX_VALUE);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    try {
      cluster.transitionToActive(0);
      NameNode nn0 = cluster.getNameNode(0);
      nn0.getRpcServer().rollEditLog();
      cluster.shutdownNameNode(0);
      createEmptyInProgressEditLog(cluster, nn0, writeHeader);
      cluster.transitionToActive(1);
    } finally {
      IOUtils.cleanup(LOG, fs);
      cluster.shutdown();
    }
  }
  
  private static void createEmptyInProgressEditLog(MiniDFSCluster cluster,
      NameNode nn, boolean writeHeader) throws IOException {
    long txid = nn.getNamesystem().getEditLog().getLastWrittenTxId();
    URI sharedEditsUri = cluster.getSharedEditsDir(0, 1);
    File sharedEditsDir = new File(sharedEditsUri.getPath());
    StorageDirectory storageDir = new StorageDirectory(sharedEditsDir);
    File inProgressFile = NameNodeAdapter.getInProgressEditsFile(storageDir,
        txid + 1);
    assertTrue("Failed to create in-progress edits file",
        inProgressFile.createNewFile());
    
    if (writeHeader) {
      DataOutputStream out = new DataOutputStream(new FileOutputStream(
          inProgressFile));
      EditLogFileOutputStream.writeHeader(
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION, out);
      out.close();
    }
  }
  

  /**
   * The secret manager needs to start/stop - the invariant should be that
   * the secret manager runs if and only if the NN is active and not in
   * safe mode. As a state diagram, we need to test all of the following
   * transitions to make sure the secret manager is started when we transition
   * into state 4, but none of the others.
   * <pre>
   *         SafeMode     Not SafeMode 
   * Standby   1 <------> 2
   *           ^          ^
   *           |          |
   *           v          v
   * Active    3 <------> 4
   * </pre>
   */
  @Test(timeout=60000)
  public void testSecretManagerState() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY, 50);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)
         .waitSafeMode(false)
        .build();
    try {
      cluster.transitionToActive(0);
      DFSTestUtil.createFile(cluster.getFileSystem(0),
          TEST_FILE_PATH, 6000, (short)1, 1L);
      
      cluster.getConfiguration(0).setInt(
          DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 60000);

      cluster.restartNameNode(0);
      NameNode nn = cluster.getNameNode(0);
      
      banner("Started in state 1.");
      assertTrue(nn.isStandbyState());
      assertTrue(nn.isInSafeMode());
      assertFalse(isDTRunning(nn));
      
      banner("Transition 1->2. Should not start secret manager");
      NameNodeAdapter.leaveSafeMode(nn);
      assertTrue(nn.isStandbyState());
      assertFalse(nn.isInSafeMode());
      assertFalse(isDTRunning(nn));
  
      banner("Transition 2->1. Should not start secret manager.");
      NameNodeAdapter.enterSafeMode(nn, false);
      assertTrue(nn.isStandbyState());
      assertTrue(nn.isInSafeMode());
      assertFalse(isDTRunning(nn));
  
      banner("Transition 1->3. Should not start secret manager.");
      nn.getRpcServer().transitionToActive(REQ_INFO);
      assertFalse(nn.isStandbyState());
      assertTrue(nn.isInSafeMode());
      assertFalse(isDTRunning(nn));
  
      banner("Transition 3->1. Should not start secret manager.");
      nn.getRpcServer().transitionToStandby(REQ_INFO);
      assertTrue(nn.isStandbyState());
      assertTrue(nn.isInSafeMode());
      assertFalse(isDTRunning(nn));
  
      banner("Transition 1->3->4. Should start secret manager.");
      nn.getRpcServer().transitionToActive(REQ_INFO);
      NameNodeAdapter.leaveSafeMode(nn);
      assertFalse(nn.isStandbyState());
      assertFalse(nn.isInSafeMode());
      assertTrue(isDTRunning(nn));
      
      banner("Transition 4->3. Should stop secret manager");
      NameNodeAdapter.enterSafeMode(nn, false);
      assertFalse(nn.isStandbyState());
      assertTrue(nn.isInSafeMode());
      assertFalse(isDTRunning(nn));
  
      banner("Transition 3->4. Should start secret manager");
      NameNodeAdapter.leaveSafeMode(nn);
      assertFalse(nn.isStandbyState());
      assertFalse(nn.isInSafeMode());
      assertTrue(isDTRunning(nn));
      
      for (int i = 0; i < 20; i++) {
        // Loop the last check to suss out races.
        banner("Transition 4->2. Should stop secret manager.");
        nn.getRpcServer().transitionToStandby(REQ_INFO);
        assertTrue(nn.isStandbyState());
        assertFalse(nn.isInSafeMode());
        assertFalse(isDTRunning(nn));
    
        banner("Transition 2->4. Should start secret manager");
        nn.getRpcServer().transitionToActive(REQ_INFO);
        assertFalse(nn.isStandbyState());
        assertFalse(nn.isInSafeMode());
        assertTrue(isDTRunning(nn));
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * This test also serves to test
   * {@link HAUtil#getProxiesForAllNameNodesInNameservice(Configuration, String)} and
   * {@link DFSUtil#getRpcAddressesForNameserviceId(Configuration, String, String)}
   * by virtue of the fact that it wouldn't work properly if the proxies
   * returned were not for the correct NNs.
   */
  @Test(timeout = 300000)
  public void testIsAtLeastOneActive() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .build();
    try {
      Configuration conf = new HdfsConfiguration();
      HATestUtil.setFailoverConfigurations(cluster, conf);
      
      List<ClientProtocol> namenodes =
          HAUtil.getProxiesForAllNameNodesInNameservice(conf,
              HATestUtil.getLogicalHostname(cluster));
      
      assertEquals(2, namenodes.size());
      
      assertFalse(HAUtil.isAtLeastOneActive(namenodes));
      cluster.transitionToActive(0);
      assertTrue(HAUtil.isAtLeastOneActive(namenodes));
      cluster.transitionToStandby(0);
      assertFalse(HAUtil.isAtLeastOneActive(namenodes));
      cluster.transitionToActive(1);
      assertTrue(HAUtil.isAtLeastOneActive(namenodes));
      cluster.transitionToStandby(1);
      assertFalse(HAUtil.isAtLeastOneActive(namenodes));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private boolean isDTRunning(NameNode nn) {
    return NameNodeAdapter.getDtSecretManager(nn.getNamesystem()).isRunning();
  }

  /**
   * Print a big banner in the test log to make debug easier.
   */
  static void banner(String string) {
    LOG.info("\n\n\n\n================================================\n" +
        string + "\n" +
        "==================================================\n\n");
  }
}
