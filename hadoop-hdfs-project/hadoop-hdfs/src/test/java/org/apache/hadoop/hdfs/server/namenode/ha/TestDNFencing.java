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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.hdfs.server.namenode.FSInodeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;


public class TestDNFencing {
  
  protected static final Log LOG = LogFactory.getLog(
      TestDNFencing.class);
  private static final String TEST_FILE_DATA = "hello highly available world";
  private static final String TEST_FILE = "/testStandbyIsHot";
  private static final Path TEST_FILE_PATH = new Path(TEST_FILE);
  private static final int SMALL_BLOCK = 1024;
  
  private Configuration conf;
  private MiniDFSCluster cluster;
  private NameNode nn1, nn2;
  private FileSystem fs;

  static {
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(BlockManager.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
  }
  
  @Before
  public void setupCluster() throws Exception {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, SMALL_BLOCK);
    // Bump up replication interval so that we only run replication
    // checks explicitly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 600);
    // Increase max streams so that we re-replicate quickly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 1000);
    // See RandomDeleterPolicy javadoc.
    conf.setClass("dfs.block.replicator.classname", RandomDeleterPolicy.class,
        BlockPlacementPolicy.class); 
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    nn1 = cluster.getNameNode(0);
    nn2 = cluster.getNameNode(1);
    
    cluster.waitActive();
    cluster.transitionToActive(0);
    // Trigger block reports so that the first NN trusts all
    // of the DNs, and will issue deletions
    cluster.triggerBlockReports();
    fs = HATestUtil.configureFailoverFs(cluster, conf);
  }
  
  @After
  public void shutdownCluster() throws Exception {
    if (cluster != null) {
      banner("Shutting down cluster. NN1 metadata:");
      doMetasave(nn1);
      banner("Shutting down cluster. NN2 metadata:");
      doMetasave(nn2);
      cluster.shutdown();
    }
  }
  

  @Test
  public void testDnFencing() throws Exception {
    // Create a file with replication level 3.
    DFSTestUtil.createFile(fs, TEST_FILE_PATH, 30*SMALL_BLOCK, (short)3, 1L);
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, TEST_FILE_PATH);
    
    // Drop its replication count to 1, so it becomes over-replicated.
    // Then compute the invalidation of the extra blocks and trigger
    // heartbeats so the invalidations are flushed to the DNs.
    nn1.getRpcServer().setReplication(TEST_FILE, (short) 1);
    BlockManagerTestUtil.computeInvalidationWork(
        nn1.getNamesystem().getBlockManager());
    cluster.triggerHeartbeats();
    
    // Transition nn2 to active even though nn1 still thinks it's active.
    banner("Failing to NN2 but let NN1 continue to think it's active");
    NameNodeAdapter.abortEditLogs(nn1);
    NameNodeAdapter.enterSafeMode(nn1, false);
    cluster.transitionToActive(1);
    
    // Check that the standby picked up the replication change.
    assertEquals(1,
        nn2.getRpcServer().getFileInfo(TEST_FILE).getReplication());

    // Dump some info for debugging purposes.
    banner("NN2 Metadata immediately after failover");
    doMetasave(nn2);
    
    // Even though NN2 considers the blocks over-replicated, it should
    // post-pone the block invalidation because the DNs are still "stale".
    assertEquals(30, nn2.getNamesystem().getPostponedMisreplicatedBlocks());
    
    banner("Triggering heartbeats and block reports so that fencing is completed");
    cluster.triggerHeartbeats();
    cluster.triggerBlockReports();
    
    banner("Metadata after nodes have all block-reported");
    doMetasave(nn2);
    
    // The blocks should no longer be postponed.
    assertEquals(0, nn2.getNamesystem().getPostponedMisreplicatedBlocks());
    
    // Wait for NN2 to enact its deletions (replication monitor has to run, etc)
    BlockManagerTestUtil.computeInvalidationWork(
        nn2.getNamesystem().getBlockManager());
    cluster.triggerHeartbeats();
    HATestUtil.waitForDNDeletions(cluster);
    cluster.triggerDeletionReports();
    assertEquals(0, nn2.getNamesystem().getUnderReplicatedBlocks());
    assertEquals(0, nn2.getNamesystem().getPendingReplicationBlocks());
    
    banner("Making sure the file is still readable");
    FileSystem fs2 = cluster.getFileSystem(1);
    DFSTestUtil.readFile(fs2, TEST_FILE_PATH);

    banner("Waiting for the actual block files to get deleted from DNs.");
    waitForTrueReplication(cluster, block, 1);
  }
  
  /**
   * Test case which restarts the standby node in such a way that,
   * when it exits safemode, it will want to invalidate a bunch
   * of over-replicated block replicas. Ensures that if we failover
   * at this point it won't lose data.
   */
  @Test
  public void testNNClearsCommandsOnFailoverAfterStartup()
      throws Exception {
    // Make lots of blocks to increase chances of triggering a bug.
    DFSTestUtil.createFile(fs, TEST_FILE_PATH, 30*SMALL_BLOCK, (short)3, 1L);

    banner("Shutting down NN2");
    cluster.shutdownNameNode(1);

    banner("Setting replication to 1, rolling edit log.");
    nn1.getRpcServer().setReplication(TEST_FILE, (short) 1);
    nn1.getRpcServer().rollEditLog();
    
    // Start NN2 again. When it starts up, it will see all of the
    // blocks as over-replicated, since it has the metadata for
    // replication=1, but the DNs haven't yet processed the deletions.
    banner("Starting NN2 again.");
    cluster.restartNameNode(1);
    nn2 = cluster.getNameNode(1);
    
    banner("triggering BRs");
    cluster.triggerBlockReports();

    // We expect that both NN1 and NN2 will have some number of
    // deletions queued up for the DNs.
    banner("computing invalidation on nn1");
    BlockManagerTestUtil.computeInvalidationWork(
        nn1.getNamesystem().getBlockManager());

    banner("computing invalidation on nn2");
    BlockManagerTestUtil.computeInvalidationWork(
        nn2.getNamesystem().getBlockManager());
    
    // Dump some info for debugging purposes.
    banner("Metadata immediately before failover");
    doMetasave(nn2);


    // Transition nn2 to active even though nn1 still thinks it's active
    banner("Failing to NN2 but let NN1 continue to think it's active");
    NameNodeAdapter.abortEditLogs(nn1);
    NameNodeAdapter.enterSafeMode(nn1, false);

    cluster.transitionToActive(1);

    // Check that the standby picked up the replication change.
    assertEquals(1,
        nn2.getRpcServer().getFileInfo(TEST_FILE).getReplication());

    // Dump some info for debugging purposes.
    banner("Metadata immediately after failover");
    doMetasave(nn2);
    
    banner("Triggering heartbeats and block reports so that fencing is completed");
    cluster.triggerHeartbeats();
    cluster.triggerBlockReports();
    
    banner("Metadata after nodes have all block-reported");
    doMetasave(nn2);
    
    // The block should no longer be postponed.
    assertEquals(0, nn2.getNamesystem().getPostponedMisreplicatedBlocks());
    
    // Wait for NN2 to enact its deletions (replication monitor has to run, etc)
    BlockManagerTestUtil.computeInvalidationWork(
        nn2.getNamesystem().getBlockManager());

    HATestUtil.waitForNNToIssueDeletions(nn2);
    cluster.triggerHeartbeats();
    HATestUtil.waitForDNDeletions(cluster);
    cluster.triggerDeletionReports();
    assertEquals(0, nn2.getNamesystem().getUnderReplicatedBlocks());
    assertEquals(0, nn2.getNamesystem().getPendingReplicationBlocks());
    
    banner("Making sure the file is still readable");
    FileSystem fs2 = cluster.getFileSystem(1);
    DFSTestUtil.readFile(fs2, TEST_FILE_PATH);
  }
  
  /**
   * Test case that reduces replication of a file with a lot of blocks
   * and then fails over right after those blocks enter the DN invalidation
   * queues on the active. Ensures that fencing is correct and no replicas
   * are lost.
   */
  @Test
  public void testNNClearsCommandsOnFailoverWithReplChanges()
      throws Exception {
    // Make lots of blocks to increase chances of triggering a bug.
    DFSTestUtil.createFile(fs, TEST_FILE_PATH, 30*SMALL_BLOCK, (short)1, 1L);

    banner("rolling NN1's edit log, forcing catch-up");
    HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
    
    // Get some new replicas reported so that NN2 now considers
    // them over-replicated and schedules some more deletions
    nn1.getRpcServer().setReplication(TEST_FILE, (short) 2);
    while (BlockManagerTestUtil.getComputedDatanodeWork(
        nn1.getNamesystem().getBlockManager()) > 0) {
      LOG.info("Getting more replication work computed");
    }
    BlockManager bm1 = nn1.getNamesystem().getBlockManager();
    while (bm1.getPendingReplicationBlocksCount() > 0) {
      BlockManagerTestUtil.updateState(bm1);
      cluster.triggerHeartbeats();
      Thread.sleep(1000);
    }
    
    banner("triggering BRs");
    cluster.triggerBlockReports();
    
    nn1.getRpcServer().setReplication(TEST_FILE, (short) 1);

    
    banner("computing invalidation on nn1");

    BlockManagerTestUtil.computeInvalidationWork(
        nn1.getNamesystem().getBlockManager());
    doMetasave(nn1);

    banner("computing invalidation on nn2");
    BlockManagerTestUtil.computeInvalidationWork(
        nn2.getNamesystem().getBlockManager());
    doMetasave(nn2);

    // Dump some info for debugging purposes.
    banner("Metadata immediately before failover");
    doMetasave(nn2);


    // Transition nn2 to active even though nn1 still thinks it's active
    banner("Failing to NN2 but let NN1 continue to think it's active");
    NameNodeAdapter.abortEditLogs(nn1);
    NameNodeAdapter.enterSafeMode(nn1, false);

    
    BlockManagerTestUtil.computeInvalidationWork(
        nn2.getNamesystem().getBlockManager());
    cluster.transitionToActive(1);

    // Check that the standby picked up the replication change.
    assertEquals(1,
        nn2.getRpcServer().getFileInfo(TEST_FILE).getReplication());

    // Dump some info for debugging purposes.
    banner("Metadata immediately after failover");
    doMetasave(nn2);
    
    banner("Triggering heartbeats and block reports so that fencing is completed");
    cluster.triggerHeartbeats();
    cluster.triggerBlockReports();
    
    banner("Metadata after nodes have all block-reported");
    doMetasave(nn2);
    
    // The block should no longer be postponed.
    assertEquals(0, nn2.getNamesystem().getPostponedMisreplicatedBlocks());
    
    // Wait for NN2 to enact its deletions (replication monitor has to run, etc)
    BlockManagerTestUtil.computeInvalidationWork(
        nn2.getNamesystem().getBlockManager());

    HATestUtil.waitForNNToIssueDeletions(nn2);
    cluster.triggerHeartbeats();
    HATestUtil.waitForDNDeletions(cluster);
    cluster.triggerDeletionReports();
    assertEquals(0, nn2.getNamesystem().getUnderReplicatedBlocks());
    assertEquals(0, nn2.getNamesystem().getPendingReplicationBlocks());
    
    banner("Making sure the file is still readable");
    FileSystem fs2 = cluster.getFileSystem(1);
    DFSTestUtil.readFile(fs2, TEST_FILE_PATH);
  }

  /**
   * Print a big banner in the test log to make debug easier.
   */
  private void banner(String string) {
    LOG.info("\n\n\n\n================================================\n" +
        string + "\n" +
        "==================================================\n\n");
  }

  private void doMetasave(NameNode nn2) {
    nn2.getNamesystem().writeLock();
    try {
      PrintWriter pw = new PrintWriter(System.err);
      nn2.getNamesystem().getBlockManager().metaSave(pw);
      pw.flush();
    } finally {
      nn2.getNamesystem().writeUnlock();
    }
  }

  private void waitForTrueReplication(final MiniDFSCluster cluster,
      final ExtendedBlock block, final int waitFor) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return getTrueReplication(cluster, block) == waitFor;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }, 500, 10000);
  }

  private int getTrueReplication(MiniDFSCluster cluster, ExtendedBlock block)
      throws IOException {
    int count = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getFSDataset().getStoredBlock(block.getBlockPoolId(), block.getBlockId()) != null) {
        count++;
      }
    }
    return count;
  }

  /**
   * A BlockPlacementPolicy which, rather than using space available, makes
   * random decisions about which excess replica to delete. This is because,
   * in the test cases, the two NNs will usually (but not quite always)
   * make the same decision of which replica to delete. The fencing issues
   * are exacerbated when the two NNs make different decisions, which can
   * happen in "real life" when they have slightly out-of-sync heartbeat
   * information regarding disk usage.
   */
  public static class RandomDeleterPolicy extends BlockPlacementPolicyDefault {

    public RandomDeleterPolicy() {
      super();
    }

    @Override
    public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode,
        Block block, short replicationFactor,
        Collection<DatanodeDescriptor> first,
        Collection<DatanodeDescriptor> second) {
      
      Collection<DatanodeDescriptor> chooseFrom =
        !first.isEmpty() ? first : second;

      List<DatanodeDescriptor> l = Lists.newArrayList(chooseFrom);
      return l.get(DFSUtil.getRandom().nextInt(l.size()));
    }
  }

}
