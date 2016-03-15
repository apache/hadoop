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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.util.ThreadUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.BindException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class TestStandbyCheckpoints {
  private static final int NUM_DIRS_IN_LOG = 200000;
  protected MiniDFSCluster cluster;
  protected NameNode nn0, nn1;
  protected FileSystem fs;
  private final Random random = new Random();
  protected File tmpOivImgDir;
  
  private static final Log LOG = LogFactory.getLog(TestStandbyCheckpoints.class);

  @SuppressWarnings("rawtypes")
  @Before
  public void setupCluster() throws Exception {
    Configuration conf = setupCommonConfig();

    // Dial down the retention of extra edits and checkpoints. This is to
    // help catch regressions of HDFS-4238 (SBN should not purge shared edits)
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY, 0);

    int retryCount = 0;
    while (true) {
      try {
        int basePort = 10060 + random.nextInt(100) * 2;
        MiniDFSNNTopology topology = new MiniDFSNNTopology()
            .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
                .addNN(new MiniDFSNNTopology.NNConf("nn1").setHttpPort(basePort))
                .addNN(new MiniDFSNNTopology.NNConf("nn2").setHttpPort(basePort + 1)));

        cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(topology)
            .numDataNodes(1)
            .build();
        cluster.waitActive();

        nn0 = cluster.getNameNode(0);
        nn1 = cluster.getNameNode(1);
        fs = HATestUtil.configureFailoverFs(cluster, conf);

        cluster.transitionToActive(0);
        ++retryCount;
        break;
      } catch (BindException e) {
        LOG.info("Set up MiniDFSCluster failed due to port conflicts, retry "
            + retryCount + " times");
      }
    }
  }

  protected Configuration setupCommonConfig() {
    tmpOivImgDir = Files.createTempDir();

    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 5);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.set(DFSConfigKeys.DFS_NAMENODE_LEGACY_OIV_IMAGE_DIR_KEY,
        tmpOivImgDir.getAbsolutePath());
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
    conf.set(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
        SlowCodec.class.getCanonicalName());
    CompressionCodecFactory.setCodecClasses(conf,
        ImmutableList.<Class>of(SlowCodec.class));
    return conf;
  }

  @After
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testSBNCheckpoints() throws Exception {
    JournalSet standbyJournalSet = NameNodeAdapter.spyOnJournalSet(nn1);
    
    doEdits(0, 10);
    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    // Once the standby catches up, it should notice that it needs to
    // do a checkpoint and save one to its local directories.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        if(tmpOivImgDir.list().length > 0) {
          return true;
        }
        else {
          return false;
        }
      }
    }, 1000, 60000);
    
    // It should have saved the oiv image too.
    assertEquals("One file is expected", 1, tmpOivImgDir.list().length);
    
    // It should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(12));
    
    // The standby should never try to purge edit logs on shared storage.
    Mockito.verify(standbyJournalSet, Mockito.never()).
      purgeLogsOlderThan(Mockito.anyLong());
  }

  /**
   * Test for the case when both of the NNs in the cluster are
   * in the standby state, and thus are both creating checkpoints
   * and uploading them to each other.
   * In this circumstance, they should receive the error from the
   * other node indicating that the other node already has a
   * checkpoint for the given txid, but this should not cause
   * an abort, etc.
   */
  @Test(timeout = 300000)
  public void testBothNodesInStandbyState() throws Exception {
    doEdits(0, 10);
    
    cluster.transitionToStandby(0);

    // Transitioning to standby closed the edit log on the active,
    // so the standby will catch up. Then, both will be in standby mode
    // with enough uncheckpointed txns to cause a checkpoint, and they
    // will each try to take a checkpoint and upload to each other.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(12));
    
    assertEquals(12, nn0.getNamesystem().getFSImage()
        .getMostRecentCheckpointTxId());
    assertEquals(12, nn1.getNamesystem().getFSImage()
        .getMostRecentCheckpointTxId());
    
    List<File> dirs = Lists.newArrayList();
    dirs.addAll(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 0));
    dirs.addAll(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 1));
    FSImageTestUtil.assertParallelFilesAreIdentical(dirs, ImmutableSet.<String>of());
  }
  
  /**
   * Test for the case when the SBN is configured to checkpoint based
   * on a time period, but no transactions are happening on the
   * active. Thus, it would want to save a second checkpoint at the
   * same txid, which is a no-op. This test makes sure this doesn't
   * cause any problem.
   */
  @Test(timeout = 300000)
  public void testCheckpointWhenNoNewTransactionsHappened()
      throws Exception {
    // Checkpoint as fast as we can, in a tight loop.
    cluster.getConfiguration(1).setInt(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 0);
    cluster.restartNameNode(1);
    nn1 = cluster.getNameNode(1);
 
    FSImage spyImage1 = NameNodeAdapter.spyOnFsImage(nn1);
    
    // We shouldn't save any checkpoints at txid=0
    Thread.sleep(1000);
    Mockito.verify(spyImage1, Mockito.never())
      .saveNamespace((FSNamesystem) Mockito.anyObject());
 
    // Roll the primary and wait for the standby to catch up
    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    Thread.sleep(2000);
    
    // We should make exactly one checkpoint at this new txid. 
    Mockito.verify(spyImage1, Mockito.times(1)).saveNamespace(
        (FSNamesystem) Mockito.anyObject(), Mockito.eq(NameNodeFile.IMAGE),
        (Canceler) Mockito.anyObject());
  }
  
  /**
   * Test cancellation of ongoing checkpoints when failover happens
   * mid-checkpoint. 
   */
  @Test(timeout=120000)
  public void testCheckpointCancellation() throws Exception {
    cluster.transitionToStandby(0);
    
    // Create an edit log in the shared edits dir with a lot
    // of mkdirs operations. This is solely so that the image is
    // large enough to take a non-trivial amount of time to load.
    // (only ~15MB)
    URI sharedUri = cluster.getSharedEditsDir(0, 1);
    File sharedDir = new File(sharedUri.getPath(), "current");
    File tmpDir = new File(MiniDFSCluster.getBaseDirectory(),
        "testCheckpointCancellation-tmp");
    FSNamesystem fsn = cluster.getNamesystem(0);
    FSImageTestUtil.createAbortedLogWithMkdirs(tmpDir, NUM_DIRS_IN_LOG, 3,
        fsn.getFSDirectory().getLastInodeId() + 1);
    String fname = NNStorage.getInProgressEditsFileName(3); 
    new File(tmpDir, fname).renameTo(new File(sharedDir, fname));

    // Checkpoint as fast as we can, in a tight loop.
    cluster.getConfiguration(1).setInt(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 0);
    cluster.restartNameNode(1);
    nn1 = cluster.getNameNode(1);

    cluster.transitionToActive(0);    
    
    boolean canceledOne = false;
    for (int i = 0; i < 10 && !canceledOne; i++) {
      
      doEdits(i*10, i*10 + 10);
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);
      canceledOne = StandbyCheckpointer.getCanceledCount() > 0;
    }
    
    assertTrue(canceledOne);
  }

  /**
   * Test cancellation of ongoing checkpoints when failover happens
   * mid-checkpoint during image upload from standby to active NN.
   */
  @Test(timeout=60000)
  public void testCheckpointCancellationDuringUpload() throws Exception {
    // Set dfs.namenode.checkpoint.txns differently on the first NN to avoid it
    // doing checkpoint when it becomes a standby
    cluster.getConfiguration(0).setInt(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 1000);

    // don't compress, we want a big image
    cluster.getConfiguration(0).setBoolean(
        DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, false);
    cluster.getConfiguration(1).setBoolean(
        DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, false);
    // Throttle SBN upload to make it hang during upload to ANN
    cluster.getConfiguration(1).setLong(
        DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 100);
    cluster.restartNameNode(0);
    cluster.restartNameNode(1);
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);

    cluster.transitionToActive(0);

    doEdits(0, 100);
    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(104));
    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);

    // Wait to make sure background TransferFsImageUpload thread was cancelled.
    // This needs to be done before the next test in the suite starts, so that a
    // file descriptor is not held open during the next cluster init.
    cluster.shutdown();
    cluster = null;
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threads = threadBean.getThreadInfo(
          threadBean.getAllThreadIds(), 1);
        for (ThreadInfo thread: threads) {
          if (thread.getThreadName().startsWith("TransferFsImageUpload")) {
            return false;
          }
        }
        return true;
      }
    }, 1000, 30000);

    // Assert that former active did not accept the canceled checkpoint file.
    assertEquals(0, nn0.getFSImage().getMostRecentCheckpointTxId());
  }
  
  /**
   * Make sure that clients will receive StandbyExceptions even when a
   * checkpoint is in progress on the SBN, and therefore the StandbyCheckpointer
   * thread will have FSNS lock. Regression test for HDFS-4591.
   */
  @Test(timeout=300000)
  public void testStandbyExceptionThrownDuringCheckpoint() throws Exception {
    
    // Set it up so that we know when the SBN checkpoint starts and ends.
    FSImage spyImage1 = NameNodeAdapter.spyOnFsImage(nn1);
    DelayAnswer answerer = new DelayAnswer(LOG);
    Mockito.doAnswer(answerer).when(spyImage1)
        .saveNamespace(Mockito.any(FSNamesystem.class),
            Mockito.eq(NameNodeFile.IMAGE), Mockito.any(Canceler.class));

    // Perform some edits and wait for a checkpoint to start on the SBN.
    doEdits(0, 1000);
    nn0.getRpcServer().rollEditLog();
    answerer.waitForCall();
    assertTrue("SBN is not performing checkpoint but it should be.",
        answerer.getFireCount() == 1 && answerer.getResultCount() == 0);
    
    // Make sure that the lock has actually been taken by the checkpointing
    // thread.
    ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
    try {
      // Perform an RPC to the SBN and make sure it throws a StandbyException.
      nn1.getRpcServer().getFileInfo("/");
      fail("Should have thrown StandbyException, but instead succeeded.");
    } catch (StandbyException se) {
      GenericTestUtils.assertExceptionContains("is not supported", se);
    }

    // Make sure new incremental block reports are processed during
    // checkpointing on the SBN.
    assertEquals(0, cluster.getNamesystem(1).getPendingDataNodeMessageCount());
    doCreate();
    Thread.sleep(1000);
    assertTrue(cluster.getNamesystem(1).getPendingDataNodeMessageCount() > 0);
    
    // Make sure that the checkpoint is still going on, implying that the client
    // RPC to the SBN happened during the checkpoint.
    assertTrue("SBN should have still been checkpointing.",
        answerer.getFireCount() == 1 && answerer.getResultCount() == 0);
    answerer.proceed();
    answerer.waitForResult();
    assertTrue("SBN should have finished checkpointing.",
        answerer.getFireCount() == 1 && answerer.getResultCount() == 1);
  }
  
  @Test(timeout=300000)
  public void testReadsAllowedDuringCheckpoint() throws Exception {
    
    // Set it up so that we know when the SBN checkpoint starts and ends.
    FSImage spyImage1 = NameNodeAdapter.spyOnFsImage(nn1);
    DelayAnswer answerer = new DelayAnswer(LOG);
    Mockito.doAnswer(answerer).when(spyImage1)
        .saveNamespace(Mockito.any(FSNamesystem.class),
            Mockito.any(NameNodeFile.class),
            Mockito.any(Canceler.class));
    
    // Perform some edits and wait for a checkpoint to start on the SBN.
    doEdits(0, 1000);
    nn0.getRpcServer().rollEditLog();
    answerer.waitForCall();
    assertTrue("SBN is not performing checkpoint but it should be.",
        answerer.getFireCount() == 1 && answerer.getResultCount() == 0);
    
    // Make sure that the lock has actually been taken by the checkpointing
    // thread.
    ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
    
    // Perform an RPC that needs to take the write lock.
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          nn1.getRpcServer().restoreFailedStorage("false");
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
    
    // Make sure that our thread is waiting for the lock.
    ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
    
    assertFalse(nn1.getNamesystem().getFsLockForTests().hasQueuedThreads());
    assertFalse(nn1.getNamesystem().getFsLockForTests().isWriteLocked());
    assertTrue(nn1.getNamesystem().getCpLockForTests().hasQueuedThreads());
    
    // Get /jmx of the standby NN web UI, which will cause the FSNS read lock to
    // be taken.
    String pageContents = DFSTestUtil.urlGet(new URL("http://" +
        nn1.getHttpAddress().getHostName() + ":" +
        nn1.getHttpAddress().getPort() + "/jmx"));
    assertTrue(pageContents.contains("NumLiveDataNodes"));
    
    // Make sure that the checkpoint is still going on, implying that the client
    // RPC to the SBN happened during the checkpoint.
    assertTrue("SBN should have still been checkpointing.",
        answerer.getFireCount() == 1 && answerer.getResultCount() == 0);
    answerer.proceed();
    answerer.waitForResult();
    assertTrue("SBN should have finished checkpointing.",
        answerer.getFireCount() == 1 && answerer.getResultCount() == 1);
    
    t.join();
  }

  private void doEdits(int start, int stop) throws IOException {
    for (int i = start; i < stop; i++) {
      Path p = new Path("/test" + i);
      fs.mkdirs(p);
    }
  }

  private void doCreate() throws IOException {
    Path p = new Path("/testFile");
    fs.delete(p, false);
    FSDataOutputStream out = fs.create(p, (short)1);
    out.write(42);
    out.close();
  }
  
  
  /**
   * A codec which just slows down the saving of the image significantly
   * by sleeping a few milliseconds on every write. This makes it easy to
   * catch the standby in the middle of saving a checkpoint.
   */
  public static class SlowCodec extends GzipCodec {
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
        throws IOException {
      CompressionOutputStream ret = super.createOutputStream(out);
      CompressionOutputStream spy = Mockito.spy(ret);
      Mockito.doAnswer(new GenericTestUtils.SleepAnswer(5))
        .when(spy).write(Mockito.<byte[]>any(), Mockito.anyInt(), Mockito.anyInt());
      return spy;
    }
  }

}
