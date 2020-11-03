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

import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.test.PathUtils;
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
import static org.mockito.ArgumentMatchers.any;

public class TestStandbyCheckpoints {
  private static final int NUM_DIRS_IN_LOG = 200000;
  protected static int NUM_NNS = 3;
  protected MiniDFSCluster cluster;
  protected NameNode[] nns = new NameNode[NUM_NNS];
  protected FileSystem fs;
  private final Random random = new Random();
  protected File tmpOivImgDir;
  
  private static final Logger LOG = LoggerFactory.getLogger(TestStandbyCheckpoints.class);

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
                .addNN(new MiniDFSNNTopology.NNConf("nn2").setHttpPort(basePort + 1))
                .addNN(new MiniDFSNNTopology.NNConf("nn3").setHttpPort(basePort + 2)));

        cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(topology)
            .numDataNodes(1)
            .build();
        cluster.waitActive();

        setNNs();

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

  protected void setNNs(){
    for (int i = 0; i < NUM_NNS; i++) {
      nns[i] = cluster.getNameNode(i);
    }
  }

  protected Configuration setupCommonConfig() {
    tmpOivImgDir = GenericTestUtils.getTestDir("TestStandbyCheckpoints");
    tmpOivImgDir.mkdirs();

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
      cluster = null;
    }

    if (tmpOivImgDir != null) {
      FileUtil.fullyDelete(tmpOivImgDir);
    }
  }

  @Test(timeout = 300000)
  public void testSBNCheckpoints() throws Exception {
    JournalSet standbyJournalSet = NameNodeAdapter.spyOnJournalSet(nns[1]);

    doEdits(0, 10);
    HATestUtil.waitForStandbyToCatchUp(nns[0], nns[1]);
    // Once the standby catches up, it should notice that it needs to
    // do a checkpoint and save one to its local directories.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        if (tmpOivImgDir.list().length > 0) {
          return true;
        } else {
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

  @Test
  public void testNewDirInitAfterCheckpointing() throws Exception {
    File hdfsDir = new File(PathUtils.getTestDir(TestStandbyCheckpoints.class),
        "testNewDirInitAfterCheckpointing");
    File nameDir = new File(hdfsDir, "name1");
    assert nameDir.mkdirs();

    // Restart nn0 with an additional name dir.
    String existingDir = cluster.getConfiguration(0).
        get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    cluster.getConfiguration(0).set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        existingDir + "," + Util.fileAsURI(nameDir).toString());
    cluster.restartNameNode(0);
    nns[0] = cluster.getNameNode(0);
    cluster.transitionToActive(0);

    // "current" is created, but current/VERSION isn't.
    File currDir = new File(nameDir, "current");
    File versionFile = new File(currDir, "VERSION");
    assert currDir.exists();
    assert !versionFile.exists();

    // Trigger a checkpointing and upload.
    doEdits(0, 10);
    HATestUtil.waitForStandbyToCatchUp(nns[0], nns[1]);

    // The version file will be created if a checkpoint is uploaded.
    // Wait for it to happen up to 10 seconds.
    for (int i = 0; i < 20; i++) {
      if (versionFile.exists()) {
        break;
      }
      Thread.sleep(500);
    }
    // VERSION must have been created.
    assert versionFile.exists();
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
    
    assertEquals(12, nns[0].getNamesystem().getFSImage()
        .getMostRecentCheckpointTxId());
    assertEquals(12, nns[1].getNamesystem().getFSImage()
        .getMostRecentCheckpointTxId());
    
    List<File> dirs = Lists.newArrayList();
    dirs.addAll(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 0));
    dirs.addAll(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 1));
    FSImageTestUtil.assertParallelFilesAreIdentical(dirs, ImmutableSet.<String>of());
  }

  /**
   * Test for the case of when there are observer NameNodes, Standby node is
   * able to upload fsImage to Observer node as well.
   */
  @Test(timeout = 300000)
  public void testStandbyAndObserverState() throws Exception {
    // Transition 2 to observer
    cluster.transitionToObserver(2);
    doEdits(0, 10);
    // After a rollEditLog, Standby(nn1) 's next checkpoint would be
    // ahead of observer(nn2).
    nns[0].getRpcServer().rollEditLog();

    // After standby creating a checkpoint, it will try to push the image to
    // active and all observer, updating it's own txid to the most recent.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(12));
    HATestUtil.waitForCheckpoint(cluster, 2, ImmutableList.of(12));

    assertEquals(12, nns[2].getNamesystem().getFSImage()
        .getMostRecentCheckpointTxId());
    assertEquals(12, nns[1].getNamesystem().getFSImage()
        .getMostRecentCheckpointTxId());

    List<File> dirs = Lists.newArrayList();
    // observer and standby both have this same image.
    dirs.addAll(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 2));
    dirs.addAll(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 1));
    FSImageTestUtil.assertParallelFilesAreIdentical(dirs, ImmutableSet.of());
    // Restore 2 back to standby
    cluster.transitionToStandby(2);
  }

  /**
   * Tests that a null FSImage is handled gracefully by the ImageServlet.
   * If putImage is called while a NameNode is still starting up, the FSImage
   * may not have been initialized yet. See HDFS-15290.
   */
  @Test(timeout = 30000)
  public void testCheckpointBeforeNameNodeInitializationIsComplete()
      throws Exception {
    final LogVerificationAppender appender = new LogVerificationAppender();
    final org.apache.log4j.Logger logger = org.apache.log4j.Logger
        .getRootLogger();
    logger.addAppender(appender);

    // Transition 2 to observer
    cluster.transitionToObserver(2);
    doEdits(0, 10);
    // After a rollEditLog, Standby(nn1)'s next checkpoint would be
    // ahead of observer(nn2).
    nns[0].getRpcServer().rollEditLog();

    NameNode nn2 = nns[2];
    FSImage nnFSImage = NameNodeAdapter.getAndSetFSImageInHttpServer(nn2, null);

    // After standby creating a checkpoint, it will try to push the image to
    // active and all observer, updating it's own txid to the most recent.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(12));

    NameNodeAdapter.getAndSetFSImageInHttpServer(nn2, nnFSImage);
    cluster.transitionToStandby(2);
    logger.removeAppender(appender);

    for (LoggingEvent event : appender.getLog()) {
      String message = event.getRenderedMessage();
      if (message.contains("PutImage failed") &&
          message.contains("FSImage has not been set in the NameNode.")) {
        //Logs have the expected exception.
        return;
      }
    }
    fail("Expected exception not present in logs.");
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
    nns[1] = cluster.getNameNode(1);

    FSImage spyImage1 = NameNodeAdapter.spyOnFsImage(nns[1]);

    // We shouldn't save any checkpoints at txid=0
    Thread.sleep(1000);
    Mockito.verify(spyImage1, Mockito.never())
      .saveNamespace(any());
 
    // Roll the primary and wait for the standby to catch up
    HATestUtil.waitForStandbyToCatchUp(nns[0], nns[1]);
    Thread.sleep(2000);
    
    // We should make exactly one checkpoint at this new txid. 
    Mockito.verify(spyImage1, Mockito.times(1)).saveNamespace(
        any(), Mockito.eq(NameNodeFile.IMAGE), any());
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
    nns[1] = cluster.getNameNode(1);

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
    for (int i = 0; i < NUM_NNS; i++) {
      cluster.getConfiguration(i).setBoolean(
          DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, false);
    }

    // Throttle SBN upload to make it hang during upload to ANN
    for (int i = 1; i < NUM_NNS; i++) {
      cluster.getConfiguration(i).setLong(
          DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 100);
    }
    for (int i = 0; i < NUM_NNS; i++) {
      cluster.restartNameNode(i);
    }

    // update references to each of the nns
    setNNs();

    cluster.transitionToActive(0);

    doEdits(0, 100);

    for (int i = 1; i < NUM_NNS; i++) {
      HATestUtil.waitForStandbyToCatchUp(nns[0], nns[i]);
      HATestUtil.waitForCheckpoint(cluster, i, ImmutableList.of(104));
    }

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
    assertEquals(0, nns[0].getFSImage().getMostRecentCheckpointTxId());
  }
  
  /**
   * Make sure that clients will receive StandbyExceptions even when a
   * checkpoint is in progress on the SBN, and therefore the StandbyCheckpointer
   * thread will have FSNS lock. Regression test for HDFS-4591.
   */
  @Test(timeout=300000)
  public void testStandbyExceptionThrownDuringCheckpoint() throws Exception {
    
    // Set it up so that we know when the SBN checkpoint starts and ends.
    FSImage spyImage1 = NameNodeAdapter.spyOnFsImage(nns[1]);
    DelayAnswer answerer = new DelayAnswer(LOG);
    Mockito.doAnswer(answerer).when(spyImage1)
        .saveNamespace(any(FSNamesystem.class),
            Mockito.eq(NameNodeFile.IMAGE), any(Canceler.class));

    // Perform some edits and wait for a checkpoint to start on the SBN.
    doEdits(0, 1000);
    nns[0].getRpcServer().rollEditLog();
    answerer.waitForCall();
    assertTrue("SBN is not performing checkpoint but it should be.",
        answerer.getFireCount() == 1 && answerer.getResultCount() == 0);
    
    // Make sure that the lock has actually been taken by the checkpointing
    // thread.
    ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
    try {
      // Perform an RPC to the SBN and make sure it throws a StandbyException.
      nns[1].getRpcServer().getFileInfo("/");
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
    FSImage spyImage1 = NameNodeAdapter.spyOnFsImage(nns[1]);
    DelayAnswer answerer = new DelayAnswer(LOG);
    Mockito.doAnswer(answerer).when(spyImage1)
        .saveNamespace(any(FSNamesystem.class),
            any(NameNodeFile.class),
            any(Canceler.class));
    
    // Perform some edits and wait for a checkpoint to start on the SBN.
    doEdits(0, 1000);
    nns[0].getRpcServer().rollEditLog();
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
          nns[1].getRpcServer().restoreFailedStorage("false");
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
    
    // Make sure that our thread is waiting for the lock.
    ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);

    assertFalse(nns[1].getNamesystem().getFsLockForTests().hasQueuedThreads());
    assertFalse(nns[1].getNamesystem().getFsLockForTests().isWriteLocked());
    assertTrue(nns[1].getNamesystem().getCpLockForTests().hasQueuedThreads());

    // Get /jmx of the standby NN web UI, which will cause the FSNS read lock to
    // be taken.
    String pageContents = DFSTestUtil.urlGet(new URL("http://" +
        nns[1].getHttpAddress().getHostName() + ":" +
        nns[1].getHttpAddress().getPort() + "/jmx"));
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

  /**
   * Test for the case standby NNs can upload FSImage to ANN after
   * become non-primary standby NN. HDFS-9787
   */
  @Test(timeout=300000)
  public void testNonPrimarySBNUploadFSImage() throws Exception {
    // Shutdown all standby NNs.
    for (int i = 1; i < NUM_NNS; i++) {
      cluster.shutdownNameNode(i);

      // Checkpoint as fast as we can, in a tight loop.
      cluster.getConfiguration(i).setInt(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 1);
    }

    doEdits(0, 10);
    cluster.transitionToStandby(0);

    // Standby NNs do checkpoint without active NN available.
    for (int i = 1; i < NUM_NNS; i++) {
      cluster.restartNameNode(i, false);
    }
    cluster.waitClusterUp();

    for (int i = 0; i < NUM_NNS; i++) {
      // Once the standby catches up, it should do a checkpoint
      // and save to local directories.
      HATestUtil.waitForCheckpoint(cluster, i, ImmutableList.of(12));
    }

    cluster.transitionToActive(0);

    // Wait for 2 seconds to expire last upload time.
    Thread.sleep(2000);

    doEdits(11, 20);
    nns[0].getRpcServer().rollEditLog();

    // One of standby NNs should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(23));
  }

  /**
   * Test that checkpointing is still successful even if an issue
   * was encountered while writing the legacy OIV image.
   */
  @Test(timeout=300000)
  public void testCheckpointSucceedsWithLegacyOIVException() throws Exception {
    // Delete the OIV image dir to cause an IOException while saving
    FileUtil.fullyDelete(tmpOivImgDir);

    doEdits(0, 10);
    HATestUtil.waitForStandbyToCatchUp(nns[0], nns[1]);
    // Once the standby catches up, it should notice that it needs to
    // do a checkpoint and save one to its local directories.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));

    // It should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(12));
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
