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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.event.Level;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Supplier;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class TestEditLogTailer {
  static {
    GenericTestUtils.setLogLevel(FSEditLog.LOG, Level.DEBUG);
  }

  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{ Boolean.FALSE });
    params.add(new Object[]{ Boolean.TRUE });
    return params;
  }

  private static boolean useAsyncEditLog;
  public TestEditLogTailer(Boolean async) {
    useAsyncEditLog = async;
  }

  private static final String DIR_PREFIX = "/dir";
  private static final int DIRS_TO_MAKE = 20;
  static final long SLEEP_TIME = 1000;
  static final long NN_LAG_TIMEOUT = 10 * 1000;
  
  static {
    GenericTestUtils.setLogLevel(FSImage.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(FSEditLog.LOG, org.slf4j.event.Level.DEBUG);
    GenericTestUtils.setLogLevel(EditLogTailer.LOG, Level.DEBUG);
  }

  private static Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING,
        useAsyncEditLog);
    return conf;
  }

  @Test
  public void testTailer() throws IOException, InterruptedException,
      ServiceFailedException {
    Configuration conf = getConf();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY, 100);
    conf.setLong(EditLogTailer.DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_KEY, 3);

    HAUtil.setAllowStandbyReads(conf, true);
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    cluster.waitActive();
    
    cluster.transitionToActive(0);
    
    NameNode nn1 = cluster.getNameNode(0);
    NameNode nn2 = cluster.getNameNode(1);
    try {
      for (int i = 0; i < DIRS_TO_MAKE / 2; i++) {
        NameNodeAdapter.mkdirs(nn1, getDirPath(i),
            new PermissionStatus("test","test", new FsPermission((short)00755)),
            true);
      }
      
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      assertEquals("Inconsistent number of applied txns on Standby",
          nn1.getNamesystem().getEditLog().getLastWrittenTxId(),
          nn2.getNamesystem().getFSImage().getLastAppliedTxId() + 1);

      for (int i = 0; i < DIRS_TO_MAKE / 2; i++) {
        assertTrue(NameNodeAdapter.getFileInfo(nn2,
            getDirPath(i), false, false, false).isDirectory());
      }
      
      for (int i = DIRS_TO_MAKE / 2; i < DIRS_TO_MAKE; i++) {
        NameNodeAdapter.mkdirs(nn1, getDirPath(i),
            new PermissionStatus("test","test", new FsPermission((short)00755)),
            true);
      }
      
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      assertEquals("Inconsistent number of applied txns on Standby",
          nn1.getNamesystem().getEditLog().getLastWrittenTxId(),
          nn2.getNamesystem().getFSImage().getLastAppliedTxId() + 1);

      for (int i = DIRS_TO_MAKE / 2; i < DIRS_TO_MAKE; i++) {
        assertTrue(NameNodeAdapter.getFileInfo(nn2,
            getDirPath(i), false, false, false).isDirectory());
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testNN0TriggersLogRolls() throws Exception {
    testStandbyTriggersLogRolls(0);
  }
  
  @Test
  public void testNN1TriggersLogRolls() throws Exception {
    testStandbyTriggersLogRolls(1);
  }

  @Test
  public void testNN2TriggersLogRolls() throws Exception {
    testStandbyTriggersLogRolls(2);
  }

  private static void testStandbyTriggersLogRolls(int activeIndex)
      throws Exception {
    Configuration conf = getConf();
    // Roll every 1s
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY, 100);

    MiniDFSCluster cluster = null;
    for (int i = 0; i < 5; i++) {
      try {
        cluster = createMiniDFSCluster(conf, 3);
        break;
      } catch (BindException e) {
        // retry if race on ports given by ServerSocketUtil#getPorts
        continue;
      }
    }
    if (cluster == null) {
      fail("failed to start mini cluster.");
    }
    try {
      cluster.transitionToActive(activeIndex);
      waitForLogRollInSharedDir(cluster, 3);
    } finally {
      cluster.shutdown();
    }
  }

  /*
    1. when all NN become standby nn, standby NN execute to roll log,
    it will be failed.
    2. when one NN become active, standby NN roll log success.
   */
  @Test
  public void testTriggersLogRollsForAllStandbyNN() throws Exception {
    Configuration conf = getConf();
    // Roll every 1s
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY, 100);

    MiniDFSCluster cluster = null;
    try {
      cluster = createMiniDFSCluster(conf, 3);
      cluster.transitionToStandby(0);
      cluster.transitionToStandby(1);
      cluster.transitionToStandby(2);
      try {
        waitForLogRollInSharedDir(cluster, 3);
        fail("After all NN become Standby state, Standby NN should roll log, " +
            "but it will be failed");
      } catch (TimeoutException ignore) {
      }
      cluster.transitionToActive(0);
      waitForLogRollInSharedDir(cluster, 3);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private static String getDirPath(int suffix) {
    return DIR_PREFIX + suffix;
  }
  
  private static void waitForLogRollInSharedDir(MiniDFSCluster cluster,
      long startTxId) throws Exception {
    URI sharedUri = cluster.getSharedEditsDir(0, 2);
    File sharedDir = new File(sharedUri.getPath(), "current");
    final File expectedInProgressLog =
        new File(sharedDir, NNStorage.getInProgressEditsFileName(startTxId));
    final File expectedFinalizedLog = new File(sharedDir,
        NNStorage.getFinalizedEditsFileName(startTxId, startTxId + 1));
    // There is a chance that multiple rolling happens by multiple NameNodes
    // And expected inprogress file would have also finalized. So look for the
    // finalized edits file as well
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return expectedInProgressLog.exists() || expectedFinalizedLog.exists();
      }
    }, 100, 10000);
  }

  @Test(timeout=20000)
  public void testRollEditTimeoutForActiveNN() throws IOException {
    Configuration conf = getConf();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_KEY, 5); // 5s
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY, 100);

    HAUtil.setAllowStandbyReads(conf, true);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .build();
    cluster.waitActive();

    cluster.transitionToActive(0);

    try {
      EditLogTailer tailer = Mockito.spy(
          cluster.getNamesystem(1).getEditLogTailer());
      AtomicInteger flag = new AtomicInteger(0);

      // Return a slow roll edit process.
      when(tailer.getNameNodeProxy()).thenReturn(
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              Thread.sleep(30000);  // sleep for 30 seconds.
              assertTrue(Thread.currentThread().isInterrupted());
              flag.addAndGet(1);
              return null;
            }
          }
      );
      tailer.triggerActiveLogRoll();
      assertEquals(0, flag.get());
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testRollEditLogIOExceptionForRemoteNN() throws IOException {
    Configuration conf = getConf();

    // Roll every 1s
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = createMiniDFSCluster(conf, 3);
      cluster.transitionToActive(0);
      EditLogTailer tailer = Mockito.spy(
          cluster.getNamesystem(1).getEditLogTailer());

      final AtomicInteger invokedTimes = new AtomicInteger(0);

      // It should go on to next name node when IOException happens.
      when(tailer.getNameNodeProxy()).thenReturn(
          tailer.new MultipleNameNodeProxy<Void>() {
            @Override
            protected Void doWork() throws IOException {
              invokedTimes.getAndIncrement();
              throw new IOException("It is an IO Exception.");
            }
          }
      );

      tailer.triggerActiveLogRoll();

      // MultipleNameNodeProxy uses Round-robin to look for active NN
      // to do RollEditLog. If doWork() fails, then IOException throws,
      // it continues to try next NN. triggerActiveLogRoll finishes
      // either due to success, or using up retries.
      // In this test case, there are 2 remote name nodes, default retry is 3.
      // For test purpose, doWork() always returns IOException,
      // so the total invoked times will be default retry 3 * remote NNs 2 = 6
      assertEquals(6, invokedTimes.get());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testStandbyTriggersLogRollsWhenTailInProgressEdits()
      throws Exception {
    // Time in seconds to wait for standby to catch up to edits from active
    final int standbyCatchupWaitTime = 2;
    // Time in seconds to wait before checking if edit logs are rolled while
    // expecting no edit log roll
    final int noLogRollWaitTime = 2;
    // Time in seconds to wait before checking if edit logs are rolled while
    // expecting edit log roll
    final int logRollWaitTime = 3;

    Configuration conf = getConf();
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
        standbyCatchupWaitTime + noLogRollWaitTime + 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);

    MiniDFSCluster cluster = createMiniDFSCluster(conf, 2);
    if (cluster == null) {
      fail("failed to start mini cluster.");
    }

    try {
      int activeIndex = new Random().nextBoolean() ? 1 : 0;
      int standbyIndex = (activeIndex == 0) ? 1 : 0;
      cluster.transitionToActive(activeIndex);
      NameNode active = cluster.getNameNode(activeIndex);
      NameNode standby = cluster.getNameNode(standbyIndex);

      long origTxId = active.getNamesystem().getFSImage().getEditLog()
          .getCurSegmentTxId();
      for (int i = 0; i < DIRS_TO_MAKE / 2; i++) {
        NameNodeAdapter.mkdirs(active, getDirPath(i),
            new PermissionStatus("test", "test",
            new FsPermission((short)00755)), true);
      }

      long activeTxId = active.getNamesystem().getFSImage().getEditLog()
          .getLastWrittenTxId();
      waitForStandbyToCatchUpWithInProgressEdits(standby, activeTxId,
          standbyCatchupWaitTime);

      for (int i = DIRS_TO_MAKE / 2; i < DIRS_TO_MAKE; i++) {
        NameNodeAdapter.mkdirs(active, getDirPath(i),
            new PermissionStatus("test", "test",
            new FsPermission((short)00755)), true);
      }

      boolean exceptionThrown = false;
      try {
        checkForLogRoll(active, origTxId, noLogRollWaitTime);
      } catch (TimeoutException e) {
        exceptionThrown = true;
      }
      assertTrue(exceptionThrown);

      checkForLogRoll(active, origTxId, logRollWaitTime);
    } finally {
      cluster.shutdown();
    }
  }

  private static void waitForStandbyToCatchUpWithInProgressEdits(
      final NameNode standby, final long activeTxId,
      int maxWaitSec) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        long standbyTxId = standby.getNamesystem().getFSImage()
            .getLastAppliedTxId();
        return (standbyTxId >= activeTxId);
      }
    }, 100, maxWaitSec * 1000);
  }

  private static void checkForLogRoll(final NameNode active,
      final long origTxId, int maxWaitSec) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        long curSegmentTxId = active.getNamesystem().getFSImage().getEditLog()
            .getCurSegmentTxId();
        return (origTxId != curSegmentTxId);
      }
    }, 100, maxWaitSec * 1000);
  }

  private static MiniDFSCluster createMiniDFSCluster(Configuration conf,
      int nnCount) throws IOException {
    int basePort = 10060 + new Random().nextInt(1000) * 2;

    // By passing in basePort, name node will have IPC port set,
    // which is needed for enabling roll log.
    MiniDFSNNTopology topology =
            MiniDFSNNTopology.simpleHATopology(nnCount, basePort);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(topology)
        .numDataNodes(0)
        .build();
    return cluster;
  }
}
