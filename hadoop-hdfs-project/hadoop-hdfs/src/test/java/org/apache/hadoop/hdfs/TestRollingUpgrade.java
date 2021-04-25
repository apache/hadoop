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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.CheckpointFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode;
import org.apache.hadoop.hdfs.server.namenode.TestFileTruncate;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.namenode.ImageServlet.RECENT_IMAGE_CHECK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * This class tests rolling upgrade.
 */
public class TestRollingUpgrade {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRollingUpgrade.class);

  public static void runCmd(DFSAdmin dfsadmin, boolean success,
      String... args) throws  Exception {
    if (success) {
      assertEquals(0, dfsadmin.run(args));
    } else {
      Assert.assertTrue(dfsadmin.run(args) != 0);
    }
  }

  /**
   * Test DFSAdmin Upgrade Command.
   */
  @Test
  public void testDFSAdminRollingUpgradeCommands() throws Exception {
    // start a cluster
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Path foo = new Path("/foo");
      final Path bar = new Path("/bar");
      final Path baz = new Path("/baz");

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        final DFSAdmin dfsadmin = new DFSAdmin(conf);
        dfs.mkdirs(foo);

        //illegal argument "abc" to rollingUpgrade option
        runCmd(dfsadmin, false, "-rollingUpgrade", "abc");

        checkMxBeanIsNull();
        //query rolling upgrade
        runCmd(dfsadmin, true, "-rollingUpgrade");

        //start rolling upgrade
        dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
        runCmd(dfsadmin, true, "-rollingUpgrade", "prepare");
        dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

        //query rolling upgrade
        runCmd(dfsadmin, true, "-rollingUpgrade", "query");
        checkMxBean();

        dfs.mkdirs(bar);

        //finalize rolling upgrade
        runCmd(dfsadmin, true, "-rollingUpgrade", "finalize");
        // RollingUpgradeInfo should be null after finalization, both via
        // Java API and in JMX
        assertNull(dfs.rollingUpgrade(RollingUpgradeAction.QUERY));
        checkMxBeanIsNull();

        dfs.mkdirs(baz);

        runCmd(dfsadmin, true, "-rollingUpgrade");

        // All directories created before upgrade, when upgrade in progress and
        // after upgrade finalize exists
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
        Assert.assertTrue(dfs.exists(baz));

        dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
        dfs.saveNamespace();
        dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      }

      // Ensure directories exist after restart
      cluster.restartNameNode();
      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
        Assert.assertTrue(dfs.exists(baz));
      }
    } finally {
      if(cluster != null) cluster.shutdown();
    }
  }

  private static Configuration setConf(Configuration conf, File dir,
      MiniJournalCluster mjc) {
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, dir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        mjc.getQuorumJournalURI("myjournal").toString());
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 0L);
    return conf;
  }

  @Test (timeout = 30000)
  public void testRollingUpgradeWithQJM() throws Exception {
    String nnDirPrefix = MiniDFSCluster.getBaseDirectory() + "/nn/";
    final File nn1Dir = new File(nnDirPrefix + "image1");
    final File nn2Dir = new File(nnDirPrefix + "image2");

    LOG.info("nn1Dir=" + nn1Dir);
    LOG.info("nn2Dir=" + nn2Dir);

    final Configuration conf = new HdfsConfiguration();
    final MiniJournalCluster mjc = new MiniJournalCluster.Builder(conf).build();
    mjc.waitActive();
    setConf(conf, nn1Dir, mjc);

    {
      // Start the cluster once to generate the dfs dirs
      final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .manageNameDfsDirs(false)
        .checkExitOnShutdown(false)
        .build();
      // Shutdown the cluster before making a copy of the namenode dir to release
      // all file locks, otherwise, the copy will fail on some platforms.
      cluster.shutdown();
    }

    MiniDFSCluster cluster2 = null;
    try {
      // Start a second NN pointed to the same quorum.
      // We need to copy the image dir from the first NN -- or else
      // the new NN will just be rejected because of Namespace mismatch.
      FileUtil.fullyDelete(nn2Dir);
      FileUtil.copy(nn1Dir, FileSystem.getLocal(conf).getRaw(),
          new Path(nn2Dir.getAbsolutePath()), false, conf);

      // Start the cluster again
      final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .format(false)
        .manageNameDfsDirs(false)
        .checkExitOnShutdown(false)
        .build();

      final Path foo = new Path("/foo");
      final Path bar = new Path("/bar");
      final Path baz = new Path("/baz");

      final RollingUpgradeInfo info1;
      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        dfs.mkdirs(foo);

        //start rolling upgrade
        dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
        info1 = dfs.rollingUpgrade(RollingUpgradeAction.PREPARE);
        dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
        LOG.info("START\n" + info1);

        //query rolling upgrade
        assertEquals(info1, dfs.rollingUpgrade(RollingUpgradeAction.QUERY));

        dfs.mkdirs(bar);
        cluster.shutdown();
      }

      // cluster2 takes over QJM
      final Configuration conf2 = setConf(new Configuration(), nn2Dir, mjc);
      cluster2 = new MiniDFSCluster.Builder(conf2)
        .numDataNodes(0)
        .format(false)
        .manageNameDfsDirs(false)
        .build();
      final DistributedFileSystem dfs2 = cluster2.getFileSystem();

      // Check that cluster2 sees the edits made on cluster1
      Assert.assertTrue(dfs2.exists(foo));
      Assert.assertTrue(dfs2.exists(bar));
      Assert.assertFalse(dfs2.exists(baz));

      //query rolling upgrade in cluster2
      assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));

      dfs2.mkdirs(baz);

      LOG.info("RESTART cluster 2");
      cluster2.restartNameNode();
      assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));
      Assert.assertTrue(dfs2.exists(foo));
      Assert.assertTrue(dfs2.exists(bar));
      Assert.assertTrue(dfs2.exists(baz));

      //restart cluster with -upgrade should fail.
      try {
        cluster2.restartNameNode("-upgrade");
      } catch(IOException e) {
        LOG.info("The exception is expected.", e);
      }

      LOG.info("RESTART cluster 2 again");
      cluster2.restartNameNode();
      assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));
      Assert.assertTrue(dfs2.exists(foo));
      Assert.assertTrue(dfs2.exists(bar));
      Assert.assertTrue(dfs2.exists(baz));

      //finalize rolling upgrade
      final RollingUpgradeInfo finalize = dfs2.rollingUpgrade(
          RollingUpgradeAction.FINALIZE);
      Assert.assertTrue(finalize.isFinalized());

      LOG.info("RESTART cluster 2 with regular startup option");
      cluster2.getNameNodeInfos()[0].setStartOpt(StartupOption.REGULAR);
      cluster2.restartNameNode();
      Assert.assertTrue(dfs2.exists(foo));
      Assert.assertTrue(dfs2.exists(bar));
      Assert.assertTrue(dfs2.exists(baz));
    } finally {
      if (cluster2 != null) cluster2.shutdown();
    }
  }

  private static CompositeDataSupport getBean()
      throws MalformedObjectNameException, MBeanException,
      AttributeNotFoundException, InstanceNotFoundException,
      ReflectionException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName =
        new ObjectName("Hadoop:service=NameNode,name=NameNodeInfo");
    return (CompositeDataSupport)mbs.getAttribute(mxbeanName,
        "RollingUpgradeStatus");
  }

  private static void checkMxBeanIsNull() throws Exception {
    CompositeDataSupport ruBean = getBean();
    assertNull(ruBean);
  }

  private static void checkMxBean() throws Exception {
    CompositeDataSupport ruBean = getBean();
    assertNotEquals(0l, ruBean.get("startTime"));
    assertEquals(0l, ruBean.get("finalizeTime"));
  }

  @Test
  public void testRollback() throws Exception {
    // start a cluster
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      final Path foo = new Path("/foo");
      final Path bar = new Path("/bar");
      cluster.getFileSystem().mkdirs(foo);

      final Path file = new Path(foo, "file");
      final byte[] data = new byte[1024];
      ThreadLocalRandom.current().nextBytes(data);
      final FSDataOutputStream out = cluster.getFileSystem().create(file);
      out.write(data, 0, data.length);
      out.close();

      checkMxBeanIsNull();
      startRollingUpgrade(foo, bar, file, data, cluster);
      checkMxBean();
      cluster.getFileSystem().rollEdits();
      cluster.getFileSystem().rollEdits();
      rollbackRollingUpgrade(foo, bar, file, data, cluster);
      checkMxBeanIsNull();

      startRollingUpgrade(foo, bar, file, data, cluster);
      cluster.getFileSystem().rollEdits();
      cluster.getFileSystem().rollEdits();
      rollbackRollingUpgrade(foo, bar, file, data, cluster);

      startRollingUpgrade(foo, bar, file, data, cluster);
      cluster.restartNameNode();
      rollbackRollingUpgrade(foo, bar, file, data, cluster);

      startRollingUpgrade(foo, bar, file, data, cluster);
      cluster.restartNameNode();
      rollbackRollingUpgrade(foo, bar, file, data, cluster);

      startRollingUpgrade(foo, bar, file, data, cluster);
      rollbackRollingUpgrade(foo, bar, file, data, cluster);

      startRollingUpgrade(foo, bar, file, data, cluster);
      rollbackRollingUpgrade(foo, bar, file, data, cluster);
    } finally {
      if(cluster != null) cluster.shutdown();
    }
  }

  private static void startRollingUpgrade(Path foo, Path bar,
      Path file, byte[] data,
      MiniDFSCluster cluster) throws IOException {
    final DistributedFileSystem dfs = cluster.getFileSystem();

    //start rolling upgrade
    dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    dfs.rollingUpgrade(RollingUpgradeAction.PREPARE);
    dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

    dfs.mkdirs(bar);
    Assert.assertTrue(dfs.exists(foo));
    Assert.assertTrue(dfs.exists(bar));

    //truncate a file
    final int newLength = ThreadLocalRandom.current().nextInt(data.length - 1)
        + 1;
    dfs.truncate(file, newLength);
    TestFileTruncate.checkBlockRecovery(file, dfs);
    AppendTestUtil.checkFullFile(dfs, file, newLength, data);
  }

  private static void rollbackRollingUpgrade(Path foo, Path bar,
      Path file, byte[] data,
      MiniDFSCluster cluster) throws IOException {
    final DataNodeProperties dnprop = cluster.stopDataNode(0);
    cluster.restartNameNode("-rollingUpgrade", "rollback");
    cluster.restartDataNode(dnprop, true);

    final DistributedFileSystem dfs = cluster.getFileSystem();
    Assert.assertTrue(dfs.exists(foo));
    Assert.assertFalse(dfs.exists(bar));
    AppendTestUtil.checkFullFile(dfs, file, data.length, data);
  }

  @Test
  public void testDFSAdminDatanodeUpgradeControlCommands() throws Exception {
    // start a cluster
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      final DFSAdmin dfsadmin = new DFSAdmin(conf);
      DataNode dn = cluster.getDataNodes().get(0);

      // check the datanode
      final String dnAddr = dn.getDatanodeId().getIpcAddr(false);
      final String[] args1 = {"-getDatanodeInfo", dnAddr};
      runCmd(dfsadmin, true, args1);

      // issue shutdown to the datanode.
      final String[] args2 = {"-shutdownDatanode", dnAddr, "upgrade" };
      runCmd(dfsadmin, true, args2);

      // the datanode should be down.
      GenericTestUtils.waitForThreadTermination(
          "Async datanode shutdown thread", 100, 10000);
      Assert.assertFalse("DataNode should exit", dn.isDatanodeUp());

      // ping should fail.
      assertEquals(-1, dfsadmin.run(args1));
    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testFinalize() throws Exception {
    testFinalize(2);
  }

  @Test(timeout = 300000)
  public void testFinalizeWithMultipleNN() throws Exception {
    testFinalize(3);
  }

  @Test(timeout = 300000)
  public void testFinalizeWithDeltaCheck() throws Exception {
    testFinalize(2, true);
  }

  @Test(timeout = 300000)
  public void testFinalizeWithMultipleNNDeltaCheck() throws Exception {
    testFinalize(3, true);
  }

  private void testFinalize(int nnCount) throws Exception {
    testFinalize(nnCount, false);
  }

  private void testFinalize(int nnCount, boolean skipImageDeltaCheck)
      throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniQJMHACluster cluster = null;
    final Path foo = new Path("/foo");
    final Path bar = new Path("/bar");

    try {
      cluster = new MiniQJMHACluster.Builder(conf).setNumNameNodes(nnCount).build();
      MiniDFSCluster dfsCluster = cluster.getDfsCluster();
      dfsCluster.waitActive();

      // let other NN tail editlog every 1s
      for(int i=1; i < nnCount; i++) {
        dfsCluster.getConfiguration(i).setInt(
            DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
      }
      dfsCluster.restartNameNodes();

      dfsCluster.transitionToActive(0);

      dfsCluster.getNameNode(0).getHttpServer()
          .setAttribute(RECENT_IMAGE_CHECK_ENABLED, skipImageDeltaCheck);

      DistributedFileSystem dfs = dfsCluster.getFileSystem(0);
      dfs.mkdirs(foo);

      FSImage fsimage = dfsCluster.getNamesystem(0).getFSImage();

      // start rolling upgrade
      RollingUpgradeInfo info = dfs
          .rollingUpgrade(RollingUpgradeAction.PREPARE);
      Assert.assertTrue(info.isStarted());
      dfs.mkdirs(bar);

      queryForPreparation(dfs);

      // The NN should have a copy of the fsimage in case of rollbacks.
      Assert.assertTrue(fsimage.hasRollbackFSImage());

      info = dfs.rollingUpgrade(RollingUpgradeAction.FINALIZE);
      Assert.assertTrue(info.isFinalized());
      Assert.assertTrue(dfs.exists(foo));

      // Once finalized, there should be no more fsimage for rollbacks.
      Assert.assertFalse(fsimage.hasRollbackFSImage());

      // Should have no problem in restart and replaying edits that include
      // the FINALIZE op.
      dfsCluster.restartNameNode(0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test (timeout = 300000)
  public void testQuery() throws Exception {
    testQuery(2);
  }

  @Test (timeout = 300000)
  public void testQueryWithMultipleNN() throws Exception {
    testQuery(3);
  }

  private void testQuery(int nnCount) throws Exception{
    final Configuration conf = new Configuration();
    MiniQJMHACluster cluster = null;
    try {
      cluster = new MiniQJMHACluster.Builder(conf).setNumNameNodes(nnCount).build();
      MiniDFSCluster dfsCluster = cluster.getDfsCluster();
      dfsCluster.waitActive();

      dfsCluster.transitionToActive(0);
      DistributedFileSystem dfs = dfsCluster.getFileSystem(0);

      // shutdown other NNs
      for (int i = 1; i < nnCount; i++) {
        dfsCluster.shutdownNameNode(i);
      }

      // start rolling upgrade
      RollingUpgradeInfo info = dfs
          .rollingUpgrade(RollingUpgradeAction.PREPARE);
      Assert.assertTrue(info.isStarted());

      info = dfs.rollingUpgrade(RollingUpgradeAction.QUERY);
      Assert.assertFalse(info.createdRollbackImages());

      // restart other NNs
      for (int i = 1; i < nnCount; i++) {
        dfsCluster.restartNameNode(i);
      }
      // check that one of the other NNs has created the rollback image and uploaded it
      queryForPreparation(dfs);

      // The NN should have a copy of the fsimage in case of rollbacks.
      Assert.assertTrue(dfsCluster.getNamesystem(0).getFSImage()
              .hasRollbackFSImage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test (timeout = 300000)
  public void testQueryAfterRestart() throws IOException, InterruptedException {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      // start rolling upgrade
      dfs.rollingUpgrade(RollingUpgradeAction.PREPARE);
      queryForPreparation(dfs);
      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      dfs.saveNamespace();
      dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      cluster.restartNameNodes();
      dfs.rollingUpgrade(RollingUpgradeAction.QUERY);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 300000)
  public void testCheckpoint() throws IOException, InterruptedException {
    testCheckpoint(2);
  }

  @Test(timeout = 300000)
  public void testCheckpointWithMultipleNN() throws IOException, InterruptedException {
    testCheckpoint(3);
  }

  @Test(timeout = 60000)
  public void testRollBackImage() throws Exception {
    final Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 10);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 2);
    MiniQJMHACluster cluster = null;
    CheckpointFaultInjector old = CheckpointFaultInjector.getInstance();
    try {
      cluster = new MiniQJMHACluster.Builder(conf).setNumNameNodes(2).build();
      MiniDFSCluster dfsCluster = cluster.getDfsCluster();
      dfsCluster.waitActive();
      dfsCluster.transitionToActive(0);
      DistributedFileSystem dfs = dfsCluster.getFileSystem(0);
      for (int i = 0; i <= 10; i++) {
        Path foo = new Path("/foo" + i);
        dfs.mkdirs(foo);
      }
      cluster.getDfsCluster().getNameNodeRpc(0).rollEdits();
      CountDownLatch ruEdit = new CountDownLatch(1);
      CheckpointFaultInjector.set(new CheckpointFaultInjector() {
        @Override
        public void duringUploadInProgess()
            throws IOException, InterruptedException {
          if (ruEdit.getCount() == 1) {
            ruEdit.countDown();
            Thread.sleep(180000);
          }
        }
      });
      ruEdit.await();
      RollingUpgradeInfo info = dfs
          .rollingUpgrade(RollingUpgradeAction.PREPARE);
      Assert.assertTrue(info.isStarted());
      FSImage fsimage = dfsCluster.getNamesystem(0).getFSImage();
      queryForPreparation(dfs);
      // The NN should have a copy of the fsimage in case of rollbacks.
      Assert.assertTrue(fsimage.hasRollbackFSImage());
    } finally {
      CheckpointFaultInjector.set(old);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void testCheckpoint(int nnCount) throws IOException, InterruptedException {
    final Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 1);

    MiniQJMHACluster cluster = null;
    final Path foo = new Path("/foo");

    try {
      cluster = new MiniQJMHACluster.Builder(conf).setNumNameNodes(nnCount).build();
      MiniDFSCluster dfsCluster = cluster.getDfsCluster();
      dfsCluster.waitActive();

      dfsCluster.transitionToActive(0);
      DistributedFileSystem dfs = dfsCluster.getFileSystem(0);

      // start rolling upgrade
      RollingUpgradeInfo info = dfs
          .rollingUpgrade(RollingUpgradeAction.PREPARE);
      Assert.assertTrue(info.isStarted());

      queryForPreparation(dfs);

      dfs.mkdirs(foo);
      long txid = dfs.rollEdits();
      Assert.assertTrue(txid > 0);

      for(int i=1; i< nnCount; i++) {
        verifyNNCheckpoint(dfsCluster, txid, i);
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Verify that the namenode at the given index has an FSImage with a TxId up to txid-1
   */
  private void verifyNNCheckpoint(MiniDFSCluster dfsCluster, long txid, int nnIndex) throws InterruptedException {
    int retries = 0;
    while (++retries < 5) {
      NNStorage storage = dfsCluster.getNamesystem(nnIndex).getFSImage()
              .getStorage();
      if (storage.getFsImageName(txid - 1) != null) {
        return;
      }
      Thread.sleep(1000);
    }
    Assert.fail("new checkpoint does not exist");
  }

  static void queryForPreparation(DistributedFileSystem dfs) throws IOException,
      InterruptedException {
    RollingUpgradeInfo info;
    int retries = 0;
    while (++retries < 10) {
      info = dfs.rollingUpgrade(RollingUpgradeAction.QUERY);
      if (info.createdRollbackImages()) {
        break;
      }
      Thread.sleep(1000);
    }

    if (retries >= 10) {
      Assert.fail("Query return false");
    }
  }

  /**
   * In non-HA setup, after rolling upgrade prepare, the Secondary NN should
   * still be able to do checkpoint
   */
  @Test
  public void testCheckpointWithSNN() throws Exception {
    MiniDFSCluster cluster = null;
    DistributedFileSystem dfs = null;
    SecondaryNameNode snn = null;

    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
          "0.0.0.0:0");
      snn = new SecondaryNameNode(conf);

      dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/test/foo"));

      snn.doCheckpoint();

      //start rolling upgrade
      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      dfs.rollingUpgrade(RollingUpgradeAction.PREPARE);
      dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      dfs.mkdirs(new Path("/test/bar"));
      // do checkpoint in SNN again
      snn.doCheckpoint();
    } finally {
      IOUtils.cleanupWithLogger(null, dfs);
      if (snn != null) {
        snn.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
