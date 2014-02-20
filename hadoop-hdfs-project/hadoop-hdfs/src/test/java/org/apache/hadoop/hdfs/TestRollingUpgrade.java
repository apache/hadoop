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
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.Assert;
import org.junit.Test;


/**
 * This class tests rolling upgrade.
 */
public class TestRollingUpgrade {
  private static final Log LOG = LogFactory.getLog(TestRollingUpgrade.class);

  private void runCmd(DFSAdmin dfsadmin, String... args) throws Exception {
    Assert.assertEquals(0, dfsadmin.run(args));
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

        {
          //illegal argument
          final String[] args = {"-rollingUpgrade", "abc"};
          Assert.assertTrue(dfsadmin.run(args) != 0);
        }

        //query rolling upgrade
        runCmd(dfsadmin, "-rollingUpgrade");

        //start rolling upgrade
        runCmd(dfsadmin, "-rollingUpgrade", "start");

        //query rolling upgrade
        runCmd(dfsadmin, "-rollingUpgrade", "query");

        dfs.mkdirs(bar);
        
        //finalize rolling upgrade
        runCmd(dfsadmin, "-rollingUpgrade", "finalize");

        dfs.mkdirs(baz);

        runCmd(dfsadmin, "-rollingUpgrade");

        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
        Assert.assertTrue(dfs.exists(baz));

        dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
        dfs.saveNamespace();
        dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      }

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
        info1 = dfs.rollingUpgrade(RollingUpgradeAction.START);
        LOG.info("START\n" + info1);

        //query rolling upgrade
        Assert.assertEquals(info1, dfs.rollingUpgrade(RollingUpgradeAction.QUERY));
  
        dfs.mkdirs(bar);
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
      Assert.assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));

      dfs2.mkdirs(baz);

      LOG.info("RESTART cluster 2");
      cluster2.restartNameNode();
      Assert.assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));
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
      Assert.assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));
      Assert.assertTrue(dfs2.exists(foo));
      Assert.assertTrue(dfs2.exists(bar));
      Assert.assertTrue(dfs2.exists(baz));

      //finalize rolling upgrade
      final RollingUpgradeInfo finalize = dfs2.rollingUpgrade(RollingUpgradeAction.FINALIZE);
      LOG.info("FINALIZE: " + finalize);
      Assert.assertEquals(info1.getStartTime(), finalize.getStartTime());

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

  @Test
  public void testRollback() throws IOException {
    // start a cluster 
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Path foo = new Path("/foo");
      final Path bar = new Path("/bar");

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        dfs.mkdirs(foo);
  
        //start rolling upgrade
        dfs.rollingUpgrade(RollingUpgradeAction.START);
  
        dfs.mkdirs(bar);
        
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
      }

      // Restart should succeed!
      cluster.restartNameNode();

      cluster.restartNameNode("-rollingUpgrade", "rollback");
      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertFalse(dfs.exists(bar));
      }
    } finally {
      if(cluster != null) cluster.shutdown();
    }
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
      Assert.assertEquals(0, dfsadmin.run(args1));

      // issue shutdown to the datanode.
      final String[] args2 = {"-shutdownDatanode", dnAddr, "upgrade" };
      Assert.assertEquals(0, dfsadmin.run(args2));

      // the datanode should be down.
      Thread.sleep(2000);
      Assert.assertFalse("DataNode should exit", dn.isDatanodeUp());

      // ping should fail.
      Assert.assertEquals(-1, dfsadmin.run(args1));
    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }

  @Test
  public void testDowngrade() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniQJMHACluster cluster = null;
    final Path foo = new Path("/foo");
    final Path bar = new Path("/bar");

    try {
      cluster = new MiniQJMHACluster.Builder(conf).build();
      MiniDFSCluster dfsCluster = cluster.getDfsCluster();
      dfsCluster.waitActive();

      dfsCluster.transitionToActive(0);
      DistributedFileSystem dfs = dfsCluster.getFileSystem(0);
      dfs.mkdirs(foo);

      // start rolling upgrade
      RollingUpgradeInfo info = dfs.rollingUpgrade(RollingUpgradeAction.START);
      Assert.assertTrue(info.isStarted());
      dfs.mkdirs(bar);
      dfs.close();

      dfsCluster.restartNameNode(0, true, "-rollingUpgrade", "downgrade");
      // shutdown NN1
      dfsCluster.shutdownNameNode(1);
      dfsCluster.transitionToActive(0);

      dfs = dfsCluster.getFileSystem(0);
      Assert.assertTrue(dfs.exists(foo));
      Assert.assertTrue(dfs.exists(bar));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static boolean existRollbackFsImage(NNStorage storage)
      throws IOException {
    final FilenameFilter filter = new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return name.indexOf(NNStorage.NameNodeFile.IMAGE_ROLLBACK.getName()) != -1;
      }
    };
    for (int i = 0; i < storage.getNumStorageDirs(); i++) {
      File dir = storage.getStorageDir(i).getCurrentDir();
      int l = dir.list(filter).length;
      if (l > 0) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testFinalize() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniQJMHACluster cluster = null;
    final Path foo = new Path("/foo");
    final Path bar = new Path("/bar");

    try {
      cluster = new MiniQJMHACluster.Builder(conf).build();
      MiniDFSCluster dfsCluster = cluster.getDfsCluster();
      dfsCluster.waitActive();

      dfsCluster.transitionToActive(0);
      DistributedFileSystem dfs = dfsCluster.getFileSystem(0);
      dfs.mkdirs(foo);

      NNStorage storage = dfsCluster.getNamesystem(0).getFSImage()
          .getStorage();

      // start rolling upgrade
      RollingUpgradeInfo info = dfs.rollingUpgrade(RollingUpgradeAction.START);
      Assert.assertTrue(info.isStarted());
      dfs.mkdirs(bar);
      // The NN should have a copy of the fsimage in case of rollbacks.
      Assert.assertTrue(existRollbackFsImage(storage));

      info = dfs.rollingUpgrade(RollingUpgradeAction.FINALIZE);
      Assert.assertTrue(info.isFinalized());
      Assert.assertTrue(dfs.exists(foo));

      // Once finalized, there should be no more fsimage for rollbacks.
      Assert.assertFalse(existRollbackFsImage(storage));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
