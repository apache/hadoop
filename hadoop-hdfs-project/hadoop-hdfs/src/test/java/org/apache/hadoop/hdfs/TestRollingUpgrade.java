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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
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
    return conf;
  }

  @Test (timeout = 30000)
  public void testRollingUpgradeWithQJM() throws Exception {
    String nnDirPrefix = MiniDFSCluster.getBaseDirectory() + "/nn/";
    final File nn1Dir = new File(nnDirPrefix + "image1");
    final File nn2Dir = new File(nnDirPrefix + "image2");
    
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

      final RollingUpgradeInfo info1;
      {
        final DistributedFileSystem dfs = cluster.getFileSystem(); 
        dfs.mkdirs(foo);
  
        //start rolling upgrade
        info1 = dfs.rollingUpgrade(RollingUpgradeAction.START);
        LOG.info("start " + info1);

        //query rolling upgrade
        Assert.assertEquals(info1, dfs.rollingUpgrade(RollingUpgradeAction.QUERY));
  
        dfs.mkdirs(bar);
      }

      // cluster2 takes over QJM
      final Configuration conf2 = setConf(new Configuration(), nn2Dir, mjc);
      //set startup option to downgrade for ignoring upgrade marker in editlog
      StartupOption.ROLLINGUPGRADE.setRollingUpgradeStartupOption("started");
      cluster2 = new MiniDFSCluster.Builder(conf2)
        .numDataNodes(0)
        .format(false)
        .manageNameDfsDirs(false)
        .startupOption(StartupOption.ROLLINGUPGRADE)
        .build();
      final DistributedFileSystem dfs2 = cluster2.getFileSystem(); 
      
      // Check that cluster2 sees the edits made on cluster1
      Assert.assertTrue(dfs2.exists(foo));
      Assert.assertTrue(dfs2.exists(bar));

      //query rolling upgrade in cluster2
      Assert.assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));

      LOG.info("finalize: " + dfs2.rollingUpgrade(RollingUpgradeAction.FINALIZE));
    } finally {
      if (cluster2 != null) cluster2.shutdown();
    }
  }
}
