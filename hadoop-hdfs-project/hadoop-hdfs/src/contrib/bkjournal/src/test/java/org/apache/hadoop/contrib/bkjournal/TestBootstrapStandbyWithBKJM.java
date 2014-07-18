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
package org.apache.hadoop.contrib.bkjournal;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.ha.BootstrapStandby;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.TestStandbyCheckpoints.SlowCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestBootstrapStandbyWithBKJM {
  private static BKJMUtil bkutil;
  protected MiniDFSCluster cluster;

  @BeforeClass
  public static void setupBookkeeper() throws Exception {
    bkutil = new BKJMUtil(3);
    bkutil.start();
  }

  @AfterClass
  public static void teardownBookkeeper() throws Exception {
    bkutil.teardown();
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 5);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, BKJMUtil
        .createJournalURI("/bootstrapStandby").toString());
    BKJMUtil.addJournalManagerDefinition(conf);
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
    conf.set(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
        SlowCodec.class.getCanonicalName());
    CompressionCodecFactory.setCodecClasses(conf,
        ImmutableList.<Class> of(SlowCodec.class));
    MiniDFSNNTopology topology = new MiniDFSNNTopology()
        .addNameservice(new MiniDFSNNTopology.NSConf("ns1").addNN(
            new MiniDFSNNTopology.NNConf("nn1").setHttpPort(10001)).addNN(
            new MiniDFSNNTopology.NNConf("nn2").setHttpPort(10002)));
    cluster = new MiniDFSCluster.Builder(conf).nnTopology(topology)
        .numDataNodes(1).manageNameDfsSharedDirs(false).build();
    cluster.waitActive();
  }

  /**
   * While boostrapping, in_progress transaction entries should be skipped.
   * Bootstrap usage for BKJM : "-force", "-nonInteractive", "-skipSharedEditsCheck"
   */
  @Test
  public void testBootstrapStandbyWithActiveNN() throws Exception {
    // make nn0 active
    cluster.transitionToActive(0);
   
    // do ops and generate in-progress edit log data
    Configuration confNN1 = cluster.getConfiguration(1);
    DistributedFileSystem dfs = (DistributedFileSystem) HATestUtil
        .configureFailoverFs(cluster, confNN1);
    for (int i = 1; i <= 10; i++) {
      dfs.mkdirs(new Path("/test" + i));
    }
    dfs.close();

    // shutdown nn1 and delete its edit log files
    cluster.shutdownNameNode(1);
    deleteEditLogIfExists(confNN1);
    cluster.getNameNodeRpc(0).setSafeMode(SafeModeAction.SAFEMODE_ENTER, true);
    cluster.getNameNodeRpc(0).saveNamespace();
    cluster.getNameNodeRpc(0).setSafeMode(SafeModeAction.SAFEMODE_LEAVE, true);

    // check without -skipSharedEditsCheck, Bootstrap should fail for BKJM
    // immediately after saveNamespace
    int rc = BootstrapStandby.run(new String[] { "-force", "-nonInteractive" },
      confNN1);
    Assert.assertEquals("Mismatches return code", 6, rc);

    // check with -skipSharedEditsCheck
    rc = BootstrapStandby.run(new String[] { "-force", "-nonInteractive",
        "-skipSharedEditsCheck" }, confNN1);
    Assert.assertEquals("Mismatches return code", 0, rc);

    // Checkpoint as fast as we can, in a tight loop.
    confNN1.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 1);
    cluster.restartNameNode(1);
    cluster.transitionToStandby(1);
   
    NameNode nn0 = cluster.getNameNode(0);
    HATestUtil.waitForStandbyToCatchUp(nn0, cluster.getNameNode(1));
    long expectedCheckpointTxId = NameNodeAdapter.getNamesystem(nn0)
        .getFSImage().getMostRecentCheckpointTxId();
    HATestUtil.waitForCheckpoint(cluster, 1,
        ImmutableList.of((int) expectedCheckpointTxId));

    // Should have copied over the namespace
    FSImageTestUtil.assertNNHasCheckpoints(cluster, 1,
        ImmutableList.of((int) expectedCheckpointTxId));
    FSImageTestUtil.assertNNFilesMatch(cluster);
  }

  private void deleteEditLogIfExists(Configuration confNN1) {
    String editDirs = confNN1.get(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY);
    String[] listEditDirs = StringUtils.split(editDirs, ',');
    Assert.assertTrue("Wrong edit directory path!", listEditDirs.length > 0);

    for (String dir : listEditDirs) {
      File curDir = new File(dir, "current");
      File[] listFiles = curDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File f) {
          if (!f.getName().startsWith("edits")) {
            return true;
          }
          return false;
        }
      });
      if (listFiles != null && listFiles.length > 0) {
        for (File file : listFiles) {
          Assert.assertTrue("Failed to delete edit files!", file.delete());
        }
      }
    }
  }
}
