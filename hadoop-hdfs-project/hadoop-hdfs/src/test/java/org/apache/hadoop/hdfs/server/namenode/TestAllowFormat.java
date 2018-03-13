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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.TestGenericJournalConf.DummyJournalManager;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Startup and format tests
 * 
 */
public class TestAllowFormat {
  public static final String NAME_NODE_HOST = "localhost:";
  public static final String NAME_NODE_HTTP_HOST = "0.0.0.0:";
  private static final Log LOG =
    LogFactory.getLog(TestAllowFormat.class.getName());
  private static final File DFS_BASE_DIR = new File(PathUtils.getTestDir(TestAllowFormat.class), "dfs");
  private static Configuration config;
  private static MiniDFSCluster cluster = null;

  @BeforeClass
  public static void setUp() throws Exception {
    config = new Configuration();
    if ( DFS_BASE_DIR.exists() && !FileUtil.fullyDelete(DFS_BASE_DIR) ) {
      throw new IOException("Could not delete hdfs directory '" + DFS_BASE_DIR +
                            "'");
    }
    
    // Test has multiple name directories.
    // Format should not really prompt us if one of the directories exist,
    // but is empty. So in case the test hangs on an input, it means something
    // could be wrong in the format prompting code. (HDFS-1636)
    LOG.info("hdfsdir is " + DFS_BASE_DIR.getAbsolutePath());
    File nameDir1 = new File(DFS_BASE_DIR, "name1");
    File nameDir2 = new File(DFS_BASE_DIR, "name2");

    // To test multiple directory handling, we pre-create one of the name directories.
    nameDir1.mkdirs();

    // Set multiple name directories.
    config.set(DFS_NAMENODE_NAME_DIR_KEY, nameDir1.getPath() + "," + nameDir2.getPath());
    config.set(DFS_DATANODE_DATA_DIR_KEY, new File(DFS_BASE_DIR, "data").getPath());

    config.set(DFS_NAMENODE_CHECKPOINT_DIR_KEY,new File(DFS_BASE_DIR, "secondary").getPath());

    FileSystem.setDefaultUri(config, "hdfs://"+NAME_NODE_HOST + "0");
  }

  /**
   * clean up
   */
  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster!=null) {
      cluster.shutdown();
      LOG.info("Stopping mini cluster");
    }
    
    if ( DFS_BASE_DIR.exists() && !FileUtil.fullyDelete(DFS_BASE_DIR) ) {
      throw new IOException("Could not delete hdfs directory in tearDown '"
                            + DFS_BASE_DIR + "'");
    }	
  }

   /**
   * start MiniDFScluster, try formatting with different settings
   * @throws IOException
   * @throws InterruptedException 
   */
  @Test
  public void testAllowFormat() throws IOException {
    LOG.info("--starting mini cluster");
    // manage dirs parameter set to false 

    NameNode nn;
    // 1. Create a new cluster and format DFS
    config.setBoolean(DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY, true);
    cluster = new MiniDFSCluster.Builder(config).manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .build();
    cluster.waitActive();
    assertNotNull(cluster);

    nn = cluster.getNameNode();
    assertNotNull(nn);
    LOG.info("Mini cluster created OK");
    
    // 2. Try formatting DFS with allowformat false.
    // NOTE: the cluster must be shut down for format to work.
    LOG.info("Verifying format will fail with allowformat false");
    config.setBoolean(DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY, false);
    try {
      cluster.shutdown();
      NameNode.format(config);
      fail("Format succeeded, when it should have failed");
    } catch (IOException e) { // expected to fail
      // Verify we got message we expected
      assertTrue("Exception was not about formatting Namenode", 
          e.getMessage().startsWith("The option " + 
                                    DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY));
      LOG.info("Expected failure: " + StringUtils.stringifyException(e));
      LOG.info("Done verifying format will fail with allowformat false");
    }
    // 3. Try formatting DFS with allowformat true
    LOG.info("Verifying format will succeed with allowformat true");
    config.setBoolean(DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY, true);
    NameNode.format(config);
    LOG.info("Done verifying format will succeed with allowformat true");
  }

  /**
   * Test to skip format for non file scheme directory configured
   *
   * @throws Exception
   */
  @Test
  public void testFormatShouldBeIgnoredForNonFileBasedDirs() throws Exception {
    Configuration conf = new HdfsConfiguration();
    String logicalName = "mycluster";

    // DFS_NAMENODE_RPC_ADDRESS_KEY are required to identify the NameNode
    // is configured in HA, then only DFS_NAMENODE_SHARED_EDITS_DIR_KEY
    // is considered.
    String localhost = "127.0.0.1";
    InetSocketAddress nnAddr1 = new InetSocketAddress(localhost, 8020);
    InetSocketAddress nnAddr2 = new InetSocketAddress(localhost, 8020);
    HATestUtil.setFailoverConfigurations(conf, logicalName, nnAddr1, nnAddr2);

    conf.set(DFS_NAMENODE_NAME_DIR_KEY,
        new File(DFS_BASE_DIR, "name").getAbsolutePath());
    conf.setBoolean(DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY, true);
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX, "dummy"),
        DummyJournalManager.class.getName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, "dummy://"
        + localhost + ":2181/ledgers");
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");

    // An internal assert is added to verify the working of test
    NameNode.format(conf);
  }
}
