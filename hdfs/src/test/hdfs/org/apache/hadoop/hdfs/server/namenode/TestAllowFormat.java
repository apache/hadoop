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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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
  private static Configuration config;
  private static MiniDFSCluster cluster = null;
  private static File hdfsDir=null;

  @BeforeClass
  public static void setUp() throws Exception {
    config = new Configuration();
    String baseDir = System.getProperty("test.build.data", "build/test/data");

    hdfsDir = new File(baseDir, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir +
                            "'");
    }
    LOG.info("hdfsdir is " + hdfsDir.getAbsolutePath());
    config.set(DFS_NAMENODE_NAME_DIR_KEY, new File(hdfsDir, "name").getPath());
    config.set(DFS_DATANODE_DATA_DIR_KEY, new File(hdfsDir, "data").getPath());

    config.set(DFS_NAMENODE_CHECKPOINT_DIR_KEY,new File(hdfsDir, "secondary").getPath());

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
    
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory in tearDown '"
                            + hdfsDir + "'");
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
}
