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
package org.apache.hadoop.fs.viewfs;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Make sure that ViewFs works when the root of an FS is mounted to a ViewFs
 * mount point.
 */
public class TestViewFsAtHdfsRoot extends ViewFsBaseTest {
  
  private static MiniDFSCluster cluster;
  private static HdfsConfiguration CONF = new HdfsConfiguration();
  private static FileContext fc;
  
  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {
    SupportsBlocks = true;
    CONF.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);

    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    cluster.waitClusterUp();
    fc = FileContext.getFileContext(cluster.getURI(0), CONF);
  }

      
  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    cluster.shutdown();   
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fcTarget = fc;
    super.setUp();    
  }
  
  /**
   * Override this so that we don't set the targetTestRoot to any path under the
   * root of the FS, and so that we don't try to delete the test dir, but rather
   * only its contents.
   */
  @Override
  void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fc.makeQualified(new Path("/"));
    RemoteIterator<FileStatus> dirContents = fc.listStatus(targetTestRoot);
    while (dirContents.hasNext()) {
      fc.delete(dirContents.next().getPath(), true);
    }
  }
  
  /**
   * This overrides the default implementation since hdfs does have delegation
   * tokens.
   */
  @Override
  int getExpectedDelegationTokenCount() {
    return 8;
  }
}
