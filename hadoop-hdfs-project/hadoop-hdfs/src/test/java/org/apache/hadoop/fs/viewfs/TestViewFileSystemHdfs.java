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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;


public class TestViewFileSystemHdfs extends ViewFileSystemBaseTest {

  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static Path defaultWorkingDirectory2;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs;
  private static FileSystem fHdfs2;
  private FileSystem fsTarget2;
  Path targetTestRoot2;
  
  @Override
  protected FileSystemTestHelper createFileSystemHelper() {
    return new FileSystemTestHelper("/tmp/TestViewFileSystemHdfs");
  }

  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {
    SupportsBlocks = true;
    CONF.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    
    cluster =
        new MiniDFSCluster.Builder(CONF).nnTopology(
                MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2)
            .build();
    cluster.waitClusterUp();
    
    fHdfs = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
    fHdfs.getConf().set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_URI.toString());
    fHdfs2.getConf().set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_URI.toString());

    defaultWorkingDirectory = fHdfs.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    defaultWorkingDirectory2 = fHdfs2.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    
    fHdfs.mkdirs(defaultWorkingDirectory);
    fHdfs2.mkdirs(defaultWorkingDirectory2);
  }

      
  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    cluster.shutdown();   
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fsTarget = fHdfs;
    fsTarget2 = fHdfs2;
    targetTestRoot2 = new FileSystemTestHelper().getAbsoluteTestRootPath(fsTarget2);
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  void setupMountPoints() {
    super.setupMountPoints();
    ConfigUtil.addLink(conf, "/mountOnNn2", new Path(targetTestRoot2,
        "mountOnNn2").toUri());
  }

  // Overriden test helper methods - changed values based on hdfs and the
  // additional mount.
  @Override
  int getExpectedDirPaths() {
    return 8;
  }
  
  @Override
  int getExpectedMountPoints() {
    return 9;
  }

  @Override
  int getExpectedDelegationTokenCount() {
    return 2; // Mount points to 2 unique hdfs 
  }

  @Override
  int getExpectedDelegationTokenCountWithCredentials() {
    return 2;
  }
}
