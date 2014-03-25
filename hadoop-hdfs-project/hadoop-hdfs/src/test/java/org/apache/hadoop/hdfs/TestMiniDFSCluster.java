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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.test.PathUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests MiniDFS cluster setup/teardown and isolation.
 * Every instance is brought up with a new data dir, to ensure that
 * shutdown work in background threads don't interfere with bringing up
 * the new cluster.
 */
public class TestMiniDFSCluster {

  private static final String CLUSTER_1 = "cluster1";
  private static final String CLUSTER_2 = "cluster2";
  private static final String CLUSTER_3 = "cluster3";
  private static final String CLUSTER_4 = "cluster4";
  private static final String CLUSTER_5 = "cluster5";
  protected File testDataPath;
  @Before
  public void setUp() {
    testDataPath = new File(PathUtils.getTestDir(getClass()), "miniclusters");
  }

  /**
   * Verify that without system properties the cluster still comes up, provided
   * the configuration is set
   *
   * @throws Throwable on a failure
   */
  @Test(timeout=100000)
  public void testClusterWithoutSystemProperties() throws Throwable {
    System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
    Configuration conf = new HdfsConfiguration();
    File testDataCluster1 = new File(testDataPath, CLUSTER_1);
    String c1Path = testDataCluster1.getAbsolutePath();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      assertEquals(new File(c1Path + "/data"),
          new File(cluster.getDataDirectory()));
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Bring up two clusters and assert that they are in different directories.
   * @throws Throwable on a failure
   */
  @Test(timeout=100000)
  public void testDualClusters() throws Throwable {
    File testDataCluster2 = new File(testDataPath, CLUSTER_2);
    File testDataCluster3 = new File(testDataPath, CLUSTER_3);
    Configuration conf = new HdfsConfiguration();
    String c2Path = testDataCluster2.getAbsolutePath();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c2Path);
    MiniDFSCluster cluster2 = new MiniDFSCluster.Builder(conf).build();
    MiniDFSCluster cluster3 = null;
    try {
      String dataDir2 = cluster2.getDataDirectory();
      assertEquals(new File(c2Path + "/data"), new File(dataDir2));
      //change the data dir
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
               testDataCluster3.getAbsolutePath());
      MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
      cluster3 = builder.build();
      String dataDir3 = cluster3.getDataDirectory();
      assertTrue("Clusters are bound to the same directory: " + dataDir2,
                        !dataDir2.equals(dataDir3));
    } finally {
      MiniDFSCluster.shutdownCluster(cluster3);
      MiniDFSCluster.shutdownCluster(cluster2);
    }
  }

  @Test(timeout=100000)
  public void testIsClusterUpAfterShutdown() throws Throwable {
    Configuration conf = new HdfsConfiguration();
    File testDataCluster4 = new File(testDataPath, CLUSTER_4);
    String c4Path = testDataCluster4.getAbsolutePath();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c4Path);
    MiniDFSCluster cluster4 = new MiniDFSCluster.Builder(conf).build();
    try {
      DistributedFileSystem dfs = cluster4.getFileSystem();
      dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
      cluster4.shutdown();
    } finally {
      while(cluster4.isClusterUp()){
        Thread.sleep(1000);
      }  
    }
  }

  /** MiniDFSCluster should not clobber dfs.datanode.hostname if requested */
  @Test(timeout=100000)
  public void testClusterSetDatanodeHostname() throws Throwable {
    assumeTrue(System.getProperty("os.name").startsWith("Linux"));
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "MYHOST");
    File testDataCluster5 = new File(testDataPath, CLUSTER_5);
    String c5Path = testDataCluster5.getAbsolutePath();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c5Path);
    MiniDFSCluster cluster5 = new MiniDFSCluster.Builder(conf)
      .numDataNodes(1)
      .checkDataNodeHostConfig(true)
      .build();
    try {
      assertEquals("DataNode hostname config not respected", "MYHOST",
          cluster5.getDataNodes().get(0).getDatanodeId().getHostName());
    } finally {
      MiniDFSCluster.shutdownCluster(cluster5);
    }
  }
}
