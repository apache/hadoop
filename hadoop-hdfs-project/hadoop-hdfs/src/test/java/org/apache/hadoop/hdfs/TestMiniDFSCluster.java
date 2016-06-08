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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.MiniDFSCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
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
    String oldPrp = System.getProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
    System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      File testDataCluster1 = new File(testDataPath, CLUSTER_1);
      String c1Path = testDataCluster1.getAbsolutePath();
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
      cluster = new MiniDFSCluster.Builder(conf).build();
      assertEquals(new File(c1Path + "/data"),
          new File(cluster.getDataDirectory()));
    } finally {
      if (oldPrp != null) {
        System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, oldPrp);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
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

  @Test
  public void testClusterSetDatanodeDifferentStorageType() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    StorageType[][] storageType = new StorageType[][] {
        {StorageType.DISK, StorageType.ARCHIVE}, {StorageType.DISK},
        {StorageType.ARCHIVE}};
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).storageTypes(storageType).build();
    try {
      cluster.waitActive();
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      // Check the number of directory in DN's
      for (int i = 0; i < storageType.length; i++) {
        assertEquals(DataNode.getStorageLocations(dataNodes.get(i).getConf())
            .size(), storageType[i].length);
      }
    } finally {
      MiniDFSCluster.shutdownCluster(cluster);
    }
  }

  @Test
  public void testClusterNoStorageTypeSetForDatanodes() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
    try {
      cluster.waitActive();
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      // Check the number of directory in DN's
      for (DataNode datanode : dataNodes) {
        assertEquals(DataNode.getStorageLocations(datanode.getConf()).size(),
            2);
      }
    } finally {
      MiniDFSCluster.shutdownCluster(cluster);
    }
  }

  @Test
  public void testSetUpFederatedCluster() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster  cluster =
            new MiniDFSCluster.Builder(conf).nnTopology(
                    MiniDFSNNTopology.simpleHAFederatedTopology(2))
                .numDataNodes(2)
                .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(1);
      cluster.transitionToActive(3);
      assertEquals("standby", cluster.getNamesystem(0).getHAState());
      assertEquals("active", cluster.getNamesystem(1).getHAState());
      assertEquals("standby", cluster.getNamesystem(2).getHAState());
      assertEquals("active", cluster.getNamesystem(3).getHAState());

      String ns0nn0 = conf.get(
          DFSUtil.addKeySuffixes(DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns0", "nn0"));
      String ns0nn1 = conf.get(
          DFSUtil.addKeySuffixes(DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns0", "nn1"));
      String ns1nn0 = conf.get(
          DFSUtil.addKeySuffixes(DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns1", "nn0"));
      String ns1nn1 = conf.get(
          DFSUtil.addKeySuffixes(DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns1", "nn1"));

      for(NameNodeInfo nnInfo : cluster.getNameNodeInfos()) {
        assertEquals(ns0nn0, nnInfo.conf.get(
            DFSUtil.addKeySuffixes(
            DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns0", "nn0")));
        assertEquals(ns0nn1, nnInfo.conf.get(
            DFSUtil.addKeySuffixes(
            DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns0", "nn1")));
        assertEquals(ns1nn0, nnInfo.conf.get(
            DFSUtil.addKeySuffixes(
            DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns1", "nn0")));
        assertEquals(ns1nn1, nnInfo.conf.get(
            DFSUtil.addKeySuffixes(
            DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns1", "nn1")));
      }
    } finally {
      MiniDFSCluster.shutdownCluster(cluster);
    }
  }
}
